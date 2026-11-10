"""prepare.py for stage "demonstration" (LIVE-DEMO-PLAN section 5): the same phases and refusals as the experiment's
preparation, on the demonstration's own objects, as an authorized occupant of ANOTHER run's reservation."""

import copy
import json
import os

import pytest
import yaml

import common_records as C
import execlib as X
import fake2 as F
import part2_fixtures as PF
import prepare
import reserve
from part2_fixtures import S_UID, W_UID, api, base_params, prepare_section, reserve_section  # noqa: F401

OWNER, DEMO = "c28-prod-1", "c28-demo-1"
STAGING = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
N, R, GEN = "c28-demo-pa-n", "c28-demo-pa-r", "c28-demo-generator"


def demo_params():
    with open(os.path.join(STAGING, "..", "params", "kind-demo.json")) as fh:
        p = json.load(fh)
    p["run_id"] = DEMO
    p["nodes"] = {"workload": "worker-w", "support": "worker-s"}
    p["reservation"]["taint_value"] = OWNER
    p["image"] = "harbor.example/autoscaler/c28-fixture@sha256:" + "a" * 64
    p["scrape"] = "VMPodScrape"
    p["residency"] = copy.deepcopy(PF.RESIDENCY)
    d = p["demonstration"]
    d["run_id"] = DEMO
    d["reservation_ref"] = {"run_id": OWNER, "taint_value": OWNER,
                            "nodes": {"workload": {"name": "worker-w", "uid": W_UID},
                                      "support": {"name": "worker-s", "uid": S_UID}}}
    d["shadow_replicas"] = 12
    d["schedule_origin_utc"] = "2026-11-01T00:00:00Z"
    return p


def owner_params():
    p = base_params()
    p["run_id"] = OWNER
    p["reservation"]["taint_value"] = OWNER
    return p


def demo_section(owner_path, **over):
    s = prepare_section()
    s.update({"stage": "demonstration",
              "reservation": {"record": os.path.basename(owner_path), "sha256": C.sha256_file(owner_path)}})
    s.update(over)
    return s


@pytest.fixture
def world(tmp_path, api):
    """A fake cluster whose W and S are reserved by the OWNER run (reserve.py apply with the owner's record), and a
    demonstration record that references it. Returns a builder: build(params=None, section=None, owner_extra=None)."""
    api.put({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": "retained-ssd"},
             "reclaimPolicy": "Retain"})
    api.put(F.node("worker-w", uid=W_UID))
    api.put(F.node("worker-s", uid=S_UID))
    locks = str(tmp_path / "shared-locks")

    def record_doc(run, params_file, evidence, **extra):
        doc = {"schema": C.RECORD_SCHEMA, "run_id": run, "cluster_uid": F.CLUSTER,
               "kubeconfig": {"path": "/home/ops/.kube/eval-prod.yaml", "context": "prodcluster"},
               "evidence_dir": evidence, "lock_dir": locks, "source": {"commit": PF.COMMIT}, "params": params_file}
        doc.update(extra)
        return doc

    (tmp_path / "owner-params.json").write_text(json.dumps(owner_params()))
    owner_path = tmp_path / "owner.json"
    owner_path.write_text(json.dumps(record_doc(OWNER, "owner-params.json", "evidence-owner",
                                                reserve=reserve_section())))
    owner = X.load_record(str(owner_path), ("reserve",))
    PF.plan_and_apply(reserve, owner, owner.kube(api))
    api.calls.clear()

    def build(params=None, section=None, **record_over):
        (tmp_path / "demo-params.json").write_text(json.dumps(params or demo_params()))
        doc = record_doc(DEMO, "demo-params.json", "evidence-demo",
                         prepare=section or demo_section(str(owner_path)))
        doc.update(record_over)
        (tmp_path / "demo.json").write_text(json.dumps(doc))
        record = X.load_record(str(tmp_path / "demo.json"), ("prepare",))
        return record, record.kube(api)
    build.owner, build.owner_path, build.tmp, build.api = owner, str(owner_path), tmp_path, api
    return build


def reviewed(record, kube, phase):
    p = prepare.plan(record, kube, phase)
    saved = X.save_plan(record, "prepare", p, f"plan-{phase}")
    return X.load_plan(saved["plan"]), saved["plan_digest"]


def run_phase(record, kube, phase):
    plan, digest = reviewed(record, kube, phase)
    assert plan["problems"] == [], plan["problems"]
    return prepare.apply(record, kube, phase, plan, digest)


def run_until(record, kube, last):
    for ph in prepare.DEMO_PHASES:
        run_phase(record, kube, ph)
        if ph == last:
            return


def problems(record, kube, phase):
    return prepare.plan(record, kube, phase)["problems"]


# --- the whole preparation ----------------------------------------------------------------------------------------------

def test_plans_are_read_only_and_pass_on_a_valid_reference(world):
    record, kube = world()
    run_phase(record, kube, "namespaces")
    world.api.calls.clear()
    for ph in ("namespaces", "storage", "values", "common", "targets", "pas"):
        p = prepare.plan(record, kube, ph)
        assert p["problems"] == [], (ph, p["problems"])
        assert p["stage"] == "demonstration" and p["run_id"] == DEMO
        assert p["reservation_gate"] == {"owner_run_id": OWNER, "owner_record_sha256": C.sha256_file(world.owner_path),
                                         "reservation": "PASS", "placement": "PASS",
                                         "ready_for_demonstration": True, "result": "PASS"}
    assert world.api.mutations() == []


def test_full_demonstration_preparation_in_phases(world):
    record, kube = world()
    api = world.api
    run_until(record, kube, "generator")
    targets = json.load(open(os.path.join(record.evidence_dir, "targets.json")))
    assert sorted(targets["deployments"]) == [N, R] and targets["run_id"] == DEMO
    for name in (N, R):
        dep = api.obj("Deployment", name, "demo")
        assert dep["metadata"]["uid"] == targets["deployments"][name]
        assert dep["spec"]["replicas"] == 12                                   # the fixed shadow count
        assert dep["metadata"]["labels"][X.RUN_LABEL] == DEMO
        assert dep["spec"]["template"]["spec"]["tolerations"][0]["value"] == OWNER   # the owner's taint, not retagged
        pa = api.obj("PredictiveAutoscaler", name, "demo")
        assert pa["spec"]["mode"] == "Recommend" and pa["spec"]["prediction"]["families"] == ["seasonalNaive"]
    assert api.obj("PredictiveAutoscaler", R, "demo")["spec"]["prediction"]["predictiveExtra"] == \
        {"maxReplicas": 0, "maxPercent": 0}
    kinds = {k for (k, _, _) in api.objects}
    assert "horizontalpodautoscalers" not in kinds and "scaledobjects" not in kinds
    assert len([1 for (k, _, _) in api.objects if k == "pa-new"]) == 2
    gen = api.obj("Deployment", GEN, "demo")
    assert gen["spec"]["replicas"] == 1 and gen["spec"]["strategy"] == {"type": "Recreate"}
    assert set(api.obj("ConfigMap", GEN, "demo")["data"]) == {"c28-demo.js", "config.json"}
    assert api.obj("PriorityClass", "c28-demo-fixture") is not None
    assert {v for v, _ in api.mutations()} == {"create"}
    # The nodes were never touched by the demonstration: still the owner's taint and labels, same resourceVersion path.
    assert not [c for c in api.mutations() if "/nodes" in c[1]]
    w = api.obj("Node", "worker-w")
    assert w["spec"]["taints"] == [{"key": "c28.devkuban.com/reserved", "value": OWNER, "effect": "NoSchedule"}]
    # Receipts and journal are the demonstration's own.
    assert os.path.isfile(os.path.join(record.evidence_dir, "receipts", "prepare.jsonl"))
    assert not os.path.exists(os.path.join(world.owner.evidence_dir, "receipts", "prepare.jsonl"))


def test_verify_passes_and_reports_the_fixed_shadow_count_and_the_single_generator(world):
    record, kube = world()
    api = world.api
    for ph in ("namespaces", "common", "targets", "pas", "generator"):     # no claims: the fake binds none
        run_phase(record, kube, ph)
    out = prepare.verify(record, kube)
    assert out["result"] == "PASS", out["problems"]
    api.obj("Deployment", N, "demo")["spec"]["replicas"] = 2
    api.obj("PredictiveAutoscaler", R, "demo")["spec"]["mode"] = "Active"
    api.obj("Deployment", GEN, "demo")["spec"]["replicas"] = 2
    for i in (1, 2):
        api.put(F.pod(f"{GEN}-{i}", "demo", "worker-s", labels={"app.kubernetes.io/name": GEN, X.RUN_LABEL: DEMO}))
    text = "\n".join(prepare.verify(record, kube)["problems"])
    assert f"{N} has 2 replicas, not the fixed 12" in text
    assert f"PredictiveAutoscaler/{R} is Active, not Recommend" in text
    assert "must have exactly one replica and strategy Recreate" in text
    assert "2 generator pods exist" in text and "two generators would send at once" in text


# --- the reservation is another run's, by explicit reference -------------------------------------------------------------

def test_without_the_reference_the_demonstrations_pods_are_unplanned_occupants(world):
    """Ownership is not relaxed: the owner's own verify (no occupant) still fails on a demonstration pod, and a pod
    with any other run's label fails the demonstration's gate."""
    record, kube = world()
    api = world.api
    api.put(F.pod("c28-demo-pa-n-abc", "demo", "worker-w", owner=("ReplicaSet", "c28-demo-pa-n-1"),
                  labels={X.RUN_LABEL: DEMO}))
    out = reserve.verify(world.owner, world.owner.kube(api))
    assert out["reservation"] == "FAIL" and any("demo/c28-demo-pa-n-abc" in p and "unplanned" in p
                                                for p in out["reservation_problems"])
    assert prepare.plan(record, kube, "targets")["reservation_gate"]["reservation"] == "PASS"   # by reference: accepted
    occ = reserve.verify(world.owner, world.owner.kube(api), occupant={"run_id": DEMO, "namespaces": ["demo"]})
    assert [e["class"] for e in occ["occupants"]["workload"]] == ["authorized-occupant"]
    # The run label alone is not enough: it must sit in the demonstration's namespaces.
    api.put(F.pod("lookalike", "shop", "worker-w", owner=("Deployment", "x"), labels={X.RUN_LABEL: DEMO}))
    assert any("reservation gate: workload: shop/lookalike" in p for p in problems(record, kube, "targets"))
    del api.objects[("pods", "shop", "lookalike")]
    api.put(F.pod("other-run", "demo", "worker-s", owner=("Deployment", "x"), labels={X.RUN_LABEL: "c28-other"}))
    assert any("reservation gate: support: demo/other-run" in p for p in problems(record, kube, "targets"))


def test_every_phase_requires_the_owners_live_reservation(world):
    record, kube = world()
    n = world.api.obj("Node", "worker-w")
    n["spec"]["taints"] = []
    for ph in prepare.DEMO_PHASES:
        p = prepare.plan(record, kube, ph)
        assert any("reservation gate: workload worker-w: the run taint is not exactly" in x for x in p["problems"]), ph
        assert p["reservation_gate"]["reservation"] == "FAIL"
        assert p["reservation_gate"]["ready_for_demonstration"] is False


def test_a_reservation_lost_between_review_and_apply_creates_nothing(world):
    record, kube = world()
    run_until(record, kube, "common")
    plan, digest = reviewed(record, kube, "targets")
    assert plan["problems"] == []
    world.api.calls.clear()
    world.api.obj("Node", "worker-s")["spec"]["taints"] = []
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        prepare.apply(record, kube, "targets", plan, digest)
    assert world.api.mutations() == []
    assert not os.path.exists(os.path.join(record.evidence_dir, "targets.json"))


def edit_owner(world, fn):
    doc = json.load(open(world.owner_path))
    fn(doc)
    open(world.owner_path, "w").write(json.dumps(doc))


def gate_text(world, params=None, section=None, **record_over):
    record, kube = world(params=params, section=section, **record_over)
    p = prepare.plan(record, kube, "namespaces")
    assert p["reservation_gate"]["result"] == "FAIL" and p["reservation_gate"]["ready_for_demonstration"] is False
    return "\n".join(p["problems"])


def test_the_reference_is_bound_to_the_owners_record_by_sha256(world):
    section = demo_section(world.owner_path)
    edit_owner(world, lambda d: d["reserve"]["movable"].append({"namespace": "x", "kind": "Deployment", "name": "y",
                                                                "uid": W_UID}))
    assert "reservation gate: the reservation owner's record differs from prepare.reservation.sha256" in \
        gate_text(world, section=section)
    missing = demo_section(world.owner_path)
    missing["reservation"]["record"] = "no-such-record.json"
    assert "does not exist" in gate_text(world, section=missing)


def test_the_reference_must_be_the_owners_nodes_by_name_and_uid(world):
    p = demo_params()
    p["demonstration"]["reservation_ref"]["nodes"]["workload"]["uid"] = "bbbbbbbb-0000-4000-8000-000000000009"
    assert "reservation_ref.nodes.workload (worker-w, bbbbbbbb-0000-4000-8000-000000000009) is not the owner's " \
           "reserved node" in gate_text(world, params=p)


def test_the_reference_must_name_the_record_owner_and_its_taint(world):
    p = demo_params()
    p["reservation"]["taint_value"] = "c28-prod-2"
    p["demonstration"]["reservation_ref"].update(run_id="c28-prod-2", taint_value="c28-prod-2")
    text = gate_text(world, params=p)
    assert "the referenced record is run c28-prod-1's, not the reservation owner c28-prod-2" in text
    q = demo_params()
    q["reservation"]["taint_key"] = "example.com/other"
    assert "is not the params' (example.com/other=c28-prod-1" in gate_text(world, params=q)


def test_the_owner_and_the_demonstration_share_one_lock_and_one_cluster(world):
    assert "uses another lock_dir" in gate_text(world, lock_dir=str(world.tmp / "private-locks"))
    edit_owner(world, lambda d: d.update(cluster_uid="99999999-2222-4333-8444-555555555555"))
    assert "is for another cluster" in gate_text(world, section=demo_section(world.owner_path))


def test_the_owner_must_name_the_same_release_and_control_namespace(world):
    p = demo_params()
    p["release"] = "pa-other"
    assert "name another control namespace or release" in gate_text(world, params=p)


def test_an_unverifiable_reference_never_asks_the_cluster_for_a_verdict(world):
    p = demo_params()
    p["demonstration"]["reservation_ref"]["nodes"]["support"]["uid"] = "bbbbbbbb-0000-4000-8000-000000000009"
    record, kube = world(params=p)
    world.api.calls.clear()
    plan = prepare.plan(record, kube, "namespaces")
    assert plan["reservation_gate"]["reservation"] == "UNVERIFIED"
    assert not [c for c in world.api.calls if "/nodes" in c[1]]


@pytest.mark.parametrize("section, text", [
    ({"stage": "demonstration"}, "prepare.reservation must be exactly {record, sha256}"),
    ({"stage": "demonstration", "reservation": {"record": "owner.json"}}, "prepare.reservation must be exactly"),
    ({"stage": "demonstration", "reservation": {"record": "owner.json", "sha256": "abc"}}, "prepare.reservation must be"),
    ({"stage": "rehearsal"}, "prepare.stage must be one of experiment, demonstration"),
    ({"reservation": {"record": "owner.json", "sha256": "0" * 64}}, "belongs to stage demonstration"),
])
def test_the_stage_and_its_reference_are_explicit(world, section, text):
    s = prepare_section()
    s.update(section)
    record, kube = world(section=s)
    with pytest.raises(X.StagingError, match=text):
        prepare.plan(record, kube, "namespaces")


def test_the_experiment_stage_is_unchanged_and_has_no_generator_phase(world, api):
    record, kube = world()
    assert prepare.phases_of("experiment") == prepare.PHASES and "generator" not in prepare.PHASES
    doc = json.load(open(world.owner_path))
    doc["prepare"] = prepare_section()
    open(world.owner_path, "w").write(json.dumps(doc))
    owner = X.load_record(world.owner_path, ("prepare",))
    with pytest.raises(X.StagingError, match="the generator phase belongs to stage demonstration"):
        prepare.plan(owner, owner.kube(api), "generator")
    assert set(prepare.split(owner_params())) == {"common", "targets", "pas", "writers", "generator"}
    assert prepare.split(owner_params())["generator"] == []


# --- the params stage -----------------------------------------------------------------------------------------------------

def test_the_demonstration_params_need_no_experiment_field_and_the_full_validation_is_not_used(world):
    record, kube = world()
    params = json.load(open(world.tmp / "demo-params.json"))
    assert not any(k in params for k in ("c0_rps", "hpa_cpu_target_percent", "keda"))
    with pytest.raises(X.StagingError):
        X.load_params(record)                       # the experiment's loader refuses them, as it must
    run_phase(record, kube, "namespaces")
    assert prepare.plan(record, kube, "common")["problems"] == []


@pytest.mark.parametrize("edit, text", [
    (lambda p: p["demonstration"]["cadence"].update(update_interval_seconds=30), "demonstration.cadence must be exactly"),
    (lambda p: p["demonstration"].update(schedule_origin_utc="<set at start>"), "placeholder"),
    (lambda p: p["demonstration"].update(shadow_replicas=13), "within the bounds"),
    (lambda p: p["fixture"].update(cpu_request="__CALIBRATED__"), "placeholder"),
    (lambda p: p.pop("residency"), "residency is required"),
    (lambda p: p.update(run_id="c28-demo-2"), "demonstration.run_id must be the params' run_id"),
])
def test_stage_validation_refusals_reach_prepare(world, edit, text):
    p = demo_params()
    edit(p)
    record, kube = world(params=p)
    with pytest.raises(X.StagingError, match=text):
        prepare.plan(record, kube, "namespaces")


# --- every existing refusal stays --------------------------------------------------------------------------------------------

def test_existing_names_are_refused_including_cluster_scoped_ones(world):
    record, kube = world()
    run_until(record, kube, "values")
    world.api.put({"apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClass",
                   "metadata": {"name": "c28-demo-fixture"}, "value": 0})
    assert "PriorityClass /c28-demo-fixture already exists (never adopted)" in problems(record, kube, "common")
    record, kube = world()
    world.api.put({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": N, "namespace": "demo"},
                   "spec": {"replicas": 1}})
    assert f"Deployment demo/{N} already exists (never adopted)" in problems(record, kube, "targets")
    world.api.put({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": GEN, "namespace": "demo"}, "data": {}})
    assert f"ConfigMap demo/{GEN} already exists (never adopted)" in problems(record, kube, "generator")


def test_an_existing_writer_naming_a_target_is_refused(world):
    record, kube = world()
    world.api.put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                   "metadata": {"name": "stray", "namespace": "demo"},
                   "spec": {"scaleTargetRef": {"kind": "Deployment", "name": R}}})
    assert any(f"HorizontalPodAutoscaler/demo/stray already writes demo/{R}" in p
               for p in problems(record, kube, "targets"))


def test_a_pa_not_in_recommend_is_refused_at_plan(world, monkeypatch):
    record, kube = world()
    c28 = X.import_harness()
    real = c28.demo.render

    def active(params, *a, **kw):
        parts = real(params, *a, **kw)
        parts["pas"][0]["spec"]["mode"] = "Active"
        return parts
    monkeypatch.setattr(c28.demo, "render", active)
    assert f"PredictiveAutoscaler/{N} is not in Recommend" in problems(record, kube, "pas")


def test_an_object_outside_the_c28_names_is_refused(world, monkeypatch):
    record, kube = world()
    c28 = X.import_harness()
    real = c28.demo.render

    def renamed(params, *a, **kw):
        parts = real(params, *a, **kw)
        parts["generator"][1]["metadata"]["name"] = "k6-generator"
        return parts
    monkeypatch.setattr(c28.demo, "render", renamed)
    assert "Deployment/k6-generator is not a c28-* name" in problems(record, kube, "generator")


def test_every_plan_runs_the_demonstrations_residency_audit(world):
    """Both targets at max plus overlap: 2 x 14 x 50m + ingress 100m + system 200m = 1700m of a 2-CPU W is 85 %."""
    p = demo_params()
    p["residency"]["workload"]["allocatable"]["cpu"] = "2"
    record, kube = world(params=p)
    for ph in prepare.DEMO_PHASES:
        found = problems(record, kube, ph)
        assert any(x.startswith("residency: workload node worker-w: CPU requests 1700m are 85.0%") for x in found), ph


def test_the_residency_audit_counts_the_release_as_the_final_values_request_it(world):
    p = demo_params()
    p["residency"]["support"]["allocatable"]["cpu"] = "2"        # routes 40m + generator 250m + op 100m + fc 1000m + 200m
    record, kube = world(params=p)
    found = problems(record, kube, "common")
    assert any(x.startswith("residency: support node worker-s: CPU requests 1590m are 79.5%") for x in found), found
    listing = [x for x in found if x.startswith("residency: support")][0]
    for part in ("Deployment/c28-demo-generator 1x(250m", "operator (chart) 1x(100m", "forecaster (chart) 1x(1000m",
                 "Deployment/c28-metrics-route 1x(20m"):
        assert part in listing, listing


def test_apply_refuses_a_drifted_render(world):
    record, kube = world()
    run_until(record, kube, "pas")
    plan, digest = reviewed(record, kube, "generator")
    p = json.load(open(world.tmp / "demo-params.json"))
    p["demonstration"]["schedule_origin_utc"] = "2026-11-02T00:00:00Z"       # another origin after review
    (world.tmp / "demo-params.json").write_text(json.dumps(p))
    with pytest.raises(X.StagingError):
        prepare.apply(record, kube, "generator", plan, digest)
    assert world.api.obj("Deployment", GEN, "demo") is None


# --- the values phase ---------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("scrape, on, off", [("VMPodScrape", "vmServiceScrape", "serviceMonitor"),
                                             ("PodMonitor", "serviceMonitor", "vmServiceScrape")])
def test_values_enable_exactly_the_releases_scrape_object(world, scrape, on, off):
    p = demo_params()
    p["scrape"] = scrape
    record, kube = world(params=p)
    run_until(record, kube, "values")
    v = yaml.safe_load(open(os.path.join(record.evidence_dir, "release-staging.yaml")))
    assert v["metrics"] == {on: {"enabled": True, "interval": "15s"}, off: {"enabled": False, "interval": "15s"}}
    assert v["operator"]["rbac"] == {"scaleWrite": False}                    # shadow: the scale write stays withheld
    assert v["forecaster"]["statistical"]["labQualification"]["configMap"] == ""
    assert v["operator"]["tolerations"][0]["value"] == OWNER and v["operator"]["watchNamespaces"] == ["demo"]
    assert v["prometheus"]["url"] == "http://c28-metrics-route.pa-eval-system.svc:8481"


def test_scrape_by_annotations_is_refused_for_the_release(world):
    p = demo_params()
    p["scrape"] = "annotations"
    record, kube = world(params=p)
    found = problems(record, kube, "values")
    assert any("exactly one scrape object" in x and 'scrape "annotations" is refused for the release' in x
               for x in found), found
    run_phase(record, kube, "namespaces")
    assert problems(record, kube, "common") == []                           # the fixture's own scrape may annotate


def test_values_problems_require_exactly_one_scrape_object_and_no_scale_targets(world):
    record, kube = world()
    params = prepare.load_params(record, "demonstration")
    s = prepare._section(record)
    v = prepare.staging_values(record, params, s)
    assert prepare.values_problems(v, params, "demonstration") == []
    both = copy.deepcopy(v)
    both["metrics"]["serviceMonitor"]["enabled"] = True
    none = copy.deepcopy(v)
    none["metrics"]["vmServiceScrape"]["enabled"] = False
    missing = copy.deepcopy(v)
    del missing["metrics"]
    for bad in (both, none, missing):
        assert any("exactly one scrape object" in x for x in prepare.values_problems(bad, params, "demonstration"))
    write = copy.deepcopy(v)
    write["operator"]["rbac"] = {"scaleWrite": True, "scaleTargets": [N, R]}
    assert "operator.rbac.scaleWrite must be false: a shadow preparation authorizes no replica write" in \
        prepare.values_problems(write, params, "demonstration")              # the Active transition is not prepare's
    named = copy.deepcopy(v)
    named["operator"]["rbac"] = {"scaleWrite": False, "scaleTargets": [N]}
    assert any("scaleTargets must be empty while operator.rbac.scaleWrite is false" in x
               for x in prepare.values_problems(named, params, "demonstration"))
    empty_active = copy.deepcopy(v)
    empty_active["operator"]["rbac"] = {"scaleWrite": True, "scaleTargets": []}
    assert any("scaleTargets is empty" in x for x in prepare.values_problems(empty_active, params, "demonstration"))


def test_the_experiments_overlay_is_byte_for_byte_what_it_was(world, api):
    """Stage experiment gains no metrics block and keeps its checks (the S3 draft overlay must not change)."""
    doc = json.load(open(world.owner_path))
    doc["prepare"] = prepare_section()
    open(world.owner_path, "w").write(json.dumps(doc))
    owner = X.load_record(world.owner_path, ("prepare",))
    v = prepare.staging_values(owner, X.load_params(owner), prepare._section(owner))
    assert "metrics" not in v
    assert sorted(v) == ["forecaster", "images", "networkPolicy", "operator", "prometheus", "training"]
    assert prepare.values_problems(v, X.load_params(owner)) == []


# --- the generator phase ------------------------------------------------------------------------------------------------------

def test_the_generator_needs_the_targets_and_pas_of_this_run(world):
    record, kube = world()
    run_until(record, kube, "common")
    found = problems(record, kube, "generator")
    for name in (N, R):
        assert f"Deployment demo/{name} was not created by this run or was replaced (phases targets and pas first)" in found
        assert f"PredictiveAutoscaler demo/{name} was not created by this run or was replaced (phases targets and " \
               "pas first)" in found
    run_phase(record, kube, "targets")
    run_phase(record, kube, "pas")
    assert problems(record, kube, "generator") == []
    world.api.obj("PredictiveAutoscaler", N, "demo")["spec"]["mode"] = "Active"
    assert f"PredictiveAutoscaler/{N} is Active, not Recommend" in problems(record, kube, "generator")
    world.api.obj("PredictiveAutoscaler", N, "demo")["spec"]["mode"] = "Recommend"
    world.api.obj("Deployment", R, "demo")["metadata"]["uid"] = F.new_uid()       # replaced under the same name
    assert any(f"Deployment demo/{R} was not created by this run or was replaced" in x
               for x in problems(record, kube, "generator"))


def test_an_origin_in_the_future_is_refused_at_the_generator_phase(world, monkeypatch):
    record, kube = world()
    run_until(record, kube, "pas")
    params = prepare.load_params(record, "demonstration")
    parts = prepare.split(params, "demonstration")
    early = prepare.generator_problems(record, kube, params, parts, [], now=lambda: "2026-10-31T23:59:59Z")
    assert any("is in the future" in x for x in early)
    assert prepare.generator_problems(record, kube, params, parts, [], now=lambda: "2026-11-01T00:00:00Z") == []


def test_a_second_generator_is_refused_and_a_failed_listing_is_never_no_generator(world):
    record, kube = world()
    run_until(record, kube, "pas")
    api = world.api
    api.put({"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "c28-demo-generator-old", "namespace": "demo",
                          "labels": {"app.kubernetes.io/name": GEN}}, "spec": {"replicas": 1}})
    assert "another generator exists: Deployment demo/c28-demo-generator-old (one generator only)" in \
        problems(record, kube, "generator")
    del api.objects[("deployments", "demo", "c28-demo-generator-old")]
    api.put(F.pod("leftover", "demo", "worker-s", labels={"app.kubernetes.io/name": GEN, X.RUN_LABEL: OWNER}))
    found = problems(record, kube, "generator")
    assert "another generator exists: Pod demo/leftover (one generator only)" in found
    del api.objects[("pods", "demo", "leftover")]
    assert problems(record, kube, "generator") == []
    # R2-16: a list that fails (the API answers "not found" for the collection) must raise, never read as empty.
    real = api.get

    def flaky(path):
        if path.endswith("/namespaces/demo/pods"):
            return None
        return real(path)
    api.get = flaky
    with pytest.raises(C.ApiError, match="list Pod: no list response"):
        prepare.plan(record, kube, "generator")


def test_the_generator_is_created_once_and_recorded(world):
    record, kube = world()
    run_until(record, kube, "generator")
    created = prepare.created_objects(record)
    assert ("Deployment", "demo", GEN) in created and ("ConfigMap", "demo", GEN) in created
    assert f"Deployment demo/{GEN} already exists" not in "\n".join(problems(record, kube, "generator"))   # ours
    p = prepare.plan(record, kube, "generator")
    assert p["actions"] == [] and len(p["already_created_by_this_run"]) == 2
    cfg = json.loads(world.api.obj("ConfigMap", GEN, "demo")["data"]["config.json"])
    assert cfg["origin_utc"] == "2026-11-01T00:00:00Z" and cfg["targets"] == [N, R] and cfg["run_id"] == DEMO


def test_cli_plans_every_demonstration_phase(world, capsys):
    record, kube = world()
    rc = prepare.main(["--record", record.path, "plan"], api=world.api)
    out = json.loads(capsys.readouterr().out)
    assert rc == 0 and len(out) == len(prepare.DEMO_PHASES)
    assert [os.path.basename(o["plan"]).split("-")[1] for o in out] == list(prepare.DEMO_PHASES)
