import copy
import json
import os

import pytest
import yaml

import execlib as X
import part2_fixtures as PF
import fake2 as F
import prepare
from part2_fixtures import (RUN, S_UID, W_UID, api, base_params, make_record, prepare_section,  # noqa: F401
                            reserve_section)


def setup(api, make_record, section=None, reserved=True):
    """A record with prepare and reserve sections; the reservation applied (preparation requires its live verdict)."""
    import reserve
    api.put({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": "retained-ssd"},
             "reclaimPolicy": "Retain"})
    api.put(F.node("worker-w", uid=W_UID))
    api.put(F.node("worker-s", uid=S_UID))
    record = X.load_record(make_record({"prepare": section or prepare_section(), "reserve": reserve_section()}),
                           ("prepare",))
    kube = record.kube(api)
    if reserved:
        PF.plan_and_apply(reserve, record, kube)
        api.calls.clear()                     # the call log starts after the reservation: the tests watch prepare
    return record, kube


def reviewed(record, kube, phase):
    p = prepare.plan(record, kube, phase)
    saved = X.save_plan(record, "prepare", p, f"plan-{phase}")
    return X.load_plan(saved["plan"]), saved["plan_digest"]


def run_phase(record, kube, phase):
    plan, digest = reviewed(record, kube, phase)
    assert plan["problems"] == [], plan["problems"]
    return prepare.apply(record, kube, phase, plan, digest)


def test_split_never_puts_writers_in_a_phase():
    parts = prepare.split(base_params())
    assert {o["kind"] for o in parts["targets"]} == {"Deployment", "Service", "ConfigMap"}
    assert {o["kind"] for o in parts["pas"]} == {"PredictiveAutoscaler"} and len(parts["pas"]) == 6
    assert {o["kind"] for o in parts["writers"]} == {"HorizontalPodAutoscaler", "ScaledObject"}
    assert len([o for o in parts["targets"] if o["kind"] == "Deployment"]) == 10


def test_plans_are_read_only(api, make_record):
    record, kube = setup(api, make_record)
    for ph in prepare.PHASES:
        prepare.plan(record, kube, ph)
    assert api.mutations() == []


def test_full_preparation_in_phases(api, make_record):
    record, kube = setup(api, make_record)
    for ph in prepare.PHASES:
        run_phase(record, kube, ph)
    targets = json.load(open(os.path.join(record.evidence_dir, "targets.json")))
    assert len(targets["deployments"]) == 10
    for name, uid in targets["deployments"].items():
        assert api.obj("Deployment", name, "demo")["metadata"]["uid"] == uid
        assert api.obj("Deployment", name, "demo")["spec"]["replicas"] == 2
    kinds = {k for (k, _, _) in api.objects}
    assert "horizontalpodautoscalers" not in kinds and "scaledobjects" not in kinds
    assert all(o["spec"]["mode"] == "Recommend" for (k, _, _), o in api.objects.items() if k == "pa-new")
    assert {v for v, _ in api.mutations()} == {"create"}
    assert api.obj("PersistentVolumeClaim", "pa-eval-ledger", "pa-eval-system")["spec"]["storageClassName"] == \
        "retained-ssd"


def test_values_overlay_has_the_staging_must_haves(api, make_record):
    record, kube = setup(api, make_record)
    run_phase(record, kube, "values")
    v = yaml.safe_load(open(os.path.join(record.evidence_dir, "release-staging.yaml")))
    assert v["forecaster"]["statistical"]["labQualification"]["configMap"] == ""
    assert v["operator"]["forecastLedger"] == {"enabled": True, "persistence": {"existingClaim": "pa-eval-ledger"}}
    assert v["images"]["operator"]["digest"] == "sha256:" + "b" * 64
    assert "c28-forecast-route.pa-eval-system.svc" in v["forecaster"]["tls"]["extraDNSNames"]
    assert v["operator"]["tolerations"][0]["value"] == RUN
    assert v["training"] == {"targets": []}
    assert any("exists" in p for p in prepare.plan(record, kube, "values")["problems"])


def test_values_overlay_keeps_the_release_pods_sidecar_free(api, make_record):
    record, kube = setup(api, make_record)
    params, s = X.load_params(record), prepare._section(record)
    v = prepare.staging_values(record, params, s)
    for k in ("operator", "forecaster"):
        assert v[k]["podAnnotations"] == {"sidecar.istio.io/inject": "false"}
        assert v[k]["podLabels"] == {"sidecar.istio.io/inject": "false"}
    assert v["forecaster"]["statistical"]["resources"]["requests"] == {"cpu": "1", "memory": "512Mi"}
    assert v["operator"]["resources"]["requests"] == {"cpu": "100m", "memory": "128Mi"}
    assert prepare.values_problems(v, params) == []
    del v["forecaster"]["podAnnotations"]
    del v["operator"]["podLabels"]
    assert prepare.values_problems(v, params) == [
        'operator.podLabels must carry sidecar.istio.io/inject: "false" (sidecar-free)',
        'forecaster.podAnnotations must carry sidecar.istio.io/inject: "false" (sidecar-free)']


def test_every_preparation_plan_runs_the_residency_gate(api, make_record):
    """Codex PR #43 repro 1: with W at 87.5 % render_all refused, but prepare planned the targets without a problem."""
    params = base_params()
    params["residency"] = copy.deepcopy(params["residency"])
    params["residency"]["workload"]["allocatable"]["cpu"] = "4"     # 3300m + 200m system = 87.5 % of W
    s = prepare_section()
    api.put({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": "retained-ssd"},
             "reclaimPolicy": "Retain"})
    record = X.load_record(make_record({"prepare": s}, params=params), ("prepare",))
    kube = record.kube(api)
    for ph in prepare.PHASES:
        probs = prepare.plan(record, kube, ph)["problems"]
        assert any(p.startswith("residency: workload node worker-w: CPU requests 3500m") for p in probs), (ph, probs)
    run_phase_ok = copy.deepcopy(base_params())                   # the same run within budget plans cleanly
    record2 = X.load_record(make_record({"prepare": s}, params=run_phase_ok), ("prepare",))
    assert not any("residency" in p for p in prepare.plan(record2, record2.kube(api), "targets")["problems"])


def test_apply_recomputes_the_residency_gate(api, make_record, tmp_path):
    record, kube = setup(api, make_record)
    plan, digest = reviewed(record, kube, "namespaces")
    run_phase(record, kube, "namespaces")
    plan, digest = reviewed(record, kube, "common")
    params = json.load(open(tmp_path / "params.json"))
    params["residency"]["workload"]["allocatable"]["cpu"] = "4"      # the budget changes after review
    (tmp_path / "params.json").write_text(json.dumps(params))
    with pytest.raises(X.StagingError):
        prepare.apply(record, kube, "common", plan, digest)
    assert not any(v == "create" and "resourcequotas" in p for v, p in api.mutations())


def test_verify_reports_unexpected_injection(api, make_record):
    record, kube = setup(api, make_record)
    for ph in ("namespaces", "common", "targets", "pas"):
        run_phase(record, kube, ph)
    api.put({"apiVersion": "v1", "kind": "Pod",
             "metadata": {"name": "c28-pa-n-s20-abcde", "namespace": "demo", "labels": {X.RUN_LABEL: RUN}},
             "spec": {"initContainers": [{"name": "istio-init"}], "containers": [{"name": "fixture"},
                                                                                 {"name": "istio-proxy"}]}})
    api.put({"apiVersion": "v1", "kind": "Pod",
             "metadata": {"name": "unrelated", "namespace": "demo", "labels": {}},
             "spec": {"containers": [{"name": "istio-proxy"}]}})
    text = "\n".join(prepare.verify(record, kube)["problems"])
    assert "demo/c28-pa-n-s20-abcde has injected containers ['istio-init', 'istio-proxy']" in text
    assert "unrelated" not in text


def test_staging_requires_the_residency_section(api, make_record):
    params = base_params()
    del params["residency"]
    record = X.load_record(make_record({"prepare": prepare_section()}, params=params), ("prepare",))
    with pytest.raises(X.StagingError, match="residency section"):
        X.load_params(record)


def test_network_policy_cannot_be_disabled(api, make_record):
    s = prepare_section()
    s["release"]["network_policy"]["enabled"] = False
    record, kube = setup(api, make_record, s)
    with pytest.raises(X.StagingError, match="never disabled"):
        prepare.plan(record, kube, "values")


def test_existing_name_is_rejected_including_priority_class(api, make_record):
    record, kube = setup(api, make_record)
    run_phase(record, kube, "namespaces")
    api.put({"apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClass", "metadata": {"name": "c28-fixture"},
             "value": 5})
    plan, digest = reviewed(record, kube, "common")
    assert any("PriorityClass /c28-fixture already exists" in x for x in plan["problems"])
    with pytest.raises(X.StagingError, match="problems"):
        prepare.apply(record, kube, "common", plan, digest)
    assert api.obj("PriorityClass", "c28-fixture")["value"] == 5


def test_existing_writer_naming_a_target_is_rejected(api, make_record):
    record, kube = setup(api, make_record)
    run_phase(record, kube, "namespaces")
    api.add("pa-legacy", {"apiVersion": "autoscaler.example.com/v1alpha1", "kind": "PredictiveAutoscaler",
                          "metadata": {"name": "old", "namespace": "demo"},
                          "spec": {"targetDeployment": {"name": "c28-pa-n-s20", "namespace": "demo"}}})
    probs = prepare.plan(record, kube, "targets")["problems"]
    assert any("LegacyPredictiveAutoscaler/demo/old already writes demo/c28-pa-n-s20" in x for x in probs)


def test_optional_api_absence_is_discovered(make_record):
    api = F.FakeApi2(groups=["apps", "batch", "autoscaling", "autoscaling.devkuban.com"])
    record, kube = setup(api, make_record)
    run_phase(record, kube, "namespaces")
    assert prepare.plan(record, kube, "targets")["problems"] == []
    api2 = F.FakeApi2(groups=["apps", "batch", "keda.sh", "autoscaling.devkuban.com"])
    record2, kube2 = setup(api2, make_record)
    with pytest.raises(X.StagingError, match="autoscaling API is not served"):
        prepare.plan(record2, kube2, "targets")


def test_storage_class_must_retain(api, make_record):
    record, kube = setup(api, make_record)
    api.obj("StorageClass", "retained-ssd")["reclaimPolicy"] = "Delete"
    run_phase(record, kube, "namespaces")
    assert any("Retain" in x for x in prepare.plan(record, kube, "storage")["problems"])


def test_apply_refuses_a_drifted_render(api, make_record, tmp_path):
    record, kube = setup(api, make_record)
    run_phase(record, kube, "namespaces")
    plan, digest = reviewed(record, kube, "common")
    params = json.load(open(tmp_path / "params.json"))
    params["proxy"]["cpu_limit"] = "300m"
    (tmp_path / "params.json").write_text(json.dumps(params))
    with pytest.raises(X.StagingError, match="changed since review"):
        prepare.apply(record, kube, "common", plan, digest)


def test_verify_detects_replacement_writers_and_dirty_faults(api, make_record):
    record, kube = setup(api, make_record)
    for ph in ("namespaces", "common", "targets", "pas"):
        run_phase(record, kube, ph)
    assert prepare.verify(record, kube)["result"] == "PASS"
    api.obj("ConfigMap", "c28-h-rps-s20-faults", "demo")["data"]["faults.json"] = '{"outages": []}'
    api.put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
             "metadata": {"name": "stray", "namespace": "demo"},
             "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "c28-h-cpu-s20"}}})
    api.obj("Deployment", "c28-pa-r-s90", "demo")["metadata"]["uid"] = F.new_uid()
    out = prepare.verify(record, kube)
    text = "\n".join(out["problems"])
    assert out["result"] == "FAIL"
    assert "c28-h-rps-s20-faults: fault schedule is not empty" in text
    assert "c28-h-cpu-s20: writers ['HorizontalPodAutoscaler/demo/stray#" in text
    assert "c28-pa-r-s90 was replaced" in text


def test_cli_apply_needs_phase_and_plan(api, make_record):
    record, _ = setup(api, make_record)
    assert prepare.main(["--record", record.path, "apply", "--plan-digest", "0" * 64], api=api) == 1
    assert api.mutations() == []


def test_lost_response_resumes_from_the_journal(api, make_record):
    """Codex #32 item 2: a committed create whose response was lost holds the shared lock; after recovery the phase
    resumes, counting the object as this run's (by its intent annotation), and targets.json lists all ten."""
    import common_records as C
    record, kube = setup(api, make_record)
    for ph in ("namespaces", "common"):
        run_phase(record, kube, ph)
    plan, digest = reviewed(record, kube, "targets")
    real = api.create
    calls = {"n": 0}

    def lost(path, body):
        out = real(path, body)
        calls["n"] += 1
        if body["kind"] == "Deployment" and calls["n"] > 3:
            raise C.ApiError("create: failed (timeout)", 504)
        return out
    api.create = lost
    with pytest.raises(X.StagingError):
        prepare.apply(record, kube, "targets", plan, digest)
    api.create = real
    lock = PF.lock_file(record)
    assert os.path.exists(lock)
    assert any("unknown outcomes" in p for p in prepare.plan(record, kube, "targets")["problems"])
    PF.recover(record, kube)
    assert not os.path.exists(lock)
    p2, d2 = reviewed(record, kube, "targets")
    assert p2["problems"] == [] and p2["already_created_by_this_run"]
    prepare.apply(record, kube, "targets", p2, d2)
    targets = json.load(open(os.path.join(record.evidence_dir, "targets.json")))
    assert len(targets["deployments"]) == 10
    for name, uid in targets["deployments"].items():
        assert api.obj("Deployment", name, "demo")["metadata"]["uid"] == uid


# --- the live reservation gate (Codex #46 review, finding 1) ----------------------------------------------------------

def _drop_w_taint(api):
    w = api.obj("Node", "worker-w")
    w["spec"]["taints"] = [t for t in w["spec"]["taints"] if t.get("key") != "c28.devkuban.com/reserved"]


def test_every_plan_requires_the_live_reservation_verdict(api, make_record):
    record, kube = setup(api, make_record, reserved=False)
    for ph in prepare.PHASES:
        p = prepare.plan(record, kube, ph)
        assert any(x.startswith("reservation gate: ") for x in p["problems"]), ph
        assert p["reservation_gate"]["reservation"] == "FAIL" and p["reservation_gate"]["ready_for_experiment"] is False


def test_a_lost_reservation_blocks_preparation(api, make_record):
    """Codex repro: apply reservation, remove W's taint, then prepare targets: reservation FAIL is a problem."""
    record, kube = setup(api, make_record)
    for ph in ("namespaces", "storage", "values", "common"):
        run_phase(record, kube, ph)
    _drop_w_taint(api)
    p = prepare.plan(record, kube, "targets")
    text = "\n".join(p["problems"])
    assert "reservation gate: workload worker-w: the run taint is not exactly" in text
    assert p["reservation_gate"] == {"reservation": "FAIL", "placement": "PASS", "ready_for_experiment": False,
                                     "result": "FAIL"}


def test_reservation_lost_between_review_and_apply_creates_nothing(api, make_record):
    record, kube = setup(api, make_record)
    for ph in ("namespaces", "storage", "values", "common"):
        run_phase(record, kube, ph)
    plan, digest = reviewed(record, kube, "targets")
    assert plan["problems"] == [] and plan["reservation_gate"]["ready_for_experiment"] is True
    api.calls.clear()
    _drop_w_taint(api)
    with pytest.raises(X.StagingError, match="changed since review|no longer produces the reviewed plan"):
        prepare.apply(record, kube, "targets", plan, digest)
    assert [c for c in api.mutations() if c[0] == "create"] == []
    assert api.mutations() == []
    assert not os.path.exists(os.path.join(record.evidence_dir, "targets.json"))


def test_an_unplanned_occupant_blocks_preparation(api, make_record):
    record, kube = setup(api, make_record)
    api.put(F.pod("stray-1", "shop", "worker-w", owner=("Deployment", "stray")))
    p = prepare.plan(record, kube, "namespaces")
    assert any("reservation gate: workload: shop/stray-1" in x for x in p["problems"])


def test_a_record_without_reservation_is_refused(api, make_record):
    api.put({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": "retained-ssd"},
             "reclaimPolicy": "Retain"})
    record = X.load_record(make_record({"prepare": prepare_section()}), ("prepare",))
    p = prepare.plan(record, record.kube(api), "namespaces")
    assert "reservation gate: the record has no reserve section (reserve.py first)" in p["problems"]


# --- explicit image pull mode: secret or anonymous (never defaulted) -------------------------------------------------

OP_IMAGE = "harbor.example/autoscaler/operator@sha256:" + "b" * 64
ST_IMAGE = "harbor.example/autoscaler/statistical-evaluation@sha256:" + "c" * 64
TOKEN = "anon-token-value-never-recorded"


class FakeRegistry:
    """fetch(method, url, headers): a v2 registry that challenges with Bearer and hands out anonymous tokens."""

    def __init__(self, manifest_status=200, challenge=True, token_status=200, served=None):
        self.manifest_status, self.challenge, self.token_status = manifest_status, challenge, token_status
        self.served = served or {}
        self.calls = []

    def __call__(self, method, url, headers):
        self.calls.append((method, url, dict(headers)))
        if url.startswith("https://harbor.example/service/token"):
            return self.token_status, {}, json.dumps({"token": TOKEN}).encode() if self.token_status == 200 else b""
        assert method == "HEAD" and "/v2/autoscaler/" in url and "/manifests/sha256:" in url, url
        if self.challenge and headers.get("Authorization") != f"Bearer {TOKEN}":
            return 401, {"www-authenticate": 'Bearer realm="https://harbor.example/service/token",'
                                             'service="harbor-registry",scope="repository:autoscaler/x:pull"'}, b""
        dig = url.rsplit("/", 1)[1]
        return self.manifest_status, {"docker-content-digest": self.served.get(dig, dig)}, b""


def anonymous_section():
    s = prepare_section()
    s["release"]["image_pull"] = {"mode": "anonymous"}
    return s


def test_anonymous_pull_renders_no_pull_secret_and_records_each_check(api, make_record):
    record, kube = setup(api, make_record, anonymous_section())
    reg = FakeRegistry()
    p = prepare.plan(record, kube, "values", fetch=reg)
    assert p["problems"] == []
    assert p["values"]["images"]["pullSecrets"] == []
    checks = p["image_pull"]["verification"]
    assert p["image_pull"]["mode"] == "anonymous"
    assert [(c["image"], c["digest"], c["status"], c["anonymous_challenge"]) for c in checks] == [
        (OP_IMAGE, "sha256:" + "b" * 64, 200, True), (ST_IMAGE, "sha256:" + "c" * 64, 200, True)]
    assert checks[0]["url"] == "https://harbor.example/v2/autoscaler/operator/manifests/sha256:" + "b" * 64
    assert all(c["checked_at"].endswith("Z") for c in checks)
    for method, url, headers in reg.calls:              # no credentials ever: only Accept, or the anonymous token
        assert set(headers) <= {"Accept", "Authorization"}
        assert headers.get("Authorization", f"Bearer {TOKEN}") == f"Bearer {TOKEN}"
        if method == "HEAD":
            assert "application/vnd.oci.image.index.v1+json" in headers["Accept"]
            assert "application/vnd.docker.distribution.manifest.v2+json" in headers["Accept"]
    assert TOKEN not in json.dumps(p)                    # the token is used, never recorded
    assert api.mutations() == []


def test_anonymous_pull_without_a_challenge_needs_no_token(api, make_record):
    record, kube = setup(api, make_record, anonymous_section())
    reg = FakeRegistry(challenge=False)
    p = prepare.plan(record, kube, "values", fetch=reg)
    assert p["problems"] == [] and not any(c["anonymous_challenge"] for c in p["image_pull"]["verification"])
    assert all("Authorization" not in h for _, _, h in reg.calls)


@pytest.mark.parametrize("reg, want", [
    (lambda: FakeRegistry(token_status=401), "returned 401"),        # the project is private: no anonymous token
    (lambda: FakeRegistry(manifest_status=401), "returned 401"),     # token issued, the pull still refused
    (lambda: FakeRegistry(manifest_status=404), "returned 404"),     # the digest is not in the registry
    (lambda: FakeRegistry(served={"sha256:" + "b" * 64: "sha256:" + "d" * 64}), "served digest sha256:dddd"),
])
def test_anonymous_pull_refusals_are_plan_problems(api, make_record, reg, want):
    record, kube = setup(api, make_record, anonymous_section())
    p = prepare.plan(record, kube, "values", fetch=reg())
    assert any(x.startswith("anonymous pull: harbor.example/autoscaler/operator@") and want in x
               for x in p["problems"]), p["problems"]
    saved = X.save_plan(record, "prepare", p, "plan-values")
    with pytest.raises(X.StagingError, match="problems"):
        prepare.apply(record, kube, "values", X.load_plan(saved["plan"]), saved["plan_digest"], fetch=reg())
    assert not os.path.exists(os.path.join(record.evidence_dir, "release-staging.yaml"))


def test_an_unreachable_registry_is_a_plan_problem(api, make_record):
    record, kube = setup(api, make_record, anonymous_section())

    def down(method, url, headers):
        raise OSError("connection refused")
    p = prepare.plan(record, kube, "values", fetch=down)
    assert sum("returned None (OSError: connection refused)" in x for x in p["problems"]) == 2


def test_anonymous_apply_checks_again_and_keeps_the_reviewed_plan(api, make_record):
    record, kube = setup(api, make_record, anonymous_section())
    p = prepare.plan(record, kube, "values", fetch=FakeRegistry())
    saved = X.save_plan(record, "prepare", p, "plan-values")
    plan, digest = X.load_plan(saved["plan"]), saved["plan_digest"]
    reg = FakeRegistry()
    r = prepare.apply(record, kube, "values", plan, digest, fetch=reg)   # a later check: only its time differs
    assert len([c for c in reg.calls if c[0] == "HEAD"]) == 4            # both images checked again (401, then 200)
    assert [c["status"] for c in r["data"]["image_pull"]["verification"]] == [200, 200]
    assert TOKEN not in json.dumps(r)
    v = yaml.safe_load(open(os.path.join(record.evidence_dir, "release-staging.yaml")))
    assert v["images"]["pullSecrets"] == []


def test_anonymous_pull_gone_between_review_and_apply_writes_nothing(api, make_record):
    record, kube = setup(api, make_record, anonymous_section())
    p = prepare.plan(record, kube, "values", fetch=FakeRegistry())
    saved = X.save_plan(record, "prepare", p, "plan-values")
    with pytest.raises(X.StagingError, match="changed since review"):
        prepare.apply(record, kube, "values", X.load_plan(saved["plan"]), saved["plan_digest"],
                      fetch=FakeRegistry(manifest_status=404))
    assert not os.path.exists(os.path.join(record.evidence_dir, "release-staging.yaml"))


@pytest.mark.parametrize("pull, want", [
    (None, "must be set explicitly"),                                   # missing: never silently anonymous
    ({}, "must be set explicitly"),
    ({"mode": "Anonymous"}, "must be set explicitly"),
    ({"mode": "secret"}, "exactly pull_ref"),
    ({"mode": "secret", "pull_ref": "Bad_Name"}, "exactly pull_ref"),
    ({"mode": "anonymous", "pull_ref": "harbor-pull"}, "takes no other field"),
])
def test_the_pull_mode_is_explicit(api, make_record, pull, want):
    s = prepare_section()
    del s["release"]["image_pull"]
    if pull is not None:
        s["release"]["image_pull"] = pull
    record, kube = setup(api, make_record, s)
    with pytest.raises(X.StagingError, match=want):
        prepare.plan(record, kube, "values", fetch=FakeRegistry())


def test_the_retired_pull_ref_is_refused(api, make_record):
    s = prepare_section()
    s["release"]["registry_pull_ref"] = "harbor-pull"
    record, kube = setup(api, make_record, s)
    with pytest.raises(X.StagingError, match="registry_pull_ref is retired"):
        prepare.plan(record, kube, "values")


def test_secret_mode_is_unchanged_and_never_fetches(api, make_record):
    record, kube = setup(api, make_record)

    def no_network(method, url, headers):
        raise AssertionError("secret mode never contacts the registry")
    p = prepare.plan(record, kube, "values", fetch=no_network)
    assert p["problems"] == []
    assert p["values"]["images"]["pullSecrets"] == [{"name": "harbor-pull"}]
    assert p["image_pull"] == {"mode": "secret"}


def test_a_pa_not_in_recommend_is_refused_at_plan(api, make_record, monkeypatch):
    record, kube = setup(api, make_record)
    run_phase(record, kube, "namespaces")
    real = prepare.split

    def active_split(params):
        parts = real(params)
        parts["pas"][0]["spec"]["mode"] = "Active"
        del parts["pas"][1]["spec"]["mode"]                 # a missing mode is refused too (never assumed)
        return parts
    monkeypatch.setattr(prepare, "split", active_split)
    p = prepare.plan(record, kube, "pas")
    names = [o["metadata"]["name"] for o in active_split(prepare.X.load_params(record))["pas"][:2]]
    assert [x for x in p["problems"] if "Recommend" in x] == [f"PredictiveAutoscaler/{n} is not in Recommend"
                                                              for n in names]
