import json
import os

import pytest
import yaml

import execlib as X
import part2_fixtures as PF
import fake2 as F
import prepare
from part2_fixtures import RUN, api, base_params, make_record, prepare_section  # noqa: F401


def setup(api, make_record, section=None):
    api.put({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": "retained-ssd"},
             "reclaimPolicy": "Retain"})
    record = X.load_record(make_record({"prepare": section or prepare_section()}), ("prepare",))
    return record, record.kube(api)


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
