import json
import os

import pytest

import common_records as C
import execlib as X
import part2_fixtures as PF
import fake2 as F
from part2_fixtures import api, make_record  # noqa: F401


def test_record_is_part_1s_with_resolved_paths(make_record, tmp_path):
    record = X.load_record(make_record({"reserve": {"x": 1}}), ("reserve",))
    assert record.evidence_dir == str(tmp_path / "evidence")
    assert record.resolve("params.json") == str(tmp_path / "params.json")
    with pytest.raises(X.StagingError, match="the prepare section is required"):
        X.load_record(make_record(), ("prepare",))
    with pytest.raises(X.StagingError, match="credential material"):
        X.load_record(make_record({"build": {"harbor_password": "x"}}))


def test_paths_and_secrets():
    assert X.path("ConfigMap", "demo", "c") == "/api/v1/namespaces/demo/configmaps/c"
    assert X.path("PriorityClass", None, "p") == "/apis/scheduling.k8s.io/v1/priorityclasses/p"
    assert X.path("LegacyPredictiveAutoscaler", "demo") == "/apis/autoscaler.example.com/v1alpha1/namespaces/demo/" \
                                                           "predictiveautoscalers"
    with pytest.raises(X.StagingError, match="never read Secrets"):
        X.path("Secret", "demo", "s")


def test_update_is_uid_and_version_bound(api, make_record):
    record = X.load_record(make_record())
    k = record.kube(api)
    cm = api.put({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c", "namespace": "demo"},
                  "data": {"a": "1"}})
    with pytest.raises(X.StagingError, match="needs the shared lock and an intent journal"):
        k.update("ConfigMap", "c", "demo", cm["metadata"]["uid"], lambda b: None)
    with pytest.raises(X.StagingError, match="replaced"), X.mutating(record, k, "reserve"):
        k.update("ConfigMap", "c", "demo", "00000000-0000-4000-8000-000000000000", lambda b: None)

    def refuse(body):
        raise X.StagingError("not as reviewed")
    with pytest.raises(X.StagingError, match="not as reviewed"), X.mutating(record, k, "reserve"):
        k.update("ConfigMap", "c", "demo", cm["metadata"]["uid"], refuse)
    assert api.mutations() == []
    with X.mutating(record, k, "reserve"):
        k.update("ConfigMap", "c", "demo", cm["metadata"]["uid"], lambda b: b["data"].update(a="2"),
                 expect=[["/data/a", "2"]])
    assert api.obj("ConfigMap", "c", "demo")["data"]["a"] == "2"


def test_mutations_check_the_cluster_first(api, make_record):
    record = X.load_record(make_record())
    k = X.Kube(api, "99999999-2222-4333-8444-555555555555")
    with pytest.raises(X.StagingError, match="does not reach the recorded cluster"), X.mutating(record, k, "prepare"):
        k.create({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "d", "namespace": "demo"}})
    assert api.mutations() == []


def test_delete_needs_the_uid(api, make_record):
    record = X.load_record(make_record())
    k = record.kube(api)
    cm = api.put({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c", "namespace": "demo"}})
    with pytest.raises(C.ApiError), X.mutating(record, k, "session"):
        k.delete("ConfigMap", "c", "demo", "00000000-0000-4000-8000-000000000000")
    assert not os.path.exists(PF.lock_file(record))  # definitive: released
    with X.mutating(record, k, "session"):
        k.delete("ConfigMap", "c", "demo", cm["metadata"]["uid"])
    assert api.obj("ConfigMap", "c", "demo") is None


def test_reviewed_plan_and_live_state(make_record):
    record = X.load_record(make_record())
    plan = X.new_plan("reserve", record, [{"x": 1}], problems=[])
    saved = X.save_plan(record, "reserve", plan)
    assert X.save_plan(record, "reserve", plan) == saved                     # idempotent, never rewritten
    reviewed = X.load_plan(saved["plan"])
    X.require_live(reviewed, saved["plan_digest"], "reserve", record, plan)
    with pytest.raises(X.StagingError, match="changed since review"):
        X.require_live(reviewed, saved["plan_digest"], "reserve", record, dict(plan, actions=[{"x": 2}]))
    with pytest.raises(X.StagingError, match="another helper"):
        X.require_live(reviewed, saved["plan_digest"], "prepare", record, plan)


def test_receipts_are_part_1s_chain(make_record):
    record = X.load_record(make_record())
    X.receipt(record, "reserve", "one", {"a": 1})
    X.receipt(record, "reserve", "two", {"a": 2})
    entries = X.receipts(record, "reserve")
    assert [e["event"] for e in entries] == ["one", "two"] and X.last(entries, "one", a=1)
    p = C.receipts_path(record.doc, "reserve")
    lines = open(p).read().splitlines()
    edited = json.loads(lines[0]); edited["data"]["a"] = 9
    open(p, "w").write(json.dumps(edited) + "\n" + lines[1] + "\n")
    with pytest.raises(X.StagingError, match="breaks the hash chain"):
        X.receipts(record, "reserve")


def test_write_new_never_overwrites(tmp_path):
    p = str(tmp_path / "x.json")
    X.write_new(p, "1")
    with pytest.raises(X.StagingError, match="written once"):
        X.write_new(p, "2")
    assert open(p).read() == "1" and os.path.exists(p)


def lost_response(api, verb):
    """The API commits the call, then the response is lost (an unknown outcome)."""
    real = getattr(api, verb)

    def flaky(*a, **kw):
        real(*a, **kw)
        raise C.ApiError(f"{verb}: failed (timeout)", 504)
    setattr(api, verb, flaky)
    return real


def test_lost_response_keeps_the_shared_lock_until_recovery(api, make_record):
    """Codex #32 item 2: intent before the call, the shared lock held on an unknown outcome, recovery reconciles."""
    record = X.load_record(make_record())
    k = record.kube(api)
    real = lost_response(api, "create")
    obj = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c28-x", "namespace": "demo"}}
    with pytest.raises(C.ApiError), X.mutating(record, k, "prepare"):
        k.create(obj)
    api.create = real
    lock = PF.lock_file(record)
    assert os.path.exists(lock)
    assert [e["event"] for e in X.receipts(record, "prepare")] == ["intent-1"]
    with pytest.raises(X.StagingError, match="exists"), X.mutating(record, k, "session"):   # ANY part-2 helper
        pass
    [a] = X.recover_plan(record, k)["actions"]
    assert a["observed"] == "done" and a["uid"] == api.obj("ConfigMap", "c28-x", "demo")["metadata"]["uid"]
    with pytest.raises(X.StagingError, match="still running"):     # recovery needs the owner proven gone
        rp = X.recover_plan(record, k)
        saved = X.save_plan(record, "recover", rp, "recover-plan")
        X.recover(record, k, X.load_plan(saved["plan"]), saved["plan_digest"])
    PF.recover(record, k)
    assert not os.path.exists(lock)
    assert X.open_intents(X.receipts(record, "prepare")) == []


def test_swallowed_unknown_outcome_still_keeps_the_lock(api, make_record):
    record = X.load_record(make_record())
    k = record.kube(api)
    cm = api.put({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c", "namespace": "demo"}})
    lost_response(api, "delete")
    with X.mutating(record, k, "session"):
        try:
            k.delete("ConfigMap", "c", "demo", cm["metadata"]["uid"])
        except C.ApiError:
            pass                                  # a caller that collects errors must not release the lock
    assert os.path.exists(PF.lock_file(record))
    assert X.recover_plan(record, k)["actions"][0]["observed"] == "done"


def test_recover_keeps_the_lock_when_unresolved(api, make_record):
    record = X.load_record(make_record())
    k = record.kube(api)
    real = lost_response(api, "create")
    with pytest.raises(C.ApiError), X.mutating(record, k, "prepare"):
        k.create({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c28-y", "namespace": "demo"}})
    api.create = real
    api.obj("ConfigMap", "c28-y", "demo")["metadata"]["annotations"] = {}       # someone replaced it meanwhile
    assert X.recover_plan(record, k)["unresolved"]
    with pytest.raises(X.StagingError, match="unresolved outcomes"):
        PF.recover(record, k)
    assert os.path.exists(PF.lock_file(record))


def test_writer_census_keeps_full_identities(api):
    """Codex #32 item 3: a foreign-namespace PA with the expected name, and an HPA whose ScaledObject owner UID is not
    the live ScaledObject's, both stay separate writers."""
    k = X.Kube(api, F.CLUSTER)
    pa = api.add("pa-new", {"kind": "PredictiveAutoscaler", "metadata": {"name": "c28-pa-n-s20", "namespace": "demo"},
                            "spec": {"targetDeployment": {"name": "c28-pa-n-s20", "namespace": "demo"}}})
    foreign = api.add("pa-new", {"kind": "PredictiveAutoscaler",
                                 "metadata": {"name": "c28-pa-n-s20", "namespace": "other"},
                                 "spec": {"targetDeployment": {"name": "c28-pa-n-s20", "namespace": "demo"}}})
    so = api.add("scaledobjects", {"kind": "ScaledObject", "metadata": {"name": "c28-h-rps-s20", "namespace": "demo"},
                                   "spec": {"scaleTargetRef": {"name": "c28-h-rps-s20"}}})
    api.add("horizontalpodautoscalers", {"kind": "HorizontalPodAutoscaler", "metadata": {
        "name": "keda-hpa-c28-h-rps-s20", "namespace": "demo", "ownerReferences": [
            {"kind": "ScaledObject", "name": "c28-h-rps-s20", "uid": so["metadata"]["uid"], "controller": True}]},
        "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "c28-h-rps-s20"}}})
    api.add("horizontalpodautoscalers", {"kind": "HorizontalPodAutoscaler", "metadata": {
        "name": "fake-keda", "namespace": "demo", "ownerReferences": [
            {"kind": "ScaledObject", "name": "c28-h-rps-s20", "uid": "deadbeef-0000-4000-8000-000000000000",
             "controller": True}]},
        "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "c28-h-rps-s20"}}})
    inv = X.writer_inventory(k, "demo", ["c28-pa-n-s20", "c28-h-rps-s20"])
    assert {(w["namespace"], w["uid"]) for w in inv["c28-pa-n-s20"]} == {
        ("demo", pa["metadata"]["uid"]), ("other", foreign["metadata"]["uid"])}
    assert sorted((w["kind"], w["name"]) for w in inv["c28-h-rps-s20"]) == [
        ("HorizontalPodAutoscaler", "fake-keda"), ("ScaledObject", "c28-h-rps-s20")]


def test_subset_diff():
    assert X.subset_diff({"a": {"b": 1, "c": [1, 2]}}, {"a": {"b": 1, "c": [1, 2], "d": 9}}) == []
    assert X.subset_diff({"a": {"b": "7"}}, {"a": {"b": "999"}}) == ["/a/b"]
    assert X.subset_diff({"spec": {"replicas": 2}}, {"spec": {"replicas": 5}}, ignore=["/spec/replicas"]) == []
