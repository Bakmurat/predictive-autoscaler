import json
import os

import pytest

import common_records as C
import execlib as X
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


def test_update_is_uid_and_version_bound(api):
    k = X.Kube(api, F.CLUSTER)
    cm = api.put({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c", "namespace": "demo"},
                  "data": {"a": "1"}})
    with pytest.raises(X.StagingError, match="replaced"):
        k.update("ConfigMap", "c", "demo", "00000000-0000-4000-8000-000000000000", lambda b: None)

    def refuse(body):
        raise X.StagingError("not as reviewed")
    with pytest.raises(X.StagingError, match="not as reviewed"):
        k.update("ConfigMap", "c", "demo", cm["metadata"]["uid"], refuse)
    assert api.mutations() == []
    k.update("ConfigMap", "c", "demo", cm["metadata"]["uid"], lambda b: b["data"].update(a="2"))
    assert api.obj("ConfigMap", "c", "demo")["data"]["a"] == "2"


def test_mutations_check_the_cluster_first(api):
    k = X.Kube(api, "99999999-2222-4333-8444-555555555555")
    with pytest.raises(X.StagingError, match="does not reach the recorded cluster"):
        k.create({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "d", "namespace": "demo"}})
    assert api.mutations() == []


def test_delete_needs_the_uid(api):
    k = X.Kube(api, F.CLUSTER)
    cm = api.put({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c", "namespace": "demo"}})
    with pytest.raises(C.ApiError):
        k.delete("ConfigMap", "c", "demo", "00000000-0000-4000-8000-000000000000")
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
