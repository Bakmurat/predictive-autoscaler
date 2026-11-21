"""The execlib parts of the image transition: the JSON Patch verb with server-side
preconditions, its journalled outcomes, the transition states read from the imageroll journal, and the fence in the
shared mutation entry point and in recovery."""

import json
import os

import pytest

import common_records as C
import execlib as X
import part2_fixtures as PF
from part2_fixtures import api, make_record  # noqa: F401
from test_common import Run

OLD = "harbor.example/autoscaler/c28-fixture@sha256:" + "a" * 64
NEW = "harbor.example/autoscaler/c28-fixture@sha256:" + "b" * 64
PATH = "/apis/apps/v1/namespaces/demo/deployments/c28-ingress"


def deployment(api, name="c28-ingress"):
    return api.put({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": name, "namespace": "demo", "annotations": {X.INTENT_ANNOTATION: "r/prepare/1"}},
                    "spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": "ingress", "image": OLD}]}}}})


def ops(d, mark="m", rv=None, old=OLD, name="ingress"):
    return [{"op": "test", "path": "/metadata/uid", "value": d["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": rv or d["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/spec/template/spec/containers/0/name", "value": name},
            {"op": "test", "path": "/spec/template/spec/containers/0/image", "value": old},
            {"op": "add", "path": "/metadata/annotations/c28.devkuban.com~1image-roll", "value": mark},
            {"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": NEW}]


def test_the_kubectl_patch_is_a_typed_json_patch_and_fails_closed():
    run = Run(out='{"metadata": {"uid": "u", "resourceVersion": "2"}}')
    api = C.KubectlApi({"path": "/k/eval.yaml", "context": "prodcluster"}, run=run)
    body = [{"op": "test", "path": "/metadata/uid", "value": "u"}]
    assert api.json_patch(PATH, body)["metadata"]["resourceVersion"] == "2"
    args, stdin = run.calls[0]
    assert stdin is None and args[6:11] == ["patch", "deployments.v1.apps", "c28-ingress", "-n", "demo"]
    assert args[11:] == ["--type=json", "-p", json.dumps(body, sort_keys=True), "-o", "json"]
    for bad in ("/apis/apps/v1/namespaces/demo/statefulsets/x", "/api/v1/namespaces/demo/configmaps/x",
                "/apis/apps/v1/namespaces/demo/deployments/x/scale", "/apis/apps/v1/deployments/x"):
        with pytest.raises(C.StagingError, match="only an apps/v1 Deployment"):
            api.json_patch(bad, body)
    # the API server's answer to a failed test, as kubectl prints it for kube-apiserver 1.35: definitive
    refused = C.KubectlApi({"path": "k", "context": "c"}, run=Run(rc=1, err=(
        "The request is invalid: the server rejected our request due to an error in our request")))
    with pytest.raises(C.ApiError) as e:
        refused.json_patch(PATH, body)
    assert e.value.status == 422 and e.value.status in X.DEFINITIVE
    with pytest.raises(C.NotFound):
        C.KubectlApi({"path": "k", "context": "c"}, run=Run(rc=1, err='deployments.apps "x" not found')
                     ).json_patch(PATH, body)
    for out in ("", "not json", "[]", '{"kind": "Status"}'):          # no object in the answer: outcome unknown
        with pytest.raises(C.ApiError) as e:
            C.KubectlApi({"path": "k", "context": "c"}, run=Run(out=out)).json_patch(PATH, body)
        assert e.value.status is None
    with pytest.raises(C.ApiError) as e:
        C.KubectlApi({"path": "k", "context": "c"}, run=Run(rc=1, err="context deadline exceeded")
                     ).json_patch(PATH, body)
    assert e.value.status not in X.DEFINITIVE


def test_the_fake_patch_is_atomic_like_the_api_server(api):
    d = deployment(api)
    for wrong in (dict(rv="1"), dict(old=NEW), dict(name="other")):
        with pytest.raises(C.ApiError) as e:
            api.json_patch(PATH, ops(d, **wrong))
        assert e.value.status == 422
    with pytest.raises(C.ApiError):
        api.json_patch(PATH, [dict(ops(d)[0], value="00000000-0000-4000-8000-000000000000")] + ops(d)[1:])
    assert api.obj("Deployment", "c28-ingress", "demo") == d            # nothing of a refused patch is applied
    out = api.json_patch(PATH, ops(d))
    assert out["spec"]["template"]["spec"]["containers"][0]["image"] == NEW
    assert out["metadata"]["annotations"]["c28.devkuban.com/image-roll"] == "m"
    assert out["metadata"]["generation"] == 2 and out["metadata"]["resourceVersion"] != d["metadata"]["resourceVersion"]
    with pytest.raises(C.ApiError):                                     # the same request again: stale version
        api.json_patch(PATH, ops(d))
    void = api.json_patch(PATH, [{"op": "add", "path": "/metadata/annotations/c28.devkuban.com~1image-roll-void",
                                  "value": "x"}])
    assert void["metadata"]["generation"] == 3                          # an annotation raises a Deployment's generation


def _patch(k, d, validate=lambda out, mark: None, **kw):
    return k.json_patch("image-roll", "Deployment", "c28-ingress", "demo", d["metadata"]["uid"],
                        lambda mark: ops(d, mark, **kw), validate, pre={"resourceVersion": d["metadata"]["resourceVersion"]})


def test_a_journalled_patch_has_three_outcomes(api, make_record):
    record = X.load_record(make_record())
    k, d, lock = record.kube(api), deployment(api), PF.lock_file(X.load_record(make_record()))
    with pytest.raises(X.StagingError, match="needs the shared lock"):
        _patch(k, d)
    # 1. a definitive refusal: recorded as not-applied, the lock is released
    with pytest.raises(C.ApiError), X.mutating(record, k, "imageroll"):
        _patch(k, d, rv="1")
    ev = X.receipts(record, "imageroll")
    assert [e["event"] for e in ev] == ["intent-1", "step-1"] and ev[1]["data"]["result"] == "not-applied"
    assert not os.path.exists(lock) and api.obj("Deployment", "c28-ingress", "demo") == d
    # 2. applied: the mark the journal chose is the operation id in the object
    with X.mutating(record, k, "imageroll"):
        r = _patch(k, d)
    assert r["mark"] == f"{record.run_id}/imageroll/2" and r["step"] == "2"
    assert r["out"]["metadata"]["annotations"]["c28.devkuban.com/image-roll"] == r["mark"]
    assert X.receipts(record, "imageroll")[-1]["data"]["result"] == "applied" and not os.path.exists(lock)


@pytest.mark.parametrize("how", ["lost", "invalid-answer"])
def test_an_unknown_patch_outcome_keeps_the_lock(api, make_record, how):
    record = X.load_record(make_record())
    k, d = record.kube(api), deployment(api)

    def reject(out, mark):
        raise X.StagingError("the answer does not show the expected post-state")
    if how == "lost":
        real = api.json_patch

        def flaky(*a, **kw):
            real(*a, **kw)
            raise C.ApiError("patch: failed (timeout)", 504)
        api.json_patch = flaky
    with pytest.raises(X.StagingError), X.mutating(record, k, "imageroll"):
        _patch(k, d, validate=reject if how == "invalid-answer" else (lambda out, mark: None))
    assert [e["event"] for e in X.receipts(record, "imageroll")] == ["intent-1"]
    assert os.path.exists(PF.lock_file(record))                    # its reconciler: tests/test_imageroll.py


def _journal(record, *events):
    for ev, data in events:
        X.receipt(record, "imageroll", ev, data)


def test_transition_states_and_the_fence(make_record):
    record = X.load_record(make_record())
    assert X.image_fence(record) is None and X.image_fence_problems(record) == []
    _journal(record, ("transition-prepared", {"id": "ir-1-aaaaaaaaaaaa"}))
    assert X.image_fence(record) == "ir-1-aaaaaaaaaaaa"            # prepared: fenced before the record is published
    assert "ir-1-aaaaaaaaaaaa" in X.image_fence_problems(record)[0]
    _journal(record, ("transition-open", {"id": "ir-1-aaaaaaaaaaaa"}))
    assert X.image_fence(record) == "ir-1-aaaaaaaaaaaa"
    _journal(record, ("transition-prepared", {"id": "ir-2-bbbbbbbbbbbb", "supersedes": "ir-1-aaaaaaaaaaaa"}))
    ts = X.image_transitions(X.receipts(record, "imageroll"))
    assert ts["ir-1-aaaaaaaaaaaa"]["state"] == "superseded" and X.image_fence(record) == "ir-2-bbbbbbbbbbbb"
    _journal(record, ("transition-open", {"id": "ir-2-bbbbbbbbbbbb"}), ("transition-complete", {"id": "ir-2-bbbbbbbbbbbb"}))
    assert X.image_fence(record) is None                            # the successor's completion closes the chain
    # a third attempt with the same binding has a new id; the earlier completion does not close it
    _journal(record, ("transition-prepared", {"id": "ir-3-cccccccccccc"}))
    assert X.image_fence(record) == "ir-3-cccccccccccc"
    _journal(record, ("transition-abandoned", {"id": "ir-3-cccccccccccc"}))
    assert X.image_fence(record) is None
    # an abandoned successor (its record was never published) gives its parent the fence back
    _journal(record, ("transition-prepared", {"id": "ir-4-dddddddddddd"}), ("transition-open", {"id": "ir-4-dddddddddddd"}),
             ("transition-prepared", {"id": "ir-5-eeeeeeeeeeee", "supersedes": "ir-4-dddddddddddd"}))
    assert X.image_fence(record) == "ir-5-eeeeeeeeeeee"
    _journal(record, ("transition-abandoned", {"id": "ir-5-eeeeeeeeeeee"}))
    ts = X.image_transitions(X.receipts(record, "imageroll"))
    assert ts["ir-4-dddddddddddd"]["state"] == "open" and X.image_fence(record) == "ir-4-dddddddddddd"


@pytest.mark.parametrize("events,match", [
    ([("transition-prepared", {"id": "a"}), ("transition-prepared", {"id": "a"})], "appears twice"),
    ([("transition-complete", {"id": "a"})], "does not follow"),
    ([("transition-prepared", {"id": "a"}), ("transition-complete", {"id": "a"})], "does not follow"),
    ([("transition-prepared", {"id": "a"}), ("transition-open", {"id": "a"}), ("transition-abandoned", {"id": "a"})],
     "does not follow"),
    ([("transition-prepared", {"id": "b", "supersedes": "a"})], "which is not open"),
    ([("transition-prepared", {"id": "a"}), ("transition-prepared", {"id": "b", "supersedes": "a"})],
     "which is not open"),
    ([("transition-prepared", {"id": "a"}), ("transition-prepared", {"id": "b"})], "more than one"),
])
def test_an_inconsistent_transition_journal_is_refused(make_record, events, match):
    record = X.load_record(make_record())
    _journal(record, *events)
    with pytest.raises(X.StagingError, match=match):
        X.image_fence(record)


def test_the_record_field_fences_without_a_journal_and_an_archived_record_does_not_lift_it(make_record, tmp_path):
    named = X.load_record(make_record({"image_transition": {"id": "ir-1-aaaaaaaaaaaa"}}))
    assert X.image_fence(named) == "ir-1-aaaaaaaaaaaa"             # re-pinned by hand, never journalled: fenced
    _journal(named, ("transition-prepared", {"id": "ir-1-aaaaaaaaaaaa"}), ("transition-open", {"id": "ir-1-aaaaaaaaaaaa"}))
    archived = dict(named.doc)
    archived.pop("image_transition")
    old = X.Record(named.path, archived)                             # the archived record: same run, same receipts
    assert X.image_fence(old) == "ir-1-aaaaaaaaaaaa"
    _journal(named, ("transition-complete", {"id": "ir-1-aaaaaaaaaaaa"}))
    assert X.image_fence(named) is None and X.image_fence(old) is None
    stale = X.Record(named.path, dict(named.doc, image_transition={"id": "ir-0-ffffffffffff"}))
    assert X.image_fence(stale) == "ir-0-ffffffffffff"             # a record naming a transition never completed


def test_the_fence_in_the_shared_mutation_entry(api, make_record):
    record = X.load_record(make_record())
    k, lock = record.kube(api), PF.lock_file(record)
    _journal(record, ("transition-prepared", {"id": "ir-1-aaaaaaaaaaaa"}))
    for helper, ctx in (("bounded", {"run": "x"}), ("prepare", None), ("session", {}), ("imageroll", None),
                        ("imageroll", {"transition": "ir-9-ffffffffffff"}),
                        ("bounded", {"transition": "ir-1-aaaaaaaaaaaa"})):
        with pytest.raises(X.StagingError, match="image transition ir-1-aaaaaaaaaaaa is open"):
            with X.mutating(record, k, helper, ctx):
                raise AssertionError("entered")
        assert not os.path.exists(lock) and k.journal is None       # refused under the lock, which is released
    for ctx in ({"operation": "grant"}, {"operation": "recover"}, {}, None):       # of that helper only the revocation
        with pytest.raises(X.StagingError, match="image transition ir-1-aaaaaaaaaaaa is open"):
            with X.mutating(record, k, "transition", ctx):
                raise AssertionError("entered")
    with pytest.raises(X.StagingError, match="is open"), X.mutating(record, k, "bounded", {"operation": "revoke"}):
        raise AssertionError("entered")
    with X.mutating(record, k, "transition", {"operation": "revoke"}):          # removing a permission is not fenced
        assert k.journal is not None
    assert not os.path.exists(lock) and X.image_fence(record) == "ir-1-aaaaaaaaaaaa"
    with X.mutating(record, k, "imageroll", {"transition": "ir-1-aaaaaaaaaaaa"}):
        assert k.journal is not None
    with X.mutating(record, k, "imageroll", {"transition": "ir-2-bbbbbbbbbbbb", "supersedes": "ir-1-aaaaaaaaaaaa"}):
        pass
    assert not os.path.exists(lock)


def test_recovery_under_the_fence(api, make_record):
    record = X.load_record(make_record())
    k = record.kube(api)
    real = api.create

    def flaky(*a, **kw):
        real(*a, **kw)
        raise C.ApiError("create: failed (timeout)", 504)
    api.create = flaky
    with pytest.raises(C.ApiError), X.mutating(record, k, "prepare"):
        k.create({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c28-x", "namespace": "demo"}})
    api.create = real
    _journal(record, ("transition-prepared", {"id": "ir-1-aaaaaaaaaaaa"}))
    ran = []
    with pytest.raises(X.StagingError, match="follow-up is refused"):
        PF.recover(record, k, follow_up=lambda actions: ran.append(actions))
    assert ran == [] and os.path.exists(PF.lock_file(record))
    assert X.open_intents(X.receipts(record, "prepare"))             # nothing was reconciled by the refused recovery
    done = PF.recover(record, k)                                     # without a follow-up: reconcile and release
    assert done["data"]["image_transition_open"] == "ir-1-aaaaaaaaaaaa" and not os.path.exists(PF.lock_file(record))
    assert X.image_fence(record) == "ir-1-aaaaaaaaaaaa"            # releasing the lock never lifts the fence
