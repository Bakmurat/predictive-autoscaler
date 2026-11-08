import json
import os

import pytest

import execlib as X
import part2_fixtures as PF
import fake2 as F
import reserve
from part2_fixtures import RUN, S_UID, W_UID, api, base_params, make_record, reserve_section  # noqa: F401

TAINT = {"key": "c28.devkuban.com/reserved", "value": RUN, "effect": "NoSchedule"}


def setup(api, make_record, w_taints=None, **over):
    api.put(F.node("worker-w", taints=w_taints, uid=W_UID))
    api.put(F.node("worker-s", uid=S_UID))
    section = reserve_section(**over)
    section["allowed"] = {"daemonsets": [{"namespace": "kube-system", "name": "cilium"}],
                          "mirror_pod_prefixes": ["kube-proxy-"], "broad_tolerations": []}
    section.update({k: v for k, v in over.items() if k == "allowed"})
    api.put(F.pod("cilium-abc", "kube-system", "worker-w", owner=("DaemonSet", "cilium"),
                  tolerations=[{"operator": "Exists"}]))
    api.put(F.pod("kube-proxy-worker-w", "kube-system", "worker-w", owner=("Node", "worker-w")))
    record = X.load_record(make_record({"reserve": section}), ("reserve",))
    return record, record.kube(api)


def reviewed(record, kube, fn=reserve.plan):
    p = fn(record, kube)
    saved = X.save_plan(record, p["helper"], p)
    return X.load_plan(saved["plan"]), saved["plan_digest"]


def do_apply(record, kube):
    return reserve.apply(record, kube, *reviewed(record, kube))


def do_rollback(record, kube):
    return reserve.rollback(record, kube, *reviewed(record, kube, reserve.rollback_plan))


def test_plan_is_read_only_and_exact(api, make_record):
    record, kube = setup(api, make_record)
    p = reserve.plan(record, kube)
    assert api.mutations() == []
    w = [a for a in p["actions"] if a["role"] == "workload"][0]
    assert w["add_taint"] == TAINT and w["add_label"] == "workload" and w["original_taints"] is None
    assert {e["class"] for e in p["occupants"]["workload"]} == {"allowed-system"}
    assert "drain" in p["never"] and p["schema"] == "c28-staging-plan/v1"


def test_apply_needs_the_reviewed_plan_and_digest(api, make_record):
    record, kube = setup(api, make_record)
    plan, digest = reviewed(record, kube)
    with pytest.raises(X.StagingError, match="needs --plan-digest"):
        reserve.apply(record, kube, plan, "")
    with pytest.raises(X.StagingError, match="differs from the reviewed digest"):
        reserve.apply(record, kube, plan, "0" * 64)
    assert api.mutations() == []


def test_apply_adds_exactly_taint_and_label_and_saves_originals(api, make_record):
    keep = {"key": "node.kubernetes.io/foo", "value": "x", "effect": "PreferNoSchedule"}
    record, kube = setup(api, make_record, w_taints=[keep])
    do_apply(record, kube)
    w = api.obj("Node", "worker-w")
    assert w["spec"]["taints"] == [keep, TAINT]
    assert w["metadata"]["labels"]["c28.devkuban.com/role"] == "workload"
    assert "unschedulable" not in w["spec"]
    s = api.obj("Node", "worker-s")
    assert s["spec"]["taints"] == [TAINT] and s["metadata"]["labels"]["c28.devkuban.com/role"] == "support"
    assert {v for v, _ in api.mutations()} == {"replace"}            # no delete, drain, evict, cordon
    held = reserve.reserved_nodes(record)
    assert held[W_UID]["original_taints"] == [keep] and held[S_UID]["original_taints"] is None
    intents = [e for e in X.receipts(record, "reserve") if e["event"].startswith("intent-")]
    assert intents[0]["data"]["pre"]["fields"]["/spec/taints"] == [keep]       # pre-state on disk before the call


def test_conflicting_existing_value_is_refused(api, make_record):
    other = {"key": "c28.devkuban.com/reserved", "value": "someone-else", "effect": "NoSchedule"}
    record, kube = setup(api, make_record, w_taints=[other])
    plan, digest = reviewed(record, kube)
    assert plan["problems"]
    with pytest.raises(X.StagingError, match="conflicts"):
        reserve.apply(record, kube, plan, digest)
    assert api.mutations() == []


def test_replaced_node_is_refused(api, make_record):
    record, kube = setup(api, make_record)
    api.obj("Node", "worker-w")["metadata"]["uid"] = "bbbbbbbb-0000-4000-8000-000000000009"
    with pytest.raises(X.StagingError, match="not the recorded"):
        reserve.plan(record, kube)


def test_change_between_review_and_apply_is_refused(api, make_record):
    record, kube = setup(api, make_record)
    plan, digest = reviewed(record, kube)
    api.obj("Node", "worker-w")["spec"]["taints"] = [{"key": "late", "value": "x", "effect": "NoSchedule"}]
    with pytest.raises(X.StagingError, match="changed since review"):
        reserve.apply(record, kube, plan, digest)
    assert api.mutations() == []


def test_rollback_removes_only_own_changes_and_keeps_cordon(api, make_record):
    keep = {"key": "node.kubernetes.io/foo", "value": "x", "effect": "PreferNoSchedule"}
    record, kube = setup(api, make_record, w_taints=[keep])
    do_apply(record, kube)
    w = api.obj("Node", "worker-w")
    later = {"key": "later", "value": "y", "effect": "NoSchedule"}
    w["spec"]["taints"].append(later)
    w["spec"]["unschedulable"] = True
    do_rollback(record, kube)
    w = api.obj("Node", "worker-w")
    assert w["spec"]["taints"] == [keep, later] and w["spec"]["unschedulable"] is True
    assert "c28.devkuban.com/role" not in w["metadata"]["labels"]
    assert api.obj("Node", "worker-s")["spec"]["taints"] == []
    restored = [e["data"] for e in X.receipts(record, "reserve") if e["event"] == "node-restored"]
    assert {r["cordon_unchanged"] for r in restored} == {False, True}
    assert reserve.rollback_plan(record, kube)["actions"] == []


def test_rollback_keeps_a_label_that_existed_before(api, make_record):
    record, kube = setup(api, make_record)
    api.obj("Node", "worker-s")["metadata"]["labels"]["c28.devkuban.com/role"] = "support"
    do_apply(record, kube)
    do_rollback(record, kube)
    assert api.obj("Node", "worker-s")["metadata"]["labels"]["c28.devkuban.com/role"] == "support"


def test_verify_reports_unplanned_stateful_and_broad_tolerations(api, make_record):
    record, kube = setup(api, make_record)
    do_apply(record, kube)
    api.put(F.pod("vmstorage-0", "monitoring", "worker-s", owner=("StatefulSet", "vmstorage")))
    api.put({"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": {
        "name": "shop-6d", "namespace": "shop",
        "ownerReferences": [{"kind": "Deployment", "name": "shop", "controller": True, "uid": "u-shop"}]}})
    api.put(F.pod("shop-1", "shop", "worker-w", owner=("ReplicaSet", "shop-6d")))
    api.put(F.pod("agent-1", "ops", "worker-x", owner=("Deployment", "agent"), tolerations=[{"operator": "Exists"}]))
    out = reserve.verify(record, kube)
    text = "\n".join(out["problems"])
    assert out["result"] == "FAIL"
    assert "vmstorage-0" in text and "stateful-blocker" in text
    assert "shop/shop-1 (Deployment shop) is unplanned" in text
    assert "ops/Deployment/agent tolerates the run taint" in text


def test_verify_passes_with_only_allowed_and_evaluation_pods(api, make_record):
    record, kube = setup(api, make_record)
    do_apply(record, kube)
    api.put(F.pod("c28-ingress-1", "demo", "worker-w", owner=("ReplicaSet", "c28-ingress-1"),
                  labels={X.RUN_LABEL: RUN}))
    out = reserve.verify(record, kube)
    assert out["problems"] == [] and out["result"] == "PASS"


def test_movable_owner_is_reported_pending_not_moved(api, make_record):
    record, kube = setup(api, make_record, movable=[{"namespace": "argocd", "kind": "Deployment",
                                                     "name": "argocd-repo", "uid": "u-argo"}])
    api.put({"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": {
        "name": "argocd-repo-1", "namespace": "argocd",
        "ownerReferences": [{"kind": "Deployment", "name": "argocd-repo", "controller": True, "uid": "u-argo"}]}})
    api.put(F.pod("argocd-repo-1-x", "argocd", "worker-w", owner=("ReplicaSet", "argocd-repo-1")))
    p = reserve.plan(record, kube)
    assert [e["class"] for e in p["occupants"]["workload"] if e["pod"] == "argocd-repo-1-x"] == \
        ["movable-pending-owner-move"]
    do_apply(record, kube)
    assert api.obj("Pod", "argocd-repo-1-x", "argocd") is not None


def test_params_must_match_the_reservation(api, make_record):
    record, kube = setup(api, make_record, taint_key="other.example/key")
    with pytest.raises(X.StagingError, match="differs from the params"):
        reserve.plan(record, kube)


def test_rendered_placement_matches():
    assert reserve.rendered_placement_problems(base_params()) == []


def test_cli_plan_then_apply(api, make_record, capsys):
    record, _ = setup(api, make_record)
    assert reserve.main(["--record", record.path], api=api) == 0
    out = json.loads(capsys.readouterr().out)
    assert api.mutations() == []
    args = ["--record", record.path, "apply", "--plan", out["plan"], "--plan-digest", out["plan_digest"]]
    assert reserve.main(args, api=api) == 0
    capsys.readouterr()
    assert reserve.main(args, api=api) == 1                       # the live state moved on: refused
    assert reserve.main(["--record", record.path, "apply", "--plan-digest", out["plan_digest"]], api=api) == 1


def test_lost_response_during_apply_is_recovered_from_the_journal(api, make_record):
    """Codex #32 item 2: the node patch commits, the response is lost; the lock is held, recovery records it, and
    rollback still knows the original taints (from the intent's pre-state)."""
    import common_records as C
    record, kube = setup(api, make_record)
    plan, digest = reviewed(record, kube)
    real = api.replace

    def lost(path, body):
        out = real(path, body)
        raise C.ApiError("replace: failed (timeout)", 504)
    api.replace = lost
    with pytest.raises(X.StagingError):
        reserve.apply(record, kube, plan, digest)
    api.replace = real
    assert os.path.exists(PF.lock_file(record))
    assert reserve.rollback_plan(record, kube)["problems"]                  # unknown outcomes: recover first
    PF.recover(record, kube)
    assert W_UID in reserve.reserved_nodes(record)
    do_rollback(record, kube)
    assert api.obj("Node", "worker-w")["spec"].get("taints") == []
