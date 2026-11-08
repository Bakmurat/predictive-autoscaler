import json
import os

import pytest

import execlib as X
import part2_fixtures as PF
import fake2 as F
import reserve
from part2_fixtures import RUN, S_UID, W_UID, api, base_params, make_record, reserve_section  # noqa: F401

TAINT = {"key": "c28.devkuban.com/reserved", "value": RUN, "effect": "NoSchedule"}


def setup(api, make_record, w_taints=None, params=None, **over):
    api.put(F.node("worker-w", taints=w_taints, uid=W_UID))
    api.put(F.node("worker-s", uid=S_UID))
    section = reserve_section(**over)
    section["allowed"] = {"daemonsets": [{"namespace": "kube-system", "name": "cilium"}],
                          "mirror_pod_prefixes": ["kube-proxy-"], "broad_tolerations": []}
    section.update({k: v for k, v in over.items() if k == "allowed"})
    api.put(F.pod("cilium-abc", "kube-system", "worker-w", owner=("DaemonSet", "cilium"),
                  tolerations=[{"operator": "Exists"}]))
    api.put(F.pod("kube-proxy-worker-w", "kube-system", "worker-w", owner=("Node", "worker-w")))
    record = X.load_record(make_record({"reserve": section}, params=params), ("reserve",))
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
    assert out["reservation"] == "PASS" and out["placement"] == "PASS" and out["ready_for_experiment"] is True
    assert out["experiment_readiness"] == "READY"
    reserve.require_ready_for_experiment(out)


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


# --- reservation-stage params (Codex: reservation applied is separate from ready for calibration/experiment) ---------

def stage_params():
    """The prodcluster template with only the fields the reservation consumes filled in: every experiment field (c0,
    fixture resources, work_iterations, HPA target, routes, image, residency, ...) is still a placeholder."""
    with open(os.path.join(PF.STAGING, "..", "params", "prodcluster.template.json")) as fh:
        p = json.load(fh)
    p["run_id"] = RUN
    p["nodes"] = {"workload": "worker-w", "support": "worker-s"}
    p["reservation"]["taint_value"] = RUN
    p["release"] = "pa-eval"
    return p


CONSUMED = ["schema", "run_id", "nodes.workload", "nodes.support", "reservation.taint_key", "reservation.taint_value",
            "reservation.role_label", "control_namespace", "release"]


def _set(p, dotted, value):
    cur = p
    *parents, leaf = dotted.split(".")
    for k in parents:
        cur = cur[k]
    cur[leaf] = value


def test_stage_fields_are_exactly_what_reserve_consumes():
    assert list(X.import_harness().render.STAGE_FIELDS["reservation"]) == CONSUMED


def test_stage_params_are_incomplete_for_the_experiment(api, make_record):
    record, _ = setup(api, make_record, params=stage_params())
    with pytest.raises(X.StagingError, match="placeholder|must be"):
        X.load_params(record)                                   # the full loader still refuses them
    problems = X.experiment_params_problems(stage_params())
    text = "; ".join(problems)
    assert "c0_rps" in text and "fixture.work_iterations" in text and "routes.metrics_upstream" in text
    assert X.experiment_params_problems(base_params()) == []


def test_reservation_works_with_stage_params(api, make_record):
    record, kube = setup(api, make_record, params=stage_params())
    p = reserve.plan(record, kube)
    assert p["problems"] == [] and api.mutations() == []
    do_apply(record, kube)
    w = api.obj("Node", "worker-w")
    assert w["spec"]["taints"] == [TAINT] and w["metadata"]["labels"]["c28.devkuban.com/role"] == "workload"
    out = reserve.verify(record, kube)
    assert out["reservation"] == "PASS" and out["reservation_problems"] == []
    assert out["placement"] == "NOT READY" and out["placement_problems"] == []
    assert out["ready_for_experiment"] is False and out["result"] == "RESERVED-NOT-READY"
    assert out["experiment_readiness"].startswith("NOT READY: params incomplete (")
    assert "c0_rps" in out["experiment_readiness"] and "fixture.cpu_request" in out["experiment_readiness"]
    # an empty problems list can never be mistaken for readiness: the NOT READY item is in it
    assert out["problems"] == ["experiment readiness: " + out["experiment_readiness"]]
    with pytest.raises(X.StagingError, match="not ready for the experiment"):
        reserve.require_ready_for_experiment(out)
    do_rollback(record, kube)
    assert api.obj("Node", "worker-w")["spec"]["taints"] == []
    assert "c28.devkuban.com/role" not in api.obj("Node", "worker-s")["metadata"]["labels"]


def test_verify_fail_is_not_reported_as_reserved(api, make_record):
    record, kube = setup(api, make_record, params=stage_params())
    out = reserve.verify(record, kube)                          # nothing applied yet
    assert out["reservation"] == "FAIL" and out["placement"] == "NOT READY"
    assert out["result"] == "FAIL" and out["ready_for_experiment"] is False


def test_rollback_needs_only_reservation_stage_params(api, make_record, tmp_path):
    """Applied with complete params; the params file then loses its calibrated values: rollback still works."""
    record, kube = setup(api, make_record)
    do_apply(record, kube)
    (tmp_path / "params.json").write_text(json.dumps(stage_params()))
    with pytest.raises(X.StagingError):
        X.load_params(record)
    do_rollback(record, kube)
    assert api.obj("Node", "worker-w")["spec"]["taints"] == []
    assert reserve.rollback_plan(record, kube)["actions"] == []


@pytest.mark.parametrize("field", CONSUMED)
def test_a_placeholder_in_a_consumed_field_is_refused(api, make_record, field):
    params = stage_params()
    _set(params, field, "<still to fill>")
    record, kube = setup(api, make_record, params=params)
    for fn in (reserve.plan, reserve.rollback_plan, reserve.verify):
        with pytest.raises(X.StagingError, match="placeholder|schema must be"):
            fn(record, kube)
    assert api.mutations() == []


@pytest.mark.parametrize("field,value,why", [
    ("run_id", "C28_R1", "short DNS label"),
    ("control_namespace", "Pa-Eval", "short DNS label"),
    ("release", "pa_eval", "Helm release"),
    ("nodes.workload", "worker-s", "must differ"),
    ("nodes.support", "Worker S", "node name"),
    ("reservation.taint_key", "bad key!", "qualified name"),
    ("reservation.role_label", "c28.devkuban.com/a/b", "qualified name"),
    ("reservation.taint_value", "c28-other", "must be the run_id"),
    ("reservation.taint_value", 7, "label value"),
])
def test_a_malformed_consumed_field_is_refused(api, make_record, field, value, why):
    params = stage_params()
    _set(params, field, value)
    record, kube = setup(api, make_record, params=params)
    with pytest.raises(X.StagingError, match=why):
        reserve.plan(record, kube)


def test_cli_verify_exit_distinguishes_reserved_from_ready(api, make_record, capsys):
    record, kube = setup(api, make_record, params=stage_params())
    do_apply(record, kube)
    assert reserve.main(["--record", record.path, "verify"], api=api) == reserve.NOT_READY_EXIT
    out = json.loads(capsys.readouterr().out)
    assert out["result"] == "RESERVED-NOT-READY" and out["ready_for_experiment"] is False


def test_cli_verify_ready_with_complete_params(api, make_record, capsys):
    record, kube = setup(api, make_record)
    do_apply(record, kube)
    assert reserve.main(["--record", record.path, "verify"], api=api) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ready_for_experiment"] is True and out["placement"] == "PASS"


def test_prepare_refuses_incomplete_params(api, make_record):
    import prepare
    api.put(F.node("worker-w", uid=W_UID))
    api.put(F.node("worker-s", uid=S_UID))
    record = X.load_record(make_record({"prepare": PF.prepare_section(), "reserve": reserve_section()},
                                       params=stage_params()), ("prepare",))
    kube = record.kube(api)
    for fn in (lambda: prepare.plan(record, kube, "values"), lambda: prepare.verify(record, kube)):
        with pytest.raises(X.StagingError, match="params"):
            fn()
    assert api.mutations() == []


def test_session_reservation_stops_include_not_ready(api, make_record):
    """session.py's reservation check turns reserve verify's problems into stops: incomplete params are a stop."""
    import session
    record, kube = setup(api, make_record, params=stage_params())
    do_apply(record, kube)
    ctx = type("Ctx", (), {"record": record, "kube": kube})()
    stops = session._occupant_stops(ctx)
    assert stops and any("NOT READY" in s for s in stops)


# --- one coherent verdict (Codex #46 review, finding 2) and the calibration-side gate ---------------------------------

def test_readiness_follows_the_combined_verdict(api, make_record):
    """Complete params with a passing rendered placement but a FAILED reservation is NOT READY, with the reasons."""
    record, kube = setup(api, make_record)
    out = reserve.verify(record, kube)                          # nothing applied
    assert out["placement"] == "PASS" and out["reservation"] == "FAIL" and out["ready_for_experiment"] is False
    assert out["experiment_readiness"].startswith("NOT READY: reservation FAIL (")
    assert "the run taint is not exactly" in out["experiment_readiness"]
    assert out["experiment_readiness"] != "READY"


def test_readiness_lists_every_failing_part(api, make_record):
    record, kube = setup(api, make_record, params=stage_params())
    out = reserve.verify(record, kube)                          # nothing applied, params incomplete
    r = out["experiment_readiness"]
    assert r.startswith("NOT READY: reservation FAIL (") and " | params incomplete (" in r and "c0_rps" in r


def test_readiness_names_a_failing_rendered_placement(api, make_record, monkeypatch):
    record, kube = setup(api, make_record)
    do_apply(record, kube)
    monkeypatch.setattr(reserve, "rendered_placement_problems", lambda p: ["c28-x: tolerations wrong"])
    out = reserve.verify(record, kube)
    assert out["reservation"] == "PASS" and out["placement"] == "FAIL" and out["result"] == "FAIL"
    assert out["experiment_readiness"] == "NOT READY: rendered placement FAIL (c28-x: tolerations wrong)"
    assert out["problems"] == ["c28-x: tolerations wrong"]


def test_reservation_pass_gate_needs_no_experiment_params(api, make_record):
    """What calibration will build on: the reservation part alone, never the complete experiment parameters."""
    record, kube = setup(api, make_record, params=stage_params())
    with pytest.raises(X.StagingError, match="does not pass"):
        reserve.require_reservation_pass(reserve.verify(record, kube))
    do_apply(record, kube)
    out = reserve.verify(record, kube)
    reserve.require_reservation_pass(out)
    with pytest.raises(X.StagingError, match="not ready for the experiment"):
        reserve.require_ready_for_experiment(out)
