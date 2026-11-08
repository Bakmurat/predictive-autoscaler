"""session.py restore-plan / restore: bringing arms back to the common starting count after a session (Codex: writer
shutdown confirmed first, UID/resourceVersion protection, wait for the excess Pods to disappear)."""

import copy
import json
import os

import pytest

import common_records as C
import execlib as X
import fake2 as F
import part2_fixtures as PF
import session as S
from part2_fixtures import Clock, api, make_record, ssh_keys, world  # noqa: F401
from test_session import (T0, confirmation_world, events, problems, review, session_record, start_session,
                          stop_session, write_plan)

NS = "demo"
TARGET = "c28-h-cpu-s20"


def fleet(world, record, clock=None):
    clock = clock or Clock(T0)
    f = S.Fleet(record, record.kube(world["api"]), now=clock.now, sleep=clock.sleep)
    f.clock = clock
    return f


def scale_live(api, name, replicas):
    """What a writer does during a session: the Deployment's desired count changes and the controller follows."""
    dep = api.obj("Deployment", name, NS)
    dep["spec"]["replicas"] = replicas
    dep["metadata"]["generation"] += 1
    dep["metadata"]["resourceVersion"] = api._next_rv()
    PF.converge(api)


def loaded_and_stopped(world, replicas=6):
    """An H-CPU preparation session whose HPA scaled its target to `replicas`, then stopped (HPA and Job gone)."""
    record = session_record(world)
    c, digest, started = start_session(world, record)
    scale_live(world["api"], TARGET, replicas)
    assert stop_session(c)["data"]["resolved"] is True
    return record, c


def writers(api):
    return {k: copy.deepcopy(o["spec"]) for k, o in api.objects.items()
            if k[0] in ("pa-new", "pa-legacy", "horizontalpodautoscalers", "scaledobjects")}


def plan_restore(world, record, clock=None):
    f = fleet(world, record, clock)
    p = S.restore_plan(f)
    plan, digest = review(record, p, S.RESTORE, "restore")
    return f, p, plan, digest


def owned_pods(api, name):
    dep = api.obj("Deployment", name, NS)
    rs = {o["metadata"]["uid"] for (k, ns, _), o in api.objects.items() if k == "replicasets" and ns == NS and
          any(r["uid"] == dep["metadata"]["uid"] for r in o["metadata"].get("ownerReferences", []))}
    return [o for (k, ns, _), o in api.objects.items() if k == "pods" and ns == NS and
            any(r["uid"] in rs for r in o["metadata"].get("ownerReferences", []))]


def settle(api):
    """A Deployment controller for the instant fake: a Deployment whose generation the status has not observed gets
    exactly spec.replicas Running and Ready pods and a settled status."""
    for (kind, ns, _), dep in list(api.objects.items()):
        if kind == "deployments" and ns == NS and \
                (dep.get("status") or {}).get("observedGeneration") != dep["metadata"].get("generation"):
            api.sync_pods(dep)
            want = dep["spec"].get("replicas") or 0
            dep["status"].update(updatedReplicas=want, availableReplicas=want,
                                 observedGeneration=dep["metadata"].get("generation", 1))
            for p in owned_pods(api, dep["metadata"]["name"]):
                p.setdefault("status", {}).update(phase="Running", conditions=[{"type": "Ready", "status": "True"}])


@pytest.fixture(autouse=True)
def controller(world):
    world["api"].on_poll.append(settle)


class SlowController:
    """A Deployment controller that marks excess pods terminating and removes them only `grace` seconds later, then
    reports the rollout settled."""

    def __init__(self, clock, grace=30, stuck=False):
        self.clock, self.grace, self.stuck, self.marked = clock, grace, stuck, {}

    def __call__(self, api):
        for (k, ns, name), dep in list(api.objects.items()):
            if k != "deployments" or ns != NS:
                continue
            want = dep["spec"].get("replicas") or 0
            pods = owned_pods(api, name)
            live = [p for p in pods if not p["metadata"].get("deletionTimestamp")]
            for p in live[want:]:
                p["metadata"]["deletionTimestamp"] = "2026-11-30T00:00:00Z"
                self.marked[p["metadata"]["name"]] = self.clock.now()
            for p in pods:
                at = self.marked.get(p["metadata"]["name"])
                if at is not None and not self.stuck and self.clock.now() >= at + self.grace:
                    del api.objects[("pods", NS, p["metadata"]["name"])]
            left = owned_pods(api, name)
            if len(left) == want and not any(p["metadata"].get("deletionTimestamp") for p in left):
                dep["status"].update(replicas=want, readyReplicas=want, updatedReplicas=want, availableReplicas=want,
                                     observedGeneration=dep["metadata"]["generation"])
            else:
                dep["status"].update(replicas=len(left), readyReplicas=len(left) - sum(
                    1 for p in left if p["metadata"].get("deletionTimestamp")))


# --- the plan -----------------------------------------------------------------------------------------------------

def test_plan_binds_identity_and_takes_the_frozen_count(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    before = len(api.mutations())
    f, p, plan, digest = plan_restore(world, record)
    assert len(api.mutations()) == before                         # read-only
    assert p["problems"] == [] and p["refused"] == {}
    dep = api.obj("Deployment", TARGET, NS)
    entries = X.receipts(record, "session")
    assert p["actions"] == [{
        "op": "scale", "name": TARGET, "uid": dep["metadata"]["uid"], "arm": "H-CPU", "profile": "s20",
        "generation": dep["metadata"]["generation"], "resourceVersion": dep["metadata"]["resourceVersion"],
        "from": 6, "to": 2, "writer": {"kind": "HorizontalPodAutoscaler", "state": "absent"},
        "loaded_by": {"session": "s-0001", "started": X.last(entries, "session-started")["sha256"],
                      "closed": X.last(entries, "session-closed")["sha256"]}}]
    assert p["target"]["replicas"] == 2 and p["target"]["idle_arm_replicas"] == 2
    assert p["target"]["source"].startswith("every arm's rendered Deployment spec.replicas")


def test_the_count_comes_from_the_rendering_not_the_cli_or_a_higher_idle_budget(world):
    params = PF.base_params()
    params["residency"] = dict(params["residency"], idle_arm_replicas=4)    # a conservative budget, not the count
    record = X.load_record(world["make_record"](dict(world["extra"]), params=params))
    scale_live(world["api"], "c28-pa-r-s90", 3)
    p = S.restore_plan(fleet(world, record))
    assert [(a["name"], a["from"], a["to"]) for a in p["actions"]] == [("c28-pa-r-s90", 3, 2)]
    assert p["target"]["idle_arm_replicas"] == 4
    with pytest.raises(SystemExit):                               # there is no count argument at all
        S.main(["--record", record.path, "restore-plan", "--replicas", "5"], api=world["api"])


def test_a_block_takes_the_count_from_the_signed_run_lock_rendering(world):
    record, target = confirmation_world(world, sign_receipts=False)
    p = S.restore_plan(fleet(world, record))
    runlock = X.import_harness().runlock
    lock_digest = runlock.load_verified(record.resolve("evidence/runlock.json"), str(world["tmp"]))[0]
    assert p["problems"] == [] and p["target"]["runlock_sha256"] == lock_digest
    params = PF.base_params()
    params["hpa_cpu_target_percent"] = 70                         # params changed after locking
    record, target = confirmation_world(world, arm="H-CPU", sign_receipts=False, params=params)
    assert "H-CPU/s20: the rendering differs from the signed run-lock's" in "\n".join(
        S.restore_plan(fleet(world, record))["problems"])


def test_cli_restore_plan_and_restore(world, capsys):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    clock = Clock(T0)
    assert S.main(["--record", record.path, "restore-plan"], api=api, now=clock.now, sleep=clock.sleep) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["problems"] == [] and os.path.exists(out["plan"])
    assert S.main(["--record", record.path, "restore"], api=api) == 1             # a mutation needs the reviewed plan
    assert S.main(["--record", record.path, "restore", "--plan", out["plan"], "--plan-digest", out["plan_digest"]],
                  api=api, now=clock.now, sleep=clock.sleep) == 0
    assert api.obj("Deployment", TARGET, NS)["spec"]["replicas"] == 2


# --- preconditions ------------------------------------------------------------------------------------------------

def test_refused_while_a_session_is_running(world):
    record = session_record(world)
    c, digest, started = start_session(world, record)
    scale_live(world["api"], TARGET, 5)
    probs = "\n".join(S.restore_plan(fleet(world, record))["problems"])
    assert "sessions still open or unresolved in the journal: ['s-0001']" in probs
    assert "generator Jobs of the run still exist: ['c28-gen-s-0001']" in probs


def test_refused_while_generator_pods_remain(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    pod = F.pod("c28-gen-s-0001-zzz", NS, "worker-s", labels={X.SESSION_LABEL: "s-0001", X.RUN_LABEL: "c28-r1"})
    pod["metadata"]["deletionTimestamp"] = "2026-11-30T00:00:00Z"
    api.put(pod)
    f, p, plan, digest = plan_restore(world, record)
    assert "generator pods of the run still exist: ['c28-gen-s-0001-zzz']" in "\n".join(p["problems"])
    with pytest.raises(X.StagingError, match="has problems"):
        S.restore(f, plan, digest)
    assert api.obj("Deployment", TARGET, NS)["spec"]["replicas"] == 6


def test_refused_when_the_baseline_writer_is_present(world):
    """An HPA cannot be paused: an idle H-CPU/H-RPS arm with any HPA or ScaledObject is refused, with the reason."""
    api = world["api"]
    scale_live(api, "c28-h-rps-s90", 4)
    api.put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
             "metadata": {"name": "stray", "namespace": NS},
             "spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "c28-h-rps-s90"},
                      "minReplicas": 1, "maxReplicas": 12}})
    p = S.restore_plan(fleet(world, world["record"]))
    assert p["actions"] == []
    assert p["refused"]["c28-h-rps-s90"].startswith("writer not provably inactive: writers "
                                                    "['HorizontalPodAutoscaler/demo/stray#")


@pytest.mark.parametrize("breakage, why", [
    ("active", "is Active, not Recommend"),
    ("unobserved", "the operator has not confirmed Recommend for generation"),
    ("extra", "expected exactly its own Recommend PredictiveAutoscaler"),
])
def test_refused_when_the_pa_writer_is_not_provably_inactive(world, breakage, why):
    api = world["api"]
    name = "c28-pa-n-s20"
    scale_live(api, name, 5)
    pa = api.obj("PredictiveAutoscaler", name, NS)
    if breakage == "active":
        pa["spec"]["mode"] = "Active"
    elif breakage == "unobserved":
        api.on_poll.remove(PF.operator_sync)
        pa["metadata"]["generation"] += 1
    else:
        api.put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                 "metadata": {"name": "second", "namespace": NS},
                 "spec": {"scaleTargetRef": {"kind": "Deployment", "name": name}, "minReplicas": 1,
                          "maxReplicas": 12}})
    p = S.restore_plan(fleet(world, world["record"]))
    assert why in p["refused"][name] and name not in [a["name"] for a in p["actions"]]


def test_pa_arm_in_confirmed_recommend_is_restored(world):
    api = world["api"]
    scale_live(api, "c28-pa-e-s90", 5)
    f, p, plan, digest = plan_restore(world, world["record"])
    pa = api.obj("PredictiveAutoscaler", "c28-pa-e-s90", NS)
    assert p["actions"][0]["writer"] == {"kind": "PredictiveAutoscaler", "uid": pa["metadata"]["uid"],
                                         "generation": pa["metadata"]["generation"], "mode": "Recommend",
                                         "observed_generation": pa["status"]["observedGeneration"]}
    S.restore(f, plan, digest)
    assert api.obj("Deployment", "c28-pa-e-s90", NS)["spec"]["replicas"] == 2
    assert pa["spec"]["mode"] == "Recommend"


def test_drift_after_review_is_refused_without_writing(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    f, p, plan, digest = plan_restore(world, record)
    scale_live(api, TARGET, 7)                                    # changed after review
    before = len(api.mutations())
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        S.restore(f, plan, digest)
    assert len(api.mutations()) == before and api.obj("Deployment", TARGET, NS)["spec"]["replicas"] == 7
    assert not os.path.exists(PF.lock_file(record)) and S.open_restores(X.receipts(record, "session")) == []
    dep = api.obj("Deployment", TARGET, NS)                        # a status-only change moves the resourceVersion
    f, p, plan, digest = plan_restore(world, record)
    dep["metadata"]["resourceVersion"] = api._next_rv()
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        S.restore(f, plan, digest)


# --- the mutation -------------------------------------------------------------------------------------------------

def test_scale_goes_through_the_subresource_with_rv_and_uid_and_bounds_stay(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    f, p, plan, digest = plan_restore(world, record)
    bodies = []
    real = api.replace

    def spy(path, body):
        bodies.append((path, copy.deepcopy(body)))
        return real(path, body)
    api.replace = spy
    before_writers = writers(api)
    out = S.restore(f, plan, digest)
    a = p["actions"][0]
    assert bodies == [(f"/apis/apps/v1/namespaces/{NS}/deployments/{TARGET}/scale", {
        "apiVersion": "autoscaling/v1", "kind": "Scale",
        "metadata": {"name": TARGET, "namespace": NS, "uid": a["uid"], "resourceVersion": a["resourceVersion"]},
        "spec": {"replicas": 2}})]
    assert writers(api) == before_writers                         # no writer bound or mode was touched
    assert out["event"] == "restore-closed" and out["data"]["resolved"] is True
    assert out["data"]["done"] == [{"name": TARGET, "from": 6, "to": 2, "generation": a["generation"] + 1}]
    ev = [e for e in X.receipts(record, "session") if e["event"].startswith(("intent-", "step-", "restore-"))]
    assert [e["event"] for e in ev][-5:-2] == ["restore-begin", ev[-4]["event"], "restore-write"]
    assert ev[-4]["event"].startswith("intent-") and ev[-2]["event"].startswith("step-")
    assert ev[-1]["event"] == "restore-closed" and ev[-3]["data"]["result"] == "applied"
    intent = [e for e in ev if e["event"].startswith("intent-")][-1]["data"]
    assert intent["op"] == "restore" and intent["pre"] == {"resourceVersion": a["resourceVersion"],
                                                          "generation": a["generation"], "replicas": 6}
    assert intent["context"] == {"phase": "restore", "restore": f"r-{digest[:12]}"}
    assert len(owned_pods(api, TARGET)) == 2 and not os.path.exists(PF.lock_file(record))


def test_a_concurrent_change_between_check_and_write_is_a_conflict(world):
    """The /scale write carries the read resourceVersion: a change after validation is a 409, nothing is written, and
    the step stays unresolved until recover decides it (not applied)."""
    record, c = loaded_and_stopped(world)
    api = world["api"]
    f, p, plan, digest = plan_restore(world, record)
    real = api.replace

    def racing(path, body):
        api.obj("Deployment", TARGET, NS)["metadata"]["resourceVersion"] = api._next_rv()   # e.g. a status update
        return real(path, body)
    api.replace = racing
    with pytest.raises(X.StagingError, match="restore failed"):
        S.restore(f, plan, digest)
    api.replace = real
    assert api.obj("Deployment", TARGET, NS)["spec"]["replicas"] == 6
    assert os.path.exists(PF.lock_file(record)) and S.open_restores(X.receipts(record, "session"))
    rp = X.recover_plan(record, f.kube)
    assert [(a["op"], a["observed"], a["state"], a["why"]) for a in rp["actions"]] == [
        ("restore", "not-done", "not-applied", "the scale was not applied (refused)")]
    PF.recover(record, f.kube, S.recover_follow_up(record))
    closed = X.last(X.receipts(record, "session"), "restore-closed")["data"]
    assert closed["resolved"] is False and closed["not_done"] == {TARGET: "not-applied"} and closed["by"] == "recover"
    assert not os.path.exists(PF.lock_file(record))
    f, p, plan, digest = plan_restore(world, record)              # re-planned from the live state
    assert S.restore(f, plan, digest)["data"]["resolved"] is True


def test_waits_until_the_excess_pods_are_gone_terminating_counted(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    clock = Clock(T0)
    api.instant_pods = False
    api.on_poll.remove(settle)
    ctl = SlowController(clock, grace=30)
    api.on_poll.append(ctl)
    f, p, plan, digest = plan_restore(world, record, clock)
    seen = []
    real = S.idle_residency_problems

    def spy(dep, rs, pods, idle):
        out = real(dep, rs, pods, idle)
        if dep["metadata"]["name"] == TARGET:
            seen.append(out)
        return out
    S.idle_residency_problems = spy
    try:
        out = S.restore(f, plan, digest)
    finally:
        S.idle_residency_problems = real
    assert out["data"]["resolved"] is True
    assert any("terminating" in "; ".join(x) for x in seen) and seen[-1] == []
    assert clock.now() >= T0 + 30
    assert len(owned_pods(api, TARGET)) == 2 and not any(p["metadata"].get("deletionTimestamp")
                                                         for p in owned_pods(api, TARGET))


def test_timeout_leaves_an_unresolved_step_that_recover_resolves(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    clock = Clock(T0)
    api.instant_pods = False
    api.on_poll.remove(settle)
    ctl = SlowController(clock, stuck=True)                       # the excess pods never finish terminating
    api.on_poll.append(ctl)
    f = fleet(world, record, clock)
    f.timeout_s = 60                                              # the record's restore.timeout_s (default 600)
    p = S.restore_plan(f)
    plan, digest = review(record, p, S.RESTORE, "restore")
    assert p["timeout_s"] == 60
    with pytest.raises(X.StagingError, match="did not converge at 2 within the restore timeout"):
        S.restore(f, plan, digest)
    entries = X.receipts(record, "session")
    assert X.open_intents(entries)[0]["data"]["op"] == "restore"
    assert S.open_restores(entries) == [f"r-{digest[:12]}"] and os.path.exists(PF.lock_file(record))
    nxt = session_record(world, id="s-0002", arm="H-RPS",
                         plan=write_plan(world, "ordinary", 102, "c28-h-rps-s20", name="p2.json"))
    probs = problems(world, nxt)
    assert "restores still open" in probs and "unknown outcomes" in probs
    rp = X.recover_plan(record, f.kube)
    assert (rp["actions"][0]["observed"], rp["actions"][0]["state"]) == ("not-done", "applied-convergence-pending")
    assert "not converged" in rp["actions"][0]["why"]
    ctl.stuck = False                                             # the pods finally go
    clock.t += 60
    rp = X.recover_plan(record, f.kube)
    assert [(a["op"], a["observed"]) for a in rp["actions"]] == [("restore", "done")]
    PF.recover(record, f.kube, S.recover_follow_up(record))
    closed = X.last(X.receipts(record, "session"), "restore-closed")["data"]
    assert closed == {"restore": f"r-{digest[:12]}", "resolved": True, "done": [TARGET], "not_done": {},
                      "by": "recover"}
    assert "start-recovered" not in events(record)                # a restore is not a session
    assert not os.path.exists(PF.lock_file(record))
    assert problems(world, nxt) == ""


def test_a_new_session_is_admitted_after_the_restore(world):
    record, c = loaded_and_stopped(world)
    nxt = session_record(world, id="s-0002", arm="H-RPS",
                         plan=write_plan(world, "ordinary", 102, "c28-h-rps-s20", name="p2.json"))
    probs = problems(world, nxt)
    assert "c28-h-cpu-s20 has 6 replicas, above the residency budget's idle count 2" in probs
    assert "restore ['c28-h-cpu-s20'] to the common starting count first: session.py --record R restore-plan" in probs
    f, p, plan, digest = plan_restore(world, record)
    S.restore(f, plan, digest)
    assert problems(world, nxt) == ""
    start_session(world, nxt)
    assert S.restore_plan(fleet(world, record))["problems"][0].startswith("sessions still open")


def test_nothing_to_restore_is_refused(world):
    f, p, plan, digest = plan_restore(world, world["record"])
    assert p["actions"] == [] and p["problems"] == []
    with pytest.raises(X.StagingError, match="no arm needs a restore"):
        S.restore(f, plan, digest)


def test_an_unconverged_arm_at_the_count_is_only_waited_for(world):
    api = world["api"]
    pods = owned_pods(api, "c28-pa-r-s20")
    pods[0]["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
    clock = Clock(T0)
    f, p, plan, digest = plan_restore(world, world["record"], clock)
    assert [(a["op"], a["name"]) for a in p["actions"]] == [("wait", "c28-pa-r-s20")]
    assert "resourceVersion" not in p["actions"][0]

    def ready_later(api):                                         # the pod becomes Ready while the restore waits
        if clock.now() > T0:
            pods[0]["status"]["conditions"] = [{"type": "Ready", "status": "True"}]
    api.on_poll.append(ready_later)
    before = [c for c in api.mutations() if c[1].endswith("/scale")]
    assert S.restore(f, plan, digest)["data"]["resolved"] is True
    assert clock.now() > T0                                       # it waited
    assert [c for c in api.mutations() if c[1].endswith("/scale")] == before      # nothing was written


# --- the /scale response ------------------------------------------------------------------------------------------

def test_a_scale_response_omitting_replicas_means_zero():
    uid = F.new_uid()
    scale = {"kind": "Scale", "metadata": {"uid": uid}, "spec": {}}
    assert X.confirm_scale(scale, "Deployment", "d", uid, 0) is scale
    with pytest.raises(X.StagingError, match="confirms 0, not 2"):
        X.confirm_scale(scale, "Deployment", "d", uid, 2)
    with pytest.raises(X.StagingError, match="not a Scale for UID"):
        X.confirm_scale({"kind": "Scale", "metadata": {"uid": F.new_uid()}, "spec": {}}, "Deployment", "d", uid, 0)
    with pytest.raises(X.StagingError, match="not a Scale for UID"):
        X.confirm_scale({"kind": "Deployment", "metadata": {"uid": uid}, "spec": {}}, "Deployment", "d", uid, 0)


def test_kube_scale_to_zero_through_the_fake_api(world):
    api = world["api"]
    record = world["record"]
    dep = api.obj("Deployment", "c28-pa-r-s20", NS)
    uid, rv = dep["metadata"]["uid"], dep["metadata"]["resourceVersion"]
    kube = record.kube(api)
    with pytest.raises(X.StagingError, match="needs the shared lock"):
        kube.scale("Deployment", "c28-pa-r-s20", NS, uid, rv, 0)
    with X.mutating(record, kube, "session", {"phase": "test"}):
        out = kube.step("restore", "Deployment", NS, "c28-pa-r-s20", uid,
                        lambda mark: kube.scale("Deployment", "c28-pa-r-s20", NS, uid, rv, 0),
                        expect=[["/spec/replicas", 0]])
    assert out["kind"] == "Scale" and "replicas" not in out["spec"] and dep["spec"]["replicas"] == 0
    entries = X.receipts(record, "session")
    assert X.open_intents(entries) == [] and [e for e in entries if e["event"].startswith("step-")][-1]["data"][
        "result"] == "applied"


def test_an_unknown_journalled_op_is_never_guessed(world):
    assert X.reconcile(world["record"].kube(world["api"]), {"op": "mystery", "kind": "Deployment", "name": "x",
                                                            "namespace": NS})["observed"] == "unresolved"


# --- Codex review of #45: regressions ------------------------------------------------------------------------------

def test_the_selected_arm_must_be_at_the_starting_count_too(world):
    """Codex #45 finding 1: stop H-CPU/s20 at six replicas, then plan another session on the same arm."""
    record, c = loaded_and_stopped(world)
    again = session_record(world, id="s-0002", plan=write_plan(world, "ordinary", 102, TARGET, name="p2.json"))
    probs = problems(world, again)
    assert "c28-h-cpu-s20 has 6 replicas, above the residency budget's idle count 2" in probs
    assert "restore ['c28-h-cpu-s20'] to the common starting count first" in probs
    scale_live(world["api"], TARGET, 1)                           # below the count is not the starting count either
    probs = problems(world, again)
    assert "c28-h-cpu-s20 has 1 replicas, not the common starting count 2" in probs


def test_an_unconverged_selected_arm_is_refused(world):
    record, c = loaded_and_stopped(world, replicas=2)             # back at the count, but a pod is not Ready
    owned_pods(world["api"], TARGET)[0]["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
    again = session_record(world, id="s-0002", plan=write_plan(world, "ordinary", 102, TARGET, name="p2.json"))
    assert "c28-h-cpu-s20 has pods that are not Running and Ready" in problems(world, again)


def test_a_writer_appearing_on_a_later_arm_is_caught_before_its_write(world):
    """Codex #45 finding 2: a foreign HPA appears on the second arm after the first arm finished."""
    record, c = loaded_and_stopped(world)
    api = world["api"]
    second = "c28-h-rps-s90"
    scale_live(api, second, 6)
    f, p, plan, digest = plan_restore(world, record)
    assert [a["name"] for a in p["actions"]] == [TARGET, second]

    def foreign_writer(api):
        if api.obj("Deployment", TARGET, NS)["spec"]["replicas"] == 2 and \
                api.obj("HorizontalPodAutoscaler", "foreign", NS) is None:
            api.put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                     "metadata": {"name": "foreign", "namespace": NS},
                     "spec": {"scaleTargetRef": {"kind": "Deployment", "name": second}, "minReplicas": 1,
                              "maxReplicas": 12}})
    api.on_poll.append(foreign_writer)
    with pytest.raises(X.StagingError, match="c28-h-rps-s90: the writer is not provably inactive before the write"):
        S.restore(f, plan, digest)
    assert api.obj("Deployment", second, NS)["spec"]["replicas"] == 6
    assert not any(c[1].endswith(f"/deployments/{second}/scale") for c in api.mutations())
    closed = X.last(X.receipts(record, "session"), "restore-closed")["data"]
    assert closed["resolved"] is False and [d["name"] for d in closed["done"]] == [TARGET]
    assert not os.path.exists(PF.lock_file(record))               # refused before any write was in flight


def test_the_pre_write_check_also_runs_inside_the_step(world):
    """The writer check sits immediately before the PUT, after the intent: a writer appearing in that window leaves the
    step for recover, which finds nothing applied."""
    record, c = loaded_and_stopped(world)
    api = world["api"]
    f, p, plan, digest = plan_restore(world, record)
    real = S._pre_write_check
    calls = []

    def second_call_sees_a_writer(f_, a):
        calls.append(a["name"])
        if len(calls) == 2:
            api.put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                     "metadata": {"name": "late", "namespace": NS},
                     "spec": {"scaleTargetRef": {"kind": "Deployment", "name": TARGET}, "minReplicas": 1,
                              "maxReplicas": 12}})
        return real(f_, a)
    S._pre_write_check = second_call_sees_a_writer
    try:
        with pytest.raises(X.StagingError, match="not provably inactive before the write"):
            S.restore(f, plan, digest)
    finally:
        S._pre_write_check = real
    assert api.obj("Deployment", TARGET, NS)["spec"]["replicas"] == 6 and os.path.exists(PF.lock_file(record))
    rp = X.recover_plan(record, f.kube)
    assert rp["actions"][0]["observed"] == "unresolved" and "writer is not provably inactive" in rp["actions"][0]["why"]
    del api.objects[("horizontalpodautoscalers", NS, "late")]
    rp = X.recover_plan(record, f.kube)
    assert (rp["actions"][0]["observed"], rp["actions"][0]["state"]) == ("not-done", "not-applied")


def test_recovery_refuses_intervening_drift(world):
    """Codex #45 finding 3: the PUT conflicts, then two external changes take replicas six -> three -> two."""
    record, c = loaded_and_stopped(world)
    api = world["api"]
    f, p, plan, digest = plan_restore(world, record)
    real = api.replace

    def racing(path, body):
        api.obj("Deployment", TARGET, NS)["metadata"]["resourceVersion"] = api._next_rv()
        return real(path, body)
    api.replace = racing
    with pytest.raises(X.StagingError):
        S.restore(f, plan, digest)
    api.replace = real
    scale_live(api, TARGET, 3)
    scale_live(api, TARGET, 2)                                    # converged at the target, by someone else
    rp = X.recover_plan(record, f.kube)
    assert rp["actions"][0]["observed"] == "unresolved" and "not explained by the intent" in rp["actions"][0]["why"]
    with pytest.raises(X.StagingError, match="unresolved outcomes"):
        PF.recover(record, f.kube, S.recover_follow_up(record))
    assert os.path.exists(PF.lock_file(record)) and S.open_restores(X.receipts(record, "session"))


def test_a_single_external_change_after_a_refused_write_is_not_ours(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    f, p, plan, digest = plan_restore(world, record)
    real = api.replace

    def racing(path, body):
        api.obj("Deployment", TARGET, NS)["metadata"]["resourceVersion"] = api._next_rv()
        return real(path, body)
    api.replace = racing
    with pytest.raises(X.StagingError):
        S.restore(f, plan, digest)
    api.replace = real
    scale_live(api, TARGET, 2)                                    # one step to the target, but our write was refused
    assert X.recover_plan(record, f.kube)["actions"][0]["observed"] == "unresolved"


def test_drift_after_an_applied_write_is_unresolved(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    clock = Clock(T0)
    api.instant_pods = False
    api.on_poll.remove(settle)
    api.on_poll.append(SlowController(clock, stuck=True))
    f = fleet(world, record, clock)
    f.timeout_s = 30
    p = S.restore_plan(f)
    plan, digest = review(record, p, S.RESTORE, "restore")
    with pytest.raises(X.StagingError, match="within the restore timeout"):
        S.restore(f, plan, digest)
    writes = [e["data"] for e in X.receipts(record, "session") if e["event"] == "restore-write"]
    assert [w["result"] for w in writes] == ["applied"] and writes[0]["generation"] == p["actions"][0]["generation"] + 1
    dep = api.obj("Deployment", TARGET, NS)                       # someone rewrites the spec at the same count
    dep["metadata"]["generation"] += 1
    rp = X.recover_plan(record, f.kube)
    assert rp["actions"][0]["observed"] == "unresolved" and "changed after the restore's applied" in \
        rp["actions"][0]["why"]


def test_a_lost_scale_response_is_recovered_as_our_one_step(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    f, p, plan, digest = plan_restore(world, record)
    real = api.replace

    def lost(path, body):
        real(path, body)
        raise C.ApiError("replace: failed (timeout)", 504)
    api.replace = lost
    with pytest.raises(X.StagingError):
        S.restore(f, plan, digest)
    api.replace = real
    writes = [e["data"]["result"] for e in X.receipts(record, "session") if e["event"] == "restore-write"]
    assert writes == ["unknown"] and api.obj("Deployment", TARGET, NS)["spec"]["replicas"] == 2
    rp = X.recover_plan(record, f.kube)
    assert [(a["observed"]) for a in rp["actions"]] == ["done"]
    PF.recover(record, f.kube, S.recover_follow_up(record))
    assert X.last(X.receipts(record, "session"), "restore-closed")["data"]["resolved"] is True


def test_recovery_rechecks_writer_inactivity(world):
    record, c = loaded_and_stopped(world)
    api = world["api"]
    f, p, plan, digest = plan_restore(world, record)
    real = api.replace

    def lost(path, body):
        real(path, body)
        raise C.ApiError("replace: failed (timeout)", 504)
    api.replace = lost
    with pytest.raises(X.StagingError):
        S.restore(f, plan, digest)
    api.replace = real
    api.put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
             "metadata": {"name": "new", "namespace": NS},
             "spec": {"scaleTargetRef": {"kind": "Deployment", "name": TARGET}, "minReplicas": 1, "maxReplicas": 12}})
    rp = X.recover_plan(record, f.kube)
    assert rp["actions"][0]["observed"] == "unresolved" and "writer is not provably inactive" in rp["actions"][0]["why"]
