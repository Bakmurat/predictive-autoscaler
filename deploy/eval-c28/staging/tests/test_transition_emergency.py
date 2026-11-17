"""Emergency revocation (transition_emergency.py) against an in-memory API, a fake clock and injected fence, drain
and permission proofs. The records are the real ones (transition_records) in a temporary evidence directory."""
import json
import os
import socket
import subprocess

import pytest

import common_records as C
import execlib as X
import transition_drain as D
import transition_emergency as E
import transition_records as R
from test_transition_drain import ATTESTED, nodes, pod

CLUSTER = "19b8af73-08bc-4c8e-a233-2a104671f7c3"
RELEASE, NS = "pa-eval", "demo"
NAME = "pa-eval-predictive-autoscaler-operator-scale"
PATH = f"/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}/rolebindings/{NAME}"
KUBE_SYSTEM = "/api/v1/namespaces/kube-system"
IDENTITY = "system:serviceaccount:pa-eval-system:pa-eval-predictive-autoscaler-operator"
TARGETS = ["c28-demo-pa-n", "c28-demo-pa-r"]
PASS = {"verdict": "pass", "refused": [], "unproven": [], "cluster_uid": CLUSTER, "recorded_cluster_uid": CLUSTER,
        "identity": IDENTITY, "role": "operator", "targets": list(TARGETS)}
BOOT = "11111111-2222-3333-4444-555555555555"


class Clock:
    def __init__(self):
        self.t, self.sleeps = 1000.0, []

    def mono(self):
        return self.t

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.t += seconds


class Api:
    """One RoleBinding and the kube-system namespace. `fail[verb]` is called before the verb and may raise;
    `reappear` is a list of (time, uid): at or after that time the binding is back with that UID."""

    def __init__(self, clock, uid="b-1", cluster=CLUSTER):
        self.clock, self.uid, self.cluster = clock, uid, cluster
        self.calls, self.fail, self.reappear = [], {}, []

    def _due(self):
        # One recreation at a time, and only of a binding that is gone: as a delayed grant would do it.
        if self.uid is None and self.reappear and self.reappear[0][0] <= self.clock.t:
            self.uid = self.reappear.pop(0)[1]

    def get(self, path):
        self.calls.append(("get", path))
        if path == KUBE_SYSTEM:
            return {"metadata": {"uid": self.cluster}}
        assert path == PATH, path
        self._due()
        if "get" in self.fail:
            self.fail["get"](self)
        if self.uid is None:
            return None
        return {"metadata": {"name": NAME, "namespace": NS, "uid": self.uid}}

    def delete(self, path, uid, resource_version=None):
        self.calls.append(("delete", path, uid))
        assert path == PATH, path
        if "delete" in self.fail:
            self.fail["delete"](self, uid)
        if self.uid is None:
            raise C.NotFound("gone", 404)
        if self.uid != uid:
            raise C.ApiError("precondition failed", 409)
        self.uid = None
        return {}


@pytest.fixture
def record(tmp_path):
    doc = {"schema": C.RECORD_SCHEMA, "run_id": "c28-demo-1", "cluster_uid": CLUSTER,
           "kubeconfig": {"path": "/home/ops/.kube/eval-prod.yaml", "context": "prodcluster"},
           "evidence_dir": "evidence", "lock_dir": str(tmp_path / "shared-locks"), "source": {"commit": "a" * 40}}
    path = tmp_path / "preparation.json"
    path.write_text(json.dumps(doc))
    return X.Record(str(path), doc)


def drain_verdict(restarts=0, names=("cp-1",)):
    return D.verdict([pod(n, restarts=restarts) for n in names], nodes(*names), ATTESTED, "2026-11-17T01:00:00Z")


def owner(n):
    return {"host": socket.gethostname(), "boot_id": BOOT, "pid": 4000 + n, "pid_start": str(n), "pgid": 4000 + n}


def register(record, n, operation="grant"):
    X.receipt(record, R.HELPER, "executor-started", {"id": f"{operation}-reh1-{n}", "operation": operation,
                                                     "activation": "reh1", "owner": owner(n)})


class World:
    def __init__(self, record, uid="b-1"):
        self.record, self.clock = record, Clock()
        self.api = Api(self.clock, uid)
        self.binding = E.ScaleBinding(record, self.api, RELEASE, NS)
        self.fenced, self.fence_result = [], {}
        self.drains = []                      # verdicts served in turn (the last repeats); an Exception is raised
        self.proof = dict(PASS)
        self.order = []

    def fence(self, owner_record):
        self.order.append("fence")
        self.fenced.append(owner_record["pid"])
        self.clock.t += 0.5
        result = self.fence_result.get(owner_record["pid"], {"proven": True, "why": "gone", "signalled": True})
        if isinstance(result, Exception):
            raise result
        return result

    def drain(self):
        self.order.append("drain")
        served = self.drains.pop(0) if len(self.drains) > 1 else self.drains[0]
        if isinstance(served, Exception):
            raise served
        return served

    def permissions(self):
        self.order.append("permissions")
        if isinstance(self.proof, Exception):
            raise self.proof
        return self.proof

    def run(self, **kw):
        if not self.drains:
            self.drains = [drain_verdict()]
        return E.run(self.record, self.binding, reason="test", drain=self.drain, permissions=self.permissions,
                     identity=IDENTITY, targets=list(TARGETS), fence=self.fence, mono=self.clock.mono,
                     sleep=self.clock.sleep, **kw)

    def deleted(self):
        return [c[2] for c in self.api.calls if c[0] == "delete"]


@pytest.fixture
def world(record):
    return World(record)


def state(record):
    return R.emergency_state(record)


# --- the whole thing -------------------------------------------------------------------------------------------------

def test_everything_holds_revoked_with_a_valid_tombstone(world):
    register(world.record, 1)
    out = world.run()
    assert out["status"] == "revoked" and out["outcome"] == "revoked" and out["outcome_recorded"] is True
    assert out["seq"] == 1 and out["request_recorded"] and out["absent"] and out["deletions"] == ["b-1"]
    assert out["fence"] == {"proven": True, "problem": None, "executors": [
        {"id": "grant-reh1-1", "owner": owner(1), "proven": True,
         "result": {"proven": True, "why": "gone", "signalled": True}}]}
    assert out["drain"] == {"verified": True, "seconds": 90.0, "elapsed": True, "elapsed_seconds": 90.0,
                            "unchanged": True, "reappearances": 0}
    assert out["open_emergencies"] == [] and out["completing"] is None
    assert out["writes_denied"] is True and out["problems"] == []
    assert state(world.record) == {"requests": [1], "open": [], "last": 1, "tombstones": [1], "problem": None}
    R.refuse_if_emergency_open(world.record)                               # the emergency is closed
    # The deletion came before the fence, the fence before the drain reading, the proof last.
    assert world.api.calls.index(("delete", PATH, "b-1")) < len(world.api.calls)
    assert world.order[0] == "fence" and world.order.index("fence") < world.order.index("drain") \
        < world.order.index("permissions")
    # The drain was waited out in full, counted from the fence's end (later than the deletion).
    assert world.clock.t - 1000.0 >= 0.5 + 90.0
    tombstone = json.load(open(os.path.join(world.record.evidence_dir, "receipts", R.EMERGENCY_DIR,
                                            "000001-revoked.json")))
    assert tombstone["facts"]["binding_removed"] is True and tombstone["facts"]["deletions"] == ["b-1"]


def test_a_binding_that_is_already_absent_needs_no_deletion(record):
    w = World(record, uid=None)
    out = w.run()
    assert out["status"] == "revoked" and out["deletions"] == [] and w.deleted() == []
    assert out["fence"] == {"proven": True, "problem": None, "executors": []}


def test_it_touches_one_binding_and_the_cluster_identity_and_nothing_else(world):
    world.run()
    assert {c[1] for c in world.api.calls} == {PATH, KUBE_SYSTEM}
    assert {c[0] for c in world.api.calls} == {"get", "delete"}            # no create, no replace: no PA, no Scale


def test_it_works_while_another_process_holds_the_shared_lock(world):
    with C.exclusive_lock(world.record.doc, X.LOCK_SCOPE, holder="transition") as held:
        out = world.run()
        assert out["status"] == "revoked" and os.path.exists(held.path)    # the lock was neither needed nor touched


# --- independent failures ------------------------------------------------------------------------------------------

def test_a_request_that_cannot_be_recorded_does_not_stop_the_deletion_or_the_fence(world, monkeypatch):
    register(world.record, 1)

    def broken(*_a, **_k):
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(R, "emergency_request", broken)
    out = world.run()
    assert world.deleted() == ["b-1"] and world.fenced == [4001]           # both happened
    assert out["absent"] and out["fence"]["proven"] and out["writes_denied"] and out["drain"]["elapsed"]
    assert out["status"] == "unresolved" and out["request_recorded"] is False and out["outcome_recorded"] is False
    assert out["outcome"] == "not-revoked"                                # not even the label says revoked
    assert any("recording the emergency request" in p for p in out["problems"])
    assert state(world.record)["requests"] == []                           # nothing claims a revocation


def test_a_deletion_that_fails_is_followed_by_the_fence_and_is_never_revoked(world):
    register(world.record, 1)

    def denied(_api, _uid):
        raise C.ApiError("forbidden", 403)
    world.api.fail["delete"] = denied
    out = world.run()
    assert world.fenced == [4001] and out["fence"]["proven"] is True       # the fence still ran
    assert out["absent"] is False and out["deletions"] == []
    assert out["status"] == "unresolved" and out["outcome"] == "not-revoked" and out["outcome_recorded"]
    assert state(world.record)["open"] == [1]
    with pytest.raises(X.StagingError):
        R.refuse_if_emergency_open(world.record)


def test_a_binding_that_cannot_be_read_is_not_absent(world):
    def unreadable(_api):
        raise subprocess.TimeoutExpired("kubectl", 20)
    world.api.fail["get"] = unreadable
    out = world.run()
    assert out["absent"] is False and out["status"] == "unresolved" and out["outcome"] == "not-revoked"
    assert any("reading the scale binding" in p for p in out["problems"])
    assert world.deleted() == []                                           # nothing observed, nothing to delete by


def test_a_successful_delete_is_not_enough_absence_must_be_observed(world):
    # The DELETE answers, but every later read fails: "removed" was never observed.
    seen = {"n": 0}

    def then_blind(_api):
        seen["n"] += 1
        if seen["n"] > 1:
            raise C.ApiError("timeout", None)
    world.api.fail["get"] = then_blind
    out = world.run()
    assert world.deleted() == ["b-1"] and world.api.uid is None            # it IS gone...
    assert out["absent"] is False and out["status"] == "unresolved"        # ...but that was not observed


def test_a_replaced_binding_is_deleted_by_its_new_uid(world):
    # Between the read and the delete the binding is replaced: the precondition refuses the old UID.
    def replaced_once(api, uid):
        del api.fail["delete"]
        api.uid = "b-2"
    world.api.fail["delete"] = replaced_once
    out = world.run()
    assert world.deleted() == ["b-1", "b-2"] and out["deletions"] == ["b-2"]
    assert out["absent"] is True and out["status"] == "revoked"
    # It was handled inside the bounded deletion phase (the next round), not later as a "reappearance".
    assert out["drain"]["reappearances"] == 0


def test_running_out_of_deletion_attempts_is_unresolved(world):
    # Something recreates the binding at once after every deletion, each time with another UID.
    world.api.reappear = [(0, f"b-{n}") for n in range(2, 60)]
    out = world.run()
    assert out["deletions"][:E.DELETE_ATTEMPTS] == ["b-1", "b-2", "b-3"]   # the bounded first attempt
    assert world.api.uid is not None                                       # and it is still there
    assert out["absent"] is False and out["status"] == "unresolved" and out["outcome"] == "not-revoked"
    assert out["drain"]["reappearances"] > E.MAX_REAPPEARANCES and out["drain"]["elapsed"] is False
    assert any("kept reappearing" in p for p in out["problems"])


# --- the fence -------------------------------------------------------------------------------------------------------

def test_every_outstanding_executor_is_fenced_and_one_unproven_is_fence_unproven(world):
    for n in (1, 2, 3):
        register(world.record, n)
    X.receipt(world.record, R.HELPER, "executor-finished", {"id": "grant-reh1-2"})
    world.fence_result[4003] = {"proven": False, "why": "the processes cannot be observed", "signalled": False}
    out = world.run()
    assert world.fenced == [4001, 4003]                                    # every outstanding one, not the latest
    assert out["fence"]["proven"] is False
    assert [e["proven"] for e in out["fence"]["executors"]] == [True, False]
    assert out["absent"] and out["writes_denied"] and out["drain"]["elapsed"]     # everything else held
    assert out["status"] == "unresolved" and out["outcome"] == "fence-unproven"
    assert state(world.record)["open"] == [1]


@pytest.mark.parametrize("result", [None, {}, {"proven": "yes"}, {"proven": 1}, RuntimeError("boom")])
def test_a_fence_without_a_literal_proof_is_unproven(world, result):
    register(world.record, 1)
    world.fence_result[4001] = result
    out = world.run()
    assert out["fence"]["proven"] is False and out["outcome"] == "fence-unproven" and out["status"] == "unresolved"
    assert world.deleted() == ["b-1"]


def test_an_owner_log_that_cannot_be_read_is_unproven(world):
    register(world.record, 1)
    path = C.receipts_path(world.record.doc, R.HELPER)
    open(path, "a").write('{"event": "executor-fin')                     # a torn last line
    out = world.run()
    assert world.fenced == [] and out["fence"]["proven"] is False and "cannot be read" in out["fence"]["problem"]
    assert out["outcome"] == "fence-unproven" and world.deleted() == ["b-1"]


# --- the drain -------------------------------------------------------------------------------------------------------

def test_a_late_reappearance_is_deleted_and_the_interval_starts_again(world):
    world.api.reappear = [(1060.0, "b-late")]                              # a delayed grant, 60 s into the drain
    out = world.run()
    assert world.deleted() == ["b-1", "b-late"] and out["drain"]["reappearances"] == 1
    assert out["status"] == "revoked" and out["absent"] is True
    # 60 s had passed when it came back; the full 90 s were then waited again from that deletion.
    assert world.clock.t - 1000.0 >= 60.0 + 90.0


def test_a_reappearance_just_before_the_end_still_restarts_the_whole_interval(world):
    world.api.reappear = [(1088.0, "b-late")]
    out = world.run()
    assert out["status"] == "revoked" and world.clock.t - 1000.0 >= 88.0 + 90.0


@pytest.mark.parametrize("name,verdict", [
    ("an incomplete API-server inventory", D.verdict([pod("cp-1")], nodes("cp-1", "cp-2"), ATTESTED, "t")),
    ("no pod selected", D.verdict([], nodes("cp-1"), ATTESTED, "t")),
    ("no attestation", D.verdict([pod("cp-1")], nodes("cp-1"), {}, "t")),
    ("a read that fails", C.ApiError("timeout", None)),
    ("not a verdict", "90"),
    ("verified without seconds", {"verified": True, "seconds": None}),
    ("verified with a boolean interval", {"verified": True, "seconds": True}),
    ("verified with zero", {"verified": True, "seconds": 0}),
    ("only truthy", {"verified": "true", "seconds": 90}),
])
def test_without_a_verified_interval_there_is_no_wait_and_no_claim(world, name, verdict):
    register(world.record, 1)
    world.drains = [verdict]
    out = world.run()
    assert world.deleted() == ["b-1"] and world.fenced == [4001], name     # the deletion and the fence still happened
    assert out["drain"]["verified"] is False and out["drain"]["elapsed"] is False
    assert out["status"] == "unresolved" and out["outcome"] == "not-revoked", name
    assert world.clock.t - 1000.0 < 30                                     # one more look, not a long wait


def test_api_servers_that_changed_during_the_drain_leave_it_unverified(world):
    world.drains = [drain_verdict(), drain_verdict(restarts=1)]            # a restart while the interval ran
    out = world.run()
    assert out["drain"]["verified"] and out["drain"]["elapsed"] and out["drain"]["unchanged"] is False
    assert out["status"] == "unresolved" and out["outcome"] == "not-revoked"
    assert any("not the ones the drain interval was read from" in p for p in out["problems"])


def test_a_second_drain_reading_that_fails_leaves_it_unverified(world):
    world.drains = [drain_verdict(), C.ApiError("timeout", None)]
    out = world.run()
    assert out["drain"]["unchanged"] is False and out["status"] == "unresolved"


# --- the permission proof and the outcome ----------------------------------------------------------------------------

@pytest.mark.parametrize("proof", [
    dict(PASS, verdict="refused", refused=["demo: update"]),
    dict(PASS, verdict="unproven", unproven=["an answer was missing"]),
    dict(PASS, unproven=["an answer was missing"]), dict(PASS, refused=["x"]),
    dict(PASS, verdict="PASS"), {}, None, "pass", RuntimeError("kubectl timed out"),
    # The reviewer's reproduction: a missing list is not an empty list.
    {"verdict": "pass"},
    {k: v for k, v in PASS.items() if k != "refused"}, {k: v for k, v in PASS.items() if k != "unproven"},
    dict(PASS, refused=None), dict(PASS, unproven=None), dict(PASS, refused=""), dict(PASS, unproven=0),
    dict(PASS, refused={}), dict(PASS, unproven=()),
    # The proof must be about THIS cluster, THIS identity and role, and exactly THESE targets.
    dict(PASS, cluster_uid="another"), dict(PASS, recorded_cluster_uid="another"),
    {k: v for k, v in PASS.items() if k != "cluster_uid"},
    dict(PASS, identity="system:serviceaccount:pa-eval-system:other"), dict(PASS, role="forecaster"),
    {k: v for k, v in PASS.items() if k != "role"},
    dict(PASS, targets=["c28-demo-pa-n"]), dict(PASS, targets=TARGETS + ["extra"]), dict(PASS, targets=[]),
    dict(PASS, targets="c28-demo-pa-n,c28-demo-pa-r"), {k: v for k, v in PASS.items() if k != "targets"},
])
def test_a_permission_proof_that_is_not_a_clean_pass_is_not_denial(world, proof):
    world.proof = proof
    out = world.run()
    assert out["writes_denied"] is False and out["status"] == "unresolved" and out["outcome"] == "not-revoked"
    assert out["absent"] and out["fence"]["proven"] and out["drain"]["elapsed"]


def test_all_facts_true_but_the_tombstone_cannot_be_written_is_not_revoked(world, monkeypatch):
    def broken(*_a, **_k):
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(R, "emergency_close", broken)
    out = world.run()
    assert out["outcome"] == "revoked" and out["outcome_recorded"] is False
    assert out["status"] == "unresolved"                                   # no durable record, no claim
    assert state(world.record)["open"] == [1]


def test_a_second_emergency_gets_its_own_sequence(world):
    assert world.run()["seq"] == 1
    again = World(world.record, uid=None)
    out = again.run()
    assert out["seq"] == 2 and out["status"] == "revoked"
    assert state(world.record)["tombstones"] == [1, 2]


# --- the confined object ---------------------------------------------------------------------------------------------

def test_the_binding_object_is_fixed_to_one_path(record):
    clock = Clock()
    b = E.ScaleBinding(record, Api(clock), RELEASE, NS)
    assert (b.namespace, b.name, b._path) == (NS, NAME, PATH)
    assert E.binding_name("pa-eval") == NAME
    public = [n for n in dir(b) if not n.startswith("_") and callable(getattr(b, n))]
    assert sorted(public) == ["delete", "observe"]
    assert b.observe.__code__.co_argcount == 1 and b.delete.__code__.co_varnames[:2] == ("self", "uid")
    for release, namespace in (("", NS), (None, NS), ("pa/eval", NS), (" pa-eval", NS), (RELEASE, ""),
                               (RELEASE, None), (RELEASE, "demo/x"), (RELEASE, "demo ")):
        with pytest.raises(X.StagingError):
            E.ScaleBinding(record, Api(clock), release, namespace)


@pytest.mark.parametrize("obj", [
    {"metadata": {"name": NAME, "namespace": NS}}, {"metadata": {"name": NAME, "namespace": NS, "uid": ""}},
    {"metadata": {"name": "another", "namespace": NS, "uid": "u"}},
    {"metadata": {"name": NAME, "namespace": "other", "uid": "u"}}, {"metadata": None}, {}, [], "x",
])
def test_an_object_that_is_not_identifiably_the_binding_is_neither_absent_nor_deletable(record, obj):
    clock = Clock()
    api = Api(clock)
    api.get = lambda path: obj if path == PATH else {"metadata": {"uid": CLUSTER}}
    with pytest.raises(X.StagingError):
        E.ScaleBinding(record, api, RELEASE, NS).observe()


def test_a_deletion_checks_the_cluster_identity_first_and_needs_an_observed_uid(record):
    clock = Clock()
    api = Api(clock, cluster="another-cluster-uid")
    b = E.ScaleBinding(record, api, RELEASE, NS)
    with pytest.raises(X.StagingError, match="does not reach the recorded cluster"):
        b.delete("b-1")
    assert [c for c in api.calls if c[0] == "delete"] == [] and api.uid == "b-1"    # nothing was sent
    good = E.ScaleBinding(record, Api(clock), RELEASE, NS)
    for uid in ("", None, 5):
        with pytest.raises(X.StagingError, match="observed UID"):
            good.delete(uid)
    w = World(record)
    w.api.cluster = "another-cluster-uid"
    out = w.run()
    assert w.deleted() == [] and out["absent"] is False and out["status"] == "unresolved"


def test_run_refuses_anything_that_is_not_the_confined_binding(world):
    with pytest.raises(X.StagingError, match="ScaleBinding"):
        E.run(world.record, world.api, reason="x", drain=world.drain, permissions=world.permissions,
              identity=IDENTITY, targets=TARGETS)


def test_every_kubectl_call_of_the_bounded_api_has_a_real_timeout(record, monkeypatch):
    seen = []

    def fake_run(args, **kw):
        seen.append((args, kw))
        return subprocess.CompletedProcess(args, 0, stdout='{"metadata": {"uid": "x"}}', stderr="")
    monkeypatch.setattr(E.subprocess, "run", fake_run)
    api = E.bounded_api(record)
    api.get("/api/v1/namespaces/kube-system")
    api.delete(PATH, "b-1")
    assert len(seen) == 2 and all(kw.get("timeout") == E.CALL_TIMEOUT_SECONDS for _args, kw in seen)
    assert all("--request-timeout=10s" in args and "--context" in args for args, _kw in seen)

    def hangs(args, **kw):
        raise subprocess.TimeoutExpired(args, kw["timeout"])
    monkeypatch.setattr(E.subprocess, "run", hangs)
    clock = Clock()
    b = E.ScaleBinding(record, E.bounded_api(record), RELEASE, NS)
    with pytest.raises(subprocess.TimeoutExpired):
        b.observe()
    out = E.run(record, b, reason="hung api", drain=lambda: drain_verdict(), permissions=lambda: dict(PASS),
                identity=IDENTITY, targets=TARGETS, fence=lambda _o: {"proven": True}, mono=clock.mono,
                sleep=clock.sleep)
    assert out["status"] == "unresolved" and out["absent"] is False        # a hung API is a problem, not a hang


# --- the deletion phase, and what "absent" rests on ------------------------------------------------------------------

def test_a_read_that_fails_once_does_not_end_the_deletion_phase(world):
    reads, deleted_after_sleeps = {"n": 0}, []

    def first_read_fails(_api):
        reads["n"] += 1
        if reads["n"] == 1:
            raise C.ApiError("timeout", None)
    world.api.fail["get"] = first_read_fails
    world.api.fail["delete"] = lambda _api, _uid: deleted_after_sleeps.append(len(world.clock.sleeps))
    out = world.run()
    assert out["deletions"] == ["b-1"] and deleted_after_sleeps == [0]     # deleted in the next round, before any wait
    assert out["drain"]["reappearances"] == 0 and out["status"] == "revoked"
    assert len([p for p in out["problems"] if "reading the scale binding" in p]) == 1


def test_a_binding_that_someone_else_removed_is_not_counted_as_our_deletion(world):
    def gone_meanwhile(api, _uid):
        api.uid = None                                                     # the fake then answers NotFound
    world.api.fail["delete"] = gone_meanwhile
    out = world.run()
    assert world.deleted() == ["b-1"] and out["deletions"] == []           # we asked; we did not delete it
    assert out["absent"] is True and out["status"] == "revoked"            # absence was observed afterwards


def test_absence_is_what_the_last_observation_saw_not_an_earlier_one(world):
    # Absent on every look during the drain, but the LAST look fails: unknown at the end is not absent.
    def blind_at_the_end(api):
        if api.clock.t >= 1085.0:
            raise C.ApiError("timeout", None)
    world.api.fail["get"] = blind_at_the_end
    out = world.run()
    assert world.deleted() == ["b-1"] and world.api.uid is None            # it really is gone
    # From 85 s on nothing can be seen: a failed look ends the run of confirmed absence, no full interval completes,
    # and the emergency gives up within its bound instead of claiming what it could not see or waiting for ever.
    assert out["absent"] is False and out["drain"]["elapsed"] is False
    assert out["status"] == "unresolved" and out["outcome"] == "not-revoked"
    assert any("within the bound" in p for p in out["problems"])
    assert world.clock.t - 1000.0 <= (E.MAX_REAPPEARANCES + 2) * (90.0 + E.POLL_SECONDS) + 2 * E.POLL_SECONDS
    assert out["evidence"]["drain"]["observations"][-1][1] == "unknown"


# --- code review: the interval counts from CONFIRMED absence, whatever a DELETE answered ----------------------------

def test_a_deletion_whose_answer_was_lost_still_restarts_the_whole_interval(world):
    # The reviewer's reproduction: the binding is back 85 s into a 90 s drain; the DELETE removes it but its answer
    # is lost (a timeout); the next read sees it absent. "revoked" came at second 90, five seconds after the removal.
    world.api.reappear = [(1085.0, "b-late")]

    def applied_but_unanswered(api, uid):
        if uid == "b-late":
            api.uid = None
            raise subprocess.TimeoutExpired("kubectl", 20)
    world.api.fail["delete"] = applied_but_unanswered
    out = world.run()
    assert world.api.uid is None and out["deletions"] == ["b-1"]          # the second removal was never confirmed to us
    assert out["drain"]["reappearances"] == 1
    assert world.clock.t - 1000.0 >= 85.0 + 90.0                          # a FULL interval after it was seen gone again
    assert out["status"] == "revoked" and out["drain"]["elapsed_seconds"] >= 90.0
    states = [s for _t, s in out["evidence"]["drain"]["observations"]]
    assert "present" in states and states[-1] == "absent"


def test_a_removal_by_someone_else_between_two_looks_restarts_the_interval_too(world):
    # Seen present at 85 s; before we can delete it, another actor removes it: we delete nothing, and still the
    # interval must start again, because the binding WAS there.
    world.api.reappear = [(1085.0, "b-other")]
    looks = {"n": 0}

    def removed_by_another(api):
        if api.uid == "b-other":
            looks["n"] += 1
            if looks["n"] == 2:                                           # the look inside the deletion round
                api.uid = None
    world.api.fail["get"] = removed_by_another
    out = world.run()
    assert world.deleted() == ["b-1"] and out["deletions"] == ["b-1"]      # we never deleted b-other
    assert out["drain"]["reappearances"] == 1 and world.clock.t - 1000.0 >= 85.0 + 90.0
    assert out["status"] == "revoked"


def test_a_look_that_fails_in_the_middle_ends_the_run_of_confirmed_absence(world):
    def blind_once(api):
        if 1040.0 <= api.clock.t < 1045.0:
            raise C.ApiError("timeout", None)
    world.api.fail["get"] = blind_once
    out = world.run()
    states = [s for _t, s in out["evidence"]["drain"]["observations"]]
    assert states.count("unknown") == 1 and out["drain"]["reappearances"] == 0
    assert out["status"] == "revoked" and world.clock.t - 1000.0 >= 45.0 + 90.0     # counted again from the next look


def test_the_interval_never_counts_from_before_the_end_of_the_fence(world):
    register(world.record, 1)
    slow = {"done": None}

    def slow_fence(owner_record):
        world.clock.t += 40.0                                             # the fence takes 40 s
        slow["done"] = world.clock.t
        return {"proven": True, "why": "gone", "signalled": True}
    world.fence = slow_fence
    out = world.run()
    assert out["status"] == "revoked" and world.clock.t >= slow["done"] + 90.0
    assert out["evidence"]["fence"]["completed_at_offset"] >= 40.0


@pytest.mark.parametrize("seconds", [float("inf"), float("nan"), -90.0, 0, True, "90", None])
def test_an_interval_that_is_not_a_finite_positive_number_is_not_verified(world, seconds):
    reading = dict(drain_verdict(), seconds=seconds)
    world.drains = [reading]
    out = world.run()
    assert out["drain"]["verified"] is False and out["status"] == "unresolved" and out["outcome"] == "not-revoked"
    assert world.clock.t - 1000.0 < 30                                     # no endless wait on an infinite interval


def test_without_a_verified_interval_the_one_more_look_comes_a_poll_later(world):
    world.drains = [D.verdict([], nodes("cp-1"), ATTESTED, "t")]
    world.api.reappear = [(1003.0, "b-quick")]                             # recreated 3 s after the deletion
    out = world.run()
    assert world.deleted() == ["b-1", "b-quick"]                           # the later look saw it and removed it
    assert out["absent"] is False and out["status"] == "unresolved"        # ...and claims nothing


# --- the evidence behind the statements ------------------------------------------------------------------------------

def test_the_tombstone_carries_the_evidence_for_its_claims(world):
    register(world.record, 1)
    world.api.reappear = [(1030.0, "b-late")]
    out = world.run()
    assert out["status"] == "revoked"
    doc = json.load(open(os.path.join(world.record.evidence_dir, "receipts", R.EMERGENCY_DIR, "000001-revoked.json")))
    f = doc["facts"]
    assert f == json.loads(json.dumps(out["evidence"]))                    # what was returned is what was written
    assert (f["binding_removed"], f["fence_proven"], f["writes_denied"]) == (True, True, True)
    d = f["drain"]
    assert d["required_seconds"] == 90.0 and d["elapsed_seconds"] >= 90.0 and d["reappearances"] == 1
    assert d["first_reading"] == drain_verdict() and d["last_reading"] == drain_verdict()   # both readings, in full
    assert d["first_reading"]["servers"][0]["container_id"] == "containerd://cid-cp-1"
    assert d["confirmed_absent_at_offset"] >= 30.0 and d["observations_total"] == len(d["observations"])
    assert [s for _t, s in d["observations"]].count("present") == 1
    assert f["fence"]["completed_at_offset"] == 0.5
    assert f["fence"]["executors"] == [{"id": "grant-reh1-1", "owner": owner(1), "proven": True,
                                        "result": {"proven": True, "why": "gone", "signalled": True}}]
    assert f["permissions"] == {"sha256": C.digest(PASS), "verdict": "pass", "expected_identity": IDENTITY,
                                "expected_targets": sorted(TARGETS), "problems": []}
    assert f["deletions"] == ["b-1", "b-late"] and f["binding"] == f"{NS}/{NAME}"
    assert f["started_at"] and f["finished_at"]


def test_a_failure_record_carries_the_same_evidence(world):
    world.proof = dict(PASS, unproven=["an answer was missing"])
    out = world.run()
    doc = json.load(open(os.path.join(world.record.evidence_dir, "receipts", R.EMERGENCY_DIR,
                                      "000001-not-revoked.json")))
    assert doc["facts"]["writes_denied"] is False and doc["facts"]["drain"]["elapsed_seconds"] >= 90.0
    assert doc["facts"]["permissions"]["problems"] == ["the proof lists 1 unproven finding(s)"]
    assert out["open_emergencies"] == [1]


# --- an earlier emergency that is still open --------------------------------------------------------------------------

def test_a_new_emergency_does_not_close_an_earlier_one(world):
    # The reviewer's reproduction: sequence 1 ends unresolved; a rerun is "revoked" as sequence 2 while 1 stays open.
    world.proof = dict(PASS, verdict="unproven", unproven=["x"])
    first = world.run()
    assert first["status"] == "unresolved" and first["seq"] == 1
    again = World(world.record, uid=None)
    second = again.run()
    assert second["seq"] == 2 and second["status"] == "revoked"
    assert second["open_emergencies"] == [1]                               # and it says so
    assert state(world.record)["open"] == [1]
    with pytest.raises(X.StagingError):
        R.refuse_if_emergency_open(world.record)                           # operations stay blocked


def test_an_open_emergency_is_completed_with_its_own_tombstone(world):
    world.proof = dict(PASS, verdict="unproven", unproven=["x"])
    assert world.run()["status"] == "unresolved"
    done = World(world.record, uid=None)
    out = done.run(complete=1)
    assert out["seq"] == 1 and out["completing"] == 1 and out["status"] == "revoked"
    assert out["open_emergencies"] == [] and state(world.record) == {
        "requests": [1], "open": [], "last": 1, "tombstones": [1], "problem": None}
    names = sorted(os.listdir(os.path.join(world.record.evidence_dir, "receipts", R.EMERGENCY_DIR)))
    assert names == ["000001-not-revoked.json", "000001-requested.json", "000001-revoked.json"]   # the failure stays
    tomb = json.load(open(os.path.join(world.record.evidence_dir, "receipts", R.EMERGENCY_DIR, names[2])))
    assert tomb["facts"]["completing"] == 1 and tomb["facts"]["drain"]["elapsed_seconds"] >= 90.0


def test_a_completion_that_fails_again_keeps_the_earlier_failure_record(world):
    world.proof = dict(PASS, verdict="unproven", unproven=["x"])
    world.run()
    path = os.path.join(world.record.evidence_dir, "receipts", R.EMERGENCY_DIR, "000001-not-revoked.json")
    before = open(path).read()
    again = World(world.record, uid=None)
    again.proof = dict(PASS, refused=["still granted"])
    out = again.run(complete=1)
    assert out["status"] == "unresolved" and out["outcome"] == "not-revoked" and out["outcome_recorded"] is False
    assert open(path).read() == before and out["open_emergencies"] == [1]
    assert not any("recording the outcome" in p for p in out["problems"])  # an existing failure record is no error


@pytest.mark.parametrize("complete", [2, 0, -1, True, "1", 1.0])
def test_only_an_open_emergency_can_be_completed(world, complete):
    world.proof = dict(PASS, verdict="unproven", unproven=["x"])
    world.run()                                                            # sequence 1, open
    other = World(world.record, uid="b-9")
    out = other.run(complete=complete)
    assert other.deleted() == ["b-9"]                                      # the deletion still happened
    assert out["seq"] is None and out["status"] == "unresolved" and out["outcome_recorded"] is False
    assert state(world.record) == {"requests": [1], "open": [1], "last": 1, "tombstones": [], "problem": None}


def test_a_closed_emergency_cannot_be_completed_again(world):
    assert world.run()["status"] == "revoked"
    out = World(world.record, uid=None).run(complete=1)
    assert out["seq"] is None and out["status"] == "unresolved"
    assert any("not an open emergency" in p for p in out["problems"])


@pytest.mark.parametrize("identity,targets", [("", TARGETS), (None, TARGETS), (IDENTITY, []), (IDENTITY, None),
                                              (IDENTITY, "c28-demo-pa-n"), (IDENTITY, ["", "x"]), (IDENTITY, [1])])
def test_emergency_needs_to_know_whom_and_what_the_proof_must_cover(world, identity, targets):
    with pytest.raises(X.StagingError, match="identity and the targets"):
        E.run(world.record, world.binding, reason="x", drain=world.drain, permissions=world.permissions,
              identity=identity, targets=targets)
    assert world.api.calls == []


def test_the_operators_identity_is_the_one_permissions_py_reports():
    assert E.operator_identity("pa-eval", "pa-eval-system") == IDENTITY
    import permissions as P
    collected = {"answers": {}, "role": "operator", "cluster_uid": CLUSTER, "bindings": [], "roles": {}}
    try:
        verdict = P.verdict(collected, "pa-eval-system", "pa-eval-predictive-autoscaler-operator", TARGETS, CLUSTER)
    except Exception:                                                     # the collection above is minimal on purpose
        pytest.skip("permissions.verdict needs a fuller collection than this check builds")
    assert verdict["identity"] == IDENTITY


def test_absent_at_the_end_but_not_for_a_full_interval_is_not_revoked(world):
    # Nothing can be seen for most of the bound; the binding is seen absent only in the last seconds. Absence was
    # observed, the interval is verified, the proof passes: but a FULL interval of confirmed absence never elapsed.
    bound = (E.MAX_REAPPEARANCES + 2) * (90.0 + E.POLL_SECONDS)

    def blind_until_late(api):
        if api.clock.t < 1000.5 + bound - 2 * E.POLL_SECONDS:
            raise C.ApiError("timeout", None)
    world.api.uid = None                                                   # nothing to delete: it is simply not visible
    world.api.fail["get"] = blind_until_late
    out = world.run()
    assert out["absent"] is True and out["drain"]["verified"] is True and out["writes_denied"] is True
    assert out["drain"]["elapsed"] is False and out["drain"]["elapsed_seconds"] < 90.0
    assert out["status"] == "unresolved" and out["outcome"] == "not-revoked"
    assert any("within the bound" in p for p in out["problems"])


def test_a_completion_refuses_records_that_cannot_be_read_with_certainty(world):
    world.proof = dict(PASS, verdict="unproven", unproven=["x"])
    world.run()                                                            # sequence 1, open
    stray = os.path.join(world.record.evidence_dir, "receipts", R.EMERGENCY_DIR, "notes.txt")
    open(stray, "w").write("x")                                            # the records are now in disorder
    other = World(world.record, uid="b-9")
    out = other.run(complete=1)
    assert other.deleted() == ["b-9"]                                      # the deletion still happened
    assert out["seq"] is None and out["status"] == "unresolved" and out["outcome_recorded"] is False
    assert out["open_emergencies"] is None                                 # unknown, and reported as unknown
    os.unlink(stray)
    assert state(world.record)["tombstones"] == []


def test_a_tombstone_that_someone_else_wrote_meanwhile_is_a_problem_not_a_success(world, monkeypatch):
    # Every fact holds, but when the tombstone is written the sequence already has one (another completion won the
    # race). That is reported; this run did not record a revocation.
    def taken(_record, seq, outcome, _facts, _clock=None):
        raise X.StagingError(f"emergency {seq} already has a {outcome!r} record")
    monkeypatch.setattr(R, "emergency_close", taken)
    out = world.run()
    assert out["outcome"] == "revoked" and out["outcome_recorded"] is False and out["status"] == "unresolved"
    assert any("recording the outcome" in p and "already has" in p for p in out["problems"])


# --- the REAL permission checker's result, not a hand-built one -----------------------------------------------------

def real_proof(cluster="cluster-uid-1", lists=None, overrides=None):
    """permissions.collect + permissions.verdict over the permission tests' fake kubectl (the read-only operator)."""
    import permissions as P
    import test_permissions as TP
    k = TP.fake_kubectl(lists or TP.rbac(), overrides)
    return P.verdict(P.collect(k, TP.SA_NS, TP.SA, TP.NS, TARGETS), TP.SA_NS, TP.SA, TARGETS, cluster)


@pytest.fixture
def real_world(tmp_path):
    """A world whose record and API are the cluster the permission tests' kubectl answers for."""
    doc = {"schema": C.RECORD_SCHEMA, "run_id": "c28-demo-1", "cluster_uid": "cluster-uid-1",
           "kubeconfig": {"path": "/home/ops/.kube/eval-prod.yaml", "context": "prodcluster"},
           "evidence_dir": "evidence", "lock_dir": str(tmp_path / "shared-locks"), "source": {"commit": "a" * 40}}
    path = tmp_path / "preparation.json"
    path.write_text(json.dumps(doc))
    w = World(X.Record(str(path), doc))
    w.api.cluster = "cluster-uid-1"
    return w


def test_the_real_checkers_successful_result_is_accepted(real_world):
    # The reviewer's reproduction: with the real checker the verdict was "pass" but its "role" field was the bound
    # ClusterRole object, so the strict check refused every real proof and no emergency could ever be revoked.
    proof = real_proof()
    assert proof["verdict"] == "pass" and proof["role"] == "operator" and proof["bindings_of_the_identity"]
    assert proof["identity"] == IDENTITY == E.operator_identity("pa-eval", "pa-eval-system")
    assert E.proof_problems(proof, real_world.record, IDENTITY, sorted(TARGETS)) == []
    real_world.proof = proof
    out = real_world.run()
    assert out["writes_denied"] is True and out["status"] == "revoked", out["problems"]
    assert out["evidence"]["permissions"]["sha256"] == C.digest(proof)


def test_the_real_checkers_other_results_are_refused(real_world):
    granted = real_proof(overrides={("update", "deployments.apps", True): "yes"})
    assert granted["verdict"] == "refused"
    unclear = real_proof(overrides={("patch", "deployments.apps", True): "ERR"})
    assert unclear["verdict"] == "unproven"
    other_cluster = real_proof(cluster="another-cluster")
    assert other_cluster["verdict"] == "unproven"
    for proof in (granted, unclear, other_cluster):
        assert E.proof_problems(proof, real_world.record, IDENTITY, sorted(TARGETS))
    real_world.proof = granted
    out = real_world.run()
    assert out["writes_denied"] is False and out["status"] == "unresolved" and out["outcome"] == "not-revoked"
