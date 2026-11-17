"""The ordinary revocation and the cluster readers (transition_revoke.py), and the commands (transition.py), against
an in-memory API and a fake clock. Records, journal and lock are the real ones in a temporary directory. Registering
an executor needs a process group of its own, so the unit tests inject the registration and one test runs the real
thing through the command in a child process."""
import copy
import hashlib
import json
import os
import subprocess
import sys
import types

import pytest

import common_records as C
import execlib as X
import transition
import transition_drain as D
import transition_emergency as E
import transition_records as R
import transition_revoke as V
import test_permissions as TP
from test_transition_drain import ATTESTED, nodes, pod
from test_transition_emergency import CLUSTER, IDENTITY, NAME, NS, PATH, RELEASE, TARGETS, Api, Clock, owner

B1, B2 = "aaaaaaaa-0000-4000-8000-000000000001", "aaaaaaaa-0000-4000-8000-000000000002"
STAGING = os.path.dirname(os.path.abspath(V.__file__))
IDENT = {"release": RELEASE, "control_namespace": "pa-eval-system", "target_namespace": NS, "targets": TARGETS,
         "decision_interval_seconds": 60, "cadence_source": transition.CADENCE_SOURCE}


def node(name, control_plane=True, uid=None):
    labels = {V.CONTROL_PLANE_LABEL: "true"} if control_plane else {"node-role.kubernetes.io/worker": "true"}
    return {"metadata": {"name": name, "uid": uid or f"node-uid-{name}", "labels": labels}}


class Cluster(Api):
    """The emergency tests' API plus the nodes, the kube-system pods and the targets' /scale."""

    def __init__(self, clock, uid=B1, cluster=CLUSTER):
        super().__init__(clock, uid, cluster)
        self.nodes = [node("cp-1"), node("w-1", control_plane=False)]
        self.pods = [pod("cp-1", labels={"component": "kube-apiserver"}),
                     pod("cp-1", name="etcd-cp-1", uid="uid-etcd", labels={"component": "etcd"})]
        self.scales = {t: {"metadata": {"resourceVersion": "100"}, "spec": {"replicas": 2}} for t in TARGETS}

    def get(self, path):
        if path == "/api/v1/nodes":
            self.calls.append(("get", path))
            return self.nodes if not isinstance(self.nodes, list) else {"items": self.nodes}
        if path == "/api/v1/namespaces/kube-system/pods":
            self.calls.append(("get", path))
            return {"items": self.pods}
        if path.endswith("/scale"):
            self.calls.append(("get", path))
            if "scale" in self.fail:
                self.fail["scale"](self, path)
            return copy.deepcopy(self.scales.get(path.split("/")[-2]))
        return super().get(path)


def record_doc(tmp_path, cluster=CLUSTER, reference=None):
    ref = {"schema": V.APISERVER_SCHEMA, "control_plane_nodes": nodes("cp-1"), "defaults": ATTESTED} \
        if reference is None else reference
    data = json.dumps(ref).encode()
    (tmp_path / "apiserver.json").write_bytes(data)
    return {"schema": C.RECORD_SCHEMA, "run_id": "c28-demo-1", "cluster_uid": cluster,
            "kubeconfig": {"path": "/home/ops/.kube/eval-prod.yaml", "context": "prodcluster"},
            "evidence_dir": "evidence", "lock_dir": str(tmp_path / "shared-locks"), "source": {"commit": "a" * 40},
            "transition": {"apiserver": {"path": "apiserver.json", "sha256": hashlib.sha256(data).hexdigest()}}}


def make_record(tmp_path, **kw):
    doc = record_doc(tmp_path, **kw)
    (tmp_path / "preparation.json").write_text(json.dumps(doc))
    return X.Record(str(tmp_path / "preparation.json"), doc)


def kubectl_run(cluster=CLUSTER, overrides=None, seen=None):
    """subprocess.run for permissions.py: the permission tests' read-only operator, answering for `cluster`."""
    k = TP.fake_kubectl(TP.rbac(), overrides)

    def run(argv, **kw):
        assert argv[:2] == ["kubectl", "--kubeconfig"] and argv[5] == "--request-timeout=10s"
        assert kw.get("timeout") == E.CALL_TIMEOUT_SECONDS
        if seen is not None:
            seen.append(argv[6:])
        if argv[6:9] == ["get", "namespace", "kube-system"]:
            return types.SimpleNamespace(returncode=0, stdout=json.dumps({"metadata": {"uid": cluster}}))
        rc, out = k(argv[6:])
        return types.SimpleNamespace(returncode=rc, stdout=out)
    return run


class World:
    def __init__(self, tmp_path, uid=B1, identity=True):
        self.record, self.clock = make_record(tmp_path), Clock()
        self.api = Cluster(self.clock, uid)
        self.kube = self.record.kube(api=self.api)
        if identity:
            R.identity_write(self.record, **IDENT)
        self.identity = R.identity_read(self.record)[0]
        self.drain = V.drain_reader(self.record, self.api)
        self.permission_calls = []
        proof = V.proof_reader(self.record, self.identity, kubectl_run())
        self.permissions = lambda: (self.permission_calls.append(self.clock.t), proof())[1]
        self.registered = []

    def started(self, record, operation, activation, lock, clock=None):
        assert R._holds_lock(record, lock) is None
        executor = f"{operation}-{activation}-{len(self.registered) + 1}"
        X.receipt(record, R.HELPER, "executor-started", {"id": executor, "operation": operation,
                                                         "activation": activation, "owner": owner(9)})
        self.registered.append(executor)
        return executor

    def finished(self, record, executor, lock, *, children_collected, clock=None):
        assert children_collected is True
        X.receipt(record, R.HELPER, "executor-finished", {"id": executor})

    def plan(self, activation="reh1"):
        return V.plan(self.record, self.kube, self.drain, activation=activation)

    def apply(self, plan=None, digest=None, **kw):
        plan = self.plan() if plan is None else plan
        return V.apply(self.record, self.kube, plan, X.digest(plan) if digest is None else digest, drain=self.drain,
                       permissions=kw.pop("permissions", self.permissions), mono=self.clock.mono,
                       sleep=self.clock.sleep, own_group=lambda: None, started=self.started,
                       finished=kw.pop("finished", self.finished), **kw)

    def deleted(self):
        return [c[2] for c in self.api.calls if c[0] == "delete"]

    def lock_held(self):
        return os.path.exists(X.lock_file(self.record))

    def events(self):
        return [e["event"] for e in X.receipts(self.record, R.HELPER)]


@pytest.fixture
def world(tmp_path):
    return World(tmp_path)


# --- the API-server reference and the drain reader -------------------------------------------------------------------

def test_the_drain_is_read_against_the_reviewed_control_plane(world):
    reading = world.drain()
    assert reading["verified"] is True and reading["seconds"] == 90.0
    assert reading["nodes"] == nodes("cp-1") and reading["servers"][0]["node"] == "cp-1"
    assert reading["reference"] == {"sha256": world.record.doc["transition"]["apiserver"]["sha256"]}
    assert D.same_servers(reading, world.drain())


@pytest.mark.parametrize("change,text", [
    (lambda a: a.nodes.__setitem__(0, node("cp-1", control_plane=False)), "not the reviewed ones"),   # label removed
    (lambda a: a.nodes.pop(0), "not the reviewed ones"),                                              # node gone
    (lambda a: a.nodes.append(node("cp-2")), "not the reviewed ones"),                                # one more
    (lambda a: a.nodes.__setitem__(0, node("cp-1", uid="another-uid")), "not the reviewed ones"),     # replaced
    (lambda a: a.nodes.__setitem__(0, {"metadata": {"labels": {V.CONTROL_PLANE_LABEL: ""}}}), "not the reviewed"),
    (lambda a: setattr(a, "nodes", {"kind": "Status"}), "not answered as a list"),
    (lambda a: setattr(a, "cluster", "another-cluster"), "cluster"),
])
def test_a_control_plane_that_is_not_the_reviewed_one_is_refused_not_shrunk(world, change, text):
    change(world.api)
    with pytest.raises(X.StagingError, match=text):
        world.drain()


def test_missing_and_additional_api_servers_are_unverified(world):
    world.api.pods.append(pod("w-1", labels={"component": "kube-apiserver"}))      # one outside the inventory
    extra = world.drain()
    assert extra["verified"] is False and "outside the inventory" in extra["why"]
    world.api.pods = [p for p in world.api.pods if p["metadata"]["labels"]["component"] != "kube-apiserver"]
    missing = world.drain()
    assert missing["verified"] is False and missing["seconds"] is None


@pytest.mark.parametrize("damage,text", [
    (lambda d, p: d.pop("transition"), "names no reviewed"),
    (lambda d, p: d["transition"].pop("apiserver"), "names no reviewed"),
    (lambda d, p: d["transition"]["apiserver"].update(sha256="0" * 64), "not the reviewed file"),
    (lambda d, p: d["transition"]["apiserver"].update(path="gone.json"), "cannot be read"),
    (lambda d, p: (p / "apiserver.json").write_text("{}"), "not the reviewed file"),
])
def test_a_reference_that_is_not_the_reviewed_file_gives_no_drain(tmp_path, damage, text):
    doc = record_doc(tmp_path)
    damage(doc, tmp_path)
    record = X.Record(str(tmp_path / "preparation.json"), doc)
    with pytest.raises(X.StagingError, match=text):
        V.drain_reader(record, Cluster(Clock()))()


@pytest.mark.parametrize("reference", [
    [], {"schema": "x", "control_plane_nodes": nodes("cp-1"), "defaults": {}},
    {"schema": V.APISERVER_SCHEMA, "control_plane_nodes": [], "defaults": {}},
    {"schema": V.APISERVER_SCHEMA, "control_plane_nodes": [{"name": "cp-1"}], "defaults": {}},
    {"schema": V.APISERVER_SCHEMA, "control_plane_nodes": nodes("cp-1", "cp-1"), "defaults": {}},
    {"schema": V.APISERVER_SCHEMA, "control_plane_nodes": [{"name": "a", "uid": "u"}, {"name": "b", "uid": "u"}],
     "defaults": {}},
    {"schema": V.APISERVER_SCHEMA, "control_plane_nodes": nodes("cp-1")},
    {"schema": V.APISERVER_SCHEMA, "control_plane_nodes": nodes("cp-1"), "defaults": []},
])
def test_a_reference_without_an_inventory_or_attestations_is_refused(tmp_path, reference):
    record = make_record(tmp_path, reference=reference)
    with pytest.raises(X.StagingError, match="does not hold a control-plane inventory"):
        V.apiserver_reference(record)


def test_without_an_attested_default_only_an_explicit_setting_verifies(tmp_path):
    record = make_record(tmp_path, reference={"schema": V.APISERVER_SCHEMA, "control_plane_nodes": nodes("cp-1"),
                                              "defaults": {}})
    api = Cluster(Clock())
    assert V.drain_reader(record, api)()["verified"] is False
    api.pods[0] = pod("cp-1", command=["kube-apiserver", "--request-timeout=45s"], labels={"component": "kube-apiserver"})
    reading = V.drain_reader(record, api)()
    assert reading["verified"] is True and reading["seconds"] == 75.0


# --- the permission proof reader -------------------------------------------------------------------------------------

def test_the_proof_is_stored_before_it_is_returned(world):
    proof = world.permissions()
    assert proof["verdict"] == "pass" and proof["identity"] == IDENTITY and proof["role"] == "operator"
    assert E.proof_problems(proof, world.record, IDENTITY, sorted(TARGETS)) == []
    stored = os.path.join(world.record.evidence_dir, R.PROOF_DIR, C.digest(proof) + ".json")
    assert json.load(open(stored)) == proof


def test_a_proof_that_cannot_be_stored_is_not_returned(world, monkeypatch):
    def full(_record, _proof):
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(R, "store_proof", full)
    with pytest.raises(OSError):
        world.permissions()
    problems = []
    assert E._attempt(problems, "the permission proof", world.permissions) is None and problems


@pytest.mark.parametrize("missing", ["release", "control_namespace", "target_namespace", "targets"])
def test_no_proof_is_asked_for_an_incomplete_identity(world, missing):
    seen = []
    reader = V.proof_reader(world.record, dict(world.identity, **{missing: None}), kubectl_run(seen=seen))
    with pytest.raises(X.StagingError, match="identity is incomplete"):
        reader()
    assert seen == []


# --- the /scale observation ------------------------------------------------------------------------------------------

def test_the_scale_observation_reports_unchanged_changed_and_unknown(world):
    def write_between(api, path):
        if api.clock.t > 1000.0 and path.endswith(f"{TARGETS[0]}/scale"):
            api.scales[TARGETS[0]] = {"metadata": {"resourceVersion": "101"}, "spec": {"replicas": 3}}
        if path.endswith(f"{TARGETS[1]}/scale") and api.clock.t > 1000.0:
            raise C.ApiError("timeout", None)
    out = V.scale_observation(world.api, NS, TARGETS, 60, world.clock.sleep)
    assert out["targets"] == {t: "unchanged" for t in TARGETS} and world.clock.sleeps == [60]
    assert out["scale_writes_total"] == "uncorroborated: not read" and "not a proof" in out["note"]
    world.api.fail["scale"] = write_between
    world.clock.t = 1000.0
    out = V.scale_observation(world.api, NS, TARGETS, 60, world.clock.sleep)
    assert out["targets"] == {TARGETS[0]: "changed", TARGETS[1]: "unknown"}
    del world.api.scales[TARGETS[0]]                                       # the target is gone: unknown, not unchanged
    world.api.fail.clear()
    assert V.scale_observation(world.api, NS, TARGETS, 1, world.clock.sleep)["targets"][TARGETS[0]] == "unknown"


# --- the plan --------------------------------------------------------------------------------------------------------

def test_the_plan_names_the_binding_the_interval_and_what_it_leaves_pending(world):
    plan = world.plan()
    assert plan["actions"] == [{"op": "delete", "kind": "RoleBinding", "namespace": NS, "name": NAME, "uid": B1}]
    assert plan["operation"] == "revoke" and plan["activation"] == "reh1"
    assert plan["scale_gap_seconds"] == 120                                 # two decision intervals of 60 s, derived
    assert plan["scale_gap_source"] == "2 x params.demonstration.cadence.update_interval_seconds"
    assert plan["identity"]["operator_identity"] == IDENTITY and plan["identity"]["targets"] == sorted(TARGETS)
    assert plan["drain"]["seconds"] == 90.0 and "observed_at" not in plan["drain"]
    assert plan["problems"] == [] and plan["chart_pending"] == [] and plan["emergency_last"] == 0
    assert plan["leaves_pending"] == V.LEFT_PENDING
    assert X.digest(plan) == X.digest(world.plan())                         # the same state plans the same
    assert world.deleted() == [] and not world.lock_held()


def test_an_absent_binding_plans_no_deletion(tmp_path):
    world = World(tmp_path, uid=None)
    plan = world.plan()
    assert plan["actions"] == [] and plan["binding"]["state"] == "absent" and plan["binding"]["uid"] is None


def test_the_plan_lists_what_must_be_resolved_first(world):
    R.emergency_request(world.record, "earlier")
    X.receipt(world.record, R.HELPER, "executor-started", {"id": "grant-reh1-1", "operation": "grant",
                                                           "activation": "reh1", "owner": owner(1)})
    problems = world.plan()["problems"]
    assert len(problems) == 2 and "emergencies [1] are open" in problems[0] and "grant-reh1-1" in problems[1]
    with pytest.raises(X.StagingError, match="cannot start"):
        world.apply()
    assert world.deleted() == [] and world.registered == []


@pytest.mark.parametrize("break_it,text", [
    (lambda w: os.unlink(os.path.join(w.record.evidence_dir, R.IDENTITY_FILE)), "complete transition identity"),
    (lambda w: w.api.pods.clear(), "drain interval is not verified"),
    (lambda w: w.api.nodes.pop(0), "not the reviewed ones"),
    (lambda w: w.api.fail.__setitem__("get", lambda api: (_ for _ in ()).throw(C.ApiError("timeout", None))), "timeout"),
    (lambda w: (os.makedirs(R._dir(w.record)), open(os.path.join(R._dir(w.record), "x"), "w").close()),
     "emergency records cannot be read"),
    (lambda w: (os.makedirs(R._chart_dir(w.record)), open(os.path.join(R._chart_dir(w.record), "x"), "w").close()),
     "chart records cannot be read"),
])
def test_what_an_ordinary_revocation_cannot_work_with_is_refused_at_planning(world, break_it, text):
    break_it(world)
    with pytest.raises(X.StagingError, match=text):
        world.plan()


@pytest.mark.parametrize("activation", ["", "Reh 1", None, "a" * 40, 7])
def test_a_plan_needs_an_activation(world, activation):
    with pytest.raises(X.StagingError, match="activation id"):
        world.plan(activation)


def test_the_gap_between_the_scale_reads_is_two_decision_intervals_of_the_identity(tmp_path):
    world = World(tmp_path, identity=False)
    R.identity_write(world.record, **dict(IDENT, decision_interval_seconds=45, cadence_source="somewhere"))
    plan = world.plan()
    assert plan["scale_gap_seconds"] == 90 and plan["scale_gap_source"] == "2 x somewhere"


# --- apply -----------------------------------------------------------------------------------------------------------

def test_a_revocation_that_holds_is_recorded_and_leaves_the_chart_pending(world):
    def chart_first(api, uid):                             # at the first deletion the obligation is already durable
        assert R.chart_state(world.record)["pending"] == [1] and world.registered == ["revoke-reh1-1"]
    world.api.fail["delete"] = chart_first
    out = world.apply()
    assert out["status"] == "permission-revoked" and out["recorded"] is True and out["problems"] == []
    f = out["evidence"]
    assert f["outcome"] == "revoked" and f["binding_removed"] is True and f["writes_denied"] is True
    assert f["deletions"] == [B1] and f["planned_uid"] == B1 and f["drift"] == [] and f["executor"] == "revoke-reh1-1"
    assert f["drain"]["required_seconds"] == 90.0 and f["drain"]["elapsed_seconds"] >= 90.0 and f["drain"]["as_planned"]
    assert f["permissions"]["sha256"] and f["permissions"]["verdict"] == "pass" and f["permissions"]["problems"] == []
    assert f["scale_observation"]["targets"] == {t: "unchanged" for t in TARGETS}
    assert f["chart"] == {"state": "restore-pending", "seq": 1} and f["not_done"] == V.LEFT_PENDING
    assert world.api.uid is None and world.clock.t - 1000.0 >= 90.0 + 120
    late = f["after_the_interval"]
    assert late["stayed_absent"] is True and len(late["observations"]) >= 120 / E.POLL_SECONDS
    assert {s for _at, s in late["observations"]} == {"absent"} and out["lock_kept"] is False
    assert world.permission_calls and min(world.permission_calls) >= 1000.0 + 90.0 + 120   # the proof came LAST
    # the journal, the registration and the result are in the transition's receipts, in this order
    assert world.events() == ["executor-started", "intent-1", "step-1", "revocation", "executor-finished"]
    last = [e for e in X.receipts(world.record, R.HELPER) if e["event"] == "revocation"][0]
    assert last["data"] == json.loads(json.dumps(f))
    assert R.outstanding(world.record) == ([], None) and not world.lock_held()
    # the chart obligation outlives the executor and blocks a grant and an activation
    assert R.chart_state(world.record)["pending"] == [1]
    with pytest.raises(X.StagingError, match="chart restoration"):
        R.refuse_if_chart_pending(world.record)


def test_an_absent_binding_still_needs_the_interval_and_the_proof(tmp_path):
    world = World(tmp_path, uid=None)
    out = world.apply()
    assert out["status"] == "permission-revoked" and world.deleted() == [] and out["evidence"]["deletions"] == []
    assert out["evidence"]["drain"]["elapsed_seconds"] >= 90.0 and "intent-1" not in world.events()


def test_only_the_reviewed_plan_of_the_live_state_is_applied(world):
    plan = world.plan()
    with pytest.raises(X.StagingError):
        world.apply(plan, "0" * 64)                                        # not the reviewed digest
    world.api.uid = B2                                                     # the binding was replaced since the review
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        world.apply(plan)
    world.api.uid = B1
    world.api.pods[0] = pod("cp-1", restarts=1, labels={"component": "kube-apiserver"})   # an API server restarted
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        world.apply(plan)
    with pytest.raises(X.StagingError, match="not a revocation plan"):
        world.apply(dict(plan, operation="grant"))
    assert world.deleted() == [] and world.registered == [] and R.chart_state(world.record)["pending"] == []


def test_an_executor_that_appears_after_the_review_stops_the_revocation_under_the_lock(world, monkeypatch):
    plan = world.plan()
    real, calls = R.outstanding, {"n": 0}

    def later(record):
        calls["n"] += 1
        if calls["n"] == 1:                                # the look under the lock (the plan is older)
            return [{"id": "grant-reh1-7", "owner": owner(7)}], None
        return real(record)
    monkeypatch.setattr(R, "outstanding", later)
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        world.apply(plan)
    assert world.deleted() == [] and world.registered == []


def test_a_binding_recreated_during_the_revocation_is_drift_and_restarts_the_interval(world):
    world.api.reappear = [(1050.0, B2)]
    out = world.apply()
    f = out["evidence"]
    assert out["status"] == "permission-revoked" and f["deletions"] == [B1, B2] and f["drift"] == [B2]
    assert f["drain"]["reappearances"] == 1 and world.clock.t - 1000.0 >= 50.0 + 90.0 + 120
    assert world.events().count("intent-2") == 1 and world.events().count("step-2") == 1    # journalled too


@pytest.mark.parametrize("overrides,text", [
    ({("update", "deployments.apps", True): "yes"}, "the verdict is 'refused'"),
    ({("patch", "deployments.apps", True): "ERR"}, "the verdict is 'unproven'"),
])
def test_without_the_proof_nothing_is_claimed_and_the_chart_stays_pending(world, overrides, text):
    out = world.apply(permissions=V.proof_reader(world.record, world.identity, kubectl_run(overrides=overrides)))
    assert out["status"] == "unresolved" and out["evidence"]["outcome"] == "not-revoked"
    assert out["evidence"]["binding_removed"] is True and out["evidence"]["writes_denied"] is False
    assert any(text in p for p in out["problems"])
    assert R.chart_state(world.record)["pending"] == [1] and R.outstanding(world.record) == ([], None)
    assert world.lock_held() and out["lock_kept"] is True                  # nothing was proven: nobody else may run
    assert R.unresolved_revocations(world.record) == (["revoke-reh1-1"], None)


def test_a_deletion_with_an_unknown_outcome_keeps_the_lock_and_deletes_nothing_more(world):
    def lost(api, uid):
        api.uid = None                                     # it was applied, but nobody was told
        raise subprocess.TimeoutExpired("kubectl", 20)
    world.api.fail["delete"] = lost
    world.api.reappear = [(1030.0, B2)]                    # and the binding comes back
    out = world.apply()
    assert out["status"] == "unresolved" and world.deleted() == [B1]       # B2 is NOT deleted through a broken journal
    assert any("outcome is unknown" in p for p in out["problems"]) and world.api.uid == B2
    assert world.lock_held()                               # an unresolved intent: the recovery's to decide
    assert "intent-1" in world.events() and "step-1" not in world.events()


def test_a_definitive_refusal_is_not_an_unknown_outcome(world):
    seen = {"n": 0}

    def conflict_once(api, uid):
        seen["n"] += 1
        if seen["n"] == 1:
            raise C.ApiError("conflict", 409)
    world.api.fail["delete"] = conflict_once
    out = world.apply()
    assert out["status"] == "permission-revoked" and world.deleted() == [B1, B1] and not world.lock_held()


def test_a_scale_that_changed_is_recorded_and_blocks_nothing(world):
    def writes(api, path):
        api.scales[TARGETS[0]]["metadata"]["resourceVersion"] = str(int(api.clock.t))
    world.api.fail["scale"] = writes
    out = world.apply()
    assert out["status"] == "permission-revoked"
    assert out["evidence"]["scale_observation"]["targets"][TARGETS[0]] == "changed"


def test_an_obligation_that_cannot_be_recorded_stops_the_revocation_before_any_deletion(world, monkeypatch):
    def broken(_record, _reason, _clock=None):
        raise X.StagingError("the chart records cannot be read with certainty (x)")
    monkeypatch.setattr(R, "chart_pending", broken)
    with pytest.raises(X.StagingError, match="chart records"):
        world.apply()
    assert world.deleted() == [] and world.api.uid == B1
    assert R.outstanding(world.record) == ([], None) and not world.lock_held()    # registered and finished again
    assert world.events() == ["executor-started", "revocation", "executor-finished"]
    assert R.unresolved_revocations(world.record) == ([], None)                   # "not-started" holds nothing


def test_a_result_that_cannot_be_recorded_is_not_a_revocation(world, monkeypatch):
    real = X.receipt

    def failing(record, helper, event, data, clock=None):
        if event == "revocation":
            raise OSError(28, "No space left on device")
        return real(record, helper, event, data, clock)
    monkeypatch.setattr(X, "receipt", failing)
    out = world.apply()
    assert out["status"] == "unresolved" and out["recorded"] is False and out["evidence"]["outcome"] == "revoked"
    assert world.lock_held() and R.unresolved_revocations(world.record) == (["revoke-reh1-1"], None)


def test_the_journalled_binding_refuses_without_the_lock(world):
    binding = V.JournalledBinding(world.record, world.kube, RELEASE, NS)
    assert binding.observe() == ("present", B1)
    with pytest.raises(X.StagingError, match="needs the shared lock"):
        binding.delete(B1)
    assert world.deleted() == [] and binding.unknown_outcome is True


# --- the commands ----------------------------------------------------------------------------------------------------

def cli(world, argv, capsys, run=None):
    rc = transition.main(["--record", world.record.path, *argv], api_factory=lambda _r: world.api,
                         kubectl_run=run or kubectl_run())
    lines = capsys.readouterr().out.splitlines()
    return rc, lines[0], (json.loads("\n".join(lines[1:])) if len(lines) > 1 else None)


@pytest.fixture
def fast(world, monkeypatch):
    """The commands use the real clock by default: give the emergency and the revocation the fake one."""
    for fn in (E.run, V.apply):
        monkeypatch.setitem(fn.__kwdefaults__, "mono", world.clock.mono)
        monkeypatch.setitem(fn.__kwdefaults__, "sleep", world.clock.sleep)
    return world


def demo_world(tmp_path, stage="demonstration", change=None):
    """A record whose params are the demonstration stage's (the prepare tests' fixture), loaded by the REAL loader."""
    import test_prepare_demo as TPD
    params = TPD.demo_params()
    (change or (lambda p: None))(params)
    (tmp_path / "params.json").write_text(json.dumps(params))
    doc = dict(record_doc(tmp_path), params="params.json", prepare={"stage": stage})
    (tmp_path / "preparation.json").write_text(json.dumps(doc))
    world = World.__new__(World)
    world.record, world.clock = X.Record(str(tmp_path / "preparation.json"), doc), Clock()
    world.api = Cluster(world.clock)
    return world, params


def test_the_identity_command_writes_it_from_the_demonstration_params(tmp_path, capsys):
    # The reviewer's reproduction: the full experiment validation refused the real demonstration packet
    # ("missing parameters: c0_rps, ..."). The demonstration's params are loaded the way that stage loads them.
    world, params = demo_world(tmp_path)
    assert "c0_rps" not in params                                           # these params are NOT experiment-complete
    rc, line, doc = cli(world, ["identity", "--targets", "c28-demo-pa-r,c28-demo-pa-n"], capsys)
    assert rc == 0 and line == "IDENTITY written; open emergencies: none; chart restoration pending: none"
    assert doc["release"] == params["release"] and doc["control_namespace"] == params["control_namespace"]
    assert doc["target_namespace"] == params["namespace"] and doc["targets"] == ["c28-demo-pa-n", "c28-demo-pa-r"]
    assert doc["decision_interval_seconds"] == 60 and doc["cadence_source"] == transition.CADENCE_SOURCE
    assert R.identity_read(world.record)[1] == []


@pytest.mark.parametrize("targets", ["c28-demo-pa-n", "c28-demo-pa-n,other", "c28-demo-pa-n,c28-demo-pa-r,extra",
                                     "c28-demo-pa-n,c28-demo-pa-n", "c28-demo-pa-n,c28-demo-pa-r,c28-demo-pa-r", ","])
def test_the_identity_is_fixed_only_for_the_stages_designated_targets(tmp_path, capsys, targets):
    world, _ = demo_world(tmp_path)
    rc, line, _ = cli(world, ["identity", "--targets", targets], capsys)
    assert rc == 1 and "are not the stage's designated targets" in line
    assert not os.path.exists(os.path.join(world.record.evidence_dir, R.IDENTITY_FILE))


@pytest.mark.parametrize("stage", ["experiment", "other", None])
def test_the_identity_exists_for_the_demonstration_stage_only(tmp_path, capsys, stage):
    world, _ = demo_world(tmp_path, stage=stage)
    rc, line, _ = cli(world, ["identity", "--targets", ",".join(TARGETS)], capsys)
    assert rc == 1 and "demonstration stage only" in line


def test_a_second_different_identity_is_refused_by_the_command(tmp_path, capsys):
    world, params = demo_world(tmp_path)
    assert cli(world, ["identity", "--targets", ",".join(TARGETS)], capsys)[0] == 0
    params["release"] = "another"
    (tmp_path / "params.json").write_text(json.dumps(params))
    rc, line, _ = cli(world, ["identity", "--targets", ",".join(TARGETS)], capsys)
    assert rc == 1 and line.startswith("IDENTITY REFUSED: this run already has a DIFFERENT transition identity")


def test_the_emergency_command_revokes_and_says_what_is_still_pending(fast, capsys):
    rc, line, out = cli(fast, ["emergency", "--reason", "test"], capsys)
    assert rc == 0 and out["status"] == "revoked" and fast.api.uid is None
    assert line == ("EMERGENCY 1: PERMISSION REVOKED (chart restoration pending; PredictiveAutoscalers untouched); "
                    "open emergencies: none; chart restoration pending: [1]")
    assert out["chart"] == {"seq": 1, "problem": None} and out["identity_problems"] == [] and out["not_done"]
    stored = os.path.join(fast.record.evidence_dir, R.PROOF_DIR, out["evidence"]["permissions"]["sha256"] + ".json")
    assert json.load(open(stored))["verdict"] == "pass"


def test_an_unresolved_emergency_exits_2_and_stays_open_until_completed(fast, capsys):
    rc, line, out = cli(fast, ["emergency", "--reason", "test"], capsys,
                        run=kubectl_run(overrides={("patch", "deployments.apps", True): "ERR"}))
    assert rc == 2 and fast.api.uid is None and out["outcome"] == "not-revoked"
    assert line == "EMERGENCY 1: UNRESOLVED (not-revoked); open emergencies: [1]; chart restoration pending: [1]"
    rc, line, out = cli(fast, ["emergency", "--reason", "again", "--complete", "1"], capsys)
    assert rc == 0 and out["seq"] == 1 and out["completing"] == 1
    assert line.endswith("open emergencies: none; chart restoration pending: [1]")


def test_an_emergency_without_an_identity_fences_and_says_it_could_not_name_the_binding(tmp_path, capsys, monkeypatch):
    world = World(tmp_path, identity=False)
    for key in ("mono", "sleep"):
        monkeypatch.setitem(E.run.__kwdefaults__, key, getattr(world.clock, key))
    fenced = []
    monkeypatch.setitem(E.run.__kwdefaults__, "fence", lambda o: fenced.append(o["pid"]) or {"proven": True})
    X.receipt(world.record, R.HELPER, "executor-started", {"id": "grant-reh1-1", "operation": "grant",
                                                           "activation": "reh1", "owner": owner(1)})
    rc, line, out = cli(world, ["emergency", "--reason", "no identity"], capsys)
    assert rc == 2 and fenced == [owner(1)["pid"]] and world.deleted() == [] and out["binding"] is None
    assert out["identity_problems"] and any("cannot be named" in p for p in out["problems"])
    assert line.startswith("EMERGENCY 1: UNRESOLVED (not-revoked); open emergencies: [1]")


def test_a_state_that_cannot_be_read_is_printed_as_unknown_never_as_none(fast, capsys):
    os.makedirs(R._chart_dir(fast.record))
    open(os.path.join(R._chart_dir(fast.record), "stray"), "w").close()
    os.makedirs(R._dir(fast.record))
    open(os.path.join(R._dir(fast.record), "stray"), "w").close()
    rc, line, out = cli(fast, ["emergency", "--reason", "test"], capsys)
    # The deletion, the interval and the proof all happened, and this sequence has its tombstone. But the chart
    # obligation could not be put on record, so the command does not exit 0, and nothing unknown reads as "none".
    assert fast.api.uid is None and out["status"] == "revoked" and out["open_emergencies"] is None
    assert out["chart"]["seq"] is None and out["chart"]["problem"] and rc == 2
    assert line == ("EMERGENCY 1: PERMISSION REVOKED, BUT THE CHART OBLIGATION COULD NOT BE RECORDED (the chart "
                    "still grants); open emergencies: UNKNOWN; chart restoration pending: UNKNOWN")


def test_the_revoke_plan_command_saves_the_plan_and_refusals_exit_1(world, capsys):
    rc, line, saved = cli(world, ["revoke", "plan", "--activation", "reh1"], capsys)
    assert rc == 0 and line.startswith("REVOKE planned; open emergencies: none") and saved["problems"] == []
    assert X.digest(X.load_plan(saved["plan"])) == saved["plan_digest"]
    rc, line, _ = cli(world, ["revoke", "apply", "--plan", saved["plan"], "--digest", "0" * 64], capsys)
    assert rc == 1 and line.startswith("REVOKE REFUSED: ") and world.deleted() == []
    R.emergency_request(world.record, "earlier")
    rc, line, saved = cli(world, ["revoke", "plan", "--activation", "reh1"], capsys)
    assert rc == 0 and line.startswith("REVOKE planned WITH PROBLEMS (apply will refuse); open emergencies: [1]")


CHILD = """
import json, os, sys
sys.path[:0] = [{staging!r}, {tests!r}]
import execlib as X, transition, transition_emergency as E, transition_revoke as V
import test_transition_revoke as T
from test_transition_emergency import Clock
record = X.load_record(sys.argv[1])
clock = Clock()
api = T.Cluster(clock)
V.apply.__kwdefaults__.update(mono=clock.mono, sleep=clock.sleep)
overrides = {{("patch", "deployments.apps", True): "ERR"}} if sys.argv[4] == "unproven" else None
def lost(a, uid):
    a.uid = None                                   # applied, and the answer never arrives
    if sys.argv[4] == "die":
        os._exit(9)                                # the executor dies right there
    raise subprocess.TimeoutExpired("kubectl", 20)
if sys.argv[4] in ("lost", "die"):
    import subprocess
    api.fail["delete"] = lost
rc = transition.main(["--record", sys.argv[1], "revoke", "apply", "--plan", sys.argv[2], "--digest", sys.argv[3]],
                     api_factory=lambda _r: api, kubectl_run=T.kubectl_run(overrides=overrides))
print(json.dumps({{"rc": rc, "pid": os.getpid(), "pgid": os.getpgid(0), "uid": api.uid}}))
"""


def test_the_revoke_command_end_to_end_with_a_real_registration(world, capsys):
    # The real R.started / R.finished and transition_fence.own_group need a process of their own.
    rc, _line, saved = cli(world, ["revoke", "plan", "--activation", "reh1"], capsys)
    assert rc == 0
    script = CHILD.format(staging=STAGING, tests=os.path.join(STAGING, "tests"))
    p = subprocess.run([sys.executable, "-c", script, world.record.path, saved["plan"], saved["plan_digest"], "pass"],
                       capture_output=True, text=True, timeout=120)
    lines = p.stdout.strip().splitlines()
    assert lines, p.stderr[-3000:]
    assert lines[0] == ("REVOKE: PERMISSION REVOKED, chart restoration pending (not a completed rollback); "
                        "open emergencies: none; chart restoration pending: [1]"), p.stdout[-2000:]
    told = json.loads(lines[-1])
    assert told["rc"] == 0 and told["uid"] is None and told["pgid"] == told["pid"]      # its own process group
    assert world.events() == ["executor-started", "intent-1", "step-1", "revocation", "executor-finished"]
    started = X.receipts(world.record, R.HELPER)[0]["data"]
    assert started["id"] == "revoke-reh1-1" and started["owner"]["pid"] == told["pid"]
    assert R.outstanding(world.record) == ([], None) and not world.lock_held()


def test_the_revoke_command_exits_2_when_the_proof_does_not_hold(world, capsys):
    rc, _line, saved = cli(world, ["revoke", "plan", "--activation", "reh1"], capsys)
    script = CHILD.format(staging=STAGING, tests=os.path.join(STAGING, "tests"))
    p = subprocess.run([sys.executable, "-c", script, world.record.path, saved["plan"], saved["plan_digest"],
                        "unproven"], capture_output=True, text=True, timeout=120)
    lines = p.stdout.strip().splitlines()
    assert lines, p.stderr[-3000:]
    assert lines[0] == ("REVOKE: UNRESOLVED, the shared lock is KEPT (next: emergency); open emergencies: none; "
                        "chart restoration pending: [1]"), p.stdout[-2000:]
    told = json.loads(lines[-1])
    assert told["rc"] == 2 and told["uid"] is None                         # the binding is gone, the claim is not made
    assert R.outstanding(world.record) == ([], None) and world.lock_held()


@pytest.mark.parametrize("completed", [False, True])
def test_an_emergency_after_the_review_stops_the_revocation_under_the_lock(world, monkeypatch, completed):
    # The reviewer's reproduction: an emergency requested AND completed after the plan was compared; the plan
    # reviewed at emergency sequence 0 still ran at sequence 1. The plan is now computed again UNDER the lock.
    plan = world.plan()
    real = C.exclusive_lock.__enter__

    def enter_after_an_emergency(lock):
        seq = R.emergency_request(world.record, "while the revocation was waiting for the lock")
        if completed:
            R.emergency_close(world.record, seq, "revoked", {"binding_removed": True, "fence_proven": True,
                                                             "writes_denied": True})
        return real(lock)
    monkeypatch.setattr(C.exclusive_lock, "__enter__", enter_after_an_emergency)
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        world.apply(plan)
    assert world.deleted() == [] and world.registered == [] and R.chart_state(world.record)["pending"] == []
    assert not world.lock_held()                                           # refused before anything: nothing to hold


def test_a_binding_that_comes_back_during_the_observation_is_deleted_and_nothing_is_claimed(world):
    # The reviewer's reproduction: the proof was asked before the /scale wait and the binding never looked at again;
    # recreated during that wait, the result was "permission-revoked" with the replacement binding in place.
    world.api.reappear = [(1000.0 + 90.0 + 30.0, B2)]                      # after the interval, inside the wait
    out = world.apply()
    f = out["evidence"]
    assert f["drain"]["elapsed"] is True and f["drain"]["reappearances"] == 0   # the interval itself was clean
    assert f["after_the_interval"]["stayed_absent"] is False
    assert "present" in {s for _at, s in f["after_the_interval"]["observations"]}
    assert world.deleted() == [B1, B2] and world.api.uid is None            # it granted: it was removed again
    assert f["deletions"] == [B1, B2] and f["drift"] == [B2] and f["binding_removed"] is False
    assert out["status"] == "unresolved" and f["outcome"] == "not-revoked" and world.lock_held()


def test_a_look_that_fails_during_the_observation_is_not_absence(world):
    def blind_once(api):
        if 1150.0 <= api.clock.t < 1155.0:
            raise C.ApiError("timeout", None)
    world.api.fail["get"] = blind_once
    out = world.apply()
    assert out["evidence"]["after_the_interval"]["stayed_absent"] is False and out["status"] == "unresolved"


def test_a_deletion_that_is_always_refused_leaves_the_binding_and_keeps_the_lock(world):
    # The reviewer's reproduction: DELETE answers 403 every time; the result was unresolved with the binding present
    # and the shared lock RELEASED.
    def forbidden(api, uid):
        raise C.ApiError("forbidden", 403)
    world.api.fail["delete"] = forbidden
    out = world.apply()
    assert out["status"] == "unresolved" and world.api.uid == B1 and out["evidence"]["binding_removed"] is False
    assert world.lock_held() and out["lock_kept"] is True and R.outstanding(world.record) == ([], None)
    with pytest.raises(X.StagingError):
        world.apply()                                                      # nobody else gets the lock, not even revoke


def test_an_executor_that_fails_midway_keeps_the_lock_and_stays_outstanding(world, monkeypatch):
    def boom(*_a, **_k):
        raise RuntimeError("unexpected")
    monkeypatch.setattr(V, "scale_observation", boom)
    with pytest.raises(RuntimeError):
        world.apply()
    assert world.lock_held() and [e["id"] for e in R.outstanding(world.record)[0]] == ["revoke-reh1-1"]


def test_an_applied_deletion_with_a_lost_answer_is_never_a_revocation(world):
    # The binding really is gone and stays gone, the interval elapses and the proof passes: but the journal does not
    # know what its deletion did. That is the recovery's to decide, not a result.
    def lost(api, uid):
        api.uid = None
        raise subprocess.TimeoutExpired("kubectl", 20)
    world.api.fail["delete"] = lost
    out = world.apply()
    f = out["evidence"]
    assert f["binding_removed"] is True and f["writes_denied"] is True and f["drain"]["elapsed"] is True
    assert out["status"] == "unresolved" and f["outcome"] == "not-revoked" and world.lock_held()


def test_absence_seen_only_at_the_end_is_not_a_full_interval(world):
    bound = (E.MAX_REAPPEARANCES + 2) * (90.0 + E.POLL_SECONDS)

    def blind_in_between(api):
        if 1000.0 < api.clock.t < 1000.0 + bound - 2 * E.POLL_SECONDS:
            raise C.ApiError("timeout", None)
    world.api.fail["get"] = blind_in_between
    out = world.apply()
    f = out["evidence"]
    assert f["binding_removed"] is True and f["writes_denied"] is True and f["drain"]["verified"] is True
    assert f["drain"]["elapsed"] is False and f["drain"]["elapsed_seconds"] < 90.0
    assert out["status"] == "unresolved" and f["outcome"] == "not-revoked"


def restarted():
    return pod("cp-1", restarts=1, labels={"component": "kube-apiserver"})


def test_an_api_server_that_restarts_during_the_interval_voids_the_claim(world):
    def restart_midway(api):
        if api.clock.t > 1040.0:
            api.pods[0] = restarted()
    world.api.fail["get"] = restart_midway
    out = world.apply()
    d = out["evidence"]["drain"]
    assert d["as_planned"] is True and d["unchanged"] is False and d["elapsed"] is True
    assert out["status"] == "unresolved" and out["evidence"]["outcome"] == "not-revoked"


def test_api_servers_other_than_the_planned_ones_void_the_claim(world):
    def restart_at_the_deletion(api, uid):                  # after the plan was compared, before the interval is read
        api.pods[0] = restarted()
    world.api.fail["delete"] = restart_at_the_deletion
    out = world.apply()
    d = out["evidence"]["drain"]
    assert d["as_planned"] is False and d["unchanged"] is True and d["elapsed"] is True
    assert out["status"] == "unresolved" and any("plan was reviewed with" in p for p in out["problems"])


@pytest.mark.parametrize("mode,why", [("lost", "did not end in a proven, recorded revocation"),
                                      ("die", "neither finished nor fenced")])
def test_a_generic_recovery_reconciles_the_journal_but_does_not_release_the_transitions_lock(world, capsys, mode, why):
    # A revocation in a process of its own leaves the mixed log behind (owner events and journal steps in one file)
    # and the lock, and is gone. The generic recovery decides the DELETE intent, and that alone must not free the
    # lock: "lost" leaves a revocation that proved nothing, "die" an executor that is neither finished nor fenced.
    rc, _line, saved = cli(world, ["revoke", "plan", "--activation", "reh1"], capsys)
    script = CHILD.format(staging=STAGING, tests=os.path.join(STAGING, "tests"))
    p = subprocess.run([sys.executable, "-c", script, world.record.path, saved["plan"], saved["plan_digest"], mode],
                       capture_output=True, text=True, timeout=120)
    assert (p.returncode == 9) is (mode == "die"), p.stderr[-2000:]
    assert world.lock_held() and "intent-1" in world.events() and "step-1" not in world.events()
    world.api.uid = None                                                   # what the child's deletion left behind
    plan = X.recover_plan(world.record, world.kube)
    assert [(a["helper"], a["op"], a["kind"], a["observed"]) for a in plan["actions"]] == [
        ("transition", "delete", "RoleBinding", "done")] and plan["unresolved"] == []
    with pytest.raises(X.StagingError, match=why):
        X.recover(world.record, world.kube, plan, X.digest(plan))
    assert world.lock_held()                                               # the journal is decided; the lock is not freed
    assert "reconciled-1" in world.events()
    assert X.receipts(world.record, "recover")[-1]["event"] == "recovery-unresolved"
    assert (R.outstanding(world.record)[0] != []) is (mode == "die")


# --- code review 2: the lock from the registration on ------------------------------------------------------------------

def test_a_registration_that_half_happened_keeps_the_lock(world):
    # The reviewer's reproduction: the owner record is appended, then the registration raises (durability).
    def half(record, operation, activation, lock, clock=None):
        world.started(record, operation, activation, lock, clock)
        raise OSError(5, "Input/output error")
    with pytest.raises(OSError):
        V.apply(world.record, world.kube, world.plan(), X.digest(world.plan()), drain=world.drain,
                permissions=world.permissions, mono=world.clock.mono, sleep=world.clock.sleep,
                own_group=lambda: None, started=half, finished=world.finished)
    assert world.lock_held() and world.deleted() == []
    assert [e["id"] for e in R.outstanding(world.record)[0]] == ["revoke-reh1-1"]


@pytest.mark.parametrize("failing", ["the not-started record", "finished"])
def test_a_never_started_revocation_releases_the_lock_only_after_its_whole_cleanup(world, monkeypatch, failing):
    # The reviewer's reproductions: the chart obligation cannot be recorded, and then the cleanup fails too.
    def broken(_record, _reason, _clock=None):
        raise X.StagingError("the chart records cannot be read with certainty (x)")
    monkeypatch.setattr(R, "chart_pending", broken)
    real = X.receipt

    def receipt(record, helper, event, data, clock=None):
        if event == "revocation" and failing == "the not-started record":
            raise OSError(28, "No space left on device")
        return real(record, helper, event, data, clock)
    monkeypatch.setattr(X, "receipt", receipt)

    def finished(record, executor, lock, *, children_collected, clock=None):
        if failing == "finished":
            raise OSError(5, "Input/output error")
        return world.finished(record, executor, lock, children_collected=children_collected, clock=clock)
    with pytest.raises(OSError):
        world.apply(finished=finished)
    assert world.lock_held() and world.deleted() == []
    assert [e["id"] for e in R.outstanding(world.record)[0]] == ["revoke-reh1-1"]
