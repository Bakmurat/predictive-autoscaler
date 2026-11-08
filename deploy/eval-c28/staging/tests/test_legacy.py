"""legacy.py: an exact, UID-bound S4 plan; execution only with the reviewed digest; evidence and archived objects
untouched; verify and the ten quiet minutes; selective restore."""

import copy
import json
import os

import pytest

import common_records as C
import legacy as LG
from fakes import CLUSTER, MARKERS, legacy_cluster, record

T0 = 1_794_000_000
REAL_PROM = LG.PromClient                                                    # before the autouse fixture patches it


class Metrics:
    """A fake metrics API: per-target request rate and data age (no network)."""

    def __init__(self, rate=0.0, age=5.0, missing=()):
        self.rate, self.age, self.missing, self.queries = rate, age, set(missing), []

    def __call__(self, expr):
        self.queries.append(expr)
        target = expr.split('workload="')[1].split('"')[0]
        if target in self.missing:
            return None
        return self.age if expr.startswith("time()") else self.rate


@pytest.fixture(autouse=True)
def quiet_metrics(monkeypatch):
    m = Metrics()
    monkeypatch.setattr(LG, "PromClient", lambda base, **kw: m)          # never a network call in tests
    return m


ARRIVALS = {"query_api": "http://vmselect.monitoring.svc:8481/select/0/prometheus",
            "rate_query": 'sum(rate(istio_requests_total{destination_workload="{target}",'
                          'destination_workload_namespace="demo"}[1m]))',
            "age_query": 'time() - max(timestamp(istio_requests_total{destination_workload="{target}"}))',
            "max_age_seconds": 120}


def crash_owner(lock_file):
    """Make a lock look left behind by a dead executor: a PID that has certainly exited."""
    import subprocess
    p = subprocess.Popen(["true"])
    p.wait()
    body = json.load(open(lock_file))
    body["pid"] = p.pid
    open(lock_file, "w").write(json.dumps(body, sort_keys=True))


def ready_record(tmp_path, **legacy):
    rec = record(tmp_path, legacy={"parked_replicas": 0, "job_deadline_seconds": 600, "wait_timeout_seconds": 60,
                                   "arrivals": dict(ARRIVALS),
                                   "external_launchers": ["pf-loop on the ops box"],
                                   "external_launchers_attested": True, **legacy})
    r = C.append_receipt(rec, "archive", "archive-complete", {"plan_sha256": "f" * 64}, clock=lambda: T0)
    rec["legacy"].setdefault("archive_receipt_sha256", r["sha256"])
    return rec


class Clock:
    def __init__(self, t=T0):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def snapshot_state(api):
    return copy.deepcopy(api.objects)


def test_the_plan_is_exact_ordered_and_read_only(tmp_path):
    api = legacy_cluster()
    plan = LG.build_plan(api, ready_record(tmp_path))
    assert api.mutations() == []
    ops = [(a["op"], a.get("name")) for a in plan["actions"]]
    def first(op, name=None):
        return next(i for i, (o, n) in enumerate(ops) if o == op and (name is None or n == name))
    assert ops[:3] == [("suspend", "ml-training"), ("suspend", "evidence-archive"), ("suspend", "load-evidence")]
    assert first("suspend") < first("wait_jobs") < first("scale", "k6-nginx-test") < \
        first("scale", "predictive-operator") < first("delete") < first("verify_no_writers") < \
        first("scale", "nginx-test") < first("scale", "ml-api")
    touched = {(a.get("kind"), a.get("name")) for a in plan["actions"] if a.get("kind")}
    assert not any(k in ("persistentvolumeclaims", "pa-legacy", "customresourcedefinitions", "leases")
                   for k, _ in touched)                                  # evidence, archived PAs, CRD, Lease: never
    for a in plan["actions"]:
        if a.get("kind"):
            assert a["uid"] and a["namespace"] in ("demo", "ml-engine")
    deletes = [a for a in plan["actions"] if a["op"] == "delete"]
    assert {a["name"] for a in deletes} == {"nginx-test-keda-fallback", "myapptwo-keda-fallback"}
    assert all(a["generated_hpa"]["name"] == f"keda-hpa-{a['name']}" for a in deletes)
    assert {r["name"] for r in plan["retained"] if r["role"] == "retained"} >= {"p8-evidence-pvc", "ml-models-pvc"}


@pytest.mark.parametrize("change, why", [
    (lambda rec, api: rec["legacy"].update(archive_receipt_sha256="0" * 64), "archive-complete"),
    (lambda rec, api: rec["legacy"].update(external_launchers_attested=False), "external_launchers_attested"),
    (lambda rec, api: api.objects.pop(("persistentvolumeclaims", "ml-engine", "p8-evidence-pvc")), "retained"),
    (lambda rec, api: api.add("pa-legacy", {"kind": "PredictiveAutoscaler",
                                            "metadata": {"name": "x", "namespace": "shop"},
                                            "spec": {"targetDeployment": {"name": "web"}}}), "another consumer"),
])
def test_the_plan_refuses_until_s0_and_consumers_are_settled(tmp_path, change, why):
    api, rec = legacy_cluster(), ready_record(tmp_path)
    change(rec, api)
    with pytest.raises(C.StagingError, match=why):
        LG.build_plan(api, rec)


def test_quiesce_runs_only_the_reviewed_plan_and_keeps_evidence(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    for digest in (None, "0" * 64):
        with pytest.raises(C.StagingError):
            LG.quiesce(api, rec, plan, digest, sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.mutations() == []
    keep = {k: copy.deepcopy(v) for k, v in api.objects.items()
            if k[0] in ("persistentvolumeclaims", "persistentvolumes", "pa-legacy", "customresourcedefinitions",
                        "leases", "secrets", "services")}
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    for k, v in keep.items():
        assert api.objects[k] == v, k                                   # byte-for-byte untouched
    deps = {k[2]: v for k, v in api.objects.items() if k[0] == "deployments"}
    assert all(d["spec"]["replicas"] == 0 for d in deps.values())
    assert all(v["spec"]["suspend"] for k, v in api.objects.items() if k[0] == "cronjobs")
    assert not any(k[0] in ("scaledobjects", "horizontalpodautoscalers") for k in api.objects)
    muts = api.mutations()
    assert all(v in ("replace", "delete") for v, _ in muts)
    assert not any("/persistentvolume" in p or "predictiveautoscalers" in p for _, p in muts)
    rlog = open(C.receipts_path(rec, "legacy")).read()
    assert not any(m in rlog for m in MARKERS)
    snaps = list((tmp_path / "evidence" / "legacy" / "deleted").iterdir())
    assert len(snaps) == 2 and not any(m in s.read_text() for s in snaps for m in MARKERS)


def test_execution_refuses_drift_and_another_cluster(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    d = C.plan_digest(plan)
    api.objects[("deployments", "demo", "k6-nginx-test")]["metadata"]["generation"] += 1      # edited after review
    with pytest.raises(C.StagingError, match="changed"):
        LG.quiesce(api, rec, plan, d, sleep=clock.sleep, monotonic=clock, clock=clock)
    api2, rec2 = legacy_cluster(), ready_record(tmp_path / "b")
    plan2 = LG.build_plan(api2, rec2)
    api2.objects[("namespaces", None, "kube-system")]["metadata"]["uid"] = "00000000-0000-4000-8000-000000000000"
    with pytest.raises(C.StagingError, match="kube-system UID"):
        LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api2.mutations() == []
    api3, rec3 = legacy_cluster(), ready_record(tmp_path / "c")
    plan3 = LG.build_plan(api3, rec3)
    old = api3.objects.pop(("scaledobjects", "demo", "myapptwo-keda-fallback"))      # recreated: new UID
    api3.add("scaledobjects", dict(old, metadata={k: v for k, v in old["metadata"].items() if k not in ("uid",)}))
    with pytest.raises(C.StagingError, match="another UID|changed since the plan"):
        LG.quiesce(api3, rec3, plan3, C.plan_digest(plan3), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert ("scaledobjects", "demo", "myapptwo-keda-fallback") in api3.objects        # never deleted


def test_active_jobs_finish_first_or_quiesce_stops(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    job = api.add("jobs", {"kind": "Job", "metadata": {"name": "load-evidence-1", "namespace": "ml-engine",
                                                       "ownerReferences": [{"kind": "CronJob",
                                                                            "name": "load-evidence"}]},
                           "status": {"active": 1}})
    plan = LG.build_plan(api, rec)
    polls = {"n": 0}

    def finish(a):
        polls["n"] += 1
        if polls["n"] > 40:
            job["status"] = {"active": 0, "succeeded": 1, "completionTime": "2026-11-08T00:05:00Z"}
    api.on_poll.append(finish)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b", job_deadline_seconds=30), Clock()
    api2.add("jobs", {"kind": "Job", "metadata": {"name": "p8-x", "namespace": "ml-engine", "labels": {"p8-run": "r"}},
                      "status": {"active": 1}})
    plan2 = LG.build_plan(api2, rec2)
    with pytest.raises(C.StagingError, match="active Jobs"):
        LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock2.sleep, monotonic=clock2, clock=clock2)
    assert api2.objects[("deployments", "demo", "k6-nginx-test")]["spec"]["replicas"] == 1   # stopped before arrivals


def test_verify_declares_retirement_only_after_ten_quiet_minutes(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    v0 = LG.verify(api, rec, 0, clock.sleep, clock, clock)
    assert v0["quiesced"] is False and v0["problems"]
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    v1 = LG.verify(api, rec, 0, clock.sleep, clock, clock)
    assert v1["quiesced"] and not v1["retired"] and v1["problems"] == []
    v2 = LG.verify(api, rec, 600, clock.sleep, clock, clock)
    assert v2["retired"] is True and v2["samples"] > 50                    # continuous, not two snapshots
    assert LG.verify(api, rec, 300, clock.sleep, clock, clock)["retired"] is False      # too short


def test_restore_undoes_exactly_what_quiesce_did(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    before = {k: (v["spec"].get("replicas"), v["spec"].get("suspend")) for k, v in api.objects.items()
              if k[0] in ("deployments", "cronjobs")}
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.add("deployments", {"kind": "Deployment", "metadata": {"name": "c28-pa-n-s20", "namespace": "demo"},
                            "spec": {"replicas": 2}})                                    # the new fixture
    rplan = LG.build_restore_plan(api, rec)
    assert rplan["actions"][0]["name"] == "ml-api"                                      # reverse order
    assert not any(a.get("name", "").startswith("c28-") for a in rplan["actions"])
    with pytest.raises(C.StagingError):
        LG.restore(api, rec, rplan, "0" * 64, clock=clock)
    LG.restore(api, rec, rplan, C.plan_digest(rplan), clock=clock)
    after = {k: (v["spec"].get("replicas"), v["spec"].get("suspend")) for k, v in api.objects.items()
             if k[0] in ("deployments", "cronjobs") and not k[2].startswith("c28-")}
    assert after == before
    assert api.objects[("deployments", "demo", "c28-pa-n-s20")]["spec"]["replicas"] == 2   # never touched
    so = api.objects[("scaledobjects", "demo", "nginx-test-keda-fallback")]
    assert so["spec"]["scaleTargetRef"] == {"name": "nginx-test"}
    ann = so["metadata"]["annotations"]
    assert ann["autoscaling.keda.sh/paused"] == "true" and ann[LG.OP_ANNOTATION].startswith("c28-r1-")
    assert set(ann) == {"autoscaling.keda.sh/paused", LG.OP_ANNOTATION}


def test_restore_never_overwrites_and_refuses_recreated_targets(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.add("scaledobjects", {"kind": "ScaledObject",
                              "metadata": {"name": "myapptwo-keda-fallback", "namespace": "demo"},
                              "spec": {}})
    rplan = LG.build_restore_plan(api, rec)
    with pytest.raises(C.StagingError, match="exists"):
        LG.restore(api, rec, rplan, C.plan_digest(rplan), clock=clock)
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b"), Clock()
    plan2 = LG.build_plan(api2, rec2)
    LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock2.sleep, monotonic=clock2, clock=clock2)
    old = api2.objects.pop(("deployments", "ml-engine", "ml-api"))
    api2.add("deployments", dict(old, metadata={k: v for k, v in old["metadata"].items() if k != "uid"}))
    rplan2 = LG.build_restore_plan(api2, rec2)
    with pytest.raises(C.StagingError, match="another UID"):
        LG.restore(api2, rec2, rplan2, C.plan_digest(rplan2), clock=clock2)


def test_the_cli_defaults_to_a_read_only_plan(tmp_path, capsys):
    api, rec = legacy_cluster(), ready_record(tmp_path)
    path = tmp_path / "rec.json"
    path.write_text(json.dumps(rec))
    assert LG.main(["--record", str(path)], api=api) == 0
    assert api.mutations() == []
    out = capsys.readouterr().out
    plan_file = out.split("plan: ")[1].split("\n")[0]
    digest = out.strip().split()[-1]
    assert C.plan_digest(C.load_plan(plan_file)) == digest
    assert LG.main(["--record", str(path), "quiesce", "--plan", plan_file, "--plan-digest", "1" * 64], api=api) == 1
    assert api.mutations() == []
    clock = Clock()
    assert LG.main(["--record", str(path), "quiesce", "--plan", plan_file, "--plan-digest", digest], api=api,
                   sleep=clock.sleep, monotonic=clock, clock=clock) == 0
    for f in (tmp_path / "evidence").rglob("*"):
        if f.is_file():
            assert not any(m in f.read_text() for m in MARKERS), f


@pytest.mark.parametrize("kind, ns, name", [("persistentvolumeclaims", "ml-engine", "p8-evidence-pvc"),
                                            ("pa-legacy", "demo", "nginx-test-autoscaler"),
                                            ("deployments", "demo", "c28-pa-n-s20")])
def test_even_a_reviewed_plan_cannot_touch_evidence_or_foreign_objects(tmp_path, kind, ns, name):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    plan["actions"].insert(0, {"step": 0, "op": "delete", "kind": kind, "namespace": ns, "name": name, "uid": "u"})
    with pytest.raises(C.StagingError, match="not a mutable legacy object"):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.mutations() == []



# --- Codex r1 on #31 ----------------------------------------------------------------------------------------------

def test_a_consumer_added_after_planning_stops_the_operator_shutdown(tmp_path):
    # Finding 2: consumers and reconcilers are re-checked live right before the operator stops.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    api.add("pa-legacy", {"kind": "PredictiveAutoscaler", "metadata": {"name": "shop-pa", "namespace": "shop"},
                          "spec": {"targetDeployment": {"name": "web", "namespace": "shop"}}})
    with pytest.raises(C.StagingError, match="another consumer"):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.objects[("deployments", "ml-engine", "predictive-operator")]["spec"]["replicas"] == 1
    assert api.objects[("deployments", "demo", "k6-nginx-test")]["spec"]["replicas"] == 0     # arrivals did stop


@pytest.mark.parametrize("change, why", [
    (lambda api: api.objects[("pa-legacy", "demo", "nginx-test-autoscaler")]["spec"]["targetDeployment"].update(
        name="checkout"), "not its arm"),
    (lambda api: api.objects[("scaledobjects", "demo", "myapptwo-keda-fallback")]["spec"]["scaleTargetRef"].update(
        name="nginx-test"), "not its arm"),
    (lambda api: api.objects[("horizontalpodautoscalers", "demo", "keda-hpa-myapptwo-keda-fallback")][
        "metadata"].update(ownerReferences=[]), "not on the allowlist"),
    (lambda api: api.objects[("horizontalpodautoscalers", "demo", "keda-hpa-myapptwo-keda-fallback")]["spec"][
        "scaleTargetRef"].update(name="nginx-reactive"), "owned by myapptwo-keda-fallback but targets"),
])
def test_allowlisted_writers_are_checked_by_actual_target_and_owner(tmp_path, change, why):
    api, rec = legacy_cluster(), ready_record(tmp_path)
    change(api)
    with pytest.raises(C.StagingError, match=why):
        LG.build_plan(api, rec)


def test_surviving_pods_or_a_renewed_lease_block_parking_and_retirement(tmp_path):
    # Finding 3: owned pods (terminating ones included) and Lease renewals are the shutdown signals.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    op = api.objects[("deployments", "ml-engine", "predictive-operator")]
    rs = api.objects[("replicasets", "ml-engine", "predictive-operator-rs")]
    api.instant_pods = False
    real_replace = api.replace

    def replace(path, body):
        out = real_replace(path, body)
        if path.endswith("/deployments/predictive-operator/scale"):          # the old pod hangs in Terminating
            api.add("pods", {"kind": "Pod", "metadata": {"name": "predictive-operator-stuck", "namespace": "ml-engine",
                                                         "deletionTimestamp": "2026-11-08T00:00:00Z",
                                                         "ownerReferences": [{"kind": "ReplicaSet",
                                                                              "uid": rs["metadata"]["uid"]}]}})
        return out
    api.replace = replace
    for k in [k for k, v in api.objects.items() if k[0] == "pods"]:          # generators' pods are gone already
        if not any(r.get("uid") == rs["metadata"]["uid"] for r in api.objects[k]["metadata"]["ownerReferences"]):
            del api.objects[k]
    with pytest.raises(C.StagingError, match="pod termination"):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert op["spec"]["replicas"] == 0
    assert api.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 3        # not parked

    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b"), Clock()
    plan2 = LG.build_plan(api2, rec2)
    lease = api2.objects[("leases", "ml-engine", "predictive-autoscaler-leader")]
    polls = {"n": 0}

    def renew(a):
        polls["n"] += 1
        if api2.objects[("deployments", "ml-engine", "predictive-operator")]["spec"]["replicas"] == 0:
            lease["spec"]["renewTime"] = f"2026-11-08T00:{polls['n'] % 60:02d}:00Z"          # someone still renews
    api2.on_poll.append(renew)
    with pytest.raises(C.StagingError, match="Lease"):
        LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock2.sleep, monotonic=clock2, clock=clock2)
    assert api2.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 3       # not parked

    api3, rec3, clock3 = legacy_cluster(), ready_record(tmp_path / "c"), Clock()
    plan3 = LG.build_plan(api3, rec3)
    LG.quiesce(api3, rec3, plan3, C.plan_digest(plan3), sleep=clock3.sleep, monotonic=clock3, clock=clock3)
    lease3 = api3.objects[("leases", "ml-engine", "predictive-autoscaler-leader")]
    n = {"i": 0}

    def renew_later(a):
        n["i"] += 1
        if n["i"] == 200:
            lease3["spec"]["renewTime"] = "2026-11-09T00:00:00Z"
    api3.on_poll.append(renew_later)
    v = LG.verify(api3, rec3, 600, clock3.sleep, clock3, clock3)
    assert v["retired"] is False and any("changed during the observation" in p for p in v["problems"])


def test_restore_refuses_drift_after_quiesce(tmp_path):
    # Finding 4: restore is bound to the post-quiesce generation and value.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    rplan = LG.build_restore_plan(api, rec)
    d = api.objects[("deployments", "demo", "nginx-reactive")]
    d["spec"]["replicas"] = 5                                               # a newer owner decision, same UID
    d["metadata"]["generation"] += 1
    with pytest.raises(C.StagingError, match="changed|drifted"):
        LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert d["spec"]["replicas"] == 5                                       # never overwritten


def test_restore_refuses_a_foreign_writer_before_restoring_writers(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.add("pa-new", {"kind": "PredictiveAutoscaler", "metadata": {"name": "adopter", "namespace": "demo"},
                       "spec": {"targetDeployment": {"name": "nginx-test", "namespace": "demo"}}})
    rplan = LG.build_restore_plan(api, rec)
    with pytest.raises(C.StagingError, match="already have writers"):
        LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert ("scaledobjects", "demo", "nginx-test-keda-fallback") not in api.objects
    assert api.objects[("deployments", "ml-engine", "predictive-operator")]["spec"]["replicas"] == 0


def test_a_lost_response_after_a_delete_stays_recoverable(tmp_path):
    # Finding 5: the intent (with the snapshot) is durable before the call; an ambiguous outcome keeps the lock and
    # restore-plan reconciles it against the live object.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    real_delete = api.delete

    def lossy(path, uid, rv=None):
        real_delete(path, uid, rv)
        raise C.ApiError("delete: failed (timeout, exit 1)", 504)            # deleted, but the response was lost
    api.delete = lossy
    with pytest.raises(C.ApiError):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert ("scaledobjects", "demo", "nginx-test-keda-fallback") not in api.objects
    lock = C.lock_path(rec, "legacy")
    assert os.path.exists(lock)                                              # fail closed
    with pytest.raises(C.StagingError, match="another execution"):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.delete = real_delete
    with pytest.raises(C.StagingError, match="run legacy.py recover"):
        LG.build_restore_plan(api, rec)                                      # no guessing over an ambiguous intent
    with pytest.raises(C.StagingError, match="sha256 differs"):
        LG.recover(api, rec, "0" * 64, clock)
    with pytest.raises(C.StagingError, match="still running"):
        LG.recover(api, rec, C.sha256_file(lock), clock)                 # this test process holds it: alive
    crash_owner(lock)                                                   # the executor has died
    report = LG.recover(api, rec, C.sha256_file(lock), clock)
    assert list(report.values()) == ["done"] and not os.path.exists(lock)
    rplan = LG.build_restore_plan(api, rec)
    recreate = [a for a in rplan["actions"] if a["op"] == "recreate"]
    assert [a["name"] for a in recreate] == ["nginx-test-keda-fallback"]
    LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert ("scaledobjects", "demo", "nginx-test-keda-fallback") in api.objects
    assert api.objects[("deployments", "demo", "k6-nginx-test")]["spec"]["replicas"] == 1



# --- Codex r2 on #31 ----------------------------------------------------------------------------------------------

def test_an_orphan_hpa_is_an_independent_writer_at_restore(tmp_path):
    # Finding 1: only a VERIFIED owner/target pair is one writer; an HPA with a dangling ScaledObject reference counts.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.add("horizontalpodautoscalers", {"kind": "HorizontalPodAutoscaler", "metadata": {
        "name": "orphan", "namespace": "demo", "ownerReferences": [{"kind": "ScaledObject", "name": "gone",
                                                                    "uid": "dead-beef", "controller": True}]},
        "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "nginx-test"}}})
    assert LG.writer_census(api)["nginx-test"] == ["horizontalpodautoscalers orphan"]
    rplan = LG.build_restore_plan(api, rec)
    with pytest.raises(C.StagingError, match="already have writers"):
        LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.objects[("deployments", "ml-engine", "predictive-operator")]["spec"]["replicas"] == 0


def test_a_change_after_validation_is_never_overwritten(tmp_path):
    # Finding 2: the PUT carries the validated object's resourceVersion; the post-state is the response's.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    step = next(r for r in C.read_receipts(C.receipts_path(rec, "legacy"))
                if r["event"].startswith("step-") and r["data"]["action"].get("name") == "nginx-reactive")
    d = api.objects[("deployments", "demo", "nginx-reactive")]
    assert step["data"]["post"]["replicas"] == 0 and step["data"]["post"]["generation"] == d["metadata"]["generation"]
    rplan = LG.build_restore_plan(api, rec)
    real = api.replace

    def racing(path, body):
        if path.endswith("/deployments/nginx-reactive/scale"):                 # another writer, after validation
            d["spec"]["replicas"] = 7
            d["metadata"]["resourceVersion"] = api._next_rv()
        return real(path, body)
    api.replace = racing
    with pytest.raises(C.ApiError, match="conflict"):
        LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert d["spec"]["replicas"] == 7                                       # the intervening change stands


def test_quiesce_and_restore_share_one_lock(tmp_path):
    # Finding 3: one mutation lock per cluster and scope, whichever helper holds it.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    rplan = LG.build_restore_plan(api, rec)
    with C.exclusive_lock(rec, "legacy", holder="a concurrent quiesce"):
        with pytest.raises(C.StagingError, match="another execution"):
            LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.objects[("deployments", "ml-engine", "ml-api")]["spec"]["replicas"] == 0


@pytest.mark.parametrize("metrics, why", [(Metrics(rate=0.4), "still receives requests"),
                                          (Metrics(missing={"myapptwo"}), "no fresh, finite request-counter"),
                                          (Metrics(age=900), "stale")])
def test_arrivals_must_be_observed_zero_before_parking(tmp_path, quiet_metrics, metrics, why):
    # Finding 5: a quiet replica count is not zero traffic; fresh per-target request counters are.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    with pytest.raises(C.StagingError, match=why):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock,
                   metrics=metrics)
    assert api.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 3        # not parked
    assert any('destination_workload="nginx-ensemble-q95"' in q for q in metrics.queries)


def test_retirement_needs_fresh_zero_arrivals_throughout(tmp_path, quiet_metrics):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert LG.verify(api, rec, 600, clock.sleep, clock, clock)["retired"] is True
    blind = Metrics(missing={"nginx-test"})
    v = LG.verify(api, rec, 600, clock.sleep, clock, clock, metrics=blind)
    assert v["retired"] is False and any("no fresh, finite request-counter observation" in p for p in v["problems"])
    with pytest.raises(C.StagingError, match="arrivals"):
        LG.build_plan(api, dict(rec, legacy={k: v for k, v in rec["legacy"].items() if k != "arrivals"}))


def test_the_metrics_client_returns_one_value_or_nothing():
    import io

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def opener(body):
        return lambda url, timeout: Resp(json.dumps(body).encode())
    now = lambda: 1000.0  # noqa: E731
    one = {"status": "success", "data": {"result": [{"value": [1000, "0"]}]}}
    two = {"status": "success", "data": {"result": [{"value": [1000, "0"]}, {"value": [1000, "3"]}]}}
    assert REAL_PROM("http://vm", opener=opener(one), now=now)("q") == 0.0
    assert REAL_PROM("http://vm", opener=opener(two), now=now)("q") is None   # ambiguous: no observation
    assert REAL_PROM("http://vm", opener=opener({"status": "success", "data": {"result": []}}), now=now)("q") is None
    stale = {"status": "success", "data": {"result": [{"value": [100, "0"]}]}}
    assert REAL_PROM("http://vm", opener=opener(stale), now=now)("q") is None # Codex r3: timestamps are checked
    nan = {"status": "success", "data": {"result": [{"value": [1000, "NaN"]}]}}
    assert REAL_PROM("http://vm", opener=opener(nan), now=now)("q") is None

    def down(url, timeout):
        raise OSError("connection refused")
    assert REAL_PROM("http://vm", opener=down, now=now)("q") is None



# --- Codex r3 on #31 ----------------------------------------------------------------------------------------------

def lost_delete(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    real_delete = api.delete

    def lossy(path, uid, rv=None):
        real_delete(path, uid, rv)
        raise C.ApiError("delete: failed (timeout, exit 1)", 504)
    api.delete = lossy
    with pytest.raises(C.ApiError):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.delete = real_delete
    return api, rec, clock, C.lock_path(rec, "legacy")


def test_recovery_never_takes_a_live_or_foreign_lock(tmp_path):
    # Finding 1: exclusive recovery, executor proven gone; a mandatory shared lock directory.
    api, rec, clock, lock = lost_delete(tmp_path)
    with pytest.raises(C.StagingError, match="still running"):
        LG.recover(api, rec, C.sha256_file(lock), clock)
    body = json.load(open(lock))
    body["host"] = "another-host"
    open(lock, "w").write(json.dumps(body))
    with pytest.raises(C.StagingError, match="another-host"):
        LG.recover(api, rec, C.sha256_file(lock), clock)
    crash_owner(lock)
    body = json.load(open(lock))
    body["host"] = __import__("socket").gethostname()
    open(lock, "w").write(json.dumps(body))
    open(lock + ".recovery", "w").close()                                 # a concurrent recoverer
    with pytest.raises(C.StagingError, match="another recovery"):
        LG.recover(api, rec, C.sha256_file(lock), clock)
    os.unlink(lock + ".recovery")
    assert LG.recover(api, rec, C.sha256_file(lock), clock) and not os.path.exists(lock)
    assert any("lock_dir is required" in p for p in C.record_problems({k: v for k, v in rec.items()
                                                                        if k != "lock_dir"}))
    with pytest.raises(C.StagingError, match="lock_dir"):
        C.lock_path({k: v for k, v in rec.items() if k != "lock_dir"}, "legacy")


def test_an_ambiguous_recreation_or_drifted_update_stays_unresolved(tmp_path):
    # Finding 2: a same-named object without this operation's identity, or an update with drift, decides nothing.
    api, rec, clock, lock = lost_delete(tmp_path)
    crash_owner(lock)
    api.objects[("deployments", "demo", "k6-nginx-test")]["metadata"]["generation"] += 5       # drift elsewhere
    api.add("scaledobjects", {"kind": "ScaledObject", "metadata": {"name": "nginx-test-keda-fallback",
                                                                   "namespace": "demo"}, "spec": {}})
    with pytest.raises(C.StagingError, match="unresolved outcomes"):
        LG.recover(api, rec, C.sha256_file(lock), clock)
    assert os.path.exists(lock)                                             # fail closed
    events = [r["event"] for r in C.read_receipts(C.receipts_path(rec, "legacy"))]
    assert events[-1] == "recovery-unresolved" and not any(e.startswith("reconciled-") for e in events)
    # The recreation identity: only the object created by this restore operation counts as its outcome.
    d = {"action": {"op": "recreate", "kind": "scaledobjects", "namespace": "demo", "name": "nginx-test-keda-fallback"},
         "operation": "op-1"}
    assert LG._observe_outcome(api, d)["observed"] == "unresolved"
    api.objects[("scaledobjects", "demo", "nginx-test-keda-fallback")]["metadata"]["annotations"] = {
        LG.OP_ANNOTATION: "op-1"}
    assert LG._observe_outcome(api, d)["observed"] == "done"
    dep = api.objects[("deployments", "demo", "nginx-reactive")]
    g = dep["metadata"]["generation"]
    up = {"action": {"op": "scale", "kind": "deployments", "namespace": "demo", "name": "nginx-reactive",
                     "uid": dep["metadata"]["uid"], "after": 0}, "pre": {"replicas": 3, "generation": g}}
    assert LG._observe_outcome(api, up)["observed"] == "not-done"
    dep["spec"]["replicas"], dep["metadata"]["generation"] = 0, g + 2       # done, plus another change
    assert LG._observe_outcome(api, up)["observed"] == "unresolved"
    dep["metadata"]["generation"] = g + 1
    assert LG._observe_outcome(api, up)["observed"] == "done"


@pytest.mark.parametrize("rate, age", [(float("nan"), 5.0), (0.0, float("nan")), (-1.0, 5.0), (0.0, -3.0),
                                       (float("inf"), 5.0), (0.0, 121.0), (1e-9, 5.0)])
def test_the_arrival_gate_needs_finite_zero_rate_and_a_valid_age(rate, age):
    # Finding 3.
    problems = LG.arrival_problems(lambda q: age if q.startswith("time()") else rate, ARRIVALS, ["nginx-test"])
    assert problems and "nginx-test" in problems[0]
    assert LG.arrival_problems(lambda q: 0.0, ARRIVALS, ["nginx-test"]) == []


def test_a_paused_scaled_object_does_not_hide_its_generated_hpa(tmp_path):
    # Finding 4: a pause annotation does not prove the generated HPA stopped writing.
    api = legacy_cluster()
    api.objects[("deployments", "ml-engine", "predictive-operator")]["spec"]["replicas"] = 0   # PAs inactive
    census = LG.writer_census(api)
    assert census["nginx-test"] == ["horizontalpodautoscalers keda-hpa-nginx-test-keda-fallback"]   # not empty
    assert census["myapptwo"] == ["scaledobjects myapptwo-keda-fallback"]          # absorbed: verified, counted owner



def test_recovery_needs_the_owning_runs_record(tmp_path):
    # Codex r4 on #31: the lock names its run, record and receipt location; another record cannot release it.
    api, rec, clock, lock = lost_delete(tmp_path)
    crash_owner(lock)
    sha = C.sha256_file(lock)
    other_run = dict(rec, run_id="c28-r2", evidence_dir=str(tmp_path / "evidence-r2"))
    same_run_elsewhere = dict(rec, evidence_dir=str(tmp_path / "evidence-copy"))
    edited = dict(rec, legacy=dict(rec["legacy"], observe_seconds=300))
    for wrong, field in ((other_run, "run_id"), (same_run_elsewhere, "receipts_dir"), (edited, "record_sha256")):
        with pytest.raises(C.StagingError, match=f"another run or record.*{field}"):
            LG.recover(api, wrong, sha, clock)
        assert os.path.exists(lock)
    assert not any(r["event"].startswith("reconciled-") for r in C.read_receipts(C.receipts_path(rec, "legacy")))
    assert LG.recover(api, rec, sha, clock) and not os.path.exists(lock)



# --- C2.8f: an absent Lease (leader election off in 0472829) -------------------------------------------------------

def no_lease_cluster():
    api = legacy_cluster()
    del api.objects[("leases", "ml-engine", "predictive-autoscaler-leader")]
    return api


def test_an_absent_lease_that_stays_absent_is_quiet(tmp_path):
    api, rec, clock = no_lease_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    obs = next(a for a in plan["actions"] if a["op"] == "observe_quiet")
    assert obs["lease"] is None
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 0
    assert LG.verify(api, rec, 600, clock.sleep, clock, clock)["retired"] is True


def appear_lease(api, after_polls):
    n = {"i": 0}

    def hook(a):
        n["i"] += 1
        if n["i"] == after_polls and ("leases", "ml-engine", "predictive-autoscaler-leader") not in a.objects:
            a.add("leases", {"kind": "Lease", "metadata": {"name": "predictive-autoscaler-leader",
                                                           "namespace": "ml-engine"},
                             "spec": {"holderIdentity": "predictive-operator-new",
                                      "renewTime": "2026-11-08T03:00:00Z"}})
    api.on_poll.append(hook)


def test_a_lease_appearing_during_the_quiet_interval_blocks_parking(tmp_path):
    api, rec, clock = no_lease_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    real_get, reads = api.get, {"n": 0}

    def get(path):                               # the observation's baseline reads it absent; then a leader appears
        if path.endswith("/leases/predictive-autoscaler-leader"):
            reads["n"] += 1
            if reads["n"] == 3:
                api.add("leases", {"kind": "Lease", "metadata": {"name": "predictive-autoscaler-leader",
                                                                 "namespace": "ml-engine"},
                                   "spec": {"holderIdentity": "x", "renewTime": "2026-11-08T03:00:00Z"}})
        return real_get(path)
    api.get = get
    with pytest.raises(C.StagingError, match="Lease appeared"):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert reads["n"] >= 3                                                   # it happened inside the observation
    assert api.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 3        # not parked


def test_a_lease_appearing_after_planning_or_during_verify_blocks(tmp_path):
    api, rec, clock = no_lease_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    appear_lease(api, 1)                                                     # before the observation starts
    with pytest.raises(C.StagingError, match="Lease appeared"):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api2, rec2, clock2 = no_lease_cluster(), ready_record(tmp_path / "b"), Clock()
    plan2 = LG.build_plan(api2, rec2)
    LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock2.sleep, monotonic=clock2, clock=clock2)
    appear_lease(api2, 150)                                                   # mid-observation
    v = LG.verify(api2, rec2, 600, clock2.sleep, clock2, clock2)
    assert v["retired"] is False and any("Lease appeared" in p for p in v["problems"])


def test_a_planned_lease_disappearing_before_the_observation_blocks(tmp_path):
    # C2.8f review: presence and UID are compared both ways when the baseline is set.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    assert next(a for a in plan["actions"] if a["op"] == "observe_quiet")["lease"] is not None
    del api.objects[("leases", "ml-engine", "predictive-autoscaler-leader")]
    with pytest.raises(C.StagingError, match="disappeared"):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 3        # not parked
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b"), Clock()
    plan2 = LG.build_plan(api2, rec2)
    api2.objects[("leases", "ml-engine", "predictive-autoscaler-leader")]["spec"]["renewTime"] = "2026-11-08T01:00:00Z"
    LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock2.sleep, monotonic=clock2, clock=clock2)
    assert api2.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 0       # renewal before: fine


# --- C2.8h: the shared lock is released only when no tool's intent is unresolved -----------------------------------

def test_legacy_recover_never_releases_a_lock_with_a_populate_intent_unresolved(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    with pytest.raises(RuntimeError):
        with C.exclusive_lock(rec, "legacy", holder="populate-s0") as lock:
            C.append_receipt(rec, "populate-s0", "intent-1", {"pvc": "p8-evidence-pvc", "nonce": "n-1",
                                                               "pod": "c28-s0-reader-1"}, clock)
            lock.safe = False                       # the reader pod's cleanup is not proven
            raise RuntimeError("populate crashed")
    C.append_receipt(rec, "populate-s0", "delete-intent-1", {"pod": "c28-s0-reader-1"}, clock)   # not an intent-
    assert C.unresolved_intents(rec) == {"populate-s0": ["1"]}
    lock = C.lock_path(rec, "legacy")
    crash_owner(lock)
    with pytest.raises(C.StagingError, match="populate-s0.*populate_s0.py recover"):
        LG.recover(api, rec, C.sha256_file(lock), clock)                   # legacy has nothing of its own to do
    assert os.path.exists(lock) and not os.path.exists(lock + ".recovery")
    C.append_receipt(rec, "populate-s0", "reconciled-1", {"observed": "done", "cleanup": "proven"}, clock)
    assert C.unresolved_intents(rec) == {}
    LG.recover(api, rec, C.sha256_file(lock), clock)
    assert not os.path.exists(lock)


def test_unresolved_intents_spans_every_journal(tmp_path):
    rec = record(tmp_path)
    C.append_receipt(rec, "legacy", "intent-3", {}, lambda: 0)
    C.append_receipt(rec, "legacy", "step-3", {}, lambda: 0)
    C.append_receipt(rec, "legacy-restore", "intent-12", {}, lambda: 0)
    C.append_receipt(rec, "populate-s0-probe", "intent-2", {}, lambda: 0)
    C.append_receipt(rec, "populate-s0-probe", "reconciled-2", {"observed": "not-done"}, lambda: 0)
    assert C.unresolved_intents(rec) == {"legacy-restore": ["12"]}
