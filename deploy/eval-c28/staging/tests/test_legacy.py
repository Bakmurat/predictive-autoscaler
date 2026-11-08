"""legacy.py: an exact, UID-bound S4 in two reviewed phases (quiesce, then park from phase A's receipt); execution only
with the reviewed digest; evidence and archived objects untouched; the pre-parking arrivals proof, the post-parking
control-state verification and the ten quiet minutes; selective restore across both phases."""

import copy
import json
import os

import pytest

import common_records as C
import legacy as LG
from fakes import CLUSTER, MARKERS, legacy_cluster, record

T0 = 1_794_000_000
REAL_PROM = LG.PromClient                                                    # before the autouse fixture patches it


class AllFresh(dict):
    """A per-reporter series map in which every pod has a series of the given age."""

    def __init__(self, age):
        super().__init__()
        self.age = age

    def get(self, key, default=None):
        return self.age


class Metrics:
    """A fake metrics API: per-target request rate and data age, and per-reporter series ages (no network). With an
    api, the series are the target's running pods (one of them stale for the targets in `stale`)."""

    def __init__(self, rate=0.0, age=5.0, missing=(), api=None, stale=(), rate_fn=None):
        self.rate, self.age, self.missing, self.queries = rate, age, set(missing), []
        self.api, self.stale, self.rate_fn = api, set(stale), rate_fn

    @staticmethod
    def target(expr):
        return expr.split('workload="')[1].split('"')[0]

    def __call__(self, expr):
        self.queries.append(expr)
        if self.target(expr) in self.missing:
            return None
        if expr.startswith("time()"):
            return self.age
        return self.rate_fn() if self.rate_fn else self.rate

    def vector(self, expr, label):
        self.queries.append(expr)
        assert label == "pod" and "by (pod)" in expr
        t = self.target(expr)
        if t in self.missing:
            return None
        if self.api is None:
            return AllFresh(self.age)
        d = self.api.objects[("deployments", "demo", t)]
        pods = LG.running_pods(self.api, "demo", d["metadata"]["uid"])
        out = {p: self.age for p in pods}
        if t in self.stale and pods:
            out[pods[0]] = 900.0                                # one reporter stale; the newest stays fresh
        return out


@pytest.fixture(autouse=True)
def quiet_metrics(monkeypatch):
    m = Metrics()
    monkeypatch.setattr(LG, "PromClient", lambda base, **kw: m)          # never a network call in tests
    return m


SEL = ('istio_requests_total{destination_workload="{target}",destination_workload_namespace="demo",'
       'reporter="destination"}')
ARRIVALS = {"query_api": "http://127.0.0.1:18481/select/0/prometheus",
            "rate_query": f"sum(rate({SEL}[1m]))",
            "age_query": f"time() - max(timestamp({SEL}))",
            "series_query": f"time() - max by (pod) (timestamp({SEL}))",
            "pod_label": "pod", "max_age_seconds": 120, "settle_seconds": 90}


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
                                   "observe_seconds": 600, "arrivals": dict(ARRIVALS),
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


def quiesce(api, rec, clock, metrics=None):
    """Phase A: plan and execute it."""
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock, metrics=metrics)
    return plan


def park(api, rec, clock, metrics=None):
    """Phase B: plan from phase A's receipt and execute it."""
    pplan = LG.build_park_plan(api, rec)
    LG.park(api, rec, pplan, C.plan_digest(pplan), sleep=clock.sleep, monotonic=clock, clock=clock, metrics=metrics)
    return pplan


def full(api, rec, clock, metrics=None):
    return quiesce(api, rec, clock, metrics), park(api, rec, clock, metrics)


def test_the_plan_is_exact_ordered_and_read_only(tmp_path):
    api = legacy_cluster()
    plan = LG.build_plan(api, ready_record(tmp_path))
    assert api.mutations() == []
    ops = [(a["op"], a.get("name")) for a in plan["actions"]]
    def first(op, name=None):
        return next(i for i, (o, n) in enumerate(ops) if o == op and (name is None or n == name))
    assert ops[:3] == [("suspend", "ml-training"), ("suspend", "evidence-archive"), ("suspend", "load-evidence")]
    assert first("suspend") < first("wait_jobs") < first("scale", "k6-nginx-test") < \
        first("scale", "predictive-operator") < first("delete") < first("verify_no_writers")
    assert [o for o, _ in ops[-4:]] == ["verify_no_writers", "settle", "observe_quiet", "capture_targets"]
    assert plan["actions"][-3]["seconds"] == 90 and plan["phase"] == "quiesce"
    assert not any(n in ("nginx-test", "ml-api") for o, n in ops if o == "scale")          # phase A never parks
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
    clock = Clock()
    rec = ready_record(tmp_path / "b")
    api = legacy_cluster()
    quiesce(api, rec, clock)
    n0 = len(api.mutations())
    pplan = LG.build_park_plan(api, rec)
    assert len(api.mutations()) == n0                                       # park-plan is read-only too
    pops = [(a["op"], a.get("name")) for a in pplan["actions"]]
    assert pops[0][0] == "recheck_quiesced" and pplan["phase"] == "park" and pplan["helper"] == "legacy-park"
    assert pops.index(("scale", "nginx-test")) < pops.index(("wait_jobs", None)) < pops.index(("scale", "ml-api"))
    assert {n for o, n in pops if o == "scale"} == set(LG.L.ARMS) | {"ml-api"}
    assert min(a["step"] for a in pplan["actions"]) > max(a["step"] for a in LG.build_plan(
        legacy_cluster(), ready_record(tmp_path / "c"))["actions"])           # one journal, steps continue


def test_phase_a_binds_target_uids_only_and_never_parks(tmp_path):
    # Codex finding 1: the old operator may change the targets until it stops; phase A binds their UIDs only.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    arms = set(LG.L.ARMS)
    assert not any(a.get("name") in arms for a in plan["actions"] if a.get("kind"))
    for op in ("observe_quiet", "capture_targets"):
        a = next(x for x in plan["actions"] if x["op"] == op)
        assert {t[1] for t in a["targets"]} == arms and all(len(t) == 3 for t in a["targets"])
    assert "generation" not in json.dumps([a for a in plan["actions"] if a["op"] in ("observe_quiet",
                                                                                    "capture_targets")])
    before = {a: api.objects[("deployments", "demo", a)]["spec"]["replicas"] for a in arms}
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert {a: api.objects[("deployments", "demo", a)]["spec"]["replicas"] for a in arms} == before
    done = [r for r in C.read_receipts(C.receipts_path(rec, "legacy")) if r["event"] == "quiesce-complete"][-1]
    cap = {t["name"]: t for t in done["data"]["targets"]}
    d = api.objects[("deployments", "demo", "nginx-test")]
    assert cap["nginx-test"] == {"namespace": "demo", "name": "nginx-test", "uid": d["metadata"]["uid"],
                                 "generation": d["metadata"]["generation"], "replicas": 3,
                                 "resourceVersion": d["metadata"]["resourceVersion"]}
    assert done["data"]["observed_seconds"] == 600 and done["data"]["observed_receipt_sha256"]
    # A phase A plan edited to park a target is refused even with its digest.
    bad = copy.deepcopy(plan)
    bad["actions"].insert(0, {"step": 0, "op": "scale", "kind": "deployments", "namespace": "demo",
                              "name": "nginx-test", "uid": d["metadata"]["uid"], "after": 0})
    with pytest.raises(C.StagingError, match="this phase may not touch"):
        LG.quiesce(api, rec, bad, C.plan_digest(bad), sleep=clock.sleep, monotonic=clock, clock=clock)


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
    pplan = LG.build_park_plan(api, rec)
    for digest in (None, "0" * 64, C.plan_digest(plan)):
        with pytest.raises(C.StagingError):
            LG.park(api, rec, pplan, digest, sleep=clock.sleep, monotonic=clock, clock=clock)
    with pytest.raises(C.StagingError, match="another helper"):
        LG.quiesce(api, rec, pplan, C.plan_digest(pplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 3
    LG.park(api, rec, pplan, C.plan_digest(pplan), sleep=clock.sleep, monotonic=clock, clock=clock)
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
    quiesce(api, rec, clock)
    va = LG.verify(api, rec, 600, clock.sleep, clock, clock)                 # phase A alone: targets not parked
    assert va["quiesced"] is False and va["retired"] is False and va["parked"] is False
    assert any("not at 0 replicas" in p for p in va["problems"])
    park(api, rec, clock)
    v1 = LG.verify(api, rec, 0, clock.sleep, clock, clock)
    assert v1["quiesced"] and not v1["retired"] and v1["problems"] == []
    v2 = LG.verify(api, rec, 600, clock.sleep, clock, clock)
    assert v2["retired"] is True and v2["samples"] > 50                    # continuous, not two snapshots
    assert LG.verify(api, rec, 300, clock.sleep, clock, clock)["retired"] is False      # too short


def test_restore_undoes_exactly_what_quiesce_did(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    before = {k: (v["spec"].get("replicas"), v["spec"].get("suspend")) for k, v in api.objects.items()
              if k[0] in ("deployments", "cronjobs")}
    full(api, rec, clock)
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
    full(api, rec, clock)
    api.add("scaledobjects", {"kind": "ScaledObject",
                              "metadata": {"name": "myapptwo-keda-fallback", "namespace": "demo"},
                              "spec": {}})
    rplan = LG.build_restore_plan(api, rec)
    with pytest.raises(C.StagingError, match="exists"):
        LG.restore(api, rec, rplan, C.plan_digest(rplan), clock=clock)
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b"), Clock()
    full(api2, rec2, clock2)
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
    full(api3, rec3, clock3)
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
    full(api, rec, clock)
    rplan = LG.build_restore_plan(api, rec)
    d = api.objects[("deployments", "demo", "nginx-reactive")]
    d["spec"]["replicas"] = 5                                               # a newer owner decision, same UID
    d["metadata"]["generation"] += 1
    with pytest.raises(C.StagingError, match="changed|drifted"):
        LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert d["spec"]["replicas"] == 5                                       # never overwritten


def test_restore_refuses_a_foreign_writer_before_restoring_writers(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    full(api, rec, clock)
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
    full(api, rec, clock)
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
    full(api, rec, clock)
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
    full(api, rec, clock)
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


def test_post_parking_verify_reads_no_metrics_and_never_takes_absence_for_zero(tmp_path, monkeypatch):
    # Codex finding 2: parking removes the reporters; retirement = the pre-parking proof + post-parking control state.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    full(api, rec, clock)

    def no_metrics(*a, **kw):
        raise AssertionError("verify must not read metrics after parking")
    monkeypatch.setattr(LG, "PromClient", no_metrics)
    v = LG.verify(api, rec, 600, clock.sleep, clock, clock)
    assert v["retired"] is True and "never a zero" in v["metrics_after_parking"]
    q = [r for r in C.read_receipts(C.receipts_path(rec, "legacy")) if r["event"] == "quiesce-complete"][-1]
    assert v["arrivals_proof"]["observed_receipt_sha256"] == q["data"]["observed_receipt_sha256"]
    assert v["arrivals_proof"]["observed_seconds"] == 600
    with pytest.raises(C.StagingError, match="arrivals"):
        LG.build_plan(api, dict(rec, legacy={k: v for k, v in rec["legacy"].items() if k != "arrivals"}))


def test_retirement_needs_the_ten_minute_pre_parking_proof(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path, observe_seconds=120), Clock()
    full(api, rec, clock)
    v = LG.verify(api, rec, 600, clock.sleep, clock, clock)
    assert v["quiesced"] is True and v["retired"] is False and v["arrivals_proof"]["observed_seconds"] == 120


def _rs_uid(api, name):
    return api.objects[("replicasets", "demo" if not name.startswith(("ml-", "predictive")) else "ml-engine",
                        f"{name}-rs")]["metadata"]["uid"]


@pytest.mark.parametrize("what, why", [
    ("pod", "nginx-test still has pods"),
    ("writer", "writer remains"),
    ("job", "legacy Jobs appeared"),
    ("scaledobject", "nginx-test-keda-fallback"),
    ("unsuspend", "CronJob ml-training is not suspended"),
])
def test_post_parking_verify_fails_on_anything_coming_back(tmp_path, what, why):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    full(api, rec, clock)
    n = {"i": 0}

    def hook(a):
        n["i"] += 1
        if n["i"] != 300:
            return
        if what == "pod":                           # a reconciler recreates a target pod
            a.add("pods", {"kind": "Pod", "metadata": {"name": "nginx-test-back", "namespace": "demo",
                                                       "ownerReferences": [{"kind": "ReplicaSet",
                                                                            "uid": _rs_uid(a, "nginx-test")}]},
                           "status": {"phase": "Running"}})
        elif what == "writer":
            a.add("horizontalpodautoscalers", {"kind": "HorizontalPodAutoscaler", "metadata": {
                "name": "late", "namespace": "demo"}, "spec": {"scaleTargetRef": {"kind": "Deployment",
                                                                                   "name": "nginx-test"}}})
        elif what == "job":                         # even a Job that already finished is a recreation
            a.add("jobs", {"kind": "Job", "metadata": {"name": "ml-training-late", "namespace": "ml-engine",
                                                       "ownerReferences": [{"kind": "CronJob",
                                                                            "name": "ml-training"}]},
                           "status": {"succeeded": 1, "completionTime": "2026-11-08T01:00:00Z"}})
        elif what == "scaledobject":
            a.add("scaledobjects", {"kind": "ScaledObject", "metadata": {"name": "nginx-test-keda-fallback",
                                                                         "namespace": "demo"},
                                    "spec": {"scaleTargetRef": {"name": "nginx-test"}}})
        else:
            a.objects[("cronjobs", "ml-engine", "ml-training")]["spec"]["suspend"] = False
    api.on_poll.append(hook)
    v = LG.verify(api, rec, 600, clock.sleep, clock, clock)
    assert v["retired"] is False and any(why in p for p in v["problems"]), v["problems"]


def test_verify_refuses_a_recreated_target(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    full(api, rec, clock)
    old = api.objects.pop(("deployments", "demo", "nginx-seasonal"))
    api.add("deployments", dict(old, metadata={k: v for k, v in old["metadata"].items() if k != "uid"}))
    v = LG.verify(api, rec, 600, clock.sleep, clock, clock)
    assert v["retired"] is False and any("not the Deployment phase A captured" in p for p in v["problems"])


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
    problems = LG.arrival_problems(Stub(rate, age), ARRIVALS, {"nginx-test": ["p1"]})
    assert problems and "nginx-test" in problems[0]
    assert LG.arrival_problems(Stub(0.0, 0.0), ARRIVALS, {"nginx-test": ["p1"]}) == []


class Stub:
    def __init__(self, rate, age, series=None):
        self.rate, self.age, self.series = rate, age, series

    def __call__(self, q):
        return self.age if q.startswith("time()") else self.rate

    def vector(self, q, label):
        return {"p1": self.age} if self.series is None else self.series


@pytest.mark.parametrize("series, pods, ok", [
    ({"p1": 5.0, "p2": 7.0}, ["p1", "p2"], True),
    ({"p1": 5.0, "p2": 700.0}, ["p1", "p2"], False),            # one reporter stale, the newest fresh
    ({"p1": 5.0}, ["p1", "p2"], False),                         # one expected reporter has no series at all
    ({"p1": 5.0, "p2": -1.0}, ["p1", "p2"], False),             # from the future
    ({"p1": 5.0, "p2": float("nan")}, ["p1", "p2"], False),
    (None, ["p1"], False),                                      # no per-reporter observation: never a zero
    ({"p1": 5.0}, [], False),                                   # no expected reporter: cannot be observed
    ({"p1": 5.0, "other": 900.0}, ["p1"], True),                # a stale series of a pod not expected: irrelevant
])
def test_every_expected_reporter_must_be_fresh(series, pods, ok):
    # Codex finding 3: max(timestamp(...)) proves only that the newest series is fresh.
    stub = Stub(0.0, 5.0, series)
    if series is None:
        stub.vector = lambda q, label: None
    problems = LG.arrival_problems(stub, ARRIVALS, {"nginx-test": pods})
    assert (problems == []) is ok, problems


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
    park(api, rec, clock)
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
    full(api2, rec2, clock2)
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
    park(api2, rec2, clock2)
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


# --- C2.8n: two reviewed phases (Codex review of plan-ec517af4a71d) ------------------------------------------------

def journal(rec):
    return C.read_receipts(C.receipts_path(rec, "legacy"))


def test_park_plan_needs_phase_a_completion(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    with pytest.raises(C.StagingError, match="no phase A completion receipt"):
        LG.build_park_plan(api, rec)
    # A failed phase A (arrivals still flowing) has no completion receipt either.
    with pytest.raises(C.StagingError, match="still receives"):
        quiesce(api, rec, clock, Metrics(rate=0.2))
    with pytest.raises(C.StagingError, match="no phase A completion receipt"):
        LG.build_park_plan(api, rec)
    assert api.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 3


def test_park_plan_refuses_a_forged_foreign_or_broken_chain(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    C.append_receipt(rec, "legacy", "quiesce-complete", {"phase": "quiesce", "targets": [],
                                                         "observed_receipt_sha256": "0" * 64}, clock)
    with pytest.raises(C.StagingError, match="does not bind its observation"):
        LG.build_park_plan(api, rec)                                         # forged: no observation in the chain
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b"), Clock()
    quiesce(api2, rec2, clock2)
    C.append_receipt(dict(rec2, run_id="c28-other"), "legacy", "note", {}, clock2)    # another run's receipt
    with pytest.raises(C.StagingError, match="another run"):
        LG.build_park_plan(api2, rec2)
    api3, rec3, clock3 = legacy_cluster(), ready_record(tmp_path / "c"), Clock()
    quiesce(api3, rec3, clock3)
    path = C.receipts_path(rec3, "legacy")
    lines = open(path).read().splitlines()
    tampered = json.loads(lines[-1])
    tampered["data"]["targets"][0]["generation"] = 99                       # edit the captured state
    lines[-1] = json.dumps(tampered, sort_keys=True)
    open(path, "w").write("\n".join(lines) + "\n")
    with pytest.raises(C.StagingError, match="breaks the hash chain"):
        LG.build_park_plan(api3, rec3)
    assert api3.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 3


def test_park_plan_refuses_a_target_changed_after_phase_a(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    quiesce(api, rec, clock)
    d = api.objects[("deployments", "demo", "nginx-reactive")]
    d["spec"]["replicas"], d["metadata"]["generation"] = 5, d["metadata"]["generation"] + 1   # someone still writes
    with pytest.raises(C.StagingError, match="nginx-reactive changed after phase A's captured state"):
        LG.build_park_plan(api, rec)
    # And a change after the park plan was reviewed is refused at execution, before that target is touched.
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b"), Clock()
    quiesce(api2, rec2, clock2)
    pplan = LG.build_park_plan(api2, rec2)
    d2 = api2.objects[("deployments", "demo", "nginx-reactive")]
    d2["metadata"]["generation"] += 1
    with pytest.raises(C.StagingError, match="nginx-reactive changed"):
        LG.park(api2, rec2, pplan, C.plan_digest(pplan), sleep=clock2.sleep, monotonic=clock2, clock=clock2)
    assert d2["spec"]["replicas"] == 3
    assert api2.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] == 0       # parked before it
    assert api2.objects[("deployments", "ml-engine", "ml-api")]["spec"]["replicas"] == 1


def test_target_drift_before_the_operator_stops_no_longer_breaks_s4(tmp_path):
    # The case that used to break: the old operator rescales a target during phase A, before it stops; the single
    # plan's parking then refused the planned generation. Now phase A captures the stable state after the stop.
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    d = api.objects[("deployments", "demo", "nginx-test")]
    g0, writes = d["metadata"]["generation"], {"n": 0}

    def operator_writes(a):
        op = a.objects[("deployments", "ml-engine", "predictive-operator")]
        if op["spec"]["replicas"] and a.objects[("deployments", "demo", "k6-nginx-test")]["spec"]["replicas"] == 0 \
                and writes["n"] < 3:
            writes["n"] += 1
            d["spec"]["replicas"] += 1
            d["metadata"]["generation"] += 1
            d["metadata"]["resourceVersion"] = a._next_rv()
            a.sync_pods(d)
    api.on_poll.append(operator_writes)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert writes["n"] == 3 and d["metadata"]["generation"] == g0 + 3
    cap = {t["name"]: t for t in journal(rec)[-1]["data"]["targets"]}
    assert cap["nginx-test"]["generation"] == g0 + 3 and cap["nginx-test"]["replicas"] == 6
    pplan = park(api, rec, clock)
    scale = next(a for a in pplan["actions"] if a.get("name") == "nginx-test")
    assert (scale["generation"], scale["before"]) == (g0 + 3, 6)
    assert d["spec"]["replicas"] == 0
    assert LG.verify(api, rec, 600, clock.sleep, clock, clock)["retired"] is True


def test_park_rechecks_quiescence_and_arrivals_right_before_parking(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    quiesce(api, rec, clock)
    pplan = LG.build_park_plan(api, rec)
    with pytest.raises(C.StagingError, match="still receives"):              # traffic resumed after phase A
        LG.park(api, rec, pplan, C.plan_digest(pplan), sleep=clock.sleep, monotonic=clock, clock=clock,
                metrics=Metrics(rate=1.0))
    api.objects[("deployments", "demo", "k6-myapptwo")]["spec"]["replicas"] = 1   # a generator came back
    with pytest.raises(C.StagingError, match="park plan refused.*k6-myapptwo is not stopped"):
        LG.build_park_plan(api, rec)
    assert all(api.objects[("deployments", "demo", a)]["spec"]["replicas"] == 3 for a in LG.L.ARMS)


def test_a_partial_park_resumes_from_its_receipts(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    quiesce(api, rec, clock)
    pplan = LG.build_park_plan(api, rec)
    real = api.replace

    def racing(path, body):
        if path.endswith("/deployments/myapptwo/scale"):                    # a status update races the PUT
            api.objects[("deployments", "demo", "myapptwo")]["metadata"]["resourceVersion"] = api._next_rv()
        return real(path, body)
    api.replace = racing
    with pytest.raises(C.ApiError, match="conflict"):
        LG.park(api, rec, pplan, C.plan_digest(pplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.replace = real
    lock = C.lock_path(rec, "legacy")
    crash_owner(lock)
    assert list(LG.recover(api, rec, C.sha256_file(lock), clock).values()) == ["not-done"]
    again = LG.build_park_plan(api, rec)
    assert again["already_parked"] == ["nginx-test", "nginx-reactive"]
    assert {a.get("name") for a in again["actions"] if a["op"] == "scale"} == \
        {"myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95", "ml-api"}
    LG.park(api, rec, again, C.plan_digest(again), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert all(api.objects[("deployments", "demo", a)]["spec"]["replicas"] == 0 for a in LG.L.ARMS)
    with pytest.raises(C.StagingError, match="already completed"):
        LG.build_park_plan(api, rec)
    assert LG.verify(api, rec, 600, clock.sleep, clock, clock)["retired"] is True


def test_restore_undoes_both_phases_newest_first(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    before = {k: (v["spec"].get("replicas"), v["spec"].get("suspend")) for k, v in api.objects.items()
              if k[0] in ("deployments", "cronjobs")}
    full(api, rec, clock)
    rplan = LG.build_restore_plan(api, rec)
    undo = [a["undoes"] for a in rplan["actions"] if "undoes" in a]
    assert [int(u) for u in undo] == sorted((int(u) for u in undo), reverse=True)
    names = [a.get("name") for a in rplan["actions"] if "undoes" in a]
    assert names.index("ml-api") < names.index("nginx-test") < names.index("myapptwo-keda-fallback") < \
        names.index("predictive-operator") < names.index("k6-nginx-test") < names.index("ml-training")
    LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    after = {k: (v["spec"].get("replicas"), v["spec"].get("suspend")) for k, v in api.objects.items()
             if k[0] in ("deployments", "cronjobs")}
    assert after == before
    with pytest.raises(C.StagingError, match="park plan refused"):
        LG.build_park_plan(api, rec)                                        # restored after phase A: re-run it


def test_restore_after_phase_a_alone_touches_no_target(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    quiesce(api, rec, clock)
    rplan = LG.build_restore_plan(api, rec)
    assert not any(a.get("name") in LG.L.ARMS + ("ml-api",) for a in rplan["actions"])
    LG.restore(api, rec, rplan, C.plan_digest(rplan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.objects[("deployments", "demo", "k6-nginx-test")]["spec"]["replicas"] == 1
    with pytest.raises(C.StagingError, match="a restore ran after phase A"):
        LG.build_park_plan(api, rec)


def test_a_stale_reporter_blocks_phase_a_even_when_the_newest_series_is_fresh(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    m = Metrics(api=api, stale={"nginx-seasonal"})
    with pytest.raises(C.StagingError, match="nginx-seasonal's reporter pods"):
        quiesce(api, rec, clock, m)
    assert any("by (pod)" in q and 'destination_workload="nginx-seasonal"' in q for q in m.queries)
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b"), Clock()
    quiesce(api2, rec2, clock2, Metrics(api=api2))                          # every reporter fresh: passes
    obs = next(r for r in journal(rec2) if r["event"].startswith("observed-"))
    assert {t: len(p) for t, p in obs["data"]["baseline"]["reporters"].items()} == {a: 3 for a in LG.L.ARMS}


def test_the_settle_period_precedes_an_uninterrupted_observation(tmp_path):
    def run(tmp, settle, busy_for, late=None):
        api, rec, clock = legacy_cluster(), ready_record(tmp, arrivals=dict(ARRIVALS, settle_seconds=settle)), Clock()
        stop = {}
        real_delete = api.delete

        def delete(path, uid, rv=None):
            stop["t"] = clock.t                                             # the last writer stops
            return real_delete(path, uid, rv)
        api.delete = delete

        def rate():
            since = clock.t - stop.get("t", float("inf"))
            return 0.3 if since < busy_for or (late and late[0] <= since < late[1]) else 0.0
        quiesce(api, rec, clock, Metrics(rate_fn=rate))
        return rec
    rec = run(tmp_path / "a", 90, 80)                                       # requests drain inside the settle
    settled = next(r for r in journal(rec) if r["event"].startswith("settled-"))
    assert settled["data"]["seconds"] == 90 and settled["data"]["waited"] >= 90
    with pytest.raises(C.StagingError, match="still receives"):
        run(tmp_path / "b", 60, 80)                                         # a shorter settle: inside the window
    with pytest.raises(C.StagingError, match="still receives"):
        run(tmp_path / "c", 90, 0, late=(400, 410))                         # a request mid-observation


@pytest.mark.parametrize("change, why", [
    (dict(age_query='time() - max(timestamp(istio_requests_total{destination_workload="{target}"}))'),
     "destination_workload_namespace"),
    (dict(age_query='time() - max(timestamp(istio_requests_total{destination_workload="{target}",'
                    'destination_workload_namespace="shop",reporter="destination"}))'), "namespace"),
    (dict(series_query='time() - max by (pod) (timestamp(istio_requests_total{destination_workload="{target}",'
                       'destination_workload_namespace="demo"}))'), "same label selector"),
    (dict(series_query=f"time() - max(timestamp({SEL}))"), "group by"),
    (dict(series_query=f"time() - max by (instance) (timestamp({SEL}))"), "group by"),
    (dict(pod_label=""), "pod_label"),
    (dict(pod_label=None), "pod_label"),
    (dict(settle_seconds=30), "settle_seconds"),
    (dict(settle_seconds=601), "settle_seconds"),
    (dict(settle_seconds=True), "settle_seconds"),
    (dict(rate_query=f"sum(rate({SEL}[2m]))"), "settle_seconds"),             # 90 s < the 2-minute window
    (dict(rate_query=f"sum({SEL})"), "range window"),
    (dict(rate_query='sum(rate(istio_requests_total{destination_workload="{target}"}[1m]))'), "namespace"),
])
def test_the_arrivals_block_is_validated(change, why):
    arr = {k: v for k, v in dict(ARRIVALS, **change).items() if v is not None}
    with pytest.raises(C.StagingError, match=why):
        LG._arrivals(arr)


def test_the_real_record_block_validates():
    # The block for c28-staging/record-s4-legacy.json (labels checked live on prodcluster 2026-11-08 ~14:20Z).
    sel = ('istio_requests_total{destination_workload="{target}",destination_workload_namespace="demo",'
           'reporter="destination"}')
    real = {"query_api": "http://127.0.0.1:18481/select/0/prometheus",
            "rate_query": f"sum(rate({sel}[1m]))",
            "age_query": f"time() - max(timestamp({sel}))",
            "series_query": f"time() - max by (pod) (timestamp({sel}))",
            "pod_label": "pod", "max_age_seconds": 120, "settle_seconds": 90}
    assert LG._arrivals(real)["settle_seconds"] == 90
    assert LG._arrivals({k: v for k, v in real.items() if k != "settle_seconds"})["settle_seconds"] == 90


def test_the_metrics_client_reads_one_series_per_reporter():
    import io

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def client(result, status="success"):
        body = {"status": status, "data": {"resultType": "vector", "result": result}}
        return REAL_PROM("http://vm", opener=lambda url, timeout: Resp(json.dumps(body).encode()), now=lambda: 1000.0)

    def s(pod, v, ts=1000):
        return {"metric": {"pod": pod} if pod else {}, "value": [ts, str(v)]}
    assert client([s("a", 3), s("b", 25)]).vector("q", "pod") == {"a": 3.0, "b": 25.0}
    assert client([]).vector("q", "pod") == {}                               # no series: every reporter missing
    assert client([s("a", 3), s("a", 4)]).vector("q", "pod") is None         # duplicated: ambiguous
    assert client([s(None, 3)]).vector("q", "pod") is None                   # unlabelled
    assert client([s("a", "NaN")]).vector("q", "pod") is None
    assert client([s("a", 3, ts=100)]).vector("q", "pod") is None            # not evaluated now
    assert client([s("a", 3)], status="error").vector("q", "pod") is None


def test_the_cli_runs_both_phases_and_verify(tmp_path, capsys):
    api, rec = legacy_cluster(), ready_record(tmp_path)
    path = tmp_path / "rec.json"
    path.write_text(json.dumps(rec))
    clock = Clock()

    def plan_of(cmd):
        assert LG.main(["--record", str(path), cmd], api=api) == 0
        out = capsys.readouterr().out
        return out.split(f"{cmd}: ")[1].split("\n")[0], out.strip().split()[-1]
    assert LG.main(["--record", str(path), "park-plan"], api=api) == 1         # no phase A yet
    f, d = plan_of("plan")
    assert LG.main(["--record", str(path), "quiesce", "--plan", f, "--plan-digest", d], api=api,
                   sleep=clock.sleep, monotonic=clock, clock=clock) == 0
    assert LG.main(["--record", str(path), "park", "--plan", f, "--plan-digest", d], api=api,
                   sleep=clock.sleep, monotonic=clock, clock=clock) == 1      # a phase A plan is not a park plan
    f2, d2 = plan_of("park-plan")
    assert os.path.basename(f2).startswith("park-plan-")
    assert LG.main(["--record", str(path), "park", "--plan", f2, "--plan-digest", d2], api=api,
                   sleep=clock.sleep, monotonic=clock, clock=clock) == 0
    capsys.readouterr()
    assert LG.main(["--record", str(path), "verify"], api=api, sleep=clock.sleep, monotonic=clock, clock=clock) == 0
    assert json.loads(capsys.readouterr().out)["retired"] is True
