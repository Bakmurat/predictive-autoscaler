"""bounded.py, the admission run's lifecycle on the in-memory API: plan refusals and the shared fence (both ways), a
complete run (journalled creates, observation, evidence written before cleanup, the verdict from the written file, the
pod proven gone before the ConfigMap goes), a stop, a VOID start and an unproven disappearance that keeps the lock.
The verdict's criteria are tested in the harness (test_demo_admission)."""

import json
import os
import subprocess
from datetime import datetime, timezone

import pytest

import bounded as B
import execlib as X
import fake2 as F
import prepare
from part2_fixtures import api  # noqa: F401
from test_prepare_demo import DEMO, N, R, run_until, world  # noqa: F401

ORIGIN = "2026-11-11T00:05:00Z"
O_MS = int(datetime(2026, 11, 11, 0, 5, tzinfo=timezone.utc).timestamp() * 1000)


class Clock:
    def __init__(self, ms):
        self.ms = ms

    def now(self):
        return self.ms

    def sleep(self, s):
        self.ms += s * 1000


def ready_world(world):  # noqa: F811
    record, kube = world()
    run_until(record, kube, "pas")
    a = world.api
    for n in ("worker-w", "worker-s"):                    # a real node reports all three pressure conditions
        conds = a.obj("Node", n).setdefault("status", {}).setdefault("conditions", [])
        if not any(x["type"] == "PIDPressure" for x in conds):
            conds.append({"type": "PIDPressure", "status": "False"})
    for name in (N, R):
        dep = a.obj("Deployment", name, "demo")
        dep["status"] = {"replicas": 12, "readyReplicas": 12, "updatedReplicas": 12}
        uids = []
        for i in range(12):
            p = F.pod(f"{name}-{i:02d}", "demo", "worker-w", labels={"c28.devkuban.com/target": name, X.RUN_LABEL: DEMO})
            p["status"]["conditions"] = [{"type": "Ready", "status": "True", "lastTransitionTime": "2026-11-10T00:00:00Z"}]
            p["status"]["containerStatuses"] = [{"name": "fixture", "containerID": f"c-{name}-{i}", "restartCount": 0}]
            a.put(p)
            uids.append(a.obj("Pod", p["metadata"]["name"], "demo")["metadata"]["uid"])
        a.put({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
               "metadata": {"name": f"{name}-x", "namespace": "demo", "labels": {"kubernetes.io/service-name": name}},
               "endpoints": [{"conditions": {"ready": True}, "targetRef": {"uid": u}} for u in uids]})
    return record, kube, a


class JobApi(F.FakeApi2):
    """A created Job gets one Running pod it owns; the Job completes (pod exit 0) once the clock passes its end."""

    def __init__(self, base, clock, end_ms, sticky=False, never_running=False):
        self.__dict__.update(base.__dict__)
        self.clock, self.end_ms, self.sticky, self.never_running = clock, end_ms, sticky, never_running

    def create(self, path, body):
        out = super().create(path, body)
        m = body["metadata"]
        if body["kind"] == "Job":
            job = self.obj("Job", m["name"], m["namespace"])
            job["metadata"]["creationTimestamp"] = datetime.fromtimestamp(self.clock.ms / 1000, timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            pod = F.pod(m["name"] + "-abcde", m["namespace"], "worker-s",
                        labels=dict(body["spec"]["template"]["metadata"]["labels"]),
                        phase="Pending" if self.never_running else "Running")
            pod["metadata"]["ownerReferences"] = [{"kind": "Job", "uid": job["metadata"]["uid"], "controller": True}]
            pod["status"]["containerStatuses"] = [{"name": "k6", "state": {"running": {}}}]
            self.put(pod)
        return out

    fail_pod_list_at = None

    def get(self, path):
        if self.fail_pod_list_at and path.endswith("/pods") and self.clock.ms >= self.fail_pod_list_at:
            self.fail_pod_list_at = None
            raise X.C.ApiError("list pods: failed (error, exit 1)", 500)
        o = super().get(path)
        if o and o.get("kind") == "Job" and self.clock.ms >= self.end_ms:
            o.setdefault("status", {})["conditions"] = [{"type": "Complete", "status": "True"}]
            for p in self.objects.values():
                if any(r.get("uid") == o["metadata"]["uid"] for r in p["metadata"].get("ownerReferences") or []):
                    p["status"]["phase"] = "Succeeded"
                    p["status"]["containerStatuses"] = [{"name": "k6", "state": {"terminated": {"exitCode": 0}}}]
        return o

    def delete(self, path, uid, resource_version=None):
        if self.sticky and "/pods/" in path:
            self.calls.append(("delete", path))
            return {}
        if "/jobs/" in path:                  # the raw API's default for a Job: its pod is ORPHANED, not collected
            self.calls.append(("delete", path))
            kind, ns, name, _ = self._parse(path)
            o = self.objects.get((kind, ns, name))
            if o is None or o["metadata"]["uid"] != uid:
                return super().delete(path, uid, resource_version)
            del self.objects[(kind, ns, name)]
            return {}
        return super().delete(path, uid, resource_version)


INSTANCES = {"worker-w": "10.0.0.1:9100", "worker-s": "10.0.0.2:9100"}


class FakeVM:
    """The observer, answering every query bounded.py sends from a SIMULATED run of the real load script (its ingress
    counter samples) and steady synthetic series (15 s scrapes) for everything else: steal 1 %, no alert, throttling
    0.01, a queue of 1, equal per-pod request increases. Every selector is VALIDATED: an unknown query raises (an
    observer refusal), so a wrong label (e.g. nodename on node_cpu_seconds_total) never silently returns data."""

    def __init__(self, sim_ingress, pod_names, steal=1.0, alerts=()):
        self.ingress, self.pod_names, self.steal, self.alerts = sim_ingress, pod_names, steal, list(alerts)
        self.queries = []

    def series(self, lo, hi, step, value, metric=None):
        return {"metric": metric or {}, "values": [[t, str(value)] for t in range(int(-(-lo // step) * step),
                                                                                  int(hi) + 1, step)]}

    def __call__(self, url):
        import re
        import urllib.parse as U
        u = U.urlparse(url)
        q = dict(U.parse_qsl(u.query))
        expr = q["query"]
        self.queries.append(expr)
        res = []
        if u.path.endswith("query_range"):
            lo, hi, step = float(q["start"]), float(q["end"]), int(q["step"][:-1])
            if "mode=\"steal\"" in expr and "node_uname_info" in expr:
                res = [self.series(lo, hi, step, self.steal)]
            elif "container_cpu_cfs_throttled_periods_total" in expr:
                res = [self.series(lo, hi, step, 0.01)]
            elif expr.startswith("sum(c28_queue{"):
                res = [self.series(lo, hi, step, 1.0)]
            else:
                raise ValueError(f"FakeVM: unknown range query {expr}")
        elif expr.startswith("ALERTS"):
            res = [{"metric": a, "value": [0, "1"]} for a in self.alerts]
        elif expr.startswith("node_uname_info{"):
            res = [{"metric": {"nodename": n, "instance": i}, "value": [0, "1"]} for n, i in INSTANCES.items()]
        elif expr.startswith("sum by (pod) (increase(c28_requests_total"):
            t = expr.split('target="')[1].split('"')[0]
            res = [{"metric": {"pod": n}, "value": [0, "1000"]} for n in self.pod_names[t]]
        else:                                               # a raw range-vector query: SELECTOR[Ns] at time
            sel, rng = expr.rsplit("[", 1)
            hi = float(q["time"])
            lo = hi - float(rng.rstrip("s]"))
            known = (r'c28_(ingress_requests_total|queue|requests_total)\{namespace="demo",target="[a-z0-9-]+"\}$',
                     r'container_cpu_cfs_(throttled_)?periods_total\{namespace="demo",pod=~"[^"]+",container!=""\}$',
                     r'node_cpu_seconds_total\{job="node-exporter",mode="steal",instance="(%s)"\}$'
                     % "|".join(re.escape(i) for i in INSTANCES.values()))
            if not any(re.match(k, sel) for k in known):
                raise ValueError(f"FakeVM: unknown raw selector {sel}")
            if sel.startswith("c28_ingress_requests_total"):
                t = expr.split('target="')[1].split('"')[0]
                res = [{"metric": {}, "values": [[a, str(b)] for a, b in self.ingress[t] if lo < a <= hi]}]
            else:
                res = [self.series(lo, hi, 15, 1.0)]
        return {"status": "success", "data": {"result": res}}


def simulated(params, label, origin_ms, stalls=()):
    """The generator configuration and the REAL script's output for one bounded run (harness simulator)."""
    import sys
    import tempfile
    import pathlib
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                                    "harness", "tests"))
    from demo_helpers import MIN, scrape, simulate
    c28 = X.import_harness()
    cfg = c28.demo.bounded_config(params, label, origin_ms)
    run_min = 40 if label == "admission" else 15
    out = simulate(pathlib.Path(tempfile.mkdtemp()), cfg, {
        "incarnations": [{"pod_uid": "pod-gen", "start_ms": origin_ms - 2 * MIN, "stalls": list(stalls)}],
        "until_ms": origin_ms + (run_min + 5) * MIN, "ingress_initial": 100,
        "scrape": scrape(origin_ms - 5 * MIN, origin_ms + (run_min + 5) * MIN)})
    return cfg, [json.dumps(l) for l in out["lines"]], out["ingress"]


def plan_now(record, kube, clock):
    c = B.Ctx(record, kube, now=clock.now, sleep=clock.sleep)
    p = B.plan_admission(c, ORIGIN)
    saved = X.save_plan(record, B.HELPER, p, "plan-admission")
    return c, X.load_plan(saved["plan"]), saved["plan_digest"]


def test_a_plan_binds_the_objects_and_passes_on_a_ready_world(world):  # noqa: F811
    record, kube, a = ready_world(world)
    clock = Clock(O_MS - 180_000)
    c, p, d = plan_now(record, kube, clock)
    assert p["problems"] == [], p["problems"]
    assert [x["create"] for x in p["actions"]] == ["ConfigMap", "Job"] and p["deadline_s"] == 300 + 2400 + 180
    assert set(p["targets"]) == {N, R} and all(v["pa_uid"] for v in p["targets"].values())


@pytest.mark.parametrize("edit, text", [
    (lambda a: a.put({"apiVersion": "apps/v1", "kind": "StatefulSet",
                      "metadata": {"name": "c28-demo-generator", "namespace": "demo",
                                   "labels": {"app.kubernetes.io/name": "c28-demo-generator"}}, "spec": {}}),
     "one generator-like workload at a time"),
    (lambda a: a.put(dict(F.pod("c28-demo-startup-x", "demo", "worker-s",
                                labels={"app.kubernetes.io/name": "c28-demo-startup"}),
                          metadata={"name": "c28-demo-startup-x", "namespace": "demo",
                                    "labels": {"app.kubernetes.io/name": "c28-demo-startup"},
                                    "deletionTimestamp": "2026-11-11T00:00:00Z"})), "(Terminating)"),
    (lambda a: a.obj("Deployment", N, "demo")["status"].update(readyReplicas=11), "is not at 12 Ready"),
    (lambda a: a.obj("PredictiveAutoscaler", R, "demo")["spec"].update(mode="Active"), "not Recommend"),
])
def test_the_plan_refuses_a_fence_or_a_target_not_as_reviewed(world, edit, text):  # noqa: F811
    record, kube, a = ready_world(world)
    edit(a)
    _, p, _ = plan_now(record, kube, Clock(O_MS - 180_000))
    assert any(text in x for x in p["problems"]), p["problems"]


def test_the_origin_must_be_two_to_five_minutes_ahead(world):  # noqa: F811
    record, kube, a = ready_world(world)
    _, p, _ = plan_now(record, kube, Clock(O_MS - 60_000))
    assert any("must be 120..300" in x for x in p["problems"])


def test_prepares_generator_phase_refuses_while_a_bounded_workload_exists(world):  # noqa: F811
    record, kube, a = ready_world(world)
    a.put(F.pod("c28-demo-admission-x", "demo", "worker-s", labels={"app.kubernetes.io/name": "c28-demo-admission"}))
    probs = prepare.plan(record, kube, "generator", now=lambda: "2026-11-11T00:00:00Z")["problems"]
    assert any("a bounded admission workload exists" in x for x in probs)


def _run(world, steal=1.0, alerts=(), **kw):  # noqa: F811
    record, kube, base = ready_world(world)
    clock = Clock(O_MS - 180_000)
    a = JobApi(base, clock, O_MS + 40 * 60_000 + 60_000, **kw)
    kube = record.kube(api=a)
    c, p, d = plan_now(record, kube, clock)
    assert p["problems"] == [], p["problems"]
    _, lines, ingress = simulated(c.params, "admission", O_MS)
    names = {t: [f"{t}-{i:02d}" for i in range(12)] for t in (N, R)}
    vm = FakeVM(ingress, names, steal=steal, alerts=alerts)
    logs = lambda c_, pod: lines
    return record, kube, a, clock, c, p, d, logs, vm


def test_a_complete_run_writes_evidence_before_cleanup_and_proves_the_pod_gone(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    out = B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    v = json.load(open(os.path.join(out["data"]["dir"], "verdict.json")))
    assert out["data"]["verdict"] == "PASS", (v["reasons"], {k: v["criteria"][k] for k in v["reasons"]})
    assert not out["data"]["stopped"]
    ops = [(m["op"], m["kind"]) for m in kube.mutations]
    assert ops == [("create", "ConfigMap"), ("create", "Job"), ("delete", "Job"), ("delete", "Pod"),
                   (B.GONE_OP, "Pod"), ("delete", "ConfigMap")]
    ev = json.load(open(os.path.join(out["data"]["dir"], "evidence.json")))
    assert ev["job"] == {"status": "Complete", "exit_code": 0, "created_s": ev["job"]["created_s"]}
    assert len(ev["pods"]["expected"][N]) == 12 and ev["endpoints"] and ev["pods"]["watch"]
    v = json.load(open(os.path.join(out["data"]["dir"], "verdict.json")))
    assert v["verdict"] == out["data"]["verdict"]
    assert not os.path.exists(X.lock_file(record)) and not X.open_intents(X.receipts(record, B.HELPER))


def _gen_pods(a):
    return [v for k, v in a.objects.items() if k[0] == "pods" and v["metadata"]["name"].startswith("c28-demo-admission-")]


@pytest.mark.parametrize("kw, text", [({"steal": 9.0}, "steal above 5 %"),
                                      ({"alerts": [{"alertname": "KubeNodeNotReady", "node": "worker-s",
                                                    "severity": "warning"}]}, "alert KubeNodeNotReady"),
                                      ({"alerts": [{"alertname": "TargetDown", "instance": "10.0.0.2:10250",
                                                    "severity": "warning"}]}, "alert TargetDown"),
                                      ({"alerts": [{"alertname": "C28Anything", "node": "worker-s"}]}, "alert C28Anything"),
                                      ({"alerts": [{"alertname": "NodeFilesystemAlmostFull", "node": "worker-x",
                                                    "severity": "warning"}]}, None)])
def test_live_stops_end_the_load_before_any_evidence_is_collected(world, kw, text):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world, **kw)
    seen = []
    inner = vm.__call__

    class Probe:
        def __call__(self, url):
            if "c28_ingress_requests_total" in url:                   # the collection's first query
                seen.append(len(_gen_pods(a)))
            return inner(url)
    out = B.run_admission(c, p, d, Probe(), "http://vm", logs=logs)
    if text is None:                                                  # another node's alert does not stop it
        assert out["data"]["verdict"] == "PASS" and not out["data"]["stopped"]
        return
    assert text in out["data"]["stopped"] and out["data"]["verdict"] == "STOPPED"
    assert seen and all(n == 0 for n in seen)                         # the generator pod was gone before collection


def test_stop_load_acts_on_recorded_identities_only(world):  # noqa: F811
    record, kube, base = ready_world(world)
    c = B.Ctx(record, kube)
    job_uid = "11111111-0000-4000-8000-000000000001"
    orphan = F.pod("c28-demo-startup-zzzzz", "demo", "worker-s", labels={"app.kubernetes.io/name": "c28-demo-startup"})
    orphan["metadata"]["ownerReferences"] = [{"kind": "Job", "uid": job_uid, "controller": True}]
    base.put(orphan)
    with X.mutating(record, kube, B.HELPER, {"test": "stop"}):
        X.receipt(record, B.HELPER, "owned", {"run": "s-1", "kind": "Job", "name": "c28-demo-startup", "uid": job_uid})
        B.stop_load(c, "s-1")                                         # the Job is gone; its orphan is still found
    assert not _gen_pods(base) and not [v for v in base.objects.values()
                                        if v["metadata"]["name"] == "c28-demo-startup-zzzzz"]
    base.put({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "c28-demo-startup", "namespace": "demo"},
              "spec": {}})                                            # a REPLACEMENT under the recorded name
    with pytest.raises(X.StagingError, match="replacement"):
        with X.mutating(record, kube, B.HELPER, {"test": "stop2"}):
            B.stop_load(c, "s-1")
    assert base.obj("Job", "c28-demo-startup", "demo") is not None   # never deleted


def test_node_pressure_stops_the_run_and_keeps_the_partial_evidence(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    real_sleep = clock.sleep

    def sleep(s):
        real_sleep(s)
        if clock.ms > O_MS + 5 * 60_000:
            n = a.obj("Node", "worker-s")
            next(x for x in n["status"]["conditions"] if x["type"] == "MemoryPressure")["status"] = "True"
    c.sleep = sleep
    out = B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert out["data"]["verdict"] == "STOPPED" and "MemoryPressure" in out["data"]["stopped"]
    assert os.path.exists(os.path.join(out["data"]["dir"], "evidence.json"))
    assert ("delete", "Job") in [(m["op"], m["kind"]) for m in kube.mutations]


def test_a_pod_not_running_before_the_origin_is_void_stopped_and_cleaned_up(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world, never_running=True)
    out = B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert "VOID" in out["data"]["stopped"] and out["data"]["verdict"] != "PASS"
    assert os.path.exists(os.path.join(out["data"]["dir"], "evidence.json"))     # partial evidence kept
    ops = [(m["op"], m["kind"]) for m in kube.mutations]
    assert ops[:2] == [("create", "ConfigMap"), ("create", "Job")] and ("delete", "ConfigMap") == ops[-1]
    assert (B.GONE_OP, "Pod") in ops                                             # the pending pod proven gone too
    assert not os.path.exists(X.lock_file(record))


def test_an_unproven_pod_disappearance_keeps_the_lock(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world, sticky=True)
    with pytest.raises(X.StagingError, match="still present"):
        B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert os.path.exists(X.lock_file(record))
    assert [it["data"]["op"] for it in X.open_intents(X.receipts(record, B.HELPER))] == [B.GONE_OP]
    assert ("delete", "ConfigMap") not in [(m["op"], m["kind"]) for m in kube.mutations]


def test_a_fresh_process_of_another_helper_decides_the_bounded_gone_step(world, tmp_path):  # noqa: F811
    import subprocess
    import sys
    from test_ledger_read import FRESH
    record, kube, a, clock, c, p, d, logs, vm = _run(world, sticky=True)
    with pytest.raises(X.StagingError, match="still present"):
        B.run_admission(c, p, d, vm, "http://vm", logs=logs)

    def fresh():
        objs = tmp_path / "objects.json"
        objs.write_text(json.dumps([v for v in a.objects.values() if v.get("kind")]))
        r = subprocess.run([sys.executable, "-I", "-c", FRESH.replace('assert "ledger_read" not in sys.modules',
                                                                     'assert "bounded" not in sys.modules'),
                            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), record.path, str(objs)],
                           capture_output=True, text=True, timeout=120, env=dict(os.environ, KUBECONFIG="/dev/null"))
        assert r.returncode == 0, r.stderr[-2000:]
        return json.loads(r.stdout.strip().splitlines()[-1])
    assert [B.GONE_OP, "unresolved"] in fresh()
    for k in [k for k, v in a.objects.items() if v["metadata"]["name"].startswith("c28-demo-admission-")
              and v.get("kind") == "Pod"]:
        del a.objects[k]
    assert [B.GONE_OP, "done"] in fresh()


# --- the startup step ---------------------------------------------------------------------------------------------------

def _ts(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class StartupApi(JobApi):
    """A /scale write makes the target's labelled pods match the count at once: removed ones vanish, new ones are
    created now and become Ready 20 s later (the Ready condition's lastTransitionTime)."""

    fail_restore = False

    def _scale(self, o, body):
        want, name = body["spec"]["replicas"], o["metadata"]["name"]
        if self.fail_restore and want == 12:
            raise X.C.ApiError("scale refused", 403) if hasattr(X, "C") else Exception("scale refused")
        out = super()._scale(o, body)
        mine = sorted(k for k, v in self.objects.items() if k[0] == "pods"
                      and (v["metadata"].get("labels") or {}).get("c28.devkuban.com/target") == name)
        for k in mine[want:]:
            del self.objects[k]
        for i in range(len(mine), want):
            p = F.pod(f"{name}-n{i:02d}", "demo", "worker-w", labels={"c28.devkuban.com/target": name, X.RUN_LABEL: DEMO})
            p["metadata"]["creationTimestamp"] = _ts(self.clock.ms)
            p["status"]["conditions"] = [{"type": "PodScheduled", "status": "True", "lastTransitionTime": _ts(self.clock.ms)},
                                         {"type": "Ready", "status": "True",
                                          "lastTransitionTime": _ts(self.clock.ms + 20_000)}]
            p["status"]["containerStatuses"] = [{"name": "fixture", "restartCount": 0,
                                                 "state": {"running": {"startedAt": _ts(self.clock.ms + 5_000)}}}]
            self.put(p)
        return out


def _startup(world, admission=True, stalls=(), **kw):  # noqa: F811
    """A world whose admission really PASSED (a full simulated run), then a startup plan 3 minutes ahead."""
    record, kube, base = ready_world(world)
    clock = Clock(O_MS - 180_000)
    if admission:
        a1 = JobApi(base, clock, O_MS + 40 * 60_000 + 60_000)
        k1 = record.kube(api=a1)
        c1, p1, d1 = plan_now(record, k1, clock)
        _, lines, ingress = simulated(c1.params, "admission", O_MS)
        vm1 = FakeVM(ingress, {t: [f"{t}-{i:02d}" for i in range(12)] for t in (N, R)})
        assert B.run_admission(c1, p1, d1, vm1, "http://vm", logs=lambda c_, pod: lines)["data"]["verdict"] == "PASS"
    o2 = (clock.ms // 60_000 + 4) * 60_000
    a = StartupApi(base, clock, o2 + 15 * 60_000 + 40_000, **kw)
    kube = record.kube(api=a)
    c = B.Ctx(record, kube, now=clock.now, sleep=clock.sleep)
    p = B.plan_startup(c, _ts(o2))
    saved = X.save_plan(record, B.HELPER, p, "plan-startup")
    _, lines, ingress = simulated(c.params, "startup", o2,
                                  stalls=[{"at_ms": o2 + x[0], "for_ms": x[1]} for x in stalls])
    vm = FakeVM(ingress, {R: []})
    return record, kube, a, clock, c, X.load_plan(saved["plan"]), saved["plan_digest"], vm, lines


def run_s(c, p, d, vm, lines):
    return B.run_startup(c, p, d, vm, "http://vm", logs=lambda c_, pod: lines)


def test_startup_requires_a_bound_admission_pass(world):  # noqa: F811
    *_, p, d, vm, lines = _startup(world, admission=False)
    assert any("requires a bound admission PASS" in x for x in p["problems"])


def test_an_admission_pass_for_another_configuration_does_not_count(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    assert p["problems"] == [], p["problems"]
    r = X.last(X.receipts(record, B.HELPER), "run-complete")["data"]
    with open(os.path.join(r["dir"], "verdict.json"), "a") as fh:      # the verdict file altered after the run
        fh.write(" ")
    assert any("no longer hashes" in x for x in B.plan_startup(c, p["origin_utc"])["problems"])


def test_a_startup_step_measures_six_starts_under_proven_load_and_meets_its_obligation(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    assert p["problems"] == [], p["problems"]
    data = run_s(c, p, d, vm, lines)["data"]
    assert data["measured"] is True, data
    assert data["creation_to_ready_s"] == [20.0] * 6 and data["load_problems"] == []
    es = [e["event"] for e in X.receipts(record, B.HELPER)]
    assert es.index("restore-obligation") < max(i for i, e in enumerate(es) if e == "scale-write") \
        and "restore-closed" in es
    assert a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12
    assert not B.open_obligations(record) and not os.path.exists(X.lock_file(record))
    ev = json.load(open(os.path.join(data["dir"], "evidence.json")))
    assert ev["summary"]["polls"] > 10 and not ev["summary"]["readiness_lost"]   # observed throughout


def test_a_startup_without_load_records_is_never_a_measurement(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    data = run_s(c, p, d, vm, [])["data"]
    assert data["measured"] is False and data["load_problems"]
    assert a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12 and not B.open_obligations(record)


def test_a_void_startup_still_stops_the_load_and_restores_twelve(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world, never_running=True)
    data = run_s(c, p, d, vm, lines)["data"]
    assert "VOID" in data["stopped"] and data["measured"] is False
    assert a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12 and not B.open_obligations(record)
    assert not [k for k in a.objects if k[0] in ("jobs", "configmaps", "pods") and "startup" in k[2]]


def test_a_log_failure_still_stops_the_load_and_restores(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)

    def broken(c_, pod):
        raise subprocess.TimeoutExpired("kubectl logs", 120)
    data = B.run_startup(c, p, d, vm, "http://vm", logs=broken)["data"]
    assert data["measured"] is False
    assert a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12 and not B.open_obligations(record)
    assert not [k for k in a.objects if k[0] in ("jobs", "configmaps", "pods") and "startup" in k[2]]


def test_an_unmet_obligation_keeps_every_fence_until_restore(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world, never_running=True)
    a.fail_restore = True
    with pytest.raises(Exception):
        run_s(c, p, d, vm, lines)
    assert B.open_obligations(record) and a.obj("Deployment", R, "demo")["spec"]["replicas"] == 6
    assert any("restore obligation is open" in x for x in B.fence_problems(c))
    assert any("restore obligation is open" in x
               for x in prepare.plan(record, kube, "generator", now=lambda: "2026-11-11T00:00:00Z")["problems"])


def test_after_a_failed_restore_recover_then_restore_meets_the_obligation(world):  # noqa: F811
    import part2_fixtures as PF
    record, kube, a, clock, c, p, d, vm, lines = _startup(world, never_running=True)
    a.fail_restore = True
    with pytest.raises(Exception):
        run_s(c, p, d, vm, lines)
    a.fail_restore = False
    if os.path.exists(X.lock_file(record)):
        PF.crash_owner(record)
        rp = X.recover_plan(record, kube)
        saved = X.save_plan(record, "recover", rp, "recover-plan")
        X.recover(record, kube, X.load_plan(saved["plan"]), saved["plan_digest"])
    assert B.open_obligations(record)                                  # recover decides intents, never mutates
    rp = B.plan_restore(c)
    assert rp["problems"] == [] and rp["obligations"][0]["replicas"] == 6
    saved = X.save_plan(record, B.HELPER, rp, "plan-restore")
    B.restore(c, X.load_plan(saved["plan"]), saved["plan_digest"])
    assert not B.open_obligations(record) and a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12



# --- the second review's reproductions ----------------------------------------------------------------------------------

def test_an_interruption_between_create_and_its_receipt_still_stops_the_load(world, monkeypatch):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    real = B.own

    def flaky(c_, run, obj):
        if obj.get("kind") == "Job":
            raise RuntimeError("interrupted before the owned receipt")
        return real(c_, run, obj)
    monkeypatch.setattr(B, "own", flaky)
    with pytest.raises(RuntimeError):
        B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert a.obj("Job", "c28-demo-admission", "demo") is None and not _gen_pods(a)   # found through the journal
    assert not X.open_intents(X.receipts(record, B.HELPER))


def test_a_failed_pod_poll_stops_the_run(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    a.fail_pod_list_at = O_MS + 10 * 60_000
    out = B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert out["data"]["verdict"] == "STOPPED" and "observation failed" in out["data"]["stopped"]


def test_an_evidence_failure_never_blocks_the_restore(world, monkeypatch):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    real = B._write

    def broken(path, doc):
        if "startup-" in path:
            raise OSError("disk full")
        return real(path, doc)
    monkeypatch.setattr(B, "_write", broken)
    data = run_s(c, p, d, vm, lines)["data"]
    assert data["measured"] is False and data["evidence_sha256"] is None
    assert a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12 and not B.open_obligations(record)


def test_a_recovered_readiness_loss_is_seen_and_the_first_ready_kept(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    real = a.get

    def get(path):
        o = real(path)
        if path.endswith("/pods") and clock.ms > _ms_of(p["origin_utc"]) + 6 * 60_000:
            for x in (o or {}).get("items", []):
                if x["metadata"]["name"] == f"{R}-n06":
                    for cond in x["status"]["conditions"]:
                        if cond["type"] == "Ready":
                            cond["lastTransitionTime"] = _ts(clock.ms)   # flapped and came back: still True
        return o
    a.get = get
    data = run_s(c, p, d, vm, lines)["data"]
    ev = json.load(open(os.path.join(data["dir"], "evidence.json")))
    assert data["measured"] is False and ev["summary"]["readiness_lost"]
    assert data["creation_to_ready_s"] == [20.0] * 6                    # the FIRST Ready, not the recovery


def _ms_of(text):
    return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


def test_an_admission_pass_is_void_for_a_changed_live_workload(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    assert p["problems"] == []
    tpl = a.obj("Deployment", N, "demo")["spec"]["template"]["spec"]["containers"][0]
    tpl.setdefault("resources", {}).setdefault("limits", {})["memory"] = "16Mi"
    assert any("other images, configuration" in x for x in B.plan_startup(c, p["origin_utc"])["problems"])


def test_a_stalled_generator_is_not_the_scheduled_load(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world, stalls=[(3 * 60_000, 20_000)])
    data = run_s(c, p, d, vm, lines)["data"]
    assert data["measured"] is False
    assert any("zero generator drops" in x for x in data["load_problems"]), data["load_problems"]
