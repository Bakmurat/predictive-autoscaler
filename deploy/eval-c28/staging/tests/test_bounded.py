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
        a.obj("Node", n)["status"]["capacity"] = {"cpu": str(CPUS)}       # node-exporter: one steal series per CPU
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
    ing = F.pod(INGRESS_POD, "demo", "worker-w", labels={"app.kubernetes.io/name": "c28-ingress", X.RUN_LABEL: DEMO})
    ing["status"]["conditions"] = [{"type": "Ready", "status": "True", "lastTransitionTime": "2026-11-10T00:00:00Z"}]
    a.put(ing)
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
INGRESS_POD = "c28-ingress-7c9d-abcde"
CPUS = 4
POD_NAMES = {t: [f"{t}-{i:02d}" for i in range(12)] for t in ("c28-demo-pa-n", "c28-demo-pa-r")}


class FakeVM:
    """The observer, answering every query bounded.py sends from a SIMULATED run of the real load script (its ingress
    counter samples) and steady synthetic series (15 s scrapes) for everything else: steal 1 %, no alert, throttling
    0.01, a queue of 1, equal per-pod request increases. Every selector is VALIDATED: an unknown query raises (an
    observer refusal), so a wrong label (e.g. nodename on node_cpu_seconds_total) never silently returns data."""

    def __init__(self, sim_ingress, pod_names, steal=1.0, alerts=(), raw_steal=None, telemetry=None,
                 raw_steal_from_s=0.0):
        self.ingress, self.pod_names, self.steal, self.alerts = sim_ingress, pod_names, steal, list(alerts)
        self.queries = []
        # telemetry: {freshness key or "cfs/generator": mode} for the RAW series the telemetry gate reads. Modes:
        # "absent" no series; ("drop", pod) that pod's series are missing; ("extra", pod) one more pod's series;
        # "twice" every series a second time under another job (scraped twice); ("stale", seconds); ("short", n)
        # only the newest n samples; ("gap", seconds) a hole that long before the newest sample; "nan-value";
        # ("future", seconds); "refused". A callable mode(now_s) returns the mode in force at that time.
        self.telemetry = dict(telemetry or {})
        # raw_steal: {instance: mode} for the UNDERLYING steal samples of that node-exporter instance, while the
        # stepped range query (the bins) keeps answering as if all were well. Modes: ("stale", seconds) the newest
        # sample is that old; "absent" no series; "empty" a series without samples; "nan" / "text" / "short" a
        # malformed timestamp; ("future", seconds); "refused" the query itself fails.
        self.raw_steal = dict(raw_steal or {})
        # ... in force only for queries evaluated at or after raw_steal_from_s: a source that is lost DURING a run
        # (one that is already lost before it is refused by the telemetry gate, before anything is created).
        self.raw_steal_from_s = raw_steal_from_s

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
            if "mode=\"steal\"" in expr and "node_uname_info" in expr:      # steal: a value, or a function of the time
                res = [self.series(lo, hi, step, self.steal(hi) if callable(self.steal) else self.steal)]
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
            mode = next((m for i, m in self.raw_steal.items() if f'mode="steal",instance="{i}"' in sel
                         and hi >= self.raw_steal_from_s), None)
            tele = self._telemetry(sel, lo, hi)
            if tele is not None:
                if tele == "refused":
                    return {"status": "error", "error": "simulated refusal"}
                res = tele
            elif mode == "refused":
                return {"status": "error", "error": "simulated refusal"}
            elif mode == "absent":
                res = []
            elif mode == "empty":
                res = [{"metric": {}, "values": []}]
            elif mode in ("nan", "text", "short", "no-value", "text-value", "nan-value", "inf-value", "bool-value",
                          "bool-time", "three"):
                good = self.series(lo, hi, 15, 1.0)["values"]
                bad = {"nan": ["NaN", "1"], "text": ["soon", "1"], "short": [], "no-value": [hi], "text-value": [hi, "not-a-number"],
                       "nan-value": [hi, "NaN"], "inf-value": [hi, "+Inf"], "bool-value": [hi, True], "bool-time": [True, "1"],
                       "three": [hi, "1", "extra"]}[mode]
                res = [{"metric": {}, "values": good + [bad]}]          # fresh, well-formed samples beside one bad one
            elif isinstance(mode, tuple) and mode[0] == "stale":
                res = [self.series(lo, hi - mode[1], 15, 1.0)]
            elif isinstance(mode, tuple) and mode[0] == "future":
                res = [self.series(lo, hi + mode[1], 15, 1.0)]
            else:
                res = [self.series(lo, hi, 15, 1.0)]
            if tele is None and sel.startswith("node_cpu_seconds_total"):
                res = self._per_cpu(res, mode, hi)
        return {"status": "success", "data": {"result": res}}

    def _per_cpu(self, res, mode, hi):
        """node-exporter's shape: the steal counter once per CPU. CPU modes: "one-cpu" (a single series, as a
        one-CPU node or a broken relabeling would give), "cpu-twice", "cpu-extra", ("cpu-stale", seconds) for ONE
        CPU while the others stay fresh."""
        out = [dict(x, metric={**x["metric"], "cpu": str(i)}) for i in range(CPUS) for x in res]
        if mode == "one-cpu":
            return out[:1]
        if mode == "cpu-twice":
            return out + [dict(out[0], metric={**out[0]["metric"], "job": "other"})]
        if mode == "cpu-extra":
            return out + [dict(out[0], metric={"cpu": str(CPUS + 3)})]
        if isinstance(mode, tuple) and mode[0] == "cpu-stale":
            return out[:-1] + [dict(out[-1], values=[v for v in out[-1]["values"] if v[0] <= hi - mode[1]])]
        return out


def _telemetry_method(self, sel, lo, hi):
    """The RAW series of the fixture, the ingress and cAdvisor: one series per pod, as a scraper stores them."""
    import re
    if sel.startswith("c28_"):
        t = sel.split('target="')[1].split('"')[0]
        kind = {"c28_ingress_requests_total": "ingress", "c28_queue": "queue", "c28_requests_total": "requests"}[
            sel.split("{")[0]]
        key = f"{kind}/{t}"
        if kind == "ingress":
            samples = self.ingress.get(t) if self.ingress else None
            series = [{"metric": {"target": t, "pod": INGRESS_POD}, "values":
                       [[a, str(b)] for a, b in samples if lo < a <= hi]}] if samples is not None else \
                [self.series(lo, hi, 15, 100.0, {"target": t, "pod": INGRESS_POD})]
        else:
            extra = {"code": "200"} if kind == "requests" else {}
            series = [self.series(lo, hi, 15, 1.0, {"target": t, "pod": n, **extra}) for n in self.pod_names.get(t, [])]
    elif sel.startswith("container_cpu_cfs_"):
        rx = sel.split('pod=~"')[1].split('"')[0]
        who = "ingress" if rx.startswith("c28-ingress") else "fixture" if ".*" in rx else "generator"
        key = ("cfs_throttled/" if "throttled" in sel else "cfs_periods/") + who
        known = [n for ns in self.pod_names.values() for n in ns] + [INGRESS_POD]
        pods = [n for n in known if re.fullmatch(rx, n)] or ([rx] if who == "generator" else [])
        series = [self.series(lo, hi, 15, 1.0, {"pod": n, "container": "c"}) for n in pods]
    else:
        return None
    mode = self.telemetry.get(key)
    if callable(mode):
        mode = mode(hi)
    if mode is None:
        return series
    if mode == "refused":
        return "refused"
    if mode == "absent":
        return []
    if mode == "twice":
        return series + [dict(x, metric={**x["metric"], "job": "kubernetes-pods"}) for x in series]
    if mode == "nan-value":
        return [dict(x, values=x["values"] + [[hi, "NaN"]]) for x in series]
    kind_, arg = mode
    if kind_ == "drop":
        return [x for x in series if x["metric"].get("pod") != arg]
    if kind_ == "extra":
        return series + [self.series(lo, hi, 15, 1.0, {**series[0]["metric"], "pod": arg})]
    if kind_ == "stale":
        return [dict(x, values=[v for v in x["values"] if v[0] <= hi - arg]) for x in series]
    if kind_ == "short":
        return [dict(x, values=x["values"][-arg:] if arg else []) for x in series]
    if kind_ == "gap":
        return [dict(x, values=[v for v in x["values"] if not hi - arg - 15 < v[0] < hi - 15]) for x in series]
    if kind_ == "future":
        return [dict(x, values=x["values"] + [[hi + arg, "1"]]) for x in series]
    raise ValueError(f"FakeVM: unknown telemetry mode {mode}")


FakeVM._telemetry = _telemetry_method


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


def plan_now(record, kube, clock, vm=None):
    """A plan made with the observer (vm; default: a steady one on which every series the gate reads is healthy)."""
    c = B.Ctx(record, kube, now=clock.now, sleep=clock.sleep)
    p = B.plan_admission(c, ORIGIN, vm or FakeVM(None, POD_NAMES), "http://vm")
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


def _run(world, steal=1.0, alerts=(), raw_steal=None, **kw):  # noqa: F811
    record, kube, base = ready_world(world)
    clock = Clock(O_MS - 180_000)
    a = JobApi(base, clock, O_MS + 40 * 60_000 + 60_000, **kw)
    kube = record.kube(api=a)
    c, p, d = plan_now(record, kube, clock)
    assert p["problems"] == [], p["problems"]
    _, lines, ingress = simulated(c.params, "admission", O_MS)
    names = {t: [f"{t}-{i:02d}" for i in range(12)] for t in (N, R)}
    vm = FakeVM(ingress, names, steal=steal, alerts=alerts, raw_steal=raw_steal, raw_steal_from_s=O_MS / 1000)
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


W_INSTANCE, S_INSTANCE = (INSTANCES[n] for n in ("worker-w", "worker-s"))


@pytest.mark.parametrize("steal", ["NaN", "+Inf", "-Inf"])
def test_a_steal_rate_that_is_not_a_finite_number_stops_the_load(world, steal):  # noqa: F811
    """Full, fresh bins whose value is NaN or infinite: NaN compares false with the limit, so without its own check
    this would read as "below 5 %". The raw counter samples are healthy here: they cannot vouch for the derived rate."""
    record, kube, a, clock, c, p, d, logs, vm = _run(world, steal=steal)
    seen = []
    inner = vm.__call__

    class Probe:
        def __call__(self, url):
            if "c28_ingress_requests_total" in url:
                seen.append(len(_gen_pods(a)))
            return inner(url)
    out = B.run_admission(c, p, d, Probe(), "http://vm", logs=logs)
    assert out["data"]["verdict"] == "STOPPED" and "is not a finite number" in out["data"]["stopped"], out["data"]["stopped"]
    assert "raw steal samples" not in out["data"]["stopped"]
    assert seen and all(n == 0 for n in seen)
    assert not os.path.exists(X.lock_file(record)) and not X.open_intents(X.receipts(record, B.HELPER))


def test_unusable_raw_values_and_unusable_bins_together_stop_through_live_stops(world):  # noqa: F811
    """The combined case from the review: fresh raw timestamps carrying NaN counter values AND fresh NaN bins."""
    # both from the origin on: sources already unusable BEFORE the run are refused by the telemetry gate instead
    record, kube, a, clock, c, p, d, logs, vm = _run(world, steal=lambda hi: "NaN" if hi >= O_MS / 1000 else 1.0,
                                                     raw_steal={W_INSTANCE: "nan-value", S_INSTANCE: "nan-value"})
    out = B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert out["data"]["verdict"] == "STOPPED"
    for text in ("steal observation of workload node is not a finite number", "steal observation of support node is not a finite number",
                 "raw steal samples of the workload node missing or malformed", "raw steal samples of the support node missing or malformed"):
        assert text in out["data"]["stopped"], out["data"]["stopped"]
    assert not os.path.exists(X.lock_file(record)) and not X.open_intents(X.receipts(record, B.HELPER))


@pytest.mark.parametrize("raw_steal, text", [
    ({S_INSTANCE: ("stale", 121)}, "the newest raw steal sample of the support node is older than 120 s"),
    ({W_INSTANCE: ("stale", 300)}, "the newest raw steal sample of the workload node is older than 120 s"),
    ({W_INSTANCE: "absent"}, "raw steal samples of the workload node missing or malformed"),
    ({S_INSTANCE: "empty"}, "raw steal samples of the support node missing or malformed"),
    ({S_INSTANCE: "nan"}, "raw steal samples of the support node missing or malformed"),
    ({W_INSTANCE: "text"}, "raw steal samples of the workload node missing or malformed"),
    ({W_INSTANCE: "short"}, "raw steal samples of the workload node missing or malformed"),
    ({S_INSTANCE: "no-value"}, "raw steal samples of the support node missing or malformed"),
    ({W_INSTANCE: "text-value"}, "raw steal samples of the workload node missing or malformed"),
    ({S_INSTANCE: "nan-value"}, "raw steal samples of the support node missing or malformed"),
    ({W_INSTANCE: "inf-value"}, "raw steal samples of the workload node missing or malformed"),
    ({S_INSTANCE: "bool-value"}, "raw steal samples of the support node missing or malformed"),
    ({W_INSTANCE: "bool-time"}, "raw steal samples of the workload node missing or malformed"),
    ({S_INSTANCE: "three"}, "raw steal samples of the support node missing or malformed"),
    ({S_INSTANCE: ("future", 60)}, "raw steal samples of the support node are from the future"),
    ({S_INSTANCE: "refused"}, "an observer query failed"),
    # Within the live limit the load is NOT stopped. (The evidence collected afterwards has its own, stricter
    # staleness criterion, so a 105 s hole still leaves that run INCOMPLETE: not stopped is not the same as accepted.)
    ({S_INSTANCE: ("stale", 105)}, "INCOMPLETE"),
    ({W_INSTANCE: ("future", 0)}, "PASS")])
def test_lost_underlying_steal_samples_stop_the_load_though_the_bins_look_fresh(world, raw_steal, text):  # noqa: F811
    """The stepped range query keeps answering with full, low bins in every case here: only the raw samples show that
    the safety observation is gone, and that must reach the executor's own stop path."""
    record, kube, a, clock, c, p, d, logs, vm = _run(world, raw_steal=raw_steal)
    seen = []
    inner = vm.__call__

    class Probe:
        def __call__(self, url):
            if "c28_ingress_requests_total" in url:                   # the collection's first query
                seen.append(len(_gen_pods(a)))
            return inner(url)
    out = B.run_admission(c, p, d, Probe(), "http://vm", logs=logs)
    assert any("node_uname_info" in q and 'mode="steal"' in q and "rate(" in q for q in vm.queries)   # the bins were asked
    if text in ("PASS", "INCOMPLETE"):
        assert not out["data"]["stopped"] and out["data"]["verdict"] == text
        return
    assert text in out["data"]["stopped"] and out["data"]["verdict"] == "STOPPED", out["data"]["stopped"]
    assert "steal above" not in out["data"]["stopped"] and "steal observation of" not in out["data"]["stopped"]
    assert seen and all(n == 0 for n in seen)                         # the generator pod was gone before collection
    assert not os.path.exists(X.lock_file(record)) and not X.open_intents(X.receipts(record, B.HELPER))


def test_the_raw_steal_check_reads_raw_samples_at_the_evaluation_time(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    urls = []
    inner = vm.__call__

    def fetch(url):
        urls.append(url)
        return inner(url)
    now = 1_800_000_000.0
    assert B.raw_steal_stops(c, fetch, "http://vm", now) == []
    import urllib.parse as U
    raw = [dict(U.parse_qsl(U.urlparse(u).query)) for u in urls if "node_cpu_seconds_total" in u]
    assert len(raw) == 2 and all(U.urlparse(u).path.endswith("/api/v1/query") for u in urls)      # never query_range
    assert sorted(r["query"] for r in raw) == sorted(
        f'node_cpu_seconds_total{{job="node-exporter",mode="steal",instance="{i}"}}[300s]' for i in (W_INSTANCE, S_INSTANCE))
    assert all(float(r["time"]) == now for r in raw)


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
    p = B.plan_startup(c, _ts(o2), FakeVM(None, POD_NAMES), "http://vm")
    saved = X.save_plan(record, B.HELPER, p, "plan-startup")
    _, lines, ingress = simulated(c.params, "startup", o2,
                                  stalls=[{"at_ms": o2 + x[0], "for_ms": x[1]} for x in stalls])
    vm = FakeVM(ingress, POD_NAMES)
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
    assert any("no longer hashes" in x for x in B.plan_startup(c, p["origin_utc"], FakeVM(None, POD_NAMES), "http://vm")["problems"])


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
        if "startup-" in path and "telemetry-" not in path:      # the evidence, not the gate's archive before it
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
    assert any("other images, configuration" in x for x in B.plan_startup(c, p["origin_utc"], FakeVM(None, POD_NAMES), "http://vm")["problems"])


def test_a_stalled_generator_is_not_the_scheduled_load(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world, stalls=[(3 * 60_000, 20_000)])
    data = run_s(c, p, d, vm, lines)["data"]
    assert data["measured"] is False
    assert any("zero generator drops" in x for x in data["load_problems"]), data["load_problems"]


# --- the third review's reproductions -----------------------------------------------------------------------------------

def test_a_configuration_changed_during_the_run_is_never_a_pass(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    inner = vm.__call__

    class Probe:
        def __call__(self, url):
            if "c28_ingress_requests_total" in url and clock.ms > O_MS:   # during the collection (not the gate)
                tpl = a.obj("Deployment", N, "demo")["spec"]["template"]["spec"]["containers"][0]
                tpl.setdefault("resources", {}).setdefault("limits", {})["memory"] = "16Mi"
            return inner(url)
    out = B.run_admission(c, p, d, Probe(), "http://vm", logs=logs)
    assert out["data"]["verdict"] == "INCOMPLETE"
    begin = X.last(X.receipts(record, B.HELPER), "run-begin")["data"]["binding"]
    assert out["data"]["binding"] == begin                            # the ORIGINAL binding is the one kept


def test_an_unobserved_tail_invalidates_the_startup_measurement(world, monkeypatch):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    origin = _ms_of(p["origin_utc"])
    real = B.PodTimeline.poll

    def paused(self):
        if clock.ms < origin + 3 * 60_000:
            return real(self)                                         # observation stops after minute two
    monkeypatch.setattr(B.PodTimeline, "poll", paused)
    data = run_s(c, p, d, vm, lines)["data"]
    ev = json.load(open(os.path.join(data["dir"], "evidence.json")))
    assert data["measured"] is False and ev["summary"]["coverage_gaps"]


def test_a_configmap_delete_error_never_blocks_the_restore(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    real = a.delete

    def delete(path, uid, resource_version=None):
        if "/configmaps/" in path:
            raise X.C.ApiError("delete configmap: failed (error, exit 1)", 500)
        return real(path, uid, resource_version)
    a.delete = delete
    with pytest.raises(X.StagingError, match="ConfigMap could not be removed"):
        run_s(c, p, d, vm, lines)
    assert a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12      # restored anyway
    assert B.open_obligations(record)                                     # but the obligation stays open


def test_a_crashed_admission_is_cleaned_up_through_plan_stop_and_stop(world):  # noqa: F811
    import part2_fixtures as PF
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    rid = "a-crashed"
    objs = {o["kind"]: o for o in c.c28.demo.bounded_objects(c.params, "admission", O_MS)}
    with X.mutating(record, kube, B.HELPER, {"run": rid, "label": "admission"}):
        B.own(c, rid, kube.create(objs["ConfigMap"]))
        kube.create(objs["Job"])                                         # the executor died before its receipt
    assert _gen_pods(a)                                                 # its pod runs on
    import common_records as C
    C.exclusive_lock(record.doc, X.LOCK_SCOPE, holder=B.HELPER).__enter__()   # the dead executor's lock, never released
    PF.crash_owner(record)
    rp = X.recover_plan(record, kube)
    s_ = X.save_plan(record, "recover", rp, "recover-plan")
    X.recover(record, kube, X.load_plan(s_["plan"]), s_["plan_digest"])
    plan = B.plan_stop(c, rid)
    assert plan["problems"] == [] and {x["stop"] for x in plan["actions"]} == {"ConfigMap", "Job"}
    s_ = X.save_plan(record, B.HELPER, plan, "plan-stop")
    out = B.stop(c, X.load_plan(s_["plan"]), s_["plan_digest"])
    assert out["event"] == "run-stopped-by-operator"
    assert not _gen_pods(a) and a.obj("Job", "c28-demo-admission", "demo") is None
    assert a.obj("ConfigMap", "c28-demo-admission", "demo") is None and not os.path.exists(X.lock_file(record))


# --- the telemetry gate (after the admission run of 2026-11-19, whose verdict could not pass) -----------------------

def _gate_plan(world, telemetry=None, raw_steal=None, edit=None):  # noqa: F811
    record, kube, a = ready_world(world)
    if edit:
        edit(a)
    vm = FakeVM(None, POD_NAMES, telemetry=telemetry, raw_steal=raw_steal)
    c, p, d = plan_now(record, kube, Clock(O_MS - 180_000), vm)
    return record, kube, a, c, p, d, vm


def test_the_plan_binds_exactly_the_verdicts_freshness_inventory_and_nothing_that_varies(world):  # noqa: F811
    record, kube, a, c, p, d, vm = _gate_plan(world)
    assert p["problems"] == [], p["problems"]
    tele = p["telemetry"]
    assert sorted(r["key"] for part in ("existing", "generator") for r in tele[part]) == \
        c.verdicts.freshness_inventory([N, R])
    assert [r["key"] for r in tele["generator"]] == ["cfs_periods/generator", "cfs_throttled/generator"]
    text = json.dumps(tele)
    assert "at_s" not in text and INGRESS_POD not in text and not any(n in text for n in POD_NAMES[N] + POD_NAMES[R])
    assert not any(i in text for i in INSTANCES.values())                 # the node instance is resolved when observed
    # what was observed is on the context, outside the plan: a later observation gives the SAME plan digest
    first = c.telemetry
    assert first["problems"] == [] and set(first["observed"]) == {r["key"] for r in tele["existing"]}
    assert first["observed"][f"queue/{N}"]["series"] == 12 and first["observed"][f"ingress/{R}"]["series"] == 1
    c2, p2, d2 = plan_now(record, kube, Clock(O_MS - 150_000), vm)
    assert d2 == d and c2.telemetry["at_s"] == first["at_s"] + 30


@pytest.mark.parametrize("telemetry, key, text", [
    ({f"queue/{N}": "absent"}, f"queue/{N}", "no series"),                 # the installation of 2026-11-19
    ({f"requests/{R}": "absent"}, f"requests/{R}", "no series"),
    ({f"ingress/{N}": "absent"}, f"ingress/{N}", "no series"),             # a counter born at the first request
    ({f"ingress/{N}": "twice"}, f"ingress/{N}", "2 series where exactly one is expected"),
    ({f"queue/{N}": ("drop", f"{N}-03")}, f"queue/{N}", "no series for 1 of 12 expected pods"),
    ({f"requests/{N}": ("drop", f"{N}-00")}, f"requests/{N}", "no series for 1 of 12 expected pods"),
    ({f"queue/{N}": ("extra", f"{N}-zz")}, f"queue/{N}", "series of 1 pods that are not live Ready pods of it"),
    ({f"queue/{R}": "twice"}, f"queue/{R}", "more than one series for the same pod (scraped twice?)"),
    ({f"requests/{N}": "twice"}, f"requests/{N}", "more than one series for the same pod and code (scraped twice?)"),
    ({f"queue/{N}": ("stale", 61)}, f"queue/{N}", "the newest sample of a series is older than 60 s"),
    ({f"requests/{N}": ("short", 2)}, f"requests/{N}", "a series with fewer than 3 samples in the lookback"),
    ({f"queue/{N}": ("short", 0)}, f"queue/{N}", "a series with fewer than 3 samples in the lookback"),
    ({f"queue/{N}": ("gap", 61)}, f"queue/{N}", "a gap above 60 s between samples"),
    ({f"queue/{N}": "nan-value"}, f"queue/{N}", "a sample that is not a finite [timestamp, value]"),
    ({f"queue/{N}": ("future", 6)}, f"queue/{N}", "a sample from the future"),
    ({f"queue/{N}": "refused"}, f"queue/{N}", "the query failed"),
    ({"cfs_periods/fixture": ("drop", f"{R}-11")}, "cfs_periods/fixture", "no series for 1 of 24 expected pods"),
    ({"cfs_throttled/ingress": "absent"}, "cfs_throttled/ingress", "no series"),
    ({"cfs_throttled/fixture": ("stale", 61)}, "cfs_throttled/fixture", "older than 60 s"),
])
def test_the_plan_refuses_when_a_series_the_verdict_reads_is_not_there_as_expected(world, telemetry, key, text):  # noqa: F811
    *_, p, d, vm = _gate_plan(world, telemetry)
    assert p["problems"] and all(x.startswith(f"telemetry {key}: ") for x in p["problems"]), p["problems"]
    assert any(text in x for x in p["problems"]), p["problems"]


@pytest.mark.parametrize("telemetry", [
    {f"queue/{N}": ("stale", 60)},                                         # exactly at the limit
    {f"queue/{N}": ("future", 5)},
    {f"requests/{N}": ("short", 3)},
    {"cfs_periods/fixture": ("extra", "some-other-pod")},                  # cAdvisor: other pods' series do not matter
    {"cfs_periods/generator": "absent"},                                   # not part of what exists before the run
])
def test_the_plan_accepts_what_is_within_the_limits(world, telemetry):  # noqa: F811
    *_, p, d, vm = _gate_plan(world, telemetry)
    assert p["problems"] == [], p["problems"]


@pytest.mark.parametrize("raw_steal, text", [
    ({W_INSTANCE: "absent"}, "telemetry steal_raw/w: no series"),
    ({S_INSTANCE: ("stale", 121)}, "telemetry steal_raw/s: the newest sample of a series is older than 120 s"),
    ({S_INSTANCE: "nan-value"}, "telemetry steal_raw/s: a sample that is not a finite [timestamp, value]"),
    ({W_INSTANCE: "refused"}, "telemetry steal_raw/w: the query failed"),
    # node-exporter exports the steal counter per CPU: exactly the node's CPUs, each once, EVERY one fresh
    ({W_INSTANCE: "one-cpu"}, "telemetry steal_raw/w: no series for 3 of 4 expected CPUs (for example 1)"),
    ({S_INSTANCE: "cpu-twice"}, "telemetry steal_raw/s: more than one series for the same cpu (scraped twice?)"),
    ({W_INSTANCE: "cpu-extra"}, "telemetry steal_raw/w: series of 1 CPUs that the node does not have (for example 7)"),
    ({S_INSTANCE: ("cpu-stale", 121)}, "telemetry steal_raw/s: the newest sample of a series is older than 120 s"),
])
def test_the_plan_refuses_a_steal_source_that_is_already_lost(world, raw_steal, text):  # noqa: F811
    *_, p, d, vm = _gate_plan(world, raw_steal=raw_steal)
    assert any(text in x for x in p["problems"]), p["problems"]


def test_a_steal_source_within_the_live_limit_is_accepted_by_the_plan(world):  # noqa: F811
    *_, c, p, d, vm = _gate_plan(world, raw_steal={S_INSTANCE: ("stale", 105)})
    assert p["problems"] == []
    seen = c.telemetry["observed"]                                         # a healthy multi-CPU node: every CPU judged
    assert seen["steal_raw/w"]["series"] == seen["steal_raw/w"]["judged"] == seen["steal_raw/w"]["expected"] == CPUS


def test_a_node_that_reports_no_cpu_count_cannot_have_its_steal_source_judged(world):  # noqa: F811
    def no_capacity(a):
        a.obj("Node", "worker-s")["status"].pop("capacity")
    *_, p, d, vm = _gate_plan(world, edit=no_capacity)
    assert p["problems"] == ["telemetry steal_raw/s: the query failed (StagingError)"]


def _unready(a):
    a.obj("Pod", f"{N}-05", "demo")["status"]["conditions"] = [{"type": "Ready", "status": "False"}]


def _ingress_leaving(a):
    a.obj("Pod", INGRESS_POD, "demo")["metadata"]["deletionTimestamp"] = "2026-11-11T00:00:00Z"


@pytest.mark.parametrize("edit, text", [
    (_unready, f"telemetry queue/{N}: series of 1 pods that are not live Ready pods of it"),
    (_ingress_leaving, "telemetry cfs_periods/ingress: no live Ready pod is known for it")])
def test_the_expected_pods_are_the_live_ready_ones(world, edit, text):  # noqa: F811
    *_, p, d, vm = _gate_plan(world, edit=edit)
    assert any(text in x for x in p["problems"]), p["problems"]


def test_planning_needs_the_observer(world):  # noqa: F811
    record, kube, a = ready_world(world)
    c = B.Ctx(record, kube, now=Clock(O_MS - 180_000).now)
    p = B.plan_admission(c, ORIGIN)
    assert p["problems"] == ["telemetry: no query API was given, so the series the verdict reads cannot be checked"]
    assert c.telemetry is None


def test_a_run_refuses_before_any_mutation_when_the_telemetry_was_lost_after_the_review(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    vm.telemetry[f"queue/{N}"] = "absent"                                  # healthy at the review, gone at apply
    with pytest.raises(X.StagingError):
        B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert kube.mutations == [] and not _gen_pods(a)
    assert X.last(X.receipts(record, B.HELPER), "run-begin") is None
    assert not os.path.exists(X.lock_file(record)) and not X.open_intents(X.receipts(record, B.HELPER))


def test_a_run_whose_generator_has_no_telemetry_is_void_and_stopped_before_the_origin(world, monkeypatch):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    vm.telemetry["cfs_throttled/generator"] = "absent"
    stopped_at = []
    real = B.stop_load
    monkeypatch.setattr(B, "stop_load", lambda c_, run: (stopped_at.append(clock.ms), real(c_, run))[1])
    out = B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert "VOID: the generator's telemetry was not complete before the origin" in out["data"]["stopped"]
    assert "telemetry cfs_throttled/generator: no series" in out["data"]["stopped"]
    assert out["data"]["verdict"] != "PASS"
    # it waited for the telemetry, gave up 30 s before the origin, and the load generator was gone before it could send
    assert stopped_at and O_MS - 30_000 <= stopped_at[0] < O_MS, (stopped_at, O_MS)
    assert not _gen_pods(a) and a.obj("Job", "c28-demo-admission", "demo") is None
    arch = json.load(open(os.path.join(out["data"]["dir"], "telemetry-generator.json")))
    assert arch["problems"] == ["telemetry cfs_throttled/generator: no series"] and arch["pod"].startswith("c28-demo-admission-")
    before = json.load(open(os.path.join(out["data"]["dir"], "telemetry-before-mutation.json")))
    assert before["problems"] == [] and before["part"] == "existing"
    assert not os.path.exists(X.lock_file(record)) and not X.open_intents(X.receipts(record, B.HELPER))


def test_generator_telemetry_that_arrives_before_the_deadline_lets_the_run_go_on(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs, vm = _run(world)
    vm.telemetry["cfs_periods/generator"] = lambda hi: "absent" if hi < O_MS / 1000 - 100 else None
    out = B.run_admission(c, p, d, vm, "http://vm", logs=logs)
    assert out["data"]["verdict"] == "PASS" and not out["data"]["stopped"]
    arch = json.load(open(os.path.join(out["data"]["dir"], "telemetry-generator.json")))
    assert arch["problems"] == [] and O_MS / 1000 - 100 <= arch["at_s"] < O_MS / 1000 - 30
    # the gate reads the collector's own selectors: every raw selector it asked for is one the collection asks for
    gate = {q.rsplit("[", 1)[0] for q in vm.queries if q.endswith("[180s]")}
    collected = {q.rsplit("[", 1)[0] for q in vm.queries if q.endswith("s]") and not q.endswith("[180s]")
                 and not q.startswith(("sum", "100"))}
    assert gate and gate <= collected, sorted(gate - collected)


def test_the_commands_take_the_observer_and_keep_the_observation_outside_the_plan(world, capsys):  # noqa: F811
    record, kube, a = ready_world(world)
    vm = FakeVM(None, POD_NAMES)
    assert B.main(["--record", record.path, "check-telemetry", "--query-api", "http://vm"], api=a, fetch=vm) == 0
    seen = json.loads(capsys.readouterr().out)
    assert seen["problems"] == [] and seen["telemetry"]["observed"][f"requests/{N}"]["expected"] == 12
    vm.telemetry[f"queue/{R}"] = "absent"
    assert B.main(["--record", record.path, "check-telemetry", "--query-api", "http://vm"], api=a, fetch=vm) == 4
    assert json.loads(capsys.readouterr().out)["problems"] == [f"telemetry queue/{R}: no series"]
    with pytest.raises(SystemExit):                                        # --query-api is required for a plan
        B.main(["--record", record.path, "plan-admission", "--origin", ORIGIN], api=a, fetch=vm)
    capsys.readouterr()
    assert B.main(["--record", record.path, "plan-admission", "--origin", ORIGIN, "--query-api", "http://vm"],
                  api=a, fetch=vm) == 0
    out = json.loads(capsys.readouterr().out)
    assert any(f"telemetry queue/{R}: no series" in x for x in out["problems"])
    obs = json.load(open(out["telemetry_observation"]))
    assert obs["problems"] == [f"telemetry queue/{R}: no series"] and "at_s" in obs
    assert os.path.dirname(out["telemetry_observation"]) == os.path.dirname(out["plan"])
    assert "at_s" not in open(out["plan"]).read()


def test_the_startup_plan_has_the_same_gate(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    assert p["problems"] == [] and sorted(r["key"] for part in ("existing", "generator") for r in p["telemetry"][part]) \
        == c.verdicts.freshness_inventory([N, R])
    bad = FakeVM(None, POD_NAMES, telemetry={f"requests/{R}": "absent"})
    assert B.plan_startup(c, p["origin_utc"], bad, "http://vm")["problems"] == [f"telemetry requests/{R}: no series"]
    assert any("no query API" in x for x in B.plan_startup(c, p["origin_utc"])["problems"])


def test_a_startup_whose_generator_has_no_telemetry_is_void_and_restores_twelve(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    vm.telemetry["cfs_periods/generator"] = "absent"
    data = run_s(c, p, d, vm, lines)["data"]
    assert "VOID: the generator's telemetry was not complete before the origin" in data["stopped"]
    assert data["measured"] is False
    assert a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12 and not B.open_obligations(record)
    assert not [k for k in a.objects if k[0] in ("jobs", "configmaps", "pods") and "startup" in k[2]]


def test_a_startup_refuses_before_any_mutation_when_the_telemetry_was_lost_after_the_review(world):  # noqa: F811
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    before = len(kube.mutations)
    vm.telemetry[f"queue/{R}"] = ("stale", 61)
    with pytest.raises(X.StagingError):
        run_s(c, p, d, vm, lines)
    assert len(kube.mutations) == before and a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12
    assert not os.path.exists(X.lock_file(record)) and not B.open_obligations(record)


# --- the generator deadline is enforced (Codex review of b48cbbd: acceptance at deadline + 5 s and after a slow query) ---

class TimedVM:
    """An observer that takes a timeout, like http_json: records the budget each generator query was given; `slow`
    (seconds) is how long a generator query takes once the clock has reached `slow_from_s`; `fail` raises instead."""

    def __init__(self, vm, clock, slow=0.0, slow_from_s=0.0, fail=False):
        self.vm, self.clock, self.slow, self.slow_from_s, self.fail, self.budgets = vm, clock, slow, slow_from_s, fail, []

    def __call__(self, url, timeout=None):
        gate = "180s" in url                              # the gate's lookback; the collection asks other ranges
        if gate and "container_cpu_cfs" in url and "c28-demo-" in url and ".%2A" not in url and ".*" not in url:
            self.budgets.append(timeout)
            if self.fail:
                raise TimeoutError("timed out")
            if self.clock.ms / 1000 >= self.slow_from_s:
                self.clock.ms += self.slow * 1000                         # a slow but SUCCESSFUL answer
        return self.vm(url)


def _executor(world, kind):  # noqa: F811
    """(record, api, clock, vm, origin_ms, run(fetch) -> data, generator name) for either bounded executor."""
    if kind == "admission":
        record, kube, a, clock, c, p, d, logs, vm = _run(world)
        return record, a, clock, vm, O_MS, (
            lambda fetch: B.run_admission(c, p, d, fetch, "http://vm", logs=logs)["data"]), "c28-demo-admission"
    record, kube, a, clock, c, p, d, vm, lines = _startup(world)
    return record, a, clock, vm, _ms_of(p["origin_utc"]), (
        lambda fetch: B.run_startup(c, p, d, fetch, "http://vm", logs=lambda c_, pod: lines)["data"]), "c28-demo-startup"


def _void_and_clean(record, a, data, gen):
    assert "VOID: the generator's telemetry was not complete before the origin" in data["stopped"], data["stopped"]
    assert data.get("verdict") != "PASS" and data.get("measured") is not True
    assert not [k for k in a.objects if k[0] in ("jobs", "configmaps", "pods") and gen in k[2]]
    assert a.obj("Deployment", R, "demo")["spec"]["replicas"] == 12 and not B.open_obligations(record)
    assert not os.path.exists(X.lock_file(record)) and not X.open_intents(X.receipts(record, B.HELPER))


@pytest.mark.parametrize("kind", ["admission", "startup"])
@pytest.mark.parametrize("ready_after_deadline_s", [0.001, 5, 25])
def test_generator_telemetry_that_becomes_ready_after_the_deadline_is_never_accepted(world, kind, ready_after_deadline_s):  # noqa: F811
    record, a, clock, vm, origin, run, gen = _executor(world, kind)
    ready_at = origin / 1000 - 30 + ready_after_deadline_s
    vm.telemetry["cfs_periods/generator"] = lambda hi: "absent" if hi < ready_at else None
    data = run(vm)
    _void_and_clean(record, a, data, gen)
    assert "telemetry cfs_periods/generator: no series" in data["stopped"]


@pytest.mark.parametrize("kind", ["admission", "startup"])
def test_generator_telemetry_ready_at_the_deadline_is_accepted(world, kind):  # noqa: F811
    record, a, clock, vm, origin, run, gen = _executor(world, kind)
    vm.telemetry["cfs_periods/generator"] = lambda hi: "absent" if hi < origin / 1000 - 35 else None
    data = run(vm)
    assert "VOID" not in (data["stopped"] or "")


@pytest.mark.parametrize("kind", ["admission", "startup"])
@pytest.mark.parametrize("slow_s", [40, 100])                               # past the deadline; past the origin itself
def test_a_slow_successful_generator_query_cannot_be_accepted_late(world, kind, slow_s):  # noqa: F811
    record, a, clock, vm, origin, run, gen = _executor(world, kind)
    vm.telemetry["cfs_periods/generator"] = lambda hi: "absent" if hi < origin / 1000 - 60 else None
    timed = TimedVM(vm, clock, slow=slow_s, slow_from_s=origin / 1000 - 60)
    data = run(timed)
    _void_and_clean(record, a, data, gen)
    assert "the observation ended after the deadline" in data["stopped"]
    # every generator query was given no more than the time left before the deadline
    assert timed.budgets and all(b is not None and 0 < b <= 270 for b in timed.budgets)
    assert min(timed.budgets) <= 30.5, timed.budgets


@pytest.mark.parametrize("kind", ["admission", "startup"])
def test_generator_queries_that_time_out_end_in_a_void_run_that_is_cleaned_up(world, kind):  # noqa: F811
    record, a, clock, vm, origin, run, gen = _executor(world, kind)
    timed = TimedVM(vm, clock, fail=True)
    data = run(timed)
    _void_and_clean(record, a, data, gen)
    assert "the query failed (TimeoutError)" in data["stopped"]
    budgets = [b for b in timed.budgets if b is not None]
    assert budgets == sorted(budgets, reverse=True) and budgets[-1] <= 5.5    # shrinking towards the deadline


def test_the_same_timestamp_twice_is_one_sample(world):  # noqa: F811
    record, kube, a = ready_world(world)
    c = B.Ctx(record, kube, now=Clock(O_MS - 180_000).now)
    r = {"min_samples": 2, "max_age_s": 60, "unique": None}
    now = (O_MS - 180_000) / 1000
    twice = [{"metric": {"pod": "g"}, "values": [[now - 5, "1"], [now - 5, "1"]]}]
    two = [{"metric": {"pod": "g"}, "values": [[now - 20, "1"], [now - 5, "2"]]}]
    assert B._judge_series(r, twice, ["g"], "pod", now, 5)[1] == ["a series with fewer than 2 samples in the lookback"]
    assert B._judge_series(r, two, ["g"], "pod", now, 5)[1] == []


def test_an_observation_archived_after_the_deadline_is_not_accepted(world, monkeypatch):  # noqa: F811
    record, a, clock, vm, origin, run, gen = _executor(world, "admission")
    vm.telemetry["cfs_periods/generator"] = lambda hi: "absent" if hi < origin / 1000 - 35 else None
    real = B._write

    def slow_disk(path, doc):
        if path.endswith("telemetry-generator.json"):
            clock.ms += 20_000                                             # complete at -35 s, on disk at -15 s
        return real(path, doc)
    monkeypatch.setattr(B, "_write", slow_disk)
    data = run(vm)
    _void_and_clean(record, a, data, gen)
    assert "the observation was archived after the deadline" in data["stopped"]
