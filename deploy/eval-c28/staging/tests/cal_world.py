"""A simulated cluster and observer for calibrate.py's offline tests (never a real cluster: conftest sets
KUBECONFIG=/dev/null and everything runs against fake2.FakeApi2).

The simulator runs at every API read (FakeApi.on_poll): calibration Deployments get labelled, Ready pods on W through
their ReplicaSet; a calibration generator Job gets one pod on S, which completes once the simulated clock passes its
settle + measure, with a k6 termination message computed from a capacity model (per-replica capacity by
work_iterations); the ingress counters advance by the dispatched arrivals. The fake query API answers the contract's
queries from that state at the 15 s grid, up to "now"."""

import json
import math
import os
import re
import urllib.parse

import common_records as C
import execlib as X
import fake2
import calibrate as K
from part2_fixtures import COMMIT, RUN, S_UID, W_UID, Clock, reserve_section

FIXTURE = "harbor.example/autoscaler/c28-fixture@sha256:" + "a" * 64
GENERATOR = "grafana/k6@sha256:" + "e" * 64
T0 = 1_796_000_000

STEAL = 'max by (node) (rate(node_cpu_seconds_total{mode="steal"}[1m])) * 100'
QUERIES = {
    "ingress_up": 'up{namespace="__NAMESPACE__",pod=~"__PODS__"}',
    "fixture_up": 'up{namespace="__NAMESPACE__",pod=~"__PODS__"}',
    "fixture_requests": 'sum(c28_requests_total{namespace="__NAMESPACE__",target="__TARGET__"})',
    "fixture_inflight": 'sum(c28_inflight{namespace="__NAMESPACE__",target="__TARGET__"})',
    "ingress_requests": 'sum(c28_ingress_requests_total{namespace="__NAMESPACE__",target="__TARGET__"})',
    "ingress_responses": 'sum(c28_ingress_responses_total{namespace="__NAMESPACE__",target="__TARGET__"})',
    "fixture_queue": 'sum(c28_queue{namespace="__NAMESPACE__",target="__TARGET__"})',
}
CPU = 'sum(rate(container_cpu_usage_seconds_total{namespace="__NAMESPACE__",pod=~"__PODS__",container!=""}[1m]))'
THR = ('sum(rate(container_cpu_cfs_throttled_periods_total{namespace="__NAMESPACE__",pod=~"__PODS__"}[1m])) / '
       'sum(rate(container_cpu_cfs_periods_total{namespace="__NAMESPACE__",pod=~"__PODS__"}[1m]))')
for comp in ("fixture", "ingress", "generator"):
    QUERIES[f"{comp}_cpu_cores"] = CPU
    QUERIES[f"{comp}_throttled_fraction"] = THR


def contract(tmp, **over):
    images = os.path.join(tmp, "evidence", "images.json")
    con = {
        "schema": K.CONTRACT_SCHEMA, "run_id": RUN, "attempt": 1, "cluster_uid": fake2.CLUSTER, "namespace": "demo",
        "nodes": {"workload": {"name": "worker-w", "uid": W_UID}, "support": {"name": "worker-s", "uid": S_UID}},
        "source": {"commit": COMMIT, "load_script_sha256": C.sha256_file(K.LOAD_SCRIPT)},
        "images": {"images_json": "evidence/images.json",
                   "sha256": C.sha256_file(images) if os.path.exists(images) else "0" * 64, "fixture": FIXTURE,
                   "generator": GENERATOR},
        "candidate": {"cpu_request": "50m", "cpu_limit": "50m", "memory_request": "32Mi", "memory_limit": "32Mi",
                      "response_bytes": 1024, "max_inflight": 1, "max_queue": 32, "queue_timeout_seconds": 1.5,
                      "gomaxprocs": 2, "startup_delay_seconds": 20},
        "work_selection": {"values": [600, 900], "rate_rps": 8, "idle_s": 120, "settle_s": 60, "measure_s": 300,
                           "cpu_ms_per_request": {"min": 2.5, "target": 2.75, "max": 3.0},
                           "tie_break": K.TIE_BREAK},
        "ingress": {"cpu_request": "100m", "cpu_limit": "500m", "memory_request": "64Mi", "memory_limit": "64Mi"},
        "generator": {"k6_version": "0.57.0", "cpu_request": "500m", "cpu_limit": "1", "memory_request": "256Mi",
                      "memory_limit": "512Mi"},
        "observer": {"query_api": "http://vmselect.monitoring.svc:8481/select/0/prometheus", "step_s": 15,
                     "max_staleness_s": 60, "scrape": "annotations", "queries": dict(QUERIES), "pods": [],
                     "pod_label": "pod"},
        "stops": {"steal_query": STEAL, "node_label": "node", "steal_percent": 5, "steal_bins": 5},
        "criteria": {"p95_ms": 200, "p99_ms": 500, "max_failure_fraction": 0.001, "max_dropped_arrivals": 0,
                     "linearity_min": 0.90, "ceiling_factor": 1.2,
                     "queue_growth": {"bin_s": 60, "edge_bins": 2, "max_increase": 2.0, "max_slope_per_min": 0.2,
                                      "max_end_backlog_per_replica": 4.0},
                     "max_generator_throttled_fraction": 0.05, "max_ingress_throttled_fraction": 0.05,
                     "reconcile_tolerance": {"absolute": 2, "relative": 0.001}},
        "search": {"replica_counts": [1, 2, 4], "repetitions": 3, "settle_s": 300, "measure_s": 900,
                   "rate_min_rps_per_replica": 2, "rate_max_rps_per_replica": 30, "initial_rps_per_replica": 18,
                   "bracket_factor": 1.25, "resolution": 0.05, "max_trials": 80,
                   "deadline_utc": "2027-01-01T00:00:00Z", "seed": 3101, "completion_grace_s": 45,
                   "job_slack_s": 120, "converge_timeout_s": 300, "poll_s": 300},
        "admission": {"limit_fraction": 0.75, "overlap_pods": 2},
        "rollout_guard": {"kinds": list(K.ROLLOUT_KINDS), "extra_owners": []},
    }
    for path, value in over.items():
        cur = con
        keys = path.split(".")
        for k in keys[:-1]:
            cur = cur[k]
        cur[keys[-1]] = value
    return con


def _env(args):
    out, it = {}, iter(args)
    for a in it:
        if a == "-e":
            k, _, v = next(it).partition("=")
            out[k] = v
    return out


class World:
    """The simulated cluster state the observer reads; knobs: cap (rps per replica by work_iterations), lin4 (the
    4-replica efficiency), drops, extra_arrivals, stale_after, missing (query kinds answered with no series),
    gen_throttle, hooks (callables run at each poll with (world, api))."""

    def __init__(self, api, clock, cal):
        self.api, self.clock, self.cal = api, clock, cal
        self.cap = {600: 20.0, 900: 13.0}
        self.cost_ms = {600: 2.73, 900: 4.10}
        self.lin4, self.drops, self.extra_arrivals, self.gen_throttle = 1.0, 0, 0, 0.0
        self.stale_after = None
        self.truncate_windows = False
        self.init_delay = 3           # k6 initialization: the measure scenario starts this long after start + settle
        self.steal = {}               # node -> steal percent (default 1.0)
        self.direct_traffic = False   # requests straight to the fixture (bypassing the ingress) during idle
        self.drop_key_absent = True   # k6 omits dropped_iterations when nothing dropped
        self.up_label = "pod"         # the label the observer's up series name pods with
        self.missing = set()
        self.counters = {}            # target -> {"req": n, "resp": n}
        self.windows = []             # [{target, start, end, rate_rps, wi, ok}]
        self.hooks = []
        self.linger = False           # generator pods survive their Job's deletion (failed cleanup)
        api.on_poll.append(self.poll)

    # --- the cluster ---
    def _cal(self, o):
        return (o.get("metadata", {}).get("labels") or {}).get(K.CAL_LABEL) == self.cal

    def poll(self, api):
        if getattr(self, "_busy", False):
            return
        self._busy = True
        try:
            for h in list(self.hooks):
                h(self, api)
            self._deployments(api)
            self._jobs(api)
        finally:
            self._busy = False

    def _deployments(self, api):
        rs_alive = {o["metadata"]["uid"] for (k, _, _), o in api.objects.items() if k == "replicasets"}
        for key, pod in list(api.objects.items()):            # cascade: pods of a deleted ReplicaSet go away
            if key[0] == "pods" and self._cal(pod):
                ref = [r for r in pod["metadata"].get("ownerReferences") or [] if r.get("kind") == "ReplicaSet"
                       and r.get("name", "").endswith("-rs")]          # only the simulator's own ReplicaSets
                if ref and ref[0]["uid"] not in rs_alive:
                    del api.objects[key]
        for (k, ns, _), dep in list(api.objects.items()):
            if k != "deployments" or not self._cal(dep):
                continue
            api.sync_pods(dep)
            want = dep["spec"].get("replicas") or 0
            dep["status"].update(updatedReplicas=want, availableReplicas=want, readyReplicas=want, replicas=want,
                                 observedGeneration=dep["metadata"].get("generation", 1))
            rs = api.objects[("replicasets", ns, f"{dep['metadata']['name']}-rs")]
            tmpl = dep["spec"]["template"]
            renamed = []
            for (pk, _, _), pod in api.objects.items():
                if pk == "pods" and any(r.get("uid") == rs["metadata"]["uid"]
                                        for r in pod["metadata"].get("ownerReferences") or []):
                    if pod["metadata"].get("labels"):
                        continue
                    old = pod["metadata"]["name"]
                    pod["metadata"]["name"] = f"{dep['metadata']['name']}-7d9f8c6b5-{old[-5:]}"
                    renamed.append((old, pod))
                    pod["metadata"]["labels"] = {**tmpl["metadata"]["labels"], "pod-template-hash": "7d9f8c6b5"}
                    pod["metadata"]["annotations"] = dict(tmpl["metadata"].get("annotations") or {})
                    pod["spec"] = {**json.loads(json.dumps(tmpl["spec"])), "nodeName": "worker-w"}
                    self._admit(pod["spec"])
                    pod["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}],
                                     "containerStatuses": [{"name": tmpl["spec"]["containers"][0]["name"],
                                                            "imageID": FIXTURE, "restartCount": 0,
                                                            "state": {"running": {}}}]}
            for old, pod in renamed:
                api.objects.pop(("pods", ns, old), None)
                api.objects[("pods", ns, pod["metadata"]["name"])] = pod

    def _jobs(self, api):
        now = self.clock.now()
        for (k, ns, name), job in list(api.objects.items()):
            if k != "jobs" or not self._cal(job):
                continue
            uid = job["metadata"]["uid"]
            pods = [o for (pk, _, _), o in api.objects.items() if pk == "pods"
                    and any(r.get("uid") == uid for r in o["metadata"].get("ownerReferences") or [])]
            env = _env(job["spec"]["template"]["spec"]["containers"][0]["args"])
            if not pods and not job.get("status", {}).get("conditions"):
                tmpl = job["spec"]["template"]
                api.add("pods", {"kind": "Pod", "metadata": {
                    "name": f"{name}-abcde", "namespace": ns,
                    "labels": {**tmpl["metadata"]["labels"], "batch.kubernetes.io/job-name": name, "job-name": name},
                    "annotations": dict(tmpl["metadata"].get("annotations") or {}),
                    "ownerReferences": [{"kind": "Job", "name": name, "uid": uid, "controller": True}]},
                    "spec": self._admit({**json.loads(json.dumps(tmpl["spec"])), "nodeName": "worker-s"}),
                    "status": {"phase": "Running", "containerStatuses": [
                        {"name": "k6", "imageID": "docker.io/" + GENERATOR, "restartCount": 0,
                         "state": {"running": {"startedAt": K._ts(now)}}}]}})
                job["status"] = {"active": 1, "startTime": K._ts(now)}
                continue
            if not pods or job.get("status", {}).get("conditions"):
                continue
            pod = pods[0]
            started = K._utc(pod["status"]["containerStatuses"][0]["state"]["running"]["startedAt"])
            settle, measure = int(env["C28CAL_SETTLE_S"]), int(env["C28CAL_MEASURE_S"])
            if now < started + self.init_delay + settle + measure + 5:
                continue
            self._complete(api, job, pod, env, started)

    @staticmethod
    def _admit(spec):
        """What the API server's defaulting and DefaultTolerationSeconds admission add to an admitted pod."""
        spec.setdefault("dnsPolicy", "ClusterFirst")
        spec.setdefault("schedulerName", "default-scheduler")
        spec.setdefault("serviceAccountName", "default")
        spec.setdefault("enableServiceLinks", True)
        for c in spec.get("containers", []):
            c.setdefault("terminationMessagePath", "/dev/termination-log")
            c.setdefault("terminationMessagePolicy", "File")
            c.setdefault("imagePullPolicy", "IfNotPresent")
        spec["tolerations"] = list(spec.get("tolerations") or []) + [
            {"key": k, "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 300}
            for k in ("node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable")]
        return spec

    def _replicas_and_wi(self, api, target):
        dep = api.objects[("deployments", "demo", target)]
        wi = int(next(e["value"] for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]
                      if e["name"] == "WORK_ITERATIONS"))
        return dep["spec"]["replicas"], wi

    def _complete(self, api, job, pod, env, started):
        target, rate = env["C28CAL_TARGET"], int(env["C28CAL_RATE_PER_MIN"])
        settle, measure = int(env["C28CAL_SETTLE_S"]), int(env["C28CAL_MEASURE_S"])
        n, wi = self._replicas_and_wi(api, target)
        cap = self.cap[wi] * (self.lin4 if n == 4 else 1.0)
        ok = rate / 60 / n <= cap
        scen = {}
        for s, dur in (("settle", settle), ("measure", measure)):
            sched = rate * dur / 60
            dropped = self.drops if s == "measure" else 0
            disp = sched - dropped
            failed = 0 if ok else math.ceil(0.05 * disp)
            scen[s] = {"iterations": {"count": disp, "rate": disp / dur},
                       "dropped_iterations": ({"count": dropped, "rate": dropped / dur}
                                              if dropped or not self.drop_key_absent else None),
                       "http_reqs": {"count": disp, "rate": disp / dur},
                       "http_req_failed": {"rate": failed / disp, "passes": failed, "fails": disp - failed},
                       "http_req_duration": {"avg": 30, "min": 5, "p(50)": 25, "p(95)": 50 if ok else 450,
                                             "p(99)": 120 if ok else 900, "max": 300 if ok else 2000,
                                             "count": disp},
                       "start_ms": {"min": (started + self.init_delay + (settle if s == "measure" else 0)) * 1000,
                                    "max": (started + self.init_delay + (settle if s == "measure" else 0)) * 1000}}
            total = disp
            c = self.counters.setdefault(target, {"req": 0, "resp": 0})
            c["req"] += total + (self.extra_arrivals if s == "measure" else 0)
            c["resp"] += total + (self.extra_arrivals if s == "measure" else 0)
        summary = {"schema": K.K6_SCHEMA, "trial": env["C28CAL_TRIAL"], "target": target, "rate_per_min": rate,
                   "settle_s": settle, "measure_s": measure, "seed": int(env["C28CAL_SEED"]), "scenarios": scen}
        ws = started + self.init_delay + settle
        self.windows.append({"target": target, "start": started, "ws": ws, "end": ws + measure,
                             "rate_rps": rate / 60, "wi": wi, "ok": ok, "job": job["metadata"]["name"]})
        cs = pod["status"]["containerStatuses"][0]
        cs["state"] = {"terminated": {"exitCode": 0, "reason": "Completed", "startedAt": K._ts(started),
                                      "finishedAt": K._ts(self.clock.now()), "message": json.dumps(summary)}}
        pod["status"]["phase"] = "Succeeded"
        job["status"] = {"succeeded": 1, "conditions": [{"type": "Complete", "status": "True"}]}

    # --- the observer ---
    def fetch(self, url):
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        query = q["query"][0]
        start, end, step = int(q["start"][0]), int(q["end"][0]), int(q["step"][0])
        now = self.clock.now()
        last = min(end, now, self.stale_after if self.stale_after is not None else now)
        grid = [t for t in range(start - start % step + (step if start % step else 0), last + 1, step)]
        if self.truncate_windows and end - start >= 300:          # a measurement window answered only in part
            grid = grid[:len(grid) // 2]
        target = (re.search(r'target="([^"]*)"', query) or [None, None])[1]
        pods = (re.search(r'pod=~"([^"]*)"', query) or [None, ""])[1]
        if query == STEAL:
            g60 = [t for t in range(start - start % 60, last + 1, 60) if t >= start]
            if "steal" in self.missing:
                return {"status": "success", "data": {"result": []}}
            return {"status": "success", "data": {"result": [
                {"metric": {"node": n}, "values": [[t, str(self.steal.get(n, 1.0))] for t in g60]}
                for n in ("worker-w", "worker-s")]}}
        if query.startswith("up{"):
            kind = "up"
        elif "c28_ingress_requests_total" in query:
            kind = "req"
        elif "c28_ingress_responses_total" in query:
            kind = "resp"
        elif "c28_queue" in query:
            kind = "backlog"
        elif "c28_inflight" in query:
            kind = "inflight"
        elif "c28_requests_total" in query:
            kind = "fixture_requests"
        elif "cfs_throttled" in query:
            kind = "throttle"
        else:
            kind = "cpu"
        if kind in self.missing:
            return {"status": "success", "data": {"result": []}}
        if kind == "fixture_requests":
            if not self.direct_traffic:
                return {"status": "success", "data": {"result": []}}
            return self._series(grid, lambda t: t - start)
        if kind == "inflight":
            return self._series(grid, lambda t: 0.5 if self._window(target=target, t=t) else 0)
        if kind in ("req", "resp"):
            c = self.counters.get(target)
            if c is None:
                return {"status": "success", "data": {"result": []}}
            return self._series(grid, lambda t: c[kind])
        if kind == "up":
            live = sorted(o["metadata"]["name"] for (k, _, _), o in self.api.objects.items()
                          if k == "pods" and re.fullmatch(pods, o["metadata"]["name"]))
            return {"status": "success", "data": {"result": [
                {"metric": {self.up_label: n}, "values": [[t, "1"] for t in grid]} for n in live]}}
        if kind == "backlog":
            def backlog(t):
                w = self._window(target=target, t=t)
                if not w:
                    return 0
                return 0.2 if w["ok"] else 0.2 + (t - w["ws"]) / 60
            return self._series(grid, backlog)
        comp = "generator" if "-gen-" in pods else "ingress" if "-ing-" in pods else "fixture"
        if kind == "throttle":
            return self._series(grid, lambda t: {"generator": self.gen_throttle, "ingress": 0.0}.get(comp, 0.1))
        if comp != "fixture":
            return self._series(grid, lambda t: 0.1 if comp == "generator" else 0.05)
        variant = pods.split("-[")[0]

        def cpu(t):
            w = self._window(target=variant, t=t)
            return 0.002 + (w["rate_rps"] * self.cost_ms[w["wi"]] / 1000 if w else 0)
        return self._series(grid, cpu)

    def _window(self, target, t):
        return next((w for w in self.windows if w["target"] == target and w["start"] <= t <= w["end"]), None)

    @staticmethod
    def _series(grid, fn):
        return {"status": "success", "data": {"result": [{"metric": {}, "values": [[t, str(fn(t))] for t in grid]}]}}


def setup(tmp_path, make_record, api, contract_over=None, images_over=None, nodes_alloc=("4", "8Gi")):
    """Nodes W/S (with allocatable), the reservation applied, images.json, the contract, and the record."""
    import reserve
    from part2_fixtures import plan_and_apply
    for name, uid in (("worker-w", W_UID), ("worker-s", S_UID)):
        n = fake2.node(name, uid=uid)
        n["status"]["allocatable"] = {"cpu": nodes_alloc[0], "memory": nodes_alloc[1]}
        api.put(n)
    (tmp_path / "evidence").mkdir(exist_ok=True)
    images = {"schema": "c28-images/v1", "run_id": RUN, "source_commit": COMMIT, "verified": True,
              "images": {"fixture": {"ref": FIXTURE}, "generator": {"ref": GENERATOR}}}
    images.update(images_over or {})
    (tmp_path / "evidence" / "images.json").write_text(json.dumps(images))
    con = contract(str(tmp_path), **(contract_over or {}))
    (tmp_path / "contract.json").write_text(json.dumps(con, indent=1))
    record = X.load_record(make_record({"reserve": reserve_section(), "calibrate": {"contract": "contract.json"}}),
                           ("reserve", "calibrate"))
    plan_and_apply(reserve, record, record.kube(api))
    return record, con


def context(record, api, clock=None, world_cls=World):
    clock = clock or Clock(T0)
    cal = K.calibration_id(C.digest(json.loads(open(K.record_contract_path(record)).read())))
    world = world_cls(api, clock, cal)
    ctx = K.Context(record, record.kube(api), fetch=world.fetch, now=clock.now, sleep=clock.sleep)
    ctx.world, ctx.clock = world, clock
    return ctx


def reviewed(ctx, fn=None, helper=K.HELPER, name="plan"):
    p = (fn or K.plan)(ctx)
    saved = X.save_plan(ctx.record, helper, p, name)
    return X.load_plan(saved["plan"]), saved["plan_digest"]
