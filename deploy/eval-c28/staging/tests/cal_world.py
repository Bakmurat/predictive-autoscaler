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

# Prod-style queries (calibration-observer-20261108T233404Z): the steal query joins node_uname_info for the node name
# (label nodename); every CPU and throttling query filters metrics_path="/metrics/cadvisor" (prod's kubelet also
# exposes container_cpu_usage_seconds_total on /metrics/resource, which a bare sum would count twice); up is limited to
# the calibration's own VMPodScrape job.
NODES_RE = 'nodename=~"worker-(w|s)"'
STEAL = ('100 * sum by (nodename) (rate(node_cpu_seconds_total{job="node-exporter",mode="steal"}[1m]) * on (instance) '
         'group_left (nodename) node_uname_info{job="node-exporter",' + NODES_RE + '}) / count by (nodename) '
         '(node_cpu_seconds_total{job="node-exporter",mode="idle"} * on (instance) group_left (nodename) '
         'node_uname_info{job="node-exporter",' + NODES_RE + '})')
CADV = 'metrics_path="/metrics/cadvisor",namespace="__NAMESPACE__",pod=~"__PODS__",container!=""'
UP = 'up{namespace="__NAMESPACE__",job=~"__NAMESPACE__/c28cal-.+",pod=~"__PODS__"}'
QUERIES = {
    "ingress_up": UP,
    "fixture_up": UP,
    "fixture_requests": 'sum(c28_requests_total{namespace="__NAMESPACE__",target="__TARGET__"})',
    "fixture_inflight": 'sum(c28_inflight{namespace="__NAMESPACE__",target="__TARGET__"})',
    "ingress_requests": 'sum(c28_ingress_requests_total{namespace="__NAMESPACE__",target="__TARGET__"})',
    "ingress_responses": 'sum(c28_ingress_responses_total{namespace="__NAMESPACE__",target="__TARGET__"})',
    "fixture_queue": 'sum(c28_queue{namespace="__NAMESPACE__",target="__TARGET__"})',
}
CPU = "sum(rate(container_cpu_usage_seconds_total{" + CADV + "}[1m]))"
THR = ("sum(rate(container_cpu_cfs_throttled_periods_total{" + CADV + "}[1m])) / "
       "sum(rate(container_cpu_cfs_periods_total{" + CADV + "}[1m]))")
for comp in ("fixture", "ingress", "generator"):
    QUERIES[f"{comp}_cpu_cores"] = CPU
    QUERIES[f"{comp}_throttled_fraction"] = THR
RAW_CPU = "min by (pod) (timestamp(container_cpu_usage_seconds_total{" + CADV + "}))"
FRESHNESS = {
    "fixture_metrics": 'min by (pod) (timestamp(c28_queue{namespace="__NAMESPACE__",pod=~"__PODS__"}))',
    "fixture_cpu": RAW_CPU, "ingress_cpu": RAW_CPU, "generator_cpu": RAW_CPU,
    "ingress_metrics": "min by (pod) (timestamp(" + UP + "))",
    "steal": ('min by (nodename) (timestamp(node_cpu_seconds_total{job="node-exporter",mode="steal",cpu="0"}) '
              '* on (instance) group_left (nodename) (0 * node_uname_info{job="node-exporter",' + NODES_RE + '} + 1))'),
}
# The ingress's endpoint discovery (contract v2): its own gauges per ingress pod, the membership series' raw timestamps
# and the per-endpoint dispatch counters (label endpoint_pod; the scrape's pod label names the ingress pod).
DISC_SEL = 'namespace="__NAMESPACE__",pod=~"__PODS__"'
DISCOVERY_V2 = {
    "synced": "max by (pod) (c28_ingress_discovery_synced{" + DISC_SEL + "})",
    "stale": "max by (pod) (c28_ingress_discovery_stale{" + DISC_SEL + "})",
    "scrape_timestamp": "min by (pod) (timestamp(c28_ingress_discovery_stale{" + DISC_SEL + "}))",
    "endpoints": ('max by (endpoint_pod) (timestamp(c28_ingress_endpoint{namespace="__NAMESPACE__",'
                  'target="__TARGET__"}))'),
    "dispatched": ('sum by (endpoint_pod) (c28_ingress_dispatched_total{namespace="__NAMESPACE__",'
                   'target="__TARGET__"})'),
}
# Contract v3 adds the variant's no-ready-endpoint rejections (503s the ingress answers itself, never dispatched).
NO_READY = 'sum(c28_ingress_no_ready_endpoints_total{namespace="__NAMESPACE__",target="__TARGET__"})'
DISCOVERY = dict(DISCOVERY_V2, no_ready=NO_READY)
V2 = "c28-calibration-contract/v2"


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
        "work_selection": {"values": [600, 900], "rate_rps": 8, "idle_s": 240, "settle_s": 60, "measure_s": 300,
                           "cpu_ms_per_request": {"min": 2.5, "target": 2.75, "max": 3.0},
                           "tie_break": K.TIE_BREAK},
        "ingress": {"cpu_request": "100m", "cpu_limit": "500m", "memory_request": "64Mi", "memory_limit": "64Mi"},
        "generator": {"k6_version": "0.57.0", "cpu_request": "500m", "cpu_limit": "1", "memory_request": "256Mi",
                      "memory_limit": "512Mi"},
        "observer": {"query_api": "http://vmselect.monitoring.svc:8481/select/0/prometheus", "step_s": 15,
                     "max_staleness_s": 60, "scrape": "VMPodScrape", "queries": dict(QUERIES), "pods": [],
                     "pod_label": "pod", "freshness": dict(FRESHNESS), "cpu_scrape_interval_s": 30,
                     "readiness_timeout_s": 300, "discovery": dict(DISCOVERY)},
        "stops": {"steal_query": STEAL, "node_label": "nodename", "steal_percent": 5, "steal_bins": 5},
        "criteria": {"p95_ms": 200, "p99_ms": 500, "max_failure_fraction": 0.001, "max_dropped_arrivals": 0,
                     "linearity_min": 0.90, "ceiling_factor": 1.2,
                     "queue_growth": {"bin_s": 60, "edge_bins": 2, "max_increase": 2.0, "max_slope_per_min": 0.2,
                                      "max_end_backlog_per_replica": 4.0},
                     "max_generator_throttled_fraction": 0.05, "max_ingress_throttled_fraction": 0.05,
                     "reconcile_tolerance": {"absolute": 0, "relative": 0}},
        "search": {"replica_counts": [1, 2, 4], "repetitions": 3, "settle_s": 300, "measure_s": 900,
                   "rate_min_rps_per_replica": 2, "rate_max_rps_per_replica": 30, "initial_rps_per_replica": 18,
                   "bracket_factor": 1.25, "resolution": 0.05, "max_trials": 80,
                   "deadline_utc": "2027-01-01T00:00:00Z", "seed": 3101, "completion_grace_s": 45,
                   "job_slack_s": 120, "converge_timeout_s": 300, "poll_s": 300},
        "admission": {"limit_fraction": 0.75, "overlap_pods": 2},
        "rollout_guard": {"kinds": list(K.ROLLOUT_KINDS), "extra_owners": []},
    }
    if over.get("schema") == V2 and "observer.discovery" not in over:
        con["observer"]["discovery"] = dict(DISCOVERY_V2)     # the v2 reference contract
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
        self.raw_age = 10             # age of the newest raw sample behind every series
        self.raw_missing = set()      # pods/nodes whose raw samples are absent (hidden inside a sum otherwise)
        self.raw_stale = set()        # pods/nodes whose newest raw sample is 200 s old
        self.born = {}                # pod name -> creation time: no samples exist before born + scrape_delay
        self.scrape_delay = 20        # a new pod's first scrape lands this long after its creation
        self.missing = set()
        self.counters = {}            # target -> {"req": n, "resp": n}
        self.windows = []             # [{target, start, end, rate_rps, wi, ok}]
        self.hooks = []
        self.linger = False           # generator pods survive their Job's deletion (failed cleanup)
        # the ingress's endpoint discovery: stale from this time on (None: never); synced only from this time on;
        # fixture pods the ingress does not route to (hidden) or extra endpoints it routes to; the discovery metrics
        # absent altogether; arrivals dispatched (per target, per fixture pod) and a dispatch shortfall
        self.disc_stale_from = None
        self.disc_synced_from = None
        self.disc_hidden, self.disc_extra = set(), set()
        self.disc_extra_window = None     # (start, end): the extra endpoints exist only then (a blip between polls)
        self.disc_missing = False
        self.dispatched = {}
        self.rr_next = {}
        self.dispatch_short = 0
        # arrivals the ingress answers 503 "no ready endpoint" (received, never dispatched; k6 sees failures), per
        # trial id in its measure scenario; the counter per target exists only from its first rejection on
        self.no_ready_trials = {}
        self.no_ready = {}
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
                    self.born[pod["metadata"]["name"]] = self.clock.now()
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
                self.born[f"{name}-abcde"] = now
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
        spec.setdefault("serviceAccount", spec["serviceAccountName"])     # the deprecated mirror the API server sets
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
            rejected = self.no_ready_trials.get(env["C28CAL_TRIAL"], 0) if s == "measure" else 0
            failed = min(disp, (0 if ok else math.ceil(0.05 * disp)) + rejected)
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
            # strict round-robin over the variant's pods, as the endpoints-rr ingress dispatches
            arrivals = int(total + (self.extra_arrivals if s == "measure" else 0)) - \
                (self.dispatch_short if s == "measure" else 0) - rejected
            if rejected:
                self.no_ready[target] = self.no_ready.get(target, 0) + rejected
            fx = self._variant_pods(target)
            d = self.dispatched.setdefault(target, {})
            off = self.rr_next.get(target, 0) % len(fx)           # the rotation continues across scenarios
            for i, name in enumerate(fx):
                d[name] = d.get(name, 0) + arrivals // len(fx) + (1 if (i - off) % len(fx) < arrivals % len(fx) else 0)
            self.rr_next[target] = off + arrivals
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
        """A query_range answer as the real API gives it (status success, resultType matrix)."""
        doc = self._answer(url)
        doc.setdefault("status", "success")
        doc.setdefault("data", {}).setdefault("resultType", "matrix")
        return doc

    def _answer(self, url):
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
                {"metric": {"nodename": n}, "values": [[t, str(self.steal.get(n, 1.0))] for t in g60]}
                for n in ("worker-w", "worker-s")]}}
        if "c28_ingress_no_ready_endpoints_total" in query:
            if target not in self.no_ready:                   # never rejected: the counter does not exist yet
                return {"status": "success", "data": {"result": []}}
            return self._series([t for t in grid if self._alive(lambda n: "-ing-" in n, t)],
                                lambda t: self.no_ready[target])
        if "c28_ingress_discovery" in query or "c28_ingress_endpoint{" in query or "c28_ingress_dispatched" in query:
            return self._discovery(query, grid, target, pods)
        if "timestamp(" in query:
            return self._raw(query, grid, pods)
        self._q_pods = pods
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
        target_pods = (lambda t: self._alive(lambda n: n.startswith(f"{target}-") and "-ing-" not in n, t))
        if kind == "fixture_requests":
            if not self.direct_traffic:
                return {"status": "success", "data": {"result": []}}
            return self._series([t for t in grid if target_pods(t)], lambda t: t - start)
        if kind == "inflight":
            return self._series([t for t in grid if target_pods(t)],
                                lambda t: 0.5 if self._window(target=target, t=t) else 0)
        if kind in ("req", "resp"):
            c = self.counters.get(target)
            if c is None:
                return {"status": "success", "data": {"result": []}}
            return self._series([t for t in grid if self._alive(lambda n: "-ing-" in n, t)], lambda t: c[kind])
        if kind == "up":
            live = sorted(o["metadata"]["name"] for (k, _, _), o in self.api.objects.items()
                          if k == "pods" and re.fullmatch(pods, o["metadata"]["name"]))
            series_ = [{"metric": {self.up_label: n}, "values": [[t, "1"] for t in grid if self._born_ok(n, t)]}
                       for n in live]
            return {"status": "success", "data": {"result": [x for x in series_ if x["values"]]}}
        if kind == "backlog":
            grid = [t for t in grid if target_pods(t)]

            def backlog(t):
                w = self._window(target=target, t=t)
                if not w:
                    return 0
                return 0.2 if w["ok"] else 0.2 + (t - w["ws"]) / 60
            return self._series(grid, backlog)
        comp = "generator" if "-gen-" in pods else "ingress" if "-ing-" in pods else "fixture"
        grid = [t for t in grid if self._alive(lambda n: bool(re.fullmatch(pods, n)), t)]
        if kind == "throttle":
            return self._series(grid, lambda t: {"generator": self.gen_throttle, "ingress": 0.0}.get(comp, 0.1))
        if comp != "fixture":
            return self._series(grid, lambda t: 0.1 if comp == "generator" else 0.05)
        variant = pods.split("-[")[0]

        def cpu(t):
            w = self._window(target=variant, t=t)
            return 0.002 + (w["rate_rps"] * self.cost_ms[w["wi"]] / 1000 if w else 0)
        return self._series(grid, cpu)

    def _raw(self, query, grid, pods):
        """timestamp() queries: one series per live pod matching the regex (or per node for steal), valued with the
        raw sample's timestamp: raw_age seconds old, unless the source is in raw_missing or raw_stale."""
        if "node_cpu_seconds_total" in query:
            label, names = "nodename", ["worker-w", "worker-s"]
        else:
            label = "pod"
            names = sorted(o["metadata"]["name"] for (k, _, _), o in self.api.objects.items()
                           if k == "pods" and re.fullmatch(pods, o["metadata"]["name"]))
        series_ = [{"metric": {label: n}, "values": [[t, str(t - (200 if n in self.raw_stale else self.raw_age))]
                                                     for t in grid if label == "nodename" or self._born_ok(n, t)]}
                   for n in names if n not in self.raw_missing]
        return {"status": "success", "data": {"result": [x for x in series_ if x["values"]]}}

    def _variant_pods(self, target):
        return sorted(o["metadata"]["name"] for (k, _, n), o in self.api.objects.items()
                      if k == "pods" and n.startswith(f"{target}-") and "-ing-" not in n and "-gen-" not in n)

    def _discovery(self, query, grid, target, pods):
        """The ingress's discovery metrics: per live ingress pod synced/stale (and the raw timestamp of its scrape);
        per endpoint the membership series' raw timestamps (the same scrape) and the dispatch counters."""
        if self.disc_missing:
            return {"status": "success", "data": {"result": []}}
        ing = [n for (k, _, n) in self.api.objects if k == "pods" and "-ing-" in n]
        if "c28_ingress_dispatched" in query:
            d = self.dispatched.get(target) or {}
            g = [t for t in grid if self._alive(lambda n: "-ing-" in n, t)]
            return {"status": "success", "data": {"result": [
                {"metric": {"endpoint_pod": n}, "values": [[t, str(v)] for t in g]} for n, v in sorted(d.items())
                if g]}}
        if "c28_ingress_endpoint{" in query:
            names = (set(self._variant_pods(target)) - self.disc_hidden) | self.disc_extra
            return {"status": "success", "data": {"result": [x for x in (
                {"metric": {"endpoint_pod": n}, "values": [[t, str(t - self.raw_age)] for t in grid
                                                           if self._alive(lambda m: "-ing-" in m, t) and
                                                           (self._extra_at(n, t) if n in self.disc_extra
                                                            else self._born_ok(n, t))]}
                for n in sorted(names)) if x["values"]]}}
        timestamp = "timestamp(" in query
        synced = "discovery_synced" in query

        def value(t):
            if timestamp:
                return t - self.raw_age
            if synced:
                return 0 if self.disc_synced_from is not None and t < self.disc_synced_from else 1
            return 1 if self.disc_stale_from is not None and t >= self.disc_stale_from else 0
        return {"status": "success", "data": {"result": [x for x in (
            {"metric": {"pod": n}, "values": [[t, str(value(t))] for t in grid if self._born_ok(n, t)]}
            for n in sorted(ing) if re.fullmatch(pods, n)) if x["values"]]}}

    def _extra_at(self, name, t):
        w = self.disc_extra_window
        return w is None or w[0] <= t <= w[1]

    def _born_ok(self, name, t):
        """A pod has samples only from its creation plus the first scrape delay on (never before it existed)."""
        return name in self.born and t >= self.born[name] + self.scrape_delay

    def _alive(self, pred, t):
        return any(self._born_ok(n, t) for (k, _, n) in self.api.objects if k == "pods" and pred(n))

    def _window(self, target, t):
        return next((w for w in self.windows if w["target"] == target and w["start"] <= t <= w["end"]), None)

    @staticmethod
    def _series(grid, fn):
        if not grid:
            return {"status": "success", "data": {"result": []}}
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
