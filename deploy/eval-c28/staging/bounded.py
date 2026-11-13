#!/usr/bin/env python3
"""The live demonstration's bounded pre-history workloads (ADMISSION-DESIGN draft 2; Codex GO for this helper):

  bounded.py --record R plan-admission --origin <UTC whole minute>      read-only: the reviewed plan and its digest
  bounded.py --record R run-admission --plan F --plan-digest D           create, observe, collect, judge, clean up
  bounded.py --record R recover-plan | recover --plan F --plan-digest D

(The startup step is added by the next slice; its label is already fenced here.)

The admission exercise is ONE Job (demo.bounded_objects, label admission): both targets at the 28-day table's maximum
for 40 minutes (10 settling, 30 measured), restartPolicy Never, backoffLimit 0, a deadline of 5 min before the origin
+ 40 min + 3 min drain. It never shares the namespace with another generator-like workload: the continuous generator
(prepare.py phase generator), an earlier admission or a startup Job, any of their pods (Terminating included) and any
unresolved intent of this helper all refuse it; the check runs in the plan, again under the shared lock, and again
immediately before each create (prepare.py's generator phase refuses symmetrically).

The plan binds the rendered ConfigMap and Job (their sha256), the configuration digest, the origin, the generator
image, both targets' and both PredictiveAutoscalers' UIDs (created by this run's prepare), the PAs' mode Recommend, both
targets at 12 Ready, the residency audit with the Job counted in place of the continuous generator, and the
reservation gate. The origin lies 2..5 minutes after the plan's apply time (checked again at apply).

Run (under the shared lock, every create and delete journalled):
  1. ConfigMap, then Job; their creation times are recorded; the Job's creation must leave the origin within 5 min.
  2. The pod must be Running before the origin, else the exercise is VOID (stopped, never shortened).
  3. Until the segment end (+ drain): every 10 s the pod census of both targets and the ingress (UIDs, container IDs,
     restart counts, the Ready condition's lastTransitionTime: a transition between polls is still seen), every 30 s
     the EndpointSlices of both targets (sampled evidence), node pressure, the generator's container state, and the
     executor's stop conditions (OOM, restart, node pressure, a failed Job, steal via the observer); the k6 log is
     pulled every 30 s and appended to a durable file (lines read twice are dropped by the reducer). A stop condition
     deletes the Job at once (journalled) and the exercise is STOPPED with its partial evidence kept.
  4. After the end: the Job's terminal status and the container's exit code, a final full log read, the observer's
     settle wait (2 scrape intervals + the remote-write push interval), then the VM evidence: RAW ingress samples,
     the waiting queue (15 s steps), throttled fractions, steal (60 s steps), per-pod request increases over M and
     every series' staleness. Everything is written to the evidence directory with checksums; the verdict
     (c28.demo_admission) is computed from the written evidence file.
  5. Cleanup only after the evidence is written: Job deleted by UID, its pod deleted by UID, the pod's disappearance
     proven as a journalled step (bounded-pod-gone, reconciler registered), then the ConfigMap. An unproven
     disappearance keeps the step unresolved and the lock held.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import common_records as C
import execlib as X
import prepare

HELPER = "bounded"
GONE_OP = "bounded-pod-gone"
LABELS = ("admission", "startup")
POLL_S, SAMPLE_S, LOG_S = 10, 30, 30
ORIGIN_MIN_S, ORIGIN_MAX_S = 120, 300
REPLICAS = 12
SETTLE_EXTRA_S = 2 * 20 + 5                  # two vmagent scrape intervals + the k6 remote-write push interval
Fetch = Callable[[str], Dict]


def _ms(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


def _utc(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Ctx:
    def __init__(self, record: X.Record, kube: X.Kube, now: Callable[[], float] = lambda: time.time() * 1000,
                 sleep: Callable[[float], None] = time.sleep):
        self.record, self.kube, self.now, self.sleep = record, kube, now, sleep
        self.s = prepare._section(record)
        if prepare.stage_of(self.s) != "demonstration":
            raise X.StagingError("bounded workloads belong to the demonstration stage")
        self.params = prepare.load_params(record, "demonstration")
        self.c28 = X.import_harness()
        self.verdicts = importlib.import_module("c28.demo_admission")
        self.ns = self.params["namespace"]

    def parts_with(self, objs: List[Dict]) -> Dict:
        parts = prepare.split(self.params, "demonstration")
        parts["generator"] = objs                     # residency counts the bounded Job in the generator's place
        return parts


# --- the shared fence -------------------------------------------------------------------------------------------------

def fence_problems(c: Ctx) -> List[str]:
    """Any generator-like workload in the namespace, by name label or component, a Terminating pod included, or any
    unresolved intent of this helper or prepare's: refuse. Listings are strict (a failed list is never "none")."""
    out = []
    names = [c.c28.demo.GENERATOR_NAME] + [c.c28.demo.bounded_name(lab) for lab in LABELS]
    for kind in ("StatefulSet", "Job", "Deployment", "Pod", "ConfigMap"):
        for o in c.kube.list(kind, c.ns, strict=True):
            ls = (o.get("metadata") or {}).get("labels") or {}
            if ls.get("app.kubernetes.io/name") in names or o["metadata"]["name"] in names:
                state = " (Terminating)" if o["metadata"].get("deletionTimestamp") else ""
                out.append(f"{kind} {c.ns}/{o['metadata']['name']}{state} exists: one generator-like workload at a time")
    for h in (HELPER, "prepare"):
        if X.open_intents(X.receipts(c.record, h)):
            out.append(f"{h} has intents with unknown outcomes: recover first")
    return out


# --- the plan ---------------------------------------------------------------------------------------------------------

def _targets_state(c: Ctx) -> Tuple[Dict, List[str]]:
    """Both targets and both PAs: created by this run, the same objects, PAs in Recommend, targets at 12 Ready."""
    out, probs = {}, []
    created = prepare.created_objects(c.record)
    for name in c.c28.demo.target_names(c.params):
        dep_ref, pa_ref = created.get(("Deployment", c.ns, name)), created.get(("PredictiveAutoscaler", c.ns, name))
        dep, pa = c.kube.get("Deployment", name, c.ns), c.kube.get("PredictiveAutoscaler", name, c.ns)
        if not dep_ref or not dep or dep["metadata"]["uid"] != dep_ref["uid"]:
            probs.append(f"Deployment {name} was not created by this run or was replaced")
            continue
        if not pa_ref or not pa or pa["metadata"]["uid"] != pa_ref["uid"]:
            probs.append(f"PredictiveAutoscaler {name} was not created by this run or was replaced")
            continue
        if pa["spec"].get("mode") != "Recommend":
            probs.append(f"PredictiveAutoscaler {name} is {pa['spec'].get('mode')}, not Recommend")
        st = dep.get("status") or {}
        if int(dep["spec"].get("replicas") or 0) != REPLICAS or st.get("readyReplicas") != REPLICAS \
                or st.get("updatedReplicas") != REPLICAS or st.get("replicas") != REPLICAS:
            probs.append(f"Deployment {name} is not at {REPLICAS} Ready (spec {dep['spec'].get('replicas')}, "
                         f"ready {st.get('readyReplicas')})")
        out[name] = {"deployment_uid": dep["metadata"]["uid"], "generation": dep["metadata"].get("generation"),
                     "pa_uid": pa["metadata"]["uid"], "pa_generation": pa["metadata"].get("generation")}
    return out, probs


def plan_admission(c: Ctx, origin_utc: str) -> Dict:
    origin = _ms(origin_utc)
    objs = c.c28.demo.bounded_objects(c.params, "admission", origin)
    cfg = json.loads(objs[0]["data"][c.c28.demo.GENERATOR_CONFIG])
    parts = c.parts_with(objs)
    problems = prepare.admission_problems(c.record, c.params, c.s, parts)
    gate, gp = prepare.demonstration_gate(c.record, c.kube, c.params, c.s, parts)
    problems += gp + fence_problems(c)
    targets, tp = _targets_state(c)
    problems += tp
    lead = (origin - c.now()) / 1000
    if not ORIGIN_MIN_S <= lead <= ORIGIN_MAX_S + 600:      # the plan may be reviewed for up to 10 more minutes
        problems.append(f"the origin is {lead:.0f} s ahead; it must be {ORIGIN_MIN_S}..{ORIGIN_MAX_S} s ahead at "
                        "apply (and at most 10 minutes more at plan time)")
    actions = [{"create": o["kind"], "namespace": c.ns, "name": o["metadata"]["name"], "sha256": X.json_digest(o)}
               for o in objs]
    return X.new_plan(HELPER, c.record, actions, problems=problems, label="admission", origin_utc=origin_utc,
                      config_digest=cfg["digest"], image=c.params["demonstration"]["generator"]["image"],
                      targets=targets, reservation_gate=gate,
                      deadline_s=c.c28.demo.bounded_deadline_s("admission"))


# --- observation --------------------------------------------------------------------------------------------------

class Observer:
    """Everything the run records while it runs; written to files as it goes."""

    def __init__(self, c: Ctx, out_dir: str, expected: Dict[str, List[str]]):
        self.c, self.dir, self.expected = c, out_dir, expected
        self.pods: Dict[str, Dict] = {}                 # uid -> last seen {name, ready_since, restarts, containers}
        self.transitions: List[Dict] = []
        self.watch: List[Dict] = []
        self.endpoints: List[Dict] = []
        self.stops: List[Dict] = []

    def _pods(self) -> List[Dict]:
        names = set(self.c.c28.demo.target_names(self.c.params)) | {"c28-ingress"}
        return [p for p in self.c.kube.list("Pod", self.c.ns, strict=True)
                if ((p["metadata"].get("labels") or {}).get("c28.devkuban.com/target") in names
                    or p["metadata"]["name"].startswith("c28-ingress-"))]

    def poll_pods(self, t_s: float) -> None:
        try:
            pods = self._pods()
        except (C.ApiError, X.StagingError):
            return                                      # a failed poll is a gap in the watch (recorded by absence)
        if self.watch and t_s - self.watch[-1]["to_s"] <= POLL_S * 3:
            self.watch[-1]["to_s"] = t_s
        else:
            self.watch.append({"from_s": t_s, "to_s": t_s})
        seen = set()
        for p in pods:
            uid = p["metadata"]["uid"]
            seen.add(uid)
            st = p.get("status") or {}
            ready = next((x for x in st.get("conditions") or [] if x.get("type") == "Ready"), {})
            cs = {x["name"]: {"id": x.get("containerID"), "restarts": x.get("restartCount", 0),
                              "oom": ((x.get("lastState") or {}).get("terminated") or {}).get("reason") == "OOMKilled"}
                  for x in st.get("containerStatuses") or []}
            now = {"name": p["metadata"]["name"], "ready": ready.get("status"), "since": ready.get("lastTransitionTime"),
                   "containers": cs, "deleting": bool(p["metadata"].get("deletionTimestamp"))}
            old = self.pods.get(uid)
            if old is not None:
                if (old["ready"], old["since"]) != (now["ready"], now["since"]):
                    self.transitions.append({"t_s": t_s, "pod_uid": uid, "what": "ready-lost"
                                             if now["ready"] != "True" else "ready-gained", "since": now["since"]})
                for k, v in cs.items():
                    o = old["containers"].get(k)
                    if o and v["restarts"] != o["restarts"]:
                        self.transitions.append({"t_s": t_s, "pod_uid": uid, "what": "restart", "container": k})
                    if o and v["id"] != o["id"]:
                        self.transitions.append({"t_s": t_s, "pod_uid": uid, "what": "container-changed",
                                                 "container": k})
                    if v["oom"] and not (o or {}).get("oom"):
                        self.transitions.append({"t_s": t_s, "pod_uid": uid, "what": "oom", "container": k})
                if now["deleting"] and not old["deleting"]:
                    self.transitions.append({"t_s": t_s, "pod_uid": uid, "what": "deleted"})
            elif self.pods:
                self.transitions.append({"t_s": t_s, "pod_uid": uid, "what": "appeared", "name": now["name"]})
            self.pods[uid] = now
        for uid in set(self.pods) - seen:
            if not self.pods[uid].get("gone"):
                self.transitions.append({"t_s": t_s, "pod_uid": uid, "what": "deleted"})
                self.pods[uid]["gone"] = True

    def sample_endpoints(self, t_s: float) -> None:
        for t in self.c.c28.demo.target_names(self.c.params):
            try:
                slices = self.c.kube.list("EndpointSlice", self.c.ns, labels={"kubernetes.io/service-name": t},
                                          strict=True)
            except (C.ApiError, X.StagingError):
                continue
            ready = sorted({(e.get("targetRef") or {}).get("uid") for s in slices for e in s.get("endpoints") or []
                            if (e.get("conditions") or {}).get("ready") and (e.get("targetRef") or {}).get("uid")})
            self.endpoints.append({"t_s": t_s, "target": t, "ready": ready})

    def node_stops(self) -> List[str]:
        out = []
        for role in ("workload", "support"):
            n = self.c.kube.get("Node", self.c.params["nodes"][role])
            conds = {x["type"]: x["status"] for x in (n or {}).get("status", {}).get("conditions", [])}
            for k in ("MemoryPressure", "DiskPressure", "PIDPressure"):
                if conds.get(k) != "False":
                    out.append(f"{role} node {k} {conds.get(k, 'unknown')}")
        return out


# --- the observer's evidence (VictoriaMetrics, read-only) -------------------------------------------------------------

def http_json(url: str) -> Dict:
    with urllib.request.urlopen(url, timeout=60) as r:          # the reviewed observer endpoint (a local forward)
        return json.loads(r.read())


def _q(fetch: Fetch, api: str, path: str, **params) -> List[Dict]:
    doc = fetch(f"{api.rstrip('/')}/api/v1/{path}?{urllib.parse.urlencode(params)}")
    if doc.get("status") != "success":
        raise X.StagingError(f"the observer refused {path}: {str(doc.get('error'))[:200]}")
    return doc["data"]["result"]


def _raw(fetch, api, selector: str, lo_s: float, hi_s: float) -> List[Dict]:
    """RAW samples (a range-vector instant query at hi_s: never a stepped range query, which hides scrape gaps)."""
    return _q(fetch, api, "query", query=f"{selector}[{int(hi_s - lo_s)}s]", time=f"{hi_s:.3f}")


def _range(fetch, api, expr: str, lo_s: float, hi_s: float, step: int) -> List[List[float]]:
    res = _q(fetch, api, "query_range", query=expr, start=f"{lo_s:.0f}", end=f"{hi_s:.0f}", step=f"{step}s")
    if len(res) != 1:
        return []
    return [[float(t), float(v)] for t, v in res[0]["values"]]


def _staleness(samples: List[List[float]], lo_s: float, hi_s: float) -> Optional[float]:
    ts = sorted(t for t, _ in samples if lo_s <= t <= hi_s)
    if not ts:
        return None
    return max([ts[0] - lo_s, hi_s - ts[-1]] + [b - a for a, b in zip(ts, ts[1:])])


def _worst(series: List[Dict], lo_s: float, hi_s: float) -> Optional[float]:
    """The worst staleness over every raw series (None: no series at all, or one without a sample in the window)."""
    vals = [_staleness([[float(t), 0.0] for t, _ in s["values"]], lo_s, hi_s) for s in series]
    return None if not vals or any(v is None for v in vals) else max(vals)


def collect_vm(c: Ctx, fetch: Fetch, api: str, origin_s: float, gen_pod: str, pods: Dict[str, Dict]) -> Dict:
    ns, names = c.ns, c.c28.demo.target_names(c.params)
    lo, m_lo, hi = origin_s, origin_s + 600, origin_s + 2400
    out = {"ingress": {}, "queue": {}, "throttled": {}, "steal": {}, "pod_requests": {}, "freshness": {},
           "problems": []}
    for t in names:
        series = _raw(fetch, api, f'c28_ingress_requests_total{{namespace="{ns}",target="{t}"}}', lo - 120, hi + 120)
        if len(series) == 1:
            out["ingress"][t] = [[float(a), float(b)] for a, b in series[0]["values"]]
            out["freshness"][f"ingress/{t}"] = _staleness(out["ingress"][t], lo, hi)
        else:
            out["problems"].append(f"ingress {t}: {len(series)} raw series (one expected)")
            out["freshness"][f"ingress/{t}"] = None
        out["queue"][t] = _range(fetch, api, f'sum(c28_queue{{namespace="{ns}",target="{t}"}})', m_lo, hi, 15)
        out["freshness"][f"queue/{t}"] = _worst(_raw(fetch, api, f'c28_queue{{namespace="{ns}",target="{t}"}}',
                                                      m_lo, hi), m_lo, hi)
        inc = _q(fetch, api, "query", query=f'sum by (pod) (increase(c28_requests_total{{namespace="{ns}",'
                                                f'target="{t}"}}[{hi - m_lo:.0f}s]))', time=f"{hi:.3f}")
        by_name = {v["name"]: u for u, v in pods.items()}
        out["pod_requests"][t] = {by_name.get(r["metric"].get("pod"), "unknown:" + str(r["metric"].get("pod"))):
                                  float(r["value"][1]) for r in inc}
    regex = {"generator": gen_pod, "ingress": "c28-ingress-.*",
             "fixture": "|".join(f"{t}-.*" for t in names)}
    for who, rx in regex.items():
        expr = (f'sum(rate(container_cpu_cfs_throttled_periods_total{{namespace="{ns}",pod=~"{rx}",container!=""}}[1m]))'
                f' / sum(rate(container_cpu_cfs_periods_total{{namespace="{ns}",pod=~"{rx}",container!=""}}[1m]))')
        out["throttled"][who] = _range(fetch, api, expr, m_lo, hi, 15)
        out["freshness"][f"throttled/{who}"] = _worst(_raw(fetch, api, f'container_cpu_cfs_periods_total{{namespace='
                                                           f'"{ns}",pod=~"{rx}",container!=""}}', m_lo, hi), m_lo, hi)
    nodes = [c.params["nodes"]["workload"], c.params["nodes"]["support"]]
    sel = "|".join(nodes)
    steal = (f'100 * sum by (nodename) (rate(node_cpu_seconds_total{{job="node-exporter",mode="steal"}}[1m]) * on '
             f'(instance) group_left (nodename) node_uname_info{{job="node-exporter",nodename=~"{sel}"}}) / count by '
             f'(nodename) (node_cpu_seconds_total{{job="node-exporter",mode="idle"}} * on (instance) group_left '
             f'(nodename) node_uname_info{{job="node-exporter",nodename=~"{sel}"}})')
    res = _q(fetch, api, "query_range", query=steal, start=f"{lo:.0f}", end=f"{hi:.0f}", step="60s")
    for r in res:
        node = r["metric"].get("nodename")
        role = "w" if node == nodes[0] else "s" if node == nodes[1] else None
        if role:
            out["steal"][role] = [[float(t), float(v)] for t, v in r["values"]]
            out["freshness"][f"steal/{role}"] = _staleness(out["steal"][role], lo, hi)
    return out


# --- the run -----------------------------------------------------------------------------------------------------------

def _logs(c: Ctx, pod: str, run: Callable = subprocess.run) -> List[str]:
    k = c.record.doc["kubeconfig"]
    p = run(["kubectl", "--kubeconfig", k["path"], "--context", k["context"], "-n", c.ns, "logs", pod, "-c", "k6"],
            capture_output=True, text=True, timeout=120)
    return (p.stdout or "").splitlines() if p.returncode == 0 else []


def _job_pod(c: Ctx, name: str, job_uid: str) -> Optional[Dict]:
    pods = [p for p in c.kube.list("Pod", c.ns, strict=True)
            if any(r.get("uid") == job_uid for r in p["metadata"].get("ownerReferences") or [])]
    return pods[0] if len(pods) == 1 else None


def _terminal(job: Optional[Dict]) -> Optional[str]:
    for cond in ((job or {}).get("status") or {}).get("conditions") or []:
        if cond.get("type") in ("Complete", "Failed") and cond.get("status") == "True":
            return cond["type"]
    return None


def _exit_code(pod: Optional[Dict]) -> Optional[int]:
    for cs in ((pod or {}).get("status") or {}).get("containerStatuses") or []:
        term = (cs.get("state") or {}).get("terminated")
        if cs.get("name") == "k6" and term:
            return term.get("exitCode")
    return None


def _prove_gone(c: Ctx, kind: str, name: str, uid: str, seconds: float) -> Dict:
    end = c.now() + seconds * 1000
    while True:
        o = c.kube.get(kind, name, c.ns)
        if o is None or o["metadata"].get("uid") != uid:
            return {"metadata": {"uid": uid}}
        if c.now() >= end:
            raise X.StagingError(f"{kind}/{name} is still present {seconds:.0f} s after its delete")
        c.sleep(2)


def _reconcile_gone(kube: X.Kube, d: Dict, record) -> Dict:
    o = kube.get(d["kind"], d["name"], d["namespace"])
    if o is None or o["metadata"].get("uid") != d["uid"]:
        return {"observed": "done", "uid": d["uid"]}
    return {"observed": "unresolved", "why": f"{d['kind']}/{d['name']} still exists"}


X.RECONCILERS[GONE_OP] = _reconcile_gone


def cleanup(c: Ctx, made: Dict[str, Dict], pod: Optional[Dict]) -> None:
    """Job by UID, its pod by UID, the pod's disappearance proven (journalled), then the ConfigMap."""
    if "Job" in made:
        if c.kube.get("Job", made["Job"]["name"], c.ns) is not None:
            c.kube.delete("Job", made["Job"]["name"], c.ns, made["Job"]["uid"])
    if pod is not None:
        name, uid = pod["metadata"]["name"], pod["metadata"]["uid"]
        live = c.kube.get("Pod", name, c.ns)
        if live is not None and live["metadata"]["uid"] == uid:
            c.kube.delete("Pod", name, c.ns, uid)
        c.kube.step(GONE_OP, "Pod", c.ns, name, uid, lambda mark: _prove_gone(c, "Pod", name, uid, 180))
    if "ConfigMap" in made:
        c.kube.delete("ConfigMap", made["ConfigMap"]["name"], c.ns, made["ConfigMap"]["uid"])


def _write(path: str, doc) -> str:
    text = json.dumps(doc, indent=1, sort_keys=True) + "\n"
    return X.write_new(path, text)


def run_admission(c: Ctx, reviewed: Dict, reviewed_digest: str, fetch: Fetch, api: str,
                  logs: Callable[[Ctx, str], List[str]] = _logs) -> Dict:
    rid = f"a-{reviewed_digest[:12]}"
    out_dir = os.path.join(c.record.evidence_dir, f"admission-{rid}")
    with X.mutating(c.record, c.kube, HELPER, {"run": rid, "label": "admission"}) as lock:
        p = plan_admission(c, reviewed["origin_utc"])            # recomputed under the shared lock
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
        if p["problems"]:
            raise X.StagingError("the plan has problems: " + "; ".join(p["problems"]))
        origin = _ms(p["origin_utc"])
        lead = (origin - c.now()) / 1000
        if not ORIGIN_MIN_S <= lead <= ORIGIN_MAX_S:
            raise X.StagingError(f"the origin is {lead:.0f} s ahead at apply; it must be {ORIGIN_MIN_S}..{ORIGIN_MAX_S}")
        os.makedirs(out_dir)
        objs = {o["kind"]: o for o in c.c28.demo.bounded_objects(c.params, "admission", origin)}
        for a in p["actions"]:
            if X.json_digest(objs[a["create"]]) != a["sha256"]:
                raise X.StagingError(f"{a['create']} rendered differently from the reviewed plan; refusing")
        X.receipt(c.record, HELPER, "run-begin", {"run": rid, "plan_sha256": reviewed_digest, "label": "admission",
                                                   "origin_utc": p["origin_utc"], "dir": out_dir})
        made: Dict[str, Dict] = {}
        pod, status, stop_reason = None, "", ""
        expected = {t: [] for t in c.c28.demo.target_names(c.params)}
        obs = Observer(c, out_dir, expected)
        log_path = os.path.join(out_dir, "k6.log")
        try:
            for kind in ("ConfigMap", "Job"):
                why = fence_problems(c) if kind == "ConfigMap" else []
                if why:
                    raise X.StagingError("fence: " + "; ".join(why))
                made[kind] = X.object_ref(c.kube.create(objs[kind]))
            job = c.kube.get("Job", made["Job"]["name"], c.ns)
            created_ms = _ms(job["metadata"]["creationTimestamp"])
            if origin - created_ms > ORIGIN_MAX_S * 1000:
                raise X.StagingError("VOID: the Job was created more than 5 minutes before the origin")
            obs.poll_pods(c.now() / 1000)                         # the baseline census
            baseline_s = c.now() / 1000
            for name in expected:
                expected[name] = sorted(u for u, v in obs.pods.items() if v["name"].startswith(f"{name}-"))
            while True:                                           # the pod must be Running before the origin
                pod = _job_pod(c, made["Job"]["name"], made["Job"]["uid"])
                if pod is not None and (pod.get("status") or {}).get("phase") == "Running":
                    break
                if c.now() >= origin:
                    raise X.StagingError("VOID: the admission pod was not Running before the origin")
                c.sleep(2)
            end_ms = origin + 40 * 60_000 + 30_000
            next_sample = next_log = 0.0
            while c.now() < end_ms:
                t = c.now() / 1000
                obs.poll_pods(t)
                if t >= next_sample:
                    obs.sample_endpoints(t)
                    next_sample = t + SAMPLE_S
                    stops = obs.node_stops()
                    stops += [f"{x['what']} of pod {x['pod_uid']}" for x in obs.transitions
                              if x["what"] in ("oom", "restart") and x["t_s"] >= baseline_s]
                    job = c.kube.get("Job", made["Job"]["name"], c.ns)
                    if _terminal(job) == "Failed":
                        stops.append("the admission Job failed before its end")
                    if stops:
                        stop_reason = "; ".join(stops)
                        obs.stops.append({"t_s": t, "why": stop_reason})
                        break
                if t >= next_log:
                    with open(log_path, "a") as fh:
                        fh.write("\n".join(logs(c, pod["metadata"]["name"])) + "\n")
                    next_log = t + LOG_S
                c.sleep(POLL_S)
            if stop_reason:
                X.receipt(c.record, HELPER, "run-stopped", {"run": rid, "why": stop_reason})
                c.kube.delete("Job", made["Job"]["name"], c.ns, made["Job"]["uid"])
                made.pop("Job")
            else:
                deadline = c.now() + 180_000
                while _terminal(c.kube.get("Job", made["Job"]["name"], c.ns)) is None and c.now() < deadline:
                    obs.poll_pods(c.now() / 1000)
                    c.sleep(POLL_S)
            completion_s = c.now() / 1000
            obs.poll_pods(completion_s)
            job = c.kube.get("Job", made["Job"]["name"], c.ns) if "Job" in made else None
            pod_now = c.kube.get("Pod", pod["metadata"]["name"], c.ns)
            status, code = _terminal(job) or ("Stopped" if stop_reason else "Unknown"), _exit_code(pod_now)
            lines = logs(c, pod["metadata"]["name"])
            with open(log_path, "a") as fh:
                fh.write("\n".join(lines) + "\n")
            c.sleep(SETTLE_EXTRA_S)
            vm = collect_vm(c, fetch, api, origin / 1000, pod["metadata"]["name"], obs.pods)
            with open(log_path) as fh:
                all_lines = [ln for ln in fh.read().splitlines() if ln.strip()]
            cfg = json.loads(objs["ConfigMap"]["data"][c.c28.demo.GENERATOR_CONFIG])
            ev = {"schema": c.verdicts.SCHEMA, "config": cfg, "lines": all_lines,
                  "ingress": vm["ingress"], "queue": vm["queue"], "throttled": vm["throttled"], "steal": vm["steal"],
                  "endpoints": obs.endpoints, "pod_requests": vm["pod_requests"],
                  "pods": {"expected": expected, "baseline_s": baseline_s, "completion_s": completion_s,
                           "watch": obs.watch, "transitions": obs.transitions},
                  "freshness": vm["freshness"],
                  "job": {"status": status, "exit_code": code, "created_s": created_ms / 1000},
                  "stops": obs.stops, "limits": c.verdicts.FROZEN, "collection_problems": vm["problems"]}
            ev_sha = _write(os.path.join(out_dir, "evidence.json"), ev)
            with open(os.path.join(out_dir, "evidence.json")) as fh:
                verdict = c.verdicts.verdict(json.load(fh))      # from the WRITTEN file
            v_sha = _write(os.path.join(out_dir, "verdict.json"), verdict)
        finally:
            if made or pod is not None:
                cleanup(c, made, pod)
        return X.receipt(c.record, HELPER, "run-complete", {"run": rid, "plan_sha256": reviewed_digest,
                                                             "verdict": verdict["verdict"], "evidence_sha256": ev_sha,
                                                             "verdict_sha256": v_sha, "dir": out_dir,
                                                             "stopped": stop_reason or None})


def main(argv: Optional[List[str]] = None, api=None, fetch: Fetch = http_json) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--record", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    pa = sub.add_parser("plan-admission")
    pa.add_argument("--origin", required=True)
    ra = sub.add_parser("run-admission")
    ra.add_argument("--plan", required=True)
    ra.add_argument("--plan-digest", required=True)
    ra.add_argument("--query-api", required=True, help="the observer's Prometheus API base (a local forward)")
    sub.add_parser("recover-plan")
    rc = sub.add_parser("recover")
    rc.add_argument("--plan", required=True)
    rc.add_argument("--plan-digest", required=True)
    a = ap.parse_args(argv)
    record = X.load_record(a.record, () if a.cmd.startswith("recover") else ("prepare",))
    kube = record.kube(api)
    if a.cmd == "recover-plan":
        X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
    elif a.cmd == "recover":
        X.emit(X.recover(record, kube, X.load_plan(a.plan), a.plan_digest))
    elif a.cmd == "plan-admission":
        X.emit(X.cli_plan(record, HELPER, plan_admission(Ctx(record, kube), a.origin), "plan-admission"))
    else:
        X.emit(run_admission(Ctx(record, kube), X.load_plan(a.plan), a.plan_digest, fetch, a.query_api))
    return 0


if __name__ == "__main__":
    sys.exit(main())
