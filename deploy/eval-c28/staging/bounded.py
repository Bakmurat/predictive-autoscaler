#!/usr/bin/env python3
"""The live demonstration's bounded pre-history workloads (ADMISSION-DESIGN draft 2; Codex GO for this helper):

  bounded.py --record R plan-admission --origin <UTC whole minute>      read-only: the reviewed plan and its digest
  bounded.py --record R run-admission --plan F --plan-digest D           create, observe, collect, judge, clean up
  bounded.py --record R recover-plan | recover --plan F --plan-digest D

  bounded.py --record R plan-startup --origin <UTC whole minute>        read-only (requires the admission PASS)
  bounded.py --record R run-startup --plan F --plan-digest D             12 -> 6 idle, peak load, 6 -> 12, measure

The startup step (section 4, Review 1 item 6): the restore obligation (the reactive target's UID back to 12) is a
receipt written BEFORE the first write; 12 -> 6 while idle (journalled /scale with the resourceVersion read just
before, then 6 Ready); the startup Job (one target, 15 minutes, constant peak); at origin + 60 s (at most 15 s late)
6 -> 12; each new pod's created / scheduled / started / Ready; then cleanup, and the obligation met (same UID at 12
Ready) before it closes. restore meets every open obligation (stop the load, prove it gone, restore 12); it never starts a
workload. An unmet obligation or an unproven disappearance keeps the fence.
After an interruption: recover-plan / recover decide the open intents, then plan-restore / restore meet every open
obligation (as session.py's restore); until then every bounded plan and prepare's generator phase refuse.

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
  3. Until the segment end (+ drain), by POLLING (not a watch stream): every 10 s the pod census of both targets and
     the ingress (UIDs, container IDs, restart counts, the Ready condition's lastTransitionTime, so a transition between
     polls is still seen; every raw poll kept in pod-polls.jsonl; a gap above 30 s breaks the coverage, which then
     fails closed); the baseline census must hold exactly 12 Ready pods per target or the run is VOID. Every 30 s the
     EndpointSlices (sampled evidence), a log pull to a durable file, and the LIVE stop conditions (live_stops): node
     pressure, an OOM kill, restart, deleted pod or changed container, a failed Job, guest steal above 5 % for 5
     one-minute bins on W or S (a missing or stale steal observation stops too), a firing neighbour alert, an observer
     failure, and from origin + 30 s the first-send deadline (a validated first send within 10 s of the origin for
     every target, else VOID). A stop or VOID is recorded and the load is stopped at once.
  4. The Job's terminal status, the container's exit code and the final log are captured BEFORE the load is stopped;
     then stop_load (the recorded Job by its recorded UID, every pod recorded or owned by it, found without the Job,
     deleted by UID and each proven gone as a journalled step); only then the observer's evidence (RAW samples for
     every freshness source), evidence.json and verdict.json from the written file. Ownership (ConfigMap, Job, pod
     UIDs) is a receipt the moment each is known; cleanup and recovery act only on those identities.
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
        self.reduce = importlib.import_module("c28.demo_reduce")
        self.ns = self.params["namespace"]

    def parts_with(self, objs: List[Dict]) -> Dict:
        parts = prepare.split(self.params, "demonstration")
        parts["generator"] = objs                     # residency counts the bounded Job in the generator's place
        return parts


# --- the shared fence -------------------------------------------------------------------------------------------------

def fence_problems(c: Ctx, own_run: Optional[str] = None) -> List[str]:
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
    if [o for o in open_obligations(c.record) if o["run"] != own_run]:      # the caller's own obligation excepted
        out.append("a startup restore obligation is open: bounded.py plan-restore / restore first")
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
        self.failures: List[Dict] = []

    def _pods(self) -> List[Dict]:
        names = set(self.c.c28.demo.target_names(self.c.params)) | {"c28-ingress"}
        return [p for p in self.c.kube.list("Pod", self.c.ns, strict=True)
                if ((p["metadata"].get("labels") or {}).get("c28.devkuban.com/target") in names
                    or p["metadata"]["name"].startswith("c28-ingress-"))]

    def poll_pods(self, t_s: float) -> None:
        try:
            pods = self._pods()
        except (C.ApiError, X.StagingError) as e:
            self.failures.append({"t_s": t_s, "what": f"pod poll: {type(e).__name__}"})   # a live stop, never silent
            return
        with open(os.path.join(self.dir, "pod-polls.jsonl"), "a") as fh:        # the raw observations, kept
            fh.write(json.dumps({"t_s": t_s, "pods": [{"uid": p["metadata"]["uid"], "name": p["metadata"]["name"],
                                                         "status": p.get("status"), "deleting":
                                                             p["metadata"].get("deletionTimestamp")} for p in pods]},
                                sort_keys=True) + "\n")
        if self.watch and t_s - self.watch[-1]["to_s"] <= POLL_S * 3:
            self.watch[-1]["to_s"] = t_s
        else:
            self.watch.append({"from_s": t_s, "to_s": t_s})
        seen, first = set(), not self.pods             # the first poll is the baseline: nothing "appears" in it
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
                for k in set(old["containers"]) - set(cs):
                    self.transitions.append({"t_s": t_s, "pod_uid": uid, "what": "container-changed", "container": k})
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
            elif not first:
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
            except (C.ApiError, X.StagingError) as e:
                self.failures.append({"t_s": t_s, "what": f"endpoint sample {t}: {type(e).__name__}"})
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
    """The observer's evidence for the admission window. Every staleness comes from RAW samples (range-vector instant
    queries), for exactly demo_admission.freshness_inventory; a query error is a collection problem, never a gap
    silently filled."""
    ns, names = c.ns, c.c28.demo.target_names(c.params)
    lo, m_lo, hi = origin_s, origin_s + 600, origin_s + 2400
    out = {"ingress": {}, "queue": {}, "throttled": {}, "steal": {}, "pod_requests": {}, "freshness": {},
           "problems": []}

    def guard(key, fn, default=None):
        try:
            return fn()
        except Exception as e:                         # an observer failure is recorded, never a silent empty series
            out["problems"].append(f"{key}: {type(e).__name__}: {str(e)[:160]}")
            return default

    by_name = {v["name"]: u for u, v in pods.items()}
    for t in names:
        series = guard(f"ingress/{t}", lambda: _raw(fetch, api, f'c28_ingress_requests_total{{namespace="{ns}",'
                                                                 f'target="{t}"}}', lo - 120, hi + 120), [])
        if len(series) == 1:
            out["ingress"][t] = [[float(a), float(b)] for a, b in series[0]["values"]]
            out["freshness"][f"ingress/{t}"] = _staleness(out["ingress"][t], lo, hi)
        else:
            out["problems"].append(f"ingress {t}: {len(series)} raw series (one expected)")
        out["queue"][t] = guard(f"queue/{t}", lambda: _range(
            fetch, api, f'sum(c28_queue{{namespace="{ns}",target="{t}"}})', m_lo, hi, 15), [])
        out["freshness"][f"queue/{t}"] = guard(f"queue-raw/{t}", lambda: _worst(_raw(
            fetch, api, f'c28_queue{{namespace="{ns}",target="{t}"}}', m_lo, hi), m_lo, hi))
        out["freshness"][f"requests/{t}"] = guard(f"requests-raw/{t}", lambda: _worst(_raw(
            fetch, api, f'c28_requests_total{{namespace="{ns}",target="{t}"}}', m_lo, hi), m_lo, hi))
        inc = guard(f"requests/{t}", lambda: _q(fetch, api, "query", query=f'sum by (pod) (increase(c28_requests_total'
                    f'{{namespace="{ns}",target="{t}"}}[{hi - m_lo:.0f}s]))', time=f"{hi:.3f}"), [])
        out["pod_requests"][t] = {by_name.get(r["metric"].get("pod"), "unknown:" + str(r["metric"].get("pod"))):
                                  float(r["value"][1]) for r in inc}
    regex = {"generator": gen_pod, "ingress": "c28-ingress-.*", "fixture": "|".join(f"{t}-.*" for t in names)}
    for who, rx in regex.items():
        sel = f'{{namespace="{ns}",pod=~"{rx}",container!=""}}'
        out["throttled"][who] = guard(f"throttled/{who}", lambda: _range(
            fetch, api, f"sum(rate(container_cpu_cfs_throttled_periods_total{sel}[1m])) / "
                        f"sum(rate(container_cpu_cfs_periods_total{sel}[1m]))", m_lo, hi, 15), [])
        for key, metric in (("cfs_periods", "container_cpu_cfs_periods_total"),
                            ("cfs_throttled", "container_cpu_cfs_throttled_periods_total")):
            out["freshness"][f"{key}/{who}"] = guard(f"{key}/{who}", lambda: _worst(_raw(
                fetch, api, f"{metric}{sel}", m_lo, hi), m_lo, hi))
    nodes = {"w": c.params["nodes"]["workload"], "s": c.params["nodes"]["support"]}
    inst = guard("node instances", lambda: node_instances(c, fetch, api), {})
    for role, node in nodes.items():
        out["steal"][role] = guard(f"steal/{role}", lambda: _range(fetch, api, steal_expr([node]), lo, hi, 60), [])
        if role in inst:
            out["freshness"][f"steal_raw/{role}"] = guard(f"steal-raw/{role}", lambda: _worst(_raw(
                fetch, api, f'node_cpu_seconds_total{{job="node-exporter",mode="steal",instance="{inst[role]}"}}',
                lo, hi), lo, hi))
    out["freshness"] = {k: v for k, v in out["freshness"].items() if v is not None}
    return out


def node_instances(c: Ctx, fetch: Fetch, api: str) -> Dict[str, str]:
    """W's and S's node-exporter instance, resolved through node_uname_info (node_cpu_seconds_total carries no node
    name: evidence/calibration-observer-20261108T233404Z.md). A missing or ambiguous mapping raises."""
    nodes = {c.params["nodes"]["workload"]: "w", c.params["nodes"]["support"]: "s"}
    sel = "|".join(nodes)
    res = _q(fetch, api, "query", query=f'node_uname_info{{job="node-exporter",nodename=~"{sel}"}}')
    out: Dict[str, str] = {}
    for r in res:
        n, inst = r["metric"].get("nodename"), r["metric"].get("instance")
        if n in nodes and inst:
            if nodes[n] in out and out[nodes[n]] != inst:
                raise X.StagingError(f"node {n} maps to two node-exporter instances")
            out[nodes[n]] = inst
    if set(out) != {"w", "s"}:
        raise X.StagingError(f"node_uname_info does not map both W and S to an instance: {sorted(out)}")
    return out


def steal_expr(nodes: List[str]) -> str:
    sel = "|".join(nodes)
    return (f'100 * sum by (nodename) (rate(node_cpu_seconds_total{{job="node-exporter",mode="steal"}}[1m]) * on '
            f'(instance) group_left (nodename) node_uname_info{{job="node-exporter",nodename=~"{sel}"}}) / count by '
            f'(nodename) (node_cpu_seconds_total{{job="node-exporter",mode="idle"}} * on (instance) group_left '
            f'(nodename) node_uname_info{{job="node-exporter",nodename=~"{sel}"}})')


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


# --- ownership, stopping the load and cleanup ---------------------------------------------------------------------------

def own(c: Ctx, run: str, obj: Dict) -> Dict:
    """Record an object this run owns (kind, name, UID) as soon as it is known: recovery and cleanup act ONLY on these
    identities, never on whatever holds the name now."""
    ref = {"run": run, "kind": obj.get("kind"), "name": obj["metadata"]["name"], "uid": obj["metadata"]["uid"]}
    X.receipt(c.record, HELPER, "owned", ref)
    return ref


def owned(record: X.Record, run: str) -> Dict[str, List[Dict]]:
    """What the run owns: its "owned" receipts AND every create of this helper's journal under the run (applied, or
    reconciled as done by recover), so an interruption between a create and its receipt still leaves the object
    known by its UID."""
    out: Dict[str, List[Dict]] = {}
    seen = set()

    def add(kind, name, uid):
        if uid and uid not in seen:
            seen.add(uid)
            out.setdefault(kind, []).append({"run": run, "kind": kind, "name": name, "uid": uid})
    es = X.receipts(record, HELPER)
    for e in es:
        if e["event"] == "owned" and e["data"]["run"] == run:
            add(e["data"]["kind"], e["data"]["name"], e["data"]["uid"])
    for it in X.applied_intents(es):
        d = it["data"]
        if d["op"] == "create" and (d.get("context") or {}).get("run") == run:
            add(d["kind"], d["name"], d.get("result_uid"))
    return out


def unresolved_creates(record: X.Record, run: str) -> List[Dict]:
    return [it["data"] for it in X.open_intents(X.receipts(record, HELPER))
            if it["data"]["op"] == "create" and (it["data"].get("context") or {}).get("run") == run]


def stop_load(c: Ctx, run: str) -> None:
    """Stop every generator pod this run owns and prove each one gone: the recorded Job (by its recorded UID; another
    object holding the name is a replacement and is refused, never deleted), then every pod recorded or owned by a
    recorded Job's UID, found WITHOUT the Job (a raw-API Job delete orphans its pod), each deleted by UID and proven
    gone as a journalled step. Idempotent."""
    pending = unresolved_creates(c.record, run)
    if pending:
        raise X.StagingError(f"{[d['kind'] + '/' + d['name'] for d in pending]}: a create of this run has an unknown "
                             "outcome; recover first (the load cannot be declared stopped, the fence stays)")
    o = owned(c.record, run)
    job_uids = {j["uid"] for j in o.get("Job", [])}
    for j in o.get("Job", []):
        live = c.kube.get("Job", j["name"], c.ns)
        if live is not None and live["metadata"]["uid"] == j["uid"]:
            c.kube.delete("Job", j["name"], c.ns, j["uid"])
        elif live is not None:
            raise X.StagingError(f"Job/{j['name']} is now another object (UID {live['metadata']['uid']}): a "
                                 "replacement is never touched; refusing")
    pods = {p["uid"]: p["name"] for p in o.get("Pod", [])}
    for p in c.kube.list("Pod", c.ns, strict=True):
        if any(r.get("uid") in job_uids for r in p["metadata"].get("ownerReferences") or []):
            pods[p["metadata"]["uid"]] = p["metadata"]["name"]
    for uid, name in sorted(pods.items(), key=lambda kv: kv[1]):
        live = c.kube.get("Pod", name, c.ns)
        if live is not None and live["metadata"]["uid"] == uid:
            c.kube.delete("Pod", name, c.ns, uid)
            c.kube.step(GONE_OP, "Pod", c.ns, name, uid, lambda mark, n=name, u=uid: _prove_gone(c, "Pod", n, u, 180))


def remove_config(c: Ctx, run: str) -> None:
    for cm in owned(c.record, run).get("ConfigMap", []):
        live = c.kube.get("ConfigMap", cm["name"], c.ns)
        if live is not None and live["metadata"]["uid"] == cm["uid"]:
            c.kube.delete("ConfigMap", cm["name"], c.ns, cm["uid"])


def _write(path: str, doc) -> str:
    text = json.dumps(doc, indent=1, sort_keys=True) + "\n"
    return X.write_new(path, text)


# --- live stop conditions -------------------------------------------------------------------------------------------------

# Expected alerts reviewed as NOT a reason to stop (by exact name, with the reason); none are reviewed yet, so every
# alert the selection below matches stops the run.
REVIEWED_EXPECTED_ALERTS: Dict[str, str] = {}


def neighbour_alerts(c: Ctx, fetch: Fetch, api: str) -> List[str]:
    """Firing alerts that concern W, S or the demonstration, by an explicit mapping: a node, nodename or kubernetes_node
    label equal to W's or S's name, or an instance whose host is W's or S's node-exporter host (any port); any alert
    of the control namespace; a critical alert of the watched namespace. Only REVIEWED_EXPECTED_ALERTS are exempt.
    An observer failure raises (the caller stops: a missing observation is a stop)."""
    names = {c.params["nodes"]["workload"], c.params["nodes"]["support"]}
    hosts = {i.rsplit(":", 1)[0] for i in node_instances(c, fetch, api).values()}
    out = []
    for r in _q(fetch, api, "query", query='ALERTS{alertstate="firing"}'):
        m = r["metric"]
        name = m.get("alertname", "")
        if name in REVIEWED_EXPECTED_ALERTS:
            continue
        on_node = any(m.get(k) in names for k in ("node", "nodename", "kubernetes_node")) or \
            (m.get("instance") or "").rsplit(":", 1)[0] in hosts
        if on_node or m.get("namespace") == c.params["control_namespace"] or \
                (m.get("namespace") == c.ns and m.get("severity") == "critical"):
            out.append(f"alert {name} firing ({m.get('severity', '?')})")
    return out


def steal_stops(c: Ctx, fetch: Fetch, api: str, now_s: float) -> List[str]:
    """Guest steal above 5 % for 5 consecutive one-minute bins on W or S; a node without a recent observation is a
    stop too."""
    out = []
    for role in ("workload", "support"):
        node = c.params["nodes"][role]
        vals = _range(fetch, api, steal_expr([node]), now_s - 360, now_s, 60)
        if len(vals) < 5 or now_s - vals[-1][0] > 120:
            out.append(f"steal observation of {role} node missing or stale")
            continue
        if all(v > c.verdicts.FROZEN["steal_percent"] for _, v in vals[-c.verdicts.FROZEN["steal_bins"]:]):
            out.append(f"steal above {c.verdicts.FROZEN['steal_percent']} % for 5 minutes on the {role} node")
    return out


def first_sends(c: Ctx, cfg: Dict, lines: List[str]) -> Dict[str, Optional[int]]:
    """The validated first send of every target of the configuration (demo_reduce: a start record bound to this
    configuration and a first_send record inside its declared span), or None."""
    red = c.reduce
    recs, _ = red.parse_lines(lines)
    try:
        r = red.reduce(cfg, recs, {}, cfg["origin_ms"], cfg["origin_ms"] + 60_000)
    except ValueError:
        return {t: None for t in cfg["targets"]}
    out: Dict[str, Optional[int]] = {t: None for t in cfg["targets"]}
    for inc in r["incarnations"]:
        if inc["start_record"] == "valid" and inc["first_send_ms"] is not None:
            t = inc["target"]
            out[t] = inc["first_send_ms"] if out[t] is None else min(out[t], inc["first_send_ms"])
    return out


FIRST_SEND_MAX_MS, FIRST_SEND_CHECK_S = 10_000, 30


def live_stops(c: Ctx, obs: "Observer", fetch: Fetch, api: str, baseline_s: float, job: Optional[Dict],
               cfg: Dict, lines: List[str], label: str) -> List[str]:
    """Every live stop condition; an observer failure is itself a stop."""
    t = c.now() / 1000
    stops = [f"observation failed: {f['what']}" for f in obs.failures]
    stops += obs.node_stops()
    stops += [f"{x['what']} of pod {x['pod_uid']}" for x in obs.transitions
              if x["what"] in ("oom", "restart", "deleted", "container-changed") and x["t_s"] >= baseline_s]
    if _terminal(job) == "Failed":
        stops.append(f"the {label} Job failed before its end")
    try:
        stops += steal_stops(c, fetch, api, t)
        stops += neighbour_alerts(c, fetch, api)
    except Exception as e:
        stops.append(f"an observer query failed: {type(e).__name__}")
    if t * 1000 >= cfg["origin_ms"] + FIRST_SEND_CHECK_S * 1000:
        fs = first_sends(c, cfg, lines)
        late = [k for k, v in fs.items() if v is None or v - cfg["origin_ms"] > FIRST_SEND_MAX_MS]
        if late:
            stops.append(f"VOID: no validated first send within {FIRST_SEND_MAX_MS // 1000} s of the origin for {late}")
    return stops


def binding(c: Ctx, targets: Dict) -> Dict:
    """What an admission PASS is bound to: the params file's bytes, the target and PA identities, the generator,
    fixture and release images, the peak, the LIVE pod templates of both targets and the ingress (resources, images,
    args, env: a changed limit voids the PASS), the live PA specs, and the node environment of W and S (UID, kernel,
    kubelet, runtime, OS image, allocatable)."""
    s = c.s["release"]
    live = {}
    for name in list(targets) + ["c28-ingress"]:
        dep = c.kube.get("Deployment", name, c.ns)
        live[name] = X.json_digest((dep or {}).get("spec", {}).get("template", {}).get("spec"))
    pas = {t: X.json_digest((c.kube.get("PredictiveAutoscaler", t, c.ns) or {}).get("spec")) for t in targets}
    env = {}
    for role in ("workload", "support"):
        n = c.kube.get("Node", c.params["nodes"][role]) or {}
        info = (n.get("status") or {}).get("nodeInfo") or {}
        env[role] = {"uid": (n.get("metadata") or {}).get("uid"), "allocatable": (n.get("status") or {}).get(
            "allocatable"), **{k: info.get(k) for k in ("kernelVersion", "kubeletVersion", "containerRuntimeVersion",
                                                          "osImage")}}
    return {"params_sha256": C.sha256_file(c.record.resolve(c.record.doc["params"])),
            "targets": {t: {"deployment_uid": v["deployment_uid"], "pa_uid": v["pa_uid"]} for t, v in targets.items()},
            "generator_image": c.params["demonstration"]["generator"]["image"], "fixture_image": c.params["image"],
            "release_images": [s["operator_image"], s["statistical_image"]],
            "peak_per_minute": max(max(r) for r in c.c28.demo.day_table(c.params)),
            "pod_templates": live, "pa_specs": pas, "nodes": env}


def _sha(path: str) -> Optional[str]:
    return C.sha256_file(path) if os.path.isfile(path) else None


def admission_passed(c: Ctx, targets: Dict) -> Tuple[Optional[Dict], str]:
    """The last admission run-complete, accepted only when its verdict is PASS, its evidence and verdict files still
    hash to the receipt's sha256, the re-read verdict file says PASS, and its binding equals the current one."""
    done = [e["data"] for e in X.receipts(c.record, HELPER) if e["event"] == "run-complete"
            and e["data"].get("label") == "admission"]
    if not done:
        return None, "no admission run-complete"
    r = done[-1]
    if r.get("verdict") != "PASS":
        return None, f"the last admission verdict is {r.get('verdict')}"
    ev, vd = os.path.join(r["dir"], "evidence.json"), os.path.join(r["dir"], "verdict.json")
    if _sha(ev) != r.get("evidence_sha256") or _sha(vd) != r.get("verdict_sha256"):
        return None, "the admission evidence or verdict file no longer hashes to its receipt"
    with open(vd) as fh:
        if json.load(fh).get("verdict") != "PASS":
            return None, "the admission verdict file does not say PASS"
    if r.get("binding") != binding(c, targets):
        return None, "the admission PASS was for other images, configuration, targets or params"
    return r, ""


def run_admission(c: Ctx, reviewed: Dict, reviewed_digest: str, fetch: Fetch, api: str,
                  logs: Callable[[Ctx, str], List[str]] = _logs) -> Dict:
    rid = f"a-{reviewed_digest[:12]}"
    out_dir = os.path.join(c.record.evidence_dir, f"admission-{rid}")
    with X.mutating(c.record, c.kube, HELPER, {"run": rid, "label": "admission"}):
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
        cfg = json.loads(objs["ConfigMap"]["data"][c.c28.demo.GENERATOR_CONFIG])
        bound = binding(c, p["targets"])                          # FROZEN before anything runs
        X.receipt(c.record, HELPER, "run-begin", {"run": rid, "plan_sha256": reviewed_digest, "label": "admission",
                                                   "binding": bound,
                                                   "origin_utc": p["origin_utc"], "dir": out_dir})
        expected: Dict[str, List[str]] = {t: [] for t in c.c28.demo.target_names(c.params)}
        obs = Observer(c, out_dir, expected)
        log_path = os.path.join(out_dir, "k6.log")
        st = {"stop": "", "pod": None, "created_ms": None, "baseline_s": None, "status": None, "code": None,
              "completion_s": None, "problems": []}
        lines: List[str] = []

        def pull():
            if st["pod"]:
                try:
                    got = logs(c, st["pod"])
                except Exception as e:                        # a log failure is recorded, never an escape
                    st["problems"].append(f"log pull: {type(e).__name__}: {str(e)[:120]}")
                    return
                with open(log_path, "a") as fh:
                    fh.write("\n".join(got) + "\n")
                with open(log_path) as fh:
                    lines[:] = [ln for ln in fh.read().splitlines() if ln.strip()]
        stopped_ok = False
        try:
            try:
                why = fence_problems(c)
                if why:
                    raise X.StagingError("fence: " + "; ".join(why))
                own(c, rid, c.kube.create(objs["ConfigMap"]))
                job = c.kube.create(objs["Job"])
                own(c, rid, job)
                job = c.kube.get("Job", job["metadata"]["name"], c.ns)
                st["created_ms"] = _ms(job["metadata"]["creationTimestamp"])
                if origin - st["created_ms"] > ORIGIN_MAX_S * 1000:
                    raise X.StagingError("VOID: the Job was created more than 5 minutes before the origin")
                obs.poll_pods(c.now() / 1000)                     # the baseline census: complete, or VOID
                st["baseline_s"] = c.now() / 1000
                for name in expected:
                    expected[name] = sorted(u for u, v in obs.pods.items() if v["name"].startswith(f"{name}-")
                                            and v["ready"] == "True")
                    if len(expected[name]) != REPLICAS:
                        raise X.StagingError(f"VOID: the baseline census of {name} has {len(expected[name])} Ready "
                                             f"pods, not {REPLICAS}")
                while True:                                       # the pod must be Running before the origin
                    pod = _job_pod(c, job["metadata"]["name"], job["metadata"]["uid"])
                    if pod is not None and st["pod"] is None:
                        own(c, rid, dict(pod, kind="Pod"))
                        st["pod"] = pod["metadata"]["name"]
                    if pod is not None and (pod.get("status") or {}).get("phase") == "Running":
                        break
                    if c.now() >= origin:
                        raise X.StagingError("VOID: the admission pod was not Running before the origin")
                    c.sleep(2)
                end_ms = origin + 40 * 60_000 + 30_000
                next_check = 0.0
                while c.now() < end_ms:
                    t = c.now() / 1000
                    obs.poll_pods(t)
                    if t >= next_check:
                        obs.sample_endpoints(t)
                        pull()
                        stops = live_stops(c, obs, fetch, api, st["baseline_s"],
                                           c.kube.get("Job", job["metadata"]["name"], c.ns), cfg, lines, "admission")
                        if stops:
                            raise X.StagingError("STOP: " + "; ".join(stops))
                        next_check = t + SAMPLE_S
                    c.sleep(POLL_S)
                deadline = c.now() + 180_000
                while _terminal(c.kube.get("Job", job["metadata"]["name"], c.ns)) is None and c.now() < deadline:
                    obs.poll_pods(c.now() / 1000)
                    c.sleep(POLL_S)
            except X.StagingError as e:
                st["stop"] = str(e)
                obs.stops.append({"t_s": c.now() / 1000, "why": st["stop"]})
                X.receipt(c.record, HELPER, "run-stopped", {"run": rid, "why": st["stop"][:300]})
            st["completion_s"] = c.now() / 1000
            try:                                                  # terminal state and the final log BEFORE the stop
                obs.poll_pods(st["completion_s"])
                ref = (owned(c.record, rid).get("Job") or [None])[0]
                job = c.kube.get("Job", ref["name"], c.ns) if ref else None
                job = job if job and job["metadata"]["uid"] == ref["uid"] else None
                st["status"] = _terminal(job) or ("Stopped" if st["stop"] else "Unknown")
                st["code"] = _exit_code(c.kube.get("Pod", st["pod"], c.ns)) if st["pod"] else None
                pull()
            except Exception as e:
                st["problems"].append(f"terminal state or final log: {type(e).__name__}: {str(e)[:160]}")
        finally:
            stop_load(c, rid)                                     # raises when unproven: the lock and fence stay
            stopped_ok = True
            try:
                c.sleep(SETTLE_EXTRA_S)
                vm = collect_vm(c, fetch, api, origin / 1000, st["pod"] or "none", obs.pods)
            except Exception as e:
                vm = {k: {} for k in ("ingress", "queue", "throttled", "steal", "pod_requests", "freshness")}
                vm["problems"] = [f"collection: {type(e).__name__}: {str(e)[:160]}"]
            ev = {"schema": c.verdicts.SCHEMA, "config": cfg, "lines": lines,
                  "ingress": vm["ingress"], "queue": vm["queue"], "throttled": vm["throttled"], "steal": vm["steal"],
                  "endpoints": obs.endpoints, "pod_requests": vm["pod_requests"],
                  "pods": {"expected": expected, "baseline_s": st["baseline_s"], "completion_s": st["completion_s"],
                           "watch": obs.watch, "transitions": obs.transitions, "method": "polling every 10 s"},
                  "freshness": vm["freshness"],
                  "job": {"status": st["status"], "exit_code": st["code"],
                          "created_s": st["created_ms"] / 1000 if st["created_ms"] else None},
                  "stops": obs.stops, "limits": c.verdicts.FROZEN,
                  "collection_problems": vm["problems"] + st["problems"]}
            try:                                                  # the tested configuration, still the same?
                if binding(c, p["targets"]) != bound:
                    ev["collection_problems"].append("the bound configuration changed during the run (images, pod "
                                                     "templates, PA specs, node environment or params)")
            except Exception as e:
                ev["collection_problems"].append(f"binding re-check: {type(e).__name__}")
            ev["binding"] = bound
            try:
                ev_sha = _write(os.path.join(out_dir, "evidence.json"), ev)
                with open(os.path.join(out_dir, "evidence.json")) as fh:
                    verdict = c.verdicts.verdict(json.load(fh))   # from the WRITTEN file
                v_sha = _write(os.path.join(out_dir, "verdict.json"), verdict)
            finally:
                if stopped_ok:
                    remove_config(c, rid)
        return X.receipt(c.record, HELPER, "run-complete", {
            "run": rid, "label": "admission", "plan_sha256": reviewed_digest, "verdict": verdict["verdict"],
            "evidence_sha256": ev_sha, "verdict_sha256": v_sha, "dir": out_dir, "stopped": st["stop"] or None,
            "binding": bound})


# --- the startup step (ADMISSION-DESIGN section 4 + Review 1 item 6) ---------------------------------------------------

SCALE_OP = "startup-scale"
STARTUP_WRITE_AT_S, STARTUP_LATE_S, STARTUP_LOW = 60, 15, 6


def open_obligations(c_or_record) -> List[Dict]:
    record = getattr(c_or_record, "record", c_or_record)
    es = X.receipts(record, HELPER)
    closed = {e["data"]["run"] for e in es if e["event"] == "restore-closed"}
    return [e["data"] for e in es if e["event"] == "restore-obligation" and e["data"]["run"] not in closed]


def plan_startup(c: Ctx, origin_utc: str) -> Dict:
    origin = _ms(origin_utc)
    objs = c.c28.demo.bounded_objects(c.params, "startup", origin)
    cfg = json.loads(objs[0]["data"][c.c28.demo.GENERATOR_CONFIG])
    parts = c.parts_with(objs)
    problems = prepare.admission_problems(c.record, c.params, c.s, parts)
    gate, gp = prepare.demonstration_gate(c.record, c.kube, c.params, c.s, parts)
    problems += gp + fence_problems(c)
    targets, tp = _targets_state(c)
    problems += tp
    passed, why = admission_passed(c, targets)
    if passed is None:
        problems.append(f"the startup step requires a bound admission PASS: {why}")
    lead = (origin - c.now()) / 1000
    if not ORIGIN_MIN_S <= lead <= ORIGIN_MAX_S + 600:
        problems.append(f"the origin is {lead:.0f} s ahead; it must be {ORIGIN_MIN_S}..{ORIGIN_MAX_S} s ahead at apply")
    target = cfg["targets"][0]
    actions = [{"create": o["kind"], "namespace": c.ns, "name": o["metadata"]["name"], "sha256": X.json_digest(o)}
               for o in objs]
    actions += [{"scale": target, "from": REPLICAS, "to": STARTUP_LOW, "when": "before the origin, idle"},
                {"scale": target, "from": STARTUP_LOW, "to": REPLICAS, "when": f"origin + {STARTUP_WRITE_AT_S} s"}]
    return X.new_plan(HELPER, c.record, actions, problems=problems, label="startup", origin_utc=origin_utc,
                      config_digest=cfg["digest"], target=target, targets=targets, reservation_gate=gate,
                      admission=(passed or {}).get("run"), deadline_s=c.c28.demo.bounded_deadline_s("startup"))


def _converged(c: Ctx, name: str, uid: str, n: int) -> bool:
    dep = c.kube.get("Deployment", name, c.ns)
    if dep is None or dep["metadata"]["uid"] != uid:
        raise X.StagingError(f"{name} is gone or replaced")
    if int(dep["spec"].get("replicas") or 0) != n:
        raise X.StagingError(f"{name}'s desired count changed to {dep['spec'].get('replicas')}: another writer")
    pods = [p for p in c.kube.list("Pod", c.ns, strict=True)
            if (p["metadata"].get("labels") or {}).get("c28.devkuban.com/target") == name]
    ready = [p for p in pods if not p["metadata"].get("deletionTimestamp") and any(
        x.get("type") == "Ready" and x.get("status") == "True" for x in (p.get("status") or {}).get("conditions") or [])]
    return len(pods) == n and len(ready) == n


def _scale_step(c: Ctx, run: str, name: str, uid: str, frm: int, to: int, wait_s: float,
                on_poll: Optional[Callable[[], None]] = None) -> Dict:
    """One journalled /scale write (UID and the resourceVersion read immediately before), then the wait until exactly
    `to` pods exist and are Ready, calling on_poll at every poll (the observation of new pods throughout). Receipts:
    scale-write submitting (durable before the request), then applied with the write time."""
    dep = c.kube.get("Deployment", name, c.ns)
    if dep is None or dep["metadata"]["uid"] != uid or int(dep["spec"].get("replicas") or 0) != frm:
        raise X.StagingError(f"{name} is not the reviewed object at {frm} replicas; refusing")

    def fn(mark):
        X.receipt(c.record, HELPER, "scale-write", {"run": run, "mark": mark, "result": "submitting", "to": to})
        c.kube.scale("Deployment", name, c.ns, uid, dep["metadata"]["resourceVersion"], to)
        at = c.now()
        X.receipt(c.record, HELPER, "scale-write", {"run": run, "mark": mark, "result": "applied", "to": to,
                                                    "at_ms": at})
        end = at + wait_s * 1000
        while True:
            if on_poll:
                on_poll()
            if _converged(c, name, uid, to):
                break
            if c.now() >= end:
                raise X.StagingError(f"{name} did not reach {to} Ready within {wait_s:.0f} s")
            c.sleep(2)
        return {"metadata": {"uid": uid, "generation": (dep["metadata"].get("generation") or 0) + 1}, "at_ms": at}
    return c.kube.step(SCALE_OP, "Deployment", c.ns, name, uid, fn,
                       pre={"generation": dep["metadata"].get("generation"), "replicas": frm},
                       expect=[["/spec/replicas", to]])


def reconcile_scale(kube: X.Kube, d: Dict, record) -> Dict:
    """done: the Deployment (same UID) desires the step's target count; not-done: still at the planned count with its
    planned generation; else unresolved. Convergence is the restore's business."""
    live = kube.get("Deployment", d["name"], d["namespace"])
    if live is None or live["metadata"].get("uid") != d["uid"]:
        return {"observed": "unresolved", "why": "the Deployment is gone or replaced"}
    cur, gen = int(live["spec"].get("replicas") or 0), live["metadata"].get("generation")
    if cur == d["expect"][0][1]:
        return {"observed": "done", "uid": d["uid"]}
    if cur == d["pre"].get("replicas") and gen == d["pre"].get("generation"):
        return {"observed": "not-done", "state": "not-applied", "uid": d["uid"]}
    return {"observed": "unresolved", "why": f"at {cur} replicas, generation {gen}: not explained by the step"}


X.RECONCILERS[SCALE_OP] = reconcile_scale


def restore_to_twelve(c: Ctx, ob: Dict) -> None:
    """The obligation: stop the run's load (stop_load: recorded identities only, orphans found without the Job, every
    pod proven gone); then, independently of the ConfigMap's removal, the SAME target UID back to 12 Ready; the
    obligation closes only when both are done. Never starts a workload."""
    stop_load(c, ob["run"])
    cleanup_error = None
    try:
        remove_config(c, ob["run"])
    except Exception as e:                                        # never blocks the same-UID restore
        cleanup_error = e
    dep = c.kube.get("Deployment", ob["target"], c.ns)
    if dep is None or dep["metadata"]["uid"] != ob["uid"]:
        raise X.StagingError(f"{ob['target']} is gone or replaced: the obligation cannot be met; the fence stays")
    cur = int(dep["spec"].get("replicas") or 0)
    if cur != REPLICAS:
        _scale_step(c, ob["run"], ob["target"], ob["uid"], cur, REPLICAS, 300)
    else:
        end = c.now() + 300_000
        while not _converged(c, ob["target"], ob["uid"], REPLICAS):
            if c.now() >= end:
                raise X.StagingError(f"{ob['target']} is not at {REPLICAS} Ready: the obligation stays open")
            c.sleep(2)
    if cleanup_error is not None:
        raise X.StagingError(f"{ob['target']} is back at {REPLICAS}, but the run's ConfigMap could not be removed "
                             f"({type(cleanup_error).__name__}): the obligation stays open until restore succeeds")
    X.receipt(c.record, HELPER, "restore-closed", {"run": ob["run"], "target": ob["target"], "uid": ob["uid"]})


class PodTimeline:
    """Every poll's view of the target's pods created after a time (kept raw): conditions, container state, restarts.
    A failed poll is recorded (a live stop and a coverage gap). Each pod's creation-to-Ready uses its FIRST observed
    Ready; any later Ready=False, or a newer Ready transition time, is a readiness loss."""

    MAX_GAP_MS = 30_000

    def __init__(self, c: Ctx, name: str):
        self.c, self.name, self.since_ms, self.polls, self.failures = c, name, None, [], []

    def poll(self) -> None:
        if self.since_ms is None:
            return
        try:
            pods = self.c.kube.list("Pod", self.c.ns, strict=True)
        except (C.ApiError, X.StagingError) as e:
            self.failures.append({"t_ms": self.c.now(), "what": f"startup pod poll: {type(e).__name__}"})
            return
        snap = []
        for p in pods:
            if (p["metadata"].get("labels") or {}).get("c28.devkuban.com/target") != self.name:
                continue
            ct = p["metadata"].get("creationTimestamp")
            if not ct or _ms(ct) < self.since_ms:
                continue
            conds = {x["type"]: x for x in (p.get("status") or {}).get("conditions") or []}
            cs = ((p.get("status") or {}).get("containerStatuses") or [{}])[0]
            snap.append({"uid": p["metadata"]["uid"], "name": p["metadata"]["name"], "created": ct,
                         "scheduled": conds.get("PodScheduled", {}).get("lastTransitionTime"),
                         "started": ((cs.get("state") or {}).get("running") or {}).get("startedAt"),
                         "ready": conds.get("Ready", {}).get("status"),
                         "ready_since": conds.get("Ready", {}).get("lastTransitionTime"),
                         "restarts": cs.get("restartCount", 0)})
        self.polls.append({"t_ms": self.c.now(), "pods": snap})

    def summary(self, end_ms: Optional[float] = None) -> Dict:
        """end_ms: the coverage endpoint (the run's completion); a gap from the last poll to it counts too, and no
        endpoint at all is itself a gap."""
        first_ready, last, lost = {}, {}, []
        for poll in self.polls:
            for x in poll["pods"]:
                u = x["uid"]
                if x["ready"] == "True" and u not in first_ready:
                    first_ready[u] = x
                elif u in first_ready and (x["ready"] != "True" or x["ready_since"] != first_ready[u]["ready_since"]):
                    if not lost or lost[-1]["uid"] != u or lost[-1]["t_ms"] != poll["t_ms"]:
                        lost.append({"uid": u, "t_ms": poll["t_ms"], "ready": x["ready"], "since": x["ready_since"]})
                last[u] = x
        new = []
        for u in sorted(last):
            fr = first_ready.get(u)
            c2r = (_ms(fr["ready_since"]) - _ms(fr["created"])) / 1000 if fr and fr["ready_since"] else None
            new.append({**last[u], "first_ready_since": (fr or {}).get("ready_since"), "creation_to_ready_s": c2r})
        if self.since_ms is None:
            gaps = [[None, "the observation never started"]]
        else:
            ts = [self.since_ms] + [p["t_ms"] for p in self.polls] + ([end_ms] if end_ms is not None else [])
            gaps = [[a, b] for a, b in zip(ts, ts[1:]) if b - a > self.MAX_GAP_MS]
            if end_ms is None:
                gaps.append([ts[-1], "no completion endpoint"])
            elif not self.polls or self.polls[-1]["t_ms"] < end_ms - self.MAX_GAP_MS:
                gaps.append([ts[-2] if len(ts) > 1 else None, "no final observation"])
        return {"new_pods": new, "readiness_lost": lost, "polls": len(self.polls), "failures": self.failures,
                "coverage_gaps": gaps + [[f["t_ms"], f["what"]] for f in self.failures],
                "creation_to_ready_s": sorted(x["creation_to_ready_s"] for x in new
                                              if x["creation_to_ready_s"] is not None)}


def startup_load_problems(c: Ctx, cfg: Dict, lines: List[str]) -> List[str]:
    """The load is proven, not assumed: a valid start record bound to the configuration, the first send within 10 s
    of the origin, and every minute of the 15 a complete generator record that dispatched traffic (overload is
    allowed; an unrecorded or idle minute is not)."""
    out = []
    late = [t for t, v in first_sends(c, cfg, lines).items()
            if v is None or v - cfg["origin_ms"] > FIRST_SEND_MAX_MS]
    if late:
        out.append(f"no validated first send within 10 s for {late}")
    red = c.reduce
    recs, _ = red.parse_lines(lines)
    try:
        r = red.reduce(cfg, recs, {}, cfg["origin_ms"], cfg["origin_ms"] + 15 * 60_000)
    except ValueError as e:
        return out + [f"the load records cannot be reduced: {e}"]
    for t in cfg["targets"]:
        bad = []
        for row in r["targets"][t]["rows"]:
            g = row["generator"]
            why = [] if row["generator_state"] == "complete" else [f"state {row['generator_state']}"]
            why += [f"due {g.get('due')} of {row['intended']} scheduled"] if g.get("due") != row["intended"] else []
            why += ["dropped"] if sum((g.get("dropped") or {}).values()) else []
            why += ["unresolved outcomes"] if g.get("unresolved") else []
            why += ["nothing dispatched"] if not g.get("dispatched") else []
            if why:
                bad.append((row["minute"], why))
        if bad:
            out.append(f"{t}: minutes without the full scheduled load, conserved carries, zero generator drops and "
                       f"complete outcomes: {bad[:6]}")
    return out


def run_startup(c: Ctx, reviewed: Dict, reviewed_digest: str, fetch: Fetch, api: str,
                logs: Callable[[Ctx, str], List[str]] = _logs) -> Dict:
    rid = f"s-{reviewed_digest[:12]}"
    out_dir = os.path.join(c.record.evidence_dir, f"startup-{rid}")
    with X.mutating(c.record, c.kube, HELPER, {"run": rid, "label": "startup"}):
        p = plan_startup(c, reviewed["origin_utc"])
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
        if p["problems"]:
            raise X.StagingError("the plan has problems: " + "; ".join(p["problems"]))
        origin = _ms(p["origin_utc"])
        if not ORIGIN_MIN_S <= (origin - c.now()) / 1000 <= ORIGIN_MAX_S:
            raise X.StagingError("the origin is not 120..300 s ahead at apply")
        os.makedirs(out_dir)
        target, uid = p["target"], p["targets"][p["target"]]["deployment_uid"]
        objs = {o["kind"]: o for o in c.c28.demo.bounded_objects(c.params, "startup", origin)}
        for a in p["actions"]:
            if "create" in a and X.json_digest(objs[a["create"]]) != a["sha256"]:
                raise X.StagingError(f"{a['create']} rendered differently from the reviewed plan; refusing")
        cfg = json.loads(objs["ConfigMap"]["data"][c.c28.demo.GENERATOR_CONFIG])
        ob = {"run": rid, "target": target, "uid": uid, "to": REPLICAS}
        X.receipt(c.record, HELPER, "restore-obligation", ob)          # BEFORE the first write
        obs = Observer(c, out_dir, {target: []})
        timeline = PodTimeline(c, target)
        log_path = os.path.join(out_dir, "k6.log")
        st = {"stop": "", "pod": None, "write_ms": None, "late_s": None, "problems": [], "status": None, "code": None}
        lines: List[str] = []

        def pull():
            if st["pod"]:
                try:
                    got = logs(c, st["pod"])
                except Exception as e:                        # a log failure is recorded, never an escape
                    st["problems"].append(f"log pull: {type(e).__name__}: {str(e)[:120]}")
                    return
                with open(log_path, "a") as fh:
                    fh.write("\n".join(got) + "\n")
                with open(log_path) as fh:
                    lines[:] = [ln for ln in fh.read().splitlines() if ln.strip()]
        try:
            try:
                _scale_step(c, rid, target, uid, REPLICAS, STARTUP_LOW, 180)
                fence = fence_problems(c, own_run=rid)
                if fence:
                    raise X.StagingError("fence: " + "; ".join(fence))
                own(c, rid, c.kube.create(objs["ConfigMap"]))
                job = c.kube.create(objs["Job"])
                own(c, rid, job)
                obs.poll_pods(c.now() / 1000)
                baseline_s = c.now() / 1000
                while True:
                    pod = _job_pod(c, job["metadata"]["name"], job["metadata"]["uid"])
                    if pod is not None and st["pod"] is None:
                        own(c, rid, dict(pod, kind="Pod"))
                        st["pod"] = pod["metadata"]["name"]
                    if pod is not None and (pod.get("status") or {}).get("phase") == "Running":
                        break
                    if c.now() >= origin:
                        raise X.StagingError("VOID: the startup pod was not Running before the origin")
                    c.sleep(2)

                def check():
                    obs.poll_pods(c.now() / 1000)
                    pull()
                    stops = live_stops(c, obs, fetch, api, baseline_s,
                                       c.kube.get("Job", job["metadata"]["name"], c.ns), cfg, lines, "startup")
                    stops += [f"observation failed: {f['what']}" for f in timeline.failures]
                    if stops:
                        raise X.StagingError("STOP: " + "; ".join(stops))
                next_check = 0.0
                while c.now() < origin + STARTUP_WRITE_AT_S * 1000:
                    if c.now() / 1000 >= next_check:
                        check()
                        next_check = c.now() / 1000 + SAMPLE_S
                    c.sleep(1)
                st["late_s"] = (c.now() - origin) / 1000 - STARTUP_WRITE_AT_S
                if st["late_s"] > STARTUP_LATE_S:
                    raise X.StagingError(f"the 6 -> 12 write would be {st['late_s']:.0f} s late "
                                         f"(allowed {STARTUP_LATE_S} s)")
                timeline.since_ms = c.now() - 5_000
                last = [0.0]

                def during():
                    timeline.poll()
                    if c.now() / 1000 - last[0] >= SAMPLE_S:
                        check()
                        last[0] = c.now() / 1000
                step = _scale_step(c, rid, target, uid, STARTUP_LOW, REPLICAS, 15 * 60 - STARTUP_WRITE_AT_S, during)
                st["write_ms"] = step.get("at_ms")
                end = origin + 15 * 60_000 + 30_000
                while c.now() < end and _terminal(c.kube.get("Job", job["metadata"]["name"], c.ns)) is None:
                    timeline.poll()
                    if c.now() / 1000 >= next_check:
                        check()
                        next_check = c.now() / 1000 + SAMPLE_S
                    c.sleep(POLL_S)
                deadline = c.now() + 180_000
                while _terminal(c.kube.get("Job", job["metadata"]["name"], c.ns)) is None and c.now() < deadline:
                    timeline.poll()
                    c.sleep(POLL_S)
            except X.StagingError as e:
                st["stop"] = str(e)
                X.receipt(c.record, HELPER, "run-stopped", {"run": rid, "why": st["stop"][:300]})
            timeline.poll()                                       # the final observation
            st["completion_ms"] = c.now()
            try:                                                  # terminal state and the final log BEFORE the stop
                ref = (owned(c.record, rid).get("Job") or [None])[0]
                job_now = c.kube.get("Job", ref["name"], c.ns) if ref else None
                job_now = job_now if job_now and job_now["metadata"]["uid"] == ref["uid"] else None
                st["status"] = _terminal(job_now)
                st["code"] = _exit_code(c.kube.get("Pod", st["pod"], c.ns)) if st["pod"] else None
                pull()
            except Exception as e:
                st["problems"].append(f"terminal state or final log: {type(e).__name__}: {str(e)[:160]}")
        finally:
            stopped_ok, summ, load, ev_sha = False, {"new_pods": [], "creation_to_ready_s": [], "readiness_lost": [],
                                                     "coverage_gaps": [], "polls": 0}, ["not evaluated"], None
            doc = {"job": {}}
            try:
                stop_load(c, rid)
                stopped_ok = True
            finally:
                try:
                    summ = timeline.summary(st.get("completion_ms"))
                    load = startup_load_problems(c, cfg, lines) if lines else ["no load records"]
                    doc = {"schema": "c28-startup-evidence/v1", "run": rid, "target": target, "uid": uid,
                           "origin_utc": p["origin_utc"], "write_ms": st["write_ms"], "late_s": st["late_s"],
                           "timeline": timeline.polls, "summary": summ, "load_problems": load,
                           "stopped": st["stop"] or None, "problems": st["problems"],
                           "job": {"status": st["status"], "exit_code": st["code"]}, "k6_lines": lines}
                    ev_sha = _write(os.path.join(out_dir, "evidence.json"), doc)
                except Exception as e:                        # evidence never blocks the restore
                    st["problems"].append(f"evidence: {type(e).__name__}: {str(e)[:160]}")
                finally:
                    if stopped_ok:
                        restore_to_twelve(c, ob)              # raises (obligation open, fence kept) if unmet
        n_new = len(summ["new_pods"])
        ok = not st["stop"] and not load and not st["problems"] and n_new == REPLICAS - STARTUP_LOW \
            and len(summ["creation_to_ready_s"]) == n_new and not summ["readiness_lost"] \
            and not summ["coverage_gaps"] and ev_sha is not None \
            and doc["job"] == {"status": "Complete", "exit_code": 0}
        return X.receipt(c.record, HELPER, "startup-complete", {
            "run": rid, "plan_sha256": reviewed_digest, "measured": ok, "stopped": st["stop"] or None,
            "admission": p.get("admission"),
            "load_problems": load, "creation_to_ready_s": summ["creation_to_ready_s"], "evidence_sha256": ev_sha,
            "dir": out_dir})


def plan_restore(c: Ctx) -> Dict:
    """Read-only: every open restore obligation with the live state of its target (after recover, if needed)."""
    obs = open_obligations(c)
    problems = []
    if not obs:
        problems.append("no open restore obligation")
    for h in (HELPER, "prepare"):
        if X.open_intents(X.receipts(c.record, h)):
            problems.append(f"{h} has intents with unknown outcomes: recover first")
    live = []
    for ob in obs:
        dep = c.kube.get("Deployment", ob["target"], c.ns)
        if dep is None or dep["metadata"]["uid"] != ob["uid"]:
            problems.append(f"{ob['target']} is gone or replaced: the obligation cannot be met by this tool")
        live.append({"run": ob["run"], "target": ob["target"], "uid": ob["uid"],
                     "replicas": None if dep is None else dep["spec"].get("replicas")})
    return X.new_plan(HELPER, c.record, [{"restore": o["target"], "uid": o["uid"], "to": REPLICAS} for o in obs],
                      problems=problems, obligations=live)


def restore(c: Ctx, reviewed: Dict, reviewed_digest: str) -> Dict:
    """Meet every open obligation: stop the startup load (Job and pod by UID, the pod proven gone) and bring the SAME
    target UID back to 12 Ready. Never starts a workload. Anything unmet raises with the fence kept."""
    with X.mutating(c.record, c.kube, HELPER, {"restore": reviewed_digest[:12]}):
        p = plan_restore(c)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
        if p["problems"]:
            raise X.StagingError("the restore plan has problems: " + "; ".join(p["problems"]))
        for ob in open_obligations(c):
            restore_to_twelve(c, ob)
        return X.receipt(c.record, HELPER, "restore-complete", {"plan_sha256": reviewed_digest,
                                                                 "met": [o["run"] for o in p["obligations"]]})


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
    ps = sub.add_parser("plan-startup")
    ps.add_argument("--origin", required=True)
    rs = sub.add_parser("run-startup")
    rs.add_argument("--plan", required=True)
    rs.add_argument("--plan-digest", required=True)
    rs.add_argument("--query-api", required=True, help="the observer's Prometheus API base (a local forward)")
    sub.add_parser("plan-restore")
    rr = sub.add_parser("restore")
    rr.add_argument("--plan", required=True)
    rr.add_argument("--plan-digest", required=True)
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
    elif a.cmd == "plan-restore":
        X.emit(X.cli_plan(record, HELPER, plan_restore(Ctx(record, kube)), "plan-restore"))
    elif a.cmd == "restore":
        X.emit(restore(Ctx(record, kube), X.load_plan(a.plan), a.plan_digest))
    elif a.cmd == "plan-startup":
        X.emit(X.cli_plan(record, HELPER, plan_startup(Ctx(record, kube), a.origin), "plan-startup"))
    elif a.cmd == "run-startup":
        X.emit(run_startup(Ctx(record, kube), X.load_plan(a.plan), a.plan_digest, fetch, a.query_api))
    elif a.cmd == "plan-admission":
        X.emit(X.cli_plan(record, HELPER, plan_admission(Ctx(record, kube), a.origin), "plan-admission"))
    else:
        X.emit(run_admission(Ctx(record, kube), X.load_plan(a.plan), a.plan_digest, fetch, a.query_api))
    return 0


if __name__ == "__main__":
    sys.exit(main())
