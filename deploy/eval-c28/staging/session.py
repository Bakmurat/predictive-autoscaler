#!/usr/bin/env python3
"""One loaded arm session: start, stop, verify and its watchdog (STAGING-C28-PROD.md, S5b/S5c; manifest stops).

  session.py --record R [plan]                              read-only: write the start plan file, print its digest
  session.py --record R start --plan F --plan-digest D      start exactly that session
  session.py --record R stop-plan                           read-only: the scoped stop plan (from the journal)
  session.py --record R stop --plan F --plan-digest D       stop it: arrivals, faults, writer, quota, then quiesce
  session.py --record R verify                              read-only: is the session still within its contract?
  session.py --record R watch --start-digest D              the watchdog (stops the session's own objects)
  session.py --record R recover-plan | recover --plan F --plan-digest D   reconcile open intents (shared lock)

A session is one target (arm/profile), one seed, one k6 plan, one generator Job. Before starting it requires:
  * the cluster identity; the target Deployment's recorded UID; the LIVE arm configuration (Deployment, Service,
    PredictiveAutoscaler) equal to its rendering on every rendered field; and for a block, that rendering equal to the
    SIGNED run-lock's (arms.<arm/profile>.rendered_sha256), the target UID equal to the lock's activation scope, and
    the generator and fixture images equal to the lock's images.
  * the exact writer census, by full identity (kind, namespace, name, UID) across HPA, KEDA and both PA API groups:
    a PA arm has exactly its own PredictiveAutoscaler (the UID prepare.py created); a baseline arm none yet (start
    creates its rendered HPA or ScaledObject); every other target only its own Recommend PA, or nothing.
  * one loaded arm: no other session of the run open or unresolved in the journal, no generator of the run active.
  * phase rules. preparation: ordinary plans only, PA arms stay in Recommend, every fault schedule empty.
    confirmation: two signed reviewer receipts for the lock (freeze.py), a permitted date, exactly the locked plan
    and fault timetable, and for PA-N/PA-E the operator's verified readiness (PredictionReady=True/Ready, eligible,
    the locked frozen lead), never an HTTP status. After the lock's qualification deadline a session may instead
    declare the scheduled intention-to-treat fallback ("itt_fallback": true): readiness is then recorded, not
    required, and every authorisation and safety prerequisite still applies. The quota intervention stays NOT READY
    until a reviewed injection plan is a signed prerequisite ("quota_injection_plan" in the reviewer receipts).
  * the arrival budget, the quota at its locked normal limit, no MemoryPressure/DiskPressure, a covering deadline.
Every mutation holds the run's shared mutation lock and is journalled (intent before the call, outcome after); an
unknown outcome keeps the lock until `recover` reconciles it against the live state. What a session owns is derived
from that journal, so a create whose response was lost is still owned and cleaned. Stop is complete only when the
session's generator pods are gone (terminating ones included), in-flight requests have timed out, and the writer is
confirmed inactive (the PA's observed mode is Recommend, or the created writer and its KEDA HPA are gone); anything
else, including objects replaced under the session, is recorded as unresolved and keeps the session open. The block
generator is created suspended and released by the watchdog at the block start. The block's start is acknowledged
only by evidence attributed to this session (owned k6 container started, the session-tagged k6 iteration counter
and exactly one matching ingress series rising after it); a release later than the lock's max_release_lateness_s,
or no attributed acknowledgment within it, stops the session and classifies the block as a late (replacement) slot.

The record's "session" section:
  {"id": "s-0001", "phase": "preparation"|"confirmation", "arm": "PA-N", "profile": "s20", "seed": 201,
   "plan": "<k6 plan file>", "block_start": "...Z" (confirmation), "runlock": "<file>" (confirmation),
   "manifest_root": "<dir>", "generator_image": "<repo>@sha256:...", "generator_k6_version": "0.57.0",
   "generator_output": {"remote_write_url": "<reviewed remote-write endpoint>", "push_interval_s": 5},
   "generator_resources": {...},
   "generator_env": {non-secret k6 settings}, "arrival_budget": {"max_rps": N, "max_total_arrivals": N},
   "deadline_utc": "...", "itt_fallback": false, "quiesce_timeout_s": 300,
   "arrival_ack": {"ingress": {"metric": "c28_ingress_requests_total", "labels": {<attribution labels>}},
                   "generator": {"metric": "k6_iterations_total"}, "step_s": 5},
   "stops": {"steal_query": "<PromQL, percent per node>", "query_api": "http://...", "node_label": "node",
             "steal_percent": 5, "steal_bins": 5}}
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import execlib as X

HELPER = "session"
STOP = "session-stop"
GENERATOR = "c28-generator"
PRE_ROLL_S = 600
_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$")
_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
WRITER_KIND = {"H-CPU": "HorizontalPodAutoscaler", "H-RPS": "ScaledObject"}
K6_MIN_RW = (0, 42)                # first k6 release with the built-in experimental-prometheus-rw output
K6_OUTPUT = "experimental-prometheus-rw"


def _utc(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def _ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Context:
    """Everything a session computation needs, loaded once."""

    def __init__(self, record: X.Record, kube: X.Kube, now: Callable[[], int] = lambda: int(time.time()),
                 sleep: Callable[[float], None] = time.sleep):
        self.record, self.kube, self.now, self.sleep = record, kube, now, sleep
        self.c28 = X.import_harness()
        self.params = X.load_params(record)
        self.s = record.section("session")
        s = self.s
        if not X._DNS.match(str(s.get("id", ""))) or len(str(s.get("id"))) > 30:
            raise X.StagingError("session.id must be a short DNS label")
        if s.get("phase") not in ("preparation", "confirmation"):
            raise X.StagingError("session.phase must be preparation or confirmation")
        self.arm, self.profile, self.seed = s.get("arm"), s.get("profile"), s.get("seed")
        try:
            self.target = self.c28.render.target_name(self.arm, self.profile)
        except self.c28.render.ParamsError as e:
            raise X.StagingError(str(e)) from e
        if not isinstance(self.seed, int):
            raise X.StagingError("session.seed must be an integer")
        if not _DIGEST_IMAGE.match(str(s.get("generator_image", ""))):
            raise X.StagingError("session.generator_image must be pinned by digest")
        out = s.get("generator_output") or {}
        if not re.match(r"^https?://[^@/\s]+/\S*$", str(out.get("remote_write_url", ""))):
            raise X.StagingError("session.generator_output.remote_write_url must be the reviewed Prometheus "
                                 "remote-write endpoint (http(s), no credentials)")
        ver = re.match(r"^(\d+)\.(\d+)\.(\d+)$", str(s.get("generator_k6_version", "")))
        if not ver or (int(ver.group(1)), int(ver.group(2))) < K6_MIN_RW:
            raise X.StagingError(f"session.generator_k6_version must name the pinned k6 release, at least "
                                 f"{K6_MIN_RW[0]}.{K6_MIN_RW[1]}.0 (built-in experimental-prometheus-rw output)")
        if not _TS.match(str(s.get("deadline_utc", ""))):
            raise X.StagingError("session.deadline_utc must be an RFC 3339 UTC time")
        b = s.get("arrival_budget") or {}
        if not all(isinstance(b.get(k), (int, float)) and b[k] > 0 for k in ("max_rps", "max_total_arrivals")):
            raise X.StagingError("session.arrival_budget needs max_rps and max_total_arrivals")
        with open(record.resolve(str(s.get("plan", "")))) as fh:
            self.plan = json.load(fh)
        if self.c28.schedule.digest(self.plan) != self.plan.get("digest"):
            raise X.StagingError("the k6 plan's digest does not match its content")
        self.ns = self.params["namespace"]
        self.lock, self.lock_digest = None, ""
        if s["phase"] == "confirmation":
            path = record.resolve(str(s.get("runlock", "")))
            root = record.resolve(s["manifest_root"]) if s.get("manifest_root") else os.path.dirname(path)
            try:
                self.lock_digest, self.lock = self.c28.runlock.load_verified(path, root)
            except (self.c28.runlock.RunLockError, OSError) as e:
                raise X.StagingError(f"the run-lock does not verify: {e}") from e
        tpath = os.path.join(record.evidence_dir, "targets.json")
        if not os.path.exists(tpath):
            raise X.StagingError("targets.json (prepare.py, phase targets) is missing")
        with open(tpath) as fh:
            self.targets = json.load(fh)
        if self.targets.get("cluster_uid") != record.cluster_uid or self.target not in self.targets["deployments"]:
            raise X.StagingError(f"{self.target} has no recorded UID for this cluster")
        self.target_uid = self.targets["deployments"][self.target]
        import prepare
        self.pa_uids = {n: ref["uid"] for (k, ns, n), ref in prepare.created_objects(record).items()
                        if k == "PredictiveAutoscaler" and ns == self.ns}

    @property
    def session_id(self) -> str:
        return self.s["id"]

    def names(self) -> Dict[str, str]:
        sid = self.session_id
        return {"job": f"c28-gen-{sid}", "plan_cm": f"c28-k6-{sid}"}

    def labels(self) -> Dict[str, str]:
        return {X.RUN_LABEL: self.record.run_id, X.SESSION_LABEL: self.session_id,
                X.COMPONENT_LABEL: GENERATOR, "c28.devkuban.com/target": self.target,
                "app.kubernetes.io/part-of": "c28-evaluation"}


# --- the session's state, from the journal ----------------------------------------------------------------------------

def owned_from_journal(c: Context, entries: List[Dict]) -> Dict:
    """What the session's start created or changed, from its journal (never from a summary written after the fact)."""
    owned: Dict = {"faults": [], "writer": None, "job": None, "plan_cm": None}
    names = c.names()
    for it in X.applied_intents(entries):
        d = it["data"]
        if d.get("context", {}).get("session") != c.session_id or d.get("context", {}).get("phase") != "start":
            continue
        uid = d.get("result_uid") or d.get("uid") or ""
        if d["op"] == "create" and d["kind"] == "Job":
            owned["job"] = {"name": d["name"], "uid": uid}
        elif d["op"] == "create" and d["kind"] == "ConfigMap" and d["name"] == names["plan_cm"]:
            owned["plan_cm"] = {"name": d["name"], "uid": uid}
        elif d["op"] == "create" and d["kind"] in ("HorizontalPodAutoscaler", "ScaledObject"):
            owned["writer"] = {"kind": d["kind"], "name": d["name"], "uid": uid, "action": "created"}
        elif d["op"] == "update" and d["kind"] == "PredictiveAutoscaler":
            owned["writer"] = {"kind": d["kind"], "name": d["name"], "uid": uid, "action": "mode"}
        elif d["op"] == "update" and d["kind"] == "ConfigMap":
            f = {"name": d["name"], "namespace": d["namespace"], "uid": uid}
            if f not in owned["faults"]:
                owned["faults"].append(f)
    return owned


def session_state(entries: List[Dict], sid: str) -> Optional[Dict]:
    """The open (or unresolved) state of a session: from start-begin until session-closed."""
    state = None
    for e in entries:
        if e.get("data", {}).get("session") != sid:
            continue
        if e["event"] == "start-begin":
            state = {"begin": e, "started": None}
        elif e["event"] == "session-started" and state is not None:
            state["started"] = e
        elif e["event"] == "session-closed":
            state = None
    return state


def _open_sessions(entries: List[Dict]) -> List[str]:
    open_ = {}
    for e in entries:
        sid = e.get("data", {}).get("session")
        if e["event"] == "start-begin":
            open_[sid] = True
        elif e["event"] == "session-closed":
            open_.pop(sid, None)
    return sorted(open_)


# --- checks ---------------------------------------------------------------------------------------------------------

def _all_targets(c: Context) -> List[str]:
    return sorted(c.targets["deployments"])


def _expected(c: Context, name: str, owned: Optional[Dict]) -> List[Dict]:
    if name.startswith("c28-pa-"):
        uid = c.pa_uids.get(name)
        return [{"kind": "PredictiveAutoscaler", "namespace": c.ns, "name": name, "uid": uid}] if uid else [
            {"kind": "PredictiveAutoscaler", "namespace": c.ns, "name": name, "uid": "<not created by prepare>"}]
    if name == c.target and owned and owned.get("writer") and owned["writer"]["action"] == "created":
        w = owned["writer"]
        return [{"kind": w["kind"], "namespace": c.ns, "name": w["name"], "uid": w["uid"]}]
    return []


def _writer_problems(c: Context, owned: Optional[Dict] = None) -> List[str]:
    """Exactly the authorised writers by full identity; every PA other than an active confirmation arm in Recommend."""
    out = []
    for name, writers in sorted(X.writer_inventory(c.kube, c.ns, _all_targets(c)).items()):
        want = _expected(c, name, owned)
        if writers != want:
            got = [f"{X.ident_str(w)}#{w['uid']}" for w in writers] or "none"
            exp = [f"{X.ident_str(w)}#{w['uid']}" for w in want] or "none"
            out.append(f"{name}: writers {got}, expected exactly {exp}")
    active_ok = owned and owned.get("writer") and owned["writer"]["action"] == "mode"
    for name in _all_targets(c):
        if not name.startswith("c28-pa-") or (name == c.target and active_ok):
            continue
        pa = c.kube.get("PredictiveAutoscaler", name, c.ns)
        if pa is not None and pa["spec"].get("mode") != "Recommend":
            out.append(f"PredictiveAutoscaler/{name} is {pa['spec'].get('mode')}: only the session arm may be Active")
    return out


def _config_problems(c: Context) -> List[str]:
    """The live arm (Deployment, Service, PredictiveAutoscaler) equals its rendering on every rendered field; for a
    block the rendering is the signed run-lock's."""
    out = []
    objs = c.c28.render.render_arm(c.params, c.arm, c.profile)
    if c.lock is not None:
        key = f"{c.arm}/{c.profile}"
        if X.json_digest(objs) != c.lock["arms"][key]["rendered_sha256"]:
            out.append(f"the arm's rendering differs from the signed run-lock's ({key}): params changed after locking")
    for o in objs:
        if o["kind"] not in ("Deployment", "Service", "PredictiveAutoscaler"):
            continue
        live = c.kube.get(o["kind"], o["metadata"]["name"], c.ns)
        if live is None:
            out.append(f"{o['kind']}/{o['metadata']['name']} is missing")
            continue
        ignore = {"Deployment": ["/spec/replicas"], "PredictiveAutoscaler": ["/spec/mode"]}.get(o["kind"], [])
        diff = X.subset_diff({"metadata": {"labels": o["metadata"].get("labels", {})}, "spec": o["spec"]},
                             live, ignore=ignore)
        if diff:
            out.append(f"{o['kind']}/{o['metadata']['name']} differs from its reviewed rendering at {diff[:5]}")
    return out


def _active_generators(c: Context) -> List[str]:
    jobs = c.kube.list("Job", c.ns, labels={X.RUN_LABEL: c.record.run_id, X.COMPONENT_LABEL: GENERATOR})
    return sorted(j["metadata"]["name"] for j in jobs
                  if not any(cd.get("type") in ("Complete", "Failed") and cd.get("status") == "True"
                             for cd in j.get("status", {}).get("conditions", [])))


def _node_stops(c: Context) -> List[str]:
    out = []
    for role in ("workload", "support"):
        n = c.kube.get("Node", c.params["nodes"][role])
        if n is None:
            out.append(f"{role} node {c.params['nodes'][role]} is missing")
            continue
        conds = {x["type"]: x["status"] for x in n.get("status", {}).get("conditions", [])}
        for k in ("MemoryPressure", "DiskPressure"):
            if conds.get(k) != "False":
                out.append(f"{role} node: {k} {conds.get(k, 'unknown')}")
    return out


def _readiness(c: Context) -> Tuple[List[str], str]:
    """(problems, classification): qualified | itt-fallback | n/a. Readiness is the operator's verified status."""
    if c.s["phase"] != "confirmation" or c.arm not in ("PA-N", "PA-E"):
        return [], "n/a"
    pa = c.kube.get("PredictiveAutoscaler", c.target, c.ns)
    if pa is None:
        return [f"PredictiveAutoscaler/{c.target} is missing"], "n/a"
    safety, forecast = [], []
    st = pa.get("status", {}) or {}
    if st.get("observedGeneration") != pa["metadata"].get("generation", st.get("observedGeneration")):
        safety.append("the status is not for the current generation")
    if pa["spec"].get("prediction", {}).get("families") != list(c.c28.frozen.PA_FAMILIES[c.arm]):
        safety.append("spec.prediction.families differs from the locked family set")
    conds = {x.get("type"): x for x in st.get("conditions", [])}
    pr = conds.get("PredictionReady") or {}
    if pr.get("status") != "True" or pr.get("reason") != "Ready":
        forecast.append(f"PredictionReady is {pr.get('status')}/{pr.get('reason')}, not True/Ready")
    pred = st.get("prediction") or {}
    if pred.get("eligible") is not True:
        forecast.append("status.prediction.eligible is not true (the operator has not verified a forecast)")
    want = c.lock["startup"][c.profile]["frozen_lead_seconds"]
    lead = (pred.get("lead") or {}).get("seconds")
    if lead != want or pred.get("frozenLeadSeconds") != want:
        forecast.append(f"lead {lead}/frozen {pred.get('frozenLeadSeconds')} differs from the locked {want} s")
    if not forecast:
        return safety, "qualified"
    if c.s.get("itt_fallback") is True:
        deadline = c.lock["schedule"].get("qualification_deadline_utc", "")
        if _TS.match(str(deadline)) and c.now() >= _utc(deadline):
            return safety, "itt-fallback"
        return safety + forecast + [f"the intention-to-treat fallback is only allowed after the qualification "
                                    f"deadline {deadline or '(not in the run-lock)'}"], "n/a"
    return safety + forecast, "n/a"


def _lock_problems(c: Context) -> List[str]:
    if c.s["phase"] != "confirmation":
        return ["a preparation session runs ordinary plans only (scored plans are confirmation)"] \
            if c.plan.get("kind") != "ordinary" else []
    out, lock = [], c.lock
    if lock["kubeconfig"]["cluster_uid"] != c.record.cluster_uid:
        out.append("the run-lock is for another cluster")
    scope = lock["activation_scope"]
    if (scope.get("targets") or {}).get(c.target) != c.target_uid or c.target_uid not in scope["target_uids"]:
        out.append(f"{c.target}'s UID {c.target_uid} is not the signed run-lock's target UID")
    if c.ns not in scope["namespaces"]:
        out.append("the fixture namespace is outside the activation scope")
    if lock["images"].get("generator") != c.s["generator_image"]:
        out.append("the generator image is not the signed run-lock's")
    if lock["images"].get("fixture") != c.params["image"]:
        out.append("the fixture image in the params is not the signed run-lock's")
    key = f"{c.arm}/{c.profile}/{c.seed}"
    if c.s.get("block_start") not in lock["schedule"]["blocks"].get(key, []):
        out.append(f"{c.s.get('block_start')} is not a permitted date for {key}")
    want = lock["workload"]["plans"].get(f"scored/{c.seed}/{c.target}")
    if c.plan.get("digest") != want or c.plan.get("accelerate") != 1 or c.plan.get("kind") != "scored":
        out.append("the plan is not the run-lock's locked confirmation plan for this seed and target")
    if float(c.plan.get("capacity_rps", -1)) != float(lock["capacity"]["c_rps"]):
        out.append("the plan's capacity differs from the run-lock")
    if not isinstance(lock["schedule"].get("max_release_lateness_s"), int):
        out.append("the run-lock predeclares no max_release_lateness_s")
    import freeze
    ok, detail = freeze.reviewer_status(c.record, c.lock_digest)
    if not ok:
        out.append(f"reviewer receipts: {detail}")
    elif not freeze.signed_prerequisite(c.record, c.lock_digest, "quota_injection_plan"):
        out.append("the quota intervention is NOT READY: no reviewed injection plan is a signed prerequisite "
                   "(quota_injection_plan)")
    return out


def _budget(c: Context) -> Dict:
    per_min = [st["target"] for st in c.plan["stages"]]
    peak_rps = max(per_min) / 60 if per_min else 0
    total = sum(c.plan["scheduled_per_minute"])
    minutes = len(c.plan["scheduled_per_minute"]) * (60 / c.plan.get("accelerate", 1)) / 60
    return {"peak_rps": round(peak_rps, 3), "total_arrivals": round(total), "duration_s": int(minutes * 60)}


def _fault_docs(c: Context) -> List[Dict]:
    if c.s["phase"] != "confirmation":
        return []
    start = datetime.strptime(c.s["block_start"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return c.c28.render.with_faults(c.c28.render.render_all(c.params), c.c28.schedule.fault_timetable(start), [c.target])


def _fault_state(c: Context) -> Dict[str, Dict]:
    out = {}
    for ns in sorted({c.ns, c.params["control_namespace"]}):
        for cm in c.kube.list("ConfigMap", ns, labels={X.RUN_LABEL: c.record.run_id}):
            if cm["metadata"]["name"].endswith("-faults"):
                out[cm["metadata"]["name"]] = {"uid": cm["metadata"]["uid"], "namespace": ns,
                                               "data": cm.get("data", {}).get("faults.json")}
    return out


def _quota_problems(c: Context) -> List[str]:
    q = c.kube.get("ResourceQuota", c.params["quota"]["name"], c.ns)
    if q is None:
        return ["the fixture quota is missing"]
    out = []
    if q["spec"].get("hard", {}).get("pods") != str(c.params["quota"]["pods_normal"]):
        out.append(f"the fixture quota is at {q['spec'].get('hard', {}).get('pods')}, not its locked normal "
                   f"{c.params['quota']['pods_normal']}")
    if c.lock and q["metadata"]["uid"] != c.lock["fault_scope"]["quota_uid"]:
        out.append("the fixture quota's UID differs from the run-lock")
    return out


def generator_job(c: Context, active_deadline: int, suspend: bool) -> Dict:
    s, names, r = c.s, c.names(), c.params["reservation"]
    # Test-wide tags on every k6 sample, the session's included, so its metrics are attributable to THIS session.
    tags = {"c28_session": c.session_id, "c28_target": c.target, "c28_plan": c.plan["digest"],
            "c28_seed": str(c.plan["seed"]), "c28_label": c.plan["label"]}
    # The metrics leave k6 through its built-in remote-write OUTPUT (--out), configured by real process env (the
    # container's env), never by script variables (-e).
    env = ["run", "--out", K6_OUTPUT] + [a for k, v in sorted(tags.items()) for a in ("--tag", f"{k}={v}")] + [
           "-e", "C28_PLAN=/plan/plan.json", "-e", f"C28_BASE_URL=http://c28-ingress.{c.ns}.svc",
           "-e", "C28_SUMMARY=/tmp/summary.json"]
    for k, v in sorted((s.get("generator_env") or {}).items()):
        env += ["-e", f"{k}={v}"]
    res = s.get("generator_resources") or {"requests": {"cpu": "500m", "memory": "256Mi"},
                                           "limits": {"cpu": "1", "memory": "512Mi"}}
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": names["job"], "namespace": c.ns, "labels": c.labels()},
            "spec": {"backoffLimit": 0, "activeDeadlineSeconds": active_deadline, "suspend": suspend,
                     "template": {"metadata": {"labels": c.labels(),
                                               "annotations": dict(c.c28.render.SIDECAR_FREE)}, "spec": {
                         "restartPolicy": "Never", "automountServiceAccountToken": False,
                         "securityContext": {"runAsNonRoot": True, "runAsUser": 12345,
                                             "seccompProfile": {"type": "RuntimeDefault"}},
                         "tolerations": [{"key": r["taint_key"], "operator": "Equal", "value": r["taint_value"],
                                          "effect": "NoSchedule"}],
                         "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                             "nodeSelectorTerms": [{"matchExpressions": [
                                 {"key": r["role_label"], "operator": "In", "values": ["support"]},
                                 {"key": "kubernetes.io/hostname", "operator": "In",
                                  "values": [c.params["nodes"]["support"]]}]}]}}},
                         "containers": [{"name": "k6", "image": s["generator_image"], "args": env + ["/plan/c28.js"],
                                         "env": [{"name": "K6_PROMETHEUS_RW_SERVER_URL",
                                                  "value": s["generator_output"]["remote_write_url"]},
                                                 {"name": "K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM",
                                                  "value": "true"},
                                                 {"name": "K6_PROMETHEUS_RW_PUSH_INTERVAL",
                                                  "value": f"{int(s['generator_output'].get('push_interval_s', 5))}s"}],
                                         "resources": res,
                                         "securityContext": {"allowPrivilegeEscalation": False,
                                                             "capabilities": {"drop": ["ALL"]}},
                                         "volumeMounts": [{"name": "plan", "mountPath": "/plan", "readOnly": True}]}],
                         "volumes": [{"name": "plan", "configMap": {"name": names["plan_cm"]}}]}}}}


def _residency_problems(c: Context, job: Dict) -> List[str]:
    """The session stays inside the params' residency budget: its generator is sidecar-free and requests no more than
    the budgeted generator, and every other fixture target sits at no more than the budgeted idle replica count (the
    W computation assumes it; nothing here parks or restores replicas)."""
    R, out = c.c28.render, []
    try:
        R.audit_sidecar_free([job])
        r = R.residency_params(c.params)
    except R.ParamsError as e:
        return [str(e)]
    req = (job["spec"]["template"]["spec"]["containers"][0].get("resources") or {}).get("requests") or {}
    try:
        cpu, mem = R.cpu_millicores(req.get("cpu", "0")), R.memory_bytes(req.get("memory", "0"))
    except R.ParamsError as e:
        return [f"session.generator_resources: {e}"]
    if cpu <= 0 or mem <= 0:
        out.append("session.generator_resources must set CPU and memory requests (the residency budget counts them)")
    if cpu > r["generator"][0] or mem > r["generator"][1]:
        out.append(f"the generator requests ({req.get('cpu')}, {req.get('memory')}) exceed the residency budget's "
                   "generator (params residency.generator)")
    for profile in c.c28.frozen.PROFILES:
        for arm in c.c28.frozen.ARMS:
            name = R.target_name(arm, profile)
            dep = c.kube.get("Deployment", name, c.ns) if name != c.target else None
            if dep is not None and (dep["spec"].get("replicas") or 0) > r["idle_arm_replicas"]:
                out.append(f"{name} has {dep['spec'].get('replicas')} replicas, above the residency budget's idle "
                           f"count {r['idle_arm_replicas']}: restore it before loading another arm")
    return out


def _load_script() -> str:
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "load", "c28.js")
    with open(path) as fh:
        return fh.read()


def job_deadline(c: Context, b: Dict) -> int:
    """activeDeadlineSeconds, counted by Kubernetes from the Job's start: to the session deadline for a block, else the
    plan's duration plus the admitted slack."""
    if c.s["phase"] == "confirmation":
        return max(1, _utc(c.s["deadline_utc"]) - _utc(c.s["block_start"]))
    return b["duration_s"] + int(c.s.get("generator_slack_s", 900))


def _writer_obj(c: Context) -> Dict:
    return [o for o in c.c28.render.render_arm(c.params, c.arm, c.profile)
            if o["kind"] in ("HorizontalPodAutoscaler", "ScaledObject")][0]


# --- start ----------------------------------------------------------------------------------------------------------

def plan_start(c: Context) -> Dict:
    problems: List[str] = []
    entries = X.receipts(c.record, HELPER)
    dep = c.kube.get("Deployment", c.target, c.ns)
    if dep is None or dep["metadata"]["uid"] != c.target_uid:
        problems.append(f"{c.target} is missing or replaced (UID differs from targets.json)")
    problems += _lock_problems(c)
    problems += _config_problems(c)
    if _open_sessions(entries):
        problems.append(f"sessions still open or unresolved in the journal: {_open_sessions(entries)} (one loaded "
                        "arm at a time)")
    if X.open_intents(entries):
        problems.append("the journal has intents with unknown outcomes: run recover first")
    active = _active_generators(c)
    if active:
        problems.append(f"generators of the run still active: {active} (one loaded arm at a time)")
    problems += _writer_problems(c)
    readiness_problems, readiness = _readiness(c)
    problems += readiness_problems
    problems += _node_stops(c)
    problems += _occupant_stops(c)                  # the reservation must verify before any session starts
    problems += _quota_problems(c)
    b, budget = _budget(c), c.s["arrival_budget"]
    if b["peak_rps"] > budget["max_rps"] or b["total_arrivals"] > budget["max_total_arrivals"]:
        problems.append(f"the plan ({b}) exceeds the admitted arrival budget {budget}")
    now, deadline = c.now(), _utc(c.s["deadline_utc"])
    if c.s["phase"] == "confirmation":
        begin = _utc(c.s["block_start"])
        if now > begin:
            problems.append("the block's start has passed: a late start is a replacement slot, never a shifted day")
        elif begin - now > PRE_ROLL_S:
            problems.append(f"too early: start within {PRE_ROLL_S} s before the block (the generator is released "
                            "at the block's start by the watchdog)")
        if deadline < begin + b["duration_s"]:
            problems.append("the deadline does not cover the block")
    elif deadline < now + b["duration_s"]:
        problems.append("the deadline does not cover the plan")
    faults = _fault_state(c)
    planned_faults = []
    if c.s["phase"] == "preparation":
        dirty = sorted(n for n, f in faults.items() if f["data"] != "{}")
        if dirty:
            problems.append(f"preparation runs fault-disabled, but these schedules are not empty: {dirty}")
    else:
        for doc in _fault_docs(c):
            name = doc["metadata"]["name"]
            cur = faults.get(name)
            if cur is None:
                problems.append(f"fault ConfigMap {name} is missing")
                continue
            if cur["data"] != "{}":
                problems.append(f"{name} is not empty before the block (a leftover schedule)")
            planned_faults.append({"name": name, "namespace": cur["namespace"], "uid": cur["uid"],
                                   "data": doc["data"]["faults.json"]})
    names = c.names()
    for kind, name in (("Job", names["job"]), ("ConfigMap", names["plan_cm"])):
        if c.kube.get(kind, name, c.ns) is not None:
            problems.append(f"{kind}/{name} already exists (a session id is used once)")
    script = _load_script()
    actions: List[Dict] = [{"op": "write-faults", "name": f["name"], "uid": f["uid"],
                            "sha256": X.sha256_bytes(f["data"].encode())} for f in planned_faults]
    activate = c.s["phase"] == "confirmation" or c.arm in WRITER_KIND
    if activate and c.arm in WRITER_KIND:
        actions.append({"op": "create-writer", "kind": WRITER_KIND[c.arm], "name": c.target,
                        "sha256": X.json_digest(_writer_obj(c))})
    elif activate:
        uid = c.pa_uids.get(c.target, "")
        actions.append({"op": "activate", "kind": "PredictiveAutoscaler", "name": c.target, "uid": uid,
                        "from": "Recommend", "to": "Active"})
    job = generator_job(c, job_deadline(c, b), suspend=c.s["phase"] == "confirmation")
    problems += _residency_problems(c, job)
    actions.append({"op": "create-plan-configmap", "name": names["plan_cm"], "script_sha256":
                    X.sha256_bytes(script.encode()), "plan_digest_k6": c.plan["digest"]})
    actions.append({"op": "create-generator", "name": names["job"], "sha256": X.json_digest(job),
                    "suspended_until": c.s.get("block_start") if c.s["phase"] == "confirmation" else None,
                    "active_deadline_seconds": job["spec"]["activeDeadlineSeconds"]})
    return X.new_plan(HELPER, c.record, actions, session=c.session_id, phase=c.s["phase"],
                      runlock_sha256=c.lock_digest, readiness=readiness,
                      target={"name": c.target, "uid": c.target_uid, "arm": c.arm, "profile": c.profile,
                              "seed": c.seed},
                      budget=b, deadline_utc=c.s["deadline_utc"], faults=planned_faults, problems=problems)


def _set_faults(expect: Optional[str], value: str):
    def mutate(body):
        if expect is not None and (body.get("data") or {}).get("faults.json") != expect:
            raise X.StagingError(f"{body['metadata']['name']} is not as reviewed; refusing")
        body.setdefault("data", {})["faults.json"] = value
    return mutate


def _set_mode(expect: Optional[str], value: str):
    def mutate(body):
        if expect is not None and body["spec"].get("mode") != expect:
            raise X.StagingError(f"{body['metadata']['name']} is in mode {body['spec'].get('mode')}, not {expect}")
        body["spec"]["mode"] = value
    return mutate


def start(c: Context, reviewed: Dict, reviewed_digest: str) -> Dict:
    with X.mutating(c.record, c.kube, HELPER, {"session": c.session_id, "phase": "start"}) as lock:
        # Validate UNDER the shared lock: no other executor can change the evaluation state in between.
        p = plan_start(c)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
        if p["problems"]:
            raise X.StagingError("the start plan has problems: " + "; ".join(p["problems"]))
        X.receipt(c.record, HELPER, "start-begin", {"session": c.session_id, "plan_sha256": reviewed_digest,
                                                    "deadline_utc": c.s["deadline_utc"], "readiness": p["readiness"]})
        try:
            for f in p["faults"]:
                c.kube.update("ConfigMap", f["name"], f["namespace"], f["uid"], _set_faults("{}", f["data"]),
                              expect=[["/data/faults.json", f["data"]]])
            for a in p["actions"]:
                if a["op"] == "activate":
                    c.kube.update("PredictiveAutoscaler", c.target, c.ns, a["uid"], _set_mode("Recommend", "Active"),
                                  expect=[["/spec/mode", "Active"]])
                elif a["op"] == "create-writer":
                    w = _writer_obj(c)
                    if X.json_digest(w) != a["sha256"]:
                        raise X.StagingError("the writer rendered differently from the reviewed plan")
                    c.kube.create(w)
            names = c.names()
            cm = {"apiVersion": "v1", "kind": "ConfigMap",
                  "metadata": {"name": names["plan_cm"], "namespace": c.ns, "labels": c.labels()},
                  "data": {"plan.json": json.dumps(c.plan, sort_keys=True), "c28.js": _load_script()}}
            c.kube.create(cm)
            job = generator_job(c, job_deadline(c, _budget(c)), suspend=c.s["phase"] == "confirmation")
            if X.json_digest(job) != p["actions"][-1]["sha256"]:
                raise X.StagingError("the generator Job differs from the reviewed plan")
            c.kube.create(job)
        except Exception as e:
            entries = X.receipts(c.record, HELPER)
            X.receipt(c.record, HELPER, "start-aborted", {"session": c.session_id, "plan_sha256": reviewed_digest,
                                                          "error": str(e)[:300], "outcome_known": lock.safe,
                                                          "owned": owned_from_journal(c, entries)})
            if lock.safe:            # every call so far has a known outcome: undo exactly what was applied
                _undo(c, reason="start aborted")
            raise
        owned = owned_from_journal(c, X.receipts(c.record, HELPER))
        return X.receipt(c.record, HELPER, "session-started", {
            "session": c.session_id, "plan_sha256": reviewed_digest, "phase": c.s["phase"], "target": p["target"],
            "runlock_sha256": c.lock_digest, "deadline_utc": c.s["deadline_utc"], "readiness": p["readiness"],
            "owned": owned})


# --- stop, quiescence and undo ----------------------------------------------------------------------------------------

def plan_stop(c: Context) -> Dict:
    entries = X.receipts(c.record, HELPER)
    state = session_state(entries, c.session_id)
    if state is None:
        raise X.StagingError(f"session {c.session_id} is not open in the journal")
    owned = owned_from_journal(c, entries)
    steps, unresolved = [], []

    def live_of(kind, ref, ns):
        live = c.kube.get(kind, ref["name"], ns)
        if live is not None and live["metadata"]["uid"] != ref["uid"]:
            unresolved.append(f"{kind}/{ref['name']} was replaced under the session (UID {live['metadata']['uid']}, "
                              f"owned {ref['uid']}): not touched, cleanup unresolved")
            return None
        return live
    if owned["job"]:
        j = live_of("Job", owned["job"], c.ns)
        if j is not None:
            steps.append({"op": "delete", "kind": "Job", "name": owned["job"]["name"], "uid": owned["job"]["uid"],
                          "status": {k: j.get("status", {}).get(k) for k in ("active", "succeeded", "failed",
                                                                              "startTime", "completionTime")}})
    for f in owned["faults"]:
        cm = c.kube.get("ConfigMap", f["name"], f["namespace"])
        if cm is None or cm["metadata"]["uid"] != f["uid"]:
            unresolved.append(f"fault ConfigMap {f['name']} is gone or replaced: its schedule cannot be cleared")
        elif cm.get("data", {}).get("faults.json") != "{}":
            steps.append({"op": "clear-faults", "name": f["name"], "namespace": f["namespace"], "uid": f["uid"],
                          "current": cm["data"]["faults.json"]})
    w = owned["writer"]
    if w:
        live = live_of(w["kind"], w, c.ns)
        if w["action"] == "mode":
            if live is None and not any(u.startswith(f"{w['kind']}/{w['name']}") for u in unresolved):
                unresolved.append(f"{w['kind']}/{w['name']} is gone: its shutdown cannot be confirmed")
            elif live is not None and live["spec"].get("mode") != "Recommend":
                steps.append({"op": "recommend", "kind": w["kind"], "name": w["name"], "uid": w["uid"],
                              "mode_now": live["spec"].get("mode")})
        elif live is not None:
            steps.append({"op": "delete", "kind": w["kind"], "name": w["name"], "uid": w["uid"]})
    if c.lock:
        fs = c.lock["fault_scope"]
        q = c.kube.get("ResourceQuota", fs["quota_name"], fs["namespace"])
        if q is not None and q["metadata"]["uid"] == fs["quota_uid"] and \
                q["spec"].get("hard", {}).get("pods") != str(fs["pods_normal"]):
            steps.append({"op": "restore-quota", "name": fs["quota_name"], "uid": fs["quota_uid"],
                          "pods_now": q["spec"]["hard"].get("pods"), "pods_normal": fs["pods_normal"]})
    if owned["plan_cm"]:
        steps.append({"op": "keep", "kind": "ConfigMap", "name": owned["plan_cm"]["name"],
                      "why": "the session's plan stays as evidence"})
    order = {"delete-Job": 0, "clear-faults": 1, "recommend": 2, "delete": 2, "restore-quota": 3, "keep": 4}
    steps.sort(key=lambda s: order.get(f"{s['op']}-{s.get('kind')}", order[s["op"]]))
    return X.new_plan(STOP, c.record, steps, session=c.session_id, owned=owned, unresolved=unresolved,
                      never=["evict", "scale a target", "touch an object the journal did not record"])


def _run_steps(c: Context, steps: List[Dict]) -> List[str]:
    """Run each step; return the errors (a failure never skips the remaining steps)."""
    errors = []
    for st in steps:
        try:
            if st["op"] == "delete":
                c.kube.delete(st["kind"], st["name"], c.ns, st["uid"])
            elif st["op"] == "clear-faults":
                c.kube.update("ConfigMap", st["name"], st["namespace"], st["uid"], _set_faults(st.get("current"), "{}"),
                              expect=[["/data/faults.json", "{}"]])
            elif st["op"] == "recommend":
                c.kube.update(st["kind"], st["name"], c.ns, st["uid"], _set_mode(None, "Recommend"),
                              expect=[["/spec/mode", "Recommend"]])
            elif st["op"] == "restore-quota":
                def restore(body, st=st):
                    body["spec"]["hard"]["pods"] = str(st["pods_normal"])     # only the run-lock's locked limit
                c.kube.update("ResourceQuota", st["name"], c.ns, st["uid"], restore,
                              expect=[["/spec/hard/pods", str(st["pods_normal"])]])
        except X.StagingError as e:
            errors.append(f"{st['op']} {st.get('name')}: {e}")
    return errors


def _stop_generation(c: Context, w: Dict) -> int:
    """The generation our latest Recommend update produced (from its step's own response)."""
    gen = 0
    for it in X.applied_intents(X.receipts(c.record, HELPER)):
        d = it["data"]
        if d["op"] == "update" and d["kind"] == w["kind"] and d["name"] == w["name"] and \
                ["/spec/mode", "Recommend"] in d.get("expect", []):
            gen = int(d.get("result_generation") or 0)
    return gen


def quiesce(c: Context, owned: Dict) -> List[str]:
    """Wait until the session's generator pods are gone (terminating ones count as running), in-flight requests have
    timed out, and the writer is confirmed inactive. Returns what is still unresolved at the timeout."""
    timeout = int(c.s.get("quiesce_timeout_s", 300))
    deadline = c.now() + timeout
    while True:
        left = []
        pods = c.kube.list("Pod", c.ns, labels={X.SESSION_LABEL: c.session_id})
        running = sorted(p["metadata"]["name"] for p in pods
                         if p.get("status", {}).get("phase") not in ("Succeeded", "Failed")
                         or p["metadata"].get("deletionTimestamp"))
        if running:
            left.append(f"generator pods still present: {running}")
        w = owned.get("writer")
        if w and w["action"] == "mode":
            pa = c.kube.get(w["kind"], w["name"], c.ns)
            if pa is None or pa["metadata"]["uid"] != w["uid"]:
                left.append(f"{w['kind']}/{w['name']} is gone or replaced: shutdown unconfirmed")
            else:
                st = pa.get("status") or {}
                stop_gen = _stop_generation(c, w) or pa["metadata"].get("generation", 0)
                if pa["spec"].get("mode") != "Recommend" or st.get("mode") != "Recommend" or \
                        not isinstance(st.get("observedGeneration"), int) or st["observedGeneration"] < stop_gen:
                    left.append(f"{w['kind']}/{w['name']}: the operator has not confirmed Recommend for generation "
                                f"{stop_gen} (spec {pa['spec'].get('mode')}, observed {st.get('mode')} at generation "
                                f"{st.get('observedGeneration')})")
        elif w:
            if c.kube.get(w["kind"], w["name"], c.ns) is not None:
                left.append(f"{w['kind']}/{w['name']} still exists")
            if w["kind"] == "ScaledObject" and c.kube.get("HorizontalPodAutoscaler", f"keda-hpa-{w['name']}", c.ns):
                left.append(f"KEDA's HPA keda-hpa-{w['name']} still exists")
        if not left:
            c.sleep(c.c28.frozen.REQUEST_TIMEOUT_S + 1)      # requests already dispatched finish or time out
            return []
        if c.now() >= deadline:
            return left
        c.sleep(5)


def _finish_stop(c: Context, steps: List[Dict], unresolved: List[str], authorised_by: str, reason: str,
                 event: str = "session-stopped") -> Dict:
    errors = _run_steps(c, steps)
    owned = owned_from_journal(c, X.receipts(c.record, HELPER))
    left = quiesce(c, owned) if not errors else ["not quiesced: cleanup steps failed"]
    open_items = list(unresolved) + errors + left
    out = X.receipt(c.record, HELPER, event, {
        "session": c.session_id, "plan_sha256": authorised_by, "reason": reason, "steps": steps,
        "resolved": not open_items, "unresolved": open_items})
    if not open_items:
        X.receipt(c.record, HELPER, "session-closed", {"session": c.session_id, "after": out["sha256"]})
    return out


def stop(c: Context, reviewed: Dict, reviewed_digest: str, reason: str = "operator stop") -> Dict:
    with X.mutating(c.record, c.kube, HELPER, {"session": c.session_id, "phase": "stop"}):
        p = plan_stop(c)
        X.require_live(reviewed, reviewed_digest, STOP, c.record, p)
        return _finish_stop(c, p["actions"], p["unresolved"], reviewed_digest, reason)


def _undo(c: Context, reason: str) -> Dict:
    """Reverse what a failed start applied (from the journal), in stop order, then confirm quiescence."""
    p = plan_stop(c)
    if c.kube.journal is not None:
        c.kube.journal.context = {"session": c.session_id, "phase": "undo"}
    return _finish_stop(c, [s for s in p["actions"] if s["op"] != "restore-quota"], p["unresolved"], "",
                        reason, event="start-undone")


# --- verify and the watchdog ----------------------------------------------------------------------------------------

def steal_stop(c: Context, fetch: Callable[[str], Dict]) -> List[str]:
    """Manifest stop: guest steal above the bar for N consecutive one-minute bins on W or S, or the observation missing
    (either node absent, a non-finite or malformed value, stale or non-consecutive samples, a failed query)."""
    st = c.s.get("stops") or {}
    if not st.get("steal_query") or not st.get("query_api"):
        return ["required observation missing: no steal query configured"]
    bins, bar = int(st.get("steal_bins", 5)), float(st.get("steal_percent", 5))
    label = st.get("node_label", "node")
    now = c.now()
    end = now - now % 60
    q = urllib.parse.urlencode({"query": st["steal_query"], "start": end - 60 * (bins + 1), "end": end, "step": 60})
    try:
        doc = fetch(f"{st['query_api'].rstrip('/')}/api/v1/query_range?{q}")
        series = {s["metric"][label]: s.get("values", []) for s in doc["data"]["result"]
                  if isinstance(s, dict) and label in (s.get("metric") or {})}
    except Exception as e:      # noqa: BLE001 - any failure is a missing observation
        return [f"required observation missing: steal query failed ({type(e).__name__})"]
    out = []
    for role in ("workload", "support"):
        node = c.params["nodes"][role]
        raw = series.get(node)
        if not raw:
            out.append(f"required observation missing: no steal series for the {role} node {node}")
            continue
        try:
            pts = [(int(float(t)), float(v)) for t, v in raw]
        except (TypeError, ValueError):
            out.append(f"required observation missing: malformed steal samples for {node}")
            continue
        pts = pts[-bins:]
        if len(pts) < bins or not all(math.isfinite(v) for _, v in pts):
            out.append(f"required observation missing: {node} lacks {bins} finite samples")
            continue
        ts = [t for t, _ in pts]
        if any(b - a != 60 for a, b in zip(ts, ts[1:])) or now - ts[-1] > 120:
            out.append(f"required observation missing: {node}'s samples are stale or not consecutive")
            continue
        if all(v > bar for _, v in pts):
            out.append(f"guest steal above {bar}% for {bins} consecutive minutes on {node}")
    return out


def _occupant_stops(c: Context) -> List[str]:
    """The full reservation verification (reserve.verify): the run taint and role labels, pressure, EVERY occupant
    that is not allowed-system or evaluation (movable-pending-owner-move, unplanned, stateful), broad tolerations
    and the rendered placement. Nothing is filtered by message text."""
    if not c.record.doc.get("reserve"):
        return ["required observation missing: the record has no reserve section for the reservation check"]
    import reserve
    occ = reserve.verify(c.record, c.kube)
    return [f"reservation: {p}" for p in occ["problems"]]


def _http_json(url: str) -> Dict:
    with urllib.request.urlopen(url, timeout=10) as r:       # noqa: S310 - the record's in-cluster query API
        return json.loads(r.read())


def verify(c: Context, fetch: Callable[[str], Dict] = _http_json) -> Dict:
    entries = X.receipts(c.record, HELPER)
    state = session_state(entries, c.session_id)
    if state is None:
        return {"helper": HELPER, "action": "verify", "session": c.session_id, "state": "not-open", "stops": []}
    owned = owned_from_journal(c, entries)
    stops: List[str] = []
    if X.open_intents(entries):
        stops.append("intents with unknown outcomes: recovery needed")
    stops += _writer_problems(c, owned)
    stops += _node_stops(c)
    stops += steal_stop(c, fetch)
    stops += _occupant_stops(c)
    if c.now() >= _utc(state["begin"]["data"]["deadline_utc"]):
        stops.append("the session deadline has passed")
    if owned.get("job"):
        j = c.kube.get("Job", owned["job"]["name"], c.ns)
        if j is None or j["metadata"]["uid"] != owned["job"]["uid"]:
            stops.append("the generator Job is gone or replaced")
        elif any(cd.get("type") == "Failed" and cd.get("status") == "True"
                 for cd in j.get("status", {}).get("conditions", [])):
            stops.append("the generator Job failed")
    return {"helper": HELPER, "action": "verify", "session": c.session_id, "state": "open",
            "started": state["started"] is not None, "stops": stops, "result": "OK" if not stops else "STOP"}


def _classify(c: Context, classification: str, lateness_s: Optional[int], k6: str = "") -> None:
    if not X.last(X.receipts(c.record, HELPER), "block-classified", session=c.session_id):
        X.receipt(c.record, HELPER, "block-classified", {"session": c.session_id, "block_start": c.s["block_start"],
                                                         "classification": classification, "lateness_s": lateness_s,
                                                         "k6_container": k6})


def release(c: Context, fetch: Callable[[str], Dict] = None) -> List[str]:
    """For a block: release the suspended generator at the block start, then classify the block by its schedule-start
    acknowledgment (the first arrival the independent ingress counted for this target), never by a container start.
    Returns stop reasons (a release or a first arrival later than the predeclared limit)."""
    if c.s["phase"] != "confirmation":
        return []
    begin = _utc(c.s["block_start"])
    limit = int(c.lock["schedule"].get("max_release_lateness_s", 0))
    entries = X.receipts(c.record, HELPER)
    owned = owned_from_journal(c, entries)
    now = c.now()
    if now < begin or not owned.get("job"):
        return []
    if not X.last(entries, "generator-released", session=c.session_id):
        if now - begin > limit:
            _classify(c, "late-replacement-slot", now - begin, "not released")
            return [f"the release window passed ({now - begin} s > {limit} s after the block start)"]
        j = owned["job"]

        def unsuspend(body):
            if body["spec"].get("suspend") is not True:
                raise X.StagingError("the generator is not suspended as reviewed; refusing")
            body["spec"]["suspend"] = False
        with X.mutating(c.record, c.kube, HELPER, {"session": c.session_id, "phase": "release"}):
            c.kube.update("Job", j["name"], c.ns, j["uid"], unsuspend, expect=[["/spec/suspend", False]])
        X.receipt(c.record, HELPER, "generator-released", {"session": c.session_id, "released_at": _ts(now),
                                                           "offset_s": now - begin})
        return []
    if X.last(entries, "block-classified", session=c.session_id):
        return []
    k6 = _k6_state(c, owned["job"]["uid"])
    first, why = acknowledgment(c, fetch, begin, k6)
    if first is not None:
        lateness = first - begin
        _classify(c, "on-time" if lateness <= limit else "late-replacement-slot", lateness, k6)
        return [] if lateness <= limit else [f"the first scheduled arrival reached the ingress {lateness} s after the "
                                             f"block start (> {limit} s)"]
    if now - begin > limit:
        _classify(c, "late-replacement-slot", None, k6)
        return [f"the block is UNACKNOWLEDGED {limit} s after its start (k6: {k6}; {why})"]
    return []


def _k6_state(c: Context, job_uid: str) -> str:
    """The k6 container of the OWNED Job's pod (bound by the pod's controller reference to that Job UID): running,
    waiting, terminated or absent. A diagnostic only: a started container is not a started arrival schedule."""
    for p in c.kube.list("Pod", c.ns, labels={X.SESSION_LABEL: c.session_id}):
        refs = [r for r in p["metadata"].get("ownerReferences", []) if r.get("controller")]
        if not refs or refs[0].get("kind") != "Job" or refs[0].get("uid") != job_uid:
            continue
        for cs in (p.get("status") or {}).get("containerStatuses") or []:
            if cs.get("name") == "k6":
                return next(iter((cs.get("state") or {"absent": {}}).keys()))
    return "absent"


def _series(c: Context, fetch: Callable[[str], Dict], selector: str, begin: int, step: int) -> Tuple[Optional[Dict], str]:
    st = c.s.get("stops") or {}
    q = urllib.parse.urlencode({"query": selector, "start": begin - 60, "end": c.now(), "step": step})
    try:
        result = fetch(f"{st['query_api'].rstrip('/')}/api/v1/query_range?{q}")["data"]["result"]
    except Exception as e:      # noqa: BLE001
        return None, f"query failed ({type(e).__name__})"
    if len(result) != 1:
        return None, f"{len(result)} series for {selector} (exactly one required)"
    return result[0], ""


def _first_rise(series: Dict, begin: int, new_series: bool) -> Tuple[Optional[int], str]:
    try:
        values = [(int(float(t)), float(v)) for t, v in series.get("values", [])]
    except (TypeError, ValueError):
        return None, "malformed samples"
    if not values or not all(math.isfinite(v) for _, v in values):
        return None, "no finite samples"
    base = [v for t, v in values if t <= begin]
    if not base and not new_series:
        return None, "no sample at the block start to compare with"
    ref = base[-1] if base else 0.0
    for t, v in values:
        if t > begin and v > ref:
            return t, ""
    return None, "no rise after the block start"


def acknowledgment(c: Context, fetch: Callable[[str], Dict], begin: int, k6: str) -> Tuple[Optional[int], str]:
    """The block's schedule-start acknowledgment, attributed to THIS session; (first arrival time, why not).
    All of these are required, or the block stays UNACKNOWLEDGED:
      * owned k6 startup evidence: the k6 container of the pod bound to the owned Job UID has started;
      * the generator's own counter (arrival_ack.generator.metric, default k6_iterations_total) carrying this
        session's c28_session, c28_target and c28_plan tags: exactly one series, rising after the block start;
      * the admitted ingress counter (arrival_ack.ingress.metric, default c28_ingress_requests_total) for this target
        with every recorded attribution label (arrival_ack.ingress.labels: e.g. namespace, pod, run): exactly one
        series whose labels match, rising after the block start and not before the generator's first iteration.
    A target-wide counter increase alone never acknowledges a block."""
    if k6 not in ("running", "terminated"):
        return None, f"no owned k6 startup evidence (k6 container {k6})"
    ack = c.s.get("arrival_ack") or {}
    st = c.s.get("stops") or {}
    if not st.get("query_api"):
        return None, "no query API configured"
    step = int(ack.get("step_s", 5))
    gen = ack.get("generator") or {}
    gsel = (f'{gen.get("metric", "k6_iterations_total")}{{c28_session="{c.session_id}",c28_target="{c.target}",'
            f'c28_plan="{c.plan["digest"]}"}}')
    gs, why = _series(c, fetch, gsel, begin, step)
    if gs is None:
        return None, f"generator: {why}"
    want = {"c28_session": c.session_id, "c28_target": c.target, "c28_plan": c.plan["digest"]}
    if any((gs.get("metric") or {}).get(k) != v for k, v in want.items()):
        return None, "generator: the series' session, target or plan labels are not this session's"
    gfirst, why = _first_rise(gs, begin, new_series=True)
    if gfirst is None:
        return None, f"generator: {why}"
    ing = ack.get("ingress") or {}
    labels = dict(ing.get("labels") or {})
    if not labels:
        return None, "ingress: no attribution labels recorded (arrival_ack.ingress.labels)"
    labels["target"] = c.target
    isel = ing.get("metric", "c28_ingress_requests_total") + "{" + ",".join(
        f'{k}="{v}"' for k, v in sorted(labels.items())) + "}"
    ins, why = _series(c, fetch, isel, begin, step)
    if ins is None:
        return None, f"ingress: {why}"
    if any((ins.get("metric") or {}).get(k) != v for k, v in labels.items()):
        return None, "ingress: the series' labels do not match the recorded attribution"
    ifirst, why = _first_rise(ins, begin, new_series=False)
    if ifirst is None:
        return None, f"ingress: {why}"
    if ifirst < gfirst - step:
        return None, "ingress: the counter rose before this session's generator started (not attributable)"
    return max(gfirst, ifirst), ""


def watch(c: Context, start_digest: str, fetch: Callable[[str], Dict] = _http_json, interval: int = 30,
          max_checks: int = 10 ** 6) -> Dict:
    """Check every `interval` seconds; on a stop condition, an observation or API failure, or the deadline, stop the
    session's own objects (authorised by the digest of the reviewed start plan that created them). A stop that cannot
    complete is recorded as unresolved and reported, never as success."""
    state = session_state(X.receipts(c.record, HELPER), c.session_id)
    if state is None or state["begin"]["data"].get("plan_sha256") != start_digest:
        raise X.StagingError("the watchdog needs the digest of the reviewed start plan of the open session")
    for _ in range(max_checks):
        try:
            v = verify(c, fetch)
            if v["state"] != "open":
                return {"helper": HELPER, "action": "watch", "session": c.session_id, "ended": "session closed"}
            stops = v["stops"] or release(c, fetch)
        except Exception as e:      # noqa: BLE001 - an API or observation failure is a stop, never a crash
            stops = [f"observation or API failure: {type(e).__name__}: {str(e)[:200]}"]
        if stops:
            reason = "watchdog: " + "; ".join(stops)[:500]
            try:
                p = plan_stop(c)
                with X.mutating(c.record, c.kube, HELPER, {"session": c.session_id, "phase": "stop"}):
                    out = _finish_stop(c, p["actions"], p["unresolved"], start_digest, reason)
            except Exception as e:      # noqa: BLE001
                X.receipt(c.record, HELPER, "stop-unresolved", {"session": c.session_id, "reason": reason,
                                                                "error": f"{type(e).__name__}: {str(e)[:200]}"})
                return {"helper": HELPER, "action": "watch", "session": c.session_id, "ended": "unresolved",
                        "stops": stops}
            return {"helper": HELPER, "action": "watch", "session": c.session_id,
                    "ended": "stopped" if out["data"]["resolved"] else "unresolved", "stops": stops,
                    "unresolved": out["data"]["unresolved"], "receipt": out["sha256"]}
        c.sleep(interval)
    return {"helper": HELPER, "action": "watch", "session": c.session_id, "ended": "check budget exhausted"}


def recover_follow_up(record: X.Record):
    """After recovery the journal is complete; a start that was in flight stays an open session whose scoped stop
    (stop-plan/stop) cleans exactly what it applied."""
    def follow(actions: List[Dict]) -> None:
        for sid in sorted({a["context"].get("session") for a in actions if a["helper"] == HELPER and a["context"]}):
            X.receipt(record, HELPER, "start-recovered", {"session": sid,
                                                          "note": "open until its stop confirms quiescence"})
    return follow


def main(argv=None, api=None, now=None, sleep=time.sleep, fetch=_http_json) -> int:
    ap = argparse.ArgumentParser(prog="session.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", required=True)
    ap.add_argument("command", nargs="?", default="plan",
                    choices=["plan", "start", "stop-plan", "stop", "verify", "watch", "recover-plan", "recover"])
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    ap.add_argument("--start-digest", default="")
    args = ap.parse_args(argv)
    try:
        record = X.load_record(args.record, ("session",))
        kube = record.kube(api)
        if args.command in ("start", "stop", "recover") and not args.plan:
            raise X.StagingError("a mutation needs --plan <the reviewed plan file> and --plan-digest")
        if args.command == "recover-plan":
            X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
            return 0
        if args.command == "recover":
            X.emit(X.recover(record, kube, X.load_plan(args.plan), args.plan_digest, recover_follow_up(record)))
            return 0
        c = Context(record, kube, now, sleep) if now else Context(record, kube, sleep=sleep)
        if args.command == "plan":
            X.emit(X.cli_plan(record, HELPER, plan_start(c), f"start-{c.session_id}"))
        elif args.command == "start":
            X.emit(start(c, X.load_plan(args.plan), args.plan_digest))
        elif args.command == "stop-plan":
            X.emit(X.cli_plan(record, STOP, plan_stop(c), f"stop-{c.session_id}"))
        elif args.command == "stop":
            X.emit(stop(c, X.load_plan(args.plan), args.plan_digest))
        elif args.command == "verify":
            out = verify(c, fetch)
            X.emit(out)
            return 0 if out.get("result", "OK") == "OK" else 1
        else:
            X.emit(watch(c, args.start_digest, fetch=fetch))
    except X.StagingError as e:
        print(f"session: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
