#!/usr/bin/env python3
"""One loaded arm session: start, stop, verify and its watchdog (STAGING-C28-PROD.md, S5b/S5c; manifest stops).

  session.py --record R [plan]                              read-only: write the start plan file, print its digest
  session.py --record R start --plan F --plan-digest D      start exactly that session
  session.py --record R stop-plan                           read-only: the scoped stop plan (from the start receipt)
  session.py --record R stop --plan F --plan-digest D       stop it: arrivals, faults, writer, quota, in that order
  session.py --record R verify                         read-only: is the session still within its contract?
  session.py --record R watch --start-digest D         the watchdog: stops the session (only its own objects) at its
                                                       deadline or on a manifest stop

A session is one target (arm/profile), one seed, one k6 plan, one generator Job. Before starting it requires:
  * the cluster identity, the target Deployment's recorded UID (prepare's targets.json), and its exact writer: a PA
    arm has exactly its own PredictiveAutoscaler and no HPA/KEDA; a baseline arm (H-CPU/H-RPS) has no writer yet
    (start creates exactly its rendered HPA or ScaledObject). Every other target: PA arms only their own PA, in
    Recommend; baseline targets no writer. Both PA API groups are inventoried.
  * a single loaded arm: no other generator of the run active, no other session of the run open in the receipts.
  * phase rules. preparation: ordinary plans only, PA arms stay in Recommend (shadow collection), every fault schedule
    empty. confirmation: a verified, locked run-lock with two signed reviewer receipts (freeze.py), the target in its
    activation scope, a date the run-lock permits for this arm/profile/seed, exactly the locked plan, the run-lock's
    fault timetable for that block, and for PA-N/PA-E the operator's own verified readiness: PredictionReady=True
    (reason Ready), status.prediction.eligible, and the frozen lead equal to the run-lock's. A successful HTTP answer
    is never a readiness verdict.
  * the arrival budget (peak rate and total scheduled arrivals of the plan), the quota at its locked normal limit,
    no MemoryPressure/DiskPressure on W or S, and a deadline that covers the plan.
The generator Job carries activeDeadlineSeconds to the session deadline, so arrivals stop even without the executor.
Stop and the watchdog touch only the objects whose UIDs the start receipt recorded.

The record's "session" section:
  {"id": "s-0001", "phase": "preparation"|"confirmation", "arm": "PA-N", "profile": "s20", "seed": 201,
   "plan": "<k6 plan file>", "block_start": "2026-11-20T00:00:00Z" (confirmation), "runlock": "<run-lock file>"
   (confirmation), "manifest_root": "<dir>" (confirmation; default the run-lock's directory),
   "generator_image": "<repo>@sha256:...", "generator_resources": {cpu/memory requests and limits},
   "generator_env": {non-secret k6 settings}, "arrival_budget": {"max_rps": N, "max_total_arrivals": N},
   "deadline_utc": "...", "stops": {"steal_query": "<PromQL per node, percent>", "query_api": "http://...",
   "steal_percent": 5, "steal_bins": 5}}
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import execlib as X

HELPER = "session"
STOP = "session-stop"
GENERATOR = "c28-generator"
PRE_ROLL_S = 600
_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$")
_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _utc(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def _ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Context:
    """Everything a session computation needs, loaded once."""

    def __init__(self, record: X.Record, kube: X.Kube, now: Callable[[], int] = lambda: int(time.time())):
        self.record, self.kube, self.now = record, kube, now
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
            root = record.resolve(s["manifest_root"]) if s.get("manifest_root") else ""
            path = record.resolve(str(s.get("runlock", "")))
            try:
                self.lock_digest, self.lock = self.c28.runlock.load_verified(
                    path, root or os.path.dirname(os.path.abspath(path)))
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


# --- checks ---------------------------------------------------------------------------------------------------------

def _all_targets(c: Context) -> List[str]:
    return sorted(c.targets["deployments"])


def _writer_problems(c: Context, phase_started: bool) -> List[str]:
    """Exactly the authorised writer on the session target; every other target only its own Recommend PA or none."""
    out = []
    inv = X.writer_inventory(c.kube, c.ns, _all_targets(c))
    kinds = {"H-CPU": "HorizontalPodAutoscaler", "H-RPS": "ScaledObject"}
    for name, writers in sorted(inv.items()):
        is_pa = name.startswith("c28-pa-")
        if name == c.target:
            if c.arm in kinds:
                want = [f"{kinds[c.arm]}/{name}"] if phase_started else []
            else:
                want = [f"PredictiveAutoscaler/{name}"]
        else:
            want = [f"PredictiveAutoscaler/{name}"] if is_pa else []
        if writers != want:
            out.append(f"{name}: writers {writers or 'none'}, expected exactly {want or 'none'}")
    for name in _all_targets(c):
        if not name.startswith("c28-pa-") or (name == c.target and phase_started and c.s["phase"] == "confirmation"):
            continue
        pa = c.kube.get("PredictiveAutoscaler", name, c.ns)
        if pa is not None and pa["spec"].get("mode") != "Recommend":
            out.append(f"PredictiveAutoscaler/{name} is {pa['spec'].get('mode')}: only the session arm may be Active")
    return out


def _open_sessions(entries: List[Dict]) -> List[str]:
    open_ = {}
    for e in entries:
        if e["event"] == "session-started":
            open_[e["data"]["session"]] = True
        elif e["event"] in ("session-stopped", "start-aborted"):
            open_.pop(e["data"]["session"], None)
    return sorted(open_)


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


def _readiness(c: Context) -> List[str]:
    """Phase-specific readiness from the operator's own verified status (never an HTTP status)."""
    if c.s["phase"] != "confirmation" or c.arm not in ("PA-N", "PA-E"):
        return []
    pa = c.kube.get("PredictiveAutoscaler", c.target, c.ns)
    if pa is None:
        return [f"PredictiveAutoscaler/{c.target} is missing"]
    out = []
    st = pa.get("status", {}) or {}
    conds = {x.get("type"): x for x in st.get("conditions", [])}
    pr = conds.get("PredictionReady") or {}
    if pr.get("status") != "True" or pr.get("reason") != "Ready":
        out.append(f"PredictionReady is {pr.get('status')}/{pr.get('reason')}, not True/Ready")
    pred = st.get("prediction") or {}
    if pred.get("eligible") is not True:
        out.append("status.prediction.eligible is not true (the operator has not verified a forecast)")
    want = c.lock["startup"][c.profile]["frozen_lead_seconds"]
    lead = (pred.get("lead") or {}).get("seconds")
    if lead != want or pred.get("frozenLeadSeconds") != want:
        out.append(f"lead {lead}/frozen {pred.get('frozenLeadSeconds')} differs from the locked {want} s")
    if st.get("observedGeneration") != pa["metadata"].get("generation", st.get("observedGeneration")):
        out.append("the status is not for the current generation")
    if pa["spec"].get("prediction", {}).get("families") != list(c.c28.frozen.PA_FAMILIES[c.arm]):
        out.append("spec.prediction.families differs from the locked family set")
    return out


def _confirmation_problems(c: Context) -> List[str]:
    if c.s["phase"] != "confirmation":
        out = []
        if c.plan.get("kind") != "ordinary":
            out.append("a preparation session runs ordinary plans only (scored plans are confirmation)")
        return out
    out, lock = [], c.lock
    if lock["kubeconfig"]["cluster_uid"] != c.record.cluster_uid:
        out.append("the run-lock is for another cluster")
    if c.target_uid not in lock["activation_scope"]["target_uids"]:
        out.append(f"{c.target}'s UID is not in the run-lock's activation scope")
    if c.ns not in lock["activation_scope"]["namespaces"]:
        out.append("the fixture namespace is outside the activation scope")
    key = f"{c.arm}/{c.profile}/{c.seed}"
    if c.s.get("block_start") not in lock["schedule"]["blocks"].get(key, []):
        out.append(f"{c.s.get('block_start')} is not a permitted date for {key}")
    want = lock["workload"]["plans"].get(f"scored/{c.seed}/{c.target}")
    if c.plan.get("digest") != want or c.plan.get("accelerate") != 1 or c.plan.get("kind") != "scored":
        out.append("the plan is not the run-lock's locked confirmation plan for this seed and target")
    if float(c.plan.get("capacity_rps", -1)) != float(lock["capacity"]["c_rps"]):
        out.append("the plan's capacity differs from the run-lock")
    import freeze
    ok, detail = freeze.reviewer_status(c.record, c.lock_digest)
    if not ok:
        out.append(f"reviewer receipts: {detail}")
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
    """Every fault ConfigMap of the run: name -> {uid, data, namespace}."""
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
    env = ["run", "-e", "C28_PLAN=/plan/plan.json", "-e", f"C28_BASE_URL=http://c28-ingress.{c.ns}.svc",
           "-e", "C28_SUMMARY=/tmp/summary.json"]
    for k, v in sorted((s.get("generator_env") or {}).items()):
        env += ["-e", f"{k}={v}"]
    res = s.get("generator_resources") or {"requests": {"cpu": "500m", "memory": "256Mi"},
                                           "limits": {"cpu": "1", "memory": "512Mi"}}
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": names["job"], "namespace": c.ns, "labels": c.labels()},
            "spec": {"backoffLimit": 0, "activeDeadlineSeconds": active_deadline, "suspend": suspend,
                     "template": {"metadata": {"labels": c.labels()}, "spec": {
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
                                         "resources": res,
                                         "securityContext": {"allowPrivilegeEscalation": False,
                                                             "capabilities": {"drop": ["ALL"]}},
                                         "volumeMounts": [{"name": "plan", "mountPath": "/plan", "readOnly": True}]}],
                         "volumes": [{"name": "plan", "configMap": {"name": names["plan_cm"]}}]}}}}


def _load_script() -> str:
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "load", "c28.js")
    with open(path) as fh:
        return fh.read()


def plan_start(c: Context) -> Dict:
    problems: List[str] = []
    entries = X.receipts(c.record, HELPER)
    dep = c.kube.get("Deployment", c.target, c.ns)
    if dep is None or dep["metadata"]["uid"] != c.target_uid:
        problems.append(f"{c.target} is missing or replaced (UID differs from targets.json)")
    problems += _confirmation_problems(c)
    if _open_sessions(entries):
        problems.append(f"sessions still open in the receipts: {_open_sessions(entries)} (one loaded arm at a time)")
    active = _active_generators(c)
    if active:
        problems.append(f"generators of the run still active: {active} (one loaded arm at a time)")
    problems += _writer_problems(c, phase_started=False)
    problems += _readiness(c)
    problems += _node_stops(c)
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
    activate = c.s["phase"] == "confirmation" or c.arm in ("H-CPU", "H-RPS")
    if activate and c.arm in ("H-CPU", "H-RPS"):
        actions.append({"op": "create-writer", "kind": _writer_obj(c)["kind"], "name": c.target,
                        "sha256": X.json_digest(_writer_obj(c))})
    elif activate:
        pa = c.kube.get("PredictiveAutoscaler", c.target, c.ns)
        if pa is None:
            problems.append(f"PredictiveAutoscaler/{c.target} is missing")
        else:
            if pa["spec"].get("mode") != "Recommend":
                problems.append(f"PredictiveAutoscaler/{c.target} is already {pa['spec'].get('mode')}")
            actions.append({"op": "activate", "kind": "PredictiveAutoscaler", "name": c.target,
                            "uid": pa["metadata"]["uid"], "from": "Recommend", "to": "Active"})
    job = generator_job(c, job_deadline(c, b), suspend=c.s["phase"] == "confirmation")
    actions.append({"op": "create-plan-configmap", "name": names["plan_cm"], "script_sha256":
                    X.sha256_bytes(script.encode()), "plan_digest_k6": c.plan["digest"]})
    actions.append({"op": "create-generator", "name": names["job"], "sha256": X.json_digest(job),
                    "suspended_until": c.s.get("block_start") if c.s["phase"] == "confirmation" else None,
                    "active_deadline_seconds": job["spec"]["activeDeadlineSeconds"]})
    return X.new_plan(HELPER, c.record, actions, session=c.session_id, phase=c.s["phase"],
                      runlock_sha256=c.lock_digest,
                      target={"name": c.target, "uid": c.target_uid, "arm": c.arm, "profile": c.profile,
                              "seed": c.seed},
                      budget=b, deadline_utc=c.s["deadline_utc"], faults=planned_faults, problems=problems)


def _writer_obj(c: Context) -> Dict:
    return [o for o in c.c28.render.render_arm(c.params, c.arm, c.profile)
            if o["kind"] in ("HorizontalPodAutoscaler", "ScaledObject")][0]


def _set_faults(expect: str, value: str):
    def mutate(body):
        if (body.get("data") or {}).get("faults.json") != expect:
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
    p = plan_start(c)
    X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
    if p["problems"]:
        raise X.StagingError("the start plan has problems: " + "; ".join(p["problems"]))
    owned: Dict = {"faults": [], "writer": None, "job": None, "plan_cm": None}
    try:
        for f in p["faults"]:
            c.kube.update("ConfigMap", f["name"], f["namespace"], f["uid"], _set_faults("{}", f["data"]))
            owned["faults"].append({k: f[k] for k in ("name", "namespace", "uid")})
        for a in p["actions"]:
            if a["op"] == "activate":
                c.kube.update("PredictiveAutoscaler", c.target, c.ns, a["uid"], _set_mode("Recommend", "Active"))
                owned["writer"] = {"kind": "PredictiveAutoscaler", "name": c.target, "uid": a["uid"], "action": "mode"}
            elif a["op"] == "create-writer":
                w = _writer_obj(c)
                if X.json_digest(w) != a["sha256"]:
                    raise X.StagingError("the writer rendered differently from the reviewed plan")
                live = c.kube.create(w)
                owned["writer"] = {"kind": w["kind"], "name": c.target, "uid": live["metadata"]["uid"],
                                   "action": "created"}
        names = c.names()
        cm = {"apiVersion": "v1", "kind": "ConfigMap",
              "metadata": {"name": names["plan_cm"], "namespace": c.ns, "labels": c.labels()},
              "data": {"plan.json": json.dumps(c.plan, sort_keys=True), "c28.js": _load_script()}}
        owned["plan_cm"] = X.object_ref(c.kube.create(cm))
        job = generator_job(c, job_deadline(c, _budget(c)), suspend=c.s["phase"] == "confirmation")
        if X.json_digest(job) != p["actions"][-1]["sha256"]:
            raise X.StagingError("the generator Job differs from the reviewed plan")
        owned["job"] = X.object_ref(c.kube.create(job))
    except Exception as e:
        X.receipt(c.record, HELPER, "start-aborted", {"session": c.session_id, "plan_sha256": reviewed_digest,
                                                      "error": str(e)[:300], "owned": owned})
        _undo(c, owned, reason="start aborted")
        raise
    return X.receipt(c.record, HELPER, "session-started", {
        "session": c.session_id, "plan_sha256": reviewed_digest, "phase": c.s["phase"], "target": p["target"],
        "runlock_sha256": c.lock_digest, "deadline_utc": c.s["deadline_utc"], "owned": owned,
        "mutations": c.kube.mutations})


def job_deadline(c: Context, b: Dict) -> int:
    """activeDeadlineSeconds of the generator, counted by Kubernetes from the Job's start: to the session deadline
    for a block (stable from its start), else the plan's duration plus the admitted slack."""
    if c.s["phase"] == "confirmation":
        return max(1, _utc(c.s["deadline_utc"]) - _utc(c.s["block_start"]))
    return b["duration_s"] + int(c.s.get("generator_slack_s", 900))


def release(c: Context, started: Dict) -> Optional[Dict]:
    """Unsuspend a block's generator at the block's start (UID-bound; recorded with the actual release time)."""
    entries = X.receipts(c.record, HELPER)
    if c.s["phase"] != "confirmation" or X.last(entries, "generator-released", session=c.session_id):
        return None
    if c.now() < _utc(c.s["block_start"]):
        return None
    j = started["data"]["owned"]["job"]

    def unsuspend(body):
        if body["spec"].get("suspend") is not True:
            raise X.StagingError("the generator is not suspended as reviewed; refusing")
        body["spec"]["suspend"] = False
    c.kube.update("Job", j["name"], c.ns, j["uid"], unsuspend)
    return X.receipt(c.record, HELPER, "generator-released", {
        "session": c.session_id, "block_start": c.s["block_start"], "released_at": _ts(c.now()),
        "offset_s": c.now() - _utc(c.s["block_start"])})


# --- stop -----------------------------------------------------------------------------------------------------------

def _started(entries: List[Dict], session_id: str) -> Optional[Dict]:
    """The session's start receipt while it is open (no stop after it)."""
    open_ = None
    for e in entries:
        if e.get("data", {}).get("session") != session_id:
            continue
        if e["event"] == "session-started":
            open_ = e
        elif e["event"] == "session-stopped":
            open_ = None
    return open_


def plan_stop(c: Context) -> Dict:
    started = _started(X.receipts(c.record, HELPER), c.session_id)
    if started is None:
        raise X.StagingError(f"session {c.session_id} is not open in the receipts")
    owned, steps = started["data"]["owned"], []
    if owned.get("job"):
        j = c.kube.get("Job", owned["job"]["name"], c.ns)
        if j is not None and j["metadata"]["uid"] == owned["job"]["uid"]:
            steps.append({"op": "delete", "kind": "Job", "name": owned["job"]["name"], "uid": owned["job"]["uid"],
                          "status": {k: j.get("status", {}).get(k) for k in ("active", "succeeded", "failed",
                                                                              "startTime", "completionTime")}})
    for f in owned.get("faults", []):
        cm = c.kube.get("ConfigMap", f["name"], f["namespace"])
        if cm is not None and cm["metadata"]["uid"] == f["uid"] and cm.get("data", {}).get("faults.json") != "{}":
            steps.append({"op": "clear-faults", "name": f["name"], "namespace": f["namespace"], "uid": f["uid"],
                          "current": cm["data"]["faults.json"]})
    w = owned.get("writer")
    if w:
        live = c.kube.get(w["kind"], w["name"], c.ns)
        if live is not None and live["metadata"]["uid"] == w["uid"]:
            if w["action"] == "mode" and live["spec"].get("mode") != "Recommend":
                steps.append({"op": "recommend", "kind": w["kind"], "name": w["name"], "uid": w["uid"],
                              "mode_now": live["spec"].get("mode")})
            elif w["action"] == "created":
                steps.append({"op": "delete", "kind": w["kind"], "name": w["name"], "uid": w["uid"]})
    if c.lock:
        fs = c.lock["fault_scope"]
        q = c.kube.get("ResourceQuota", fs["quota_name"], fs["namespace"])
        if q is not None and q["metadata"]["uid"] == fs["quota_uid"] and \
                q["spec"].get("hard", {}).get("pods") != str(fs["pods_normal"]):
            steps.append({"op": "restore-quota", "name": fs["quota_name"], "uid": fs["quota_uid"],
                          "pods_now": q["spec"]["hard"].get("pods"), "pods_normal": fs["pods_normal"]})
    if owned.get("plan_cm"):
        cm = c.kube.get("ConfigMap", owned["plan_cm"]["name"], c.ns)
        if cm is not None and cm["metadata"]["uid"] == owned["plan_cm"]["uid"]:
            steps.append({"op": "keep", "kind": "ConfigMap", "name": owned["plan_cm"]["name"],
                          "why": "the session's plan stays as evidence"})
    order = {"delete-Job": 0, "clear-faults": 1, "recommend": 2, "delete": 2, "restore-quota": 3, "keep": 4}
    steps.sort(key=lambda s: order.get(f"{s['op']}-{s.get('kind')}", order[s["op"]]))
    return X.new_plan(STOP, c.record, steps, session=c.session_id, start_receipt_sha256=started["sha256"],
                      never=["evict", "scale a target", "touch an object the start receipt did not record"])


def _run_steps(c: Context, steps: List[Dict]) -> List[Dict]:
    done = []
    for st in steps:
        if st["op"] == "delete":
            c.kube.delete(st["kind"], st["name"], c.ns, st["uid"])
        elif st["op"] == "clear-faults":
            c.kube.update("ConfigMap", st["name"], st["namespace"], st["uid"],
                          _set_faults(st["current"], "{}") if "current" in st else _clear_any)
        elif st["op"] == "recommend":
            c.kube.update(st["kind"], st["name"], c.ns, st["uid"], _set_mode(None, "Recommend"))
        elif st["op"] == "restore-quota":
            def restore(body, st=st):
                body["spec"]["hard"]["pods"] = str(st["pods_normal"])      # only the run-lock's locked normal limit
            c.kube.update("ResourceQuota", st["name"], c.ns, st["uid"], restore)
        done.append({k: st[k] for k in ("op", "name") if k in st})
    return done


def _clear_any(body):
    body.setdefault("data", {})["faults.json"] = "{}"


def _stop(c: Context, p: Dict, authorised_by: str, reason: str) -> Dict:
    done = _run_steps(c, p["actions"])
    return X.receipt(c.record, HELPER, "session-stopped", {
        "session": c.session_id, "plan_sha256": authorised_by, "reason": reason, "steps": p["actions"],
        "done": done, "mutations": c.kube.mutations})


def stop(c: Context, reviewed: Dict, reviewed_digest: str, reason: str = "operator stop") -> Dict:
    p = plan_stop(c)
    X.require_live(reviewed, reviewed_digest, STOP, c.record, p)
    return _stop(c, p, reviewed_digest, reason)


def _undo(c: Context, owned: Dict, reason: str) -> None:
    """Best-effort reversal of what a failed start created, in stop order; each step is UID-bound."""
    steps = []
    if owned.get("job"):
        steps.append({"op": "delete", "kind": "Job", "name": owned["job"]["name"], "uid": owned["job"]["uid"]})
    for f in owned.get("faults", []):
        steps.append({"op": "clear-faults", **f})
    w = owned.get("writer")
    if w and w["action"] == "mode":
        steps.append({"op": "recommend", "kind": w["kind"], "name": w["name"], "uid": w["uid"]})
    elif w:
        steps.append({"op": "delete", "kind": w["kind"], "name": w["name"], "uid": w["uid"]})
    errors = []
    for st in steps:
        try:
            _run_steps(c, [st])
        except X.StagingError as e:
            errors.append(f"{st['op']} {st['name']}: {e}")
    X.receipt(c.record, HELPER, "start-undone", {"session": c.session_id, "reason": reason, "steps": steps,
                                                 "errors": errors})


# --- verify and the watchdog ----------------------------------------------------------------------------------------

def steal_stop(c: Context, fetch: Callable[[str], Dict]) -> List[str]:
    """Manifest stop: guest steal above the bar for N consecutive one-minute bins, or the observation missing."""
    st = c.s.get("stops") or {}
    if not st.get("steal_query"):
        return ["required observation missing: no steal query configured"]
    bins, bar = int(st.get("steal_bins", 5)), float(st.get("steal_percent", 5))
    end = c.now() - c.now() % 60
    q = urllib.parse.urlencode({"query": st["steal_query"], "start": end - 60 * bins, "end": end, "step": 60})
    try:
        doc = fetch(f"{st['query_api'].rstrip('/')}/api/v1/query_range?{q}")
        series = doc["data"]["result"]
    except Exception as e:      # noqa: BLE001 - any failure is a missing observation
        return [f"required observation missing: steal query failed ({str(e)[:120]})"]
    if not series:
        return ["required observation missing: the steal query returned no series"]
    out = []
    for s in series:
        vals = [float(v) for _, v in s.get("values", [])][-bins:]
        if len(vals) < bins:
            out.append(f"required observation missing: {s.get('metric', {})} has {len(vals)}/{bins} bins")
        elif all(v > bar for v in vals):
            out.append(f"guest steal above {bar}% for {bins} consecutive minutes on {s.get('metric', {})}")
    return out


def _http_json(url: str) -> Dict:
    with urllib.request.urlopen(url, timeout=10) as r:       # noqa: S310 - the record's in-cluster query API
        return json.loads(r.read())


def verify(c: Context, fetch: Callable[[str], Dict] = _http_json) -> Dict:
    started = _started(X.receipts(c.record, HELPER), c.session_id)
    stops: List[str] = []
    if started is None:
        return {"helper": HELPER, "action": "verify", "session": c.session_id, "state": "not-open", "stops": []}
    data = started["data"]
    stops += _writer_problems(c, phase_started=True)
    stops += _node_stops(c)
    stops += steal_stop(c, fetch)
    if c.now() >= _utc(data["deadline_utc"]):
        stops.append("the session deadline has passed")
    owned = data["owned"]
    if owned.get("job"):
        j = c.kube.get("Job", owned["job"]["name"], c.ns)
        if j is None or j["metadata"]["uid"] != owned["job"]["uid"]:
            stops.append("the generator Job is gone or replaced")
        elif any(cd.get("type") == "Failed" and cd.get("status") == "True"
                 for cd in j.get("status", {}).get("conditions", [])):
            stops.append("the generator Job failed")
    if "c28-occupants" in (c.s.get("checks") or []) and c.record.doc.get("reserve"):
        import reserve
        occ = reserve.verify(c.record, c.kube)
        stops += [p for p in occ["problems"] if "unplanned" in p or "stateful" in p]
    return {"helper": HELPER, "action": "verify", "session": c.session_id, "state": "open",
            "start_receipt_sha256": started["sha256"], "stops": stops, "result": "OK" if not stops else "STOP"}


def watch(c: Context, start_digest: str, sleep: Callable[[float], None] = time.sleep,
          fetch: Callable[[str], Dict] = _http_json, interval: int = 30, max_checks: int = 10 ** 6) -> Dict:
    """Check every `interval` seconds; on a stop condition or the deadline, stop the session's own objects (the
    scoped stop is authorised by the digest of the reviewed start plan, which recorded exactly those objects)."""
    started = _started(X.receipts(c.record, HELPER), c.session_id)
    if started is None or started["data"].get("plan_sha256") != start_digest:
        raise X.StagingError("the watchdog needs the digest of the reviewed start plan of the open session")
    for _ in range(max_checks):
        v = verify(c, fetch)
        if v["state"] != "open":
            return {"helper": HELPER, "action": "watch", "session": c.session_id, "ended": "session already stopped"}
        if not v["stops"]:
            release(c, started)
        else:
            out = _stop(c, plan_stop(c), start_digest, reason="watchdog: " + "; ".join(v["stops"])[:500])
            return {"helper": HELPER, "action": "watch", "session": c.session_id, "ended": "stopped",
                    "stops": v["stops"], "receipt": out["sha256"]}
        sleep(interval)
    return {"helper": HELPER, "action": "watch", "session": c.session_id, "ended": "check budget exhausted"}


def main(argv=None, api=None, now=None, sleep=time.sleep, fetch=_http_json) -> int:
    ap = argparse.ArgumentParser(prog="session.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", required=True)
    ap.add_argument("command", nargs="?", default="plan",
                    choices=["plan", "start", "stop-plan", "stop", "verify", "watch"])
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    ap.add_argument("--start-digest", default="")
    args = ap.parse_args(argv)
    try:
        record = X.load_record(args.record, ("session",))
        kube = record.kube(api)
        c = Context(record, kube, now) if now else Context(record, kube)
        if args.command in ("start", "stop") and not args.plan:
            raise X.StagingError("a mutation needs --plan <the reviewed plan file> and --plan-digest")
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
            X.emit(watch(c, args.start_digest, sleep=sleep, fetch=fetch))
    except X.StagingError as e:
        print(f"session: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
