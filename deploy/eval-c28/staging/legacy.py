#!/usr/bin/env python3
"""Quiesce (and selectively restore) the legacy benchmark (STAGING-C28-PROD.md S4; "Helpers to implement": legacy.py).

  legacy.py --record R plan                     read-only: bind the allowlist to live UIDs, write the S4 plan (default)
  legacy.py --record R quiesce --plan P --plan-digest D     execute exactly that reviewed plan
  legacy.py --record R verify [--observe-seconds N]         read-only: post-conditions over a continuous observation;
                                                            "retired" only after >= 600 s of it, all clean
  legacy.py --record R restore-plan             read-only: the selective undo of what quiesce did (from its receipts)
  legacy.py --record R restore --plan P --plan-digest D     execute exactly that reviewed restore plan

The plan names only allowlisted objects (legacy_allowlist.py), each bound to its UID and generation from a fresh
read-only inventory that checks every writer's ACTUAL target and every generated HPA's owner, in S4 order: suspend the
launcher CronJobs and let their active Jobs finish (deadline); stop the k6 generators and wait until their pods are
gone; re-check live, right before it, that every legacy PA is a benchmark arm and nothing reconciles the namespaces;
stop the old operator and wait until its pods are gone; delete the benchmark ScaledObjects with UID preconditions
(their generated HPAs must go with them; the shared KEDA operator keeps running); prove no writer remains; observe the
shutdown signals continuously (the old Lease is not renewed, no stopped workload has a pod, no Job runs, the targets
do not change, no writer or consumer appears); park the arm Deployments; stop the old API. Legacy PAs, PVCs, the
legacy CRD and the Lease are never touched, even by an edited plan.

Execution holds an exclusive lock, re-checks the cluster UID before every mutation, refuses an object whose UID or
generation changed since the plan, scales through /scale with the read resourceVersion and the UID, deletes with a
UID precondition, and records each mutation's INTENT and pre-state (a ScaledObject's sanitized snapshot included)
durably BEFORE the call and its outcome after; an interrupted call leaves the lock and an intent that restore-plan
reconciles against the live object. Restore undoes only what was done, newest first, bound to the post-quiesce state
(generation and value: a later change is drift and refused), checks that no foreign writer owns a target before it
restores a writer, and that each target is ready with a single writer before it restarts the generators. It never
overwrites an existing object and never restores a whole namespace.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from typing import Callable, Dict, List, Mapping, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common_records as C  # noqa: E402
import inventory as I  # noqa: E402
import legacy_allowlist as L  # noqa: E402

HELPER = "legacy"
RESTORE = "legacy-restore"
QUIET_SECONDS = 600
SAMPLE_SECONDS = 10.0
NEVER = ("persistentvolumeclaims", "persistentvolumes", "pa-legacy", "customresourcedefinitions", "namespaces",
         "leases")                                  # evidence, models, archived PAs, CRDs, namespaces: never mutated
ALLOWED = {(k, ns, n) for k, ns, n, role in L.ALLOWLIST if role not in ("retained", "pa", "lease")}
MUTATING = ("suspend", "scale", "delete", "recreate")


def _settings(rec: Mapping) -> Dict:
    s = rec["legacy"]
    out = {"parked_replicas": s.get("parked_replicas", 0), "job_deadline_seconds": s.get("job_deadline_seconds", 3600),
           "wait_timeout_seconds": s.get("wait_timeout_seconds", 600), "observe_seconds": s.get("observe_seconds", 120)}
    if not all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in out.values()):
        raise C.StagingError("legacy settings must be non-negative integers")
    if out["observe_seconds"] < 60:
        raise C.StagingError("legacy.observe_seconds must be at least 60 (a bounded continuous observation)")
    return out


def _archive_complete(rec: Mapping) -> Optional[str]:
    want = rec["legacy"].get("archive_receipt_sha256")
    receipts = C.read_receipts(C.receipts_path(rec, "archive"))
    done = [r["sha256"] for r in receipts if r["event"] == "archive-complete"]
    return want if want and want in done else None


def _ts(text: str) -> float:
    return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()


# --- plan -------------------------------------------------------------------------------------------------------

def build_plan(api, rec: Mapping) -> Dict:
    s = _settings(rec)
    inv = I.collect(api, rec)
    leg = inv["legacy"]
    refusals = list(leg["blockers"])
    if not _archive_complete(rec):
        refusals.append("no archive-complete receipt matching legacy.archive_receipt_sha256 (S0 must finish first)")
    if rec["legacy"].get("external_launchers_attested") is not True:
        refusals.append("legacy.external_launchers_attested must be true (detached generators/watchdogs stopped and "
                        "listed in legacy.external_launchers)")
    missing_retained = [m for m in leg["missing"] if m["role"] == "retained"]
    if missing_retained:
        refusals.append(f"retained evidence/model objects are missing: {missing_retained}; investigate before S4")
    if refusals:
        raise C.StagingError("plan refused: " + "; ".join(refusals))
    actions: List[Dict] = []

    def add(op, kind=None, ns=None, name=None, **kw):
        a = {"step": len(actions) + 1, "op": op}
        if kind:
            o = I.find(inv, kind, ns, name)
            a.update(kind=kind, namespace=ns, name=name, uid=o["uid"], generation=o.get("generation"))
        a.update(kw)
        actions.append(a)

    def deployments(role):
        return [(ns, n, I.find(inv, "deployments", ns, n)) for _, ns, n, _r in L.entries(role)
                if I.find(inv, "deployments", ns, n)]

    lease = I.find(inv, "leases", "ml-engine", L.LEGACY_LEASE)
    jobs = dict(cronjobs=[n for _, _, n, _ in L.entries("launcher")], namespace="ml-engine", labels=["p8-run"])
    for _, ns, n, _r in L.entries("launcher"):
        o = I.find(inv, "cronjobs", ns, n)
        if o:
            add("suspend", "cronjobs", ns, n, before=o["suspend"], after=True)
    add("wait_jobs", timeout_seconds=s["job_deadline_seconds"], **jobs)
    gens = deployments("generator")
    for ns, n, o in gens:
        add("scale", "deployments", ns, n, before=o["replicas"], after=0)
    add("wait_pods_gone", timeout_seconds=s["wait_timeout_seconds"],
        deployments=[[ns, n, o["uid"]] for ns, n, o in gens])
    add("recheck_consumers")
    ops = deployments("operator")
    for ns, n, o in ops:
        add("scale", "deployments", ns, n, before=o["replicas"], after=0)
    add("wait_pods_gone", timeout_seconds=s["wait_timeout_seconds"],
        deployments=[[ns, n, o["uid"]] for ns, n, o in ops])
    for _, ns, n, _r in L.entries("keda"):
        o = I.find(inv, "scaledobjects", ns, n)
        if o is None:
            continue
        hpa = next((h for h in inv["writers"]["horizontalpodautoscalers"] or []
                    if any(r.get("uid") == o["uid"] and r.get("controller") for r in h["ownerReferences"])), None)
        add("delete", "scaledobjects", ns, n, restore_to_original=o["restore_to_original"],
            timeout_seconds=s["wait_timeout_seconds"],
            generated_hpa=None if hpa is None else {"name": hpa["name"], "uid": hpa["uid"]})
    add("verify_no_writers", targets=[["demo", a] for a in L.ARMS], timeout_seconds=s["wait_timeout_seconds"])
    stopped = [[ns, n, o["uid"]] for ns, n, o in gens + ops]
    targets = deployments("target")
    add("observe_quiet", seconds=s["observe_seconds"], stopped=stopped, lease=None if lease is None else
        {"uid": lease["uid"], "renew": lease.get("renew"), "holder": lease.get("holder")},
        targets=[[ns, n, o["uid"]] for ns, n, o in targets], jobs=jobs)
    for ns, n, o in targets:
        add("scale", "deployments", ns, n, before=o["replicas"], after=s["parked_replicas"])
    add("wait_jobs", timeout_seconds=s["job_deadline_seconds"], **jobs)
    api_dep = deployments("api")
    for ns, n, o in api_dep:
        add("scale", "deployments", ns, n, before=o["replicas"], after=0)
    add("wait_pods_gone", timeout_seconds=s["wait_timeout_seconds"],
        deployments=[[ns, n, o["uid"]] for ns, n, o in api_dep])
    retained = [dict(m) for m in leg["found"] if m["role"] in ("retained", "pa", "lease")]
    skipped = [m for m in leg["missing"] if m["role"] not in ("retained",)]
    return C.make_plan(HELPER, rec, actions, inventory_sha256=C.digest(inv), retained=retained, skipped=skipped,
                       unclassified=leg["unclassified"], archive_receipt_sha256=_archive_complete(rec),
                       external_launchers=rec["legacy"].get("external_launchers", []))


def _guard(plan: Mapping) -> None:
    """Defence in depth: a plan (even a reviewed one) may only touch mutable allowlisted objects."""
    for a in plan["actions"]:
        if "kind" not in a:
            continue
        if a["kind"] in NEVER or (a["kind"], a.get("namespace"), a.get("name")) not in ALLOWED:
            raise C.StagingError(f"the plan touches {a['kind']} {a.get('namespace')}/{a.get('name')}, which is not "
                                 f"a mutable legacy object; refusing")


# --- live reads -------------------------------------------------------------------------------------------------

def _bind(api, a: Mapping, generation: Optional[int] = None) -> Dict:
    o = api.get(C.path_of(a["kind"], a["namespace"], a["name"]))
    if o is None:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} disappeared; refusing")
    m = o.get("metadata") or {}
    if m.get("uid") != a["uid"]:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} has another UID than the plan; refusing")
    want = a.get("generation") if generation is None else generation
    if want is not None and m.get("generation") != want:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} changed (generation) since it was bound; "
                             f"refusing")
    return o


def _state(o: Mapping) -> Dict:
    m, sp = o.get("metadata") or {}, o.get("spec") or {}
    return {"generation": m.get("generation"), "replicas": sp.get("replicas"), "suspend": sp.get("suspend")}


def owned_pods(api, ns: str, deploy_uid: str) -> List[str]:
    """Pods (terminating ones included) of a Deployment, through its ReplicaSets' owner references."""
    rs = {r["metadata"]["uid"] for r in C.list_items(api, "replicasets", ns)
          if any(o.get("uid") == deploy_uid for o in r["metadata"].get("ownerReferences") or [])}
    return sorted(p["metadata"]["name"] for p in C.list_items(api, "pods", ns)
                  if any(o.get("uid") in rs for o in p["metadata"].get("ownerReferences") or []))


def _active_jobs(api, j: Mapping) -> List[str]:
    active = []
    for job in C.list_items(api, "jobs", j["namespace"]):
        m, st = job.get("metadata") or {}, job.get("status") or {}
        owned = any(r.get("kind") == "CronJob" and r.get("name") in j["cronjobs"]
                    for r in m.get("ownerReferences") or [])
        labelled = any(k in (m.get("labels") or {}) for k in j["labels"])
        if (owned or labelled) and st.get("active", 0) and not st.get("completionTime"):
            active.append(m.get("name"))
    return sorted(active)


def _writers_of_targets(api, targets) -> List[str]:
    keys = {(ns, n) for ns, n in targets}
    found = []
    for kind in ("horizontalpodautoscalers", "scaledobjects", "pa-new"):
        for w in C.list_items(api, kind):
            m, sp = w.get("metadata") or {}, w.get("spec") or {}
            t = sp.get("scaleTargetRef") or sp.get("targetDeployment") or {}
            if (t.get("namespace") or m.get("namespace"), t.get("name")) in keys:
                found.append(f"{kind} {m.get('namespace')}/{m.get('name')}")
    return found


def _lease(api) -> Optional[Dict]:
    o = api.get(C.path_of("leases", "ml-engine", L.LEGACY_LEASE))
    if o is None:
        return None
    sp = o.get("spec") or {}
    return {"uid": o["metadata"].get("uid"), "renew": sp.get("renewTime"), "holder": sp.get("holderIdentity")}


def _wait(cond: Callable[[], Optional[str]], timeout: int, sleep: Callable, monotonic: Callable, what: str) -> None:
    """Poll cond() (None when satisfied, else the reason) until timeout; fail closed."""
    end = monotonic() + timeout
    while True:
        why = cond()
        if why is None:
            return
        if monotonic() >= end:
            raise C.StagingError(f"{what} did not happen within {timeout} s: {why}")
        sleep(min(SAMPLE_SECONDS, max(0.1, end - monotonic())))


def _observe(sample: Callable[[], List[str]], seconds: int, sleep: Callable, monotonic: Callable, what: str) -> int:
    """Sample continuously for `seconds`; any problem in any sample fails (no averaging, no retry). Returns samples."""
    end, n = monotonic() + seconds, 0
    while True:
        problems = sample()
        n += 1
        if problems:
            raise C.StagingError(f"{what}: {'; '.join(problems)}")
        if monotonic() >= end:
            return n
        sleep(min(SAMPLE_SECONDS, max(0.1, end - monotonic())))


def quiet_problems(api, rec: Mapping, obs: Mapping, baseline: Mapping) -> List[str]:
    """The shutdown signals at one instant against their baseline."""
    out = []
    lease = _lease(api)
    if obs.get("lease") and lease != baseline.get("lease"):
        out.append("the old operator's Lease was renewed or changed")
    for ns, n, uid in obs["stopped"]:
        pods = owned_pods(api, ns, uid)
        if pods:
            out.append(f"{n} still has pods {pods}")
    active = _active_jobs(api, obs["jobs"])
    if active:
        out.append(f"active Jobs {active}")
    for ns, n, uid in obs["targets"]:
        o = api.get(C.path_of("deployments", ns, n))
        if o is None or o["metadata"].get("uid") != uid or _state(o) != baseline["targets"].get(n):
            out.append(f"target {n} changed (a writer or arrivals are still active)")
    writers = _writers_of_targets(api, [(ns, n) for ns, n, _ in obs["targets"]])
    out += [f"writer {w}" for w in writers]
    out += I.live_blockers(api, rec)
    return out


# --- quiesce -------------------------------------------------------------------------------------------------------

def _mutate(api, rec: Mapping, lock, a: Mapping, clock, call: Callable[[Dict], Dict], extra=None) -> Dict:
    """Intent (with pre-state) durably first, then the call, then the outcome (with post-state)."""
    pre = _bind(api, a)
    intent = {"action": a, "pre": _state(pre), **(extra or {})}
    C.append_receipt(rec, HELPER, f"intent-{a['step']}", intent, clock)
    lock.safe = False
    result = call(pre)
    post = api.get(C.path_of(a["kind"], a["namespace"], a["name"]))
    data = {"action": a, "pre": intent["pre"], "result": result, "post": None if post is None else _state(post),
            **(extra or {})}
    r = C.append_receipt(rec, HELPER, f"step-{a['step']}", data, clock)
    lock.safe = True
    return r


def _scale_call(api, a: Mapping, replicas: int):
    def call(pre):
        path = C.path_of("deployments", a["namespace"], a["name"], "scale")
        sc = api.get(path)
        if sc is None or (sc.get("metadata") or {}).get("uid") != a["uid"]:
            raise C.StagingError(f"the scale subresource of {a['namespace']}/{a['name']} is not the bound object")
        api.replace(path, {"apiVersion": "autoscaling/v1", "kind": "Scale",
                           "metadata": {"name": a["name"], "namespace": a["namespace"], "uid": a["uid"],
                                        "resourceVersion": sc["metadata"]["resourceVersion"]},
                           "spec": {"replicas": replicas}})
        return {"from": (sc.get("spec") or {}).get("replicas"), "to": replicas}
    return call


def _suspend_call(api, a: Mapping, value: bool):
    def call(pre):
        before = bool((pre.get("spec") or {}).get("suspend"))
        pre["spec"]["suspend"] = value           # the whole object as read: its resourceVersion guards the update
        api.replace(C.path_of("cronjobs", a["namespace"], a["name"]), pre)
        return {"from": before, "to": value}
    return call


def quiesce(api, rec: Mapping, plan: Mapping, reviewed: str, sleep: Callable = time.sleep,
            monotonic: Callable = time.monotonic, clock=None) -> List[Dict]:
    C.require_reviewed(plan, reviewed, HELPER, rec)
    _guard(plan)
    if plan.get("archive_receipt_sha256") != _archive_complete(rec):
        raise C.StagingError("the archive-complete receipt no longer matches the plan; refusing")
    done = []
    with C.exclusive_lock(rec, HELPER, clock) as lock:
        C.append_receipt(rec, HELPER, "quiesce-start", {"plan_sha256": reviewed}, clock)
        for a in plan["actions"]:
            C.require_cluster(api, rec["cluster_uid"])
            op = a["op"]
            if op == "suspend":
                done.append(_mutate(api, rec, lock, a, clock, _suspend_call(api, a, True)))
            elif op == "scale":
                done.append(_mutate(api, rec, lock, a, clock, _scale_call(api, a, a["after"])))
            elif op == "delete":
                o = _bind(api, a)
                spath = os.path.join(rec["evidence_dir"], "legacy", "deleted",
                                     f"{a['kind']}-{a['namespace']}-{a['name']}-{a['uid']}.json")
                snap = {"path": spath, "sha256": C.write_json_once(spath, C.sanitize_object(o))}

                def call(pre, a=a):
                    api.delete(C.path_of(a["kind"], a["namespace"], a["name"]), a["uid"])
                    return {"deleted": a["uid"]}
                done.append(_mutate(api, rec, lock, a, clock, call, {"snapshot": snap}))
                hpa = a.get("generated_hpa")
                if hpa:
                    hp = C.path_of("horizontalpodautoscalers", a["namespace"], hpa["name"])
                    _wait(lambda hp=hp: None if api.get(hp) is None else "still present", a["timeout_seconds"],
                          sleep, monotonic, f"removal of the generated HPA {hpa['name']}")
            elif op == "wait_jobs":
                _wait(lambda a=a: (lambda j: f"active Jobs {j}" if j else None)(_active_jobs(api, a)),
                      a["timeout_seconds"], sleep, monotonic, "active Jobs finishing")
            elif op == "wait_pods_gone":
                def gone(a=a):
                    left = {n: owned_pods(api, ns, uid) for ns, n, uid in a["deployments"]}
                    left = {k: v for k, v in left.items() if v}
                    return f"pods remain {left}" if left else None
                _wait(gone, a["timeout_seconds"], sleep, monotonic, "pod termination")
            elif op == "recheck_consumers":
                blockers = I.live_blockers(api, rec)
                if blockers:
                    raise C.StagingError("consumers/reconcilers changed since the plan: " + "; ".join(blockers))
            elif op == "verify_no_writers":
                _wait(lambda a=a: (lambda w: None if not w else f"writers {w}")(_writers_of_targets(api, a["targets"])),
                      a["timeout_seconds"], sleep, monotonic, "no writer on the legacy targets")
            elif op == "observe_quiet":
                baseline = {"lease": _lease(api),
                            "targets": {n: _state(api.get(C.path_of("deployments", ns, n)) or {})
                                        for ns, n, _ in a["targets"]}}
                if a.get("lease") and baseline["lease"] and baseline["lease"]["uid"] != a["lease"]["uid"]:
                    raise C.StagingError("the old operator's Lease was recreated; refusing")
                n = _observe(lambda a=a, b=baseline: quiet_problems(api, rec, a, b), a["seconds"], sleep, monotonic,
                             "the shutdown signals are not quiet")
                C.append_receipt(rec, HELPER, f"observed-{a['step']}", {"seconds": a["seconds"], "samples": n,
                                                                        "baseline": baseline}, clock)
            else:
                raise C.StagingError(f"unknown plan action {op!r}")
        C.append_receipt(rec, HELPER, "quiesce-complete", {"plan_sha256": reviewed, "steps": len(done)}, clock)
    return done


# --- verify --------------------------------------------------------------------------------------------------------

def _snapshot_problems(api, rec: Mapping, s: Mapping) -> (List[str], Dict):
    problems, state = [], {}
    for _, ns, n, role in L.ALLOWLIST:
        if role == "launcher":
            o = api.get(C.path_of("cronjobs", ns, n))
            if o and not (o.get("spec") or {}).get("suspend"):
                problems.append(f"CronJob {n} is not suspended")
        elif role in ("generator", "operator", "api", "target"):
            o = api.get(C.path_of("deployments", ns, n))
            if o:
                want = s["parked_replicas"] if role == "target" else 0
                if (o.get("spec") or {}).get("replicas") != want:
                    problems.append(f"Deployment {ns}/{n} is not at {want} replicas")
                pods = owned_pods(api, ns, o["metadata"]["uid"])
                if want == 0 and pods:
                    problems.append(f"{n} still has pods {pods} (terminating ones included)")
                state[f"{ns}/{n}"] = dict(_state(o), uid=o["metadata"]["uid"])
        elif role == "keda":
            if api.get(C.path_of("scaledobjects", ns, n)) is not None:
                problems.append(f"ScaledObject {n} still exists")
        elif role == "retained":
            kind = "customresourcedefinitions" if ns is None else "persistentvolumeclaims"
            o = api.get(C.path_of(kind, ns, n))
            if o is None:
                problems.append(f"retained {kind} {n} is missing")
            elif kind == "persistentvolumeclaims" and (o.get("status") or {}).get("phase") != "Bound":
                problems.append(f"retained claim {n} is not Bound")
    for h in C.list_items(api, "horizontalpodautoscalers", "demo"):
        if any(r.get("kind") == "ScaledObject" and r.get("name") in L.KEDA_OBJECTS
               for r in h["metadata"].get("ownerReferences") or []):
            problems.append(f"generated HPA {h['metadata']['name']} still exists")
    jobs = _active_jobs(api, {"namespace": "ml-engine", "cronjobs": [n for _, _, n, _ in L.entries("launcher")],
                              "labels": ["p8-run"]})
    problems += [f"active Job {j}" for j in jobs]
    problems += [f"writer remains: {w}" for w in _writers_of_targets(api, [("demo", a) for a in L.ARMS])]
    problems += I.live_blockers(api, rec)
    state["lease"] = _lease(api)
    return problems, state


def verify(api, rec: Mapping, observe_seconds: int = 0, sleep: Callable = time.sleep,
           monotonic: Callable = time.monotonic, clock=None) -> Dict:
    """The post-conditions sampled continuously for observe_seconds: every sample clean and identical (the Lease not
    renewed, no pod of a stopped workload, no Job, no writer or consumer, no change). Retired only after a completed
    quiesce and at least QUIET_SECONDS of such observation."""
    C.require_cluster(api, rec["cluster_uid"])
    s = _settings(rec)
    first_problems, baseline = _snapshot_problems(api, rec, s)
    problems, samples = list(first_problems), 1
    if not problems and observe_seconds > 0:
        def sample():
            p, st = _snapshot_problems(api, rec, s)
            return p + (["the state changed during the observation (Lease renewal, a scale or a recreated object)"]
                        if st != baseline else [])
        try:
            samples = _observe(sample, observe_seconds, sleep, monotonic, "not quiet")
        except C.StagingError as e:
            problems.append(str(e))
    receipts = C.read_receipts(C.receipts_path(rec, HELPER))
    completed = any(r["event"] == "quiesce-complete" for r in receipts)
    result = {"problems": problems, "state": baseline, "observed_seconds": observe_seconds if not problems else 0,
              "samples": samples, "quiesced": completed and not problems,
              "retired": completed and not problems and observe_seconds >= QUIET_SECONDS}
    C.append_receipt(rec, HELPER, "verify", result, clock)
    return result


# --- restore -------------------------------------------------------------------------------------------------------

def _reconciled(api, d: Mapping) -> Optional[Dict]:
    """An intent without an outcome (interrupted call): the outcome read from the live object, or None if it did not
    happen."""
    a = d["action"]
    o = api.get(C.path_of(a["kind"], a["namespace"], a["name"]))
    if a["op"] == "delete":
        if o is None or o["metadata"].get("uid") != a["uid"]:
            return {"action": a, "pre": d["pre"], "result": {"deleted": a["uid"], "reconciled": True}, "post": None,
                    "snapshot": d["snapshot"]}
        return None
    if o is None or o["metadata"].get("uid") != a["uid"]:
        raise C.StagingError(f"{a['kind']} {a['namespace']}/{a['name']} vanished after an interrupted step; "
                             f"reconcile by hand")
    st = _state(o)
    happened = (st["replicas"] == a["after"]) if a["op"] == "scale" else (st["suspend"] is True)
    if happened and st["generation"] != d["pre"]["generation"]:
        key = "replicas" if a["op"] == "scale" else "suspend"
        return {"action": a, "pre": d["pre"], "result": {"from": d["pre"][key], "to": st[key], "reconciled": True},
                "post": st}
    return None


def build_restore_plan(api, rec: Mapping) -> Dict:
    """The exact undo of the recorded quiesce steps, newest first, each bound to its post-quiesce state. Read-only."""
    C.require_cluster(api, rec["cluster_uid"])
    receipts = C.read_receipts(C.receipts_path(rec, HELPER))
    outcomes = {r["event"][5:]: r["data"] for r in receipts if r["event"].startswith("step-")}
    steps = []
    for r in receipts:
        if not r["event"].startswith("intent-"):
            continue
        n = r["event"][7:]
        d = outcomes.get(n) or _reconciled(api, r["data"])
        if d is not None:
            steps.append(d)
    actions = []
    for d in reversed(steps):
        a, res, post = d["action"], d["result"], d.get("post")
        base = {k: a[k] for k in ("kind", "namespace", "name", "uid")}
        if a["op"] == "scale":
            actions.append(dict(base, op="scale", after=res["from"], expect={"replicas": res["to"],
                                                                             "generation": post["generation"]}))
        elif a["op"] == "suspend":
            actions.append(dict(base, op="suspend", after=res["from"], expect={"suspend": True,
                                                                               "generation": post["generation"]}))
        elif a["op"] == "delete":
            actions.append(dict(base, op="recreate", snapshot=d["snapshot"]))
    if not actions:
        raise C.StagingError("no recorded quiesce step to restore")
    roles = {(k, ns, n): role for k, ns, n, role in L.ALLOWLIST}
    ordered, checked_writers, checked_ready = [], False, False
    for a in actions:
        role = roles.get((a["kind"], a["namespace"], a["name"]))
        if role in ("keda", "operator") and not checked_writers:
            ordered.append({"op": "check_no_foreign_writer", "targets": [["demo", t] for t in L.ARMS]})
            checked_writers = True
        if role == "generator" and not checked_ready:
            ordered.append({"op": "check_ready_single_writer", "targets": [["demo", t] for t in L.ARMS]})
            checked_ready = True
        ordered.append(a)
        if role == "api" and a["op"] == "scale" and a["after"]:
            ordered.append({"op": "wait_ready", "deployments": [[a["namespace"], a["name"]]]})
    for i, a in enumerate(ordered, 1):
        a["step"] = i
    return C.make_plan(RESTORE, rec, ordered, quiesce_receipts=[r["sha256"] for r in receipts])


def _recreate_body(snapshot: Mapping) -> Dict:
    m = snapshot.get("metadata") or {}
    meta = {k: m[k] for k in ("name", "namespace", "labels", "annotations") if k in m}
    body = {"apiVersion": snapshot.get("apiVersion"), "kind": snapshot.get("kind"), "metadata": meta,
            "spec": snapshot.get("spec")}
    if C.REDACTED in json.dumps(body):
        raise C.StagingError("the snapshot has redacted fields; recreate it from the protected backup instead")
    return body


def writer_census(api) -> Dict[str, List[str]]:
    """Active writers per legacy target: legacy PAs while the old operator runs, ScaledObjects that are not paused,
    any new-group PA or unowned HPA (generated HPAs count as their ScaledObject)."""
    op = api.get(C.path_of("deployments", "ml-engine", "predictive-operator"))
    operator_on = bool(op and (op.get("spec") or {}).get("replicas"))
    out = {t: [] for t in L.ARMS}
    for p in C.list_items(api, "pa-legacy"):
        t = (p.get("spec") or {}).get("targetDeployment") or {}
        if operator_on and t.get("name") in out and (t.get("namespace") or p["metadata"]["namespace"]) == "demo":
            out[t["name"]].append(f"legacy PA {p['metadata']['name']}")
    for kind in ("scaledobjects", "pa-new", "horizontalpodautoscalers"):
        for w in C.list_items(api, kind, "demo"):
            m, sp = w["metadata"], w.get("spec") or {}
            t = (sp.get("scaleTargetRef") or sp.get("targetDeployment") or {}).get("name")
            if t not in out:
                continue
            if kind == "scaledobjects" and (m.get("annotations") or {}).get("autoscaling.keda.sh/paused") == "true":
                continue
            if kind == "horizontalpodautoscalers" and any(r.get("kind") == "ScaledObject"
                                                          for r in m.get("ownerReferences") or []):
                continue
            out[t].append(f"{kind} {m['name']}")
    return out


def restore(api, rec: Mapping, plan: Mapping, reviewed: str, sleep: Callable = time.sleep,
            monotonic: Callable = time.monotonic, clock=None) -> List[Dict]:
    C.require_reviewed(plan, reviewed, RESTORE, rec)
    _guard(plan)
    s, done = _settings(rec), []
    with C.exclusive_lock(rec, RESTORE, clock) as lock:
        for a in plan["actions"]:
            C.require_cluster(api, rec["cluster_uid"])
            if a["op"] in ("scale", "suspend"):
                o = _bind(api, a, generation=a["expect"]["generation"])        # drift since quiesce: refused
                key = "replicas" if a["op"] == "scale" else "suspend"
                if _state(o)[key] != a["expect"][key]:
                    raise C.StagingError(f"{a['namespace']}/{a['name']} drifted from its post-quiesce state; refusing")
                call = _scale_call(api, a, a["after"]) if a["op"] == "scale" else _suspend_call(api, a, a["after"])
                C.append_receipt(rec, RESTORE, f"intent-{a['step']}", {"action": a, "pre": _state(o)}, clock)
                lock.safe = False
                res = call(o)
            elif a["op"] == "recreate":
                snap = a["snapshot"]
                if C.sha256_file(snap["path"]) != snap["sha256"]:
                    raise C.StagingError(f"the snapshot {snap['path']} changed; refusing")
                path = C.path_of(a["kind"], a["namespace"], a["name"])
                if api.get(path) is not None:
                    raise C.StagingError(f"{a['kind']} {a['namespace']}/{a['name']} exists; never overwritten")
                with open(snap["path"]) as fh:
                    body = _recreate_body(json.load(fh))
                C.append_receipt(rec, RESTORE, f"intent-{a['step']}", {"action": a}, clock)
                lock.safe = False
                created = api.create(C.path_of(a["kind"], a["namespace"]), body)
                res = {"recreated_uid": (created.get("metadata") or {}).get("uid")}
            elif a["op"] == "check_no_foreign_writer":
                legacy_names = {f"legacy PA {n}" for _, _, n, r in L.entries("pa")} | \
                    {f"scaledobjects {n}" for n in L.KEDA_OBJECTS}
                foreign = {t: [x for x in w if x not in legacy_names] for t, w in writer_census(api).items()}
                foreign = {t: w for t, w in foreign.items() if w}
                if foreign:
                    raise C.StagingError(f"legacy targets already have writers {foreign}; refusing to add another")
                res = {"foreign": {}}
            elif a["op"] == "check_ready_single_writer":
                def ready_single(a=a):
                    census, bad = writer_census(api), []
                    for ns, t in a["targets"]:
                        d = api.get(C.path_of("deployments", ns, t))
                        if d and (d.get("status") or {}).get("readyReplicas", 0) < (d.get("spec") or {}).get(
                                "replicas", 0):
                            bad.append(f"{t} not ready")
                        if len(census.get(t, [])) > 1:
                            bad.append(f"{t} has writers {census[t]}")
                    return "; ".join(bad) or None
                _wait(ready_single, s["wait_timeout_seconds"], sleep, monotonic, "ready targets with one writer each")
                res = {"census": writer_census(api)}
            elif a["op"] == "wait_ready":
                def ready(a=a):
                    for ns, n in a["deployments"]:
                        d = api.get(C.path_of("deployments", ns, n)) or {}
                        if (d.get("status") or {}).get("readyReplicas", 0) < (d.get("spec") or {}).get("replicas", 0):
                            return f"{n} not ready"
                    return None
                _wait(ready, s["wait_timeout_seconds"], sleep, monotonic, "readiness")
                res = {}
            else:
                raise C.StagingError(f"unknown restore action {a['op']!r}")
            done.append(C.append_receipt(rec, RESTORE, f"step-{a['step']}", {"action": a, "result": res}, clock))
            lock.safe = True
        C.append_receipt(rec, RESTORE, "restore-complete", {"plan_sha256": reviewed, "steps": len(done)}, clock)
    return done


# --- CLI ------------------------------------------------------------------------------------------------------------

def main(argv=None, api=None, sleep=time.sleep, monotonic=time.monotonic, clock=None) -> int:
    ap = argparse.ArgumentParser(prog="legacy.py", description=__doc__.splitlines()[0])
    C.add_common_args(ap)
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("plan")
    for name in ("quiesce", "restore"):
        p = sub.add_parser(name)
        p.add_argument("--plan", required=True)
        p.add_argument("--plan-digest", required=True)
    v = sub.add_parser("verify")
    v.add_argument("--observe-seconds", type=int, default=QUIET_SECONDS)
    sub.add_parser("restore-plan")
    a = ap.parse_args(argv)
    cmd = a.cmd or "plan"
    try:
        rec = C.load_record(a.record, ("legacy",))
        api = api or C.api_for(rec)
        if cmd in ("plan", "restore-plan"):
            plan = build_plan(api, rec) if cmd == "plan" else build_restore_plan(api, rec)
            name = "plan" if cmd == "plan" else "restore-plan"
            path = os.path.join(rec["evidence_dir"], "legacy", f"{name}-{C.digest(plan)[:12]}.json")
            C.write_json_once(path, plan)
            print(f"{name}: {path}\nplan digest (review, then pass --plan-digest): {C.plan_digest(plan)}")
        elif cmd == "quiesce":
            quiesce(api, rec, C.load_plan(a.plan), a.plan_digest, sleep, monotonic, clock)
            print("quiesce complete; run verify (600 s of continuous observation) to declare retirement")
        elif cmd == "restore":
            restore(api, rec, C.load_plan(a.plan), a.plan_digest, sleep, monotonic, clock)
            print("restore complete")
        else:
            print(json.dumps(verify(api, rec, a.observe_seconds, sleep, monotonic, clock), indent=1, sort_keys=True))
    except C.StagingError as e:
        print(f"legacy: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
