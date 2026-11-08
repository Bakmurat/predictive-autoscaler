#!/usr/bin/env python3
"""Quiesce (and selectively restore) the legacy benchmark (STAGING-C28-PROD.md S4; "Helpers to implement": legacy.py).

  legacy.py --record R plan                     read-only: bind the allowlist to live UIDs, write the S4 plan (default)
  legacy.py --record R quiesce --plan P --plan-digest D     execute exactly that reviewed plan
  legacy.py --record R verify [--plan P]        read-only: the post-conditions, and "retired" after 10 quiet minutes
  legacy.py --record R restore-plan             read-only: the selective undo of what quiesce did (from its receipts)
  legacy.py --record R restore --plan P --plan-digest D     execute exactly that reviewed restore plan

The plan names only allowlisted objects (legacy_allowlist.py), each bound to its UID and generation from a fresh
read-only inventory, in S4 order: suspend the launcher CronJobs, let their active Jobs finish (deadline), stop the k6
generators, stop the old operator (only when every legacy PA is a benchmark arm), delete the two benchmark
ScaledObjects with UID preconditions (the shared KEDA operator keeps running; their generated HPAs must go with them),
prove no writer remains, park the arm Deployments, then stop the old API. Legacy PAs, PVCs and the legacy CRD are never
touched. The plan refuses while the archive-complete receipt (archive.py) is missing, an external launcher is not
attested stopped, or the inventory reports a blocker. Execution re-checks the cluster UID before every mutation,
refuses an object whose UID or generation changed since the plan, scales through /scale with the read
resourceVersion and the UID, deletes with a UID precondition, and appends a receipt per step (a ScaledObject's
sanitized snapshot is written before its deletion, so restore can recreate exactly it). Restore touches only what
quiesce changed, in reverse order, never a whole namespace, and refuses when an object was recreated under a new UID
or a same-named object exists.
"""

from __future__ import annotations

import argparse
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
NEVER = ("persistentvolumeclaims", "persistentvolumes", "pa-legacy", "customresourcedefinitions", "namespaces",
         "leases")                                  # evidence, models, archived PAs, CRDs, namespaces: never mutated
ALLOWED = {(k, ns, n) for k, ns, n, role in L.ALLOWLIST if role not in ("retained", "pa", "lease")}


def _guard(plan: Mapping) -> None:
    """Defence in depth: a plan (even a reviewed one) may only touch mutable allowlisted objects."""
    for a in plan["actions"]:
        if "kind" not in a:
            continue
        if a["kind"] in NEVER or (a["kind"], a.get("namespace"), a.get("name")) not in ALLOWED:
            raise C.StagingError(f"the plan touches {a['kind']} {a.get('namespace')}/{a.get('name')}, which is not "
                                 f"a mutable legacy object; refusing")


def _settings(rec: Mapping) -> Dict:
    s = rec["legacy"]
    out = {"parked_replicas": s.get("parked_replicas", 0), "job_deadline_seconds": s.get("job_deadline_seconds", 3600),
           "wait_timeout_seconds": s.get("wait_timeout_seconds", 600)}
    if not all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in out.values()):
        raise C.StagingError("legacy settings must be non-negative integers")
    return out


def _archive_complete(rec: Mapping) -> Optional[str]:
    want = rec["legacy"].get("archive_receipt_sha256")
    receipts = C.read_receipts(C.receipts_path(rec, "archive"))
    done = [r["sha256"] for r in receipts if r["event"] == "archive-complete"]
    return want if want and want in done else None


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
        o = I.find(inv, kind, ns, name) if kind else None
        if kind and o is None:
            return                                   # already absent: nothing to do (reported in the plan's skipped)
        a = {"step": len(actions) + 1, "op": op}
        if o is not None:
            a.update(kind=kind, namespace=ns, name=name, uid=o["uid"], generation=o.get("generation"))
        a.update(kw)
        actions.append(a)

    for _, ns, n, _r in L.entries("launcher"):
        o = I.find(inv, "cronjobs", ns, n)
        if o:
            add("suspend", "cronjobs", ns, n, before=o["suspend"], after=True)
    add("wait_jobs", cronjobs=[n for _, _, n, _ in L.entries("launcher")], namespace="ml-engine",
        labels=["p8-run"], timeout_seconds=s["job_deadline_seconds"])
    for _, ns, n, _r in L.entries("generator"):
        o = I.find(inv, "deployments", ns, n)
        if o:
            add("scale", "deployments", ns, n, before=o["replicas"], after=0)
    _, ns, n, _r = L.entries("operator")[0]
    o = I.find(inv, "deployments", ns, n)
    if o:
        add("scale", "deployments", ns, n, before=o["replicas"], after=0)
    add("wait_scaled_down", timeout_seconds=s["wait_timeout_seconds"],
        deployments=[[a["namespace"], a["name"]] for a in actions if a["op"] == "scale"])
    for _, ns, n, _r in L.entries("keda"):
        o = I.find(inv, "scaledobjects", ns, n)
        if o is None:
            continue
        hpa = I.find(inv, "horizontalpodautoscalers", ns, o.get("hpa_name") or L.generated_hpa(n))
        add("delete", "scaledobjects", ns, n, restore_to_original=o["restore_to_original"],
            generated_hpa=None if hpa is None else {"name": hpa["name"], "uid": hpa["uid"]})
    add("verify_no_writers", targets=[["demo", a] for a in L.ARMS], timeout_seconds=s["wait_timeout_seconds"])
    for _, ns, n, _r in L.entries("target"):
        o = I.find(inv, "deployments", ns, n)
        if o:
            add("scale", "deployments", ns, n, before=o["replicas"], after=s["parked_replicas"])
    _, ns, n, _r = L.entries("api")[0]
    o = I.find(inv, "deployments", ns, n)
    if o:
        add("scale", "deployments", ns, n, before=o["replicas"], after=0)
    retained = [dict(m) for m in leg["found"] if m["role"] in ("retained", "pa", "lease")]
    skipped = [m for m in leg["missing"] if m["role"] not in ("retained",)]
    return C.make_plan(HELPER, rec, actions, inventory_sha256=C.digest(inv), retained=retained, skipped=skipped,
                       unclassified=leg["unclassified"], archive_receipt_sha256=_archive_complete(rec),
                       external_launchers=rec["legacy"].get("external_launchers", []))


# --- execution primitives ---------------------------------------------------------------------------------------

def _bind(api, a: Mapping) -> Dict:
    o = api.get(C.path_of(a["kind"], a["namespace"], a["name"]))
    if o is None:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} disappeared since the plan; refusing")
    m = o.get("metadata") or {}
    if m.get("uid") != a["uid"]:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} has another UID than the plan; refusing")
    if a.get("generation") is not None and m.get("generation") != a["generation"]:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} changed (generation) since the plan; refusing")
    return o


def _scale(api, a: Mapping, replicas: int) -> Dict:
    _bind(api, a)
    path = C.path_of("deployments", a["namespace"], a["name"], "scale")
    sc = api.get(path)
    if sc is None or (sc.get("metadata") or {}).get("uid") != a["uid"]:
        raise C.StagingError(f"the scale subresource of {a['namespace']}/{a['name']} is not the planned object")
    body = {"apiVersion": "autoscaling/v1", "kind": "Scale",
            "metadata": {"name": a["name"], "namespace": a["namespace"], "uid": a["uid"],
                         "resourceVersion": sc["metadata"]["resourceVersion"]},
            "spec": {"replicas": replicas}}
    api.replace(path, body)
    return {"from": (sc.get("spec") or {}).get("replicas"), "to": replicas}


def _set_suspend(api, a: Mapping, value: bool) -> Dict:
    o = _bind(api, a)
    before = bool((o.get("spec") or {}).get("suspend"))
    o["spec"]["suspend"] = value                 # the whole object as read: its resourceVersion guards the update
    api.replace(C.path_of("cronjobs", a["namespace"], a["name"]), o)
    return {"from": before, "to": value}


def _wait(cond: Callable[[], Optional[str]], timeout: int, sleep: Callable, monotonic: Callable, what: str) -> None:
    """Poll cond() (None when satisfied, else the reason) until timeout; fail closed."""
    end = monotonic() + timeout
    while True:
        why = cond()
        if why is None:
            return
        if monotonic() >= end:
            raise C.StagingError(f"{what} did not happen within {timeout} s: {why}")
        sleep(min(10.0, max(0.1, end - monotonic())))


def _active_jobs(api, a: Mapping) -> Optional[str]:
    active = []
    for j in C.list_items(api, "jobs", a["namespace"]):
        m, st = j.get("metadata") or {}, j.get("status") or {}
        owned = any(r.get("kind") == "CronJob" and r.get("name") in a["cronjobs"]
                    for r in m.get("ownerReferences") or [])
        labelled = any(k in (m.get("labels") or {}) for k in a["labels"])
        if (owned or labelled) and st.get("active", 0) and not st.get("completionTime"):
            active.append(m.get("name"))
    return f"active Jobs {sorted(active)}" if active else None


def _scaled_down(api, a: Mapping) -> Optional[str]:
    waiting = []
    for ns, name in a["deployments"]:
        d = api.get(C.path_of("deployments", ns, name))
        if d and ((d.get("status") or {}).get("replicas") or 0) > (d.get("spec") or {}).get("replicas", 0):
            waiting.append(name)
    return f"pods still terminating for {waiting}" if waiting else None


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


# --- quiesce -------------------------------------------------------------------------------------------------------

def quiesce(api, rec: Mapping, plan: Mapping, reviewed: str, sleep: Callable = time.sleep,
            monotonic: Callable = time.monotonic, clock=None) -> List[Dict]:
    C.require_reviewed(plan, reviewed, HELPER, rec)
    _guard(plan)
    if plan.get("archive_receipt_sha256") != _archive_complete(rec):
        raise C.StagingError("the archive-complete receipt no longer matches the plan; refusing")
    done = []
    C.append_receipt(rec, HELPER, "quiesce-start", {"plan_sha256": reviewed}, clock)
    for a in plan["actions"]:
        C.require_cluster(api, rec["cluster_uid"])
        op, result = a["op"], {}
        if op == "suspend":
            result = _set_suspend(api, a, True)
        elif op == "scale":
            result = _scale(api, a, a["after"])
        elif op == "wait_jobs":
            _wait(lambda: _active_jobs(api, a), a["timeout_seconds"], sleep, monotonic, "active Jobs finishing")
        elif op == "wait_scaled_down":
            _wait(lambda: _scaled_down(api, a), a["timeout_seconds"], sleep, monotonic, "the scale-down")
        elif op == "delete":
            o = _bind(api, a)
            snap = C.sanitize_object(o)
            spath = os.path.join(rec["evidence_dir"], "legacy", "deleted",
                                 f"{a['kind']}-{a['namespace']}-{a['name']}-{a['uid']}.json")
            result["snapshot"] = {"path": spath, "sha256": C.write_json_once(spath, snap)}
            api.delete(C.path_of(a["kind"], a["namespace"], a["name"]), a["uid"])
            hpa = a.get("generated_hpa")
            if hpa:
                hp = C.path_of("horizontalpodautoscalers", a["namespace"], hpa["name"])
                _wait(lambda: None if api.get(hp) is None else "still present", a.get("timeout_seconds", 120),
                      sleep, monotonic, f"removal of the generated HPA {hpa['name']}")
        elif op == "verify_no_writers":
            _wait(lambda: (lambda w: None if not w else f"writers {w}")(_writers_of_targets(api, a["targets"])),
                  a["timeout_seconds"], sleep, monotonic, "no writer on the legacy targets")
        else:
            raise C.StagingError(f"unknown plan action {op!r}")
        r = C.append_receipt(rec, HELPER, f"step-{a['step']}", {"action": a, "result": result}, clock)
        done.append(r)
    C.append_receipt(rec, HELPER, "quiesce-complete", {"plan_sha256": reviewed, "steps": len(done)}, clock)
    return done


# --- verify --------------------------------------------------------------------------------------------------------

def verify(api, rec: Mapping, clock=None, now: Optional[float] = None) -> Dict:
    """Read-only post-conditions of a quiesced legacy benchmark; retired after QUIET_SECONDS without change."""
    C.require_cluster(api, rec["cluster_uid"])
    s = _settings(rec)
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
                st = o.get("status") or {}
                if (o.get("spec") or {}).get("replicas") != want or (st.get("replicas") or 0) > want:
                    problems.append(f"Deployment {ns}/{n} is not at {want} replicas")
                state[f"{ns}/{n}"] = {"uid": o["metadata"]["uid"], "generation": o["metadata"].get("generation"),
                                      "replicas": (o.get("spec") or {}).get("replicas")}
        elif role == "keda":
            if api.get(C.path_of("scaledobjects", ns, n)) is not None:
                problems.append(f"ScaledObject {n} still exists")
            if api.get(C.path_of("horizontalpodautoscalers", ns, L.generated_hpa(n))) is not None:
                problems.append(f"generated HPA of {n} still exists")
        elif role == "retained":
            kind = "customresourcedefinitions" if ns is None else "persistentvolumeclaims"
            o = api.get(C.path_of(kind, ns, n))
            if o is None:
                problems.append(f"retained {kind} {n} is missing")
            elif kind == "persistentvolumeclaims" and (o.get("status") or {}).get("phase") != "Bound":
                problems.append(f"retained claim {n} is not Bound")
    writers = _writers_of_targets(api, [("demo", a) for a in L.ARMS])
    problems += [f"writer remains: {w}" for w in writers]
    receipts = C.read_receipts(C.receipts_path(rec, HELPER))
    completes = [r for r in receipts if r["event"] == "quiesce-complete"]
    quiet = False
    if completes and not problems:
        # Retired: every verify since quiesce completed was clean with this same state, and the first of them is at
        # least QUIET_SECONDS old (S4 step 6: ten minutes without arrivals, /scale activity or recreated writers).
        since = [r for r in receipts[receipts.index(completes[-1]):] if r["event"] == "verify"]
        t_now = now if now is not None else time.time()
        quiet = bool(since) and all(not r["data"]["problems"] and r["data"]["state"] == state for r in since) and \
            t_now - _ts(since[0]["at"]) >= QUIET_SECONDS
    result = {"problems": problems, "state": state, "quiesced": bool(completes) and not problems, "retired": quiet}
    C.append_receipt(rec, HELPER, "verify", result, clock)
    return result


def _ts(text: str) -> float:
    import datetime as dt
    return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()


# --- restore -------------------------------------------------------------------------------------------------------

def build_restore_plan(api, rec: Mapping) -> Dict:
    """The exact undo of the recorded quiesce steps, newest first. Read-only."""
    C.require_cluster(api, rec["cluster_uid"])
    receipts = C.read_receipts(C.receipts_path(rec, HELPER))
    steps = [r["data"] for r in receipts if r["event"].startswith("step-")]
    actions = []
    for d in reversed(steps):
        a, res = d["action"], d["result"]
        base = {k: a[k] for k in ("kind", "namespace", "name", "uid") if k in a}
        if a["op"] == "scale":
            actions.append(dict(base, step=len(actions) + 1, op="scale", after=res["from"], before=res["to"]))
        elif a["op"] == "suspend":
            actions.append(dict(base, step=len(actions) + 1, op="suspend", after=res["from"], before=True))
        elif a["op"] == "delete":
            actions.append(dict(base, step=len(actions) + 1, op="recreate", snapshot=res["snapshot"]))
    if not actions:
        raise C.StagingError("no recorded quiesce step to restore")
    return C.make_plan(RESTORE, rec, actions, quiesce_receipts=[r["sha256"] for r in receipts])


def _recreate_body(snapshot: Mapping) -> Dict:
    m = snapshot.get("metadata") or {}
    meta = {k: m[k] for k in ("name", "namespace", "labels", "annotations") if k in m}
    if any(v == C.REDACTED for v in (meta.get("annotations") or {}).values()):
        raise C.StagingError("the snapshot has redacted fields; recreate it from the protected backup instead")
    body = {"apiVersion": snapshot.get("apiVersion"), "kind": snapshot.get("kind"), "metadata": meta,
            "spec": snapshot.get("spec")}
    if C.REDACTED in json.dumps(body):
        raise C.StagingError("the snapshot has redacted fields; recreate it from the protected backup instead")
    return body


def restore(api, rec: Mapping, plan: Mapping, reviewed: str, clock=None) -> List[Dict]:
    C.require_reviewed(plan, reviewed, RESTORE, rec)
    _guard(plan)
    done = []
    for a in plan["actions"]:
        C.require_cluster(api, rec["cluster_uid"])
        if a["op"] == "scale":
            res = _scale(api, dict(a, generation=None), a["after"])
        elif a["op"] == "suspend":
            res = _set_suspend(api, dict(a, generation=None), a["after"])
        elif a["op"] == "recreate":
            snap = a["snapshot"]
            if C.sha256_file(snap["path"]) != snap["sha256"]:
                raise C.StagingError(f"the snapshot {snap['path']} changed; refusing")
            path = C.path_of(a["kind"], a["namespace"], a["name"])
            if api.get(path) is not None:
                raise C.StagingError(f"{a['kind']} {a['namespace']}/{a['name']} exists; never overwritten")
            with open(snap["path"]) as fh:
                body = _recreate_body(json.load(fh))
            created = api.create(C.path_of(a["kind"], a["namespace"]), body)
            res = {"recreated_uid": (created.get("metadata") or {}).get("uid")}
        else:
            raise C.StagingError(f"unknown restore action {a['op']!r}")
        done.append(C.append_receipt(rec, RESTORE, f"step-{a['step']}", {"action": a, "result": res}, clock))
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
    sub.add_parser("verify")
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
            print("quiesce complete; run verify, then again after 10 quiet minutes")
        elif cmd == "restore":
            restore(api, rec, C.load_plan(a.plan), a.plan_digest, clock)
            print("restore complete; verify a single writer per legacy Deployment before restarting arrivals")
        else:
            print(json.dumps(verify(api, rec, clock), indent=1, sort_keys=True))
    except C.StagingError as e:
        print(f"legacy: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
