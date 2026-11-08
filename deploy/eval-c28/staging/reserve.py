#!/usr/bin/env python3
"""S2 reservation of the workload (W) and support (S) nodes (STAGING-C28-PROD.md, S2).

  reserve.py --record R [plan]                                     read-only: write the plan file, print its digest
  reserve.py --record R apply    --plan F --plan-digest D          apply exactly the reviewed taint/label deltas
  reserve.py --record R rollback-plan                              read-only: what rollback would remove
  reserve.py --record R rollback --plan F --plan-digest D          remove only this run's own taint and label
  reserve.py --record R verify                                     read-only: reservation, occupants, placement
                                                                   (exit 0 ready, 3 reserved but NOT READY, 1 fail)

What it changes: on each of the two recorded nodes (bound by name AND UID) it adds the run's NoSchedule taint
<taint key>=<run id> and the role label <role label>=workload|support, nothing else. A taint is not an evacuation:
existing pods stay. It never drains, cordons, evicts, uses NoExecute, restarts or moves anything; the approved movable
stateless owners move through their own source of truth, one at a time, and this helper only reports whether they
have left (verify). A different existing value for the taint key or the role label is a conflict, never overwritten.
Each node is replaced with the read resourceVersion and UID after re-checking that its taints and role label are
still exactly the reviewed originals, which the receipts keep. Rollback removes only the exact taint and label this
run added, never touches the original cordon state, and evicts nothing. A mutation also recomputes the plan from the
live state and refuses if it no longer matches the reviewed digest.

The record's "reserve" section:
  {"nodes": {"workload": {"name", "uid"}, "support": {"name", "uid"}},
   "taint_key": "c28.devkuban.com/reserved", "role_label": "c28.devkuban.com/role",
   "allowed": {"daemonsets": [{"namespace", "name"}],          node-level system pods allowed on W and S
               "mirror_pod_prefixes": ["kube-proxy-"],          static (Node-owned) pods allowed by name prefix
               "broad_tolerations": [{"namespace", "kind", "name"}]},  reviewed owners whose tolerations admit the taint
   "movable": [{"namespace", "kind", "name", "uid"}]}            owner-approved stateless owners still to move
The taint value is the record's run_id. The params file must name the same nodes, taint key/value and role label.

Params stage: the reservation comes BEFORE capacity calibration on the reserved W, so the experiment parameters
(c0_rps, fixture resources, work_iterations, the HPA target, routes, ...) cannot be complete yet. Every command
therefore validates only the reservation stage of the params (execlib.load_params_stage / render.validate_stage):
run_id, nodes.workload/support, reservation.taint_key/taint_value/role_label, and control_namespace + release (the
evaluation release's pods on S classify as evaluation occupants), with the full validation's strictness; rollback
needs nothing more. verify separates "reservation verified" (require_reservation_pass: what calibration will build on,
with its own calibration-configuration and admission checks) from "ready for the experiment" (complete parameters,
require_ready_for_experiment; prepare.py enforces it live in every plan and again under the lock at apply):
  reservation: PASS|FAIL                 taint, role labels, cordon, pressure, occupants, broad tolerations
  placement:   PASS|FAIL|NOT READY       the rendered pods' tolerations and node affinity, checked only when the params
                                         validate fully (otherwise NOT READY, never PASS)
  experiment_readiness: "READY" | "NOT READY: " + every failing part with its reasons: "reservation FAIL (...)",
                         "params incomplete (<missing/placeholder fields; refusal>)", "rendered placement FAIL (...)"
  ready_for_experiment: true only when reservation and placement both PASS
  problems: everything blocking the experiment, the NOT READY item included, so an empty list implies
            ready_for_experiment (callers still check ready_for_experiment, never an empty list alone)
  result: PASS (ready) | RESERVED-NOT-READY | FAIL
"""

from __future__ import annotations

import argparse
import copy
import sys
from typing import Dict, List

import execlib as X

ROLES = ("workload", "support")
HELPER = "reserve"
ROLLBACK = "reserve-rollback"
STAGE = "reservation"            # the params stage reserve consumes (c28.render.STAGE_FIELDS["reservation"])
NOT_READY_EXIT = 3               # verify's exit status when the reservation PASSES but the experiment is NOT READY


def _section(record: X.Record, params: Dict) -> Dict:
    s = record.section("reserve")
    for role in ROLES:
        n = (s.get("nodes") or {}).get(role) or {}
        if not (X._DNS.match(str(n.get("name", ""))) and X._UUID.match(str(n.get("uid", "")))):
            raise X.StagingError(f"reserve.nodes.{role} needs name and uid")
    if s["nodes"]["workload"]["name"] == s["nodes"]["support"]["name"]:
        raise X.StagingError("W and S must be different nodes")
    r = params["reservation"]
    want = {"taint_key": r["taint_key"], "role_label": r["role_label"]}
    got = {k: s.get(k) for k in want}
    if got != want or r["taint_value"] != record.run_id:
        raise X.StagingError(f"the record's reservation {got} (value {record.run_id}) differs from the params "
                             f"{want} (value {r['taint_value']})")
    for role in ROLES:
        if params["nodes"][role] != s["nodes"][role]["name"]:
            raise X.StagingError(f"params.nodes.{role} differs from reserve.nodes.{role}")
    return s


def _taint(s: Dict, run_id: str) -> Dict:
    return {"key": s["taint_key"], "value": run_id, "effect": "NoSchedule"}


def _tolerates(tol: Dict, taint: Dict) -> bool:
    key, op = tol.get("key", ""), tol.get("operator", "Equal")
    if key and key != taint["key"]:
        return False
    if not key and op != "Exists":
        return False
    if op == "Equal" and tol.get("value", "") != taint["value"]:
        return False
    return tol.get("effect", "") in ("", taint["effect"])


def _owner(kube: X.Kube, pod: Dict, cache: Dict) -> Dict:
    """The pod's top controller: {kind, namespace, name, uid} (a ReplicaSet resolved to its Deployment)."""
    m = pod["metadata"]
    refs = [r for r in m.get("ownerReferences", []) if r.get("controller")]
    if not refs:
        return {"kind": "", "namespace": m.get("namespace"), "name": "", "uid": ""}
    r = refs[0]
    out = {"kind": r["kind"], "namespace": m.get("namespace"), "name": r["name"], "uid": r.get("uid", "")}
    if r["kind"] == "ReplicaSet":
        key = (m.get("namespace"), r["name"])
        if key not in cache:
            cache[key] = kube.get("ReplicaSet", r["name"], m.get("namespace"))
        rrefs = [x for x in ((cache[key] or {}).get("metadata", {}).get("ownerReferences", [])) if x.get("controller")]
        if rrefs:
            out = {"kind": rrefs[0]["kind"], "namespace": m.get("namespace"), "name": rrefs[0]["name"],
                   "uid": rrefs[0].get("uid", "")}
    return out


def classify(kube: X.Kube, record: X.Record, params: Dict, s: Dict, role: str, pod: Dict, cache: Dict) -> Dict:
    m, spec = pod["metadata"], pod.get("spec", {})
    owner = _owner(kube, pod, cache)
    allowed = s.get("allowed") or {}
    entry = {"pod": m["name"], "namespace": m.get("namespace"), "owner": owner}
    labels = m.get("labels", {}) or {}
    stateful = any("persistentVolumeClaim" in v for v in spec.get("volumes", []) or [])
    ds_allow = {(d.get("namespace"), d.get("name")) for d in allowed.get("daemonsets", [])}
    if owner["kind"] == "DaemonSet" and (owner["namespace"], owner["name"]) in ds_allow:
        entry["class"] = "allowed-system"
    elif owner["kind"] == "Node" and any(m["name"].startswith(p) for p in allowed.get("mirror_pod_prefixes", [])):
        entry["class"] = "allowed-system"
    elif labels.get(X.RUN_LABEL) == record.run_id:
        entry["class"] = "evaluation"
    elif role == "support" and m.get("namespace") == params["control_namespace"] and \
            labels.get("app.kubernetes.io/instance") == params["release"]:
        entry["class"] = "evaluation"
    elif owner["uid"] and owner["uid"] in {x.get("uid") for x in s.get("movable", [])} and not stateful:
        entry["class"] = "movable-pending-owner-move"
    else:
        entry["class"] = "stateful-blocker" if stateful or owner["kind"] == "StatefulSet" else "unplanned"
    return entry


def _occupants(kube: X.Kube, record: X.Record, params: Dict, s: Dict) -> Dict[str, List[Dict]]:
    out, cache = {}, {}
    pods = kube.list("Pod")
    for role in ROLES:
        live = [p for p in pods if (p.get("spec") or {}).get("nodeName") == s["nodes"][role]["name"]
                and p.get("status", {}).get("phase") not in ("Succeeded", "Failed")]
        out[role] = sorted((classify(kube, record, params, s, role, p, cache) for p in live),
                           key=lambda e: (e["namespace"] or "", e["pod"]))
    return out


def _broad_tolerations(kube: X.Kube, record: X.Record, s: Dict, taint: Dict) -> List[Dict]:
    """Owners of pods anywhere that would tolerate the run taint, other than allowed DaemonSets, the run's own pods
    and reviewed exceptions: they could land on W/S later."""
    allowed = s.get("allowed") or {}
    ds = {(d.get("namespace"), d.get("name")) for d in allowed.get("daemonsets", [])}
    ok = {(d.get("namespace"), d.get("kind"), d.get("name")) for d in allowed.get("broad_tolerations", [])}
    found, cache = {}, {}
    for p in kube.list("Pod"):
        if (p["metadata"].get("labels") or {}).get(X.RUN_LABEL) == record.run_id:
            continue
        if not any(_tolerates(t, taint) for t in p.get("spec", {}).get("tolerations", []) or []):
            continue
        o = _owner(kube, p, cache)
        if (o["kind"] == "DaemonSet" and (o["namespace"], o["name"]) in ds) or (o["namespace"], o["kind"], o["name"]) in ok:
            continue
        found[(o["namespace"] or "", o["kind"], o["name"] or p["metadata"]["name"])] = True
    return [{"namespace": k[0], "kind": k[1], "name": k[2]} for k in sorted(found)]


def _node(kube: X.Kube, s: Dict, role: str) -> Dict:
    want = s["nodes"][role]
    n = kube.get("Node", want["name"])
    if n is None:
        raise X.StagingError(f"node {want['name']} ({role}) does not exist")
    if n["metadata"]["uid"] != want["uid"]:
        raise X.StagingError(f"node {want['name']} has UID {n['metadata']['uid']}, not the recorded {want['uid']}: "
                             "a replaced node is a new inventory, refusing")
    return n


def plan(record: X.Record, kube: X.Kube) -> Dict:
    params = X.load_params_stage(record, STAGE)
    s = _section(record, params)
    taint = _taint(s, record.run_id)
    actions = []
    for role in ROLES:
        n = _node(kube, s, role)
        taints = n.get("spec", {}).get("taints")
        labels = n["metadata"].get("labels", {}) or {}
        conflicts = []
        same_key = [t for t in (taints or []) if t.get("key") == taint["key"]]
        add_taint = None
        if not same_key:
            add_taint = taint
        elif same_key != [taint]:
            conflicts.append(f"taint key {taint['key']} already present as {same_key}: a conflict, never overwritten")
        cur_label = labels.get(s["role_label"])
        add_label = None
        if cur_label is None:
            add_label = role
        elif cur_label != role:
            conflicts.append(f"label {s['role_label']}={cur_label} already present: a conflict, never overwritten")
        actions.append({"role": role, "node": n["metadata"]["name"], "uid": n["metadata"]["uid"],
                        "unschedulable": bool(n.get("spec", {}).get("unschedulable", False)),
                        "original_taints": copy.deepcopy(taints), "original_role_label": cur_label,
                        "add_taint": add_taint, "add_label": add_label, "conflicts": conflicts})
    occupants = _occupants(kube, record, params, s)
    broad = _broad_tolerations(kube, record, s, taint)
    problems = [c for a in actions for c in a["conflicts"]]
    return X.new_plan(HELPER, record, actions, taint=taint, role_label=s["role_label"], occupants=occupants,
                      broad_tolerations=broad, problems=problems,
                      never=["drain", "cordon", "evict", "NoExecute", "restart", "delete", "stateful move"])


def reserved_nodes(record: X.Record) -> Dict[str, Dict]:
    """The nodes this run's reservation currently holds, from the journal (applied intents, live or recovered):
    uid -> {node, uid, original_taints, original_role_label, unschedulable, added_taint, added_label}."""
    held: Dict[str, Dict] = {}
    for it in X.applied_intents(X.receipts(record, HELPER)):
        d = it["data"]
        if d["kind"] != "Node" or d["op"] != "update":
            continue
        ctx = d.get("context", {})
        if ctx.get("action") == "reserve":
            f = d["pre"].get("fields", {})
            held[d["uid"]] = {"role": ctx.get("role"), "node": d["name"], "uid": d["uid"],
                              "original_taints": f.get("/spec/taints"),
                              "original_role_label": f.get(ctx.get("label_ptr", "")),
                              "unschedulable": bool(f.get("/spec/unschedulable")),
                              "added_taint": ctx.get("added_taint"), "added_label": ctx.get("added_label")}
        elif ctx.get("action") == "rollback":
            held.pop(d["uid"], None)
    return held


def _ptr(role_label: str) -> str:
    return f"/metadata/labels/{X.json_pointer(role_label)}"


def apply(record: X.Record, kube: X.Kube, reviewed: Dict, reviewed_digest: str) -> Dict:
    with X.mutating(record, kube, HELPER) as _:
        live = plan(record, kube)                       # validated under the shared lock
        X.require_live(reviewed, reviewed_digest, HELPER, record, live)
        if live["problems"]:
            raise X.StagingError("the plan has conflicts: " + "; ".join(live["problems"]))
        ptr = _ptr(live["role_label"])
        X.receipt(record, HELPER, "apply-start", {"plan_sha256": reviewed_digest})
        for a in live["actions"]:
            if not (a["add_taint"] or a["add_label"]):
                continue

            def mutate(body, a=a):
                if body.get("spec", {}).get("taints") != a["original_taints"] or \
                        (body["metadata"].get("labels") or {}).get(live["role_label"]) != a["original_role_label"]:
                    raise X.StagingError(f"node {a['node']}'s taints or role label changed since the plan; refusing")
                if a["add_taint"]:
                    body.setdefault("spec", {}).setdefault("taints", []).append(a["add_taint"])
                if a["add_label"]:
                    body["metadata"].setdefault("labels", {})[live["role_label"]] = a["add_label"]
            expect = [["/spec/taints", {"contains": live["taint"]}], [ptr, a["add_label"] or a["original_role_label"]]]
            kube.journal.context = {"action": "reserve", "role": a["role"], "label_ptr": ptr,
                                    "added_taint": a["add_taint"], "added_label": a["add_label"]}
            kube.update("Node", a["node"], None, a["uid"], mutate, expect=expect,
                        pre_fields=["/spec/taints", ptr, "/spec/unschedulable"])
        return X.receipt(record, HELPER, "apply-complete", {"plan_sha256": reviewed_digest,
                                                            "nodes": list(reserved_nodes(record).values()),
                                                            "mutations": kube.mutations})


def rollback_plan(record: X.Record, kube: X.Kube) -> Dict:
    params = X.load_params_stage(record, STAGE)
    s = _section(record, params)
    taint = _taint(s, record.run_id)
    entries = X.receipts(record, HELPER)
    actions = []
    for ch in reserved_nodes(record).values():
        n = kube.get("Node", ch["node"])
        if n is None or n["metadata"]["uid"] != ch["uid"]:
            actions.append({"node": ch["node"], "uid": ch["uid"], "skip": "the node is gone or replaced"})
            continue
        taints = n.get("spec", {}).get("taints") or []
        idx = [i for i, t in enumerate(taints) if t == taint] if ch["added_taint"] else []
        label_now = (n["metadata"].get("labels", {}) or {}).get(s["role_label"])
        actions.append({"role": ch["role"], "node": ch["node"], "uid": ch["uid"], "taints_now": taints,
                        "remove_taint": len(idx) == 1, "taint_ambiguous": len(idx) > 1,
                        "remove_label": bool(ch["added_label"]) and label_now == ch["added_label"],
                        "label_now": label_now, "original_unschedulable": ch["unschedulable"],
                        "unschedulable_now": bool(n.get("spec", {}).get("unschedulable", False))})
    problems = [f"node {a['node']} carries the run taint more than once" for a in actions if a.get("taint_ambiguous")]
    if X.open_intents(entries):
        problems.append("the journal has intents with unknown outcomes: run recover first")
    return X.new_plan(ROLLBACK, record, actions, taint=taint, role_label=s["role_label"],
                      receipts_head=entries[-1]["sha256"] if entries else "", problems=problems,
                      never=["evict", "uncordon", "cordon", "remove another taint or label"])


def rollback(record: X.Record, kube: X.Kube, reviewed: Dict, reviewed_digest: str) -> Dict:
    with X.mutating(record, kube, HELPER):
        live = rollback_plan(record, kube)              # validated under the shared lock
        X.require_live(reviewed, reviewed_digest, ROLLBACK, record, live)
        if live["problems"]:
            raise X.StagingError("; ".join(live["problems"]))
        ptr = _ptr(live["role_label"])
        for a in live["actions"]:
            if a.get("skip"):
                continue
            def mutate(body, a=a):
                if (body.get("spec", {}).get("taints") or []) != a["taints_now"] or \
                        (body["metadata"].get("labels") or {}).get(live["role_label"]) != a["label_now"]:
                    raise X.StagingError(f"node {a['node']} changed since the rollback plan; refusing")
                if a["remove_taint"]:
                    body["spec"]["taints"] = [t for t in body["spec"]["taints"] if t != live["taint"]]
                if a["remove_label"]:
                    del body["metadata"]["labels"][live["role_label"]]
            expect = [["/spec/taints", {"lacks": live["taint"]}]]
            if a["remove_label"]:
                expect.append([ptr, None])
            kube.journal.context = {"action": "rollback"}
            kube.update("Node", a["node"], None, a["uid"], mutate, expect=expect,
                        pre_fields=["/spec/taints", ptr, "/spec/unschedulable"])
            X.receipt(record, HELPER, "node-restored", {"plan_sha256": reviewed_digest,
                                                        "node": {"node": a["node"], "uid": a["uid"]},
                                                        "removed_taint": a["remove_taint"],
                                                        "removed_label": a["remove_label"],
                                                        "cordon_unchanged": a["unschedulable_now"] ==
                                                        a["original_unschedulable"]})
        return X.receipt(record, HELPER, "rollback-complete", {"plan_sha256": reviewed_digest,
                                                               "mutations": kube.mutations})


def rendered_placement_problems(params: Dict) -> List[str]:
    """Inspect the RENDERED pods (not just the parameters): every fixture/ingress pod tolerates exactly the run taint and
    requires the W role and hostname; every route pod the S role and hostname."""
    c28 = X.import_harness()
    r = params["reservation"]
    want_tol = [{"key": r["taint_key"], "operator": "Equal", "value": r["taint_value"], "effect": "NoSchedule"}]
    out = []
    for o in c28.render.render_all(params):
        if o["kind"] != "Deployment":
            continue
        spec = o["spec"]["template"]["spec"]
        name = o["metadata"]["name"]
        role = "support" if name.endswith("-route") else "workload"
        if spec.get("tolerations") != want_tol:
            out.append(f"{name}: tolerations {spec.get('tolerations')} are not exactly the run taint")
        terms = spec.get("affinity", {}).get("nodeAffinity", {}).get("requiredDuringSchedulingIgnoredDuringExecution",
                                                                     {}).get("nodeSelectorTerms", [])
        want_terms = [{"matchExpressions": [{"key": r["role_label"], "operator": "In", "values": [role]},
                                            {"key": "kubernetes.io/hostname", "operator": "In",
                                             "values": [params["nodes"][role]]}]}]
        if terms != want_terms:
            out.append(f"{name}: required node affinity is not the {role} role and hostname")
    return out


def verify(record: X.Record, kube: X.Kube) -> Dict:
    params = X.load_params_stage(record, STAGE)
    s = _section(record, params)
    taint = _taint(s, record.run_id)
    problems, nodes = [], []                  # problems: the reservation's own
    applied = reserved_nodes(record)
    for role in ROLES:
        n = _node(kube, s, role)
        taints = n.get("spec", {}).get("taints") or []
        labels = n["metadata"].get("labels", {}) or {}
        conds = {c["type"]: c["status"] for c in n.get("status", {}).get("conditions", [])}
        if [t for t in taints if t.get("key") == taint["key"]] != [taint]:
            problems.append(f"{role} {n['metadata']['name']}: the run taint is not exactly {taint}")
        if labels.get(s["role_label"]) != role:
            problems.append(f"{role} {n['metadata']['name']}: role label is not {role}")
        for c in ("MemoryPressure", "DiskPressure"):
            if conds.get(c) != "False":
                problems.append(f"{role} {n['metadata']['name']}: {c} is {conds.get(c, 'unknown')} (a manifest stop)")
        a = applied.get(n["metadata"]["uid"])
        unsched = bool(n.get("spec", {}).get("unschedulable", False))
        if a is not None and a["unschedulable"] != unsched:
            problems.append(f"{role} {n['metadata']['name']}: cordon state changed from {a['unschedulable']}")
        nodes.append({"role": role, "name": n["metadata"]["name"], "uid": n["metadata"]["uid"], "taints": taints,
                      "role_label": labels.get(s["role_label"]), "unschedulable": unsched, "conditions": conds})
    occ = _occupants(kube, record, params, s)
    for role, entries in occ.items():
        for e in entries:
            if e["class"] not in ("allowed-system", "evaluation"):
                problems.append(f"{role}: {e['namespace']}/{e['pod']} ({e['owner']['kind']} {e['owner']['name']}) is "
                                f"{e['class']}")
    broad = _broad_tolerations(kube, record, s, taint)
    for b in broad:
        problems.append(f"{b['namespace']}/{b['kind']}/{b['name']} tolerates the run taint and is not allowlisted")
    incomplete = X.experiment_params_problems(params)
    if incomplete:
        placement, placement_problems = "NOT READY", []
    else:
        try:
            placement_problems = rendered_placement_problems(params)
        except X.import_harness().render.ParamsError as e:
            placement_problems = [f"the params do not render: {e}"]
        placement = "PASS" if not placement_problems else "FAIL"
    reservation = "PASS" if not problems else "FAIL"
    ready = reservation == "PASS" and placement == "PASS"
    # experiment_readiness comes from the COMBINED verdict, every failing part with its specific reasons
    incomplete_item = f"params incomplete ({'; '.join(incomplete)})"
    reasons = ([f"reservation FAIL ({'; '.join(problems)})"] if problems else []) + \
        ([incomplete_item] if incomplete else []) + \
        ([f"rendered placement FAIL ({'; '.join(placement_problems)})"] if placement_problems else [])
    readiness = "READY" if ready else "NOT READY: " + " | ".join(reasons)
    blocking = problems + placement_problems + ([f"experiment readiness: NOT READY: {incomplete_item}"]
                                                if incomplete else [])
    result = "PASS" if ready else ("RESERVED-NOT-READY" if reservation == "PASS" and placement == "NOT READY"
                                   else "FAIL")
    return {"helper": HELPER, "action": "verify", "run_id": record.run_id, "nodes": nodes, "occupants": occ,
            "broad_tolerations": broad, "reservation": reservation, "reservation_problems": problems,
            "placement": placement, "placement_problems": placement_problems, "experiment_readiness": readiness,
            "ready_for_experiment": ready, "problems": blocking, "result": result}


def require_reservation_pass(out: Dict) -> None:
    """The reservation part of reserve verify's verdict only (taint, role labels, cordon, pressure, occupants, broad
    tolerations), whatever the experiment parameters. Capacity calibration on W will need this PLUS its own separately
    validated calibration configuration and admission checks (not implemented here); it never needs complete experiment
    parameters, which would recreate the c0/HPA-target dependency cycle."""
    if not (out.get("reservation") == "PASS" and out.get("reservation_problems") == []):
        raise X.StagingError(f"the reservation does not pass: {out.get('reservation')} "
                             f"({'; '.join(out.get('reservation_problems') or []) or 'no verdict'})")


def require_ready_for_experiment(out: Dict) -> None:
    """For callers that need the COMPLETE experiment parameters on a passing reservation (preparation, sessions):
    reservation and rendered placement PASS, never an empty problems list alone. It is NOT the calibration gate:
    it requires the calibrated parameters themselves (see require_reservation_pass)."""
    if not (out.get("ready_for_experiment") is True and out.get("reservation") == "PASS"
            and out.get("placement") == "PASS" and not out.get("problems")):
        raise X.StagingError(f"the reservation is not ready for the experiment: reservation {out.get('reservation')}, "
                             f"placement {out.get('placement')} ({out.get('experiment_readiness')})")


def main(argv=None, api=None) -> int:
    ap = argparse.ArgumentParser(prog="reserve.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", required=True)
    ap.add_argument("command", nargs="?", default="plan",
                    choices=["plan", "apply", "rollback-plan", "rollback", "verify", "recover-plan", "recover"])
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    args = ap.parse_args(argv)
    try:
        record = X.load_record(args.record, ("reserve",))
        kube = record.kube(api)
        if args.command == "recover-plan":
            X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
        elif args.command == "recover":
            if not args.plan:
                raise X.StagingError("recover needs --plan <the reviewed recover plan> and --plan-digest")
            X.emit(X.recover(record, kube, X.load_plan(args.plan), args.plan_digest))
        elif args.command == "plan":
            X.emit(X.cli_plan(record, HELPER, plan(record, kube)))
        elif args.command == "rollback-plan":
            X.emit(X.cli_plan(record, ROLLBACK, rollback_plan(record, kube), "rollback-plan"))
        elif args.command in ("apply", "rollback"):
            if not args.plan:
                raise X.StagingError("a mutation needs --plan <the reviewed plan file> and --plan-digest")
            fn = apply if args.command == "apply" else rollback
            X.emit(fn(record, kube, X.load_plan(args.plan), args.plan_digest))
        else:
            out = verify(record, kube)
            X.emit(out)
            return 0 if out["ready_for_experiment"] else NOT_READY_EXIT if out["result"] == "RESERVED-NOT-READY" else 1
    except X.StagingError as e:
        print(f"reserve: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
