#!/usr/bin/env python3
"""Roll ONE new fixture image onto the Deployments of an existing demonstration installation, as a journalled, fenced
transition. `prepare` creates objects and never updates them; this helper is the only way the shared fixture digest of
an existing installation changes.

  imageroll.py --record R status                               read-only: the transition and every member's state
  imageroll.py --record R plan-open --new-params P [--supersedes ID]
  imageroll.py --record R open --plan F --plan-digest D        journal the transition, THEN publish the new record
  imageroll.py --record R plan-open-recover | open-recover --plan F --plan-digest D     a crash inside `open`
  imageroll.py --record R plan-roll | roll --plan F --plan-digest D
  imageroll.py --record R plan-void | void --plan F --plan-digest D     the fencing write for one lost patch
  imageroll.py --record R plan-complete --query-api U | complete --plan F --plan-digest D --query-api U
  imageroll.py --record R recover-plan | recover --plan F --plan-digest D

The transition. Its id is `ir-<seq>-<12 hex>`: the journal sequence plus a hash over the binding and a nonce drawn at
planning, so a second attempt at the same change never shares an id. `open` appends `transition-prepared` (from that
receipt on execlib.image_fence refuses every other helper, whichever record is loaded), replaces the canonical record
by the candidate (same record, the new params file, an `image_transition` block; the old bytes are kept beside it), and
appends `transition-open`. A crash in between is decided by `open-recover`, from the canonical record's path, which
the transition binds (an archived copy decides nothing): abandoned while the old record is still canonical, opened
when the new one already is. An abandoned successor returns its parent to open.

One member = one patch + one verified rollout, in the bound order. The patch is ONE JSON Patch whose `test` operations
(UID, resourceVersion, container name, old image) the API server enforces in the same request; it also writes the
operation id into the annotation c28.devkuban.com/image-roll. A refusal is recorded as not-applied; an answer is
validated (UID, annotation, the complete expected spec, a moved resourceVersion); anything else leaves the intent
unresolved and keeps the lock. The rollout is then waited for and verified (generation observed, exact counts, no pod
of an older ReplicaSet left, terminating ones included, every pod's imageID, placement, the Service's ready endpoints
being exactly the new pods) before the next member is touched. The safety conditions are re-checked before EVERY patch
and on every resume. Nothing is retried and nothing is rolled back automatically: a stop leaves the fence in place and
a new reviewed `plan-roll` resumes (rolled members are re-verified and skipped).

Member states: `from` (the spec is the render with the old image, no operation of this transition), `patched` (THIS
transition's applied patch, its annotation, the expected spec), `rolled` (patched + a member-rolled receipt whose pods
still verify), `target` (a successor's member that already was at the target image when it was planned), `unresolved`
(a patch with an unknown outcome), `divergent` (everything else; a new image without this transition's patch is
divergent, never accepted). The live spec is compared whole with the render; a server default is ignored only where it
holds its default value AND the render does not set that field (DEFAULTS, enumerated; plus the deprecated
serviceAccount field where it mirrors the rendered serviceAccountName).

A lost patch whose object still shows the old image and the old resourceVersion stays UNRESOLVED (the request may
still commit). `void` is the reviewed way out: under the recovery guard it sends one JSON Patch that tests the ORIGINAL
request's UID and resourceVersion and adds c28.devkuban.com/image-roll-void with the operation id. Once that is
applied the resourceVersion has moved and the lost patch can never commit; `recover` then reconciles it as not-applied.

`complete` writes `transition-complete` only after one final pass: every member rolled and re-verified, the
structural checks, both PredictiveAutoscalers in Recommend, the telemetry gate, and the zero-counter evidence the gate
does not give: the replacement processes expose the ingress arrival counters, the dispatch counters of every
discovered endpoint and the fixture request counters at exactly zero before any traffic. A successor transition
(plan-open --supersedes) is a REVERSE only when it restores exactly the params file and image a forward transition
started from: then its completion needs neither the zero counters nor the gate, and says so (the old binary creates
its counters at the first request). Every other successor completes like a forward transition.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import secrets
import sys
import time
from typing import Callable, Dict, List, Optional, Tuple

import common_records as C
import execlib as X
import prepare

HELPER = X.IMAGEROLL
PATCH_OP, VOID_OP = "image-roll", "image-roll-void"
ROLL_ANNOTATION, VOID_ANNOTATION = "c28.devkuban.com/image-roll", "c28.devkuban.com/image-roll-void"
WAIT_SINGLE_S, WAIT_TARGET_S, POLL_S = 300, 900, 5
ZERO_LOOKBACK_S, ZERO_MIN_SAMPLES, ZERO_MAX_AGE_S, ZERO_FUTURE_S = 180, 3, 60, 5
SERVICE_LABEL = "kubernetes.io/service-name"
Fetch = Callable[[str], Dict]

# Server defaults of a Deployment (apps/v1, kube-apiserver 1.35), by path with list indices written []. A live field is
# ignored ONLY when the render does not set it and its live value is the default here; everything else is compared.
_POD, _CTR = r"spec\.template\.spec", r"spec\.template\.spec\.(?:initContainers|containers)\[\]"
_PROBE = _CTR + r"\.(?:readinessProbe|livenessProbe|startupProbe)"
DEFAULTS: List[Tuple[str, object]] = [
    (r"spec\.progressDeadlineSeconds", 600), (r"spec\.revisionHistoryLimit", 10),
    (r"spec\.strategy", {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": "25%", "maxUnavailable": "25%"}}),
    (r"spec\.template\.metadata\.creationTimestamp", None),
    (_POD + r"\.dnsPolicy", "ClusterFirst"), (_POD + r"\.restartPolicy", "Always"),
    (_POD + r"\.schedulerName", "default-scheduler"), (_POD + r"\.terminationGracePeriodSeconds", 30),
    (_POD + r"\.securityContext", {}),
    (_CTR + r"\.imagePullPolicy", "IfNotPresent"), (_CTR + r"\.terminationMessagePath", "/dev/termination-log"),
    (_CTR + r"\.terminationMessagePolicy", "File"), (_CTR + r"\.resources", {}),
    (_CTR + r"\.ports\[\]\.protocol", "TCP"),
    (_PROBE + r"\.timeoutSeconds", 1), (_PROBE + r"\.periodSeconds", 10), (_PROBE + r"\.successThreshold", 1),
    (_PROBE + r"\.failureThreshold", 3), (_PROBE + r"\.httpGet\.scheme", "HTTP"),
    (_CTR + r"\.env\[\]\.valueFrom\.fieldRef\.apiVersion", "v1"),
    (_POD + r"\.volumes\[\]\.(?:configMap|secret|projected)\.defaultMode", 420),
]
_DEFAULTS = [(re.compile(rx + "$"), v) for rx, v in DEFAULTS]


class Ctx:
    def __init__(self, record: X.Record, kube: X.Kube, now: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep,
                 permission_check: Optional[Callable[[], Dict]] = None):
        self.record, self.kube, self.now, self.sleep = record, kube, now, sleep
        self.s = prepare._section(record)
        if prepare.stage_of(self.s) != "demonstration":
            raise X.StagingError("the image roll belongs to the demonstration stage")
        self.params = prepare.load_params(record, "demonstration")
        self.c28 = X.import_harness()
        self.ns, self.control_ns = self.params["namespace"], self.params["control_namespace"]
        self.permission_check = permission_check or (lambda: _operator_permissions(self))


# --- the spec comparison ---------------------------------------------------------------------------------------------

def spec_diff(want, live, path: str = "spec") -> List[str]:
    """Every difference between the rendered spec and the live one, by path. A live field the render does not set is
    ignored only when it holds an enumerated server default."""
    if isinstance(want, dict) and isinstance(live, dict):
        out = []
        for k in sorted(set(want) | set(live)):
            p = f"{path}.{k}"
            if k not in live:
                out.append(f"{p}: rendered, absent live")
            elif k not in want:
                # the API server mirrors serviceAccountName into the deprecated serviceAccount field
                alias = p == "spec.template.spec.serviceAccount" and want.get("serviceAccountName") == live[k]
                if not alias and not any(rx.match(p) and live[k] == v for rx, v in _DEFAULTS):
                    out.append(f"{p}: live only ({json.dumps(live[k], sort_keys=True)[:80]})")
            else:
                out += spec_diff(want[k], live[k], p)
        return out
    if isinstance(want, list) and isinstance(live, list):
        if len(want) != len(live):
            return [f"{path}: {len(want)} rendered entries, {len(live)} live"]
        return [d for a, b in zip(want, live) for d in spec_diff(a, b, path + "[]")]
    return [] if want == live and type(want) is type(live) else [
        f"{path}: rendered {json.dumps(want, sort_keys=True)[:80]}, live {json.dumps(live, sort_keys=True)[:80]}"]


def _rendered(params: Dict) -> Dict[Tuple[str, str], Dict]:
    """The rendered Deployments that run the fixture image, by (namespace, name), with phase and container index."""
    out = {}
    parts = prepare.split(params, "demonstration")
    for phase in ("common", "targets"):
        for o in parts[phase]:
            if o["kind"] != "Deployment":
                continue
            idx = [i for i, ctr in enumerate(o["spec"]["template"]["spec"]["containers"])
                   if ctr["image"] == params["image"]]
            if len(idx) == 1:
                out[(o["metadata"]["namespace"], o["metadata"]["name"])] = {"object": o, "phase": phase,
                                                                            "index": idx[0]}
            elif idx:
                raise X.StagingError(f"{o['metadata']['name']}: more than one container runs the fixture image")
    return out


def _spec_with(obj: Dict, index: int, image: str) -> Dict:
    spec = copy.deepcopy(obj["spec"])
    spec["template"]["spec"]["containers"][index]["image"] = image
    return spec


def _order(c: Ctx, key: Tuple[str, str], phase: str) -> Tuple[int, str]:
    """The control namespace's routes first, then the ingress, then the targets (least visible first)."""
    return (0 if key[0] == c.control_ns else 1 if phase == "common" else 2, key[1])


# --- the journal -----------------------------------------------------------------------------------------------------

def _transition(record: X.Record) -> Optional[Dict]:
    """The prepared or open transition: {"id", "state", "data"}."""
    ts = X.image_transitions(X.receipts(record, HELPER))
    held = [(i, t) for i, t in ts.items() if t["state"] in ("prepared", "open")]
    return {"id": held[0][0], "state": held[0][1]["state"], "data": held[0][1]["data"]} if held else None


def _ops(record: X.Record, tid: str, member: int, op: str = PATCH_OP) -> Dict[str, List[Dict]]:
    """This transition's journalled operations on one member: {"applied": [...], "refused": [...], "open": [...]},
    each entry {"step", "data"} in journal order."""
    out: Dict[str, List[Dict]] = {"applied": [], "refused": [], "open": []}
    steps = X._steps(X.receipts(record, HELPER))
    for n, s in sorted(steps.items(), key=lambda kv: int(kv[0])):
        d = s["intent"]
        if d is None or d["op"] != op or d["pre"].get("transition") != tid or d["pre"].get("member") != member:
            continue
        res, rec = (s["outcome"] or {}).get("result"), (s["reconciled"] or {}).get("observed")
        key = "applied" if res == "applied" or rec == "done" else "refused" if res or rec else "open"
        out[key].append({"step": n, "data": d})
    return out


def operation_id(tid: str, member: int, mark: str) -> str:
    return f"{tid}/{member}/{mark}"


def _rolled_receipt(record: X.Record, tid: str, member: int) -> Optional[Dict]:
    e = X.last(X.receipts(record, HELPER), "member-rolled", id=tid, member=member)
    return e["data"] if e else None


# --- pods, ReplicaSets and endpoints of a member ----------------------------------------------------------------------

def _owned(objs: List[Dict], uids: set) -> List[Dict]:
    return [o for o in objs if any(r.get("uid") in uids for r in o["metadata"].get("ownerReferences") or [])]


def _member_pods(c: Ctx, dep: Dict) -> Tuple[List[Dict], List[Dict]]:
    """(the Deployment's ReplicaSets, every existing pod of any of them, terminating ones included). Strict lists."""
    ns = dep["metadata"]["namespace"]
    rss = _owned(c.kube.list("ReplicaSet", ns, strict=True), {dep["metadata"]["uid"]})
    return rss, _owned(c.kube.list("Pod", ns, strict=True), {r["metadata"]["uid"] for r in rss})


def _digest_of(image: str) -> str:
    return image.rsplit("@", 1)[1]


def rollout_problems(c: Ctx, m: Dict, image: str, nodes: List[str], fresh: bool,
                     dep: Optional[Dict] = None) -> Tuple[List[str], List[Dict]]:
    """Every reason member m is not completely rolled out on `image`, and its pods as recorded. fresh: the pods are
    this transition's replacements (0 restarts required). dep: the Deployment as the caller just read it."""
    dep = dep if dep is not None else c.kube.get("Deployment", m["name"], m["namespace"])
    if dep is None or dep["metadata"]["uid"] != m["uid"]:
        return [f"{m['name']}: the Deployment is gone or was replaced"], []
    out, st, want = [], dep.get("status") or {}, int(dep["spec"].get("replicas") or 0)
    if int(st.get("observedGeneration") or 0) < int(dep["metadata"].get("generation") or 0):
        out.append(f"{m['name']}: generation {dep['metadata'].get('generation')} not yet observed")
    counts = {k: int(st.get(k) or 0) for k in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")}
    if any(v != want for v in counts.values()):
        out.append(f"{m['name']}: wants {want}, status {counts}")
    rss, pods = _member_pods(c, dep)
    cur = [r for r in rss if int(r["spec"].get("replicas") or 0) > 0]
    if len(cur) != 1 or cur[0]["spec"]["template"]["spec"]["containers"][m["index"]]["image"] != image:
        out.append(f"{m['name']}: not exactly one active ReplicaSet running the expected image")
        return out, []
    new_uid, seen = cur[0]["metadata"]["uid"], []
    for p in sorted(pods, key=lambda p: p["metadata"]["name"]):
        name, pm = p["metadata"]["name"], p["metadata"]
        if not any(r.get("uid") == new_uid for r in pm.get("ownerReferences") or []):
            state = " (terminating)" if pm.get("deletionTimestamp") else ""
            out.append(f"{name}{state}: a pod of an older ReplicaSet still exists")
            continue
        cs = [s for s in (p.get("status") or {}).get("containerStatuses") or [] if s.get("name") == m["container"]]
        ready = any(k.get("type") == "Ready" and k.get("status") == "True"
                    for k in (p.get("status") or {}).get("conditions") or [])
        image_id = cs[0].get("imageID", "") if cs else ""
        if pm.get("deletionTimestamp"):
            out.append(f"{name}: terminating")
        if not ready:
            out.append(f"{name}: not Ready")
        if not image_id.endswith(_digest_of(image)):
            out.append(f"{name}: imageID is not the expected digest")
        if fresh and cs and int(cs[0].get("restartCount") or 0) != 0:
            out.append(f"{name}: restarted {cs[0].get('restartCount')} times")
        if p["spec"].get("nodeName") not in nodes:
            out.append(f"{name}: on {p['spec'].get('nodeName')}, not on {nodes}")
        seen.append({"name": name, "uid": pm["uid"], "imageID": image_id, "node": p["spec"].get("nodeName")})
    if len(seen) != want:
        out.append(f"{m['name']}: {len(seen)} pods of the active ReplicaSet, {want} wanted")
    ready_uids = set()
    for es in c.kube.list("EndpointSlice", m["namespace"], labels={SERVICE_LABEL: m["name"]}, strict=True):
        for ep in es.get("endpoints") or []:
            if (ep.get("conditions") or {}).get("ready") is True:
                ready_uids.add((ep.get("targetRef") or {}).get("uid"))
    if ready_uids != {p["uid"] for p in seen}:
        out.append(f"{m['name']}: the Service's ready endpoints are not exactly the active pods")
    return out, seen


def member_view(c: Ctx, t: Dict, m: Dict, live: Optional[Dict] = None) -> Dict:
    """One member's state under transition t (the journal's data), with the differences that explain it. live: the
    Deployment as the caller just read it (the state then describes exactly that snapshot)."""
    tid, d = t["id"], t["data"]
    live = live if live is not None else c.kube.get("Deployment", m["name"], m["namespace"])
    if live is None or live["metadata"]["uid"] != m["uid"]:
        return {"member": m["member"], "name": m["name"], "state": "divergent", "why": ["gone or replaced"]}
    obj = _rendered(c.params)[(m["namespace"], m["name"])]["object"]
    d_from = spec_diff(_spec_with(obj, m["index"], d["from_image"]), live["spec"])
    d_to = spec_diff(_spec_with(obj, m["index"], d["to_image"]), live["spec"])
    ann = (live["metadata"].get("annotations") or {}).get(ROLL_ANNOTATION) or ""
    mine, ops = ann.startswith(tid + "/"), _ops(c.record, tid, m["member"])
    base = {"member": m["member"], "name": m["name"], "namespace": m["namespace"],
            "generation": live["metadata"].get("generation"), "spec_sha256": C.digest(live["spec"])}
    if ops["open"]:
        return dict(base, state="unresolved", why=[f"patch step {ops['open'][0]['step']} has no known outcome"])
    last = ops["applied"][-1] if ops["applied"] else None
    if last and ann == operation_id(tid, m["member"], last["data"]["mark"]) and not d_to:
        rec = _rolled_receipt(c.record, tid, m["member"])
        if rec:
            probs, pods = rollout_problems(c, m, d["to_image"], m["nodes"], fresh=True, dep=live)
            if not probs and sorted(p["uid"] for p in pods) == sorted(p["uid"] for p in rec["pods"]):
                return dict(base, state="rolled", why=[])
            return dict(base, state="patched", why=probs or ["the pods differ from the member-rolled receipt"])
        return dict(base, state="patched", why=["rollout not yet verified"])
    if not last and not mine and not d_from:
        return dict(base, state="from", why=[])
    if m["start"] == "target" and not last and not mine and not d_to:
        return dict(base, state="target", why=[])
    why = (d_to if (last or mine) else d_from)[:6] or ["the image is not explained by this transition's journal"]
    return dict(base, state="divergent", why=why)


# --- safety conditions (before every patch and on every resume) ------------------------------------------------------

def _operator_permissions(c: Ctx) -> Dict:
    """The permissions helper's proof, in process: the operator has no replica-write permission."""
    import permissions
    import subprocess
    k = c.record.doc["kubeconfig"]
    sa = f"{c.params['release']}-predictive-autoscaler-operator"
    targets = c.c28.demo.target_names(c.params)

    def kubectl(args):
        p = subprocess.run(["kubectl", "--kubeconfig", k["path"], "--context", k["context"], "--request-timeout=30s",
                            *args], capture_output=True, text=True, timeout=120)
        return p.returncode, p.stdout or ""
    return permissions.verdict(permissions.collect(kubectl, c.control_ns, sa, c.ns, targets, "operator"),
                               c.control_ns, sa, targets, c.record.cluster_uid)


def headroom_problems(c: Ctx, m: Dict) -> List[str]:
    """Room for the member's worst case of extra pods: maxSurge bounds starting pods, not existing ones, so a target's
    terminating pods can overlap all of their replacements; a single-replica member has one surge pod plus one
    terminating. Checked against the namespace quota, the nodes' pod capacity and their unrequested CPU and memory."""
    obj = _rendered(c.params)[(m["namespace"], m["name"])]["object"]
    replicas = int(obj["spec"].get("replicas") or 1)
    extra, out = (replicas if replicas > 1 else 2), []
    for q in c.kube.list("ResourceQuota", m["namespace"], strict=True):
        hard = ((q.get("status") or {}).get("hard") or (q.get("spec") or {}).get("hard") or {}).get("pods")
        used = ((q.get("status") or {}).get("used") or {}).get("pods")
        if hard is not None and (used is None or int(used) + extra > int(hard)):
            out.append(f"{m['name']}: quota {q['metadata']['name']} has pods {used}/{hard}, {extra} more may exist")
    cpu, mem = c.c28.render.cpu_millicores, c.c28.render.memory_bytes

    def requests(spec: Dict) -> Tuple:
        rs = [(ctr.get("resources") or {}).get("requests") or {} for ctr in spec.get("containers") or []]
        return sum(cpu(r.get("cpu", 0)) for r in rs), sum(mem(r.get("memory", 0)) for r in rs)
    try:
        need = requests(obj["spec"]["template"]["spec"])
        for node in m["nodes"]:
            n = c.kube.get("Node", node)
            alloc = ((n or {}).get("status") or {}).get("allocatable") or {}
            pods = [p for p in c.kube.list("Pod", node_name=node, strict=True)
                    if (p.get("status") or {}).get("phase") not in ("Succeeded", "Failed")]
            used = [requests(p["spec"]) for p in pods]
            if n is None or len(pods) + extra > int(alloc.get("pods", 0)):
                out.append(f"{m['name']}: node {node} has {len(pods)} pods of {alloc.get('pods')}, {extra} more may exist")
            if cpu(alloc.get("cpu", 0)) - sum(u[0] for u in used) < extra * need[0] or \
                    mem(alloc.get("memory", 0)) - sum(u[1] for u in used) < extra * need[1]:
                out.append(f"{m['name']}: node {node} lacks unrequested CPU or memory for {extra} more pods")
    except (c.c28.render.ParamsError, ValueError, TypeError) as e:
        out.append(f"{m['name']}: the headroom cannot be evaluated ({e})")
    return out


def safety_problems(c: Ctx, t: Dict) -> List[str]:
    """The conditions that must hold whenever this helper is about to change a member."""
    import bounded
    out = list(bounded.fence_problems(bounded.Ctx(c.record, c.kube), image_roll=True))
    left = C.unresolved_intents(c.record.doc)
    if left:
        out.append(f"intents with unknown outcomes {left}: recover first")
    # every created object still the same one, both PredictiveAutoscalers in Recommend, empty fault schedules, the
    # single replica writer per target, the fixed replica counts
    out += prepare.verify(c.record, c.kube)["problems"]
    out += prepare.demonstration_gate(c.record, c.kube, c.params, c.s, prepare.split(c.params, "demonstration"))[1]
    perm = c.permission_check()
    if perm.get("verdict") != "pass" or perm.get("write_grants"):
        out.append(f"the operator's replica-write permission is not proven absent (verdict {perm.get('verdict')})")
    for node in sorted({n for m in t["data"]["members"] for n in m["nodes"]}):
        n = c.kube.get("Node", node)
        ready = any(k.get("type") == "Ready" and k.get("status") == "True"
                    for k in ((n or {}).get("status") or {}).get("conditions") or [])
        if n is None or not ready or (n.get("spec") or {}).get("unschedulable"):
            out.append(f"node {node} is not Ready and schedulable")
    return out


# --- open: journal first, then the record ----------------------------------------------------------------------------

def _raw_record(record: X.Record) -> Dict:
    return C.load_record(record.path)


def plan_open(c: Ctx, new_params: str, nonce: Optional[str] = None, supersedes: Optional[str] = None) -> Dict:
    record, problems = c.record, []
    ts = X.image_transitions(X.receipts(record, HELPER))
    fence = X.image_fence(record)
    parent = (ts.get(supersedes) or {}) if supersedes else {}
    if supersedes:
        if fence != supersedes or parent.get("state") != "open":
            problems.append(f"{supersedes} is not the open image transition ({fence})")
        elif os.path.realpath(record.path) != parent["data"]["record_path"]:
            problems.append(f"the loaded record is not {supersedes}'s canonical record")
    elif fence:
        problems.append(f"image transition {fence} is open: complete it, or plan its successor with --supersedes")
    left = C.unresolved_intents(record.doc)
    if left:
        problems.append(f"intents with unknown outcomes {left}: resolve them before a transition is opened")
    old_path, new_path = record.resolve(record.doc["params"]), record.resolve(new_params)
    try:
        with open(new_path) as fh:
            new = json.load(fh)
        c.c28.render.validate_stage(new, "demonstration")
    except (OSError, ValueError, c.c28.render.ParamsError) as e:
        raise X.StagingError(f"the new params file cannot be used: {e}") from e
    if {k: v for k, v in new.items() if k != "image"} != {k: v for k, v in c.params.items() if k != "image"}:
        problems.append("the new params differ from the current ones in more than `image`")
    if new["image"] == c.params["image"]:
        problems.append("the new params name the image that is already current")
    # A REVERSE restores exactly what a forward transition started from (its params file and image); only a reverse
    # may leave members that were never patched where they are. Every other successor is a forward transition.
    pd = parent.get("data") or {}
    kind = "reverse" if (pd.get("kind") == "forward" and new["image"] == pd.get("from_image") and
                         X.sha256_file(new_path) == pd["from_params"]["sha256"]) else "forward"
    members = []
    rendered = _rendered(c.params)
    created = prepare.created_objects(record)
    for key, r in sorted(rendered.items(), key=lambda kv: _order(c, kv[0], kv[1]["phase"])):
        ref, live = created.get(("Deployment",) + key), c.kube.get("Deployment", key[1], key[0])
        if not ref or live is None or live["metadata"]["uid"] != ref["uid"]:
            problems.append(f"Deployment {key[0]}/{key[1]} was not created by this run or was replaced")
            continue
        ctr = r["object"]["spec"]["template"]["spec"]["containers"][r["index"]]
        m = {"member": len(members) + 1, "namespace": key[0], "name": key[1], "uid": ref["uid"],
             "container": ctr["name"], "index": r["index"]}
        _rss, pods = _member_pods(c, live)
        m["nodes"] = sorted({p["spec"].get("nodeName") for p in pods if p["spec"].get("nodeName")})
        at_from = not spec_diff(_spec_with(r["object"], r["index"], c.params["image"]), live["spec"])
        at_to = not spec_diff(_spec_with(r["object"], r["index"], new["image"]), live["spec"])
        m["start"] = "from" if at_from else "target" if (kind == "reverse" and at_to) else "divergent"
        if m["start"] == "divergent":
            problems.append(f"{key[1]}: the live spec is neither render (it differs from the current params' render "
                            f"in {spec_diff(_spec_with(r['object'], r['index'], c.params['image']), live['spec'])[:4]})")
        elif not supersedes:
            problems += rollout_problems(c, m, c.params["image"], m["nodes"], fresh=False)[0]
        members.append(m)
    if not members:
        problems.append("no rendered Deployment runs the fixture image")
    binding = {"run_id": record.run_id, "cluster_uid": record.cluster_uid, "kind": kind,
               "record_path": os.path.realpath(record.path), "old_record_sha256": record.sha256,
               "from_params": {"path": record.doc["params"], "sha256": X.sha256_file(old_path)},
               "to_params": {"path": new_params, "sha256": X.sha256_file(new_path)},
               "from_image": c.params["image"], "to_image": new["image"], "members": members,
               **({"supersedes": supersedes} if supersedes else {})}
    nonce = nonce or secrets.token_hex(16)
    binding_sha = C.digest(binding)
    tid = f"ir-{len(ts) + 1}-{hashlib.sha256((binding_sha + nonce).encode()).hexdigest()[:12]}"
    if tid in ts:
        problems.append(f"transition id {tid} already appears in the journal")
    candidate = dict(_raw_record(record), params=new_params,
                     image_transition={"id": tid, "binding_sha256": binding_sha, **binding})
    return X.new_plan(HELPER, record, [{"journal": "transition-prepared"},
                                       {"publish": record.path, "record_sha256": C.digest(candidate)},
                                       {"journal": "transition-open"}],
                      command="open", id=tid, nonce=nonce, binding=binding, binding_sha256=binding_sha,
                      candidate=candidate, problems=problems)


def _publish(path: str, tid: str, doc: Dict) -> None:
    """Replace the canonical record atomically; the bytes it replaces are kept beside it, written once."""
    with open(path, "rb") as fh:
        C.write_once(f"{path}.before-{tid}", fh.read())
    tmp = f"{path}.candidate-{tid}"
    with open(tmp, "w") as fh:
        fh.write(json.dumps(doc, indent=1, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    fd = os.open(os.path.dirname(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def open_(c: Ctx, reviewed: Dict, reviewed_digest: str, publish: Callable[[str, str, Dict], None] = _publish) -> Dict:
    b = reviewed.get("binding") or {}
    live = plan_open(c, (b.get("to_params") or {}).get("path", ""), reviewed.get("nonce"), b.get("supersedes"))
    X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
    if live["problems"]:
        raise X.StagingError("; ".join(live["problems"]))
    tid, cand = live["id"], live["candidate"]
    ctx = {"transition": tid, **({"supersedes": b["supersedes"]} if b.get("supersedes") else {})}
    with X.mutating(c.record, c.kube, HELPER, ctx):
        X.receipt(c.record, HELPER, "transition-prepared", {"id": tid, "binding_sha256": live["binding_sha256"],
                                                            **live["binding"], "new_record_sha256": C.digest(cand)})
        publish(c.record.path, tid, cand)
        return X.receipt(c.record, HELPER, "transition-open", {"id": tid, "record_sha256": C.digest(cand)})


def plan_open_recover(c: Ctx) -> Dict:
    t, problems, actions = _transition(c.record), [], []
    if not t or t["state"] != "prepared":
        problems.append("no prepared image transition waits for its record")
    elif os.path.realpath(c.record.path) != t["data"]["record_path"]:
        problems.append(f"the loaded record is not the transition's canonical record {t['data']['record_path']}: "
                        "an archived copy decides nothing")
    else:
        d = t["data"]
        canonical = C.digest(C.load_record(d["record_path"]))          # the bound path, whatever was loaded
        touched = [k for m in d["members"] for k, v in _ops(c.record, t["id"], m["member"]).items() if v]
        if canonical == d["new_record_sha256"]:
            actions.append({"journal": "transition-open", "id": t["id"]})
        elif canonical == d["old_record_sha256"] and not touched:
            actions.append({"journal": "transition-abandoned", "id": t["id"]})
        else:
            problems.append("the canonical record is neither the transition's old nor its new record, or a member "
                            "was already touched: nothing is decided here")
    return X.new_plan(HELPER, c.record, actions, command="open-recover", problems=problems)


def open_recover(c: Ctx, reviewed: Dict, reviewed_digest: str) -> Dict:
    live = plan_open_recover(c)
    X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
    if live["problems"]:
        raise X.StagingError("; ".join(live["problems"]))
    a = live["actions"][0]
    with X.mutating(c.record, c.kube, HELPER, {"transition": a["id"]}):
        if X.digest(plan_open_recover(c)) != reviewed_digest:           # again, under the lock
            raise X.StagingError("the state changed before the lock was taken; re-plan")
        data = {"id": a["id"]}
        if a["journal"] == "transition-open":
            data["record_sha256"] = C.digest(_raw_record(c.record))
        return X.receipt(c.record, HELPER, a["journal"], data)


# --- roll ------------------------------------------------------------------------------------------------------------

def _open_transition(c: Ctx) -> Tuple[Optional[Dict], List[str]]:
    t = _transition(c.record)
    if not t or t["state"] != "open":
        return None, ["no image transition is open"]
    named = (c.record.doc.get("image_transition") or {}).get("id")
    opened = X.last(X.receipts(c.record, HELPER), "transition-open", id=t["id"])["data"]
    if os.path.realpath(c.record.path) != t["data"]["record_path"] or named != t["id"] or \
            C.digest(_raw_record(c.record)) != opened["record_sha256"]:
        return t, [f"the loaded record is not the one transition {t['id']} published"]
    return t, []


def status(c: Ctx) -> Dict:
    t = _transition(c.record)
    out = {"helper": HELPER, "action": "status", "fence": X.image_fence(c.record),
           "transition": {"id": t["id"], "state": t["state"]} if t else None, "members": []}
    if t and t["state"] == "open":
        out["members"] = [member_view(c, t, m) for m in t["data"]["members"]]
    return out


def plan_roll(c: Ctx) -> Dict:
    t, problems = _open_transition(c)
    actions = []
    if t and not problems:
        problems += safety_problems(c, t)
        blocked = False
        for m in t["data"]["members"]:
            v = member_view(c, t, m)
            do = {"rolled": "verify", "target": "none", "patched": "wait", "from": "patch"}.get(v["state"])
            if do is None:
                problems.append(f"{m['name']} is {v['state']}: {v['why']}")
                blocked = True
                continue
            if do in ("wait", "patch"):
                if blocked:
                    problems.append(f"{m['name']}: an earlier member blocks the order")
                problems += headroom_problems(c, m)
            if do == "patch":
                problems += rollout_problems(c, m, t["data"]["from_image"], m["nodes"], fresh=False)[0]
            if do == "none":
                problems += rollout_problems(c, m, t["data"]["to_image"], m["nodes"], fresh=False)[0]
            actions.append({"member": m["member"], "name": m["name"], "namespace": m["namespace"], "uid": m["uid"],
                            "do": do, "generation": v["generation"], "spec_sha256": v["spec_sha256"]})
    return X.new_plan(HELPER, c.record, actions, command="roll", id=t["id"] if t else None, problems=problems)


def _patch(c: Ctx, t: Dict, m: Dict, reviewed: Dict) -> Dict:
    """ONE read decides everything: that snapshot must be the reviewed one (generation and spec digest of the plan's
    action), in state `from` and fully rolled out; its resourceVersion is what the patch tests, so the API server
    refuses the request if anything changed after this read."""
    tid, d = t["id"], t["data"]
    live = c.kube.get("Deployment", m["name"], m["namespace"])
    v = member_view(c, t, m, live=live)
    if v["state"] != "from" or v["generation"] != reviewed["generation"] or v["spec_sha256"] != reviewed["spec_sha256"]:
        raise X.StagingError(f"{m['name']} is not as reviewed (state {v['state']}, generation {v.get('generation')}, "
                             f"{v['why'][:3]}); nothing was sent")
    unready = rollout_problems(c, m, d["from_image"], m["nodes"], fresh=False, dep=live)[0]
    if unready:
        raise X.StagingError(f"{m['name']} is not fully rolled out on its current image ({unready[:3]}); nothing "
                             "was sent")
    expected = copy.deepcopy(live["spec"])
    ctr = expected["template"]["spec"]["containers"][m["index"]]
    if ctr["name"] != m["container"] or ctr["image"] != d["from_image"]:
        raise X.StagingError(f"{m['name']} is not as planned; nothing was sent")
    ctr["image"] = d["to_image"]
    rv, base = live["metadata"]["resourceVersion"], f"/spec/template/spec/containers/{m['index']}"
    pre = {"transition": tid, "member": m["member"], "resourceVersion": rv, "generation": live["metadata"]["generation"],
           "container": m["container"], "index": m["index"], "from_image": d["from_image"], "to_image": d["to_image"],
           "spec_sha256": C.digest(live["spec"]), "expected_spec_sha256": C.digest(expected)}

    def build(mark: str) -> List[Dict]:
        return [{"op": "test", "path": "/metadata/uid", "value": m["uid"]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": rv},
                {"op": "test", "path": base + "/name", "value": m["container"]},
                {"op": "test", "path": base + "/image", "value": d["from_image"]},
                {"op": "add", "path": "/metadata/annotations/" + X.json_pointer(ROLL_ANNOTATION),
                 "value": operation_id(tid, m["member"], mark)},
                {"op": "replace", "path": base + "/image", "value": d["to_image"]}]

    def validate(out: Dict, mark: str) -> None:
        om = out.get("metadata") or {}
        if om.get("uid") != m["uid"] or om.get("resourceVersion") in (None, rv) or \
                (om.get("annotations") or {}).get(ROLL_ANNOTATION) != operation_id(tid, m["member"], mark) or \
                C.digest(out.get("spec")) != pre["expected_spec_sha256"]:
            raise X.StagingError(f"{m['name']}: the patch's answer does not show the expected post-state")
    return c.kube.json_patch(PATCH_OP, "Deployment", m["name"], m["namespace"], m["uid"], build, validate, pre)


def _wait(c: Ctx, t: Dict, m: Dict) -> Dict:
    obj = _rendered(c.params)[(m["namespace"], m["name"])]["object"]
    limit = WAIT_TARGET_S if int(obj["spec"].get("replicas") or 1) > 1 else WAIT_SINGLE_S
    deadline = c.now() + limit
    op = _ops(c.record, t["id"], m["member"])["applied"][-1]["data"]          # the patch this rollout belongs to
    while True:
        dep = c.kube.get("Deployment", m["name"], m["namespace"])
        if dep is None or dep["metadata"]["uid"] != m["uid"] or \
                C.digest(dep["spec"]) != op["pre"]["expected_spec_sha256"] or \
                (dep["metadata"].get("annotations") or {}).get(ROLL_ANNOTATION) != \
                operation_id(t["id"], m["member"], op["mark"]):
            raise X.StagingError(f"{m['name']}: the object is no longer this patch's post-state; stopped")
        probs, pods = rollout_problems(c, m, t["data"]["to_image"], m["nodes"], fresh=True, dep=dep)
        if not probs:
            return X.receipt(c.record, HELPER, "member-rolled", {"id": t["id"], "member": m["member"],
                                                                 "name": m["name"], "pods": pods})
        if c.now() >= deadline:
            raise X.StagingError(f"{m['name']}: the rollout did not verify within {limit} s ({probs[:4]}); stopped: "
                                 "no retry, no rollback; the fence stays")
        c.sleep(POLL_S)


def _earlier_problems(c: Ctx, t: Dict, m: Dict) -> List[str]:
    """Every member before m must still be rolled by this transition (its pods re-verified against its receipt), or,
    in a reverse, untouched and fully rolled out on the target image."""
    out = []
    for e in t["data"]["members"]:
        if e["member"] >= m["member"]:
            break
        v = member_view(c, t, e)
        if v["state"] == "target":
            out += rollout_problems(c, e, t["data"]["to_image"], e["nodes"], fresh=False)[0]
        elif v["state"] != "rolled":
            out.append(f"the earlier member {e['name']} is {v['state']}: {v['why'][:3]}")
    return out


def roll(c: Ctx, reviewed: Dict, reviewed_digest: str) -> Dict:
    live = plan_roll(c)
    X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
    if live["problems"]:
        raise X.StagingError("; ".join(live["problems"]))
    t, _ = _open_transition(c)
    members = {m["member"]: m for m in t["data"]["members"]}
    with X.mutating(c.record, c.kube, HELPER, {"transition": t["id"]}):
        for a in live["actions"]:
            m = members[a["member"]]
            if a["do"] in ("verify", "none"):
                continue
            # before EVERY patch, under the lock: the safety conditions, the room, and every earlier member still
            # where this transition left it
            again = safety_problems(c, t) + headroom_problems(c, m) + _earlier_problems(c, t, m)
            if again:
                raise X.StagingError(f"{m['name']}: not safe to continue ({again[:4]}); stopped")
            if a["do"] == "patch":
                _patch(c, t, m, a)
            elif member_view(c, t, m)["state"] != "patched":
                raise X.StagingError(f"{m['name']} is no longer waiting for its rollout; stopped")
            _wait(c, t, m)
    return {"helper": HELPER, "action": "roll", "id": t["id"], "members": status(c)["members"]}


# --- the fencing write -----------------------------------------------------------------------------------------------

def _lock_sha(record: X.Record) -> str:
    p = X.lock_file(record)
    return C.sha256_file(p) if os.path.exists(p) else ""


def plan_void(c: Ctx) -> Dict:
    """Read-only: every patch of the open transition whose outcome is unknown while the object still shows the old
    image, no annotation of that operation and the ORIGINAL resourceVersion (a delayed request could still commit)."""
    t, problems = _open_transition(c)
    actions = []
    for m in (t["data"]["members"] if t and not problems else []):
        for it in _ops(c.record, t["id"], m["member"])["open"]:
            d, live = it["data"], c.kube.get("Deployment", m["name"], m["namespace"])
            if reconcile_patch(c.kube, d, c.record).get("fence") != "possible":
                problems.append(f"{m['name']} step {it['step']}: not a case for the fencing write (recover-plan "
                                "says what it is)")
                continue
            if _ops(c.record, t["id"], m["member"], VOID_OP)["open"]:
                problems.append(f"{m['name']}: a fencing write is already unresolved")
                continue
            actions.append({"void": it["step"], "member": m["member"], "name": m["name"], "namespace": m["namespace"],
                            "uid": m["uid"], "operation": operation_id(t["id"], m["member"], d["mark"]),
                            "resourceVersion": live["metadata"]["resourceVersion"]})
    if not actions and not problems:
        problems.append("no patch waits for a fencing write")
    return X.new_plan(HELPER, c.record, actions, command="void", id=t["id"] if t else None,
                      lock_sha256=_lock_sha(c.record), problems=problems)


def void(c: Ctx, reviewed: Dict, reviewed_digest: str) -> Dict:
    live = plan_void(c)
    X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
    if live["problems"]:
        raise X.StagingError("; ".join(live["problems"]))
    done = []
    with C.recovery_guard(c.record.doc, X.LOCK_SCOPE, live["lock_sha256"]):   # the previous executor is proven gone
        holder = type("Held", (), {"safe": True})()            # the stale lock stays in place; recover releases it
        c.kube.journal = X.Journal(c.record, HELPER, holder, {"transition": live["id"]})
        try:
            for a in live["actions"]:
                rv, op = a["resourceVersion"], a["operation"]

                def build(mark, a=a, rv=rv, op=op):
                    return [{"op": "test", "path": "/metadata/uid", "value": a["uid"]},
                            {"op": "test", "path": "/metadata/resourceVersion", "value": rv},
                            {"op": "add", "path": "/metadata/annotations/" + X.json_pointer(VOID_ANNOTATION),
                             "value": op}]

                def validate(out, mark, a=a, rv=rv, op=op):
                    om = out.get("metadata") or {}
                    if om.get("uid") != a["uid"] or om.get("resourceVersion") in (None, rv) or \
                            (om.get("annotations") or {}).get(VOID_ANNOTATION) != op:
                        raise X.StagingError(f"{a['name']}: the fencing write's answer does not prove it")
                r = c.kube.json_patch(VOID_OP, "Deployment", a["name"], a["namespace"], a["uid"], build, validate,
                                      {"transition": live["id"], "member": a["member"], "operation": op,
                                       "original_step": a["void"], "resourceVersion": rv})
                done.append({"member": a["member"], "step": r["step"], "operation": op})
        finally:
            c.kube.journal = None
    return {"helper": HELPER, "action": "void", "id": live["id"], "voided": done,
            "next": "recover-plan / recover reconciles the original patch as not-applied and releases the lock"}


def _void_evidence(record: X.Record, op: str, live_ann: Dict) -> bool:
    """The void marker of exactly this operation is live AND this journal holds the fencing write that put it there."""
    if live_ann.get(VOID_ANNOTATION) != op:
        return False
    for s in X._steps(X.receipts(record, HELPER)).values():
        d = s["intent"]
        if d and d["op"] == VOID_OP and d["pre"].get("operation") == op and \
                (s["outcome"] or {}).get("result") != "not-applied":
            return True
    return False


def reconcile_patch(kube: X.Kube, d: Dict, record) -> Dict:
    """done: the same object carries THIS operation's annotation and exactly the expected spec. not-done: a journalled
    fencing write for this operation is live (the resourceVersion the patch tested is gone for good). The old image
    with the original resourceVersion stays unresolved: the request can still commit ("fence": "possible")."""
    pre, live = d["pre"], kube.get("Deployment", d["name"], d["namespace"])
    if live is None or live["metadata"].get("uid") != d["uid"]:
        return {"observed": "unresolved", "why": "the Deployment is gone or replaced"}
    op = operation_id(pre["transition"], pre["member"], d["mark"])
    ann, rv = live["metadata"].get("annotations") or {}, live["metadata"].get("resourceVersion")
    if ann.get(ROLL_ANNOTATION) == op:
        if C.digest(live["spec"]) == pre["expected_spec_sha256"]:
            return {"observed": "done", "uid": d["uid"]}
        return {"observed": "unresolved", "why": "the operation's annotation is live but the spec is not the expected "
                                                 "post-state"}
    if rv != pre["resourceVersion"] and _void_evidence(record, op, ann):
        return {"observed": "not-done", "state": "not-applied", "uid": d["uid"],
                "why": "fenced: the tested resourceVersion was replaced by this operation's void write"}
    if rv == pre["resourceVersion"] and C.digest(live["spec"]) == pre["spec_sha256"]:
        return {"observed": "unresolved", "fence": "possible",
                "why": "unchanged so far, but the request can still commit: plan-void / void, then recover"}
    return {"observed": "unresolved", "why": "changed since the intent, neither by this patch nor by its void write"}


def reconcile_void(kube: X.Kube, d: Dict, record) -> Dict:
    pre, live = d["pre"], kube.get("Deployment", d["name"], d["namespace"])
    if live is None or live["metadata"].get("uid") != d["uid"]:
        return {"observed": "unresolved", "why": "the Deployment is gone or replaced"}
    ann, rv = live["metadata"].get("annotations") or {}, live["metadata"].get("resourceVersion")
    if ann.get(VOID_ANNOTATION) == pre["operation"] and rv != pre["resourceVersion"]:
        return {"observed": "done", "uid": d["uid"]}
    if ann.get(ROLL_ANNOTATION) == pre["operation"]:
        return {"observed": "not-done", "state": "not-applied", "uid": d["uid"],
                "why": "the original patch committed first: the fencing write's test failed"}
    return {"observed": "unresolved", "why": "neither the void marker nor the original operation is live yet"}


X.RECONCILERS[PATCH_OP] = reconcile_patch
X.RECONCILERS[VOID_OP] = reconcile_void


# --- completion ------------------------------------------------------------------------------------------------------

def zero_counter_evidence(c: Ctx, t: Dict, fetch: Fetch, api: str) -> Tuple[Dict, List[str]]:
    """What the telemetry gate does not show: the REPLACEMENT processes expose their counters at exactly zero before
    any traffic. For every target, from RAW samples of the last ZERO_LOOKBACK_S:
      ingress-arrivals   c28_ingress_requests_total: exactly one series, scraped from the new ingress pod;
      ingress-dispatch   c28_ingress_dispatched_total: exactly one series per discovered endpoint (endpoint_pod) =
                         exactly the replacement pods of that target, scraped from the new ingress pod;
      fixture-requests   c28_requests_total: exactly one series with code="200" per replacement pod of that target,
                         and no series with any other code (another code means a request was already answered).
    Every series needs at least ZERO_MIN_SAMPLES samples, the newest at most ZERO_MAX_AGE_S old, no gap longer than
    that, none in the future, and every value exactly 0. The evidence keeps the selectors, every sample with its
    timestamp and the replacement pods' names and UIDs from the member-rolled receipts."""
    import bounded
    now, problems, checks = c.now(), [], []
    rolled = {m["name"]: _rolled_receipt(c.record, t["id"], m["member"]) for m in t["data"]["members"]}
    names = c.c28.demo.target_names(c.params)
    ingress = [m["name"] for m in t["data"]["members"] if m["namespace"] == c.ns and m["name"] not in names]
    if any(r is None for r in rolled.values()) or len(ingress) != 1:
        return {}, ["not every member was rolled by this transition: no zero-counter evidence is possible"]
    ingress_pods = {p["name"] for p in rolled[ingress[0]]["pods"]}

    def judge(where: str, vals: List) -> None:
        try:
            ts, vs = [float(x[0]) for x in vals], [float(x[1]) for x in vals]
        except (TypeError, ValueError, IndexError):
            problems.append(f"{where}: samples that cannot be read")
            return
        gaps = [b - a for a, b in zip(ts, ts[1:])]
        if len(ts) < ZERO_MIN_SAMPLES or ts != sorted(ts) or now - ts[-1] > ZERO_MAX_AGE_S or \
                ts[-1] - now > ZERO_FUTURE_S or (gaps and max(gaps) > ZERO_MAX_AGE_S):
            problems.append(f"{where}: {len(ts)} samples, the newest {now - ts[-1] if ts else None} s old: not at "
                            f"least {ZERO_MIN_SAMPLES} fresh samples without a gap")
        if not vs or any(v != 0.0 for v in vs):                    # NaN compares unequal: never zero
            problems.append(f"{where}: not exactly zero in every sample")

    def read(label: str, metric: str, target: str, by: str, want: set, scraped: set, code: bool = False) -> None:
        sel = bounded.target_selector(metric, c.ns, target)
        got: Dict[str, int] = {}
        kept = []
        for s in bounded._raw(fetch, api, sel, now - ZERO_LOOKBACK_S, now):
            lab, vals = s.get("metric") or {}, s.get("values") or []
            ident = str(lab.get(by, ""))
            kept.append({"labels": {k: lab[k] for k in sorted(lab)}, "samples": vals})
            if lab.get("pod") not in scraped:
                problems.append(f"{label}/{target}: a series of pod {lab.get('pod')}, which is not a replacement pod")
            if code and lab.get("code") != "200":
                problems.append(f"{label}/{target} {ident}: a series with code {lab.get('code')!r}: a request was "
                                "already answered, or the counter is not the one created at start")
                continue
            got[ident] = got.get(ident, 0) + 1
            judge(f"{label}/{target} {ident}", vals)
        if set(got) != want or any(n != 1 for n in got.values()):
            problems.append(f"{label}/{target}: series for {sorted(got)} (x{sorted(set(got.values()))}), expected "
                            f"exactly one each for {sorted(want)}")
        checks.append({"check": f"{label}/{target}", "selector": sel, "series": kept})
    for tname in names:
        pods = {p["name"] for p in rolled[tname]["pods"]}
        read("ingress-arrivals", "c28_ingress_requests_total", tname, "pod", ingress_pods, ingress_pods)
        read("ingress-dispatch", "c28_ingress_dispatched_total", tname, "endpoint_pod", pods, ingress_pods)
        read("fixture-requests", "c28_requests_total", tname, "pod", pods, pods, code=True)
    return {"schema": "c28-image-roll-zero-counters/v2", "id": t["id"], "evaluated_at_s": now,
            "lookback_s": ZERO_LOOKBACK_S, "min_samples": ZERO_MIN_SAMPLES, "max_age_s": ZERO_MAX_AGE_S,
            "replacements": {name: [{"name": p["name"], "uid": p["uid"]} for p in r["pods"]]
                             for name, r in sorted(rolled.items())},
            "checks": checks, "series": sum(len(k["series"]) for k in checks)}, problems


REVERSE_LIMIT = ("reverse completion: the old binary creates its counters at the first request, so this installation "
                 "has none; it is not admissible until a forward roll or a separately reviewed remedy (stage A "
                 "Amendment 2, gap B)")


def _completion(c: Ctx, fetch: Optional[Fetch], api: Optional[str]) -> Tuple[Optional[Dict], Dict, List[str], Dict]:
    import bounded
    t, problems = _open_transition(c)
    if not t or problems:
        return t, {}, problems, {}
    reverse = t["data"]["kind"] == "reverse"
    for m in t["data"]["members"]:
        v = member_view(c, t, m)
        if v["state"] == "target":
            problems += rollout_problems(c, m, t["data"]["to_image"], m["nodes"], fresh=False)[0]
        elif v["state"] != "rolled":
            problems.append(f"{m['name']} is {v['state']}: {v['why']}")
    problems += safety_problems(c, t)
    evidence: Dict = {}
    if not reverse:
        if fetch is None or not api:
            problems.append("a forward completion needs --query-api (the telemetry gate and the zero-counter evidence)")
        elif not problems:
            bc = bounded.Ctx(c.record, c.kube)
            problems += bounded.existing_telemetry(bc, fetch, api, bounded.telemetry_requirements(bc))
            evidence, zp = zero_counter_evidence(c, t, fetch, api)
            problems += zp
    kind = {"kind": "reverse", "limitation": REVERSE_LIMIT} if reverse else {"kind": "forward"}
    return t, kind, problems, evidence


def plan_complete(c: Ctx, fetch: Optional[Fetch] = None, api: Optional[str] = None) -> Dict:
    t, kind, problems, _evidence = _completion(c, fetch, api)
    return X.new_plan(HELPER, c.record, [{"journal": "transition-complete"}] if t else [], command="complete",
                      id=t["id"] if t else None, **kind, problems=problems)


def complete(c: Ctx, reviewed: Dict, reviewed_digest: str, fetch: Optional[Fetch] = None,
             api: Optional[str] = None) -> Dict:
    live = plan_complete(c, fetch, api)
    X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
    if live["problems"]:
        raise X.StagingError("; ".join(live["problems"]))
    with X.mutating(c.record, c.kube, HELPER, {"transition": live["id"]}):
        t, kind, problems, evidence = _completion(c, fetch, api)           # the final pass, under the lock
        if problems:
            raise X.StagingError("; ".join(problems))
        data = {"id": t["id"], **kind}
        if kind["kind"] == "forward":
            # every attempt keeps its own evidence file (an interrupted attempt's stays); the terminal receipt binds
            # the one that completed
            folder = os.path.join(c.record.evidence_dir, HELPER)
            prefix = f"zero-counters-{t['id']}-"
            n = 1 + len([f for f in (os.listdir(folder) if os.path.isdir(folder) else []) if f.startswith(prefix)])
            name = f"{prefix}{n:02d}.json"
            data["zero_counters"] = {"file": name, "sha256": C.write_json_once(os.path.join(folder, name), evidence)}
            X.receipt(c.record, HELPER, "zero-counters", {"id": t["id"], "attempt": n, **data["zero_counters"],
                                                          "series": evidence["series"]})
        return X.receipt(c.record, HELPER, "transition-complete", data)


# --- CLI -------------------------------------------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None, api=None, fetch: Optional[Fetch] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--record", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("status", "plan-open-recover", "plan-roll", "plan-void", "recover-plan"):
        sub.add_parser(name)
    po = sub.add_parser("plan-open")
    po.add_argument("--new-params", required=True, help="the new params file, as the record will name it")
    po.add_argument("--supersedes", default=None, help="the open transition this one reverses")
    pc = sub.add_parser("plan-complete")
    pc.add_argument("--query-api", default=None, help="the observer's Prometheus API base (a local forward)")
    for name in ("open", "open-recover", "roll", "void", "complete", "recover"):
        p = sub.add_parser(name)
        p.add_argument("--plan", required=True)
        p.add_argument("--plan-digest", required=True)
        if name == "complete":
            p.add_argument("--query-api", default=None)
    a = ap.parse_args(argv)
    record = X.load_record(a.record, () if a.cmd.startswith("recover") else ("prepare",))
    kube = record.kube(api)
    if a.cmd == "recover-plan":
        X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
        return 0
    if a.cmd == "recover":
        X.emit(X.recover(record, kube, X.load_plan(a.plan), a.plan_digest, image_transition=X.image_fence(record)))
        return 0
    c = Ctx(record, kube)
    if fetch is None:
        import bounded
        fetch = bounded.http_json
    if a.cmd == "status":
        X.emit(status(c))
    elif a.cmd == "plan-open":
        X.emit(X.cli_plan(record, HELPER, plan_open(c, a.new_params, supersedes=a.supersedes), "plan-open"))
    elif a.cmd == "open":
        X.emit(open_(c, X.load_plan(a.plan), a.plan_digest))
    elif a.cmd == "plan-open-recover":
        X.emit(X.cli_plan(record, HELPER, plan_open_recover(c), "plan-open-recover"))
    elif a.cmd == "open-recover":
        X.emit(open_recover(c, X.load_plan(a.plan), a.plan_digest))
    elif a.cmd == "plan-roll":
        X.emit(X.cli_plan(record, HELPER, plan_roll(c), "plan-roll"))
    elif a.cmd == "roll":
        X.emit(roll(c, X.load_plan(a.plan), a.plan_digest))
    elif a.cmd == "plan-void":
        X.emit(X.cli_plan(record, HELPER, plan_void(c), "plan-void"))
    elif a.cmd == "void":
        X.emit(void(c, X.load_plan(a.plan), a.plan_digest))
    elif a.cmd == "plan-complete":
        X.emit(X.cli_plan(record, HELPER, plan_complete(c, fetch, a.query_api), "plan-complete"))
    else:
        X.emit(complete(c, X.load_plan(a.plan), a.plan_digest, fetch, a.query_api))
    return 0


if __name__ == "__main__":
    sys.exit(main())
