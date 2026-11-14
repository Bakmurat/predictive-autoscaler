"""Part-2 adapter over common_records.py for the staging helpers reserve, prepare, freeze, session and build_images.

STAGING-C28-PROD.md, "Helpers to implement". Everything shared comes from common_records (part 1, never edited here):
the c28-preparation/v1 record (load_record), the injectable raw API (KubectlApi: get/replace/create/delete with UID
preconditions, require_cluster before every mutation), plans (make_plan, plan_digest, require_reviewed, load_plan),
write-once evidence (write_json_once), the hash-chained receipts (append_receipt, read_receipts) and sanitizing.
legacy_allowlist names the legacy API group. This module only adds what part 2 needs on top:

  * Record: the loaded record with evidence_dir made absolute and the helper's own file paths resolved against the
    record's directory.
  * Kube: typed access by kind over the raw API (the extra kinds part 2 touches: ConfigMaps, ResourceQuotas,
    ReplicaSets, PriorityClasses, StorageClasses, NetworkPolicies, scrape objects, the ingress's ServiceAccount, Role and
    RoleBinding), with client-side filtering only
    (nothing is selected server-side by label or name substring), update = read, mutate a copy, then replace with the
    read resourceVersion and UID (optimistic concurrency; a replaced or concurrently edited object is refused), and
    require_cluster before every create/replace/delete. Secrets are never read.
  * Reviewed plans: `plan` writes the plan once (evidence/<helper>/<name>-<digest12>.json) and prints its digest; a
    mutation takes --plan <file> --plan-digest <digest>, checks it with require_reviewed, and ALSO recomputes the plan
    from the live state: if the live plan's digest differs, the state changed since review and nothing is done.
  * writer_inventory: every replica writer of the fixture targets, as FULL identities (kind, namespace, name, UID),
    across HPA, KEDA (an HPA collapses into its ScaledObject only when the live ScaledObject's UID matches) and BOTH
    PredictiveAutoscaler API groups, with optional APIs skipped only when discovery shows them absent.
  * Durable intents on common_records' protocol: mutating() holds the ONE shared lock of the evaluation scope
    (exclusive_lock(rec, "evaluation") in the record's lock_dir) for reserve, prepare, session and calibrate; every
    create, replace and delete goes through mutation_step (intent-<n> with the validated pre-state BEFORE the call, step-<n>
    after); an unknown outcome keeps the lock; recover() takes recovery_guard (owner proven terminated), writes
    reconciled-<n> for what it can decide from the live objects, and releases the stale lock only when nothing stays
    unresolved. What a helper owns is derived from this journal.
  * Helper-defined steps: Kube.step journals an operation the helper composes itself (session.py's restore: a
    Kube.scale write through /scale with the read resourceVersion and UID, then the wait for convergence); any failure
    inside it stays unresolved, and recover decides it with the reconciler the helper registered in RECONCILERS. An op
    without a loaded reconciler is never guessed.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
import os
import re
import subprocess
import sys
from typing import Callable, Dict, Iterable, List, Optional

import common_records as C
import legacy_allowlist

StagingError = C.StagingError
RUN_LABEL = "c28.devkuban.com/run"
SESSION_LABEL = "c28.devkuban.com/session"
COMPONENT_LABEL = "app.kubernetes.io/component"
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")
_DNS = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

# Kinds part 2 touches beyond common_records.KINDS: key -> (API prefix, plural, namespaced).
EXTRA_KINDS = {
    "configmaps": ("/api/v1", "configmaps", True),
    "resourcequotas": ("/api/v1", "resourcequotas", True),
    "replicasets": ("/apis/apps/v1", "replicasets", True),
    "priorityclasses": ("/apis/scheduling.k8s.io/v1", "priorityclasses", False),
    "storageclasses": ("/apis/storage.k8s.io/v1", "storageclasses", False),
    "networkpolicies": ("/apis/networking.k8s.io/v1", "networkpolicies", True),
    "endpointslices": ("/apis/discovery.k8s.io/v1", "endpointslices", True),
    "podmonitors": ("/apis/monitoring.coreos.com/v1", "podmonitors", True),
    "vmpodscrapes": ("/apis/operator.victoriametrics.com/v1beta1", "vmpodscrapes", True),
    "volumeattachments": ("/apis/storage.k8s.io/v1", "volumeattachments", False),
    # kubernetes-cronhpa-controller's scheduled replica writer, and the Vertical Pod Autoscaler (a resource writer)
    "cronhpas": ("/apis/autoscaling.alibabacloud.com/v1beta1", "cronhorizontalpodautoscalers", True),
    "vpas": ("/apis/autoscaling.k8s.io/v1", "verticalpodautoscalers", True),
    # the ingress's namespaced identity (endpoints-rr routing: get/list/watch EndpointSlices only)
    "serviceaccounts": ("/api/v1", "serviceaccounts", True),
    "roles": ("/apis/rbac.authorization.k8s.io/v1", "roles", True),
    "rolebindings": ("/apis/rbac.authorization.k8s.io/v1", "rolebindings", True),
}
# Object kind -> table key.
KIND_KEY = {
    "Namespace": "namespaces", "Node": "nodes", "Pod": "pods", "Service": "services",
    "PersistentVolumeClaim": "persistentvolumeclaims", "PersistentVolume": "persistentvolumes",
    "Deployment": "deployments", "StatefulSet": "statefulsets", "DaemonSet": "daemonsets", "Job": "jobs",
    "CronJob": "cronjobs",
    "HorizontalPodAutoscaler": "horizontalpodautoscalers", "ScaledObject": "scaledobjects",
    "PredictiveAutoscaler": "pa-new", "LegacyPredictiveAutoscaler": "pa-legacy",
    "ConfigMap": "configmaps", "ResourceQuota": "resourcequotas", "ReplicaSet": "replicasets",
    "PriorityClass": "priorityclasses", "StorageClass": "storageclasses", "NetworkPolicy": "networkpolicies",
    "PodMonitor": "podmonitors", "VMPodScrape": "vmpodscrapes", "VolumeAttachment": "volumeattachments",
    "CronHorizontalPodAutoscaler": "cronhpas", "VerticalPodAutoscaler": "vpas",
    "ServiceAccount": "serviceaccounts", "Role": "roles", "RoleBinding": "rolebindings",
    "EndpointSlice": "endpointslices",
}
if C.KINDS["pa-legacy"][0] != f"/apis/{legacy_allowlist.LEGACY_GROUP}/v1alpha1":     # pragma: no cover
    raise ImportError("common_records and legacy_allowlist disagree on the legacy API group")


def validate_discovery(doc) -> Dict[str, List[str]]:
    """{group: [versions]} (plus the core group) from a raw /apis document, refusing anything but a well-formed
    APIGroupList: a JSON object (kind APIGroupList when a kind is given) whose groups is a list; every group an object
    with a non-empty string name, no name twice, and versions a non-empty list of objects each with a non-empty string
    version or groupVersion."""
    bad = lambda why: C.ApiError(f"API discovery (/apis) returned a malformed document: {why}")  # noqa: E731
    if not isinstance(doc, dict):
        raise bad("not a JSON object (or no answer)")
    if "kind" in doc and doc["kind"] != "APIGroupList":
        raise bad(f"kind {doc['kind']!r} is not APIGroupList")
    groups = doc.get("groups")
    if not isinstance(groups, list):
        raise bad("groups is not a list")
    out: Dict[str, List[str]] = {"": ["v1"]}
    for g in groups:
        if not isinstance(g, dict) or not isinstance(g.get("name"), str) or not g["name"]:
            raise bad("a group without a non-empty string name")
        if g["name"] in out:
            raise bad(f"group {g['name']!r} listed twice")
        versions = g.get("versions")
        if not isinstance(versions, list) or not versions:
            raise bad(f"group {g['name']!r} has no versions list")
        vs = []
        for v in versions:
            name = v.get("version") if isinstance(v, dict) else None
            gv = v.get("groupVersion") if isinstance(v, dict) else None
            if not ((isinstance(name, str) and name) or (isinstance(gv, str) and gv)) or \
                    (name is not None and not (isinstance(name, str) and name)) or \
                    (gv is not None and not (isinstance(gv, str) and gv)):
                raise bad(f"group {g['name']!r} has a version without a non-empty version/groupVersion")
            vs.append(name or gv.rsplit("/", 1)[-1])
        out[g["name"]] = sorted(vs)
    return out


def table(key: str):
    return C.KINDS.get(key) or EXTRA_KINDS[key]


def path(kind: str, namespace: Optional[str] = None, name: Optional[str] = None) -> str:
    if kind == "Secret":
        raise StagingError("the staging helpers never read Secrets")
    if kind not in KIND_KEY:
        raise StagingError(f"unsupported kind {kind}")
    prefix, plural, namespaced = table(KIND_KEY[kind])
    if namespaced and name and not namespace:
        raise StagingError(f"{kind}/{name} needs a namespace")
    p = f"{prefix}/namespaces/{namespace}/{plural}" if namespaced and namespace else f"{prefix}/{plural}"
    return p + (f"/{name}" if name else "")


def group_of(kind: str) -> str:
    prefix = table(KIND_KEY[kind])[0]
    return "" if prefix == "/api/v1" else prefix.split("/")[2]


# --- the record -----------------------------------------------------------------------------------------------------

class Record:
    """The loaded preparation record (common_records.load_record); evidence_dir absolute, helper paths resolvable."""

    def __init__(self, path_: str, doc: Dict):
        self.path = os.path.abspath(path_)
        self.dir = os.path.dirname(self.path)
        self.doc = copy.deepcopy(doc)
        self.doc["evidence_dir"] = self.resolve(doc["evidence_dir"])
        self.sha256 = C.digest(doc)

    run_id = property(lambda self: self.doc["run_id"])
    cluster_uid = property(lambda self: self.doc["cluster_uid"])
    evidence_dir = property(lambda self: self.doc["evidence_dir"])
    commit = property(lambda self: self.doc["source"]["commit"])

    def resolve(self, p: str) -> str:
        p = os.path.expanduser(str(p))
        return p if os.path.isabs(p) else os.path.normpath(os.path.join(self.dir, p))

    def section(self, name: str) -> Dict:
        s = self.doc.get(name)
        if not isinstance(s, dict) or not s:
            raise StagingError(f"the record has no {name!r} section")
        return s

    def kube(self, api=None, run: Callable = subprocess.run) -> "Kube":
        return Kube(api if api is not None else C.api_for(self.doc, run), self.cluster_uid)


def load_record(path_: str, sections: Iterable[str] = ()) -> Record:
    return Record(path_, C.load_record(path_, tuple(sections)))


# --- the shared mutation lock and durable intents (common_records' protocol) ----------------------------------------

LOCK_SCOPE = "evaluation"                 # ONE lock (common_records.exclusive_lock) for every part-2 helper's mutations
INTENT_ANNOTATION = "c28.devkuban.com/intent"
DEFINITIVE = (400, 401, 403, 404, 409, 422, 429)   # the API refused: nothing was applied
PART2_HELPERS = ("reserve", "prepare", "session", "calibrate", "ledger_read", "bounded", "boundary")   # journals under the shared evaluation lock


def lock_file(record: "Record") -> str:
    return C.lock_path(record.doc, LOCK_SCOPE)


def _steps(entries: List[Dict]) -> Dict[str, Dict]:
    """{n: {"intent": data, "outcome": step data | None, "reconciled": data | None}} of a helper's receipts."""
    out: Dict[str, Dict] = {}
    for e in entries:
        ev = e["event"]
        for prefix, key in (("intent-", "intent"), ("step-", "outcome"), ("reconciled-", "reconciled")):
            if ev.startswith(prefix):
                out.setdefault(ev[len(prefix):], {"intent": None, "outcome": None, "reconciled": None})[key] = e["data"]
    return out


def applied_intents(entries: List[Dict]) -> List[Dict]:
    """Intents applied (step-<n> result applied, or reconciled-<n> observed done), in order, with data.result_uid."""
    res = []
    for n, s in sorted(_steps(entries).items(), key=lambda kv: int(kv[0])):
        if s["intent"] is None:
            continue
        done = (s["outcome"] or {}).get("result") == "applied" or (s["reconciled"] or {}).get("observed") == "done"
        if done:
            uid = ((s["outcome"] or {}).get("post") or {}).get("uid") or (s["reconciled"] or {}).get("uid") \
                or s["intent"].get("uid")
            gen = ((s["outcome"] or {}).get("post") or {}).get("generation")
            res.append({"step": n, "data": dict(s["intent"], result_uid=uid, result_generation=gen)})
    return res


def open_intents(entries: List[Dict]) -> List[Dict]:
    """Intents with neither an outcome nor a reconciliation: UNRESOLVED (they keep the lock)."""
    return [{"step": n, "data": s["intent"]} for n, s in sorted(_steps(entries).items(), key=lambda kv: int(kv[0]))
            if s["intent"] is not None and s["outcome"] is None and s["reconciled"] is None]


class Journal:
    """Every mutation through common_records.mutation_step: intent-<n> (operation, target, validated pre-state, the
    post-state it expects, the annotation a create carries) durable BEFORE the call, step-<n> after it with the
    post-state from the call's own response. A definitive API refusal is recorded as step-<n> result not-applied (the
    lock stays safe); any other failure leaves the intent unresolved and the lock held until `recover`."""

    def __init__(self, record: "Record", helper: str, lock, context: Optional[Dict] = None):
        self.record, self.helper, self.lock, self.context = record, helper, lock, dict(context or {})

    def run(self, op: str, kind: str, namespace: Optional[str], name: str, fn: Callable[[str], Dict], uid: str = "",
            pre: Optional[Dict] = None, expect: Optional[List] = None, body_sha256: str = "") -> Dict:
        entries = receipts(self.record, self.helper)
        n = str(max([int(k) for k in _steps(entries)] or [0]) + 1)
        mark = f"{self.record.run_id}/{self.helper}/{n}"
        intent = {"op": op, "kind": kind, "namespace": namespace, "name": name, "uid": uid, "pre": pre or {},
                  "expect": expect or [], "body_sha256": body_sha256, "mark": mark, "context": self.context}
        refused: List = []

        def call():
            try:
                out = fn(mark)
            except C.ApiError as e:
                if getattr(e, "status", None) in DEFINITIVE:
                    refused.append(e)
                    return "not-applied", {"status": e.status}
                raise
            m = (out or {}).get("metadata") or {}
            return "applied", {"uid": m.get("uid") or uid, "resourceVersion": m.get("resourceVersion"),
                               "generation": m.get("generation")}
        C.mutation_step(self.record.doc, self.helper, self.lock, n, intent, call)
        if refused:
            raise refused[0]
        return {"mark": mark, "step": n}


def pointer_get(obj, ptr: str):
    cur = obj
    for part in [p.replace("~1", "/").replace("~0", "~") for p in ptr.lstrip("/").split("/")]:
        if isinstance(cur, list):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return None
        elif isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
        if cur is None:
            return None
    return cur


def _expect_holds(obj: Dict, expect: List) -> bool:
    for ptr, value in expect:
        got = pointer_get(obj, ptr)
        if isinstance(value, dict) and "contains" in value:
            if value["contains"] not in (got or []):
                return False
        elif isinstance(value, dict) and "lacks" in value:
            if value["lacks"] in (got or []):
                return False
        elif got != value:
            return False
    return True


# Reconcilers of the journalled ops a helper defines itself (op -> fn(kube, intent data) -> observation); a helper
# registers its own at import (session.py: "restore"). An op without a loaded reconciler is never guessed: unresolved.
RECONCILERS: Dict[str, Callable[["Kube", Dict, Optional["Record"]], Dict]] = {}
# Helper-defined ops whose reconciler lives in a module another helper's process may not have loaded: recover (from any
# helper's CLI) imports it on demand, so an open intent of that op is never left "no reconciler loaded".
RECONCILER_MODULES: Dict[str, str] = {"ledger-reader-gone": "ledger_read", "bounded-pod-gone": "bounded",
                                       "startup-scale": "bounded"}


def reconcile(kube: "Kube", d: Dict, record: Optional["Record"] = None) -> Dict:
    """What an unresolved intent actually did, read live: {"observed": done | not-done | unresolved, ...}. A helper's
    reconciler also gets the record (its journal holds what the step persisted before the outcome was lost)."""
    if d["op"] not in RECONCILERS and d["op"] in RECONCILER_MODULES:     # a fresh process of another helper
        importlib.import_module(RECONCILER_MODULES[d["op"]])           # registers it on import
    if d["op"] in RECONCILERS:
        if record is None:
            return {"observed": "unresolved", "why": f"the {d['op']!r} reconciler needs the record"}
        return RECONCILERS[d["op"]](kube, d, record)
    if d["op"] not in ("create", "delete", "update"):
        return {"observed": "unresolved", "why": f"no reconciler for the {d['op']!r} intent is loaded (recover "
                                                 "through the helper that wrote it)"}
    live = kube.get(d["kind"], d["name"], d["namespace"])
    if d["op"] == "create":
        if live is None:
            return {"observed": "not-done", "state": "not-applied", "why": "absent"}
        if (live["metadata"].get("annotations") or {}).get(INTENT_ANNOTATION) == d["mark"]:
            return {"observed": "done", "uid": live["metadata"]["uid"]}
        return {"observed": "unresolved", "why": "an object of that name exists without this intent's annotation"}
    if d["op"] == "delete":
        if live is None or live["metadata"].get("uid") != d["uid"]:
            return {"observed": "done", "uid": d["uid"]}
        return {"observed": "not-done", "uid": d["uid"]}
    if live is None or live["metadata"].get("uid") != d["uid"]:
        return {"observed": "unresolved", "why": "the object is gone or replaced"}
    if _expect_holds(live, d["expect"]):
        return {"observed": "done", "uid": d["uid"]}
    if live["metadata"].get("resourceVersion") == d["pre"].get("resourceVersion"):
        return {"observed": "not-done", "uid": d["uid"]}
    return {"observed": "unresolved", "why": "changed since the intent, but not to the expected state"}


def recover_plan(record: "Record", kube: "Kube") -> Dict:
    """Read-only: the held lock (with its sha256, which recovery must be given) and every unresolved intent of the
    part-2 helpers, observed against the live state."""
    actions = []
    for helper in PART2_HELPERS:
        for it in open_intents(receipts(record, helper)):
            d = it["data"]
            actions.append({"helper": helper, "step": it["step"], "op": d["op"], "kind": d["kind"],
                            "namespace": d["namespace"], "name": d["name"], "context": d.get("context", {}),
                            **reconcile(kube, d, record)})
    path_ = lock_file(record)
    sha = C.sha256_file(path_) if os.path.exists(path_) else ""
    return new_plan("recover", record, actions, lock=path_, lock_sha256=sha,
                    unresolved=[a for a in actions if a["observed"] == "unresolved"])


def recover(record: "Record", kube: "Kube", reviewed: Dict, reviewed_digest: str,
            follow_up: Optional[Callable[[List[Dict]], None]] = None) -> Dict:
    """common_records.recovery_guard (one recoverer, the lock's executor proven terminated), then reconciled-<n> for
    every decided outcome; the stale lock is released only when nothing stays unresolved."""
    live = recover_plan(record, kube)
    require_live(reviewed, reviewed_digest, "recover", record, live)
    with C.recovery_guard(record.doc, LOCK_SCOPE, live["lock_sha256"]) as guard:
        C.require_cluster(kube.api, record.cluster_uid)
        for a in live["actions"]:
            if a["observed"] != "unresolved":
                C.append_receipt(record.doc, a["helper"], f"reconciled-{a['step']}",
                                 {k: a[k] for k in ("observed", "state", "uid", "why", "context") if k in a})
        if live["unresolved"]:
            receipt(record, "recover", "recovery-unresolved", {"plan_sha256": reviewed_digest,
                                                               "unresolved": live["unresolved"]})
            raise StagingError(f"unresolved outcomes {[(a['helper'], a['step']) for a in live['unresolved']]}: "
                               "the lock stays; reconcile them by hand")
        if follow_up:
            follow_up(live["actions"])
        guard.release_stale()
        return receipt(record, "recover", "lock-recovered", {"plan_sha256": reviewed_digest,
                                                              "lock_sha256": live["lock_sha256"],
                                                              "reconciled": len(live["actions"])})


# --- typed API access -----------------------------------------------------------------------------------------------

class Kube:
    """Typed access over a common_records API (KubectlApi or a fake with the same methods). Every mutation goes through
    a Journal (intent before, outcome after) and checks the cluster identity first; without a journal it refuses."""

    def __init__(self, api, cluster_uid: str):
        self.api, self.cluster_uid = api, cluster_uid
        self.mutations: List[Dict] = []
        self.journal: Optional[Journal] = None
        self._groups: Optional[Dict] = None

    def _journal(self) -> Journal:
        if self.journal is None:
            raise StagingError("a mutation needs the shared lock and an intent journal")
        return self.journal

    def identity(self) -> str:
        return C.cluster_uid(self.api)

    def served(self, kind: str) -> bool:
        if self._groups is None:
            self._groups = C.discover_groups(self.api)
        return group_of(kind) in self._groups

    def discover_fresh(self) -> Dict[str, List[str]]:
        """UNCACHED API discovery (GET /apis now; served() keeps its per-instance cache). Only a successful, well-formed
        answer is returned; the RAW document is validated before anything is normalized (validate_discovery). A failed
        request (auth, timeout, not found) or a malformed document raises ApiError, so an absent group in the result
        is proven absent by discovery, never inferred from a failure."""
        return validate_discovery(self.api.get("/apis"))

    def get(self, kind: str, name: str, namespace: Optional[str] = None) -> Optional[Dict]:
        o = self.api.get(path(kind, namespace, name))
        if o is not None:
            o.setdefault("kind", kind)
        return o

    def list(self, kind: str, namespace: Optional[str] = None, labels: Optional[Dict[str, str]] = None,
             node_name: str = "", strict: bool = False) -> List[Dict]:
        """Client-side filtered list. strict: the API must answer with a real list (a JSON object carrying an items
        array); a missing answer (KubectlApi returns None for 404) or a malformed one raises instead of reading as an
        empty list. The writer censuses use strict; other callers keep the lenient default."""
        doc = self.api.get(path(kind, namespace))
        if strict and not (isinstance(doc, dict) and isinstance(doc.get("items"), list)):
            raise C.ApiError(f"list {kind}: no list response from a served API (missing endpoint or malformed answer)")
        items = (doc or {}).get("items", [])
        if not isinstance(items, list):
            raise C.ApiError(f"list {kind}: not a list")
        out = []
        for o in items:
            o.setdefault("kind", kind)
            ls = (o.get("metadata") or {}).get("labels") or {}
            if labels and any(ls.get(k) != v for k, v in labels.items()):
                continue
            if node_name and (o.get("spec") or {}).get("nodeName") != node_name:
                continue
            out.append(o)
        return out

    def create(self, obj: Dict, pre_write: Optional[Callable[[str], None]] = None) -> Dict:
        """Create through the journal. pre_write(mark), when given, runs INSIDE the journalled operation (after the
        intent, immediately before the request) and may raise to refuse: nothing is sent, the intent stays unresolved
        and recover finds the object absent (not applied)."""
        j = self._journal()
        m = obj["metadata"]
        C.require_cluster(self.api, self.cluster_uid)
        result: Dict = {}

        def do(mark):
            if pre_write is not None:
                pre_write(mark)
            body = copy.deepcopy(obj)
            body["metadata"].setdefault("annotations", {})[INTENT_ANNOTATION] = mark
            result["out"] = self.api.create(path(obj["kind"], m.get("namespace")), body)
            return result["out"]
        j.run("create", obj["kind"], m.get("namespace"), m["name"], do, body_sha256=C.digest(obj))
        out = result["out"]
        out.setdefault("kind", obj["kind"])
        self.mutations.append({"op": "create", **C.summary(out)})
        return out

    def update(self, kind: str, name: str, namespace: Optional[str], uid: str, mutate: Callable[[Dict], None],
               expect: Optional[List] = None, pre_fields: Iterable[str] = (), attempts: int = 3) -> Dict:
        """Read, check the UID, apply `mutate` to a copy (it raises StagingError when the object is not as reviewed),
        record the intent (pre-state resourceVersion, and `expect`: [json pointer, value | {"contains": x} |
        {"lacks": x}] that hold after the change), then replace with the read resourceVersion and UID. A conflict is
        retried by re-reading and re-checking; a replaced object is refused."""
        j = self._journal()
        if not (isinstance(uid, str) and _UUID.match(uid)):
            raise StagingError(f"{kind}/{name}: an update needs the bound UID")
        last_err = None
        for _ in range(attempts):
            cur = self.get(kind, name, namespace)
            if cur is None or cur["metadata"].get("uid") != uid:
                raise StagingError(f"{kind}/{name} is gone or was replaced (UID differs); refusing")
            body = copy.deepcopy(cur)
            mutate(body)
            body["metadata"]["uid"] = uid
            body["metadata"]["resourceVersion"] = cur["metadata"]["resourceVersion"]
            C.require_cluster(self.api, self.cluster_uid)
            pre = {"resourceVersion": cur["metadata"]["resourceVersion"],
                   "fields": {f: pointer_get(cur, f) for f in pre_fields}}
            try:
                result: Dict = {}

                def do(mark, body=body):
                    result["out"] = self.api.replace(path(kind, namespace, name), body)
                    return result["out"]
                j.run("update", kind, namespace, name, do, uid=uid, pre=pre, expect=expect or [],
                      body_sha256=C.digest(body))
                out = result["out"]
            except C.ApiError as e:
                if getattr(e, "status", None) == 409:
                    last_err = e
                    continue
                raise
            out.setdefault("kind", kind)
            self.mutations.append({"op": "replace", **C.summary(out), "body_sha256": C.digest(body)})
            return out
        raise StagingError(f"{kind}/{name}: kept losing to concurrent updates ({last_err}); refusing")

    def step(self, op: str, kind: str, namespace: Optional[str], name: str, uid: str, fn: Callable[[str], Dict],
             pre: Optional[Dict] = None, expect: Optional[List] = None) -> Dict:
        """A helper-defined journalled step (e.g. session.py's restore: a /scale write plus the wait for its
        convergence): intent-<n> with the validated pre-state BEFORE fn(mark) runs (mark: the intent's journal mark, for
        what fn persists in between), step-<n> only when fn returns (its result object's metadata is the post-state).
        ANY exception, a definitive API refusal included, leaves the intent unresolved and the lock held: the helper's
        registered reconciler (RECONCILERS[op], given the record) decides it in recover."""
        j = self._journal()
        if not (isinstance(uid, str) and _UUID.match(uid)):
            raise StagingError(f"{kind}/{name}: a {op} step needs the bound UID")
        C.require_cluster(self.api, self.cluster_uid)
        result: Dict = {}

        def do(mark):
            try:
                result["out"] = fn(mark)
            except C.ApiError as e:          # not a plain refusal here: the step's outcome is decided by recover
                raise StagingError(f"{kind}/{name}: {op} failed ({e})") from e
            return result["out"]
        j.run(op, kind, namespace, name, do, uid=uid, pre=pre, expect=expect or [])
        self.mutations.append({"op": op, "kind": kind, "namespace": namespace, "name": name, "uid": uid})
        return result["out"]

    def scale(self, kind: str, name: str, namespace: str, uid: str, resource_version: str, replicas: int) -> Dict:
        """PUT <object>/scale (autoscaling/v1 Scale) guarded by the read resourceVersion and the UID: any change after
        that read, even a status update, is a 409 and nothing is written. Call it inside a journalled step; the cluster
        identity is re-checked immediately before the write. Returns the confirmed Scale."""
        self._journal()
        if not (isinstance(uid, str) and _UUID.match(uid)) or not resource_version:
            raise StagingError(f"{kind}/{name}: a scale needs the bound UID and the read resourceVersion")
        C.require_cluster(self.api, self.cluster_uid)
        resp = self.api.replace(path(kind, namespace, name) + "/scale", {
            "apiVersion": "autoscaling/v1", "kind": "Scale",
            "metadata": {"name": name, "namespace": namespace, "uid": uid, "resourceVersion": resource_version},
            "spec": {"replicas": replicas}})
        return confirm_scale(resp, kind, name, uid, replicas)

    def delete(self, kind: str, name: str, namespace: Optional[str], uid: str) -> None:
        j = self._journal()
        if not (isinstance(uid, str) and _UUID.match(uid)):
            raise StagingError(f"{kind}/{name}: a delete needs the bound UID")
        C.require_cluster(self.api, self.cluster_uid)
        j.run("delete", kind, namespace, name, lambda mark: self.api.delete(path(kind, namespace, name), uid), uid=uid)
        self.mutations.append({"op": "delete", "kind": kind, "namespace": namespace, "name": name, "uid": uid})


class mutating:
    """`with X.mutating(record, kube, helper, context) as lock:` holds the run's shared evaluation lock
    (common_records.exclusive_lock) and journals every call through mutation_step. The lock is released only when
    every call's outcome is known (lock.safe), even if a caller swallowed the exception of an unknown outcome."""

    def __init__(self, record: "Record", kube: Kube, helper: str, context: Optional[Dict] = None):
        self.record, self.kube, self.helper, self.context = record, kube, helper, context
        self.lock = C.exclusive_lock(record.doc, LOCK_SCOPE, holder=helper)

    def __enter__(self):
        self.lock.__enter__()
        self.kube.journal = Journal(self.record, self.helper, self.lock, self.context)
        return self.lock

    def __exit__(self, exc_type, exc, tb):
        """Release only when EVERY intent of the part-2 journals is resolved (an outcome or a reconciliation), not
        merely the last call: a later successful step must never clear an earlier unknown outcome."""
        self.kube.journal = None
        unresolved = [(h, it["step"]) for h in PART2_HELPERS for it in open_intents(receipts(self.record, h))]
        if self.lock.safe and not unresolved and os.path.exists(self.lock.path):
            os.unlink(self.lock.path)
        return False


def confirm_scale(resp: Optional[Dict], kind: str, name: str, uid: str, replicas: int) -> Dict:
    """The /scale response must be a Scale for exactly that UID confirming `replicas`. autoscaling/v1 ScaleSpec has
    replicas `omitempty`, so a Scale response for the expected UID without spec.replicas means 0."""
    resp = resp or {}
    m = resp.get("metadata") or {}
    if resp.get("kind") != "Scale" or m.get("uid") != uid:
        raise StagingError(f"the scale response of {kind}/{name} is not a Scale for UID {uid}")
    got = (resp.get("spec") or {}).get("replicas", 0)
    if got != replicas:
        raise StagingError(f"the scale response of {kind}/{name} confirms {got}, not {replicas}")
    return resp


def object_ref(obj: Dict) -> Dict:
    m = obj.get("metadata", {})
    return {"kind": obj.get("kind"), "namespace": m.get("namespace"), "name": m.get("name"), "uid": m.get("uid")}


# --- plans ----------------------------------------------------------------------------------------------------------

def new_plan(helper: str, record: Record, actions: List[Dict], **extra) -> Dict:
    return C.make_plan(helper, record.doc, actions, record_sha256=record.sha256, **extra)


def digest(plan: Dict) -> str:
    return C.plan_digest(plan)


def save_plan(record: Record, helper: str, plan: Dict, name: str = "plan") -> Dict:
    d = C.plan_digest(plan)
    p = os.path.join(record.evidence_dir, helper, f"{name}-{d[:12]}.json")
    if os.path.exists(p):
        with open(p) as fh:
            if C.plan_digest(json.load(fh)) != d:
                raise StagingError(f"{p} exists with other content")
    else:
        C.write_json_once(p, plan)
    return {"plan": p, "plan_digest": d}


def require_live(reviewed: Dict, reviewed_digest: str, helper: str, record: Record, live: Dict) -> None:
    """The reviewed plan (its file and digest), and the live state still producing exactly that plan."""
    C.require_reviewed(reviewed, reviewed_digest, helper, record.doc)
    if C.plan_digest(live) != reviewed_digest:
        raise StagingError("the live state no longer produces the reviewed plan (it changed since review); re-plan "
                           "and review again")


def load_plan(p: str) -> Dict:
    return C.load_plan(p)


def cli_plan(record: Record, helper: str, plan: Dict, name: str = "plan") -> Dict:
    """What `plan` prints: the saved plan file and its digest (review it, then pass both to the mutation)."""
    saved = save_plan(record, helper, plan, name)
    return {**saved, "problems": plan.get("problems", [])}


# --- receipts -------------------------------------------------------------------------------------------------------

def receipt(record: Record, helper: str, event: str, data: Dict, clock=None) -> Dict:
    return C.append_receipt(record.doc, helper, event, json.loads(json.dumps(data)), clock)


def receipts(record: Record, helper: str) -> List[Dict]:
    return C.read_receipts(C.receipts_path(record.doc, helper))


def last(entries: List[Dict], event: str, **match) -> Optional[Dict]:
    for e in reversed(entries):
        if e.get("event") == event and all(e.get("data", {}).get(k) == v for k, v in match.items()):
            return e
    return None


# --- shared helpers -------------------------------------------------------------------------------------------------

def emit(obj) -> None:
    sys.stdout.write(json.dumps(obj, indent=1, sort_keys=True) + "\n")


def harness_path() -> str:
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "harness")


def import_harness():
    hp = harness_path()
    if hp not in sys.path:
        sys.path.insert(0, hp)
    import c28
    from c28 import demo, frozen, render, runlock, schedule  # noqa: F401  (loads the submodules)
    return c28


def load_params(record: Record) -> Dict:
    """The c28-params/v1 file the record names (validated by the harness renderer)."""
    c28 = import_harness()
    p = record.resolve(str(record.doc.get("params", "")))
    if not record.doc.get("params") or not os.path.isfile(p):
        raise StagingError("the record must name its params file (c28-params/v1)")
    with open(p) as fh:
        params = json.load(fh)
    if "residency" not in params:
        raise StagingError("params: the residency section (W/S allocatable and system requests) is required for the "
                           "prodcluster staging helpers")
    try:
        c28.render.validate(params)
    except c28.render.ParamsError as e:
        raise StagingError(f"params: {e}") from e
    if params["run_id"] != record.run_id:
        raise StagingError("params.run_id differs from the record's run_id")
    return params


def load_params_stage(record: Record, stage: str) -> Dict:
    """The c28-params/v1 file the record names, validated ONLY for `stage` (render.validate_stage): every field that
    stage consumes is complete and strict, other fields may still be placeholders. It is for steps that must run before
    the experiment parameters can be completed (reserve.py: the reservation precedes capacity calibration on W). It
    never implies experiment readiness: check experiment_params_problems(params) == [] for that, and every helper that
    renders, prepares, freezes or runs the experiment uses load_params (the full validation)."""
    c28 = import_harness()
    p = record.resolve(str(record.doc.get("params", "")))
    if not record.doc.get("params") or not os.path.isfile(p):
        raise StagingError("the record must name its params file (c28-params/v1)")
    with open(p) as fh:
        params = json.load(fh)
    try:
        c28.render.validate_stage(params, stage)
    except c28.render.ParamsError as e:
        raise StagingError(f"params: {e}") from e
    if params["run_id"] != record.run_id:
        raise StagingError("params.run_id differs from the record's run_id")
    return params


def experiment_params_problems(params: Dict) -> List[str]:
    """Why load_params would refuse these params (the residency section, then the full validation), without raising:
    empty exactly when the experiment parameters are complete."""
    c28 = import_harness()
    out = [] if isinstance(params, dict) and "residency" in params else \
        ["missing: residency (W/S allocatable and system requests, required for the prodcluster staging helpers)"]
    return out + c28.render.full_validation_problems(params)


def injection_problems(kube: "Kube", params: Dict, owner_uids: Optional[set] = None) -> List[str]:
    """Admitted evaluation pods must carry no istio-proxy/istio-init container (render.unexpected_injection). The pods:
    those with the run's label in the fixture and control namespaces, the evaluation release's pods, and any pod whose
    controller is one of `owner_uids` (e.g. a session's generator Job, whatever its labels), replacements included."""
    c28 = import_harness()
    owner_uids = owner_uids or set()
    pods = []
    for ns in sorted({params["namespace"], params["control_namespace"]}):
        for p in kube.list("Pod", ns):
            m = p.get("metadata") or {}
            ls = m.get("labels") or {}
            owners = {r.get("uid") for r in m.get("ownerReferences") or [] if r.get("controller")}
            if ls.get(RUN_LABEL) == params["run_id"] or ls.get("app.kubernetes.io/instance") == params["release"] \
                    or owners & owner_uids:
                pods.append(p)
    return c28.render.unexpected_injection(pods)


WRITER_KINDS = ("HorizontalPodAutoscaler", "ScaledObject", "PredictiveAutoscaler", "LegacyPredictiveAutoscaler",
                "CronHorizontalPodAutoscaler")
OPTIONAL_WRITERS = ("ScaledObject", "LegacyPredictiveAutoscaler", "CronHorizontalPodAutoscaler")
RESOURCE_WRITER_KINDS = ("VerticalPodAutoscaler",)
VPA_DEFAULT_MODE = "Auto"          # the VPA API's default updateMode when spec.updatePolicy omits it


def ident(kind: str, obj: Dict) -> Dict:
    m = obj.get("metadata") or {}
    return {"kind": kind, "namespace": m.get("namespace"), "name": m.get("name"), "uid": m.get("uid")}


def ident_str(i: Dict) -> str:
    return f"{i['kind']}/{i['namespace']}/{i['name']}"


def writer_inventory(kube: Kube, namespace: str, names: Iterable[str], groups: Optional[Dict] = None,
                     absent_ok: bool = False) -> Dict[str, List[Dict]]:
    """Every replica writer of each named Deployment, as FULL identities {kind, namespace, name, uid}: HPAs,
    ScaledObjects and PredictiveAutoscalers of BOTH API groups, from any namespace that can target these Deployments
    (an optional API is skipped only when discovery shows it absent; the others are required). An HPA counts as a
    ScaledObject only when its controller owner reference names a LIVE ScaledObject of that name in its namespace
    with exactly that UID; any other HPA is its own writer.

    `groups` (optional): a discovery result that decides what is served (e.g. Kube.discover_fresh()) instead of the
    cached Kube.served; with `absent_ok` a writer API group this discovery shows unserved is skipped (API
    discovery-proven absent) instead of refused. The defaults keep the original behaviour for every other caller."""
    c28 = import_harness()
    names = set(names)
    found: Dict[str, List[Dict]] = {n: [] for n in names}
    scaled = {}
    served = (lambda kind: group_of(kind) in groups) if groups is not None else kube.served
    if served("ScaledObject"):
        scaled = {(o["metadata"].get("namespace"), o["metadata"]["name"]): o["metadata"].get("uid")
                  for o in kube.list("ScaledObject", None, strict=True)}
    for kind in WRITER_KINDS:
        if not served(kind):
            if kind in OPTIONAL_WRITERS or absent_ok:
                continue
            raise StagingError(f"the {group_of(kind)} API is not served: the writer inventory cannot be complete")
        cross = kind in ("PredictiveAutoscaler", "LegacyPredictiveAutoscaler")
        for o in kube.list(kind, None, strict=True):         # a served writer API must answer with a real list
            as_writer = dict(o, kind="PredictiveAutoscaler" if cross else kind)
            targets = _cronhpa_targets(o) if kind == "CronHorizontalPodAutoscaler" else \
                c28.render._writer_targets(as_writer)
            for ns, name in targets:
                if ns != namespace or name not in names:
                    continue
                w = ident(kind, o)
                if kind == "HorizontalPodAutoscaler":
                    owners = [r for r in o.get("metadata", {}).get("ownerReferences", [])
                              if r.get("kind") == "ScaledObject" and r.get("controller")]
                    so_ns = o["metadata"].get("namespace")
                    if owners and scaled.get((so_ns, owners[0].get("name"))) == owners[0].get("uid"):
                        w = {"kind": "ScaledObject", "namespace": so_ns, "name": owners[0]["name"],
                             "uid": owners[0]["uid"]}
                if w not in found[name]:
                    found[name].append(w)
    return {k: sorted(v, key=lambda i: (i["kind"], i["namespace"] or "", i["name"], i["uid"] or ""))
            for k, v in found.items()}


def _cronhpa_targets(o: Dict) -> List[tuple]:
    """A CronHorizontalPodAutoscaler writes the replica count of its scaleTargetRef (a Deployment by default) in its own
    namespace on its schedule: a replica writer like an HPA."""
    ref = (o.get("spec") or {}).get("scaleTargetRef") or {}
    if (ref.get("kind") or "Deployment") != "Deployment" or not ref.get("name"):
        return []
    return [((o.get("metadata") or {}).get("namespace"), ref["name"])]


def resource_writer_inventory(kube: Kube, namespace: str, names: Iterable[str],
                              groups: Optional[Dict] = None) -> Dict[str, List[Dict]]:
    """Every VerticalPodAutoscaler whose targetRef is one of the named Deployments in `namespace`, with its effective
    updateMode (VPA_DEFAULT_MODE when unset): {name: [{kind, namespace, name, uid, update_mode}]}. Only "Off" writes
    nothing; Auto, Recreate, Initial and InPlaceOrRecreate write the pods' resources. The VPA API is optional: skipped
    only when discovery (`groups`, else the cached Kube.served) shows it absent; a listing failure raises."""
    names = set(names)
    found: Dict[str, List[Dict]] = {n: [] for n in names}
    served = (lambda kind: group_of(kind) in groups) if groups is not None else kube.served
    if not served("VerticalPodAutoscaler"):
        return found
    for o in kube.list("VerticalPodAutoscaler", None, strict=True):
        spec, m = o.get("spec") or {}, o.get("metadata") or {}
        ref = spec.get("targetRef") or {}
        if m.get("namespace") != namespace or (ref.get("kind") or "") != "Deployment" or ref.get("name") not in names:
            continue
        mode = ((spec.get("updatePolicy") or {}).get("updateMode")) or VPA_DEFAULT_MODE
        found[ref["name"]].append(dict(ident("VerticalPodAutoscaler", o), update_mode=mode))
    return {k: sorted(v, key=lambda i: (i["namespace"] or "", i["name"])) for k, v in found.items()}


def subset_diff(want, live, path_: str = "", ignore: Iterable[str] = ()) -> List[str]:
    """Paths where the rendered (reviewed) object differs from the live one. Every rendered leaf must be present and
    equal live; fields the API server only ADDS (defaults, status) are not differences. `ignore`: path prefixes."""
    if any(path_ == p or path_.startswith(p + "/") for p in ignore):
        return []
    if isinstance(want, dict):
        if not isinstance(live, dict):
            return [path_ or "/"]
        out = []
        for k, v in want.items():
            out += subset_diff(v, live.get(k), f"{path_}/{k}", ignore)
        return out
    if isinstance(want, list):
        if not isinstance(live, list) or len(live) != len(want):
            return [path_]
        out = []
        for i, (a, b) in enumerate(zip(want, live)):
            out += subset_diff(a, b, f"{path_}/{i}", ignore)
        return out
    if want == live or (isinstance(want, (int, float)) and not isinstance(want, bool) and str(want) == str(live)):
        return []
    return [path_]


def write_new(p: str, text: str) -> str:
    """A new evidence file (common_records.write_once: never overwritten); returns its sha256."""
    C.assert_clean(text)
    return C.write_once(p, text.encode())


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


sha256_file = C.sha256_file
canonical = C.canonical
json_digest = C.digest


def utcnow() -> str:
    return C.utc_now()


def json_pointer(key: str) -> str:
    return key.replace("~", "~0").replace("/", "~1")
