"""Part-2 adapter over common_records.py for the staging helpers reserve, prepare, freeze, session and build_images.

STAGING-C28-PROD.md, "Helpers to implement". Everything shared comes from common_records (part 1, never edited here):
the c28-preparation/v1 record (load_record), the injectable raw API (KubectlApi: get/replace/create/delete with UID
preconditions, require_cluster before every mutation), plans (make_plan, plan_digest, require_reviewed, load_plan),
write-once evidence (write_json_once), the hash-chained receipts (append_receipt, read_receipts) and sanitizing.
legacy_allowlist names the legacy API group. This module only adds what part 2 needs on top:

  * Record: the loaded record with evidence_dir made absolute and the helper's own file paths resolved against the
    record's directory.
  * Kube: typed access by kind over the raw API (the extra kinds part 2 touches: ConfigMaps, ResourceQuotas,
    ReplicaSets, PriorityClasses, StorageClasses, NetworkPolicies, scrape objects), with client-side filtering only
    (nothing is selected server-side by label or name substring), update = read, mutate a copy, then replace with the
    read resourceVersion and UID (optimistic concurrency; a replaced or concurrently edited object is refused), and
    require_cluster before every create/replace/delete. Secrets are never read.
  * Reviewed plans: `plan` writes the plan once (evidence/<helper>/<name>-<digest12>.json) and prints its digest; a
    mutation takes --plan <file> --plan-digest <digest>, checks it with require_reviewed, and ALSO recomputes the plan
    from the live state: if the live plan's digest differs, the state changed since review and nothing is done.
  * writer_inventory: every replica writer of the fixture targets across HPA, KEDA and BOTH PredictiveAutoscaler API
    groups, with optional APIs skipped only when discovery shows them absent.
"""

from __future__ import annotations

import copy
import hashlib
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
    "podmonitors": ("/apis/monitoring.coreos.com/v1", "podmonitors", True),
    "vmpodscrapes": ("/apis/operator.victoriametrics.com/v1beta1", "vmpodscrapes", True),
}
# Object kind -> table key.
KIND_KEY = {
    "Namespace": "namespaces", "Node": "nodes", "Pod": "pods", "Service": "services",
    "PersistentVolumeClaim": "persistentvolumeclaims", "PersistentVolume": "persistentvolumes",
    "Deployment": "deployments", "StatefulSet": "statefulsets", "DaemonSet": "daemonsets", "Job": "jobs",
    "HorizontalPodAutoscaler": "horizontalpodautoscalers", "ScaledObject": "scaledobjects",
    "PredictiveAutoscaler": "pa-new", "LegacyPredictiveAutoscaler": "pa-legacy",
    "ConfigMap": "configmaps", "ResourceQuota": "resourcequotas", "ReplicaSet": "replicasets",
    "PriorityClass": "priorityclasses", "StorageClass": "storageclasses", "NetworkPolicy": "networkpolicies",
    "PodMonitor": "podmonitors", "VMPodScrape": "vmpodscrapes",
}
if C.KINDS["pa-legacy"][0] != f"/apis/{legacy_allowlist.LEGACY_GROUP}/v1alpha1":     # pragma: no cover
    raise ImportError("common_records and legacy_allowlist disagree on the legacy API group")


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


# --- typed API access -----------------------------------------------------------------------------------------------

class Kube:
    """Typed access over a common_records API (KubectlApi or a fake with the same methods)."""

    def __init__(self, api, cluster_uid: str):
        self.api, self.cluster_uid = api, cluster_uid
        self.mutations: List[Dict] = []
        self._groups: Optional[Dict] = None

    def identity(self) -> str:
        return C.cluster_uid(self.api)

    def served(self, kind: str) -> bool:
        if self._groups is None:
            self._groups = C.discover_groups(self.api)
        return group_of(kind) in self._groups

    def get(self, kind: str, name: str, namespace: Optional[str] = None) -> Optional[Dict]:
        o = self.api.get(path(kind, namespace, name))
        if o is not None:
            o.setdefault("kind", kind)
        return o

    def list(self, kind: str, namespace: Optional[str] = None, labels: Optional[Dict[str, str]] = None,
             node_name: str = "") -> List[Dict]:
        doc = self.api.get(path(kind, namespace))
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

    def create(self, obj: Dict) -> Dict:
        m = obj["metadata"]
        C.require_cluster(self.api, self.cluster_uid)
        out = self.api.create(path(obj["kind"], m.get("namespace")), obj)
        out.setdefault("kind", obj["kind"])
        self.mutations.append({"op": "create", **C.summary(out)})
        return out

    def update(self, kind: str, name: str, namespace: Optional[str], uid: str, mutate: Callable[[Dict], None],
               attempts: int = 3) -> Dict:
        """Read, check the UID, apply `mutate` to a copy (it raises StagingError when the object is not as reviewed),
        then replace with the read resourceVersion and UID. A concurrent edit is retried only by re-reading and
        re-checking; a replaced object is refused."""
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
            try:
                out = self.api.replace(path(kind, namespace, name), body)
            except C.ApiError as e:
                if getattr(e, "status", None) == 409 or "conflict" in str(e).lower():
                    last_err = e
                    continue
                raise
            out.setdefault("kind", kind)
            self.mutations.append({"op": "replace", **C.summary(out), "body_sha256": C.digest(body)})
            return out
        raise StagingError(f"{kind}/{name}: kept losing to concurrent updates ({last_err}); refusing")

    def delete(self, kind: str, name: str, namespace: Optional[str], uid: str) -> None:
        if not (isinstance(uid, str) and _UUID.match(uid)):
            raise StagingError(f"{kind}/{name}: a delete needs the bound UID")
        C.require_cluster(self.api, self.cluster_uid)
        self.api.delete(path(kind, namespace, name), uid)
        self.mutations.append({"op": "delete", "kind": kind, "namespace": namespace, "name": name, "uid": uid})


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
    from c28 import frozen, render, runlock, schedule  # noqa: F401  (loads the submodules)
    return c28


def load_params(record: Record) -> Dict:
    """The c28-params/v1 file the record names (validated by the harness renderer)."""
    c28 = import_harness()
    p = record.resolve(str(record.doc.get("params", "")))
    if not record.doc.get("params") or not os.path.isfile(p):
        raise StagingError("the record must name its params file (c28-params/v1)")
    with open(p) as fh:
        params = json.load(fh)
    try:
        c28.render.validate(params)
    except c28.render.ParamsError as e:
        raise StagingError(f"params: {e}") from e
    if params["run_id"] != record.run_id:
        raise StagingError("params.run_id differs from the record's run_id")
    return params


WRITER_KINDS = ("HorizontalPodAutoscaler", "ScaledObject", "PredictiveAutoscaler", "LegacyPredictiveAutoscaler")
OPTIONAL_WRITERS = ("ScaledObject", "LegacyPredictiveAutoscaler")


def writer_inventory(kube: Kube, namespace: str, names: Iterable[str]) -> Dict[str, List[str]]:
    """Every replica writer of each named Deployment: HPAs, ScaledObjects and PredictiveAutoscalers of BOTH API groups
    (an optional API is skipped only when discovery shows it absent; the others are required). A KEDA-generated HPA
    counts as its ScaledObject; PredictiveAutoscalers in other namespaces naming these targets are included."""
    c28 = import_harness()
    names = set(names)
    found: Dict[str, List[str]] = {n: [] for n in names}
    for kind in WRITER_KINDS:
        if not kube.served(kind):
            if kind in OPTIONAL_WRITERS:
                continue
            raise StagingError(f"the {group_of(kind)} API is not served: the writer inventory cannot be complete")
        cross = kind in ("PredictiveAutoscaler", "LegacyPredictiveAutoscaler")
        for o in kube.list(kind, None if cross else namespace):
            as_writer = dict(o, kind="PredictiveAutoscaler" if cross else kind)
            owners = [r for r in o.get("metadata", {}).get("ownerReferences", [])
                      if r.get("kind") == "ScaledObject" and r.get("controller")]
            for ns, name in c28.render._writer_targets(as_writer):
                if ns != namespace or name not in names:
                    continue
                w = f"{kind}/{o['metadata']['name']}"
                if kind == "HorizontalPodAutoscaler" and owners:
                    w = f"ScaledObject/{owners[0]['name']}"
                if w not in found[name]:
                    found[name].append(w)
    return {k: sorted(v) for k, v in found.items()}


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
