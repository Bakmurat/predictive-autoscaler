"""Shared records and guards of the C2.8 staging helpers (STAGING-C28-PROD.md, "Helpers to implement").

Every helper (inventory, archive, sanitize-render, legacy; reserve, prepare, freeze, session) builds on this module:

  * the PREPARATION RECORD (`--record <preparation.json>`, schema c28-preparation/v1): the run's non-secret inputs.
    load_record() validates the common part (run_id, cluster_uid, kubeconfig path+context, evidence_dir, source) and
    refuses placeholders and credential material; each helper reads its own section (record["legacy"], ...).
  * the API: an explicit, injectable Kubernetes client. KubectlApi runs `kubectl --kubeconfig <path> --context <ctx>
    get|replace|create|delete --raw` (never a default kubeconfig, a 30 s request timeout, errors fail closed); tests
    use a fake with the same five methods. require_cluster() re-reads the kube-system namespace UID before every
    mutation.
  * PLANS and RECEIPTS: a plan is a canonical JSON document whose sha256 (plan_digest) the reviewer approves; a
    mutation runs only with `--plan-digest <that digest>` and only while the plan file still has it
    (require_reviewed). Evidence files are written once (write_once refuses to overwrite); receipts are appended to a
    hash-chained JSON-lines log (append_receipt), never rewritten.
  * SANITIZING: sanitize_object() turns any API object into a reviewable record: no Secret data (Secrets are reduced
    to a stub), no ConfigMap contents (per-key sha256 only), no managedFields, no last-applied configuration, no
    literal credential env values or credential flags, no URL user-info. assert_clean() is the last guard on every
    output: it refuses text that still carries a credential pattern or any value a caller declares secret.

Mutations always go through UID preconditions (delete) or optimistic concurrency (replace with the read
resourceVersion); nothing here selects objects by a label, a name substring or a whole namespace.
"""

from __future__ import annotations

import copy
import datetime as _dt
import hashlib
import json
import os
import re
import subprocess
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence

RECORD_SCHEMA = "c28-preparation/v1"
PLAN_SCHEMA = "c28-staging-plan/v1"
RECEIPT_SCHEMA = "c28-staging-receipt/v1"
LAST_APPLIED = "kubectl.kubernetes.io/last-applied-configuration"

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_RUN_ID = re.compile(r"^[a-z0-9]([-a-z0-9]{0,40}[a-z0-9])?$")
_UNRESOLVED = re.compile(r"(<[^>]*>|\bTODO\b|\bTBD\b|CHANGEME|\$\{)", re.IGNORECASE)
# Keys that may never appear in a record, a plan or an annotation (the harness run-lock's rule).
SECRET_KEY = re.compile(r"(token|password|passwd|secret|client-key|client_key|private|credential|bearer)", re.I)
# Env names whose literal value is redacted (broader: a key, an auth string, a certificate).
CREDENTIAL_NAME = re.compile(r"(token|passw|secret|credential|private|bearer|api[_-]?key|access[_-]?key|auth|"
                             r"client[_-]?key|cert|^key$|_key$|-key$)", re.IGNORECASE)
_URL_USERINFO = re.compile(r"([a-z][a-z0-9+.-]*://)[^/@\s:]+:[^/@\s]+@", re.IGNORECASE)
_CRED_FLAG = re.compile(r"^(--?[A-Za-z0-9_.-]*(token|passw|secret|key|credential|auth)[A-Za-z0-9_.-]*=).+$", re.I)
# Text that must never leave a helper, whatever field it hides in.
_LEAK = re.compile(r"(-----BEGIN [A-Z ]*PRIVATE KEY-----|\"?" + re.escape(LAST_APPLIED) +
                   r"|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.|[a-z][a-z0-9+.-]*://[^/@\s:\"]+:[^/@\s\"]+@)")
REDACTED = "<redacted>"


class StagingError(RuntimeError):
    """A helper refuses to continue (fail closed); the message never carries a secret value."""


# --- canonical JSON, digests, time ------------------------------------------------------------------------------

def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def digest(obj) -> str:
    return hashlib.sha256(canonical(obj)).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def utc_now(clock: Optional[Callable[[], float]] = None) -> str:
    t = clock() if clock else _dt.datetime.now(_dt.timezone.utc).timestamp()
    return _dt.datetime.fromtimestamp(int(t), _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- the preparation record -------------------------------------------------------------------------------------

def _walk(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            yield p, k, v
            yield from _walk(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield f"{path}[{i}]", None, v
            yield from _walk(v, f"{path}[{i}]")


def record_problems(rec: Mapping, sections: Sequence[str] = ()) -> List[str]:
    """Every reason the preparation record cannot be used (empty when it can)."""
    if not isinstance(rec, dict):
        return ["the preparation record is not a JSON object"]
    out = []
    if rec.get("schema") != RECORD_SCHEMA:
        out.append(f"schema must be {RECORD_SCHEMA}")
    if not _RUN_ID.match(str(rec.get("run_id", ""))):
        out.append("run_id must be a short DNS label")
    if not _UUID.match(str(rec.get("cluster_uid", ""))):
        out.append("cluster_uid must be the kube-system namespace UID")
    kc = rec.get("kubeconfig")
    if not isinstance(kc, dict) or set(kc) != {"path", "context"} or not all(isinstance(kc.get(k), str) and kc[k]
                                                                           for k in ("path", "context")):
        out.append("kubeconfig must be exactly {path, context} (an explicit file, never its contents)")
    elif os.path.basename(kc["path"]) in ("", "config") and kc["path"].rstrip("/").endswith(".kube/config"):
        out.append("kubeconfig.path must be an explicit evaluation kubeconfig, not the default ~/.kube/config")
    if not isinstance(rec.get("evidence_dir"), str) or not rec.get("evidence_dir"):
        out.append("evidence_dir is required")
    src = rec.get("source")
    if not isinstance(src, dict) or not _COMMIT.match(str(src.get("commit", ""))):
        out.append("source.commit must be a full git commit")
    for s in sections:
        if not isinstance(rec.get(s), dict):
            out.append(f"the {s} section is required")
    for path, key, v in _walk(rec):
        if key is not None and SECRET_KEY.search(str(key)):
            out.append(f"{path}: credential material is never recorded (reference its location)")
        if isinstance(v, str) and (_UNRESOLVED.search(v) or _LEAK.search(v)):
            out.append(f"{path}: unresolved or credential-bearing value")
    return out


def load_record(path: str, sections: Sequence[str] = ()) -> Dict:
    try:
        with open(path) as fh:
            rec = json.load(fh)
    except (OSError, ValueError) as e:
        raise StagingError(f"the preparation record cannot be read: {e}") from e
    errs = record_problems(rec, sections)
    if errs:
        raise StagingError("; ".join(errs))
    return rec


# --- the API --------------------------------------------------------------------------------------------------------

class ApiError(StagingError):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class NotFound(ApiError):
    pass


class KubectlApi:
    """Raw API access through kubectl with an explicit kubeconfig and context. get() returns None for 404."""

    def __init__(self, kubeconfig: Mapping, run: Callable = subprocess.run, timeout: str = "30s"):
        self.base = ["kubectl", "--kubeconfig", kubeconfig["path"], "--context", kubeconfig["context"],
                     f"--request-timeout={timeout}"]
        self.run = run

    def _call(self, verb: str, path: str, body: Optional[Mapping] = None) -> Optional[Dict]:
        args = self.base + [verb, "--raw", path]
        stdin = None
        if body is not None:
            args += ["-f", "-"]
            stdin = json.dumps(body)
        p = self.run(args, input=stdin, capture_output=True, text=True)
        if p.returncode != 0:
            err = (p.stderr or "").strip().splitlines()
            msg = err[-1] if err else "kubectl failed"
            if "NotFound" in msg or "not found" in msg:
                if verb == "get":
                    return None
                raise NotFound(f"{verb} {path}: not found", 404)
            raise ApiError(f"{verb} {path}: {msg[:300]}")
        out = (p.stdout or "").strip()
        return json.loads(out) if out else {}

    def get(self, path: str) -> Optional[Dict]:
        return self._call("get", path)

    def replace(self, path: str, body: Mapping) -> Dict:
        """PUT: the body carries metadata.resourceVersion, so a concurrent change is refused (409)."""
        return self._call("replace", path, body)

    def create(self, path: str, body: Mapping) -> Dict:
        return self._call("create", path, body)

    def delete(self, path: str, uid: str, resource_version: Optional[str] = None) -> Dict:
        pre = {"uid": uid}
        if resource_version:
            pre["resourceVersion"] = resource_version
        return self._call("delete", path, {"kind": "DeleteOptions", "apiVersion": "v1", "preconditions": pre,
                                           "propagationPolicy": "Background"})


# Resource kinds the helpers read: name -> (API prefix, plural, namespaced).
KINDS = {
    "namespaces": ("/api/v1", "namespaces", False),
    "nodes": ("/api/v1", "nodes", False),
    "pods": ("/api/v1", "pods", True),
    "services": ("/api/v1", "services", True),
    "persistentvolumeclaims": ("/api/v1", "persistentvolumeclaims", True),
    "persistentvolumes": ("/api/v1", "persistentvolumes", False),
    "deployments": ("/apis/apps/v1", "deployments", True),
    "statefulsets": ("/apis/apps/v1", "statefulsets", True),
    "daemonsets": ("/apis/apps/v1", "daemonsets", True),
    "cronjobs": ("/apis/batch/v1", "cronjobs", True),
    "jobs": ("/apis/batch/v1", "jobs", True),
    "horizontalpodautoscalers": ("/apis/autoscaling/v2", "horizontalpodautoscalers", True),
    "poddisruptionbudgets": ("/apis/policy/v1", "poddisruptionbudgets", True),
    "leases": ("/apis/coordination.k8s.io/v1", "leases", True),
    "customresourcedefinitions": ("/apis/apiextensions.k8s.io/v1", "customresourcedefinitions", False),
    "scaledobjects": ("/apis/keda.sh/v1alpha1", "scaledobjects", True),
    "pa-legacy": ("/apis/autoscaler.example.com/v1alpha1", "predictiveautoscalers", True),
    "pa-new": ("/apis/autoscaling.devkuban.com/v1alpha1", "predictiveautoscalers", True),
    "applications": ("/apis/argoproj.io/v1alpha1", "applications", True),
    "backups": ("/apis/velero.io/v1", "backups", True),
}


def kind_group(kind: str) -> str:
    prefix = KINDS[kind][0]
    return "" if prefix == "/api/v1" else prefix.split("/")[2]


def path_of(kind: str, namespace: Optional[str] = None, name: Optional[str] = None, sub: str = "") -> str:
    prefix, plural, namespaced = KINDS[kind]
    if namespaced and namespace:
        p = f"{prefix}/namespaces/{namespace}/{plural}"
    elif namespaced and name:
        raise StagingError(f"{kind}/{name} needs a namespace")
    else:
        p = f"{prefix}/{plural}"
    if name:
        p += f"/{name}"
    return p + (f"/{sub}" if sub else "")


def list_items(api, kind: str, namespace: Optional[str] = None) -> List[Dict]:
    doc = api.get(path_of(kind, namespace))
    if doc is None:
        return []
    items = doc.get("items")
    if not isinstance(items, list):
        raise ApiError(f"list {kind}: not a list")
    return items


def discover_groups(api) -> Dict[str, List[str]]:
    """Every served API group and its versions (GET /apis), plus the core group."""
    doc = api.get("/apis")
    if not isinstance(doc, dict) or not isinstance(doc.get("groups"), list):
        raise ApiError("API discovery (/apis) failed")
    out = {"": ["v1"]}
    for g in doc["groups"]:
        out[g["name"]] = sorted(v["version"] for v in g.get("versions", []))
    return out


def cluster_uid(api) -> str:
    ns = api.get(path_of("namespaces", name="kube-system"))
    uid = ((ns or {}).get("metadata") or {}).get("uid")
    if not isinstance(uid, str) or not uid:
        raise StagingError("the cluster identity (kube-system namespace UID) cannot be read")
    return uid


def require_cluster(api, expected: str) -> None:
    """Called immediately before every mutation (and at the start of every plan)."""
    if cluster_uid(api) != expected:
        raise StagingError("the kubeconfig does not reach the recorded cluster (kube-system UID differs); refusing")


# --- plans, reviews, receipts ---------------------------------------------------------------------------------

def make_plan(helper: str, rec: Mapping, actions: List[Dict], **extra) -> Dict:
    return {"schema": PLAN_SCHEMA, "helper": helper, "run_id": rec["run_id"], "cluster_uid": rec["cluster_uid"],
            "source_commit": rec["source"]["commit"], "actions": actions, **extra}


def plan_digest(plan: Mapping) -> str:
    return digest(plan)


def write_once(path: str, data: bytes) -> str:
    """Write a new evidence file (never overwrite one); returns its sha256."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    except FileExistsError as e:
        raise StagingError(f"{path} exists: evidence is written once") from e
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    return hashlib.sha256(data).hexdigest()


def write_json_once(path: str, obj, secrets: Iterable[str] = ()) -> str:
    text = json.dumps(obj, indent=1, sort_keys=True) + "\n"
    assert_clean(text, secrets)
    return write_once(path, text.encode())


def load_plan(path: str) -> Dict:
    with open(path) as fh:
        plan = json.load(fh)
    if not isinstance(plan, dict) or plan.get("schema") != PLAN_SCHEMA:
        raise StagingError(f"{path} is not a {PLAN_SCHEMA} document")
    return plan


def require_reviewed(plan: Mapping, reviewed_digest: Optional[str], helper: str, rec: Mapping) -> None:
    """A mutation needs the plan the reviewer approved: same digest, same helper, run and cluster."""
    if not reviewed_digest or not _SHA.match(reviewed_digest):
        raise StagingError("a mutation needs --plan-digest <the reviewed plan's sha256>")
    if plan_digest(plan) != reviewed_digest:
        raise StagingError("the plan differs from the reviewed digest; refusing")
    if (plan.get("helper"), plan.get("run_id"), plan.get("cluster_uid")) != (helper, rec["run_id"], rec["cluster_uid"]):
        raise StagingError("the plan belongs to another helper, run or cluster; refusing")


def receipts_path(rec: Mapping, helper: str) -> str:
    return os.path.join(rec["evidence_dir"], "receipts", f"{helper}.jsonl")


def read_receipts(path: str) -> List[Dict]:
    """The receipts of a log, verifying its hash chain."""
    out, prev = [], "0" * 64
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        for n, line in enumerate(fh, 1):
            r = json.loads(line)
            body = {k: v for k, v in r.items() if k != "sha256"}
            if r.get("prev") != prev or digest(body) != r.get("sha256"):
                raise StagingError(f"{path}: receipt {n} breaks the hash chain")
            out.append(r)
            prev = r["sha256"]
    return out


def append_receipt(rec: Mapping, helper: str, event: str, data: Mapping, clock=None,
                   secrets: Iterable[str] = ()) -> Dict:
    """Append one receipt to the helper's hash-chained log (append-only; the chain is verified first)."""
    path = receipts_path(rec, helper)
    prior = read_receipts(path)
    body = {"schema": RECEIPT_SCHEMA, "helper": helper, "run_id": rec["run_id"], "event": event,
            "at": utc_now(clock), "prev": prior[-1]["sha256"] if prior else "0" * 64, "data": data}
    r = dict(body, sha256=digest(body))
    line = json.dumps(r, sort_keys=True)
    assert_clean(line, secrets)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as fh:
        fh.write(line + "\n")
    return r


# --- sanitizing -------------------------------------------------------------------------------------------------

_BENIGN = re.compile(r"^(/[^\s]*|true|false|[0-9]+)$")       # file paths, booleans, numbers: never credentials


def _redact_value(name: str, value):
    if isinstance(value, str) and _LEAK.search(value):
        return REDACTED
    if isinstance(value, str) and CREDENTIAL_NAME.search(name) and not _BENIGN.match(value):
        return REDACTED
    if isinstance(value, str):
        return _URL_USERINFO.sub(r"\1" + REDACTED + "@", value)
    return value


def _sanitize_env(env: List) -> List:
    out = []
    for e in env or []:
        if not isinstance(e, dict):
            continue
        item = {"name": e.get("name")}
        if "value" in e:
            item["value"] = _redact_value(str(e.get("name", "")), e.get("value"))
        if "valueFrom" in e:                     # a reference (Secret/ConfigMap name and key), never the value
            item["valueFrom"] = copy.deepcopy(e["valueFrom"])
        out.append(item)
    return out


def _sanitize_args(args: List) -> List:
    out = []
    for a in args or []:
        if isinstance(a, str):
            a = _CRED_FLAG.sub(r"\1" + REDACTED, a)
            a = REDACTED if _LEAK.search(a) else _URL_USERINFO.sub(r"\1" + REDACTED + "@", a)
        out.append(a)
    return out


def _sanitize_pod_spec(spec: Dict) -> None:
    for key in ("containers", "initContainers", "ephemeralContainers"):
        for c in spec.get(key) or []:
            if "env" in c:
                c["env"] = _sanitize_env(c["env"])
            for k in ("args", "command"):
                if k in c:
                    c[k] = _sanitize_args(c[k])


def _find_pod_specs(obj):
    """Every embedded pod spec (Deployment, StatefulSet, DaemonSet, Job, CronJob templates, a Pod itself)."""
    if not isinstance(obj, dict):
        return
    if obj.get("kind") == "Pod":
        yield obj.get("spec") or {}
    spec = obj.get("spec") or {}
    tmpl = spec.get("template") or (spec.get("jobTemplate") or {}).get("spec", {}).get("template")
    if isinstance(tmpl, dict):
        yield tmpl.get("spec") or {}


def sanitize_metadata(meta: Mapping) -> Dict:
    m = {k: copy.deepcopy(v) for k, v in (meta or {}).items() if k not in ("managedFields",)}
    ann = m.get("annotations")
    if isinstance(ann, dict):
        m["annotations"] = {k: (REDACTED if (SECRET_KEY.search(k) or (isinstance(v, str) and _LEAK.search(v)))
                                else v)
                            for k, v in ann.items() if k != LAST_APPLIED and "last-applied" not in k}
    return m


def sanitize_object(obj: Mapping) -> Dict:
    """A reviewable copy of an API object: no Secret data, ConfigMap contents, managedFields, last-applied
    configuration, literal credential env values or flags, or URL user-info."""
    o = copy.deepcopy(dict(obj))
    o["metadata"] = sanitize_metadata(o.get("metadata") or {})
    kind = o.get("kind")
    if kind == "Secret":
        return {"apiVersion": "v1", "kind": "Secret", "metadata": {k: o["metadata"].get(k) for k in
                                                                   ("name", "namespace", "uid") if k in o["metadata"]},
                "type": o.get("type"), "data_keys": len((obj.get("data") or {})) + len(obj.get("stringData") or {}),
                "sanitized": "secret-values-removed"}
    if kind == "ConfigMap":
        data = {**(o.pop("data", None) or {}), **(o.pop("binaryData", None) or {})}
        o["data_sha256"] = {k: hashlib.sha256(str(v).encode()).hexdigest() for k, v in sorted(data.items())}
        o["sanitized"] = "configmap-contents-hashed"
    for spec in _find_pod_specs(o):
        _sanitize_pod_spec(spec)
    return o


def assert_clean(text: str, secrets: Iterable[str] = ()) -> None:
    """Refuse output that still carries a credential pattern, a last-applied configuration or a declared secret."""
    if _LEAK.search(text):
        raise StagingError("the output would carry credential material or a last-applied configuration; refusing")
    for s in secrets:
        if s and s in text:
            raise StagingError("the output would carry a declared secret value; refusing")


def summary(obj: Mapping) -> Dict:
    """The identity fields of an object (kind, namespace, name, uid, resourceVersion, generation)."""
    m = obj.get("metadata") or {}
    return {"kind": obj.get("kind"), "namespace": m.get("namespace"), "name": m.get("name"), "uid": m.get("uid"),
            "resourceVersion": m.get("resourceVersion"), "generation": m.get("generation")}


# --- CLI helpers ----------------------------------------------------------------------------------------------------

def add_common_args(p) -> None:
    p.add_argument("--record", required=True, help="the preparation record (c28-preparation/v1)")


def api_for(rec: Mapping, run: Callable = subprocess.run) -> KubectlApi:
    return KubectlApi(rec["kubeconfig"], run=run)


__all__ = ["ApiError", "KINDS", "KubectlApi", "NotFound", "StagingError", "add_common_args", "api_for",
           "append_receipt", "assert_clean", "canonical", "cluster_uid", "digest", "discover_groups", "kind_group",
           "list_items", "load_plan", "load_record", "make_plan", "path_of", "plan_digest", "read_receipts",
           "receipts_path", "record_problems", "require_cluster", "require_reviewed", "sanitize_metadata",
           "sanitize_object", "sha256_file", "summary", "utc_now", "write_json_once", "write_once"]
