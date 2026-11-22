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
  * LOCKS and INTENTS (stable interface, shared with part 2): exclusive_lock(rec, scope) is the ONE mutation lock
    of a cluster and resource scope in the mandatory shared lock_dir, recording host, boot, PID and process start;
    mutation_step() writes intent-<n> before and step-<n> after each mutation; a recovery uses recovery_guard(rec,
    scope, lock_sha256) (exclusive, owner proven terminated) and writes reconciled-<n> only for outcomes it can
    decide; anything else stays unresolved and keeps the lock. recovery_guard.release_stale() refuses while ANY
    journal of the owning record (unresolved_intents: every tool that wrote under the lock) has an unresolved intent.
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
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat as _stat
import subprocess
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

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
# Record paths whose name matches SECRET_KEY but which are a helper's own boolean switch (exact path, bool only).
SECRET_NAMED_BOOLEANS = frozenset({"archive.velero.exclude_secrets"})
# Env names whose literal value is redacted (broader: a key, an auth string, a certificate).
CREDENTIAL_NAME = re.compile(r"(token|passw|secret|credential|private|bearer|api[_-]?key|access[_-]?key|auth|"
                             r"client[_-]?key|cert|^key$|_key$|-key$)", re.IGNORECASE)
_URL_USERINFO = re.compile(r"([a-z][a-z0-9+.-]*://)[^/@\s:]+:[^/@\s]+@", re.IGNORECASE)
_CRED_FLAG = re.compile(r"^(--?[A-Za-z0-9_.-]*(token|passw|secret|key|credential|auth|cert)[A-Za-z0-9_.-]*=).+$",
                        re.I)
# A credential flag whose value is the NEXT argument ("--password value", "-p value").
_CRED_FLAG_SPLIT = re.compile(r"^(--?[A-Za-z0-9_.-]*(token|passw|secret|key|credential|auth|cert)[A-Za-z0-9_.-]*|-p)$",
                              re.I)
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
    if not isinstance(rec.get("lock_dir"), str) or not os.path.isabs(rec.get("lock_dir", "")):
        out.append("lock_dir is required: the absolute, shared mutation-lock directory of the executor host")
    src = rec.get("source")
    if not isinstance(src, dict) or not _COMMIT.match(str(src.get("commit", ""))):
        out.append("source.commit must be a full git commit")
    for s in sections:
        if not isinstance(rec.get(s), dict):
            out.append(f"the {s} section is required")
    for path, key, v in _walk(rec):
        if key is not None and SECRET_KEY.search(str(key)) and \
                not (path in SECRET_NAMED_BOOLEANS and isinstance(v, bool)):
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


_STATUS = {400: "bad request", 401: "unauthorized", 403: "forbidden", 404: "not found", 409: "conflict",
           422: "invalid", 429: "throttled", 500: "server error", 504: "timeout"}
_STATUS_WORDS = ((404, ("NotFound", "not found")), (409, ("Conflict", "conflict", "the object has been modified",
                                                         "Precondition failed")),
                 (403, ("Forbidden", "forbidden")), (401, ("Unauthorized", "unauthorized")),
                 (422, ("Invalid", "is invalid")), (429, ("TooManyRequests",)), (400, ("BadRequest",)),
                 (504, ("Timeout", "timeout", "deadline exceeded")), (500, ("InternalError",)))


def _classify(stderr: str) -> Optional[int]:
    for status, words in _STATUS_WORDS:
        if any(w in stderr for w in words):
            return status
    return None


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
            # Never echo kubectl's stderr (it can quote a request body or a credential): classify it.
            status = _classify(p.stderr or "")
            if status == 404:
                if verb == "get":
                    return None
                raise NotFound(f"{verb} {path}: not found", 404)
            raise ApiError(f"{verb} {path}: failed ({_STATUS.get(status, 'error')}, exit {p.returncode})", status)
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

    def json_patch(self, path: str, ops: Sequence[Mapping]) -> Dict:
        """PATCH application/json-patch+json through `kubectl patch --type=json` (kubectl has no raw PATCH). Only a
        namespaced apps/v1 Deployment path is accepted. The API server applies the operations atomically: a failed
        `test` (or any invalid patch) refuses the whole request with 422 and changes nothing (observed against
        kube-apiserver 1.35: "The request is invalid", reason Invalid). An answer without the object is an error with
        no status: the caller must treat the outcome as unknown."""
        m = _DEPLOYMENT_PATH.match(path)
        if not m:
            raise StagingError(f"json_patch: only an apps/v1 Deployment path is supported, not {path!r}")
        args = self.base + ["patch", "deployments.v1.apps", m.group(3), "-n", m.group(1), "--type=json",
                            "-p", json.dumps([dict(o) for o in ops], sort_keys=True), "-o", "json"]
        p = self.run(args, input=None, capture_output=True, text=True)
        if p.returncode != 0:
            status = _classify(p.stderr or "")              # never echo kubectl's stderr: classify it
            if status == 404:
                raise NotFound(f"patch {path}: not found", 404)
            raise ApiError(f"patch {path}: failed ({_STATUS.get(status, 'error')}, exit {p.returncode})", status)
        try:
            out = json.loads((p.stdout or "").strip())
        except ValueError:
            out = None
        if not isinstance(out, dict) or not isinstance(out.get("metadata"), dict):
            raise ApiError(f"patch {path}: the answer carries no object", None)
        return out


_DEPLOYMENT_PATH = re.compile(r"^/apis/apps/v1/namespaces/([a-z0-9]([-a-z0-9]*[a-z0-9])?)/deployments/"
                              r"([a-z0-9]([-a-z0-9.]*[a-z0-9])?)$")

# Resource kinds the helpers read: name -> (API prefix, plural, namespaced).
KINDS = {
    "namespaces": ("/api/v1", "namespaces", False),
    "nodes": ("/api/v1", "nodes", False),
    "pods": ("/api/v1", "pods", True),
    "services": ("/api/v1", "services", True),
    "persistentvolumeclaims": ("/api/v1", "persistentvolumeclaims", True),
    "persistentvolumes": ("/api/v1", "persistentvolumes", False),
    "deployments": ("/apis/apps/v1", "deployments", True),
    "replicasets": ("/apis/apps/v1", "replicasets", True),
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
    with open(path, "a") as fh:                  # crash-safe: on disk before the caller acts on it
        fh.write(line + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    return r


def lock_path(rec: Mapping, scope: str) -> str:
    """The ONE mutation lock of a cluster and resource scope (e.g. "legacy" for every legacy quiesce/restore) in the
    record's MANDATORY shared lock directory (lock_dir: one absolute directory on the executor host, the same for every
    run and helper, so separate runs can never take separate locks)."""
    root = rec.get("lock_dir")
    if not isinstance(root, str) or not os.path.isabs(root):
        raise StagingError("the record needs lock_dir: the absolute, shared mutation-lock directory")
    return os.path.join(root, f"{rec['cluster_uid']}-{scope}.lock")


def _boot_id() -> str:
    try:
        with open("/proc/sys/kernel/random/boot_id") as fh:
            return fh.read().strip()
    except OSError:
        return ""


def process_start(pid: int) -> Optional[str]:
    """A process's start time (clock ticks since boot, /proc/<pid>/stat field 22), or None when it does not exist."""
    try:
        with open(f"/proc/{pid}/stat") as fh:
            stat = fh.read()
    except OSError:
        return None
    return stat.rsplit(")", 1)[1].split()[19]


def _process_state(pid: int) -> Tuple[str, Optional[str]]:
    """("absent", None) when the process certainly does not exist, ("present", <start time>) when it does, and
    ("unknown", None) for anything else: /proc unreadable for a reason other than absence, or a stat line that cannot
    be parsed. Unlike process_start (which is for a process describing ITSELF), this never turns a failed read into
    "gone"."""
    try:
        with open(f"/proc/{pid}/stat") as fh:
            stat = fh.read()
    except (FileNotFoundError, ProcessLookupError):
        return "absent", None
    except OSError:
        return "unknown", None
    fields = stat.rsplit(")", 1)[-1].split() if ")" in stat else []
    if len(fields) < 20 or not fields[19].isdigit():
        return "unknown", None
    return "present", fields[19]


def owner_state(body: Mapping) -> Tuple[str, str]:
    """"terminated" | "alive" | "unknown" for the process an owner record names, with the reason. Terminated ONLY on
    evidence: a complete record (a positive pid, its start time, host, boot id), this host, this boot, and either no
    such process or a process with that pid that started at another time. An incomplete record, another host or
    boot, or a /proc that cannot be read is unknown, never terminated."""
    import socket
    pid, start, host, boot = body.get("pid"), body.get("pid_start"), body.get("host"), body.get("boot_id")
    if not (isinstance(pid, int) and not isinstance(pid, bool) and pid > 0
            and isinstance(start, str) and start.isdigit()       # /proc's start time: clock ticks, digits only
            and isinstance(host, str) and host and isinstance(boot, str) and boot):
        return "unknown", "its owner record is incomplete (pid, start time, host, boot id): nothing can be proven"
    if host != socket.gethostname():
        return "unknown", f"the lock was taken on host {host!r}: verify and recover there"
    mine = _boot_id()
    if not mine or boot != mine:
        return "unknown", "the lock was taken in another boot of this host: its executor cannot be checked"
    state, now = _process_state(pid)
    if state == "unknown":
        return "unknown", f"the state of its executor (pid {pid}) could not be read from /proc"
    if state == "present" and now == start:
        return "alive", f"its executor (pid {pid}) is still running"
    return "terminated", "executor gone"


def owner_terminated(body: Mapping) -> (bool, str):
    """Whether the process that wrote an owner record has CERTAINLY ended (owner_state == "terminated")."""
    state, why = owner_state(body)
    return state == "terminated", why


LOCK_DIR_BODY_BYTES = 65536
_LOCK_NAME = re.compile(r"^(?P<uid>[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})-(?P<scope>[A-Za-z0-9_]+)\.lock"
                        r"(?P<rest>.*)$")


def lock_dir_state(rec: Mapping) -> Dict:
    """READ-ONLY classification of EVERY entry of the record's shared lock directory, for a pre-flight. Nothing is
    created, written, locked or removed, and no file is followed through a symbolic link.

    For this record's cluster an entry is one of:
      lock            <uid>-<scope>.lock                    a mutation lock: an execution is running or crashed
      recovery guard  <uid>-<scope>.lock.recovery           a recovery in progress or interrupted
      coordination    <uid>-<scope>.lock.recovery.flock     the recoverers' flock file. It is EXPECTED and PERMANENT:
                                                            it is never removed, by a helper or by hand, because a
                                                            removed file would let two recoverers hold "the" flock
    Each lock and guard is reported with what its owner record proves (owner_state: alive, terminated or unknown).
    Entries of another cluster that are regular files of one of these three forms are named and not judged.
    Everything else is unexpected, whichever cluster its name carries: an interrupted guard publication (.tmp-...),
    a name of no known form, a directory, a symbolic link, an entry that cannot be examined. The lock directory
    itself must be a real directory: a symbolic link in its place, dangling or not, is a problem and is not followed;
    only a path that does not exist at all counts as "never used yet".

    `clear` is True only when there is no lock, no recovery guard, nothing unexpected and no problem; coordination
    files do not prevent it. It says nothing about open intents or obligations: those are each helper's own status."""
    root, uid = rec.get("lock_dir"), rec.get("cluster_uid")
    if not isinstance(root, str) or not os.path.isabs(root) or not isinstance(uid, str) or not uid:
        raise StagingError("the record needs lock_dir (absolute) and cluster_uid")
    out = {"schema": "c28-lock-dir/v1", "lock_dir": root, "cluster_uid": uid, "exists": True, "locks": [],
           "recovery_guards": [], "coordination": [], "other_clusters": [], "unexpected": [], "problems": []}
    names = []
    try:
        mode = os.lstat(root).st_mode                          # the directory ITSELF: a link is not followed either
        if _stat.S_ISLNK(mode):
            out["problems"].append("the lock directory is a symbolic link (dangling or not): it is not looked into")
        elif not _stat.S_ISDIR(mode):
            out["problems"].append("the lock directory's path is not a directory")
        else:
            names = sorted(os.listdir(root))
    except FileNotFoundError:
        out["exists"] = False                                  # never used yet: the first lock creates it
    except OSError as e:
        out["problems"].append(f"the lock directory could not be listed ({type(e).__name__})")

    def owner(path: str) -> Dict:
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                raw = os.read(fd, LOCK_DIR_BODY_BYTES + 1)
            finally:
                os.close(fd)
            body = json.loads(raw.decode("utf-8")) if len(raw) <= LOCK_DIR_BODY_BYTES else None
        except (OSError, ValueError):
            body = None
        if not isinstance(body, dict):
            return {"owner": "unknown", "why": "its owner record could not be read as one JSON object"}
        state, why = owner_state(body)
        said = {k: (body[k] if isinstance(body[k], int) else body[k][:120])       # what the record says of itself
                for k in ("run_id", "holder", "scope", "pid", "host", "at")
                if isinstance(body.get(k), (str, int)) and not isinstance(body.get(k), bool)}
        return {"owner": state, "why": why, **said}

    for name in names:
        path = os.path.join(root, name)
        m = _LOCK_NAME.match(name)
        try:
            mode = os.lstat(path).st_mode
        except OSError as e:
            out["unexpected"].append({"name": name, "why": f"it could not be examined ({type(e).__name__})"})
            continue
        rest = m.group("rest") if m is not None else None
        if not _stat.S_ISREG(mode):                            # the type and the form first, whoever's entry it is
            out["unexpected"].append({"name": name, "why": "it is not a regular file"})
        elif rest is not None and rest.startswith(".recovery.tmp-"):
            out["unexpected"].append({"name": name, "why": "a recovery guard's publication was interrupted"})
        elif rest not in ("", ".recovery", ".recovery.flock"):
            out["unexpected"].append({"name": name, "why": "its name is not of a known form"})
        elif m.group("uid") != uid:                            # exactly as lock_path() writes it; a KNOWN form only
            out["other_clusters"].append(name)
        elif rest == "":
            out["locks"].append({"name": name, **owner(path)})
        elif rest == ".recovery":
            out["recovery_guards"].append({"name": name, **owner(path)})
        else:
            out["coordination"].append(name)
    out["clear"] = not (out["locks"] or out["recovery_guards"] or out["unexpected"] or out["problems"])
    return out


class exclusive_lock:
    """One executor per cluster and resource scope (lock_path), created exclusively; the lock records the owner's
    host, boot, PID and process start time. It is released on a clean finish, or on an error while `safe` is true (no
    mutation in flight: mutation_step sets safe False after recording a mutation's intent and True after recording its
    outcome). Otherwise it stays (fail closed) until the helper's recovery has proved the owner gone (recovery_guard)
    and reconciled every ambiguous intent against the live objects."""

    def __init__(self, rec: Mapping, scope: str, clock=None, holder: str = ""):
        self.safe = True
        self.path = lock_path(rec, scope)
        self._rec, self._scope, self._holder, self._clock = rec, scope, holder or scope, clock
        self._entered: Optional[str] = None

    def _record(self) -> str:
        """The owner record of the process that calls this, now."""
        import socket
        rec = self._rec
        return json.dumps({"scope": self._scope, "holder": self._holder, "run_id": rec["run_id"],
                           "cluster_uid": rec["cluster_uid"], "record_sha256": digest(rec),
                           "receipts_dir": os.path.realpath(os.path.join(rec["evidence_dir"], "receipts")),
                           "pid": os.getpid(), "pid_start": process_start(os.getpid()),
                           "host": socket.gethostname(), "boot_id": _boot_id(), "at": utc_now(self._clock)},
                          sort_keys=True)

    @property
    def body(self) -> str:
        """What the lock file holds once entered; before that, what this process would write."""
        return self._entered if self._entered is not None else self._record()

    def __enter__(self):
        # The executor is whoever ENTERS, described at this moment: an identity captured earlier could be another
        # process's (the object may have been made before a fork), and recovery would later judge the wrong one.
        # A lock whose owner record could not later be judged would be unrecoverable by design: an executor that
        # cannot describe itself verifiably (its own record must read back, now, as a live process) takes no lock
        # and so mutates nothing.
        body = self._record()
        state, why = owner_state(json.loads(body))
        if state != "alive":
            raise StagingError(f"this executor cannot describe itself verifiably ({why}): the mutation lock is not "
                               "taken and nothing is changed")
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        try:
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        except FileExistsError as e:
            raise StagingError(f"{self.path} exists: another execution is running or crashed; inspect its receipts "
                               f"and run the helper's recover command") from e
        with os.fdopen(fd, "w") as fh:
            fh.write(body)
            fh.flush()
            os.fsync(fh.fileno())
        self._entered = body
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None or self.safe:
            os.unlink(self.path)
        return False


GUARD_SCHEMA = "c28-recovery-guard/v1"
WITHDRAW_ATTEMPTS = 3                     # how often a failed entry tries to take back the record it published


class recovery_guard:
    """Exclusive recovery of a stale lock. Given the lock file's sha256, it

    1. takes an flock on <lock>.recovery.flock, without waiting, and holds it for the WHOLE recovery: one recoverer
       at a time, and the kernel releases it when a recoverer dies (that file is never removed: its inode is the
       coordination point);
    2. validates the stale lock: the sha256 given, this run and record, its executor proven terminated;
    3. looks at <lock>.recovery, the guard record of a recovery in progress. None: fine. One left behind by a
       recoverer that is PROVEN terminated (owner_state on its complete record): it is taken over, and that is
       journalled durably first (`recover` journal, recovery-guard-takeover, with the old guard's sha256 and owner).
       Anything else (a live owner, another host or boot, an empty or unreadable record) refuses;
    4. publishes its own complete guard record atomically (a fsynced temporary file, then an exclusive link or, for
       a takeover, a rename; the directory synced): who recovers which lock of which record, with a fresh token.

    The stale lock stays in place while the caller reconciles, so no executor can start meanwhile. release_stale()
    removes it once every intent is resolved. On leaving, the guard record is removed only if it is still this
    recovery's (its token)."""

    def __init__(self, rec: Mapping, scope: str, lock_sha256: str):
        self.path, self.sha, self.rec, self.scope = lock_path(rec, scope), lock_sha256, rec, scope
        self.guard = self.path + ".recovery"
        self.coordination = self.path + ".recovery.flock"
        self.token: Optional[str] = None
        self._fd: Optional[int] = None

    def __enter__(self):
        if not os.path.exists(self.path):
            raise StagingError("no mutation lock is held: nothing to recover")
        # The descriptor is this object's from the moment it exists: whatever interrupts the acquisition or anything
        # after it, the one cleanup below closes it (which is what releases the flock).
        self._fd = os.open(self.coordination, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o640)
        body = None
        try:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise StagingError("another recovery is in progress") from None
            self._validate_lock()                         # before anything is written to this run's journal
            stale = self._stale_guard()
            body = self._body()                           # this recoverer's own identity, complete, or a refusal
            if stale is not None:                         # durable BEFORE the guard changes hands
                append_receipt(self.rec, "recover", "recovery-guard-takeover", {
                    "scope": self.scope, "lock_sha256": self.sha, "stale_guard": stale,
                    "new_guard": {k: body[k] for k in ("token", "pid", "pid_start", "host", "boot_id")}})
            self._publish(body, replace=stale is not None)
            self.token = body["token"]
        except BaseException:
            try:
                if body is not None:                      # still under the flock: only what THIS attempt published
                    for _again in range(WITHDRAW_ATTEMPTS):      # an interruption of the cleanup is not the end of it
                        try:
                            self._withdraw(body)
                            break
                        except BaseException:
                            continue
            finally:
                self._unlock()                            # whatever happened above: the coordination lock is released
            raise                                         # the failure that ended the entry, not the cleanup's
        return self

    def _withdraw(self, body: Dict) -> None:
        """After a failed entry: remove the guard record only if it is exactly the one this attempt published (a
        failure after the link or the rename, e.g. while syncing the directory, would otherwise leave a record of a
        LIVE process that never entered recovery, and nobody could recover any more). Anything else stays."""
        try:
            with open(self.guard) as fh:
                on_disk = json.load(fh)
            if isinstance(on_disk, dict) and all(on_disk.get(k) == body[k] for k in ("token", "pid", "pid_start")):
                os.unlink(self.guard)
        except (OSError, ValueError):
            pass

    def _validate_lock(self) -> None:
        if sha256_file(self.path) != self.sha:
            raise StagingError("the lock file's sha256 differs from the one given; refusing")
        with open(self.path) as fh:
            body = json.load(fh)
        # The recovery must reconcile the OWNER's intents: the same run, record and receipt location.
        mine = {"run_id": self.rec["run_id"], "cluster_uid": self.rec["cluster_uid"],
                "record_sha256": digest(self.rec),
                "receipts_dir": os.path.realpath(os.path.join(self.rec["evidence_dir"], "receipts"))}
        diff = sorted(k for k, v in mine.items() if body.get(k) != v)
        if diff:
            raise StagingError(f"the lock belongs to another run or record (differs in {diff}); recover it with "
                               f"the owning run's record")
        gone, why = owner_terminated(body)
        if not gone:
            raise StagingError(f"refusing to recover: {why}")

    def _stale_guard(self) -> Optional[Dict]:
        """None when no guard record exists; what is known of a guard whose recoverer is PROVEN terminated; raises
        for every other guard."""
        try:
            with open(self.guard, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            return None
        try:
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise ValueError
        except ValueError:
            raise StagingError("another recovery is in progress, or left a guard record that cannot be read "
                               f"({len(raw)} bytes): it is not proven gone") from None
        state, why = owner_state(body)
        if state != "terminated":
            raise StagingError(f"another recovery is in progress or is not proven gone: {why}")
        return {"sha256": hashlib.sha256(raw).hexdigest(),
                "owner": {k: body.get(k) for k in ("token", "pid", "pid_start", "host", "boot_id", "at")}}

    def _body(self) -> Dict:
        """This recovery's guard record. It is only made when this process can describe itself verifiably: the same
        check that will later judge the record must read it back, now, as a live process."""
        import socket
        body = {"schema": GUARD_SCHEMA, "token": secrets.token_hex(16), "pid": os.getpid(),
                "pid_start": process_start(os.getpid()), "host": socket.gethostname(), "boot_id": _boot_id(),
                "at": utc_now(), "scope": self.scope, "run_id": self.rec["run_id"],
                "cluster_uid": self.rec["cluster_uid"], "record_sha256": digest(self.rec), "lock_sha256": self.sha}
        state, why = owner_state(body)
        if state != "alive":
            raise StagingError(f"this recoverer cannot describe itself verifiably ({why}): nothing was journalled "
                               "or published")
        return body

    def _publish(self, body: Dict, replace: bool) -> None:
        tmp = f"{self.guard}.tmp-{body['token']}"
        try:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
            with os.fdopen(fd, "w") as fh:
                fh.write(json.dumps(body, sort_keys=True))
                fh.flush()
                os.fsync(fh.fileno())
            if replace:
                os.replace(tmp, self.guard)               # under the flock: the stale record gives way atomically
            else:
                os.link(tmp, self.guard)                  # exclusive: fails if a record appeared meanwhile
                os.unlink(tmp)
            d = os.open(os.path.dirname(self.guard), os.O_RDONLY)
            try:
                os.fsync(d)
            finally:
                os.close(d)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def owns(self) -> bool:
        """Whether the guard record is still this recovery's."""
        mine = process_start(os.getpid())
        if self.token is None or not mine:                # an identity that cannot be read owns nothing
            return False
        try:
            with open(self.guard) as fh:
                body = json.load(fh)
        except (OSError, ValueError):
            return False
        return isinstance(body, dict) and body.get("token") == self.token and body.get("pid") == os.getpid() \
            and body.get("pid_start") == mine

    def _unlock(self) -> None:
        if self._fd is not None:
            os.close(self._fd)                            # releases the flock; the coordination file stays
            self._fd = None

    def release_stale(self) -> None:
        """Release only when NO journal of the owning record (whatever tool wrote it under this lock) keeps an
        unresolved intent; each tool's recover resolves its own, and the last one releases. Only the recovery that
        still owns the guard may release."""
        if not self.owns():
            raise StagingError("the stale lock is released only by the recovery that holds the guard")
        left = unresolved_intents(self.rec)
        if left:
            todo = sorted({RECOVER_COMMANDS.get(j, f"the {j} tool's recover") for j in left})
            raise StagingError(f"unresolved intents remain under this lock {left}: the lock stays; run "
                               f"{' and '.join(todo)} first")
        os.unlink(self.path)

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.owns():                               # never another recovery's record
                os.unlink(self.guard)
        finally:
            self.token = None
            self._unlock()
        return False


# Which command resolves a journal's intents (the executor is told; any unknown journal still blocks).
RECOVER_COMMANDS = {"legacy": "legacy.py recover", "legacy-restore": "legacy.py recover",
                    "populate-s0": "populate_s0.py recover", "populate-s0-probe": "populate_s0.py recover"}


def unresolved_intents(rec: Mapping) -> Dict[str, List[str]]:
    """{journal (helper): [steps]} of every intent-<n> in the record's receipt directory with neither step-<n> nor
    reconciled-<n>, across ALL tools' journals (the generic view a shared lock needs)."""
    root = os.path.join(rec["evidence_dir"], "receipts")
    out: Dict[str, List[str]] = {}
    if not os.path.isdir(root):
        return out
    for name in sorted(os.listdir(root)):
        if not name.endswith(".jsonl"):
            continue
        entries = read_receipts(os.path.join(root, name))
        closed = {e["event"].split("-", 1)[1] for e in entries if e["event"].startswith(("step-", "reconciled-"))}
        pending = [e["event"][len("intent-"):] for e in entries
                   if e["event"].startswith("intent-") and e["event"][len("intent-"):] not in closed]
        if pending:
            out[name[:-len(".jsonl")]] = pending
    return out


def mutation_step(rec: Mapping, helper: str, lock: exclusive_lock, step, intent: Mapping, call: Callable,
                  clock=None) -> Dict:
    """The intent protocol shared by every mutating helper. Receipts in <evidence_dir>/receipts/<helper>.jsonl:
      intent-<step>      {"action", "pre", ...}: durable (fsync) BEFORE the call; pre is the validated state;
      step-<step>        {"action", "pre", "result", "post", ...}: after it, post taken from the call's response;
      reconciled-<step>  {"observed": "done"|"not-done", ...}: written only by a recovery that could decide;
    an intent with neither outcome is UNRESOLVED and keeps the lock. call() returns (result, post)."""
    data = dict(intent)
    append_receipt(rec, helper, f"intent-{step}", data, clock)
    lock.safe = False
    result, post = call()
    r = append_receipt(rec, helper, f"step-{step}", {**data, "result": result, "post": post}, clock)
    lock.safe = True
    return r


# --- sanitizing -------------------------------------------------------------------------------------------------

def _redact_value(name: str, value):
    """A credential-named variable is redacted whatever its value looks like (a number or a path can be a secret)."""
    if CREDENTIAL_NAME.search(name):
        return REDACTED
    if isinstance(value, str) and _LEAK.search(value):
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
    out, redact_next = [], False
    for a in args or []:
        if redact_next:                          # unconditionally: a credential may itself start with "-"
            out.append(REDACTED)
            redact_next = False
            continue
        if isinstance(a, str):
            if _CRED_FLAG_SPLIT.match(a):
                redact_next = True                   # "--password value": the value is the next argument
            a = _CRED_FLAG.sub(r"\1" + REDACTED, a)
            a = REDACTED if _LEAK.search(a) else _URL_USERINFO.sub(r"\1" + REDACTED + "@", a)
        else:
            a = REDACTED if CREDENTIAL_NAME.search(str(a)) else a
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


__all__ = ["exclusive_lock", "lock_path", "mutation_step", "unresolved_intents", "owner_terminated", "process_start",
           "recovery_guard",
           "ApiError", "KINDS", "KubectlApi", "NotFound", "StagingError",
           "add_common_args",
           "api_for",
           "append_receipt", "assert_clean", "canonical", "cluster_uid", "digest", "discover_groups", "kind_group",
           "list_items", "load_plan", "load_record", "make_plan", "path_of", "plan_digest", "read_receipts",
           "receipts_path", "record_problems", "require_cluster", "require_reviewed", "sanitize_metadata",
           "sanitize_object", "sha256_file", "summary", "utc_now", "write_json_once", "write_once"]
