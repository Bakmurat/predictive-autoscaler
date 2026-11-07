"""The C2.8 run-lock (EVAL-C28-MANIFEST section 9): the immutable record of every resolved input, written before
execution and checked before every step.

A run-lock is a JSON object (schema c28-runlock/v1). `lock` validates it and writes <file>.sha256 holding the SHA256 of
its canonical form; `verify` recomputes both and checks the manifest's own hash. Validation refuses anything
unresolved (placeholders, tags instead of digests, "latest"), any credential material, frozen values that differ from
the manifest (seeds, operating point, caps, bootstrap), and a session order other than the frozen one. Once a run has
started, a changed input is a new run-lock and a new confirmation cohort, never an edit.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Dict, List

from . import frozen as F
from .schedule import session_order

SCHEMA = "c28-runlock/v1"
_SHA = re.compile(r"^[0-9a-f]{64}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$")
_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_UNRESOLVED = re.compile(r"(<[^>]*>|\bTODO\b|\bTBD\b|CHANGEME|\blatest\b|\$\{)", re.IGNORECASE)
_SECRET_KEYS = re.compile(r"(token|password|passwd|secret|client-key|client_key|private|credential|bearer)", re.I)

# Section 9's resolved inputs: key -> required type.
REQUIRED = {
    "schema": str, "run_id": str, "manifest": dict, "source": dict, "images": dict, "chart_sha256": str,
    "crd_sha256": str, "kubeconfig": dict, "infrastructure": dict, "slo": dict, "workload": dict, "capacity": dict,
    "cpu_tuning": dict, "arms": dict, "operating_point": dict, "seeds": dict, "session_order": list, "startup": dict,
    "queries": list, "collection": dict, "schedule": dict, "admission": dict, "data_licenses": list,
    "retention": dict, "scorer": dict, "activation_scope": dict, "fault_scope": dict, "acknowledgments": dict,
}
_RUN_LABEL = "c28.devkuban.com/run"


class RunLockError(ValueError):
    pass


def canonical(doc: Dict) -> bytes:
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _walk(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield f"{path}.{k}" if path else k, k, v
            yield from _walk(v, f"{path}.{k}" if path else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield f"{path}[{i}]", None, v
            yield from _walk(v, f"{path}[{i}]")


def problems(doc: Dict) -> List[str]:
    """Every reason the run-lock cannot be used (empty when it can)."""
    out = []
    if not isinstance(doc, dict):
        return ["the run-lock is not a JSON object"]
    for k, t in REQUIRED.items():
        if k not in doc:
            out.append(f"missing {k}")
        elif not isinstance(doc[k], t):
            out.append(f"{k} must be a {t.__name__}")
    if out:
        return out
    if doc["schema"] != SCHEMA:
        out.append(f"schema must be {SCHEMA}")
    for path, key, v in _walk(doc):
        if key is not None and _SECRET_KEYS.search(key) and key not in ("secret_values_excluded",):
            out.append(f"{path}: credential material is never recorded (reference it by location)")
        if isinstance(v, str) and _UNRESOLVED.search(v):
            out.append(f"{path}: unresolved value {v!r}")
    m = doc["manifest"]
    if not _SHA.match(str(m.get("sha256", ""))) or not m.get("path"):
        out.append("manifest needs path and sha256")
    if not re.match(r"^[0-9a-f]{40}$", str(doc["source"].get("commit", ""))):
        out.append("source.commit must be a full git commit")
    if not doc["images"]:
        out.append("images must list every image")
    for name, ref in doc["images"].items():
        if not _DIGEST_IMAGE.match(str(ref)):
            out.append(f"images.{name} must be pinned by digest")
    for k in ("chart_sha256", "crd_sha256"):
        if not _SHA.match(doc[k]):
            out.append(f"{k} must be a sha256")
    kc = doc["kubeconfig"]
    if set(kc) != {"path", "context", "cluster_uid"} or not _UUID.match(str(kc.get("cluster_uid", ""))):
        out.append("kubeconfig must be exactly {path, context, cluster_uid} (by path, never contents)")
    infra = doc["infrastructure"]
    roles = sorted(n.get("role") for n in infra.get("nodes", []))
    if roles != ["support", "workload"] or not all(_UUID.match(str(n.get("uid", ""))) for n in infra.get("nodes", [])):
        out.append("infrastructure.nodes must be exactly one workload and one support node, with UIDs")
    slo = doc["slo"]
    if (slo.get("p95_seconds"), slo.get("p99_seconds"), slo.get("error_fraction"), slo.get("timeout_seconds")) != (
            F.SLO_P95_S, F.SLO_P99_S, F.SLO_ERROR_FRACTION, F.REQUEST_TIMEOUT_S):
        out.append("slo differs from the frozen SLO")
    cap = doc["capacity"]
    c0, c = cap.get("c0_rps"), cap.get("c_rps")
    if not (isinstance(c0, (int, float)) and c0 > 0 and isinstance(c, str)):
        out.append("capacity needs c0_rps (number) and c_rps (decimal string)")
    else:
        from .render import capacity_rps
        if capacity_rps(c0) != c:
            out.append(f"capacity.c_rps must be 0.875 x c0 = {capacity_rps(c0)}")
    if doc["cpu_tuning"].get("selected_target_percent") not in F.HPA_CPU_CANDIDATES:
        out.append("cpu_tuning.selected_target_percent must be a tuning candidate")
    expected_arms = {f"{a}/{p}" for a in F.ARMS for p in F.PROFILES}
    if set(doc["arms"]) != expected_arms or not all(_SHA.match(str(v.get("rendered_sha256", "")))
                                                   for v in doc["arms"].values()):
        out.append("arms must list every arm/profile with its rendered manifest sha256")
    op = doc["operating_point"]
    if (op.get("capacity_ratio"), op.get("nominal_coverage_bps"), op.get("extra_max_replicas"),
            op.get("extra_max_percent"), op.get("pa_headroom_percent")) != (
            F.CAPACITY_RATIO, F.NOMINAL_COVERAGE_BPS, F.EXTRA_MAX_REPLICAS, F.EXTRA_MAX_PERCENT, F.PA_HEADROOM_PERCENT):
        out.append("operating_point differs from the frozen primary point")
    s = doc["seeds"]
    if (tuple(s.get("confirmation", ())), tuple(s.get("tuning", ())), s.get("order"), s.get("bootstrap"),
            s.get("bootstrap_resamples")) != (F.CONFIRMATION_SEEDS, F.TUNING_SEEDS, F.ORDER_SEED, F.BOOTSTRAP_SEED,
                                             F.BOOTSTRAP_RESAMPLES):
        out.append("seeds differ from the frozen seeds")
    if doc["session_order"] != session_order():
        out.append("session_order is not the frozen order generated from seed 20261106")
    for p in F.PROFILES:
        st = doc["startup"].get(p, {})
        lo, hi = F.PROFILE_P90_BANDS[p]
        if not (isinstance(st.get("creation_to_ready_p90_s"), (int, float)) and lo <= st["creation_to_ready_p90_s"] <= hi):
            out.append(f"startup.{p}: measured creation-to-Ready p90 must lie in {lo}-{hi} s")
        if not isinstance(st.get("samples"), int) or st["samples"] < 30:
            out.append(f"startup.{p}: at least 30 measured starts")
        if not isinstance(st.get("frozen_lead_seconds"), int) or st["frozen_lead_seconds"] % F.SAMPLE_INTERVAL_S:
            out.append(f"startup.{p}: frozen_lead_seconds must be a multiple of the 60 s grid")
    for k in ("admission", "retention", "workload"):
        if not doc[k]:
            out.append(f"{k} must not be empty")
    plans = doc["workload"].get("plans")
    if not isinstance(plans, dict) or not plans or not all(_SHA.match(str(v)) for v in plans.values()):
        out.append("workload.plans must map every k6 plan to its digest")
    else:
        from .render import target_name
        want = {f"scored/{s}/{target_name(*k.split('/'))}" for k in doc["arms"] if "/" in k
                for s in F.CONFIRMATION_SEEDS} if set(doc["arms"]) == {f"{a}/{p}" for a in F.ARMS for p in F.PROFILES} else set()
        missing = sorted(want - set(plans))
        if missing:
            out.append(f"workload.plans lacks the confirmation plans {missing[:3]}{' ...' if len(missing) > 3 else ''}")
    if not _SHA.match(str(doc["workload"].get("programs_sha256", ""))):
        out.append("workload.programs_sha256 must be a sha256")
    names = [q.get("name") for q in doc["queries"] if isinstance(q, dict)]
    if len(names) != len(doc["queries"]) or len(set(names)) != len(names):
        out.append("queries must be a list of uniquely named {name, expr, step}")
    for q in doc["queries"]:
        if not isinstance(q, dict) or set(q) != {"name", "expr", "step"} or "{target}" not in str(q.get("expr", "")) \
                or q.get("step") not in (5, 15, 60):
            out.append(f"query {q!r} must be {{name, expr with {{target}}, step 5|15|60}}")
    missing_q = sorted(set(F.REQUIRED_QUERIES) - set(names))
    if missing_q:
        out.append(f"queries lack {', '.join(missing_q)}")
    col = doc["collection"]
    if set(col) != {"query_api", "namespace"} or not re.match(r"^https?://[^@/\s]+(/\S*)?$", str(col.get("query_api", ""))):
        out.append("collection must be {query_api (http(s), no credentials), namespace}")
    fs = doc["fault_scope"]
    if set(fs) != {"namespace", "quota_name", "quota_uid", "run_label", "priority_class", "pods_normal"} or \
            not _UUID.match(str(fs.get("quota_uid", ""))) or fs.get("run_label") != f"{_RUN_LABEL}={doc['run_id']}" or \
            not isinstance(fs.get("pods_normal"), int) or isinstance(fs.get("pods_normal"), bool) or fs["pods_normal"] < 1:
        out.append("fault_scope must be {namespace, quota_name, quota_uid, run_label=<run label>=<run_id>, priority_class, "
                   "pods_normal (the approved original limit)}")
    if not (fs.get("namespace") == col.get("namespace") and fs.get("namespace") in doc["activation_scope"].get("namespaces", [])):
        out.append("fault_scope.namespace must be the collection namespace and inside the activation scope")
    sch = doc["schedule"]
    for k in ("start_utc", "end_utc"):
        if not _TS.match(str(sch.get(k, ""))):
            out.append(f"schedule.{k} must be an RFC 3339 UTC time")
    blocks = sch.get("blocks")
    want_blocks = {f"{a}/{p}/{s}" for a in F.ARMS for p in F.PROFILES for s in F.CONFIRMATION_SEEDS}
    if not isinstance(blocks, dict) or set(blocks) != want_blocks or not all(
            isinstance(v, list) and v and all(isinstance(t, str) and t.endswith("T00:00:00Z") and _TS.match(t)
                                              and sch.get("start_utc", "") <= t < sch.get("end_utc", "") for t in v)
            for v in blocks.values()):
        out.append("schedule.blocks must give every arm/profile/seed its permitted UTC-midnight start dates "
                   "(planned, then predeclared replacement slots), inside the schedule")
    if not _SHA.match(str(doc["scorer"].get("sha256", ""))) or doc["scorer"].get("bootstrap_seed") != F.BOOTSTRAP_SEED:
        out.append("scorer needs its code sha256 and bootstrap_seed 2808")
    scope = doc["activation_scope"]
    if not (_UUID.match(str(scope.get("cluster_uid", ""))) and scope.get("namespaces") and
            all(_UUID.match(str(u)) for u in scope.get("target_uids", [])) and scope.get("target_uids")):
        out.append("activation_scope needs the cluster UID, namespaces and target UIDs")
    if scope.get("cluster_uid") != kc.get("cluster_uid"):
        out.append("activation_scope.cluster_uid must be the kubeconfig's cluster")
    ack = doc["acknowledgments"]
    for role in ("executor", "reviewer"):
        a = ack.get(role, {})
        if not a.get("name") or not _TS.match(str(a.get("at", ""))):
            out.append(f"acknowledgments.{role} needs name and at")
    return out


def lock(path: str) -> str:
    with open(path) as fh:
        doc = json.load(fh)
    errs = problems(doc)
    if errs:
        raise RunLockError("; ".join(errs))
    digest = hashlib.sha256(canonical(doc)).hexdigest()
    sha_path = path + ".sha256"
    if os.path.exists(sha_path):
        raise RunLockError(f"{sha_path} exists: a run-lock is locked once (a change is a new run-lock)")
    with open(sha_path, "w") as fh:
        fh.write(f"{digest}  {os.path.basename(path)}\n")
    return digest


def verify(path: str, manifest_root: str = "") -> str:
    """The run-lock's digest when it is valid, unchanged since `lock`, and the manifest file still has its hash."""
    with open(path) as fh:
        doc = json.load(fh)
    errs = problems(doc)
    if errs:
        raise RunLockError("; ".join(errs))
    digest = hashlib.sha256(canonical(doc)).hexdigest()
    try:
        with open(path + ".sha256") as fh:
            recorded = fh.read().split()[0]
    except OSError as e:
        raise RunLockError(f"the run-lock is not locked ({e})") from e
    if recorded != digest:
        raise RunLockError("the run-lock changed after it was locked")
    mpath = doc["manifest"]["path"]
    if manifest_root:
        mpath = os.path.join(manifest_root, mpath)
    if os.path.exists(mpath) and sha256_file(mpath) != doc["manifest"]["sha256"]:
        raise RunLockError("the manifest's hash differs from the run-lock")
    if not os.path.exists(mpath):
        raise RunLockError(f"the manifest {mpath} is not available to verify")
    return digest


def locked_targets(doc: Dict) -> List[str]:
    """The fixture target (Deployment) names the run-lock covers."""
    from .render import target_name
    return sorted(target_name(*k.split("/")) for k in doc["arms"])


def load_verified(path: str, manifest_root: str = "") -> tuple:
    """(digest, document) of a verified run-lock."""
    digest = verify(path, manifest_root)
    with open(path) as fh:
        return digest, json.load(fh)
