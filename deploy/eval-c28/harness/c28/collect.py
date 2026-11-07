"""Evidence collector (EVAL-C28-MANIFEST sections 7 and 9): Prometheus-API range queries, k6 summaries, decision ledgers
and any other run artifact, written to an off-cluster bundle with a SHA256 for every file.

It reads only: an HTTP Prometheus-compatible query API (no credentials in the URL) and local files the executor already
copied off the cluster. It never talks to the Kubernetes API. A failed or partial query is recorded as an evidence gap
in the index and makes the collection exit non-zero; it is never replaced by an empty series.

Bundle layout (a new, empty directory):
  index.json            run-lock digest, time range, every query (expression, step, chunks, status), every file
  queries/<name>/<target>/<chunk>.json   the raw API answers
  files/<label>/<name>  copied artifacts (k6 summaries, decision ledgers, journals, logs)
  SHA256SUMS            sha256 of every other file, sorted by path
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

MAX_POINTS = 10_000          # per range request (Prometheus refuses more than 11,000)

# The default query set. {target} and {namespace} are filled per target. Steps in seconds.
DEFAULT_QUERIES = [
    {"name": "offered_rps", "step": 15,
     "expr": 'sum(rate(c28_ingress_requests_total{{target="{target}"}}[60s]))'},
    {"name": "responses_per_minute", "step": 60,
     "expr": 'sum by (code) (increase(c28_ingress_responses_total{{target="{target}"}}[1m]))'},
    {"name": "latency_buckets_per_minute", "step": 60,
     "expr": 'sum by (le, outcome) (increase(c28_ingress_duration_seconds_bucket{{target="{target}"}}[1m]))'},
    {"name": "ready_replicas", "step": 5,
     "expr": 'max(kube_deployment_status_replicas_ready{{namespace="{namespace}",deployment="{target}"}})'},
    {"name": "desired_replicas", "step": 5,
     "expr": 'max(kube_deployment_spec_replicas{{namespace="{namespace}",deployment="{target}"}})'},
    {"name": "pod_replicas", "step": 5,
     "expr": 'max(kube_deployment_status_replicas{{namespace="{namespace}",deployment="{target}"}})'},
    {"name": "requested_cpu_cores", "step": 5,
     "expr": 'sum(kube_pod_container_resource_requests{{namespace="{namespace}",resource="cpu",pod=~"{target}-.*"}})'},
]


class CollectError(RuntimeError):
    pass


def _ts(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def chunks(start: int, end: int, step: int) -> List[tuple]:
    """[start, end] in pieces of at most MAX_POINTS samples, aligned on step, without overlap."""
    if end <= start or step <= 0:
        raise CollectError("the range must be positive")
    span = (MAX_POINTS - 1) * step
    out, a = [], start
    while a <= end:
        b = min(a + span, end)
        out.append((a, b))
        a = b + step
    return out


def _fetch(base: str, expr: str, a: int, b: int, step: int, timeout: float) -> Dict:
    q = urllib.parse.urlencode({"query": expr, "start": a, "end": b, "step": step, "nocache": 1})
    req = urllib.request.Request(f"{base.rstrip('/')}/api/v1/query_range?{q}", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (the base URL is validated as http(s))
        return json.loads(resp.read())


def _sha(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def collect(out_dir: str, base_url: str, targets: List[str], namespace: str, start: int, end: int, runlock_digest: str,
            files: Optional[Dict[str, List[str]]] = None, queries: Optional[List[Dict]] = None, timeout: float = 60,
            fetch: Callable = _fetch) -> Dict:
    parsed = urllib.parse.urlparse(base_url)
    if parsed.scheme not in ("http", "https") or parsed.username or parsed.password or not parsed.netloc:
        raise CollectError("the query API must be an http(s) URL without credentials")
    if os.path.exists(out_dir) and os.listdir(out_dir):
        raise CollectError(f"{out_dir} is not empty: a bundle is written once")
    os.makedirs(out_dir, exist_ok=True)
    index = {"schema": "c28-evidence/v1", "runlock_sha256": runlock_digest, "start": _ts(start), "end": _ts(end),
             "query_api": f"{parsed.scheme}://{parsed.netloc}{parsed.path}", "namespace": namespace,
             "collected_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "queries": [], "files": [],
             "gaps": []}
    for q in queries or DEFAULT_QUERIES:
        for target in targets:
            expr = q["expr"].format(target=target, namespace=namespace)
            entry = {"name": q["name"], "target": target, "expr": expr, "step": q["step"], "chunks": []}
            for i, (a, b) in enumerate(chunks(start, end, q["step"])):
                rel = os.path.join("queries", q["name"], target, f"{i:04d}.json")
                status = "ok"
                try:
                    answer = fetch(base_url, expr, a, b, q["step"], timeout)
                    if answer.get("status") != "success":
                        status = f"error: {answer.get('errorType', '')} {answer.get('error', '')}".strip()
                    elif answer.get("isPartial"):
                        status = "partial"
                except (urllib.error.URLError, OSError, ValueError) as e:
                    answer, status = {"status": "error", "error": str(e)}, f"error: {e}"
                path = os.path.join(out_dir, rel)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as fh:
                    json.dump(answer, fh, sort_keys=True)
                entry["chunks"].append({"file": rel, "start": _ts(a), "end": _ts(b), "status": status})
                if status != "ok":
                    index["gaps"].append({"query": q["name"], "target": target, "start": _ts(a), "end": _ts(b),
                                          "status": status})
            index["queries"].append(entry)
    for label, paths in (files or {}).items():
        for src in paths:
            if not os.path.isfile(src):
                index["gaps"].append({"file": src, "status": "missing"})
                continue
            rel = os.path.join("files", label, os.path.basename(src))
            dst = os.path.join(out_dir, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copyfile(src, dst)
            index["files"].append({"label": label, "source": os.path.abspath(src), "file": rel, "sha256": _sha(dst)})
    with open(os.path.join(out_dir, "index.json"), "w") as fh:
        json.dump(index, fh, sort_keys=True, indent=1)
    write_sums(out_dir)
    return index


def write_sums(out_dir: str) -> None:
    lines = []
    for root, _, names in os.walk(out_dir):
        for n in names:
            rel = os.path.relpath(os.path.join(root, n), out_dir)
            if rel != "SHA256SUMS":
                lines.append(f"{_sha(os.path.join(out_dir, rel))}  {rel}")
    with open(os.path.join(out_dir, "SHA256SUMS"), "w") as fh:
        fh.write("\n".join(sorted(lines, key=lambda l: l.split("  ", 1)[1])) + "\n")


def verify_bundle(out_dir: str) -> List[str]:
    """Files whose hash differs from SHA256SUMS, missing files, and files not listed (empty when intact)."""
    problems, listed = [], set()
    with open(os.path.join(out_dir, "SHA256SUMS")) as fh:
        for line in fh:
            if not line.strip():
                continue
            digest, rel = line.rstrip("\n").split("  ", 1)
            listed.add(rel)
            p = os.path.join(out_dir, rel)
            if not os.path.isfile(p):
                problems.append(f"missing {rel}")
            elif _sha(p) != digest:
                problems.append(f"changed {rel}")
    for root, _, names in os.walk(out_dir):
        for n in names:
            rel = os.path.relpath(os.path.join(root, n), out_dir)
            if rel != "SHA256SUMS" and rel not in listed:
                problems.append(f"unlisted {rel}")
    return problems
