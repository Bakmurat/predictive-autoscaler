#!/usr/bin/env python3
"""Export the canonical benchmark series from Prometheus, READ ONLY (Codex D-124).

This reads and writes NOTHING in the cluster. It issues one `/api/v1/query_range` GET against
a Prometheus endpoint (normally a temporary `kubectl port-forward`) and saves the response
verbatim, plus a provenance block. Raw data is never edited here: the validity mask, the grid
check and the bounded gap fill are applied downstream by `eval/replay_real.py`, using the
same `ml-engine` code the trainer runs, so the replay and the trainer cannot disagree.

The series and the mask are NOT parameters to be chosen at export time. Both are read from
`deploy/eks-benchmark/validity-mask.json`, which was predeclared before any benchmark
training (Codex C-14), so an export cannot quietly widen the window or change the query.

    kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 19090:9090 &
    eval/.venv/bin/python eval/export_benchmark_series.py \
        --base-url http://127.0.0.1:19090 --out eval/data/benchmark-real-<utc>.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MASK_PATH = ROOT / "deploy" / "eks-benchmark" / "validity-mask.json"


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: str) -> float:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


def query_range(base_url: str, query: str, start: float, end: float, step: int,
                timeout: int = 60) -> dict:
    params = urllib.parse.urlencode({"query": query, "start": f"{start:.0f}",
                                     "end": f"{end:.0f}", "step": str(step)})
    url = f"{base_url.rstrip('/')}/api/v1/query_range?{params}"
    with urllib.request.urlopen(url, timeout=timeout) as resp:   # GET only; no mutation
        return json.loads(resp.read().decode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:19090")
    ap.add_argument("--mask", default=str(MASK_PATH))
    ap.add_argument("--end", default=None,
                    help="UTC end, YYYY-MM-DDTHH:MM:SSZ (default: now, floored to the grid)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    mask = json.loads(Path(args.mask).read_text())
    query, step = mask["series"], int(mask["grid_seconds"])
    start = parse_iso(mask["benchmark_history_start"])
    end = parse_iso(args.end) if args.end else datetime.now(tz=timezone.utc).timestamp()
    end = (int(end) // step) * step          # align the last slot to the declared grid

    body = query_range(args.base_url, query, start, end, step)
    if body.get("status") != "success":
        raise SystemExit(f"prometheus refused the query: {body}")
    result = body["data"]["result"]
    n = len(result[0]["values"]) if result else 0

    payload = {
        "kind": "benchmark_series_export",
        "exported_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "access": "read-only GET /api/v1/query_range via a temporary port-forward; "
                  "no cluster object was created, changed or deleted",
        "provenance": {
            "query": query,
            "step_seconds": step,
            "start": iso(start), "end": iso(end),
            "validity_mask_version": mask.get("version"),
            "validity_mask_sha256": hashlib.sha256(
                Path(args.mask).read_bytes()).hexdigest(),
            "benchmark_history_start": mask["benchmark_history_start"],
            "mask_applied_here": False,
            "mask_applied_by": "eval/replay_real.py, using ml-engine/data/gapfill.py -- the "
                               "same rule the trainer applies",
        },
        "raw_points": n,
        "response": body,
    }
    Path(args.out).write_text(json.dumps(payload, indent=2))
    print(f"{n} raw points  {iso(start)} -> {iso(end)}  step {step}s")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
