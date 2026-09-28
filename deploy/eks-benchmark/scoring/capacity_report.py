#!/usr/bin/env python3
"""Per-arm capacity accounting over a live window (descriptive; not a scored benchmark result).

For every arm and every sample time (30 s by default) it compares Ready replicas with the replicas
the arm's own served traffic required, ceil(rpm / (targetRPS x 60)), and reports:
  shortage replica-minutes  sum of max(0, required - ready) x step
  surplus replica-minutes   sum of max(0, ready - required) x step
  short minutes             minutes with at least one replica missing
  mean Ready replicas, scale changes (Ready count changes), minutes at the replica ceiling,
  and the sample coverage (samples with both series present / expected samples).

A missing sample is counted and never filled. The traffic series is each arm's own destination
request rate (the same canonical query as the load gates), so an arm is judged on the demand it
actually served. Nothing here measures latency or errors.

Usage:
  capacity_report.py --prom http://localhost:9090 --start 2026-09-28T00:00:00Z --end 2026-09-29T00:00:00Z \
      [--arms nginx-ensemble,nginx-seasonal,nginx-test,nginx-reactive,myapptwo] [--per-pod-rpm 600] \
      [--ceiling 12] [--step 30] [--json out.json]
"""
import argparse
import datetime
import json
import math
import sys
import urllib.parse
import urllib.request

ARMS = {"nginx-ensemble": "E1 seasonal ensemble + q90", "nginx-ensemble-q95": "E2 seasonal ensemble + q95",
        "nginx-seasonal": "S1 seasonal only",
        "nginx-test": "neural/seasonal hybrid", "nginx-reactive": "reactive only", "myapptwo": "KEDA"}
RPM_Q = ('sum(rate(istio_requests_total{{reporter="destination",destination_workload="{app}",'
         'destination_workload_namespace="demo"}}[1m])) * 60')
READY_Q = 'max(kube_deployment_status_replicas_ready{{namespace="demo",deployment="{app}"}})'


def summarize(ready, rpm, start, end, step, per_pod_rpm, ceiling):
    """ready, rpm: {epoch_second: value}. Pure function; every sample is on start + k*step."""
    expected = int((end - start) // step)
    short = surplus = short_min = ceiling_min = 0.0
    changes, prev, total_ready, n = 0, None, 0.0, 0
    for k in range(expected):
        t = start + k * step
        if t not in ready or t not in rpm:
            continue
        r, d = ready[t], rpm[t]
        need = max(1, math.ceil(d / per_pod_rpm - 1e-9)) if d > 0 else 1
        gap = need - r
        short += max(0.0, gap) * step / 60.0
        surplus += max(0.0, -gap) * step / 60.0
        short_min += (step / 60.0) if gap > 0 else 0.0
        ceiling_min += (step / 60.0) if r >= ceiling else 0.0
        if prev is not None and r != prev:
            changes += 1
        prev = r
        total_ready += r
        n += 1
    return {"samples": n, "expected_samples": expected,
            "coverage": round(n / expected, 4) if expected else None,
            "shortage_replica_minutes": round(short, 2), "surplus_replica_minutes": round(surplus, 2),
            "short_minutes": round(short_min, 2), "minutes_at_ceiling": round(ceiling_min, 2),
            "mean_ready_replicas": round(total_ready / n, 3) if n else None, "ready_changes": changes}


def fetch(prom, query, start, end, step):
    q = urllib.parse.urlencode({"query": query, "start": start, "end": end - step, "step": step})
    res = json.load(urllib.request.urlopen(f"{prom}/api/v1/query_range?{q}", timeout=60))["data"]["result"]
    return {int(float(t)): float(v) for t, v in res[0]["values"]} if res else {}


def iso_ts(value):
    return int(datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--arms", default=",".join(ARMS))
    ap.add_argument("--per-pod-rpm", type=float, default=600.0)
    ap.add_argument("--ceiling", type=int, default=12)
    ap.add_argument("--step", type=int, default=30)
    ap.add_argument("--json")
    a = ap.parse_args()
    start, end = iso_ts(a.start), iso_ts(a.end)
    start -= start % a.step
    report = {"window": [a.start, a.end], "step_seconds": a.step, "per_pod_rpm": a.per_pod_rpm,
              "ceiling": a.ceiling, "arms": {}}
    for app in a.arms.split(","):
        ready = fetch(a.prom, READY_Q.format(app=app), start, end, a.step)
        rpm = fetch(a.prom, RPM_Q.format(app=app), start, end, a.step)
        report["arms"][app] = {"label": ARMS.get(app, app),
                               **summarize(ready, rpm, start, end, a.step, a.per_pod_rpm, a.ceiling)}
    print(f"window {a.start} .. {a.end}  (required = ceil(own rpm / {a.per_pod_rpm:g}); step {a.step}s)")
    print(f"{'arm':28s} {'short':>8s} {'surplus':>9s} {'short-min':>9s} {'mean pods':>9s} {'changes':>7s} {'@ceil':>6s} {'cover':>6s}")
    for app, r in report["arms"].items():
        print(f"{r['label']:28s} {r['shortage_replica_minutes']:8.1f} {r['surplus_replica_minutes']:9.1f} "
              f"{r['short_minutes']:9.1f} {r['mean_ready_replicas'] or 0:9.2f} {r['ready_changes']:7d} "
              f"{r['minutes_at_ceiling']:6.1f} {r['coverage'] or 0:6.3f}")
    if a.json:
        json.dump(report, open(a.json, "w"), indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
