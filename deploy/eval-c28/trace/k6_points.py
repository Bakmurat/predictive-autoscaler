#!/usr/bin/env python3
"""Per-minute METRIC-POINT buckets of k6 counter metrics from a `k6 run --out json=...` file (trace demo, contract v3).

What this measures, and what it does not:

* Each counter sample contributes its VALUE (a `dropped_iterations` sample can carry more than 1), never just 1.
* Buckets are anchored to the scenario start the caller declares (`--scenario-start`, from the run's own start
  receipt), never to the first metric point in the file.
* The time of a k6 metric point is when k6 emitted it. For HTTP metrics (`http_reqs`, `http_req_*`) and `iterations`
  that is request/iteration COMPLETION, so a request dispatched near the end of minute k with a slow response lands in
  minute k+1. These buckets therefore are NOT dispatch times and NOT ingress receipt times. They are reported only as
  "metric-point buckets"; dispatched and received per-minute counts need their own sources (contract v3, timing).

Standard library only; run with `python -I`.

  k6_points.py --points points.json --scenario-start 2026-11-09T04:20:00Z --out buckets.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime
from typing import Dict, Iterable, Optional, Sequence

COUNTERS = ("http_reqs", "iterations", "dropped_iterations")
TIME_BASIS = ("k6 metric emission time (HTTP metrics and iterations: completion), anchored to the declared scenario "
              "start; NOT dispatch times and NOT ingress receipt times")


class PointsError(Exception):
    pass


def parse_time(s: str) -> float:
    if not isinstance(s, str) or not s:
        raise PointsError(f"bad timestamp {s!r}")
    try:
        t = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        raise PointsError(f"bad timestamp {s!r}") from None
    if t.tzinfo is None:
        raise PointsError(f"timestamp without a timezone: {s!r}")
    return t.timestamp()


def bucket(lines: Iterable[str], scenario_start: float, metrics: Sequence[str] = COUNTERS) -> Dict:
    """Sum counter sample values per whole minute since `scenario_start`. Samples before the start are counted apart."""
    if not math.isfinite(scenario_start):
        raise PointsError("scenario start must be finite")
    out = {m: {} for m in metrics}
    before = {m: 0.0 for m in metrics}
    totals = {m: 0.0 for m in metrics}
    for n, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        try:
            o = json.loads(line)
        except json.JSONDecodeError:
            raise PointsError(f"line {n}: not JSON") from None
        if not isinstance(o, dict) or o.get("type") != "Point" or o.get("metric") not in out:
            continue
        data = o.get("data") or {}
        v = data.get("value")
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
            raise PointsError(f"line {n}: counter value {v!r} is not a finite non-negative number")
        t = parse_time(data.get("time"))
        m = o["metric"]
        totals[m] += v
        if t < scenario_start:
            before[m] += v
            continue
        k = int((t - scenario_start) // 60)
        out[m][k] = out[m].get(k, 0.0) + v
    return {"schema": "k6-metric-point-buckets/v1", "label": "metric-point buckets", "time_basis": TIME_BASIS,
            "buckets": {m: {str(k): out[m][k] for k in sorted(out[m])} for m in metrics},
            "before_start": before, "totals": totals}


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--points", required=True)
    ap.add_argument("--scenario-start", required=True, help="ISO-8601 with timezone, from the run's start receipt")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    try:
        if os.path.lexists(a.out):
            raise PointsError(f"{a.out} exists; outputs are never overwritten")
        with open(a.points, encoding="utf-8") as f:
            res = bucket(f, parse_time(a.scenario_start))
        res["scenario_start"] = a.scenario_start
        with open(a.out, "x", encoding="utf-8") as f:
            json.dump(res, f, indent=1, sort_keys=True)
            f.write("\n")
        return 0
    except (PointsError, OSError) as e:
        print(f"k6_points: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

