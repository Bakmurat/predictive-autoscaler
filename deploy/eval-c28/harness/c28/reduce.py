"""Reduce an evidence bundle to the per-window block metrics the scorer uses (EVAL-C28-MANIFEST section 7).

Per one-minute bin, from the independent ingress: requests, failures (any non-2xx/3xx, timeouts and gateway errors
included), and successful-request p95/p99 from the merged bucket counts of that minute (never an average of
percentiles). A bin is an SLO-violation minute when p95 > 200 ms, p99 > 500 ms or the failure fraction > 0.1 %; a bin
with failures and no success violates; a bin with no requests or no data is unknown, never healthy. Capacity proxies use
n*(t) = offered_RPS(t) / c at 5 s resolution: surplus = integral max(Ready - n*, 0), shortage = integral max(n* - Ready,
0); intervals without both signals are unknown and lower the recorder coverage.
"""

from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from . import frozen as F

DAY_S = 86_400


def load_series(bundle: str, name: str, target: str) -> List[Tuple[Dict[str, str], List[Tuple[int, float]]]]:
    """[(labels, [(t, value)])] merged over the chunks of one query (non-finite samples dropped)."""
    with open(os.path.join(bundle, "index.json")) as fh:
        index = json.load(fh)
    entry = next((q for q in index["queries"] if q["name"] == name and q["target"] == target), None)
    if entry is None:
        return []
    merged: Dict[str, Tuple[Dict[str, str], Dict[int, float]]] = {}
    for ch in entry["chunks"]:
        if ch["status"] != "ok":
            continue
        with open(os.path.join(bundle, ch["file"])) as fh:
            ans = json.load(fh)
        for s in ans.get("data", {}).get("result", []):
            key = json.dumps(s.get("metric", {}), sort_keys=True)
            labels, points = merged.setdefault(key, (s.get("metric", {}), {}))
            for t, v in s.get("values", []):
                f = float(v)
                if math.isfinite(f):
                    points[int(float(t))] = f
    return [(labels, sorted(points.items())) for labels, points in merged.values()]


def quantile(q: float, buckets: List[Tuple[float, float]]) -> Optional[float]:
    """histogram_quantile on cumulative (le, count) pairs: linear inside the bucket; None without observations."""
    buckets = sorted(buckets)
    if not buckets or buckets[-1][1] <= 0:
        return None
    total = buckets[-1][1]
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in buckets:
        if c >= rank:
            if math.isinf(le):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return buckets[-1][0]


def minute_bins(bundle: str, target: str, block_start: int) -> List[Dict]:
    """1,440 bins: requests, failures, success p95/p99, status (ok | violation | unknown) and the failing components."""
    req = [0.0] * 1440
    fail = [0.0] * 1440
    seen = [False] * 1440
    for labels, pts in load_series(bundle, "responses_per_minute", target):
        code = labels.get("code", "")
        failed = not (code.isdigit() and 200 <= int(code) < 400)
        for t, v in pts:
            k = (t - block_start) // 60 - 1          # an increase over (t-60, t] belongs to the minute ending at t
            if 0 <= k < 1440:
                seen[k] = True
                req[k] += v
                if failed:
                    fail[k] += v
    succ_buckets: Dict[int, Dict[float, float]] = defaultdict(dict)
    for labels, pts in load_series(bundle, "latency_buckets_per_minute", target):
        if labels.get("outcome") != "success":
            continue
        le = float("inf") if labels.get("le") in ("+Inf", "Inf") else float(labels.get("le", "nan"))
        for t, v in pts:
            k = (t - block_start) // 60 - 1
            if 0 <= k < 1440 and not math.isnan(le):
                succ_buckets[k][le] = succ_buckets[k].get(le, 0.0) + v
    bins = []
    for k in range(1440):
        b = {"minute": k, "requests": req[k], "failures": fail[k], "p95": None, "p99": None, "status": "unknown",
             "violations": []}
        if seen[k] and req[k] > 0:
            hist = sorted(succ_buckets.get(k, {}).items())
            b["p95"], b["p99"] = quantile(0.95, hist), quantile(0.99, hist)
            frac = fail[k] / req[k]
            if frac > F.SLO_ERROR_FRACTION:
                b["violations"].append("errors")
            if b["p95"] is None:
                b["violations"].append("no_success")
            else:
                if b["p95"] > F.SLO_P95_S:
                    b["violations"].append("p95")
                if b["p99"] > F.SLO_P99_S:
                    b["violations"].append("p99")
            b["status"] = "violation" if b["violations"] else "ok"
        bins.append(b)
    return bins


def _held(series: List[Tuple[int, float]], max_age: int):
    """A step function over the samples: the latest value at most max_age seconds old, else None."""
    ts = [t for t, _ in series]
    vs = [v for _, v in series]
    import bisect

    def at(t: int) -> Optional[float]:
        i = bisect.bisect_right(ts, t) - 1
        if i < 0 or t - ts[i] > max_age:
            return None
        return vs[i]
    return at


def capacity_samples(bundle: str, target: str, block_start: int, capacity_rps: float, step: int = 5) -> List[Dict]:
    def single(name):
        s = load_series(bundle, name, target)
        return s[0][1] if s else []
    offered = _held(single("offered_rps"), 2 * F.SCRAPE_INTERVAL_S)
    ready = _held(single("ready_replicas"), 3 * step)
    pods = _held(single("pod_replicas"), 3 * step)
    out = []
    for t in range(block_start, block_start + DAY_S, step):
        o, r, p = offered(t), ready(t), pods(t)
        out.append({"t": t, "n_star": None if o is None else o / capacity_rps, "ready": r, "pods": p})
    return out


def block_metrics(bundle: str, target: str, block_start: int, capacity_rps: float) -> Dict:
    """Per-window and whole-day metrics of one scored block of one arm."""
    bins = minute_bins(bundle, target, block_start)
    caps = capacity_samples(bundle, target, block_start, capacity_rps)
    step = 5

    def window(start_h: float, end_h: float) -> Dict:
        lo, hi = int(start_h * 60), int(end_h * 60)
        wb = bins[lo:hi]
        known = [b for b in wb if b["status"] != "unknown"]
        requests = sum(b["requests"] for b in wb)
        failures = sum(b["failures"] for b in wb)
        wc = [c for c in caps if block_start + start_h * 3600 <= c["t"] < block_start + end_h * 3600]
        full = [c for c in wc if c["n_star"] is not None and c["ready"] is not None]
        surplus = sum(max(c["ready"] - c["n_star"], 0.0) for c in full) * step
        shortage = sum(max(c["n_star"] - c["ready"], 0.0) for c in full) * step
        return {"minutes": hi - lo, "observed_minutes": len(known),
                "slo_violation_minutes": sum(1 for b in known if b["status"] == "violation"),
                "slo_ok_minutes": sum(1 for b in known if b["status"] == "ok"),
                "requests": requests, "failures": failures,
                "error_fraction": failures / requests if requests else None,
                "surplus_replica_seconds": surplus, "shortage_replica_seconds": shortage,
                "ready_replica_seconds": sum(c["ready"] for c in full) * step,
                "provisioned_replica_seconds": sum(c["pods"] for c in wc if c["pods"] is not None) * step,
                "capacity_coverage": len(full) / len(wc) if wc else 0.0}
    out = {"windows": {wid: window(s, e) for wid, s, e, _ in F.WINDOWS}, "day": window(0, 24)}
    out["recorder_coverage"] = min(out["day"]["observed_minutes"] / 1440, out["day"]["capacity_coverage"])
    return out
