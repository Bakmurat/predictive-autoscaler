"""Reduce an evidence bundle to the per-window block metrics the scorer uses (EVAL-C28-MANIFEST section 7).

Per one-minute bin: the scheduled arrivals of the locked k6 plan (the denominator, section 7), the client's terminal
outcomes (k6: dropped iterations, failures including timeouts and no-response, successes), the ingress's admitted count
and successful-request p95/p99 from the merged bucket counts of that minute (never an average of percentiles). A bin
is an SLO-violation minute when p95 > 200 ms, p99 > 500 ms or (dropped + failed) / scheduled > 0.1 %; a bin with
failures and no success violates; a bin without client data, or with successes but no latency record, is unknown,
never healthy. Per window, scheduled = dropped + dispatched is reconciled (1 % tolerance); scheduled arrivals with no
observed outcome count as failures; dispatched minus admitted are the pre-ingress failures (reported, never counted
twice). Capacity proxies use
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


def _per_minute(bundle, name, target, block_start, by_label=None):
    """{minute: value} (or {minute: {label value: value}}) of a per-minute increase query; an increase over (t-60, t]
    belongs to the minute ending at t."""
    out: Dict[int, object] = {}
    for labels, pts in load_series(bundle, name, target):
        for t, v in pts:
            k = (t - block_start) // 60 - 1
            if not 0 <= k < 1440:
                continue
            if by_label is None:
                out[k] = out.get(k, 0.0) + v
            else:
                d = out.setdefault(k, {})
                key = labels.get(by_label, "")
                d[key] = d.get(key, 0.0) + v
    return out


def _ok_status(code: str) -> bool:
    return code.isdigit() and 200 <= int(code) < 400


def minute_bins(bundle: str, target: str, block_start: int, scheduled: List[float]) -> List[Dict]:
    """1,440 bins. The denominator is the scheduled arrivals of the locked plan; each scheduled arrival has exactly one
    terminal outcome on the client: dropped by the generator (k6 dropped_iterations), failed (no HTTP response,
    timeout, or a non-2xx/3xx status) or succeeded. Failures are counted once, from the client; the ingress supplies
    successful latency and the admitted count (reconciliation only)."""
    if len(scheduled) != 1440:
        raise ValueError("the plan must schedule 1,440 minutes")
    k6 = _per_minute(bundle, "k6_requests_per_minute", target, block_start, by_label="status")
    dropped = _per_minute(bundle, "k6_dropped_per_minute", target, block_start)
    admitted = _per_minute(bundle, "ingress_admitted_per_minute", target, block_start)
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
        codes = k6.get(k)
        disp = sum(codes.values()) if codes else 0.0
        fail = sum(v for c, v in codes.items() if not _ok_status(c)) if codes else 0.0
        b = {"minute": k, "scheduled": scheduled[k], "dispatched": disp, "dropped": dropped.get(k, 0.0),
             "client_failures": fail, "admitted": admitted.get(k, 0.0), "p95": None, "p99": None, "status": "unknown",
             "violations": []}
        # k6 emits dropped_iterations only once an iteration is dropped: with client data present, no series means 0.
        if codes is not None and scheduled[k] > 0:
            frac = (b["dropped"] + fail) / scheduled[k]
            hist = sorted(succ_buckets.get(k, {}).items())
            successes = disp - fail
            b["p95"], b["p99"] = quantile(0.95, hist), quantile(0.99, hist)
            if frac > F.SLO_ERROR_FRACTION:
                b["violations"].append("errors")
            if successes <= 0:
                b["violations"].append("no_success")
            elif b["p95"] is None:
                b["violations"] = []                 # successes without a latency record: a recorder gap, unknown
                bins.append(b)
                continue
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


def block_metrics(bundle: str, target: str, block_start: int, capacity_rps: float, scheduled: List[float]) -> Dict:
    """Per-window and whole-day metrics of one scored block of one arm, with the arrival reconciliation."""
    bins = minute_bins(bundle, target, block_start, scheduled)
    caps = capacity_samples(bundle, target, block_start, capacity_rps)
    step = 5

    def window(start_h: float, end_h: float) -> Dict:
        lo, hi = int(start_h * 60), int(end_h * 60)
        wb = bins[lo:hi]
        known = [b for b in wb if b["status"] != "unknown"]
        sched = sum(b["scheduled"] for b in wb)
        disp = sum(b["dispatched"] for b in wb)
        drop = sum(b["dropped"] for b in wb)
        cfail = sum(b["client_failures"] for b in wb)
        adm = sum(b["admitted"] for b in wb)
        unaccounted = max(sched - drop - disp, 0.0)        # scheduled arrivals with no observed outcome: failures
        failures = drop + cfail + unaccounted
        wc = [c for c in caps if block_start + start_h * 3600 <= c["t"] < block_start + end_h * 3600]
        full = [c for c in wc if c["n_star"] is not None and c["ready"] is not None]
        surplus = sum(max(c["ready"] - c["n_star"], 0.0) for c in full) * step
        shortage = sum(max(c["n_star"] - c["ready"], 0.0) for c in full) * step
        return {"minutes": hi - lo, "observed_minutes": len(known),
                "slo_violation_minutes": sum(1 for b in known if b["status"] == "violation"),
                "slo_ok_minutes": sum(1 for b in known if b["status"] == "ok"),
                "scheduled": sched, "dispatched": disp, "dropped": drop, "client_failures": cfail, "admitted": adm,
                "unaccounted": unaccounted, "pre_ingress_failures": max(disp - adm, 0.0),
                "reconciled": abs(sched - drop - disp) <= F.RECONCILE_TOLERANCE * sched if sched else False,
                "dispatch_fraction": disp / sched if sched else 0.0,
                "requests": sched, "failures": failures, "error_fraction": failures / sched if sched else None,
                "surplus_replica_seconds": surplus, "shortage_replica_seconds": shortage,
                "ready_replica_seconds": sum(c["ready"] for c in full) * step,
                "provisioned_replica_seconds": sum(c["pods"] for c in wc if c["pods"] is not None) * step,
                "capacity_coverage": len(full) / len(wc) if wc else 0.0}
    out = {"windows": {wid: window(s, e) for wid, s, e, _ in F.WINDOWS}, "day": window(0, 24)}
    out["recorder_coverage"] = min(out["day"]["observed_minutes"] / 1440, out["day"]["capacity_coverage"])
    out["dispatch_fraction_min"] = min(w["dispatch_fraction"] for w in out["windows"].values())
    out["reconciled"] = all(w["reconciled"] for w in out["windows"].values())
    return out
