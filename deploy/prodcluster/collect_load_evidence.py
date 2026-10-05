#!/usr/bin/env python3
"""Hourly load evidence for the six benchmark arms on a VictoriaMetrics cluster (prodcluster campaign).

Replaces deploy/eks-benchmark/scripts/collect-k6-summaries.py for this campaign. Differences, each from a measured fact:
  * counters are read as RAW samples and incremented here, not with increase(): a k6 counter series that first appears
    inside the hour (k6 emits k6_dropped_iterations_total only after the first drop; its first sample was 190 in the
    2026-10-05 positive control, never 0) is counted from 0, and a decrease is a restart (the post-reset value is added);
  * failed requests are increments of k6_http_reqs_total{expected_response="false"} (not the failure-rate gauge);
  * an absent dropped-iterations series means ZERO only under the validated sparse-emission rule: the generator ran the
    whole hour (no counter reset, no container restart, no pod replacement) and its delivered counters were pushed at
    every interval (largest gap <= 30 s, first and last samples within 30 s of the hour edges); otherwise INCOMPLETE;
  * latency is SERVER-SIDE: p95 from Istio's classic histogram istio_request_duration_milliseconds_bucket
    (reporter="destination"), bucket increments summed per arm by `le`, then the Prometheus interpolation. k6's own
    latency (native histograms) is not ingested by VictoriaMetrics v1.116.
Gate (protocol §3, prodcluster revision P4): delivered within ±5 % of planned; dropped <= 0.05 % of planned; failed
<= 0.1 % of delivered; destination-observed within ±5 % of delivered. Any missing input makes the hour INCOMPLETE.

Usage: collect_load_evidence.py --prom http://localhost:18481/select/0/prometheus [--hour 2026-10-05T06:00:00Z] [--json out]
(default hour: the last completed UTC hour). The query base is any Prometheus-compatible API (vmselect).
"""
import argparse, datetime, json, math, os, sys, urllib.parse, urllib.request

APPS = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
NAMESPACE = "demo"
PUSH_GAP_MAX = 30.0          # k6 pushes every 10 s; allow two missed pushes
EDGE_MAX = 30.0
LOOKBACK = 1800              # seconds before the hour to find each series' base value

_WORKLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "eks-benchmark", "workload", "challenge-v1")
sys.path.insert(0, _WORKLOAD)


# ---------- pure functions (tested offline) ----------

def finite(samples):
    """Drop non-finite samples: VictoriaMetrics returns vmagent's staleness markers (NaN) in raw range reads; they mark
    the end of a series (a scraped pod disappeared), they are not values."""
    return [(t, v) for t, v in samples if math.isfinite(v)]


def series_increment(samples, t0, t1):
    """Increment of one counter series over (t0, t1]. samples: [(t, v)] sorted. Base = last value at/before t0, or 0 if
    the series has no sample at/before t0 (it started inside the window, so its first sample counts). A decrease is a
    reset: the post-reset value is added. Returns (increment, resets)."""
    base, inc, resets = 0.0, 0.0, 0
    for t, v in samples:
        if t <= t0:
            base = v
    prev = base
    for t, v in samples:
        if t0 < t <= t1:
            if v >= prev:
                inc += v - prev
            else:
                inc += v
                resets += 1
            prev = v
    return inc, resets


def coverage(samples, t0, t1):
    """(ok, detail) for a pushed counter over [t0, t1]: samples at every push interval."""
    ts = [t for t, _ in samples if t0 - EDGE_MAX <= t <= t1]
    if not ts:
        return False, "no samples"
    inside = [t for t in ts if t0 <= t <= t1] or ts
    first, last = min(inside), max(inside)
    gaps = [b - a for a, b in zip(ts, ts[1:])]
    gap = max(gaps) if gaps else (t1 - t0)
    ok = first - t0 <= EDGE_MAX and t1 - last <= EDGE_MAX and gap <= PUSH_GAP_MAX
    return ok, f"first +{first - t0:.0f}s, last -{t1 - last:.0f}s, max gap {gap:.0f}s"


def histogram_quantile(q, buckets):
    """Prometheus histogram_quantile on {le: cumulative count}; le may be the string '+Inf'."""
    items = sorted(((math.inf if str(le) in ("+Inf", "inf") else float(le), c) for le, c in buckets.items()), key=lambda x: x[0])
    if not items or items[-1][0] != math.inf:
        return None
    total = items[-1][1]
    if total <= 0:
        return None
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in items:
        if c >= rank:
            if le == math.inf:
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return None


def evaluate(planned, delivered, failed, dropped, observed):
    """Gate booleans and status. dropped None = INCOMPLETE (absence not provable)."""
    if None in (planned, delivered, failed, observed) or dropped is None or not planned or not delivered:
        return {"status": "INCOMPLETE"}
    g = {"delivered_within_5pct": abs(delivered - planned) <= 0.05 * planned,
         "dropped_le_0.05pct": dropped <= 0.0005 * planned,
         "failed_le_0.1pct": failed <= 0.001 * delivered,
         "observed_within_5pct": abs(observed - delivered) <= 0.05 * delivered}
    g["status"] = "PASS" if all(g.values()) else "FAIL"
    return g


# ---------- collection ----------

def raw(prom, selector, t1, window):
    url = prom + "/api/v1/query?" + urllib.parse.urlencode({"query": f"{selector}[{window}s]", "time": t1})
    d = json.load(urllib.request.urlopen(url, timeout=120))
    if d.get("status") != "success":
        raise RuntimeError(d)
    return [(r["metric"], finite([(float(t), float(v)) for t, v in r.get("values", [])])) for r in d["data"]["result"]]


def summed(series, t0, t1):
    inc = resets = 0
    for _, s in series:
        i, r = series_increment(s, t0, t1)
        inc += i
        resets += r
    return inc, resets


def collect(prom, hour_start):
    import challenge_profile  # noqa: E402  (profile of the challenge-v1 workload)
    t0 = hour_start.timestamp(); t1 = t0 + 3600; win = 3600 + LOOKBACK
    rows = []
    for app in APPS:
        r = {"app": app, "hour_start": hour_start.strftime("%Y-%m-%dT%H:%M:%SZ")}
        try:
            planned = challenge_profile.planned_requests(int(t0))
            reqs = raw(prom, f'k6_http_reqs_total{{testid="{app}"}}', t1, win)
            failed_s = [(m, s) for m, s in reqs if m.get("expected_response") == "false"]
            delivered, k6_resets = summed(reqs, t0, t1)
            failed, _ = summed(failed_s, t0, t1)
            cov = [coverage(s, t0, t1) for _, s in reqs]
            cov_ok = bool(cov) and all(ok for ok, _ in cov)
            restarts_s = raw(prom, f'kube_pod_container_status_restarts_total{{namespace="{NAMESPACE}",pod=~"k6-{app}-.*"}}', t1, win)
            restarts, _ = summed(restarts_s, t0, t1)
            pods = {m.get("pod") for m, s in raw(prom, f'kube_pod_info{{namespace="{NAMESPACE}",pod=~"k6-{app}-.*"}}', t1, 3600)}
            dropped_s = raw(prom, f'k6_dropped_iterations_total{{testid="{app}"}}', t1, win)
            if dropped_s:
                dropped, _ = summed(dropped_s, t0, t1); dropped_basis = "series present"
            elif cov_ok and k6_resets == 0 and restarts == 0 and len(pods) == 1:
                dropped, dropped_basis = 0.0, "zero inferred under the validated sparse-emission rule"
            else:
                dropped, dropped_basis = None, "INCOMPLETE: absent series and the generator did not run the whole hour with full push coverage"
            observed, istio_resets = summed(raw(prom, f'istio_requests_total{{reporter="destination",destination_workload="{app}",destination_workload_namespace="{NAMESPACE}"}}', t1, win), t0, t1)
            buckets = {}
            for m, s in raw(prom, f'istio_request_duration_milliseconds_bucket{{reporter="destination",destination_workload="{app}",destination_workload_namespace="{NAMESPACE}"}}', t1, win):
                i, _ = series_increment(s, t0, t1)
                buckets[m.get("le")] = buckets.get(m.get("le"), 0.0) + i
            p95 = histogram_quantile(0.95, buckets)
            r.update(planned=round(planned), delivered=round(delivered), failed=round(failed),
                     dropped=None if dropped is None else round(dropped), dropped_basis=dropped_basis,
                     observed=round(observed), p95_server_ms=None if p95 is None else round(p95, 1),
                     k6_counter_resets=k6_resets, generator_restarts=round(restarts), generator_pods=sorted(p for p in pods if p),
                     istio_counter_resets=istio_resets, push_coverage=[d for _, d in cov][:3], push_coverage_ok=cov_ok)
            r.update(evaluate(planned, delivered, failed, dropped, observed))
        except Exception as e:  # any query error makes the hour INCOMPLETE, never PASS
            r.update(status="INCOMPLETE", error=str(e)[:300])
        rows.append(r)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", required=True)
    ap.add_argument("--hour", help="UTC hour start, e.g. 2026-10-05T06:00:00Z (default: last completed hour)")
    ap.add_argument("--json")
    a = ap.parse_args()
    if a.hour:
        h = datetime.datetime.strptime(a.hour, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
    else:
        n = datetime.datetime.now(datetime.timezone.utc)
        h = n.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
    rows = collect(a.prom, h)
    for r in rows:
        print(f"{r['app']:20s} {r['hour_start']} {r['status']:10s} planned={r.get('planned')} delivered={r.get('delivered')} "
              f"failed={r.get('failed')} dropped={r.get('dropped')} observed={r.get('observed')} p95_server_ms={r.get('p95_server_ms')}"
              + (f" error={r['error']}" if 'error' in r else ""))
    if a.json:
        json.dump(rows, open(a.json, "w"), indent=1)
    return 0 if all(r["status"] == "PASS" for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
