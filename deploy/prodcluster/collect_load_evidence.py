#!/usr/bin/env python3
"""Hourly load evidence for the six benchmark arms on a VictoriaMetrics cluster (prodcluster campaign), v3.

v3 (2026-10-05, Codex Task 03 r16) bounds every counter at the hour edges by the source's real export instants.
Facts it relies on, measured on prodcluster 2026-10-05:
  * k6 (experimental-prometheus-rw, push every 10 s) pushes k6_vus at every flush (a 1-s ticker, timestamps x.475 s);
    a counter series is pushed only in flushes in which it changed, stamped with the time of its last event in that
    flush: 0.76-0.97 s after the flush's k6_vus sample for steady traffic (05:00-06:00Z), earlier when traffic paused
    (-1.27 s at 04:57:47Z, during the arm re-roll);
    k6_dropped_iterations_total appears only after the first drop, its first sample being the count so far (190);
  * Envoy sidecars are scraped every 30 s by the `istio-system/envoy-stats` job; the target's `up` sample and all its
    counter samples share the scrape timestamp exactly; Envoy exposes every series it has on each scrape;
  * vmagent writes NaN staleness markers when a target disappears (counted, never used as values);
  * kube-state-metrics here exports no pod UID: a generator is identified by its pod name and a constant start time
    (a name/start-time identity, not a verified UID), plus the k6 container's start time and restart counter.
Rules:
  R1 collect only after the hour + GRACE; every query uses deny_partial_response=1 and a partial result is INCOMPLETE.
  R2 generator lifecycle every hour: exactly one pod owned (via its ReplicaSet) by Deployment k6-<app>, observed
     through the hour; its pod start time and k6 container start time sampled through the hour, constant and before
     it; restart counter present through the hour without increment.
  R3 k6 flushes: heartbeat samples bracket both edges (a flush at/before and one after each edge, at most GAP_MAX
     apart); every counter sample in the window is attributed to exactly one flush (within -1/+2.5 s of a heartbeat),
     otherwise INCOMPLETE. Value at an edge t lies in [value after the last flush whose heartbeat is <= t - 2.5 s,
     value after the first flush whose heartbeat is >= t].
     A sparse series' value before the window comes from its last sample since the k6 container start; if it has none,
     its value is 0 only when the heartbeat has a sample in every minute since the container start (the full expected
     grid, no missing or duplicate minute).
  R4 Envoy: each series is read at the successful scrapes (up == 1) of ITS target (same job, instance, pod), at
     most ENVOY_GAP apart inside the hour; value at a scrape = the series' sample at that scrape, or 0 if absent from a
     successful scrape; value at an edge t lies in [value at the last scrape <= t, value at the first scrape > t] (a
     birth between two scrapes is an interval, never an exact zero). A target first scraped inside the hour counts
     from 0 only if its pod started after the hour start; a target that vanished inside the hour has no upper bound
     (requests after its last scrape are unobserved), and the arm's observed upper bound is then capped by k6's
     delivered upper bound (no source sidecar, no retries; flagged in the output).
  R5 every value sequence must be non-decreasing (historical base included), every bound finite with low <= high;
     otherwise INCOMPLETE. Gate on bounds: PASS only if it holds for every value in the bounds, FAIL if it fails even
     for the most favourable ones, else INCOMPLETE. Server-side p95 (Istio buckets) must be computable when traffic ran.
Totals are bounds between export instants bracketing the hour, not exact calendar-hour counts.

Usage: collect_load_evidence.py --prom <vmselect>/select/0/prometheus [--hour 2026-10-05T06:00:00Z] [--json out]
"""
import argparse, bisect, datetime, json, math, os, sys, time, urllib.parse, urllib.request

APPS = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
NAMESPACE = "demo"
ENVOY_JOB = "istio-system/envoy-stats"
GRACE = 180
GAP_MAX = 30.0            # k6 heartbeat gap limit (push every 10 s)
ENVOY_GAP = 70.0          # scrape every 30 s: allow one missed scrape
KSM_GAP = 90.0            # kube-state-metrics series gap limit
FLUSH_LAG = 1.0          # a counter sample belongs to the first heartbeat >= (sample time - FLUSH_LAG)
FLUSH_SLACK = 2.5        # a flush whose heartbeat is <= t - FLUSH_SLACK has certainly completed before t
SHORT = 180

_WORKLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "eks-benchmark", "workload", "challenge-v1")
sys.path.insert(0, _WORKLOAD)


class Incomplete(Exception):
    pass


# ---------- pure functions (tested offline) ----------

def finite(samples):
    keep = [(t, v) for t, v in samples if math.isfinite(v)]
    return keep, len(samples) - len(keep)


def covers(ts, t0, t1, gap):
    """True if timestamps bracket [t0, t1] and no two consecutive ones in the bracketing range are more than gap apart."""
    pts = sorted(ts)
    i0 = bisect.bisect_right(pts, t0) - 1
    i1 = bisect.bisect_right(pts, t1)
    if i0 < 0 or i1 >= len(pts):
        return False
    seg = pts[i0:i1 + 1]
    return all(b - a <= gap for a, b in zip(seg, seg[1:]))


def monotone(values):
    if any(not math.isfinite(v) or v < 0 for v in values):
        raise Incomplete("non-finite or negative counter value")
    if any(b < a for a, b in zip(values, values[1:])):
        raise Incomplete("counter decrease (reset or inconsistent base)")


def flush_values(samples, heartbeats, base):
    """{heartbeat: counter value after that flush}. samples: the series' finite samples in the window; heartbeats: the
    k6_vus timestamps; base: value before the first heartbeat in the window. A sample belongs to the first heartbeat
    >= sample time - FLUSH_LAG (it is stamped with its last event, at most FLUSH_LAG after the flush's heartbeat).
    Raises Incomplete if two samples of the series fall in one flush or the sequence decreases."""
    hb = sorted(heartbeats)
    by_flush = {}
    for t, v in samples:
        i = bisect.bisect_left(hb, t - FLUSH_LAG)
        if i >= len(hb):
            continue                       # after the last heartbeat in the window: beyond the collection window
        if hb[i] in by_flush:
            raise Incomplete(f"two samples attributed to the k6 flush at {hb[i]:.1f}")
        by_flush[hb[i]] = v
    monotone([base] + [by_flush[h] for h in hb if h in by_flush])
    out, cur = {}, base
    for h in hb:
        cur = by_flush.get(h, cur)
        out[h] = cur
    return out


def edge_interval(points, t):
    """(low, high) from {instant: value}: value after the last instant <= t, value after the first instant > t."""
    ks = sorted(points)
    i = bisect.bisect_right(ks, t)
    if i == 0 or i >= len(ks):
        raise Incomplete("edge not bracketed by export instants")
    return points[ks[i - 1]], points[ks[i]]


def k6_edge(points, t):
    """(low, high) for a k6 counter at t from {heartbeat: value after that flush}."""
    ks = sorted(points)
    lows = [k for k in ks if k <= t - FLUSH_SLACK]
    highs = [k for k in ks if k >= t]
    if not lows or not highs:
        raise Incomplete("edge not bracketed by k6 flushes")
    return points[lows[-1]], points[highs[0]]


def increment_bounds(points, t0, t1, edge=edge_interval):
    a0, b0 = edge(points, t0)
    a1, b1 = edge(points, t1)
    low, high = max(0.0, a1 - b0), b1 - a0
    if not (math.isfinite(low) and math.isfinite(high)) or low > high or high < 0:
        raise Incomplete(f"invalid bounds ({low}, {high})")
    return low, high


def envoy_values(samples, scrapes):
    """{scrape instant: value}: the sample at that scrape (exact timestamp), else 0 (absent from a successful scrape)."""
    at = {round(t, 3): v for t, v in samples}
    vals = {s: at.get(round(s, 3), 0.0) for s in sorted(scrapes)}
    stray = set(at) - {round(s, 3) for s in scrapes}
    if stray:
        raise Incomplete("Envoy samples outside the target's successful scrapes")
    monotone([vals[s] for s in sorted(vals)])
    return vals


def minute_grid_complete(points, start, end, minimum=3):
    """points: [(ts, count)] from count_over_time(...[1m]) with step 60 over [start, end]. True if every expected step
    is present exactly once and each count >= minimum."""
    ts = [t for t, _ in points]
    if len(ts) != len(set(ts)) or not ts:
        return False
    first, last = min(ts), max(ts)
    if first > start + 120 or last < end - 60:
        return False
    expected = {first + 60 * k for k in range(int(round((last - first) / 60)) + 1)}
    return set(ts) == expected and all(c >= minimum for _, c in points)


def histogram_quantile(q, buckets):
    items = sorted(((math.inf if str(le) in ("+Inf", "inf") else float(le), c) for le, c in buckets.items()), key=lambda x: x[0])
    if not items or items[-1][0] != math.inf or items[-1][1] <= 0:
        return None
    rank = q * items[-1][1]
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


def gate_on_bounds(planned, delivered, failed, dropped, observed):
    (dl, du), (fl, fu), (xl, xu), (ol, ou) = delivered, failed, dropped, observed

    def check(d, f, x, o):
        return {"delivered_within_5pct": abs(d - planned) <= 0.05 * planned,
                "dropped_le_0.05pct": x <= 0.0005 * planned,
                "failed_le_0.1pct": d > 0 and f <= 0.001 * d,
                "observed_within_5pct": d > 0 and abs(o - d) <= 0.05 * d}
    res = [check(d, f, x, o) for d in (dl, du) for f in (fl, fu) for x in (xl, xu) for o in (ol, ou)]
    worst = {k: all(r[k] for r in res) for k in res[0]}
    best = {k: any(r[k] for r in res) for k in res[0]}
    status = "PASS" if all(worst.values()) else ("FAIL" if not all(best.values()) else "INCOMPLETE")
    return status, worst


# ---------- collection ----------

class VM:
    def __init__(self, prom):
        self.prom = prom

    def _get(self, path, params):
        params = dict(params, deny_partial_response=1)
        d = json.load(urllib.request.urlopen(self.prom + path + "?" + urllib.parse.urlencode(params), timeout=120))
        if d.get("status") != "success" or d.get("isPartial"):
            raise Incomplete(f"query failed or partial: {str(d)[:200]}")
        return d["data"]["result"]

    def raw(self, selector, t_end, window):
        out = []
        for r in self._get("/api/v1/query", {"query": f"{selector}[{int(window)}s]", "time": t_end}):
            s, nan = finite([(float(t), float(v)) for t, v in r.get("values", [])])
            out.append((r["metric"], s, nan))
        return out

    def instant(self, query, t):
        return [(r["metric"], float(r["value"][1])) for r in self._get("/api/v1/query", {"query": query, "time": t})]

    def minute_counts(self, selector, start, end):
        out, a = [], start + 60
        while a <= end:
            b = min(end, a + 7 * 86400)
            res = self._get("/api/v1/query_range", {"query": f"count_over_time({selector}[1m])", "start": a, "end": b, "step": 60})
            if len(res) != 1:
                raise Incomplete(f"heartbeat grid query returned {len(res)} series")
            out += [(float(t), float(v)) for t, v in res[0]["values"]]
            a = b + 60
        return out


def const_through(series, t0, t1, what):
    if len(series) != 1 or not covers([t for t, _ in series[0][1]], t0, t1, KSM_GAP):
        raise Incomplete(f"{what} not sampled through the hour")
    vals = {v for _, v in series[0][1]}
    if len(vals) != 1:
        raise Incomplete(f"{what} changed in the hour")
    return next(iter(vals))


def generator_lifecycle(vm, app, t0, t1):
    end, win = t1 + GRACE, (t1 + GRACE) - (t0 - 600)
    rs = {m.get("replicaset") for m, s, _ in vm.raw(f'kube_replicaset_owner{{namespace="{NAMESPACE}",owner_kind="Deployment",owner_name="k6-{app}"}}', end, win) if s}
    if not rs:
        raise Incomplete(f"no ReplicaSet owned by k6-{app}")
    pods = {}
    for m, s, _ in vm.raw(f'kube_pod_owner{{namespace="{NAMESPACE}",owner_kind="ReplicaSet"}}', end, win):
        if m.get("owner_name") in rs and s:
            pods.setdefault(m.get("pod"), []).extend(t for t, _ in s)
    live = {p: ts for p, ts in pods.items() if any(t0 <= t <= t1 for t in ts)}
    if len(live) != 1:
        raise Incomplete(f"generator pods in the hour: {sorted(live)}")
    pod, ts = next(iter(live.items()))
    if not covers(ts, t0, t1, KSM_GAP):
        raise Incomplete(f"generator pod {pod} not observed through the hour")
    pod_start = const_through(vm.raw(f'kube_pod_start_time{{namespace="{NAMESPACE}",pod="{pod}"}}', end, win), t0, t1, "pod start time")
    k6_start = const_through(vm.raw(f'kube_pod_container_state_started{{namespace="{NAMESPACE}",pod="{pod}",container="k6"}}', end, win), t0, t1, "k6 container start time")
    const_through(vm.raw(f'kube_pod_container_status_restarts_total{{namespace="{NAMESPACE}",pod="{pod}",container="k6"}}', end, win), t0, t1, "k6 restart counter")
    if pod_start > t0 or k6_start > t0:
        raise Incomplete("generator pod or k6 container started inside the hour")
    return pod, k6_start


def k6_counter(vm, selector, t0, t1, hb, k6_start, grid_ok):
    """Summed increment bounds of a k6 counter selector, flush-attributed (R3)."""
    end, lo, hi = t1 + GRACE, 0.0, 0.0
    first_hb = min(hb)
    for m, s, _ in vm.raw(selector, end, end - (t0 - SHORT)):
        labels = {k: x for k, x in m.items() if k != "__name__"}
        prior = [v for mm, v in vm.instant(f'last_over_time({selector}[{int(first_hb - FLUSH_LAG - k6_start)}s])', first_hb - FLUSH_LAG)
                 if {k: x for k, x in mm.items() if k != "__name__"} == labels]
        if prior:
            base = prior[0]
        elif grid_ok:
            base = 0.0
        else:
            raise Incomplete("sparse series without history and the heartbeat grid is incomplete")
        pts = flush_values([(t, v) for t, v in s if t >= first_hb - FLUSH_LAG], hb, base)
        a, b = increment_bounds(pts, t0, t1, k6_edge)
        lo += a; hi += b
    return lo, hi


def target_bounds(samples, scrapes, t0, t1, pod_start):
    """(low, high) for one Envoy series; high None when the target vanished inside the hour (its requests after the
    last scrape are unobserved). A target first scraped inside the hour counts from 0 only when its pod started after
    the hour start (proven birth); otherwise the start edge must be bracketed by scrapes."""
    scr = sorted(scrapes)
    if not scr:
        raise Incomplete("target has no successful scrape")
    if scr[-1] <= t0:                      # gone before the hour: contributes nothing to it
        return 0.0, 0.0
    seg = [t for t in scr if t0 - ENVOY_GAP <= t <= t1 + ENVOY_GAP]
    if any(b - a > ENVOY_GAP for a, b in zip(seg, seg[1:])):
        raise Incomplete("scrape gap inside the hour")
    pts = envoy_values([(t, v) for t, v in samples if scr[0] <= t <= scr[-1]], scr)
    if scr[0] > t0:                        # first scraped inside the hour: value at t0 is 0 only if born after t0
        if pod_start is None or pod_start < t0:
            raise Incomplete("target first scraped inside the hour but its birth after the hour start is not proven")
        v0 = (0.0, 0.0)
    else:
        v0 = edge_interval(pts, t0)
    if scr[-1] <= t1:                      # vanished inside the hour: lower bound only
        return max(0.0, pts[scr[-1]] - v0[1]), None
    a1, b1 = edge_interval(pts, t1)
    low, high = max(0.0, a1 - v0[1]), b1 - v0[0]
    if low > high:
        raise Incomplete(f"invalid bounds ({low}, {high})")
    return low, high


def envoy_counter(vm, selector, t0, t1, scrapes_by_target, pod_starts, buckets=None):
    end, lo, hi, stale, unbounded = t1 + GRACE, 0.0, 0.0, 0, False
    for m, s, nan in vm.raw(selector, end, end - (t0 - SHORT)):
        stale += nan
        key = (m.get("job"), m.get("instance"), m.get("pod"))
        a, b = target_bounds(s, scrapes_by_target.get(key, []), t0, t1, pod_starts.get(m.get("pod")))
        if buckets is not None:
            buckets[m.get("le")] = buckets.get(m.get("le"), 0.0) + a
        lo += a
        if b is None:
            unbounded = True
        else:
            hi += b
    return (lo, None if unbounded else hi), stale


def collect(vm, hour_start, now=None):
    import challenge_profile  # noqa: E402
    t0 = hour_start.timestamp(); t1 = t0 + 3600; end = t1 + GRACE
    now = time.time() if now is None else now
    rows = []
    for app in APPS:
        r = {"app": app, "hour_start": hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"), "collector": "collect_load_evidence.py v3"}
        try:
            if now < t1 + GRACE:
                raise Incomplete(f"collected before the hour closed + {GRACE}s")
            planned = challenge_profile.planned_requests(int(t0))
            pod, k6_start = generator_lifecycle(vm, app, t0, t1)
            hbs = vm.raw(f'k6_vus{{testid="{app}"}}', end, end - (t0 - SHORT))
            if len(hbs) != 1 or not covers([t for t, _ in hbs[0][1]], t0, t1, GAP_MAX):
                raise Incomplete("k6 heartbeat does not bracket and cover the hour")
            hb = [t for t, _ in hbs[0][1]]
            grid_ok = minute_grid_complete(vm.minute_counts(f'k6_vus{{testid="{app}"}}', k6_start, min(hb)), k6_start, min(hb))
            delivered = k6_counter(vm, f'k6_http_reqs_total{{testid="{app}"}}', t0, t1, hb, k6_start, grid_ok)
            failed = k6_counter(vm, f'k6_http_reqs_total{{testid="{app}",expected_response="false"}}', t0, t1, hb, k6_start, grid_ok)
            dropped = k6_counter(vm, f'k6_dropped_iterations_total{{testid="{app}"}}', t0, t1, hb, k6_start, grid_ok)
            scrapes = {}
            for m, s, _ in vm.raw(f'up{{job="{ENVOY_JOB}",namespace="{NAMESPACE}"}}', end, end - (t0 - SHORT)):
                scrapes[(m.get("job"), m.get("instance"), m.get("pod"))] = [t for t, v in s if v == 1]
            pod_starts = {m.get("pod"): s[-1][1] for m, s, _ in vm.raw(f'kube_pod_start_time{{namespace="{NAMESPACE}",pod=~"{app}-[a-z0-9]+-[a-z0-9]+"}}', end, end - (t0 - SHORT)) if s}
            sel = f'reporter="destination",destination_workload="{app}",destination_workload_namespace="{NAMESPACE}"'
            observed, _ = envoy_counter(vm, f'istio_requests_total{{{sel}}}', t0, t1, scrapes, pod_starts)
            capped = observed[1] is None
            if capped:   # a pod vanished in the hour: without a source sidecar or retries the destination cannot observe
                observed = (observed[0], delivered[1])   # more requests than k6 delivered (checked live, flagged)
            buckets = {}
            _, stale = envoy_counter(vm, f'istio_request_duration_milliseconds_bucket{{{sel}}}', t0, t1, scrapes, pod_starts, buckets)
            p95 = histogram_quantile(0.95, buckets)
            if delivered[1] > 0 and p95 is None:
                raise Incomplete("server-side latency (Istio buckets) not computable")
            status, gate = gate_on_bounds(planned, delivered, failed, dropped, observed)
            r.update(status=status, gate_worst_case=gate, planned=round(planned), generator_pod=pod, heartbeat_grid_complete=grid_ok,
                     delivered=[round(x) for x in delivered], failed=[round(x) for x in failed], dropped=[round(x) for x in dropped],
                     observed=[round(x) for x in observed], p95_server_ms=round(p95, 1) if p95 is not None else None,
                     staleness_markers_in_latency_series=stale, observed_upper_capped_by_delivered=capped)
        except Incomplete as e:
            r.update(status="INCOMPLETE", reason=str(e))
        except Exception as e:  # any query/transport error => INCOMPLETE, never PASS
            r.update(status="INCOMPLETE", reason=f"error: {str(e)[:250]}")
        rows.append(r)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", required=True)
    ap.add_argument("--hour")
    ap.add_argument("--json")
    a = ap.parse_args()
    if a.hour:
        h = datetime.datetime.strptime(a.hour, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
    else:
        n = datetime.datetime.now(datetime.timezone.utc)
        h = n.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
    rows = collect(VM(a.prom), h)
    for r in rows:
        print(f"{r['app']:20s} {r['hour_start']} {r['status']:10s} planned={r.get('planned')} delivered={r.get('delivered')} "
              f"failed={r.get('failed')} dropped={r.get('dropped')} observed={r.get('observed')} p95_server_ms={r.get('p95_server_ms')}"
              + (f" reason={r['reason']}" if "reason" in r else ""))
    if a.json:
        json.dump(rows, open(a.json, "w"), indent=1)
    return 0 if all(r["status"] == "PASS" for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
