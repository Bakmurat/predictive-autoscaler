#!/usr/bin/env python3
"""Hourly load evidence for the six benchmark arms on a VictoriaMetrics cluster (prodcluster campaign), v2.

v2 (2026-10-05, Codex Task 03 r15) replaces v1's exact-count rules, which could PASS on incomplete evidence. Facts it
relies on, measured on prodcluster:
  * k6 (experimental-prometheus-rw) pushes a counter series only in intervals in which it changed (a 04:5xZ failure
    series had no sample for the next 10+ minutes); k6_vus is pushed every 10 s and is the generator's export heartbeat;
  * k6_dropped_iterations_total appears only after the first drop, its first sample is the count so far (190, not 0);
  * Envoy (Istio sidecars, scraped every 20 s) exposes every counter series it has on each scrape; `up{pod=…}` from the
    envoy-stats scrape shows that a pod was being scraped;
  * vmagent writes NaN staleness markers when a scraped pod disappears; they end a series, they are not values (counted
    separately as lifecycle evidence);
  * kube-state-metrics here exports no pod UID label; a generator pod's identity is its name plus a constant start time
    (a replaced Deployment pod always has a new name and a new start time).
Rules:
  1. Collection only after the hour has closed plus GRACE seconds (late pushes and scrapes have landed); else INCOMPLETE.
  2. Generator lifecycle (for every hour, not only when drops are absent): exactly one pod owned (via its ReplicaSet) by
     Deployment k6-<app>, observed through the whole hour, started before it with a constant start time, and a present
     restart counter with no increment. Missing or ambiguous lifecycle evidence => INCOMPLETE.
  3. Export coverage: k6_vus{testid} samples bracket both hour edges and are never more than 30 s apart over the hour,
     and every minute from the pod start to the hour start has >= 3 heartbeat samples (so absence of a sparse counter
     before the hour means it had no events).
  4. Every counter yields BOUNDS, not an exact count: value at an edge is the last sample at/before it, raised up to the
     first sample after it when that sample lies within one push/scrape window (events in the straddling interval).
     A series with no sample before the hour has base 0 only if its absence is proven (rule 3 for k6; for Envoy series,
     the pod was scraped just before the hour, or the pod started inside it); otherwise INCOMPLETE. A decrease
     (counter reset) anywhere => INCOMPLETE.
  5. Gate on bounds: PASS only if the gate holds for every value in the bounds; FAIL if it fails even for the most
     favourable values; else INCOMPLETE. Latency (server-side Istio p95) must be computable when traffic was delivered.
Totals are window counts between scrape/push instants bracketing the hour, not exact calendar-hour counts.

Usage: collect_load_evidence.py --prom <vmselect>/select/0/prometheus [--hour 2026-10-05T06:00:00Z] [--json out]
"""
import argparse, datetime, json, math, os, sys, time, urllib.parse, urllib.request

APPS = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
NAMESPACE = "demo"
GRACE = 120            # seconds after the hour before collecting
PUSH = 10.0            # k6 push interval
SCRAPE = 20.0          # vmagent scrape interval
EDGE_K6 = PUSH + 5     # a sample within this after an edge may contain events from before the edge
EDGE_ENVOY = SCRAPE + 5
GAP_MAX = 30.0         # heartbeat gap limit inside the hour
KSM_GAP = 60.0         # kube-state-metrics series gap limit (scrape 20 s)
SHORT = 120            # short lookback (s) for dense series

_WORKLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "eks-benchmark", "workload", "challenge-v1")
sys.path.insert(0, _WORKLOAD)


class Incomplete(Exception):
    pass


# ---------- pure functions (tested offline) ----------

def finite(samples):
    """(finite samples, number of NaN staleness markers dropped)."""
    keep = [(t, v) for t, v in samples if math.isfinite(v)]
    return keep, len(samples) - len(keep)


def edge_bounds(samples, t, edge, base=None):
    """(low, high) for a counter's value at instant t. samples sorted, finite. low = last sample at/before t (or `base`
    when there is none and absence is proven; None => unknown). high = first sample in (t, t + edge] if any, else low."""
    before = [v for ts, v in samples if ts <= t]
    low = before[-1] if before else base
    if low is None:
        return None
    after = [v for ts, v in samples if t < ts <= t + edge]
    return (low, after[0] if after else low)


def counter_bounds(samples, t0, t1, edge, base0=None):
    """(inc_low, inc_high) over (t0, t1]; raises Incomplete on a reset or an unprovable start."""
    vals = [v for _, v in samples]
    if any(b < a for a, b in zip(vals, vals[1:])):
        raise Incomplete("counter reset (decrease) in the window")
    v0 = edge_bounds(samples, t0, edge, base0)
    if v0 is None:
        raise Incomplete("no value before the hour and absence not proven")
    v1 = edge_bounds(samples, t1, edge, v0[0])
    return max(0.0, v1[0] - v0[1]), v1[1] - v0[0]


def max_gap(ts, t0, t1):
    pts = sorted(t for t in ts if t0 - GAP_MAX <= t <= t1 + GAP_MAX)
    if not pts or pts[0] > t0 or pts[-1] < t1:
        return math.inf
    return max(b - a for a, b in zip(pts, pts[1:])) if len(pts) > 1 else math.inf


def histogram_quantile(q, buckets):
    """Prometheus histogram_quantile on {le: cumulative count}; le may be the string '+Inf'."""
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
    """Each argument except planned is (low, high). PASS if the gate holds at the worst case, FAIL if it fails at the best."""
    (dl, du), (fl, fu), (xl, xu), (ol, ou) = delivered, failed, dropped, observed

    def check(d, f, x, o):
        return {"delivered_within_5pct": abs(d - planned) <= 0.05 * planned,
                "dropped_le_0.05pct": x <= 0.0005 * planned,
                "failed_le_0.1pct": d > 0 and f <= 0.001 * d,
                "observed_within_5pct": d > 0 and abs(o - d) <= 0.05 * d}

    def all_combos(fn):
        res = [fn(d, f, x, o) for d in (dl, du) for f in (fl, fu) for x in (xl, xu) for o in (ol, ou)]
        return {k: [r[k] for r in res] for k in res[0]}

    combos = all_combos(check)
    worst = {k: all(v) for k, v in combos.items()}
    best = {k: any(v) for k, v in combos.items()}
    status = "PASS" if all(worst.values()) else ("FAIL" if not all(best.values()) else "INCOMPLETE")
    return status, worst


# ---------- collection ----------

class VM:
    def __init__(self, prom):
        self.prom = prom

    def _get(self, path, params):
        d = json.load(urllib.request.urlopen(self.prom + path + "?" + urllib.parse.urlencode(params), timeout=120))
        if d.get("status") != "success":
            raise Incomplete(f"query failed: {str(d)[:200]}")
        return d["data"]["result"]

    def raw(self, selector, t_end, window):
        out = []
        for r in self._get("/api/v1/query", {"query": f"{selector}[{int(window)}s]", "time": t_end}):
            s, nan = finite([(float(t), float(v)) for t, v in r.get("values", [])])
            out.append((r["metric"], s, nan))
        return out

    def instant(self, query, t):
        return [(r["metric"], float(r["value"][1])) for r in self._get("/api/v1/query", {"query": query, "time": t})]

    def per_minute_counts(self, selector, start, end):
        """Samples per minute from start to end, in chunks of <= 7 days (VictoriaMetrics limits points per series)."""
        out, a = [], start + 60
        while a <= end:
            b = min(end, a + 7 * 86400)
            res = self._get("/api/v1/query_range", {"query": f"count_over_time({selector}[1m])", "start": a, "end": b, "step": 60})
            if not res:
                raise Incomplete(f"no heartbeat samples between {a:.0f} and {b:.0f}")
            out += [float(v) for _, v in res[0]["values"]]
            a = b + 60
        return out


def generator_lifecycle(vm, app, t0, t1):
    """(pod, start_time) of the single generator pod that ran the whole hour, or raise Incomplete."""
    win = (t1 + GRACE) - (t0 - 600)
    rs = {m.get("replicaset") for m, s, _ in vm.raw(f'kube_replicaset_owner{{namespace="{NAMESPACE}",owner_kind="Deployment",owner_name="k6-{app}"}}', t1 + GRACE, win) if s}
    if not rs:
        raise Incomplete(f"no ReplicaSet owned by k6-{app}")
    pods = {}
    for m, s, _ in vm.raw(f'kube_pod_owner{{namespace="{NAMESPACE}",owner_kind="ReplicaSet"}}', t1 + GRACE, win):
        if m.get("owner_name") in rs and s:
            pods.setdefault(m.get("pod"), []).extend(t for t, _ in s)
    live = {p: ts for p, ts in pods.items() if any(t0 <= t <= t1 for t in ts)}
    if len(live) != 1:
        raise Incomplete(f"generator pods in the hour: {sorted(live)}")
    pod, ts = next(iter(live.items()))
    if max_gap(ts, t0, t1) > KSM_GAP:
        raise Incomplete(f"generator pod {pod} not observed through the hour")
    starts = {v for m, s, _ in vm.raw(f'kube_pod_start_time{{namespace="{NAMESPACE}",pod="{pod}"}}', t1 + GRACE, win) for _, v in s}
    if len(starts) != 1 or next(iter(starts)) > t0:
        raise Incomplete(f"generator pod start time not constant or not before the hour: {sorted(starts)}")
    restarts = vm.raw(f'kube_pod_container_status_restarts_total{{namespace="{NAMESPACE}",pod="{pod}",container="k6"}}', t1 + GRACE, win)
    if len(restarts) != 1 or max_gap([t for t, _ in restarts[0][1]], t0, t1) > KSM_GAP:
        raise Incomplete("restart counter missing or not covering the hour")
    vals = [v for _, v in restarts[0][1]]
    if max(vals) != min(vals):
        raise Incomplete("generator container restarted")
    return pod, next(iter(starts))


def k6_bounds(vm, selector, t0, t1, pod_start):
    """Summed (low, high) over all series of a k6 counter selector; sparse series get their base from the last value
    since the pod start (absence proven by the heartbeat coverage checked by the caller)."""
    lo = hi = 0.0
    short = vm.raw(selector, t1 + GRACE, (t1 + GRACE) - (t0 - SHORT))
    seen = set()
    for m, s, _ in short:
        key = json.dumps(m, sort_keys=True); seen.add(key)
        base = None
        if not [1 for t, _ in s if t <= t0]:
            # sparse series: its last value since the pod start, if any (functions drop __name__, so compare the rest);
            # none => the series had no event before the hour (heartbeat coverage proven by the caller) => base 0
            labels = {k: x for k, x in m.items() if k != "__name__"}
            prior = [v for mm, v in vm.instant(f'last_over_time({selector}[{int(t0 - pod_start)}s])', t0)
                     if {k: x for k, x in mm.items() if k != "__name__"} == labels]
            base = prior[0] if prior else 0.0
        a, b = counter_bounds(s, t0, t1, EDGE_K6, base)
        lo += a; hi += b
    return lo, hi


def envoy_bounds(vm, selector, t0, t1, scraped_before, started_inside):
    lo = hi = 0.0
    for m, s, _ in vm.raw(selector, t1 + GRACE, (t1 + GRACE) - (t0 - SHORT)):
        pod = m.get("pod")
        base = 0.0 if (pod in started_inside or pod in scraped_before) else None
        a, b = counter_bounds(s, t0, t1, EDGE_ENVOY, base)
        lo += a; hi += b
    return lo, hi


def collect(vm, hour_start, now=None):
    import challenge_profile  # noqa: E402
    t0 = hour_start.timestamp(); t1 = t0 + 3600
    now = time.time() if now is None else now
    rows = []
    for app in APPS:
        r = {"app": app, "hour_start": hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"), "collector": "collect_load_evidence.py v2"}
        try:
            if now < t1 + GRACE:
                raise Incomplete(f"collected before the hour closed + {GRACE}s")
            planned = challenge_profile.planned_requests(int(t0))
            pod, pod_start = generator_lifecycle(vm, app, t0, t1)
            hb = vm.raw(f'k6_vus{{testid="{app}"}}', t1 + GRACE, (t1 + GRACE) - (t0 - SHORT))
            if len(hb) != 1 or max_gap([t for t, _ in hb[0][1]], t0, t1) > GAP_MAX:
                raise Incomplete("k6 export heartbeat (k6_vus) does not cover the hour")
            mins = vm.per_minute_counts(f'k6_vus{{testid="{app}"}}', pod_start, t0)
            if mins and min(mins) < 3:
                raise Incomplete("k6 export heartbeat has a gap between the pod start and the hour")
            delivered = k6_bounds(vm, f'k6_http_reqs_total{{testid="{app}"}}', t0, t1, pod_start)
            failed = k6_bounds(vm, f'k6_http_reqs_total{{testid="{app}",expected_response="false"}}', t0, t1, pod_start)
            dropped = k6_bounds(vm, f'k6_dropped_iterations_total{{testid="{app}"}}', t0, t1, pod_start)
            ups = vm.raw(f'up{{namespace="{NAMESPACE}",pod=~"{app}-[a-z0-9]+-[a-z0-9]+"}}', t1 + GRACE, (t1 + GRACE) - (t0 - SHORT))
            scraped_before = {m.get("pod") for m, s, _ in ups if any(t0 - EDGE_ENVOY <= t <= t0 and v == 1 for t, v in s)}
            starts = vm.raw(f'kube_pod_start_time{{namespace="{NAMESPACE}",pod=~"{app}-[a-z0-9]+-[a-z0-9]+"}}', t1 + GRACE, (t1 + GRACE) - (t0 - SHORT))
            started_inside = {m.get("pod") for m, s, _ in starts if s and t0 < s[-1][1] <= t1}
            sel = f'reporter="destination",destination_workload="{app}",destination_workload_namespace="{NAMESPACE}"'
            observed = envoy_bounds(vm, f'istio_requests_total{{{sel}}}', t0, t1, scraped_before, started_inside)
            buckets, stale = {}, 0
            for m, s, nan in vm.raw(f'istio_request_duration_milliseconds_bucket{{{sel}}}', t1 + GRACE, (t1 + GRACE) - (t0 - SHORT)):
                stale += nan
                base = 0.0 if m.get("pod") in (scraped_before | started_inside) else None
                low, _ = counter_bounds(s, t0, t1, EDGE_ENVOY, base)      # raises Incomplete like any counter
                buckets[m.get("le")] = buckets.get(m.get("le"), 0.0) + low
            p95 = histogram_quantile(0.95, buckets)
            if delivered[1] > 0 and p95 is None:
                raise Incomplete("server-side latency (Istio buckets) not computable")
            status, gate = gate_on_bounds(planned, delivered, failed, dropped, observed)
            r.update(status=status, gate_worst_case=gate, planned=round(planned), generator_pod=pod,
                     delivered=[round(x) for x in delivered], failed=[round(x) for x in failed],
                     dropped=[round(x) for x in dropped], observed=[round(x) for x in observed],
                     p95_server_ms=round(p95, 1) if p95 is not None else None,
                     staleness_markers_in_latency_series=stale)
        except Incomplete as e:
            r.update(status="INCOMPLETE", reason=str(e))
        except Exception as e:   # any query/transport error => INCOMPLETE, never PASS
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
