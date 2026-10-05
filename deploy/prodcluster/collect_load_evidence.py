#!/usr/bin/env python3
"""Hourly load evidence for the six benchmark arms on a VictoriaMetrics cluster (prodcluster campaign), v5.

v5 (2026-10-05, Codex Task 03 r17-r19). The hour is [t0, t1); every time comparison is in integer milliseconds.

Gate (protocol P4 revision):
  G1 destination-observed requests within +-5 % of PLANNED, for every value of the observed bounds. Server-side, no
     client timing assumption: Envoy counters read at each target's own successful scrapes, every potentially serving
     arm pod inventoried, and pods that end in or near the hour closed by their final snapshot (final-snapshot.sh);
  G2 dropped iterations <= 0.05 % of planned and G3 failed requests <= 0.1 % of planned (k6), CONDITIONAL on A1':
     a k6 sample reaches the exporter within LAG = 600 s of its event time (user approval pending; Codex r19).
  k6 delivered (DELTA = 2 s) and observed/delivered are diagnostics only.
k6 contract (k6 v0.48.0 -> xk6-output-prometheus-remote v0.3.1, remotewrite.go): a pushed counter sample (L, V) has
  L = the latest event time aggregated (floored to ms) and V = every event buffered so far, so all counted events are
  < L + 1 ms; a series is pushed only when a flush brings an event later than its previous L; failed writes are not
  retried. k6_vus is pushed by every flush, so a gap above HB_GAP is a lost flush. Sparse k6 bounds for [t0, t1):
  upper = V[first L >= t1 + LAG] - V[last L < t0] (base: the last value since the container start, else 0); exactly 0
  if no sample has L >= t0 and no flush was lost over [t0, t1 + LAG + PUSH]; a sample in [t0, t1 + LAG) without a
  later one leaves the tail unbounded -> INCOMPLETE (collect again later).
Envoy: value at a successful scrape = the series' sample there, or 0 if absent (a successful scrape exposes every
  series). N(< t) of a series lies in [value at the last scrape < t, value at the first scrape >= t]; a pod started at
  or after t0 has N(< t0) = 0. A target without a scrape at or after t1 needs its final snapshot F (capture time c,
  listeners stopped and inbound side quiescent before c): N(< t1) = F if c < t1, else in [last scrape < t1, F]; c < t0
  makes the hour's increment 0. A snapshot is accepted only with exactly one receipt for the pod, the receipt's series
  count present at the capture time, F >= every earlier scrape, every later scrape == F, and Envoy started (capture -
  proxy uptime) no later than the target's first scrape. Every arm pod alive in the hour must have an Envoy target and,
  if its scrapes stop before t1, a snapshot; its istio-proxy restart counter must not move. A2 (declared): a pod with
  no snapshot, not alive in the hour and last scraped more than 3 min before it served nothing in the hour (listed).
Also: generator lifecycle every hour (a name/start-time identity: kube-state-metrics here has no UID label);
deny_partial_response=1 on every query; any query/transport error or inconsistency -> INCOMPLETE, never PASS.

Usage: collect_load_evidence.py --prom <vmselect>/select/0/prometheus [--hour 2026-10-05T08:00:00Z] [--json out]
"""
import argparse, bisect, datetime, json, math, os, re, sys, time, urllib.parse, urllib.request

APPS = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
NAMESPACE = "demo"
ENVOY_JOB = "istio-system/envoy-stats"
LAG = 600.0              # A1' (k6 sample -> exporter), conditional
PUSH = 10.0              # k6 remote-write push interval
HB_GAP = 15.0            # heartbeat gap above this = a lost flush
DELTA = 2.0              # diagnostic delivered bounds only
GRACE = LAG + 120.0      # collect at t1 + 12 min
ENVOY_GAP = 70.0         # scrape every 30 s: allow one missed scrape
KSM_GAP = 90.0
SHORT = 180.0
SCRAPE_LABELS = {"__name__", "app", "container", "endpoint", "instance", "job", "prometheus", "namespace", "pod", "service"}
SNAP_LABELS = {"__name__", "namespace", "pod", "pod_uid", "snapshot_version"}
CONDITIONS = ["G2/G3 (k6 dropped/failed) conditional on A1': a k6 sample reaches the exporter within 600 s (user approval pending)",
              "G1 conditional on A2 for pods without a final snapshot that left Kubernetes and the scrapes > 3 min before the hour"]

_WORKLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "eks-benchmark", "workload", "challenge-v1")
sys.path.insert(0, _WORKLOAD)


class Incomplete(Exception):
    pass


def ms(t):
    return int(round(float(t) * 1000))


# ---------- pure functions (tested offline) ----------

def finite(samples):
    """(finite samples, timestamps of non-finite ones = staleness markers)."""
    return [(t, v) for t, v in samples if math.isfinite(v)], [t for t, v in samples if not math.isfinite(v)]


def covers(ts, t0, t1, gap):
    """True if timestamps bracket [t0, t1] and no two consecutive ones in the bracketing range are more than gap apart."""
    pts = sorted(ts)
    i0 = bisect.bisect_right(pts, t0) - 1
    i1 = bisect.bisect_left(pts, t1)
    if i0 < 0 or i1 >= len(pts):
        return False
    seg = pts[i0:i1 + 1]
    return all(b - a <= gap for a, b in zip(seg, seg[1:]))


def monotone(values):
    if any(not math.isfinite(v) or v < 0 for v in values):
        raise Incomplete("non-finite or negative counter value")
    if any(b < a for a, b in zip(values, values[1:])):
        raise Incomplete("counter decrease (reset or inconsistent base)")


def k6_bounds(samples, base, t0, t1, lag, zero_allowed=True):
    """(low, high) of the k6 counter's events in [t0, t1); samples [(L, V)] after the container start."""
    s = sorted((ms(L), v) for L, v in samples)
    T0, T1, LG = ms(t0), ms(t1), ms(lag)
    monotone([base] + [v for _, v in s])
    lo0 = ([base] + [v for L, v in s if L < T0])[-1]
    lo1 = ([base] + [v for L, v in s if L < T1])[-1]
    hi0 = next((v for L, v in s if L >= T0 + LG), None)
    hi1 = next((v for L, v in s if L >= T1 + LG), None)
    if hi1 is None:
        if zero_allowed and not any(L >= T0 for L, _ in s):
            return 0.0, 0.0
        raise Incomplete("k6 series changed in or after the hour and no later sample bounds its tail yet")
    low = max(0.0, lo1 - hi0) if hi0 is not None else 0.0
    high = hi1 - lo0
    if low > high:
        raise Incomplete(f"invalid k6 bounds ({low}, {high})")
    return low, high


def envoy_values(samples, scrapes):
    """{scrape ms: value}: the sample at that scrape (exact timestamp), else 0 (absent from a successful scrape)."""
    at = {ms(t): v for t, v in samples}
    sc = sorted({ms(s) for s in scrapes})
    if set(at) - set(sc):
        raise Incomplete("Envoy samples outside the target's successful scrapes")
    vals = {s: at.get(s, 0.0) for s in sc}
    monotone([vals[s] for s in sc])
    return vals


def target_series_bounds(samples, scrapes, t0, t1, pod_start, snap):
    """(low, high) of one Envoy series' increment over [t0, t1). snap: None or (capture time, final value F)."""
    T0, T1 = ms(t0), ms(t1)
    pts = envoy_values(samples, scrapes)
    sc = sorted(pts)
    if snap is not None:
        c, F = ms(snap[0]), snap[1]
        if not math.isfinite(F) or any(pts[s] > F for s in sc if s <= c) or any(pts[s] != F for s in sc if s > c):
            raise Incomplete("final snapshot inconsistent with the target's scrapes")
    before = lambda t: max((s for s in sc if s < t), default=None)
    after = lambda t: min((s for s in sc if s >= t), default=None)
    # N(< t0)
    if pod_start is not None and ms(pod_start) >= T0:
        v0 = (0.0, 0.0)
    elif snap is not None and ms(snap[0]) < T0:
        v0 = (snap[1], snap[1])
    else:
        b, a = before(T0), after(T0)
        hi = pts[a] if a is not None else (snap[1] if snap is not None else None)
        if hi is None:
            raise Incomplete("hour start not bracketed (no later scrape, no final snapshot)")
        v0 = (pts[b] if b is not None else 0.0, hi)          # no scrape before t0: a birth interval
    # N(< t1)
    a1 = after(T1)
    if a1 is not None:
        b1 = before(T1)
        v1 = (pts[b1] if b1 is not None else 0.0, pts[a1])
    elif snap is not None:
        c, F = ms(snap[0]), snap[1]
        b1 = before(T1)
        v1 = (F, F) if c < T1 else (pts[b1] if b1 is not None else 0.0, F)
    else:
        raise Incomplete("target ends before the hour end without a final snapshot")
    low, high = max(0.0, v1[0] - v0[1]), v1[1] - v0[0]
    if low > high:
        raise Incomplete(f"invalid Envoy bounds ({low}, {high})")
    return low, high


def native(labels, drop):
    return tuple(sorted((k, v) for k, v in labels.items() if k not in drop))


def check_snapshot(receipts, payload_counts, first_scrape):
    """receipts: [(labels, capture time)] for one pod; payload_counts: {capture ms: bench_final_* series stored there}.
    Returns the accepted capture time or raises Incomplete."""
    if len(receipts) != 1:
        raise Incomplete(f"{len(receipts)} final-snapshot receipts for one pod")
    lab, c = receipts[0]
    try:
        n, up = int(lab["series"]), float(lab["proxy_uptime_s"])
    except (KeyError, ValueError):
        raise Incomplete("malformed final-snapshot receipt")
    if lab.get("hot_restart_epoch") != "0" or payload_counts.get(ms(c), 0) != n:
        raise Incomplete("final snapshot incomplete (series count) or proxy restarted")
    if first_scrape is not None and ms(c - up) > ms(first_scrape) + 1000:
        raise Incomplete("final snapshot from a later proxy epoch than the target's first scrape")
    return c


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


def gate(planned, observed, failed, dropped):
    (ol, ou), (fl, fu), (xl, xu) = observed, failed, dropped
    worst = {"G1_observed_within_5pct_of_planned": abs(ol - planned) <= 0.05 * planned and abs(ou - planned) <= 0.05 * planned,
             "G2_dropped_le_0.05pct_of_planned": xu <= 0.0005 * planned,
             "G3_failed_le_0.1pct_of_planned": fu <= 0.001 * planned}
    best = {"G1_observed_within_5pct_of_planned": ol <= 1.05 * planned and ou >= 0.95 * planned,
            "G2_dropped_le_0.05pct_of_planned": xl <= 0.0005 * planned,
            "G3_failed_le_0.1pct_of_planned": fl <= 0.001 * planned}
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
        """[(labels, finite samples, staleness-marker times)] of the raw samples in (t_end - window, t_end]."""
        out = []
        for r in self._get("/api/v1/query", {"query": f"{selector}[{int(window)}s]", "time": t_end}):
            s, stale = finite([(float(t), float(v)) for t, v in r.get("values", [])])
            out.append((r["metric"], s, stale))
        return out

    def instant(self, query, t):
        return [(r["metric"], float(r["value"][1])) for r in self._get("/api/v1/query", {"query": query, "time": t})]


def const_through(series, t0, t1, what):
    if len(series) != 1 or not covers([t for t, _ in series[0][1]], t0, t1, KSM_GAP):
        raise Incomplete(f"{what} not sampled through the hour")
    vals = {v for _, v in series[0][1]}
    if len(vals) != 1:
        raise Incomplete(f"{what} changed in the hour")
    return next(iter(vals))


def generator_lifecycle(vm, app, t0, t1, end):
    win = end - (t0 - 600)
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
    hold = t1 + LAG + PUSH
    if not covers(ts, t0, hold, KSM_GAP):
        raise Incomplete(f"generator pod {pod} not observed through the hour and the k6 lag allowance")
    pod_start = const_through(vm.raw(f'kube_pod_start_time{{namespace="{NAMESPACE}",pod="{pod}"}}', end, win), t0, hold, "pod start time")
    k6_start = const_through(vm.raw(f'kube_pod_container_state_started{{namespace="{NAMESPACE}",pod="{pod}",container="k6"}}', end, win), t0, hold, "k6 container start time")
    const_through(vm.raw(f'kube_pod_container_status_restarts_total{{namespace="{NAMESPACE}",pod="{pod}",container="k6"}}', end, win), t0, hold, "k6 restart counter")
    if pod_start > t0 or k6_start > t0:
        raise Incomplete("generator pod or k6 container started inside the hour")
    return pod, k6_start


def k6_counter(vm, selector, t0, t1, k6_start, end, lag, zero_allowed=True):
    lo, hi = 0.0, 0.0
    ws = max(t0 - SHORT, k6_start)
    for m, s, _ in vm.raw(selector, end, end - ws):
        labels = {k: x for k, x in m.items() if k != "__name__"}
        base = 0.0
        if ws > k6_start:
            prior = [v for mm, v in vm.instant(f'last_over_time({selector}[{int(ws - k6_start)}s])', ws)
                     if {k: x for k, x in mm.items() if k != "__name__"} == labels]
            base = prior[0] if prior else 0.0
        a, b = k6_bounds([(t, v) for t, v in s if t > ws], base, t0, t1, lag, zero_allowed)
        lo += a; hi += b
    return lo, hi


def arm_envoy(vm, app, t0, t1, end):
    """Observed bounds, latency buckets (lower increments) and the inventory, over all potentially serving arm pods."""
    rx = re.compile(re.escape(app) + r"-[a-z0-9]+-[a-z0-9]+")
    prx = f'{app}-[a-z0-9]+-[a-z0-9]+'
    win = end - (t0 - 3600)
    info = {m.get("pod"): (s, st) for m, s, st in vm.raw(f'kube_pod_info{{namespace="{NAMESPACE}",pod=~"{prx}"}}', end, win)}
    alive = {p for p, (s, st) in info.items() if s and min(t for t, _ in s) <= t1 and
             (max(t for t, _ in s) >= t0 - KSM_GAP or any(x >= t0 for x in st))}
    starts = {m.get("pod"): s[-1][1] for m, s, _ in vm.raw(f'kube_pod_start_time{{namespace="{NAMESPACE}",pod=~"{prx}"}}', end, win) if s}
    for m, s, _ in vm.raw(f'kube_pod_container_status_restarts_total{{namespace="{NAMESPACE}",pod=~"{prx}",container="istio-proxy"}}', end, win):
        if s and max(v for _, v in s) != min(v for _, v in s):
            raise Incomplete(f"istio-proxy restarted in pod {m.get('pod')}")
    targets = {}
    for m, s, _ in vm.raw(f'up{{job="{ENVOY_JOB}",namespace="{NAMESPACE}",pod=~"{prx}"}}', end, win):
        if rx.fullmatch(m.get("pod", "")):
            if m.get("pod") in targets:
                raise Incomplete(f"two Envoy targets for pod {m.get('pod')}")
            targets[m.get("pod")] = [t for t, v in s if v == 1]
    missing = sorted(p for p in alive if p not in targets)
    if missing:
        raise Incomplete(f"arm pods alive in the hour without an Envoy target: {missing}")
    receipts = {}
    for m, s, _ in vm.raw(f'bench_final_snapshot_receipt{{namespace="{NAMESPACE}",pod=~"{prx}"}}', end, win):
        for t, _ in s:
            receipts.setdefault(m.get("pod"), []).append((m, t))
    snaps = {}
    for p, sc in targets.items():
        if not sc or max(sc) >= t1:
            continue
        if p not in receipts:
            continue                                   # target_series_bounds raises for series that need it
        counts = {}
        for m, s, _ in vm.raw(f'{{__name__=~"bench_final_istio_.*",namespace="{NAMESPACE}",pod="{p}"}}', end, win):
            for t, _ in s:
                counts[ms(t)] = counts.get(ms(t), 0) + 1
        snaps[p] = check_snapshot(receipts[p], counts, min(sc))
    sel = f'reporter="destination",destination_workload="{app}",destination_workload_namespace="{NAMESPACE}"'
    # A2: a target without a final snapshot whose pod is not alive in the hour and whose last successful scrape is more
    # than SHORT before t0 served nothing in the hour (object deleted -> no Service endpoint; requests are per connection)
    relevant = {p for p, sc in targets.items() if p in alive or p in snaps or (sc and max(sc) >= t0 - SHORT)}
    excluded = sorted(set(targets) - relevant)
    for p in sorted(relevant):
        sc = targets[p]
        if (not sc or max(sc) < t1) and p not in snaps:
            raise Incomplete(f"pod {p} stops being scraped before the hour end and has no accepted final snapshot")
    out = {}
    for metric in ("istio_requests_total", "istio_request_duration_milliseconds_bucket"):
        scraped = {}
        for m, s, _ in vm.raw(f'{metric}{{{sel},pod=~"{prx}"}}', end, win):
            if m.get("job") != ENVOY_JOB or m.get("pod") not in targets:
                raise Incomplete(f"{metric} series without its own Envoy target: {m.get('pod')}")
            scraped.setdefault(m.get("pod"), {})[native(m, SCRAPE_LABELS)] = s
        finals = {}
        for p, c in snaps.items():
            for m, s, _ in vm.raw(f'bench_final_{metric}{{{sel},namespace="{NAMESPACE}",pod="{p}"}}', end, win):
                v = [x for t, x in s if ms(t) == ms(c)]
                if len(v) != 1:
                    raise Incomplete(f"final snapshot of {p} lacks {metric} at its capture time")
                finals.setdefault(p, {})[native(m, SNAP_LABELS)] = v[0]
        lo, hi, buckets = 0.0, 0.0, {}
        for p in sorted(relevant):
            for key in set(scraped.get(p, {})) | set(finals.get(p, {})):
                snap = (snaps[p], finals.get(p, {}).get(key, 0.0)) if p in snaps else None
                a, b = target_series_bounds(scraped.get(p, {}).get(key, []), targets[p], t0, t1, starts.get(p), snap)
                lo += a; hi += b
                if metric.endswith("_bucket"):
                    le = dict(key).get("le")
                    buckets[le] = buckets.get(le, 0.0) + a
        out[metric] = (lo, hi, buckets)
    return out["istio_requests_total"][:2], out["istio_request_duration_milliseconds_bucket"][2], {
        "pods_alive": sorted(alive), "envoy_targets_relevant": len(relevant), "final_snapshots": sorted(snaps),
        "excluded_under_A2": excluded}


def collect(vm, hour_start, now=None):
    import challenge_profile  # noqa: E402
    t0 = hour_start.timestamp(); t1 = t0 + 3600; end = t1 + GRACE
    now = time.time() if now is None else now
    rows = []
    for app in APPS:
        r = {"app": app, "hour_start": hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"), "collector": "collect_load_evidence.py v5",
             "conditions": CONDITIONS}
        try:
            if now < end:
                raise Incomplete(f"collected before the hour closed + {int(GRACE)} s")
            planned = challenge_profile.planned_requests(int(t0))
            r["planned"] = round(planned)
            observed, buckets, inv = arm_envoy(vm, app, t0, t1, end)
            p95 = histogram_quantile(0.95, buckets)
            r.update(observed=[round(x) for x in observed], p95_server_ms=round(p95, 1) if p95 is not None else None, **inv)
            pod, k6_start = generator_lifecycle(vm, app, t0, t1, end)
            r["generator_pod"] = pod
            hbs = vm.raw(f'k6_vus{{testid="{app}"}}', end, end - (t0 - SHORT))
            if len(hbs) != 1 or not covers([t for t, _ in hbs[0][1]], t0, t1 + LAG + PUSH, HB_GAP):
                raise Incomplete("k6 heartbeat shows a lost flush (or is missing) over the hour and the lag allowance")
            failed = k6_counter(vm, f'k6_http_reqs_total{{testid="{app}",expected_response="false"}}', t0, t1, k6_start, end, LAG)
            dropped = k6_counter(vm, f'k6_dropped_iterations_total{{testid="{app}"}}', t0, t1, k6_start, end, LAG)
            r.update(failed=[round(x) for x in failed], dropped=[round(x) for x in dropped])
            try:
                d = k6_counter(vm, f'k6_http_reqs_total{{testid="{app}"}}', t0, t1, k6_start, end, DELTA, zero_allowed=False)
                r["diagnostic_delivered_conditional_on_2s_lag"] = [round(x) for x in d]
            except Incomplete as e:
                r["diagnostic_delivered_conditional_on_2s_lag"] = f"unavailable: {e}"
            if observed[1] > 0 and p95 is None:
                raise Incomplete("server-side latency (Istio buckets) not computable")
            status, g = gate(planned, observed, failed, dropped)
            r.update(status=status, gate_worst_case=g)
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
        n = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=GRACE)
        h = n.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
    rows = collect(VM(a.prom), h)
    for r in rows:
        print(f"{r['app']:20s} {r['hour_start']} {r['status']:10s} planned={r.get('planned')} observed={r.get('observed')} "
              f"failed={r.get('failed')} dropped={r.get('dropped')} p95_server_ms={r.get('p95_server_ms')}"
              + (f" reason={r['reason']}" if "reason" in r else ""))
    if a.json:
        json.dump(rows, open(a.json, "w"), indent=1)
    return 0 if all(r["status"] == "PASS" for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
