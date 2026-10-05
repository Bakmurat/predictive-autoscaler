#!/usr/bin/env python3
"""Hourly load evidence for the six benchmark arms on a VictoriaMetrics cluster (prodcluster campaign), v7.

v7 (2026-10-05, Codex Task 03 r21): no acceptance cache (every snapshot re-verified from VictoriaMetrics each hour,
including that no scrape after its capture changed); one constant pod-start and container-start identity per pod;
restart-counter observations must really bracket the scrapes used; approvals and manual termination records are
validated records (affirmative decision, UTC time, reference), malformed ones count as absent.
v6 (2026-10-05, Codex Task 03 r20): inventory since a declared start, no A2, verified snapshot identity and canonical
digest, lifecycle evidence required, capture second treated as [c, c+1 s), separate qualification outcome.
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
  if its scrapes stop before t1, a snapshot; its istio-proxy restart counter must not move.
Inventory (v6): every arm pod with any record (pod info, Envoy target, istio series, receipt) since --inventory-start
  (the moment the final hook ran on every arm pod; earlier pods are closed by a one-time record kept with the
  campaign). A pod without a successful scrape after t1 needs an accepted snapshot (re-verified every hour) or a
  validated manual termination record (--terminations); otherwise every later hour is INCOMPLETE. A pod created at or
  after the hour end (kube_pod_created) did not serve in it (created_after_hour). A pod that was never scheduled is
  closed only by a record (never_scheduled) whose evidence the collector re-checks: one creation time equal to the
  record, an empty node label in every kube-state-metrics sample, scheduled condition never true, no start time, no
  Envoy target, no istio series, nothing after the recorded deletion bound (Codex r24). An accepted snapshot: exactly one
  receipt (snapshot_version 2), the same non-empty pod_uid on receipt and payload, the receipt's series count stored at
  the capture time, the canonical sha256 recomputed from the stored integer counter series, no duplicate series,
  hot-restart epoch 0, Envoy start (capture - proxy uptime) within -2..+30 s of kube-state-metrics' istio-proxy
  container start, every later scrape
  equal to the final value. Snapshot times are whole seconds: capture c means [c, c+1 s).
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
ASSUMPTIONS = {"A1'": "G2/G3 (k6 dropped/failed): a k6 sample reaches the exporter within 600 s of its event time"}
EPOCH_WINDOW = (-2.0, 30.0)  # Envoy start (capture - uptime) minus KSM istio-proxy container start: Envoy starts after
                             # pilot-agent (measured +2 s on prodcluster); whole seconds on both sides

_WORKLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "eks-benchmark", "workload", "challenge-v1")
sys.path.insert(0, _WORKLOAD)


class Incomplete(Exception):
    pass


with open(os.path.abspath(__file__), "rb") as _fh:
    import hashlib as _hashlib
    COLLECTOR_SHA256 = _hashlib.sha256(_fh.read()).hexdigest()   # identifies the exact collector code in every row
IDENTITY_NOTE = ("pods identified by name + kube-state-metrics start times (pod and istio-proxy container); this cluster's "
                 "kube-state-metrics exports no pod UID, so snapshot pod_uid labels are only checked for consistency")


def utc(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    elif snap is not None and ms(snap[0]) + 1000 <= T0:          # capture second [c, c+1) entirely before t0
        v0 = (snap[1], snap[1])
    else:
        b, a = before(T0), after(T0)
        hi = pts[a] if a is not None else (snap[1] if snap is not None else None)
        if hi is None:
            raise Incomplete("hour start not bracketed (no later scrape, no final snapshot)")
        v0 = (pts[b] if b is not None else 0.0, hi)          # no scrape before t0: a birth interval
    # N(< t1)
    a1 = after(T1)
    if pod_start is not None and ms(pod_start) >= T1:
        v1 = (0.0, 0.0)
    elif a1 is not None:
        b1 = before(T1)
        v1 = (pts[b1] if b1 is not None else 0.0, pts[a1])
    elif snap is not None:
        c, F = ms(snap[0]), snap[1]
        b1 = before(T1)
        v1 = (F, F) if c + 1000 <= T1 else (pts[b1] if b1 is not None else 0.0, F)
    else:
        raise Incomplete("target ends before the hour end without a final snapshot")
    low, high = max(0.0, v1[0] - v0[1]), v1[1] - v0[0]
    if low > high:
        raise Incomplete(f"invalid Envoy bounds ({low}, {high})")
    return low, high


def native(labels, drop):
    return tuple(sorted((k, v) for k, v in labels.items() if k not in drop))


def canonical_digest(series):
    """series: [(labels, value)] of one snapshot (stored bench_final_* series). Recomputes final-snapshot.sh's canonical
    form: integer counter families, native labels sorted, 'name{k="v",...} value' lines sorted, sha256."""
    import hashlib
    lines = []
    for lab, v in series:
        name = lab.get("__name__", "")
        if not name.startswith("bench_final_"):
            raise Incomplete("unexpected series in a final snapshot")
        name = name[len("bench_final_"):]
        if not re.search(r"(_total|_bucket|_count)$", name):
            continue
        if not (math.isfinite(v) and v >= 0 and float(v).is_integer()):
            raise Incomplete("non-integer counter in a final snapshot")
        kv = ",".join(f'{k}="{x}"' for k, x in sorted(lab.items()) if k not in SNAP_LABELS and x != "")
        lines.append(f"{name}{{{kv}}} {int(v)}")
    if len(lines) != len(set(lines)) or len({line.rsplit(" ", 1)[0] for line in lines}) != len(lines):
        raise Incomplete("duplicate series in a final snapshot")
    lines.sort()
    return len(lines), hashlib.sha256("".join(line + "\n" for line in lines).encode()).hexdigest()


def check_snapshot(receipts, payload, proxy_started):
    """receipts: [(labels, capture time)] for one pod; payload: [(labels, value)] stored at that capture time;
    proxy_started: kube-state-metrics' istio-proxy start time. Returns (capture time, pod_uid) or raises Incomplete."""
    if len(receipts) != 1:
        raise Incomplete(f"{len(receipts)} final-snapshot receipts for one pod")
    lab, c = receipts[0]
    try:
        n, nc, up = int(lab["series"]), int(lab["canonical_series"]), float(lab["proxy_uptime_s"])
        digest, uid = lab["canonical_sha256"], lab["pod_uid"]
    except (KeyError, ValueError):
        raise Incomplete("malformed final-snapshot receipt (or snapshot_version 1)")
    if lab.get("snapshot_version") != "2" or not uid or lab.get("hot_restart_epoch") != "0":
        raise Incomplete("final-snapshot receipt: wrong version, no pod UID or proxy hot-restarted")
    if len(payload) != n or any(m.get("pod_uid") != uid or m.get("snapshot_version") != "2" for m, _ in payload):
        raise Incomplete("final snapshot payload incomplete or bound to another pod UID")
    if canonical_digest(payload) != (nc, digest):
        raise Incomplete("final snapshot canonical digest mismatch")
    if proxy_started is None or not EPOCH_WINDOW[0] <= (c - up) - proxy_started <= EPOCH_WINDOW[1]:
        raise Incomplete("final snapshot not from the istio-proxy instance kube-state-metrics recorded")
    return c, uid


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


def _utc(x):
    try:
        return datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()
    except (TypeError, ValueError):
        return None


def valid_approvals(raw):
    """{assumption: record}; a record counts only as {"decision": "approved", "by": "user", "at": UTC, "ref": "..."}."""
    if not isinstance(raw, dict):
        return set()
    return {a for a, r in raw.items() if isinstance(r, dict) and r.get("decision") == "approved" and r.get("by") == "user"
            and _utc(r.get("at")) is not None and isinstance(r.get("ref"), str) and r["ref"].strip()}


def valid_terminations(raw):
    """{pod: record} -> {pod: (kind, bound, identity)} for well-formed records only:
    terminated:      {"terminated_before": UTC, "pod_start": UTC (< bound), "pod_uid", "evidence", "recorded_by", "at"}
    never_scheduled: {"never_scheduled": true, "created": UTC, "deleted_before": UTC (> created), "pod_uid", "evidence",
                      "recorded_by", "at"} (the evidence must name the retained UID-bound object/event record)."""
    out = {}
    if not isinstance(raw, dict):
        return out
    for pod, r in raw.items():
        if not isinstance(r, dict) or _utc(r.get("at")) is None or \
                not all(isinstance(r.get(k), str) and r[k].strip() for k in ("pod_uid", "evidence", "recorded_by")):
            continue
        if r.get("never_scheduled") is True:
            cr, db = _utc(r.get("created")), _utc(r.get("deleted_before"))
            if cr is not None and db is not None and cr < db:
                out[pod] = ("never_scheduled", db, cr)
            continue
        tb, ps = _utc(r.get("terminated_before")), _utc(r.get("pod_start"))
        if tb is not None and ps is not None and ps < tb:
            out[pod] = ("terminated", tb, ps)
    return out


def lifecycle_value(series, what, pod):
    vals = {v for _, v in series}
    if not series or len(vals) != 1:
        raise Incomplete(f"{what} of pod {pod} missing or changing")
    return next(iter(vals))


def verify_never_scheduled(vm, pod, rec, end, W):
    """Re-check a never_scheduled record against kube-state-metrics; returns 0.0 (closed for every hour)."""
    _, deleted_before, created = rec
    cr = vm.raw(f'kube_pod_created{{namespace="{NAMESPACE}",pod="{pod}"}}', end, W)
    if len(cr) != 1 or abs(lifecycle_value(cr[0][1], "pod creation time", pod) - created) > 1:
        raise Incomplete(f"pod {pod}: never_scheduled record does not match the observed creation time")
    info = vm.raw(f'kube_pod_info{{namespace="{NAMESPACE}",pod="{pod}"}}', end, W)
    if not info or any(m.get("node", "") != "" for m, s, _ in info if s) or \
            any(t > deleted_before + KSM_GAP for _, s, _ in info for t, _ in s):
        raise Incomplete(f"pod {pod}: never_scheduled contradicted (assigned to a node, or alive after the bound)")
    sched = vm.raw(f'kube_pod_status_scheduled{{namespace="{NAMESPACE}",pod="{pod}",condition="true"}}', end, W)
    if not sched or any(v != 0 for _, s, _ in sched for _, v in s):
        raise Incomplete(f"pod {pod}: scheduled condition missing or true")
    for q in (f'kube_pod_start_time{{namespace="{NAMESPACE}",pod="{pod}"}}', f'up{{job="{ENVOY_JOB}",namespace="{NAMESPACE}",pod="{pod}"}}',
              f'istio_requests_total{{namespace="{NAMESPACE}",pod="{pod}"}}'):
        if any(s for _, s, _ in vm.raw(q, end, W)):
            raise Incomplete(f"pod {pod}: never_scheduled contradicted by {q.split('{')[0]}")
    return 0.0


def verify_snapshot(vm, app, pod, t_inv, end, terminations):
    """(capture, uid) of a pod whose scrapes stopped: its final snapshot, re-verified from VictoriaMetrics (no cache), or
    (terminated_before, None) from a validated manual termination record."""
    W = int(end - t_inv)
    receipts = [(m, t) for m, s, _ in vm.raw(f'bench_final_snapshot_receipt{{namespace="{NAMESPACE}",pod="{pod}"}}', end, W) for t, _ in s]
    if not receipts:
        if pod in terminations and terminations[pod][0] == "never_scheduled":
            return verify_never_scheduled(vm, pod, terminations[pod], end, W), None
        if pod in terminations:
            _, tb, rec_start = terminations[pod]
            # the record closes ONE incarnation (name + pod start): the observed start must match it, and nothing of
            # that name may be observed alive after the recorded termination bound
            st = vm.raw(f'kube_pod_start_time{{namespace="{NAMESPACE}",pod="{pod}"}}', end, W)
            if len(st) != 1 or abs(lifecycle_value(st[0][1], "pod start time", pod) - rec_start) > 1:
                raise Incomplete(f"pod {pod}: manual termination record does not match the observed incarnation")
            later = [t for q in (f'kube_pod_info{{namespace="{NAMESPACE}",pod="{pod}"}}', f'up{{job="{ENVOY_JOB}",namespace="{NAMESPACE}",pod="{pod}"}}')
                     for _, s, _ in vm.raw(q, end, W) for t, v in s if t > tb + KSM_GAP and (not q.startswith("up") or v == 1)]
            if later:
                raise Incomplete(f"pod {pod}: observed alive after its recorded termination")
            return tb, None
        raise Incomplete(f"pod {pod} stopped being scraped and has no final snapshot")
    c = receipts[0][1]
    payload = [(m, v) for m, s, _ in vm.raw(f'{{__name__=~"bench_final_istio_.*",namespace="{NAMESPACE}",pod="{pod}"}}', end, W)
               for t, v in s if ms(t) == ms(c)]
    started = vm.raw(f'kube_pod_container_state_started{{namespace="{NAMESPACE}",pod="{pod}",container="istio-proxy"}}', end, W)
    if len(started) != 1:
        raise Incomplete(f"pod {pod}: istio-proxy container start not observed exactly once")
    c, uid = check_snapshot(receipts, payload, lifecycle_value(started[0][1], "istio-proxy start time", pod))
    # nothing may change after the capture: every scrape after it equals the final value, none before it exceeds it
    finals = {native(m, SNAP_LABELS): v for m, v in payload if m.get("__name__") == "bench_final_istio_requests_total"}
    sel = f'reporter="destination",destination_workload="{app}",destination_workload_namespace="{NAMESPACE}"'
    for m, s, _ in vm.raw(f'istio_requests_total{{{sel},pod="{pod}"}}', end, end - (c - 300)):   # through the cutoff
        F = finals.get(native(m, SCRAPE_LABELS))
        if F is None or any(v > F for t, v in s if t <= c) or any(v != F for t, v in s if t > c):
            raise Incomplete(f"pod {pod}: scrapes around the capture disagree with its final snapshot")
    return c, uid


def arm_envoy(vm, app, t0, t1, end, t_inv, terminations):
    """Observed bounds, latency buckets (lower increments) and the inventory over every arm pod since t_inv."""
    rx = re.compile(re.escape(app) + r"-[a-z0-9]+-[a-z0-9]+")
    prx = f'{app}-[a-z0-9]+-[a-z0-9]+'
    sel = f'reporter="destination",destination_workload="{app}",destination_workload_namespace="{NAMESPACE}"'
    W = int(end - t_inv)
    inv = set()
    for q in (f'kube_pod_info{{namespace="{NAMESPACE}",pod=~"{prx}"}}', f'up{{job="{ENVOY_JOB}",namespace="{NAMESPACE}",pod=~"{prx}"}}',
              f'bench_final_snapshot_receipt{{namespace="{NAMESPACE}",pod=~"{prx}"}}', f'istio_requests_total{{{sel},pod=~"{prx}"}}'):
        inv |= {m.get("pod") for m, _ in vm.instant(f'count by (pod) (count_over_time({q}[{W}s]))', end)}
    inv = {p for p in inv if p and rx.fullmatch(p)}
    serving_after = {m.get("pod") for m, v in vm.instant(
        f'max by (pod) (max_over_time(up{{job="{ENVOY_JOB}",namespace="{NAMESPACE}",pod=~"{prx}"}}[{int(end - t1)}s]))', end) if v == 1}
    snaps, closed, created_after = {}, [], []
    for p in sorted(inv - serving_after):
        cr = vm.raw(f'kube_pod_created{{namespace="{NAMESPACE}",pod="{p}"}}', end, W)
        if len(cr) == 1 and ms(lifecycle_value(cr[0][1], "pod creation time", p)) >= ms(t1):
            # the pod object did not exist before the hour end: it served nothing in [t0, t1) (Codex r24)
            early = [t for q in (f'up{{job="{ENVOY_JOB}",namespace="{NAMESPACE}",pod="{p}"}}', f'istio_requests_total{{{sel},pod="{p}"}}')
                     for _, s, _ in vm.raw(q, end, W) for t, _ in s if t < t1]
            if early:
                raise Incomplete(f"pod {p}: created after the hour but observed in it")
            created_after.append(p)
            continue
        c, uid = verify_snapshot(vm, app, p, t_inv, end, terminations)
        if ms(c) + 1000 <= ms(t0):
            closed.append(p)                          # finished before the hour: contributes nothing
        elif uid is None:
            raise Incomplete(f"pod {p} has only a manual termination record inside the hour")
        else:
            snaps[p] = c
    relevant = sorted(inv - set(closed) - set(created_after))
    win = end - (t0 - SHORT)
    info = {m.get("pod") for m, s, _ in vm.raw(f'kube_pod_info{{namespace="{NAMESPACE}",pod=~"{prx}"}}', end, win) if s}
    starts = {}
    for m, s, _ in vm.raw(f'kube_pod_start_time{{namespace="{NAMESPACE}",pod=~"{prx}"}}', end, win):
        p = m.get("pod")
        if p in starts:
            raise Incomplete(f"pod {p}: more than one pod-start series (name reuse?)")
        starts[p] = lifecycle_value(s, "pod start time", p)
    restarts = {}
    for m, s, _ in vm.raw(f'kube_pod_container_status_restarts_total{{namespace="{NAMESPACE}",pod=~"{prx}",container="istio-proxy"}}', end, win):
        if m.get("pod") in restarts:
            raise Incomplete(f"pod {m.get('pod')}: more than one istio-proxy restart series")
        restarts[m.get("pod")] = s
    proxy_starts = {}
    for m, s, _ in vm.raw(f'kube_pod_container_state_started{{namespace="{NAMESPACE}",pod=~"{prx}",container="istio-proxy"}}', end, win):
        if m.get("pod") in proxy_starts:
            raise Incomplete(f"pod {m.get('pod')}: more than one istio-proxy start series")
        proxy_starts[m.get("pod")] = s
    targets = {}
    for m, s, _ in vm.raw(f'up{{job="{ENVOY_JOB}",namespace="{NAMESPACE}",pod=~"{prx}"}}', end, win):
        if m.get("pod") in targets:
            raise Incomplete(f"two Envoy targets for pod {m.get('pod')}")
        targets[m.get("pod")] = [t for t, v in s if v == 1]
    for p in relevant:
        if p not in info or p not in starts:
            raise Incomplete(f"pod {p}: no kube-state-metrics pod record in the hour")
        if p not in targets or not targets[p]:
            raise Incomplete(f"pod {p}: no successful Envoy scrape in the hour window")
        lifecycle_value(restarts.get(p, []), "istio-proxy restart counter", p)
        lifecycle_value(proxy_starts.get(p, []), "istio-proxy container start", p)
        # the counter must really be observed across the scrapes the bounds use (an unobserved proxy restart could be
        # hidden by a counter that grows past its old value): from the last scrape before t0 (else the first) to the
        # first scrape at or after t1 (else the last)
        sc = sorted(targets[p])
        a = max((x for x in sc if x < t0), default=sc[0])
        b = min((x for x in sc if x >= t1), default=sc[-1])
        obs = [t for t, _ in restarts[p]]
        # a counter that reads 0 at its first sample has not restarted since the container started: the container start
        # (kube-state-metrics) is then a valid observation point (a pod's first scrape can precede the first KSM sample)
        if restarts[p] and restarts[p][0][1] == 0 and proxy_starts.get(p):
            obs.append(lifecycle_value(proxy_starts[p], "istio-proxy container start", p))
        if not covers(obs, a, b, KSM_GAP):
            raise Incomplete(f"pod {p}: istio-proxy restart counter not observed through its scrapes")
    out = {}
    for metric in ("istio_requests_total", "istio_request_duration_milliseconds_bucket"):
        scraped = {}
        for m, s, _ in vm.raw(f'{metric}{{{sel},pod=~"{prx}"}}', end, win):
            p = m.get("pod")
            if p not in relevant:
                if any(t >= t0 for t, _ in s):
                    raise Incomplete(f"{metric} samples in the hour from a pod closed before it: {p}")
                continue
            if m.get("job") != ENVOY_JOB or p not in targets:
                raise Incomplete(f"{metric} series without its own Envoy target: {p}")
            key = native(m, SCRAPE_LABELS)
            if key in scraped.setdefault(p, {}):
                raise Incomplete(f"duplicate {metric} series for pod {p}")
            scraped[p][key] = s
        finals = {}
        for p, c in snaps.items():
            for m, s, _ in vm.raw(f'bench_final_{metric}{{{sel},namespace="{NAMESPACE}",pod="{p}"}}', end, W):
                v = [x for t, x in s if ms(t) == ms(c)]
                if len(v) != 1:
                    raise Incomplete(f"final snapshot of {p} lacks {metric} at its capture time")
                key = native(m, SNAP_LABELS)
                if key in finals.setdefault(p, {}):
                    raise Incomplete(f"duplicate final {metric} series for pod {p}")
                finals[p][key] = v[0]
        lo, hi, buckets = 0.0, 0.0, {}
        for p in relevant:
            for key in set(scraped.get(p, {})) | set(finals.get(p, {})):
                snap = (snaps[p], finals.get(p, {}).get(key, 0.0)) if p in snaps else None
                a, b = target_series_bounds(scraped.get(p, {}).get(key, []), targets[p], t0, t1, starts.get(p), snap)
                lo += a; hi += b
                if metric.endswith("_bucket"):
                    le = dict(key).get("le")
                    buckets[le] = buckets.get(le, 0.0) + a
        out[metric] = (lo, hi, buckets)
    return out["istio_requests_total"][:2], out["istio_request_duration_milliseconds_bucket"][2], {
        "pods_inventoried": len(inv), "pods_relevant": relevant, "final_snapshots_in_hour": sorted(snaps),
        "pods_closed_before_hour": len(closed), "pods_created_after_hour": created_after}


def collect(vm, hour_start, t_inv, terminations=None, approvals=None, now=None):
    import challenge_profile  # noqa: E402
    t0 = hour_start.timestamp(); t1 = t0 + 3600; end = t1 + GRACE
    terminations = valid_terminations(terminations or {})
    approved = valid_approvals(approvals or {})
    now = time.time() if now is None else now
    rows = []
    for app in APPS:
        r = {"app": app, "hour_start": hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"), "collector": "collect_load_evidence.py v7",
             "collector_sha256": COLLECTOR_SHA256, "inventory_start": utc(t_inv), "collection_cutoff": utc(end),
             "terminations_used": sorted(terminations), "identity": IDENTITY_NOTE}
        try:
            if now < end:
                raise Incomplete(f"collected before the hour closed + {int(GRACE)} s")
            if t0 < t_inv:
                raise Incomplete("hour starts before the inventory start")
            planned = challenge_profile.planned_requests(int(t0))
            r["planned"] = round(planned)
            observed, buckets, inv = arm_envoy(vm, app, t0, t1, end, t_inv, terminations)
            p95 = histogram_quantile(0.95, buckets)
            # p95 over the requests certainly inside the hour (lower-bound increments), not necessarily all of them
            r.update(observed=[round(x) for x in observed], p95_server_ms_interior=round(p95, 1) if p95 is not None else None, **inv)
            pod, k6_start = generator_lifecycle(vm, app, t0, t1, end)
            r["generator_pod"] = pod
            hbs = vm.raw(f'k6_vus{{testid="{app}"}}', end, end - (t0 - SHORT))
            if len(hbs) != 1 or not covers([t for t, _ in hbs[0][1]], t0, t1 + LAG + PUSH, HB_GAP):
                raise Incomplete("k6 heartbeat shows a lost flush (or is missing) over the hour and the lag allowance")
            # sparse k6 series may need a sample well after the hour to bound their tail (A1'): search up to collection
            k6_end = max(end, now - 60)
            failed = k6_counter(vm, f'k6_http_reqs_total{{testid="{app}",expected_response="false"}}', t0, t1, k6_start, k6_end, LAG)
            dropped = k6_counter(vm, f'k6_dropped_iterations_total{{testid="{app}"}}', t0, t1, k6_start, k6_end, LAG)
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
        pending = sorted(a for a in ASSUMPTIONS if a not in approved)
        r["qualification"] = {"qualifies": r["status"] == "PASS" and not pending, "conditional_on": ASSUMPTIONS,
                              "pending_user_approval": pending}
        rows.append(r)
    return rows


def write_rows(rows, directory, hour_start):
    """Atomically write the hour's rows to <directory>/load-<YYYYmmddTHHMMZ>.json (temporary file, fsync, rename)."""
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, "load-" + hour_start.strftime("%Y%m%dT%H%MZ") + ".json")
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(rows, fh, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", required=True)
    ap.add_argument("--hour")
    ap.add_argument("--json")
    ap.add_argument("--json-dir", help="write load-<hour>.json atomically into this directory (in-cluster runner)")
    ap.add_argument("--exit-zero", action="store_true", help="exit 0 whatever the outcome (the JSON carries it)")
    ap.add_argument("--recheck-hours", type=int, default=0,
                    help="with --json-dir and no --hour: also re-evaluate the N hours before the latest (a sparse k6 tail can "
                         "only be bounded by a later sample); each file is rewritten with its collection time")
    ap.add_argument("--inventory-start", required=True, help="UTC time from which every arm pod runs the final hook")
    ap.add_argument("--terminations", help='JSON {pod: {"terminated_before", "pod_uid", "evidence", "recorded_by", "at"}}')
    ap.add_argument("--approvals", help='JSON {assumption: {"decision": "approved", "by": "user", "at", "ref"}}')
    a = ap.parse_args()
    t_inv = datetime.datetime.strptime(a.inventory_start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()
    terminations = json.load(open(a.terminations)) if a.terminations else {}
    approvals = json.load(open(a.approvals)) if a.approvals else {}
    if a.hour:
        hours = [datetime.datetime.strptime(a.hour, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)]
    else:
        n = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=GRACE)
        last = n.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
        hours = [last - datetime.timedelta(hours=k) for k in range(a.recheck_hours + 1)]
    ok = True
    for h in hours:
        rows = collect(VM(a.prom), h, t_inv, terminations, approvals)
        for r in rows:
            r["collected_at"] = utc(time.time())
            print(f"{r['app']:20s} {r['hour_start']} {r['status']:10s} planned={r.get('planned')} observed={r.get('observed')} "
                  f"failed={r.get('failed')} dropped={r.get('dropped')} qualifies={r['qualification']['qualifies']}"
                  + (f" reason={r['reason']}" if "reason" in r else ""))
        if a.json and len(hours) == 1:
            json.dump(rows, open(a.json, "w"), indent=1)
        if a.json_dir:
            print("wrote", write_rows(rows, a.json_dir, h))
        ok = ok and all(r["qualification"]["qualifies"] for r in rows)
    return 0 if (a.exit_zero or ok) else 1


if __name__ == "__main__":
    sys.exit(main())
