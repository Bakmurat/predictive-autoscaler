#!/usr/bin/env python3
"""Capacity scoring for the prodcluster campaign (evaluation protocol P9: C1 and the capacity part of C4), v3.

Per arm and UTC day, on the 30-s grid tau = day start + 30 s * k:
  planned step reference  R_p(tau) = max(1, ceil(rate_at(tau) / 600)) from the frozen challenge-v1 schedule
                          (deploy/eks-benchmark/workload/challenge-v1/challenge_profile.py + profile.json): the same
                          offered demand for every arm (primary)
  observed reference      R_o(tau) = max(1, ceil(the arm's own destination-observed rpm at tau / 600)): shortage
                          relative to the traffic the arm received (secondary, never shown alone)
  Ready(tau)              per exporter series, its latest raw kube_deployment_status_replicas_ready sample at or before
                          tau; it counts when finite and inside (tau - 30 s, tau]; a staleness marker invalidates the
                          series until its next finite sample; the largest across series; missing otherwise, never
                          filled. Values must be non-negative integers.
  shortage = sum max(0, R - Ready) * 30/60 and surplus = sum max(0, Ready - R) * 30/60 replica-minutes.

Validity (P8): the detector's result must be v5 and bound to the frozen detector, identity file and mask; its
segments and gate hours are validated, every capacity slot of the scoring window is recomputed from them and must equal
the saved classification, and the 20 % budget is counted from that same map; an instant whose slot is invalid is
excluded for every arm, other classes are kept and counted. A difference between two arms uses only
instants eligible for both (and, for the observed reference, with both rpm samples). Per day and pair: expected,
invalid-excluded, eligible, missing and paired instants with both fractions; a day under 90 % paired/eligible is
labelled incomplete. Nothing is zero-filled. The aggregate is a rate per paired day (sum / paired minutes * 1,440); its
interval resamples whole days with one saved, hashed draw matrix shared by every comparison and both scorer halves
(10,000 draws, seed 20261013, percentiles); a draw without paired exposure is undefined and counted, and the interval is
labelled conditional on the defined draws. Every VictoriaMetrics response is archived with its hash and request, and a
run can be replayed offline from that archive. Descriptive only (U-27): no significance, no superiority.
"""
import argparse, datetime, hashlib, importlib.util, json, math, os, random, sys, tempfile, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROD = os.path.join(HERE, "..")
PROFILE_DIR = os.path.join(PROD, "..", "eks-benchmark", "workload", "challenge-v1")
MS, STEP, DAY, SLOT = 1000, 30_000, 86_400_000, 600_000
PER_POD_RPM = 600
INCOMPLETE = 0.90
CLASSES = ("invalid", "unknown", "unverified", "gate_fail", "verified_clean")
ARMS = {"hybrid": "nginx-test", "reactive": "nginx-reactive", "keda": "myapptwo", "s1": "nginx-seasonal",
        "e1": "nginx-ensemble", "e2": "nginx-ensemble-q95"}
LABELS = {"hybrid": "hybrid (LSTM + pattern)", "reactive": "reactive", "keda": "KEDA", "s1": "S1 seasonal",
          "e1": "E1 ensemble, absolute q90", "e2": "E2′ ensemble, relative q90"}
E2_SWITCH = 1_791_414_520_000     # 2026-10-07T23:08:40Z: E2 absolute q95 -> relative q90 (D-1084); earlier rows are q95
C1 = [(f, c) for f in ("hybrid", "s1", "e1", "e2") for c in ("reactive", "keda")]
C4 = [("e2", "e1")]
READY_Q = 'kube_deployment_status_replicas_ready{{namespace="demo",deployment="{app}"}}'
RPM_Q = ('sum(rate(istio_requests_total{{reporter="destination",destination_workload="{app}",'
         'destination_workload_namespace="demo"}}[1m])) * 60')
FAILED_Q = 'sum(increase(k6_bench_req_failed_total{{testid="{app}"}}[1d]))'     # diagnostic estimates, not P4 counts
DROPPED_Q = 'sum(increase(k6_dropped_iterations_total{{testid="{app}"}}[1d]))'
SCORED_DRAWS = (10_000, 20261013)                 # P9: a scored run uses exactly these


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def iso(ms):
    return datetime.datetime.fromtimestamp(ms // MS, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha_bytes(b):
    return hashlib.sha256(b).hexdigest()


def sha(path):
    return sha_bytes(open(path, "rb").read())


def write_atomic(path, text):
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp-")
    with os.fdopen(fd, "w") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


# ---------------------------------------------------------------------------------------------------- pure logic
def check_ready(series, what):
    for s in series:
        for t, v in s:
            if v is not None and (v < 0 or v != int(v)):
                raise ValueError(f"{what}: Ready value {v} at {iso(t)} is not a non-negative integer")


def ready_at(series, taus):
    """series: [[(t_ms, value|None)]] (one sorted list per exporter series); taus sorted. Per series the latest sample
    at or before tau counts when it is finite and inside (tau - 30 s, tau]; a staleness marker as the latest sample
    invalidates the series. The largest across series; None when none counts. Linear in samples + instants."""
    out, idx = {}, [0] * len(series)
    for tau in taus:
        best = None
        for i, s in enumerate(series):
            j = idx[i]
            while j < len(s) and s[j][0] <= tau:
                j += 1
            idx[i] = j
            if j:
                t, v = s[j - 1]
                if v is not None and t > tau - STEP and (best is None or v > best):
                    best = v
        out[tau] = best
    return out


def required(rpm):
    return max(1, math.ceil(rpm / PER_POD_RPM - 1e-9)) if rpm > 0 else 1


def gap(req, ready):
    """(shortage, surplus) replica-minutes of one 30-s instant."""
    return max(0.0, req - ready) * STEP / 60_000, max(0.0, ready - req) * STEP / 60_000


def score(taus, planned, ready, rpm, p8_class=None, pairs=C1 + C4):
    """taus: sorted instants; planned {tau: rpm}; ready/rpm {arm: {tau: value|None}}; p8_class(tau) -> class or None.
    Returns (per-arm day rows, per-pair day rows)."""
    cls = {tau: (p8_class(tau) if p8_class else "not_applied") for tau in taus}
    days = sorted({tau - tau % DAY for tau in taus})
    by_day = {d: [t for t in taus if d <= t < d + DAY] for d in days}
    arm_rows, pair_rows = [], []
    for arm in ready:
        for d in days:
            ts = by_day[d]
            row = {"arm": arm, "day": iso(d), "expected": len(ts), "classes": {}}
            for t in ts:
                row["classes"][cls[t]] = row["classes"].get(cls[t], 0) + 1
            elig = [t for t in ts if cls[t] != "invalid"]
            have = [t for t in elig if ready[arm].get(t) is not None]
            obs = [t for t in have if rpm[arm].get(t) is not None]
            row.update(invalid=len(ts) - len(elig), eligible=len(elig), ready_present=len(have), rpm_present=len(obs))
            for ref, pts, req in (("planned", have, lambda t: required(planned[t])),
                                  ("observed", obs, lambda t: required(rpm[arm][t]))):
                g = [gap(req(t), ready[arm][t]) for t in pts]
                row[ref] = {"shortage": round(sum(x for x, _ in g), 2), "surplus": round(sum(y for _, y in g), 2),
                            "minutes": len(pts) * STEP / 60_000}
            arm_rows.append(row)
    for a, b in pairs:
        if a not in ready or b not in ready:
            continue
        for d in days:
            ts = by_day[d]
            elig = [t for t in ts if cls[t] != "invalid"]
            row = {"pair": [a, b], "day": iso(d), "expected": len(ts), "invalid": len(ts) - len(elig), "eligible": len(elig)}
            for ref in ("planned", "observed"):
                pts = [t for t in elig if ready[a].get(t) is not None and ready[b].get(t) is not None and
                       (ref == "planned" or (rpm[a].get(t) is not None and rpm[b].get(t) is not None))]
                acc = {"a_shortage": 0.0, "a_surplus": 0.0, "b_shortage": 0.0, "b_surplus": 0.0}
                for t in pts:
                    for side, arm in (("a", a), ("b", b)):
                        req = required(planned[t]) if ref == "planned" else required(rpm[arm][t])
                        s, u = gap(req, ready[arm][t])
                        acc[side + "_shortage"] += s
                        acc[side + "_surplus"] += u
                n = len(pts)
                row[ref] = dict({k: round(v, 2) for k, v in acc.items()}, paired=n, missing=len(elig) - n,
                                paired_minutes=n * STEP / 60_000,
                                paired_over_expected=round(n / len(ts), 4) if ts else None,
                                paired_over_eligible=round(n / len(elig), 4) if elig else None,
                                incomplete=(not elig) or n / len(elig) < INCOMPLETE)
            pair_rows.append(row)
    return arm_rows, pair_rows


def make_draws(days, n_boot, seed):
    """The whole-day resampling matrix shared by every comparison and both scorer halves."""
    rng = random.Random(seed)
    return {"seed": seed, "n_boot": n_boot, "days": list(days),
            "matrix": [[rng.randrange(len(days)) for _ in days] for _ in range(n_boot)] if days else []}


def check_draws(d, days, n_boot, seed):
    """A loaded draw bank must be exactly the declared one: seed, count, ordered days, shape and index range."""
    if d.get("seed") != seed or d.get("n_boot") != n_boot:
        raise ValueError(f"draw bank has seed {d.get('seed')} / {d.get('n_boot')} draws, not {seed} / {n_boot}")
    if d.get("days") != list(days):
        raise ValueError("draw bank was made for other days")
    m = d.get("matrix")
    if not isinstance(m, list) or len(m) != n_boot or any(not isinstance(r, list) or len(r) != len(days) for r in m):
        raise ValueError("draw bank has the wrong shape")
    if any(type(i) is not int or not 0 <= i < len(days) for r in m for i in r):
        raise ValueError("draw bank holds an index outside the days")
    if m != make_draws(days, n_boot, seed)["matrix"]:                # Codex r42: the contents, not only the shape
        raise ValueError("draw bank differs from the declared bank for its seed")


def rate(rows, ref, key):
    """Sum over the rows of key / their paired minutes * 1,440 (None when there is no paired exposure)."""
    minutes = sum(r[ref]["paired_minutes"] for r in rows)
    return None if minutes == 0 else sum(r[ref][key] for r in rows) / minutes * 1440


def aggregate(pair_rows, draws):
    """Rates per paired day per pair and reference, with the shared day-block draws of the differences."""
    pairs = sorted({tuple(r["pair"]) for r in pair_rows})
    days = draws["days"]
    if sorted({r["day"] for r in pair_rows}) != sorted(days):
        raise ValueError("the draw matrix was made for other days")
    by = {(tuple(r["pair"]), r["day"]): r for r in pair_rows}
    out = []
    for p in pairs:
        rows = [by[(p, d)] for d in days if (p, d) in by]
        for ref in ("planned", "observed"):
            res = {"pair": list(p), "reference": ref, "days": len(rows),
                   "incomplete_days": sum(1 for r in rows if r[ref]["incomplete"]),
                   "paired_minutes": sum(r[ref]["paired_minutes"] for r in rows)}
            for m in ("shortage", "surplus"):
                ra, rb = rate(rows, ref, "a_" + m), rate(rows, ref, "b_" + m)
                vals, undefined = [], 0
                for draw in draws["matrix"]:
                    sample = [by[(p, days[i])] for i in draw if (p, days[i]) in by]
                    da, db = rate(sample, ref, "a_" + m), rate(sample, ref, "b_" + m)
                    if da is None:
                        undefined += 1
                    else:
                        vals.append(da - db)
                vals.sort()
                pct = lambda q: vals[min(len(vals) - 1, int(q * len(vals)))]
                res[m] = {"a_per_day": ra, "b_per_day": rb, "difference_per_day": None if ra is None else ra - rb,
                          "nominal_95": [pct(0.025), pct(0.975)] if vals else None,
                          "bootstrap_defined": len(vals), "bootstrap_undefined": undefined,
                          "interval_note": ("conditional on the defined draws" if undefined and vals else None)}
            out.append(res)
    return out


def p8_check(res, start, stop, frozen, ie):
    """Validate a detector result for this scoring window. frozen: sha256 of the frozen detector, identity file and mask.
    Every capacity slot of the window is recomputed from the result's segments and gate hours and must equal the saved
    classification; the budget is counted from that map. Returns the classifier and the window summary."""
    if not str(res.get("detector", "")).startswith("infra_events.py v5"):
        raise ValueError(f"P8 result from {res.get('detector')!r}, not detector v5")
    ri = res.get("run_identity") or {}
    for name, key in (("detector", "detector_sha256"), ("identities", "identities_sha256"), ("mask", "mask_sha256")):
        if ri.get(key) != frozen[name]:
            raise ValueError(f"P8 result's {key} differs from the frozen {name}")
    if ie.parse(res["start"]) > start or ie.parse(res["stop"]) < stop:
        raise ValueError("P8 result does not cover the scoring window")
    segs = []
    for x in res["segments_with_washout_ms"]:
        if (not isinstance(x, list) or len(x) != 3 or any(type(v) is not int for v in x[:2]) or x[0] > x[1]
                or x[2] not in ("invalid", "unknown")):
            raise ValueError(f"P8 result has a malformed segment {x!r}")
        segs.append(tuple(x))
    gates = {}
    for h, g in res["gate_hours"].items():
        t = ie.parse(h)
        if t % ie.HOUR or g not in ("verified", "gate_fail", "unverified"):
            raise ValueError(f"P8 result has a malformed gate hour {h!r}: {g!r}")
        gates[t] = g
    cl = ie.Classifier(segs, gates)
    saved = {ie.parse(k): v for k, v in res["capacity_slots"].items()}
    need = list(range(start, stop, SLOT))
    classes = {}
    for t in need:
        if t not in saved:
            raise ValueError(f"P8 result lacks the capacity slot {iso(t)}")
        classes[t] = cl.capacity(t, t + SLOT)
        if classes[t] != saved[t]:
            raise ValueError(f"P8 slot {iso(t)} saved as {saved[t]!r} but recomputes to {classes[t]!r}")
    n = {c: sum(1 for t in need if classes[t] == c) for c in CLASSES}
    budget = n["invalid"] + n["unknown"] + n["unverified"]
    return {"classifier": cl, "gates": gates, "classes": classes,
            "summary": dict(n, total=len(need), budget_slots=budget, compromised=budget > 0.2 * len(need))}


# ---------------------------------------------------------------------------------------------------- data access
class Store:
    """Every VictoriaMetrics response is archived with its request and hash (record mode) or read back from such an
    archive and verified (replay mode)."""

    def __init__(self, base, archive, replay=False):
        self.base, self.archive, self.replay = (base or "").rstrip("/"), archive, replay
        self.manifest = os.path.join(archive, "manifest.jsonl")
        if replay:
            if not os.path.exists(self.manifest):
                raise ValueError(f"no recorded run in {archive}")
        else:
            os.makedirs(archive)                             # a recorded run never reuses a directory
            os.makedirs(os.path.join(archive, "raw"))
        self.lock = open(os.path.join(archive, ".lock"), "w")
        try:
            import fcntl
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f"another process holds {archive}")
        self.index = {}
        if replay:
            for line in open(self.manifest):
                e = json.loads(line)
                self.index[e["request"]] = e

    def get(self, path, params):
        params = dict(params, deny_partial_response="1")
        request = path + "?" + urllib.parse.urlencode(sorted(params.items()))
        if self.replay:
            e = self.index.get(request)
            if e is None:
                raise ValueError(f"request not in the archive: {request}")
            data = open(os.path.join(self.archive, e["file"]), "rb").read()
            if sha_bytes(data) != e["sha256"] or len(data) != e["bytes"]:
                raise ValueError(f"archived response {e['file']} does not match its hash")
            return data
        with urllib.request.urlopen(self.base + request, timeout=120) as r:
            data = r.read()
        name = f"raw/{sha_bytes(request.encode())[:16]}.bin"
        with open(os.path.join(self.archive, name), "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        e = {"request": request, "endpoint": self.base, "file": name, "bytes": len(data), "sha256": sha_bytes(data),
             "fetched_at": iso(int(datetime.datetime.now(datetime.timezone.utc).timestamp() * MS))}
        with open(self.manifest, "a") as fh:
            fh.write(json.dumps(e) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        self.index[request] = e
        return data


def range_values(store, query, start, stop):
    """Values of a range query on the 30-s grid in [start, stop), per day; partial or conflicting results rejected."""
    vals = {}
    for d in range(start, stop, DAY):
        body = json.loads(store.get("/api/v1/query_range", {"query": query, "step": "30s", "start": str(d // MS),
                                                           "end": str((min(d + DAY, stop) - STEP) // MS)}))
        if body.get("status") != "success" or body.get("isPartial"):
            raise RuntimeError(f"range query failed or was partial: {query}")
        for r in body["data"]["result"]:
            for t, v in r["values"]:
                t, v = int(round(float(t) * MS)), float(v)
                if not math.isfinite(v):
                    continue
                if t in vals and vals[t] != v:
                    raise ValueError(f"conflicting values at {iso(t)} for {query}")
                vals[t] = v
    return vals


def instant(store, query, at):
    body = json.loads(store.get("/api/v1/query", {"query": query, "time": str(at // MS)}))
    if body.get("status") != "success" or body.get("isPartial"):
        raise RuntimeError(f"instant query failed or was partial: {query}")
    res = body["data"]["result"]
    return float(res[0]["value"][1]) if res else None


def fetch(store, start, stop, ie):
    """Ready raw samples (export API, per day), observed rpm on the grid, k6 failed/dropped per day."""
    taus = list(range(start, stop, STEP))
    ready, rpm, k6 = {}, {}, {}
    for arm, app in ARMS.items():
        recs = []
        for d in range(start, stop, DAY):
            recs += ie.parse_lines(store.get("/api/v1/export", {"match[]": READY_Q.format(app=app),
                                                                "start": "%d.%03d" % divmod(d - STEP, MS),
                                                                "end": "%d.%03d" % divmod(min(d + DAY, stop), MS)}))
        series = [s for _, s in ie.normalize(recs)]
        check_ready(series, app)
        ready[arm] = ready_at(series, taus)
        vals = range_values(store, RPM_Q.format(app=app), start, stop)
        rpm[arm] = {t: vals.get(t) for t in taus}
        k6[arm] = {iso(d): {"failed": instant(store, FAILED_Q.format(app=app), d + DAY),
                            "dropped": instant(store, DROPPED_Q.format(app=app), d + DAY)}
                   for d in range(start, stop, DAY)}
    return taus, ready, rpm, k6


# ---------------------------------------------------------------------------------------------------- report
def label(arm, day=None):
    """E2 served the absolute q95 margin until E2_SWITCH (D-1084)."""
    if arm == "e2" and day is not None and day < iso(E2_SWITCH - E2_SWITCH % DAY + DAY):
        return "E2 ensemble (absolute q95 until 10-07 23:08Z)"
    return LABELS[arm]


def pair_label(arm, meta):
    if arm == "e2" and meta.get("mixed_e2"):
        return "E2 (mixed: absolute q95 until 10-07 23:08Z, then relative q90)"
    return LABELS[arm]


def markdown(arm_rows, pair_rows, aggregates, k6, gate_days, meta):
    f = lambda x: "—" if x is None else f"{x:.1f}"
    lines = [f"# Capacity — {meta['start']} → {meta['stop']}", "", meta["note"], ""]
    if meta.get("mixed_e2"):
        lines += ["**E2 regime:** before 2026-10-07T23:08:40Z the E2 arm served the absolute q95 margin (D-1084); paired "
                  "E2 figures in this window mix two policies.", ""]
    if meta.get("p8"):
        s = meta["p8"]
        lines += [f"**P8 (scoring window):** {s['total']} capacity slots — " +
                  ", ".join(f"{c} {s[c]}" for c in CLASSES) +
                  f"; budget {s['budget_slots']} slots → {'COMPROMISED' if s['compromised'] else 'within 20 %'}.", ""]
    lines += ["## Per arm and day", "",
              "Planned step reference; observed reference in brackets. k6 failed/dropped are diagnostic `increase()`",
              "estimates, not the P4 collector's counts.", "",
              "| day | arm | eligible | Ready present | shortage | surplus | k6 failed / dropped | P8 classes |",
              "|---|---|---|---|---|---|---|---|"]
    for r in arm_rows:
        kk = k6.get(r["arm"], {}).get(r["day"], {})
        val = lambda ref, k: f(r[ref][k]) if r[ref]["minutes"] else "—"
        lines.append(f"| {r['day'][:10]} | {label(r['arm'], r['day'])} | {r['eligible']} | "
                     f"{r['ready_present']} ({r['ready_present'] / r['eligible']:.1%})" if r["eligible"] else
                     f"| {r['day'][:10]} | {label(r['arm'], r['day'])} | 0 | — (undefined)")
        lines[-1] += (f" | {val('planned', 'shortage')} ({val('observed', 'shortage')}) | {val('planned', 'surplus')} "
                      f"({val('observed', 'surplus')}) | {f(kk.get('failed'))} / {f(kk.get('dropped'))} | "
                      f"{', '.join(f'{k} {v}' for k, v in sorted(r['classes'].items()))} |")
    if gate_days:
        lines += ["", "## Load-gate hours per day (from the P8 result)", "", "| day | verified | gate_fail | unverified |",
                  "|---|---|---|---|"]
        lines += [f"| {d} | {g.get('verified', 0)} | {g.get('gate_fail', 0)} | {g.get('unverified', 0)} |"
                  for d, g in sorted(gate_days.items())]
    for ref in ("planned", "observed"):
        lines += ["", f"## Paired exposure per day ({ref} reference)", "",
                  "| day | a vs b | expected | invalid | eligible | paired | missing | paired/expected | paired/eligible | incomplete |",
                  "|---|---|---|---|---|---|---|---|---|---|"]
        for r in pair_rows:
            p = r[ref]
            lines.append(f"| {r['day'][:10]} | {r['pair'][0]} vs {r['pair'][1]} | {r['expected']} | {r['invalid']} | "
                         f"{r['eligible']} | {p['paired']} | {p['missing']} | {p['paired_over_expected'] if p['paired_over_expected'] is not None else '—'} | "
                         f"{p['paired_over_eligible'] if p['paired_over_eligible'] is not None else '—'} | "
                         f"{'yes' if p['incomplete'] else 'no'} |")
    for ref in ("planned", "observed"):
        lines += ["", f"## Paired rates per paired day (a − b), {ref} reference", "",
                  "| a vs b | days (incomplete) | shortage a / b / diff [nominal 95 %] | surplus a / b / diff [nominal 95 %] | undefined draws |",
                  "|---|---|---|---|---|"]
        for g in aggregates:
            if g["reference"] != ref:
                continue
            cell = lambda m: (f"{f(g[m]['a_per_day'])} / {f(g[m]['b_per_day'])} / {f(g[m]['difference_per_day'])} "
                              f"[{f((g[m]['nominal_95'] or [None, None])[0])}, {f((g[m]['nominal_95'] or [None, None])[1])}]")
            lines.append(f"| {pair_label(g['pair'][0], meta)} vs {pair_label(g['pair'][1], meta)} | {g['days']} "
                         f"({g['incomplete_days']}) | {cell('shortage')} | {cell('surplus')} | {g['shortage']['bootstrap_undefined']} |")
    lines += ["", "Rates are per 24 h of paired exposure. Intervals are nominal 95 % whole-day bootstrap percentiles "
              "(conditional on the defined draws when some are undefined); calibration not established (U-27)."]
    return "\n".join(lines) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", help="vmselect prometheus URL (record mode)")
    ap.add_argument("--start", required=True, help="UTC day boundary, e.g. 2026-10-18T00:00:00Z")
    ap.add_argument("--stop", required=True, help="UTC day boundary (exclusive)")
    ap.add_argument("--archive", required=True, help="directory for the raw responses (must be new in record mode)")
    ap.add_argument("--replay", action="store_true", help="read the responses from --archive instead of the network")
    ap.add_argument("--p8-result", help="detector v5 result JSON (a scored run); without it a diagnostic run")
    ap.add_argument("--draws", help="shared draw bank JSON (default <archive>/draws.json; created when absent)")
    ap.add_argument("--n-boot", type=int, default=SCORED_DRAWS[0])
    ap.add_argument("--seed", type=int, default=SCORED_DRAWS[1])
    ap.add_argument("--json", required=True)
    ap.add_argument("--md")
    a = ap.parse_args(argv)
    ie = load("infra_events", os.path.join(PROD, "infra_events.py"))
    cp = load("challenge_profile", os.path.join(PROFILE_DIR, "challenge_profile.py"))
    start, stop = ie.parse(a.start), ie.parse(a.stop)
    if start % DAY or stop % DAY or stop <= start:
        raise SystemExit("start and stop must be UTC day boundaries with start < stop")
    if not a.replay and not a.prom:
        raise SystemExit("--prom is required unless --replay")
    if a.p8_result and (a.n_boot, a.seed) != SCORED_DRAWS:
        raise SystemExit(f"a scored run uses {SCORED_DRAWS[0]} draws with seed {SCORED_DRAWS[1]} (P9)")
    p8_bytes = open(a.p8_result, "rb").read() if a.p8_result else None          # bind the bytes actually consumed
    store = Store(a.prom, a.archive, replay=a.replay)
    taus, ready, rpm, k6 = fetch(store, start, stop, ie)
    planned = {t: cp.rate_at(t // MS) for t in taus}
    p8, p8_summary, gate_days = None, None, {}
    if p8_bytes is not None:
        frozen = {"detector": sha(os.path.join(PROD, "infra_events.py")),
                  "identities": sha(os.path.join(PROD, "infra-identities.json")),
                  "mask": sha(os.path.join(PROD, "validity-mask.json"))}
        chk = p8_check(json.loads(p8_bytes), start, stop, frozen, ie)
        classes = chk["classes"]
        p8 = lambda t: classes[t - t % SLOT]
        p8_summary = chk["summary"]
        for h, g in chk["gates"].items():
            if start <= h < stop:
                d = iso(h - h % DAY)[:10]
                gate_days.setdefault(d, {})[g] = gate_days.setdefault(d, {}).get(g, 0) + 1
    arm_rows, pair_rows = score(taus, planned, ready, rpm, p8)
    days = sorted({r["day"] for r in pair_rows})
    draws_path = a.draws or os.path.join(a.archive, "draws.json")
    if os.path.exists(draws_path):
        draws_bytes = open(draws_path, "rb").read()
        draws = json.loads(draws_bytes)
        check_draws(draws, days, a.n_boot, a.seed)
    else:
        draws = make_draws(days, a.n_boot, a.seed)
        draws_bytes = json.dumps(draws).encode()
        write_atomic(draws_path, draws_bytes.decode())
    aggregates = aggregate(pair_rows, draws)
    meta = {"scorer": "capacity.py v3", "scorer_sha256": sha(os.path.abspath(__file__)),
            "profile_sha256": {"challenge_profile.py": sha(os.path.join(PROFILE_DIR, "challenge_profile.py")),
                               "profile.json": sha(os.path.join(PROFILE_DIR, "profile.json"))},
            "inputs_manifest_sha256": sha(store.manifest), "replay": a.replay,
            "p8_result_sha256": sha_bytes(p8_bytes) if p8_bytes is not None else None, "p8": p8_summary,
            "draws_sha256": sha_bytes(draws_bytes), "draws_path": draws_path,
            "start": a.start, "stop": a.stop, "mixed_e2": start < E2_SWITCH,
            "k6_counts": "diagnostic increase() estimates; the P4 collector's finalized rows are authoritative",
            "note": ("P8 applied (validated)." if p8_bytes is not None else "DIAGNOSTIC: P8 not applied (nothing "
                     "excluded); not a scored result.") + " Descriptive only (U-27)."}
    out = {"meta": meta, "arms": arm_rows, "pairs": pair_rows, "aggregates": aggregates, "k6": k6, "gate_days": gate_days}
    write_atomic(a.json, json.dumps(out, indent=1))
    if a.md:
        write_atomic(a.md, markdown(arm_rows, pair_rows, aggregates, k6, gate_days, meta))
    print(json.dumps({"days": len(days), "p8": p8_bytes is not None, "replay": a.replay}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
