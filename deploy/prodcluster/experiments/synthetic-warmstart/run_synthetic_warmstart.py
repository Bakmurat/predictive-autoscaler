#!/usr/bin/env python3
"""Synthetic warm-start experiment (descriptive; NOT a benchmark result; the live arms are untouched).

Question (user, 2026-10-06): how would the forecasting arms behave today if they already had history? The live arms
cannot forecast yet (history starts 2026-10-05 06:40Z). This experiment gives every method 14 days of SYNTHETIC
history — the challenge-v1 traffic process with a fresh seed (never the sealed one), sampled like the live 1-minute
rate — followed by the REAL observed series of the arm since the history start, and replays the operator's rule
slot by slot on the real part only:
  S1      seasonal pattern only: the serving path's weighted percentile of the same slot on previous days
          (weights 0.3^(d-1)) at its directional percentile: 70 when the last hour fell, else 75 (MAPE term 0)
  E1      the deployed seasonal ensemble (0.5 Holt-Winters + 0.5 profile-AR) + q90 margin of its own lead errors
  E2      the same forecaster + q95 margin
  hybrid  the deployed BiLSTM (128/64/32, tanh, asymmetric loss) trained on the synthetic rows only, served as a
          blend with the pattern: the deployed ramp (0.70 -> 0.908 over the six steps), 0.85 and 0.95 (the trainer's
          candidates; its fourth candidate, pattern-only, is S1). The network alone is reported as hybrid_net.
  reactive_only, oracle (perfect knowledge of the next two slots)
HARNESS APPROXIMATION (Codex r28): the replay omits confidence damping, scheduling and readiness; the neural harness
fits its scaler on all synthetic rows; S1's MAPE term is taken as 0. Replay (deploy/eks-benchmark/scoring/
challenge_bakeoff.py `replay`, mirrored here with per-slot traces and checked against it): predictive pods = ceil(max(+10, +20 lead + margin) / 600); desired = clamp(max(predictive, reactive),
1, 12); scale-down after one full slot below Ready; a shortage is closed by the reactive path after 2 minutes.
Outputs: a JSON summary and per-slot traces; optionally imported into VictoriaMetrics as bench_synth_* series
(own metric names, label experiment=...) for the Grafana dashboard pa-synth-experiment.
"""
import argparse, datetime, hashlib, json, math, os, platform, subprocess, sys, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "deploy", "eks-benchmark", "scoring"))
sys.path.insert(0, os.path.join(ROOT, "ml-engine"))
sys.path.insert(0, os.path.join(ROOT, "deploy", "eks-benchmark", "workload", "challenge-v1"))
import numpy as np                     # noqa: E402
import challenge_bakeoff as cb         # noqa: E402
import neural_bakeoff as nb            # noqa: E402

SLOT, STEPS = cb.SLOT, cb.STEPS
PER_POD, MIN_R, MAX_R = 600.0, 1, 12


def iso(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(s):
    return int(datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp())


def fetch_real(prom, app, start, end):
    q = (f'sum(rate(istio_requests_total{{reporter="destination",destination_workload="{app}",'
         f'destination_workload_namespace="demo"}}[1m])) * 60')
    url = prom + "/api/v1/query_range?" + urllib.parse.urlencode({"query": q, "start": start, "end": end, "step": SLOT})
    raw = urllib.request.urlopen(url, timeout=120).read()
    body = json.loads(raw)
    if body.get("status") != "success" or body.get("isPartial"):
        raise SystemExit(f"real series: status {body.get('status')} partial {body.get('isPartial')}")
    res = body["data"]["result"]
    if len(res) != 1:
        raise SystemExit(f"real series: expected one series, got {len(res)}")
    pts = [(int(float(t)), float(v)) for t, v in res[0]["values"]]
    want = list(range(start, end + 1, SLOT))
    if [t for t, _ in pts] != want or not all(math.isfinite(v) and v >= 0 for _, v in pts):
        missing = sorted(set(want) - {t for t, _ in pts})
        raise SystemExit(f"real series incomplete: {len(pts)} of {len(want)} slots, missing {[iso(t) for t in missing[:5]]}")
    return pts, hashlib.sha256(raw).hexdigest()


def replay_trace(y, origins, lead_of):
    """cb.replay with a per-slot trace: {slot k+1: (lead, desired, ready after the slot's reactive catch-up, required)}."""
    ready = min(MAX_R, max(MIN_R, cb.pods(y[origins[0]], PER_POD)))
    prev_desired = ready
    short = surplus = 0.0
    trace = {}
    for k in origins:
        if k + 1 >= len(y) or not np.isfinite(y[k + 1]):
            break
        lead = lead_of(k)
        pred = cb.pods(lead, PER_POD) if np.isfinite(lead) else 0
        desired = min(MAX_R, max(MIN_R, max(pred, cb.pods(y[k], PER_POD))))
        if desired >= ready or prev_desired < ready:
            nxt = desired
        else:
            nxt = ready
        prev_desired = desired
        ready = nxt
        required = min(MAX_R, cb.pods(y[k + 1], PER_POD))
        ready_start = ready
        if ready >= required:
            surplus += (ready - required) * SLOT / 60.0
        else:
            short += (required - ready) * 2.0
            ready = required
        trace[k + 1] = (lead, desired, ready_start, required)
    return {"shortage_replica_min": round(short, 1), "surplus_replica_min": round(surplus, 1)}, trace


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", required=True)
    ap.add_argument("--app", default="nginx-test")
    ap.add_argument("--start", default="2026-10-05T06:40:00Z", help="first real slot (the live history start)")
    ap.add_argument("--end", help="last real slot (default: last completed grid slot)")
    ap.add_argument("--synth-days", type=int, default=14)
    ap.add_argument("--seed", type=int, default=101, help="synthetic traffic seed (never the sealed one)")
    ap.add_argument("--net-seed", type=int, default=1)
    ap.add_argument("--experiment", default="synthetic-warmstart-1")
    ap.add_argument("--json", required=True)
    ap.add_argument("--import-file", help="write Prometheus text (bench_synth_*) for VictoriaMetrics import")
    a = ap.parse_args()
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%MZ")
    label = f"{a.experiment}-s{a.seed}-n{a.net_seed}-{run_id}"
    cp = cb.cp
    assert a.seed != cp.PROFILE["seed"], "never use the sealed seed for synthetic history"
    start = parse(a.start)
    end = parse(a.end) if a.end else (int(datetime.datetime.now(datetime.timezone.utc).timestamp()) // SLOT) * SLOT - SLOT
    if start % SLOT or end % SLOT or end <= start:
        raise SystemExit("start/end must be grid instants with end > start")
    real, real_sha = fetch_real(a.prom, a.app, start, end)
    t_syn0 = start - a.synth_days * 86400
    rates, _ = cb.offered_rates(a.seed, 0, a.synth_days, t0=t_syn0)
    synth = [(t, v) for t, v in cb.sampled_series(rates, a.seed, t_syn0) if t < start]
    points = synth + [(t, v) for t, v in real if t >= start]
    grid = cb.se.Grid.from_points(points)
    y = grid.y
    o0 = grid.index(start)
    origins = list(range(o0, len(y) - cb.LEAD_STEPS))
    warm = list(range(max(0, o0 - cb.se.MARGIN_WINDOW_SLOTS), o0))
    print(f"synthetic {len(synth)} slots (seed {a.seed}) + real {len(real)} slots; {len(origins)} scored origins "
          f"{iso(start)}..{iso(end)}", flush=True)

    # seasonal methods (the serving modules via the harness), S1 pattern
    F = {k: cb.forecasts_at(grid, k, f"synth-{a.seed}") for k in warm + origins}
    lead = lambda f: max(f[0], f[1]) if np.isfinite(f[0]) and np.isfinite(f[1]) else float("nan")
    e1 = {k: lead(F[k]["e1"]) for k in F}
    m90 = cb.q90_margins(y, e1, origins)
    m95 = cb.q90_margins(y, e1, origins, quantile=0.95)
    def direction_pct(k):                   # lstm_model.predict: PRED-03 with the MAPE term at 0
        return 70.0 if np.mean(y[k - 5:k + 1]) < np.mean(y[k - 11:k - 5]) else 75.0
    pattern = {k: [nb.pattern_pct(y, k + s, direction_pct(k)) for s in range(1, STEPS + 1)] for k in warm + origins}
    # hybrid: the deployed network trained on the rows before the real part, served with the pattern blend
    feats = nb.time_features(grid.t0, len(y))
    yf = grid.filled()
    net, fit_info = nb.run_variant("deployed", a.net_seed, yf, feats, o0, origins)
    blend = lambda ws: {o: [(1 - w) * net[o][s] + w * pattern[o][s] for s, w in enumerate(ws)] for o in origins}
    hybrids = {"hybrid_ramp": blend(nb.PATTERN_WEIGHTS), "hybrid_0.85": blend([0.85] * STEPS),
               "hybrid_0.95": blend([0.95] * STEPS), "hybrid_net": net}
    arms = {
        "reactive_only": lambda k: float("nan"),
        "S1": lambda k: lead(pattern[k]),
        "E1": lambda k: e1[k] + m90.get(k, 0.0),
        "E2": lambda k: e1[k] + m95.get(k, 0.0),
        **{n: (lambda h: (lambda k: lead(h[k])))(h) for n, h in hybrids.items()},
        "oracle": lambda k: max(y[k + 1], y[k + 2]) if k + 2 < len(y) else float("nan"),
    }
    forecasts = {"S1": pattern, "E1": {k: F[k]["e1"] for k in F}, **hybrids}
    sha = lambda f: hashlib.sha256(open(f, "rb").read()).hexdigest()
    try:
        head = subprocess.run(["git", "-C", ROOT, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", ROOT, "status", "--porcelain", "--", HERE, "deploy/eks-benchmark/scoring",
                                "ml-engine/models"], capture_output=True, text=True).stdout.strip()
    except OSError:
        head, dirty = None, None
    import tensorflow as tf, sklearn, pandas
    identity = {"git_head": head, "dirty_paths": dirty.splitlines() if dirty else [],
                "sha256": {os.path.relpath(f, ROOT): sha(f) for f in
                           [os.path.abspath(__file__), cb.__file__, nb.__file__, cb.se.__file__, cb.cp.__file__]},
                "versions": {"python": platform.python_version(), "numpy": np.__version__, "pandas": pandas.__version__,
                             "tensorflow": tf.__version__, "sklearn": sklearn.__version__},
                "real_input_sha256": real_sha, "prom": a.prom}
    out = {"experiment": label, "app": a.app, "seed": a.seed, "net_seed": a.net_seed, "synth_days": a.synth_days,
           "identity": identity,
           "uncapped_required_over_max": sum(1 for k in origins if k + 1 < len(y) and cb.pods(y[k + 1], PER_POD) > MAX_R),
           "label": "offline harness approximation; 14 synthetic days then real observations; not benchmark evidence",
           "real_window": [iso(start), iso(end)], "origins": len(origins), "hybrid_fit": fit_info,
           "note": "descriptive experiment on synthetic warm-up history; not a benchmark result", "arms": {}}
    traces = {}
    for name, fn in arms.items():
        res, tr = replay_trace(y, origins, fn)
        ref = cb.replay(y, origins, fn, PER_POD, MIN_R, MAX_R)
        assert (res["shortage_replica_min"], res["surplus_replica_min"]) == (ref["shortage_replica_min"], ref["surplus_replica_min"]), name
        traces[name] = tr
        mae = {}
        if name in forecasts:
            fk = forecasts[name]
            e10 = [abs(fk[k][0] - y[k + 1]) for k in origins if k + 1 < len(y) and np.isfinite(fk[k][0])]
            e20 = [abs(fk[k][1] - y[k + 2]) for k in origins if k + 2 < len(y) and np.isfinite(fk[k][1])]
            mae = {"mae10_rpm": round(float(np.mean(e10)), 1) if e10 else None,
                   "mae20_rpm": round(float(np.mean(e20)), 1) if e20 else None}
        refused = sum(1 for k in origins if k + 1 < len(y) and not np.isfinite(fn(k)))
        out["arms"][name] = dict(res, mean_ready=ref["mean_ready"], scale_changes=ref["changes"], scored_slots=ref["slots"],
                                 refused_origins=refused if name != "reactive_only" else None, **mae)
        if ref["slots"] != len(origins):
            raise SystemExit(f"{name}: replay scored {ref['slots']} of {len(origins)} origins")
        print(f"{name:14s} slots {ref['slots']} refused {refused:3d} shortage {res['shortage_replica_min']:8.1f}  surplus {res['surplus_replica_min']:8.1f}  "
              f"mean ready {ref['mean_ready']}  {mae}", flush=True)
    out["traces"] = {name: {iso(grid.t0 + j * SLOT): {"lead_rpm": None if not np.isfinite(ld) else round(float(ld), 1),
                                                     "desired": d, "ready": r, "required": q,
                                                     "actual_rpm": round(float(y[j]), 1)}
                            for j, (ld, d, r, q) in tr.items()} for name, tr in traces.items()}
    json.dump(out, open(a.json, "w"), indent=1)
    if a.import_file:
        lines = []
        lab = f'experiment="{label}",app="{a.app}"'
        for k in origins:
            if k + 1 < len(y) and np.isfinite(y[k + 1]):
                ts = (grid.t0 + (k + 1) * SLOT) * 1000
                lines.append(f"bench_synth_actual_rpm{{{lab}}} {y[k + 1]:.1f} {ts}")
                if k + 2 < len(y):
                    lines.append(f"bench_synth_actual_window_max_rpm{{{lab}}} {max(y[k + 1], y[k + 2]):.1f} {ts}")
        for name, tr in traces.items():
            for j, (ld, desired, ready, required) in tr.items():
                ts = (grid.t0 + j * SLOT) * 1000
                lines.append(f'bench_synth_ready_replicas{{{lab},arm="{name}"}} {ready} {ts}')
                lines.append(f'bench_synth_shortage_replica_min{{{lab},arm="{name}"}} {max(0, required - ready) * 2.0} {ts}')
                lines.append(f'bench_synth_surplus_replica_min{{{lab},arm="{name}"}} {max(0, ready - required) * SLOT / 60.0} {ts}')
                if name == "reactive_only":
                    lines.append(f"bench_synth_required_replicas{{{lab}}} {required} {ts}")
                if np.isfinite(ld) and name != "reactive_only":
                    lines.append(f'bench_synth_lead_rpm{{{lab},arm="{name}"}} {ld:.1f} {ts}')
        open(a.import_file, "w").write("\n".join(lines) + "\n")
        print(f"wrote {len(lines)} import lines to {a.import_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
