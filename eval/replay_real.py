#!/usr/bin/env python3
"""Chronological replay of the REAL accumulated benchmark traffic (Codex D-124).

Every comparison in this repository so far has run on SYNTHETIC series. This file replays
the genuine history the benchmark cluster has accumulated -- in chronological order, with
rolling origins, each forecaster seeing only `series[:i+1]` -- and drives the REAL Go
controller over the result, one replay per arm with its own state.

It is OFFLINE. It reads an export produced by `eval/export_benchmark_series.py`; it never
contacts the cluster itself.

THE RULE THAT DECIDES WHETHER IT SCORES ANYTHING
------------------------------------------------
Stated here, in code, before the data is read, so it cannot be relaxed after seeing the
answer. The replay REFUSES to report a comparison unless all three hold:

  1. At least `MIN_ORIGIN_DAYS` complete daily cycles of origins. The workload is
     daily-periodic. A window shorter than several whole cycles measures the time of day
     rather than the forecaster, and pairs each arm against a single phase of the rhythm.
  2. At least `MIN_WARMUP_DAYS` days of history BEFORE the first origin. The deployed
     pattern component averages up to seven same-time-yesterday observations; with one day
     back it collapses to a single unweighted value per step (D-84) -- that is a different
     estimator, and scoring it would not be scoring what is deployed.
  3. At least `MIN_NON_OVERLAPPING_BLOCKS` non-overlapping origin blocks. Origins six steps
     apart share no TARGET, origins closer than that do; overlapping rolling origins are
     plainly not independent samples (Codex C-92), so the honest count is
     origins / STEPS_AHEAD.

     NOT INDEPENDENT, ONLY NON-OVERLAPPING (Codex C-100). Disjoint targets are the weakest
     of the things independence would require. Blocks that share no target still share the
     history every forecaster reads, the same daily periodicity, the same generator, and --
     because the controller is replayed as one continuous run per arm -- the carried
     controller state that entered the block. Treat the count as a bound on how much
     non-overlapping evidence exists, never as a sample size for an interval that assumes
     independence. A block bootstrap over WHOLE DAYS remains the honest resampling unit.

TWO MODES, WHICH CANNOT BE CONFUSED FOR EACH OTHER (Codex C-100)
---------------------------------------------------------------
The warmup used to be a free `--warmup-days` flag defaulting to 1 while the scoring rule
required `MIN_WARMUP_DAYS` = 3. The same export therefore produced two different answers
and only the flag said which: on an 870-point history, 720 origins and FAIL under the
default, 432 origins and PASS with three days. That is exactly the confusion a predeclared
gate exists to prevent, so the flag no longer decides it -- `--mode` does, and it is
REQUIRED so that neither mode can be reached by accident:

  --mode scoring   warmup is FORCED to MIN_WARMUP_DAYS. `--warmup-days` is refused. This is
                   the only mode that can ever emit SCORED, and only when all three checks
                   pass.
  --mode census    warmup is free (default `CENSUS_WARMUP_DAYS` = 1). The verdict is ALWAYS
                   "CENSUS ONLY -- NOT SCORED", even if the checks happen to pass, because a
                   census run measures how much history exists, not which arm is better.

Below the bar the run still executes end to end -- the forecasters and the real controller
do run over the real history -- but the output is a CENSUS, explicitly not a comparison.
Descriptive measurement before the bar is met is legitimate; calling it a score is not.

    eval/.venv/bin/python eval/replay_real.py --mode census \
        --export eval/data/benchmark-real-<utc>.json --out eval/replay-real-<utc>.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "ml-engine"))

import offline_eval as oe  # noqa: E402
from data import gapfill  # noqa: E402
from models.lstm_model import STEPS_AHEAD  # noqa: E402
from training.train_lstm_from_vm import (CADENCE_TOLERANCE_S,  # noqa: E402
                                         EXPECTED_CADENCE_S, preflight_history)

PER_DAY = oe.PER_DAY
GRID_MIN = oe.GRID_MIN

# ---- Predeclared minimum scale (see the module docstring) -----------------------------
MIN_ORIGIN_DAYS = 3
MIN_WARMUP_DAYS = 3
MIN_NON_OVERLAPPING_BLOCKS = 72      # = MIN_ORIGIN_DAYS * PER_DAY / STEPS_AHEAD
                                     # non-overlapping, NOT independent -- see the docstring

CENSUS_WARMUP_DAYS = 1               # census mode only; can never produce a SCORED verdict

MODE_SCORING = "scoring"
MODE_CENSUS = "census"

MIN_REPLICAS, MAX_REPLICAS = 1, 12

ARMS = {
    "persistence": oe.Persistence(),
    "previous_day": oe.PreviousDay(),
    "seasonal_pattern": oe.SeasonalPattern(),
    "trend_adaptive": oe.TrendAdaptive(),
}


def load_series(export_path: str, mask_path: str):
    """Export -> masked, grid-checked, gap-filled series.

    Uses the SAME functions the trainer uses (`ml-engine/data/gapfill.py`) so the replay and
    the trainer cannot disagree about which samples are valid. It deliberately does NOT use
    `preflight_history`'s verdict as a gate: that gate asks "is there enough history to
    TRAIN", a different and much larger question than "which samples are valid". The
    trainer's verdict is recorded anyway, because "the trainer would still refuse" is itself
    a finding about how short this history is.
    """
    payload = json.loads(Path(export_path).read_text())
    result = payload["response"]["data"]["result"]
    if not result:
        raise SystemExit("the export contains no series")
    if len(result) > 1:
        raise SystemExit(f"the export contains {len(result)} series; expected exactly one")
    raw = result[0]["values"]
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp(int(t), unit="s", tz="UTC") for t, _ in raw],
        "value": [float(v) for _, v in raw]})
    mask = json.loads(Path(mask_path).read_text())

    # --- grid normalisation, exactly as the trainer does it -----------------------------
    d = df.copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    n_nonfinite = int((~np.isfinite(d["value"])).sum())
    d = d[np.isfinite(d["value"])].sort_values("timestamp")
    n_dup = int(d["timestamp"].duplicated().sum())
    d = d.drop_duplicates("timestamp", keep="last")
    epoch = ((d["timestamp"] - pd.Timestamp("1970-01-01", tz="UTC"))
             // pd.Timedelta(seconds=1)).astype("int64")
    slot = ((epoch + EXPECTED_CADENCE_S // 2) // EXPECTED_CADENCE_S) * EXPECTED_CADENCE_S
    off = (epoch - slot).abs()
    n_offgrid = int((off > CADENCE_TOLERANCE_S).sum())
    keep = off <= CADENCE_TOLERANCE_S
    d = d[keep].copy()
    d["slot"] = slot[keep]
    d = d.drop_duplicates("slot", keep="last")

    # --- validity mask and bounded interior fill, via the trainer's own module ----------
    points = list(zip(d["slot"].astype(int).tolist(), d["value"].astype(float).tolist()))
    points, mask_info = gapfill.apply_mask(points, mask, role="benchmark")
    if not points:
        raise SystemExit("nothing left after the validity mask")
    run_pts, flags, fill_rec = gapfill.fill_interior_gaps(
        points, cutoff=int(points[-1][0]), boundaries=[],
        forbidden=gapfill.mask_intervals(mask))

    series = pd.Series([v for _, v in run_pts],
                       index=pd.DatetimeIndex([pd.Timestamp(t, unit="s") for t, _ in run_pts]))
    imputed = np.asarray(flags, dtype=bool)

    ok, why, _prepared, pf = preflight_history(df, mask=mask, role="benchmark", fill=True)
    return series, imputed, {
        "trainer_verdict": why,
        "trainer_would_train": bool(ok),
        "trainer_preflight": {k: pf.get(k) for k in
                              ("raw_points", "offgrid_dropped", "expected_slots",
                               "present_slots", "missing_slots", "contiguous_runs",
                               "contiguous_run_points", "run_first", "run_last")},
        "grid_normalisation": {"nonfinite_dropped": n_nonfinite, "duplicates_dropped": n_dup,
                               "offgrid_dropped": n_offgrid},
        "mask": mask_info,
        "gap_fill": fill_rec,
        "export": payload["provenance"],
        "exported_at": payload["exported_at"],
    }


def eligible_origins(series: pd.Series, imputed: np.ndarray, warmup_points: int):
    """Chronological rolling origins: enough history behind, six GENUINE targets ahead."""
    out = []
    for i in range(warmup_points, len(series) - STEPS_AHEAD):
        if imputed[i]:
            continue                                   # do not forecast from a filled slot
        if imputed[i + 1:i + 1 + STEPS_AHEAD].any():
            continue                                   # never score against a filled target
        out.append(i)
    return out


def genuine_previous_day_steps(series: pd.Series, i: int) -> int:
    """How many of the six targets have a real same-time-yesterday observation."""
    n = 0
    for h in range(1, STEPS_AHEAD + 1):
        if i + h - PER_DAY >= 0:
            n += 1
    return n


def run(series, imputed, warmup_points, use_replay=True, log=print):
    origins = eligible_origins(series, imputed, warmup_points)
    if not origins:
        return {"origins": 0, "first_origin_index": None, "arms": {}}

    truth = np.stack([series.values[i + 1:i + 1 + STEPS_AHEAD] for i in origins])
    genuine = [genuine_previous_day_steps(series, i) for i in origins]

    forecasts, timings = {}, {}
    for name, pred in ARMS.items():
        t0 = time.time()
        rows = []
        for i in origins:
            hist = series.iloc[:i + 1]                 # chronological: nothing after i
            rows.append(np.asarray(pred.forecast(hist, series.index[i], STEPS_AHEAD),
                                   dtype=float))
        forecasts[name] = np.stack(rows)
        timings[name] = round(time.time() - t0, 2)
        log(f"  {name:18s} {len(origins)} origins  [{timings[name]}s]")

    target_rpm = float(np.percentile(series.values[:origins[0]], 60)) / 3.0
    arms = {}
    for name, fc in forecasts.items():
        finite = np.all(np.isfinite(fc), axis=1)
        mae = float(np.mean(np.abs(fc[finite] - truth[finite]))) if finite.any() else None
        bias = float(np.mean(fc[finite] - truth[finite])) if finite.any() else None
        arms[name] = {
            "origins": len(origins),
            "non_finite_origins": int((~finite).sum()),
            "mae": None if mae is None else round(mae, 2),
            "bias": None if bias is None else round(bias, 2),
            "per_step_mae": [round(float(np.mean(np.abs(fc[finite, s] - truth[finite, s]))), 2)
                             for s in range(STEPS_AHEAD)] if finite.any() else None,
            "seconds": timings[name],
        }

    if use_replay:
        actual = series.iloc[origins[0]:origins[-1] + 1 + STEPS_AHEAD]
        for name, fc in forecasts.items():
            fmap = {series.index[i]: fc[k] for k, i in enumerate(origins)}
            rep = oe.replay_controller(actual, fmap, target_rpm=target_rpm,
                                       min_r=MIN_REPLICAS, max_r=MAX_REPLICAS)
            arms[name].update({
                "replica_minutes": rep["replica_minutes"],
                "scaling_events": rep["scaling_events"],
                "shortage_minutes": rep["deficit_minutes"],
                "shortage_replicas_max": rep["deficit_replicas_max"],
                "minutes_demand_exceeded_ceiling": rep["minutes_demand_exceeded_ceiling"],
                "decisions_from": rep["decisions_from"],
            })
            log(f"  replay {name:18s} {rep['replica_minutes']:8.1f} rm  "
                f"{rep['deficit_minutes']:6.1f} short  {rep['scaling_events']:3d} ev  "
                f"({rep['decisions_from']})")

    return {
        "origins": len(origins),
        "first_origin_index": origins[0],
        "first_origin": series.index[origins[0]].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_origin": series.index[origins[-1]].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "target_rpm_per_replica": round(target_rpm, 1),
        "genuine_previous_day_steps_per_origin": {
            "min": int(min(genuine)), "max": int(max(genuine)),
            "all_six": int(sum(1 for g in genuine if g == STEPS_AHEAD))},
        "arms": arms,
    }


def census(series, imputed, origins_n: int, first_origin_index: int | None) -> dict:
    """The three predeclared checks, each answered with the number it was answered by."""
    span_h = (series.index[-1] - series.index[0]).total_seconds() / 3600.0
    origin_days = origins_n / PER_DAY
    blocks = origins_n // STEPS_AHEAD
    warmup_days = (first_origin_index / PER_DAY) if first_origin_index is not None else 0.0
    checks = {
        "origin_days": {"observed": round(origin_days, 2), "required": MIN_ORIGIN_DAYS,
                        "pass": origin_days >= MIN_ORIGIN_DAYS},
        "warmup_days_before_first_origin": {
            "observed": round(warmup_days, 2), "required": MIN_WARMUP_DAYS,
            "pass": warmup_days >= MIN_WARMUP_DAYS},
        "non_overlapping_blocks": {"observed": blocks,
                                   "required": MIN_NON_OVERLAPPING_BLOCKS,
                                   "pass": blocks >= MIN_NON_OVERLAPPING_BLOCKS,
                                   "note": "non-overlapping TARGETS only; these blocks still "
                                           "share history, daily structure and carried "
                                           "controller state, so this is NOT an independent "
                                           "sample count (Codex C-100)"},
    }
    return {
        "valid_points": int(len(series)),
        "imputed_points": int(imputed.sum()),
        "span_hours": round(span_h, 2),
        "first": series.index[0].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last": series.index[-1].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": checks,
        "scoreable": all(c["pass"] for c in checks.values()),
    }


def points_needed() -> int:
    """History length at which the predeclared minimum scale is first met.

    Independent of the mode: the SCORING gate always requires MIN_WARMUP_DAYS of warmup and
    MIN_ORIGIN_DAYS of origins, so the answer is the same however a census was run.
    """
    return (MIN_WARMUP_DAYS + MIN_ORIGIN_DAYS) * PER_DAY + STEPS_AHEAD


def resolve_warmup_days(mode: str, warmup_days: int | None) -> int:
    """The warmup a mode is allowed to use. Scoring does not get a choice (Codex C-100)."""
    if mode == MODE_SCORING:
        if warmup_days is not None and warmup_days != MIN_WARMUP_DAYS:
            raise ValueError(
                f"--warmup-days is refused in scoring mode: the gate requires "
                f"{MIN_WARMUP_DAYS} days of warmup and the run may not choose another. "
                f"Use --mode census to run a descriptive replay with a shorter warmup.")
        return MIN_WARMUP_DAYS
    if mode == MODE_CENSUS:
        return CENSUS_WARMUP_DAYS if warmup_days is None else warmup_days
    raise ValueError(f"unknown mode {mode!r}")


def verdict_for(mode: str, scoreable: bool) -> str:
    """Three outcomes, worded so no two of them can be read as each other."""
    if mode == MODE_CENSUS:
        return "CENSUS ONLY -- NOT SCORED (census mode: scoring not attempted)"
    if scoreable:
        return "SCORED"
    return "NOT SCORED -- SCORING GATE FAILED"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--export", required=True)
    ap.add_argument("--mask", default=str(ROOT / "deploy/eks-benchmark/validity-mask.json"))
    ap.add_argument("--mode", required=True, choices=[MODE_CENSUS, MODE_SCORING],
                    help="census: descriptive only, free warmup, NEVER emits SCORED. "
                         "scoring: warmup forced to MIN_WARMUP_DAYS, the only mode that can.")
    ap.add_argument("--warmup-days", type=int, default=None,
                    help=f"census mode only (default {CENSUS_WARMUP_DAYS}). Refused in "
                         f"scoring mode, where the gate fixes it at {MIN_WARMUP_DAYS}.")
    ap.add_argument("--no-replay", action="store_true")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    try:
        warmup_days = resolve_warmup_days(args.mode, args.warmup_days)
    except ValueError as exc:
        ap.error(str(exc))

    series, imputed, prov = load_series(args.export, args.mask)
    warmup_points = warmup_days * PER_DAY
    print(f"series: {len(series)} valid points, {imputed.sum()} imputed, "
          f"{series.index[0]} -> {series.index[-1]}")

    result = run(series, imputed, warmup_points, use_replay=not args.no_replay)
    cen = census(series, imputed, result["origins"], result.get("first_origin_index"))

    have = len(series)
    need = points_needed()
    short_by = max(0, need - have)
    verdict = verdict_for(args.mode, cen["scoreable"])

    payload = {
        "kind": "chronological_replay_of_real_benchmark_traffic",
        "generated_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mode": args.mode,
        "warmup_days_used": warmup_days,
        "verdict": verdict,
        "predeclared_minimum_scale": {
            "min_origin_days": MIN_ORIGIN_DAYS,
            "min_warmup_days": MIN_WARMUP_DAYS,
            "min_non_overlapping_blocks": MIN_NON_OVERLAPPING_BLOCKS,
            "blocks_are_non_overlapping_not_independent": True,
            "points_required": need,
            "points_available": have,
            "points_short": short_by,
            "hours_short": round(short_by * GRID_MIN / 60.0, 1),
            "stated": "in eval/replay_real.py before the data is read",
        },
        "provenance": prov,
        "census": cen,
        "replay": result,
        "what_this_can_establish": [
            "that the real accumulated series runs end to end through the deployed "
            "forecasters and the real Go controller, in chronological order, with rolling "
            "origins and no look-ahead",
            "how much genuine history exists after the validity mask, and how many origins "
            "it yields",
            "which arms are even DEFINED on this much history, and how many of each "
            "forecast's six steps rest on a real same-time-yesterday observation",
        ],
        "what_this_cannot_establish": [
            "which forecaster is better: the window covers too few whole daily cycles, and "
            "overlapping rolling origins are not independent samples",
            "an independent sample count: non-overlapping blocks share history, daily "
            "structure and carried controller state, so the block count bounds the evidence "
            "rather than sizing an interval (Codex C-100)",
            "anything about the neural arm: no model has ever been trained on this history "
            "(the trainer requires 235 points and refuses below that), so there is no "
            "network forecast to replay",
            "an operational benefit: the controller replay models capacity arriving after a "
            "readiness delay and prices no queueing, latency or dropped request",
            "generalisation beyond this one workload, one cluster and one generator design",
        ],
    }
    if args.mode == MODE_CENSUS:
        payload["not_scored_because"] = [
            "census mode: this run measures how much history exists and that the pipeline "
            "runs end to end. Scoring was not attempted, whatever the checks say."]
    elif not cen["scoreable"]:
        payload["refused_to_score_because"] = [
            f"{k}: observed {v['observed']}, required {v['required']}"
            for k, v in cen["checks"].items() if not v["pass"]]

    Path(args.out).write_text(json.dumps(payload, indent=2, default=str))
    print(f"\nmode: {args.mode} (warmup {warmup_days} d)")
    print(f"verdict: {verdict}")
    print(f"origins: {result['origins']}  "
          f"({result['origins'] / PER_DAY:.2f} daily cycles, "
          f"{result['origins'] // STEPS_AHEAD} non-overlapping -- not independent -- blocks)")
    if short_by:
        print(f"short by {short_by} points = {short_by * GRID_MIN / 60.0:.1f} h of history")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
