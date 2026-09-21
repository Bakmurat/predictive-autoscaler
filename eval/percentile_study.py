#!/usr/bin/env python3
"""Fixed percentile rules versus the adaptive rule, on evidence (Codex C-69 / D-90).

The adaptive rule is NOT signed error correction. `lstm_model.py:492-508` picks
    direction_pct = 70 if the last hour is below the hour before it, else 75
    mape_pct      = 75 - max(0, MAPE-10) * 1.25, clamped to [50, 75]   (UNSIGNED)
and combines them. So it stays at 75 on rising or flat input and LOWERS the percentile to 70
on falling input -- which reduces the forecast further and can worsen under-prediction when the
fall is temporary. Unsigned MAPE cannot distinguish over- from under-prediction at all.

Two things are measured, separately:

  1. Inertness. `weighted_percentile():65-66` returns the sole observation regardless of the
     percentile, so with one day of support the whole adaptive apparatus changes nothing. This
     is checked on the real benchmark series, where support is exactly 1 for every step.

  2. Comparison. With multi-day support the percentile does move the forecast. Fixed rules
     (50, 70, 75) are compared against the adaptive rule over matured targets, reporting MAE,
     signed bias, and the under-prediction rate -- the quantity the 2:1 loss and the percentile
     are both supposed to control.

No minimum-day threshold is invented (D-90); the rules are compared and the numbers reported.

Usage:
    eval/.venv/bin/python eval/percentile_study.py --series eval/data/series-fresh.json \
        --out eval/percentile-study-<utc>.json

LIMITATION (Codex C-77), recorded 2026-09-22
--------------------------------------------
The "adaptive_deployed" arm below calls effective_percentile(window) with the MAPE
argument left at its default of 0.0 (accuracy_baseline.py:53). At MAPE 0 the mape_pct
term is pinned at 75, so the arm exercises only the DIRECTIONAL 70/75 switch -- not the
feedback loop that the deployed rule actually runs, where a matured-error MAPE above 10
pulls the percentile down towards 50.

So this study does not test the deployed adaptive rule. It tests one half of it. The
conclusion it supports is narrower than "the adaptive rule is inside noise": it is
"the directional switch alone is inside noise on these series".

To mean more, the study needs: a chronological replay that feeds MATURED forecast errors
back into mape_for_floor as the deployed API does, held-out paired errors per origin, a
signed-bias column, and an uncertainty estimate -- "inside noise" is a claim about
variance and requires one. None of those are present.

The percentile policy is UNCHANGED as a result of this study.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "ml-engine"))
sys.path.insert(0, str(REPO / "eval"))

from models.lstm_model import weighted_percentile  # noqa: E402
from accuracy_baseline import effective_percentile, load_series  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144
STEPS_AHEAD = 6


def pattern_at(series: pd.Series, origin: pd.Timestamp, pct: float) -> list[dict]:
    """Previous-day pattern for each step at a given percentile, with its support."""
    tol = pd.Timedelta(minutes=5)
    out = []
    for step in range(STEPS_AHEAD):
        target = origin + pd.Timedelta(minutes=10 * (step + 1))
        vals, wts = [], []
        for d in range(1, 8):
            want = target - pd.Timedelta(days=d)
            if want < series.index[0] - tol:
                break
            pos = series.index.get_indexer([want], method="nearest")[0]
            if pos >= 0 and abs(series.index[pos] - want) <= tol:
                vals.append(float(series.iloc[pos]))
                wts.append(0.3 ** (d - 1))
        out.append({
            "step": step + 1,
            "support": len(vals),
            "value": float(weighted_percentile(vals, wts, pct)) if vals else None,
        })
    return out


def synthetic_multiday(days: int, end: datetime, seed: int) -> pd.Series:
    """A repeating daily profile with day-to-day noise, so support > 1 actually matters."""
    rng = np.random.default_rng(seed)
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = []
    for t in idx:
        minute = t.hour * 60 + t.minute
        base = 600 + 400 * np.sin(2 * np.pi * minute / 1440.0)
        vals.append(max(1.0, base * (1.0 + rng.normal(0, 0.18))))
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


def score(series: pd.Series, origins: list[pd.Timestamp], rule) -> dict:
    """Score one percentile rule over matured targets. `rule(window)->pct`."""
    errs, unders, per_step = [], 0, {s: [] for s in range(1, STEPS_AHEAD + 1)}
    scored = skipped = 0
    for origin in origins:
        pos = series.index.get_loc(origin)
        window = series.iloc[max(0, pos - PER_DAY + 1): pos + 1].values
        pct = rule(window)
        for rec in pattern_at(series, origin, pct):
            tpos = pos + rec["step"]
            if rec["value"] is None or tpos >= len(series):
                skipped += 1
                continue
            actual = float(series.iloc[tpos])
            err = rec["value"] - actual      # signed: positive = over-prediction
            errs.append(err)
            per_step[rec["step"]].append(err)
            if err < 0:
                unders += 1
            scored += 1
    a = np.array(errs, dtype=float)
    return {
        "scored": scored,
        "skipped": skipped,
        "mae": float(np.mean(np.abs(a))) if len(a) else None,
        "signed_bias": float(np.mean(a)) if len(a) else None,
        "under_rate": float(unders / scored) if scored else None,
        "per_step_mae": {k: (float(np.mean(np.abs(v))) if v else None)
                         for k, v in per_step.items()},
        "per_step_bias": {k: (float(np.mean(v)) if v else None)
                          for k, v in per_step.items()},
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--history-start", default="2026-09-20T18:30:00Z")
    ap.add_argument("--days", type=int, default=9)
    ap.add_argument("--seeds", type=int, nargs="+", default=[11, 29, 47])
    args = ap.parse_args()

    result = {
        "kind": "percentile_fixed_vs_adaptive",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # --- 1. Inertness on the real series -------------------------------------------------
    real = load_series(Path(args.series), args.history_start)
    inert_rows, support_counts = [], []
    last_usable = len(real) - STEPS_AHEAD - 1
    for pos in range(max(PER_DAY - 1, last_usable - 20), last_usable + 1):
        origin = real.index[pos]
        vals = {}
        for pct in (50, 70, 75):
            vals[pct] = [r["value"] for r in pattern_at(real, origin, pct)]
        supports = [r["support"] for r in pattern_at(real, origin, 75)]
        support_counts.extend(s for s in supports if s)
        identical = all(
            (a is None and b is None) or (a is not None and b is not None and abs(a - b) < 1e-9)
            for a, b in zip(vals[50], vals[75]))
        inert_rows.append({"origin": origin.isoformat(), "supports": supports,
                           "pct50_equals_pct75": identical})
    result["limitations"] = {
        "adaptive_arm_is_partial": (
            "effective_percentile() is called with mape_for_floor defaulting to 0.0, so the "
            "MAPE feedback term is pinned at 75 and only the directional 70/75 switch is "
            "exercised; the deployed rule's feedback loop is NOT tested (Codex C-77)"),
        "no_uncertainty_estimate": (
            "no held-out paired errors, no signed bias, no variance estimate -- 'inside noise' "
            "is not supported by this design"),
        "policy_unchanged": True,
    }
    result["inertness_on_real_series"] = {
        "origins": len(inert_rows),
        "max_support_seen": int(max(support_counts)) if support_counts else 0,
        "all_percentiles_identical": all(r["pct50_equals_pct75"] for r in inert_rows),
        "note": ("with support 1 weighted_percentile returns the sole observation regardless "
                 "of the percentile, so the adaptive rule changes nothing here"),
        "rows": inert_rows[:5],
    }

    # --- 2. Comparison where support > 1 -------------------------------------------------
    rules = {
        "adaptive_deployed": lambda w: effective_percentile(w)["effective_pct"],
        "fixed_50": lambda w: 50.0,
        "fixed_70": lambda w: 70.0,
        "fixed_75": lambda w: 75.0,
    }
    per_seed = []
    for seed in args.seeds:
        end = datetime(2026, 9, 21, 12, 0)
        s = synthetic_multiday(args.days, end, seed)
        last = len(s) - STEPS_AHEAD - 1
        # Origins from the last two days only, so every step has multi-day support.
        origins = [s.index[p] for p in range(last - 2 * PER_DAY, last + 1, 3)]
        row = {"seed": seed, "origins": len(origins)}
        for name, rule in rules.items():
            row[name] = score(s, origins, rule)
        per_seed.append(row)

    summary = {}
    for name in rules:
        maes = [r[name]["mae"] for r in per_seed]
        biases = [r[name]["signed_bias"] for r in per_seed]
        unders = [r[name]["under_rate"] for r in per_seed]
        summary[name] = {
            "mae_mean": float(np.mean(maes)),
            "mae_per_seed": maes,
            "signed_bias_mean": float(np.mean(biases)),
            "under_rate_mean": float(np.mean(unders)),
        }
    best = min(summary, key=lambda k: summary[k]["mae_mean"])
    adaptive = summary["adaptive_deployed"]["mae_mean"]
    result["comparison"] = {
        "days": args.days,
        "seeds": args.seeds,
        "per_seed": per_seed,
        "summary": summary,
        "best_by_mae": best,
        "adaptive_vs_best_pct": float((adaptive - summary[best]["mae_mean"])
                                      / summary[best]["mae_mean"] * 100.0),
    }

    Path(args.out).write_text(json.dumps(result, indent=2))
    print(json.dumps({
        "inert_on_real": result["inertness_on_real_series"]["all_percentiles_identical"],
        "max_support": result["inertness_on_real_series"]["max_support_seen"],
        "summary": {k: {"mae": round(v["mae_mean"], 2),
                        "bias": round(v["signed_bias_mean"], 2),
                        "under_rate": round(v["under_rate_mean"], 3)}
                    for k, v in summary.items()},
        "best_by_mae": best,
        "adaptive_worse_than_best_pct": round(result["comparison"]["adaptive_vs_best_pct"], 2),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
