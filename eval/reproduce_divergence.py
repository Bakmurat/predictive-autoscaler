#!/usr/bin/env python3
"""Reproduce the conditions under which the network diverged, then test the arms there.

Codex C-78: the stabilization run cannot claim tanh removes the divergence, because its
matched ReLU control also had zero failed seeds (spread 1.38 against tanh's 1.10). The
ten-orders-of-magnitude failure appeared under DIFFERENT conditions:

    unseeded initialisation, 12 epochs, EXPANDING history (not a rolling 7-day window)

So the honest experiment is: reproduce that configuration, confirm the failure still
occurs, and only then ask whether activation or gradient clipping prevents it. Anything
else is testing a fix against conditions that never failed.

RECORD SHAPE (Codex C-95 / D-120)
---------------------------------
The first version of this file keyed every repetition by `str(model_seed)`. Unseeded
repetitions all have the seed `None`, so all five landed under the single key "None" and
overwrote each other: the JSON kept one outcome per arm while the console log held five.
Every repetition now carries a UNIQUE id (`rep01`...), its own `failure_type`, and the
spread is computed over SUCCESSFUL repetitions only, with the denominator stated.

WHAT THIS EXPERIMENT CAN AND CANNOT SAY
---------------------------------------
Supported: "failures occurred only in the clipping arm here."
NOT supported: "clipping caused divergence." The comparison is small, unmatched (unseeded
initialisations are not paired across arms) and each repetition trains ONCE, whereas the
withdrawn sweep retrained on a rolling schedule roughly eleven times per run. Single-training
trials therefore do not reproduce the original rolling-retraining exposure at all.

Usage:
    eval/.venv/bin/python eval/reproduce_divergence.py --out eval/divergence.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ml-engine"))
sys.path.insert(0, str(ROOT / "eval"))

from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402
from offline_eval import PER_DAY, SEQ, make_series, _model_predict  # noqa: E402

# The configuration that failed, as recorded in the withdrawn run.
ORIGINAL = {"epochs": 12, "history": "expanding", "seeded": False}

ARMS = {
    "original_relu": {"activation": "relu", "clipnorm": None},
    "a_tanh": {"activation": "tanh", "clipnorm": None},
    "b_clipnorm": {"activation": "relu", "clipnorm": 1.0},
    "c_both": {"activation": "tanh", "clipnorm": 1.0},
}

DIVERGENCE_FACTOR = 10.0   # network MAE this many times the seasonal baseline's = threshold

# The only claim this experiment supports, carried inside its own artifact so that no reader
# has to go looking for the caveat (Codex C-95 / D-120).
PERMITTED_CLAIM = "failures occurred only in the clipping arm here"
WITHHELD_CLAIM = ("clipping caused divergence -- NOT supported: the comparison is small and "
                  "unmatched (unseeded initialisations are not paired across arms), and each "
                  "repetition trains ONCE while the withdrawn sweep retrained on a rolling "
                  "schedule roughly eleven times per run, so single-training trials do not "
                  "reproduce the original rolling-retraining exposure")

# Every repetition ends in exactly one of these states. `None` means it produced a finite MAE.
FAILURE_TRAIN = "train_exception"
FAILURE_PREDICT = "predict_exception"
FAILURE_NON_FINITE = "non_finite_forecast"
FAILURE_NO_ORIGINS = "no_scored_origins"


def score_network(model, series, origins):
    """MAE of the raw network over the origins, and the worst single error.

    On failure returns {"failure_type": ...} and NO `mae`, so a failed repetition can never
    enter a spread denominator.
    """
    errs, worst = [], 0.0
    for i in origins:
        origin = series.index[i]
        hist = series.iloc[:i + 1]
        truth = series.iloc[i + 1:i + 1 + STEPS_AHEAD].values
        if len(truth) < STEPS_AHEAD:
            continue
        try:
            out = _model_predict(model, hist, origin, STEPS_AHEAD, seasonal=None)
            pred = np.asarray(out["components"]["lstm"], dtype=float)
        except Exception as exc:
            return {"failure_type": FAILURE_PREDICT,
                    "failure_detail": f"{type(exc).__name__}: {exc}"}
        if not np.all(np.isfinite(pred)):
            return {"failure_type": FAILURE_NON_FINITE,
                    "failure_detail": "non-finite forecast"}
        for p, t in zip(pred, truth):
            errs.append(abs(p - t))
            worst = max(worst, abs(p - t))
    if not errs:
        return {"failure_type": FAILURE_NO_ORIGINS, "failure_detail": "no scored origins"}
    return {"failure_type": None,
            "mae": round(float(np.mean(errs)), 2),
            "worst_abs_error": round(float(worst), 2)}


def summarise_arm(reps: list, threshold=None) -> dict:
    """Roll repetitions up WITHOUT losing any of them.

    `reps` is the ordered, complete list of repetition records -- one per repetition, each with
    a unique `rep_id`. Failures and threshold exceedances are counted SEPARATELY: a repetition
    that produced no finite MAE cannot be compared against a threshold, and a repetition whose
    MAE merely exceeded the threshold did not fail. Spread is max/min over the SUCCESSFUL
    repetitions only, and the denominator is reported next to it.
    """
    ok = [r for r in reps if r.get("failure_type") is None and r.get("mae") is not None]
    failed = [r for r in reps if r.get("failure_type") is not None]
    maes = [r["mae"] for r in ok]
    over = [r["rep_id"] for r in ok
            if threshold is not None and r["mae"] > threshold]
    by_type = {}
    for r in failed:
        by_type.setdefault(r["failure_type"], []).append(r["rep_id"])
    return {
        "repetitions": reps,
        "runs": len(reps),
        "successful_runs": len(ok),
        "failed_runs": len(failed),
        "failed_rep_ids": [r["rep_id"] for r in failed],
        "failures_by_type": by_type,
        "threshold_exceeded_runs": len(over),
        "threshold_exceeded_rep_ids": over,
        "mae_min": round(min(maes), 2) if maes else None,
        "mae_max": round(max(maes), 2) if maes else None,
        "max_over_min": (round(max(maes) / min(maes), 2)
                         if maes and min(maes) > 0 else None),
        "spread_basis": {"over": "successful repetitions only",
                         "n_successful": len(ok), "n_total": len(reps)},
    }


def baseline_mae(series, origins):
    """Seasonal-pattern MAE over the same origins, as the reference scale."""
    helper = LSTMForecastModel(sequence_length=SEQ)
    errs = []
    for i in origins:
        origin = series.index[i]
        hist = series.iloc[:i + 1]
        truth = series.iloc[i + 1:i + 1 + STEPS_AHEAD].values
        if len(truth) < STEPS_AHEAD:
            continue
        vals, _src = helper._pattern_forecast(origin=origin.to_pydatetime(),
                                              steps_ahead=STEPS_AHEAD,
                                              seasonal_history=hist, effective_pct=75)
        if vals is None:
            continue
        errs.extend(abs(np.asarray(vals) - truth))
    return round(float(np.mean(errs)), 2) if errs else None


def run(scenario: str, data_seed: int, model_seeds, epochs: int, origins_n: int,
        history: str, days: int = 12):
    """One arm sweep under a stated training configuration."""
    series = make_series(scenario, days=days, seed=data_seed)
    warmup = max(SEQ + STEPS_AHEAD, 2 * PER_DAY)
    all_origins = list(range(warmup, len(series) - STEPS_AHEAD))
    origins = all_origins[-origins_n:]
    train_end = origins[0]

    if history == "expanding":
        train_hist = series.iloc[:train_end]              # the original, growing history
    else:
        train_hist = series.iloc[max(0, train_end - 7 * PER_DAY):train_end]

    ref = baseline_mae(series, origins)
    out = {"scenario": scenario, "data_seed": data_seed, "epochs": epochs,
           "history": history, "train_points": len(train_hist),
           "scored_origins": len(origins), "seasonal_baseline_mae": ref,
           "divergence_threshold": None if ref is None else round(ref * DIVERGENCE_FACTOR, 2),
           "trainings_per_repetition": 1,
           "trainings_per_run_in_the_withdrawn_sweep": "~11 (rolling retrain) -- NOT matched here",
           "arms": {}}

    threshold = None if ref is None else ref * DIVERGENCE_FACTOR

    import tensorflow as tf
    for arm, cfg in ARMS.items():
        reps = []
        for n, ms in enumerate(model_seeds, start=1):
            # Every repetition gets its own id. Unseeded repetitions all carry model_seed
            # None, which is exactly why the seed cannot be the key (C-95).
            rec = {"rep_id": f"rep{n:02d}", "model_seed": ms}
            t0 = time.time()
            if ms is not None:
                tf.keras.utils.set_random_seed(ms)
            # ms None reproduces the ORIGINAL unseeded condition.
            m = LSTMForecastModel(sequence_length=SEQ)
            try:
                m.train(pd.DataFrame({"value": train_hist.values}, index=train_hist.index),
                        epochs=epochs, **cfg)
            except Exception as exc:
                rec.update({"failure_type": FAILURE_TRAIN,
                            "failure_detail": f"{type(exc).__name__}: {exc}",
                            "train_seconds": round(time.time() - t0, 1)})
                reps.append(rec)
                print(f"  {arm:14s} {rec['rep_id']}: FAILED ({FAILURE_TRAIN})", flush=True)
                continue
            rec.update(score_network(m, series, origins))
            rec["train_seconds"] = round(time.time() - t0, 1)
            rec["exceeded_threshold"] = bool(
                threshold is not None and rec.get("mae") is not None
                and rec["mae"] > threshold)
            reps.append(rec)
            shown = rec.get("mae") if rec.get("failure_type") is None else rec["failure_type"]
            print(f"  {arm:14s} {rec['rep_id']} seed {str(ms):>5s}: MAE {shown}"
                  f"{'  OVER THRESHOLD' if rec.get('exceeded_threshold') else ''}"
                  f"{'  FAILED' if rec.get('failure_type') else ''}"
                  f"  [{rec['train_seconds']}s]", flush=True)

        summary = summarise_arm(reps, threshold=threshold)
        summary["config"] = cfg
        out["arms"][arm] = summary
        print(f"  -> {arm}: {summary['failed_runs']}/{summary['runs']} failed, "
              f"{summary['threshold_exceeded_runs']}/{summary['successful_runs']} over "
              f"threshold, spread {summary['max_over_min']} "
              f"(over {summary['successful_runs']} successful)", flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", default="repeating,weekly")
    ap.add_argument("--data-seed", type=int, default=2)
    ap.add_argument("--runs", type=int, default=5, help="unseeded repetitions per arm")
    ap.add_argument("--epochs", type=int, default=12)
    ap.add_argument("--origins", type=int, default=80)
    ap.add_argument("--history", default="expanding", choices=("expanding", "rolling7"))
    ap.add_argument("--out", default=str(Path(__file__).parent / "divergence.json"))
    args = ap.parse_args()

    # None means "do not seed", reproducing the original condition.
    model_seeds = [None] * args.runs
    results = []
    for scenario in args.scenarios.split(","):
        print(f"\n=== {scenario} (data seed {args.data_seed}, {args.history} history, "
              f"{args.epochs} epochs, unseeded)", flush=True)
        results.append(run(scenario, args.data_seed, model_seeds, args.epochs,
                           args.origins, args.history))

    Path(args.out).write_text(json.dumps(
        {"generated_at": datetime.utcnow().isoformat() + "Z",
         "kind": "divergence_reproduction",
         "record_version": 2,
         "original_conditions": ORIGINAL,
         "divergence_factor": DIVERGENCE_FACTOR,
         "permitted_claim": PERMITTED_CLAIM,
         "withheld_claim": WITHHELD_CLAIM,
         "arms": ARMS, "results": results}, indent=2, default=str))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
