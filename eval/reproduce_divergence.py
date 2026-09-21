#!/usr/bin/env python3
"""Reproduce the conditions under which the network diverged, then test the arms there.

Codex C-78: the stabilization run cannot claim tanh removes the divergence, because its
matched ReLU control also had zero failed seeds (spread 1.38 against tanh's 1.10). The
ten-orders-of-magnitude failure appeared under DIFFERENT conditions:

    unseeded initialisation, 12 epochs, EXPANDING history (not a rolling 7-day window)

So the honest experiment is: reproduce that configuration, confirm the failure still
occurs, and only then ask whether activation or gradient clipping prevents it. Anything
else is testing a fix against conditions that never failed.

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

DIVERGENCE_FACTOR = 10.0   # network MAE this many times the seasonal baseline's = diverged


def score_network(model, series, origins):
    """MAE of the raw network over the origins, and the worst single error."""
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
            return {"failed": f"{type(exc).__name__}: {exc}", "diverged": True}
        if not np.all(np.isfinite(pred)):
            return {"failed": "non-finite forecast", "diverged": True}
        for p, t in zip(pred, truth):
            errs.append(abs(p - t))
            worst = max(worst, abs(p - t))
    if not errs:
        return {"failed": "no scored origins"}
    return {"mae": round(float(np.mean(errs)), 2),
            "worst_abs_error": round(float(worst), 2)}


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
           "arms": {}}

    import tensorflow as tf
    for arm, cfg in ARMS.items():
        seeds_out, maes, diverged = {}, [], []
        for ms in model_seeds:
            t0 = time.time()
            if ms is not None:
                tf.keras.utils.set_random_seed(ms)
            # ms None reproduces the ORIGINAL unseeded condition.
            m = LSTMForecastModel(sequence_length=SEQ)
            try:
                m.train(pd.DataFrame({"value": train_hist.values}, index=train_hist.index),
                        epochs=epochs, **cfg)
            except Exception as exc:
                seeds_out[str(ms)] = {"failed": f"train: {exc}", "diverged": True}
                diverged.append(str(ms))
                continue
            sc = score_network(m, series, origins)
            sc["train_seconds"] = round(time.time() - t0, 1)
            is_div = bool(sc.get("diverged") or
                          (ref and sc.get("mae") and sc["mae"] > ref * DIVERGENCE_FACTOR))
            sc["diverged"] = is_div
            if is_div:
                diverged.append(str(ms))
            if sc.get("mae") is not None:
                maes.append(sc["mae"])
            seeds_out[str(ms)] = sc
            print(f"  {arm:14s} seed {str(ms):>5s}: MAE {sc.get('mae', sc.get('failed'))}"
                  f"{'  DIVERGED' if is_div else ''}  [{sc.get('train_seconds')}s]", flush=True)

        out["arms"][arm] = {
            "config": cfg, "seeds": seeds_out,
            "runs": len(model_seeds), "diverged_runs": len(diverged),
            "diverged_seeds": diverged,
            "mae_min": round(min(maes), 2) if maes else None,
            "mae_max": round(max(maes), 2) if maes else None,
            "max_over_min": round(max(maes) / min(maes), 2) if maes and min(maes) > 0 else None,
        }
        print(f"  -> {arm}: {len(diverged)}/{len(model_seeds)} diverged, "
              f"spread {out['arms'][arm]['max_over_min']}", flush=True)
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
         "original_conditions": ORIGINAL,
         "divergence_factor": DIVERGENCE_FACTOR,
         "arms": ARMS, "results": results}, indent=2, default=str))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
