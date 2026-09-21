#!/usr/bin/env python3
"""One bounded stabilization experiment (Codex C-55).

PRIMARY QUESTION: does the across-seed divergence disappear? The evaluation found the same
data and the same code producing errors ten orders of magnitude apart on different random
initialisations. Accuracy is the secondary question and is judged against the STRONGEST
baseline from the corrected sweep, not previous-day alone.

Arms, measured separately so an improvement can be attributed:

    a  activation="tanh"           (bounded activations; the Keras default)
    b  clipnorm=1.0                (gradient-norm clipping, relu retained)
    c  both                        (ONLY interpreted after a and b are each measured)
    baseline  activation="relu"    (what ships today)

Each arm is trained once per model seed on a fixed window, then scored over a common set of
origins. Training once per arm/seed rather than on the full retrain schedule is deliberate:
the question is the variance of the fitted model, and one fit per seed isolates it.

Usage:
    eval/.venv/bin/python eval/stabilization.py --out eval/stabilization.json
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
from offline_eval import (  # noqa: E402
    PER_DAY, SEQ, make_series, _model_predict, Persistence, PreviousDay,
    SeasonalPattern, TrendAdaptive,
)

ARMS = {
    "baseline_relu": {"activation": "relu", "clipnorm": None},
    "a_tanh": {"activation": "tanh", "clipnorm": None},
    "b_clipnorm": {"activation": "relu", "clipnorm": 1.0},
    "c_both": {"activation": "tanh", "clipnorm": 1.0},
}


def score(pred_fn, series, origins, label):
    """MAE, bias and the worst single error over a common origin set."""
    abs_err, signed, worst = [], [], 0.0
    for i in origins:
        origin = series.index[i]
        hist = series.iloc[:i + 1]
        truth = series.iloc[i + 1:i + 1 + STEPS_AHEAD].values
        if len(truth) < STEPS_AHEAD:
            continue
        try:
            pred = np.asarray(pred_fn(hist, origin), dtype=float)
        except Exception as exc:  # preserved, not skipped
            return {"label": label, "failed": f"{type(exc).__name__}: {exc}"}
        if not np.all(np.isfinite(pred)):
            return {"label": label, "failed": "non-finite forecast", "diverged": True}
        for p, t in zip(pred, truth):
            abs_err.append(abs(p - t))
            signed.append(p - t)
            worst = max(worst, abs(p - t))
    if not abs_err:
        return {"label": label, "failed": "no scored origins"}
    return {
        "label": label,
        "origins": len(origins),
        "mae": round(float(np.mean(abs_err)), 2),
        "bias": round(float(np.mean(signed)), 2),
        "worst_abs_error": round(float(worst), 2),
    }


def run(scenario: str, data_seed: int, model_seeds, origins_n: int, epochs: int,
        train_days: int = 7, days: int = 12):
    series = make_series(scenario, days=days, seed=data_seed)
    warmup = max(SEQ + STEPS_AHEAD, 2 * PER_DAY)
    all_origins = list(range(warmup, len(series) - STEPS_AHEAD))
    origins = all_origins[-origins_n:]
    train_end = origins[0]                      # train strictly before the scored window
    train_hist = series.iloc[max(0, train_end - train_days * PER_DAY):train_end]

    out = {"scenario": scenario, "data_seed": data_seed,
           "scored_origins": len(origins),
           "scored_window": [series.index[origins[0]].isoformat(),
                             series.index[origins[-1]].isoformat()],
           "train_window": [train_hist.index[0].isoformat(), train_hist.index[-1].isoformat()],
           "epochs": epochs, "arms": {}, "baselines": {}}

    # Baselines once -- they have no seed.
    helper = LSTMForecastModel(sequence_length=SEQ)
    for p in (Persistence(), PreviousDay(), SeasonalPattern(), TrendAdaptive()):
        out["baselines"][p.name] = score(
            lambda h, o, _p=p: _p.forecast(h, o, STEPS_AHEAD, model=helper),
            series, origins, p.name)

    import tensorflow as tf
    for arm, cfg in ARMS.items():
        out["arms"][arm] = {"config": cfg, "seeds": {}}
        for ms in model_seeds:
            t0 = time.time()
            tf.keras.utils.set_random_seed(ms)
            m = LSTMForecastModel(sequence_length=SEQ)
            try:
                m.train(pd.DataFrame({"value": train_hist.values}, index=train_hist.index),
                        epochs=epochs, **cfg)
            except Exception as exc:
                out["arms"][arm]["seeds"][str(ms)] = {"failed": f"train: {exc}"}
                continue
            net = score(lambda h, o, _m=m: np.asarray(
                _model_predict(_m, h, o, STEPS_AHEAD, seasonal=None)["components"]["lstm"]),
                series, origins, "network_only")
            blend = score(lambda h, o, _m=m: np.asarray(
                _model_predict(_m, h, o, STEPS_AHEAD, seasonal=h)["predictions"]),
                series, origins, "served_blend")
            out["arms"][arm]["seeds"][str(ms)] = {
                "network_only": net, "served_blend": blend,
                "train_seconds": round(time.time() - t0, 1),
            }
            print(f"  {arm:14s} seed {ms}: network MAE "
                  f"{net.get('mae', net.get('failed'))}, blend MAE "
                  f"{blend.get('mae', blend.get('failed'))}  [{time.time()-t0:.0f}s]", flush=True)

        # The primary measure: spread across seeds.
        maes = [v["network_only"]["mae"] for v in out["arms"][arm]["seeds"].values()
                if "network_only" in v and "mae" in v["network_only"]]
        diverged = [k for k, v in out["arms"][arm]["seeds"].items()
                    if v.get("network_only", {}).get("diverged")
                    or v.get("network_only", {}).get("failed")]
        out["arms"][arm]["spread"] = {
            "seeds_ok": len(maes), "seeds_failed": diverged,
            "mae_min": round(min(maes), 2) if maes else None,
            "mae_max": round(max(maes), 2) if maes else None,
            "max_over_min": round(max(maes) / min(maes), 2) if maes and min(maes) > 0 else None,
        }
        print(f"  -> {arm}: spread {out['arms'][arm]['spread']}", flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", default="repeating,levelshift")
    ap.add_argument("--data-seed", type=int, default=1)
    ap.add_argument("--model-seeds", default="11,22,33,44")
    ap.add_argument("--origins", type=int, default=120)
    ap.add_argument("--epochs", type=int, default=50)
    ap.add_argument("--out", default=str(Path(__file__).parent / "stabilization.json"))
    args = ap.parse_args()

    model_seeds = [int(x) for x in args.model_seeds.split(",")]
    results = []
    for scenario in args.scenarios.split(","):
        print(f"\n=== {scenario} (data seed {args.data_seed})", flush=True)
        results.append(run(scenario, args.data_seed, model_seeds, args.origins, args.epochs))

    Path(args.out).write_text(json.dumps(
        {"generated_at": datetime.utcnow().isoformat() + "Z",
         "kind": "stabilization_arms", "arms": ARMS,
         "model_seeds": model_seeds, "results": results}, indent=2, default=str))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
