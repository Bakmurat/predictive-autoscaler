#!/usr/bin/env python3
"""Offline bake-off of neural forecaster variants on the real benchmark series (the weak arm).

Trains the deployed formulation (BiLSTM 128/64/32, Dense(6) direct multi-step, RobustScaler, five input
features, asymmetric MSE, Adam 1e-3, early stopping) and variants on the rows before a scoring day, then
scores every variant at rolling origins over that day: network alone and the served hybrid (the deployed
pattern ramp 0.70 -> 0.908 over the six steps), next to the pattern alone, the seven-day profile and
persistence. The trainer's logged "mae" is the TRAINING-set fit, so this is the first held-out,
rolling-origin view of the neural arm on its own data. Descriptive evidence; not a benchmark result.

Variants:
  deployed   the in-cluster training as is (unseeded in the cluster; seeded here for repeatability)
  tuned      deployed net, seeded, 120 epochs, patience 10, ReduceLROnPlateau
  small      LSTM(64) + Dense(6), dropout 0.1, seeded, patience 10
  residual   small net on the residual y - profile7 (the lab's lstm_residual); served = profile7 + residual
  small_x3   the small net averaged over three seeds

Usage: neural_bakeoff.py --series real-series.json --app nginx-test --score-start 2026-09-27T14:40:00Z
                         [--seeds 1,2,3] [--variants deployed,tuned,small,residual,small_x3] [--json out.json]
"""
import argparse
import datetime
import json
import os
import sys
import time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "ml-engine"))
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")

SEQ, STEPS, SLOT, SEASON = 144, 6, 600, 144
PATTERN_WEIGHTS = [min(0.95, 0.7 + (s / STEPS) * 0.25) for s in range(STEPS)]   # deployed ramp
PATTERN_PCT = 70.0                                                             # effective_pct of the serving path


def load_series(path, app):
    pts = sorted((int(t), float(v)) for t, v in json.load(open(path))[app])
    t0 = pts[0][0] - pts[0][0] % SLOT
    n = (pts[-1][0] - t0) // SLOT + 1
    y = np.full(n, np.nan)
    for t, v in pts:
        if t % SLOT == 0:
            y[(t - t0) // SLOT] = v
    # tiny interior gaps are forward-filled (the trainer's bounded gap fill does the same for <= 6 slots)
    last = None
    for i in range(n):
        if np.isfinite(y[i]):
            last = y[i]
        elif last is not None:
            y[i] = last
    return t0, y


def time_features(t0, n):
    from models.lstm_model import generate_time_features
    ts = [datetime.datetime.fromtimestamp(t0 + i * SLOT, datetime.timezone.utc) for i in range(n)]
    return generate_time_features(ts)


def profile7(y, i):
    vals = [y[i - SEASON * d] for d in range(1, 8) if i - SEASON * d >= 0 and np.isfinite(y[i - SEASON * d])]
    return float(np.mean(vals)) if vals else float("nan")


def pattern_pct(y, i, pct=PATTERN_PCT):
    """The serving path's previous-day lookup: weighted percentile over up to seven previous days,
    weights 0.3^(d-1) (models/lstm_model.py _pattern_forecast)."""
    from models.lstm_model import weighted_percentile
    vals, w = [], []
    for d in range(1, 8):
        j = i - SEASON * d
        if j < 0:
            break
        if np.isfinite(y[j]):
            vals.append(y[j]); w.append(0.3 ** (d - 1))
    return float(weighted_percentile(vals, w, pct)) if vals else float("nan")


def make_xy(series_scaled, feats, lo, hi):
    """Sequences i in [lo, hi): input rows i..i+SEQ-1, targets i+SEQ..i+SEQ+STEPS-1 (all rows must be < len)."""
    X, Y, idx = [], [], []
    for i in range(lo, hi):
        if i + SEQ + STEPS > len(series_scaled):
            break
        X.append(np.hstack([series_scaled[i:i + SEQ].reshape(-1, 1), feats[i:i + SEQ]]))
        Y.append(series_scaled[i + SEQ:i + SEQ + STEPS]); idx.append(i)
    return np.array(X, dtype=np.float32), np.array(Y, dtype=np.float32), idx


def build(kind, seed):
    import tensorflow as tf
    from tensorflow.keras import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional, Input
    tf.keras.utils.set_random_seed(seed)
    if kind in ("deployed", "tuned"):
        m = Sequential([Input(shape=(SEQ, 5)),
                        Bidirectional(LSTM(128, return_sequences=True)), Dropout(0.2),
                        Bidirectional(LSTM(64, return_sequences=True)), Dropout(0.2),
                        Bidirectional(LSTM(32)), Dropout(0.2),
                        Dense(16, activation="relu"), Dense(STEPS)])
    else:
        m = Sequential([Input(shape=(SEQ, 5)), LSTM(64), Dropout(0.1), Dense(STEPS)])
    return m


def fit(kind, seed, X, Y, split_row_seq):
    import tensorflow as tf
    from models.lstm_model import asymmetric_mse
    m = build(kind, seed)
    m.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.001), loss=asymmetric_mse, metrics=["mae"])
    Xtr, Ytr, Xva, Yva = X[:split_row_seq], Y[:split_row_seq], X[split_row_seq:], Y[split_row_seq:]
    cbs = [tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=15 if kind == "deployed" else 10,
                                            restore_best_weights=True)]
    epochs = 50 if kind == "deployed" else 120
    if kind == "tuned":
        cbs.append(tf.keras.callbacks.ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=4, min_lr=1e-5))
    t = time.time()
    h = m.fit(Xtr, Ytr, batch_size=32, epochs=epochs, validation_data=(Xva, Yva), verbose=0, callbacks=cbs)
    return m, {"epochs": len(h.history["loss"]), "final_val_loss": float(h.history["val_loss"][-1]),
               "best_val_loss": float(min(h.history["val_loss"])), "fit_seconds": round(time.time() - t, 1)}


def run_variant(kind, seed, y, feats, train_end, origins):
    """Train on rows < train_end (purged 80/20 time split inside for early stopping), predict at origins."""
    from sklearn.preprocessing import RobustScaler
    if kind == "residual":
        base = np.array([profile7(y, i) for i in range(len(y))])
        target = y - base
    else:
        target = y.copy()
    fit_rows = target[:train_end]
    ok = np.isfinite(fit_rows)
    scaler = RobustScaler().fit(fit_rows[ok].reshape(-1, 1))
    scaled = np.where(np.isfinite(target), scaler.transform(np.nan_to_num(target).reshape(-1, 1)).ravel(), 0.0)
    X, Y, idx = make_xy(scaled, feats, 0, train_end - SEQ - STEPS + 1)
    # purged time split of the training rows (labels before 80 % row go to training, after it to validation)
    split_row = int(0.8 * train_end)
    tr = [k for k, i in enumerate(idx) if i + SEQ + STEPS - 1 < split_row]
    va = [k for k, i in enumerate(idx) if i + SEQ >= split_row]
    Xs, Ys = np.concatenate([X[tr], X[va]]), np.concatenate([Y[tr], Y[va]])
    model, info = fit(kind, seed, Xs, Ys, len(tr))
    preds = {}
    for o in origins:
        x = np.hstack([scaled[o - SEQ + 1:o + 1].reshape(-1, 1), feats[o - SEQ + 1:o + 1]])[None].astype(np.float32)
        p = scaler.inverse_transform(model.predict(x, verbose=0).reshape(-1, 1)).ravel()
        if kind == "residual":
            p = p + np.array([profile7(y, o + s) for s in range(1, STEPS + 1)])
        preds[o] = [float(v) for v in p]
    return preds, info


def score(preds, y, origins, blend_with=None):
    """MAE per step (rpm), mean over steps, under-forecast rate on the lead window (max of +10/+20)."""
    err = np.zeros(STEPS); n = 0; under = 0; lead_n = 0
    for o in origins:
        p = preds.get(o)
        if p is None or not all(np.isfinite(p)) or o + STEPS >= len(y):
            continue
        served = [(1 - w) * p[s] + w * blend_with[o][s] for s, w in enumerate(PATTERN_WEIGHTS)] if blend_with else p
        if not all(np.isfinite(served)):
            continue
        a = y[o + 1:o + 1 + STEPS]
        err += np.abs(np.array(served) - a); n += 1
        if max(a[0], a[1]) > max(served[0], served[1]):
            under += 1
        lead_n += 1
    if n == 0:
        return None
    per = (err / n).round(1).tolist()
    return {"mae_steps": per, "mae10": per[0], "mae20": per[1], "mae_mean": round(float(np.mean(per)), 1),
            "under_rate": round(under / lead_n, 3), "origins": n}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True); ap.add_argument("--app", default="nginx-test")
    ap.add_argument("--score-start", required=True); ap.add_argument("--seeds", default="1,2,3")
    ap.add_argument("--variants", default="deployed,tuned,small,residual,small_x3"); ap.add_argument("--json")
    a = ap.parse_args()
    t0, y = load_series(a.series, a.app)
    feats = time_features(t0, len(y))
    s0 = int(datetime.datetime.fromisoformat(a.score_start.replace("Z", "+00:00")).timestamp())
    train_end = (s0 - t0) // SLOT
    origins = list(range(train_end, len(y) - STEPS))
    seeds = [int(s) for s in a.seeds.split(",")]
    print(f"series {len(y)} points from {datetime.datetime.fromtimestamp(t0, datetime.timezone.utc).isoformat()}, "
          f"train rows {train_end}, scoring origins {len(origins)} from {a.score_start}", flush=True)
    # baselines
    pattern = {o: [pattern_pct(y, o + s) for s in range(1, STEPS + 1)] for o in origins}
    prof = {o: [profile7(y, o + s) for s in range(1, STEPS + 1)] for o in origins}
    persist = {o: [float(y[o])] * STEPS for o in origins}
    results = {"baselines": {"pattern_p70": score(pattern, y, origins), "profile7": score(prof, y, origins),
                             "persistence": score(persist, y, origins)}, "variants": {}}
    for kind in a.variants.split(","):
        results["variants"][kind] = {}
        if kind == "small_x3":
            acc = {}
            for sd in seeds:
                p, info = run_variant("small", sd, y, feats, train_end, origins)
                for o, v in p.items():
                    acc.setdefault(o, []).append(v)
            preds = {o: [float(np.mean([v[s] for v in vs])) for s in range(STEPS)] for o, vs in acc.items()}
            results["variants"][kind]["mean_of_seeds"] = {
                "network": score(preds, y, origins), "served_blend": score(preds, y, origins, pattern)}
            print(f"  {kind}: net {results['variants'][kind]['mean_of_seeds']['network']['mae20']} "
                  f"served {results['variants'][kind]['mean_of_seeds']['served_blend']['mae20']} (mae20)", flush=True)
            continue
        for sd in (seeds if kind != "residual" else seeds):
            preds, info = run_variant(kind, sd, y, feats, train_end, origins)
            net = score(preds, y, origins)
            served = net if kind == "residual" else score(preds, y, origins, pattern)
            results["variants"][kind][f"seed{sd}"] = {"fit": info, "network": net, "served_blend": served}
            print(f"  {kind} seed {sd}: epochs {info['epochs']} fit {info['fit_seconds']}s | network mae10/20 "
                  f"{net['mae10']}/{net['mae20']} | served {served['mae10']}/{served['mae20']} | under {served['under_rate']}", flush=True)
    b = results["baselines"]
    print(f"baselines mae10/20: pattern_p70 {b['pattern_p70']['mae10']}/{b['pattern_p70']['mae20']}  "
          f"profile7 {b['profile7']['mae10']}/{b['profile7']['mae20']}  persistence {b['persistence']['mae10']}/{b['persistence']['mae20']}")
    if a.json:
        json.dump(results, open(a.json, "w"), indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
