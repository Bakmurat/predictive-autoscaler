#!/usr/bin/env python3
"""Offline evaluation: does the network add anything over trivial baselines?

Runs the repository's REAL training and inference code (ml-engine/models/lstm_model.py)
over rolling origins, against baselines that every forecaster must beat to be worth its
complexity, and replays the operator's own decision rule to answer the operational
question as well as the statistical one.

Design follows the reviewer's C-49/C-50:
  * rolling origins, identical information availability at each origin
  * simulated training and publication delay (a model is only usable after it is published)
  * multiple seeds and several scenario families
  * per-step MAE and signed bias primary; MAPE secondary (it misbehaves near zero)
  * controller replay with readiness delay and calibrated per-replica capacity;
    deficit magnitude and duration, replica-minutes, churn
  * decision rule: beat the strongest baseline by a preregistered margin, repeatably

Usage:
    eval/.venv/bin/python eval/offline_eval.py --quick          # smoke, ~1 min
    eval/.venv/bin/python eval/offline_eval.py --full           # the reported run
    eval/.venv/bin/python eval/offline_eval.py --failure-modes  # robustness only
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ml-engine"))

from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402

logging.basicConfig(level=os.getenv("EVAL_LOG", "WARNING"),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("offline-eval")

GRID_MIN = 10
PER_DAY = 24 * 60 // GRID_MIN                 # 144
SEQ = 144                                      # the deployed sequence_length
PREREGISTERED_MARGIN = 0.10                    # C-50: 10% lower MAE than the best baseline

# --- operator constants, mirrored from k8s-operator/controllers ------------------------
SCALE_DOWN_STABILIZATION_MIN = 5
SCALE_DOWN_COOLDOWN_MIN = 2
SCALE_DOWN_MAX_PERCENT = 10
SCALE_DOWN_MIN_PODS = 2
OVERESTIMATE_RATIO = 1.2
OVERESTIMATE_HEADROOM = 1.1
OVERESTIMATE_STREAK = 3
READINESS_DELAY_MIN = 2                        # pod scheduled -> serving
DEFAULT_TARGET_RPM_PER_REPLICA = 1800          # calibrated per scenario below


# ======================================================================================
# Scenario generation
# ======================================================================================
def _daily_shape(ts: pd.Timestamp) -> float:
    """The k6 profile's shape: a broad daytime peak with a quiet night."""
    frac = (ts.hour * 60 + ts.minute) / (24 * 60.0)
    return 0.25 + 0.75 * (0.5 * (1 - np.cos(2 * np.pi * frac))) ** 1.6


def make_series(kind: str, days: int, seed: int, base: float = 6000.0,
                start: datetime = datetime(2026, 3, 1)) -> pd.Series:
    """Deterministic synthetic scenarios. `kind` selects the family."""
    rng = np.random.default_rng(seed)
    n = days * PER_DAY
    idx = pd.DatetimeIndex([start + timedelta(minutes=GRID_MIN * i) for i in range(n)])
    shape = np.array([_daily_shape(t) for t in idx])
    vals = base * shape

    if kind == "repeating":
        pass
    elif kind == "trend":
        vals = vals * (1.0 + np.linspace(0, 0.6, n))
    elif kind == "levelshift":
        cut = int(n * 0.62)
        vals = vals.copy()
        vals[cut:] *= 1.55
    elif kind == "spike":
        vals = vals.copy()
        for _ in range(3):
            at = rng.integers(int(n * 0.55), n - 12)
            width = rng.integers(2, 6)
            vals[at:at + width] *= rng.uniform(2.0, 3.0)
    elif kind == "weekly":
        dow = np.array([t.weekday() for t in idx])
        vals = vals * np.where(dow >= 5, 0.55, 1.0)
    else:
        raise ValueError(f"unknown scenario {kind}")

    noise = rng.normal(0, 0.035, n) * vals
    return pd.Series(np.maximum(vals + noise, 1.0), index=idx)


def load_benchmark_series(path: Path) -> pd.Series | None:
    """The real history exported from the benchmark cluster (read-only)."""
    if not path.exists():
        return None
    payload = json.loads(path.read_text())
    result = payload.get("data", {}).get("result", [])
    if not result:
        return None
    pairs = result[0]["values"]
    idx = pd.DatetimeIndex([datetime.utcfromtimestamp(int(t)) for t, _ in pairs])
    vals = [float(v) for _, v in pairs]
    return pd.Series(vals, index=idx).sort_index()


# ======================================================================================
# Predictors -- each sees ONLY data at or before its origin
# ======================================================================================
class Predictor:
    name = "base"
    needs_model = False

    def forecast(self, hist: pd.Series, origin: pd.Timestamp, steps: int, model=None):
        raise NotImplementedError


class Persistence(Predictor):
    name = "persistence"

    def forecast(self, hist, origin, steps, model=None):
        return np.full(steps, float(hist.iloc[-1]))


class PreviousDay(Predictor):
    """Value observed at the same clock time yesterday. The bar the network must clear."""
    name = "previous_day"

    def forecast(self, hist, origin, steps, model=None):
        out = []
        for s in range(steps):
            target = origin + pd.Timedelta(minutes=GRID_MIN * (s + 1)) - pd.Timedelta(days=1)
            if target < hist.index[0]:
                out.append(float(hist.iloc[-1]))
                continue
            pos = hist.index.get_indexer([target], method="nearest")[0]
            out.append(float(hist.iloc[pos]))
        return np.array(out)


class SeasonalPattern(Predictor):
    """The repo's own pattern component in isolation (weighted percentile over 7 days)."""
    name = "seasonal_pattern"

    def forecast(self, hist, origin, steps, model=None):
        helper = model if model is not None else LSTMForecastModel(sequence_length=SEQ)
        vals, _src = helper._pattern_forecast(
            origin=origin.to_pydatetime(), steps_ahead=steps,
            seasonal_history=hist, effective_pct=75)
        if vals is None:
            return np.full(steps, float(hist.iloc[-1]))
        return np.asarray(vals, dtype=float)


class TrendAdaptive(Predictor):
    """Previous-day value scaled by the recent level ratio -- damped (C-49)."""
    name = "trend_adaptive"

    def forecast(self, hist, origin, steps, model=None):
        prev = PreviousDay().forecast(hist, origin, steps)
        if len(hist) < PER_DAY + 6:
            return prev
        recent = float(hist.iloc[-6:].mean())
        same_yesterday = hist.iloc[-(PER_DAY + 6):-PER_DAY]
        if len(same_yesterday) == 0 or same_yesterday.mean() <= 0:
            return prev
        ratio = recent / float(same_yesterday.mean())
        ratio = float(np.clip(ratio, 0.5, 2.0))
        damp = np.array([1.0 + (ratio - 1.0) * (0.9 ** s) for s in range(steps)])
        return prev * damp


class NetworkOnly(Predictor):
    """The LSTM with the blend disabled (ablation)."""
    name = "network_only"
    needs_model = True

    def forecast(self, hist, origin, steps, model=None):
        out = _model_predict(model, hist, origin, steps, seasonal=None)
        return np.asarray(out["components"]["lstm"], dtype=float)


class ServedBlend(Predictor):
    """Exactly what the deployed service returns: network blended with the pattern."""
    name = "served_blend"
    needs_model = True

    def forecast(self, hist, origin, steps, model=None):
        out = _model_predict(model, hist, origin, steps, seasonal=hist)
        return np.asarray(out["predictions"], dtype=float)


def _model_predict(model, hist, origin, steps, seasonal):
    """Drive the repo's predict() with the window ending at `origin`."""
    window = hist.iloc[-SEQ:]
    model.last_sequence = model.scaler.transform(window.values.reshape(-1, 1)).flatten()
    return model.predict(
        steps_ahead=steps,
        origin=origin.to_pydatetime(),
        input_timestamps=list(window.index.to_pydatetime()),
        seasonal_history=seasonal,
    )


# ======================================================================================
# Controller replay -- the operator's rules, not a paraphrase
# ======================================================================================
@dataclass
class ControllerState:
    current: int = 1
    last_scale_up: datetime | None = None
    last_scale_down: datetime | None = None
    below_since: datetime | None = None
    override_active: bool = False
    streak: int = 0


def replica_need(rpm: float, target_rpm_per_replica: float, min_r: int, max_r: int) -> int:
    return int(np.clip(np.ceil(rpm / target_rpm_per_replica), min_r, max_r))


def replay_controller(actual: pd.Series, forecasts: dict, target_rpm: float,
                      min_r: int = 1, max_r: int = 12, lead_steps: int = 2):
    """Replay max(forecast, reactive, min) with the real stabilization rules.

    Returns replica-minutes, churn, and the shortfall against what the reactive rule
    would have required at each moment -- accounting for the readiness delay, so a
    scale-up decided now only serves traffic READINESS_DELAY_MIN later.
    """
    st = ControllerState(current=min_r)
    times = list(actual.index)
    served_capacity = {}
    replica_minutes = 0.0
    churn = 0
    pending = []  # (ready_at, replicas)

    for i, now in enumerate(times):
        for ready_at, count in list(pending):
            if now >= ready_at:
                served_capacity[now] = count
                pending.remove((ready_at, count))

        reactive = replica_need(float(actual.iloc[i]), target_rpm, min_r, max_r)
        fc = forecasts.get(now)
        predicted = min_r
        if fc is not None and len(fc) >= lead_steps:
            predicted = replica_need(float(np.max(fc[:lead_steps])), target_rpm, min_r, max_r)

        # Overestimate detection, as implemented in controllers/overestimate.go
        if reactive > 0 and predicted > reactive and predicted / reactive > OVERESTIMATE_RATIO:
            st.streak += 1
            if st.streak >= OVERESTIMATE_STREAK:
                predicted = int(min(predicted, np.ceil(reactive * OVERESTIMATE_HEADROOM)))
                st.override_active = True
        else:
            st.streak = 0
            st.override_active = False

        desired = max(predicted, reactive, min_r)
        prev = st.current

        if desired > st.current:
            st.current = desired
            st.last_scale_up = now
            st.below_since = None
            pending.append((now + timedelta(minutes=READINESS_DELAY_MIN), st.current))
        elif desired < st.current:
            blocked = False
            if not st.override_active:
                if st.last_scale_up and (now - st.last_scale_up) < timedelta(minutes=SCALE_DOWN_STABILIZATION_MIN):
                    blocked = True
                elif st.below_since is None:
                    st.below_since = now
                    blocked = True
                elif (now - st.below_since) < timedelta(minutes=SCALE_DOWN_STABILIZATION_MIN):
                    blocked = True
            if not blocked and st.last_scale_down and \
                    (now - st.last_scale_down) < timedelta(minutes=SCALE_DOWN_COOLDOWN_MIN):
                blocked = True
            if not blocked:
                max_remove = max(SCALE_DOWN_MIN_PODS,
                                 int(np.ceil(st.current * SCALE_DOWN_MAX_PERCENT / 100.0)))
                st.current = max(desired, st.current - max_remove, 1)
                st.last_scale_down = now
                st.below_since = None
        else:
            st.below_since = None

        if st.current != prev:
            churn += 1
        replica_minutes += st.current * GRID_MIN

        # Capacity actually serving: replicas that have finished starting.
        effective = min(st.current, prev) if st.current > prev else st.current
        served_capacity[now] = effective

    deficits, deficit_minutes = [], 0.0
    for i, now in enumerate(times):
        need = replica_need(float(actual.iloc[i]), target_rpm, min_r, max_r)
        have = served_capacity.get(now, st.current)
        if have < need:
            deficits.append(need - have)
            deficit_minutes += GRID_MIN
    return {
        "replica_minutes": round(replica_minutes, 1),
        "scaling_events": churn,
        "deficit_intervals": len(deficits),
        "deficit_minutes": round(deficit_minutes, 1),
        "deficit_replicas_max": int(max(deficits)) if deficits else 0,
        "deficit_replicas_mean": round(float(np.mean(deficits)), 2) if deficits else 0.0,
    }


# ======================================================================================
# Rolling-origin evaluation
# ======================================================================================
@dataclass
class Score:
    n: int = 0
    abs_err: list = field(default_factory=list)
    signed: list = field(default_factory=list)
    ape: list = field(default_factory=list)

    def add(self, pred, truth):
        for p, t in zip(pred, truth):
            self.abs_err.append(abs(p - t))
            self.signed.append(p - t)
            if t > 1e-6:
                self.ape.append(abs(p - t) / t * 100.0)
        self.n += 1

    def summary(self):
        return {
            "origins": self.n,
            "mae": round(float(np.mean(self.abs_err)), 1) if self.abs_err else None,
            "bias": round(float(np.mean(self.signed)), 1) if self.signed else None,
            "mape": round(float(np.mean(self.ape)), 2) if self.ape else None,
        }


def evaluate_scenario(series: pd.Series, scenario: str, seed: int, *,
                      retrain_every_h: int = 6, publication_delay_min: int = 20,
                      epochs: int = 12, max_origins: int | None = None,
                      target_rpm: float | None = None, verbose: bool = True):
    """Rolling origins with a retrain schedule and a publication delay."""
    predictors = [Persistence(), PreviousDay(), SeasonalPattern(), TrendAdaptive(),
                  NetworkOnly(), ServedBlend()]
    per_step = {p.name: [Score() for _ in range(STEPS_AHEAD)] for p in predictors}
    overall = {p.name: Score() for p in predictors}
    forecast_by_origin = {p.name: {} for p in predictors}

    warmup = max(SEQ + STEPS_AHEAD, 2 * PER_DAY)
    origins = list(range(warmup, len(series) - STEPS_AHEAD))
    if max_origins:
        origins = origins[:max_origins]
    if not origins:
        return None

    model = None
    model_published_at = None
    pending_model = None
    last_train_idx = -10 ** 9
    retrain_every = retrain_every_h * 60 // GRID_MIN
    train_failures = []

    if target_rpm is None:
        target_rpm = float(np.percentile(series.values, 60)) / 3.0

    t_start = time.time()
    for k, i in enumerate(origins):
        origin = series.index[i]
        hist = series.iloc[:i + 1]                    # information available AT the origin

        # Retrain on the schedule; the artifact only becomes usable after publication.
        if i - last_train_idx >= retrain_every:
            last_train_idx = i
            try:
                m = LSTMForecastModel(sequence_length=SEQ)
                m.train(pd.DataFrame({"value": hist.values}, index=hist.index), epochs=epochs)
                pending_model = (m, origin + pd.Timedelta(minutes=publication_delay_min))
            except Exception as exc:
                train_failures.append(f"{origin.isoformat()}: {exc}")
                log.warning("train failed at %s: %s", origin, exc)

        if pending_model and origin >= pending_model[1]:
            model, model_published_at = pending_model[0], pending_model[1]
            pending_model = None

        truth = series.iloc[i + 1:i + 1 + STEPS_AHEAD].values
        if len(truth) < STEPS_AHEAD:
            break

        for p in predictors:
            if p.needs_model and model is None:
                continue                              # cold start: no published model yet
            try:
                pred = p.forecast(hist, origin, STEPS_AHEAD, model=model)
            except Exception as exc:
                log.warning("%s failed at %s: %s", p.name, origin, exc)
                continue
            pred = np.asarray(pred, dtype=float)
            overall[p.name].add(pred, truth)
            for s in range(STEPS_AHEAD):
                per_step[p.name][s].add([pred[s]], [truth[s]])
            forecast_by_origin[p.name][origin] = pred

        if verbose and k % 25 == 0:
            print(f"    origin {k + 1}/{len(origins)} ({origin})  "
                  f"[{time.time() - t_start:.0f}s]", flush=True)

    actual_window = series.iloc[origins[0]:origins[-1] + 1]
    operational = {}
    for p in predictors:
        if not forecast_by_origin[p.name]:
            continue
        operational[p.name] = replay_controller(actual_window, forecast_by_origin[p.name],
                                                target_rpm=target_rpm)
    operational["reactive_only"] = replay_controller(actual_window, {}, target_rpm=target_rpm)

    return {
        "scenario": scenario,
        "seed": seed,
        "points": len(series),
        "origins_scored": len(origins),
        "target_rpm_per_replica": round(target_rpm, 1),
        "model_published_at": model_published_at.isoformat() if model_published_at is not None else None,
        "train_failures": train_failures,
        "overall": {k: v.summary() for k, v in overall.items()},
        "per_step": {k: [s.summary() for s in v] for k, v in per_step.items()},
        "operational": operational,
    }


# ======================================================================================
# Failure modes
# ======================================================================================
def failure_modes(verbose: bool = True):
    """Deliberately provoke the conditions a forecaster meets in production."""
    results = {}
    base = make_series("repeating", days=10, seed=1)

    def _train(series, **kw):
        m = LSTMForecastModel(sequence_length=SEQ)
        m.train(pd.DataFrame({"value": series.values}, index=series.index), epochs=5, **kw)
        return m

    # 1. Cold start -- no history at all.
    try:
        _train(base.iloc[:SEQ // 2])
        results["cold_start"] = "FAIL: trained on a series shorter than one window"
    except ValueError as e:
        results["cold_start"] = f"refused cleanly: {e}"

    # 2. All zeros.
    zeros = pd.Series(np.zeros(len(base)), index=base.index)
    try:
        m = _train(zeros)
        out = _model_predict(m, zeros, zeros.index[-1], STEPS_AHEAD, seasonal=zeros)
        vals = np.asarray(out["predictions"])
        results["all_zeros"] = ("handled: finite output"
                                if np.all(np.isfinite(vals)) else "FAIL: non-finite output")
    except Exception as e:
        results["all_zeros"] = f"raised: {type(e).__name__}: {e}"

    # 3. A gap in the seasonal history -- the pattern must not invent a value.
    gapped = base.copy()
    gapped = gapped.drop(gapped.index[-(PER_DAY + 30):-(PER_DAY + 10)])
    helper = LSTMForecastModel(sequence_length=SEQ)
    vals, src = helper._pattern_forecast(origin=base.index[-1].to_pydatetime(),
                                         steps_ahead=STEPS_AHEAD,
                                         seasonal_history=gapped, effective_pct=75)
    results["gap_in_history"] = f"pattern source={src}, finite={bool(vals is None or np.all(np.isfinite(vals)))}"

    # 4. Delayed observation -- origin must follow the data, not the wall clock.
    stale_origin = base.index[-1] - pd.Timedelta(minutes=40)
    m = _train(base)
    out = _model_predict(m, base.loc[:stale_origin], stale_origin, STEPS_AHEAD,
                         seasonal=base.loc[:stale_origin])
    first_target = pd.Timestamp(out["target_timestamps"][0])
    results["delayed_observation"] = (
        "anchored to the data"
        if first_target == stale_origin + pd.Timedelta(minutes=GRID_MIN)
        else f"FAIL: first target {first_target} for origin {stale_origin}")

    # 5. Phase shift and 6. level shift: how fast does the error recover?
    for kind in ("levelshift", "spike"):
        s = make_series(kind, days=8, seed=3)
        r = evaluate_scenario(s, kind, 3, epochs=5, max_origins=40, verbose=False)
        if r:
            results[f"{kind}_recovery"] = {
                "served_blend_mae": r["overall"]["served_blend"]["mae"],
                "previous_day_mae": r["overall"]["previous_day"]["mae"],
            }

    # 7. Corrupt artifact -- covered by the API tests; assert the format guard here.
    class _Old:
        model = type("m", (), {"output_shape": (None, 1)})()
    results["old_format_artifact"] = ("detected"
                                      if LSTMForecastModel._is_old_model_format(_Old())
                                      else "FAIL: old format not detected")

    # 8. Retrain landing mid-peak.
    peak = make_series("repeating", days=8, seed=7)
    r = evaluate_scenario(peak, "repeating", 7, epochs=5, max_origins=40,
                          publication_delay_min=20, verbose=False)
    results["retrain_midpeak"] = ("no failures" if r and not r["train_failures"]
                                  else f"train failures: {r['train_failures'] if r else 'n/a'}")

    if verbose:
        for k, v in results.items():
            print(f"  {k}: {v}")
    return results


# ======================================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="smoke run")
    ap.add_argument("--full", action="store_true", help="the reported run")
    ap.add_argument("--failure-modes", action="store_true")
    ap.add_argument("--out", default=str(Path(__file__).parent / "results.json"))
    args = ap.parse_args()

    if args.failure_modes:
        print("Failure modes:")
        res = failure_modes()
        Path(args.out).write_text(json.dumps({"failure_modes": res}, indent=2, default=str))
        return

    if args.quick:
        plan = [("repeating", [1], 6, 5, 40)]
        days = 6
    else:
        # Origins are capped at 400 per run (about 2.8 days of rolling origins) to keep the
        # whole sweep under half an hour on one laptop CPU. The cap is stated in RESULTS.
        plan = [("repeating", [1, 2, 3], 12, 12, 400),
                ("trend", [1, 2], 12, 12, 400),
                ("levelshift", [1, 2], 12, 12, 400),
                ("spike", [1, 2], 12, 12, 400),
                ("weekly", [1], 12, 12, 400)]
        days = 12

    all_results = []
    for scenario, seeds, epochs, _e2, cap in plan:
        for seed in seeds:
            print(f"\n== {scenario} (seed {seed})", flush=True)
            s = make_series(scenario, days=days, seed=seed)
            r = evaluate_scenario(s, scenario, seed, epochs=epochs, max_origins=cap)
            if r:
                all_results.append(r)
                b = r["overall"]
                print(f"   served_blend MAE {b['served_blend']['mae']}  "
                      f"network_only MAE {b['network_only']['mae']}  "
                      f"previous_day MAE {b['previous_day']['mae']}  "
                      f"seasonal MAE {b['seasonal_pattern']['mae']}", flush=True)

    bench = load_benchmark_series(Path(__file__).parent / "data" / "benchmark-nginx-test.json")
    if bench is not None and len(bench) > SEQ + 2 * STEPS_AHEAD:
        print(f"\n== benchmark cluster history ({len(bench)} points)", flush=True)
        r = evaluate_scenario(bench, "benchmark_real", 0, epochs=12,
                              max_origins=None if args.full else 20)
        if r:
            all_results.append(r)

    out = {"generated_at": datetime.utcnow().isoformat() + "Z",
           "preregistered_margin": PREREGISTERED_MARGIN,
           "runs": all_results}
    Path(args.out).write_text(json.dumps(out, indent=2, default=str))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
