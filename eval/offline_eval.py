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
# The generator drives an hourly STEP profile (a ramping-arrival-rate stage per hour), not a
# smooth curve. A cosine made the series easier to forecast than the real one (C-54).
K6_HOURLY_MULTIPLIER = [
    0.30, 0.25, 0.22, 0.20, 0.22, 0.30,   # 00-05 night trough
    0.45, 0.65, 0.85, 0.95, 1.00, 0.98,   # 06-11 morning ramp to peak
    0.92, 0.90, 0.94, 1.00, 0.97, 0.88,   # 12-17 plateau with a second peak
    0.78, 0.68, 0.58, 0.48, 0.40, 0.34,   # 18-23 evening decay
]


def _daily_shape(ts: pd.Timestamp) -> float:
    """The k6 profile: one constant arrival rate per hour, stepping on the hour."""
    return K6_HOURLY_MULTIPLIER[ts.hour]


def scenario_event_indices(kind: str, n: int, seed: int) -> list:
    """Indices at which `kind` introduces its event, so the window can be asserted (C-51)."""
    if kind in ("repeating",):
        return []
    if kind == "trend":
        return [int(n * 0.35)]            # the trend is continuous; this is a mid-point probe
    if kind == "levelshift":
        return [int(n * 0.45)]
    if kind == "spike":
        rng = np.random.default_rng(seed)
        return sorted(int(rng.integers(int(n * 0.35), int(n * 0.85))) for _ in range(3))
    if kind == "weekly":
        return [i for i in range(n) if i % PER_DAY == 0]
    return []


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
        vals = vals.copy()
        cut = scenario_event_indices("levelshift", n, seed)[0]
        vals[cut:] *= 1.55
    elif kind == "spike":
        vals = vals.copy()
        for at in scenario_event_indices("spike", n, seed):
            width = int(rng.integers(2, 6))
            vals[at:at + width] *= float(rng.uniform(2.0, 3.0))
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


def go_controller_decisions(steps: list, min_r: int, max_r: int, timeout_s: int = 600):
    """Run the REAL Go controller over a recorded input sequence (C-52).

    Returns its per-step decisions, or None when the Go toolchain is unavailable. This
    exists so the evaluation does not depend on a Python re-implementation of the
    controller's safeguards: the first attempt omitted the ramp-up detection and diverged
    from the real decisions, which the differential test caught.
    """
    import shutil, subprocess, tempfile
    go_dir = ROOT / "k8s-operator"
    if shutil.which("go") is None or not go_dir.exists():
        return None
    with tempfile.TemporaryDirectory() as td:
        in_path, out_path = Path(td) / "in.json", Path(td) / "out.json"
        in_path.write_text(json.dumps({"min": min_r, "max": max_r, "steps": steps}))
        env = {**os.environ, "REPLAY_IN": str(in_path), "REPLAY_OUT": str(out_path)}
        proc = subprocess.run(
            ["go", "test", "./controllers", "-run", "TestReplayHarness", "-count=1"],
            cwd=go_dir, env=env, capture_output=True, text=True, timeout=timeout_s)
        if proc.returncode != 0 or not out_path.exists():
            log.warning("go replay harness failed: %s", proc.stdout[-500:])
            return None
        return json.loads(out_path.read_text())["decisions"]


def replica_need(rpm: float, target_rpm_per_replica: float) -> int:
    """Replicas required by demand, UNCONSTRAINED by the ceiling (C-52).

    Clipping the requirement to max_replicas hid overload: a workload needing 20 replicas
    against a ceiling of 12 reported no deficit. Required capacity and permitted replicas
    are now separate quantities.
    """
    return int(np.ceil(rpm / target_rpm_per_replica))


def replay_controller(actual: pd.Series, forecasts: dict, target_rpm: float,
                      min_r: int = 1, max_r: int = 12, lead_steps: int = 2,
                      readiness_min: int = READINESS_DELAY_MIN,
                      tick_min: int = 1, use_go: bool = True):
    """Replay the controller on an EVENT-TIME grid (C-52).

    Rebuilt after review. Three defects in the first version:
      * pending readiness updates were overwritten before they took effect, so capacity
        appeared instantly;
      * a two-minute readiness delay was judged on ten-minute ticks, so every deficit was
        counted as a full ten minutes;
      * demand was clipped to the replica ceiling, hiding overload.

    Now: decisions are taken on the observation grid, capacity is accounted on a
    one-minute event-time grid with explicit ready-at times, and the deficit is measured
    against unconstrained requirement.

    NOTE: this remains a Python model of the controller. It is validated against the real
    Go decision functions by eval/test_replay_differential.py; do not quote its output
    without that check passing.
    """
    obs_times = list(actual.index)
    obs_value = {t: float(v) for t, v in actual.items()}
    grid = pd.date_range(obs_times[0], obs_times[-1], freq=f"{tick_min}min")

    # Decisions come from the REAL controller when the Go toolchain is present; the Python
    # path below is a fallback and is labelled as unvalidated in the result.
    go_decisions = None
    if use_go:
        steps = []
        for t in obs_times:
            required = replica_need(obs_value[t], target_rpm)
            reactive = int(np.clip(required, min_r, max_r))
            fc = forecasts.get(t)
            if fc is not None and len(fc) >= lead_steps:
                predicted = int(np.clip(replica_need(float(np.max(fc[:lead_steps])), target_rpm),
                                        min_r, max_r))
                preds = [float(x) for x in fc]
            else:
                predicted, preds = min_r, []
            steps.append({"t": t.isoformat() + "Z", "reactive": reactive,
                          "predicted": predicted, "current_rpm": obs_value[t],
                          "predictions": preds})
        go_decisions = go_controller_decisions(steps, min_r, max_r)
    go_by_time = ({pd.Timestamp(d["t"].replace("Z", "")): d for d in go_decisions}
                  if go_decisions else None)

    current = min_r
    ready_now = min_r
    arrivals: list[tuple[pd.Timestamp, int]] = []
    last_scale_up = last_scale_down = below_since = None
    streak, override_active = 0, False

    replica_minutes = 0.0
    events = 0
    deficit_minutes = 0.0
    deficit_amounts: list[int] = []
    over_ceiling_minutes = 0.0
    decisions = []

    obs_set = set(obs_times)
    for now in grid:
        # 1. Apply capacity that has become ready at or before this instant.
        for at, count in [a for a in arrivals if a[0] <= now]:
            ready_now = count
            arrivals.remove((at, count))

        # 2. On an observation tick, take a decision.
        if now in obs_set and go_by_time is not None:
            # Authoritative path: the real controller decided this.
            d = go_by_time.get(now)
            if d is not None:
                previous = current
                current = int(d["applied"])
                if current > previous:
                    arrivals.append((now + pd.Timedelta(minutes=readiness_min), current))
                elif current < previous:
                    ready_now = min(ready_now, current)
                if current != previous:
                    events += 1
                decisions.append(d)
        elif now in obs_set:
            required = replica_need(obs_value[now], target_rpm)      # unconstrained
            reactive = int(np.clip(required, min_r, max_r))           # what the controller may ask
            fc = forecasts.get(now)
            predicted = min_r
            if fc is not None and len(fc) >= lead_steps:
                predicted = int(np.clip(replica_need(float(np.max(fc[:lead_steps])), target_rpm),
                                        min_r, max_r))

            if reactive > 0 and predicted > reactive and predicted / reactive > OVERESTIMATE_RATIO:
                streak += 1
                if streak >= OVERESTIMATE_STREAK:
                    predicted = int(min(predicted, np.ceil(reactive * OVERESTIMATE_HEADROOM)))
                    override_active = True
            else:
                streak = 0
                override_active = False

            desired = int(np.clip(max(predicted, reactive, min_r), min_r, max_r))
            previous = current

            if desired > current:
                current = desired
                last_scale_up = now
                below_since = None
                arrivals.append((now + pd.Timedelta(minutes=readiness_min), current))
            elif desired < current:
                blocked = False
                if not override_active:
                    if last_scale_up is not None and (now - last_scale_up) < pd.Timedelta(minutes=SCALE_DOWN_STABILIZATION_MIN):
                        blocked = True
                    elif below_since is None:
                        below_since = now
                        blocked = True
                    elif (now - below_since) < pd.Timedelta(minutes=SCALE_DOWN_STABILIZATION_MIN):
                        blocked = True
                if not blocked and last_scale_down is not None and \
                        (now - last_scale_down) < pd.Timedelta(minutes=SCALE_DOWN_COOLDOWN_MIN):
                    blocked = True
                if not blocked:
                    max_remove = max(SCALE_DOWN_MIN_PODS,
                                     int(np.ceil(current * SCALE_DOWN_MAX_PERCENT / 100.0)))
                    current = max(desired, current - max_remove, 1)
                    last_scale_down = now
                    below_since = None
                    ready_now = min(ready_now, current)   # removal is immediate
            else:
                below_since = None

            if current != previous:
                events += 1
            decisions.append({"t": now.isoformat(), "current": previous, "desired": desired,
                              "applied": current, "override_active": override_active,
                              "streak": streak})

        # 3. Account capacity and shortfall for this minute, at event time.
        replica_minutes += current * tick_min
        last_obs = max((t for t in obs_times if t <= now), default=obs_times[0])
        required = replica_need(obs_value[last_obs], target_rpm)
        if required > max_r:
            over_ceiling_minutes += tick_min
        if ready_now < required:
            deficit_minutes += tick_min
            deficit_amounts.append(required - ready_now)

    return {
        "replica_minutes": round(replica_minutes, 1),
        "scaling_events": events,
        "deficit_minutes": round(deficit_minutes, 1),
        "deficit_replicas_max": int(max(deficit_amounts)) if deficit_amounts else 0,
        "deficit_replicas_mean": round(float(np.mean(deficit_amounts)), 2) if deficit_amounts else 0.0,
        "minutes_demand_exceeded_ceiling": round(over_ceiling_minutes, 1),
        "decisions_from": "go_controller" if go_by_time is not None else "python_fallback",
        "decisions": decisions,
    }


class WindowCoverageError(AssertionError):
    """The evaluation window does not contain the scenario's event (C-51)."""


def assert_window_covers_events(scenario: str, seed: int, n: int, origins: list,
                                min_side: int = PER_DAY // 2) -> None:
    """Fail loudly unless the ORIGINS ACTUALLY PRESENT cover pre-event, transition, recovery.

    Two defects this has had to survive:
      * the first harness scored windows that ended before the event ever occurred;
      * the first version of this check tested only origins[0]..origins[-1], i.e. the RANGE,
        so a window whose event-region origins had all been excluded still passed (C-74).
    It now counts origins that are actually in the list, on each side of and spanning each
    event, so exclusions cannot hollow out the evaluation unnoticed.
    """
    events = scenario_event_indices(scenario, n, seed)
    if not events:
        return
    present = set(origins)
    if not present:
        raise WindowCoverageError(f"{scenario} (seed {seed}): no origins at all")

    covered = []
    for e in events:
        # An origin "spans" the event when one of its six targets lands on it.
        spanning = [o for o in origins if o + 1 <= e <= o + STEPS_AHEAD]
        before = [o for o in origins if o + STEPS_AHEAD < e]
        after = [o for o in origins if o + 1 > e]
        if spanning and len(before) >= min_side and len(after) >= min_side:
            covered.append({"event": e, "spanning_origins": len(spanning),
                            "origins_before": len(before), "origins_after": len(after)})
    if not covered:
        detail = []
        for e in events[:5]:
            detail.append(
                f"event {e}: spanning={len([o for o in origins if o + 1 <= e <= o + STEPS_AHEAD])} "
                f"before={len([o for o in origins if o + STEPS_AHEAD < e])} "
                f"after={len([o for o in origins if o + 1 > e])}")
        raise WindowCoverageError(
            f"{scenario} (seed {seed}): no event is covered by origins that are actually "
            f"present with at least {min_side} origins on each side. "
            f"{'; '.join(detail)}. Scored origins: {len(origins)}.")


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
                      epochs: int = 50, max_origins: int | None = None,
                      target_rpm: float | None = None, verbose: bool = True,
                      model_seed: int = 0, train_window_days: int = 7):
    """Rolling origins with a retrain schedule and a publication delay.

    Production-equivalent by default (C-54): the trainer's 50 epochs, a rolling
    seven-day training window rather than expanding history, a seeded TensorFlow
    initialisation recorded separately from the data seed, and capacity calibrated on
    training data only. Origins are paired -- a predictor that fails at an origin removes
    that origin from EVERY predictor's score, so all columns cover the same targets.
    """
    import tensorflow as tf
    tf.keras.utils.set_random_seed(model_seed)
    predictors = [Persistence(), PreviousDay(), SeasonalPattern(), TrendAdaptive(),
                  NetworkOnly(), ServedBlend()]
    per_step = {p.name: [Score() for _ in range(STEPS_AHEAD)] for p in predictors}
    overall = {p.name: Score() for p in predictors}
    forecast_by_origin = {p.name: {} for p in predictors}

    warmup = max(SEQ + STEPS_AHEAD, 2 * PER_DAY)
    origins = list(range(warmup, len(series) - STEPS_AHEAD))
    if max_origins and len(origins) > max_origins:
        # C-51: when the origin count is capped, CENTRE the window on the scenario's events
        # so the scored targets span pre-event, transition and recovery. Taking the first N
        # (the original bug) or the last N both miss events that sit in between.
        events = scenario_event_indices(scenario, len(series), seed)
        if events:
            focus = int(np.median(events))
            want_start = focus - max_origins // 2
            lo = min(max(origins[0], want_start), origins[-1] - max_origins + 1)
            lo = max(lo, origins[0])
            origins = [i for i in origins if lo <= i < lo + max_origins]
        else:
            origins = origins[:max_origins]
    if not origins:
        return None

    assert_window_covers_events(scenario, seed, len(series), origins)

    model = None
    model_published_at = None
    pending_model = None
    last_train_idx = -10 ** 9
    predictor_failures: list = []
    scored_origins: list = []
    scored_indices: list = []
    availability: dict = {}
    exclusions: list = []
    retrain_every = retrain_every_h * 60 // GRID_MIN
    train_failures = []

    if target_rpm is None:
        # C-54: calibrate on the pre-origin (training) portion only -- using the whole
        # series lets the future set the capacity denominator.
        target_rpm = float(np.percentile(series.values[:origins[0]], 60)) / 3.0

    t_start = time.time()
    for k, i in enumerate(origins):
        origin = series.index[i]
        hist = series.iloc[:i + 1]                    # information available AT the origin

        # Retrain on the schedule; the artifact only becomes usable after publication.
        if i - last_train_idx >= retrain_every:
            last_train_idx = i
            try:
                m = LSTMForecastModel(sequence_length=SEQ)
                # Rolling seven-day window, as the deployed trainer uses (C-54).
                window = hist.iloc[-(train_window_days * PER_DAY):]
                # C-73: the deployed trainer applies an OUTER 80/20 split first
                # (train_lstm_from_vm.py:222-224) and fits on the first 80% only. Training
                # on the whole window gave the evaluation more data than production gets.
                outer = int(0.8 * len(window))
                train_hist = window.iloc[:outer]
                m.train(pd.DataFrame({"value": train_hist.values}, index=train_hist.index),
                        epochs=epochs)
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

        # C-54: score an origin only when EVERY predictor produced a forecast for it, so all
        # columns cover identical targets. A predictor that fails is recorded, not skipped
        # silently.
        # C-74: compute EVERY arm's forecast and record its availability BEFORE forming the
        # intersection. Breaking out of the loop early (the previous behaviour) hid which
        # arms could have produced a value, so per-arm availability could not be reported
        # and the exclusion reason was attributed to whichever arm happened to fail first.
        this_origin, reasons = {}, {}
        for p in predictors:
            if p.needs_model and model is None:
                reasons[p.name] = "no published model (cold start)"
                continue
            try:
                pred = np.asarray(p.forecast(hist, origin, STEPS_AHEAD, model=model), dtype=float)
            except Exception as exc:
                reasons[p.name] = f"error: {type(exc).__name__}: {exc}"
                predictor_failures.append(f"{p.name} @ {origin.isoformat()}: {exc}")
                log.warning("%s failed at %s: %s", p.name, origin, exc)
                continue
            if not np.all(np.isfinite(pred)):
                reasons[p.name] = "non-finite forecast"
                predictor_failures.append(f"{p.name} @ {origin.isoformat()}: non-finite forecast")
                continue
            this_origin[p.name] = pred

        for name in this_origin:
            availability[name] = availability.get(name, 0) + 1
        if reasons:
            exclusions.append({"origin": origin.isoformat(), "index": i, "reasons": reasons})

        if len(this_origin) == len(predictors):
            scored_origins.append(origin)
            scored_indices.append(i)
            for name, pred in this_origin.items():
                overall[name].add(pred, truth)
                for st in range(STEPS_AHEAD):
                    per_step[name][st].add([pred[st]], [truth[st]])
                forecast_by_origin[name][origin] = pred

        if verbose and k % 25 == 0:
            print(f"    origin {k + 1}/{len(origins)} ({origin})  "
                  f"[{time.time() - t_start:.0f}s]", flush=True)

    # C-74: the pre-scoring assertion cannot see exclusions. Re-check coverage against the
    # origins that were ACTUALLY scored -- an evaluation whose event origins were all
    # excluded looks fine to the pre-check and is still worthless.
    coverage_after = {"ok": True, "detail": "no events (by design)"}
    if scored_indices:
        try:
            assert_window_covers_events(scenario, seed, len(series), scored_indices)
            ev = scenario_event_indices(scenario, len(series), seed)
            coverage_after = {"ok": True, "events_in_scored_window":
                              len([e for e in ev
                                   if scored_indices[0] + 1 <= e <= scored_indices[-1] + STEPS_AHEAD]),
                              "events_total": len(ev)}
        except WindowCoverageError as exc:
            coverage_after = {"ok": False, "detail": str(exc)}
            log.warning("post-exclusion coverage FAILED: %s", exc)
    else:
        coverage_after = {"ok": False, "detail": "no origins scored"}

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
        "origins_offered": len(origins),
        "origins_scored": len(scored_origins),
        "predictor_failures": predictor_failures,
        "availability_per_arm": availability,
        "exclusions": exclusions[:200],
        "exclusions_total": len(exclusions),
        "event_coverage_after_exclusions": coverage_after,
        "equivalence_caveats": [
            "trainer: outer 80/20 split applied (C-73), matching train_lstm_from_vm.py",
            "NOT production-equivalent: the API's matured-error feedback loop "
            "(mape_for_floor -> effective percentile) is not exercised",
            "NOT production-equivalent: the Go replay calls selected controller functions, "
            "not the full forecasting and reconciliation path; confidence dampening absent",
        ],
        "epochs": epochs,
        "train_window_days": train_window_days,
        "data_seed": seed,
        "model_seed": model_seed,
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
    """Deliberately provoke the conditions a forecaster meets in production.

    Every entry is marked SCORED or NOT SCORED (C-51/C-74). The first version of this suite
    reported level-shift and spike "recovery" results whose events lay outside the evaluation
    window -- they measured ordinary traffic. Anything that cannot be scored now says so.
    """
    results = {}
    base = make_series("repeating", days=10, seed=1)

    def _train(series, **kw):
        m = LSTMForecastModel(sequence_length=SEQ)
        m.train(pd.DataFrame({"value": series.values}, index=series.index), epochs=5, **kw)
        return m

    def mark(status, detail):
        return {"status": status, "detail": detail}

    # 1. Cold start -- no history at all.
    try:
        _train(base.iloc[:SEQ // 2])
        results["cold_start"] = mark("SCORED", "FAIL: trained on a series shorter than one window")
    except ValueError as e:
        results["cold_start"] = mark("SCORED", f"refused cleanly: {e}")

    # 2. All zeros.
    zeros = pd.Series(np.zeros(len(base)), index=base.index)
    try:
        m = _train(zeros)
        out = _model_predict(m, zeros, zeros.index[-1], STEPS_AHEAD, seasonal=zeros)
        vals = np.asarray(out["predictions"])
        results["all_zeros"] = mark("SCORED", "handled: finite output"
                                    if np.all(np.isfinite(vals)) else "FAIL: non-finite output")
    except Exception as e:
        results["all_zeros"] = mark("SCORED", f"raised: {type(e).__name__}: {e}")

    # 3. A gap in the seasonal history -- the pattern must not invent a value.
    gapped = base.drop(base.index[-(PER_DAY + 30):-(PER_DAY + 10)])
    helper = LSTMForecastModel(sequence_length=SEQ)
    vals, src = helper._pattern_forecast(origin=base.index[-1].to_pydatetime(),
                                         steps_ahead=STEPS_AHEAD,
                                         seasonal_history=gapped, effective_pct=75)
    results["gap_in_history"] = mark(
        "SCORED", f"pattern source={src}, finite={bool(vals is None or np.all(np.isfinite(vals)))}")

    # 4. Delayed observation -- origin must follow the data, not the wall clock.
    stale_origin = base.index[-1] - pd.Timedelta(minutes=40)
    m = _train(base)
    out = _model_predict(m, base.loc[:stale_origin], stale_origin, STEPS_AHEAD,
                         seasonal=base.loc[:stale_origin])
    first_target = pd.Timestamp(out["target_timestamps"][0])
    results["delayed_observation"] = mark(
        "SCORED",
        "anchored to the data" if first_target == stale_origin + pd.Timedelta(minutes=GRID_MIN)
        else f"FAIL: first target {first_target} for origin {stale_origin}")

    # 5/6. Level shift and spike recovery -- scored ONLY if the window covers the event.
    for kind in ("levelshift", "spike"):
        s_series = make_series(kind, days=12, seed=3)
        try:
            r = evaluate_scenario(s_series, kind, 3, epochs=5, max_origins=300, verbose=False)
        except WindowCoverageError as exc:
            results[f"{kind}_recovery"] = mark("NOT SCORED", f"window coverage: {exc}")
            continue
        if not r:
            results[f"{kind}_recovery"] = mark("NOT SCORED", "no origins")
            continue
        cov = r.get("event_coverage_after_exclusions", {})
        status = "SCORED" if cov.get("ok") else "NOT SCORED"
        results[f"{kind}_recovery"] = mark(status, {
            "served_blend_mae": r["overall"]["served_blend"]["mae"],
            "strongest_baseline": min(
                (k for k in ("seasonal_pattern", "previous_day", "persistence", "trend_adaptive")),
                key=lambda k: r["overall"][k]["mae"]),
            "coverage_after_exclusions": cov,
            "origins_scored": r["origins_scored"],
        })

    # 7. Corrupt / mismatched artifact -- the format guard.
    class _Old:
        model = type("m", (), {"output_shape": (None, 1)})()
    results["old_format_artifact"] = mark(
        "SCORED", "detected" if LSTMForecastModel._is_old_model_format(_Old())
        else "FAIL: old format not detected")

    # 8. API failure -- covered by the operator's Go tests and the API unit tests, not here.
    results["api_failure"] = mark(
        "NOT SCORED",
        "exercised in k8s-operator/controllers (refusal vs transport failure, bounded cache) "
        "and ml-engine/tests/test_model_swap.py, not by this harness")

    # 9. Retrain landing mid-peak.
    peak = make_series("repeating", days=8, seed=7)
    try:
        r = evaluate_scenario(peak, "repeating", 7, epochs=5, max_origins=300,
                              publication_delay_min=20, verbose=False)
        results["retrain_midpeak"] = mark(
            "SCORED", "no failures" if r and not r["train_failures"]
            else f"train failures: {r['train_failures'] if r else 'n/a'}")
    except WindowCoverageError as exc:
        results["retrain_midpeak"] = mark("NOT SCORED", str(exc))

    if verbose:
        for k, v in results.items():
            print(f"  [{v['status']:10s}] {k}: {v['detail']}")
    return results


# ======================================================================================
def _print_run(r):
    o = r["overall"]
    order = ("served_blend", "network_only", "seasonal_pattern", "previous_day",
             "persistence", "trend_adaptive")
    best = min((k for k in order if o.get(k, {}).get("mae") is not None),
               key=lambda k: o[k]["mae"])
    print("   " + "  ".join(f"{k.split('_')[0]}={o[k]['mae']}" for k in order
                            if o.get(k, {}).get("mae") is not None), flush=True)
    print(f"   scored={r['origins_scored']}/{r['origins_offered']}  "
          f"strongest baseline/arm: {best}  decisions={r['operational'].get('reactive_only', {}).get('decisions_from')}",
          flush=True)


def run_sweep(out_path: Path, days: int = 12, cap: int = 400, epochs: int = 50,
              data_seeds=(1, 2), model_seed: int = 101):
    """The production-equivalent sweep (Codex C-54)."""
    scenarios = ("repeating", "trend", "levelshift", "spike", "weekly")
    runs, skipped = [], []
    for scenario in scenarios:
        for seed in data_seeds:
            print(f"\n== {scenario} (data seed {seed}, model seed {model_seed})", flush=True)
            series = make_series(scenario, days=days, seed=seed)
            try:
                r = evaluate_scenario(series, scenario, seed, epochs=epochs, max_origins=cap,
                                      model_seed=model_seed, verbose=False)
            except WindowCoverageError as exc:
                print(f"   SKIPPED (window coverage): {exc}", flush=True)
                skipped.append({"scenario": scenario, "seed": seed, "reason": str(exc)})
                continue
            if r:
                runs.append(r)
                _print_run(r)

    out = {"generated_at": datetime.utcnow().isoformat() + "Z",
           "kind": "production_equivalent_sweep",
           "preregistered_margin": PREREGISTERED_MARGIN,
           "config": {"days": days, "origin_cap": cap, "epochs": epochs,
                      "train_window_days": 7, "data_seeds": list(data_seeds),
                      "model_seed": model_seed, "profile": "k6 hourly step"},
           "skipped": skipped, "runs": runs}
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nwrote {out_path}", flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="smoke run")
    ap.add_argument("--full", action="store_true", help="the production-equivalent sweep")
    ap.add_argument("--failure-modes", action="store_true")
    ap.add_argument("--epochs", type=int, default=50)
    ap.add_argument("--cap", type=int, default=400)
    ap.add_argument("--model-seed", type=int, default=101)
    ap.add_argument("--out", default=str(Path(__file__).parent / "results.json"))
    args = ap.parse_args()

    if args.failure_modes:
        print("Failure modes:")
        res = failure_modes()
        Path(args.out).write_text(json.dumps({"failure_modes": res}, indent=2, default=str))
        return

    if args.quick:
        run_sweep(Path(args.out), days=8, cap=200, epochs=5, data_seeds=(1,),
                  model_seed=args.model_seed)
        return

    run_sweep(Path(args.out), epochs=args.epochs, cap=args.cap, model_seed=args.model_seed)


if __name__ == "__main__":
    main()
