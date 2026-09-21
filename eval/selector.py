#!/usr/bin/env python3
"""Offline experiment: does an online selector between cheap forecasters beat a fixed one?

This is an OFFLINE experiment (Codex C-92 / D-116). Nothing here deploys, reads a cluster,
or changes a served forecast. It answers one question on recorded/synthetic series:

    Serving whichever cheap forecaster has been cheapest lately -- does that beat serving
    the single best fixed forecaster, by enough to be worth the extra moving part?

The design follows Autopilot's *mechanism* (Rzadca et al., EuroSys 2020, Eq. 8-9): keep an
exponentially-smoothed realised cost per candidate and serve the arg-min, with an explicit
penalty for switching candidate. It borrows no number from that paper. Autopilot's published
31% -> 23% slack difference is a comparison of observational job cohorts, not a selector-only
ablation, and its candidates are vertical resource-limit recommenders rather than forecasts;
see docs/RESEARCH-2026-09-22.md, section 1, "[Corrected 2026-09-22]".

WHAT THE COST NUMBER IS, AND IS NOT
-----------------------------------
Arms are ranked by an asymmetric absolute-error PROXY, in requests per minute:

    cost(origin) = sum_h w_h * ( 2 * max(0, actual_h - forecast_h)      # under-forecast
                               + 1 * max(0, forecast_h - actual_h) )    # over-forecast
                   / sum_h w_h

The 2:1 asymmetry mirrors the weighting the trainer already uses. Being an ABSOLUTE loss, its
population optimum is the 2/3 quantile (Ehm et al. 2016, Eq. 5-6); the trainer's
`asymmetric_mse` is a SQUARED loss and so targets the 2/3 expectile instead. That difference
is deliberate here: a quantile-consistent score is the right way to RANK forecasts.

It is a proxy and not operating cost. It prices no pod start-up delay, no integer replica
step, no stabilisation window, no dropped request and no dollars. Operational consequences
are reported separately, from a full controller replay run independently for every arm.

INFORMATION HYGIENE (the rules this file must not break)
--------------------------------------------------------
1. Every arm forecasts from exactly the same history at every origin: `series[:i+1]`.
2. A forecast issued at origin i targets i+1..i+6. It has MATURED only once index i+6 has
   been observed. The selector at origin k therefore updates from origin k-6 and no later
   one. Nothing is scored, and nothing feeds selection, before its target exists.
3. Every arm produces a forecast at every origin, so the selector never enjoys an
   availability advantage. Missing-data behaviour is explicit (see `LaggedLinear`) and
   counted.
4. Each arm gets its OWN controller replay, with its own state. No arm's operational result
   is inferred from another arm's decisions.
5. Selector parameters are chosen on VALIDATION series only (seeds and calendar dates that
   the test set does not contain) and then frozen. `_assert_disjoint()` enforces it.
6. ONE external objective (`EVAL_OBJECTIVE`) scores everything that is compared against
   anything else. A parameter setting is never ranked by the weighting it chose for itself.

TWO NOTES CARRIED FROM THE REVIEW (D-117)
-----------------------------------------
* A lag of 1008 ten-minute steps is a full SEVEN DAYS -- the whole rolling window the
  deployed trainer reads -- so that feature would be undefined for every row unless the
  history is expanded. `LaggedLinear` therefore uses lags {1,2,3,6,144} and omits 1008.
* The scenario series is OFFERED demand, and `offline_eval.replica_need()` is deliberately
  unconstrained by the replica ceiling, so an overloaded period shows up as a deficit rather
  than as falling demand. Minutes where demand exceeded the ceiling are reported separately.

Run:
    eval/.venv/bin/python eval/selector.py --out eval/selector-<utc>.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import offline_eval as oe  # noqa: E402  (brings ml-engine onto sys.path itself)
from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402

GRID_MIN = oe.GRID_MIN
PER_DAY = oe.PER_DAY

# ======================================================================================
# Predeclared experiment design. Everything in this block is fixed BEFORE the test run.
# ======================================================================================
SCENARIOS = ("repeating", "trend", "levelshift", "spike", "weekly")

# Validation uses the data seeds and calendar window the project has already run against
# (RESULTS-2026-09-22-corrected.md used seeds 1 and 2 starting 2026-03-01). Test uses seeds
# and dates that no run in this repository has touched.
VALIDATION_SEEDS = (1, 2)
VALIDATION_START = datetime(2026, 3, 1)
TEST_SEEDS = (11, 12, 13)
TEST_START = datetime(2026, 6, 7)

DAYS = 14
MAX_ORIGINS = 720                 # 5 days of origins, so block bootstrap has whole-day blocks
MIN_REPLICAS, MAX_REPLICAS = 1, 12

COST_UNDER, COST_OVER = 2.0, 1.0  # asymmetric absolute proxy, 2:1, in rpm

CANDIDATES = ("seasonal_pattern", "trend_adaptive", "lagged_linear")  # canonical tie-break order
CONTROL_ARM = "constant_blend"

# Parameter grid searched on VALIDATION ONLY.
GRID_HALF_LIFE = (6, 18, 72)                  # matured origins: 1 h, 3 h, 12 h
GRID_WEIGHTS = ("uniform", "deployed", "lead2")
GRID_MIN_OBS = (6, 18)
GRID_SWITCH_FRAC = (0.0, 0.02, 0.05, 0.10)    # fraction of the median validation cost
GRID_DEFAULT_ARM = CANDIDATES
GRID_BLEND_WEIGHT = (0.25, 0.5, 0.75)         # control arm's seasonal share

HORIZON_WEIGHTS = {
    "uniform": np.ones(STEPS_AHEAD),
    "deployed": np.array([1.0, 0.9, 0.8, 0.7, 0.6, 0.5])[:STEPS_AHEAD],
    "lead2": np.array([1.0, 1.0, 0.0, 0.0, 0.0, 0.0])[:STEPS_AHEAD],
}

# ---- THE SINGLE EXTERNAL EVALUATION OBJECTIVE (Codex C-96 / D-121) --------------------
# Fixed here, before any comparison, and used for EVERY score that is compared against any
# other score: the parameter sweep's ranking, the control blend's weight, the test tables,
# the bootstrap and the bar.
#
# The defect this replaces: the sweep ranked each parameter setting using THAT SETTING'S OWN
# horizon weights, so a setting scoring only the two nearest steps (`lead2`) was compared
# against a setting scoring all six. That ranks the difficulty of the horizon, not the
# quality of the forecast, and it let the sweep "win" by choosing an easier question. The
# control blend had the mirror-image fault: chosen under `uniform`, reported under `lead2`.
#
# `uniform` is the choice because the preregistered decision rule (evaluation-protocol.md
# section 9, D-86) already states uniform horizon weights with per-step MAE reported in full.
# The objective is a PROPERTY OF THE EXPERIMENT; `SelectorParams.weights` is a knob INSIDE
# the selector -- which horizon weighting it uses to rank candidates while it is running --
# and is tuned against the external objective like any other parameter.
EVAL_OBJECTIVE = "uniform"


def eval_weights() -> np.ndarray:
    return HORIZON_WEIGHTS[EVAL_OBJECTIVE]


# ---- Predeclared success bar (D-116). Written before any test number was produced. -----
BAR_POOLED_IMPROVEMENT = 0.05     # S1: >= 5% lower pooled cost proxy than the best fixed arm
BAR_REPEATING_TOLERANCE = 0.02    # S2: <= 2% worse than the best fixed arm on `repeating`
BAR_REPEATING_CI_UPPER = 0.05     # S2: 95% CI upper bound on that relative gap <= +5%
BAR_SHORTAGE_TOLERANCE = 0.10     # S3: pooled deficit minutes <= +10% of the best fixed arm
BAR_SWITCH_FRACTION = 0.10        # S3: selector changes arm on <= 10% of origins

BOOTSTRAP_DRAWS = 2000
BOOTSTRAP_SEED = 20260922


# ======================================================================================
# The added candidate: a cheap direct lagged linear forecaster (D-117)
# ======================================================================================
LINEAR_LAGS = (1, 2, 3, 6, 144)   # 1008 (= 7 days) deliberately omitted; see module docstring
LINEAR_FIT_WINDOW = 7 * PER_DAY   # rows of history used for the fit, at most
LINEAR_MIN_ROWS = 200             # below this, fall back (and say so)
LINEAR_RIDGE = 1e-3               # on standardised features


class LaggedLinear:
    """Direct per-step ridge regression on {1,2,3,6,144}-step lags.

    One independent model per horizon step, refit at every origin on the trailing window --
    "direct", not recursive, so error does not compound across steps (Ben Taieb et al.).

    MISSING-DATA BEHAVIOUR, explicit and counted: when fewer than LINEAR_MIN_ROWS usable
    rows exist for a step (early origins, or a history shorter than the longest lag), that
    step falls back to the same-time-yesterday value, and to the last observation if even
    that is unavailable. The arm always returns a finite forecast, so no origin is dropped
    from any arm's score on its account; every fallback is counted as a `degraded` origin
    and reported.
    """

    name = "lagged_linear"

    def __init__(self, series: pd.Series):
        self.values = series.values.astype(float)
        self.index = series.index
        n = len(self.values)
        max_lag = max(LINEAR_LAGS)
        # design[t] holds the features usable at origin t (lag L -> value at t-(L-1))
        self.design = np.full((n, len(LINEAR_LAGS)), np.nan)
        for col, lag in enumerate(LINEAR_LAGS):
            back = lag - 1
            if back == 0:
                self.design[:, col] = self.values
            else:
                self.design[back:, col] = self.values[:-back]
        self.first_valid = max_lag - 1
        self._cache: dict[int, tuple[np.ndarray, bool]] = {}

    def forecast(self, origin_i: int) -> tuple[np.ndarray, bool]:
        if origin_i in self._cache:
            return self._cache[origin_i]
        out = np.empty(STEPS_AHEAD)
        degraded = False
        lo = max(self.first_valid, origin_i - LINEAR_FIT_WINDOW + 1)
        x_now = self.design[origin_i]
        for h in range(1, STEPS_AHEAD + 1):
            hi = origin_i - h                       # last row whose target is observed
            rows = hi - lo + 1
            coef = None
            if rows >= LINEAR_MIN_ROWS and np.all(np.isfinite(x_now)):
                X = self.design[lo:hi + 1]
                y = self.values[lo + h:hi + 1 + h]
                if np.all(np.isfinite(X)) and len(y) == len(X):
                    coef = self._fit(X, y)
            if coef is None:
                degraded = True
                out[h - 1] = self._fallback(origin_i, h)
            else:
                mu, sigma, beta, intercept = coef
                z = (x_now - mu) / sigma
                out[h - 1] = float(intercept + z @ beta)
        out = np.maximum(out, 1.0)
        self._cache[origin_i] = (out, degraded)
        return out, degraded

    @staticmethod
    def _fit(X: np.ndarray, y: np.ndarray):
        mu = X.mean(axis=0)
        sigma = X.std(axis=0)
        sigma = np.where(sigma < 1e-9, 1.0, sigma)
        Z = (X - mu) / sigma
        ybar = float(y.mean())
        A = Z.T @ Z + LINEAR_RIDGE * len(Z) * np.eye(Z.shape[1])
        b = Z.T @ (y - ybar)
        try:
            beta = np.linalg.solve(A, b)
        except np.linalg.LinAlgError:
            return None
        if not np.all(np.isfinite(beta)):
            return None
        return mu, sigma, beta, ybar

    def _fallback(self, origin_i: int, h: int) -> float:
        prev = origin_i + h - PER_DAY
        if 0 <= prev <= origin_i:
            return float(self.values[prev])
        return float(self.values[origin_i])


# ======================================================================================
# Cost proxy
# ======================================================================================
def origin_cost(pred: np.ndarray, truth: np.ndarray, weights: np.ndarray) -> float:
    """Weighted asymmetric ABSOLUTE error, in rpm. A proxy, not operating cost."""
    err = truth - pred
    per_step = np.where(err > 0.0, COST_UNDER * err, COST_OVER * (-err))
    return float(np.sum(weights * per_step) / np.sum(weights))


# ======================================================================================
# One run = one (scenario, data seed). Forecasts are produced once and reused by every
# parameter setting, so the sweep can never change what a candidate predicted.
# ======================================================================================
@dataclass
class Run:
    scenario: str
    seed: int
    start: datetime
    series: pd.Series
    origins: list                       # integer positions into `series`
    target_rpm: float
    forecasts: dict = field(default_factory=dict)   # arm -> (n_origins, STEPS_AHEAD)
    truth: np.ndarray = field(default_factory=lambda: np.empty(0))
    degraded: dict = field(default_factory=dict)    # arm -> count
    failures: dict = field(default_factory=dict)    # arm -> count of non-finite forecasts
    # Per-ORIGIN masks, so a derived arm (the blend, the selector) inherits the failures of
    # whatever it was built from at each origin instead of asserting zero (Codex C-97).
    degraded_mask: dict = field(default_factory=dict)   # arm -> bool array over origins
    failure_mask: dict = field(default_factory=dict)    # arm -> bool array over origins

    @property
    def key(self) -> str:
        return f"{self.scenario}/seed{self.seed}"


def build_run(scenario: str, seed: int, start: datetime, helper: LSTMForecastModel) -> Run:
    series = oe.make_series(scenario, DAYS, seed, start=start)
    n = len(series)
    warmup = max(oe.SEQ + STEPS_AHEAD, 2 * PER_DAY)
    origins = list(range(warmup, n - STEPS_AHEAD))
    events = oe.scenario_event_indices(scenario, n, seed)
    if len(origins) > MAX_ORIGINS:
        if events:
            focus = int(np.median(events))
            lo = min(max(origins[0], focus - MAX_ORIGINS // 2), origins[-1] - MAX_ORIGINS + 1)
            lo = max(lo, origins[0])
            origins = [i for i in origins if lo <= i < lo + MAX_ORIGINS]
        else:
            origins = origins[:MAX_ORIGINS]
    assert origins == list(range(origins[0], origins[-1] + 1)), "origins must be contiguous"
    oe.assert_window_covers_events(scenario, seed, n, origins)

    # Capacity denominator from PRE-origin data only -- the future must not set it.
    target_rpm = float(np.percentile(series.values[:origins[0]], 60)) / 3.0

    run = Run(scenario, seed, start, series, origins, target_rpm)
    values = series.values.astype(float)
    run.truth = np.stack([values[i + 1:i + 1 + STEPS_AHEAD] for i in origins])

    sp, ta = oe.SeasonalPattern(), oe.TrendAdaptive()
    ll = LaggedLinear(series)
    fc = {a: np.empty((len(origins), STEPS_AHEAD)) for a in CANDIDATES}
    deg_mask = {a: np.zeros(len(origins), dtype=bool) for a in CANDIDATES}
    fail_mask = {a: np.zeros(len(origins), dtype=bool) for a in CANDIDATES}

    for k, i in enumerate(origins):
        hist = series.iloc[:i + 1]                    # identical information for every arm
        origin_ts = series.index[i]
        fc["seasonal_pattern"][k] = sp.forecast(hist, origin_ts, STEPS_AHEAD, model=helper)
        fc["trend_adaptive"][k] = ta.forecast(hist, origin_ts, STEPS_AHEAD)
        lin, deg = ll.forecast(i)
        fc["lagged_linear"][k] = lin
        if deg:
            deg_mask["lagged_linear"][k] = True

    for a in CANDIDATES:
        bad = ~np.all(np.isfinite(fc[a]), axis=1)
        fail_mask[a] = bad
        if bad.any():
            # Documented repair: a non-finite forecast is replaced by the last observation,
            # which is what the deployed service would fall back to, and counted.
            for k in np.flatnonzero(bad):
                fc[a][k] = float(series.values[origins[k]])
            deg_mask[a] |= bad

    run.forecasts = fc
    run.degraded_mask = deg_mask
    run.failure_mask = fail_mask
    run.degraded = {a: int(deg_mask[a].sum()) for a in CANDIDATES}
    run.failures = {a: int(fail_mask[a].sum()) for a in CANDIDATES}
    return run


BLEND_INPUTS = ("seasonal_pattern", "trend_adaptive")


def add_blend(run: Run, weight: float) -> None:
    """Control arm: a constant mixture of the two incumbent forecasters.

    It inherits its inputs' failures at each origin (C-97): a mixture containing a repaired
    forecast is itself a repaired forecast, and asserting zero would exempt the control from
    the same gate the selector is judged by.
    """
    run.forecasts[CONTROL_ARM] = (weight * run.forecasts["seasonal_pattern"]
                                  + (1.0 - weight) * run.forecasts["trend_adaptive"])
    deg = np.zeros_like(run.degraded_mask[BLEND_INPUTS[0]])
    fail = np.zeros_like(run.failure_mask[BLEND_INPUTS[0]])
    for a in BLEND_INPUTS:
        deg |= run.degraded_mask[a]
        fail |= run.failure_mask[a]
    run.degraded_mask[CONTROL_ARM] = deg
    run.failure_mask[CONTROL_ARM] = fail
    run.degraded[CONTROL_ARM] = int(deg.sum())
    run.failures[CONTROL_ARM] = int(fail.sum())


def attach_selector_arm(run: Run, served: list) -> None:
    """Materialise the selector as an arm, INCLUDING what it inherited (Codex C-97 / D-122).

    The defect this replaces hardcoded the selector's degradation and failure counts to zero,
    so the S3 failure gate could not see a failure the selector had served. The selector owns
    whatever the arm it chose did at that origin -- serving a repaired forecast is serving a
    repaired forecast, whoever computed it.
    """
    run.forecasts["selector"] = selector_forecasts(run, served)
    deg = np.array([bool(run.degraded_mask[a][k]) for k, a in enumerate(served)])
    fail = np.array([bool(run.failure_mask[a][k]) for k, a in enumerate(served)])
    run.degraded_mask["selector"] = deg
    run.failure_mask["selector"] = fail
    run.degraded["selector"] = int(deg.sum())
    run.failures["selector"] = int(fail.sum())


def per_origin_costs(run: Run, weights: np.ndarray, arms) -> dict:
    return {a: np.array([origin_cost(run.forecasts[a][k], run.truth[k], weights)
                         for k in range(len(run.origins))]) for a in arms}


# ======================================================================================
# The selector
# ======================================================================================
@dataclass(frozen=True)
class SelectorParams:
    half_life: int
    weights: str
    min_obs: int
    switch_penalty: float      # absolute, in the cost proxy's units (rpm)
    default_arm: str
    blend_weight: float

    def as_dict(self):
        return {"ewma_half_life_matured_origins": self.half_life,
                "horizon_weights": self.weights,
                "min_matured_observations": self.min_obs,
                "switching_penalty_rpm": round(self.switch_penalty, 3),
                "startup_default_arm": self.default_arm,
                "control_blend_seasonal_weight": self.blend_weight}


def run_selector(costs: dict, params: SelectorParams) -> dict:
    """Serve one candidate per origin; return the served index per origin and the switches.

    Startup, deterministically: until EVERY candidate has at least `min_obs` matured cost
    observations, serve `default_arm`. No candidate is ever preferred for having been seen
    less often.

    Maturation, deterministically: at origin k the newest usable evidence is origin
    k - STEPS_AHEAD, whose six targets are all observed by k. Nothing newer is consulted.

    Ties, deterministically: the INCUMBENT wins an exact tie on the penalised cost; among
    non-incumbents, the earliest arm in CANDIDATES order wins. Comparisons use a 1e-9
    relative tolerance.

    Codex C-97 / D-122: the previous implementation scanned CANDIDATES in canonical order and
    replaced the best only on a strict improvement, so the FIRST arm scanned won every tie --
    including ties the incumbent was part of. An exact tie therefore switched, which is the
    opposite of what a switching penalty is for: a penalty that does not make the incumbent
    win an equal contest is not a switching penalty.
    """
    n = len(next(iter(costs.values())))
    decay = 1.0 - 0.5 ** (1.0 / params.half_life)
    ewma = {a: None for a in CANDIDATES}
    n_obs = {a: 0 for a in CANDIDATES}
    served, switches = [], 0
    current = params.default_arm

    for k in range(n):
        j = k - STEPS_AHEAD                      # newest fully matured origin
        if j >= 0:
            for a in CANDIDATES:
                c = costs[a][j]
                if not np.isfinite(c):
                    continue                     # missing observation: no update, no penalty
                ewma[a] = c if ewma[a] is None else decay * c + (1.0 - decay) * ewma[a]
                n_obs[a] += 1

        if min(n_obs[a] for a in CANDIDATES) < params.min_obs:
            choice = params.default_arm
        else:
            keys = {a: ewma[a] + (0.0 if a == current else params.switch_penalty)
                    for a in CANDIDATES}
            best_key = min(keys.values())
            tol = 1e-9 * max(1.0, abs(best_key))
            tied = [a for a in CANDIDATES if keys[a] <= best_key + tol]
            # The incumbent first: an equal contest is not a reason to move.
            choice = current if current in tied else tied[0]
        if choice != current:
            switches += 1
        current = choice
        served.append(choice)

    return {"served": served, "switches": switches,
            "switch_fraction": round(switches / n, 4) if n else 0.0}


def selector_forecasts(run: Run, served: list) -> np.ndarray:
    out = np.empty((len(served), STEPS_AHEAD))
    for k, arm in enumerate(served):
        out[k] = run.forecasts[arm][k]
    return out


# ======================================================================================
# Validation sweep -- parameter selection, on validation runs ONLY
# ======================================================================================
def preselect(runs: list, log=print) -> tuple[SelectorParams, list]:
    # The switching penalty is expressed in cost units, scaled from the median per-origin
    # cost on validation so that "5% of a typical origin's cost" is a stated quantity.
    base = np.median(np.concatenate([
        np.concatenate(list(per_origin_costs(r, eval_weights(), CANDIDATES).values()))
        for r in runs]))
    log(f"validation median per-origin cost proxy ({EVAL_OBJECTIVE}): {base:.1f} rpm")

    # Two distinct things, kept distinct (C-96):
    #   cost_cache  -- what the SELECTOR sees while running, under its own internal weights;
    #   ext_cost    -- the single EXTERNAL objective every setting is ranked by.
    cost_cache = {}
    for wname, w in HORIZON_WEIGHTS.items():
        for r in runs:
            cost_cache[(wname, r.key)] = per_origin_costs(r, w, CANDIDATES)
    ext_cost = {r.key: cost_cache[(EVAL_OBJECTIVE, r.key)] for r in runs}

    # The control arm's mixture weight is chosen separately -- it has no effect on the
    # selector -- by giving the control its own best-on-validation setting, so the selector
    # is compared against the strongest constant blend rather than an arbitrary one.
    blend_scores = {}
    for bw in GRID_BLEND_WEIGHT:
        vals = []
        for r in runs:
            mix = bw * r.forecasts["seasonal_pattern"] + (1 - bw) * r.forecasts["trend_adaptive"]
            vals += [origin_cost(mix[k], r.truth[k], eval_weights())
                     for k in range(len(r.origins))]
        blend_scores[bw] = float(np.mean(vals))
    best_bw = min(GRID_BLEND_WEIGHT, key=lambda b: (round(blend_scores[b], 6), b))
    log("validation constant blend: " +
        ", ".join(f"w={b}->{v:.1f}" for b, v in blend_scores.items()) +
        f"  chosen w={best_bw}")

    table = []
    grid = [(wn, hl, mo, fr, df)
            for wn in GRID_WEIGHTS for hl in GRID_HALF_LIFE for mo in GRID_MIN_OBS
            for fr in GRID_SWITCH_FRAC for df in GRID_DEFAULT_ARM]
    for rank, (wname, hl, mo, frac, dflt) in enumerate(grid):
        p = SelectorParams(hl, wname, mo, frac * float(base), dflt, best_bw)
        tot, cnt, sw, org = 0.0, 0, 0, 0
        for r in runs:
            # The selector RUNS on its own internal weighting ...
            res = run_selector(cost_cache[(wname, r.key)], p)
            # ... and is SCORED on the single external objective, so settings that score an
            # easier horizon cannot win by scoring an easier horizon (C-96).
            ext = ext_cost[r.key]
            picked = np.array([ext[a][k] for k, a in enumerate(res["served"])])
            tot += float(picked.sum()); cnt += len(picked)
            sw += res["switches"]; org += len(picked)
        table.append({"params": p, "mean_cost": tot / cnt,
                      "switch_fraction": sw / org, "rank": rank})
    # Deterministic ordering: lowest validation cost, then fewest switches, then the
    # canonical ordering of the grid itself.
    table.sort(key=lambda e: (round(e["mean_cost"], 6), round(e["switch_fraction"], 6),
                              e["rank"]))
    best = table[0]["params"]

    # Fixed-arm reference on validation, for the write-up only.
    fixed = {}
    for a in CANDIDATES:
        vals = np.concatenate([ext_cost[r.key][a] for r in runs])
        fixed[a] = float(vals.mean())
    log(f"validation winner: {best.as_dict()}  mean={table[0]['mean_cost']:.1f} rpm "
        f"({EVAL_OBJECTIVE}) switch_frac={table[0]['switch_fraction']:.3f}")
    log("validation fixed arms: " + ", ".join(f"{a}={v:.1f}" for a, v in fixed.items()))
    summary = {
        "external_objective": EVAL_OBJECTIVE,
        "scored_by": f"every entry below is the mean per-origin cost proxy under the single "
                     f"external objective '{EVAL_OBJECTIVE}', whatever internal horizon "
                     f"weighting the setting itself uses",
        "median_per_origin_cost_rpm": round(float(base), 2),
        "constant_blend_weight_scores": {str(b): round(v, 2) for b, v in blend_scores.items()},
        "fixed_arm_validation_cost": {a: round(v, 2) for a, v in fixed.items()},
        "top12": [{"params": e["params"].as_dict(), "mean_cost": round(e["mean_cost"], 2),
                   "switch_fraction": round(e["switch_fraction"], 4)} for e in table[:12]],
        "worst": {"params": table[-1]["params"].as_dict(),
                  "mean_cost": round(table[-1]["mean_cost"], 2)},
        "grid_size": len(table),
    }
    return best, summary


# ======================================================================================
# Block bootstrap over WHOLE DAYS (overlapping rolling origins are not independent)
# ======================================================================================
def block_bootstrap(blocks: dict, arm_a: str, arm_b: str, draws=BOOTSTRAP_DRAWS):
    """Paired difference arm_a - arm_b, resampling whole (run, calendar day) blocks.

    Blocks are resampled jointly for both arms, because the arms share origins; that keeps
    the comparison paired, which is the only honest way to compare on the same targets.
    """
    keys = sorted(blocks.keys())
    sums_a = np.array([blocks[k][arm_a].sum() for k in keys])
    sums_b = np.array([blocks[k][arm_b].sum() for k in keys])
    counts = np.array([len(blocks[k][arm_a]) for k in keys], dtype=float)
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    n = len(keys)
    diffs = np.empty(draws)
    rels = np.empty(draws)
    for d in range(draws):
        pick = rng.integers(0, n, n)
        c = counts[pick].sum()
        ma = sums_a[pick].sum() / c
        mb = sums_b[pick].sum() / c
        diffs[d] = ma - mb
        rels[d] = (ma - mb) / mb if mb > 0 else np.nan
    point_a = sums_a.sum() / counts.sum()
    point_b = sums_b.sum() / counts.sum()
    return {
        "mean_a": round(float(point_a), 2),
        "mean_b": round(float(point_b), 2),
        "diff": round(float(point_a - point_b), 2),
        "diff_ci95": [round(float(np.percentile(diffs, 2.5)), 2),
                      round(float(np.percentile(diffs, 97.5)), 2)],
        "rel": round(float((point_a - point_b) / point_b), 4),
        "rel_ci95": [round(float(np.nanpercentile(rels, 2.5)), 4),
                     round(float(np.nanpercentile(rels, 97.5)), 4)],
        "blocks": n,
    }


# ======================================================================================
# Test evaluation
# ======================================================================================
def evaluate_test(runs: list, params: SelectorParams, use_replay: bool, log=print) -> dict:
    # ONE external objective for every number that is compared against another number
    # (C-96). The selector's own internal weighting is a parameter of the selector, used
    # only to decide what it serves.
    w = eval_weights()
    w_internal = HORIZON_WEIGHTS[params.weights]
    arms = list(CANDIDATES) + [CONTROL_ARM, "selector"]
    per_run, blocks = {}, {}

    for r in runs:
        add_blend(r, params.blend_weight)
        costs = per_origin_costs(r, w_internal, CANDIDATES)
        sel = run_selector(costs, params)
        attach_selector_arm(r, sel["served"])
        all_costs = per_origin_costs(r, w, arms)

        # Blocks for the bootstrap: one per (run, calendar day of the origin).
        day_of = np.array([r.series.index[i].date().isoformat() for i in r.origins])
        for day in sorted(set(day_of)):
            mask = day_of == day
            blocks[(r.key, day)] = {a: all_costs[a][mask] for a in arms}

        entry = {"scenario": r.scenario, "seed": r.seed,
                 "start": r.start.isoformat(), "origins": len(r.origins),
                 "target_rpm_per_replica": round(r.target_rpm, 1),
                 "switches": sel["switches"], "switch_fraction": sel["switch_fraction"],
                 "served_share": {a: round(sel["served"].count(a) / len(sel["served"]), 3)
                                  for a in CANDIDATES},
                 "arms": {}}
        for a in arms:
            mae = float(np.mean(np.abs(r.forecasts[a] - r.truth)))
            entry["arms"][a] = {
                "cost_proxy_mean": round(float(all_costs[a].mean()), 2),
                "mae": round(mae, 1),
                "degraded_origins": int(r.degraded.get(a, 0)),
                "forecast_failures": int(r.failures.get(a, 0)),
            }

        if use_replay:
            actual = r.series.iloc[r.origins[0]:r.origins[-1] + 1 + STEPS_AHEAD]
            for a in arms:
                fmap = {r.series.index[i]: r.forecasts[a][k] for k, i in enumerate(r.origins)}
                t0 = time.time()
                rep = oe.replay_controller(actual, fmap, target_rpm=r.target_rpm,
                                           min_r=MIN_REPLICAS, max_r=MAX_REPLICAS)
                entry["arms"][a].update({
                    "replica_minutes": rep["replica_minutes"],
                    "scaling_events": rep["scaling_events"],
                    "shortage_minutes": rep["deficit_minutes"],
                    "shortage_replicas_mean": rep["deficit_replicas_mean"],
                    "shortage_replicas_max": rep["deficit_replicas_max"],
                    "minutes_demand_exceeded_ceiling": rep["minutes_demand_exceeded_ceiling"],
                    "decisions_from": rep["decisions_from"],
                })
                log(f"  replay {r.key:24s} {a:18s} "
                    f"{rep['replica_minutes']:9.1f} rm  "
                    f"{rep['deficit_minutes']:7.1f} short  "
                    f"{rep['scaling_events']:4d} ev  ({time.time()-t0:.1f}s, "
                    f"{rep['decisions_from']})")
        per_run[r.key] = entry
        log(f"{r.key:24s} " + "  ".join(
            f"{a}={entry['arms'][a]['cost_proxy_mean']:.0f}" for a in arms))

    pooled = {}
    for a in arms:
        vals = np.concatenate([blocks[k][a] for k in sorted(blocks)])
        pooled[a] = {"cost_proxy_mean": round(float(vals.mean()), 2),
                     "mae": round(float(np.mean([
                         per_run[r.key]["arms"][a]["mae"] for r in runs])), 1)}
        if use_replay:
            for m in ("replica_minutes", "scaling_events", "shortage_minutes",
                      "minutes_demand_exceeded_ceiling"):
                pooled[a][m] = round(sum(per_run[r.key]["arms"][a][m] for r in runs), 1)

    fixed_arms = list(CANDIDATES) + [CONTROL_ARM]
    best_fixed = min(fixed_arms, key=lambda a: pooled[a]["cost_proxy_mean"])
    comparisons = {a: block_bootstrap(blocks, "selector", a) for a in fixed_arms}

    # Per-scenario diagnostics, each with its own block bootstrap. These are DIAGNOSTICS:
    # the pooled comparison above is the one the bar is read from, because a per-scenario
    # winner is not something a single deployed configuration could serve.
    per_scenario = {}
    for sc in SCENARIOS:
        sc_blocks = {k: v for k, v in blocks.items() if k[0].startswith(sc + "/")}
        means = {a: float(np.concatenate([sc_blocks[k][a] for k in sorted(sc_blocks)]).mean())
                 for a in arms}
        local_best = min(fixed_arms, key=lambda a: means[a])
        per_scenario[sc] = {
            "arm_cost_proxy_mean": {a: round(v, 2) for a, v in means.items()},
            "best_fixed_here": local_best,
            "selector_vs_best_fixed_here": block_bootstrap(sc_blocks, "selector", local_best),
            "selector_vs_pooled_best_fixed": block_bootstrap(sc_blocks, "selector", best_fixed),
        }

    rep_blocks = {k: v for k, v in blocks.items() if k[0].startswith("repeating/")}
    rep_pooled = {a: float(np.concatenate([rep_blocks[k][a] for k in sorted(rep_blocks)]).mean())
                  for a in arms}
    best_fixed_rep = min(fixed_arms, key=lambda a: rep_pooled[a])
    rep_cmp = block_bootstrap(rep_blocks, "selector", best_fixed_rep)

    switch_fraction = float(np.mean([per_run[r.key]["switch_fraction"] for r in runs]))

    # ---- Predeclared bar, evaluated ----------------------------------------------------
    s1_rel = comparisons[best_fixed]["rel"]
    s1 = (s1_rel <= -BAR_POOLED_IMPROVEMENT) and (comparisons[best_fixed]["diff_ci95"][1] < 0.0)
    s2 = (rep_cmp["rel"] <= BAR_REPEATING_TOLERANCE) and \
         (rep_cmp["rel_ci95"][1] <= BAR_REPEATING_CI_UPPER)
    s3_short = True
    if use_replay:
        s3_short = pooled["selector"]["shortage_minutes"] <= \
            pooled[best_fixed]["shortage_minutes"] * (1.0 + BAR_SHORTAGE_TOLERANCE)
    # The selector's failure count is now INHERITED from the arms it served (C-97), so this
    # gate can actually fire. It asks: did serving the selector expose more repaired
    # forecasts than serving the worst fixed arm would have?
    s3_fail = all(per_run[r.key]["arms"]["selector"]["forecast_failures"] <=
                  max(per_run[r.key]["arms"][a]["forecast_failures"] for a in fixed_arms)
                  for r in runs)
    s3_switch = switch_fraction <= BAR_SWITCH_FRACTION
    s3 = s3_short and s3_fail and s3_switch

    # Diagnostics only -- a hindsight per-scenario winner is NOT a baseline anyone could serve.
    hindsight = {}
    for r in runs:
        hindsight[r.key] = min(fixed_arms,
                               key=lambda a: per_run[r.key]["arms"][a]["cost_proxy_mean"])

    return {
        "external_objective": EVAL_OBJECTIVE,
        "objective_note": ("every cost proxy, bootstrap and bar reading below uses the single "
                           f"external objective '{EVAL_OBJECTIVE}'; "
                           f"preselected_parameters.horizon_weights ('{params.weights}') is "
                           "the selector's INTERNAL ranking weight only"),
        "per_run": per_run,
        "pooled": pooled,
        "best_fixed_arm_pooled": best_fixed,
        "comparisons_vs_selector": comparisons,
        "repeating_noninferiority": {"comparator": best_fixed_rep, **rep_cmp},
        "switch_fraction_mean": round(switch_fraction, 4),
        "per_scenario_DIAGNOSTIC": per_scenario,
        "bar": {
            "S1_pooled_cost_improvement": {
                "required": f"<= -{BAR_POOLED_IMPROVEMENT:.0%} vs best fixed arm and CI upper < 0",
                "observed_rel": s1_rel,
                "observed_ci95": comparisons[best_fixed]["diff_ci95"],
                "pass": bool(s1)},
            "S2_repeating_noninferiority": {
                "required": f"<= +{BAR_REPEATING_TOLERANCE:.0%} and CI upper <= "
                            f"+{BAR_REPEATING_CI_UPPER:.0%}",
                "observed_rel": rep_cmp["rel"],
                "observed_rel_ci95": rep_cmp["rel_ci95"],
                "pass": bool(s2)},
            "S3_operational": {
                "required": f"shortage <= +{BAR_SHORTAGE_TOLERANCE:.0%}, no new failures, "
                            f"switching <= {BAR_SWITCH_FRACTION:.0%} of origins",
                "shortage_pass": bool(s3_short), "failures_pass": bool(s3_fail),
                "switching_pass": bool(s3_switch), "pass": bool(s3)},
        },
        "verdict": "ADOPT" if (s1 and s2 and s3) else "KEEP THE FIXED BASELINE",
        "hindsight_per_run_winner_DIAGNOSTIC_ONLY": hindsight,
    }


# ======================================================================================
def _assert_disjoint():
    assert not (set(VALIDATION_SEEDS) & set(TEST_SEEDS)), "validation and test seeds overlap"
    v_end = VALIDATION_START + pd.Timedelta(days=DAYS)
    assert TEST_START >= v_end, "validation and test calendar windows overlap"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=str(HERE / "selector.json"))
    ap.add_argument("--no-replay", action="store_true",
                    help="skip the controller replay (accuracy tables only)")
    ap.add_argument("--validation-only", action="store_true",
                    help="re-derive the preselected parameters on the validation seeds and "
                         "STOP. Use this after any change to the objective or the selection "
                         "rule: the test seeds are consumed evidence (D-124) and re-scoring "
                         "them after a repair would be a second look at a used test set.")
    args = ap.parse_args()
    _assert_disjoint()

    helper = LSTMForecastModel(sequence_length=oe.SEQ)   # used only for _pattern_forecast
    t0 = time.time()

    print("=== validation: building runs "
          f"(seeds {VALIDATION_SEEDS}, from {VALIDATION_START.date()}) ===")
    val = [build_run(s, seed, VALIDATION_START, helper)
           for s in SCENARIOS for seed in VALIDATION_SEEDS]
    print(f"    {len(val)} runs, {sum(len(r.origins) for r in val)} origins "
          f"({time.time()-t0:.0f}s)")

    print("=== validation: parameter preselection ===")
    params, sweep = preselect(val)

    if args.validation_only:
        payload = {
            "generated_utc": datetime.utcnow().isoformat() + "Z",
            "experiment": "offline selector -- VALIDATION PRESELECTION ONLY; no test run",
            "external_evaluation_objective": EVAL_OBJECTIVE,
            "why_no_test": "test seeds %s are consumed evidence (Codex D-124); a repaired "
                           "selection rule must be evaluated on new held-out seeds, dates "
                           "and regime shapes, frozen before the run"
                           % (list(TEST_SEEDS),),
            "preselected_parameters": params.as_dict(),
            "validation_sweep": sweep,
        }
        Path(args.out).write_text(json.dumps(payload, indent=2))
        print(f"\nvalidation-only: wrote {args.out}  ({time.time()-t0:.0f}s)")
        return

    print(f"=== test: building runs (seeds {TEST_SEEDS}, from {TEST_START.date()}) "
          "-- untouched by preselection ===")
    test = [build_run(s, seed, TEST_START, helper)
            for s in SCENARIOS for seed in TEST_SEEDS]
    print(f"    {len(test)} runs, {sum(len(r.origins) for r in test)} origins")

    print("=== test: evaluation ===")
    result = evaluate_test(test, params, use_replay=not args.no_replay)

    payload = {
        "generated_utc": datetime.utcnow().isoformat() + "Z",
        "experiment": "offline online-selector experiment (D-116); nothing deployed",
        "cost_proxy": {
            "definition": "weighted asymmetric ABSOLUTE error, under:over = "
                          f"{COST_UNDER:g}:{COST_OVER:g}, units rpm",
            "is_not": "operating cost -- no start-up delay, no integer replica step, "
                      "no stabilisation, no dropped request, no dollars",
        },
        "design": {
            "external_evaluation_objective": {
                "horizon_weights": EVAL_OBJECTIVE,
                "fixed_before": "any comparison; used for the sweep ranking, the control "
                                "blend's weight, the test tables, the bootstrap and the bar",
                "rationale": "evaluation-protocol.md section 9 (D-86) preregisters uniform "
                             "horizon weights with per-step MAE reported in full",
            },
            "scenarios": list(SCENARIOS), "days": DAYS, "origins_per_run": MAX_ORIGINS,
            "validation_seeds": list(VALIDATION_SEEDS),
            "validation_start": VALIDATION_START.isoformat(),
            "test_seeds": list(TEST_SEEDS), "test_start": TEST_START.isoformat(),
            "candidates": list(CANDIDATES), "control_arm": CONTROL_ARM,
            "maturation_lag_origins": STEPS_AHEAD,
            "lagged_linear_lags": list(LINEAR_LAGS),
            "lag_1008_omitted_because": "1008 steps = 7 days = the whole training window",
            "bootstrap": {"blocks": "whole calendar days within a run",
                          "draws": BOOTSTRAP_DRAWS, "seed": BOOTSTRAP_SEED},
        },
        "preselected_parameters": params.as_dict(),
        "validation_sweep_top12": sweep,
        "test": result,
    }
    Path(args.out).write_text(json.dumps(payload, indent=2))
    print(f"\nverdict: {result['verdict']}")
    print(f"written {args.out}  ({time.time()-t0:.0f}s)")


if __name__ == "__main__":
    main()
