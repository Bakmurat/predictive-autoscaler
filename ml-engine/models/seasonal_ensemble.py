"""Seasonal ensemble forecaster ("candidate A") with an empirical q90 capacity margin.

The forecast for each of six ten-minute steps is the equal-weight average of two structurally
different seasonal models, both refitted on absolute UTC boundaries every six hours:

* A: additive Holt-Winters (additive trend, additive daily season of 144 ten-minute slots),
     fitted by minimising the one-step squared error over the smoothing parameters, with the
     heuristic initial states of Hyndman & Athanasopoulos (fpp3 section 8.6; the same rule as
     statsmodels' `_initialization_heuristic`). Between refits the state advances with every
     observation.
* B: the mean of the same slot on the previous seven days (observed values only) plus an AR(3)
     model of the residual, whose coefficients are refitted on the trailing seven days.

The served value adds a margin: the 0.90 quantile of this forecaster's own lead-window errors
over the trailing 24 hours of matured decision ticks (at least 30 samples, else zero), clipped
to [0, 0.8 x lead-window forecast]. Over-provisioning is preferred to under-provisioning (user
decision, 2026-09-27).

Design choice (Task 03 D-1053): the forecaster is stateless per request. Every generation is a
deterministic function of the source observations up to its boundary and is cached by boundary
and input fingerprint; the state between refits and the margin's past forecasts are recomputed
from the same observations. A restart therefore reproduces the same forecasts, with no
checkpoint store to corrupt.

Units are whatever the input series carries (requests per minute in serving). Missing slots are
NaN: they never become margin targets, and model inputs carry the last observation forward.
"""

import hashlib
import math
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import minimize

SLOT_SECONDS = 600
SEASON = 144                   # ten-minute slots per day
STEPS = 6                      # +10 ... +60 minutes
REFIT_SECONDS = 6 * 3600       # refit on 00:00 / 06:00 / 12:00 / 18:00 UTC
HW_WINDOW_SLOTS = 14 * SEASON  # trailing 14 days (= the expanding window for a young benchmark)
PROFILE_DAYS = 7
AR_ORDER = 3
AR_WINDOW_SLOTS = 7 * SEASON
MIN_AR_ROWS = 36
HW_WEIGHT = 0.5
MARGIN_QUANTILE = 0.90
MARGIN_WINDOW_SLOTS = SEASON   # trailing 24 hours of decision ticks
MARGIN_MIN_SAMPLES = 30
MARGIN_CLIP_FRACTION = 0.8
MAX_ORIGIN_AGE_SECONDS = 20 * 60
STALE_GENERATIONS = 1          # an invalid refit may fall back to the previous boundary once
HW_STARTS = ((0.3, 0.1, 0.1), (0.1, 0.01, 0.05), (0.6, 0.05, 0.3))
VERSION = "seasonal-ensemble-1.2.0"   # 1.2.0: per-experiment margin_mode (absolute|relative) and partial_rule (refuse|finite); defaults unchanged
MARGIN_MODES = ("absolute", "relative")
PARTIAL_RULES = ("refuse", "finite")

SETTINGS = {
    "version": VERSION, "slot_seconds": SLOT_SECONDS, "season": SEASON, "steps": STEPS,
    "refit_seconds": REFIT_SECONDS, "hw_window_slots": HW_WINDOW_SLOTS, "profile_days": PROFILE_DAYS,
    "ar_order": AR_ORDER, "ar_window_slots": AR_WINDOW_SLOTS, "min_ar_rows": MIN_AR_ROWS,
    "hw_weight": HW_WEIGHT, "margin_quantile": MARGIN_QUANTILE, "margin_window_slots": MARGIN_WINDOW_SLOTS,
    "margin_min_samples": MARGIN_MIN_SAMPLES, "margin_clip_fraction": MARGIN_CLIP_FRACTION,
    "max_origin_age_seconds": MAX_ORIGIN_AGE_SECONDS, "stale_generations": STALE_GENERATIONS,
}


class ForecastUnavailable(Exception):
    """No forecast can be served; the caller must refuse (the operator then scales reactively)."""


# ----------------------------------------------------------------------------------------------
# Input grid
# ----------------------------------------------------------------------------------------------

@dataclass
class Grid:
    """Observations on the absolute ten-minute UTC grid; `y` is NaN where nothing was observed."""
    t0: int
    y: np.ndarray
    off_grid_dropped: int = 0

    @classmethod
    def from_points(cls, points: Iterable[Tuple[int, float]]) -> "Grid":
        values: Dict[int, float] = {}
        off_grid = 0
        for t, v in points:
            t = int(t)
            if t % SLOT_SECONDS != 0:
                off_grid += 1
                continue
            v = float(v)
            if not math.isfinite(v):
                continue
            if t in values and values[t] != v:
                raise ValueError(f"conflicting duplicate observation at {t}")
            values[t] = v
        if not values:
            raise ForecastUnavailable("no on-grid observations")
        t0, t1 = min(values), max(values)
        y = np.full((t1 - t0) // SLOT_SECONDS + 1, np.nan)
        for t, v in values.items():
            y[(t - t0) // SLOT_SECONDS] = v
        return cls(t0=t0, y=y, off_grid_dropped=off_grid)

    def ts(self, i: int) -> int:
        return self.t0 + i * SLOT_SECONDS

    def index(self, t: int) -> int:
        return (t - self.t0) // SLOT_SECONDS

    def filled(self) -> np.ndarray:
        """Forward-filled observations (leading NaN stay NaN)."""
        return pd.Series(self.y).ffill().to_numpy()


# ----------------------------------------------------------------------------------------------
# Component A: Holt-Winters
# ----------------------------------------------------------------------------------------------

def heuristic_init(y: np.ndarray, m: int = SEASON) -> Tuple[float, float, np.ndarray]:
    """Initial level, trend and seasonal states (Hyndman & Athanasopoulos, fpp3 section 8.6).

    Same rule as statsmodels' `_initialization_heuristic` for additive trend and season: a
    centred 2 x m moving average over the first k cycles gives the trend; the average detrended
    value per season position (normalised to sum to zero) gives the seasonal states; a straight
    line through the first ten trend values gives the level and slope.
    """
    n = len(y)
    if n < 2 * m:
        raise ValueError("Holt-Winters needs at least two full seasonal cycles")
    min_obs = 10 + 2 * (m // 2)
    k = max(min(5, n // m), int(np.ceil(min_obs / m)))
    series = pd.Series(np.asarray(y[: m * k], dtype=float))
    trend = series.rolling(m, center=True).mean()
    if m % 2 == 0:
        trend = trend.shift(-1).rolling(2).mean()
    detrended = (series - trend).to_numpy()
    tmp = np.full(k * m, np.nan)
    tmp[: len(detrended)] = detrended
    seasonal = np.nanmean(tmp.reshape(k, m).T, axis=1)
    seasonal = seasonal - np.mean(seasonal)
    level_input = trend.dropna().to_numpy()[:10]
    exog = np.c_[np.ones(10), np.arange(10) + 1]
    coef = np.linalg.pinv(exog).dot(level_input)
    return float(coef[0]), float(coef[1]), seasonal.astype(float)


def hw_run(y: Sequence[float], alpha: float, beta: float, gamma: float,
           level: float, trend: float, season: Sequence[float], collect: bool = False):
    """Advance the additive Holt-Winters recursions (Hyndman form) through `y`.

    `season[0]` is the seasonal state that applies to the next observation. Returns
    (sse, level, trend, season_rolled, fitted), where `season_rolled[0]` applies to the
    observation after the last one consumed and `fitted` (when `collect`) holds the one-step
    forecast that preceded each observation.
    """
    s = [float(v) for v in season]
    m = len(s)
    ac, bc, gc = 1.0 - alpha, 1.0 - beta, 1.0 - gamma
    l, b = float(level), float(trend)
    sse = 0.0
    fitted: Optional[List[float]] = [] if collect else None
    pos = 0
    for obs in y:
        sp = s[pos]
        pred = l + b + sp
        err = obs - pred
        sse += err * err
        if fitted is not None:
            fitted.append(pred)
        l_new = alpha * (obs - sp) + ac * (l + b)
        b_new = beta * (l_new - l) + bc * b
        s[pos] = gamma * (obs - l - b) + gc * sp
        l, b = l_new, b_new
        pos += 1
        if pos == m:
            pos = 0
    rolled = s[pos:] + s[:pos]
    return sse, l, b, rolled, fitted


@dataclass
class HWFit:
    alpha: float
    beta: float
    gamma: float
    level0: float
    trend0: float
    season0: List[float]
    level: float          # state after the last training observation
    trend: float
    season: List[float]   # season[0] applies to the observation after the boundary
    sse: float
    n_train: int
    nit: int
    message: str


def _params(x) -> Tuple[float, float, float]:
    """Admissible smoothing parameters: 0 <= beta <= alpha, 0 <= gamma <= 1 - alpha."""
    a = float(np.clip(x[0], 0.0, 1.0))
    return a, float(np.clip(x[1], 0.0, 1.0)) * a, float(np.clip(x[2], 0.0, 1.0)) * (1.0 - a)


def fit_hw(y: np.ndarray, m: int = SEASON) -> HWFit:
    """Least-squares Holt-Winters fit (L-BFGS-B from several starts; the best converged fit)."""
    y = np.asarray(y, dtype=float)
    if not np.all(np.isfinite(y)):
        raise ValueError("Holt-Winters training input must be finite (forward-filled)")
    level0, trend0, season0 = heuristic_init(y, m)
    obs = y.tolist()
    best = None
    for start in HW_STARTS:
        a, b, g = start
        x0 = (a, b / a if a > 0 else 0.0, g / (1.0 - a))

        def objective(x):
            al, be, ga = _params(x)
            return hw_run(obs, al, be, ga, level0, trend0, season0)[0]

        res = minimize(objective, x0=np.array(x0), method="L-BFGS-B",
                       bounds=[(0.0, 1.0)] * 3, options={"maxiter": 200})
        if not res.success or not np.isfinite(res.fun):
            continue
        if best is None or res.fun < best.fun:
            best = res
    if best is None:
        raise ValueError("Holt-Winters optimiser did not converge from any start")
    al, be, ga = _params(best.x)
    sse, level, trend, season, _ = hw_run(obs, al, be, ga, level0, trend0, season0)
    return HWFit(alpha=al, beta=be, gamma=ga, level0=level0, trend0=trend0,
                 season0=list(season0), level=level, trend=trend, season=season, sse=float(sse),
                 n_train=len(obs), nit=int(best.nit), message=str(best.message))


# ----------------------------------------------------------------------------------------------
# Component B: seven-day profile + AR(3) residual
# ----------------------------------------------------------------------------------------------

def profile_at(y: np.ndarray, i: int) -> float:
    """Mean of the observed values at the same slot on the previous seven days (NaN if none)."""
    vals = [y[i - SEASON * d] for d in range(1, PROFILE_DAYS + 1) if 0 <= i - SEASON * d < len(y)]
    vals = [v for v in vals if np.isfinite(v)]
    return float(np.mean(vals)) if vals else float("nan")


def residuals(y: np.ndarray, yf: np.ndarray, upto: int) -> np.ndarray:
    """r[t] = yf[t] - profile[t] for t <= upto (NaN where either is missing)."""
    r = np.full(upto + 1, np.nan)
    for t in range(upto + 1):
        p = profile_at(y, t)
        if np.isfinite(p) and np.isfinite(yf[t]):
            r[t] = yf[t] - p
    return r


def fit_ar(r: np.ndarray, boundary: int) -> Tuple[np.ndarray, int]:
    """OLS AR(3) with intercept on the trailing seven days of residuals ending at `boundary`."""
    first = max(AR_ORDER, boundary - AR_WINDOW_SLOTS + 1 + AR_ORDER)
    rows, targets = [], []
    for t in range(first, boundary + 1):
        lags = r[t - AR_ORDER:t][::-1]
        if np.isfinite(r[t]) and np.all(np.isfinite(lags)):
            rows.append([1.0, *lags])
            targets.append(r[t])
    if len(rows) < MIN_AR_ROWS:
        raise ValueError(f"AR fit needs {MIN_AR_ROWS} complete rows, got {len(rows)}")
    coef, *_ = np.linalg.lstsq(np.array(rows), np.array(targets), rcond=None)
    if not np.all(np.isfinite(coef)):
        raise ValueError("AR coefficients not finite")
    return coef, len(rows)


# ----------------------------------------------------------------------------------------------
# Generations and forecasts
# ----------------------------------------------------------------------------------------------

@dataclass
class Generation:
    boundary_ts: int
    hw: HWFit
    ar_coef: np.ndarray
    ar_rows: int
    fingerprint: str
    hw_window_start_ts: int

    def summary(self) -> Dict:
        return {"boundary": _iso(self.boundary_ts), "fingerprint": self.fingerprint,
                "hw": {"alpha": self.hw.alpha, "beta": self.hw.beta, "gamma": self.hw.gamma,
                       "sse": self.hw.sse, "n_train": self.hw.n_train, "nit": self.hw.nit,
                       "window_start": _iso(self.hw_window_start_ts)},
                "ar": {"coef": [float(c) for c in self.ar_coef], "rows": self.ar_rows}}


_CACHE: "OrderedDict[Tuple, Generation]" = OrderedDict()
_CACHE_LOCK = threading.Lock()
_CACHE_SIZE = 32


def _iso(ts: int) -> str:
    return pd.Timestamp(ts, unit="s", tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _boundary_of(ts: int) -> int:
    return ts - ts % REFIT_SECONDS


def generation_for(grid: Grid, boundary_ts: int, cache_key: str = "") -> Generation:
    """Fit (or fetch from cache) the generation that serves [boundary, boundary + 6 h)."""
    b = grid.index(boundary_ts)
    if b < 0 or b >= len(grid.y):
        raise ValueError("boundary outside the observed series")
    y, yf = grid.y, grid.filled()
    lo = max(0, b - HW_WINDOW_SLOTS + 1)
    train = yf[lo:b + 1]
    finite = np.flatnonzero(np.isfinite(train))
    if finite.size == 0:
        raise ValueError("no observations before the boundary")
    train = train[finite[0]:]
    # The fingerprint binds everything the fit reads: observed values of the HW window and of the
    # profile/AR history (7 profile days before the AR window).
    ar_lo = max(0, b - AR_WINDOW_SLOTS - PROFILE_DAYS * SEASON)
    h = hashlib.sha256()
    h.update(f"{VERSION}|{boundary_ts}|{grid.ts(lo)}|{grid.ts(ar_lo)}".encode())
    h.update(np.ascontiguousarray(y[min(lo, ar_lo):b + 1]).tobytes())
    fingerprint = h.hexdigest()
    key = (cache_key, boundary_ts, fingerprint)
    with _CACHE_LOCK:
        if key in _CACHE:
            _CACHE.move_to_end(key)
            return _CACHE[key]
    hw = fit_hw(train)
    r = residuals(y, yf, b)
    coef, rows = fit_ar(r, b)
    gen = Generation(boundary_ts=boundary_ts, hw=hw, ar_coef=coef, ar_rows=rows,
                     fingerprint=fingerprint, hw_window_start_ts=grid.ts(lo + finite[0]))
    with _CACHE_LOCK:
        _CACHE[key] = gen
        while len(_CACHE) > _CACHE_SIZE:
            _CACHE.popitem(last=False)
    return gen


def _usable_generation(grid: Grid, origin: int, cache_key: str) -> Tuple[Generation, bool]:
    boundary = _boundary_of(grid.ts(origin))
    errors = []
    for back in range(STALE_GENERATIONS + 1):
        try:
            return generation_for(grid, boundary - back * REFIT_SECONDS, cache_key), back > 0
        except ValueError as e:
            errors.append(f"{_iso(boundary - back * REFIT_SECONDS)}: {e}")
    raise ForecastUnavailable("no valid generation: " + "; ".join(errors))


def components_at(grid: Grid, origin: int, cache_key: str = "") -> Dict:
    """Both component forecasts and their average for the six steps after `origin`."""
    gen, stale = _usable_generation(grid, origin, cache_key)
    y, yf = grid.y, grid.filled()
    b = grid.index(gen.boundary_ts)
    # A: advance the boundary state through the observations after the boundary.
    _, level, trend, season, _ = hw_run(yf[b + 1:origin + 1].tolist(), gen.hw.alpha, gen.hw.beta,
                                        gen.hw.gamma, gen.hw.level, gen.hw.trend, gen.hw.season)
    hw = [level + h * trend + season[(h - 1) % SEASON] for h in range(1, STEPS + 1)]
    # B: profile of each target slot plus the AR recursion from the three latest residuals.
    hist = []
    for t in (origin, origin - 1, origin - 2):
        p = profile_at(y, t)
        hist.append(yf[t] - p if (t >= 0 and np.isfinite(p) and np.isfinite(yf[t])) else float("nan"))
    c = gen.ar_coef
    prof_ar = []
    for s in range(1, STEPS + 1):
        nxt = c[0] + c[1] * hist[0] + c[2] * hist[1] + c[3] * hist[2]
        prof_ar.append(profile_at(y, origin + s) + nxt)
        hist = [nxt, hist[0], hist[1]]
    raw, raw_finite = [], []
    for a, bb in zip(hw, prof_ar):
        if np.isfinite(a) and np.isfinite(bb):
            raw.append(max(0.0, HW_WEIGHT * a + (1.0 - HW_WEIGHT) * bb))
            raw_finite.append(raw[-1])
        else:
            raw.append(float("nan"))
            # partial_rule "finite": serve the finite component alone; NaN only when both are missing
            finite = [v for v in (a, bb) if np.isfinite(v)]
            raw_finite.append(max(0.0, float(np.mean(finite))) if finite else float("nan"))
    return {"hw": [float(v) for v in hw], "profile_ar": [float(v) for v in prof_ar],
            "raw": raw, "raw_finite": raw_finite, "generation": gen, "stale_generation": stale}


def margin_at(grid: Grid, origin: int, lead: float, cache_key: str = "",
              quantile: float = MARGIN_QUANTILE, mode: str = "absolute",
              raw_key: str = "raw") -> Tuple[float, int]:
    """`quantile` (default q90) of past lead-window errors over the trailing 24 h of matured ticks.

    mode "absolute" (the 1.0.0 rule): samples are max(a10, a20) - lead_j in rpm; the margin is the
    quantile clipped to [0, 0.8 x lead]. mode "relative" (model lab, 2026-09-28): samples are
    max(a10, a20) / lead_j - 1; the margin is lead x clip(quantile, 0, 0.8), so it scales with the
    load instead of carrying daytime errors into the night. `raw_key` selects which served series the
    errors are measured against ("raw" or "raw_finite", matching the experiment's partial rule)."""
    if not (0.0 < quantile < 1.0):
        raise ValueError(f"margin quantile must lie in (0, 1), got {quantile}")
    if mode not in MARGIN_MODES:
        raise ValueError(f"margin mode must be one of {MARGIN_MODES}, got {mode!r}")
    y = grid.y
    samples = []
    for j in range(max(0, origin - MARGIN_WINDOW_SLOTS + 1), origin - 1):
        a10, a20 = y[j + 1], y[j + 2]
        if not (np.isfinite(a10) and np.isfinite(a20)) or not np.isfinite(y[j]):
            continue
        try:
            raw = components_at(grid, j, cache_key)[raw_key]
        except ForecastUnavailable:
            continue
        if not (np.isfinite(raw[0]) and np.isfinite(raw[1])):
            continue
        lead_j = max(raw[0], raw[1])
        if mode == "relative":
            if lead_j <= 0:
                continue
            samples.append(max(a10, a20) / lead_j - 1.0)
        else:
            samples.append(max(a10, a20) - lead_j)
    if len(samples) < MARGIN_MIN_SAMPLES:
        return 0.0, len(samples)
    q = float(np.quantile(samples, quantile))
    if mode == "relative":
        return float(lead * np.clip(q, 0.0, MARGIN_CLIP_FRACTION)), len(samples)
    return float(np.clip(q, 0.0, MARGIN_CLIP_FRACTION * lead)), len(samples)


def forecast(points: Iterable[Tuple[int, float]], now_ts: int, cache_key: str = "",
             margin_quantile: float = MARGIN_QUANTILE, margin_mode: str = "absolute",
             partial_rule: str = "refuse") -> Dict:
    """Serve one forecast from on-grid (epoch seconds, value) observations.

    `margin_quantile` selects the error quantile added as capacity margin (q90 by default; an
    experiment may declare another); `margin_mode` "absolute" (rpm) or "relative" (fraction of the
    lead); `partial_rule` "refuse" (a step with a missing component is refused, the 1.0.0 rule) or
    "finite" (the finite component is served alone). Raises ForecastUnavailable when the input is
    stale, a generation cannot be fitted, or any served step is not finite.
    """
    if partial_rule not in PARTIAL_RULES:
        raise ValueError(f"partial rule must be one of {PARTIAL_RULES}, got {partial_rule!r}")
    grid = Grid.from_points(points)
    observed = np.flatnonzero(np.isfinite(grid.y))
    origin = int(observed[-1])
    origin_ts = grid.ts(origin)
    if now_ts - origin_ts > MAX_ORIGIN_AGE_SECONDS:
        raise ForecastUnavailable(f"latest observation {_iso(origin_ts)} is older than "
                                  f"{MAX_ORIGIN_AGE_SECONDS // 60} minutes")
    comp = components_at(grid, origin, cache_key)
    raw_key = "raw_finite" if partial_rule == "finite" else "raw"
    raw = comp[raw_key]
    if not all(np.isfinite(v) for v in raw):
        raise ForecastUnavailable("ensemble step(s) not finite: hw=%s profile_ar=%s"
                                  % (comp["hw"], comp["profile_ar"]))
    lead = max(raw[0], raw[1])
    margin, n = margin_at(grid, origin, lead, cache_key, margin_quantile, margin_mode, raw_key)
    gen: Generation = comp["generation"]
    return {
        "origin": _iso(origin_ts),
        "target_timestamps": [_iso(origin_ts + s * SLOT_SECONDS) for s in range(1, STEPS + 1)],
        "hw": comp["hw"], "profile_ar": comp["profile_ar"], "raw": raw,
        "margin": margin, "margin_samples": n, "margin_quantile": margin_quantile,
        "margin_mode": margin_mode, "partial_rule": partial_rule,
        "served_components": sum(1 for a, bb in zip(comp["hw"], comp["profile_ar"]) if np.isfinite(a) and np.isfinite(bb)),
        "served": [v + margin for v in raw],
        "generation": gen.summary(), "stale_generation": comp["stale_generation"],
        "off_grid_dropped": grid.off_grid_dropped,
        "settings": {**SETTINGS, "margin_quantile": margin_quantile, "margin_mode": margin_mode,
                     "partial_rule": partial_rule},
    }
