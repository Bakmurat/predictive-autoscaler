"""Pure statistical kernels for forecast contract v2: daily seasonal-naive and E1-product (profile+AR with Holt-Winters).

Deterministic functions of a Series (one signal's RPS on its grid, NaN where nothing was observed), generalized to any
admitted cadence Δ: a season has m = 86400/Δ slots, a lag of one day is m slots. No pandas, no TensorFlow; NumPy and
SciPy only. Nothing here reads a clock, a file or the network.

Daily seasonal-naive (recipe seasonal-naive/daily/v1): x(s - 1 day) for every target s. It needs every lagged slot,
observed and no later than the input end; otherwise it is unavailable (reason history). No fit, so no artifact.

E1-product (recipe e1-product/v1), the benchmark's E1 ensemble (models/seasonal_ensemble.py) generalized, with the
product's stricter input rules (design C2, section 4):
  - fit at a cutoff c (a grid time at or before the input end) from at least 7 days of history, with at least 95% of the
    slots of the trailing 7-day window observed;
  - Holt-Winters (additive trend and daily season, least squares from E1's three starts, E1's heuristic initial
    states) on the longest COMPLETE contiguous segment ending at c, at most 14 days and at least 2 days. Nothing is
    forward-filled: a gap shortens the segment, and a segment under 2 days makes E1-product unavailable;
  - profile(t): the mean of the observed values at t - d days for d = 1..7, only when at least 3 of them were observed
    (causal: a profile uses only earlier days, so a residual never sees its own slot);
  - AR(3) with intercept on the residuals r = y - profile over the trailing 7 days ending at c (complete rows only),
    with at least 36 rows spanning at least 6 hours; the fitted recursion must be stable (every root of the AR
    polynomial strictly inside AR_MAX_ROOT), otherwise the fit is refused (reason numeric), never repaired.
  - forecast from the input end e (the newest finalized observation), not from the decision origin: Holt-Winters
    advances its state through every observation in (c, e] (all must be present) and steps h = (s - e)/Δ ahead; the
    AR recursion starts from the residuals at e, e-Δ, e-2Δ (all must be present) and the profile of each target needs
    its support. The point forecast is max(0, 0.5 HW + 0.5 profile/AR). A missing component makes E1-product
    unavailable for that origin; there is no silent partial substitution (the caller falls to seasonal-naive, by
    name).
With complete data on the 600 s grid these rules reproduce E1's component forecasts (tests compare them numerically).
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
from scipy.optimize import minimize

DAY = 86400
RECIPE_NAIVE = "seasonal-naive/daily/v1"
RECIPE_E1 = "e1-product/v1"
FAILURE_REASONS = ("history", "numeric", "timeout", "resource", "storage")
HW_STARTS = ((0.3, 0.1, 0.1), (0.1, 0.01, 0.05), (0.6, 0.05, 0.3))      # E1's starts
AR_MAX_ROOT = 0.999


class KernelUnavailable(Exception):
    """A kernel cannot produce a fit or a forecast; `reason` is one of FAILURE_REASONS (a bounded metric label)."""

    def __init__(self, reason: str, message: str):
        if reason not in FAILURE_REASONS:
            raise ValueError(f"unknown failure reason {reason!r}")
        super().__init__(f"{reason}: {message}")
        self.reason, self.message = reason, message


@dataclass(frozen=True)
class KernelSettings:
    interval_seconds: int
    season_seconds: int = DAY
    profile_days: int = 7
    profile_min_support: int = 3
    ar_order: int = 3
    ar_window_days: int = 7
    ar_min_rows: int = 36
    ar_min_span_seconds: int = 6 * 3600
    hw_min_days: int = 2
    hw_max_days: int = 14
    hw_weight: float = 0.5
    min_history_days: int = 7
    min_observed_fraction: float = 0.95
    hw_maxiter: int = 200

    def __post_init__(self):
        if self.season_seconds % self.interval_seconds:
            raise ValueError("the season must be a whole number of grid intervals")

    @property
    def m(self) -> int:
        return self.season_seconds // self.interval_seconds

    def canonical(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


@dataclass(frozen=True)
class Series:
    """values[i] is the RPS at start + i*interval; NaN = not observed."""
    start: int
    interval: int
    values: np.ndarray

    def __post_init__(self):
        v = np.asarray(self.values, dtype=float)
        if v.ndim != 1 or np.any(v[np.isfinite(v)] < 0) or np.any(np.isinf(v)):
            raise ValueError("a series holds non-negative finite values or NaN")
        object.__setattr__(self, "values", v)

    @classmethod
    def from_points(cls, points: Sequence[Tuple[int, float]], interval: int, start: int, end: int) -> "Series":
        v = np.full((end - start) // interval + 1, np.nan)
        for t, x in points:
            if (t - start) % interval or not start <= t <= end:
                raise ValueError(f"point {t} is off the grid or outside {start}..{end}")
            v[(t - start) // interval] = x
        return cls(start, interval, v)

    @classmethod
    def from_grid(cls, grid) -> "Series":
        """From a forecasting.history.RpsGrid."""
        return cls(grid.start, grid.interval_seconds,
                   np.array([np.nan if x is None else x for x in grid.values], dtype=float))

    @property
    def end(self) -> int:
        return self.start + (len(self.values) - 1) * self.interval

    def index(self, t: int) -> int:
        if (t - self.start) % self.interval:
            raise ValueError(f"{t} is off the grid")
        return (t - self.start) // self.interval

    def at(self, t: int) -> float:
        i = (t - self.start) // self.interval
        return float(self.values[i]) if (t - self.start) % self.interval == 0 and 0 <= i < len(self.values) else math.nan

    def latest_observed(self) -> Optional[int]:
        idx = np.flatnonzero(np.isfinite(self.values))
        return int(self.start + idx[-1] * self.interval) if idx.size else None

    def digest(self, lo: int, hi: int) -> str:
        """sha256 of the exact values on [lo, hi] (NaN included as its bit pattern), with the grid."""
        a, b = self.index(lo), self.index(hi)
        h = hashlib.sha256(f"series|{self.interval}|{lo}|{hi}|".encode())
        h.update(np.ascontiguousarray(self.values[a:b + 1], dtype="<f8").tobytes())
        return h.hexdigest()


# --- seasonal-naive -----------------------------------------------------------------------------------------------

def seasonal_naive(series: Series, input_end: int, targets: Sequence[int], ks: KernelSettings) -> List[float]:
    """x(s - season) for every target; every lagged slot must be observed and no later than input_end."""
    out = []
    for s in targets:
        lag = s - ks.season_seconds
        v = series.at(lag)
        if lag > input_end or not math.isfinite(v):
            raise KernelUnavailable("history", f"seasonal-naive lag {lag} for target {s} is not available")
        out.append(v)
    return out


# --- Holt-Winters (E1's recursions, generalized) --------------------------------------------------------------------

def heuristic_init(y: np.ndarray, m: int) -> Tuple[float, float, np.ndarray]:
    """E1's initial level, trend and seasonal states (Hyndman & Athanasopoulos, fpp3 8.6), in NumPy: a centred 2 x m
    moving average over the first k cycles gives the trend; the mean detrended value per season position, normalized
    to sum to zero, the seasonal states; a line through the first ten trend values the level and slope."""
    n = len(y)
    if n < 2 * m:
        raise KernelUnavailable("history", "Holt-Winters needs at least two full seasonal cycles")
    min_obs = 10 + 2 * (m // 2)
    k = max(min(5, n // m), int(np.ceil(min_obs / m)))
    x = np.asarray(y[: m * k], dtype=float)
    c = np.concatenate([[0.0], np.cumsum(x)])
    trend = np.full(len(x), np.nan)
    lo = np.arange(len(x)) - m // 2                       # pandas' centred window starts at i - m//2
    ok = (lo >= 0) & (lo + m <= len(x))
    trend[ok] = (c[lo[ok] + m] - c[lo[ok]]) / m
    if m % 2 == 0:                                        # 2 x m: average adjacent centred means
        t2 = np.full(len(x), np.nan)
        t2[:-1] = (trend[:-1] + trend[1:]) / 2
        trend = t2
    detrended = x - trend
    tmp = np.full(k * m, np.nan)
    tmp[: len(detrended)] = detrended
    seasonal = np.nanmean(tmp.reshape(k, m).T, axis=1)
    seasonal = seasonal - np.mean(seasonal)
    level_input = trend[np.isfinite(trend)][:10]
    exog = np.c_[np.ones(10), np.arange(10) + 1]
    coef = np.linalg.pinv(exog).dot(level_input)
    return float(coef[0]), float(coef[1]), seasonal.astype(float)


def hw_run(y: Sequence[float], alpha: float, beta: float, gamma: float, level: float, trend: float,
           season: Sequence[float]):
    """Advance the additive Holt-Winters recursions (Hyndman form) through y: (sse, level, trend, season_rolled), where
    season_rolled[0] applies to the observation after the last one consumed (E1's hw_run)."""
    s = [float(v) for v in season]
    m = len(s)
    ac, bc, gc = 1.0 - alpha, 1.0 - beta, 1.0 - gamma
    lv, b = float(level), float(trend)
    sse, pos = 0.0, 0
    for obs in y:
        sp = s[pos]
        err = obs - (lv + b + sp)
        sse += err * err
        l_new = alpha * (obs - sp) + ac * (lv + b)
        b_new = beta * (l_new - lv) + bc * b
        s[pos] = gamma * (obs - lv - b) + gc * sp
        lv, b = l_new, b_new
        pos += 1
        if pos == m:
            pos = 0
    return sse, lv, b, s[pos:] + s[:pos]


def _params(x) -> Tuple[float, float, float]:
    """Admissible smoothing parameters: 0 <= beta <= alpha, 0 <= gamma <= 1 - alpha."""
    a = float(np.clip(x[0], 0.0, 1.0))
    return a, float(np.clip(x[1], 0.0, 1.0)) * a, float(np.clip(x[2], 0.0, 1.0)) * (1.0 - a)


@dataclass(frozen=True)
class HWState:
    alpha: float
    beta: float
    gamma: float
    level: float                 # after the last training observation (the fit cutoff)
    trend: float
    season: Tuple[float, ...]    # season[0] applies to the slot after the cutoff
    sse: float
    n_train: int
    segment_start: int


def fit_hw(y: np.ndarray, m: int, segment_start: int, maxiter: int = 200) -> HWState:
    """Least-squares Holt-Winters from E1's three starts (L-BFGS-B); the best converged fit."""
    y = np.asarray(y, dtype=float)
    if not np.all(np.isfinite(y)):
        raise KernelUnavailable("history", "Holt-Winters input must be complete")
    level0, trend0, season0 = heuristic_init(y, m)
    obs = y.tolist()
    best = None
    for a, b, g in HW_STARTS:
        x0 = (a, b / a if a > 0 else 0.0, g / (1.0 - a))

        def objective(x):
            al, be, ga = _params(x)
            return hw_run(obs, al, be, ga, level0, trend0, season0)[0]

        res = minimize(objective, x0=np.array(x0), method="L-BFGS-B", bounds=[(0.0, 1.0)] * 3,
                       options={"maxiter": maxiter})
        if not res.success or not np.isfinite(res.fun):
            continue
        if best is None or res.fun < best.fun:
            best = res
    if best is None:
        raise KernelUnavailable("numeric", "the Holt-Winters optimizer did not converge from any start")
    al, be, ga = _params(best.x)
    sse, level, trend, season = hw_run(obs, al, be, ga, level0, trend0, season0)
    if not all(math.isfinite(v) for v in (sse, level, trend, *season)):
        raise KernelUnavailable("numeric", "Holt-Winters state is not finite")
    return HWState(al, be, ga, level, trend, tuple(season), float(sse), len(obs), segment_start)


# --- profile and AR -------------------------------------------------------------------------------------------------

def profile(series: Series, t: int, ks: KernelSettings) -> float:
    """Mean of the observed values at t - d days, d = 1..profile_days; NaN with fewer than profile_min_support."""
    vals = [series.at(t - d * ks.season_seconds) for d in range(1, ks.profile_days + 1)]
    vals = [v for v in vals if math.isfinite(v)]
    return float(np.mean(vals)) if len(vals) >= ks.profile_min_support else math.nan


def residual(series: Series, t: int, ks: KernelSettings) -> float:
    y, p = series.at(t), profile(series, t, ks)
    return y - p if math.isfinite(y) and math.isfinite(p) else math.nan


def ar_roots_max(coef: Sequence[float]) -> float:
    """Largest modulus of the roots of z^p - c1 z^(p-1) - ... - cp (the companion eigenvalues)."""
    phi = list(coef[1:])
    if not any(phi):
        return 0.0
    return float(max(abs(np.roots([1.0, *[-c for c in phi]]))))


def fit_ar(series: Series, cutoff: int, ks: KernelSettings) -> Tuple[np.ndarray, int]:
    p, dt = ks.ar_order, ks.interval_seconds
    first = max(series.start + p * dt, cutoff - ks.ar_window_days * DAY + dt + p * dt)
    rows, targets, times = [], [], []
    for t in range(first, cutoff + 1, dt):
        r = residual(series, t, ks)
        lags = [residual(series, t - k * dt, ks) for k in range(1, p + 1)]
        if math.isfinite(r) and all(math.isfinite(v) for v in lags):
            rows.append([1.0, *lags])
            targets.append(r)
            times.append(t)
    if len(rows) < ks.ar_min_rows or times[-1] - times[0] < ks.ar_min_span_seconds:
        raise KernelUnavailable("history", f"AR({p}) needs {ks.ar_min_rows} complete rows spanning "
                                           f"{ks.ar_min_span_seconds} s, got {len(rows)}")
    coef, *_ = np.linalg.lstsq(np.array(rows), np.array(targets), rcond=None)
    if not np.all(np.isfinite(coef)):
        raise KernelUnavailable("numeric", "AR coefficients are not finite")
    root = ar_roots_max(coef)
    if not root < AR_MAX_ROOT:
        raise KernelUnavailable("numeric", f"AR recursion is not stable (largest root {root:.4f})")
    return coef, len(rows)


# --- E1-product fit and forecast ------------------------------------------------------------------------------------

@dataclass(frozen=True)
class E1Fit:
    cutoff: int
    interval_seconds: int
    hw: HWState
    ar_coef: Tuple[float, ...]
    ar_rows: int
    input_lo: int
    input_sha256: str
    settings: Dict = field(default_factory=dict)


def complete_segment_start(series: Series, cutoff: int, max_slots: int) -> int:
    """The first grid time of the longest complete run ending at cutoff (at most max_slots long)."""
    i = series.index(cutoff)
    j = i
    while j > 0 and i - (j - 1) < max_slots and math.isfinite(series.values[j - 1]):
        j -= 1
    if not math.isfinite(series.values[i]):
        raise KernelUnavailable("history", "the fit cutoff itself was not observed")
    return series.start + j * series.interval


def fit_e1(series: Series, cutoff: int, ks: KernelSettings) -> E1Fit:
    """Fit E1-product at `cutoff` from the series' values at or before it (later values are never read)."""
    dt, m = ks.interval_seconds, ks.m
    if series.interval != dt or cutoff % dt or not series.start <= cutoff <= series.end:
        raise KernelUnavailable("history", "the cutoff is not on the series' grid")
    if cutoff - series.start + dt < ks.min_history_days * DAY:
        raise KernelUnavailable("history", f"E1-product needs {ks.min_history_days} days of history")
    w_lo = cutoff - ks.ar_window_days * DAY + dt
    window = series.values[series.index(w_lo):series.index(cutoff) + 1]
    if np.mean(np.isfinite(window)) < ks.min_observed_fraction:
        raise KernelUnavailable("history", f"under {ks.min_observed_fraction:.0%} of the fit window was observed")
    seg_lo = complete_segment_start(series, cutoff, ks.hw_max_days * m)
    if (cutoff - seg_lo) // dt + 1 < ks.hw_min_days * m:
        raise KernelUnavailable("history", f"Holt-Winters needs a complete {ks.hw_min_days}-day segment ending at "
                                           "the cutoff")
    hw = fit_hw(series.values[series.index(seg_lo):series.index(cutoff) + 1], m, seg_lo, ks.hw_maxiter)
    coef, rows = fit_ar(series, cutoff, ks)
    lo = max(series.start, min(seg_lo, w_lo - ks.profile_days * DAY - ks.ar_order * dt))
    return E1Fit(cutoff=cutoff, interval_seconds=dt, hw=hw, ar_coef=tuple(float(c) for c in coef), ar_rows=rows,
                 input_lo=lo, input_sha256=series.digest(lo, cutoff), settings=ks.canonical())


def forecast_e1(fit: E1Fit, series: Series, input_end: int, targets: Sequence[int],
                ks: KernelSettings) -> Dict[str, List[float]]:
    """{"hw", "profile_ar", "points"} for the targets, from input_end. Raises KernelUnavailable (no partial result)."""
    dt, m = ks.interval_seconds, ks.m
    if fit.interval_seconds != dt or input_end < fit.cutoff or (input_end - fit.cutoff) % dt:
        raise KernelUnavailable("history", "the fit does not precede the input end on this grid")
    if any(s <= input_end or (s - input_end) % dt for s in targets):
        raise KernelUnavailable("history", "targets must lie on the grid after the input end")
    if any(s - input_end > ks.season_seconds for s in targets):     # a profile would read past the input end
        raise KernelUnavailable("history", "targets must lie within one season of the input end")
    since = [series.at(t) for t in range(fit.cutoff + dt, input_end + 1, dt)]
    if not all(math.isfinite(v) for v in since) or not math.isfinite(series.at(input_end)):
        raise KernelUnavailable("history", "observations between the fit cutoff and the input end are missing")
    h = fit.hw
    _, level, trend, season = hw_run(since, h.alpha, h.beta, h.gamma, h.level, h.trend, h.season)
    steps = [(s - input_end) // dt for s in targets]
    hw = [level + k * trend + season[(k - 1) % m] for k in steps]
    hist = [residual(series, input_end - k * dt, ks) for k in range(ks.ar_order)]
    if not all(math.isfinite(v) for v in hist):
        raise KernelUnavailable("history", "the recent AR lags are incomplete")
    c = fit.ar_coef
    ar = {}
    for k in range(1, max(steps) + 1):
        nxt = c[0] + sum(c[j + 1] * hist[j] for j in range(ks.ar_order))
        ar[k] = nxt
        hist = [nxt, *hist[:-1]]
    pa = []
    for s, k in zip(targets, steps):
        p = profile(series, s, ks)
        if not math.isfinite(p):
            raise KernelUnavailable("history", f"the profile of target {s} lacks support")
        pa.append(p + ar[k])
    points = [max(0.0, ks.hw_weight * a + (1.0 - ks.hw_weight) * b) for a, b in zip(hw, pa)]
    if not all(math.isfinite(v) for v in (*hw, *pa, *points)):
        raise KernelUnavailable("numeric", "the E1-product forecast is not finite")
    return {"hw": [float(v) for v in hw], "profile_ar": [float(v) for v in pa], "points": points}


# --- shadow candidates ----------------------------------------------------------------------------------------------

def shadow_candidates(series: Series, input_end: int, targets: Sequence[int], ks: KernelSettings,
                      fit: Optional[E1Fit]) -> Dict[str, Dict]:
    """Every family's forecast at the same origin, each either {"points": [...]} or {"unavailable": reason, ...}.
    Nothing is selected or substituted here: a failed E1-product stays unavailable under its own name, and the
    caller may fall back to seasonal_naive only by that name (and only when it is itself trusted, C2.5)."""
    out: Dict[str, Dict] = {}
    try:
        out["seasonal_naive"] = {"recipe": RECIPE_NAIVE, "points": seasonal_naive(series, input_end, targets, ks)}
    except KernelUnavailable as e:
        out["seasonal_naive"] = {"recipe": RECIPE_NAIVE, "unavailable": e.reason, "message": e.message}
    if fit is None:
        out["statistical"] = {"recipe": RECIPE_E1, "unavailable": "history", "message": "no compatible fit"}
    else:
        try:
            out["statistical"] = {"recipe": RECIPE_E1, **forecast_e1(fit, series, input_end, targets, ks)}
        except KernelUnavailable as e:
            out["statistical"] = {"recipe": RECIPE_E1, "unavailable": e.reason, "message": e.message}
    return out
