"""Relative-residual profile-AR forecaster ("pr_ar_rob"; arm R1 of the AWS benchmark, Task 03).

Port of the model lab's specification of 2026-09-29 (`SPEC-pr_ar_rob.md`, reference implementation
`pr_ar_rob_reference.py`, test vectors reproduced to 1e-6 in tests/test_relative_profile_ar.py).

The forecaster has one component and no optimiser:

  profile(i)  = mean of the observed values at the same ten-minute slot on the previous seven days
                (identical to seasonal_ensemble.profile_at)
  q[t]        = yf[t] / profile(t) - 1        (yf = forward-filled observations; NaN where the
                                              profile is missing or <= 0, or yf is missing)
  generation  = OLS AR(3) with intercept on q over the seven days ending at the latest 00/06/12/18Z
                boundary at or before the origin (boundary included), with the TRAINING copy of q
                clipped to +/- 0.5; at least 36 complete rows, else the generation is invalid
  forecast    = seeded with the UNCLIPPED q at origin, origin - 1, origin - 2;
                y_hat[s] = max(0, profile(origin + s) * (1 + q_hat[s])), s = 1..6

Serving choices the lab left to the port (spec section 3), declared here:
  * an invalid generation falls back once to the previous boundary (seasonal_ensemble's
    STALE_GENERATIONS = 1 rule; it changes nothing on valid data);
  * generations are cached by (source, boundary, SHA-256 of the 14 days of observations the fit
    reads: seven AR-window days plus the seven profile days before them);
  * the capacity margin is computed on THIS forecaster's own past leads (seasonal_ensemble.margin_at
    with raw_fn), never on the ensemble's;
  * there is no partial rule (one component): a step whose target profile is missing is refused.
"""
from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import threading
from typing import Dict, Iterable, Tuple

import numpy as np

from models import seasonal_ensemble as se
from models.seasonal_ensemble import ForecastUnavailable, Grid, profile_at, _iso, _boundary_of

SLOT_SECONDS = se.SLOT_SECONDS
SEASON = se.SEASON
STEPS = se.STEPS
REFIT_SECONDS = se.REFIT_SECONDS
PROFILE_DAYS = se.PROFILE_DAYS
AR_ORDER = 3
AR_WINDOW_SLOTS = 7 * SEASON
MIN_AR_ROWS = 36
REL_CLIP = 0.5                       # training residuals clipped to +/- 50 % of the profile
STALE_GENERATIONS = se.STALE_GENERATIONS
MAX_ORIGIN_AGE_SECONDS = se.MAX_ORIGIN_AGE_SECONDS
VERSION = "relative-profile-ar-1.0.0"
COMPONENT = "profile_ar_rel"

SETTINGS = {
    "version": VERSION, "slot_seconds": SLOT_SECONDS, "season": SEASON, "steps": STEPS,
    "refit_seconds": REFIT_SECONDS, "profile_days": PROFILE_DAYS, "ar_order": AR_ORDER,
    "ar_window_slots": AR_WINDOW_SLOTS, "min_ar_rows": MIN_AR_ROWS, "rel_clip": REL_CLIP,
    "margin_window_slots": se.MARGIN_WINDOW_SLOTS, "margin_min_samples": se.MARGIN_MIN_SAMPLES,
    "margin_clip_fraction": se.MARGIN_CLIP_FRACTION, "max_origin_age_seconds": MAX_ORIGIN_AGE_SECONDS,
    "stale_generations": STALE_GENERATIONS, "components": 1,
}


def relative_residuals(y: np.ndarray, yf: np.ndarray, upto: int) -> np.ndarray:
    """q[t] = yf[t] / profile[t] - 1 for t <= upto; NaN where the profile is missing or <= 0, or yf is."""
    q = np.full(upto + 1, np.nan)
    for t in range(upto + 1):
        p = profile_at(y, t)
        if np.isfinite(p) and p > 0 and np.isfinite(yf[t]):
            q[t] = yf[t] / p - 1.0
    return q


def fit_generation(q: np.ndarray, b: int) -> Tuple[np.ndarray, int]:
    """AR(3) with intercept on the trailing seven days of q ending at boundary index b (inclusive).

    A copy of q is clipped to +/- REL_CLIP on the training window only (NaN stays NaN); a row needs a
    finite target and three finite lags. Raises ValueError for an invalid generation (fewer than
    MIN_AR_ROWS rows or a non-finite solution)."""
    lo = max(0, b - AR_WINDOW_SLOTS + 1)
    qt = np.array(q[:b + 1], dtype=float)
    qt[lo:] = np.clip(qt[lo:], -REL_CLIP, REL_CLIP)
    rows, targets = [], []
    for t in range(max(AR_ORDER, lo + AR_ORDER), b + 1):
        lags = qt[t - AR_ORDER:t][::-1]                       # q[t-1], q[t-2], q[t-3]
        if np.isfinite(qt[t]) and np.all(np.isfinite(lags)):
            rows.append([1.0, *lags])
            targets.append(qt[t])
    if len(rows) < MIN_AR_ROWS:
        raise ValueError(f"AR fit needs {MIN_AR_ROWS} complete rows, got {len(rows)}")
    coef, *_ = np.linalg.lstsq(np.array(rows), np.array(targets), rcond=None)
    if not np.all(np.isfinite(coef)):
        raise ValueError("AR coefficients not finite")
    return coef, len(rows)


@dataclass
class Generation:
    boundary_ts: int
    ar_coef: np.ndarray
    ar_rows: int
    fingerprint: str
    window_start_ts: int

    def summary(self) -> Dict:
        return {"boundary": _iso(self.boundary_ts), "fingerprint": self.fingerprint,
                "forecaster": VERSION,
                "ar": {"coef": [float(c) for c in self.ar_coef], "rows": self.ar_rows,
                       "window_start": _iso(self.window_start_ts), "rel_clip": REL_CLIP}}


_CACHE: "OrderedDict[Tuple, Generation]" = OrderedDict()
_CACHE_LOCK = threading.Lock()
_CACHE_SIZE = 32


def generation_for(grid: Grid, boundary_ts: int, cache_key: str = "") -> Generation:
    """Fit (or fetch from cache) the generation that serves [boundary, boundary + 6 h)."""
    b = grid.index(boundary_ts)
    if b < 0 or b >= len(grid.y):
        raise ValueError("boundary outside the observed series")
    y, yf = grid.y, grid.filled()
    lo = max(0, b - AR_WINDOW_SLOTS + 1)
    fp_lo = max(0, lo - PROFILE_DAYS * SEASON)               # the profile reads seven days before the window
    h = hashlib.sha256()
    h.update(f"{VERSION}|{boundary_ts}|{grid.ts(fp_lo)}".encode())
    h.update(np.ascontiguousarray(y[fp_lo:b + 1]).tobytes())
    fingerprint = h.hexdigest()
    key = (cache_key, boundary_ts, fingerprint)
    with _CACHE_LOCK:
        if key in _CACHE:
            _CACHE.move_to_end(key)
            return _CACHE[key]
    coef, rows = fit_generation(relative_residuals(y, yf, b), b)
    gen = Generation(boundary_ts=boundary_ts, ar_coef=coef, ar_rows=rows, fingerprint=fingerprint,
                     window_start_ts=grid.ts(lo))
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


def raw_at(grid: Grid, origin: int, cache_key: str = "") -> Dict:
    """The six raw (margin-free) forecasts after `origin`; NaN for a step whose target profile is
    missing. Raises ForecastUnavailable when no generation is valid or a seed residual is missing."""
    gen, stale = _usable_generation(grid, origin, cache_key)
    y, yf = grid.y, grid.filled()
    hist = []
    for t in (origin, origin - 1, origin - 2):
        p = profile_at(y, t) if t >= 0 else float("nan")
        if t >= 0 and np.isfinite(p) and p > 0 and np.isfinite(yf[t]):
            hist.append(yf[t] / p - 1.0)                       # the UNCLIPPED residual seeds the recursion
        else:
            raise ForecastUnavailable("relative residual missing at origin, origin-1 or origin-2")
    c = gen.ar_coef
    raw = []
    for s in range(1, STEPS + 1):
        nxt = c[0] + c[1] * hist[0] + c[2] * hist[1] + c[3] * hist[2]
        p = profile_at(y, origin + s)
        raw.append(max(0.0, float(p * (1.0 + nxt))) if np.isfinite(p) else float("nan"))
        hist = [nxt, hist[0], hist[1]]
    return {"raw": raw, "generation": gen, "stale_generation": stale}


def _raw_fn(grid: Grid, j: int, cache_key: str):
    return raw_at(grid, j, cache_key)["raw"]


def forecast(points: Iterable[Tuple[int, float]], now_ts: int, cache_key: str = "",
             margin_quantile: float = se.MARGIN_QUANTILE, margin_mode: str = "relative") -> Dict:
    """Serve one forecast from on-grid (epoch seconds, value) observations, with the capacity margin
    (default: relative q90 of this forecaster's own past lead errors) added to every step.
    The result has the same shape as seasonal_ensemble.forecast so the API serves both alike."""
    grid = Grid.from_points(points)
    observed = np.flatnonzero(np.isfinite(grid.y))
    origin = int(observed[-1])
    origin_ts = grid.ts(origin)
    if now_ts - origin_ts > MAX_ORIGIN_AGE_SECONDS:
        raise ForecastUnavailable(f"latest observation {_iso(origin_ts)} is older than "
                                  f"{MAX_ORIGIN_AGE_SECONDS // 60} minutes")
    out = raw_at(grid, origin, cache_key)
    raw = out["raw"]
    if not all(np.isfinite(v) for v in raw):
        raise ForecastUnavailable("profile missing for a target slot: %s" % raw)
    lead = max(raw[0], raw[1])
    margin, n = se.margin_at(grid, origin, lead, cache_key, margin_quantile, margin_mode, raw_fn=_raw_fn)
    gen: Generation = out["generation"]
    return {
        "origin": _iso(origin_ts),
        "target_timestamps": [_iso(origin_ts + s * SLOT_SECONDS) for s in range(1, STEPS + 1)],
        "components": {COMPONENT: raw}, "raw": raw,
        "margin": margin, "margin_samples": n, "margin_quantile": margin_quantile,
        "margin_mode": margin_mode, "partial_rule": "single-component", "served_components": 1,
        "served": [v + margin for v in raw],
        "generation": gen.summary(), "stale_generation": out["stale_generation"],
        "off_grid_dropped": grid.off_grid_dropped,
        "settings": {**SETTINGS, "margin_quantile": margin_quantile, "margin_mode": margin_mode},
    }
