"""Acceptance protocol A of the calibration repair: measured coverage of the window bound on declared synthetic noise
models with known truth (forecasting.calibration, method normalized-window-score/two-part-scale/v1).

Everything here is declared BEFORE a validation result exists and is deterministic:

  - Level: one diurnal curve with ramps (LEVEL_POINTS, RPS by hour of day), the same every day.
  - Configurations (CONFIGS): the two evaluation profiles on the 60 s grid (leads 180 s and 300 s) carry the
    criteria; the default 300 s grid with a 1200 s lead is reported beside them (72 cohort windows a day, so its
    intervals are wider and its score buffer is still filling during the measurement).
  - Noise models (MODELS): additive, proportional and mixed Gaussian noise on the level, each also with AR(1) noise
    (rho = AR_RHO per grid step); a near-zero load; a discrete load that is exactly zero at night (Poisson counts).
    Samples are clipped at zero.
  - Seeds: DEVELOPMENT_SEEDS were used while the method was built. VALIDATION_SEEDS are frozen: they are run once,
    after the implementation is finished, and nothing is tuned afterwards.
  - Forecaster: the product's daily seasonal-naive forecast, b = max of yesterday's samples over the window, issued
    at every cohort origin (origin mod lead == 0), with the service's timing: the newest input is origin - settlement,
    a window is folded into the calibration once origin >= window end + settlement, the scale is fitted at the first
    origin with enough day-lag pairs and refitted at the first origin of each UTC day.
  - Calibration: the product's own functions (calibration.fit_scale, upper_bound, apply), unchanged. kernel="fast" is
    an exact re-implementation of upper_bound/apply for long simulations (same order statistic, same integer level);
    tests compare the two bound for bound.
  - Comparison: method="additive" runs the same path with the unit scale from the first window, which is the earlier
    additive method (score Y - b in RPS) number for number.
  - Measurement: cohort windows with a bound from MEASURE_FROM_DAY on. Marginal coverage, coverage per forecast-level
    tercile (TERCILES: the 1/3 and 2/3 quantiles of the noise-free level, fixed by the declared curve), ramp versus
    plateau (the slope of the noise-free level over the window), near-zero load, pinball loss and bound width.
  - Uncertainty: independent replications (one path per seed); every figure is the mean over replications with a
    95 % t interval, which is valid under any dependence inside a path.
  - Reading: a tolerance is MET when the whole 95 % interval lies inside it, NOT MET when the estimate lies outside,
    and INCONCLUSIVE otherwise.

These are empirical engineering criteria on declared models. They are not a distribution-free guarantee of conditional
coverage, and normalization does not supply one.

  python -m evaluation.calibration_acceptance --seeds development --out dev.json
  python -m evaluation.calibration_acceptance --seeds validation --out validation.json     # once, at the end
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import math
import sys
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from forecasting import calibration as cal

DAY, GRID = 86400, 60
SETTLEMENT = 120
HISTORY_DAYS = 14
NOMINAL_BPS = 1000
LEADS = (180, 300)
# (grid seconds, lead seconds, carries the criteria)
CONFIGS = ((60, 180, True), (60, 300, True), (300, 1200, False))
PATH_DAYS = 14
MEASURE_FROM_DAY = 4                      # two days until the first full scale fit, a day of scores, a day of margin
AR_RHO = 0.7

DEVELOPMENT_SEEDS = tuple(range(101, 111))
VALIDATION_SEEDS = tuple(range(900_001, 900_041))        # frozen; run once after the implementation is finished

# (hour of day, noise-free level in RPS), piecewise linear: night plateau, morning ramp, plateau, midday dip with its
# own ramps, evening plateau, decline.
LEVEL_POINTS = ((0, 10.0), (6, 10.0), (9, 60.0), (12, 60.0), (14, 40.0), (16, 40.0), (18, 60.0), (20, 60.0),
                (23, 10.0), (24, 10.0))
NEAR_ZERO_POINTS = ((0, 0.05), (6, 0.05), (9, 2.0), (12, 2.0), (14, 1.2), (16, 1.2), (18, 2.0), (20, 2.0),
                    (23, 0.05), (24, 0.05))
DISCRETE_POINTS = ((0, 0.0), (6, 0.0), (9, 0.5), (12, 0.5), (14, 0.3), (16, 0.3), (18, 0.5), (20, 0.5), (23, 0.0),
                   (24, 0.0))
NEAR_ZERO_LEVEL = 0.1                     # "near zero": noise-free level at or below this (RPS), near_zero model


@dataclass(frozen=True)
class Model:
    name: str
    points: tuple
    additive_sd: float          # RPS
    proportional_sd: float      # share of the level
    rho: float = 0.0            # AR(1) coefficient of the standardized noise per grid step
    poisson: bool = False       # per-minute Poisson counts instead of Gaussian noise
    criterion: bool = True      # False: reported separately (near-zero, discrete)


MODELS = (
    Model("additive", LEVEL_POINTS, 3.0, 0.0),
    Model("proportional", LEVEL_POINTS, 0.0, 0.05),
    Model("mixed", LEVEL_POINTS, 1.5, 0.04),
    Model("additive_ar1", LEVEL_POINTS, 3.0, 0.0, AR_RHO),
    Model("proportional_ar1", LEVEL_POINTS, 0.0, 0.05, AR_RHO),
    Model("mixed_ar1", LEVEL_POINTS, 1.5, 0.04, AR_RHO),
    Model("near_zero", NEAR_ZERO_POINTS, 0.05, 0.08, criterion=False),
    Model("discrete_zero", DISCRETE_POINTS, 0.0, 0.0, poisson=True, criterion=False),
)
MODEL = {m.name: m for m in MODELS}
MARGINAL_TOLERANCE, TERCILE_TOLERANCE = 0.02, 0.03


def level_curve(points: Sequence[Tuple[float, float]], grid: int = GRID) -> np.ndarray:
    """The noise-free level at every grid time of one day (sample i is the time i * grid)."""
    hours, levels = zip(*points)
    t = np.arange(DAY // grid) * grid / 3600.0
    return np.interp(t, hours, levels)


def terciles(points: Sequence[Tuple[float, float]]) -> Tuple[float, float]:
    """The 1/3 and 2/3 quantiles of the noise-free level over a day: the predeclared forecast-level terciles."""
    lv = np.sort(level_curve(points))
    return float(lv[len(lv) // 3]), float(lv[2 * len(lv) // 3])


TERCILES = {m.name: terciles(m.points) for m in MODELS}


def signal(model: Model, seed: int, days: int, grid: int = GRID) -> np.ndarray:
    """One path of the sampled RPS on the grid (days * 86400 / grid samples). The stream depends on the seed and the
    model; the noise is per SAMPLE of whatever grid (the same standard deviation and AR coefficient per step)."""
    rng = np.random.default_rng(np.random.SeedSequence([seed, MODELS.index(model)]))
    level = np.tile(level_curve(model.points, grid), days)
    if model.poisson:
        return rng.poisson(level * grid) / float(grid)
    z = rng.standard_normal(len(level))
    if model.rho:
        out, innov = np.empty_like(z), math.sqrt(1.0 - model.rho ** 2)
        out[0] = z[0]
        for i in range(1, len(z)):
            out[i] = model.rho * out[i - 1] + innov * z[i]
        z = out
    sd = np.sqrt(model.additive_sd ** 2 + (model.proportional_sd * level) ** 2)
    return np.maximum(0.0, level + sd * z)


def policy(lead: int) -> cal.CalibrationPolicy:
    return cal.CalibrationPolicy(lead_seconds=lead, nominal_bps=NOMINAL_BPS)


class FastCalibration:
    """calibration.upper_bound and calibration.apply for cohort windows arriving in window-end order, with a sorted
    score list instead of a sort per issuance. Exact while the retention never binds (asserted), which holds when
    max_scores windows span less than the retention."""

    def __init__(self, p: cal.CalibrationPolicy):
        if p.max_scores * p.lead_seconds >= p.retention_seconds - DAY:
            raise ValueError("the fast kernel needs max_scores windows well inside the retention")
        self.p, self.a_e8 = p, p.nominal_bps * cal.BPS
        self.by_end: deque = deque()
        self.sorted: List[float] = []

    def upper_bound(self, base: float, scale: Optional[cal.Scale]) -> Optional[float]:
        n = len(self.sorted)
        k = cal.rank(n, self.a_e8)
        if scale is None or n < self.p.min_windows or k > n:
            return None
        return max(0.0, base + self.sorted[k - 1] * scale.at(base))

    def apply(self, y: float, base: float, upper: Optional[float], scale: Optional[cal.Scale]) -> None:
        if scale is None:
            return
        s = (y - base) / scale.at(base)
        self.by_end.append(s)
        bisect.insort(self.sorted, s)
        if len(self.by_end) > self.p.max_scores:
            del self.sorted[bisect.bisect_left(self.sorted, self.by_end.popleft())]
        if upper is not None:
            a = self.a_e8 + self.p.eta_bps * (self.p.nominal_bps - int(y > upper) * cal.BPS)
            self.a_e8 = min(self.p.max_bps * cal.BPS, max(self.p.min_bps * cal.BPS, a))


class ExactCalibration:
    """The product's functions, unchanged: one calibration state, records in journal order."""

    def __init__(self, p: cal.CalibrationPolicy):
        self.p, self.state, self.seq = p, cal.initial(p, "e" * 64, "seasonal_naive", "acceptance"), 0

    @property
    def a_e8(self) -> int:
        return self.state["a_e8"]

    def upper_bound(self, base: float, scale: Optional[cal.Scale], now_s: int) -> Optional[float]:
        return cal.upper_bound(self.state, self.p, base, now_s, scale)[0]

    def apply(self, origin: int, y: float, base: float, upper: Optional[float], scale: Optional[cal.Scale]) -> None:
        self.seq += 1
        rec = cal.CalibrationRecord(self.seq, "e" * 64, "seasonal_naive", "acceptance", f"iss-{self.seq}", origin,
                                    origin + self.p.lead_seconds, base, upper, "scored", y, scale)
        self.state = cal.apply(self.state, rec, self.p)


REQUEST_OFFSET = 1                        # the operator asks one second after the decision origin


@dataclass(frozen=True)
class Timing:
    """The service's timing for one autoscaler: the sample grid, the settlement, the history window, the epoch of
    x[0] (a UTC midnight) and how long after a decision origin the request arrives."""
    grid: int = GRID
    settlement: int = SETTLEMENT
    history_days: int = HISTORY_DAYS
    start: int = 0
    request_offset: int = REQUEST_OFFSET

    def newest(self, origin: int) -> int:
        """Index of the newest finalized sample at the request for `origin` (forecasting.history.watermark)."""
        return ((origin + self.request_offset - self.settlement) // self.grid * self.grid - self.start) // self.grid

    def pairs(self, origin: int) -> int:
        """Day-lag pairs in the history window at `origin`, for a series observed from x[0] on without gaps."""
        slots = DAY // self.grid
        return max(0, min(self.newest(origin) - slots + 1, (self.history_days - 1) * slots))


def scale_at(x: np.ndarray, origin: int, p: cal.CalibrationPolicy, timing: Timing = Timing()) -> Optional[cal.Scale]:
    """The fit the service makes at `origin`: day-lag pairs of the finalized history window."""
    slots = DAY // timing.grid
    end = timing.newest(origin)
    if end < 0:
        return None
    start = max(0, end - timing.history_days * slots + 1)
    levels, residuals = cal.season_pairs(x[start:end + 1].tolist(), slots)
    return cal.fit_scale(levels, residuals, p, slots)


@dataclass
class Path:
    """Per cohort window (issue order): origin, forecast level b, bound U (NaN: none), truth Y, the working
    miscoverage at issue, and the scale's sigma at b (NaN: no scale)."""
    lead: int
    origin: np.ndarray
    base: np.ndarray
    upper: np.ndarray
    y: np.ndarray
    working: np.ndarray
    sigma: np.ndarray
    scales: List[Tuple[int, Optional[dict]]]          # (fit origin, scale) at every fit point


def simulate(x: np.ndarray, lead: int, *, method: str = "two_part", kernel: str = "exact",
             p: Optional[cal.CalibrationPolicy] = None, timing: Timing = Timing(),
             first_origin: Optional[int] = None) -> Path:
    """Run the calibration over one signal path (module docstring); x[i] is the sample at timing.start + i * grid.
    method: two_part | additive. The defaults are the acceptance configuration; a test passes the service's own
    settings and compares this simulation with the real service, bound for bound.

    The service's rules reproduced here, for a request at origin + request_offset:
      - forecast: b = max of the samples one day before the window's targets (origin + grid ... origin + lead);
      - a window issued at u is folded into the calibration once origin + request_offset >= u + lead + settlement;
      - the scale is fitted at the first grid origin (from first_origin on) whose history holds enough day-lag pairs,
        and again at the first grid origin of each UTC day. No cohort window is issued between a fit point and the
        next cohort origin, so fitting at exactly those origins gives every cohort window the scale the service
        records."""
    if method not in ("two_part", "additive") or kernel not in ("exact", "fast"):
        raise ValueError("unknown method or kernel")
    p = p or policy(lead)
    grid, t0 = timing.grid, timing.start
    if p.lead_seconds != lead or lead % grid or t0 % DAY:
        raise ValueError("the lead must be whole grid steps and the policy's lead; the series starts at a UTC midnight")
    k, slots = lead // grid, DAY // grid
    calib = ExactCalibration(p) if kernel == "exact" else FastCalibration(p)
    scale: Optional[cal.Scale] = cal.UNIT_SCALE if method == "additive" else None
    scale_day, scales = None, []
    begin = t0 + DAY - grid if first_origin is None else first_origin   # every lagged target exists from here on
    first = -(-begin // lead) * lead
    last = t0 + (len(x) - 1) * grid - lead
    need = p.min_pairs(slots)
    first_fit = None
    if method == "two_part" and (timing.history_days - 1) * slots >= need:
        first_fit = -(-begin // grid) * grid
        while timing.pairs(first_fit) < need:
            first_fit += grid
    pending: deque = deque()                                 # issued cohort windows not folded yet
    cols: Dict[str, list] = {c: [] for c in ("origin", "base", "upper", "y", "working", "sigma")}
    for origin in range(first, last + 1, lead):
        while pending and pending[0][0] + lead + timing.settlement <= origin + timing.request_offset:
            o, yv, bv, uv, sc = pending.popleft()
            if kernel == "exact":
                calib.apply(o, yv, bv, uv, sc)
            else:
                calib.apply(yv, bv, uv, sc)
        if first_fit is not None:
            fits = []
            if scale_day is None and origin >= first_fit:
                fits.append(first_fit)
                scale_day = first_fit // DAY
            if scale_day is not None and origin // DAY != scale_day:
                fits.append(origin - origin % DAY)
                scale_day = origin // DAY
            for at in fits:
                fit = scale_at(x, at, p, timing)
                scale = fit if fit is not None else scale     # a refit without enough pairs keeps the scale in force
                scales.append((at, None if fit is None else fit.to_wire()))
        i = (origin - t0) // grid
        base = float(x[i + 1 - slots:i + k + 1 - slots].max())
        y = float(x[i + 1:i + k + 1].max())
        upper = calib.upper_bound(base, scale, origin) if kernel == "exact" else calib.upper_bound(base, scale)
        cols["origin"].append(origin)
        cols["base"].append(base)
        cols["upper"].append(math.nan if upper is None else upper)
        cols["y"].append(y)
        cols["working"].append(calib.a_e8 / cal.E8)
        cols["sigma"].append(math.nan if scale is None else scale.at(base))
        pending.append((origin, y, base, upper, scale))
    return Path(lead, np.array(cols["origin"]), np.array(cols["base"]), np.array(cols["upper"]), np.array(cols["y"]),
                np.array(cols["working"]), np.array(cols["sigma"]), scales)


def _ratio(num: float, den: float) -> Optional[float]:
    return num / den if den else None


def measure(path: Path, model: Model, grid: int = GRID) -> Dict[str, Optional[float]]:
    """The statistics of one replication over its measured windows (bounded, from MEASURE_FROM_DAY on)."""
    ok = np.isfinite(path.upper) & (path.origin >= MEASURE_FROM_DAY * DAY)
    o, b, u, y = path.origin[ok], path.base[ok], path.upper[ok], path.y[ok]
    covered = y <= u
    level = level_curve(model.points, grid)
    k = path.lead // grid
    slot = (o // grid) % len(level)
    # The noise-free level over the window's targets: a ramp when it changes, a plateau when it does not.
    lo = np.array([level[(s + 1 + np.arange(k)) % len(level)].min() for s in slot])
    hi = np.array([level[(s + 1 + np.arange(k)) % len(level)].max() for s in slot])
    ramp = hi > lo
    t1, t2 = TERCILES[model.name]
    groups = {"low": b <= t1, "mid": (b > t1) & (b <= t2), "high": b > t2, "ramp": ramp, "plateau": ~ramp,
              "near_zero": hi <= NEAR_ZERO_LEVEL, "zero_forecast": b == 0.0}
    tau = 1.0 - NOMINAL_BPS / cal.BPS
    pin = np.where(y >= u, tau * (y - u), (1.0 - tau) * (u - y))
    out = {"windows": int(ok.sum()), "coverage": _ratio(float(covered.sum()), float(ok.sum())),
           "pinball_rps": float(pin.mean()) if ok.any() else None,
           "width_rps": float((u - b).mean()) if ok.any() else None,
           "width_over_level": float(((u - b) / np.maximum(hi, 1e-9)).mean()) if ok.any() else None,
           "unbounded_share": float(1.0 - np.isfinite(path.upper[path.origin >= MEASURE_FROM_DAY * DAY]).mean())}
    for name, g in groups.items():
        out[f"coverage_{name}"] = _ratio(float(covered[g].sum()), float(g.sum()))
        out[f"windows_{name}"] = int(g.sum())
        out[f"width_{name}_rps"] = float((u - b)[g].mean()) if g.any() else None
    return out


# Two-sided 97.5 % Student t quantiles by degrees of freedom (replications - 1), for the 95 % interval.
_T975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
         11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131, 19: 2.093, 24: 2.064, 29: 2.045, 39: 2.023, 49: 2.010,
         59: 2.001, 79: 1.990, 99: 1.984}


def t975(df: int) -> float:
    if df in _T975:
        return _T975[df]
    below = [d for d in _T975 if d <= df]
    return _T975[max(below)] if below else float("inf")       # fewer degrees of freedom: the wider (safe) quantile


def interval(values: Sequence[Optional[float]], *, ratio: bool = False) -> Optional[Dict[str, float]]:
    """Mean over replications with its 95 % t interval; None when no replication has the statistic. n is the number
    of replications that have it. With ratio=True the statistic is a share (a coverage): its interval is cut to
    [0, 1], which a t interval of few or sparse replications otherwise leaves (a single replication gives [0, 1])."""
    v = [x for x in values if x is not None]
    if not v:
        return None
    n, mean = len(v), sum(v) / len(v)
    lo, hi = -math.inf, math.inf
    if n >= 2:
        half = t975(n - 1) * math.sqrt(sum((x - mean) ** 2 for x in v) / (n - 1) / n)
        lo, hi = mean - half, mean + half
    if ratio:
        lo, hi = max(0.0, lo), min(1.0, hi)
    return {"mean": mean, "lo": lo, "hi": hi, "n": n}


GROUPS = ("low", "mid", "high", "ramp", "plateau", "near_zero", "zero_forecast")
RATIO_STATS = ("coverage",) + tuple(f"coverage_{g}" for g in GROUPS) + ("unbounded_share",)


def pooled_counts(rows: Sequence[Dict[str, Optional[float]]]) -> Dict[str, Dict[str, int]]:
    """Per group, over all replications: the windows, the covered ones, and the replications that have any. A sparse
    group (zero forecasts in a few replications only) is read from these counts, not from a mean alone."""
    out = {}
    for g in GROUPS:
        have = [r for r in rows if r.get(f"windows_{g}")]
        out[g] = {"windows": sum(r[f"windows_{g}"] for r in have),
                  "covered": sum(int(round(r[f"coverage_{g}"] * r[f"windows_{g}"])) for r in have),
                  "replications": len(have)}
    return out


def reading(ci: Optional[Dict[str, float]], tolerance: float) -> str:
    """MET | NOT MET | INCONCLUSIVE against nominal +- tolerance (module docstring)."""
    if ci is None:
        return "NOT MEASURED"
    nominal = 1.0 - NOMINAL_BPS / cal.BPS
    if abs(ci["mean"] - nominal) > tolerance:
        return "NOT MET"
    return "MET" if ci["lo"] >= nominal - tolerance and ci["hi"] <= nominal + tolerance else "INCONCLUSIVE"


STATS = ("coverage", "coverage_low", "coverage_mid", "coverage_high", "coverage_ramp", "coverage_plateau",
         "coverage_near_zero", "coverage_zero_forecast", "pinball_rps", "width_rps", "width_over_level",
         "unbounded_share")


def run(seeds: Sequence[int], configs: Sequence[Tuple[int, int, bool]] = CONFIGS,
        models: Sequence[str] = tuple(MODEL), *, days: int = PATH_DAYS, kernel: str = "exact", progress=None) -> dict:
    """Every model, configuration and method over the given seeds: per-replication statistics and their intervals."""
    cells = []
    for name in models:
        model = MODEL[name]
        for grid, lead, criterion in configs:
            timing = Timing(grid=grid)
            p = policy(lead)
            # The fast kernel is exact only while the retention never binds (FastCalibration); otherwise the product's
            # own functions are used whatever was asked.
            kern = kernel if p.max_scores * lead < p.retention_seconds - DAY else "exact"
            reps = {"two_part": [], "additive": []}
            for seed in seeds:
                x = signal(model, seed, days, grid)
                for method in reps:
                    reps[method].append(measure(simulate(x, lead, method=method, kernel=kern, timing=timing), model,
                                                grid))
                if progress:
                    progress(name, grid, lead, seed)
            for method, rows in reps.items():
                summary = {s: interval([r.get(s) for r in rows], ratio=s in RATIO_STATS) for s in STATS}
                verdicts = {"marginal": reading(summary["coverage"], MARGINAL_TOLERANCE)}
                for g in ("low", "mid", "high"):
                    verdicts[f"tercile_{g}"] = reading(summary[f"coverage_{g}"], TERCILE_TOLERANCE)
                for g in ("ramp", "plateau", "near_zero"):
                    verdicts[g] = reading(summary[f"coverage_{g}"], TERCILE_TOLERANCE)
                cells.append({"model": name, "criterion": model.criterion and criterion, "grid": grid, "lead": lead,
                              "method": method, "kernel": kern, "replications": rows, "summary": summary,
                              "verdicts": verdicts, "windows": sum(r["windows"] for r in rows),
                              "pooled": pooled_counts(rows)})
    doc = {"schema": "calibration-acceptance/v2", "calibration_method": cal.CALIBRATION_POLICY_REVISION,
           "seeds": list(seeds), "configs": [list(c) for c in configs], "days": days,
           "measure_from_day": MEASURE_FROM_DAY, "nominal_bps": NOMINAL_BPS, "kernel": kernel, "terciles": TERCILES,
           "tolerances": {"marginal": MARGINAL_TOLERANCE, "tercile": TERCILE_TOLERANCE}, "cells": cells}
    doc["sha256"] = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()
    return doc


def _fmt(ci: Optional[Dict[str, float]], scale: float = 100.0, digits: int = 1) -> str:
    if ci is None:
        return "n/a"
    return f"{ci['mean'] * scale:.{digits}f} [{ci['lo'] * scale:.{digits}f}, {ci['hi'] * scale:.{digits}f}]"


def _counts(c: dict, group: str) -> str:
    """A sparse group with its counts: mean coverage [95 %, within 0..100], covered / windows, replications."""
    p = c.get("pooled", {}).get(group)
    if not p or not p["windows"]:
        return "no window"
    return (f"{_fmt(c['summary'][f'coverage_{group}'])}; {p['covered']} / {p['windows']} windows in "
            f"{p['replications']} replications")


def table(doc: dict) -> str:
    """The acceptance table as Markdown: coverage in percent with its 95 % interval (never outside 0..100); the
    zero-forecast windows with their counts; pinball and width in RPS."""
    head = ("| Model | Grid / lead | Method | Marginal | Low tercile | Mid tercile | High tercile | Ramp | Plateau | "
            "Near zero | Zero forecast | Pinball (RPS) | Width (RPS) | Criteria |")
    out = [head, "|" + "---|" * 14]
    for c in doc["cells"]:
        s, v = c["summary"], c["verdicts"]
        crit = ", ".join(f"{k} {v[k]}" for k in ("marginal", "tercile_low", "tercile_mid", "tercile_high"))
        out.append(f"| {c['model']} | {c['grid']} / {c['lead']} s | {c['method']} | {_fmt(s['coverage'])} | "
                   f"{_fmt(s['coverage_low'])} | {_fmt(s['coverage_mid'])} | {_fmt(s['coverage_high'])} | "
                   f"{_fmt(s['coverage_ramp'])} | {_fmt(s['coverage_plateau'])} | {_fmt(s['coverage_near_zero'])} | "
                   f"{_counts(c, 'zero_forecast')} | "
                   f"{_fmt(s['pinball_rps'], 1.0, 4)} | {_fmt(s['width_rps'], 1.0, 3)} | "
                   f"{crit if c['criterion'] else 'reported separately'} |")
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--seeds", required=True, choices=("development", "validation"))
    ap.add_argument("--configs", default=",".join(f"{g}:{lead}" for g, lead, _ in CONFIGS),
                    help="grid:lead pairs; only the declared ones carry criteria")
    ap.add_argument("--models", default=",".join(MODEL))
    ap.add_argument("--kernel", default="exact", choices=("exact", "fast"))
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    seeds = DEVELOPMENT_SEEDS if a.seeds == "development" else VALIDATION_SEEDS
    declared = {(g, lead): crit for g, lead, crit in CONFIGS}
    configs = []
    for item in a.configs.split(","):
        g, lead = (int(v) for v in item.split(":"))
        configs.append((g, lead, declared.get((g, lead), False)))
    doc = run(seeds, configs, a.models.split(","), kernel=a.kernel,
              progress=lambda m, g, lead, s: print(f"{m} grid {g} lead {lead} seed {s}", file=sys.stderr, flush=True))
    doc["seed_set"] = a.seeds
    with open(a.out, "w") as fh:
        json.dump(doc, fh, indent=1, sort_keys=True)
    print(table(doc))
    print(f"\nresult sha256 {doc['sha256']} -> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
