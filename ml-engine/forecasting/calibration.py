"""Window calibration for forecast contract v2: a calibrated upper bound for the maximum over the issued window.

The target is Y_t = max{x(s): s in W_t}, the exact maximum of the sampled RPS over the immutable window W_t = {t+Δ, ...,
t+L} (forecasting.maturation). The point forecast of that target is b_t = max(point_rps over W_t). We calibrate the
scalar Y_t - b_t directly; we never take the maximum of per-step quantiles and call it joint coverage (design C2,
sections 5 and 6).

  - Score (method normalized-window-score/two-part-scale/v1): s_i = (Y_i - b_i) / sigma_i, signed (never truncated at
    zero), with the two-part scale sigma_i = max(s_min, a + beta * b_i) in RPS. beta = 0 is an additive score, a = 0 a
    proportional one. The scale (s_min, a, beta) in force when a window was ISSUED is recorded in its issuance and is
    the one its score is computed with when the outcome matures: a score is never reinterpreted with a later scale. A
    window issued without a scale (none could be fitted yet) has no bound and leaves no score.
  - Scale fit (fit_scale; strictly prior data, deterministic). From the finalized history at an issuance, every pair
    of observed samples one season apart gives a residual r = x(t) - x(t - season) (the error of the seasonal-naive
    forecast of x(t)) and a level l = (x(t) + x(t - season)) / 2. The level is the pair's mean, not the forecast
    itself: under equal noise in both samples the mean is uncorrelated with the difference, so sorting by it does not
    select residuals by their own noise (sorting by the forecast does, and understates the scale wherever the level
    stays flat). The pairs are sorted by level into `scale_bins` equal-count bins; each bin has a
    level L_j (median) and a robust scale S_j = 1.4826 x the median absolute deviation of its residuals about their
    median. (a, beta) is the least-squares line S ~ a + beta L through the bins under a >= 0 and beta >= 0, and
    s_min = scale_floor x the pooled robust scale (not the metric's resolution). The fit needs pairs for at least
    `scale_min_season` of one season; with fewer there is no scale. It is made at the first decision origin that
    has enough pairs and again at the first decision origin of each UTC day, never in between (the service keeps
    the scale in force in its checkpointed state). Only the SHAPE of the scale matters for coverage: a constant
    factor is absorbed by the calibrated quantile. Failure modes are listed at fit_scale.
  - Cohort: windows whose decision origin is a multiple of L (origin mod L == 0) do not overlap in event time. Only
    cohort windows enter the score buffer and the adaptive update; every scored window still counts in the all-window
    diagnostics. This reduces overlap, not serial dependence.
  - Buffer: the cohort scores of windows that ended within the retention (7 days) before the newest one, at most
    max_scores (512), the newest kept.
  - Bound: with n buffered scores and working miscoverage a_t, k = ceil((n + 1)(1 - a_t)). If n < min_windows (128)
    or k > n the upper bound is unavailable (None): never the sample maximum, never a zero margin. Otherwise
    q = s_(k) (the k-th smallest normalized score) and U_t = max(0, b_t + q * sigma_t) with sigma_t the scale in
    force at b_t. Ties do not matter: s_(k) is a value.
  - Adaptive level: after a cohort window with an issued bound matures, miss = 1[Y > U] for the bound ACTUALLY issued,
    and a <- clip(a + eta (alpha - miss), a_min, a_max), starting at alpha. A window issued without a bound (cold
    start) seeds the buffer but cannot claim coverage or move the level. Unscorable windows do neither.
  - Exactly once: records carry the journal sequence number of their outcome; a record at or below the last applied
    sequence is ignored, so a replayed or retried outcome never updates the level twice. Records are applied in
    journal order, which is window-end order within each processing tick; a delayed outcome is applied when it
    matures and never rewrites a bound already issued.
  - Source: a record is built only from a committed issuance bundle and its terminal outcome (from_journal). The
    outcome's model selects that model's issued values in the bundle (its candidate); the outcome must have been
    scored on the issued target grid (same digest and count) against that candidate's base and bound. Replica counts,
    caps and actions never enter: a capped action is not a forecast and cannot calibrate one.
  - Per model: a state belongs to one epoch, one model and one recipe. Each candidate of a multi-model issuance has
    its own scores, level and bound; a record of another model or recipe is refused, so two models never pool their
    windows to meet the minimum count.
The level is held as an integer in units of 1e-8 (bps x bps), so ranks and updates are exact and deterministic. This
delayed-feedback, clipped, finite-buffer variant is conformal-STYLE (after Gibbs and Candès, NeurIPS 2021): it inherits
none of the unmodified method's guarantees, and coverage must be measured, not assumed. Normalizing by a fitted scale
does not supply conditional coverage either: evaluation/calibration_acceptance.py measures it on declared noise models.

A state belongs to one method (STATE_SCHEMA and the policy's method): a buffer of the earlier additive method
(window-calibration/v1, scores in RPS) is refused, never read as normalized scores. The method is part of the
calibration settings, so it is in the settings fingerprint and in the evidence key: old evidence stays in its own epoch.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from forecasting.contracts import CALIBRATION_POLICY_REVISION, ForecastSettings
from forecasting.maturation import candidate, targets_sha256

STATE_SCHEMA = "window-calibration/v2"
E8 = 100_000_000                       # the working level's unit: 1e-8
BPS = 10_000
MAD_TO_SD = 1.4826                     # median absolute deviation -> standard deviation (normal)
MEAN_AD_TO_SD = 1.2533                 # mean absolute deviation -> standard deviation (normal)


class CalibrationError(Exception):
    """A record that cannot be applied to this state (another epoch, inconsistent with its issuance)."""


@dataclass(frozen=True)
class CalibrationPolicy:
    lead_seconds: int
    nominal_bps: int = 1000
    eta_bps: int = 50
    min_bps: int = 100
    max_bps: int = 2500
    min_windows: int = 128
    max_scores: int = 512
    retention_seconds: int = 7 * 86400
    method: str = CALIBRATION_POLICY_REVISION
    scale_bins: int = 8
    scale_floor_bps: int = 500
    scale_min_season_bps: int = 8000

    def __post_init__(self):
        if not (0 < self.min_bps <= self.nominal_bps <= self.max_bps < BPS) or self.lead_seconds <= 0 or \
                self.min_windows < 1 or self.max_scores < self.min_windows or not 0 <= self.eta_bps <= BPS:
            raise ValueError(f"inconsistent calibration policy {self}")
        if self.method != CALIBRATION_POLICY_REVISION:
            raise ValueError(f"unknown calibration method {self.method!r}")
        if self.scale_bins < 2 or not 0 < self.scale_floor_bps <= BPS or not 0 < self.scale_min_season_bps <= BPS:
            raise ValueError(f"inconsistent scale-fit constants in {self}")

    @classmethod
    def from_settings(cls, settings: ForecastSettings, lead_seconds: int) -> "CalibrationPolicy":
        c = settings.calibration
        return cls(lead_seconds, settings.risk.miscoverage_bps, c.eta_bps, c.min_miscoverage_bps,
                   c.max_miscoverage_bps, c.min_windows, c.max_scores, c.retention_days * 86400, c.method,
                   c.scale_bins, c.scale_floor_bps, c.scale_min_season_bps)

    def min_pairs(self, season_slots: int) -> int:
        """The day-lag pairs a scale fit needs on a grid with `season_slots` samples per season."""
        return max(4 * self.scale_bins, -(-season_slots * self.scale_min_season_bps // BPS))

    @property
    def nominal_ratio(self) -> float:
        return self.nominal_bps / BPS


# --- the two-part scale -----------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Scale:
    """sigma(b) = max(s_min, a + beta * b) in RPS, for a point maximum b >= 0."""
    s_min: float
    a: float
    beta: float

    def __post_init__(self):
        vals = (self.s_min, self.a, self.beta)
        if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) for v in vals) or \
                self.s_min <= 0 or self.a < 0 or self.beta < 0:
            raise CalibrationError(f"a scale needs finite s_min > 0, a >= 0 and beta >= 0, got {vals}")

    def at(self, base_rps: float) -> float:
        return max(self.s_min, self.a + self.beta * base_rps)

    def to_wire(self) -> Dict[str, float]:
        return {"s_min": self.s_min, "a": self.a, "beta": self.beta}

    @classmethod
    def from_wire(cls, w) -> "Scale":
        if not isinstance(w, Mapping) or set(w) != {"s_min", "a", "beta"}:
            raise CalibrationError("a recorded scale holds exactly s_min, a and beta")
        return cls(w["s_min"], w["a"], w["beta"])


# sigma = 1 RPS at every level: the additive score Y - b. Used by callers that DECLARE it (the offline replay, tests)
# and returned by fit_scale only for a history without any variation, where every score is 0 whatever the scale.
UNIT_SCALE = Scale(1.0, 1.0, 0.0)


def season_pairs(values: Sequence[Optional[float]], season_slots: int) -> Tuple[List[float], List[float]]:
    """(levels, residuals) of every pair of observed samples one season apart in a grid series (None or NaN: not
    observed): level = the mean of the two, residual = the later minus the earlier. Grid order."""
    levels, residuals = [], []
    for i in range(season_slots, len(values)):
        x, lag = values[i], values[i - season_slots]
        if x is None or lag is None or not (math.isfinite(x) and math.isfinite(lag)):
            continue
        levels.append(0.5 * (float(x) + float(lag)))
        residuals.append(float(x) - float(lag))
    return levels, residuals


def _median(sorted_vals: Sequence[float]) -> float:
    n = len(sorted_vals)
    return sorted_vals[n // 2] if n % 2 else 0.5 * (sorted_vals[n // 2 - 1] + sorted_vals[n // 2])


def _mad(vals: Sequence[float]) -> Tuple[float, float]:
    """(median, median absolute deviation about it)."""
    med = _median(sorted(vals))
    return med, _median(sorted(abs(v - med) for v in vals))


def _round(v: float) -> float:
    return float(f"{v:.12g}")


def fit_scale(levels: Sequence[float], residuals: Sequence[float], policy: "CalibrationPolicy",
              season_slots: int) -> Optional[Scale]:
    """The two-part scale fitted to day-lag pairs (module docstring), or None with fewer pairs than the rule needs.

    Deterministic: the pairs are ordered by (level, position), every statistic is a median or a closed-form
    least-squares solution, and the result is rounded to 12 significant digits.

    Failure modes (none is silent about coverage, which the trust gates keep measuring):
      - no variation at all (every residual zero, e.g. byte-identical days): UNIT_SCALE; every score is then 0 and
        the bound equals the point forecast, exactly as with the additive method;
      - more than half the residuals of a bin equal (zero-inflated or coarsely quantized load): that bin's robust
        scale is 0. If every bin is 0 the scale is the constant pooled mean absolute deviation; otherwise the line
        is fitted through the zeros and s_min keeps sigma positive;
      - a narrow level range (a flat workload): a and beta are not identified separately; the line still matches
        the scale at the observed levels, and an excursion far outside them is extrapolated linearly;
      - a scale that depends on something other than the level (time of day, a ramp's slope) or is not close to
        a + beta * level: the shape is wrong and coverage tilts with the level; calibration then holds marginally
        only, as it did before;
      - a history that is not representative (the first days after a launch): the next daily fit corrects the
        scale, and the scores of the earlier scale leave the buffer as they age."""
    n = len(levels)
    if n != len(residuals):
        raise CalibrationError("levels and residuals differ in length")
    if any(not math.isfinite(v) for v in levels) or any(not math.isfinite(v) for v in residuals):
        raise CalibrationError("a scale is fitted to finite values")
    if n < policy.min_pairs(season_slots):
        return None
    med, mad = _mad(residuals)
    pooled = MAD_TO_SD * mad
    if pooled <= 0:
        pooled = MEAN_AD_TO_SD * sum(abs(r - med) for r in residuals) / n
    if pooled <= 0:
        return UNIT_SCALE
    order = sorted(range(n), key=lambda i: (levels[i], i))
    bins = policy.scale_bins
    ls, ss = [], []
    for j in range(bins):
        idx = order[j * n // bins:(j + 1) * n // bins]
        ls.append(_median(sorted(levels[i] for i in idx)))
        ss.append(MAD_TO_SD * _mad([residuals[i] for i in idx])[1])
    ml, ms = sum(ls) / bins, sum(ss) / bins
    var = sum((x - ml) ** 2 for x in ls)
    beta = sum((x - ml) * (y - ms) for x, y in zip(ls, ss)) / var if var > 0 else 0.0
    a = ms - beta * ml
    if beta < 0:
        a, beta = ms, 0.0
    elif a < 0:
        sq = sum(x * x for x in ls)
        a, beta = 0.0, (sum(x * y for x, y in zip(ls, ss)) / sq if sq > 0 else 0.0)
    if a + beta * ml <= 0:                                  # every bin's robust scale is zero
        a, beta = pooled, 0.0
    return Scale(_round(policy.scale_floor_bps / BPS * pooled), _round(a), _round(max(0.0, beta)))


def rank(n: int, a_e8: int) -> int:
    """k = ceil((n + 1)(1 - a)) in exact integer arithmetic (a in 1e-8 units)."""
    return -(-(n + 1) * (E8 - a_e8) // E8)


def in_cohort(decision_origin: int, lead_seconds: int) -> bool:
    return decision_origin % lead_seconds == 0


def pinball_rps(y: float, upper: float, miscoverage: float) -> float:
    """Pinball loss of the bound at quantile level tau = 1 - miscoverage."""
    tau = 1.0 - miscoverage
    return tau * (y - upper) if y >= upper else (1.0 - tau) * (upper - y)


def initial(policy: CalibrationPolicy, epoch: str, model: str, recipe: str) -> dict:
    return {"schema": STATE_SCHEMA, "method": policy.method, "epoch": epoch, "model": model, "recipe": recipe,
            "a_e8": policy.nominal_bps * BPS, "scores": [],
            "last_seq": 0, "newest_window_end": None,
            "all": {"scored": 0, "unscorable": 0, "bounded": 0, "covered": 0, "abs_error_sum_rps": 0.0},
            "gate": {"scored": 0, "unscorable": 0, "unscaled": 0, "bounded": 0, "covered": 0, "pinball_sum_rps": 0.0},
            "level_updates": 0, "at_bound": 0}


def _same_method(state: dict, policy: CalibrationPolicy) -> None:
    if state.get("schema") != STATE_SCHEMA or state.get("method") != policy.method:
        raise CalibrationError(f"the calibration state belongs to another method ({state.get('schema')!r}, "
                               f"{state.get('method')!r}): its scores cannot be read as {policy.method} scores")


@dataclass(frozen=True)
class CalibrationRecord:
    seq: int                         # the outcome's journal sequence number
    epoch: str
    model: str
    recipe: str
    issuance_id: str
    decision_origin: int
    window_end: int
    base_rps: float                  # b_t as issued
    upper_rps: Optional[float]       # U_t as issued (None: issued without a bound)
    status: str                      # scored | unscorable
    y_rps: Optional[float]           # Y_t (scored only)
    scale: Optional[Scale] = None    # the scale recorded AT ISSUANCE (None: issued without one, so without a bound)

    @classmethod
    def from_journal(cls, issuance, outcome) -> "CalibrationRecord":
        """From a committed forecasting.store Issuance and its Outcome; refuses anything that does not agree."""
        b, p = issuance.payload(), outcome.payload()
        if outcome.issuance_id != issuance.issuance_id or outcome.evidence_key != issuance.key.evidence_key or \
                outcome.window_end != issuance.key.window_end:
            raise CalibrationError("the outcome does not belong to this issuance")
        targets, dt, origin = b["target_timestamps"], b["sample_interval_seconds"], issuance.key.decision_origin
        if b["decision_origin"] != origin or targets != list(range(origin + dt, issuance.key.window_end + 1, dt)):
            raise CalibrationError("the issuance's targets are not exactly its window's grid")
        if outcome.model not in issuance.models:
            raise CalibrationError(f"the issuance expected no outcome for model {outcome.model!r}")
        try:
            c = candidate(b, outcome.model)
        except (ValueError, KeyError, TypeError) as e:
            raise CalibrationError(str(e)) from e
        if len(c["point_rps"]) != len(targets):
            raise CalibrationError("the candidate's trajectory does not cover the issued targets")
        if (p.get("targets"), p.get("targets_sha256")) != (len(targets), targets_sha256(targets)):
            raise CalibrationError("the outcome was not matured on the issued target grid")
        base, upper = c["base_window_max_rps"], c["upper_window_max_rps"]
        if outcome.status == "scored" and (p["base_window_max_rps"], p["upper_window_max_rps"]) != (base, upper):
            raise CalibrationError("the outcome was not scored against the bound that was issued")
        # The scale as recorded in the immutable issuance (a multi-model bundle keeps it per candidate, a
        # single-model bundle at the top level). No recorded scale is never read as some default scale.
        info = c.get("calibration") if "candidates" in b else b.get("calibration")
        wire = info.get("scale") if isinstance(info, Mapping) else None
        scale = None if wire is None else Scale.from_wire(wire)
        if scale is None and upper is not None:
            raise CalibrationError("the issuance carries a bound but no scale: it was not issued by this method")
        return cls(outcome.seq, issuance.key.evidence_key, outcome.model, c["recipe"], issuance.issuance_id,
                   origin, issuance.key.window_end, float(base), None if upper is None else float(upper),
                   outcome.status, float(p["y_window_max_rps"]) if outcome.status == "scored" else None, scale)


def buffered_scores(state: dict, policy: CalibrationPolicy, now_s: int) -> List[float]:
    """The cohort scores usable at now_s: windows that ended within the retention before now (newest max_scores)."""
    return [e for end, e in state["scores"] if now_s - policy.retention_seconds < end <= now_s][-policy.max_scores:]


def upper_bound(state: dict, policy: CalibrationPolicy, base_rps: float, now_s: int,
                scale: Optional[Scale]) -> Tuple[Optional[float], dict]:
    """(U or None, details) at issue time now_s for a window whose point maximum is base_rps, from the scores that
    had matured by then and the scale in force (None: no scale could be fitted yet, so no bound). The details carry
    the scale: they are what the issuance records."""
    _same_method(state, policy)
    if not math.isfinite(base_rps) or base_rps < 0:
        raise CalibrationError("the point maximum must be a finite, non-negative rate")
    scores = sorted(buffered_scores(state, policy, now_s))
    n, a = len(scores), state["a_e8"]
    k = rank(n, a)
    info = {"n": n, "rank": k, "working_miscoverage": a / E8, "scale": None if scale is None else scale.to_wire()}
    if scale is None:
        return None, {**info, "unavailable": "ScaleUnavailable"}
    if n < policy.min_windows:
        return None, {**info, "unavailable": "CalibrationInsufficient"}
    if k > n:
        return None, {**info, "unavailable": "RankOverflow"}
    q, sigma = scores[k - 1], scale.at(base_rps)
    return max(0.0, base_rps + q * sigma), {**info, "quantile": q, "sigma_rps": sigma}


def apply(state: dict, record: CalibrationRecord, policy: CalibrationPolicy) -> dict:
    """The state after one matured record; unchanged when the record was already applied (seq <= last_seq)."""
    _same_method(state, policy)
    if record.epoch != state["epoch"]:
        raise CalibrationError("the record belongs to another calibration epoch")
    if (record.model, record.recipe) != (state["model"], state["recipe"]):
        raise CalibrationError("the record belongs to another model or recipe")
    if record.seq <= state["last_seq"]:
        return state
    if record.status not in ("scored", "unscorable"):
        raise CalibrationError(f"unknown outcome status {record.status!r}")
    s = {**state, "all": dict(state["all"]), "gate": dict(state["gate"]), "scores": [list(x) for x in state["scores"]]}
    s["last_seq"] = record.seq
    cohort = in_cohort(record.decision_origin, policy.lead_seconds)
    if record.status == "unscorable":
        s["all"]["unscorable"] += 1
        if cohort:
            s["gate"]["unscorable"] += 1
        return s
    y, b, u = record.y_rps, record.base_rps, record.upper_rps
    if y is None or not math.isfinite(y):
        raise CalibrationError("a scored record carries a finite Y")
    s["all"]["scored"] += 1
    s["all"]["abs_error_sum_rps"] += abs(y - b)
    if u is not None:
        s["all"]["bounded"] += 1
        s["all"]["covered"] += int(y <= u)
    if not cohort:
        return s
    s["gate"]["scored"] += 1
    if record.scale is None:                                  # issued without a scale: no score, and no bound either
        s["gate"]["unscaled"] += 1
        return s
    s["scores"].append([record.window_end, (y - b) / record.scale.at(b)])   # the scale recorded at issuance
    s["scores"].sort(key=lambda x: x[0])                       # a delayed record lands in its window-end place
    newest = max(record.window_end, s["newest_window_end"] or record.window_end)
    s["newest_window_end"] = newest
    s["scores"] = [x for x in s["scores"] if x[0] > newest - policy.retention_seconds][-policy.max_scores:]
    if u is not None:                                         # only a bound actually issued moves the level
        miss = int(y > u)
        s["gate"]["bounded"] += 1
        s["gate"]["covered"] += 1 - miss
        s["gate"]["pinball_sum_rps"] += pinball_rps(y, u, policy.nominal_ratio)
        a = s["a_e8"] + policy.eta_bps * (policy.nominal_bps - miss * BPS)
        lo, hi = policy.min_bps * BPS, policy.max_bps * BPS
        s["at_bound"] += int(a <= lo or a >= hi)
        s["a_e8"] = min(hi, max(lo, a))
        s["level_updates"] += 1
    return s


def coverage(state: dict, cohort: str = "gate") -> Dict[str, Optional[float]]:
    """{"covered", "bounded", "ratio"}: ratio = covered / bounded over issued bounds, None when nothing was bounded
    (never 100% of nothing). Cumulative over the epoch; time-windowed gates belong to the trust state (C2.5)."""
    c = state[cohort]
    return {"covered": c["covered"], "bounded": c["bounded"],
            "ratio": c["covered"] / c["bounded"] if c["bounded"] else None}
