"""Window calibration for forecast contract v2: a calibrated upper bound for the maximum over the issued window.

The target is Y_t = max{x(s): s in W_t}, the exact maximum of the sampled RPS over the immutable window W_t = {t+Δ, ...,
t+L} (forecasting.maturation). The point forecast of that target is b_t = max(point_rps over W_t). We calibrate the
scalar Y_t - b_t directly; we never take the maximum of per-step quantiles and call it joint coverage (design C2,
sections 5 and 6).

  - Score: e_i = Y_i - b_i in RPS, signed (never truncated at zero).
  - Cohort: windows whose decision origin is a multiple of L (origin mod L == 0) do not overlap in event time. Only
    cohort windows enter the score buffer and the adaptive update; every scored window still counts in the all-window
    diagnostics. This reduces overlap, not serial dependence.
  - Buffer: the cohort scores of windows that ended within the retention (7 days) before the newest one, at most
    max_scores (512), the newest kept.
  - Bound: with n buffered scores and working miscoverage a_t, k = ceil((n + 1)(1 - a_t)). If n < min_windows (128)
    or k > n the upper bound is unavailable (None): never the sample maximum, never a zero margin. Otherwise
    q = e_(k) (the k-th smallest) and U_t = max(0, b_t + q). Ties do not matter: e_(k) is a value.
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
none of the unmodified method's guarantees, and coverage must be measured, not assumed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from forecasting.contracts import ForecastSettings
from forecasting.maturation import candidate, targets_sha256

STATE_SCHEMA = "window-calibration/v1"
E8 = 100_000_000                       # the working level's unit: 1e-8
BPS = 10_000


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

    def __post_init__(self):
        if not (0 < self.min_bps <= self.nominal_bps <= self.max_bps < BPS) or self.lead_seconds <= 0 or \
                self.min_windows < 1 or self.max_scores < self.min_windows or not 0 <= self.eta_bps <= BPS:
            raise ValueError(f"inconsistent calibration policy {self}")

    @classmethod
    def from_settings(cls, settings: ForecastSettings, lead_seconds: int) -> "CalibrationPolicy":
        c = settings.calibration
        return cls(lead_seconds, settings.risk.miscoverage_bps, c.eta_bps, c.min_miscoverage_bps,
                   c.max_miscoverage_bps, c.min_windows, c.max_scores, c.retention_days * 86400)

    @property
    def nominal_ratio(self) -> float:
        return self.nominal_bps / BPS


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
    return {"schema": STATE_SCHEMA, "epoch": epoch, "model": model, "recipe": recipe,
            "a_e8": policy.nominal_bps * BPS, "scores": [],
            "last_seq": 0, "newest_window_end": None,
            "all": {"scored": 0, "unscorable": 0, "bounded": 0, "covered": 0, "abs_error_sum_rps": 0.0},
            "gate": {"scored": 0, "unscorable": 0, "bounded": 0, "covered": 0, "pinball_sum_rps": 0.0},
            "level_updates": 0, "at_bound": 0}


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
        return cls(outcome.seq, issuance.key.evidence_key, outcome.model, c["recipe"], issuance.issuance_id,
                   origin, issuance.key.window_end, float(base), None if upper is None else float(upper),
                   outcome.status, float(p["y_window_max_rps"]) if outcome.status == "scored" else None)


def buffered_scores(state: dict, policy: CalibrationPolicy, now_s: int) -> List[float]:
    """The cohort scores usable at now_s: windows that ended within the retention before now (newest max_scores)."""
    return [e for end, e in state["scores"] if now_s - policy.retention_seconds < end <= now_s][-policy.max_scores:]


def upper_bound(state: dict, policy: CalibrationPolicy, base_rps: float, now_s: int) -> Tuple[Optional[float], dict]:
    """(U or None, details) at issue time now_s for a window whose point maximum is base_rps, from the scores that
    had matured by then."""
    if not math.isfinite(base_rps) or base_rps < 0:
        raise CalibrationError("the point maximum must be a finite, non-negative rate")
    scores = sorted(buffered_scores(state, policy, now_s))
    n, a = len(scores), state["a_e8"]
    k = rank(n, a)
    info = {"n": n, "rank": k, "working_miscoverage": a / E8}
    if n < policy.min_windows:
        return None, {**info, "unavailable": "CalibrationInsufficient"}
    if k > n:
        return None, {**info, "unavailable": "RankOverflow"}
    q = scores[k - 1]
    return max(0.0, base_rps + q), {**info, "quantile_rps": q}


def apply(state: dict, record: CalibrationRecord, policy: CalibrationPolicy) -> dict:
    """The state after one matured record; unchanged when the record was already applied (seq <= last_seq)."""
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
    s["scores"].append([record.window_end, y - b])
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
