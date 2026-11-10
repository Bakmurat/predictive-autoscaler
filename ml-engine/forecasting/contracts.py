"""Forecast contract v2 (window-max-rps/v1): typed request, resolved settings, forecast window and response.

V2 forecasts the maximum of the sampled request rate (RPS of the compiled query, signal contract requests-per-second/v1)
over an immutable future window W_t = {t+Δ, ..., t+L}, where t is the canonical decision origin on the sample grid, Δ
the cadence and L the effective lead. Units are requests/second throughout; nothing here converts to req/min, and the
v1 /predict RPM array is never populated from a v2 answer.

The caller sends a reference (the v1 identity fields), the forecast-contract version, the decision origin and the hash
of the settings it resolved. bind_request() resolves the reference with api.identity (one read of the live autoscaler
and its Deployment) and derives every setting from that same autoscaler object: the caller cannot inject history, a
query, a model, a looser risk or a wider horizon. Unknown request fields are refused rather than ignored.

Settings come from spec.prediction. The v2 keys are proposals until the CRD carries them (design C2, section 4); absent
keys take the defaults below, unknown keys are refused, and the fingerprint covers every resolved value. Ratios are
integer basis points so the fingerprint is exact. The lead L comes from status.prediction.lead (written by the operator,
bound to the observed generation); without a valid one no window can be issued (reason LeadInsufficient).

Refusals are ContractRefused (HTTP 422); identity refusals and lookup failures keep api.identity's ProvenanceRefused
(422) and LookupFailed (503). No model code is imported here.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, Mapping, Optional, Tuple

from api.identity import API_GROUP, API_VERSION, CONTRACT as SIGNAL_CONTRACT, KubeReader, ResolvedSignal, \
    resolve_signal

FORECAST_CONTRACT = "window-max-rps/v1"
SETTINGS_SCHEMA = "forecast-settings/v1"
EVIDENCE_SCHEMA = "forecast-evidence-key/v1"
# The calibration-policy revision (the calibration method): part of the settings fingerprint AND of the evidence key
# (through the calibration settings, as their field `method`), so a change of the score's meaning starts a new
# evidence epoch on both sides and can never be read as the old one. Revision 1 (implicit, never named) was the
# additive score Y - b in RPS. A stable name: other modules pin and compare it.
CALIBRATION_POLICY_REVISION = "normalized-window-score/two-part-scale/v1"
# The trust-policy revision (forecasting.trust): what the coverage rules mean. Part of the settings fingerprint (the
# trust settings' field `policy`) and of the evidence key, so trust earned under other rules is never carried into
# these. Revision 1 (implicit, never named) was "the last 32 gate windows at least 85 % covered" with an unadjusted
# worst-day rule on partial days. A stable name: other modules pin and compare it.
TRUST_POLICY_REVISION = "budgeted-coverage-tests/v1"
UNIT = "requests/second"

# Minute multiples in 60..600 that divide a UTC day, so a one-day lag lands on the same epoch grid. 7- and 9-minute
# cadences are refused: their daily lag falls between grid points and interpolating it would change the contract.
ALLOWED_INTERVALS = (60, 120, 180, 240, 300, 360, 480, 600)
LEGACY_LSTM_INTERVAL = 600
MAX_HORIZON_SECONDS = 3600
MAX_STEPS = 60
MAX_HISTORY_DAYS = 28
ENGINES = ("statistical", "legacyLstm")
SEASONALITIES = ("daily",)            # dailyWeekly is admitted once the weekly implementation and its tests ship
PHASES = ("Collecting", "Calibrating", "Shadow", "Trusted", "Degraded")
REASONS = ("HistoryInsufficient", "CalibrationInsufficient", "ShadowInsufficient", "CoverageLow", "DataUnavailable",
           "IdentityMismatch", "StateUnavailable", "LeadInsufficient", "ReplayGateMissing", "Ready")
MODEL_FAMILIES = ("seasonal_naive", "statistical", "lstm")
LEAD_METHODS = ("configured", "measured")
BPS = 10_000

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TS = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{3}))?Z$")
# The spec.prediction keys of the v1 operator, accepted and either used (horizonMinutes, enabled) or left to it.
_LEGACY_PREDICTION_KEYS = ("enabled", "horizonMinutes", "leadTimeMinutes", "updateIntervalSeconds")


class ContractRefused(Exception):
    """The request, the autoscaler's settings or a response does not satisfy the v2 contract (HTTP 422)."""


# --- timestamps -------------------------------------------------------------------------------------------------
# Grid times are integer epoch seconds; receipt and issue times are integer epoch milliseconds. On the wire both are
# RFC 3339 UTC with a literal Z (grid times without a fraction, receipt times with exactly three digits).

def format_ts(epoch_s: int) -> str:
    if not _is_int(epoch_s):
        raise ContractRefused(f"grid time {epoch_s!r} is not integer seconds")
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_ms(epoch_ms: int) -> str:
    if not _is_int(epoch_ms):
        raise ContractRefused(f"receipt time {epoch_ms!r} is not integer milliseconds")
    s, ms = divmod(epoch_ms, 1000)
    return datetime.fromtimestamp(s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"


def _parse(text, what: str) -> Tuple[int, Optional[int]]:
    m = _TS.match(text) if isinstance(text, str) else None
    if not m:
        raise ContractRefused(f"{what} {text!r} is not an RFC 3339 UTC timestamp ending in Z")
    y, mo, d, h, mi, s, frac = m.groups()
    try:
        dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s), tzinfo=timezone.utc)
    except ValueError as e:
        raise ContractRefused(f"{what} {text!r}: {e}") from e
    return int(dt.timestamp()), (int(frac) if frac is not None else None)


def parse_ts(text, what: str = "timestamp") -> int:
    """A grid time: whole seconds, no fraction."""
    s, frac = _parse(text, what)
    if frac is not None:
        raise ContractRefused(f"{what} {text!r} must be whole seconds")
    return s


def parse_ms(text, what: str = "timestamp") -> int:
    """A receipt time: exactly three fractional digits."""
    s, frac = _parse(text, what)
    if frac is None:
        raise ContractRefused(f"{what} {text!r} must carry milliseconds")
    return s * 1000 + frac


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _finite_nonneg(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v >= 0


def canonical_json(obj) -> bytes:
    """The one serialization that is hashed: sorted keys, no whitespace, ASCII, no NaN/Infinity."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- settings ---------------------------------------------------------------------------------------------------

# Frozen by the threshold-selection procedure of the trust-repair protocol (evaluation/trust_qualification.py).
ACUTE_RECENT_WINDOWS = 64
ROLLING_ALARM_PPM = 35_000
CONSECUTIVE_ALARM_PPM = 7_000
DAY_ALARM_PPM = 8_000
COVERAGE_ALARM_BUDGET_PPM = 50_000           # the target: 5 % per target and 7 days, the three rules together


@dataclass(frozen=True)
class RiskSettings:
    miscoverage_bps: int = 1000              # 90% nominal window coverage; 100..2500 (99%..75%)


@dataclass(frozen=True)
class CalibrationSettings:
    min_windows: int = 128                   # prior nonoverlap scores before an upper bound exists
    max_scores: int = 512                    # trailing buffer size
    retention_days: int = 7
    eta_bps: int = 50                        # adaptive step 0.005
    min_miscoverage_bps: int = 100           # adaptive level bounds 0.01..0.25
    max_miscoverage_bps: int = 2500
    # The method and the constants of its scale-fitting rule (forecasting.calibration). They are resolved, never read
    # from the spec: an autoscaler cannot choose them, and changing one changes the fingerprint and the evidence key.
    method: str = CALIBRATION_POLICY_REVISION
    scale_bins: int = 8                      # equal-count level bins of the scale fit
    scale_floor_bps: int = 500               # s_min = 5 % of the pooled residual scale
    scale_min_season_bps: int = 8000         # the fit needs day-lag pairs for 80 % of one season


@dataclass(frozen=True)
class TrustSettings:
    min_shadow_hours: int = 48
    min_prospective_windows: int = 64
    coverage_tolerance_bps: int = 500        # 7-day gate coverage >= nominal - 0.05
    worst_day_min_outcomes: int = 24         # a completed UTC day is tested once it has this many gate outcomes
    min_availability_bps: int = 9500
    acute_consecutive_misses: int = 5        # the LEAST run of misses that alarms; more where the budget needs it
    acute_recent_windows: int = ACUTE_RECENT_WINDOWS          # n of the rolling binomial-tail detector
    repromotion_hours: int = 24
    repromotion_windows: int = 32
    challenger_min_pairs: int = 64
    challenger_min_hours: int = 48
    challenger_loss_ratio_bps: int = 9500        # enter: pinball <= 0.95 x the naive's
    challenger_exit_loss_ratio_bps: int = 10000  # leave (no safety failure): only once worse than the naive
    challenger_cost_ratio_bps: int = 10200
    max_fit_age_hours: int = 12
    # The policy revision and the false-alarm budgets its thresholds are derived from (forecasting.trust). Resolved,
    # never read from the spec; each is a probability per target and 7 days under independent nominal misses, in ppm,
    # shared equally by the enabled families.
    policy: str = TRUST_POLICY_REVISION
    rolling_alarm_ppm: int = ROLLING_ALARM_PPM
    consecutive_alarm_ppm: int = CONSECUTIVE_ALARM_PPM
    day_alarm_ppm: int = DAY_ALARM_PPM


@dataclass(frozen=True)
class PredictiveExtraSettings:
    max_replicas: int = 2                    # the stricter of the two caps applies; 0 disables predictive excess
    max_percent: int = 50


@dataclass(frozen=True)
class ForecastSettings:
    engine: str = "statistical"
    sample_interval_seconds: int = 300
    seasonality: str = "daily"
    history_days: int = 14
    horizon_seconds: int = 3600
    settlement_seconds: int = 120
    outcome_grace_seconds: int = 600
    risk: RiskSettings = field(default_factory=RiskSettings)
    calibration: CalibrationSettings = field(default_factory=CalibrationSettings)
    trust: TrustSettings = field(default_factory=TrustSettings)
    predictive_extra: PredictiveExtraSettings = field(default_factory=PredictiveExtraSettings)
    families: Tuple[str, ...] = ("seasonalNaive", "statistical")   # sorted; seasonalNaive always present

    @property
    def freshness_seconds(self) -> int:
        """The newest input may be settlement + Δ old by construction; older than settlement + 2Δ is unavailable."""
        return self.settlement_seconds + 2 * self.sample_interval_seconds

    @property
    def steps(self) -> int:
        return self.horizon_seconds // self.sample_interval_seconds

    def canonical(self) -> dict:
        return {"schema": SETTINGS_SCHEMA, "settings": asdict(self)}

    def fingerprint(self) -> str:
        return sha256_hex(canonical_json(self.canonical()))


# (spec key, attribute, lower bound, upper bound) for each integer group; bounds are inclusive.
_RISK = (("miscoverageBps", "miscoverage_bps", 100, 2500),)
_CALIBRATION = (("minWindows", "min_windows", 16, 4096), ("maxScores", "max_scores", 16, 4096),
                ("retentionDays", "retention_days", 1, 28), ("etaBps", "eta_bps", 1, 1000),
                ("minMiscoverageBps", "min_miscoverage_bps", 1, 5000),
                ("maxMiscoverageBps", "max_miscoverage_bps", 1, 5000))
_TRUST = (("minShadowHours", "min_shadow_hours", 1, 24 * 28), ("minProspectiveWindows", "min_prospective_windows", 1, 4096),
          ("coverageToleranceBps", "coverage_tolerance_bps", 0, 2500),
          ("worstDayMinOutcomes", "worst_day_min_outcomes", 1, 1440),
          ("minAvailabilityBps", "min_availability_bps", 0, BPS),
          ("acuteConsecutiveMisses", "acute_consecutive_misses", 1, 1000),
          ("acuteRecentWindows", "acute_recent_windows", 1, 4096),
          ("repromotionHours", "repromotion_hours", 0, 24 * 28),
          ("repromotionWindows", "repromotion_windows", 1, 4096),
          ("challengerMinPairs", "challenger_min_pairs", 1, 4096),
          ("challengerMinHours", "challenger_min_hours", 1, 24 * 28),
          ("challengerLossRatioBps", "challenger_loss_ratio_bps", 1, BPS),
          ("challengerExitLossRatioBps", "challenger_exit_loss_ratio_bps", 1, 2 * BPS),
          ("challengerCostRatioBps", "challenger_cost_ratio_bps", BPS, 2 * BPS),
          ("maxFitAgeHours", "max_fit_age_hours", 1, 24))
_EXTRA = (("maxReplicas", "max_replicas", 0, 1000), ("maxPercent", "max_percent", 0, 1000))
_TOP = ("engine", "sampleIntervalSeconds", "seasonality", "historyDays", "settlementSeconds", "outcomeGraceSeconds",
        "risk", "calibration", "trust", "predictiveExtra", "families")
FAMILIES = ("seasonalNaive", "statistical")
FAMILY_MODELS = {"seasonalNaive": "seasonal_naive", "statistical": "statistical"}   # spec name -> model family


def _object(parent: Mapping, key: str, where: str) -> Mapping:
    v = parent.get(key)
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise ContractRefused(f"{where}.{key} is not an object")
    return v


def _int_group(obj: Mapping, spec, cls, where: str):
    unknown = sorted(set(obj) - {k for k, *_ in spec})
    if unknown:
        raise ContractRefused(f"unknown {where} settings: {', '.join(unknown)}")
    values = {}
    for key, attr, lo, hi in spec:
        if key not in obj:
            continue
        v = obj[key]
        if not _is_int(v) or not lo <= v <= hi:
            raise ContractRefused(f"{where}.{key} must be an integer in {lo}..{hi}, got {v!r}")
        values[attr] = v
    return cls(**values)


def _rank(n: int, miscoverage_bps: int) -> int:
    """k = ceil((n+1)(1-a)) in exact integer arithmetic."""
    return -(-(n + 1) * (BPS - miscoverage_bps) // BPS)


def resolve_settings(pa: Mapping) -> ForecastSettings:
    """The v2 settings of a live autoscaler object (its spec.prediction), validated field by field and across fields."""
    spec = _object(pa, "spec", "") if isinstance(pa, Mapping) else None
    if spec is None:
        raise ContractRefused("the autoscaler is not an object")
    pred = _object(spec, "prediction", "spec")
    unknown = sorted(set(pred) - set(_TOP) - set(_LEGACY_PREDICTION_KEYS))
    if unknown:
        raise ContractRefused(f"unknown spec.prediction settings: {', '.join(unknown)}")
    if pred.get("enabled", True) is not True:
        raise ContractRefused("forecasting is disabled for this autoscaler (spec.prediction.enabled)")
    d = ForecastSettings()
    engine = pred.get("engine", d.engine)
    if engine not in ENGINES:
        raise ContractRefused(f"spec.prediction.engine must be one of {ENGINES}, got {engine!r}")
    seasonality = pred.get("seasonality", d.seasonality)
    if seasonality not in SEASONALITIES:
        raise ContractRefused(f"spec.prediction.seasonality {seasonality!r} is not supported (admitted: {SEASONALITIES})")
    interval = pred.get("sampleIntervalSeconds", d.sample_interval_seconds)
    if not _is_int(interval) or interval not in ALLOWED_INTERVALS:
        raise ContractRefused(f"spec.prediction.sampleIntervalSeconds must be one of {ALLOWED_INTERVALS}, "
                              f"got {interval!r}")
    if engine == "legacyLstm" and interval != LEGACY_LSTM_INTERVAL:
        raise ContractRefused(f"engine legacyLstm supports only the {LEGACY_LSTM_INTERVAL} s grid")
    horizon_min = pred.get("horizonMinutes", d.horizon_seconds // 60)
    if not _is_int(horizon_min) or horizon_min <= 0:
        raise ContractRefused(f"spec.prediction.horizonMinutes must be a positive integer, got {horizon_min!r}")
    horizon = horizon_min * 60
    if horizon > MAX_HORIZON_SECONDS or horizon % interval or horizon // interval > MAX_STEPS:
        raise ContractRefused(f"horizon {horizon_min} min must be at most {MAX_HORIZON_SECONDS // 60} min, at most "
                              f"{MAX_STEPS} steps and a multiple of the {interval} s cadence")
    history_days = pred.get("historyDays", d.history_days)
    if not _is_int(history_days) or not 1 <= history_days <= MAX_HISTORY_DAYS:
        raise ContractRefused(f"spec.prediction.historyDays must be in 1..{MAX_HISTORY_DAYS}, got {history_days!r}")
    settle = pred.get("settlementSeconds", d.settlement_seconds)
    if not _is_int(settle) or not 0 <= settle <= 900:
        raise ContractRefused(f"spec.prediction.settlementSeconds must be in 0..900, got {settle!r}")
    grace = pred.get("outcomeGraceSeconds", d.outcome_grace_seconds)
    if not _is_int(grace) or not 0 <= grace <= 3600:
        raise ContractRefused(f"spec.prediction.outcomeGraceSeconds must be in 0..3600, got {grace!r}")
    risk = _int_group(_object(pred, "risk", "spec.prediction"), _RISK, RiskSettings, "spec.prediction.risk")
    cal = _int_group(_object(pred, "calibration", "spec.prediction"), _CALIBRATION, CalibrationSettings,
                     "spec.prediction.calibration")
    trust = _int_group(_object(pred, "trust", "spec.prediction"), _TRUST, TrustSettings, "spec.prediction.trust")
    extra = _int_group(_object(pred, "predictiveExtra", "spec.prediction"), _EXTRA, PredictiveExtraSettings,
                       "spec.prediction.predictiveExtra")
    families = pred.get("families", None)
    if families is None:
        families = list(d.families)
    if not isinstance(families, list) or not families or len(set(families)) != len(families) or \
            any(f not in FAMILIES for f in families):
        raise ContractRefused(f"spec.prediction.families must list distinct families of {FAMILIES}, got {families!r}")
    if "seasonalNaive" not in families:
        raise ContractRefused("spec.prediction.families must include seasonalNaive (the statistical family is selected "
                              "only over a trusted seasonal-naive forecast)")
    s = ForecastSettings(engine=engine, sample_interval_seconds=interval, seasonality=seasonality,
                         history_days=history_days, horizon_seconds=horizon, settlement_seconds=settle,
                         outcome_grace_seconds=grace, risk=risk, calibration=cal, trust=trust, predictive_extra=extra,
                         families=tuple(sorted(families)))
    _check_feasible(s)
    return s


def _check_feasible(s: ForecastSettings) -> None:
    c = s.calibration
    if not c.min_miscoverage_bps <= s.risk.miscoverage_bps <= c.max_miscoverage_bps:
        raise ContractRefused("the nominal miscoverage must lie within the adaptive bounds")
    if c.min_miscoverage_bps >= c.max_miscoverage_bps:
        raise ContractRefused("calibration.minMiscoverageBps must be below maxMiscoverageBps")
    if c.max_scores < c.min_windows:
        raise ContractRefused("calibration.maxScores must hold at least minWindows scores")
    if _rank(c.min_windows, c.min_miscoverage_bps) > c.min_windows:
        raise ContractRefused("calibration.minMiscoverageBps is infeasible: with minWindows scores the order-statistic "
                              "rank exceeds the sample count")
    # The nonoverlap cohort has one window per lead L <= horizon; retention must be able to hold minWindows of them.
    if (c.retention_days * 86400) // s.horizon_seconds < c.min_windows:
        raise ContractRefused("calibration.retentionDays cannot hold minWindows nonoverlapping windows at this horizon")
    # The scale is fitted from residuals one season apart: a one-day history never holds such a pair. The legacy
    # engine has no such fit and keeps its one-day minimum (the CRD's rule and the operator's resolver say the same).
    if s.engine == "statistical" and s.history_days < 2:
        raise ContractRefused("historyDays must be at least 2 with engine statistical: the calibration scale is "
                              "fitted from residuals one day apart")
    t = s.trust
    if t.acute_recent_windows > c.max_scores or t.min_prospective_windows > c.max_scores:
        raise ContractRefused("trust window counts exceed the calibration buffer")
    if t.challenger_exit_loss_ratio_bps < t.challenger_loss_ratio_bps:
        raise ContractRefused("trust.challengerExitLossRatioBps must not be below challengerLossRatioBps")
    if t.min_shadow_hours > c.retention_days * 24:
        raise ContractRefused("trust.minShadowHours exceeds the retention")
    if t.rolling_alarm_ppm + t.consecutive_alarm_ppm + t.day_alarm_ppm > COVERAGE_ALARM_BUDGET_PPM:
        raise ContractRefused("the coverage rules' false-alarm budgets exceed the 5 % target")


# --- lead and window --------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class LeadSelection:
    """The operator's lead choice (status.prediction.lead): L, the startup budget D and the actuation allowance A."""
    lead_seconds: int
    startup_delay_seconds: int
    actuation_allowance_seconds: int
    method: str
    sample_count: int
    observed_generation: int


_LEAD_KEYS = {"seconds": "lead_seconds", "startupDelaySeconds": "startup_delay_seconds",
              "actuationAllowanceSeconds": "actuation_allowance_seconds", "method": "method",
              "sampleCount": "sample_count", "observedGeneration": "observed_generation"}


def automatic_lead_seconds(startup_delay_s: int, decision_interval_s: int, allowance_s: int, interval_s: int) -> int:
    """L = ceil((D + C + A + Δ) / Δ) * Δ: covers D + C + A with a full grid interval of origin-age slack."""
    total = startup_delay_s + decision_interval_s + allowance_s + interval_s
    return -(-total // interval_s) * interval_s


def resolve_lead(pa: Mapping, settings: ForecastSettings, generation: int) -> Tuple[Optional[LeadSelection], str]:
    """(lead, "") when status.prediction.lead is valid for this generation and these settings, else (None, why)."""
    status = pa.get("status") if isinstance(pa, Mapping) else None
    pred = status.get("prediction") if isinstance(status, dict) else None
    lead = pred.get("lead") if isinstance(pred, dict) else None
    if lead is None:
        return None, "no lead recorded in status.prediction.lead"
    if not isinstance(lead, dict) or set(lead) != set(_LEAD_KEYS):
        return None, "status.prediction.lead is malformed"
    vals = {attr: lead[k] for k, attr in _LEAD_KEYS.items()}
    if vals["method"] not in LEAD_METHODS:
        return None, f"lead method {vals['method']!r} is unknown"
    if not all(_is_int(v) and v >= 0 for k, v in vals.items() if k != "method"):
        return None, "lead fields must be non-negative integers"
    sel = LeadSelection(**vals)
    if sel.observed_generation != generation:
        return None, f"the lead was selected for generation {sel.observed_generation}, the object is at {generation}"
    dt = settings.sample_interval_seconds
    if sel.lead_seconds < dt or sel.lead_seconds % dt or sel.lead_seconds > settings.horizon_seconds:
        return None, "the lead must be a positive multiple of the cadence within the horizon"
    if sel.lead_seconds < sel.startup_delay_seconds + dt + sel.actuation_allowance_seconds:
        return None, "the lead does not cover startup delay + decision interval + actuation allowance"
    return sel, ""


@dataclass(frozen=True)
class ForecastWindow:
    """W_t = {t+Δ, ..., t+L}: window_start = t (exclusive), window_end = t + L (inclusive)."""
    decision_origin: int
    interval_seconds: int
    lead_seconds: int

    @property
    def window_start(self) -> int:
        return self.decision_origin

    @property
    def window_end(self) -> int:
        return self.decision_origin + self.lead_seconds

    @property
    def target_timestamps(self) -> Tuple[int, ...]:
        dt = self.interval_seconds
        return tuple(self.decision_origin + k * dt for k in range(1, self.lead_seconds // dt + 1))

    def valid_until(self, lead: LeadSelection) -> int:
        """min(t + Δ - A, t + L - D - C - A) with C = Δ: the last instant a write may act on this window."""
        t, dt, a = self.decision_origin, self.interval_seconds, lead.actuation_allowance_seconds
        return min(t + dt - a, t + self.lead_seconds - lead.startup_delay_seconds - dt - a)

    def actionable_at(self, now_ms: int, lead: LeadSelection) -> bool:
        """Usable only while now + A precedes the first target and the deadline has not passed."""
        a_ms = lead.actuation_allowance_seconds * 1000
        return now_ms + a_ms < self.target_timestamps[0] * 1000 and now_ms < self.valid_until(lead) * 1000


def make_window(origin: int, settings: ForecastSettings, lead: LeadSelection) -> ForecastWindow:
    dt = settings.sample_interval_seconds
    if not _is_int(origin) or origin % dt:
        raise ContractRefused(f"decision origin {origin!r} is not on the {dt} s grid")
    if lead.lead_seconds % dt or not dt <= lead.lead_seconds <= settings.horizon_seconds:
        raise ContractRefused("the lead must be a positive multiple of the cadence within the horizon")
    return ForecastWindow(origin, dt, lead.lead_seconds)


def evidence_key(signal: ResolvedSignal, settings: ForecastSettings, lead_seconds: int) -> str:
    """The identity forecast evidence belongs to. A new PA or Deployment, query, cadence, lead, seasonality, engine,
    risk, calibration policy (its method included), outcome timing or trust-policy REVISION starts a new key (a new
    epoch); a generation that changes only other fields (mode, caps, trust thresholds) keeps it, and callers check the
    generation separately.

    Keeping forecast evidence is not keeping policy readiness: a change of capacity, predictive-extra caps or trust
    policy must separately invalidate or re-evaluate policy readiness (trust state, C2.5; operator policy, C2.7)."""
    body = {"schema": EVIDENCE_SCHEMA, "autoscaler_uid": signal.autoscaler_uid, "target_uid": signal.target_uid,
            "query_sha256": signal.sha256, "signal_contract": signal.contract, "forecast_contract": FORECAST_CONTRACT,
            "interval_seconds": settings.sample_interval_seconds, "lead_seconds": lead_seconds,
            "engine": settings.engine, "seasonality": settings.seasonality, "history_days": settings.history_days,
            "settlement_seconds": settings.settlement_seconds,
            "outcome_grace_seconds": settings.outcome_grace_seconds,
            "risk": asdict(settings.risk), "calibration": asdict(settings.calibration),
            "trust_policy": settings.trust.policy}
    return sha256_hex(canonical_json(body))


# --- request ----------------------------------------------------------------------------------------------------

_IDENTITY_FIELDS = ("autoscaler_namespace", "autoscaler_name", "autoscaler_uid", "autoscaler_generation", "target_uid",
                    "metric_query_sha256", "contract", "application", "namespace")
_REQUEST_FIELDS = frozenset(_IDENTITY_FIELDS + ("metric_type", "forecast_contract", "decision_origin",
                                                "settings_sha256"))
_REQUIRED_FIELDS = frozenset(_REQUEST_FIELDS - {"metric_type"})


@dataclass(frozen=True)
class V2Request:
    identity: Mapping            # the v1 reference fields, passed to api.identity.resolve_signal unchanged
    decision_origin: int
    settings_sha256: str


def parse_request(body) -> V2Request:
    """Schema-level validation, no lookups. Unknown fields (history, query, horizon, risk, model ...) are refused."""
    if not isinstance(body, dict):
        raise ContractRefused("the request is not an object")
    unknown = sorted(set(body) - _REQUEST_FIELDS)
    if unknown:
        raise ContractRefused(f"unknown request fields: {', '.join(unknown)}")
    missing = sorted(_REQUIRED_FIELDS - set(body))
    if missing:
        raise ContractRefused(f"missing request fields: {', '.join(missing)}")
    if body["forecast_contract"] != FORECAST_CONTRACT:
        raise ContractRefused(f"forecast_contract {body['forecast_contract']!r} is not {FORECAST_CONTRACT!r}")
    if body["contract"] != SIGNAL_CONTRACT:
        raise ContractRefused(f"contract {body['contract']!r} is not {SIGNAL_CONTRACT!r}")
    sha = body["settings_sha256"]
    if not (isinstance(sha, str) and _SHA256.match(sha)):
        raise ContractRefused("settings_sha256 must be a lowercase sha256")
    origin = parse_ts(body["decision_origin"], "decision_origin")
    ident = {k: body[k] for k in _IDENTITY_FIELDS}
    if "metric_type" in body:
        ident["metric_type"] = body["metric_type"]
    return V2Request(identity=ident, decision_origin=origin, settings_sha256=sha)


class _RecordingReader:
    """Passes reads through and keeps what they returned, so settings come from the very object identity checked."""

    def __init__(self, inner):
        self.inner, self.objects = inner, {}

    def get(self, path: str):
        obj = self.inner.get(path)
        self.objects[path] = obj
        return obj


@dataclass(frozen=True)
class BoundRequest:
    autoscaler_namespace: str
    autoscaler_name: str
    signal: ResolvedSignal
    settings: ForecastSettings
    settings_sha256: str
    decision_origin: int
    lead: Optional[LeadSelection]
    lead_problem: str             # why lead is None ("" when it is set)
    spec: Mapping = field(default_factory=dict)    # the same object's spec (policy inputs: capacity, bounds, holds)

    @property
    def window(self) -> Optional[ForecastWindow]:
        return make_window(self.decision_origin, self.settings, self.lead) if self.lead else None

    @property
    def evidence_key(self) -> Optional[str]:
        return evidence_key(self.signal, self.settings, self.lead.lead_seconds) if self.lead else None


def bind_request(body, reader: KubeReader, *, now_ms: int) -> BoundRequest:
    """Validate a v2 request against the live autoscaler: identity (api.identity), exact settings hash, and a decision
    origin that is the current grid origin at now_ms. Nothing else about the forecast is taken from the caller."""
    req = parse_request(body)
    rec = _RecordingReader(reader)
    sig = resolve_signal(dict(req.identity), rec)
    path = (f"/apis/{API_GROUP}/{API_VERSION}/namespaces/{req.identity['autoscaler_namespace']}"
            f"/predictiveautoscalers/{req.identity['autoscaler_name']}")
    pa = rec.objects.get(path)
    if not isinstance(pa, dict):
        raise ContractRefused("the autoscaler object was not read")
    settings = resolve_settings(pa)
    sha = settings.fingerprint()
    if sha != req.settings_sha256:
        raise ContractRefused("settings_sha256 does not match the autoscaler's resolved settings")
    dt = settings.sample_interval_seconds
    if req.decision_origin % dt:
        raise ContractRefused(f"decision_origin is not on the {dt} s grid")
    if not _is_int(now_ms) or not req.decision_origin * 1000 <= now_ms < (req.decision_origin + dt) * 1000:
        raise ContractRefused("decision_origin is not the current grid origin")
    lead, why = resolve_lead(pa, settings, sig.generation)
    return BoundRequest(autoscaler_namespace=req.identity["autoscaler_namespace"],
                        autoscaler_name=req.identity["autoscaler_name"], signal=sig, settings=settings, settings_sha256=sha, decision_origin=req.decision_origin,
                        lead=lead, lead_problem=why, spec=copy.deepcopy(pa.get("spec") or {}))


# --- response ---------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class V2Response:
    # identity echo
    autoscaler_namespace: str
    autoscaler_name: str
    autoscaler_uid: str
    autoscaler_generation: int
    target_namespace: str
    target_name: str
    target_uid: str
    metric_query_sha256: str
    contract: str
    settings_sha256: str
    forecast_contract: str
    unit: str
    # issuance
    issuance_id: str
    issued_at_ms: int
    input_end: int
    input_received_through_ms: int
    fit_cutoff: Optional[int]
    model_family: str
    model_recipe: str
    artifact_sha256: Optional[str]
    calibration_epoch: str
    checkpoint_sha256: Optional[str]
    # window
    decision_origin: int
    window_start: int
    window_end: int
    target_timestamps: Tuple[int, ...]
    sample_interval_seconds: int
    lead_seconds: int
    startup_delay_seconds: int
    actuation_allowance_seconds: int
    valid_until: int
    point_rps: Tuple[float, ...]
    base_window_max_rps: float
    upper_window_max_rps: Optional[float]
    nominal_miscoverage_bps: int
    working_miscoverage_ratio: Optional[float]
    # evidence
    calibration_windows: int
    prospective_windows: int
    gate_coverage_ratio: Optional[float]
    missing_outcome_ratio: Optional[float]
    selected_candidate: str
    trust_state: str
    reason: str
    predictive_eligible: bool
    evidence_cutoff: Optional[int]

    def to_wire(self) -> Dict:
        w = asdict(self)
        for k in ("issued_at_ms", "input_received_through_ms"):
            w[k[:-3]] = format_ms(w.pop(k))
        for k in ("input_end", "fit_cutoff", "decision_origin", "window_start", "window_end", "valid_until",
                  "evidence_cutoff"):
            w[k] = format_ts(w[k]) if w[k] is not None else None
        w["target_timestamps"] = [format_ts(t) for t in self.target_timestamps]
        w["point_rps"] = list(self.point_rps)
        return w


_TS_FIELDS = ("input_end", "decision_origin", "window_start", "window_end", "valid_until")
_NULLABLE_TS_FIELDS = ("fit_cutoff", "evidence_cutoff")
_MS_FIELDS = ("issued_at", "input_received_through")


def response_from_wire(w) -> V2Response:
    """Decode a wire response strictly (every field present, no extra ones) and check its internal coherence."""
    if not isinstance(w, dict):
        raise ContractRefused("the response is not an object")
    names = [f for f in V2Response.__dataclass_fields__]
    expected = {n[:-3] if n in ("issued_at_ms", "input_received_through_ms") else n for n in names}
    if set(w) != expected:
        raise ContractRefused(f"response fields differ: missing {sorted(expected - set(w))}, "
                              f"extra {sorted(set(w) - expected)}")
    v = dict(w)
    for k in _MS_FIELDS:
        v[k + "_ms"] = parse_ms(v.pop(k), k)
    for k in _TS_FIELDS:
        v[k] = parse_ts(v[k], k)
    for k in _NULLABLE_TS_FIELDS:
        v[k] = parse_ts(v[k], k) if v[k] is not None else None
    if not isinstance(v["target_timestamps"], list) or not isinstance(v["point_rps"], list):
        raise ContractRefused("target_timestamps and point_rps must be arrays")
    v["target_timestamps"] = tuple(parse_ts(t, "target timestamp") for t in v["target_timestamps"])
    v["point_rps"] = tuple(v["point_rps"])
    r = V2Response(**v)
    check_response(r)
    return r


def check_response(r: V2Response, bound: Optional[BoundRequest] = None) -> None:
    """Internal coherence, and when `bound` is given, agreement with the request it answers. Raises ContractRefused."""
    if r.forecast_contract != FORECAST_CONTRACT or r.contract != SIGNAL_CONTRACT or r.unit != UNIT:
        raise ContractRefused("unknown forecast contract, signal contract or unit")
    for name in ("autoscaler_generation", "sample_interval_seconds", "lead_seconds", "startup_delay_seconds",
                 "actuation_allowance_seconds", "nominal_miscoverage_bps", "calibration_windows",
                 "prospective_windows", "issued_at_ms", "input_received_through_ms", "input_end", "decision_origin",
                 "window_start", "window_end", "valid_until"):
        if not _is_int(getattr(r, name)) or getattr(r, name) < 0:
            raise ContractRefused(f"{name} must be a non-negative integer")
    if not isinstance(r.predictive_eligible, bool):
        raise ContractRefused("predictive_eligible must be a boolean")
    if r.trust_state not in PHASES or r.reason not in REASONS:
        raise ContractRefused("unknown trust state or reason")
    if r.model_family not in MODEL_FAMILIES or r.selected_candidate not in MODEL_FAMILIES:
        raise ContractRefused("unknown model family")
    for name in ("metric_query_sha256", "settings_sha256"):
        if not _SHA256.match(getattr(r, name) or ""):
            raise ContractRefused(f"{name} is not a sha256")
    dt, lead = r.sample_interval_seconds, LeadSelection(r.lead_seconds, r.startup_delay_seconds,
                                                         r.actuation_allowance_seconds, "configured", 0, 0)
    if dt not in ALLOWED_INTERVALS or r.decision_origin % dt or r.lead_seconds % dt or r.lead_seconds < dt:
        raise ContractRefused("the window is not on an admitted grid")
    win = ForecastWindow(r.decision_origin, dt, r.lead_seconds)
    if (r.window_start, r.window_end, r.target_timestamps) != (win.window_start, win.window_end,
                                                                win.target_timestamps):
        raise ContractRefused("window bounds or target timestamps do not match the origin and lead")
    if r.valid_until != win.valid_until(lead):
        raise ContractRefused("valid_until does not follow from the window and the lead")
    if len(r.point_rps) != len(r.target_timestamps) or not all(_finite_nonneg(x) for x in r.point_rps):
        raise ContractRefused("point_rps must hold one finite, non-negative rate per target")
    if not _finite_nonneg(r.base_window_max_rps) or r.base_window_max_rps != max(r.point_rps):
        raise ContractRefused("base_window_max_rps must be the maximum of point_rps")
    if r.upper_window_max_rps is not None and not _finite_nonneg(r.upper_window_max_rps):
        raise ContractRefused("upper_window_max_rps must be null or a finite, non-negative rate")
    for name in ("gate_coverage_ratio", "missing_outcome_ratio", "working_miscoverage_ratio"):
        x = getattr(r, name)
        if x is not None and not (_finite_nonneg(x) and x <= 1):
            raise ContractRefused(f"{name} must be null or within 0..1")
    if r.gate_coverage_ratio is not None and r.calibration_windows == 0 and r.prospective_windows == 0:
        raise ContractRefused("coverage without any scored window")
    if r.input_end > r.decision_origin or (r.fit_cutoff is not None and r.fit_cutoff > r.input_end):
        raise ContractRefused("input or fit cutoff after the decision origin")
    if r.input_received_through_ms > r.issued_at_ms or r.issued_at_ms < r.decision_origin * 1000:
        raise ContractRefused("issue/receipt times are out of order")
    if r.evidence_cutoff is not None and r.evidence_cutoff * 1000 > r.issued_at_ms:
        raise ContractRefused("evidence cutoff after the issue time")
    if r.target_namespace != r.autoscaler_namespace:
        raise ContractRefused("the target must be in the autoscaler's namespace")
    # A forecast issued at or after its first target is a hindcast, never a forecast.
    if r.issued_at_ms >= r.target_timestamps[0] * 1000:
        raise ContractRefused("the issuance is not before the first target")
    if r.predictive_eligible and (r.upper_window_max_rps is None or r.trust_state != "Trusted" or r.reason != "Ready"):
        raise ContractRefused("an eligible forecast needs an upper bound, trust state Trusted and reason Ready")
    # Eligibility also needs the remaining window at issuance: now + A before the first target and before valid_until.
    if r.predictive_eligible and not win.actionable_at(r.issued_at_ms, lead):
        raise ContractRefused("an eligible forecast must be issued while its window is still actionable")
    if bound is not None:
        s = bound.signal
        echo = (r.autoscaler_namespace, r.autoscaler_name, r.autoscaler_uid, r.autoscaler_generation,
                r.target_namespace, r.target_name, r.target_uid, r.metric_query_sha256, r.settings_sha256,
                r.decision_origin, r.sample_interval_seconds, r.nominal_miscoverage_bps)
        want = (bound.autoscaler_namespace, bound.autoscaler_name, s.autoscaler_uid, s.generation, s.namespace,
                s.name, s.target_uid, s.sha256, bound.settings_sha256, bound.decision_origin,
                bound.settings.sample_interval_seconds, bound.settings.risk.miscoverage_bps)
        if echo != want:
            raise ContractRefused("the response does not echo the request's identity, settings and origin")
        if bound.lead is None or (r.lead_seconds, r.startup_delay_seconds, r.actuation_allowance_seconds) != (
                bound.lead.lead_seconds, bound.lead.startup_delay_seconds, bound.lead.actuation_allowance_seconds):
            raise ContractRefused("the response's lead differs from the autoscaler's")
        if r.calibration_epoch != bound.evidence_key:
            raise ContractRefused("the response's calibration epoch is not the request's evidence key")

