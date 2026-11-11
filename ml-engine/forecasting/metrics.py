"""Bounded forecast-quality metrics for forecast contract v2 (design C2, section 8).

Every series starts with predictive_autoscaler_forecast_ and carries {namespace, autoscaler, model}, with model from
the fixed enum (seasonal_naive, statistical, lstm), plus at most one fixed extra label (result, cohort, horizon or
reason). No UID, generation, query, version, timestamp, risk value, issuance ID or free-form error is ever a label.
These are realised-error and coverage measurements of matured windows: they are NOT the operator's deprecated
predictive_autoscaler_prediction_error_percent (a replica disagreement within one decision), and nothing here reuses
that name or meaning.

ForecastMetrics is a prometheus_client collector over immutable per-autoscaler snapshots. update() replaces an
autoscaler's snapshot whole (a new UID replaces the old one: its counters restart, nothing is mixed across identities);
release() drops it after confirmed deletion, replacement or registration expiry, and its series disappear. Admission
is bounded: at most max_autoscalers registered (8 by default) and at most max_series_per_autoscaler series each (256);
beyond that update() and reserve() refuse (RegistryFull / CardinalityExceeded) rather than evicting anything: the
caller (forecasting.service) decides what may be released, and only on evidence. A missing value (no denominator, no
bound) removes that series instead of exporting 0 or 1.

Every registration carries a token, new whenever the name is registered or its UID is replaced. Work admitted under one
registration publishes with that token (update(token=...)): once the registration was replaced or released the token no
longer matches and the publication is dropped, so a slow old request can neither overwrite its replacement nor bring a
released registration back.

ServiceMetrics is the service's own handful of series (predictive_autoscaler_forecaster_*, no autoscaler or model
label): the registrations in use, their limit, the refusals at the limit and the releases by reason; and the journal's
health: since when it is unhealthy (absent while healthy), whether its bounded recovery attempts are used up, and the
failures, recovery attempts and recoveries so far.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Callable, Dict, Iterator, List, Mapping, NamedTuple, Optional, Tuple

from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector

from forecasting.calibration import E8
from forecasting.contracts import MODEL_FAMILIES, REASONS
from forecasting.trust import DAY, availability, expected_opportunities, window_sums

PREFIX = "predictive_autoscaler_forecast_"
BASE_LABELS = ("namespace", "autoscaler", "model")
FIT_FAILURE_REASONS = ("history", "numeric", "timeout", "resource", "storage")
HORIZONS = ("first", "middle", "last")
COHORTS = ("all", "gate")
OUTCOME_RESULTS = ("covered", "missed", "unscorable")
MAX_AUTOSCALERS = 8
MAX_SERIES_PER_AUTOSCALER = 256
SERVICE_PREFIX = "predictive_autoscaler_forecaster_"
RELEASE_REASONS = ("not_found", "replaced", "idle")

# name suffix -> (type, extra label or None, help)
METRICS: Dict[str, Tuple[str, Optional[str], str]] = {
    "issued_total": ("counter", None, "Unique committed window issuances (retries excluded)."),
    "outcomes_total": ("counter", "result", "Terminal window outcomes of issued bounds (a window issued without a "
                                            "bound is neither covered nor missed)."),
    "scored_windows": ("gauge", "cohort", "Scorable windows with an issued bound in the trailing 7 days."),
    "window_coverage_ratio": ("gauge", "cohort", "Covered / bounded windows (Y <= U) over 7 days; absent when none."),
    "outcome_availability_ratio": ("gauge", None, "Scorable / (matured + overdue) issued windows over the last day; "
                                                  "absent when none."),
    "window_mae_rps": ("gauge", None, "Mean |Y - b| of matured windows over 7 days, bound or not (requests/second)."),
    "window_bias_rps": ("gauge", None, "Mean b - Y over 7 days; positive means over-forecast (requests/second)."),
    "window_pinball_loss_rps": ("gauge", None, "Mean nominal-level pinball loss of issued gate bounds over 7 days."),
    "step_mae_rps": ("gauge", "horizon", "Mean |point - realised| at the first, middle and last target over 7 days."),
    "nominal_coverage_ratio": ("gauge", None, "The configured window coverage target (1 - nominal miscoverage)."),
    "working_miscoverage_ratio": ("gauge", None, "The adaptive working miscoverage level."),
    "history_seconds": ("gauge", None, "History span available to the model."),
    "calibration_windows": ("gauge", None, "Cohort scores in the calibration buffer."),
    "evidence_age_seconds": ("gauge", None, "Seconds since the last prospective gate outcome; absent when none."),
    "trusted": ("gauge", None, "1 only for a trusted candidate; otherwise 0."),
    "prediction_availability_ratio": ("gauge", None, "Issued / expected decision origins over the last day, as the "
                                                     "trust gate reads it; absent when none was expected."),
    "prediction_issued_windows": ("gauge", None, "Its numerator: decision origins of the last day with an issuance."),
    "prediction_expected_windows": ("gauge", None, "Its denominator: decision origins of the last day since the first "
                                                   "issuance (0: nothing was expected yet, the ratio is unknown)."),
    "trust_reason": ("gauge", "reason", "The trust gate's reason for the candidate when its latest committed forecast "
                                        "was issued (history, not whether it may act now): one series with value 1; "
                                        "absent when the candidate was not evaluated."),
    "upper_window_rps": ("gauge", None, "The latest committed upper bound, trusted or not; absent when none was issued."),
    "base_window_rps": ("gauge", None, "The latest committed point maximum over the window, trusted or not."),
    "latest_issued_timestamp_seconds": ("gauge", None, "When the latest committed forecast was issued (Unix seconds)."),
    "latest_window_start_timestamp_seconds": ("gauge", None, "Start of the future window the latest committed "
                                              "forecast covers (Unix seconds): its bounds are about that window."),
    "latest_window_end_timestamp_seconds": ("gauge", None, "End of that window (Unix seconds)."),
    "fit_failures_total": ("counter", "reason", "Fits that failed, by bounded reason."),
}
METRIC_NAMES = tuple(PREFIX + k for k in METRICS)
_EXTRA = {"result": OUTCOME_RESULTS, "cohort": COHORTS, "horizon": HORIZONS, "reason": FIT_FAILURE_REASONS}
_SUFFIX_ENUM = {"trust_reason": REASONS}          # a label name may carry another fixed enum on one metric
LATEST_KEYS = ("upper_window_rps", "base_window_rps", "latest_issued_timestamp_seconds",
               "latest_window_start_timestamp_seconds", "latest_window_end_timestamp_seconds")


def label_values(suffix: str) -> Tuple[str, ...]:
    """The fixed values of the metric's extra label (() when it has none)."""
    label = METRICS[suffix][1]
    return () if label is None else _SUFFIX_ENUM.get(suffix, _EXTRA[label])


class RegistryFull(Exception):
    pass


class CardinalityExceeded(Exception):
    pass


Sample = Tuple[str, Tuple[str, ...], float]       # (suffix, extra label values, value)


def model_samples(*, ledger: dict, calibration: Optional[dict], nominal_miscoverage: float, trusted: bool,
                  history_seconds: int, now_s: int, latest: Optional[Mapping[str, Optional[float]]] = None,
                  fit_failures: Optional[Mapping[str, int]] = None, trust_reason: Optional[str] = None) -> List[Sample]:
    """The samples of one model from its evidence ledger (forecasting.trust) and calibration state. `latest` is the
    candidate's part of the latest committed forecast (LATEST_KEYS; a missing or None value has no series: a bound is
    never made up) and `trust_reason` the gate's reason for it there."""
    out: List[Sample] = []
    tot = ledger["totals"]
    w7 = window_sums(ledger, now_s, 7 * DAY)
    avail = availability(ledger, now_s)["outcomes"]
    out.append(("issued_total", (), float(tot["issued"])))
    for r in OUTCOME_RESULTS:
        out.append(("outcomes_total", (r,), float(tot[r])))
    for cohort, bd, cv in (("all", w7["a_bd"], w7["a_cv"]), ("gate", w7["g_bd"], w7["g_cv"])):
        out.append(("scored_windows", (cohort,), float(bd)))
        if bd:
            out.append(("window_coverage_ratio", (cohort,), cv / bd))
    if avail is not None:
        out.append(("outcome_availability_ratio", (), avail))
    expected = expected_opportunities(ledger, now_s, DAY)             # exactly trust.availability's "predictions"
    issued = min(window_sums(ledger, now_s, DAY)["iss"], expected)
    out.append(("prediction_expected_windows", (), float(expected)))
    out.append(("prediction_issued_windows", (), float(issued)))
    if expected:
        out.append(("prediction_availability_ratio", (), issued / expected))
    if trust_reason is not None:
        out.append(("trust_reason", (trust_reason,), 1.0))
    if w7["sc"]:
        out.append(("window_mae_rps", (), w7["abs"] / w7["sc"]))
        out.append(("window_bias_rps", (), w7["bias"] / w7["sc"]))
    if w7["g_bd"]:
        out.append(("window_pinball_loss_rps", (), w7["pin"] / w7["g_bd"]))
    if w7["n_step"]:
        for h, f in zip(HORIZONS, ("st0", "st1", "st2")):
            out.append(("step_mae_rps", (h,), w7[f] / w7["n_step"]))
    out.append(("nominal_coverage_ratio", (), 1.0 - nominal_miscoverage))
    if calibration is not None:
        out.append(("working_miscoverage_ratio", (), calibration["a_e8"] / E8))
        out.append(("calibration_windows", (), float(len(calibration["scores"]))))
    out.append(("history_seconds", (), float(history_seconds)))
    if ledger["last_prospective_outcome_s"] is not None:
        out.append(("evidence_age_seconds", (), float(now_s - ledger["last_prospective_outcome_s"])))
    out.append(("trusted", (), 1.0 if trusted else 0.0))
    for key in LATEST_KEYS:
        v = (latest or {}).get(key)
        if v is not None:
            out.append((key, (), float(v)))
    for r in FIT_FAILURE_REASONS:
        out.append(("fit_failures_total", (r,), float((fit_failures or {}).get(r, 0))))
    return out


@dataclass(frozen=True)
class Snapshot:
    """An immutable copy: the caller's dicts and lists are copied into tuples at construction, so nothing the caller
    changes afterwards reaches an admitted snapshot."""
    uid: str
    models: Mapping[str, List[Sample]] = field(default_factory=dict)

    def __post_init__(self):
        frozen = {str(m): tuple((str(n), tuple(str(x) for x in e), float(v)) for n, e, v in samples)
                  for m, samples in dict(self.models).items()}
        object.__setattr__(self, "models", MappingProxyType(frozen))

    def series(self) -> int:
        return sum(len(v) for v in self.models.values())


class UidConflict(Exception):
    """The name is registered to another UID. Which of the two objects is live is not the registry's to decide: the
    caller confirms it and then names the UID it replaces (reserve(replacing=...))."""

    def __init__(self, held: str):
        super().__init__(f"registered to UID {held}")
        self.held = held


class Reservation(NamedTuple):
    token: int                    # the registration this request was admitted under
    replaced: Optional[str]       # the UID whose registration this request's UID replaced, if any


class ForecastMetrics(Collector):
    def __init__(self, max_autoscalers: int = MAX_AUTOSCALERS,
                 max_series_per_autoscaler: int = MAX_SERIES_PER_AUTOSCALER):
        self.max_autoscalers, self.max_series = max_autoscalers, max_series_per_autoscaler
        self._snaps: Dict[Tuple[str, str], Snapshot] = {}
        self._seen: Dict[Tuple[str, str], int] = {}               # the registration's last request (clock ms)
        self._tokens: Dict[Tuple[str, str], int] = {}             # the registration's token (see the module text)
        self._issued_tokens = 0
        self.refused = 0                                          # requests refused at the limit
        self.released = {r: 0 for r in RELEASE_REASONS}
        self._lock = threading.Lock()

    def update(self, namespace: str, autoscaler: str, snapshot: Snapshot, token: Optional[int] = None) -> bool:
        """Replace the autoscaler's snapshot. With `token` (a Reservation's), only while that registration still
        stands: False, and nothing changes, once it was replaced or released. Without one the name is registered if
        it is not yet (direct use; the service always publishes with its token)."""
        snapshot = Snapshot(snapshot.uid, snapshot.models)        # validate and keep our own frozen copy
        for model, samples in snapshot.models.items():
            if model not in MODEL_FAMILIES:
                raise ValueError(f"model {model!r} is not in the fixed enum {MODEL_FAMILIES}")
            for suffix, extra, value in samples:
                kind, label, _ = METRICS[suffix]
                if (label is None) != (not extra) or (label and (len(extra) != 1 or extra[0] not in label_values(suffix))):
                    raise ValueError(f"{suffix}: label values {extra!r} are outside the fixed enum")
        if snapshot.series() > self.max_series:
            raise CardinalityExceeded(f"{snapshot.series()} series for {namespace}/{autoscaler} "
                                      f"(at most {self.max_series})")
        key = (namespace, autoscaler)
        with self._lock:
            if token is not None:
                if self._tokens.get(key) != token:
                    return False
            elif key not in self._snaps:
                if len(self._snaps) >= self.max_autoscalers:
                    raise RegistryFull(f"{self.max_autoscalers} autoscalers are already registered")
                self._tokens[key] = self._new_token()
            self._snaps[key] = snapshot
            self._seen.setdefault(key, 0)
            return True

    def _new_token(self) -> int:
        self._issued_tokens += 1
        return self._issued_tokens

    def reserve(self, namespace: str, autoscaler: str, uid: str = "", now_ms: int = 0,
                replacing: Optional[str] = None) -> Reservation:
        """Admit an autoscaler before any work is done for it (atomic; an empty snapshot until its first update) and
        note the request's time. A registration held by another UID raises UidConflict and nothing changes, unless
        `replacing` names exactly that UID (compare and swap): then it is replaced by an empty one for `uid` (the same
        slot, a new identity with a new token: none of the old object's series survive) and the old UID is returned
        with the token. Raises RegistryFull when the limit is reached and it is not registered yet."""
        key = (namespace, autoscaler)
        with self._lock:
            have = self._snaps.get(key)
            if have is None:
                if len(self._snaps) >= self.max_autoscalers:
                    raise RegistryFull(f"{self.max_autoscalers} autoscalers are already registered")
                self._snaps[key] = Snapshot(uid, {})
                self._tokens[key] = self._new_token()
            elif uid and have.uid and have.uid != uid and replacing != have.uid:
                raise UidConflict(have.uid)                       # before anything is touched, the last request too
            self._seen[key] = max(now_ms, self._seen.get(key, 0))
            if have is None or not uid or have.uid == uid:
                return Reservation(self._tokens[key], None)
            if not have.uid:                                      # registered without a UID so far: it is this one's
                self._snaps[key] = Snapshot(uid, have.models)
                return Reservation(self._tokens[key], None)
            self._snaps[key] = Snapshot(uid, {})
            self._tokens[key] = self._new_token()
            self.released["replaced"] += 1
            return Reservation(self._tokens[key], have.uid)

    def holds(self, namespace: str, autoscaler: str, token: int) -> bool:
        """Whether the registration a Reservation was admitted under still stands (not replaced, not released)."""
        with self._lock:
            return self._tokens.get((namespace, autoscaler)) == token

    def release(self, namespace: str, autoscaler: str, reason: str, *, uid: Optional[str] = None,
                seen_ms: Optional[int] = None) -> bool:
        """Free a registration and drop its series, counted by reason. With `uid` / `seen_ms`, only if it still is that
        identity and has had no request since (evidence gathered about one registration never frees another)."""
        if reason not in RELEASE_REASONS:
            raise ValueError(f"release reason {reason!r} is not one of {RELEASE_REASONS}")
        key = (namespace, autoscaler)
        with self._lock:
            have = self._snaps.get(key)
            if have is None or (uid is not None and have.uid != uid) or \
                    (seen_ms is not None and self._seen.get(key, 0) != seen_ms):
                return False
            del self._snaps[key]
            self._seen.pop(key, None)
            self._tokens.pop(key, None)
            self.released[reason] += 1
            return True

    def count_refusal(self) -> None:
        with self._lock:
            self.refused += 1

    def remove(self, namespace: str, autoscaler: str) -> None:
        with self._lock:
            self._snaps.pop((namespace, autoscaler), None)
            self._seen.pop((namespace, autoscaler), None)
            self._tokens.pop((namespace, autoscaler), None)

    def registered(self) -> List[Tuple[str, str]]:
        with self._lock:
            return sorted(self._snaps)

    def slots(self) -> List[Tuple[str, str, str, int]]:
        """[(namespace, autoscaler, registered UID, last request ms)], the longest without a request first."""
        with self._lock:
            return sorted(((ns, name, snap.uid, self._seen.get((ns, name), 0))
                           for (ns, name), snap in self._snaps.items()), key=lambda s: (s[3], s[0], s[1]))

    def counts(self) -> Tuple[int, int, Dict[str, int]]:
        """(registered now, refusals at the limit, releases by reason)."""
        with self._lock:
            return len(self._snaps), self.refused, dict(self.released)

    def collect(self) -> Iterator:
        with self._lock:
            snaps = dict(self._snaps)
        families = {}
        for suffix, (kind, label, help_text) in METRICS.items():
            labels = [*BASE_LABELS, *([label] if label else [])]
            cls = CounterMetricFamily if kind == "counter" else GaugeMetricFamily
            name = PREFIX + (suffix[: -len("_total")] if kind == "counter" else suffix)
            families[suffix] = cls(name, help_text, labels=labels)
        for (ns, name), snap in sorted(snaps.items()):
            for model, samples in sorted(snap.models.items()):
                for suffix, extra, value in samples:
                    families[suffix].add_metric([ns, name, model, *extra], value)
        yield from families.values()


class ServiceMetrics(Collector):
    """The service's own series over a ForecastMetrics registry: bounded (one label at most, from a fixed enum)."""

    def __init__(self, registry: ForecastMetrics, journal: Optional[Callable[[], Mapping]] = None):
        self.registry, self.journal = registry, journal       # journal: ForecastService.journal_status

    def collect(self) -> Iterator:
        if self.journal is not None:
            j = self.journal()
            since = GaugeMetricFamily(SERVICE_PREFIX + "journal_unhealthy_since_seconds", "Unix time since which the "
                                      "forecast journal cannot support issuance; absent while it is healthy.")
            if j["unhealthy_since_ms"] is not None:
                since.add_metric([], j["unhealthy_since_ms"] / 1000)
            yield since
            yield GaugeMetricFamily(SERVICE_PREFIX + "journal_permanent_failure", "1 while the journal is unhealthy "
                                    "and its bounded recovery attempts are used up (operator action required; still "
                                    "retried slowly); otherwise 0.", value=1.0 if j["permanent_failure"] else 0.0)
            for name, key, text in (("journal_failures", "failures", "Times the journal turned unhealthy."),
                                    ("journal_recovery_attempts", "attempts", "Journal reopen attempts."),
                                    ("journal_recoveries", "recoveries", "Journal reopen attempts that succeeded.")):
                yield CounterMetricFamily(SERVICE_PREFIX + name, text, value=float(j[key]))
        used, refused, released = self.registry.counts()
        yield GaugeMetricFamily(SERVICE_PREFIX + "autoscaler_slots", "Autoscalers registered with the service now.",
                                value=float(used))
        yield GaugeMetricFamily(SERVICE_PREFIX + "autoscaler_slot_limit", "The most autoscalers the service registers "
                                "(forecaster.statistical.maxAutoscalers).", value=float(self.registry.max_autoscalers))
        yield CounterMetricFamily(SERVICE_PREFIX + "autoscaler_slot_refusals", "Requests refused because every "
                                  "registration was in use (AutoscalerLimitReached).", value=float(refused))
        releases = CounterMetricFamily(SERVICE_PREFIX + "autoscaler_slot_releases", "Registrations released: the "
                                       "autoscaler was deleted (not_found), replaced by another UID, or idle.",
                                       labels=["reason"])
        for reason in RELEASE_REASONS:
            releases.add_metric([reason], float(released[reason]))
        yield releases
