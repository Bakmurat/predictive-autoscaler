"""Bounded forecast-quality metrics for forecast contract v2 (design C2, section 8).

Every series starts with predictive_autoscaler_forecast_ and carries {namespace, autoscaler, model}, with model from
the fixed enum (seasonal_naive, statistical, lstm), plus at most one fixed extra label (result, cohort, horizon or
reason). No UID, generation, query, version, timestamp, risk value, issuance ID or free-form error is ever a label.
These are realised-error and coverage measurements of matured windows: they are NOT the operator's deprecated
predictive_autoscaler_prediction_error_percent (a replica disagreement within one decision), and nothing here reuses
that name or meaning.

ForecastMetrics is a prometheus_client collector over immutable per-autoscaler snapshots. update() replaces an
autoscaler's snapshot whole (a new UID replaces the old one: its counters restart, nothing is mixed across identities);
remove() drops it after confirmed deletion or registration expiry, and its series disappear. Admission is bounded:
at most max_autoscalers registered (8) and at most max_series_per_autoscaler series each (256); beyond that update()
refuses (RegistryFull / CardinalityExceeded) rather than evicting anything. A missing value (no denominator, no bound)
removes that series instead of exporting 0 or 1.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Dict, Iterator, List, Mapping, Optional, Tuple

from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector

from forecasting.calibration import E8
from forecasting.contracts import MODEL_FAMILIES
from forecasting.trust import DAY, availability, window_sums

PREFIX = "predictive_autoscaler_forecast_"
BASE_LABELS = ("namespace", "autoscaler", "model")
FIT_FAILURE_REASONS = ("history", "numeric", "timeout", "resource", "storage")
HORIZONS = ("first", "middle", "last")
COHORTS = ("all", "gate")
OUTCOME_RESULTS = ("covered", "missed", "unscorable")
MAX_AUTOSCALERS = 8
MAX_SERIES_PER_AUTOSCALER = 256

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
    "upper_window_rps": ("gauge", None, "The latest committed upper bound; absent when none was issued."),
    "base_window_rps": ("gauge", None, "The latest committed point maximum over the window."),
    "fit_failures_total": ("counter", "reason", "Fits that failed, by bounded reason."),
}
METRIC_NAMES = tuple(PREFIX + k for k in METRICS)
_EXTRA = {"result": OUTCOME_RESULTS, "cohort": COHORTS, "horizon": HORIZONS, "reason": FIT_FAILURE_REASONS}


class RegistryFull(Exception):
    pass


class CardinalityExceeded(Exception):
    pass


Sample = Tuple[str, Tuple[str, ...], float]       # (suffix, extra label values, value)


def model_samples(*, ledger: dict, calibration: Optional[dict], nominal_miscoverage: float, trusted: bool,
                  history_seconds: int, now_s: int, latest: Optional[Mapping[str, Optional[float]]] = None,
                  fit_failures: Optional[Mapping[str, int]] = None) -> List[Sample]:
    """The samples of one model from its evidence ledger (forecasting.trust) and calibration state."""
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
    for key in ("upper_window_rps", "base_window_rps"):
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


class ForecastMetrics(Collector):
    def __init__(self, max_autoscalers: int = MAX_AUTOSCALERS,
                 max_series_per_autoscaler: int = MAX_SERIES_PER_AUTOSCALER):
        self.max_autoscalers, self.max_series = max_autoscalers, max_series_per_autoscaler
        self._snaps: Dict[Tuple[str, str], Snapshot] = {}
        self._lock = threading.Lock()

    def update(self, namespace: str, autoscaler: str, snapshot: Snapshot) -> None:
        snapshot = Snapshot(snapshot.uid, snapshot.models)        # validate and keep our own frozen copy
        for model, samples in snapshot.models.items():
            if model not in MODEL_FAMILIES:
                raise ValueError(f"model {model!r} is not in the fixed enum {MODEL_FAMILIES}")
            for suffix, extra, value in samples:
                kind, label, _ = METRICS[suffix]
                if (label is None) != (not extra) or (label and (len(extra) != 1 or extra[0] not in _EXTRA[label])):
                    raise ValueError(f"{suffix}: label values {extra!r} are outside the fixed enum")
        if snapshot.series() > self.max_series:
            raise CardinalityExceeded(f"{snapshot.series()} series for {namespace}/{autoscaler} "
                                      f"(at most {self.max_series})")
        key = (namespace, autoscaler)
        with self._lock:
            if key not in self._snaps and len(self._snaps) >= self.max_autoscalers:
                raise RegistryFull(f"{self.max_autoscalers} autoscalers are already registered")
            self._snaps[key] = snapshot

    def remove(self, namespace: str, autoscaler: str) -> None:
        with self._lock:
            self._snaps.pop((namespace, autoscaler), None)

    def registered(self) -> List[Tuple[str, str]]:
        with self._lock:
            return sorted(self._snaps)

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
