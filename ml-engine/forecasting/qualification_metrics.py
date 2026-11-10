"""The release-gate basis this forecaster can answer under, as service-level metrics (a dashboard and an alert).

Three families, none with an autoscaler, namespace, target, digest or free-form label (so they are not part of the
per-autoscaler predictive_autoscaler_forecast_* series of forecasting.metrics and count against no registration limit):

  predictive_autoscaler_forecaster_qualification_basis{basis}                    gauge, basis in BASES
      1 for the basis in force for this process right now, 0 for the others: release (the release gates passed), lab or
      demonstration (that document is mounted and inside its validity window), none (no document, not yet valid, or
      expired). A document's scope is checked per request: 1 says a basis is available, not that an answer used it.
  predictive_autoscaler_forecaster_qualification_expires_timestamp_seconds{document}    gauge, document in DOCUMENTS
      The mounted document's expiry (Unix seconds); absent without one. It stays exported after the expiry, so
      "mounted and expired" is this series beside basis="none".
  predictive_autoscaler_forecaster_demonstration_refusals_total{reason}          counter, reason in the fixed enum
      Release-gate evaluations (issuances and re-checks of stored answers) a mounted demonstration qualification did
      not cover, by the first failing check; exported only while a demonstration qualification is mounted.
"""

from __future__ import annotations

import threading
from typing import Callable, Dict, Iterator, Mapping, Sequence, Tuple

from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector

PREFIX = "predictive_autoscaler_forecaster_"
BASES = ("none", "lab", "demonstration", "release")
DOCUMENTS = ("lab", "demonstration")
Status = Tuple[str, Mapping[str, int]]        # (the basis in force, {document: expiry in Unix seconds})


class QualificationMetrics(Collector):
    def __init__(self, status: Callable[[], Status], refusal_reasons: Sequence[str] = ()):
        self._status = status
        self._refusals: Dict[str, int] = {r: 0 for r in refusal_reasons}
        self._lock = threading.Lock()

    def refused(self, reason: str) -> None:
        with self._lock:
            if reason not in self._refusals:
                raise ValueError(f"refusal reason {reason!r} is outside the fixed enum")
            self._refusals[reason] += 1

    def collect(self) -> Iterator:
        basis, expires = self._status()
        if basis not in BASES or set(expires) - set(DOCUMENTS):
            raise ValueError(f"basis {basis!r} or documents {sorted(expires)} are outside the fixed enums")
        g = GaugeMetricFamily(PREFIX + "qualification_basis",
                              "1 for the release-gate basis in force for this forecaster now, 0 for the others.",
                              labels=["basis"])
        for b in BASES:
            g.add_metric([b], 1.0 if b == basis else 0.0)
        yield g
        e = GaugeMetricFamily(PREFIX + "qualification_expires_timestamp_seconds",
                              "The mounted qualification document's expiry (Unix seconds); absent without one.",
                              labels=["document"])
        for doc in DOCUMENTS:
            if doc in expires:
                e.add_metric([doc], float(expires[doc]))
        yield e
        if "demonstration" in expires:
            with self._lock:
                counts = dict(self._refusals)
            c = CounterMetricFamily(PREFIX + "demonstration_refusals",
                                    "Release-gate evaluations a mounted demonstration qualification did not cover, by "
                                    "the first failing check.", labels=["reason"])
            for reason, n in counts.items():
                c.add_metric([reason], float(n))
            yield c


__all__ = ["BASES", "DOCUMENTS", "PREFIX", "QualificationMetrics"]
