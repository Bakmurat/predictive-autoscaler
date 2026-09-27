"""Strict, opt-in routing for the declared seasonal-ensemble arm (Task 03 U-21).

`ENSEMBLE_EXPERIMENT` names one application whose forecasts come from the seasonal ensemble
(models/seasonal_ensemble.py) computed on a source application's history, the same shared-input
design as SEASONAL_EXPERIMENT. Unset means off; malformed configuration fails at startup.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import re

from models import seasonal_ensemble


@dataclass(frozen=True)
class EnsembleExperiment:
    id: str
    application: str
    namespace: str
    source_application: str
    source_namespace: str

    @classmethod
    def parse(cls, raw: str):
        try:
            value = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("ENSEMBLE_EXPERIMENT must be a JSON object") from exc
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("ENSEMBLE_EXPERIMENT requires exactly id, application, namespace, "
                             "source_application, source_namespace")
        for key, name in value.items():
            if not isinstance(name, str) or len(name) > 63 or not re.fullmatch(
                    r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", name):
                raise ValueError(f"ENSEMBLE_EXPERIMENT invalid {key}")
        if value["application"] == value["source_application"]:
            raise ValueError("ENSEMBLE_EXPERIMENT requires distinct application names")
        return cls(**value)

    @property
    def config_sha256(self):
        canonical = json.dumps({**asdict(self), "settings": seasonal_ensemble.SETTINGS},
                               sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    def matches(self, application, namespace, metric_type):
        return (application, namespace, metric_type) == (self.application, self.namespace, "requests")

    def provenance(self):
        return {**asdict(self), "config_sha256": self.config_sha256,
                "forecast_mode": "seasonal-ensemble-q90", "feedback_actuals": "source",
                "forecaster_version": seasonal_ensemble.VERSION}
