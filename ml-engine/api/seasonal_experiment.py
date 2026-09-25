"""Strict, opt-in routing for one declared shared-input seasonal experiment."""
from dataclasses import asdict, dataclass
import hashlib
import json
import re


@dataclass(frozen=True)
class SeasonalExperiment:
    id: str
    application: str
    namespace: str
    source_application: str
    source_namespace: str

    @classmethod
    def parse(cls, raw: str):
        """Reject malformed configuration at startup instead of changing defaults."""
        try:
            value = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("SEASONAL_EXPERIMENT must be a JSON object") from exc
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("SEASONAL_EXPERIMENT requires exactly id, application, namespace, "
                             "source_application, source_namespace")
        for key, name in value.items():
            if not isinstance(name, str) or len(name) > 63 or not re.fullmatch(
                    r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", name):
                raise ValueError(f"SEASONAL_EXPERIMENT invalid {key}")
        if value["application"] == value["source_application"]:
            # The current checkpoint store is application-keyed, not namespace-keyed.
            raise ValueError("SEASONAL_EXPERIMENT requires distinct application names")
        return cls(**value)

    @property
    def config_sha256(self):
        canonical = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    def matches(self, application, namespace, metric_type):
        return (application, namespace, metric_type) == (self.application, self.namespace, "requests")

    def provenance(self):
        return {**asdict(self), "config_sha256": self.config_sha256,
                "forecast_mode": "seasonal-only", "feedback_actuals": "source",
                "checkpoint_reload": "source-route", "pattern_weight": 1.0}
