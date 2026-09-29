"""Strict, opt-in routing for the declared forecaster arms (Task 03 U-21, U-23, R1).

`ENSEMBLE_EXPERIMENT` is one JSON object or a JSON list of objects. Each names one application
whose forecasts come from a declared forecaster computed on a source application's history, the
same shared-input design as SEASONAL_EXPERIMENT: `forecaster` "seasonal-ensemble" (the default;
models/seasonal_ensemble.py) or "relative-profile-ar" (models/relative_profile_ar.py, the model
lab's pr_ar_rob), with an optional `margin_quantile` (default 0.90), `margin_mode` and, for the
ensemble only, `partial_rule`. Unset means off; malformed configuration fails at startup. Several
experiments may share a source; their applications and ids must be distinct.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import re

from models import relative_profile_ar, seasonal_ensemble

REQUIRED = ("id", "application", "namespace", "source_application", "source_namespace")
OPTIONAL = ("margin_quantile", "margin_mode", "partial_rule", "forecaster")
QUANTILE_RANGE = (0.5, 0.99)
FORECASTERS = {"seasonal-ensemble": seasonal_ensemble, "relative-profile-ar": relative_profile_ar}
DEFAULT_FORECASTER = "seasonal-ensemble"


@dataclass(frozen=True)
class EnsembleExperiment:
    id: str
    application: str
    namespace: str
    source_application: str
    source_namespace: str
    margin_quantile: float = seasonal_ensemble.MARGIN_QUANTILE
    margin_mode: str = "absolute"
    partial_rule: str = "refuse"
    forecaster: str = DEFAULT_FORECASTER

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or not set(REQUIRED) <= set(value) or \
                not set(value) <= set(REQUIRED) | set(OPTIONAL):
            raise ValueError("ENSEMBLE_EXPERIMENT requires exactly id, application, namespace, "
                             "source_application, source_namespace (plus optional margin_quantile, "
                             "margin_mode, partial_rule, forecaster)")
        for key in REQUIRED:
            name = value[key]
            if not isinstance(name, str) or len(name) > 63 or not re.fullmatch(
                    r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", name):
                raise ValueError(f"ENSEMBLE_EXPERIMENT invalid {key}")
        if value["application"] == value["source_application"]:
            raise ValueError("ENSEMBLE_EXPERIMENT requires distinct application names")
        q = value.get("margin_quantile", seasonal_ensemble.MARGIN_QUANTILE)
        if isinstance(q, bool) or not isinstance(q, (int, float)) or not (QUANTILE_RANGE[0] <= q <= QUANTILE_RANGE[1]):
            raise ValueError(f"ENSEMBLE_EXPERIMENT margin_quantile must be a number in "
                             f"[{QUANTILE_RANGE[0]}, {QUANTILE_RANGE[1]}]")
        mode = value.get("margin_mode", "absolute")
        rule = value.get("partial_rule", "refuse")
        if mode not in seasonal_ensemble.MARGIN_MODES:
            raise ValueError(f"ENSEMBLE_EXPERIMENT margin_mode must be one of {seasonal_ensemble.MARGIN_MODES}")
        if rule not in seasonal_ensemble.PARTIAL_RULES:
            raise ValueError(f"ENSEMBLE_EXPERIMENT partial_rule must be one of {seasonal_ensemble.PARTIAL_RULES}")
        forecaster = value.get("forecaster", DEFAULT_FORECASTER)
        if forecaster not in FORECASTERS:
            raise ValueError(f"ENSEMBLE_EXPERIMENT forecaster must be one of {tuple(FORECASTERS)}")
        if forecaster != DEFAULT_FORECASTER and "partial_rule" in value:
            raise ValueError(f"ENSEMBLE_EXPERIMENT partial_rule applies to {DEFAULT_FORECASTER} only "
                             f"({forecaster} has one component)")
        return cls(**{**{k: value[k] for k in REQUIRED}, "margin_quantile": float(q), "margin_mode": mode,
                      "partial_rule": rule, "forecaster": forecaster})

    @classmethod
    def parse(cls, raw: str):
        """One experiment from a JSON object (the pre-U-23 form)."""
        try:
            value = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("ENSEMBLE_EXPERIMENT must be a JSON object") from exc
        return cls.from_dict(value)

    @classmethod
    def parse_all(cls, raw: str):
        """Every experiment from a JSON object or a JSON list of objects; ids and applications distinct."""
        try:
            value = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("ENSEMBLE_EXPERIMENT must be a JSON object or list") from exc
        items = value if isinstance(value, list) else [value]
        if not items:
            raise ValueError("ENSEMBLE_EXPERIMENT list must not be empty")
        exps = [cls.from_dict(v) for v in items]
        ids = [e.id for e in exps]
        apps = [(e.namespace, e.application) for e in exps]
        if len(set(ids)) != len(ids) or len(set(apps)) != len(apps):
            raise ValueError("ENSEMBLE_EXPERIMENT ids and applications must be distinct")
        return exps

    @property
    def module(self):
        return FORECASTERS[self.forecaster]

    @property
    def config_sha256(self):
        # The default forecaster is left out of the canonical form so the hash of a pre-R1
        # configuration (E1, E2) is unchanged by the field's existence.
        fields = asdict(self)
        if fields["forecaster"] == DEFAULT_FORECASTER:
            fields.pop("forecaster")
        canonical = json.dumps({**fields, "settings": self.module.SETTINGS},
                               sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    @property
    def forecast_mode(self):
        q = int(round(self.margin_quantile * 100))
        return f"{self.forecaster}-{'r' if self.margin_mode == 'relative' else ''}q{q}" + \
            ("-finite" if self.partial_rule == "finite" else "")

    def forecast_kwargs(self):
        """Keyword arguments for `self.module.forecast(points, now_ts, cache_key, **kwargs)`."""
        kwargs = {"margin_quantile": self.margin_quantile, "margin_mode": self.margin_mode}
        if self.forecaster == DEFAULT_FORECASTER:
            kwargs["partial_rule"] = self.partial_rule
        return kwargs

    def matches(self, application, namespace, metric_type):
        return (application, namespace, metric_type) == (self.application, self.namespace, "requests")

    def provenance(self):
        return {**asdict(self), "config_sha256": self.config_sha256,
                "forecast_mode": self.forecast_mode, "feedback_actuals": "source",
                "forecaster_version": self.module.VERSION}
