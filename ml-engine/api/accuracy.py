"""Accuracy tracking for ML predictions.

Tracks MAPE (Mean Absolute Percentage Error) and MAE (Mean Absolute Error)
by comparing previous predictions against current actuals over a rolling
24-hour window (144 entries at 10-min intervals).
"""

from collections import deque
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple


WINDOW_SIZE = 144  # 24h at 10-min intervals
MIN_TRAFFIC_RPM = 1000  # Phase 14 (INFRA-02): minimum actual RPM for MAPE inclusion


class AccuracyTracker:
    """Tracks prediction accuracy using a rolling window of (predicted, actual) pairs."""

    def __init__(self):
        # Key: (application, namespace, metric_type) -> deque of (timestamp, predicted, actual)
        self.history: Dict[tuple, deque] = {}
        # Key: (application, namespace, metric_type) -> (timestamp, predicted_value)
        self.last_predictions: Dict[tuple, Tuple[datetime, float]] = {}
        # Per-component tracking: key (app, ns, metric_type, component) -> deque of (ts, predicted, actual)
        self.component_history: Dict[tuple, deque] = {}
        # C-48: forecasts awaiting their target time.
        # Key: (application, namespace, metric_type, component|None) -> deque of (target_at, predicted)
        self.pending: Dict[tuple, deque] = {}

    def _key(self, application: str, namespace: str, metric_type: str) -> tuple:
        return (application, namespace, metric_type)

    def record(self, application: str, namespace: str, metric_type: str,
               predicted: float, actual: float) -> None:
        """Record a (predicted, actual) comparison entry."""
        key = self._key(application, namespace, metric_type)
        if key not in self.history:
            self.history[key] = deque(maxlen=WINDOW_SIZE)
        self.history[key].append((datetime.now(timezone.utc), predicted, actual))

    def mape_stats(self, application: str, namespace: str, metric_type: str) -> Dict:
        """Traffic-weighted MAPE over the rolling window, WITH its provenance (C-85).

        A zero from an empty window used to read as perfect accuracy when it meant nothing
        had been scored. Every error figure now travels with `scored` (entries that entered
        the calculation), `recorded` (entries in the window), `availability` (scored /
        recorded) and `measured`. `value` is None -- never 0.0 -- when nothing was measured.

        Entries whose actual is below MIN_TRAFFIC_RPM are recorded but not scored (the
        documented trough filter); the counts make that visible instead of silent.
        Formula: sum(abs(actual - predicted)) / sum(actual) * 100, rounded to 2 decimals.
        """
        key = self._key(application, namespace, metric_type)
        entries = list(self.history.get(key, []))
        valid = [(p, a) for (_, p, a) in entries if a >= MIN_TRAFFIC_RPM]
        recorded, scored = len(entries), len(valid)
        out = {"value": None, "scored": scored, "recorded": recorded,
               "availability": (round(scored / recorded, 4) if recorded else None),
               "measured": False}
        if scored < 1:
            return out
        denominator = sum(a for _, a in valid)
        if denominator < 0.01:
            return out
        numerator = sum(abs(a - p) for p, a in valid)
        out["value"] = round((numerator / denominator) * 100, 2)
        out["measured"] = True
        return out

    def get_mape(self, application: str, namespace: str, metric_type: str) -> Optional[float]:
        """MAPE value, or None when nothing was measured (C-85: never 0.0 for "unknown")."""
        return self.mape_stats(application, namespace, metric_type)["value"]

    def mae_stats(self, application: str, namespace: str, metric_type: str) -> Dict:
        """MAE over the rolling window, with provenance (C-85). See mape_stats()."""
        key = self._key(application, namespace, metric_type)
        entries = list(self.history.get(key, []))
        recorded = scored = len(entries)
        out = {"value": None, "scored": scored, "recorded": recorded,
               "availability": (1.0 if recorded else None), "measured": False}
        if scored < 1:
            return out
        out["value"] = round(sum(abs(a - p) for (_, p, a) in entries) / scored, 2)
        out["measured"] = True
        return out

    def get_mae(self, application: str, namespace: str, metric_type: str) -> Optional[float]:
        """MAE value, or None when nothing was measured (C-85)."""
        return self.mae_stats(application, namespace, metric_type)["value"]

    def store_prediction(self, application: str, namespace: str, metric_type: str,
                         predicted_value: float) -> None:
        """Store a prediction value for later comparison against actuals.

        Deprecated for accuracy purposes (C-48): it carries no target timestamp, so the
        consumer cannot tell whether the target has matured. Kept for compatibility;
        use store_forecast()/take_matured() instead.
        """
        key = self._key(application, namespace, metric_type)
        self.last_predictions[key] = (datetime.now(timezone.utc), predicted_value)

    # --- timestamp-matched scoring (C-48) ------------------------------------------------
    def store_forecast(self, application: str, namespace: str, metric_type: str,
                       target_at: datetime, predicted_value: float,
                       component: Optional[str] = None) -> None:
        """Queue a forecast for scoring WHEN ITS TARGET TIME ARRIVES.

        A forecast for t+10min is not an error signal until t+10min has actually passed and
        the observation for that timestamp exists. Comparing it against 'now' -- which the
        previous code did on every call, typically 60 s later -- measures nothing.
        """
        key = (application, namespace, metric_type, component)
        pend = self.pending.setdefault(key, deque(maxlen=512))
        pend.append((self._as_utc(target_at), float(predicted_value)))

    def take_matured(self, application: str, namespace: str, metric_type: str,
                     observation_at: datetime, tolerance_s: int = 300,
                     component: Optional[str] = None):
        """Return queued forecasts whose target matches `observation_at`, and drop stale ones.

        Returns a list of predicted values to score against the observation at that timestamp.
        Entries whose target is still in the future are left queued; entries older than the
        tolerance are discarded as unmatched (they can never be scored correctly).
        """
        key = (application, namespace, metric_type, component)
        pend = self.pending.get(key)
        if not pend:
            return []
        obs = self._as_utc(observation_at)
        matured, keep = [], deque(maxlen=512)
        for target_at, predicted in pend:
            delta = (obs - target_at).total_seconds()
            if abs(delta) <= tolerance_s:
                matured.append(predicted)
            elif delta < -tolerance_s:
                keep.append((target_at, predicted))  # target still in the future
            # delta > tolerance: the observation for that target never arrived -- drop it
        self.pending[key] = keep
        return matured

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def get_last_prediction(self, application: str, namespace: str,
                            metric_type: str) -> Optional[float]:
        """Get the last stored prediction value, or None if not stored."""
        key = self._key(application, namespace, metric_type)
        entry = self.last_predictions.get(key)
        if entry is None:
            return None
        return entry[1]

    def record_and_update(self, application: str, namespace: str, metric_type: str,
                          predicted: float, actual: float) -> Tuple[float, float]:
        """Record entry and return current (MAPE, MAE)."""
        self.record(application, namespace, metric_type, predicted, actual)
        mape = self.get_mape(application, namespace, metric_type)
        mae = self.get_mae(application, namespace, metric_type)
        return mape, mae

    def record_component(self, application: str, namespace: str, metric_type: str,
                         component: str, predicted: float, actual: float) -> None:
        """Record a per-component (predicted, actual) comparison entry."""
        key = (application, namespace, metric_type, component)
        if key not in self.component_history:
            self.component_history[key] = deque(maxlen=WINDOW_SIZE)
        self.component_history[key].append((datetime.now(timezone.utc), predicted, actual))

    def component_mape_stats(self, application: str, namespace: str, metric_type: str,
                             component: str) -> Dict:
        """Per-component traffic-weighted MAPE with provenance (C-85). See mape_stats()."""
        key = (application, namespace, metric_type, component)
        entries = list(self.component_history.get(key, []))
        valid = [(p, a) for (_, p, a) in entries if a >= MIN_TRAFFIC_RPM]
        recorded, scored = len(entries), len(valid)
        out = {"value": None, "scored": scored, "recorded": recorded,
               "availability": (round(scored / recorded, 4) if recorded else None),
               "measured": False}
        if scored < 1:
            return out
        denominator = sum(a for _, a in valid)
        if denominator < 0.01:
            return out
        out["value"] = round((sum(abs(a - p) for p, a in valid) / denominator) * 100, 2)
        out["measured"] = True
        return out

    def get_component_mape(self, application: str, namespace: str, metric_type: str,
                           component: str) -> Optional[float]:
        """Component MAPE value, or None when nothing was measured (C-85)."""
        return self.component_mape_stats(application, namespace, metric_type, component)["value"]
