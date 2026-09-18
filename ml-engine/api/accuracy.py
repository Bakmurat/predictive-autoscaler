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

    def _key(self, application: str, namespace: str, metric_type: str) -> tuple:
        return (application, namespace, metric_type)

    def record(self, application: str, namespace: str, metric_type: str,
               predicted: float, actual: float) -> None:
        """Record a (predicted, actual) comparison entry."""
        key = self._key(application, namespace, metric_type)
        if key not in self.history:
            self.history[key] = deque(maxlen=WINDOW_SIZE)
        self.history[key].append((datetime.now(timezone.utc), predicted, actual))

    def get_mape(self, application: str, namespace: str, metric_type: str) -> float:
        """Calculate traffic-weighted MAPE over rolling window.

        Returns 0.0 if fewer than 2 valid entries. Skips entries where actual < MIN_TRAFFIC_RPM (1000 RPM).
        Formula: sum(abs(actual - predicted)) / sum(actual) * 100, rounded to 2 decimals.
        Traffic-weighted -- errors at higher traffic contribute proportionally more.
        """
        key = self._key(application, namespace, metric_type)
        entries = self.history.get(key, [])

        # Filter to entries above minimum traffic threshold
        valid = [(p, a) for (_, p, a) in entries if a >= MIN_TRAFFIC_RPM]

        if len(valid) < 2:
            return 0.0

        numerator = sum(abs(a - p) for p, a in valid)
        denominator = sum(a for _, a in valid)

        if denominator < 0.01:
            return 0.0

        mape = (numerator / denominator) * 100
        return round(mape, 2)

    def get_mae(self, application: str, namespace: str, metric_type: str) -> float:
        """Calculate MAE over rolling window.

        Returns 0.0 if fewer than 2 entries.
        Formula: mean(abs(actual - predicted)), rounded to 2 decimals.
        """
        key = self._key(application, namespace, metric_type)
        entries = self.history.get(key, [])

        if len(entries) < 2:
            return 0.0

        total = sum(abs(a - p) for (_, p, a) in entries)
        mae = total / len(entries)
        return round(mae, 2)

    def store_prediction(self, application: str, namespace: str, metric_type: str,
                         predicted_value: float) -> None:
        """Store a prediction value for later comparison against actuals."""
        key = self._key(application, namespace, metric_type)
        self.last_predictions[key] = (datetime.now(timezone.utc), predicted_value)

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

    def get_component_mape(self, application: str, namespace: str, metric_type: str,
                           component: str) -> float:
        """Calculate traffic-weighted MAPE for a specific component over rolling window.

        Same formula as get_mape() but scoped to a single component.
        Returns 0.0 if fewer than 2 valid entries.
        """
        key = (application, namespace, metric_type, component)
        entries = self.component_history.get(key, [])
        valid = [(p, a) for (_, p, a) in entries if a >= MIN_TRAFFIC_RPM]
        if len(valid) < 2:
            return 0.0
        numerator = sum(abs(a - p) for p, a in valid)
        denominator = sum(a for _, a in valid)
        if denominator < 0.01:
            return 0.0
        mape = (numerator / denominator) * 100
        return round(mape, 2)
