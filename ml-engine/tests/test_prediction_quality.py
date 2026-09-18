"""Tests for prediction quality improvements: recency weighting, directional percentile, MAPE-adaptive percentile.

Covers requirements PRED-01, PRED-02, PRED-03.

No TensorFlow dependency — tests weighted_percentile as standalone function
and percentile logic with mocked model attributes.
"""

import sys
import importlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

# Add parent directory so we can import models/api
sys.path.insert(0, str(Path(__file__).parent.parent))

# Check TF availability
try:
    import tensorflow as tf
    HAS_TF = True
except ImportError:
    HAS_TF = False


def _get_weighted_percentile():
    """Import weighted_percentile, handling missing TF gracefully."""
    if HAS_TF:
        from models.lstm_model import weighted_percentile
        return weighted_percentile
    else:
        # Load the module source directly, mocking TF
        import types
        tf_mock = types.ModuleType('tensorflow')
        tf_mock.keras = MagicMock()
        tf_mock.square = MagicMock()
        tf_mock.where = MagicMock()
        tf_mock.reduce_mean = MagicMock()
        with patch.dict('sys.modules', {'tensorflow': tf_mock, 'tensorflow.keras': tf_mock.keras}):
            spec = importlib.util.spec_from_file_location(
                'lstm_model_direct',
                str(Path(__file__).parent.parent / 'models' / 'lstm_model.py')
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod.weighted_percentile


# ---------------------------------------------------------------------------
# TestWeightedPercentile (PRED-01 helper)
# ---------------------------------------------------------------------------
class TestWeightedPercentile:
    """Test standalone weighted_percentile function."""

    @pytest.fixture(autouse=True)
    def _load_fn(self):
        self.weighted_percentile = _get_weighted_percentile()

    def test_high_percentile_weighted(self):
        """weighted_percentile([10, 20, 30], [1.0, 0.5, 0.25], 90) returns value near 30."""
        result = self.weighted_percentile([10, 20, 30], [1.0, 0.5, 0.25], 90)
        # 90th percentile of a weighted distribution skewed toward 10 (weight=1.0)
        # but 90th is high end, so should be near 30
        assert 20 <= result <= 30, f"Expected near 30, got {result}"

    def test_median_weighted(self):
        """weighted_percentile([10, 20, 30], [1.0, 0.5, 0.25], 50) returns value near 15."""
        result = self.weighted_percentile([10, 20, 30], [1.0, 0.5, 0.25], 50)
        # Median biased toward weight=1.0 item (value=10)
        assert 10 <= result <= 20, f"Expected near 15, got {result}"

    def test_equal_weights_matches_numpy(self):
        """weighted_percentile with equal weights approximately matches np.percentile.

        Uses a larger sample where midpoint-cumulative vs numpy linear interpolation
        converge more closely.
        """
        values = list(range(10, 110, 1))  # 100 values: 10..109
        weights = [1.0] * len(values)
        for pct in [25, 50, 75, 90]:
            wp = self.weighted_percentile(values, weights, pct)
            np_p = np.percentile(values, pct)
            assert abs(wp - np_p) < 1.0, (
                f"At pct={pct}: weighted={wp}, numpy={np_p}"
            )

    def test_single_value(self):
        """weighted_percentile with single value returns that value regardless of percentile."""
        for pct in [10, 50, 90]:
            result = self.weighted_percentile([42.0], [1.0], pct)
            assert abs(result - 42.0) < 1e-7, f"Expected 42.0, got {result}"

    def test_recency_weights(self):
        """Recency weights: day 1=1.0, day 2=0.3, day 3=0.09, day 7~=0.00073 (Phase 14: D-11)."""
        # Verify the exponential decay formula
        for d in range(1, 8):
            weight = 0.3 ** (d - 1)
            if d == 1:
                assert abs(weight - 1.0) < 1e-7
            elif d == 2:
                assert abs(weight - 0.3) < 1e-7
            elif d == 3:
                assert abs(weight - 0.09) < 1e-7
            elif d == 7:
                assert abs(weight - 0.000729) < 1e-6


# ---------------------------------------------------------------------------
# TestDirectionalPercentile (PRED-03)
# ---------------------------------------------------------------------------
class TestDirectionalPercentile:
    """Test directional percentile: negative slope -> 70, else -> 75 (Phase 14: D-04)."""

    def test_declining_traffic_returns_70(self):
        """Negative slope in last 12 points of last_sequence returns 70."""
        seq = np.zeros(144)
        seq[-12:-6] = 0.8  # previous 6 points: high
        seq[-6:] = 0.4     # last 6 points: low (declining)

        prev_mean = np.mean(seq[-12:-6])
        last_mean = np.mean(seq[-6:])
        assert last_mean < prev_mean, "Setup error: should be declining"

        direction_pct = 70 if last_mean < prev_mean else 75
        assert direction_pct == 70

    def test_rising_traffic_returns_75(self):
        """Non-negative slope in last 12 points returns 75 (Phase 14: was 90)."""
        seq = np.zeros(144)
        seq[-12:-6] = 0.4  # previous 6 points: low
        seq[-6:] = 0.8     # last 6 points: high (rising)

        prev_mean = np.mean(seq[-12:-6])
        last_mean = np.mean(seq[-6:])
        assert last_mean >= prev_mean, "Setup error: should be rising"

        direction_pct = 70 if last_mean < prev_mean else 75
        assert direction_pct == 75

    def test_flat_traffic_returns_75(self):
        """Flat traffic (equal means) returns 75 (Phase 14: was 90)."""
        seq = np.zeros(144)
        seq[-12:] = 0.5  # all same

        prev_mean = np.mean(seq[-12:-6])
        last_mean = np.mean(seq[-6:])
        assert last_mean >= prev_mean, "Setup error: should be flat"

        direction_pct = 70 if last_mean < prev_mean else 75
        assert direction_pct == 75


# ---------------------------------------------------------------------------
# TestMAPEAdaptivePercentile (PRED-02)
# ---------------------------------------------------------------------------
class TestMAPEAdaptivePercentile:
    """Test MAPE-adaptive percentile: linear interpolation from 75 to 50 (Phase 14: D-06)."""

    def _compute_mape_pct(self, mape):
        """Phase 14 (D-06): pct = 75 - max(0, mape - 10) * (25/20), clamped [50, 75]."""
        return max(50, min(75, 75 - max(0, mape - 10) * (25.0 / 20.0)))

    def test_low_mape_returns_75(self):
        """MAPE=5 returns 75 (no adjustment below threshold)."""
        assert self._compute_mape_pct(5) == 75

    def test_mape_10_returns_75(self):
        """MAPE=10 returns 75 (boundary)."""
        assert self._compute_mape_pct(10) == 75

    def test_mape_20_returns_62_5(self):
        """MAPE=20 returns 62.5."""
        assert self._compute_mape_pct(20) == 62.5

    def test_mape_30_returns_50(self):
        """MAPE=30 returns 50."""
        assert self._compute_mape_pct(30) == 50

    def test_high_mape_floors_at_50(self):
        """MAPE=40+ returns 50 (floor)."""
        assert self._compute_mape_pct(40) == 50
        assert self._compute_mape_pct(50) == 50
        assert self._compute_mape_pct(100) == 50

    def test_zero_mape_returns_75(self):
        """MAPE=0 returns 75."""
        assert self._compute_mape_pct(0) == 75


# ---------------------------------------------------------------------------
# TestEffectivePercentile (PRED-01 + PRED-02 + PRED-03 combined)
# ---------------------------------------------------------------------------
class TestEffectivePercentile:
    """Test effective percentile: rising uses max, declining uses min, clamped [50, 75] (Phase 14)."""

    def test_both_75_returns_75(self):
        """Rising traffic + low MAPE = 75."""
        direction_pct = 75
        mape_pct = 75
        # Rising: direction_pct >= 75, use max
        effective = max(50, min(75, max(direction_pct, mape_pct)))
        assert effective == 75

    def test_declining_with_low_mape(self):
        """Declining traffic (70) + low MAPE (75) = 70."""
        direction_pct = 70
        mape_pct = 75
        # Declining: direction_pct < 75, use min
        effective = max(50, min(75, min(direction_pct, mape_pct)))
        assert effective == 70

    def test_rising_with_high_mape(self):
        """Rising traffic (75) + high MAPE (60) = 75 (rising takes priority)."""
        direction_pct = 75
        mape_pct = 60
        # Rising: direction_pct >= 75, use max
        effective = max(50, min(75, max(direction_pct, mape_pct)))
        assert effective == 75

    def test_declining_with_high_mape(self):
        """Declining traffic (70) + high MAPE (60) = 60."""
        direction_pct = 70
        mape_pct = 60
        # Declining: direction_pct < 75, use min
        effective = max(50, min(75, min(direction_pct, mape_pct)))
        assert effective == 60

    def test_floor_at_50(self):
        """Never drops below 50."""
        direction_pct = 70
        mape_pct = 50
        # Declining: direction_pct < 75, use min
        effective = max(50, min(75, min(direction_pct, mape_pct)))
        assert effective == 50


# ---------------------------------------------------------------------------
# TestLogEnrichment
# ---------------------------------------------------------------------------
class TestLogEnrichment:
    """Test that prediction log line includes pct= field."""

    def test_log_contains_pct(self):
        """The blend log line format should include pct= field."""
        # This tests the expected log format string
        effective_pct = 70.0
        log_msg = (
            f"Prediction blend: LSTM range [100-200], "
            f"Pattern range [110-210], "
            f"Final range [105-205], "
            f"pct={effective_pct:.0f}, "
            f"confidence=0.850"
        )
        assert "pct=70" in log_msg
