"""Tests for AccuracyTracker MAPE/MAE calculation."""

import sys
from pathlib import Path

# Add parent directory so we can import api.accuracy
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.accuracy import AccuracyTracker, MIN_TRAFFIC_RPM


class TestAccuracyTrackerRecord:
    """Test that record() stores entries in per-key deques."""

    def test_record_stores_entry(self):
        tracker = AccuracyTracker()
        tracker.record("app1", "ns1", "cpu", 100.0, 90.0)
        key = ("app1", "ns1", "cpu")
        assert key in tracker.history
        assert len(tracker.history[key]) == 1

    def test_record_multiple_entries(self):
        tracker = AccuracyTracker()
        tracker.record("app1", "ns1", "cpu", 100.0, 90.0)
        tracker.record("app1", "ns1", "cpu", 110.0, 100.0)
        key = ("app1", "ns1", "cpu")
        assert len(tracker.history[key]) == 2

    def test_record_separate_keys(self):
        tracker = AccuracyTracker()
        tracker.record("app1", "ns1", "cpu", 100.0, 90.0)
        tracker.record("app1", "ns1", "memory", 200.0, 180.0)
        assert len(tracker.history[("app1", "ns1", "cpu")]) == 1
        assert len(tracker.history[("app1", "ns1", "memory")]) == 1

    def test_deque_maxlen_144(self):
        tracker = AccuracyTracker()
        for i in range(145):
            tracker.record("app1", "ns1", "cpu", float(i + 10), float(i))
        key = ("app1", "ns1", "cpu")
        assert len(tracker.history[key]) == 144
        # Oldest entry (i=0) should have been dropped; first entry should be i=1
        _, predicted, actual = tracker.history[key][0]
        assert actual == 1.0
        assert predicted == 11.0


class TestGetMAPE:
    """Test MAPE calculation."""

    def test_mape_known_values(self):
        """predicted=[1100,900], actual=[1000,1000] -> weighted MAPE=10.0%
        numerator = |1000-1100| + |1000-900| = 200, denominator = 2000, MAPE = 10.0%
        """
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 1100.0, 1000.0)
        tracker.record("a", "b", "c", 900.0, 1000.0)
        mape = tracker.get_mape("a", "b", "c")
        assert mape == 10.0

    def test_mape_returns_zero_fewer_than_2_entries(self):
        tracker = AccuracyTracker()
        assert tracker.get_mape("a", "b", "c") == 0.0
        tracker.record("a", "b", "c", 1100.0, 1000.0)
        assert tracker.get_mape("a", "b", "c") == 0.0

    def test_mape_skips_below_threshold_actual(self):
        """Entries with actual < MIN_TRAFFIC_RPM (1000 RPM) should be skipped (Phase 14 D-08)."""
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 1100.0, 1000.0)
        tracker.record("a", "b", "c", 600.0, 500.0)  # below 1000 RPM threshold, should be skipped
        tracker.record("a", "b", "c", 900.0, 1000.0)
        # Only two valid entries: (1100,1000) and (900,1000) -> MAPE=10%
        mape = tracker.get_mape("a", "b", "c")
        assert mape == 10.0

    def test_mape_includes_entries_at_threshold(self):
        """Entries with actual >= 1000 RPM are included in MAPE."""
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 1100.0, 1000.0)
        tracker.record("a", "b", "c", 900.0, 1000.0)
        mape = tracker.get_mape("a", "b", "c")
        assert mape == 10.0

    def test_mape_all_below_threshold_returns_zero(self):
        """If all actuals are below 1000 RPM threshold, MAPE should return 0.0."""
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 200.0, 500.0)
        tracker.record("a", "b", "c", 300.0, 800.0)
        assert tracker.get_mape("a", "b", "c") == 0.0

    def test_mape_night_filter_entries_still_stored(self):
        """Entries below threshold are stored in deque but excluded from MAPE calculation (D-08)."""
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 200.0, 500.0)   # below threshold
        tracker.record("a", "b", "c", 1100.0, 1000.0)  # above threshold
        tracker.record("a", "b", "c", 900.0, 1000.0)   # above threshold
        key = ("a", "b", "c")
        assert len(tracker.history[key]) == 3  # all stored
        mape = tracker.get_mape("a", "b", "c")
        assert mape == 10.0  # only above-threshold entries used

    def test_mape_rounded_to_2_decimals(self):
        """predicted=[10500,9500,10300], actual=[10000,10000,10000] -> weighted MAPE=4.33%
        numerator = 500+500+300 = 1300, denominator = 30000, MAPE = 4.333...%
        """
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 10500.0, 10000.0)
        tracker.record("a", "b", "c", 9500.0, 10000.0)
        tracker.record("a", "b", "c", 10300.0, 10000.0)
        mape = tracker.get_mape("a", "b", "c")
        assert mape == 4.33

    def test_mape_weighted_trough_marginalized(self):
        """Trough error should be marginalized by high-traffic entry.
        predicted=[60000, 1500], actual=[60000, 1000]
        numerator = |60000-60000| + |1000-1500| = 0 + 500 = 500
        denominator = 60000 + 1000 = 61000
        weighted MAPE = 500/61000*100 = 0.82%
        """
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 60000.0, 60000.0)
        tracker.record("a", "b", "c", 1500.0, 1000.0)
        mape = tracker.get_mape("a", "b", "c")
        assert mape == 0.82

    def test_mape_weighted_vs_unweighted_differs(self):
        """Weighted and unweighted should differ when actuals have different magnitudes.
        predicted=[60000, 3000], actual=[60000, 1500]
        numerator = 0 + 1500 = 1500, denominator = 61500
        weighted = 1500/61500*100 = 2.44%
        unweighted = (0% + 100%) / 2 = 50% -- very different!
        """
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 60000.0, 60000.0)
        tracker.record("a", "b", "c", 3000.0, 1500.0)
        mape = tracker.get_mape("a", "b", "c")
        assert mape == 2.44

    def test_mape_all_below_threshold_is_zero(self):
        """When all entries are below MIN_TRAFFIC_RPM, MAPE returns 0.0."""
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 1.0, 10.0)
        tracker.record("a", "b", "c", 2.0, 20.0)
        mape = tracker.get_mape("a", "b", "c")
        assert mape == 0.0


class TestGetMAE:
    """Test MAE calculation."""

    def test_mae_known_values(self):
        """predicted=[110,90], actual=[100,100] -> MAE=10.0"""
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 110.0, 100.0)
        tracker.record("a", "b", "c", 90.0, 100.0)
        mae = tracker.get_mae("a", "b", "c")
        assert mae == 10.0

    def test_mae_returns_zero_fewer_than_2_entries(self):
        tracker = AccuracyTracker()
        assert tracker.get_mae("a", "b", "c") == 0.0
        tracker.record("a", "b", "c", 110.0, 100.0)
        assert tracker.get_mae("a", "b", "c") == 0.0

    def test_mae_rounded_to_2_decimals(self):
        tracker = AccuracyTracker()
        tracker.record("a", "b", "c", 105.0, 100.0)
        tracker.record("a", "b", "c", 95.0, 100.0)
        tracker.record("a", "b", "c", 103.0, 100.0)
        mae = tracker.get_mae("a", "b", "c")
        # (5+5+3)/3 = 4.333...
        assert mae == 4.33


class TestStorePrediction:
    """Test store/retrieve last prediction."""

    def test_store_and_get_prediction(self):
        tracker = AccuracyTracker()
        tracker.store_prediction("app1", "ns1", "cpu", 42.0)
        result = tracker.get_last_prediction("app1", "ns1", "cpu")
        assert result == 42.0

    def test_get_prediction_returns_none_if_not_stored(self):
        tracker = AccuracyTracker()
        assert tracker.get_last_prediction("app1", "ns1", "cpu") is None

    def test_store_prediction_overwrites(self):
        tracker = AccuracyTracker()
        tracker.store_prediction("app1", "ns1", "cpu", 42.0)
        tracker.store_prediction("app1", "ns1", "cpu", 99.0)
        assert tracker.get_last_prediction("app1", "ns1", "cpu") == 99.0


class TestRecordAndUpdate:
    """Test record_and_update convenience method."""

    def test_record_and_update_returns_mape_mae(self):
        tracker = AccuracyTracker()
        # Need at least 2 entries for non-zero MAPE/MAE (actual >= 1000 RPM for MAPE)
        tracker.record("a", "b", "c", 1100.0, 1000.0)
        mape, mae = tracker.record_and_update("a", "b", "c", 900.0, 1000.0)
        assert mape == 10.0
        assert mae == 100.0

    def test_record_and_update_first_entry_returns_zeros(self):
        tracker = AccuracyTracker()
        mape, mae = tracker.record_and_update("a", "b", "c", 1100.0, 1000.0)
        assert mape == 0.0
        assert mae == 0.0


# ---------------------------------------------------------------------------
# PRED-06: Per-component MAPE tracking (record_component / get_component_mape)
# ---------------------------------------------------------------------------

class TestRecordComponent:
    """Test that record_component stores entries in per-component deques."""

    def test_record_component_stores_entry(self):
        tracker = AccuracyTracker()
        tracker.record_component("app1", "ns1", "requests", "lstm", 1000.0, 900.0)
        key = ("app1", "ns1", "requests", "lstm")
        assert key in tracker.component_history
        assert len(tracker.component_history[key]) == 1

    def test_record_component_separate_components_use_separate_keys(self):
        tracker = AccuracyTracker()
        tracker.record_component("app1", "ns1", "requests", "lstm", 1000.0, 900.0)
        tracker.record_component("app1", "ns1", "requests", "pattern", 950.0, 900.0)
        tracker.record_component("app1", "ns1", "requests", "blended", 960.0, 900.0)
        assert len(tracker.component_history) == 3
        assert len(tracker.component_history[("app1", "ns1", "requests", "lstm")]) == 1
        assert len(tracker.component_history[("app1", "ns1", "requests", "pattern")]) == 1
        assert len(tracker.component_history[("app1", "ns1", "requests", "blended")]) == 1

    def test_component_history_deque_maxlen_is_144(self):
        """component_history deque must have maxlen=144 (WINDOW_SIZE)."""
        tracker = AccuracyTracker()
        for i in range(145):
            tracker.record_component("app1", "ns1", "requests", "lstm", float(i + 10), float(i + 1))
        key = ("app1", "ns1", "requests", "lstm")
        assert len(tracker.component_history[key]) == 144, (
            f"component_history deque should cap at 144 entries (WINDOW_SIZE), "
            f"got {len(tracker.component_history[key])}"
        )

    def test_component_history_deque_drops_oldest_entry(self):
        """When 145 entries are recorded the oldest (i=0) is evicted."""
        tracker = AccuracyTracker()
        for i in range(145):
            tracker.record_component("app1", "ns1", "requests", "lstm", float(i + 10), float(i + 1))
        key = ("app1", "ns1", "requests", "lstm")
        _, first_predicted, first_actual = tracker.component_history[key][0]
        # i=0 (actual=1.0) is evicted; first remaining is i=1 (actual=2.0)
        assert first_actual == 2.0, f"Expected oldest evicted entry to be gone, got actual={first_actual}"


class TestGetComponentMape:
    """Test get_component_mape() calculations."""

    def test_get_component_mape_known_values(self):
        """predicted=[1100,900], actual=[1000,1000] -> traffic-weighted MAPE=10.0%"""
        tracker = AccuracyTracker()
        tracker.record_component("a", "b", "c", "lstm", 1100.0, 1000.0)
        tracker.record_component("a", "b", "c", "lstm", 900.0, 1000.0)
        mape = tracker.get_component_mape("a", "b", "c", "lstm")
        assert mape == 10.0

    def test_get_component_mape_returns_zero_with_fewer_than_2_entries(self):
        """Returns 0.0 when there are 0 or 1 valid entries."""
        tracker = AccuracyTracker()
        assert tracker.get_component_mape("a", "b", "c", "lstm") == 0.0
        tracker.record_component("a", "b", "c", "lstm", 1100.0, 1000.0)
        assert tracker.get_component_mape("a", "b", "c", "lstm") == 0.0

    def test_get_component_mape_skips_below_threshold_actual(self):
        """Entries where actual < MIN_TRAFFIC_RPM (1000) must be excluded from MAPE calculation."""
        tracker = AccuracyTracker()
        tracker.record_component("a", "b", "c", "pattern", 1100.0, 1000.0)
        tracker.record_component("a", "b", "c", "pattern", 600.0, 500.0)  # below threshold — skip
        tracker.record_component("a", "b", "c", "pattern", 900.0, 1000.0)
        mape = tracker.get_component_mape("a", "b", "c", "pattern")
        # Only two valid entries: (1100,1000) and (900,1000) -> MAPE=10%
        assert mape == 10.0

    def test_get_component_mape_traffic_weighted_formula(self):
        """Uses same traffic-weighted formula as get_mape(): sum(|a-p|)/sum(a)*100."""
        tracker = AccuracyTracker()
        # numerator = |60000-60000| + |1000-1500| = 500
        # denominator = 60000 + 1000 = 61000
        # MAPE = 500/61000*100 = 0.82%
        tracker.record_component("a", "b", "c", "blended", 60000.0, 60000.0)
        tracker.record_component("a", "b", "c", "blended", 1500.0, 1000.0)
        mape = tracker.get_component_mape("a", "b", "c", "blended")
        assert mape == 0.82

    def test_get_component_mape_rounded_to_2_decimals(self):
        """Result is rounded to 2 decimal places."""
        tracker = AccuracyTracker()
        # numerator = 500+500+300 = 1300, denominator = 30000, MAPE = 4.333...% -> 4.33
        tracker.record_component("a", "b", "c", "lstm", 10500.0, 10000.0)
        tracker.record_component("a", "b", "c", "lstm", 9500.0, 10000.0)
        tracker.record_component("a", "b", "c", "lstm", 10300.0, 10000.0)
        mape = tracker.get_component_mape("a", "b", "c", "lstm")
        assert mape == 4.33

    def test_get_component_mape_components_are_independent(self):
        """MAPE for one component must not be influenced by entries for another component."""
        tracker = AccuracyTracker()
        # lstm: perfect predictions -> MAPE=0%
        tracker.record_component("a", "b", "c", "lstm", 1000.0, 1000.0)
        tracker.record_component("a", "b", "c", "lstm", 1000.0, 1000.0)
        # pattern: large errors -> MAPE=50%
        tracker.record_component("a", "b", "c", "pattern", 1500.0, 1000.0)
        tracker.record_component("a", "b", "c", "pattern", 1500.0, 1000.0)
        lstm_mape = tracker.get_component_mape("a", "b", "c", "lstm")
        pattern_mape = tracker.get_component_mape("a", "b", "c", "pattern")
        assert lstm_mape == 0.0
        assert pattern_mape == 50.0

    def test_get_component_mape_returns_float(self):
        """Return type must always be float."""
        tracker = AccuracyTracker()
        result = tracker.get_component_mape("a", "b", "c", "lstm")
        assert isinstance(result, float)
