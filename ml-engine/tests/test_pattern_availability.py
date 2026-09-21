"""Per-step previous-day availability (Codex C-67 / D-88).

Two gates threw away usable previous-day observations, and a third invented ones that did not
exist:

  1. `models/lstm_model.py` refused the whole lookup when the history spanned less than 24h,
     on the premise that "less than one full day cannot contain yesterday's value for any
     target". False: targets lie in the FUTURE of the last observation, so step k's target at
     origin+10(k+1)min has its previous-day time at origin-1440+10(k+1)min -- no earlier than
     origin-23h50m, which a complete 144-point window already covers.
  2. `api/main.py` withheld the seasonal history entirely unless the full series was LONGER
     than the inference window, so a caller supplying exactly one window got no pattern at all.
  3. `_pattern_forecast` filled a step with no previous-day observation from its nearest
     neighbour, inventing an observation and hiding the gap from confidence and from the
     forecast record.

Measured on the real benchmark series before the fix: at the origin holding exactly 144 points
(23h50m span) all six steps had a genuine previous-day observation and the deployed code served
none of them.

These tests fail on the pre-fix code.
"""

import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144


def series_ending(end: datetime, points: int, base: float = 600.0, amplitude: float = 400.0):
    """Deterministic daily profile on the ten-minute grid, `points` long, ending at `end`."""
    idx = [end - GRID * (points - 1 - i) for i in range(points)]
    vals = [base + amplitude * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / 1440.0)
            for t in idx]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


def model():
    return LSTMForecastModel.__new__(LSTMForecastModel)


ORIGIN = datetime(2026, 9, 21, 18, 20)


def test_exactly_one_window_serves_every_step():
    """144 points span 23h50m: every one of the six targets has yesterday's value."""
    s = series_ending(ORIGIN, PER_DAY)
    span_h = (s.index[-1] - s.index[0]).total_seconds() / 3600
    assert span_h < 24, "precondition: this is the sub-24h span the old guard refused"

    arr, source = model()._pattern_forecast(
        origin=ORIGIN, steps_ahead=STEPS_AHEAD, seasonal_history=s, effective_pct=75)

    assert arr is not None, "the old span guard returned None here"
    assert source == "seasonal_history"
    assert not np.isnan(arr).any(), "all six steps must resolve from a complete window"


def test_per_step_availability_is_reported():
    s = series_ending(ORIGIN, PER_DAY)
    m = model()
    m._pattern_forecast(origin=ORIGIN, steps_ahead=STEPS_AHEAD,
                        seasonal_history=s, effective_pct=75)
    per_step = m.last_pattern_per_step
    assert len(per_step) == STEPS_AHEAD
    for i, rec in enumerate(per_step):
        assert rec["step"] == i + 1
        assert rec["available"] is True
        assert rec["support"] >= 1
        assert rec["matched_source_timestamps"], "the matched source must be recorded"
        # The matched timestamp must be exactly one day before this step's target.
        target = pd.Timestamp(ORIGIN) + pd.Timedelta(minutes=10 * (i + 1))
        want = target - pd.Timedelta(days=1)
        assert abs(pd.Timestamp(rec["matched_source_timestamps"][0]) - want) <= pd.Timedelta(
            minutes=5)


def test_partial_coverage_marks_missing_steps_nan_not_borrowed():
    """A short history covers only the later steps; the earlier ones must say so."""
    # 140 points spans 23h10m: step k's previous-day time is origin-1440+10(k+1) min, so only
    # the steps whose previous-day time is at or after the series start can resolve.
    s = series_ending(ORIGIN, 140)
    arr, source = model()._pattern_forecast(
        origin=ORIGIN, steps_ahead=STEPS_AHEAD, seasonal_history=s, effective_pct=75)

    assert arr is not None
    missing = np.isnan(arr)
    assert missing.any(), "precondition: this history cannot cover every step"
    assert not missing.all(), "precondition: it must cover some step"
    assert source == "seasonal_history_partial"
    # The missing ones must be the EARLY steps (their previous-day times are oldest).
    first_available = int(np.flatnonzero(~missing)[0])
    assert missing[:first_available].all()
    assert not missing[first_available:].any()


def test_no_neighbour_substitution():
    """A step with no previous-day observation must not borrow its neighbour's value."""
    s = series_ending(ORIGIN, 140)
    arr, _ = model()._pattern_forecast(
        origin=ORIGIN, steps_ahead=STEPS_AHEAD, seasonal_history=s, effective_pct=75)
    missing = np.isnan(arr)
    assert missing.any(), "precondition"
    # Pre-fix, every entry was finite because gaps were filled from the nearest good step.
    assert np.isnan(arr[missing]).all()


def test_gap_at_the_matched_time_makes_that_step_unavailable():
    """A hole in the history at one step's previous-day time affects only that step."""
    s = series_ending(ORIGIN, PER_DAY + 20)
    target3 = pd.Timestamp(ORIGIN) + pd.Timedelta(minutes=30)
    hole = target3 - pd.Timedelta(days=1)
    s = s.drop(index=[i for i in s.index if abs(i - hole) <= pd.Timedelta(minutes=1)])

    m = model()
    arr, _ = m._pattern_forecast(origin=ORIGIN, steps_ahead=STEPS_AHEAD,
                                 seasonal_history=s, effective_pct=75)
    per_step = m.last_pattern_per_step
    assert per_step[2]["available"] is False, "step 3 lost its only source"
    assert np.isnan(arr[2])
    for i in (0, 1, 3, 4, 5):
        assert per_step[i]["available"] is True, f"step {i+1} must be unaffected"


def test_timezone_aware_history_is_aligned_not_shifted():
    """A tz-aware index must be converted, not silently offset."""
    s = series_ending(ORIGIN, PER_DAY)
    aware = pd.Series(s.values, index=s.index.tz_localize("UTC"))
    arr_naive, _ = model()._pattern_forecast(
        origin=ORIGIN, steps_ahead=STEPS_AHEAD, seasonal_history=s, effective_pct=75)
    arr_aware, _ = model()._pattern_forecast(
        origin=ORIGIN, steps_ahead=STEPS_AHEAD, seasonal_history=aware, effective_pct=75)
    assert np.allclose(arr_naive, arr_aware, equal_nan=True)


def test_observation_cutoff_never_uses_the_future():
    """Nothing at or after the origin may be matched: targets are in the future."""
    s = series_ending(ORIGIN + GRID * 12, PER_DAY + 12)  # history extends PAST the origin
    m = model()
    m._pattern_forecast(origin=ORIGIN, steps_ahead=STEPS_AHEAD,
                        seasonal_history=s, effective_pct=75)
    for rec in m.last_pattern_per_step:
        for ts in rec["matched_source_timestamps"]:
            assert pd.Timestamp(ts) < pd.Timestamp(ORIGIN), (
                "a previous-day match must precede the origin")


def test_no_history_at_all_returns_none():
    s = series_ending(ORIGIN, 12)  # two hours: no step can reach yesterday
    arr, source = model()._pattern_forecast(
        origin=ORIGIN, steps_ahead=STEPS_AHEAD, seasonal_history=s, effective_pct=75)
    assert arr is None
    assert source == "no_matching_history"
