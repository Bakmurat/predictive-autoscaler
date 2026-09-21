"""A forecast is not matured by an observation taken BEFORE its target (Codex C-87 / D-109).

`take_matured()` accepted any observation within +/- the tolerance of the target, so a 12:10
forecast was scored against the 12:05 observation -- five minutes before the thing it
predicted had happened. That is not a forecast error; it measures the forecast against a past
it already knew.

Maturity and tolerance are different requirements:
  * maturity  -- the target time must have PASSED (observation >= target)
  * tolerance -- how stale an observation may be once matured (observation - target <= tol)
A target still in the future stays queued.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.accuracy import AccuracyTracker  # noqa: E402

APP, NS, MT = "nginx-test", "demo", "requests"
T0 = datetime(2026, 9, 22, 12, 10, 0, tzinfo=timezone.utc)


def _tracker(target_at=T0, value=1234.0):
    t = AccuracyTracker()
    t.store_forecast(APP, NS, MT, target_at, value)
    return t


def test_observation_five_minutes_before_target_does_not_mature():
    """Codex's exact case: target 12:10, observation 12:05.

    Pre-fix: returned for scoring, because abs(-300) <= 300.
    """
    t = _tracker()
    matured = t.take_matured(APP, NS, MT, T0 - timedelta(minutes=5))
    assert matured == [], "an observation before the target must not mature the forecast"


def test_a_premature_observation_leaves_the_forecast_queued():
    """It must not be dropped either -- the target still arrives later."""
    t = _tracker()
    t.take_matured(APP, NS, MT, T0 - timedelta(minutes=5))
    later = t.take_matured(APP, NS, MT, T0)
    assert later == [1234.0], "the forecast must still be scorable when its target arrives"


def test_observation_exactly_at_target_matures():
    t = _tracker()
    assert t.take_matured(APP, NS, MT, T0) == [1234.0]


def test_observation_within_tolerance_after_target_matures():
    t = _tracker()
    assert t.take_matured(APP, NS, MT, T0 + timedelta(seconds=300)) == [1234.0]


def test_observation_past_the_tolerance_is_dropped_not_scored():
    t = _tracker()
    assert t.take_matured(APP, NS, MT, T0 + timedelta(seconds=301)) == []
    # and it is gone -- it can never be scored correctly now
    assert t.take_matured(APP, NS, MT, T0 + timedelta(seconds=10)) == []


@pytest.mark.parametrize("before_s", [1, 60, 299, 300, 600])
def test_no_observation_before_the_target_ever_matures(before_s):
    """The boundary on the early side: nothing before the target matures, at any distance."""
    t = _tracker()
    assert t.take_matured(APP, NS, MT, T0 - timedelta(seconds=before_s)) == []
    # still queued for its real target
    assert t.take_matured(APP, NS, MT, T0) == [1234.0]
