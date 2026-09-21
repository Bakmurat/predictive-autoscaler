"""Accuracy must compare a forecast with the observation it actually targeted.

Codex C-48: the API stored the +10min forecast and compared it against whatever
observation arrived next -- typically 60 s later, because that is the reconcile
interval. That is not a forecast error, and it was fed back into the adaptive
percentile, so the model tuned itself on a meaningless signal.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.accuracy import AccuracyTracker  # noqa: E402

APP, NS, MT = "nginx-test", "demo", "requests"
T0 = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)


def test_a_forecast_is_not_scored_before_its_target_arrives():
    t = AccuracyTracker()
    t.store_forecast(APP, NS, MT, target_at=T0 + timedelta(minutes=10), predicted_value=900.0)

    # The next observation is 60 s later -- the reconcile interval, not the horizon.
    assert t.take_matured(APP, NS, MT, observation_at=T0 + timedelta(seconds=60)) == []
    assert t.get_mape(APP, NS, MT) == 0.0


def test_the_forecast_is_scored_when_its_target_arrives():
    t = AccuracyTracker()
    target = T0 + timedelta(minutes=10)
    t.store_forecast(APP, NS, MT, target_at=target, predicted_value=900.0)

    matured = t.take_matured(APP, NS, MT, observation_at=target)
    assert matured == [900.0]
    # And it is consumed: scoring the same forecast twice would double-count it.
    assert t.take_matured(APP, NS, MT, observation_at=target) == []


def test_each_step_is_matched_to_its_own_target():
    t = AccuracyTracker()
    for step in range(6):
        t.store_forecast(APP, NS, MT,
                         target_at=T0 + timedelta(minutes=10 * (step + 1)),
                         predicted_value=100.0 * (step + 1))

    assert t.take_matured(APP, NS, MT, observation_at=T0 + timedelta(minutes=10)) == [100.0]
    assert t.take_matured(APP, NS, MT, observation_at=T0 + timedelta(minutes=30)) == [300.0]
    # Step 2's target passed without a matching observation: it is dropped, not misscored.
    assert t.take_matured(APP, NS, MT, observation_at=T0 + timedelta(minutes=60)) == [600.0]


def test_a_target_whose_observation_never_arrived_is_discarded_not_misscored():
    t = AccuracyTracker()
    t.store_forecast(APP, NS, MT, target_at=T0 + timedelta(minutes=10), predicted_value=900.0)

    # An observation an hour later must not be matched to the +10min forecast.
    assert t.take_matured(APP, NS, MT, observation_at=T0 + timedelta(minutes=70)) == []
    # ...and the stale entry is gone, so it cannot be matched later either.
    assert t.take_matured(APP, NS, MT, observation_at=T0 + timedelta(minutes=10)) == []


def test_components_are_queued_and_matured_separately():
    t = AccuracyTracker()
    target = T0 + timedelta(minutes=10)
    t.store_forecast(APP, NS, MT, target_at=target, predicted_value=900.0)
    t.store_forecast(APP, NS, MT, target_at=target, predicted_value=800.0, component="lstm")
    t.store_forecast(APP, NS, MT, target_at=target, predicted_value=950.0, component="pattern")

    assert t.take_matured(APP, NS, MT, target, component="lstm") == [800.0]
    assert t.take_matured(APP, NS, MT, target, component="pattern") == [950.0]
    assert t.take_matured(APP, NS, MT, target) == [900.0]


def test_naive_and_aware_timestamps_are_treated_as_utc():
    t = AccuracyTracker()
    target_naive = datetime(2026, 3, 2, 12, 10)
    t.store_forecast(APP, NS, MT, target_at=target_naive, predicted_value=900.0)
    assert t.take_matured(APP, NS, MT, observation_at=T0 + timedelta(minutes=10)) == [900.0]


def test_observation_timestamp_helper_accepts_epoch_and_iso():
    import api.main as main

    assert main._observation_timestamp({"timestamp": "2026-03-02T12:00:00Z"}) == datetime(2026, 3, 2, 12, 0)
    assert main._observation_timestamp({"timestamp": T0.timestamp()}) == datetime(2026, 3, 2, 12, 0)
    assert main._observation_timestamp({"value": 1}) is None
    assert main._observation_timestamp(None) is None
