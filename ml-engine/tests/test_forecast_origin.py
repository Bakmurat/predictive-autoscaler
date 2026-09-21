"""Inference must use the data's clock, not the wall clock.

Codex C-45: predict() built its calendar features from datetime.utcnow() while
serving reported inference_input_end from the last OBSERVED timestamp. Replay of
historical data therefore used today's hour-of-day and day-of-week, and a live
observation that arrived late was scored against the wrong calendar position.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.lstm_model import STEPS_AHEAD, LSTMForecastModel, generate_time_features  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144


class _EchoTimeFeatures:
    """A stub network that returns the mean hour_sin of its input window.

    That makes the output a direct function of the calendar features, so a test can
    detect which clock produced them.
    """

    output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        # The LAST row's hour feature: a full-day window has the same MEAN whatever
        # time it ends, so the mean would hide the very difference under test.
        hour_sin_last = float(x[0, -1, 1])
        return np.full((1, STEPS_AHEAD), hour_sin_last, dtype=float)


def _model(window_index):
    from sklearn.preprocessing import RobustScaler

    m = LSTMForecastModel(sequence_length=PER_DAY)
    scaler = RobustScaler()
    scaler.fit(np.linspace(0, 1000, 500).reshape(-1, 1))
    m.scaler = scaler
    m.last_sequence = np.zeros(PER_DAY)
    m.input_timestamps = list(window_index)
    m.model = _EchoTimeFeatures()
    m.is_trained = True
    return m


def test_features_come_from_the_input_timestamps_not_the_wall_clock():
    """Two windows at different clock times must yield different features."""
    end_a = datetime(2026, 3, 2, 3, 0)    # 03:00
    end_b = datetime(2026, 3, 2, 15, 0)   # 15:00, opposite side of the daily cycle
    idx_a = [end_a - GRID * (PER_DAY - 1 - i) for i in range(PER_DAY)]
    idx_b = [end_b - GRID * (PER_DAY - 1 - i) for i in range(PER_DAY)]

    out_a = _model(idx_a).predict(steps_ahead=STEPS_AHEAD, origin=end_a)
    out_b = _model(idx_b).predict(steps_ahead=STEPS_AHEAD, origin=end_b)

    assert not np.allclose(out_a["components"]["lstm"], out_b["components"]["lstm"]), (
        "the two windows produced identical features: inference is reading a clock "
        "that does not depend on the input timestamps"
    )


def test_replay_is_deterministic():
    """The same historical window must give the same answer whenever it is replayed."""
    end = datetime(2026, 3, 2, 3, 0)
    idx = [end - GRID * (PER_DAY - 1 - i) for i in range(PER_DAY)]
    first = _model(idx).predict(steps_ahead=STEPS_AHEAD, origin=end)
    second = _model(idx).predict(steps_ahead=STEPS_AHEAD, origin=end)
    assert np.allclose(first["components"]["lstm"], second["components"]["lstm"])
    assert first["origin"] == second["origin"] == end.isoformat()


def test_target_timestamps_follow_the_origin():
    """Targets are origin + 10 min * (step + 1), reported explicitly."""
    end = datetime(2026, 3, 2, 3, 0)
    idx = [end - GRID * (PER_DAY - 1 - i) for i in range(PER_DAY)]
    out = _model(idx).predict(steps_ahead=STEPS_AHEAD, origin=end)
    targets = [pd.Timestamp(t).to_pydatetime() for t in out["target_timestamps"]]
    assert targets == [end + GRID * (s + 1) for s in range(STEPS_AHEAD)]


def test_delayed_observation_keeps_its_own_calendar_position():
    """A window whose last point is 40 minutes old is anchored there, not at 'now'."""
    stale_end = datetime(2026, 3, 2, 3, 0)
    idx = [stale_end - GRID * (PER_DAY - 1 - i) for i in range(PER_DAY)]
    out = _model(idx).predict(steps_ahead=STEPS_AHEAD, origin=stale_end)
    # First target is 10 minutes after the last observation, not after wall-clock now.
    assert pd.Timestamp(out["target_timestamps"][0]).to_pydatetime() == stale_end + GRID


def test_generate_time_features_is_cyclical():
    """Guard the feature helper itself: midnight and noon must differ."""
    a = generate_time_features([datetime(2026, 3, 2, 0, 0)])
    b = generate_time_features([datetime(2026, 3, 2, 12, 0)])
    assert not np.allclose(a, b)
