"""Validation-selected blend weight (training/blend_selection.py): the network keeps weight only when the
served blend it produces beats the pattern alone on the held-out partition."""
import os
import sys
from datetime import datetime, timedelta

import joblib
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402
from training import blend_selection as bs  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144


def daily_series(days, end, amplitude=400.0, base=1000.0):
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = [base + amplitude * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / (24 * 60.0)) for t in idx]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


class _Stub:
    """Stands in for the Keras network: a constant (or NaN) at every step, batch-shaped."""
    def __init__(self, value_scaled):
        self.value_scaled = value_scaled
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        return np.full((len(x), STEPS_AHEAD), self.value_scaled, dtype=float)


def _model(series, network_rpm):
    from sklearn.preprocessing import RobustScaler
    m = LSTMForecastModel(sequence_length=PER_DAY)
    m.scaler = RobustScaler().fit(series.values.reshape(-1, 1))
    m.model = _Stub(float(m.scaler.transform(np.array([[network_rpm]]))[0][0]) if np.isfinite(network_rpm) else float("nan"))
    m.is_trained = True
    return m


@pytest.fixture
def data():
    end = datetime(2026, 9, 28, 12, 0)
    s = daily_series(9, end)
    test = s.iloc[-int(0.2 * len(s)):].to_frame("value")
    return s, test


def test_a_useless_network_loses_all_weight_and_the_choice_is_recorded(data):
    s, test = data
    m = _model(s, network_rpm=6000.0)          # far above the series everywhere
    bs.attach_history(m, s)
    rec = bs.select_blend_weight(m, test)
    assert rec["chosen"] == "pattern_only" and m.pattern_weight_override == 1.0
    assert [c["candidate"] for c in rec["candidates"]] == ["deployed_ramp", "pattern_0.85", "pattern_0.95", "pattern_only"]
    assert all(c["scored"] == "served_blend" for c in rec["candidates"])
    maes = [c["mae"] for c in rec["candidates"]]
    assert maes == sorted(maes, reverse=True) and maes[-1] < maes[0] / 5   # the more pattern, the better, by a wide margin
    assert m.evaluate(test)["mae"] == pytest.approx(rec["mae"])


def test_a_nan_network_never_poisons_the_selection(data):
    s, test = data
    m = _model(s, network_rpm=float("nan"))
    bs.attach_history(m, s)
    rec = bs.select_blend_weight(m, test)
    assert rec["chosen"] == "pattern_only"
    assert np.isfinite(rec["mae"]) and all(c["mae"] is None or not np.isfinite(c["mae"]) for c in rec["candidates"][:3])


def test_without_history_the_deployed_ramp_is_kept_and_the_reason_recorded(data):
    s, test = data
    m = _model(s, network_rpm=6000.0)
    rec = bs.select_blend_weight(m, test)
    assert rec["chosen"] == "deployed_ramp" and m.pattern_weight_override is None
    assert "no seasonal history" in rec["reason"]


def test_the_chosen_weight_survives_the_artifact_round_trip(tmp_path, data):
    s, test = data
    m = _model(s, network_rpm=6000.0)
    bs.attach_history(m, s)
    bs.select_blend_weight(m, test)
    joblib.dump(m, tmp_path / "m.pkl")
    loaded = joblib.load(tmp_path / "m.pkl")
    assert loaded.pattern_weight_override == 1.0


def test_ties_keep_the_earlier_candidate():
    class M:
        seasonal_history = pd.Series([1.0, 2.0])
        pattern_weight_override = None
        def evaluate(self, td, target_column="value", imputed=None):
            return {"mae": 10.0, "rmse": 12.0, "scored": "served_blend", "network_only": {"mae": 10.0}}
    m = M()
    rec = bs.select_blend_weight(m, pd.DataFrame({"value": [1.0]}, index=pd.DatetimeIndex([datetime(2026, 1, 1)])))
    assert rec["chosen"] == "deployed_ramp" and m.pattern_weight_override is None
