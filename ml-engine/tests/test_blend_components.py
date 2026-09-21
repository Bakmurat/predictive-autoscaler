"""The served forecast must actually blend two different components.

Codex C-44: serving passed the 144-point inference window as the seasonal
history, but the pattern lookup required MORE than 144 points, so the pattern
was silently replaced by the network's own output and the blend became a no-op
(and component agreement, hence confidence, was pinned at 1.0).

These tests fail on the pre-fix code.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144


def daily_series(days: int, end: datetime, amplitude: float = 400.0, base: float = 600.0):
    """A deterministic repeating daily profile on the ten-minute grid."""
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = [base + amplitude * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / (24 * 60.0)) for t in idx]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


class _StubModel:
    """Stands in for the Keras model so these tests need no training."""

    def __init__(self, value_scaled):
        self.value_scaled = value_scaled
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        return np.full((1, STEPS_AHEAD), self.value_scaled, dtype=float)


def _fitted_model(seasonal: pd.Series):
    """An LSTMForecastModel with a stub network, ready to predict."""
    from sklearn.preprocessing import RobustScaler

    m = LSTMForecastModel(sequence_length=PER_DAY)
    scaler = RobustScaler()
    scaler.fit(seasonal.values.reshape(-1, 1))
    m.scaler = scaler
    window = seasonal.iloc[-PER_DAY:]
    m.last_sequence = scaler.transform(window.values.reshape(-1, 1)).flatten()
    m.input_timestamps = list(window.index.to_pydatetime())
    # The network always predicts a constant far from the seasonal values.
    m.model = _StubModel(float(scaler.transform(np.array([[5000.0]]))[0][0]))
    m.is_trained = True
    return m


def test_pattern_component_differs_from_network():
    """With a real seasonal history the pattern must not equal the network output."""
    end = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    seasonal = daily_series(days=8, end=end)
    m = _fitted_model(seasonal)
    m.seasonal_history = seasonal

    out = m.predict(steps_ahead=STEPS_AHEAD, origin=end)
    net = np.asarray(out["components"]["lstm"], dtype=float)
    pat = np.asarray(out["components"]["pattern"], dtype=float)

    assert out["components"].get("pattern_source") == "seasonal_history", out["components"]
    assert not np.allclose(net, pat), (
        "pattern equals the network output: the blend is a no-op "
        f"(net={net.tolist()}, pattern={pat.tolist()})"
    )
    # And the blend must land strictly between the two components.
    final = np.asarray(out["predictions"], dtype=float)
    lo, hi = np.minimum(net, pat), np.maximum(net, pat)
    assert np.all(final >= lo - 1e-6) and np.all(final <= hi + 1e-6)


def test_one_day_of_history_is_not_enough_and_is_declared():
    """Exactly sequence_length points cannot supply a previous-day lookup: say so."""
    end = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    seasonal = daily_series(days=8, end=end)
    m = _fitted_model(seasonal)
    m.seasonal_history = seasonal.iloc[-PER_DAY:]  # the inference window only

    out = m.predict(steps_ahead=STEPS_AHEAD, origin=end)
    assert out["components"]["pattern_source"] == "network_fallback"
    assert out["components"]["pattern_available"] is False
    assert out["confidence"] <= 0.9


def test_agreement_is_not_pinned_when_pattern_is_missing():
    """A missing pattern must not be reported as perfect component agreement."""
    end = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    seasonal = daily_series(days=8, end=end)

    with_pattern = _fitted_model(seasonal)
    with_pattern.seasonal_history = seasonal
    without = _fitted_model(seasonal)
    without.seasonal_history = seasonal.iloc[-PER_DAY:]

    a = with_pattern.predict(steps_ahead=STEPS_AHEAD, origin=end)
    b = without.predict(steps_ahead=STEPS_AHEAD, origin=end)

    assert b["components"]["agreement"] is None, (
        "agreement was computed from the pattern standing in for the network, "
        "which pins it at 1.0"
    )
    assert a["components"]["agreement"] is not None


def test_documented_blend_weights_are_the_implemented_ones():
    """README claims 0.70..0.95; the implementation gives 0.70..0.908 over six steps."""
    end = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    seasonal = daily_series(days=8, end=end)
    m = _fitted_model(seasonal)
    m.seasonal_history = seasonal

    out = m.predict(steps_ahead=STEPS_AHEAD, origin=end)
    w = out["components"]["pattern_weights"]
    assert len(w) == STEPS_AHEAD
    assert w[0] == pytest.approx(0.70, abs=1e-9)
    assert w[-1] == pytest.approx(0.70 + (5 / 6) * 0.25, abs=1e-9)  # 0.9083...
    assert max(w) < 0.95
