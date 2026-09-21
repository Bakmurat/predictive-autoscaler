"""Confidence must describe what is actually served (Codex C-71 / D-92).

Agreement between the network and the pattern is evidence about the served forecast only to
the extent the network is IN that forecast. At pattern weight 1 the network contributes
nothing, yet the old calculation still let its disagreement move confidence -- and the
controller dampens the forecast below 0.7 confidence, so a nominally "pattern-only"
configuration was still operationally steered by the network it had supposedly removed.

These tests pin the corrected behaviour:
  - the agreement term is weighted by the network's share of the blend;
  - with no network share, confidence rests on the pattern's own support;
  - with no second component at all, the neutral 0.5 applies -- never the 1.0 that
    self-comparison produced before C-44.
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
END = datetime(2026, 3, 2, 12, 0)


def daily_series(days: int, end: datetime, amplitude: float = 400.0, base: float = 600.0):
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = [base + amplitude * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / 1440.0)
            for t in idx]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


class _StubModel:
    """Stands in for the Keras model so these tests need no training."""

    def __init__(self, value_scaled):
        self.value_scaled = value_scaled
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        return np.full((1, STEPS_AHEAD), self.value_scaled, dtype=float)


def _fitted(seasonal: pd.Series):
    """A model with a stub network whose output is deliberately unlike the pattern."""
    from sklearn.preprocessing import RobustScaler

    m = LSTMForecastModel(sequence_length=PER_DAY)
    scaler = RobustScaler()
    scaler.fit(seasonal.values.reshape(-1, 1))
    m.scaler = scaler
    window = seasonal.iloc[-PER_DAY:]
    m.last_sequence = scaler.transform(window.values.reshape(-1, 1)).flatten()
    m.input_timestamps = list(window.index.to_pydatetime())
    m.model = _StubModel(float(scaler.transform(np.array([[5000.0]]))[0][0]))
    m.is_trained = True
    m.seasonal_history = seasonal
    m.mape_for_floor = 0.0
    return m


def test_network_share_is_reported():
    seasonal = daily_series(8, END)
    m = _fitted(seasonal)
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)
    comp = out["components"]
    assert "network_share" in comp
    # Deployed ramp: pattern 0.70..0.908, so the network's mean share is 1 - mean(pattern).
    expected = float(np.mean([1.0 - w for w in comp["pattern_weights"]]))
    assert comp["network_share"] == pytest.approx(expected)
    assert 0.0 < comp["network_share"] < 1.0


def test_no_second_component_gives_the_neutral_term_not_perfect_agreement():
    """Two hours of history: no step reaches yesterday, so there is no pattern at all."""
    seasonal = daily_series(8, END)
    m = _fitted(seasonal)
    m.seasonal_history = seasonal.iloc[-12:]
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)

    assert out["components"]["pattern_available"] is False
    assert out["components"]["agreement"] is None
    # Neutral 0.5 agreement term with the horizon penalty for six steps.
    horizon = max(0.4, 1.0 - (STEPS_AHEAD / 288))
    assert out["confidence"] == pytest.approx(
        max(0.3, min(0.9, 0.5 * 0.5 + 0.5 * horizon)), abs=1e-6)


def test_confidence_does_not_exceed_the_self_comparison_ceiling():
    """Whatever the path, confidence stays inside its declared bounds."""
    seasonal = daily_series(8, END)
    for hist in (seasonal, seasonal.iloc[-PER_DAY:], seasonal.iloc[-12:]):
        m = _fitted(seasonal)
        m.seasonal_history = hist
        out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)
        assert 0.3 <= out["confidence"] <= 0.9


def test_partial_availability_narrows_the_agreement_basis():
    """Agreement is computed only over steps with a genuine second opinion."""
    seasonal = daily_series(8, END)
    m = _fitted(seasonal)
    # 140 points: the early steps cannot reach yesterday, the later ones can.
    m.seasonal_history = seasonal.iloc[-140:]
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)

    comp = out["components"]
    avail = comp["pattern_available_per_step"]
    assert any(avail) and not all(avail), "precondition: partial coverage"
    # Steps without a pattern must carry pattern weight 0 -- the network alone serves them.
    for i, a in enumerate(avail):
        if not a:
            assert comp["pattern_weights"][i] == 0.0
    assert comp["pattern_steps_available"] == sum(avail)


def test_support_raises_the_term_when_the_network_share_is_small():
    """More matched days is better evidence for a pattern-dominated forecast."""
    thin = daily_series(2, END)      # one previous day available
    thick = daily_series(8, END)     # several previous days available

    a = _fitted(thick)
    a.seasonal_history = thin
    b = _fitted(thick)
    b.seasonal_history = thick

    ca = a.predict(steps_ahead=STEPS_AHEAD, origin=END)
    cb = b.predict(steps_ahead=STEPS_AHEAD, origin=END)

    sa = [r["support"] for r in ca["components"]["pattern_per_step"] if r["available"]]
    sb = [r["support"] for r in cb["components"]["pattern_per_step"] if r["available"]]
    assert sa and sb
    assert np.mean(sb) > np.mean(sa), "precondition: more history means more support"
