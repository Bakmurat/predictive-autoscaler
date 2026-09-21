"""Per-step pattern availability must survive every consumer (Codex C-75 / C-76).

8f6abb6 fixed the lookup: a step with no previous-day observation stays NaN. But three
consumers then lost that information, and one confidence branch was wrong:

  1. `predict()` replaced unavailable pattern entries with the network's values, so a
     substituted number reached the record, the observer and evaluate() as a "pattern" value.
  2. `observer/shadow.py` ignored per-step availability and rejected NumPy `predictions`, so
     EVERY served-hybrid step was recorded as unavailable.
  3. `evaluate()` applied no per-step fallback, so one missing step made the whole MAE NaN.
  4. With the pattern weight forced to one, non-null agreement plus zero network share fell
     through to the neutral 0.5, bypassing the pattern's support: thin and thick history both
     returned 0.73958.

Each test here fails on the code before its fix.
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
END = datetime(2026, 3, 2, 12, 0)
NETWORK_VALUE = 5000.0  # deliberately far from any seasonal value


def daily_series(days: int, end: datetime, amplitude: float = 400.0, base: float = 600.0):
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = [base + amplitude * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / 1440.0)
            for t in idx]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


class _StubModel:
    def __init__(self, value_scaled):
        self.value_scaled = value_scaled
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        return np.full((1, STEPS_AHEAD), self.value_scaled, dtype=float)


def fitted(seasonal: pd.Series, history: pd.Series | None = None):
    from sklearn.preprocessing import RobustScaler

    m = LSTMForecastModel(sequence_length=PER_DAY)
    scaler = RobustScaler()
    scaler.fit(seasonal.values.reshape(-1, 1))
    m.scaler = scaler
    window = seasonal.iloc[-PER_DAY:]
    m.last_sequence = scaler.transform(window.values.reshape(-1, 1)).flatten()
    m.input_timestamps = list(window.index.to_pydatetime())
    m.model = _StubModel(float(scaler.transform(np.array([[NETWORK_VALUE]]))[0][0]))
    m.is_trained = True
    m.seasonal_history = seasonal if history is None else history
    m.mape_for_floor = 0.0
    return m


# ---------------------------------------------------------------------------------------
# 1. predict() must keep an unavailable step unavailable, all the way to the record.
# ---------------------------------------------------------------------------------------
def test_predict_keeps_unavailable_pattern_steps_none_not_substituted():
    seasonal = daily_series(8, END)
    # 140 points: the early steps cannot reach yesterday; the later ones can.
    m = fitted(seasonal, history=seasonal.iloc[-140:])
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)
    comp = out["components"]

    avail = comp["pattern_available_per_step"]
    assert any(avail) and not all(avail), "precondition: partial coverage"

    for i, a in enumerate(avail):
        if a:
            assert comp["pattern"][i] is not None
            assert abs(comp["pattern"][i] - NETWORK_VALUE) > 1.0, "a genuine step is not the network"
        else:
            # Pre-fix: this held the network's value, masquerading as a seasonal forecast.
            assert comp["pattern"][i] is None
            assert comp["pattern_weights"][i] == 0.0
            # The served value at an unavailable step is the network alone.
            assert out["predictions"][i] == pytest.approx(comp["lstm"][i])


def test_pattern_weight_override_is_honoured_only_where_the_pattern_exists():
    seasonal = daily_series(8, END)
    m = fitted(seasonal, history=seasonal.iloc[-140:])
    m.pattern_weight_override = 1.0
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)
    comp = out["components"]
    for i, a in enumerate(comp["pattern_available_per_step"]):
        assert comp["pattern_weights"][i] == (1.0 if a else 0.0)


# ---------------------------------------------------------------------------------------
# 3. evaluate() must score over the steps that have genuine values, never NaN the whole score.
# ---------------------------------------------------------------------------------------
class _StubModelN(_StubModel):
    """Same constant network, but shaped (N, STEPS) for batched evaluate()."""

    def predict(self, x, verbose=0):
        return np.full((x.shape[0], STEPS_AHEAD), self.value_scaled, dtype=float)


def test_evaluate_with_partial_history_returns_finite_mae_and_reports_genuine_steps():
    seasonal = daily_series(8, END)
    m = fitted(seasonal)
    m.model = _StubModelN(m.model.value_scaled)
    # A seasonal history that is deliberately SHORT relative to the evaluation window, so
    # some evaluated origins cannot find a previous-day value for every step.
    m.seasonal_history = seasonal.iloc[-(PER_DAY + 30):]

    test_df = pd.DataFrame({"value": seasonal.values[-(PER_DAY + 60):]},
                           index=seasonal.index[-(PER_DAY + 60):])
    out = m.evaluate(test_df, target_column="value")

    assert out["scored"] == "served_blend"
    assert np.isfinite(out["mae"]), "pre-fix: one NaN pattern step made the whole MAE NaN"
    assert np.isfinite(out["rmse"])
    assert out["pattern_steps_total"] == out["sequences_scored"] * STEPS_AHEAD
    assert 0 < out["pattern_steps_genuine"] < out["pattern_steps_total"], (
        "precondition: this history must leave some steps genuine and some not")


# ---------------------------------------------------------------------------------------
# 4. Pattern-only confidence must rest on the pattern's support (the zero-share branch).
# ---------------------------------------------------------------------------------------
def _pattern_only_confidence(days: int) -> tuple[float, float, int]:
    seasonal = daily_series(8, END)
    m = fitted(seasonal, history=daily_series(days, END))
    m.pattern_weight_override = 1.0          # the network is NOT in the served forecast
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)
    comp = out["components"]
    assert comp["pattern_available"] is True and all(comp["pattern_available_per_step"])
    assert comp["network_share"] == 0.0, "precondition: this must exercise the zero-share branch"
    supports = [r["support"] for r in comp["pattern_per_step"] if r["available"]]
    return out["confidence"], comp["network_share"], int(np.mean(supports))


NEUTRAL_FALLTHROUGH = 0.5 * 0.5 + 0.5 * max(0.4, 1.0 - (STEPS_AHEAD / 288))   # 0.73958


def test_pattern_only_confidence_reflects_support_and_differs_thin_vs_thick():
    """Codex C-76: with the weight forced to one, thin and thick history both returned
    0.73958, because non-null agreement plus zero network share fell through to the neutral
    default and bypassed support. This test drives the ACTUAL zero-share branch."""
    thin_conf, thin_share, thin_support = _pattern_only_confidence(days=2)   # one matched day
    thick_conf, thick_share, thick_support = _pattern_only_confidence(days=8)  # several

    assert thin_support < thick_support, "precondition: more history means more support"
    assert thin_conf != pytest.approx(NEUTRAL_FALLTHROUGH, abs=1e-6), (
        f"thin history fell through to the neutral value {thin_conf:.5f}")
    assert thick_conf != pytest.approx(NEUTRAL_FALLTHROUGH, abs=1e-6), (
        f"thick history fell through to the neutral value {thick_conf:.5f}")
    assert thick_conf > thin_conf, (thin_conf, thick_conf)
    # One matched day is thin evidence: support term 1/3.
    horizon = max(0.4, 1.0 - (STEPS_AHEAD / 288))
    assert thin_conf == pytest.approx(max(0.3, min(0.9, 0.5 * (1 / 3) + 0.5 * horizon)), abs=1e-6)
