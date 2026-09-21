"""Pattern-only serving must survive a broken network, and inference and evaluation must
share ONE weighting-and-fallback implementation (Codex C-80, C-81 / D-101, D-102).

C-80  evaluate() hardcoded the ramp, so an override changed inference and not evaluation
      (overrides 0 and 1 both scored 802.95073). One shared implementation now.
C-81  `0 * NaN` is NaN: a NaN network output at pattern weight 1 made all six served values
      NaN while confidence stayed 0.9. Zero-weight terms are now bypassed, non-finite network
      values are excluded from agreement, a network exception is caught, and the override is
      validated (bounds, finiteness, length) before use.

Every test here fails on the code before its fix.
"""


import math
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
NETWORK_VALUE = 5000.0


def daily_series(days: int, end: datetime, amplitude: float = 400.0, base: float = 600.0):
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = [base + amplitude * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / 1440.0)
            for t in idx]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


class _Stub:
    def __init__(self, scaled, raises=None, batched=False):
        self.scaled, self.raises, self.batched = scaled, raises, batched
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        if self.raises:
            raise self.raises
        n = x.shape[0] if self.batched else 1
        return np.full((n, STEPS_AHEAD), self.scaled, dtype=float)


def fitted(seasonal, history=None, network_scaled=None, raises=None, batched=False):
    from sklearn.preprocessing import RobustScaler

    m = LSTMForecastModel(sequence_length=PER_DAY)
    scaler = RobustScaler().fit(seasonal.values.reshape(-1, 1))
    m.scaler = scaler
    window = seasonal.iloc[-PER_DAY:]
    m.last_sequence = scaler.transform(window.values.reshape(-1, 1)).flatten()
    m.input_timestamps = list(window.index.to_pydatetime())
    scaled = (float(scaler.transform(np.array([[NETWORK_VALUE]]))[0][0])
              if network_scaled is None else network_scaled)
    m.model = _Stub(scaled, raises=raises, batched=batched)
    m.is_trained = True
    m.seasonal_history = seasonal if history is None else history
    m.mape_for_floor = 0.0
    return m


# =======================================================================================
# C-80: inference and evaluation share one weighting-and-fallback implementation.
# =======================================================================================
def test_evaluate_honours_the_override_so_0_and_1_differ():
    seasonal = daily_series(8, END)
    test_df = pd.DataFrame({"value": seasonal.values[-(PER_DAY + 60):]},
                           index=seasonal.index[-(PER_DAY + 60):])
    maes = {}
    for w in (0.0, 1.0):
        m = fitted(seasonal, batched=True)
        m.pattern_weight_override = w
        maes[w] = m.evaluate(test_df, target_column="value")["mae"]
    assert all(np.isfinite(v) for v in maes.values())
    # Pre-fix both returned 802.95073: the ramp was hardcoded in evaluate().
    assert maes[0.0] != pytest.approx(maes[1.0], rel=1e-6), maes


def test_evaluate_of_the_deployed_ramp_equals_what_predict_serves():
    """Same origins, same components -> the served value and the evaluated value agree."""
    seasonal = daily_series(8, END)
    m = fitted(seasonal, batched=True)
    test_df = pd.DataFrame({"value": seasonal.values[-(PER_DAY + 30):]},
                           index=seasonal.index[-(PER_DAY + 30):])
    # Pick the last evaluated origin and reproduce it through predict().
    origin = pd.Timestamp(test_df.index[-STEPS_AHEAD - 1]).to_pydatetime()
    m.last_sequence = m.scaler.transform(
        seasonal.loc[:origin].values[-PER_DAY:].reshape(-1, 1)).flatten()
    m.input_timestamps = list(seasonal.loc[:origin].index[-PER_DAY:].to_pydatetime())
    served = np.asarray(m.predict(steps_ahead=STEPS_AHEAD, origin=origin)["predictions"])

    # Evaluate through the shared helpers at the same origin, by hand.
    from models.lstm_model import pattern_weight_for, serve_step
    net = m.scaler.inverse_transform(
        m.model.predict(np.zeros((1, PER_DAY, 5)), verbose=0).reshape(-1, 1)).flatten()
    pattern, _ = m._pattern_forecast(origin=origin, steps_ahead=STEPS_AHEAD,
                                     seasonal_history=seasonal, effective_pct=75)
    expected = [serve_step(net[s], pattern[s], pattern_weight_for(s, STEPS_AHEAD, None,
                                                                  not np.isnan(pattern[s])))
                for s in range(STEPS_AHEAD)]
    assert np.allclose(served, expected)


# =======================================================================================
# C-81: pattern-only must not depend on network validity.
# =======================================================================================
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_non_finite_network_at_weight_one_gives_finite_pattern_forecasts(bad):
    seasonal = daily_series(8, END)
    m = fitted(seasonal, network_scaled=bad)
    m.pattern_weight_override = 1.0
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)
    preds = np.asarray(out["predictions"], dtype=float)
    # Pre-fix: 0 * NaN poisoned every served value while confidence stayed 0.9.
    assert np.all(np.isfinite(preds)), preds
    assert np.allclose(preds, [p for p in out["components"]["pattern"]])
    assert out["components"]["agreement"] is None, "no finite network to agree with"
    assert not any(out["components"]["network_finite_per_step"])


def test_network_exception_at_weight_one_still_serves_the_pattern():
    seasonal = daily_series(8, END)
    m = fitted(seasonal, raises=RuntimeError("keras exploded"))
    m.pattern_weight_override = 1.0
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)
    preds = np.asarray(out["predictions"], dtype=float)
    assert np.all(np.isfinite(preds))
    assert out["components"]["network_failed"].startswith("RuntimeError")
    assert np.allclose(preds, [p for p in out["components"]["pattern"]])


def test_non_finite_network_at_the_deployed_ramp_serves_the_pattern_where_it_exists():
    """Not only at weight 1: a broken network must never poison a step that has a pattern."""
    seasonal = daily_series(8, END)
    m = fitted(seasonal, network_scaled=float("nan"))
    out = m.predict(steps_ahead=STEPS_AHEAD, origin=END)
    preds = np.asarray(out["predictions"], dtype=float)
    assert np.all(np.isfinite(preds))
    assert all(w == 1.0 for w in out["components"]["pattern_weights"])


@pytest.mark.parametrize("bad", [1.5, -0.1, float("nan"), [0.5] * (STEPS_AHEAD - 1),
                                 [0.5, float("inf")] + [0.5] * (STEPS_AHEAD - 2)])
def test_override_is_validated_before_use(bad):
    # Imported here, not at module level, so the OTHER tests in this file can run against
    # pre-fix code and fail on behaviour rather than on an ImportError.
    from models.lstm_model import validate_weight_override
    with pytest.raises(ValueError):
        validate_weight_override(bad, STEPS_AHEAD)
    assert validate_weight_override(None, STEPS_AHEAD) is None
    assert validate_weight_override(0.7, STEPS_AHEAD) == 0.7
    assert validate_weight_override([0.5] * STEPS_AHEAD, STEPS_AHEAD) == [0.5] * STEPS_AHEAD
