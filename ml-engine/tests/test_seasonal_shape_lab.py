"""Exact mechanism and information-boundary fixtures for offline shape experiments."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

SPEC = importlib.util.spec_from_file_location(
    'shape_lab', Path(__file__).resolve().parents[2] / 'eval/seasonal_shape_lab.py')
lab = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lab)


def step_series():
    s = pd.Series(100., index=pd.date_range('2027-02-01', periods=21*144, freq='10min'))
    s.iloc[1800:] = 155.
    return s


def test_shift_adaptation_and_anniversary_arithmetic():
    s = step_series()
    at_shift, _ = lab.forecast(s, s.index[1800])
    after_shift, _ = lab.forecast(s, s.index[1803])
    anniversary, _ = lab.forecast(s, s.index[1944])
    after_anniversary, _ = lab.forecast(s, s.index[1947])
    for arm in ('pattern_ratio', 'mean_ratio'):
        np.testing.assert_allclose(at_shift[arm], 100.)
        np.testing.assert_allclose(after_shift[arm], 155.)
        np.testing.assert_allclose(after_anniversary[arm], 155.)
    np.testing.assert_allclose(anniversary['pattern_ratio'], 155*1.55)
    np.testing.assert_allclose(anniversary['mean7'], 100*(1+.55/7))
    np.testing.assert_allclose(anniversary['mean_ratio'], 100*(1+.55/7)*1.55)


def test_future_mutation_cannot_change_shapes_or_level_factors():
    s = step_series()
    origin = s.index[1944]
    before, factors = lab.forecast(s, origin)
    s.loc[s.index > origin] = 1e12
    after, after_factors = lab.forecast(s, origin)
    for key in before:
        np.testing.assert_array_equal(before[key], after[key])
    assert factors == after_factors


def test_missing_or_nonpositive_pair_keeps_shape():
    s = step_series()
    origin = s.index[1944]
    for bad in (np.nan, 0., -1.):
        changed = s.copy()
        changed.loc[origin] = bad
        forecasts, detail = lab.forecast(changed, origin)
        assert detail['pattern_ratio']['fallback']
        assert detail['mean_ratio']['fallback']
        np.testing.assert_array_equal(forecasts['mean_ratio'], forecasts['mean7'])
        np.testing.assert_array_equal(forecasts['pattern_ratio'], forecasts['pattern70'])


def test_missing_day_uses_same_finite_support_for_both_shapes():
    s = step_series()
    origin = s.index[1700]
    s.iloc[1701-144] = np.nan
    targets = pd.date_range(origin+pd.Timedelta(minutes=10), periods=6, freq='10min')
    shapes, support = lab.shapes(s, targets, origin)
    assert support[0] == 6
    np.testing.assert_allclose(shapes['mean7'], 100)
    np.testing.assert_allclose(shapes['pattern70'], 100)


def test_percentile_equals_serving_helper_on_exact_grid():
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from models.lstm_model import LSTMForecastModel
    rng = np.random.default_rng(19)
    s = pd.Series(rng.uniform(1, 200, 21*144), index=pd.date_range('2027-02-01', periods=21*144, freq='10min'))
    helper = LSTMForecastModel(sequence_length=144)
    for i in (1010, 1800, 1944, 2700):
        origin = s.index[i]
        targets = pd.date_range(origin+pd.Timedelta(minutes=10), periods=6, freq='10min')
        got, _ = lab.shapes(s, targets, origin)
        expected, _ = helper._pattern_forecast(origin.to_pydatetime(), 6, s.loc[:origin], 70)
        np.testing.assert_allclose(got['pattern70'], expected, rtol=0, atol=1e-9)


def test_absent_third_burst_can_echo_in_both_shapes():
    s = pd.Series(100., index=pd.date_range('2027-02-01', periods=21*144, freq='10min'))
    for day in (12.5, 13.5):
        i = int(day*144)
        s.iloc[i:i+3] = 250.
    forecasts, _ = lab.forecast(s, s.index[int(14.5*144)])
    assert s.iloc[int(14.5*144)+1] == 100
    np.testing.assert_allclose(forecasts['pattern70'][:2], 250.)
    np.testing.assert_allclose(forecasts['mean7'][:2], (2*250+5*100)/7)
