"""Information-boundary tests for the optional offline notebook laboratory."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

SPEC = importlib.util.spec_from_file_location(
    'notebook_lab', Path(__file__).resolve().parents[2] / 'eval/notebook_lab.py')
lab = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lab)


def test_grid_keeps_missing_targets_and_only_fills_past_inputs():
    s = pd.Series([1., np.nan, 99.], index=pd.date_range('2020-01-01', periods=3, freq='10min'))
    observed, filled = lab.regularize(s)
    assert np.isnan(observed.iloc[1])
    assert filled.iloc[1] == 1
    assert observed.iloc[0] == 1


def test_training_targets_do_not_cross_validation_boundary():
    s = pd.Series(np.arange(40.), index=pd.date_range('2020-01-01', periods=40, freq='10min'))
    samples = lab.make_samples(s, lookback=4, steps=3)
    train, val = lab.split_samples(samples, s.index[25])
    assert max(train['target_end']) < s.index[25]
    assert min(val['origin']) >= s.index[25]
    assert np.max(train['y']) == 24


def test_future_mutation_cannot_change_baseline_forecasts():
    s = pd.Series(np.tile(np.arange(1., 145.), 9), index=pd.date_range('2020-01-01', periods=1296, freq='10min'))
    origin = s.index[1100]
    before = lab.baselines(s, origin, 6)
    s.loc[s.index > origin] = 1e9
    after = lab.baselines(s, origin, 6)
    for name in before:
        np.testing.assert_array_equal(before[name], after[name])


def test_adaptive_zero_or_missing_denominator_falls_back():
    x = np.ones(300)
    pattern = np.arange(1., 7.)
    x[-145] = 0
    out, fallback = lab.adaptive(x, pattern, 6)
    assert fallback
    np.testing.assert_array_equal(out, pattern)
    x[-145] = np.nan
    assert lab.adaptive(x, pattern, 6)[1]


def test_adaptive_exact_repeat_and_level_change():
    x = np.tile(np.arange(1., 145.), 3)
    pattern = np.arange(1., 7.)
    np.testing.assert_allclose(lab.adaptive(x, pattern, 6)[0], pattern)
    x[-6:] *= 1.5
    np.testing.assert_allclose(lab.adaptive(x, pattern, 6)[0], pattern * 1.5)


def test_missing_training_target_is_not_filled_into_label():
    s = pd.Series(np.arange(20.), index=pd.date_range('2020-01-01', periods=20, freq='10min'))
    s.iloc[10] = np.nan
    samples = lab.make_samples(s, lookback=3, steps=2)
    assert np.isfinite(samples['y']).all()
    assert not any(a <= s.index[10] <= b for a, b in zip(samples['target_start'], samples['target_end']))


def test_runner_uses_origin_history_and_identical_future_targets(tmp_path, monkeypatch):
    series = pd.Series(np.arange(1020.)+1, index=pd.date_range('2020-01-01', periods=1020, freq='10min'))
    seen = []
    monkeypatch.setattr(lab, 'fit_classical', lambda name, history: history.index[-1])

    def probe(name, fit_origin, fit_history, history):
        seen.append((fit_origin, history.index[-1]))
        return np.full(6, history.iloc[-1])

    monkeypatch.setattr(lab, 'predict_classical', probe)
    config = {'classical': ['probe'], 'model_seeds': [], 'first_scored_day': 7,
              'adaptive_k': 6, 'epochs': 1}
    rows, fits = lab.run_dataset('fixture', series, 'units', config, tmp_path)
    checked = [r for r in rows if r['model'] == 'probe']
    assert seen[1][0] < seen[1][1]  # State advanced even though parameter fit is older.
    for row in checked:
        origin, target = pd.Timestamp(row['origin']), pd.Timestamp(row['target'])
        assert target-origin == pd.Timedelta(minutes=row['horizon_minutes'])
        assert row['forecast'] == series.loc[origin]
        assert row['actual'] == series.loc[target]
        assert row['forecast'] != row['actual']
    assert all(len(g) == 6 for _, g in pd.DataFrame(checked).groupby('origin'))


def test_missing_forecasts_cannot_receive_complete_mae(tmp_path):
    rows = [{'dataset': 'fixture', 'units': 'units', 'model': 'broken', 'origin': str(i),
             'target': str(i+1), 'horizon_minutes': 10, 'actual': 3., 'forecast': p}
            for i, p in enumerate([3., None])]
    _, scores = lab.summarize(rows, [], tmp_path)
    assert scores.iloc[0]['unavailable'] == 1
    assert pd.isna(scores.iloc[0]['mae'])



def test_baseline_exception_is_unavailable_not_whole_run_abort(tmp_path, monkeypatch):
    s = pd.Series(100., index=pd.date_range('2020-01-01', periods=1020, freq='10min'))

    def failed(*args, **kwargs):
        raise ValueError('deliberate baseline failure')

    monkeypatch.setattr(lab, 'baselines', failed)
    monkeypatch.setattr(lab, 'fit_classical', lambda *args: None)
    monkeypatch.setattr(lab, 'predict_classical', lambda *args: np.full(6, 100.))
    config = {'classical': ['probe'], 'model_seeds': [], 'first_scored_day': 7,
              'adaptive_k': 6, 'epochs': 1}
    rows, _ = lab.run_dataset('fixture', s, 'units', config, tmp_path)
    assert all(r['forecast'] is None for r in rows if r['model'] == 'pattern70')
    assert all(r['forecast'] == 100 for r in rows if r['model'] == 'probe')
    assert any(r['model'] == 'pattern70' for r in rows)


def test_trend_exception_does_not_erase_other_arms(tmp_path, monkeypatch):
    import sys
    import types
    s = pd.Series(100., index=pd.date_range('2020-01-01', periods=1020, freq='10min'))

    class BrokenTrend:
        def forecast(self, *args):
            raise RuntimeError('deliberate trend failure')

    monkeypatch.setitem(sys.modules, 'offline_eval', types.SimpleNamespace(TrendAdaptive=BrokenTrend))
    config = {'classical': [], 'model_seeds': [], 'first_scored_day': 7,
              'adaptive_k': 6, 'epochs': 1, 'include_existing_trend_adaptive': True}
    rows, _ = lab.run_dataset('fixture', s, 'units', config, tmp_path)
    assert all(r['forecast'] is None for r in rows if r['model'] == 'trend_adaptive')
    assert all(r['forecast'] == 100 for r in rows if r['model'] == 'persistence')


def test_pattern_skips_missing_day_but_yesterday_keeps_gap():
    s = pd.Series(100., index=pd.date_range('2020-01-01', periods=1296, freq='10min'))
    origin = s.index[1200]
    s.iloc[1201-144] = np.nan
    forecasts = lab.baselines(s, origin)
    np.testing.assert_allclose(forecasts['pattern70'], 100.)
    assert np.isnan(forecasts['yesterday'][0])


def test_yesterday_matches_exact_target_clock_time():
    s = pd.Series(np.arange(1296.), index=pd.date_range('2020-01-01', periods=1296, freq='10min'))
    np.testing.assert_array_equal(lab.baselines(s, s.index[1200])['yesterday'], np.arange(1201,1207)-144)


def test_missing_target_excludes_origin_for_every_arm(tmp_path):
    s = pd.Series(100., index=pd.date_range('2020-01-01', periods=1026, freq='10min'))
    s.iloc[1012] = np.nan
    config = {'classical': [], 'model_seeds': [], 'first_scored_day': 7,
              'adaptive_k': 6, 'epochs': 1}
    rows, _ = lab.run_dataset('fixture', s, 'units', config, tmp_path)
    assert rows
    assert not any(pd.Timestamp(r['origin']) in (s.index[1008], s.index[1011]) for r in rows)
    groups = pd.DataFrame(rows).groupby('model').origin.apply(set)
    assert all(origins == groups.iloc[0] for origins in groups)
