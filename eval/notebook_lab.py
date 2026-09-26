#!/usr/bin/env python3
"""Optional local forecasting lab: causal, common-target notebook comparisons.

This module is independent of the served model and benchmark qualification. Local
CSV data and fitted artifacts are never bundled with the source. Imports for model
training are lazy so the information-boundary tests need no optional dependencies.
"""
from pathlib import Path
import sys

import numpy as np
import pandas as pd

GRID = 10
PER_DAY = 144
STEPS = 6


def regularize(series):
    """Retain exact ten-minute targets; forward-fill only the input copy."""
    series = series.sort_index()
    if series.index.has_duplicates or not isinstance(series.index, pd.DatetimeIndex):
        raise ValueError('Expected unique datetime observations')
    grid = pd.date_range(series.index.min().ceil('10min'),
                         series.index.max().floor('10min'), freq='10min')
    observed = series.reindex(grid).astype(float)
    return observed, observed.ffill()


def make_samples(observed, lookback=PER_DAY, steps=STEPS):
    """Build history windows and observed labels; never fill missing labels."""
    filled = observed.ffill().to_numpy(dtype=float)
    raw = observed.to_numpy(dtype=float)
    result = {k: [] for k in ('x', 'y', 'origin', 'target_start', 'target_end')}
    for i in range(lookback - 1, len(observed) - steps):
        x = filled[i - lookback + 1:i + 1]
        y = raw[i + 1:i + steps + 1]
        if not np.isfinite(x).all() or not np.isfinite(y).all():
            continue
        result['x'].append(x)
        result['y'].append(y)
        result['origin'].append(observed.index[i])
        result['target_start'].append(observed.index[i + 1])
        result['target_end'].append(observed.index[i + steps])
    return {k: np.asarray(v) for k, v in result.items()}


def split_samples(samples, validation_start):
    """Purge training labels that reach validation; retain chronological validation."""
    train = np.asarray([t < validation_start for t in samples['target_end']])
    val = np.asarray([t >= validation_start for t in samples['origin']])
    return ({k: v[train] for k, v in samples.items()},
            {k: v[val] for k, v in samples.items()})


def adaptive(history, pattern, k):
    """Median recent/yesterday level ratio; unavailable support keeps the pattern."""
    x = np.asarray(history, dtype=float)
    if k not in (3, 6, 12):
        raise ValueError('Predeclared lookback must be 3, 6 or 12')
    if len(x) < PER_DAY + k:
        return np.asarray(pattern).copy(), True
    recent, prior = x[-k:], x[-PER_DAY-k:-PER_DAY]
    if not (np.isfinite(recent).all() and np.isfinite(prior).all() and (prior > 0).all()):
        return np.asarray(pattern).copy(), True
    return np.asarray(pattern) * np.median(recent / prior), False


def baselines(series, origin, k=6):
    """Each forecast can read observations at or before its declared origin only."""
    history = series.loc[:origin]
    filled = history.ffill()
    if len(history) < PER_DAY + STEPS or not np.isfinite(filled.iloc[-1]):
        raise ValueError('Insufficient causal history')
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ml-engine'))
    from models.lstm_model import LSTMForecastModel
    helper = LSTMForecastModel(sequence_length=PER_DAY)
    pattern, _ = helper._pattern_forecast(
        origin=pd.Timestamp(origin).to_pydatetime(), steps_ahead=STEPS,
        seasonal_history=history[np.isfinite(history.to_numpy(dtype=float))],
        effective_pct=70)
    if pattern is None:
        pattern = np.full(STEPS, np.nan)
    targets = pd.date_range(pd.Timestamp(origin) + pd.Timedelta(minutes=GRID),
                            periods=STEPS, freq='10min')
    yesterday = history.reindex(targets - pd.Timedelta(days=1)).to_numpy(dtype=float)
    adjusted, _ = adaptive(history.to_numpy(), pattern, k)
    return {'persistence': np.full(STEPS, filled.iloc[-1]),
            'yesterday': yesterday, 'pattern70': np.asarray(pattern),
            'adaptive': adjusted}


def calendar(index):
    """Known clock features, not future traffic observations."""
    hour = index.hour.to_numpy() + index.minute.to_numpy() / 60
    day = index.dayofweek.to_numpy()
    return np.column_stack([np.sin(2*np.pi*hour/24), np.cos(2*np.pi*hour/24),
                            np.sin(2*np.pi*day/7), np.cos(2*np.pi*day/7)])


def fit_lstm(observed, seed, artifact_dir, epochs=32):
    """Train a compact direct model with purged chronological validation."""
    import json
    import time
    import tensorflow as tf
    from sklearn.preprocessing import MinMaxScaler
    tf.keras.backend.clear_session()
    tf.keras.utils.set_random_seed(seed)
    start = time.monotonic()
    samples = make_samples(observed)
    boundary = observed.index[-PER_DAY]
    train, valid = split_samples(samples, boundary)
    if len(train['x']) < 100 or len(valid['x']) < 20:
        raise ValueError('Insufficient training/validation samples')
    scaler = MinMaxScaler().fit(observed.loc[observed.index < boundary].dropna().to_numpy().reshape(-1, 1))

    def features(part):
        level = scaler.transform(part['x'].reshape(-1, 1)).reshape(part['x'].shape)
        clocks = np.stack([calendar(pd.date_range(t - pd.Timedelta(minutes=GRID*(PER_DAY-1)),
                                                  periods=PER_DAY, freq='10min'))
                           for t in part['origin']])
        return np.concatenate([level[..., None], clocks], axis=2).astype(np.float32)

    x, xv = features(train), features(valid)
    y = scaler.transform(train['y'].reshape(-1, 1)).reshape(train['y'].shape).astype(np.float32)
    yv = scaler.transform(valid['y'].reshape(-1, 1)).reshape(valid['y'].shape).astype(np.float32)
    model = tf.keras.Sequential([tf.keras.Input((PER_DAY, 5)),
                                tf.keras.layers.LSTM(32, activation='tanh'),
                                tf.keras.layers.Dense(STEPS)])
    model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.001, clipnorm=1), loss='mae')
    rng = np.random.default_rng(seed)
    history, best, best_weights, stale = [], float('inf'), None, 0
    for epoch in range(epochs):
        model.reset_metrics()
        order = rng.permutation(len(x))
        for offset in range(0, len(x), 64):
            batch = order[offset:offset+64]
            model.train_on_batch(x[batch], y[batch])
        pred = model(xv, training=False).numpy()
        loss = float(np.mean(np.abs(pred-yv)))
        if not np.isfinite(loss):
            raise ValueError('Nonfinite validation loss; do not silently accept this fit')
        history.append({'epoch': epoch+1, 'validation_mae_scaled': loss})
        if loss < best:
            best, best_weights, stale = loss, model.get_weights(), 0
        else:
            stale += 1
        if stale >= 5:
            break
    model.set_weights(best_weights)
    artifact_dir = Path(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=False)
    model.save(artifact_dir / 'model.keras')
    metadata = {'seed': seed, 'epochs': len(history), 'history': history,
                'train_samples': len(x), 'validation_samples': len(xv),
                'validation_start': str(boundary), 'fit_input_end': str(observed.index[-1]),
                'last_training_target': str(max(train['target_end'])),
                'scaler_min': scaler.data_min_.tolist(), 'scaler_max': scaler.data_max_.tolist(),
                'seconds': time.monotonic()-start}
    (artifact_dir/'training.json').write_text(json.dumps(metadata, indent=2))
    return (model, scaler), metadata


def predict_lstm(bundle, history):
    """Six direct steps from the same observed history given to the other arms."""
    model, scaler = bundle
    recent = history.ffill().iloc[-PER_DAY:]
    x = np.column_stack([scaler.transform(recent.to_numpy().reshape(-1, 1)).ravel(),
                         calendar(recent.index)])[None].astype(np.float32)
    prediction = model(x, training=False).numpy().ravel()
    return scaler.inverse_transform(prediction.reshape(-1, 1)).ravel()


def fit_classical(name, observed):
    """Fit the classical candidates; all receive only the available history."""
    x = observed.ffill().to_numpy()
    if not np.isfinite(x).all():
        raise ValueError('Training history has no preceding value for a gap')
    if name == 'prophet':
        from prophet import Prophet
        model = Prophet(daily_seasonality=True, weekly_seasonality=False,
                        yearly_seasonality=False, seasonality_mode='additive',
                        changepoint_prior_scale=0.05)
        model.fit(pd.DataFrame({'ds': observed.index, 'y': x}))
        return model
    if name == 'arima':
        from statsmodels.tsa.arima.model import ARIMA
        return ARIMA(x, order=(5, 1, 2)).fit(method_kwargs={'maxiter': 100})
    if name == 'holt_winters':
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
        return ExponentialSmoothing(x, trend='add', seasonal='add',
                                    seasonal_periods=PER_DAY,
                                    initialization_method='heuristic').fit(
                                        optimized=True, use_brute=False,
                                        method='L-BFGS-B',
                                        minimize_kwargs={'options': {'maxiter': 100}})
    raise ValueError(name)


def predict_classical(name, model, fit_history, history):
    """Advance state causally between fits, with smoothing/ARIMA parameters fixed."""
    if name == 'prophet':
        targets = pd.date_range(history.index[-1]+pd.Timedelta(minutes=GRID),
                                periods=STEPS, freq='10min')
        return model.predict(pd.DataFrame({'ds': targets}))['yhat'].to_numpy()
    if name == 'arima':
        new = history.ffill().loc[history.index > fit_history.index[-1]].to_numpy()
        updated = model.extend(new) if len(new) else model
        return np.asarray(updated.forecast(STEPS))
    if name == 'holt_winters':
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
        p = model.params
        # Recompute only the causal state path from the same known initial state;
        # optimization is disabled, so observations do not refit parameters.
        updated = ExponentialSmoothing(history.ffill().to_numpy(), trend='add', seasonal='add',
                                       seasonal_periods=PER_DAY, initialization_method='known',
                                       initial_level=p['initial_level'], initial_trend=p['initial_trend'],
                                       initial_seasonal=p['initial_seasons']).fit(
                                           optimized=False, smoothing_level=p['smoothing_level'],
                                           smoothing_trend=p['smoothing_trend'],
                                           smoothing_seasonal=p['smoothing_seasonal'])
        return np.asarray(updated.forecast(STEPS))
    raise ValueError(name)


def build_datasets(csv_path, data_seed):
    """Private observed counts plus public generator with declared test-period shocks."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import offline_eval as oe
    frame = pd.read_csv(csv_path)
    observed, _ = regularize(pd.Series(pd.to_numeric(frame['Expected'], errors='raise').to_numpy(),
                                       index=pd.to_datetime(frame['Time'])))
    datasets = {'local_replicas': (observed, 'replicas')}
    for name in ('repeating', 'trend', 'level_shift', 'burst_revert'):
        kind = 'trend' if name == 'trend' else 'repeating'
        s = oe.make_series(kind, days=14, seed=data_seed,
                           start=pd.Timestamp('2027-02-01').to_pydatetime())
        if name == 'level_shift':
            s.iloc[int(12.5*PER_DAY):] *= 1.55
        elif name == 'burst_revert':
            for day in (12.5, 13.5):
                at = int(day*PER_DAY)
                s.iloc[at:at+3] *= 2.5
        datasets[name] = (s, 'RPM')
    return datasets


def run_dataset(name, series, units, config, output):
    """Identical rolling origins, six-hour refits and explicit failure accounting."""
    import json
    import time
    import warnings
    out = Path(output)/name
    out.mkdir(exist_ok=False)
    series.to_csv(out/'observations.csv', header=['value'], index_label='time')
    first = config.get('first_scored_day', 12)*PER_DAY
    origins = list(range(first, len(series)-STEPS, 3))
    rows, fits, skips, errors = [], [], [], []
    bundles = {}
    models = config['classical'] + [f'lstm_{seed}' for seed in config['model_seeds']]
    last_fit = -1
    for number, i in enumerate(origins):
        origin = series.index[i]
        if last_fit < 0 or i-last_fit >= 36:
            last_fit = i
            fit_start = max(0, i+1-7*PER_DAY)
            fit_history = series.iloc[fit_start:i+1]
            bundles = {}
            for model_name in models:
                started = time.monotonic()
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always')
                    try:
                        if model_name.startswith('lstm_'):
                            seed = int(model_name.split('_')[1])
                            bundle, metadata = fit_lstm(fit_history, seed, out/f'fit-{i}-{model_name}',
                                                        epochs=config['epochs'])
                        else:
                            bundle = fit_classical(model_name, fit_history)
                            metadata = {}
                        bundles[model_name] = bundle
                        status, error = 'ok', None
                    except Exception as exc:
                        status, error, metadata = 'failed', repr(exc), {}
                fit = dict(model=model_name, origin=str(origin), status=status, error=error,
                           seconds=time.monotonic()-started, metadata=metadata,
                           warnings=[str(w.message) for w in caught])
                fits.append(fit)
                (out/'fits.json').write_text(json.dumps(fits, indent=2))
                print(json.dumps({'dataset': name, 'refit': str(origin), 'model': model_name,
                                  'status': status, 'seconds': round(fit['seconds'], 2)}), flush=True)
        targets = series.iloc[i+1:i+1+STEPS]
        if len(targets) != STEPS or not np.isfinite(targets).all():
            skips.append(str(origin))
            continue  # Excluded for EVERY arm, solely because target evidence is absent.
        history = series.iloc[:i+1]
        try:
            forecasts = baselines(series, origin, config['adaptive_k'])
        except Exception as exc:
            forecasts = {arm: np.full(STEPS, np.nan) for arm in
                         ('persistence', 'yesterday', 'pattern70', 'adaptive')}
            for arm in forecasts:
                errors.append(dict(model=arm, origin=str(origin), error=repr(exc)))
        if config.get('include_existing_trend_adaptive', False):
            try:
                from offline_eval import TrendAdaptive
                forecasts['trend_adaptive'] = TrendAdaptive().forecast(history, origin, STEPS)
            except Exception as exc:
                forecasts['trend_adaptive'] = np.full(STEPS, np.nan)
                errors.append(dict(model='trend_adaptive', origin=str(origin), error=repr(exc)))
        _, fallback = adaptive(history.to_numpy(), forecasts['pattern70'], config['adaptive_k'])
        if config.get('origin_refit_prophet', False):
            started = time.monotonic()
            try:
                fresh_history = history.iloc[-7*PER_DAY:]
                fresh = fit_classical('prophet', fresh_history)
                forecasts['prophet_origin'] = predict_classical('prophet', fresh, fresh_history, fresh_history)
                status, error = 'ok', None
            except Exception as exc:
                forecasts['prophet_origin'] = np.full(STEPS, np.nan)
                status, error = 'failed', repr(exc)
            fits.append(dict(model='prophet_origin', origin=str(origin), status=status,
                             error=error, seconds=time.monotonic()-started,
                             metadata={'fit_input_end': str(origin)}, warnings=[]))
        for model_name in models:
            try:
                if model_name not in bundles:
                    raise ValueError('Required refit unavailable')
                if model_name.startswith('lstm_'):
                    forecasts[model_name] = predict_lstm(bundles[model_name], history)
                else:
                    forecasts[model_name] = predict_classical(model_name, bundles[model_name],
                                                             fit_history, series.iloc[fit_start:i+1])
            except Exception as exc:
                forecasts[model_name] = np.full(STEPS, np.nan)
                errors.append(dict(model=model_name, origin=str(origin), error=repr(exc)))
        for arm, pred in forecasts.items():
            pred = np.asarray(pred, dtype=float)
            if pred.shape != (STEPS,):
                raise ValueError(f'{arm}: wrong forecast shape')
            for step, (target, actual) in enumerate(targets.items()):
                value = max(0., float(pred[step])) if np.isfinite(pred[step]) else None
                rows.append(dict(dataset=name, units=units, model=arm, origin=str(origin),
                                 target=str(target), horizon_minutes=(step+1)*GRID,
                                 actual=float(actual), forecast=value,
                                 adaptive_fallback=bool(fallback) if arm == 'adaptive' else None))
        if number % 12 == 0:
            (out/'progress.json').write_text(json.dumps({'completed_origins': number+1,
                                                       'planned_origins': len(origins)}))
    (out/'fits.json').write_text(json.dumps(fits, indent=2))
    pd.DataFrame(rows).to_csv(out/'predictions.csv', index=False)
    (out/'failures.json').write_text(json.dumps({'fits': [f for f in fits if f['status'] != 'ok'],
                                               'predictions': errors, 'missing_target_origins': skips}, indent=2))
    return rows, fits


def summarize(rows, fits, output):
    """Report separately by dataset, horizon and arm; missing forecasts cannot win."""
    import json
    frame = pd.DataFrame(rows)
    frame['error'] = frame.forecast-frame.actual
    summary = []
    for (dataset, model, horizon), g in frame.groupby(['dataset', 'model', 'horizon_minutes']):
        missing = int(g.forecast.isna().sum())
        summary.append(dict(dataset=dataset, model=model, horizon_minutes=int(horizon),
                            units=g.units.iloc[0], targets=len(g), unavailable=missing,
                            mae=float(g.error.abs().mean()) if missing == 0 else None,
                            bias=float(g.error.mean()) if missing == 0 else None,
                            rmse=float(np.sqrt(np.mean(g.error**2))) if missing == 0 else None))
    out = Path(output)
    pd.DataFrame(summary).to_csv(out/'scores.csv', index=False)
    (out/'results.json').write_text(json.dumps({'scores': summary,
                                               'fit_failures': sum(f['status'] != 'ok' for f in fits),
                                               'meaning': 'Offline mechanism diagnostic; not live qualification or deployment evidence'}, indent=2))
    return frame, pd.DataFrame(summary)


def main():
    """Execute a frozen, explicitly supplied protocol; never overwrite a run."""
    import argparse
    import hashlib
    import importlib.metadata
    import json
    import os
    import time
    import tensorflow as tf
    import logging
    logging.getLogger('cmdstanpy').setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, required=True)
    parser.add_argument('--protocol', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.protocol.read_text())
    if hashlib.sha256(args.csv.read_bytes()).hexdigest() != config['csv_sha256']:
        raise ValueError('CSV changed after protocol freeze')
    for relative, digest in config['source_sha256'].items():
        if hashlib.sha256((Path(__file__).parent/relative).read_bytes()).hexdigest() != digest:
            raise ValueError(f'Frozen source changed: {relative}')
    tf.config.threading.set_intra_op_parallelism_threads(2)
    tf.config.threading.set_inter_op_parallelism_threads(2)
    tf.config.experimental.enable_op_determinism()
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out/'protocol.json').write_bytes(args.protocol.read_bytes())
    versions = {p: importlib.metadata.version(p) for p in ('numpy', 'pandas', 'tensorflow',
                                                         'statsmodels', 'prophet', 'scikit-learn')}
    (args.out/'environment.json').write_text(json.dumps({'versions': versions, 'python': sys.version,
                                                        'platform': sys.platform,
                                                        'threads': 2}, indent=2))
    started = time.monotonic()
    rows, fits = [], []
    try:
        datasets = build_datasets(args.csv, config['data_seed'])
        for name in config['datasets']:
            s, units = datasets[name]
            expected_data = config.get('dataset_sha256', {}).get(name)
            if expected_data and hashlib.sha256(s.to_csv(header=['value'], index_label='time').encode()).hexdigest() != expected_data:
                raise ValueError(f'Frozen prepared dataset changed: {name}')
            new_rows, new_fits = run_dataset(name, s, units, config, args.out)
            rows.extend(new_rows)
            fits.extend(new_fits)
            summarize(rows, fits, args.out)
        incomplete_arm = any(f['status'] != 'ok' for f in fits) or any(r['forecast'] is None for r in rows)
        status = 'COMPLETE_WITH_FAILURES' if incomplete_arm else 'COMPLETE'
    except BaseException as exc:
        (args.out/'status.json').write_text(json.dumps({'status': 'INCOMPLETE', 'error': repr(exc),
                                                      'elapsed_seconds': time.monotonic()-started}))
        raise
    (args.out/'status.json').write_text(json.dumps({'status': status, 'elapsed_seconds': time.monotonic()-started,
                                                  'pid': os.getpid()}, indent=2))


if __name__ == '__main__':
    main()
