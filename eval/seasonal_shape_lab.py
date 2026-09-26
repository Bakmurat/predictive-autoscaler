"""Offline factorial probe of daily shape and recent level; no serving integration."""
from pathlib import Path
import sys

import numpy as np
import pandas as pd


def shapes(series, targets, as_of):
    """Use identical finite, exact-clock daily support for two shape estimators."""
    if not isinstance(series.index, pd.DatetimeIndex) or series.index.has_duplicates:
        raise ValueError('Unique datetime observations required')
    history = series.sort_index().loc[:as_of]
    engine = str(Path(__file__).resolve().parents[1] / 'ml-engine')
    if engine not in sys.path:
        sys.path.insert(0, engine)
    from models.lstm_model import weighted_percentile
    result = {'pattern70': [], 'mean7': []}
    support = []
    for target in targets:
        times = pd.DatetimeIndex([pd.Timestamp(target)-pd.Timedelta(days=d) for d in range(1, 8)])
        values = history.reindex(times).to_numpy(dtype=float)
        valid = np.isfinite(values)
        support.append(int(valid.sum()))
        result['pattern70'].append(weighted_percentile(values[valid],
                                  np.power(.3, np.arange(7))[valid], 70) if valid.any() else np.nan)
        result['mean7'].append(float(values[valid].mean()) if valid.any() else np.nan)
    return {name: np.asarray(values) for name, values in result.items()}, support


def forecast(series, origin):
    """Cross two daily shapes with an optional six-slot as-of level adjustment.

    The adjustment deliberately remains a mechanism probe: it can overcorrect
    after a burst or around the anniversary of a level change. Require all six
    finite positive observation/shape pairs; otherwise retain the unadjusted shape.
    """
    origin = pd.Timestamp(origin)
    targets = pd.date_range(origin+pd.Timedelta(minutes=10), periods=6, freq='10min')
    future, support = shapes(series, targets, origin)
    recent_times = pd.date_range(origin-pd.Timedelta(minutes=50), origin, freq='10min')
    recent_shapes, recent_support = shapes(series, recent_times, origin)
    actual = series.loc[:origin].reindex(recent_times).to_numpy(dtype=float)
    details = {'target_support': support, 'recent_support': recent_support}
    for base, arm in [('pattern70', 'pattern_ratio'), ('mean7', 'mean_ratio')]:
        denominator = recent_shapes[base]
        valid = np.isfinite(actual) & np.isfinite(denominator) & (actual > 0) & (denominator > 0)
        fallback = not bool(valid.all())
        ratio = 1. if fallback else float(np.median(actual/denominator))
        future[arm] = future[base]*ratio
        details[arm] = {'ratio': ratio, 'fallback': fallback,
                        'valid_pairs': int(valid.sum()),
                        'denominators': [float(x) if np.isfinite(x) else None for x in denominator]}
    return {name: np.maximum(0, values) for name, values in future.items()}, details
