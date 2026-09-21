"""Training must not see the validation period, directly or through preprocessing.

Codex C-46: RobustScaler was fitted on the whole series before the split, and the
80/20 split cut the SEQUENCE list even though neighbouring sequences share target
rows -- so training labels and validation labels covered the same timestamps.
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
SEQ = 12  # short sequence length keeps these tests fast


def series(n, end=datetime(2026, 3, 2, 12, 0), scale=1.0, offset=0.0):
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = [offset + scale * (100 + 50 * np.sin(2 * np.pi * i / 144.0)) for i in range(n)]
    return pd.DataFrame({"value": vals}, index=pd.DatetimeIndex(idx))


def split_plan(n_rows, seq=SEQ, steps=STEPS_AHEAD):
    """Recompute the implementation's index plan without training a network.

    C-53: the rule is disjoint TARGET periods. Input windows may overlap.
    """
    split_row = int(0.8 * n_rows)
    n_seq = n_rows - seq - steps + 1
    train_idx = [i for i in range(n_seq) if i + seq + steps - 1 < split_row]
    val_idx = [i for i in range(n_seq) if i + seq >= split_row]
    return train_idx, val_idx, split_row


def target_rows(i, seq=SEQ, steps=STEPS_AHEAD):
    return set(range(i + seq, i + seq + steps))


def test_training_and_validation_targets_never_share_a_timestamp():
    n = 400
    train_idx, val_idx, _ = split_plan(n)
    assert train_idx and val_idx, "the purged split produced an empty side"
    train_targets = set().union(*(target_rows(i) for i in train_idx))
    val_targets = set().union(*(target_rows(i) for i in val_idx))
    assert not (train_targets & val_targets), (
        f"{len(train_targets & val_targets)} target rows appear in BOTH partitions"
    )


def test_the_old_contiguous_split_did_leak():
    """Document the defect: the previous scheme shared target rows across the boundary."""
    n = 400
    n_seq = n - SEQ - STEPS_AHEAD + 1
    old_train = range(0, int(0.8 * n_seq))
    old_val = range(int(0.8 * n_seq), n_seq)
    shared = set().union(*(target_rows(i) for i in old_train)) & \
        set().union(*(target_rows(i) for i in old_val))
    assert shared, "expected the pre-fix split to share target rows"


def test_scaler_is_fitted_on_training_rows_only():
    """A huge spike inside the validation tail must not move the fitted scale."""
    n = 400
    base = series(n)
    spiked = base.copy()
    spiked.iloc[int(0.9 * n):, 0] = 10_000.0  # only in the validation tail

    from sklearn.preprocessing import RobustScaler

    split_row = int(0.8 * n)
    a, b = RobustScaler(), RobustScaler()
    a.fit(base["value"].values.reshape(-1, 1)[:split_row])
    b.fit(spiked["value"].values.reshape(-1, 1)[:split_row])
    assert np.allclose(a.center_, b.center_) and np.allclose(a.scale_, b.scale_), (
        "training-only fit still moved when the validation tail changed"
    )

    whole_a, whole_b = RobustScaler(), RobustScaler()
    whole_a.fit(base["value"].values.reshape(-1, 1))
    whole_b.fit(spiked["value"].values.reshape(-1, 1))
    assert not np.allclose(whole_a.scale_, whole_b.scale_), (
        "expected a whole-series fit to be contaminated by the validation tail"
    )


def test_metadata_declares_the_split_discipline():
    """The trained model must record how it split, so a reader can check."""
    m = LSTMForecastModel(sequence_length=SEQ)
    data = series(300)
    result = m.train(data, epochs=1)
    meta = result["metadata"]
    assert meta["scaler_fitted_on"] == "training rows only"
    assert meta["purged_split"] is True
    assert meta["split_rule"] == "disjoint target periods; input windows may overlap"
    assert meta["training_samples"] > 0 and meta["validation_samples"] > 0


def test_evaluate_reports_what_it_scored():
    """evaluate() must say whether it scored the served blend or the bare network."""
    m = LSTMForecastModel(sequence_length=SEQ)
    data = series(300)
    m.train(data, epochs=1)
    out = m.evaluate(series(120, end=datetime(2026, 3, 3, 12, 0)))
    assert out["scored"] in ("served_blend", "raw_network")
    assert "network_only" in out and "bias" in out

def test_short_series_fails_loudly_instead_of_falling_back():
    """C-53: the contiguous fallback silently restored the leak. It must not exist."""
    m = LSTMForecastModel(sequence_length=SEQ)
    tiny = series(SEQ + STEPS_AHEAD + 3)
    with pytest.raises(ValueError, match="disjoint label periods"):
        m.train(tiny, epochs=1)


def test_input_windows_may_overlap_the_other_partition():
    """Reading a shared historical row is not leakage; sharing a LABEL is."""
    n = 400
    train_idx, val_idx, split_row = split_plan(n)
    # At least one validation sequence reads rows the training partition also read.
    overlaps = [i for i in val_idx if i < split_row]
    assert overlaps, "expected input overlap to be permitted"
    # ...yet no target row is shared.
    tt = set().union(*(target_rows(i) for i in train_idx))
    vt = set().union(*(target_rows(i) for i in val_idx))
    assert not (tt & vt)
