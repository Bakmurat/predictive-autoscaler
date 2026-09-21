"""The preflight and the model must agree on eligibility, exactly at the boundary.

Codex C-57: the preflight counted an 80/20 split of the sequence LIST while the deployed
model splits on disjoint TARGET periods. It therefore called 189 points trainable when the
model raises there, so a scheduled run at 189 would have skipped or failed, not trained.
Both now call models.lstm_model.purged_split_indices.

These tests fail on the pre-fix preflight.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.lstm_model import STEPS_AHEAD, purged_split_indices  # noqa: E402
from training.train_lstm_from_vm import (  # noqa: E402
    DEFAULT_SEQUENCE_LENGTH as L,
    min_points_for_training,
    sequence_budget,
)

MINIMUM = 235


def test_the_minimum_is_235_points():
    """Derivation: the inner split needs int(0.8*R) >= L+S = 150, so R >= 188; the outer
    split gives R = int(0.8*n), so int(0.8*n) >= 188, so n >= 235 (0.8*234 -> 187)."""
    assert min_points_for_training(L, STEPS_AHEAD) == MINIMUM


def test_one_point_below_the_minimum_has_no_training_sequence():
    b = sequence_budget(MINIMUM - 1, L, STEPS_AHEAD)
    assert b["train_sequences"] == 0, b
    assert b["train_rows"] == 187 and int(0.8 * b["train_rows"]) == 149


def test_at_the_minimum_both_sides_are_non_empty():
    b = sequence_budget(MINIMUM, L, STEPS_AHEAD)
    assert b["train_sequences"] >= 1 and b["validation_sequences"] >= 1, b


def test_189_is_not_eligible_despite_the_old_claim():
    """The number the old preflight published, and the reason a run there would not train."""
    b = sequence_budget(189, L, STEPS_AHEAD)
    assert b["train_sequences"] == 0, "189 points must not be reported as trainable"


@pytest.mark.parametrize("n", [MINIMUM - 1, MINIMUM, MINIMUM + 1, 400, 1008])
def test_preflight_matches_the_model_split_exactly(n):
    """One function decides for both: the preflight's counts ARE the model's counts."""
    b = sequence_budget(n, L, STEPS_AHEAD)
    train_idx, val_idx = purged_split_indices(b["train_rows"], L, STEPS_AHEAD,
                                              n_sequences=b["sequences"])
    assert (b["train_sequences"], b["validation_sequences"]) == (len(train_idx), len(val_idx))


@pytest.mark.parametrize("n_rows", [188, 300, 1008])
def test_label_periods_are_disjoint(n_rows):
    seqs = max(0, n_rows - L - STEPS_AHEAD + 1)
    train_idx, val_idx = purged_split_indices(n_rows, L, STEPS_AHEAD, n_sequences=seqs)
    def targets(idx):
        out = set()
        for i in idx:
            out.update(range(i + L, i + L + STEPS_AHEAD))
        return out
    assert not (targets(train_idx) & targets(val_idx))


def test_there_is_no_contaminated_fallback():
    """Below the minimum the split must yield an empty side, never a contiguous rescue."""
    train_idx, _ = purged_split_indices(187, L, STEPS_AHEAD)
    assert len(train_idx) == 0
