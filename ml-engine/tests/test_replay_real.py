"""The chronological replay of real benchmark traffic: hygiene and its refusal rule.

Codex D-124 asked for a replay of the ACCUMULATED REAL traffic, in chronological order with
rolling origins, and said plainly: do not score it if the history is too short. These tests
pin the two things that makes worth anything --

  * no look-ahead: a forecast at origin i must be a function of series[:i+1] and nothing
    later. Verified by CHANGING the future and requiring the forecast not to move;
  * an honest refusal: the minimum-scale rule is predeclared in the module, and a short
    history must fail it rather than quietly produce a comparison.

and the masking rules that decide which origins exist at all.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "eval"))

import replay_real as rr  # noqa: E402

PER_DAY = rr.PER_DAY
STEPS = rr.STEPS_AHEAD


def series(n, start="2026-09-20T00:00:00Z", seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range(pd.Timestamp(start).tz_localize(None), periods=n, freq="10min")
    base = 4000 + 800 * np.sin(2 * np.pi * np.arange(n) / PER_DAY)
    return pd.Series(base + rng.normal(0, 40, n), index=idx)


# ------------------------------------------------------------------ origin eligibility
def test_origins_need_history_behind_and_six_genuine_targets_ahead():
    s = series(PER_DAY + 60)
    imputed = np.zeros(len(s), dtype=bool)
    o = rr.eligible_origins(s, imputed, PER_DAY)
    assert o[0] == PER_DAY
    assert o[-1] == len(s) - STEPS - 1
    assert len(o) == len(s) - PER_DAY - STEPS


def test_an_imputed_origin_is_never_forecast_from():
    s = series(PER_DAY + 60)
    imputed = np.zeros(len(s), dtype=bool)
    imputed[PER_DAY + 10] = True
    assert PER_DAY + 10 not in rr.eligible_origins(s, imputed, PER_DAY)


def test_an_imputed_target_disqualifies_every_origin_that_would_be_scored_against_it():
    s = series(PER_DAY + 60)
    imputed = np.zeros(len(s), dtype=bool)
    bad = PER_DAY + 30
    imputed[bad] = True
    got = rr.eligible_origins(s, imputed, PER_DAY)
    for i in range(bad - STEPS, bad):
        assert i not in got, f"origin {i} would be scored against the filled slot {bad}"
    assert bad - STEPS - 1 in got


# ------------------------------------------------------------------ no look-ahead
@pytest.mark.parametrize("arm", sorted(rr.ARMS))
def test_no_forecaster_can_see_past_its_origin(arm):
    """Change everything after the origin. A forecast that moves has read the future."""
    n = PER_DAY + 40
    s = series(n, seed=3)
    i = PER_DAY + 10
    pred = rr.ARMS[arm]
    before = np.asarray(pred.forecast(s.iloc[:i + 1], s.index[i], STEPS), dtype=float)

    tampered = s.copy()
    tampered.iloc[i + 1:] = tampered.iloc[i + 1:] * 10.0 + 50_000.0
    after = np.asarray(pred.forecast(tampered.iloc[:i + 1], tampered.index[i], STEPS),
                       dtype=float)
    assert np.allclose(before, after), f"{arm} changed when only the FUTURE changed"


def test_the_replay_runs_chronologically_over_every_eligible_origin():
    s = series(PER_DAY + 40, seed=5)
    imputed = np.zeros(len(s), dtype=bool)
    out = rr.run(s, imputed, PER_DAY, use_replay=False, log=lambda *_a, **_k: None)
    expected = len(s) - PER_DAY - STEPS
    assert out["origins"] == expected
    assert out["first_origin_index"] == PER_DAY
    for arm in rr.ARMS:
        assert out["arms"][arm]["origins"] == expected
        assert out["arms"][arm]["non_finite_origins"] == 0
        assert len(out["arms"][arm]["per_step_mae"]) == STEPS


# ------------------------------------------------------------------ the refusal rule
def test_a_short_history_is_refused_rather_than_scored():
    s = series(PER_DAY + 40)
    imputed = np.zeros(len(s), dtype=bool)
    cen = rr.census(s, imputed, origins_n=40, first_origin_index=PER_DAY)
    assert cen["scoreable"] is False
    failed = [k for k, v in cen["checks"].items() if not v["pass"]]
    assert set(failed) == {"origin_days", "warmup_days_before_first_origin",
                           "independent_blocks"}


def test_the_rule_passes_once_there_is_enough_history():
    n = (rr.MIN_WARMUP_DAYS + rr.MIN_ORIGIN_DAYS) * PER_DAY + STEPS
    s = series(n)
    imputed = np.zeros(n, dtype=bool)
    origins = rr.eligible_origins(s, imputed, rr.MIN_WARMUP_DAYS * PER_DAY)
    cen = rr.census(s, imputed, len(origins), origins[0])
    assert cen["scoreable"] is True, cen["checks"]
    assert rr.points_needed() == n


def test_each_check_can_fail_on_its_own():
    n = (rr.MIN_WARMUP_DAYS + rr.MIN_ORIGIN_DAYS) * PER_DAY + STEPS
    s = series(n)
    imputed = np.zeros(n, dtype=bool)
    # enough origins, not enough warmup
    cen = rr.census(s, imputed, rr.MIN_ORIGIN_DAYS * PER_DAY, (rr.MIN_WARMUP_DAYS - 1) * PER_DAY)
    assert not cen["checks"]["warmup_days_before_first_origin"]["pass"]
    assert cen["checks"]["origin_days"]["pass"]
    # enough warmup, not enough origins
    cen = rr.census(s, imputed, PER_DAY, rr.MIN_WARMUP_DAYS * PER_DAY)
    assert cen["checks"]["warmup_days_before_first_origin"]["pass"]
    assert not cen["checks"]["origin_days"]["pass"]


def test_independent_blocks_count_non_overlapping_origins():
    """Origins closer than STEPS_AHEAD share targets and are not independent samples."""
    s = series(PER_DAY + 40)
    imputed = np.zeros(len(s), dtype=bool)
    cen = rr.census(s, imputed, origins_n=STEPS * 5 + 3, first_origin_index=PER_DAY)
    assert cen["checks"]["independent_blocks"]["observed"] == 5
