"""Offline challenge bake-off harness: generator fidelity, sampling artefact, replay rules, margin parity."""
import importlib.util
import math
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "challenge_bakeoff", ROOT / "deploy" / "eks-benchmark" / "scoring" / "challenge_bakeoff.py")
bo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bo)
se, cp = bo.se, bo.cp


def test_generator_is_bit_identical_to_the_sealed_generator_for_the_sealed_seed():
    n = 3 * bo.SEASON
    mine = bo.multipliers_for(cp.PROFILE["seed"], n)
    theirs = [m[0] for m in cp.multipliers(n)]
    assert mine == theirs


def test_other_seeds_give_other_traffic_within_the_declared_bounds():
    a, b = bo.multipliers_for(1, 288), bo.multipliers_for(2, 288)
    assert a != b
    lo, hi = cp.PROFILE["multiplier_bounds"]
    assert all(lo <= m <= hi for m in a + b)
    assert cp.PROFILE["seed"] not in (1, 2)


def test_warm_days_are_exactly_repeating_v2_and_sampling_carries_the_previous_slot():
    rates = bo.offered_rates(seed=3, warm_days=1, challenge_days=1)
    base = cp.PROFILE["base_pattern_utc_rpm"]
    assert rates[:bo.SEASON] == [base[k // 6] for k in range(bo.SEASON)]
    pts = bo.sampled_series(rates, seed=3, noise=0.0)
    # the sample at slot k is the rate offered during slot k-1
    assert [v for _, v in pts[1:]] == [float(r) for r in rates[:-1]]
    assert all(t % bo.SLOT == 0 for t, _ in pts)


def _flat(n, level=1000.0, t0=bo.DEFAULT_T0):
    return np.full(n, level), list(range(0, n - 2))


def test_replay_perfect_forecast_has_no_shortage_and_reactive_only_pays_the_lag_on_a_rise():
    y = np.array([600.0] * 5 + [1800.0] * 5)           # 1 pod, then 3 pods needed
    origins = list(range(0, 8))
    oracle = bo.replay(y, origins, lambda k: max(y[k + 1], y[k + 2]), 600.0, 1, 12)
    assert oracle["shortage_replica_min"] == 0.0
    reactive = bo.replay(y, origins, lambda k: float("nan"), 600.0, 1, 12, reactive_lag_min=2.0)
    assert reactive["shortage_replica_min"] == 2 * 2.0      # two missing pods for two minutes, once
    assert reactive["short_min"] == 2.0


def test_replay_scale_down_waits_one_full_slot_below_the_ready_count():
    y = np.array([1800.0] * 3 + [600.0] * 6)
    origins = list(range(0, 7))
    r = bo.replay(y, origins, lambda k: max(y[k + 1], y[k + 2]), 600.0, 1, 12)
    # Ready 3 for slot 3 (reactive still sees 1800 at origin 2), 3 again for slot 4 (hold), then 1
    assert r["surplus_replica_min"] == pytest.approx(2 * 10 + 2 * 10)
    assert r["changes"] == 1


def test_replay_respects_the_replica_ceiling():
    y = np.full(6, 60000.0)
    r = bo.replay(y, list(range(4)), lambda k: 60000.0, 600.0, 1, 12)
    assert r["mean_ready"] == 12 and r["shortage_replica_min"] == 0.0   # requirement is capped too


def test_generic_q90_margin_matches_the_deployed_rule_for_e1():
    rates = bo.offered_rates(seed=4, warm_days=7, challenge_days=2)
    pts = bo.sampled_series(rates, seed=4)
    grid = se.Grid.from_points(pts)
    origins = list(range(7 * bo.SEASON - se.MARGIN_WINDOW_SLOTS, len(grid.y) - 2))
    leads = {}
    for k in origins:
        raw = se.components_at(grid, k, "parity")["raw"]
        leads[k] = max(raw[0], raw[1])
    mine = bo.q90_margins(grid.y, leads, origins)
    for k in (origins[se.MARGIN_WINDOW_SLOTS + 40], origins[se.MARGIN_WINDOW_SLOTS + 150], origins[-1]):
        assert k - se.MARGIN_WINDOW_SLOTS + 1 >= origins[0]      # the window is fully covered
        theirs, n = se.margin_at(grid, k, leads[k], "parity")
        assert mine[k] == pytest.approx(theirs, abs=1e-9)


def test_theta_reproduces_a_pure_daily_cycle():
    n = 8 * bo.SEASON
    slots = np.arange(n) % bo.SEASON
    y = 1000.0 + 500.0 * np.sin(2 * math.pi * slots / bo.SEASON)
    f = bo.theta_forecast(y, n - 1)
    truth = [1000.0 + 500.0 * math.sin(2 * math.pi * ((n - 1 + h) % bo.SEASON) / bo.SEASON) for h in range(1, 7)]
    assert np.allclose(f, truth, rtol=0.02)


def test_forecasts_at_returns_every_method_with_six_finite_steps_on_the_process():
    rates = bo.offered_rates(seed=5, warm_days=7, challenge_days=1)
    grid = se.Grid.from_points(bo.sampled_series(rates, seed=5))
    F = bo.forecasts_at(grid, 7 * bo.SEASON + 30, "all")
    assert set(F) == {"e1", "hw", "profile_ar", "profile7", "yesterday", "persistence",
                      "profile_ratio", "e1_bc", "theta", "median3", "mean3"}
    for name, f in F.items():
        assert len(f) == 6 and all(np.isfinite(f)), name


def test_profile_override_merges_nested_keys_without_touching_the_sealed_profile():
    p = bo.merged_profile({"noise": {"sigma": 0.08}, "shifts": {"max_abs_level": 0.25}})
    assert p["noise"]["sigma"] == 0.08 and p["noise"]["phi"] == cp.PROFILE["noise"]["phi"]
    assert p["shifts"]["max_abs_level"] == 0.25 and cp.PROFILE["shifts"]["max_abs_level"] != 0.25
    assert cp.PROFILE["noise"]["sigma"] != 0.08
    heavier = bo.multipliers_for(7, 288, p)
    assert max(abs(m - 1) for m in heavier) >= max(abs(m - 1) for m in bo.multipliers_for(7, 288))


def test_higher_quantile_and_shorter_window_margins_behave():
    rates = bo.offered_rates(seed=6, warm_days=7, challenge_days=2)
    grid = se.Grid.from_points(bo.sampled_series(rates, seed=6))
    origins = list(range(7 * bo.SEASON - se.MARGIN_WINDOW_SLOTS, len(grid.y) - 2))
    leads = {}
    for k in origins:
        raw = se.components_at(grid, k, "quant")["raw"]
        leads[k] = max(raw[0], raw[1])
    m80 = bo.q90_margins(grid.y, leads, origins, quantile=0.80)
    m90 = bo.q90_margins(grid.y, leads, origins)
    m95 = bo.q90_margins(grid.y, leads, origins, quantile=0.95)
    m12 = bo.q90_margins(grid.y, leads, origins, window=se.MARGIN_WINDOW_SLOTS // 2)
    late = origins[se.MARGIN_WINDOW_SLOTS + 200:]
    assert all(m80[k] <= m90[k] <= m95[k] for k in late)
    assert any(m12[k] != m90[k] for k in late)            # a shorter window really changes the margin
    assert all(0.0 <= m95[k] <= 0.8 * leads[k] for k in late)


def test_bias_correction_is_neutral_on_an_unbiased_series_and_clipped():
    rates = bo.offered_rates(seed=6, warm_days=7, challenge_days=1)
    grid = se.Grid.from_points(bo.sampled_series(rates, seed=6))
    k = 7 * bo.SEASON + 20
    raw = se.components_at(grid, k, "bc")["raw"]
    bc = bo.bias_corrected(grid, k, "bc", raw)
    ratios = [b / r for b, r in zip(bc, raw)]
    assert all(0.85 <= x <= 1.20 for x in ratios) and max(ratios) - min(ratios) < 1e-9


def test_report_prints_for_a_single_unaggregated_series(capsys):
    rates = bo.offered_rates(seed=8, warm_days=7, challenge_days=1)
    pts = bo.sampled_series(rates, seed=8)
    res, origins = bo.evaluate_series(pts, bo.DEFAULT_T0 + 7 * bo.SEASON * bo.SLOT, "single",
                                      methods=["e1", "persistence"], per_pod=600.0, min_r=1, max_r=12)
    bo.print_report(res, ["real"], 1)             # the --real path: no worst_shortage key
    out = capsys.readouterr().out
    assert "e1" in out and "reactive_only" in out and len(origins) == bo.SEASON - bo.LEAD_STEPS
