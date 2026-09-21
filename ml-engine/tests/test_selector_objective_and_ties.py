"""The selector's two broken promises, and its scoring objective.

Codex C-96 / D-121 -- ONE EXTERNAL OBJECTIVE. The parameter sweep ranked each setting using
that setting's own horizon weights, so a setting that scored only the two nearest steps was
compared against a setting that scored all six. That ranks the difficulty of the horizon, not
the quality of the forecast. The control blend had the mirror fault: chosen under `uniform`,
reported under `lead2`.

Codex C-97 / D-122 -- TWO IMPLEMENTATION PROMISES.
  (a) "the incumbent wins an exact tie": it did not. The scan ran in canonical order and
      replaced the best only on a strict improvement, so the first arm scanned won every tie.
  (b) "no new failures": the selector's degradation and failure counts were hardcoded to
      zero, so the S3 gate could not see a failure the selector had itself served.
"""

import math
import os
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "eval"))

import selector as sel  # noqa: E402

STEPS = sel.STEPS_AHEAD
A, B, C = sel.CANDIDATES          # seasonal_pattern, trend_adaptive, lagged_linear


# --------------------------------------------------------------------------- fixtures
def make_run(forecasts, truth, scenario="repeating", seed=1):
    """A Run carrying only what the scoring and selection paths read."""
    n = len(truth)
    r = sel.Run(scenario=scenario, seed=seed, start=sel.datetime(2026, 3, 1),
                series=None, origins=list(range(n)), target_rpm=100.0)
    r.forecasts = {a: np.asarray(f, dtype=float) for a, f in forecasts.items()}
    r.truth = np.asarray(truth, dtype=float)
    r.degraded_mask = {a: np.zeros(n, dtype=bool) for a in r.forecasts}
    r.failure_mask = {a: np.zeros(n, dtype=bool) for a in r.forecasts}
    r.degraded = {a: 0 for a in r.forecasts}
    r.failures = {a: 0 for a in r.forecasts}
    return r


def flat_costs(values_per_arm, n):
    return {a: np.full(n, v, dtype=float) for a, v in values_per_arm.items()}


def params(**kw):
    base = dict(half_life=6, weights="uniform", min_obs=1, switch_penalty=0.0,
                default_arm=B, blend_weight=0.5)
    base.update(kw)
    return sel.SelectorParams(**base)


# ============================================================ C-97 (a): tie-breaking
def test_an_exact_penalised_cost_tie_keeps_the_incumbent():
    """Every arm equally good, incumbent B: a tie is not a reason to move."""
    n = 40
    costs = flat_costs({A: 10.0, B: 10.0, C: 10.0}, n)
    res = sel.run_selector(costs, params(default_arm=B, switch_penalty=0.0))
    assert res["switches"] == 0, "an exact tie switched away from the incumbent"
    assert set(res["served"]) == {B}


def test_the_tie_is_broken_for_each_possible_incumbent():
    n = 40
    costs = flat_costs({A: 7.5, B: 7.5, C: 7.5}, n)
    for incumbent in sel.CANDIDATES:
        res = sel.run_selector(costs, params(default_arm=incumbent))
        assert res["switches"] == 0 and set(res["served"]) == {incumbent}


def test_a_tie_created_by_the_switching_penalty_keeps_the_incumbent():
    """The challenger is genuinely cheaper, by EXACTLY the switching penalty. The penalised
    costs tie, and a penalty that loses the tie it creates is not a penalty."""
    n = 40
    penalty = 2.0
    costs = flat_costs({A: 10.0 - penalty, B: 10.0, C: 10.0}, n)
    res = sel.run_selector(costs, params(default_arm=B, switch_penalty=penalty))
    assert res["switches"] == 0, "a tie on penalised cost switched away from the incumbent"
    assert set(res["served"]) == {B}


def test_a_challenger_that_beats_the_penalty_still_wins():
    """Guard against over-fixing: the incumbent must not become unbeatable."""
    n = 40
    costs = flat_costs({A: 10.0 - 2.001, B: 10.0, C: 10.0}, n)
    res = sel.run_selector(costs, params(default_arm=B, switch_penalty=2.0))
    assert res["served"][-1] == A and res["switches"] == 1


def test_among_non_incumbents_the_canonical_order_still_decides():
    n = 40
    costs = flat_costs({A: 5.0, B: 10.0, C: 5.0}, n)
    res = sel.run_selector(costs, params(default_arm=B))
    assert res["served"][-1] == A, "canonical order must still break non-incumbent ties"


# ============================================================ C-97 (b): failure inheritance
def test_the_selector_inherits_the_failures_of_the_arm_it_served():
    """A deliberately failing candidate. Pre-fix the selector reported zero regardless."""
    n = 12
    truth = np.zeros((n, STEPS))
    fc = {a: np.zeros((n, STEPS)) for a in sel.CANDIDATES}
    r = make_run(fc, truth)
    r.failure_mask[C][[3, 4, 9]] = True       # lagged_linear failed at three origins
    r.degraded_mask[C][[3, 4, 9]] = True
    served = [C] * n
    sel.attach_selector_arm(r, served)
    assert r.failures["selector"] == 3, "the selector reported none of its served failures"
    assert r.degraded["selector"] == 3


def test_the_selector_inherits_only_what_it_actually_served():
    n = 12
    truth = np.zeros((n, STEPS))
    fc = {a: np.zeros((n, STEPS)) for a in sel.CANDIDATES}
    r = make_run(fc, truth)
    r.failure_mask[C][[3, 4, 9]] = True
    served = [C if k == 3 else A for k in range(n)]   # served the failing arm once
    sel.attach_selector_arm(r, served)
    assert r.failures["selector"] == 1


def test_the_failure_gate_can_now_fire():
    """The S3 promise: 'no new failures'. With a failing candidate the selector served,
    the gate expression must be able to evaluate False."""
    n = 12
    truth = np.zeros((n, STEPS))
    fc = {a: np.zeros((n, STEPS)) for a in sel.CANDIDATES}
    r = make_run(fc, truth)
    r.failure_mask[C][:] = True
    r.failures[C] = n
    sel.add_blend(r, 0.5)
    sel.attach_selector_arm(r, [C] * n)
    fixed_arms = list(sel.CANDIDATES) + [sel.CONTROL_ARM]
    worst_fixed = max(r.failures[a] for a in fixed_arms)
    # Every fixed arm except lagged_linear is clean, so serving lagged_linear everywhere is
    # not "more than the worst fixed arm" -- but the count is real and visible.
    assert r.failures["selector"] == n == worst_fixed
    r.failures["selector"] += 1              # the gate must be able to read False
    assert not (r.failures["selector"] <= worst_fixed)


def test_the_control_blend_inherits_its_inputs_failures():
    n = 10
    truth = np.zeros((n, STEPS))
    fc = {a: np.zeros((n, STEPS)) for a in sel.CANDIDATES}
    r = make_run(fc, truth)
    r.failure_mask[A][[1, 2]] = True
    r.failure_mask[B][[2, 7]] = True
    r.degraded_mask[A][[1, 2]] = True
    r.degraded_mask[B][[2, 7]] = True
    sel.add_blend(r, 0.5)
    assert r.failures[sel.CONTROL_ARM] == 3, "a mixture of a repaired forecast is repaired"
    assert r.degraded[sel.CONTROL_ARM] == 3


# ============================================================ C-96: one external objective
def horizon_split_run(n=60, seed=0):
    """A fixture where the arms' ranking DEPENDS on the horizon weighting.

    seasonal_pattern is exact on the first two steps and badly wrong on the last four;
    trend_adaptive is uniformly mediocre. Under `lead2` the first arm looks perfect; under
    `uniform` it is the worst. Any objective confusion therefore shows up as a large number.
    """
    rng = np.random.default_rng(seed)
    truth = rng.uniform(900.0, 1100.0, size=(n, STEPS))
    near_perfect = truth.copy()
    near_perfect[:, 2:] += 600.0                   # ruinous on the far steps only
    mediocre = truth + 60.0
    plain = truth + 80.0
    return make_run({A: near_perfect, B: mediocre, C: plain}, truth)


def only_grid(monkeypatch, weights):
    """Collapse the sweep to a single setting whose internal weighting is `weights`."""
    monkeypatch.setattr(sel, "GRID_WEIGHTS", (weights,))
    monkeypatch.setattr(sel, "GRID_HALF_LIFE", (6,))
    monkeypatch.setattr(sel, "GRID_MIN_OBS", (1,))
    monkeypatch.setattr(sel, "GRID_SWITCH_FRAC", (0.0,))
    monkeypatch.setattr(sel, "GRID_DEFAULT_ARM", (B,))
    monkeypatch.setattr(sel, "GRID_BLEND_WEIGHT", (0.5,))


def served_cost(run, p, weights_name):
    costs = sel.per_origin_costs(run, sel.HORIZON_WEIGHTS[p.weights], sel.CANDIDATES)
    served = sel.run_selector(costs, p)["served"]
    scored = sel.per_origin_costs(run, sel.HORIZON_WEIGHTS[weights_name], sel.CANDIDATES)
    return float(np.mean([scored[a][k] for k, a in enumerate(served)]))


def test_a_setting_is_ranked_by_the_external_objective_not_its_own_weights(monkeypatch):
    run = horizon_split_run()
    only_grid(monkeypatch, "lead2")
    best, sweep = sel.preselect([run], log=lambda *_a, **_k: None)
    assert best.weights == "lead2"
    reported = sweep["top12"][0]["mean_cost"]
    external = served_cost(run, best, sel.EVAL_OBJECTIVE)
    internal = served_cost(run, best, "lead2")
    assert not math.isclose(external, internal, rel_tol=0.05), "fixture does not separate them"
    assert reported == pytest.approx(external, rel=1e-3), (
        f"ranked under its own weights ({internal:.1f}) instead of the external "
        f"objective ({external:.1f})")
    assert sweep["external_objective"] == sel.EVAL_OBJECTIVE


def test_settings_with_different_internal_weights_are_on_one_comparable_scale(monkeypatch):
    """The point of the repair: two settings that score different horizons internally must
    still come back on the SAME external scale, so the sweep compares forecasts."""
    scores = {}
    for wname in ("lead2", "uniform"):
        run = horizon_split_run()
        mp = pytest.MonkeyPatch()
        try:
            only_grid(mp, wname)
            best, sweep = sel.preselect([run], log=lambda *_a, **_k: None)
            scores[wname] = (sweep["top12"][0]["mean_cost"],
                             served_cost(run, best, sel.EVAL_OBJECTIVE))
        finally:
            mp.undo()
    for wname, (reported, external) in scores.items():
        assert reported == pytest.approx(external, rel=1e-3), wname
    # The lead2 selector really does serve a worse forecaster -- the repair does not hide it.
    assert scores["lead2"][0] > scores["uniform"][0]


def test_the_control_blend_is_chosen_under_the_objective_it_is_reported_under(monkeypatch):
    run = horizon_split_run()
    monkeypatch.setattr(sel, "GRID_WEIGHTS", ("lead2",))
    monkeypatch.setattr(sel, "GRID_HALF_LIFE", (6,))
    monkeypatch.setattr(sel, "GRID_MIN_OBS", (1,))
    monkeypatch.setattr(sel, "GRID_SWITCH_FRAC", (0.0,))
    monkeypatch.setattr(sel, "GRID_DEFAULT_ARM", (B,))
    monkeypatch.setattr(sel, "GRID_BLEND_WEIGHT", (0.25, 0.5, 0.75))
    best, sweep = sel.preselect([run], log=lambda *_a, **_k: None)

    direct = {}
    for bw in (0.25, 0.5, 0.75):
        mix = bw * run.forecasts[A] + (1 - bw) * run.forecasts[B]
        direct[bw] = float(np.mean([sel.origin_cost(mix[k], run.truth[k], sel.eval_weights())
                                    for k in range(len(run.origins))]))
    assert best.blend_weight == min(direct, key=direct.get)
    for bw, v in direct.items():
        assert sweep["constant_blend_weight_scores"][str(bw)] == pytest.approx(v, abs=0.01)


# ============================================================ end to end through evaluate_test
def tiny_runs(seed=0, n=48):
    """One small run per scenario, so evaluate_test's per-scenario block can be exercised."""
    rng = np.random.default_rng(seed)
    runs = []
    for s_i, sc in enumerate(sel.SCENARIOS):
        truth = rng.uniform(900.0, 1100.0, size=(n, STEPS))
        fc = {A: truth + 40.0, B: truth + 60.0, C: truth + 80.0}
        r = make_run(fc, truth, scenario=sc, seed=11 + s_i)
        # a real DatetimeIndex: evaluate_test blocks on the calendar day of each origin
        idx = pd.date_range("2026-06-07", periods=n + STEPS + 1, freq="10min")
        r.series = pd.Series(np.concatenate([truth[:, 0], truth[-1]]), index=idx[:n + STEPS])
        r.origins = list(range(n))
        runs.append(r)
    return runs


def test_evaluate_test_runs_end_to_end_and_reports_one_objective(monkeypatch):
    monkeypatch.setattr(sel, "BOOTSTRAP_DRAWS", 50)
    runs = tiny_runs()
    p = params(weights="lead2", min_obs=1, default_arm=B)
    out = sel.evaluate_test(runs, p, use_replay=False, log=lambda *_a, **_k: None)

    assert out["external_objective"] == sel.EVAL_OBJECTIVE
    # the reported cost of a FIXED arm must not depend on the selector's internal weighting
    p2 = params(weights="uniform", min_obs=1, default_arm=B)
    out2 = sel.evaluate_test(tiny_runs(), p2, use_replay=False, log=lambda *_a, **_k: None)
    for arm in sel.CANDIDATES:
        assert out["pooled"][arm]["cost_proxy_mean"] == pytest.approx(
            out2["pooled"][arm]["cost_proxy_mean"], rel=1e-9), (
            f"{arm}'s reported cost moved when only the selector's INTERNAL weighting changed")
    assert out["verdict"] in ("ADOPT", "KEEP THE FIXED BASELINE")


def test_evaluate_test_reports_inherited_selector_failures(monkeypatch):
    """A deliberately failing candidate, driven all the way through the reported tables."""
    monkeypatch.setattr(sel, "BOOTSTRAP_DRAWS", 50)
    runs = tiny_runs(seed=1)
    for r in runs:
        r.failure_mask[A][:] = True          # the arm the selector will serve (cheapest)
        r.degraded_mask[A][:] = True
        r.failures[A] = len(r.origins)
        r.degraded[A] = len(r.origins)
    p = params(min_obs=1, default_arm=A)
    out = sel.evaluate_test(runs, p, use_replay=False, log=lambda *_a, **_k: None)
    for r in runs:
        entry = out["per_run"][r.key]["arms"]
        assert entry["selector"]["forecast_failures"] == len(r.origins), (
            "the selector still reports zero failures for an arm it served everywhere")
        assert entry[sel.CONTROL_ARM]["forecast_failures"] == len(r.origins), (
            "the control blend still reports zero failures for a failed input")
