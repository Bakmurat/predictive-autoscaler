"""Seasonal-ensemble forecaster (candidate A) and its opt-in API route (Task 03 U-21)."""
import json
import math
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from models import seasonal_ensemble as se  # noqa: E402

# The benchmark's k6 hourly profile (requests per minute by UTC hour).
PROFILE = {0: 250, 1: 200, 2: 150, 3: 150, 4: 200, 5: 300, 6: 750, 7: 1250, 8: 2000, 9: 3000,
           10: 3750, 11: 4250, 12: 4500, 13: 5000, 14: 5500, 15: 6000, 16: 5000, 17: 4250,
           18: 3000, 19: 2400, 20: 1000, 21: 600, 22: 400, 23: 300}
DAY0 = 1790035200  # a UTC midnight


def staircase(days=7, extra=60, noise=0.003, seed=1, t0=DAY0):
    """The benchmark's traffic on the 10-minute grid. The sample at hh:00 is the 1-minute rate
    ending at hh:00, so it still carries the previous hour's level."""
    rng = np.random.default_rng(seed)
    pts = []
    for i in range(days * se.SEASON + extra):
        t = t0 + i * se.SLOT_SECONDS
        hour = (t // 3600) % 24
        level = PROFILE[hour] if t % 3600 else PROFILE[(hour - 1) % 24]
        pts.append((t, level * (1 + noise * rng.standard_normal())))
    return pts


@pytest.fixture(autouse=True)
def clear_cache():
    se._CACHE.clear()
    yield
    se._CACHE.clear()


# ---------------------------------------------------------------------------------------------
# Holt-Winters component
# ---------------------------------------------------------------------------------------------

def test_heuristic_init_matches_statsmodels():
    init = pytest.importorskip("statsmodels.tsa.exponential_smoothing.initialization")
    y = np.array([v for _, v in staircase(days=5, extra=0)])
    l0, b0, s0 = se.heuristic_init(y, se.SEASON)
    ref_l, ref_b, ref_s = init._initialization_heuristic(y, trend="add", seasonal="add",
                                                          seasonal_periods=se.SEASON)
    assert l0 == pytest.approx(ref_l, rel=1e-12, abs=1e-9)
    assert b0 == pytest.approx(ref_b, rel=1e-12, abs=1e-9)
    np.testing.assert_allclose(s0, ref_s, rtol=1e-12, atol=1e-9)


def test_recursion_reproduces_statsmodels_fitted_values():
    hw = pytest.importorskip("statsmodels.tsa.holtwinters")
    y = np.array([v for _, v in staircase(days=5, extra=0)])
    res = hw.ExponentialSmoothing(y, trend="add", damped_trend=False, seasonal="add",
                                  seasonal_periods=se.SEASON, initialization_method="heuristic").fit(
        optimized=True, use_brute=False, method="L-BFGS-B", minimize_kwargs={"options": {"maxiter": 200}})
    p = res.params
    _, _, _, _, fitted = se.hw_run(y.tolist(), p["smoothing_level"], p["smoothing_trend"],
                                   p["smoothing_seasonal"], p["initial_level"], p["initial_trend"],
                                   p["initial_seasons"], collect=True)
    np.testing.assert_allclose(fitted, res.fittedvalues, rtol=1e-9, atol=1e-6)


def test_fit_is_at_least_as_good_as_statsmodels():
    hw = pytest.importorskip("statsmodels.tsa.holtwinters")
    y = np.array([v for _, v in staircase(days=5, extra=0, noise=0.02)])
    res = hw.ExponentialSmoothing(y, trend="add", damped_trend=False, seasonal="add",
                                  seasonal_periods=se.SEASON, initialization_method="heuristic").fit(
        optimized=True, use_brute=False, method="L-BFGS-B", minimize_kwargs={"options": {"maxiter": 200}})
    ours = se.fit_hw(y)
    assert ours.sse <= res.sse * 1.001


def test_fit_parameters_admissible_and_recorded():
    y = np.array([v for _, v in staircase(days=5, extra=0, noise=0.02)])
    fit = se.fit_hw(y)
    assert 0 <= fit.beta <= fit.alpha <= 1 and 0 <= fit.gamma <= 1 - fit.alpha
    assert fit.n_train == len(y) and len(fit.season) == se.SEASON and math.isfinite(fit.sse)


def test_short_history_is_refused():
    with pytest.raises(se.ForecastUnavailable):
        se.forecast(staircase(days=1, extra=10), staircase(days=1, extra=10)[-1][0] + 60)


# ---------------------------------------------------------------------------------------------
# Forecast behaviour
# ---------------------------------------------------------------------------------------------

def test_tracks_the_benchmark_staircase_and_anticipates_the_next_hour():
    pts = staircase(days=7, extra=60)  # origin at 09:50 on day 8 (index 7*144 + 59)
    out = se.forecast(pts, pts[-1][0] + 120)
    assert out["origin"].endswith("09:50:00Z")
    raw = out["raw"]
    assert raw[0] == pytest.approx(3000, rel=0.02)   # 10:00 sample still carries 09:xx
    assert raw[1] == pytest.approx(3750, rel=0.02)   # 10:10 is the next hour's level
    assert all(math.isfinite(v) for v in out["served"])
    assert out["served"] == [v + out["margin"] for v in raw]


def test_margin_is_q90_of_matured_lead_errors_and_nonnegative():
    pts = staircase(days=7, extra=100, noise=0.03, seed=5)
    grid = se.Grid.from_points(pts)
    origin = len(grid.y) - 1
    raw = se.components_at(grid, origin)["raw"]
    margin, n = se.margin_at(grid, origin, max(raw[0], raw[1]))
    samples = []
    for j in range(origin - se.MARGIN_WINDOW_SLOTS + 1, origin - 1):
        r = se.components_at(grid, j)["raw"]
        samples.append(max(grid.y[j + 1], grid.y[j + 2]) - max(r[0], r[1]))
    assert n == len(samples) >= se.MARGIN_MIN_SAMPLES
    expected = float(np.clip(np.quantile(samples, 0.9), 0, 0.8 * max(raw[0], raw[1])))
    assert margin == pytest.approx(expected)
    assert margin >= 0


def test_margin_is_zero_below_the_minimum_sample_count():
    pts = staircase(days=7, extra=60)
    grid = se.Grid.from_points(pts)
    # Remove observations so that fewer than 30 matured ticks remain in the last 24 hours.
    grid.y[-se.MARGIN_WINDOW_SLOTS:-20] = np.nan
    origin = len(grid.y) - 1
    margin, n = se.margin_at(grid, origin, 5000.0)
    assert n < se.MARGIN_MIN_SAMPLES and margin == 0.0


def test_no_future_leakage():
    """A forecast issued at t is identical whether or not later observations exist."""
    pts = staircase(days=7, extra=80, noise=0.02, seed=9)
    full = se.Grid.from_points(pts)
    for origin in (len(full.y) - 30, len(full.y) - 13):
        truncated = se.Grid.from_points(pts[: origin + 1])
        a = se.components_at(full, origin)
        se._CACHE.clear()
        b = se.components_at(truncated, origin)
        assert a["raw"] == b["raw"] and a["hw"] == b["hw"] and a["profile_ar"] == b["profile_ar"]


def test_restart_reproduces_the_same_forecast():
    pts = staircase(days=7, extra=70, noise=0.02, seed=3)
    now = pts[-1][0] + 60
    first = se.forecast(pts, now)
    se._CACHE.clear()  # a restarted process has no cache
    second = se.forecast(pts, now)
    assert first["served"] == second["served"] and first["generation"] == second["generation"]


def test_refits_on_absolute_six_hour_utc_boundaries():
    pts = staircase(days=7, extra=70)
    out = se.forecast(pts, pts[-1][0] + 60)
    boundary = out["generation"]["boundary"]
    assert boundary[11:] in ("00:00:00Z", "06:00:00Z", "12:00:00Z", "18:00:00Z")
    assert boundary <= out["origin"]


def test_stale_input_is_refused():
    pts = staircase(days=7, extra=60)
    with pytest.raises(se.ForecastUnavailable, match="older than"):
        se.forecast(pts, pts[-1][0] + se.MAX_ORIGIN_AGE_SECONDS + 1)


def test_missing_latest_slot_uses_the_last_observation_as_origin():
    pts = staircase(days=7, extra=60)
    out = se.forecast(pts[:-1], pts[-1][0] + 60)
    assert out["origin"] == se._iso(pts[-2][0])


def test_off_grid_points_dropped_and_conflicting_duplicates_rejected():
    pts = staircase(days=7, extra=60)
    grid = se.Grid.from_points(pts + [(pts[-1][0] + 7, 1.0)])
    assert grid.off_grid_dropped == 1
    with pytest.raises(ValueError, match="duplicate"):
        se.Grid.from_points(pts + [(pts[-1][0], pts[-1][1] + 1)])


def test_masked_gap_does_not_break_the_forecast():
    pts = staircase(days=7, extra=60, noise=0.01)
    gap = set(range(len(pts) - 400, len(pts) - 380))  # a 200-minute hole two days ago
    out = se.forecast([p for k, p in enumerate(pts) if k not in gap], pts[-1][0] + 60)
    assert all(math.isfinite(v) for v in out["served"])


def test_invalid_generation_falls_back_once_to_the_previous_boundary(monkeypatch):
    pts = staircase(days=7, extra=60)
    real = se.fit_hw
    calls = {"n": 0}

    def failing_first(y, m=se.SEASON):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("optimiser did not converge")
        return real(y, m)

    monkeypatch.setattr(se, "fit_hw", failing_first)
    out = se.forecast(pts, pts[-1][0] + 60)
    assert out["stale_generation"] is True


# ---------------------------------------------------------------------------------------------
# API route
# ---------------------------------------------------------------------------------------------

@pytest.fixture
def api(monkeypatch):
    from fastapi.testclient import TestClient
    from api import main
    from api.ensemble_experiment import EnsembleExperiment
    cfg = [{"id": "ensemble-q90-v1", "application": "nginx-ensemble", "namespace": "demo",
            "source_application": "nginx-test", "source_namespace": "demo"},
           {"id": "ensemble-q95-v1", "application": "nginx-ensemble-q95", "namespace": "demo",
            "source_application": "nginx-test", "source_namespace": "demo", "margin_quantile": 0.95}]
    monkeypatch.setattr(main, "ensemble_experiments", EnsembleExperiment.parse_all(json.dumps(cfg)))
    import time
    wall = int(time.time())
    last = wall - wall % se.SLOT_SECONDS
    n = 7 * se.SEASON + 60
    pts = staircase(days=7, extra=60, t0=last - (n - 1) * se.SLOT_SECONDS)
    seen = {}

    async def fake_fetch(application, namespace, metric_type, hours=168):
        seen.update(application=application, namespace=namespace, hours=hours)
        return [{"timestamp": se._iso(t), "value": v} for t, v in pts]

    monkeypatch.setattr(main, "fetch_metrics_from_vm", fake_fetch)
    return TestClient(main.app), seen, pts


def test_api_serves_the_ensemble_with_margin_for_the_configured_app(api):
    client, seen, pts = api
    r = client.post("/predict", json={"application": "nginx-ensemble", "namespace": "demo",
                                      "metric_type": "requests", "horizon_minutes": 60})
    assert r.status_code == 200, r.text
    body = r.json()
    assert seen == {"application": "nginx-test", "namespace": "demo", "hours": 360}
    assert len(body["predictions"]) == 6 and all(math.isfinite(v) for v in body["predictions"])
    raw, margin = body["ensemble"]["raw"], body["ensemble"]["margin"]
    assert body["predictions"] == [round(v + margin, 2) for v in raw]
    assert body["confidence"] >= 0.7          # never triggers the operator's confidence damping
    assert "components" not in body           # the operator records hybrid components only
    assert body["inference_input_end"] == se._iso(pts[-1][0])
    assert body["target_timestamps"][0] == se._iso(pts[-1][0] + 600)[:-1]
    assert len(body["artifact_sha256"]) == 64
    assert body["experiment"]["forecast_mode"] == "seasonal-ensemble-q90"


def test_api_refuses_caller_supplied_history(api):
    client, _, _ = api
    r = client.post("/predict", json={"application": "nginx-ensemble", "namespace": "demo",
                                      "metric_type": "requests", "metric_data": [{"timestamp": 1, "value": 1}]})
    assert r.status_code == 422


def test_api_refuses_other_horizons(api):
    client, _, _ = api
    r = client.post("/predict", json={"application": "nginx-ensemble", "namespace": "demo",
                                      "metric_type": "requests", "horizon_minutes": 30})
    assert r.status_code == 422


def test_ensemble_config_is_strict():
    from api.ensemble_experiment import EnsembleExperiment
    with pytest.raises(ValueError):
        EnsembleExperiment.parse("{}")
    with pytest.raises(ValueError):
        EnsembleExperiment.parse(json.dumps({"id": "x", "application": "a", "namespace": "d",
                                             "source_application": "a", "source_namespace": "d"}))

    with pytest.raises(ValueError):
        EnsembleExperiment.parse(json.dumps({"id": "x", "application": "a", "namespace": "d",
                                             "source_application": "b", "source_namespace": "d",
                                             "margin_quantile": 1.5}))
    with pytest.raises(ValueError):
        EnsembleExperiment.parse(json.dumps({"id": "x", "application": "a", "namespace": "d",
                                             "source_application": "b", "source_namespace": "d",
                                             "margin_quantile": "0.95"}))
    with pytest.raises(ValueError):              # duplicate application across the list
        EnsembleExperiment.parse_all(json.dumps([
            {"id": "x", "application": "a", "namespace": "d", "source_application": "b", "source_namespace": "d"},
            {"id": "y", "application": "a", "namespace": "d", "source_application": "b", "source_namespace": "d"}]))
    with pytest.raises(ValueError):
        EnsembleExperiment.parse_all("[]")


def test_two_experiments_parse_with_their_own_quantiles_and_provenance():
    from api.ensemble_experiment import EnsembleExperiment
    exps = EnsembleExperiment.parse_all(json.dumps([
        {"id": "e1", "application": "a", "namespace": "d", "source_application": "s", "source_namespace": "d"},
        {"id": "e2", "application": "b", "namespace": "d", "source_application": "s", "source_namespace": "d",
         "margin_quantile": 0.95}]))
    assert [e.margin_quantile for e in exps] == [0.90, 0.95]
    assert [e.forecast_mode for e in exps] == ["seasonal-ensemble-q90", "seasonal-ensemble-q95"]
    assert exps[0].config_sha256 != exps[1].config_sha256
    assert exps[0].provenance()["margin_quantile"] == 0.90 and exps[1].provenance()["margin_quantile"] == 0.95
    one = EnsembleExperiment.parse_all(json.dumps({"id": "e1", "application": "a", "namespace": "d",
                                                   "source_application": "s", "source_namespace": "d"}))
    assert len(one) == 1 and one[0] == exps[0]


def test_q95_margin_is_at_least_the_q90_margin_and_recorded():
    pts = staircase(days=7, extra=60, noise=0.02, seed=3)
    now = pts[-1][0]
    f90 = se.forecast(pts, now, "q")
    f95 = se.forecast(pts, now, "q", margin_quantile=0.95)
    assert f90["raw"] == f95["raw"]                       # same forecaster, only the margin differs
    assert f95["margin"] >= f90["margin"] > 0
    assert f90["margin_quantile"] == 0.90 and f95["margin_quantile"] == 0.95
    assert f90["settings"]["margin_quantile"] == 0.90 and f95["settings"]["margin_quantile"] == 0.95
    assert f95["served"] == [v + f95["margin"] for v in f95["raw"]]
    with pytest.raises(ValueError):
        se.forecast(pts, now, "q", margin_quantile=1.0)


def test_api_serves_each_ensemble_app_with_its_own_quantile(api):
    client, seen, pts = api
    r90 = client.post("/predict", json={"application": "nginx-ensemble", "namespace": "demo",
                                        "metric_type": "requests", "horizon_minutes": 60})
    r95 = client.post("/predict", json={"application": "nginx-ensemble-q95", "namespace": "demo",
                                        "metric_type": "requests", "horizon_minutes": 60})
    assert r90.status_code == 200 and r95.status_code == 200, (r90.text, r95.text)
    b90, b95 = r90.json(), r95.json()
    assert b90["experiment"]["forecast_mode"] == "seasonal-ensemble-q90"
    assert b95["experiment"]["forecast_mode"] == "seasonal-ensemble-q95"
    assert b95["experiment"]["margin_quantile"] == 0.95 and b95["experiment"]["id"] == "ensemble-q95-v1"
    assert b90["ensemble"]["raw"] == b95["ensemble"]["raw"]
    assert b95["ensemble"]["margin"] >= b90["ensemble"]["margin"]
    assert b95["ensemble"]["settings"]["margin_quantile"] == 0.95
    assert b95["model_version"].startswith("ensemble-q95-v1:") and "seasonal-ensemble-1.1.0@" in b95["model_version"]
    assert b95["model_name"] == "ensemble_nginx-ensemble-q95_requests"
    assert seen["application"] == "nginx-test"                # both read the shared source history
