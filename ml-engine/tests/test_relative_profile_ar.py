"""Relative-residual profile-AR forecaster (arm R1): parity with the model lab's vectors and rules.

The vectors (tests/fixtures/relative_profile_ar_vectors.json) come from the lab's port package of
2026-09-29 (`2026-09-29-pr-ar-port/test-vectors.json`, reproduced by its standalone reference to
1e-6): three eight-day series on the ten-minute grid, five origins on the eighth day, the six
forecasts and the AR coefficients and row counts of every generation.
"""
import json
import math
import os
import sys
import time

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from models import relative_profile_ar as rpa  # noqa: E402
from models import seasonal_ensemble as se  # noqa: E402
from api.ensemble_experiment import EnsembleExperiment  # noqa: E402

V = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "relative_profile_ar_vectors.json")))


@pytest.fixture(autouse=True)
def clear_caches():
    rpa._CACHE.clear()
    se._CACHE.clear()
    yield
    rpa._CACHE.clear()
    se._CACHE.clear()


def grid_of(kind):
    values = V["series"][kind]["values"]
    return se.Grid.from_points([(V["t0"] + i * rpa.SLOT_SECONDS, float(v)) for i, v in enumerate(values)])


def test_vector_constants_match_the_port():
    c = V["constants"]
    assert (c["season"], c["steps"], c["refit_seconds"], c["profile_days"]) == (
        rpa.SEASON, rpa.STEPS, rpa.REFIT_SECONDS, rpa.PROFILE_DAYS)
    assert (c["ar_order"], c["ar_window_slots"], c["min_ar_rows"], c["rel_clip"]) == (
        rpa.AR_ORDER, rpa.AR_WINDOW_SLOTS, rpa.MIN_AR_ROWS, rpa.REL_CLIP)
    assert V["version"] == rpa.VERSION and V["slot_seconds"] == rpa.SLOT_SECONDS


@pytest.mark.parametrize("kind", ["A", "B", "C"])
def test_vectors_reproduced_to_1e6(kind):
    grid = grid_of(kind)
    s = V["series"][kind]
    for c in s["cases"]:
        out = rpa.raw_at(grid, c["origin_index"], f"vec-{kind}")
        gen = out["generation"]
        assert gen.boundary_ts == c["boundary_ts"] and out["stale_generation"] is False
        assert max(abs(a - b) for a, b in zip(out["raw"], c["forecast"])) < V["tolerance"]
        expect = s["generations"][str(c["boundary_ts"])]
        assert gen.ar_rows == expect["ar_rows"]
        assert max(abs(a - b) for a, b in zip(gen.ar_coef, expect["ar_coef"])) < V["tolerance"]


def test_serving_path_uses_the_latest_observation_as_origin_and_adds_its_own_margin():
    kind = "B"
    grid = grid_of(kind)
    c = V["series"][kind]["cases"][2]
    o = c["origin_index"]
    pts = [(grid.ts(i), float(grid.y[i])) for i in range(o + 1)]
    now = grid.ts(o) + 30
    out = rpa.forecast(pts, now, "serve-B", margin_quantile=0.9, margin_mode="relative")
    assert out["origin"] == se._iso(grid.ts(o)) and out["served_components"] == 1
    assert max(abs(a - b) for a, b in zip(out["raw"], c["forecast"])) < V["tolerance"]
    assert out["served"] == [v + out["margin"] for v in out["raw"]]
    assert out["margin"] >= 0 and out["margin_samples"] >= se.MARGIN_MIN_SAMPLES
    assert out["margin_mode"] == "relative" and out["margin_quantile"] == 0.9
    assert out["components"] == {"profile_ar_rel": out["raw"]}
    expect_rows = V["series"][kind]["generations"][str(c["boundary_ts"])]["ar_rows"]
    assert out["generation"]["forecaster"] == rpa.VERSION and out["generation"]["ar"]["rows"] == expect_rows
    # the margin is this forecaster's own q90 of relative lead errors, recomputed by hand
    lead = max(out["raw"][0], out["raw"][1])
    samples = []
    for j in range(max(0, o - se.MARGIN_WINDOW_SLOTS + 1), o - 1):
        rj = rpa.raw_at(grid, j, "serve-B")["raw"]
        lj = max(rj[0], rj[1])
        samples.append(max(grid.y[j + 1], grid.y[j + 2]) / lj - 1.0)
    expect = lead * float(np.clip(np.quantile(samples, 0.9), 0.0, se.MARGIN_CLIP_FRACTION))
    assert out["margin"] == pytest.approx(expect)


def test_margin_comes_from_this_forecaster_not_the_ensemble():
    grid = grid_of("C")
    o = V["series"]["C"]["cases"][3]["origin_index"]
    lead = 1000.0
    own, n_own = se.margin_at(grid, o, lead, "m", 0.9, "relative", raw_fn=rpa._raw_fn)
    ens, n_ens = se.margin_at(grid, o, lead, "m", 0.9, "relative")
    assert n_own >= se.MARGIN_MIN_SAMPLES and n_ens >= se.MARGIN_MIN_SAMPLES
    assert own != ens


def test_clip_applies_to_training_only():
    """A residual beyond +/- 50 % is clipped when it is a training row or lag, and NOT when it seeds
    the recursion at the origin: the fit on a +200 % residual equals the fit on exactly +50 %, while
    the forecast from a +200 % seed differs from the forecast from a +50 % seed."""
    grid = grid_of("B")
    o = V["series"]["B"]["cases"][1]["origin_index"]
    b = grid.index(rpa._boundary_of(grid.ts(o)))
    q = rpa.relative_residuals(grid.y, grid.filled(), b)
    spiked, clipped = q.copy(), q.copy()
    spiked[b - 100], clipped[b - 100] = 2.0, rpa.REL_CLIP
    assert np.array_equal(rpa.fit_generation(spiked, b)[0], rpa.fit_generation(clipped, b)[0])
    assert not np.array_equal(rpa.fit_generation(spiked, b)[0], rpa.fit_generation(q, b)[0])
    # the seed is unclipped: tripling the origin's observation moves the forecast beyond what a
    # +50 % residual could produce (the generation is refitted on the same boundary either way)
    f1 = rpa.raw_at(grid, o, "clip-1")["raw"]
    tripled = se.Grid(t0=grid.t0, y=grid.y.copy())
    tripled.y[o] *= 3.0
    f2 = rpa.raw_at(tripled, o, "clip-2")["raw"]
    p1 = rpa.profile_at(grid.y, o + 1)
    assert f2[0] != f1[0] and f2[0] > p1 * (1.0 + rpa.REL_CLIP)


def test_young_series_is_refused_and_a_stale_generation_serves_once():
    grid = grid_of("A")
    young = se.Grid(t0=grid.t0, y=grid.y[:200].copy())
    with pytest.raises(se.ForecastUnavailable):
        rpa.raw_at(young, 150, "young")                     # fewer than 36 AR rows at any boundary
    # every generation valid: no stale fallback
    o = V["series"]["A"]["cases"][4]["origin_index"]
    assert rpa.raw_at(grid, o, "fresh")["stale_generation"] is False
    # the latest boundary invalid: fall back once to the previous one, and say so
    calls = {"n": 0}
    real = rpa.fit_generation

    def failing_first(q, b):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("forced invalid generation")
        return real(q, b)

    rpa._CACHE.clear()
    rpa.fit_generation = failing_first
    try:
        out = rpa.raw_at(grid, o, "stale")
    finally:
        rpa.fit_generation = real
    assert out["stale_generation"] is True
    assert out["generation"].boundary_ts == rpa._boundary_of(grid.ts(o)) - rpa.REFIT_SECONDS


def test_generation_is_cached_by_boundary_and_fingerprint_and_the_fingerprint_binds_the_inputs():
    grid = grid_of("B")
    o = V["series"]["B"]["cases"][0]["origin_index"]
    g1 = rpa.raw_at(grid, o, "cache")["generation"]
    g2 = rpa.raw_at(grid, o + 5, "cache")["generation"]
    assert g1 is g2                                          # same boundary, same inputs: one fit
    changed = se.Grid(t0=grid.t0, y=grid.y.copy())
    b = grid.index(g1.boundary_ts)
    changed.y[b - 2000] += 1.0                               # inside the 14-day fingerprint window
    assert rpa.raw_at(changed, o, "cache")["generation"].fingerprint != g1.fingerprint
    outside = se.Grid(t0=grid.t0, y=grid.y.copy())
    outside.y[o + 3] += 1.0                                  # after the boundary: not read by the fit
    assert rpa.raw_at(outside, o, "cache")["generation"] is g1


def test_stale_input_is_refused():
    grid = grid_of("A")
    o = 1100
    pts = [(grid.ts(i), float(grid.y[i])) for i in range(o + 1)]
    with pytest.raises(se.ForecastUnavailable):
        rpa.forecast(pts, grid.ts(o) + rpa.MAX_ORIGIN_AGE_SECONDS + 1, "stale-input")


def test_experiment_config_declares_the_forecaster():
    base = {"id": "relative-profile-ar-rq90-v1", "application": "nginx-seasonal", "namespace": "demo",
            "source_application": "nginx-test", "source_namespace": "demo"}
    r1 = EnsembleExperiment.from_dict({**base, "forecaster": "relative-profile-ar", "margin_mode": "relative"})
    assert r1.module is rpa and r1.forecast_mode == "relative-profile-ar-rq90"
    assert r1.forecast_kwargs() == {"margin_quantile": 0.9, "margin_mode": "relative"}
    assert r1.provenance()["forecaster_version"] == rpa.VERSION
    assert r1.provenance()["forecaster"] == "relative-profile-ar"
    with pytest.raises(ValueError):
        EnsembleExperiment.from_dict({**base, "forecaster": "theta"})
    with pytest.raises(ValueError):                          # one component: no partial rule
        EnsembleExperiment.from_dict({**base, "forecaster": "relative-profile-ar", "partial_rule": "finite"})
    e1 = EnsembleExperiment.from_dict({**base, "id": "seasonal-ensemble-q90-v1", "application": "nginx-ensemble"})
    assert e1.forecaster == "seasonal-ensemble" and e1.module is se
    assert e1.forecast_kwargs() == {"margin_quantile": 0.9, "margin_mode": "absolute", "partial_rule": "refuse"}


def test_default_forecaster_leaves_the_live_e1_and_e2_config_hashes_unchanged():
    """Pinned before the `forecaster` field existed (seasonal-ensemble-1.2.0 settings, 2026-09-29)."""
    cfg = [{"application": "nginx-ensemble", "id": "seasonal-ensemble-q90-v1", "namespace": "demo",
            "source_application": "nginx-test", "source_namespace": "demo"},
           {"application": "nginx-ensemble-q95", "id": "seasonal-ensemble-q95-v1", "margin_quantile": 0.95,
            "namespace": "demo", "source_application": "nginx-test", "source_namespace": "demo"}]
    e1, e2 = EnsembleExperiment.parse_all(json.dumps(cfg))
    assert e1.config_sha256 == "891ca57abafe1f6453fed13fe85a8e43343f263d74e82cf00475becf599ed6c7"
    assert e2.config_sha256 == "c73c7e1b53c422353e49d6fc07be836ff61714409a218ad50c944e9ec5be5cda"
    assert e1.forecast_mode == "seasonal-ensemble-q90" and e2.forecast_mode == "seasonal-ensemble-q95"


@pytest.fixture
def api(monkeypatch):
    from fastapi.testclient import TestClient
    from api import main
    cfg = [{"id": "ensemble-q90-v1", "application": "nginx-ensemble", "namespace": "demo",
            "source_application": "nginx-test", "source_namespace": "demo"},
           {"id": "relative-profile-ar-rq90-v1", "application": "nginx-seasonal", "namespace": "demo",
            "source_application": "nginx-test", "source_namespace": "demo",
            "forecaster": "relative-profile-ar", "margin_mode": "relative"}]
    monkeypatch.setattr(main, "ensemble_experiments", EnsembleExperiment.parse_all(json.dumps(cfg)))
    grid = grid_of("B")
    wall = int(time.time())
    last = wall - wall % se.SLOT_SECONDS
    n = len(grid.y)
    pts = [(last - (n - 1 - i) * se.SLOT_SECONDS, float(grid.y[i])) for i in range(n)]
    seen = {}

    async def fake_fetch(application, namespace, metric_type, hours=168):
        seen.update(application=application, namespace=namespace, hours=hours)
        return [{"timestamp": se._iso(t), "value": v} for t, v in pts]

    monkeypatch.setattr(main, "fetch_metrics_from_vm", fake_fetch)
    return TestClient(main.app), seen, pts, grid


def test_api_serves_r1_with_its_own_forecaster_and_e1_unchanged(api, monkeypatch):
    from api import main
    client, seen, pts, grid = api
    messages = []
    real_info = main.logger.info
    monkeypatch.setattr(main.logger, "info", lambda msg, *a, **k: (messages.append(str(msg)), real_info(msg, *a, **k)))
    r = client.post("/predict", json={"application": "nginx-seasonal", "namespace": "demo",
                                      "metric_type": "requests", "horizon_minutes": 60})
    assert r.status_code == 200, r.text
    body = r.json()
    assert seen == {"application": "nginx-test", "namespace": "demo", "hours": 360}
    assert body["provenance"] == "relative-profile-ar"
    assert body["experiment"]["forecast_mode"] == "relative-profile-ar-rq90"
    assert body["experiment"]["forecaster_version"] == rpa.VERSION
    assert body["model_version"].startswith("relative-profile-ar-rq90-v1:") and rpa.VERSION in body["model_version"]
    raw, margin = body["ensemble"]["raw"], body["ensemble"]["margin"]
    assert body["predictions"] == [round(v + margin, 2) for v in raw]
    assert list(body["ensemble"]["components"]) == ["profile_ar_rel"] and "hw" not in body["ensemble"]
    assert body["ensemble"]["generation"]["forecaster"] == rpa.VERSION
    # the raw forecast equals the module's own answer on the same (masked, wall-clock) points, whose
    # 6-hourly boundaries differ from the vector grid's
    from data import gapfill
    mpts, _ = gapfill.apply_mask(pts, main.predictor.validity_mask, role="benchmark")
    g = se.Grid.from_points(mpts)
    direct = rpa.raw_at(g, int(np.flatnonzero(np.isfinite(g.y))[-1]), "demo/nginx-test")["raw"]
    assert max(abs(a - b) for a, b in zip(raw, direct)) < 1e-6
    r90 = client.post("/predict", json={"application": "nginx-ensemble", "namespace": "demo",
                                        "metric_type": "requests", "horizon_minutes": 60})
    assert r90.status_code == 200 and r90.json()["provenance"] == "seasonal-ensemble"
    assert set(r90.json()["ensemble"]["components"]) == {"hw", "profile_ar"} and "hw" in r90.json()["ensemble"]
    lines = [m for m in messages if m.startswith("ENSEMBLE_ISSUANCE ")]
    logged = [json.loads(l[len("ENSEMBLE_ISSUANCE "):]) for l in lines]
    assert {(d["experiment"], d["forecaster"]) for d in logged} == {
        ("relative-profile-ar-rq90-v1", "relative-profile-ar"), ("ensemble-q90-v1", "seasonal-ensemble")}
