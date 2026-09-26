"""Only finite observations count as seasonal support, through model and HTTP."""
from datetime import timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from test_api_availability import fitted, daily_series, NETWORK_VALUE
from test_pattern_availability import model, ORIGIN, series_ending, PER_DAY
from test_seasonal_experiment import experiment  # noqa: F401


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_invalid_newest_day_uses_older_finite_support(bad):
    target = pd.Timestamp(ORIGIN) + timedelta(minutes=10)
    newest, older = target - timedelta(days=1), target - timedelta(days=2)
    history = pd.Series([bad, 123.0], index=[newest, older])
    m = model()
    values, source = m._pattern_forecast(ORIGIN, 1, history, 70)
    np.testing.assert_array_equal(values, [123.0])
    assert source == "seasonal_history"
    assert m.last_pattern_per_step[0] == {
        "step": 1, "target_at": target.isoformat(), "available": True,
        "support": 1, "matched_source_timestamps": [older.isoformat()]}


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_all_nonfinite_support_is_unavailable(bad):
    history = series_ending(ORIGIN, PER_DAY * 3)
    history[:] = bad
    m = model()
    values, source = m._pattern_forecast(ORIGIN, 6, history, 70)
    assert values is None
    assert source == "no_matching_history"
    assert all(not r["available"] and r["support"] == 0
               and r["matched_source_timestamps"] == [] for r in m.last_pattern_per_step)


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_partial_nonfinite_step_falls_back_to_network(bad):
    finite = daily_series(3, ORIGIN)
    history = finite.copy()
    for days in (1, 2, 3):
        history.loc[pd.Timestamp(ORIGIN) + timedelta(minutes=10, days=-days)] = bad
    m = fitted(finite)
    result = m.predict(steps_ahead=6, origin=ORIGIN, seasonal_history=history)
    comp = result["components"]
    assert comp["pattern_available_per_step"] == [False, True, True, True, True, True]
    assert comp["pattern_weights"][0] == 0
    assert comp["pattern_per_step"][0]["support"] == 0
    assert comp["pattern_per_step"][0]["matched_source_timestamps"] == []
    assert result["predictions"][0] == pytest.approx(NETWORK_VALUE)
    assert np.isfinite(result["predictions"]).all()


def test_invalid_exact_match_does_not_borrow_nearby_timestamp():
    target = pd.Timestamp(ORIGIN) + timedelta(minutes=10)
    want = target - timedelta(days=1)
    history = pd.Series([np.nan, 999.0], index=[want, want + timedelta(minutes=1)])
    m = model()
    values, source = m._pattern_forecast(ORIGIN, 1, history, 70)
    assert values is None
    assert source == "no_matching_history"
    assert m.last_pattern_per_step[0]["support"] == 0


@pytest.mark.parametrize("bad", ["nan", "inf", "-inf"])
def test_http_older_nonfinite_support_keeps_finite_forecast(experiment, monkeypatch, bad):
    main, predictor, m, data = experiment
    # Leave the recent inference day finite. Poison only the second day's support,
    # simulating Prometheus's string-form nonfinite sample at the fetch boundary.
    origin = pd.Timestamp(data[-1]["timestamp"])
    want = origin + timedelta(minutes=10, days=-2)
    for point in data:
        if pd.Timestamp(point["timestamp"]) == want:
            point["value"] = bad
    async def fetch(*args):
        return data
    monkeypatch.setattr(main, "fetch_metrics_from_vm", fetch)
    client = TestClient(main.app)
    for application in ("nginx-test", "nginx-seasonal"):
        response = client.post("/predict", json={"application": application,
            "namespace": "demo", "metric_type": "requests", "horizon_minutes": 60})
        assert response.status_code == 200, response.text
        body = response.json()
        assert np.isfinite(body["predictions"]).all()
        support = body["components"]["pattern_per_step"][0]
        assert support["support"] == 2
        assert all(pd.Timestamp(t) != want.tz_convert(None)
                   for t in support["matched_source_timestamps"])


def test_http_missing_step_uses_network_but_seasonal_arm_refuses(experiment, monkeypatch):
    main, predictor, m, data = experiment
    origin = pd.Timestamp(data[-1]["timestamp"])
    missing = {origin + timedelta(minutes=10, days=-days) for days in (1, 2, 3)}
    for point in data:
        if pd.Timestamp(point["timestamp"]) in missing:
            point["value"] = "nan"
    async def fetch(*args):
        return data
    monkeypatch.setattr(main, "fetch_metrics_from_vm", fetch)
    client = TestClient(main.app)
    request = {"application": "nginx-test", "namespace": "demo",
               "metric_type": "requests", "horizon_minutes": 60}
    response = client.post("/predict", json=request)
    assert response.status_code == 200, response.text
    body = response.json()
    assert np.isfinite(body["predictions"]).all()
    assert body["predictions"][0] == pytest.approx(NETWORK_VALUE)
    comp = body["components"]
    assert comp["pattern_weights"][0] == 0
    assert comp["pattern_per_step"][0]["support"] == 0
    assert comp["pattern_available_per_step"] == [False, True, True, True, True, True]
    response = client.post("/predict", json={**request, "application": "nginx-seasonal"})
    assert response.status_code == 422
    assert "incomplete seasonal support" in response.json()["detail"]
