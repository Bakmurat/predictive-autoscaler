"""Opt-in shared-input seasonal arm: isolation, provenance and refusal boundaries."""
import copy
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent))
from test_api_availability import daily_series, fitted

CONFIG = {"id": "seasonal-pattern-v1", "application": "nginx-seasonal",
          "namespace": "demo", "source_application": "nginx-test", "source_namespace": "demo"}


@pytest.fixture
def experiment(monkeypatch):
    from api import main
    # The environment is the public configuration surface; construct a fresh predictor.
    monkeypatch.setenv("SEASONAL_EXPERIMENT", json.dumps(CONFIG))
    monkeypatch.setattr(main.LSTMPredictor, "_load_pretrained_models", lambda self: None)
    monkeypatch.delenv("MODEL_DIR", raising=False)
    predictor = main.LSTMPredictor()
    end = datetime.utcnow().replace(second=0, microsecond=0)
    end = end.replace(minute=end.minute // 10 * 10)
    series = daily_series(3, end)
    model = fitted(series)
    predictor.trained_models["nginx-test_requests"] = model
    predictor.model_meta["nginx-test_requests"] = {"artifact_sha256": "a" * 64}
    predictor.model_train_times["nginx-test_requests"] = end
    monkeypatch.setattr(predictor, "_check_and_reload_model", lambda key: None)
    monkeypatch.setattr(main, "predictor", predictor)
    monkeypatch.setattr(main, "resolve_floor_mape", lambda *args: 37.0 if args[0] == "nginx-seasonal" else 0.0)
    data = [{"timestamp": t.isoformat() + "Z", "value": float(v)} for t, v in series.items()]
    return main, predictor, model, data


def test_seasonal_uses_source_pattern_without_mutating_baseline(experiment, monkeypatch):
    main, predictor, model, data = experiment
    before = predictor.predict("nginx-test", data, 60, "requests", "demo")
    state = copy.deepcopy({k: v for k, v in vars(model).items() if k not in ("model", "scaler")})
    def no_reload(key):
        pytest.fail("seasonal route must not trigger a source checkpoint reload")
    monkeypatch.setattr(predictor, "_check_and_reload_model", no_reload)
    seasonal = predictor.predict("nginx-seasonal", data, 60, "requests", "demo")
    assert seasonal["predictions"] == [round(v, 2) for v in seasonal["components"]["pattern"]]
    assert seasonal["components"]["pattern_weights"] == [1.0] * 6
    assert seasonal["artifact_sha256"] == "a" * 64
    assert "seasonal-pattern-v1" in seasonal["model_version"]
    assert seasonal["experiment"]["source_application"] == "nginx-test"
    assert len(seasonal["experiment"]["config_sha256"]) == 64
    assert set(vars(model)) - {"model", "scaler"} == set(state)
    for k, v in state.items():
        if isinstance(v, np.ndarray):
            np.testing.assert_array_equal(getattr(model, k), v)
        elif hasattr(v, "equals"):
            assert v.equals(getattr(model, k)), k
        else:
            assert getattr(model, k) == v, k
    monkeypatch.setattr(predictor, "_check_and_reload_model", lambda key: None)
    after = predictor.predict("nginx-test", data, 60, "requests", "demo")
    for field in ("predictions", "components", "confidence", "model_version", "artifact_sha256"):
        assert before[field] == after[field], field
    assert "experiment" not in after


def test_seasonal_refuses_partial_pattern_even_with_finite_network(experiment, monkeypatch):
    main, predictor, model, data = experiment
    original = type(model)._pattern_forecast
    def partial(self, *args, **kwargs):
        values, source = original(self, *args, **kwargs)
        values[0] = np.nan
        return values, source
    monkeypatch.setattr(type(model), "_pattern_forecast", partial)
    with pytest.raises(HTTPException) as exc:
        predictor.predict("nginx-seasonal", data, 60, "requests", "demo")
    assert exc.value.status_code == 422
    assert "seasonal" in str(exc.value.detail)
    async def fetch(*args):
        return data
    monkeypatch.setattr(main, "fetch_metrics_from_vm", fetch)
    response = TestClient(main.app).post("/predict", json={
        "application": "nginx-seasonal", "namespace": "demo", "metric_type": "requests"})
    assert response.status_code == 422, response.text
    assert "seasonal" in response.json()["detail"]


def test_endpoint_fetches_source_and_tracks_separate_target(experiment, monkeypatch):
    main, predictor, model, data = experiment
    fetches, observations = [], []
    async def fetch(application, namespace, metric_type):
        fetches.append((application, namespace, metric_type))
        return data
    def matured(application, namespace, metric_type, at, **kwargs):
        observations.append((application, namespace, metric_type, at))
        return []
    monkeypatch.setattr(main, "fetch_metrics_from_vm", fetch)
    monkeypatch.setattr(main.accuracy_tracker, "take_matured", matured)
    main.accuracy_tracker.pending.clear()
    client = TestClient(main.app)
    body = {"application": "nginx-seasonal", "namespace": "demo", "metric_type": "requests"}
    response = client.post("/predict", json=body)
    assert response.status_code == 200, response.text
    assert fetches == [("nginx-test", "demo", "requests")]
    assert observations[0][:3] == ("nginx-seasonal", "demo", "requests")
    assert observations[0][3].isoformat().startswith(data[-1]["timestamp"].removesuffix("Z"))
    assert all(k[:2] == ("nginx-seasonal", "demo") for k in main.accuracy_tracker.pending)
    assert client.post("/predict", json={**body, "metric_data": data}).status_code == 422


@pytest.mark.parametrize("missing", ["model", "hash"])
def test_missing_source_refuses_without_mutation(experiment, missing):
    _, predictor, model, data = experiment
    if missing == "model":
        predictor.trained_models.clear()
    else:
        predictor.model_meta.clear()
    before = model.last_sequence.copy()
    with pytest.raises(HTTPException) as exc:
        predictor.predict("nginx-seasonal", data, 60, "requests", "demo")
    assert exc.value.status_code == 422
    np.testing.assert_array_equal(model.last_sequence, before)
    assert not hasattr(model, "pattern_weight_override")


def test_network_failure_does_not_change_seasonal_forecast_or_confidence(experiment):
    _, predictor, model, data = experiment
    good = predictor.predict("nginx-seasonal", data, 60, "requests", "demo")
    model.model.raises = RuntimeError("network unavailable")
    failed = predictor.predict("nginx-seasonal", data, 60, "requests", "demo")
    assert failed["components"]["network_failed"]
    assert failed["predictions"] == good["predictions"]
    assert failed["confidence"] == good["confidence"]


def test_invalid_config_fails_predictor_startup(monkeypatch):
    from api.main import LSTMPredictor
    monkeypatch.setenv("SEASONAL_EXPERIMENT", "{}")
    with pytest.raises(ValueError, match="SEASONAL_EXPERIMENT"):
        LSTMPredictor()


@pytest.mark.parametrize("raw", ["{}", "null", "[]", "", "{", json.dumps({**CONFIG, "typo": 1}),
                                     json.dumps({**CONFIG, "application": "Bad Name"}),
                                     json.dumps({**CONFIG, "source_application": "nginx-seasonal"})])
def test_invalid_optin_fails_closed(monkeypatch, raw):
    from api.seasonal_experiment import SeasonalExperiment
    with pytest.raises(ValueError):
        SeasonalExperiment.parse(raw)


def test_config_identity_is_canonical_and_namespace_scoped():
    from api.seasonal_experiment import SeasonalExperiment
    a = SeasonalExperiment.parse(json.dumps(CONFIG))
    b = SeasonalExperiment.parse(json.dumps(dict(reversed(list(CONFIG.items()))), indent=2))
    assert a.config_sha256 == b.config_sha256
    assert a.matches("nginx-seasonal", "demo", "requests")
    assert not a.matches("nginx-seasonal", "other", "requests")
    assert not a.matches("nginx-seasonal", "demo", "cpu")
