"""A broken network must not turn a finite seasonal forecast into an HTTP refusal
(Codex C-83 / D-104).

0d19247 made pattern-only serving survive a NaN, infinite or raising network at the model
level. But `components.lstm` still carried the NaN, and FastAPI's JSONResponse uses
json.dumps(allow_nan=False), so the whole response died at the boundary:

    HTTP 400 "Out of range float values are not JSON compliant: nan"

The forecast was finite; an UNUSED diagnostic refused it. These tests go THROUGH HTTP -- the
real /predict handler, the real LSTMPredictor.predict(), a real LSTMForecastModel with a stub
network -- and fail on the code before the fix.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144
KEY = "nginx-test_requests"


class _Stub:
    def __init__(self, scaled, raises=None):
        self.scaled, self.raises = scaled, raises
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        if self.raises:
            raise self.raises
        return np.full((1, STEPS_AHEAD), self.scaled, dtype=float)


def _fresh_two_days():
    """Two days on the ten-minute grid ending at wall-clock now (the inference-window check
    refuses a latest sample older than two grid steps), with a daily profile so the
    previous-day lookup is genuine for every step."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    last = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
    n = 2 * PER_DAY
    idx = [last - GRID * (n - 1 - i) for i in range(n)]
    vals = [1500.0 + 400.0 * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / 1440.0) for t in idx]
    return idx, vals


def _model_with_network(idx, vals, network_scaled=None, raises=None):
    from sklearn.preprocessing import RobustScaler

    seasonal = pd.Series(vals, index=pd.DatetimeIndex(idx))
    m = LSTMForecastModel(sequence_length=PER_DAY)
    m.scaler = RobustScaler().fit(seasonal.values.reshape(-1, 1))
    m.model = _Stub(network_scaled if network_scaled is not None else 0.0, raises=raises)
    m.is_trained = True
    m.mape_for_floor = 0.0
    m.pattern_weight_override = 1.0        # pattern-only: the network is NOT in the forecast
    return m


@pytest.fixture
def api(monkeypatch):
    from fastapi.testclient import TestClient
    from api import main as api_main

    monkeypatch.setattr(api_main.predictor, "_check_and_reload_model", lambda *a, **k: None)
    monkeypatch.setitem(api_main.predictor.model_train_times, KEY, datetime.utcnow())
    monkeypatch.setitem(api_main.predictor.model_meta, KEY, {"artifact_sha256": "a" * 64})
    api_main.accuracy_tracker.pending.clear()
    api_main.accuracy_tracker.history.clear()
    return TestClient(api_main.app), api_main


def _post(client, idx, vals):
    body = {"application": "nginx-test", "namespace": "demo", "metric_type": "requests",
            "horizon_minutes": 60,
            "metric_data": [{"timestamp": t.isoformat() + "Z", "value": v}
                            for t, v in zip(idx, vals)]}
    return client.post("/predict", json=body)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")], ids=["nan", "inf", "-inf"])
def test_non_finite_network_at_weight_one_returns_200_with_finite_pattern_forecasts(api, monkeypatch, bad):
    client, api_main = api
    idx, vals = _fresh_two_days()
    monkeypatch.setitem(api_main.predictor.trained_models, KEY, _model_with_network(idx, vals, bad))

    r = _post(client, idx, vals)
    # Pre-fix: 400 "Out of range float values are not JSON compliant: nan".
    assert r.status_code == 200, r.text
    body = r.json()
    preds = body["predictions"]
    assert len(preds) == STEPS_AHEAD and all(isinstance(p, (int, float)) for p in preds)
    assert all(np.isfinite(preds))
    comp = body["components"]
    # The failure is STILL reported -- as null diagnostics and a status, not as NaN.
    assert comp["lstm"] == [None] * STEPS_AHEAD
    assert comp["network_finite_per_step"] == [False] * STEPS_AHEAD
    assert comp["pattern_available"] is True
    assert all(comp["pattern_available_per_step"])
    assert comp["pattern_weights"] == [1.0] * STEPS_AHEAD
    assert comp["agreement"] is None
    # The served values are the pattern's.
    assert np.allclose(preds, comp["pattern"], rtol=1e-6, atol=1e-2)


def test_network_exception_at_weight_one_returns_200_with_finite_pattern_forecasts(api, monkeypatch):
    client, api_main = api
    idx, vals = _fresh_two_days()
    monkeypatch.setitem(api_main.predictor.trained_models, KEY,
                        _model_with_network(idx, vals, raises=RuntimeError("keras exploded")))

    r = _post(client, idx, vals)
    assert r.status_code == 200, r.text
    body = r.json()
    assert all(np.isfinite(body["predictions"]))
    comp = body["components"]
    assert comp["network_failed"].startswith("RuntimeError")
    assert comp["lstm"] == [None] * STEPS_AHEAD
    assert np.allclose(body["predictions"], comp["pattern"], rtol=1e-6, atol=1e-2)


def test_response_never_contains_a_non_finite_number(api, monkeypatch):
    """Belt and braces: whatever the model emits, the boundary sweep makes the payload
    JSON-compliant. Assert by re-parsing with a strict decoder."""
    import json

    client, api_main = api
    idx, vals = _fresh_two_days()
    monkeypatch.setitem(api_main.predictor.trained_models, KEY, _model_with_network(idx, vals, float("nan")))
    r = _post(client, idx, vals)
    assert r.status_code == 200
    # json.loads with a constant-rejecting hook: NaN/Infinity literals would raise.
    json.loads(r.text, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))
