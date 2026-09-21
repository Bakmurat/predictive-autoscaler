"""A served step that is not a number must be refused, never sent as null (Codex C-86 / D-108).

C-83 made the payload JSON-compliant by mapping non-finite floats to None. That is right for
DIAGNOSTICS. It is wrong for `predictions`: the operator decodes that array into []float64
(predictiveautoscaler_controller.go:93), and encoding/json leaves a null as the zero value.
A step the model could not forecast therefore arrives at the controller as a forecast of
**zero requests per minute** -- indistinguishable from a genuine quiet period, and capable of
driving a scale-down.

The contract these tests pin: either every served step is finite, or the API refuses the
forecast with HTTP 422 -- which the operator already maps to forecastRefusedError and
reactive fallback (C-17). Diagnostic nulls stay as they are.
"""

import json
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
    """A network that returns a constant in scaled space, or raises."""

    def __init__(self, scaled, raises=None):
        self.scaled, self.raises = scaled, raises
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        if self.raises:
            raise self.raises
        return np.full((1, STEPS_AHEAD), self.scaled, dtype=float)


def _fresh_two_days():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    last = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
    n = 2 * PER_DAY
    idx = [last - GRID * (n - 1 - i) for i in range(n)]
    vals = [1500.0 + 400.0 * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / 1440.0) for t in idx]
    return idx, vals


def _drop_backing_observations(idx, vals, drop_steps):
    """Remove the previous-day observations that back `drop_steps` from the POSTED data.

    The API builds the seasonal history from metric_data, so this is how a step loses its
    pattern source in reality. Step k targets last + (k+1)*10min; its previous-day source is
    that timestamp minus 24h -- roughly a day back, outside the final 144-point inference
    window (which spans 23h50m), so the window itself stays contiguous and complete.
    The lookup searches up to seven days back, so EVERY day's source for that step must go --
    with two days of history a step has two sources, and dropping one only lowers its support.
    """
    if not drop_steps:
        return list(idx), list(vals)
    last = idx[-1]
    drop_at = {
        last + GRID * (k + 1) - timedelta(days=d)
        for k in drop_steps
        for d in range(1, 8)
    }
    kept = [(t, v) for t, v in zip(idx, vals) if t not in drop_at]
    return [t for t, _ in kept], [v for _, v in kept]


def _model(idx, vals, *, network_scaled=None, raises=None):
    """A pattern-only model: the network is excluded from the forecast, so a step with no
    previous-day observation has nothing to fall back on."""
    from sklearn.preprocessing import RobustScaler

    m = LSTMForecastModel(sequence_length=PER_DAY)
    m.scaler = RobustScaler().fit(np.asarray(vals).reshape(-1, 1))
    m.model = _Stub(network_scaled if network_scaled is not None else 0.0, raises=raises)
    m.is_trained = True
    m.mape_for_floor = 0.0
    m.pattern_weight_override = 1.0
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


def test_unservable_step_is_refused_not_served_as_null(api, monkeypatch):
    """The exact case Codex ran: a failed network and one missing seasonal observation.

    Pre-fix: HTTP 200 with predictions[2] = null.
    Post-fix: HTTP 422, so the operator falls back to its own reactive calculation.
    """
    client, api_main = api
    idx, vals = _fresh_two_days()
    pidx, pvals = _drop_backing_observations(idx, vals, (2,))
    monkeypatch.setitem(api_main.predictor.trained_models, KEY,
                        _model(idx, vals, raises=RuntimeError("keras exploded")))

    r = _post(client, pidx, pvals)
    assert r.status_code == 422, (
        f"an unservable step must be refused, not served; got {r.status_code}: {r.text[:300]}"
    )
    detail = r.json()["detail"]
    assert "step" in detail.lower(), detail


def test_a_served_response_never_carries_a_null_prediction(api, monkeypatch):
    """Whatever the model emits, `predictions` in a 200 response is all finite numbers."""
    client, api_main = api
    idx, vals = _fresh_two_days()
    pidx, pvals = _drop_backing_observations(idx, vals, (0, 3, 5))
    monkeypatch.setitem(api_main.predictor.trained_models, KEY,
                        _model(idx, vals, raises=RuntimeError("keras exploded")))

    r = _post(client, pidx, pvals)
    if r.status_code == 200:
        preds = r.json()["predictions"]
        assert all(isinstance(p, (int, float)) and np.isfinite(p) for p in preds), preds
    else:
        assert r.status_code == 422, r.text


def test_full_coverage_still_serves_200_with_all_finite_steps(api, monkeypatch):
    """The refusal must not fire when every step IS servable -- pattern-only, network dead."""
    client, api_main = api
    idx, vals = _fresh_two_days()
    monkeypatch.setitem(api_main.predictor.trained_models, KEY,
                        _model(idx, vals, raises=RuntimeError("keras exploded")))

    r = _post(client, idx, vals)
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["predictions"]) == STEPS_AHEAD
    assert all(np.isfinite(body["predictions"]))
    # Diagnostic nulls are still correct and still present.
    assert body["components"]["lstm"] == [None] * STEPS_AHEAD


def test_refusal_body_is_the_shape_the_operator_decodes(api, monkeypatch):
    """The 422 body must be JSON the Go client can read as a refusal (it reads the raw body
    into forecastRefusedError). Assert it parses and carries a detail string."""
    client, api_main = api
    idx, vals = _fresh_two_days()
    pidx, pvals = _drop_backing_observations(idx, vals, (2,))
    monkeypatch.setitem(api_main.predictor.trained_models, KEY,
                        _model(idx, vals, raises=RuntimeError("keras exploded")))

    r = _post(client, pidx, pvals)
    assert r.status_code == 422
    parsed = json.loads(r.text)
    assert isinstance(parsed.get("detail"), str) and parsed["detail"]
