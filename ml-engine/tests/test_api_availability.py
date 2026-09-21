"""Per-step availability must survive the API's own consumers (Codex C-79 / D-100).

The accuracy queue did float(None) on a null pattern step and raised after queuing only the
first forecast; the gauge loop aborted the same way and left the remaining gauges stale.
Worse, the queue keyed on `target_timestamps`, which the predictor's own return dict never
carried -- so `targets` was always empty and NOTHING had ever been queued for scoring.

Endpoint-level: the tests drive the real /predict handler and the real LSTMPredictor.predict.
Every test here fails on the code before its fix.
"""


import math
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144
END = datetime(2026, 3, 2, 12, 0)
NETWORK_VALUE = 5000.0


def daily_series(days: int, end: datetime, amplitude: float = 400.0, base: float = 600.0):
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = [base + amplitude * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / 1440.0)
            for t in idx]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


class _Stub:
    def __init__(self, scaled, raises=None, batched=False):
        self.scaled, self.raises, self.batched = scaled, raises, batched
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        if self.raises:
            raise self.raises
        n = x.shape[0] if self.batched else 1
        return np.full((n, STEPS_AHEAD), self.scaled, dtype=float)


def fitted(seasonal, history=None, network_scaled=None, raises=None, batched=False):
    from sklearn.preprocessing import RobustScaler

    m = LSTMForecastModel(sequence_length=PER_DAY)
    scaler = RobustScaler().fit(seasonal.values.reshape(-1, 1))
    m.scaler = scaler
    window = seasonal.iloc[-PER_DAY:]
    m.last_sequence = scaler.transform(window.values.reshape(-1, 1)).flatten()
    m.input_timestamps = list(window.index.to_pydatetime())
    scaled = (float(scaler.transform(np.array([[NETWORK_VALUE]]))[0][0])
              if network_scaled is None else network_scaled)
    m.model = _Stub(scaled, raises=raises, batched=batched)
    m.is_trained = True
    m.seasonal_history = seasonal if history is None else history
    m.mape_for_floor = 0.0
    return m


# =======================================================================================
# C-79: endpoint-level, through the real handler with a partial-history prediction.
# =======================================================================================
def _partial_prediction(steps=STEPS_AHEAD):
    """What the API returns for a 140-point history: steps 1-2 have no pattern."""
    targets = [(END + GRID * (i + 1)).isoformat() for i in range(steps)]
    avail = [False, False, True, True, True, True][:steps]
    final = [700.0 + i for i in range(steps)]
    lstm = [500.0 + i for i in range(steps)]
    pattern = [None if not a else 900.0 + i for i, a in enumerate(avail)]
    return {
        "application": "nginx-test", "metric_type": "requests",
        "model_version": "t@abc", "model_trained_at": "", "training_cutoff": "",
        "artifact_sha256": "a" * 64, "sequence_length": PER_DAY,
        "inference_input_end": END.isoformat(), "inference_window": {},
        "provenance": "sidecar", "predictions": final, "target_timestamps": targets,
        "confidence": 0.7, "model_name": "lstm_nginx-test_requests",
        "timestamp": END.isoformat(), "horizon_minutes": 60, "data_points_used": 140,
        "model_age_hours": 1.0, "floor_pct": 0.0,
        "components": {"lstm": lstm, "pattern": pattern, "blended": final, "final": final,
                       "pattern_available": True, "pattern_available_per_step": avail,
                       "pattern_weights": [0.0 if not a else 0.7 for a in avail]},
    }


@pytest.fixture
def api(monkeypatch):
    from fastapi.testclient import TestClient
    from api import main as api_main

    monkeypatch.setattr(api_main.predictor, "predict",
                        lambda *a, **k: _partial_prediction(), raising=True)
    # Fresh queue and fresh gauge state for this test.
    api_main.accuracy_tracker.pending.clear()
    for labels in list(api_main.PREDICTION_RPM_GAUGE._metrics.keys()):
        try:
            api_main.PREDICTION_RPM_GAUGE.remove(*labels)
        except KeyError:
            pass
    return TestClient(api_main.app), api_main


def _post(client):
    body = {"application": "nginx-test", "namespace": "demo", "metric_type": "requests",
            "horizon_minutes": 60,
            "metric_data": [{"timestamp": (END - GRID * (PER_DAY - 1 - i)).isoformat() + "Z",
                             "value": 600.0} for i in range(PER_DAY)]}
    return client.post("/predict", json=body)


def _gauge_present(api_main, component, step):
    key = ("nginx-test", "demo", component, str(step))
    return key in api_main.PREDICTION_RPM_GAUGE._metrics


def test_every_valid_step_is_queued_and_unavailable_steps_are_skipped(api):
    client, api_main = api
    r = _post(client)
    assert r.status_code == 200, r.text

    pend = api_main.accuracy_tracker.pending
    by_comp = {k[3]: len(v) for k, v in pend.items()
               if k[0] == "nginx-test" and k[1] == "demo" and k[2] == "requests"}
    # Pre-fix: float(None) raised at step 1's pattern, after only step 1's final and lstm
    # were queued -- so final == 1, pattern == 0, and steps 2-6 never arrived.
    assert by_comp.get(None) == STEPS_AHEAD, by_comp          # every final step
    assert by_comp.get("lstm") == STEPS_AHEAD, by_comp
    assert by_comp.get("blended") == STEPS_AHEAD, by_comp
    assert by_comp.get("pattern") == 4, by_comp               # only the four backed steps


def test_unavailable_gauges_are_cleared_not_left_stale(api):
    client, api_main = api
    # Pre-seed a STALE pattern gauge for steps 1-2 from a previous issuance.
    for step in (1, 2):
        api_main.PREDICTION_RPM_GAUGE.labels(application="nginx-test", namespace="demo",
                                             component="pattern", step=str(step)).set(123.0)
    assert _gauge_present(api_main, "pattern", 1)

    r = _post(client)
    assert r.status_code == 200, r.text
    # Unavailable at this issuance -> the stale series is gone.
    assert not _gauge_present(api_main, "pattern", 1)
    assert not _gauge_present(api_main, "pattern", 2)
    # Available steps are set, and the other components are complete.
    for step in (3, 4, 5, 6):
        assert _gauge_present(api_main, "pattern", step)
    for comp in ("lstm", "blended", "final"):
        for step in range(1, STEPS_AHEAD + 1):
            assert _gauge_present(api_main, comp, step), (comp, step)


class _FakeTrainedModel:
    """Stands in for a trained LSTMForecastModel inside the REAL LSTMPredictor.predict().

    Returns what the model's predict() returns, including `target_timestamps` -- the key the
    predictor's own return dict never carried (C-79).
    """
    sequence_length = PER_DAY
    is_trained = True

    def predict(self, steps_ahead=STEPS_AHEAD, confidence_level=0.95, origin=None,
                input_timestamps=None, seasonal_history=None, **kw):
        assert origin is not None, "the predictor must pass the forecast origin (C-45)"
        targets = [(origin + GRID * (i + 1)).isoformat() for i in range(steps_ahead)]
        final = [700.0 + i for i in range(steps_ahead)]
        return {"predictions": np.asarray(final), "confidence": 0.7,
                "target_timestamps": targets, "floor_pct": 0.0,
                "components": {"lstm": final, "pattern": [None] * steps_ahead,
                               "blended": final, "final": final,
                               "pattern_available": False,
                               "pattern_available_per_step": [False] * steps_ahead,
                               "pattern_weights": [0.0] * steps_ahead}}


def test_predictor_return_dict_carries_target_timestamps(monkeypatch):
    """Drives the REAL LSTMPredictor.predict(), not a stub of it. Without this key the queue
    loop breaks at step 0: nothing is ever scored. (The earlier version of this test patched
    predictor.predict itself and so tested the fixture, not the code -- it passed pre-fix.)"""
    from api import main as api_main
    from datetime import timezone

    key = "nginx-test_requests"
    monkeypatch.setitem(api_main.predictor.trained_models, key, _FakeTrainedModel())
    monkeypatch.setitem(api_main.predictor.model_train_times, key, datetime.utcnow())
    monkeypatch.setitem(api_main.predictor.model_meta, key, {"artifact_sha256": "a" * 64})
    monkeypatch.setattr(api_main.predictor, "_check_and_reload_model", lambda *a, **k: None)

    # The inference-window check refuses a latest sample older than two grid steps, measured
    # against the wall clock -- so the data must end at (roughly) now, on the ten-minute grid.
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    last = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
    metric_data = [{"timestamp": (last - GRID * (PER_DAY - 1 - i)).isoformat() + "Z",
                    "value": 600.0} for i in range(PER_DAY)]

    out = api_main.predictor.predict("nginx-test", metric_data, 60, "requests", "demo")
    assert isinstance(out, dict)
    assert len(out.get("target_timestamps", [])) == STEPS_AHEAD, out.keys()
    # And they are the model's, keyed off the forecast origin, one grid step apart.
    ts = [datetime.fromisoformat(t) for t in out["target_timestamps"]]
    assert all((b - a) == GRID for a, b in zip(ts, ts[1:]))
