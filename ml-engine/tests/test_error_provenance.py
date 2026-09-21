"""Every error figure travels with how much evidence is behind it (Codex C-85 / D-106).

The accuracy gauges and the response's `mape` returned 0.0 on an empty window
(`accuracy.py` get_mape/get_mae/get_component_mape), which reads as PERFECT accuracy when
it means nothing was scored. Now:

  - the tracker exposes `*_stats()` with value / scored / recorded / availability / measured,
    and the bare getters return None -- never 0.0 -- when nothing was measured;
  - a single scored entry IS a measurement, reported with scored=1;
  - the /predict response carries mape/mae beside mape_scored, mape_recorded,
    mape_availability and mape_measured;
  - a Prometheus error gauge is REMOVED when unmeasured (absent from the scrape, not 0.0),
    and a scored-count gauge sits beside it.

The HTTP tests drive the real handler and the real LSTMPredictor.predict() so that the
queue -> matured observation -> measured error chain is exercised end to end, and fail on
the code before the fix.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.accuracy import AccuracyTracker  # noqa: E402
from models.lstm_model import STEPS_AHEAD  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144
KEY = "nginx-test_requests"
APP, NS, MT = "nginx-test", "demo", "requests"


# ---------------------------------------------------------------------------------------
# Tracker level
# ---------------------------------------------------------------------------------------
def test_fresh_tracker_reports_not_measured_not_zero():
    t = AccuracyTracker()
    assert t.get_mape(APP, NS, MT) is None   # pre-fix: 0.0, indistinguishable from perfect
    assert t.get_mae(APP, NS, MT) is None
    for st in (t.mape_stats(APP, NS, MT), t.mae_stats(APP, NS, MT),
               t.component_mape_stats(APP, NS, MT, "lstm")):
        assert st["measured"] is False and st["value"] is None
        assert st["scored"] == 0 and st["recorded"] == 0 and st["availability"] is None


def test_one_matured_observation_reports_count_one_and_a_finite_error():
    t = AccuracyTracker()
    t.record(APP, NS, MT, predicted=1100.0, actual=1000.0)
    assert t.get_mape(APP, NS, MT) == 10.0   # pre-fix: 0.0 (a 2-entry floor hid the sample)
    st = t.mape_stats(APP, NS, MT)
    assert st["measured"] is True and st["scored"] == 1 and st["recorded"] == 1
    assert st["availability"] == 1.0 and st["value"] == 10.0
    mae = t.mae_stats(APP, NS, MT)
    assert mae["measured"] is True and mae["scored"] == 1 and mae["value"] == 100.0


def test_trough_filtered_entries_are_counted_as_recorded_not_scored():
    t = AccuracyTracker()
    t.record(APP, NS, MT, predicted=1100.0, actual=1000.0)   # scored
    t.record(APP, NS, MT, predicted=300.0, actual=500.0)     # below MIN_TRAFFIC_RPM
    assert t.get_mape(APP, NS, MT) == 10.0   # pre-fix: 0.0 (one valid entry < the 2 floor)
    st = t.mape_stats(APP, NS, MT)
    assert st["recorded"] == 2 and st["scored"] == 1 and st["availability"] == 0.5
    assert st["measured"] is True and st["value"] == 10.0


# ---------------------------------------------------------------------------------------
# HTTP level: fresh service -> not measured; one matured observation -> scored 1, finite.
# ---------------------------------------------------------------------------------------
class _FakeTrainedModel:
    """Inside the REAL LSTMPredictor.predict(): a constant forecast of 1100 rpm."""
    sequence_length = PER_DAY
    is_trained = True

    def predict(self, steps_ahead=STEPS_AHEAD, confidence_level=0.95, origin=None,
                input_timestamps=None, seasonal_history=None, **kw):
        targets = [(origin + GRID * (i + 1)).isoformat() for i in range(steps_ahead)]
        final = [1100.0] * steps_ahead
        return {"predictions": np.asarray(final), "confidence": 0.7,
                "target_timestamps": targets, "floor_pct": 0.0,
                "components": {"lstm": final, "pattern": [None] * steps_ahead,
                               "blended": final, "final": final, "pattern_available": False,
                               "pattern_available_per_step": [False] * steps_ahead,
                               "pattern_weights": [0.0] * steps_ahead}}


@pytest.fixture
def api(monkeypatch):
    from fastapi.testclient import TestClient
    from api import main as api_main

    monkeypatch.setattr(api_main.predictor, "_check_and_reload_model", lambda *a, **k: None)
    monkeypatch.setitem(api_main.predictor.trained_models, KEY, _FakeTrainedModel())
    monkeypatch.setitem(api_main.predictor.model_train_times, KEY, datetime.utcnow())
    monkeypatch.setitem(api_main.predictor.model_meta, KEY, {"artifact_sha256": "a" * 64})
    api_main.accuracy_tracker.pending.clear()
    api_main.accuracy_tracker.history.clear()
    api_main.accuracy_tracker.component_history.clear()
    for name in ("MAPE_GAUGE", "MAE_GAUGE", "ACCURACY_SCORED_GAUGE"):
        g = getattr(api_main, name, None)   # absent on pre-fix code; the tests then fail on behaviour
        if g is None:
            continue
        for labels in list(g._metrics.keys()):
            try:
                g.remove(*labels)
            except KeyError:
                pass
    return TestClient(api_main.app), api_main


def _grid_now():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)


def _post_ending_at(client, last, value=1200.0):
    idx = [last - GRID * (PER_DAY - 1 - i) for i in range(PER_DAY)]
    body = {"application": APP, "namespace": NS, "metric_type": MT, "horizon_minutes": 60,
            "metric_data": [{"timestamp": t.isoformat() + "Z", "value": value} for t in idx]}
    return client.post("/predict", json=body)


def _gauge(api_main, gauge):
    return gauge._metrics.get((APP, NS, MT))


def test_fresh_service_reports_not_measured_over_http(api):
    client, api_main = api
    r = _post_ending_at(client, _grid_now() - GRID)
    assert r.status_code == 200, r.text
    body = r.json()
    # Pre-fix: "mape": 0.0 -- indistinguishable from perfect.
    assert body["mape"] is None
    assert body["mape_measured"] is False
    assert body["mape_scored"] == 0 and body["mape_recorded"] == 0
    assert body["mape_availability"] is None
    assert body["mae"] is None and body["mae_measured"] is False
    # And the gauges are ABSENT, not 0.0.
    assert _gauge(api_main, api_main.MAPE_GAUGE) is None
    assert _gauge(api_main, api_main.MAE_GAUGE) is None


def test_after_one_matured_observation_http_reports_count_one_and_finite_error(api):
    client, api_main = api
    last = _grid_now()
    # First call: data ends one grid step ago, so step 1 targets `last` -- which is now.
    r1 = _post_ending_at(client, last - GRID, value=1200.0)
    assert r1.status_code == 200, r1.text
    assert r1.json()["mape_measured"] is False
    # Second call: the observation for `last` arrives (actual 1000 vs forecast 1100).
    r2 = _post_ending_at(client, last, value=1000.0)
    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["mape"] == pytest.approx(10.0)   # pre-fix: 0.0 with one matured entry
    assert body["mape_measured"] is True
    assert body["mape_scored"] == 1 and body["mape_recorded"] == 1
    assert body["mape_availability"] == 1.0
    assert body["mape"] == pytest.approx(10.0)
    assert body["mae_measured"] is True and body["mae_scored"] == 1
    assert body["mae"] == pytest.approx(100.0)
    # Gauges present with the value, and the scored count beside them.
    assert _gauge(api_main, api_main.MAPE_GAUGE) is not None
    assert api_main.ACCURACY_SCORED_GAUGE._metrics[(APP, NS, MT)]._value.get() == 1.0


# ---------------------------------------------------------------------------------------
# The validation mape is an error figure too: it must carry the number of holdout sequences
# it rests on, from train_on_data() through validation_metadata to /models (C-85).
# ---------------------------------------------------------------------------------------
def test_validation_mape_carries_its_scored_count_through_to_models_endpoint():
    from unittest.mock import patch
    from fastapi.testclient import TestClient

    from tests.test_validation import FakeLSTMModel, _make_metric_data, _make_predictor

    class _CountingFake(FakeLSTMModel):
        def evaluate(self, test_data, target_column="value"):
            return {**super().evaluate(test_data, target_column), "sequences_scored": 9}

    fake = _CountingFake(mape=8.0)
    with patch("api.main.joblib.dump"), patch("api.main.LSTMForecastModel") as MockModel:
        MockModel.return_value = fake
        predictor = _make_predictor()
        predictor.train_on_data("app1", _make_metric_data(), "requests")

    meta = predictor.validation_metadata["app1_requests"]
    assert meta["status"] == "accepted"
    assert meta["mape"] == 8.0
    assert meta["scored"] == 9, meta          # pre-fix: KeyError, the count was never recorded

    # And it is exposed beside the figure on /models.
    from api import main as api_main
    api_main.predictor.validation_metadata["app1_requests"] = meta
    api_main.predictor.trained_models["app1_requests"] = fake
    api_main.predictor.model_train_times["app1_requests"] = datetime.utcnow()
    try:
        body = TestClient(api_main.app).get("/models").json()
        info = body["models"]["app1_requests"]
        assert info["validation_mape"] == 8.0
        assert info["validation_scored"] == 9
    finally:
        api_main.predictor.validation_metadata.pop("app1_requests", None)
        api_main.predictor.trained_models.pop("app1_requests", None)
        api_main.predictor.model_train_times.pop("app1_requests", None)
