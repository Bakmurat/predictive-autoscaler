"""B4b-2: the forecasting service's product path (metric contract requests-per-second/v1).

Requests carry a reference, never a query or a history; the service resolves the autoscaler's compiled query, reads
the history itself, forecasts with the pinned model trained on exactly that query, and attests the query in the answer.
"""

import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tests.b4_helpers import install_model, make_signal, product_body, route_product_path  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144
SIGNAL = make_signal(namespace="shop", name="web")


class _Model:
    """A trained-model stand-in for the real predictor path."""
    sequence_length = PER_DAY
    is_trained = True

    def predict(self, steps_ahead=6, confidence_level=0.95, origin=None, input_timestamps=None,
                seasonal_history=None, **kw):
        targets = [(origin + GRID * (i + 1)).isoformat() for i in range(steps_ahead)]
        final = [700.0 + i for i in range(steps_ahead)]
        return {"predictions": np.asarray(final), "confidence": 0.8, "target_timestamps": targets, "floor_pct": 0.0,
                "components": {"lstm": final, "pattern": final, "blended": final, "final": final,
                               "pattern_available": True, "pattern_available_per_step": [True] * steps_ahead,
                               "pattern_weights": [0.5] * steps_ahead}}


def _history():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    last = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
    return [{"timestamp": (last - GRID * (PER_DAY - 1 - i)).isoformat(), "value": 600.0} for i in range(PER_DAY)]


@pytest.fixture
def api():
    from fastapi.testclient import TestClient
    from api import main as api_main
    api_main.accuracy_tracker.pending.clear()
    return TestClient(api_main.app), api_main


def test_the_product_answer_attests_the_compiled_query(api, monkeypatch):
    client, api_main = api
    install_model(api_main.predictor, _Model(), SIGNAL)
    route_product_path(monkeypatch, api_main, SIGNAL, _history())
    r = client.post("/predict", json=product_body(SIGNAL))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["metric_query_sha256"] == SIGNAL.sha256 and body["contract"] == SIGNAL.contract
    assert body["model_identity"] == {"namespace": "shop", "name": "web", "metric": "requests"}
    assert body["predictions"] == [700.0, 701.0, 702.0, 703.0, 704.0, 705.0]


@pytest.mark.parametrize("model_for", ["nothing", "other_query", "other_target", "other_autoscaler"])
def test_without_a_model_for_exactly_this_signal_the_forecast_is_refused(api, monkeypatch, model_for):
    client, api_main = api
    trained_for = {"other_query": make_signal(namespace="shop", name="web", query="sum(rate(x[1m]))"),
                   "other_target": make_signal(namespace="shop", name="web", target_uid="old-dep"),
                   "other_autoscaler": make_signal(namespace="shop", name="web", autoscaler_uid="old-pa")}
    if model_for != "nothing":
        install_model(api_main.predictor, _Model(), trained_for[model_for])
    route_product_path(monkeypatch, api_main, SIGNAL, _history())
    r = client.post("/predict", json=product_body(SIGNAL))
    assert r.status_code == 422, r.text
    want = "ModelUnavailable" if model_for == "nothing" else "ModelQueryMismatch"
    assert want in r.json()["detail"]


@pytest.mark.parametrize("failure,status", [
    ("ProvenanceRefused", 422), ("LookupFailed", 503), ("HistoryRefused", 422), ("HistoryUnavailable", 503),
])
def test_refusals_and_outages_are_told_apart(api, monkeypatch, failure, status):
    client, api_main = api
    install_model(api_main.predictor, _Model(), SIGNAL)
    exc = getattr(api_main, failure)("simulated")
    if failure in ("ProvenanceRefused", "LookupFailed"):
        monkeypatch.setattr(api_main, "resolve_signal", lambda request, reader: (_ for _ in ()).throw(exc))
    else:
        monkeypatch.setattr(api_main, "resolve_signal", lambda request, reader: SIGNAL)
        monkeypatch.setattr(api_main, "query_history", lambda url, query: (_ for _ in ()).throw(exc))
    r = client.post("/predict", json=product_body(SIGNAL))
    assert r.status_code == status, r.text


def test_caller_history_and_training_through_the_api_are_gone(api):
    client, _ = api
    r = client.post("/predict", json={**product_body(SIGNAL), "metric_data": _history()})
    assert r.status_code == 422 and "caller-supplied history" in r.json()["detail"]
    assert client.post("/train", json={"application": "web", "metric_data": _history()}).status_code == 410


def test_a_full_house_is_503_and_a_slot_is_held_until_the_worker_finishes(api, monkeypatch):
    client, api_main = api
    install_model(api_main.predictor, _Model(), SIGNAL)
    monkeypatch.setattr(api_main, "_admission", threading.BoundedSemaphore(1))
    entered, release = threading.Event(), threading.Event()

    def slow_resolve(request, reader):
        entered.set()
        release.wait(10)
        return SIGNAL

    monkeypatch.setattr(api_main, "resolve_signal", slow_resolve)
    monkeypatch.setattr(api_main, "query_history", lambda url, query: _history())
    first = threading.Thread(target=lambda: client.post("/predict", json=product_body(SIGNAL)), daemon=True)
    first.start()
    try:
        assert entered.wait(10)
        r = client.post("/predict", json=product_body(SIGNAL))   # the only slot is taken
        assert r.status_code == 503 and "capacity" in r.json()["detail"]
    finally:
        release.set()
        first.join(10)
    for _ in range(100):   # the slot returns when the first job really ends
        if api_main._admission.acquire(blocking=False):
            api_main._admission.release()
            break
        time.sleep(0.02)
    else:
        pytest.fail("the admission slot never came back")


def test_the_slot_is_released_when_the_job_fails(api, monkeypatch):
    import asyncio
    _, api_main = api
    sem = threading.BoundedSemaphore(1)
    monkeypatch.setattr(api_main, "_admission", sem)

    def boom():
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        asyncio.run(api_main._run_admitted(boom))
    assert sem.acquire(blocking=False), "a failed job must release its admission slot"
    sem.release()


@pytest.mark.parametrize("when", ["queued", "running"])
def test_a_cancelled_request_never_leaks_or_frees_a_busy_slot(api, monkeypatch, when):
    # Codex r19: cancelling the awaiting coroutine before the job starts must release the slot (the job never runs);
    # cancelling while the job runs must keep the slot until the job really ends.
    import asyncio
    import concurrent.futures
    _, api_main = api
    sem = threading.BoundedSemaphore(1)
    monkeypatch.setattr(api_main, "_admission", sem)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    blocker_go, job_started, job_go, ran = threading.Event(), threading.Event(), threading.Event(), []
    if when == "queued":
        executor.submit(blocker_go.wait, 10)          # the only worker is busy: our job stays queued

    def job():
        job_started.set()
        ran.append(1)
        job_go.wait(10)

    async def scenario():
        task = asyncio.ensure_future(api_main._run_admitted(job, executor=executor))
        if when == "running":
            while not job_started.is_set():
                await asyncio.sleep(0.01)
        else:
            await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        asyncio.run(scenario())
        if when == "queued":
            assert sem.acquire(blocking=False), "a job cancelled before it started must release its slot"
            sem.release()
            blocker_go.set()
            executor.shutdown(wait=True)
            assert ran == [], "the cancelled job must never run"
        else:
            assert not sem.acquire(blocking=False), "the slot must stay taken while the job still runs"
            job_go.set()
            executor.shutdown(wait=True)
            assert sem.acquire(blocking=False), "the slot returns when the job really ends"
            sem.release()
    finally:
        blocker_go.set()
        job_go.set()
        executor.shutdown(wait=False)


def test_benchmark_experiments_need_an_explicit_opt_in():
    env = {**os.environ, "SEASONAL_EXPERIMENT": '{"id": "x"}', "KUBECONFIG": "/dev/null"}
    env.pop("ALLOW_BENCHMARK_EXPERIMENTS", None)
    root = os.path.join(os.path.dirname(__file__), "..")
    r = subprocess.run([sys.executable, "-c", "import api.main"], cwd=root, env=env, capture_output=True, text=True,
                       timeout=300)
    assert r.returncode != 0 and "ALLOW_BENCHMARK_EXPERIMENTS" in r.stderr


def test_cold_start_runs_kubectl_only_on_explicit_opt_in(monkeypatch, tmp_path):
    import api.main as main
    calls = []
    monkeypatch.setattr(main.subprocess, "run", lambda *a, **k: calls.append(a[0]) or
                        subprocess.CompletedProcess(a[0], 0, "created", ""))
    monkeypatch.setenv("MODEL_DIR", str(tmp_path))
    monkeypatch.delenv("COLD_START_CRONJOB", raising=False)
    main.LSTMPredictor()
    assert calls == [], "without COLD_START_CRONJOB no kubectl may run"
    monkeypatch.setenv("COLD_START_CRONJOB", "ml-training")
    monkeypatch.setenv("POD_NAMESPACE", "ml-engine")
    main.LSTMPredictor()
    assert calls == [["kubectl", "create", "job", "--from=cronjob/ml-training", "ml-training-coldstart",
                      "-n", "ml-engine"]]
