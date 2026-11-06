"""C2.6: the statistical service's app: benchmark switches refused, the open and protected paths under TokenReview (the
legacy service's middleware, unchanged), no legacy routes, the allowlisted entrypoint and TLS rotation."""

import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api import serve, statistical  # noqa: E402
from forecasting.metrics import ForecastMetrics  # noqa: E402
from forecasting.service import ServiceError  # noqa: E402

OPERATOR = "system:serviceaccount:pa-system:pa-operator"


class FakeService:
    def __init__(self, answer=None, error=None):
        self.answer, self.error, self.calls, self.closed = answer or {"ok": True}, error, [], False
        self.metrics = ForecastMetrics()

    def predict(self, body):
        self.calls.append(body)
        if self.error:
            raise self.error
        return self.answer

    def health(self):
        return {"status": "healthy", "state": "healthy"}

    def close(self):
        self.closed = True


def client(env=None, svc=None):
    svc = svc or FakeService()
    return TestClient(statistical.create_app(env or {}, service_factory=lambda: svc)), svc


@pytest.mark.parametrize("switch", statistical.BENCHMARK_SWITCHES)
def test_every_benchmark_switch_is_refused(switch):
    for value in ("1", "0", ""):                       # present at all, whatever its value
        with pytest.raises(statistical.Refused):
            statistical.create_app({switch: value}, service_factory=FakeService)


def test_the_service_needs_a_prometheus_url(tmp_path):
    with pytest.raises(statistical.Refused):
        statistical.default_service({"FORECAST_STATE_DIR": str(tmp_path)})


def test_predict_health_ready_and_metrics(tmp_path):
    c, svc = client()
    with c:
        assert c.get("/health").json()["state"] == "healthy"
        assert c.get("/ready").status_code == 200
        r = c.post("/v2/predict", json={"x": 1})
        assert r.status_code == 200 and r.json() == {"ok": True} and svc.calls == [{"x": 1}]
        m = c.get("/metrics")
        assert m.status_code == 200 and "process_" in m.text
        assert c.post("/v2/predict", content=b"not json").status_code == 422
        assert c.post("/v2/predict", content=b"x" * (statistical.MAX_BODY + 1)).status_code == 413
    assert svc.closed


@pytest.mark.parametrize("status,reason", [(422, "IdentityMismatch"), (503, "StateUnavailable"),
                                           (503, "LeadInsufficient")])
def test_service_errors_keep_their_status(status, reason):
    c, _ = client(svc=FakeService(error=ServiceError(status, reason, "why")))
    with c:
        r = c.post("/v2/predict", json={})
    assert r.status_code == status and r.json()["detail"]["error"] == reason


@pytest.mark.parametrize("path,method", [("/predict", "post"), ("/train", "post"), ("/models", "get"),
                                         ("/docs", "get"), ("/openapi.json", "get"), ("/ensemble", "post")])
def test_no_legacy_or_benchmark_routes(path, method):
    c, _ = client()
    with c:
        assert getattr(c, method)(path).status_code == 404


def test_tokenreview_protects_everything_but_the_open_paths(tmp_path):
    env = {"FORECASTER_AUTH": "tokenreview", "FORECASTER_AUTH_SUBJECTS": OPERATOR,
           "FORECASTER_AUTH_ALLOW_PLAINTEXT": "true"}
    c, svc = client(env)
    with c:
        assert c.get("/health").status_code == 200 and c.get("/ready").status_code == 200
        assert c.get("/metrics").status_code == 200
        for path in ("/v2/predict", "/v2/predict/", "//v2/predict", "/docs"):
            r = c.post(path, json={})
            assert r.status_code == 401, path
        r = c.post("/v2/predict", json={}, headers={"Authorization": "Bearer x"})
        assert r.status_code == 503                    # TokenReview unreachable outside a cluster: never open
    assert svc.calls == []


def test_tokenreview_without_tls_does_not_start():
    from api.auth import AuthMisconfigured
    with pytest.raises(AuthMisconfigured):
        statistical.create_app({"FORECASTER_AUTH": "tokenreview", "FORECASTER_AUTH_SUBJECTS": OPERATOR},
                               service_factory=FakeService)


# --- the entrypoint ------------------------------------------------------------------------------------------------

def test_the_entrypoint_serves_only_allowlisted_apps():
    assert serve.app_ref([]) == "api.main:app"                   # the legacy default is unchanged
    assert serve.app_ref(["legacy"]) == "api.main:app"
    assert serve.app_ref(["statistical"]) == "api.statistical:app"
    for bad in (["api.main:app"], ["os:system"], ["statistical", "x"]):
        with pytest.raises(SystemExit):
            serve.app_ref(bad)


def test_the_entrypoint_keeps_tls_and_rotation_for_the_statistical_app(tmp_path, monkeypatch):
    cert, key = tmp_path / "tls.crt", tmp_path / "tls.key"
    cert.write_text("c")
    key.write_text("k")
    seen = {}

    class Server:
        should_exit = False

        def __init__(self, config):
            seen["app"], seen["ssl"] = config.app, (config.ssl_certfile, config.ssl_keyfile)

        def run(self):
            seen["ran"] = True

    started = []
    monkeypatch.setattr(serve.uvicorn, "Server", Server)
    monkeypatch.setattr(serve.threading, "Thread", lambda target, args, daemon: type(
        "T", (), {"start": lambda self: started.append(args[1])})())
    monkeypatch.setenv("FORECASTER_TLS_CERT", str(cert))
    monkeypatch.setenv("FORECASTER_TLS_KEY", str(key))
    serve.main(["statistical"])
    assert seen == {"app": "api.statistical:app", "ssl": (str(cert), str(key)), "ran": True}
    assert started == [[str(cert), str(key)]]                     # the rotation watcher watches both files
