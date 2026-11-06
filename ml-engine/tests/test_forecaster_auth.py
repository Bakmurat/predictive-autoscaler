"""B5e: caller authentication of the forecasting service (api/auth.py; Codex task-08 r34)."""

import asyncio
import base64
import json
import logging
import os
import sys
import threading
import time

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api import auth  # noqa: E402
from data.bounded_http import TransportFailure  # noqa: E402

OPERATOR = "system:serviceaccount:pa:pa-predictive-autoscaler-operator"
SCRAPER = "system:serviceaccount:monitoring:vmagent"
NOW = 1_800_000_000.0


def jwt(exp=NOW + 3600, extra=None):
    claims = {"aud": [auth.DEFAULT_AUDIENCE], **({"exp": exp} if exp is not None else {}), **(extra or {})}
    b64 = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()
    return f"{b64({'alg': 'RS256'})}.{b64(claims)}.signature"


def review_body(user=OPERATOR, authenticated=True, audiences=(auth.DEFAULT_AUDIENCE,), error=None):
    st = {"authenticated": authenticated, "user": {"username": user}, "audiences": list(audiences)}
    if error:
        st["error"] = error
    return json.dumps({"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview", "status": st}).encode()


class FakeAPI:
    """The Kubernetes API's TokenReview endpoint: records calls, answers as told."""

    def __init__(self, status=201, body=None, exc=None, delay=0.0):
        self.status, self.body, self.exc, self.delay = status, body or review_body(), exc, delay
        self.calls = []

    def __call__(self, method, url, *, body=None, headers=None, deadline_s=None, max_bytes=None, ssl_context=None):
        self.calls.append({"method": method, "url": url, "body": json.loads(body), "headers": headers})
        if self.delay:
            time.sleep(self.delay)
        if self.exc:
            raise self.exc
        return self.status, self.body


@pytest.fixture
def sa_dir(tmp_path):
    (tmp_path / "token").write_text("own-token\n")
    return str(tmp_path)


CONFIG = auth.AuthConfig(mode="tokenreview", subjects=frozenset({OPERATOR}), allow_plaintext=False)


def reviewer(sa_dir, api, clock=None, **kw):
    clock = clock or {"mono": 100.0, "wall": NOW}
    return auth.TokenReviewer(CONFIG, host="https://k8s:443", sa_dir=sa_dir, request=api,
                              monotonic=lambda: clock["mono"], wall=lambda: clock["wall"], **kw)


# --- configuration -----------------------------------------------------------------------------------------------------

def test_authentication_is_off_unless_asked_for():
    assert not auth.config_from_env({}).enabled


@pytest.mark.parametrize("env, message", [
    ({"FORECASTER_AUTH": "basic"}, "must be off or tokenreview"),
    ({"FORECASTER_AUTH": "tokenreview"}, "needs FORECASTER_AUTH_SUBJECTS"),
    ({"FORECASTER_AUTH": "tokenreview", "FORECASTER_AUTH_SUBJECTS": "admin"}, "not service account subjects"),
    ({"FORECASTER_AUTH": "tokenreview", "FORECASTER_AUTH_SUBJECTS": OPERATOR}, "needs TLS"),
    ({"FORECASTER_AUTH": "tokenreview", "FORECASTER_AUTH_SUBJECTS": OPERATOR, "FORECASTER_AUTH_ALLOW_PLAINTEXT": "true",
      "FORECASTER_AUTH_METRICS": "true"}, "needs FORECASTER_AUTH_METRICS_SUBJECTS"),
])
def test_unsafe_configurations_refuse_to_start(env, message):
    with pytest.raises(auth.AuthMisconfigured, match=message):
        auth.config_from_env(env)


def test_a_complete_configuration(tmp_path):
    cert, key = tmp_path / "tls.crt", tmp_path / "tls.key"
    cert.write_text("c")
    key.write_text("k")
    cfg = auth.config_from_env({"FORECASTER_AUTH": "tokenreview", "FORECASTER_AUTH_SUBJECTS": f" {OPERATOR} ",
                                "FORECASTER_TLS_CERT": str(cert), "FORECASTER_TLS_KEY": str(key),
                                "FORECASTER_AUTH_METRICS": "true", "FORECASTER_AUTH_METRICS_SUBJECTS": SCRAPER})
    assert cfg.enabled and cfg.subjects == {OPERATOR} and cfg.metrics_subjects == {SCRAPER}
    assert cfg.open_paths == {"/health", "/ready"} and not cfg.allow_plaintext       # /metrics protected here


# --- TokenReview --------------------------------------------------------------------------------------------------------

def test_a_review_asks_for_the_audience_with_the_services_own_credentials(sa_dir):
    api = FakeAPI()
    assert reviewer(sa_dir, api).review(jwt(), CONFIG.subjects) == OPERATOR
    call = api.calls[0]
    assert call["method"] == "POST" and call["url"].endswith("/apis/authentication.k8s.io/v1/tokenreviews")
    assert call["body"]["spec"]["audiences"] == [auth.DEFAULT_AUDIENCE]
    assert call["headers"]["Authorization"] == "Bearer own-token"


@pytest.mark.parametrize("api, exc", [
    (FakeAPI(body=review_body(authenticated=False)), auth.Unauthenticated),
    (FakeAPI(body=review_body(audiences=("kubernetes",))), auth.Unauthenticated),
    (FakeAPI(body=review_body(user="system:serviceaccount:pa:intruder")), auth.Forbidden),
    (FakeAPI(body=review_body(error="webhook timeout")), auth.AuthUnavailable),
    (FakeAPI(body=review_body(authenticated="yes")), auth.AuthUnavailable),
    (FakeAPI(body=b"not json"), auth.AuthUnavailable),
    (FakeAPI(body=json.dumps({"status": "x"}).encode()), auth.AuthUnavailable),
    (FakeAPI(status=401), auth.AuthUnavailable),                                     # our own credentials refused
    (FakeAPI(status=403), auth.AuthUnavailable),
    (FakeAPI(status=500), auth.AuthUnavailable),
    (FakeAPI(exc=TransportFailure("down")), auth.AuthUnavailable),
])
def test_review_outcomes(sa_dir, api, exc):
    with pytest.raises(exc):
        reviewer(sa_dir, api).review(jwt(), CONFIG.subjects)


@pytest.mark.parametrize("status", [
    {"authenticated": None}, {"authenticated": 1}, {"authenticated": "yes"},
    {"authenticated": True, "audiences": "wrong"},
    {"authenticated": True, "audiences": [auth.DEFAULT_AUDIENCE, 1]},
    {"authenticated": True, "audiences": None}, {"error": False},
])
def test_malformed_review_fields_are_an_outage_not_a_caller_refusal(sa_dir, status):
    api = FakeAPI(body=json.dumps({"status": status}).encode())
    with pytest.raises(auth.AuthUnavailable):
        reviewer(sa_dir, api).review(jwt(), CONFIG.subjects)


@pytest.mark.parametrize("status", [{}, {"user": {}}, {"authenticated": False},
                                     {"authenticated": True, "user": {"username": OPERATOR}}])
def test_optional_fields_follow_the_kubernetes_wire_defaults(sa_dir, status):
    # authenticated:false and empty audiences are omitted by Kubernetes' JSON serializer.
    with pytest.raises(auth.Unauthenticated):
        reviewer(sa_dir, FakeAPI(body=json.dumps({"status": status}).encode())).review(jwt(), CONFIG.subjects)


def test_a_kind_wrong_audience_error_is_an_outage(sa_dir):
    api = FakeAPI(body=json.dumps({"status": {"user": {}, "error": "token audiences do not match"}}).encode())
    with pytest.raises(auth.AuthUnavailable):
        reviewer(sa_dir, api).review(jwt(), CONFIG.subjects)


@pytest.mark.parametrize("broken", ["invalid", "unreadable"])
def test_an_invalid_or_unreadable_api_ca_is_an_authentication_outage(sa_dir, monkeypatch, broken):
    from pathlib import Path
    (Path(sa_dir) / "ca.crt").write_text("not a certificate")
    if broken == "unreadable":
        def refused(**kwargs):
            raise PermissionError("unreadable")
        monkeypatch.setattr(auth.ssl, "create_default_context", refused)
    api = FakeAPI()
    with pytest.raises(auth.AuthUnavailable, match="CA is unreadable or invalid"):
        reviewer(sa_dir, api).review(jwt(), CONFIG.subjects)
    assert not api.calls


def test_positive_answers_are_cached_for_at_most_60_s_and_never_past_the_tokens_expiry(sa_dir):
    clock = {"mono": 100.0, "wall": NOW}
    api = FakeAPI()
    r = reviewer(sa_dir, api, clock)
    token = jwt(exp=NOW + 3600)
    for _ in range(3):
        r.review(token, CONFIG.subjects)
    assert len(api.calls) == 1
    clock["mono"] += 60
    r.review(token, CONFIG.subjects)
    assert len(api.calls) == 2, "60 s at most"
    short = jwt(exp=NOW + 10)
    r.review(short, CONFIG.subjects)
    clock["mono"] += 11
    r.review(short, CONFIG.subjects)
    assert len(api.calls) == 4, "never past the token's own expiry"


@pytest.mark.parametrize("token", [jwt(exp=None), jwt(exp="soon"), jwt(exp=float("nan")), "not-a-jwt", jwt(exp=True)])
def test_a_token_without_a_usable_expiry_is_never_cached(sa_dir, token):
    api = FakeAPI()
    r = reviewer(sa_dir, api)
    r.review(token, CONFIG.subjects)
    r.review(token, CONFIG.subjects)
    assert len(api.calls) == 2


def test_negative_answers_are_never_cached(sa_dir):
    api = FakeAPI(body=review_body(user="system:serviceaccount:pa:intruder"))
    r = reviewer(sa_dir, api)
    for _ in range(2):
        with pytest.raises(auth.Forbidden):
            r.review(jwt(), CONFIG.subjects)
    assert len(api.calls) == 2


def test_an_unexpired_hit_is_served_during_an_outage_an_expired_one_is_not(sa_dir):
    clock = {"mono": 100.0, "wall": NOW}
    api = FakeAPI()
    r = reviewer(sa_dir, api, clock)
    token = jwt()
    r.review(token, CONFIG.subjects)
    api.exc = TransportFailure("down")
    assert r.review(token, CONFIG.subjects) == OPERATOR
    clock["mono"] += 61
    with pytest.raises(auth.AuthUnavailable):
        r.review(token, CONFIG.subjects)


def test_the_cache_is_bounded(sa_dir):
    api = FakeAPI()
    r = reviewer(sa_dir, api, max_entries=2)
    tokens = [jwt(extra={"n": i}) for i in range(3)]
    for t in tokens:
        r.review(t, CONFIG.subjects)
    r.review(tokens[0], CONFIG.subjects)
    assert len(api.calls) == 4, "the oldest entry was evicted"


def test_tokens_are_never_logged(sa_dir, caplog):
    caplog.set_level(logging.DEBUG)
    token = jwt(extra={"marker": "TOKEN-MARKER"})
    with pytest.raises(auth.Forbidden):
        reviewer(sa_dir, FakeAPI(body=review_body(user="system:serviceaccount:pa:intruder"))).review(token, CONFIG.subjects)
    assert token not in caplog.text and "TOKEN-MARKER" not in caplog.text


# --- the middleware -----------------------------------------------------------------------------------------------------

class StubReviewer:
    def __init__(self, outcome=OPERATOR, delay=0.0):
        self.outcome, self.delay, self.calls = outcome, delay, []

    def review(self, token, allowed):
        self.calls.append((token, allowed))
        if self.delay:
            time.sleep(self.delay)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        if self.outcome not in allowed:
            raise auth.Forbidden(self.outcome)
        return self.outcome


def client(stub, config=CONFIG, admission=None, max_body=auth.MAX_BODY):
    app = FastAPI()

    @app.post("/predict")
    async def predict(request: Request):
        body = await request.body()
        return {"bytes": len(body), "subject": request.state.auth_subject}

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/metrics")
    async def metrics():
        return {"metrics": True}

    app.add_middleware(auth.AuthMiddleware, config=config, reviewer=stub, admission=admission, max_body=max_body)
    return TestClient(app, base_url="https://testserver")


def test_an_authenticated_operator_gets_through(sa_dir):
    r = client(StubReviewer()).post("/predict", json={"a": 1}, headers={"Authorization": f"Bearer {jwt()}"})
    assert r.status_code == 200 and r.json()["subject"] == OPERATOR and r.json()["bytes"] > 0


@pytest.mark.parametrize("headers", [
    [],
    [("Authorization", "Basic abc")],
    [("Authorization", "Bearer ")],
    [("Authorization", "Bearer " + "x" * auth.MAX_BEARER)],
    [("Authorization", f"Bearer {jwt()}"), ("Authorization", f"Bearer {jwt()}")],
])
def test_missing_malformed_oversized_or_repeated_credentials_get_401(headers):
    stub = StubReviewer()
    r = client(stub).post("/predict", json={}, headers=headers)
    assert r.status_code == 401 and r.json()["detail"]["error"] == "Unauthenticated"
    assert r.headers["www-authenticate"].startswith("Bearer") and stub.calls == []


@pytest.mark.parametrize("outcome, status, error", [
    (auth.Unauthenticated("no"), 401, "Unauthenticated"),
    ("system:serviceaccount:pa:intruder", 403, "Forbidden"),
    (auth.AuthUnavailable("down"), 503, "AuthUnavailable"),
])
def test_review_outcomes_map_to_statuses(outcome, status, error):
    r = client(StubReviewer(outcome)).post("/predict", json={}, headers={"Authorization": f"Bearer {jwt()}"})
    assert r.status_code == status and r.json()["detail"]["error"] == error


def test_plain_http_is_refused_when_authentication_is_on():
    tc = client(StubReviewer())
    tc.base_url = "http://testserver"
    r = tc.post("/predict", json={}, headers={"Authorization": f"Bearer {jwt()}"})
    assert r.status_code == 500 and r.json()["detail"]["error"] == "TLSRequired"


def test_health_is_open_and_metrics_follow_their_own_subjects():
    stub = StubReviewer()
    tc = client(stub)
    assert tc.get("/health").status_code == 200 and tc.get("/metrics").status_code == 200 and stub.calls == []
    protected = auth.AuthConfig(mode="tokenreview", subjects=frozenset({OPERATOR}), metrics_subjects=frozenset({SCRAPER}),
                                open_paths=frozenset({"/health", "/ready"}))
    assert client(StubReviewer(OPERATOR), protected).get("/metrics", headers={"Authorization": "Bearer t"}).status_code == 403
    assert client(StubReviewer(SCRAPER), protected).get("/metrics", headers={"Authorization": "Bearer t"}).status_code == 200
    assert client(StubReviewer(SCRAPER), protected).post("/predict", json={}, headers={"Authorization": "Bearer t"}).status_code == 403


def test_the_body_is_not_read_before_authentication():
    received = []

    async def receive():
        received.append(1)
        return {"type": "http.request", "body": b"{}", "more_body": False}

    sent = []

    async def send(message):
        sent.append(message)

    async def app(scope, receive, send):
        raise AssertionError("the app must not run")

    mw = auth.AuthMiddleware(app, CONFIG, StubReviewer(auth.Unauthenticated("no")))
    scope = {"type": "http", "path": "/predict", "scheme": "https", "headers": [(b"authorization", b"Bearer t")]}
    asyncio.run(mw(scope, receive, send))
    assert received == [] and sent[0]["status"] == 401


def test_an_oversized_body_is_refused_even_when_chunked():
    chunks = [b"x" * 600, b"x" * 600]

    async def receive():
        return {"type": "http.request", "body": chunks.pop(0), "more_body": bool(chunks)}

    sent = []

    async def send(message):
        sent.append(message)

    async def app(scope, receive, send):
        raise AssertionError("the app must not run")

    mw = auth.AuthMiddleware(app, CONFIG, StubReviewer(), max_body=1000)
    scope = {"type": "http", "path": "/predict", "scheme": "https", "headers": [(b"authorization", b"Bearer t")]}
    asyncio.run(mw(scope, receive, send))
    assert sent[0]["status"] == 413


def test_review_slots_are_bounded_and_held_until_the_work_finishes():
    admission = auth._Admission(slots=1, wait_s=0.2)
    release = threading.Event()

    def slow(token, allowed):
        release.wait(5)
        return OPERATOR

    async def scenario():
        first = asyncio.ensure_future(admission.run(slow, "t", frozenset()))
        await asyncio.sleep(0.05)
        with pytest.raises(auth.AuthUnavailable):
            await admission.run(slow, "t", frozenset())          # no slot within wait_s
        first.cancel()                                          # the request goes away; its review still runs
        await asyncio.sleep(0.05)
        assert not admission.slots.acquire(blocking=False), "the slot must stay held while the review runs"
        release.set()
        for _ in range(50):
            await asyncio.sleep(0.02)
            if admission.slots.acquire(blocking=False):
                admission.slots.release()
                return
        raise AssertionError("the slot was not released when the review finished")

    asyncio.run(scenario())


def test_the_real_service_behind_the_middleware():
    """The forecasting service's own app wrapped as main.py does with authentication on."""
    from api import main
    wrapped = auth.AuthMiddleware(main.app, CONFIG, StubReviewer())
    tc = TestClient(wrapped, base_url="https://testserver")
    assert tc.post("/predict", json={}).status_code == 401
    assert tc.get("/models").status_code == 401
    assert tc.get("/health").status_code == 200
    passed = tc.post("/predict", json={"application": "x"}, headers={"Authorization": f"Bearer {jwt()}"})
    assert passed.status_code not in (401, 403, 500), passed.text     # reached the service's own validation


@pytest.mark.parametrize("path", ["/predict/", "//predict", "/docs", "/openapi.json", "/redoc", "/not-yet-an-endpoint",
                                  "/models", "/"])
def test_every_path_but_the_open_ones_needs_a_token(path):
    stub = StubReviewer()
    tc = client(stub)
    assert tc.get(path).status_code == 401 and tc.post(path, json={}).status_code == 401
    assert stub.calls == []


def test_a_websocket_is_refused():
    sent = []

    async def send(message):
        sent.append(message)

    async def app(scope, receive, send):
        raise AssertionError("the app must not run")

    asyncio.run(auth.AuthMiddleware(app, CONFIG, StubReviewer())({"type": "websocket", "path": "/predict"}, None, send))
    assert sent == [{"type": "websocket.close", "code": 1008}]
