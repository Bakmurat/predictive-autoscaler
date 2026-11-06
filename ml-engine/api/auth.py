"""Caller authentication for the forecasting service (B5e; DESIGN-B5, Codex task-08 r23, r24, r34).

With FORECASTER_AUTH=tokenreview, every request must carry exactly one
"Authorization: Bearer <projected service account token>". The token is checked with the Kubernetes TokenReview API for
the service's audience, and the authenticated user must be one of the configured subjects (normally only the
operator's service account). Every path is protected except the open ones (/health and /ready, plus /metrics unless
FORECASTER_AUTH_METRICS=true), so a new endpoint, FastAPI's /docs and /openapi.json, or a path variant (/predict/,
//predict) is never reachable without a token. The token is checked BEFORE the request body is read; the body is then
read through a hard cap.

Outcomes:
  - 401: a missing, repeated, malformed or oversized Authorization header, or a token the API server does not
    authenticate for the audience;
  - 403: an authenticated caller that is not a configured subject;
  - 503 AuthUnavailable: the service cannot judge (TokenReview unreachable, an error or malformed answer, its own
    credentials refused, or all review slots busy). Never an open door;
  - 500 TLSRequired: the request did not arrive over TLS (authentication is never served in clear text, unless
    FORECASTER_AUTH_ALLOW_PLAINTEXT=true, which is for tests only).

Positive results are cached by the token's SHA-256 until the earlier of 60 s and the token's own expiry, read from the
JWT payload unverified, after TokenReview authenticated it, and only to shorten the cache. A token without a usable
exp is not cached. Negative results are never cached. Expired entries are never served, outage or not. Revoking a
token therefore takes effect within 60 s. Tokens and raw reviews are never logged.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import concurrent.futures
import hashlib
import json
import logging
import math
import os
import re
import ssl
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, FrozenSet, Optional

from data.bounded_http import BodyTooLarge, TransportFailure, bounded_request

logger = logging.getLogger(__name__)

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
DEFAULT_AUDIENCE = "predictive-autoscaler-forecaster"
SUBJECT = re.compile(r"^system:serviceaccount:[a-z0-9]([-a-z0-9]*[a-z0-9])?:[a-z0-9]([-.a-z0-9]*[a-z0-9])?$")
MAX_BEARER = 8192                 # bytes of the Authorization header value
MAX_BODY = 1 << 20                # bytes of a protected request's body
REVIEW_DEADLINE_S = 5.0
REVIEW_MAX_RESPONSE = 256 * 1024
CACHE_TTL_S = 60.0


class AuthMisconfigured(Exception):
    """The service refuses to start: authentication is requested but cannot be served safely."""


class Unauthenticated(Exception):
    pass


class Forbidden(Exception):
    pass


class AuthUnavailable(Exception):
    pass


@dataclass(frozen=True)
class AuthConfig:
    mode: str = "off"                                   # off | tokenreview
    audience: str = DEFAULT_AUDIENCE
    subjects: FrozenSet[str] = frozenset()
    metrics_subjects: FrozenSet[str] = frozenset()      # non-empty: /metrics requires one of these (and only these)
    allow_plaintext: bool = False
    open_paths: FrozenSet[str] = field(default_factory=lambda: frozenset({"/health", "/ready", "/metrics"}))

    @property
    def enabled(self) -> bool:
        return self.mode == "tokenreview"


def _subjects(raw: str, name: str) -> FrozenSet[str]:
    subjects = frozenset(s.strip() for s in raw.split(",") if s.strip())
    bad = [s for s in subjects if not SUBJECT.match(s)]
    if bad:
        raise AuthMisconfigured(f"{name}: not service account subjects: {', '.join(sorted(bad))}")
    return subjects


def config_from_env(env=os.environ) -> AuthConfig:
    mode = env.get("FORECASTER_AUTH", "off").strip() or "off"
    if mode == "off":
        return AuthConfig()
    if mode != "tokenreview":
        raise AuthMisconfigured(f"FORECASTER_AUTH must be off or tokenreview, not {mode!r}")
    subjects = _subjects(env.get("FORECASTER_AUTH_SUBJECTS", ""), "FORECASTER_AUTH_SUBJECTS")
    if not subjects:
        raise AuthMisconfigured("FORECASTER_AUTH=tokenreview needs FORECASTER_AUTH_SUBJECTS (the operator's account)")
    metrics = env.get("FORECASTER_AUTH_METRICS", "false").strip().lower() == "true"
    metrics_subjects = _subjects(env.get("FORECASTER_AUTH_METRICS_SUBJECTS", ""), "FORECASTER_AUTH_METRICS_SUBJECTS")
    if metrics and not metrics_subjects:
        raise AuthMisconfigured("FORECASTER_AUTH_METRICS=true needs FORECASTER_AUTH_METRICS_SUBJECTS (the scrapers)")
    allow_plaintext = env.get("FORECASTER_AUTH_ALLOW_PLAINTEXT", "false").strip().lower() == "true"
    if not allow_plaintext:
        cert, key = env.get("FORECASTER_TLS_CERT", ""), env.get("FORECASTER_TLS_KEY", "")
        if not (cert and key and os.path.isfile(cert) and os.path.isfile(key)):
            raise AuthMisconfigured("FORECASTER_AUTH=tokenreview needs TLS: FORECASTER_TLS_CERT and FORECASTER_TLS_KEY")
    audience = env.get("FORECASTER_AUTH_AUDIENCE", DEFAULT_AUDIENCE).strip() or DEFAULT_AUDIENCE
    open_paths = {"/health", "/ready"} | (set() if metrics else {"/metrics"})
    return AuthConfig(mode=mode, audience=audience, subjects=subjects, metrics_subjects=metrics_subjects if metrics
                      else frozenset(), allow_plaintext=allow_plaintext, open_paths=frozenset(open_paths))


def _jwt_exp(token: str) -> Optional[float]:
    """The token's exp claim (seconds since the epoch), read without verification; None when absent or unusable."""
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        exp = claims.get("exp") if isinstance(claims, dict) else None
    except (IndexError, ValueError, TypeError):
        return None
    if isinstance(exp, bool) or not isinstance(exp, (int, float)) or not math.isfinite(exp):
        return None
    return float(exp)


class TokenReviewer:
    """Checks bearer tokens with TokenReview; caches positive answers as described in the module docstring."""

    def __init__(self, config: AuthConfig, host: Optional[str] = None, sa_dir: str = SA_DIR,
                 request: Callable = bounded_request, monotonic: Callable[[], float] = time.monotonic,
                 wall: Callable[[], float] = time.time, max_entries: int = 1024):
        if host is None:
            h, p = os.getenv("KUBERNETES_SERVICE_HOST"), os.getenv("KUBERNETES_SERVICE_PORT", "443")
            host = f"https://{h}:{p}" if h else ""
        self.config, self.host, self.sa_dir, self.request = config, host.rstrip("/"), sa_dir, request
        self.monotonic, self.wall, self.max_entries = monotonic, wall, max_entries
        self._cache: "collections.OrderedDict[str, tuple]" = collections.OrderedDict()
        self._lock = threading.Lock()

    def _cached(self, key: str) -> Optional[str]:
        with self._lock:
            hit = self._cache.get(key)
            if hit is None:
                return None
            username, expires = hit
            if self.monotonic() >= expires:
                del self._cache[key]                    # expired: never served, outage or not
                return None
            self._cache.move_to_end(key)
            return username

    def _remember(self, key: str, username: str, token: str) -> None:
        exp = _jwt_exp(token)
        if exp is None:
            return                                      # no usable expiry: no caching
        ttl = min(CACHE_TTL_S, exp - self.wall())
        if ttl <= 0:
            return
        with self._lock:
            self._cache[key] = (username, self.monotonic() + ttl)
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)

    def review(self, token: str, allowed: FrozenSet[str]) -> str:
        """The authenticated subject, if it is in allowed. Raises Unauthenticated, Forbidden or AuthUnavailable."""
        key = hashlib.sha256(token.encode()).hexdigest()
        username = self._cached(key)
        if username is None:
            username = self._review_remote(token)
            if username in allowed:
                self._remember(key, username, token)
        if username not in allowed:
            raise Forbidden(f"{username} may not call this endpoint")
        return username

    def _review_remote(self, token: str) -> str:
        if not self.host:
            raise AuthUnavailable("not running in a cluster (KUBERNETES_SERVICE_HOST unset)")
        try:
            with open(os.path.join(self.sa_dir, "token")) as fh:
                own = fh.read().strip()
        except OSError as e:
            raise AuthUnavailable(f"own service account token unreadable: {e}") from e
        ca = os.path.join(self.sa_dir, "ca.crt")
        try:
            ctx = ssl.create_default_context(cafile=ca) if os.path.exists(ca) else ssl.create_default_context()
        except (OSError, ValueError) as e:
            raise AuthUnavailable("TokenReview CA is unreadable or invalid") from e
        body = json.dumps({"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
                           "spec": {"token": token, "audiences": [self.config.audience]}}).encode()
        try:
            status, raw = self.request("POST", f"{self.host}/apis/authentication.k8s.io/v1/tokenreviews", body=body,
                                       headers={"Authorization": f"Bearer {own}", "Content-Type": "application/json"},
                                       deadline_s=REVIEW_DEADLINE_S, max_bytes=REVIEW_MAX_RESPONSE, ssl_context=ctx)
        except (TransportFailure, BodyTooLarge) as e:
            raise AuthUnavailable(f"TokenReview failed: {type(e).__name__}") from e
        if status not in (200, 201):                    # incl. 401/403: the service's own credentials were refused
            raise AuthUnavailable(f"TokenReview answered HTTP {status}")
        try:
            review = json.loads(raw)
        except ValueError as e:
            raise AuthUnavailable("TokenReview answer is not JSON") from e
        st = review.get("status") if isinstance(review, dict) else None
        if not isinstance(st, dict):
            raise AuthUnavailable("TokenReview answer has no status")
        error = st.get("error", "")
        if not isinstance(error, str):
            raise AuthUnavailable("TokenReview error field is malformed")
        if error:
            raise AuthUnavailable("TokenReview reported an error")
        # Kubernetes omits a false boolean on the wire (authenticated is optional, json omitempty).
        authenticated = st.get("authenticated", False)
        if not isinstance(authenticated, bool):
            raise AuthUnavailable("TokenReview authenticated field is malformed")
        audiences = st.get("audiences", [])
        if not isinstance(audiences, list) or any(not isinstance(a, str) for a in audiences):
            raise AuthUnavailable("TokenReview audiences field is malformed")
        if authenticated is not True:
            raise Unauthenticated("the token is not authenticated for this service")
        if self.config.audience not in audiences:
            raise Unauthenticated("the token is not for this service's audience")
        user = st.get("user")
        username = user.get("username") if isinstance(user, dict) else None
        if not isinstance(username, str) or not username:
            raise AuthUnavailable("TokenReview answer has no username")
        return username


class _Admission:
    """At most `slots` concurrent reviews, off the event loop. A request waits at most wait_s for a slot (then
    AuthUnavailable). The slot is released by the submitted future's done callback, when the work finishes, so a
    cancelled request never frees a slot while its review still runs."""

    def __init__(self, slots: int = 4, wait_s: float = 2.0):
        self.slots, self.wait_s = threading.BoundedSemaphore(slots), wait_s
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=slots, thread_name_prefix="tokenreview")

    async def run(self, fn, *args):
        deadline = time.monotonic() + self.wait_s
        while not self.slots.acquire(blocking=False):
            if time.monotonic() >= deadline:
                raise AuthUnavailable("all token review slots are busy")
            await asyncio.sleep(0.05)
        try:
            fut = self.executor.submit(fn, *args)
        except BaseException:
            self.slots.release()
            raise
        fut.add_done_callback(lambda _f: self.slots.release())
        return await asyncio.wrap_future(fut)


async def _respond(send, status: int, error: str, message: str, extra_headers=()) -> None:
    body = json.dumps({"detail": {"error": error, "message": message}}).encode()
    headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
    headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


class AuthMiddleware:
    """ASGI middleware: authenticate every path but the open ones from the headers alone, then read the body through a
    cap. Non-HTTP scopes (lifespan) pass; a websocket is refused (the service has none)."""

    def __init__(self, app, config: AuthConfig, reviewer: TokenReviewer, admission: Optional[_Admission] = None,
                 max_body: int = MAX_BODY):
        self.app, self.config, self.reviewer = app, config, reviewer
        self.admission, self.max_body = admission or _Admission(), max_body

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "websocket":
            return await send({"type": "websocket.close", "code": 1008})
        if scope.get("type") != "http" or scope.get("path") in self.config.open_paths:
            return await self.app(scope, receive, send)
        if scope.get("scheme") != "https" and not self.config.allow_plaintext:
            return await _respond(send, 500, "TLSRequired", "authentication is served only over TLS")
        values = [v for k, v in scope.get("headers", []) if k.lower() == b"authorization"]
        challenge = [(b"www-authenticate", b'Bearer realm="predictive-autoscaler-forecaster"')]
        if len(values) != 1:
            return await _respond(send, 401, "Unauthenticated", "exactly one Authorization header is required",
                                  challenge)
        value = values[0]
        if len(value) > MAX_BEARER or not value.startswith(b"Bearer ") or len(value) <= len(b"Bearer "):
            return await _respond(send, 401, "Unauthenticated", "a bearer token is required", challenge)
        try:
            token = value[len(b"Bearer "):].decode("ascii")
        except UnicodeDecodeError:
            return await _respond(send, 401, "Unauthenticated", "a bearer token is required", challenge)
        allowed = self.config.metrics_subjects if scope["path"] == "/metrics" else self.config.subjects
        try:
            subject = await self.admission.run(self.reviewer.review, token, allowed)
        except Unauthenticated as e:
            return await _respond(send, 401, "Unauthenticated", str(e), challenge)
        except Forbidden as e:
            logger.info("forecasting service: caller refused (%s)", e)
            return await _respond(send, 403, "Forbidden", "this caller may not use this endpoint")
        except AuthUnavailable as e:
            logger.warning("forecasting service: authentication unavailable (%s)", e)
            return await _respond(send, 503, "AuthUnavailable", "authentication is temporarily unavailable")
        # Authenticated: now the body, through a hard cap (chunked bodies included; Content-Length alone is not trusted).
        chunks, size = [], 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > self.max_body:
                return await _respond(send, 413, "BodyTooLarge", f"the request body exceeds {self.max_body} bytes")
            chunks.append(chunk)
            if not message.get("more_body", False):
                break
        body, delivered = b"".join(chunks), False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        scope = dict(scope)
        scope.setdefault("state", {})["auth_subject"] = subject
        return await self.app(scope, replay, send)
