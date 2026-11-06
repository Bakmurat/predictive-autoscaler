"""The statistical forecasting service's app (forecast contract v2, C2.6): TensorFlow-free, never imports api.main.

Endpoints: POST /v2/predict (forecasting.service), GET /health (always answers; reports the journal's state), GET /ready
(the service started), GET /metrics (the bounded forecast-quality series and process metrics). Nothing else: no legacy
/predict, no /train, no /models, no benchmark route.

It reuses the legacy service's contracts unchanged: caller authentication (api/auth.py: TokenReview over TLS, every
path but the open ones protected, the token checked before the body is read), identity (api/identity.py via
forecasting.contracts) and bounded admission (api/admission.py, the same rule as the legacy _run_admitted). Started by
api/serve.py (`python -m api.serve statistical`), which also restarts it when the TLS files rotate.

Configuration (environment): PROMETHEUS_URL (required), FORECAST_STATE_DIR (default /state: the journal and fit
artifacts, a retained volume), FORECAST_STATE_QUOTA_BYTES, FORECAST_ADMISSION, FORECAST_FIT_MODE (worker | off) and
the FORECASTER_AUTH* / FORECASTER_TLS_* settings of api/auth.py. The benchmark switches of the legacy service
(ALLOW_BENCHMARK_EXPERIMENTS, SEASONAL_EXPERIMENT, ENSEMBLE_EXPERIMENT, ...) are REFUSED: present at all, the app does
not start.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Callable, Mapping, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, ProcessCollector, generate_latest

from api.admission import Admission, Busy
from api.auth import AuthMiddleware, TokenReviewer, config_from_env
from api.identity import KubeReader

logger = logging.getLogger("api.statistical")
VERSION = "0.1.0-preview"
BENCHMARK_SWITCHES = ("ALLOW_BENCHMARK_EXPERIMENTS", "SEASONAL_EXPERIMENT", "ENSEMBLE_EXPERIMENT",
                      "ENSEMBLE_HISTORY_HOURS", "VALIDITY_MASK", "COLD_START_CRONJOB")
MAX_BODY = 64 << 10


class Refused(Exception):
    """The configuration asks for something this service never does: it does not start."""


def refuse_benchmark_switches(env: Mapping[str, str]) -> None:
    present = sorted(k for k in BENCHMARK_SWITCHES if k in env)
    if present:
        raise Refused(f"the statistical forecasting service has no benchmark or legacy experiment modes; refusing "
                      f"{', '.join(present)}")


def default_service(env: Mapping[str, str]):
    from forecasting.service import ForecastService, ServiceConfig
    url = env.get("PROMETHEUS_URL", "")
    if not url:
        raise Refused("PROMETHEUS_URL is required")
    cfg = ServiceConfig(prometheus_url=url, state_dir=env.get("FORECAST_STATE_DIR", "/state"),
                        quota_bytes=int(env.get("FORECAST_STATE_QUOTA_BYTES", str(1536 << 20))),
                        fit_mode=env.get("FORECAST_FIT_MODE", "worker"))
    return ForecastService(cfg, reader=KubeReader())


def create_app(env: Mapping[str, str] = os.environ, service_factory: Optional[Callable] = None) -> FastAPI:
    refuse_benchmark_switches(env)
    auth_config = config_from_env(env)
    factory = service_factory or (lambda: default_service(env))
    holder = {"service": None}

    @asynccontextmanager
    async def lifespan(_app):
        holder["service"] = factory()
        try:
            yield
        finally:
            svc, holder["service"] = holder["service"], None
            admission.shutdown()
            if svc is not None:
                svc.close()

    app = FastAPI(title="Predictive Autoscaler statistical forecaster", version=VERSION, lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)
    if auth_config.enabled:
        app.add_middleware(AuthMiddleware, config=auth_config, reviewer=TokenReviewer(auth_config))
    admission = Admission(int(env.get("FORECAST_ADMISSION", "4")))
    registry = CollectorRegistry()
    ProcessCollector(registry=registry)

    def service():
        svc = holder["service"]
        if svc is None:
            raise Busy("the service is starting or stopping")
        return svc

    def error(status: int, reason: str, message: str) -> JSONResponse:
        return JSONResponse({"detail": {"error": reason, "message": message}}, status_code=status)

    @app.get("/health")
    async def health():
        svc = holder["service"]
        return {"status": "healthy", "version": VERSION, **({"state": "starting"} if svc is None else svc.health())}

    @app.get("/ready")
    async def ready():
        if holder["service"] is None:
            return error(503, "Starting", "the service is not started")
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics():
        svc = holder["service"]
        body = generate_latest(registry)
        if svc is not None:
            extra = CollectorRegistry()
            extra.register(svc.metrics)
            body += generate_latest(extra)
        return Response(body, media_type=CONTENT_TYPE_LATEST)

    @app.post("/v2/predict")
    async def predict(request: Request):
        from forecasting.service import ServiceError
        raw = await request.body()
        if len(raw) > MAX_BODY:
            return error(413, "BodyTooLarge", f"the request body exceeds {MAX_BODY} bytes")
        try:
            import json
            body = json.loads(raw)
        except ValueError:
            return error(422, "IdentityMismatch", "the request is not JSON")
        try:
            return await admission.run(service().predict, body)
        except Busy as e:
            return error(503, "StateUnavailable", str(e))
        except ServiceError as e:
            return error(e.status, e.reason, e.message)

    return app


app = create_app()
