"""The statistical forecasting service's app (forecast contract v2, C2.6): TensorFlow-free, never imports api.main.

Endpoints: POST /v2/predict (forecasting.service), GET /health (liveness: always answers; reports the journal's state),
GET /ready (503 until started and while the journal cannot support issuance, with the reason), GET /metrics (the
bounded forecast-quality series, the service's own and process metrics). Nothing else: no legacy /predict, no /train,
no /models, no benchmark route.

Liveness never follows the journal: a full or corrupt state volume is not repaired by a restart, so a failing /health
would only crash-loop the pod. The service recovers the journal itself: a background task calls its maintain() every
MAINTENANCE_SECONDS, in a worker thread (a reopen reads the whole journal, and /health must keep answering meanwhile).

It reuses the legacy service's contracts unchanged: caller authentication (api/auth.py: TokenReview over TLS, every
path but the open ones protected, the token checked before the body is read), identity (api/identity.py via
forecasting.contracts) and bounded admission (api/admission.py, the same rule as the legacy _run_admitted). Started by
api/serve.py (`python -m api.serve statistical`), which also restarts it when the TLS files rotate.

Configuration (environment): PROMETHEUS_URL (required), FORECAST_STATE_DIR (default /state: the journal and fit
artifacts, a retained volume), FORECAST_STATE_QUOTA_BYTES, FORECAST_ADMISSION, FORECAST_FIT_MODE (worker | off),
FORECAST_MAX_AUTOSCALERS (default 8: the autoscalers registered at once; one more gets 503 AutoscalerLimitReached),
FORECAST_AUTOSCALER_IDLE_SECONDS (default 86400: a registration without a request this long is released; 0: never) and
the FORECASTER_AUTH* / FORECASTER_TLS_* settings of api/auth.py. FORECAST_LAB_QUALIFICATION names a reviewed
lab-qualification/v1 file (forecasting.qualification): honoured only by the evaluation build and image digest
(FORECAST_IMAGE_DIGEST) it names; in any other image, or malformed, expired or for another source tree, the app does
not start. FORECAST_DEMONSTRATION_QUALIFICATION and FORECAST_DEMONSTRATION_CONTRACT name a reviewed
demonstration-qualification/v1 file and the demonstration-contract/v1 file it is bound to (forecasting.demonstration):
both or neither, under the same build and digest rules, never beside a lab qualification and never once the release
gates have passed; anything else and the app does not start. The benchmark switches of the legacy service
(ALLOW_BENCHMARK_EXPERIMENTS, SEASONAL_EXPERIMENT,
ENSEMBLE_EXPERIMENT, ...) are REFUSED: present at all, the app does not start.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
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
MAINTENANCE_SECONDS = 5.0      # how often the service's housekeeping runs (journal recovery, when an attempt is due)


MIN_STATE_QUOTA_BYTES, MAX_STATE_QUOTA_BYTES = 1 << 20, 1 << 40    # FORECAST_STATE_QUOTA_BYTES: 1 MiB to 1 TiB
MAX_ADMISSION = 64                                                 # FORECAST_ADMISSION: 1 to 64 requests at once


class Refused(Exception):
    """The configuration asks for something this service never does: it does not start."""


def refuse_benchmark_switches(env: Mapping[str, str]) -> None:
    present = sorted(k for k in BENCHMARK_SWITCHES if k in env)
    if present:
        raise Refused(f"the statistical forecasting service has no benchmark or legacy experiment modes; refusing "
                      f"{', '.join(present)}")


def _int_env(env: Mapping[str, str], name: str, default: int, minimum: int, maximum: Optional[int] = None) -> int:
    try:
        value = int(env.get(name, str(default)))
    except ValueError:
        value = None
    if value is None or value < minimum or (maximum is not None and value > maximum):
        bounds = f"at least {minimum}" if maximum is None else f"{minimum} to {maximum}"
        raise Refused(f"{name} must be an integer of {bounds}, got {env.get(name)!r}")
    return value


def default_service(env: Mapping[str, str]):
    from forecasting.service import ForecastService, ServiceConfig
    url = env.get("PROMETHEUS_URL", "")
    if not url:
        raise Refused("PROMETHEUS_URL is required")
    from forecasting import demonstration as D
    from forecasting import qualification as Q
    from forecasting import service as S
    build = Q.read_build_info()
    demo_paths = (env.get("FORECAST_DEMONSTRATION_QUALIFICATION") or None,
                  env.get("FORECAST_DEMONSTRATION_CONTRACT") or None)
    if any(demo_paths) and env.get("FORECAST_LAB_QUALIFICATION"):
        raise Refused("a lab qualification and a demonstration qualification are both configured: at most one of the "
                      "two documents")
    if any(demo_paths) and S.RELEASE_GATES_PASSED:
        raise Refused("the release gates have passed: a demonstration qualification is refused (no silent precedence)")
    try:
        lab = Q.load(env.get("FORECAST_LAB_QUALIFICATION") or None, build, int(time.time()),
                     env.get("FORECAST_IMAGE_DIGEST") or None)
    except Q.QualificationRefused as e:
        raise Refused(f"the lab qualification is refused: {e}") from e
    try:
        demo = D.load(*demo_paths, build, int(time.time()), env.get("FORECAST_IMAGE_DIGEST") or None)
    except Q.QualificationRefused as e:
        raise Refused(f"the demonstration qualification is refused: {e}") from e
    cfg = ServiceConfig(prometheus_url=url, state_dir=env.get("FORECAST_STATE_DIR", "/state"),
                        quota_bytes=_int_env(env, "FORECAST_STATE_QUOTA_BYTES", 1536 << 20, MIN_STATE_QUOTA_BYTES,
                                             MAX_STATE_QUOTA_BYTES),
                        fit_mode=env.get("FORECAST_FIT_MODE", "worker"),
                        max_autoscalers=_int_env(env, "FORECAST_MAX_AUTOSCALERS", 8, 1),
                        slot_idle_seconds=_int_env(env, "FORECAST_AUTOSCALER_IDLE_SECONDS", 86400, 0),
                        build=build, lab_qualification=lab, demonstration_qualification=demo)
    if lab is not None:
        logger.warning("LAB QUALIFICATION %s active for cluster %s, namespaces %s, %d targets, until %s (evaluation "
                       "build %s)", lab.sha256, lab.cluster_uid, sorted(lab.namespaces), len(lab.targets),
                       lab.expires, build.source_tree)
    if demo is not None:
        logger.warning("DEMONSTRATION QUALIFICATION %s (contract %s) active for cluster %s, namespace %s, %d targets, "
                       "families %s, until %s (evaluation build %s): a demonstration basis, not a release and not an "
                       "evaluation qualification", demo.sha256, demo.contract_sha256, demo.cluster_uid, demo.namespace,
                       len(demo.targets), sorted(demo.families), demo.expires, build.source_tree)
    return ForecastService(cfg, reader=KubeReader())


def create_app(env: Mapping[str, str] = os.environ, service_factory: Optional[Callable] = None,
               maintenance_seconds: float = MAINTENANCE_SECONDS) -> FastAPI:
    refuse_benchmark_switches(env)
    auth_config = config_from_env(env)
    factory = service_factory or (lambda: default_service(env))
    holder = {"service": None}

    async def maintenance(svc):
        while True:
            await asyncio.sleep(maintenance_seconds)
            try:
                await asyncio.to_thread(svc.maintain)
            except Exception:                    # housekeeping must outlive any one failed pass
                logger.exception("the service's maintenance pass failed")

    @asynccontextmanager
    async def lifespan(_app):
        holder["service"] = factory()
        task = asyncio.create_task(maintenance(holder["service"]))
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            svc, holder["service"] = holder["service"], None
            admission.shutdown()
            if svc is not None:
                svc.close()

    app = FastAPI(title="Predictive Autoscaler statistical forecaster", version=VERSION, lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)
    if auth_config.enabled:
        app.add_middleware(AuthMiddleware, config=auth_config, reviewer=TokenReviewer(auth_config))
    admission = Admission(_int_env(env, "FORECAST_ADMISSION", 4, 1, MAX_ADMISSION))
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
        svc = holder["service"]
        if svc is None:
            return error(503, "Starting", "the service is not started")
        if not svc.ready():                  # liveness (/health) stays separate: the process itself is fine
            h = svc.health()
            why = "the forecast journal cannot support issuance"
            if h.get("state_problem"):
                why += f": {h['state_problem']}"
            if (h.get("journal") or {}).get("permanent_failure"):
                why += " (permanent failure: the automatic recovery attempts are used up, operator action required)"
            return error(503, "StateUnavailable", why)
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics():
        svc = holder["service"]
        body = generate_latest(registry)
        if svc is not None:
            extra = CollectorRegistry()
            extra.register(svc.metrics)
            basis = getattr(svc, "qualification_metrics", None)
            if basis is not None:
                extra.register(basis)
            extra.register(svc.service_metrics)
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
