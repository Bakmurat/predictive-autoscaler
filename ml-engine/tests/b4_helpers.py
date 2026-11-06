"""Test helpers for the metric contract (B4b): a resolved signal, a registry record for it, and the product request."""

import hashlib
from datetime import datetime
from types import MappingProxyType

from api.identity import CONTRACT, ResolvedSignal
from api.registry import ModelRecord, model_key


def make_signal(namespace="default", name="nginx-test", query=None, autoscaler_uid="pa-uid", target_uid="dep-uid",
                generation=1):
    q = query or (f'sum(rate(istio_requests_total{{reporter="destination",destination_workload="{name}",'
                  f'destination_workload_namespace="{namespace}"}}[1m]))')
    return ResolvedSignal(namespace=namespace, name=name, metric="requests", query=q,
                          sha256=hashlib.sha256(q.encode()).hexdigest(), contract=CONTRACT,
                          autoscaler_uid=autoscaler_uid, target_uid=target_uid, generation=generation)


def install_model(predictor, model, signal, artifact_sha256="a" * 64, trained_at=None, **meta_extra):
    """Install `model` as the served record for `signal` (bypasses the file loader, keeps the full provenance)."""
    meta = {"namespace": signal.namespace, "name": signal.name, "metric": signal.metric,
            "metric_query_sha256": signal.sha256, "contract": signal.contract, "pa_uid": signal.autoscaler_uid,
            "target_uid": signal.target_uid, "artifact_sha256": artifact_sha256,
            "trained_at": trained_at if trained_at is not None else datetime.utcnow().isoformat()}
    meta.update(meta_extra)
    rec = ModelRecord(key=model_key(signal.namespace, signal.name, signal.metric), namespace=signal.namespace,
                      name=signal.name, metric=signal.metric, model=model, meta=MappingProxyType(meta),
                      artifact_sha256=artifact_sha256)
    predictor.registry.install(rec)
    return rec


def product_body(signal, horizon=60):
    return {"application": signal.name, "namespace": signal.namespace, "metric_type": "requests",
            "horizon_minutes": horizon, "autoscaler_name": signal.name, "autoscaler_namespace": signal.namespace,
            "autoscaler_uid": signal.autoscaler_uid, "autoscaler_generation": signal.generation,
            "target_uid": signal.target_uid, "metric_query_sha256": signal.sha256, "contract": signal.contract}


def route_product_path(monkeypatch, api_main, signal, history):
    """The service resolves `signal` and reads `history` (list of {"timestamp", "value" (req/min)})."""
    monkeypatch.setattr(api_main, "resolve_signal", lambda request, reader: signal)
    monkeypatch.setattr(api_main, "query_history", lambda url, query: list(history))
