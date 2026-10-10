"""Resolve a forecast request to the PredictiveAutoscaler's compiled query (metric contract requests-per-second/v1).

The operator sends a reference (autoscaler namespace/name, UID, generation, target UID, query hash, contract), never a
query. This module reads the PredictiveAutoscaler and its target Deployment straight from the Kubernetes API (no cache,
the service account's token re-read on every call) and accepts the request only when everything agrees:
  generation: request == metadata.generation == status.metricSource.observedGeneration;
  UID: request == PA; target UID: request == status == the live Deployment;
  hash: request == status == sha256(status query); contract: the supported one everywhere;
  target: the PA's spec target, in the PA's namespace (the request's application/namespace must name it).
A resolved incompatibility (including a PA or Deployment that does not exist) is ProvenanceRefused (HTTP 422); a lookup
that could not be completed (transport, timeout, 401/403, 5xx) is LookupFailed (HTTP 503).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import ssl
from dataclasses import dataclass
from typing import Callable, Optional

from data.bounded_http import BodyTooLarge, TransportFailure, bounded_get

CONTRACT = "requests-per-second/v1"
API_GROUP = "autoscaler.example.com"
API_VERSION = "v1alpha1"
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
DEADLINE_S = 8.0               # the whole exchange, headers and body
MAX_RESPONSE_BYTES = 1 << 20   # one object

_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DNS_SUBDOMAIN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ProvenanceRefused(Exception):
    """The request does not match the autoscaler's current compiled query (HTTP 422)."""


class LookupFailed(Exception):
    """The autoscaler or its target could not be read (HTTP 503)."""


@dataclass(frozen=True)
class ResolvedSignal:
    namespace: str          # the target's namespace (= the autoscaler's)
    name: str               # the target Deployment
    metric: str             # "requests"
    query: str
    sha256: str
    contract: str
    autoscaler_uid: str
    target_uid: str
    generation: int


def valid_namespace(v) -> bool:
    return isinstance(v, str) and len(v) <= 63 and bool(_DNS_LABEL.match(v))


def valid_name(v) -> bool:
    return isinstance(v, str) and len(v) <= 253 and bool(_DNS_SUBDOMAIN.match(v))


class KubeReader:
    """GET JSON objects from the Kubernetes API with the pod's service account (bounded exchange). None for 404."""

    def __init__(self, host: Optional[str] = None, sa_dir: str = SA_DIR, fetch: Callable = bounded_get):
        if host is None:
            h, p = os.getenv("KUBERNETES_SERVICE_HOST"), os.getenv("KUBERNETES_SERVICE_PORT", "443")
            host = f"https://{h}:{p}" if h else ""
        self.host, self.sa_dir, self.fetch = host.rstrip("/"), sa_dir, fetch

    def get(self, path: str) -> Optional[dict]:
        if not self.host:
            raise LookupFailed("not running in a cluster (KUBERNETES_SERVICE_HOST unset)")
        try:
            with open(os.path.join(self.sa_dir, "token")) as fh:  # re-read: projected tokens rotate
                token = fh.read().strip()
        except OSError as e:
            raise LookupFailed(f"service account token unreadable: {e}") from e
        ca = os.path.join(self.sa_dir, "ca.crt")
        ctx = ssl.create_default_context(cafile=ca) if os.path.exists(ca) else ssl.create_default_context()
        try:
            status, body = self.fetch(f"{self.host}{path}", headers={"Authorization": f"Bearer {token}"},
                                      deadline_s=DEADLINE_S, max_bytes=MAX_RESPONSE_BYTES, ssl_context=ctx)
        except (TransportFailure, BodyTooLarge) as e:
            raise LookupFailed(f"Kubernetes API read failed: {e}") from e
        if status == 404:
            return None
        if status != 200:
            raise LookupFailed(f"Kubernetes API answered HTTP {status} for {path}")
        try:
            obj = json.loads(body)
        except ValueError as e:
            raise LookupFailed(f"Kubernetes API answer is not JSON: {e}") from e
        if not isinstance(obj, dict):
            raise LookupFailed("Kubernetes API answer is not an object")
        return obj


def _obj(parent, field: str) -> dict:
    """A nested object, or {} when absent; anything else in its place is a malformed object (refused)."""
    v = parent.get(field) if isinstance(parent, dict) else None
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise ProvenanceRefused(f"malformed object: {field} is not an object")
    return v


def _require(request: dict, field: str, check: Callable[[object], bool], what: str):
    v = request.get(field)
    if not check(v):
        raise ProvenanceRefused(f"{field} missing or malformed ({what} required)")
    return v


def resolve_signal(request: dict, reader: KubeReader) -> ResolvedSignal:
    """Validate the request's reference against the live objects and return the compiled query to use."""
    if not isinstance(request, dict):
        raise ProvenanceRefused("the request is not an object")
    pa_ns = _require(request, "autoscaler_namespace", valid_namespace, "a namespace")
    pa_name = _require(request, "autoscaler_name", valid_name, "a name")
    req_uid = _require(request, "autoscaler_uid", lambda v: isinstance(v, str) and v != "", "the autoscaler UID")
    req_gen = _require(request, "autoscaler_generation",
                       lambda v: isinstance(v, int) and not isinstance(v, bool) and v > 0, "a positive generation")
    req_target_uid = _require(request, "target_uid", lambda v: isinstance(v, str) and v != "", "the target UID")
    req_sha = _require(request, "metric_query_sha256", lambda v: isinstance(v, str) and bool(_SHA256.match(v)),
                       "a sha256")
    if request.get("contract") != CONTRACT:
        raise ProvenanceRefused(f"contract {request.get('contract')!r} is not {CONTRACT!r}")
    metric = request.get("metric_type", "requests")
    if metric != "requests":
        raise ProvenanceRefused("metric_type must be 'requests'")

    pa = reader.get(f"/apis/{API_GROUP}/{API_VERSION}/namespaces/{pa_ns}/predictiveautoscalers/{pa_name}")
    if pa is None:
        raise ProvenanceRefused(f"PredictiveAutoscaler {pa_ns}/{pa_name} not found")
    meta, spec, status = _obj(pa, "metadata"), _obj(pa, "spec"), _obj(pa, "status")
    if meta.get("uid") != req_uid:
        raise ProvenanceRefused("the autoscaler was replaced (UID differs)")
    ms = status.get("metricSource")
    if not isinstance(ms, dict):
        raise ProvenanceRefused("the autoscaler has no compiled metric source")
    gens = (req_gen, meta.get("generation"), ms.get("observedGeneration"))
    if not (gens[0] == gens[1] == gens[2]):
        raise ProvenanceRefused(f"generation mismatch (request, object, compiled) = {gens}")
    query, sha = ms.get("query"), ms.get("sha256")
    if not isinstance(query, str) or not query or not isinstance(sha, str):
        raise ProvenanceRefused("the compiled metric source is incomplete")
    if hashlib.sha256(query.encode("utf-8")).hexdigest() != sha or sha != req_sha:
        raise ProvenanceRefused("query hash mismatch")
    if ms.get("contract") != CONTRACT:
        raise ProvenanceRefused(f"compiled contract {ms.get('contract')!r} is not {CONTRACT!r}")

    target = _obj(spec, "targetDeployment")
    t_name, t_ns = target.get("name"), target.get("namespace") or pa_ns
    if t_ns != pa_ns:
        raise ProvenanceRefused("the target must be in the autoscaler's namespace")
    if not valid_name(t_name):
        raise ProvenanceRefused("the autoscaler's target name is malformed")
    if request.get("application") != t_name or request.get("namespace", t_ns) != t_ns:
        raise ProvenanceRefused("application/namespace do not name the autoscaler's target")
    dep = reader.get(f"/apis/apps/v1/namespaces/{t_ns}/deployments/{t_name}")
    if dep is None:
        raise ProvenanceRefused(f"target Deployment {t_ns}/{t_name} not found")
    live_uid = _obj(dep, "metadata").get("uid")
    if not isinstance(live_uid, str) or not live_uid:
        raise ProvenanceRefused("the target Deployment has no UID")
    if not (req_target_uid == ms.get("targetUID") == live_uid):
        raise ProvenanceRefused("the target Deployment was replaced (UID differs)")
    return ResolvedSignal(namespace=t_ns, name=t_name, metric=metric, query=query, sha256=sha, contract=CONTRACT,
                          autoscaler_uid=req_uid, target_uid=live_uid, generation=req_gen)
