"""Validated, pinned model records (metric contract requests-per-second/v1).

A model artifact is `lstm_<key>.pkl` with its sidecar `lstm_<key>.meta.json`, where key = model_key(namespace, name,
metric): bounded, traversal-proof, and namespace-aware. A record is loaded from the exact bytes that are hashed, and it is
accepted only when its sidecar names the same identity as its file key, carries the full provenance (query hash,
contract, autoscaler UID, target UID) and states the digest of those bytes. Artifacts without that provenance (every
model trained before the metric contract) are incompatible: they are not loaded; retrain.

A request pins one record for its whole inference and builds its response from that record only; a record replaced by a
reload is cleaned up when its last user unpins it, never under a running inference. Use `record.lock` to serialize any
request-time mutation of the shared model object.
"""

from __future__ import annotations

import copy
import hashlib
import io
import json
import re
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Dict, Iterator, List, Mapping, Optional

from api.identity import CONTRACT, ResolvedSignal, valid_name, valid_namespace

REQUIRED_META = ("namespace", "name", "metric", "metric_query_sha256", "contract", "pa_uid", "target_uid",
                 "artifact_sha256")
_KEY = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ModelIncompatible(Exception):
    """An artifact that must not be loaded (missing or inconsistent provenance)."""


def model_key(namespace: str, name: str, metric: str) -> str:
    return hashlib.sha256(f"{namespace}/{name}/{metric}".encode("utf-8")).hexdigest()[:32]


def artifact_paths(model_dir: Path, key: str):
    if not _KEY.match(key):
        raise ValueError(f"malformed model key {key!r}")
    return Path(model_dir) / f"lstm_{key}.pkl", Path(model_dir) / f"lstm_{key}.meta.json"


def _joblib_from_bytes(data: bytes):
    import joblib
    return joblib.load(io.BytesIO(data))


class _Lifecycle:
    """Mutable registry bookkeeping, kept apart from the immutable provenance (guarded by the registry's lock)."""

    def __init__(self):
        self.refs = 0
        self.installed = False
        self.retired = False   # terminal: a retired record is never installed again; reloading builds a new record


@dataclass(frozen=True, eq=False)
class ModelRecord:
    key: str
    namespace: str
    name: str
    metric: str
    model: object
    meta: Mapping[str, object]   # read-only copy of the validated sidecar
    artifact_sha256: str
    lock: threading.RLock = field(default_factory=threading.RLock)   # serializes request-time use of `model`
    life: _Lifecycle = field(default_factory=_Lifecycle)

    def incompatibility(self, signal: ResolvedSignal) -> Optional[str]:
        """None if this model was trained for exactly the resolved signal, else the reason (ModelQueryMismatch)."""
        checks = (("namespace", self.namespace, signal.namespace), ("name", self.name, signal.name),
                  ("metric", self.metric, signal.metric),
                  ("metric_query_sha256", self.meta.get("metric_query_sha256"), signal.sha256),
                  ("contract", self.meta.get("contract"), signal.contract),
                  ("pa_uid", self.meta.get("pa_uid"), signal.autoscaler_uid),
                  ("target_uid", self.meta.get("target_uid"), signal.target_uid))
        for field_name, have, want in checks:
            if have != want:
                return f"model {field_name} {have!r} does not match the request's {want!r}"
        return None


def load_record(path: Path, loader: Callable[[bytes], object] = _joblib_from_bytes) -> ModelRecord:
    """Load and validate one artifact. The digest covers the exact bytes the model is built from, and the sidecar is
    read once, so a file replaced during loading is detected as a digest mismatch instead of being mislabelled."""
    path = Path(path)
    m = re.match(r"^lstm_([0-9a-f]{32})\.pkl$", path.name)
    if not m:
        raise ModelIncompatible(f"{path.name}: not a namespace-aware artifact name (legacy model; retrain)")
    key = m.group(1)
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    try:
        meta = json.loads(path.with_name(f"lstm_{key}.meta.json").read_bytes())
    except FileNotFoundError as e:
        raise ModelIncompatible(f"{path.name}: no provenance sidecar") from e
    except ValueError as e:
        raise ModelIncompatible(f"{path.name}: unreadable provenance sidecar: {e}") from e
    if not isinstance(meta, dict):
        raise ModelIncompatible(f"{path.name}: provenance sidecar is not an object")
    missing = [k for k in REQUIRED_META if not isinstance(meta.get(k), str) or not meta.get(k)]
    if missing:
        raise ModelIncompatible(f"{path.name}: provenance lacks {', '.join(missing)} (trained before the metric "
                                f"contract; retrain)")
    ns, name, metric = meta["namespace"], meta["name"], meta["metric"]
    if not (valid_namespace(ns) and valid_name(name) and metric == "requests"):
        raise ModelIncompatible(f"{path.name}: malformed identity {ns!r}/{name!r}/{metric!r}")
    if model_key(ns, name, metric) != key:
        raise ModelIncompatible(f"{path.name}: sidecar identity {ns}/{name}/{metric} does not match the file key")
    if meta["contract"] != CONTRACT or not _SHA256.match(meta["metric_query_sha256"]):
        raise ModelIncompatible(f"{path.name}: unsupported contract or malformed query hash")
    if meta["artifact_sha256"] != digest:
        raise ModelIncompatible(f"{path.name}: artifact digest does not match its sidecar (replaced during loading?)")
    return ModelRecord(key=key, namespace=ns, name=name, metric=metric, model=loader(data),
                       meta=MappingProxyType(copy.deepcopy(meta)), artifact_sha256=digest)


def _scaler_range(model) -> Optional[dict]:
    scaler = getattr(model, "scaler", None)
    try:
        return {"center": float(scaler.center_[0]), "scale": float(scaler.scale_[0])}
    except (AttributeError, IndexError, TypeError):
        return None


class ModelRegistry:
    """The service's loaded models, by key. Install replaces atomically; pin keeps a record alive while in use."""

    def __init__(self, cleanup: Optional[Callable[[ModelRecord], None]] = None):
        self._records: Dict[str, ModelRecord] = {}
        self._lock = threading.Lock()
        self._cleanup = cleanup or (lambda rec: None)

    def install(self, record: ModelRecord) -> None:
        """Install a freshly loaded record (never a retired one, never one already installed)."""
        with self._lock:
            if record.life.retired or record.life.installed:
                raise ValueError(f"record {record.key} was already installed once; load the artifact again")
            old = self._records.get(record.key)
            self._records[record.key] = record
            record.life.installed = True
            dispose = old if old is not None and self._retire(old) else None
        if dispose is not None:
            self._cleanup(dispose)

    def remove(self, key: str) -> None:
        with self._lock:
            old = self._records.pop(key, None)
            dispose = old if old is not None and self._retire(old) else None
        if dispose is not None:
            self._cleanup(dispose)

    def _retire(self, rec: ModelRecord) -> bool:
        """Mark retired (terminal); True when it can be cleaned up now (nobody holds it). Caller holds the lock."""
        rec.life.retired = True
        return rec.life.refs == 0

    @contextmanager
    def pin(self, key: str) -> Iterator[Optional[ModelRecord]]:
        with self._lock:
            rec = self._records.get(key)
            if rec is not None:
                rec.life.refs += 1
        try:
            yield rec
        finally:
            if rec is not None:
                with self._lock:
                    rec.life.refs -= 1
                    dispose = rec.life.retired and rec.life.refs == 0
                if dispose:
                    self._cleanup(rec)

    def snapshot(self) -> List[dict]:
        """Identity and provenance of the installed records (for /health, /models, the cold-start check)."""
        with self._lock:
            return [{"key": r.key, "namespace": r.namespace, "name": r.name, "metric": r.metric,
                     "artifact_sha256": r.artifact_sha256, "metric_query_sha256": r.meta.get("metric_query_sha256"),
                     "trained_at": r.meta.get("trained_at"), "provenance": copy.deepcopy(dict(r.meta)),
                     "scaler_range": _scaler_range(r.model)} for r in self._records.values()]

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._records
