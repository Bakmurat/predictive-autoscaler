"""A failed reload must never leave the service without a model (Codex C-47), now through the model registry (B4b).

The replacement is loaded and validated before it replaces the incumbent; publication is two steps (artifact renamed,
sidecar written after), so a reload can catch a new artifact beside a stale sidecar: that pair is refused and retried
when either file changes. A replaced record is released once, after its last user.
"""

import hashlib
import json
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tests.b4_helpers import install_model, make_signal  # noqa: E402

SIGNAL = make_signal(namespace="demo", name="nginx-test")


@pytest.fixture()
def predictor(tmp_path, monkeypatch):
    import api.main as main
    from api.registry import ModelRegistry

    released = []
    p = object.__new__(main.LSTMPredictor)
    p.model_dir = tmp_path
    p.registry = ModelRegistry(cleanup=lambda rec: released.append(rec))
    p.record_stamps = {}
    p._reload_locks, p._reload_guard, p._reload_retry = {}, __import__("threading").Lock(), {}
    return p, main, released


def _model(tag):
    m = types.SimpleNamespace(tag=tag)
    m.model = types.SimpleNamespace(output_shape=(None, 6))   # the current multi-step format
    return m


def _write_pair(tmp_path, payload, declared=None, bump=0):
    """Publish an artifact and its sidecar for SIGNAL; `declared` overrides the sidecar's artifact digest."""
    from api.registry import artifact_paths, model_key
    key = model_key(SIGNAL.namespace, SIGNAL.name, SIGNAL.metric)
    pkl, meta = artifact_paths(tmp_path, key)
    pkl.write_bytes(payload)
    meta.write_text(json.dumps({
        "namespace": SIGNAL.namespace, "name": SIGNAL.name, "metric": SIGNAL.metric,
        "metric_query_sha256": SIGNAL.sha256, "contract": SIGNAL.contract, "pa_uid": SIGNAL.autoscaler_uid,
        "target_uid": SIGNAL.target_uid, "trained_at": "2026-10-09T18:00:00",
        "artifact_sha256": declared or hashlib.sha256(payload).hexdigest()}))
    for f in (pkl, meta):   # distinct stamps for consecutive publications in one test
        st = f.stat()
        os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns + bump))
    return key


def _with_loader(monkeypatch, main, loader):
    real = main.load_record
    monkeypatch.setattr(main, "load_record", lambda path: real(path, loader=loader))


def test_incumbent_survives_a_corrupt_replacement(predictor, tmp_path, monkeypatch):
    p, main, released = predictor
    keep = install_model(p, _model("incumbent"), SIGNAL)
    key = _write_pair(tmp_path, b"corrupt-bytes")
    _with_loader(monkeypatch, main, lambda data: (_ for _ in ()).throw(ValueError("corrupt")))
    p._reload_if_changed(key)
    with p.registry.pin(key) as rec:
        assert rec is keep, "the incumbent must keep serving when the replacement cannot be loaded"
    assert released == []


def test_mismatched_sidecar_is_refused_and_retried(predictor, tmp_path, monkeypatch):
    p, main, released = predictor
    keep = install_model(p, _model("incumbent"), SIGNAL)
    loads = []
    _with_loader(monkeypatch, main, lambda data: loads.append(data) or _model("new"))
    key = _write_pair(tmp_path, b"new-bytes", declared="0" * 64)   # sidecar of another artifact: mid-publication
    p._reload_if_changed(key)
    with p.registry.pin(key) as rec:
        assert rec is keep and loads == []
    p._reload_if_changed(key)   # nothing changed: not re-read
    assert loads == []
    _write_pair(tmp_path, b"new-bytes", bump=1_000_000)          # the trainer finishes publishing
    p._reload_if_changed(key)
    with p.registry.pin(key) as rec:
        assert rec.model.tag == "new" and rec.artifact_sha256 == hashlib.sha256(b"new-bytes").hexdigest()
    assert released == [keep]


def test_matching_pair_is_swapped_in(predictor, tmp_path, monkeypatch):
    p, main, released = predictor
    keep = install_model(p, _model("incumbent"), SIGNAL)
    _with_loader(monkeypatch, main, lambda data: _model("new"))
    key = _write_pair(tmp_path, b"new-bytes")
    with p.registry.pin(key) as in_flight:          # a request still running on the incumbent
        p._reload_if_changed(key)
        assert in_flight is keep and released == []
        with p.registry.pin(key) as rec:
            assert rec.model.tag == "new"
    assert released == [keep]


def test_old_format_does_not_rescan_the_same_file(predictor, tmp_path, monkeypatch):
    p, main, released = predictor
    loads = []

    def old_format(data):
        loads.append(data)
        m = _model("old")
        m.model = types.SimpleNamespace(output_shape=(None, 1))   # Dense(1): retrain
        return m

    _with_loader(monkeypatch, main, old_format)
    key = _write_pair(tmp_path, b"old-bytes")
    p._reload_if_changed(key)
    p._reload_if_changed(key)
    with p.registry.pin(key) as rec:
        assert rec is None
    assert len(loads) == 1, "an unusable artifact would be re-read on every request"


def test_a_load_superseded_while_loading_is_not_installed(predictor, tmp_path, monkeypatch):
    # Codex r19: A loads pair 1; pair 2 is published meanwhile; A must not install pair 1 over it.
    import threading
    p, main, released = predictor
    started, go = threading.Event(), threading.Event()

    def slow_then_tag(data):
        if data == b"pair-1":
            started.set()
            go.wait(10)
        return _model(data.decode())

    _with_loader(monkeypatch, main, slow_then_tag)
    key = _write_pair(tmp_path, b"pair-1")
    t = threading.Thread(target=p._reload_if_changed, args=(key,))
    t.start()
    assert started.wait(10)
    p._reload_if_changed(key)                                   # concurrent caller: does not wait, serves the incumbent
    _write_pair(tmp_path, b"pair-2", bump=1_000_000)            # a newer publication during A's load
    go.set()
    t.join(10)
    with p.registry.pin(key) as rec:
        assert rec is None, "the superseded pair-1 load must not be installed"
    p._reload_if_changed(key)
    with p.registry.pin(key) as rec:
        assert rec.model.tag == "pair-2"


def test_a_transient_load_failure_is_retried_after_a_backoff(predictor, tmp_path, monkeypatch):
    p, main, released = predictor
    attempts = []

    def flaky(data):
        attempts.append(data)
        if len(attempts) == 1:
            raise OSError("volume hiccup")
        return _model("ok")

    _with_loader(monkeypatch, main, flaky)
    key = _write_pair(tmp_path, b"pair")
    p._reload_if_changed(key)
    p._reload_if_changed(key)                 # inside the backoff: not retried yet
    assert len(attempts) == 1
    with p.registry.pin(key) as rec:
        assert rec is None
    not_before, backoff, stamp = p._reload_retry[key]
    p._reload_retry[key] = (0.0, backoff, stamp)   # the backoff has passed
    p._reload_if_changed(key)
    with p.registry.pin(key) as rec:
        assert rec.model.tag == "ok" and len(attempts) == 2
