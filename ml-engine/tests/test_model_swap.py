"""A failed reload must never leave the service without a model.

Codex C-47: _check_and_reload_model() freed the incumbent BEFORE loading the
replacement and then logged "keeping old model" when the load failed -- which was
false. Publication is also two steps (artifact renamed, sidecar written after), so
a reload can catch a new artifact beside a stale sidecar.
"""

import json
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture()
def predictor(tmp_path, monkeypatch):
    monkeypatch.setenv("MODEL_DIR", str(tmp_path))
    import api.main as main

    p = object.__new__(main.LSTMPredictor)
    p.trained_models = {}
    p.model_train_times = {}
    p.model_file_mtimes = {}
    p.model_meta = {}
    p._model_locks = {}
    p._model_locks_guard = __import__("threading").Lock()
    p.model_dir = tmp_path
    p.validation_metadata = {}
    return p, main


def _incumbent():
    m = types.SimpleNamespace()
    m.model = types.SimpleNamespace(output_shape=(None, 6))
    m.tag = "incumbent"
    return m


def _write_artifact(tmp_path, key, payload=b"artifact-bytes"):
    path = tmp_path / f"lstm_{key}.pkl"
    path.write_bytes(payload)
    return path


def test_incumbent_survives_a_corrupt_replacement(predictor, tmp_path, monkeypatch):
    p, main = predictor
    key = "nginx-test_requests"
    keep = _incumbent()
    p.trained_models[key] = keep
    p.model_file_mtimes[key] = 0

    _write_artifact(tmp_path, key)
    monkeypatch.setattr(main.joblib, "load", lambda *_a, **_k: (_ for _ in ()).throw(ValueError("corrupt")))

    p._check_and_reload_model(key)

    assert p.trained_models[key] is keep, "the incumbent was discarded by a failed reload"
    assert p.trained_models[key].tag == "incumbent"


def test_mismatched_sidecar_is_refused_and_retried(predictor, tmp_path, monkeypatch):
    """Artifact renamed, sidecar not yet rewritten: keep serving, retry later."""
    p, main = predictor
    key = "nginx-test_requests"
    keep = _incumbent()
    p.trained_models[key] = keep
    p.model_file_mtimes[key] = 0

    _write_artifact(tmp_path, key)
    (tmp_path / f"lstm_{key}.meta.json").write_text(json.dumps({"artifact_sha256": "0" * 64}))
    monkeypatch.setattr(main.joblib, "load", lambda *_a, **_k: _incumbent())

    p._check_and_reload_model(key)

    assert p.trained_models[key] is keep, "loaded an artifact whose sidecar did not match it"
    # mtime not advanced, so the next call retries once publication completes.
    assert p.model_file_mtimes[key] == 0


def test_matching_pair_is_swapped_in(predictor, tmp_path, monkeypatch):
    import hashlib

    p, main = predictor
    key = "nginx-test_requests"
    p.trained_models[key] = _incumbent()
    p.model_file_mtimes[key] = 0

    payload = b"new-artifact"
    path = _write_artifact(tmp_path, key, payload)
    digest = hashlib.sha256(payload).hexdigest()
    (tmp_path / f"lstm_{key}.meta.json").write_text(
        json.dumps({"artifact_sha256": digest, "trained_at": "2026-03-02T12:00:00"})
    )

    replacement = _incumbent()
    replacement.tag = "replacement"
    monkeypatch.setattr(main.joblib, "load", lambda *_a, **_k: replacement)
    monkeypatch.setattr(p, "_release_model_object", lambda *_a, **_k: None)
    monkeypatch.setattr(p, "_get_rss_bytes", lambda: 0)

    p._check_and_reload_model(key)

    assert p.trained_models[key].tag == "replacement"
    assert p.model_meta[key]["artifact_sha256"] == digest
    assert p.model_file_mtimes[key] == path.stat().st_mtime


def test_old_format_does_not_rescan_the_same_file(predictor, tmp_path, monkeypatch):
    p, main = predictor
    key = "nginx-test_requests"
    p.model_file_mtimes[key] = 0
    path = _write_artifact(tmp_path, key)

    old = types.SimpleNamespace(model=types.SimpleNamespace(output_shape=(None, 1)))
    monkeypatch.setattr(main.joblib, "load", lambda *_a, **_k: old)

    p._check_and_reload_model(key)

    assert key not in p.trained_models
    assert p.model_file_mtimes[key] == path.stat().st_mtime, (
        "an unusable artifact would be re-read on every request"
    )


def test_model_lock_is_per_key_and_reentrant(predictor):
    p, _ = predictor
    a1 = p._model_lock("a")
    a2 = p._model_lock("a")
    b = p._model_lock("b")
    assert a1 is a2 and a1 is not b
    with a1:
        with a2:  # RLock: the reload swap may nest inside a held inference lock
            pass
