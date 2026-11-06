"""B4c: the trainer trains for one PredictiveAutoscaler, on exactly its compiled query, and publishes a servable model."""

import hashlib
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.identity import CONTRACT, ProvenanceRefused, resolve_autoscaler  # noqa: E402
from api.registry import artifact_paths, load_record, model_key  # noqa: E402
from tests.b4_helpers import make_signal  # noqa: E402
from tests.test_metric_contract import QUERY, SHA, FakeReader, pa_object  # noqa: E402
from training import train_lstm_from_vm as t  # noqa: E402
from training import train_nginx_test as entry  # noqa: E402

SIGNAL = make_signal(namespace="shop", name="web", query=QUERY, autoscaler_uid="pa-uid", target_uid="dep-uid",
                     generation=3)


# --- resolving the training target --------------------------------------------------------------------------------

def test_the_training_target_resolves_to_the_compiled_query():
    sig = resolve_autoscaler(FakeReader(), "shop", "web-pa")
    assert (sig.namespace, sig.name, sig.query, sig.sha256) == ("shop", "web", QUERY, SHA)
    assert (sig.autoscaler_uid, sig.target_uid, sig.generation) == ("pa-uid", "dep-uid", 3)


@pytest.mark.parametrize("reader", [
    FakeReader(pa=pa_object(**{"metadata.generation": 4})),                      # compiled for an older generation
    FakeReader(pa=pa_object(**{"status.metricSource": None})),                   # not compiled (invalid config)
    FakeReader(pa=pa_object(**{"status.metricSource.sha256": "0" * 64})),       # hash does not match its query
    FakeReader(pa=pa_object(**{"status.metricSource.contract": "x"})),
    FakeReader(dep={"metadata": {"uid": "new-dep"}}),                             # target re-created
    FakeReader(pa=pa_object(**{"spec.targetDeployment.namespace": "other"})),
    FakeReader(pa="missing"), FakeReader(dep="missing"),
])
def test_no_training_for_an_inconsistent_autoscaler(reader):
    with pytest.raises(ProvenanceRefused):
        resolve_autoscaler(reader, "shop", "web-pa")


# --- provenance and previous evidence -----------------------------------------------------------------------------

def test_the_trainers_sidecar_makes_a_servable_model(tmp_path):
    key = model_key(SIGNAL.namespace, SIGNAL.name, SIGNAL.metric)
    pkl, meta_path = artifact_paths(tmp_path, key)
    pkl.write_bytes(b"trained-model")
    meta = {"artifact_sha256": hashlib.sha256(b"trained-model").hexdigest(), "trained_at": "2026-10-10T00:02:00",
            **entry.provenance_fields(SIGNAL, "shop", "web-pa")}
    meta_path.write_text(json.dumps(meta))
    rec = load_record(pkl, loader=lambda b: b)
    assert rec.incompatibility(SIGNAL) is None
    assert meta["autoscaler"] == {"namespace": "shop", "name": "web-pa", "generation": 3} and meta["metric_query"] == QUERY


def test_previous_blend_evidence_counts_only_for_the_same_signal(tmp_path):
    path = tmp_path / "prev.meta.json"
    same = {**entry.provenance_fields(SIGNAL, "shop", "web-pa"), "blend_selection": {"chosen": 0.4}}
    path.write_text(json.dumps(same))
    assert entry.previous_evidence(path, SIGNAL)["blend_selection"] == {"chosen": 0.4}
    for field, value in (("metric_query_sha256", "0" * 64), ("target_uid", "old-dep"), ("pa_uid", "old-pa"),
                         ("contract", "x"), ("name", "api"), ("metric", "cpu")):
        path.write_text(json.dumps({**same, field: value}))
        assert entry.previous_evidence(path, SIGNAL) == {}, field
    path.write_text("not json")
    assert entry.previous_evidence(path, SIGNAL) == {}
    assert entry.previous_evidence(tmp_path / "absent.json", SIGNAL) == {}


# --- training reads the compiled query and publishes under the namespace-aware key --------------------------------

def test_training_history_comes_from_the_compiled_query(monkeypatch, tmp_path):
    import data.history as history
    seen, captured = {}, {}
    start = datetime(2026, 10, 1)
    points = [{"timestamp": (start + timedelta(minutes=10 * i)).isoformat(), "value": 600.0 + i} for i in range(300)]

    def fake_history(base_url, query, hours=168, **kw):
        seen.update(base_url=base_url, query=query, hours=hours)
        return points

    monkeypatch.setattr(history, "query_history", fake_history)
    monkeypatch.setattr(t, "preflight_history", lambda df, **kw: (True, "ok", df, {"present": len(df)}))
    monkeypatch.setattr(t, "train_lstm_for_metric", lambda **kw: captured.update(kw) or {"success": True})
    key = model_key("shop", "web", "requests")
    out = t.train_requests_only(vm_url="http://prom", namespace="shop", workload_name="web", app_name="web",
                                hours=168, model_dir=tmp_path, query=QUERY, artifact_key=key,
                                previous_meta={"blend_selection": {"chosen": 0.4}})
    assert out["success"] and seen == {"base_url": "http://prom", "query": QUERY, "hours": 168}
    assert captured["artifact_key"] == key and captured["previous_meta"] == {"blend_selection": {"chosen": 0.4}}
    assert list(captured["df"]["value"][:2]) == [600.0, 601.0] and len(captured["df"]) == 300


def test_an_unusable_history_fails_the_training(monkeypatch, tmp_path):
    import data.history as history

    def refused(base_url, query, hours=168, **kw):
        raise history.HistoryRefused("2 series, want exactly one")

    monkeypatch.setattr(history, "query_history", refused)
    out = t.train_requests_only(vm_url="http://prom", namespace="shop", workload_name="web", app_name="web",
                                model_dir=tmp_path, query=QUERY, artifact_key=model_key("shop", "web", "requests"))
    assert out["success"] is False and "2 series" in out["error"]


class _Stub:
    def __init__(self, sequence_length=144):
        self.sequence_length = sequence_length

    def train(self, *a, **k):
        return {"epochs": 1}

    def evaluate(self, *a, **k):
        return {"rmse": 1.0, "mae": 1.0, "scored": 10}

    def predict(self, *a, **k):
        return {"predictions": [1.0] * 6, "confidence": 0.9}


def test_the_artifact_is_published_under_the_namespace_aware_key(monkeypatch, tmp_path):
    import training.blend_selection as bs
    evidence = []
    monkeypatch.setattr(t, "LSTMForecastModel", _Stub)
    monkeypatch.setattr(bs, "attach_history", lambda *a, **k: None)
    monkeypatch.setattr(bs, "history_from_sidecar", lambda meta, *a: evidence.append(meta) or (None, []))
    monkeypatch.setattr(bs, "select_blend_weight", lambda *a, **k: {"chosen": 0.5})
    idx = pd.date_range("2026-09-20", periods=400, freq="10min")
    df = pd.DataFrame({"timestamp": idx, "value": 1000 + 100 * np.sin(np.arange(400) / 20.0)})
    key = model_key("shop", "web", "requests")
    out = t.train_lstm_for_metric(df, "web", "requests", tmp_path, epochs=1, atomic=True, artifact_key=key,
                                  previous_meta={})
    assert out["success"], out
    assert re.search(rf"lstm_{key}\.pkl\.\d+\.[0-9a-f]{{8}}\.tmp$", out["tmp_model_path"]), out["tmp_model_path"]
    assert out["model_path"].endswith(f"lstm_{key}.pkl")
    second = t.train_lstm_for_metric(df, "web", "requests", tmp_path, epochs=1, atomic=True, artifact_key=key,
                                     previous_meta={})
    assert second["tmp_model_path"] != out["tmp_model_path"], "every attempt writes a private candidate"
    assert evidence == [{}, {}], "the caller-validated previous evidence is used, not a legacy sidecar"
    assert not list(tmp_path.glob("lstm_web_requests*")), "nothing is written under the legacy name"


# --- the CronJob entry point ----------------------------------------------------------------------------------------

@pytest.mark.parametrize("target", [None, "", "web-pa", "a/b/c"])
def test_the_trainer_needs_a_target(monkeypatch, target):
    if target is None:
        monkeypatch.delenv("TRAINING_TARGET", raising=False)
    else:
        monkeypatch.setenv("TRAINING_TARGET", target)
    with pytest.raises(SystemExit) as exc:
        entry.main()
    assert exc.value.code == 2


def test_the_trainer_does_not_train_for_an_inconsistent_autoscaler(monkeypatch):
    monkeypatch.setenv("TRAINING_TARGET", "shop/web-pa")
    monkeypatch.setattr(entry, "resolve_autoscaler",
                        lambda reader, ns, name: (_ for _ in ()).throw(ProvenanceRefused("generation mismatch")))
    called = []
    monkeypatch.setattr(entry, "train_requests_only", lambda **kw: called.append(kw))
    with pytest.raises(SystemExit) as exc:
        entry.main()
    assert exc.value.code == 1 and called == []


# --- overlapping trainings (Codex r21 BLOCKER) ----------------------------------------------------------------------

def test_the_publication_lock_serializes_trainings_of_one_key(tmp_path):
    key = model_key("shop", "web", "requests")
    held = entry.acquire_publication_lock(tmp_path, key, wait_s=1)
    try:
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            entry.acquire_publication_lock(tmp_path, key, wait_s=0.3)   # a second writer waits, bounded
        assert time.monotonic() - t0 >= 0.3
        other = entry.acquire_publication_lock(tmp_path, model_key("shop", "api", "requests"), wait_s=0.3)
        other.close()                                                       # another key is independent
    finally:
        held.close()
    entry.acquire_publication_lock(tmp_path, key, wait_s=0.3).close()       # free again after release


def test_a_waiting_writer_proceeds_when_the_first_finishes(tmp_path):
    key = model_key("shop", "web", "requests")
    held = entry.acquire_publication_lock(tmp_path, key, wait_s=1)
    got = []
    waiter = threading.Thread(target=lambda: got.append(entry.acquire_publication_lock(tmp_path, key, wait_s=5)))
    waiter.start()
    time.sleep(0.3)
    assert got == []
    held.close()
    waiter.join(10)
    assert len(got) == 1
    got[0].close()


def test_publication_uses_the_candidates_own_digest(tmp_path):
    cand, final = tmp_path / "lstm_x.pkl.1.aa.tmp", tmp_path / "lstm_x.pkl"
    cand.write_bytes(b"candidate-A")
    assert entry.publish_candidate(cand, final) == hashlib.sha256(b"candidate-A").hexdigest()
    assert final.read_bytes() == b"candidate-A" and not cand.exists()


def test_a_final_file_replaced_during_publication_is_detected(tmp_path, monkeypatch):
    # The interleaving Codex reproduced: B replaces the final file between A's rename and A's sidecar. The lock prevents
    # it; if anything bypasses the lock, A must not attest B's bytes.
    cand, final = tmp_path / "lstm_x.pkl.1.aa.tmp", tmp_path / "lstm_x.pkl"
    cand.write_bytes(b"candidate-A")
    real_rename = os.rename

    def rename_then_overwrite(src, dst):
        real_rename(src, dst)
        open(dst, "wb").write(b"candidate-B")

    monkeypatch.setattr(entry.os, "rename", rename_then_overwrite)
    with pytest.raises(RuntimeError, match="changed during publication"):
        entry.publish_candidate(cand, final)
