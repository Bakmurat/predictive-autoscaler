"""The run-lock: complete, resolved, frozen values, credential-free, locked once (manifest section 9)."""

import copy
import hashlib
import json
import os

import pytest

from c28 import frozen as F
from c28 import runlock
from c28.schedule import session_order

UID = "0b6c1a52-4f0e-4c1e-9a77-3d1f0a2b9c10"


def valid_doc(manifest_path):
    with open(manifest_path, "rb") as fh:
        msha = hashlib.sha256(fh.read()).hexdigest()
    return {
        "schema": "c28-runlock/v1", "run_id": "c28-r1",
        "manifest": {"path": os.path.basename(manifest_path), "sha256": msha},
        "source": {"commit": "a" * 40},
        "images": {"fixture": "harbor.example/c28-fixture@sha256:" + "b" * 64,
                   "operator": "harbor.example/operator@sha256:" + "c" * 64},
        "chart_sha256": "d" * 64, "crd_sha256": "e" * 64,
        "kubeconfig": {"path": "/home/ops/.kube/eval.yaml", "context": "prodcluster", "cluster_uid": UID},
        "infrastructure": {"nodes": [{"name": "w", "uid": UID, "role": "workload"},
                                     {"name": "s", "uid": UID, "role": "support"}]},
        "slo": {"p95_seconds": 0.2, "p99_seconds": 0.5, "error_fraction": 0.001, "timeout_seconds": 2},
        "workload": {"programs_sha256": "f" * 64},
        "capacity": {"c0_rps": 40, "c_rps": "35"},
        "cpu_tuning": {"selected_target_percent": 60},
        "arms": {f"{a}/{p}": {"rendered_sha256": "1" * 64} for a in F.ARMS for p in F.PROFILES},
        "operating_point": {"capacity_ratio": 0.875, "nominal_coverage_bps": 9000, "extra_max_replicas": 2,
                            "extra_max_percent": 50, "pa_headroom_percent": 0},
        "seeds": {"confirmation": list(F.CONFIRMATION_SEEDS), "tuning": list(F.TUNING_SEEDS), "order": 20261106,
                  "bootstrap": 2808, "bootstrap_resamples": 10000},
        "session_order": session_order(),
        "startup": {"s20": {"creation_to_ready_p90_s": 24.0, "samples": 30, "frozen_lead_seconds": 180},
                    "s90": {"creation_to_ready_p90_s": 101.0, "samples": 31, "frozen_lead_seconds": 300}},
        "queries": {"offered_rps": 'sum(rate(c28_ingress_requests_total{target="x"}[60s]))'},
        "schedule": {"start_utc": "2026-11-20T00:00:00Z", "end_utc": "2026-12-20T00:00:00Z"},
        "admission": {"evidence": "receipts/admission.json"}, "data_licenses": [],
        "retention": {"days": 90}, "scorer": {"sha256": "2" * 64, "bootstrap_seed": 2808},
        "activation_scope": {"cluster_uid": UID, "namespaces": ["demo"], "target_uids": [UID]},
        "acknowledgments": {"executor": {"name": "claude", "at": "2026-11-19T10:00:00Z"},
                            "reviewer": {"name": "codex", "at": "2026-11-19T11:00:00Z"}},
    }


@pytest.fixture
def setup(tmp_path):
    manifest = tmp_path / "EVAL-C28-MANIFEST.md"
    manifest.write_text("frozen manifest\n")
    doc = valid_doc(str(manifest))
    path = tmp_path / "runlock.json"
    path.write_text(json.dumps(doc))
    return tmp_path, doc, path


def test_a_complete_runlock_locks_once_and_verifies(setup):
    root, doc, path = setup
    assert runlock.problems(doc) == []
    digest = runlock.lock(str(path))
    assert runlock.verify(str(path), str(root)) == digest
    with pytest.raises(runlock.RunLockError, match="locked once"):
        runlock.lock(str(path))
    changed = dict(doc, run_id="c28-r2")
    path.write_text(json.dumps(changed))
    with pytest.raises(runlock.RunLockError, match="changed after it was locked"):
        runlock.verify(str(path), str(root))


def test_a_changed_manifest_breaks_verification(setup):
    root, _, path = setup
    runlock.lock(str(path))
    (root / "EVAL-C28-MANIFEST.md").write_text("edited\n")
    with pytest.raises(runlock.RunLockError, match="manifest's hash"):
        runlock.verify(str(path), str(root))


@pytest.mark.parametrize("change, message", [
    (lambda d: d["images"].update(fixture="harbor.example/c28-fixture:latest"), "pinned by digest"),
    (lambda d: d.update(run_id="<run id>"), "unresolved"),
    (lambda d: d["kubeconfig"].update(token="abc"), "credential"),
    (lambda d: d["seeds"].update(bootstrap=1), "seeds differ"),
    (lambda d: d["seeds"].update(confirmation=[201, 202, 203, 204, 206]), "seeds differ"),
    (lambda d: d["operating_point"].update(extra_max_replicas=3), "operating_point"),
    (lambda d: d["capacity"].update(c_rps="36"), "0.875"),
    (lambda d: d["session_order"].reverse(), "session_order"),
    (lambda d: d["startup"]["s20"].update(creation_to_ready_p90_s=31), "20.0-30.0"),
    (lambda d: d["startup"]["s90"].update(samples=29), "30 measured"),
    (lambda d: d["slo"].update(p95_seconds=0.25), "frozen SLO"),
    (lambda d: d["arms"].pop("PA-E/s90"), "every arm/profile"),
    (lambda d: d["activation_scope"].update(cluster_uid="1" * 8 + "-1111-1111-1111-" + "1" * 12), "kubeconfig's cluster"),
    (lambda d: d["acknowledgments"].pop("reviewer"), "reviewer"),
    (lambda d: d.pop("scorer"), "missing scorer"),
])
def test_incomplete_or_altered_runlocks_are_refused(setup, change, message):
    _, doc, _ = setup
    d = copy.deepcopy(doc)
    change(d)
    errs = runlock.problems(d)
    assert any(message in e for e in errs), errs
