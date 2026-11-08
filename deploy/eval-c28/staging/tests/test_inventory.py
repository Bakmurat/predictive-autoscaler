"""inventory.py: read-only, explicit discovery, the exact legacy allowlist, blockers, sanitized output."""

import json

import pytest

import common_records as C
import inventory as I
from fakes import CLUSTER, MARKERS, legacy_cluster, record


def test_the_inventory_is_read_only_and_resolves_the_allowlist(tmp_path):
    api = legacy_cluster()
    doc = I.collect(api, record(tmp_path))
    assert api.mutations() == [] and I.problems(doc) == []
    leg = doc["legacy"]
    assert leg["missing"] == [] and leg["blockers"] == [] and leg["unclassified"] == []
    roles = {}
    for e in leg["found"]:
        roles.setdefault(e["role"], set()).add(e["name"])
        assert e["uid"]
    assert roles["generator"] == {f"k6-{a}" for a in ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal",
                                                       "nginx-ensemble", "nginx-ensemble-q95")}
    assert roles["pa"] == {"nginx-test-autoscaler", "nginx-reactive-autoscaler", "nginx-seasonal-autoscaler",
                           "nginx-ensemble-autoscaler", "nginx-ensemble-q95-autoscaler"}
    assert roles["keda"] == {"nginx-test-keda-fallback", "myapptwo-keda-fallback"}
    assert roles["operator"] == {"predictive-operator"} and roles["api"] == {"ml-api"}
    assert roles["lease"] == {"predictive-autoscaler-leader"}
    assert "predictiveautoscalers.autoscaler.example.com" in roles["retained"]
    assert doc["absent_groups"] == ["argoproj.io"] and doc["writers"]["applications"] is None
    so = next(w for w in doc["writers"]["scaledobjects"] if w["name"] == "nginx-test-keda-fallback")
    assert so["paused"] == "true" and so["restore_to_original"] is False
    assert {p["name"] for p in doc["objects"]["persistentvolumes"]} == {
        f"pv-{c}" for c in ("ml-models-pvc", "forecast-log-pvc", "evidence-archive-pvc", "load-evidence-pvc",
                            "p8-evidence-pvc")}
    assert not any("/secrets" in path or "/configmaps" in path for _, path in api.calls)   # never read
    text = json.dumps(doc)
    assert not any(m in text for m in MARKERS) and C.LAST_APPLIED not in text


def test_unlisted_writers_and_consumers_block(tmp_path):
    api = legacy_cluster()
    api.add("pa-legacy", {"kind": "PredictiveAutoscaler", "metadata": {"name": "shop-pa", "namespace": "shop"},
                          "spec": {"targetDeployment": {"name": "web", "namespace": "shop"}}})
    api.add("horizontalpodautoscalers", {"kind": "HorizontalPodAutoscaler",
                                         "metadata": {"name": "manual", "namespace": "demo"},
                                         "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "nginx-test"}}})
    api.add("deployments", {"kind": "Deployment", "metadata": {"name": "other", "namespace": "demo"},
                            "spec": {"replicas": 1, "template": {"spec": {"containers": []}}}})
    leg = I.collect(api, record(tmp_path))["legacy"]
    assert any("shop/shop-pa" in b and "another consumer" in b for b in leg["blockers"])
    assert any("demo/manual writes legacy target nginx-test" in b for b in leg["blockers"])
    assert [u["name"] for u in leg["unclassified"]] == ["other"]


def test_a_missing_api_group_is_reported_not_skipped(tmp_path):
    api = legacy_cluster(extra_groups=["apps", "batch", "autoscaling", "policy", "coordination.k8s.io",
                                       "apiextensions.k8s.io", "autoscaler.example.com"])
    doc = I.collect(api, record(tmp_path))
    assert "keda.sh" in doc["absent_groups"] and doc["writers"]["scaledobjects"] is None
    assert any("keda.sh" in b for b in doc["legacy"]["blockers"])


def test_another_cluster_is_refused_and_the_cli_writes_once(tmp_path):
    api = legacy_cluster()
    with pytest.raises(C.StagingError, match="kube-system"):
        I.collect(api, record(tmp_path, cluster_uid="00000000-0000-4000-8000-000000000000"))
    rec_path = tmp_path / "rec.json"
    rec_path.write_text(json.dumps(record(tmp_path)))
    out = tmp_path / "inv.json"
    assert I.main(["--record", str(rec_path), "--out", str(out)], api=api) == 0
    assert json.loads(out.read_text())["cluster_uid"] == CLUSTER
    assert I.main(["--record", str(rec_path), "--out", str(out)], api=api) == 1          # never overwritten
