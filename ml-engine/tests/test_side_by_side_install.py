"""deploy/migration renders this version NEXT TO a legacy install (Codex task-08 r26 BLOCKER): nothing of the legacy
install (k8s-manifests/base's names in namespace ml-engine, which the legacy release also used) is replaced, renamed or
loses permissions, and every reference inside the overlay points at its own objects."""

import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
NS = "predictive-autoscaler"
NEW_CRD = "predictiveautoscalers.autoscaling.devkuban.com"


def render(path):
    try:
        p = subprocess.run(["kubectl", "kustomize", str(ROOT / path)], capture_output=True, text=True, timeout=60)
    except OSError as e:
        if os.environ.get("CI"):
            pytest.fail(f"kubectl is required in CI: {e}")
        pytest.skip(f"kubectl not usable here: {e}")
    assert p.returncode == 0, p.stderr
    return [d for d in yaml.safe_load_all(p.stdout) if d]


@pytest.fixture(scope="module")
def overlay():
    return render("deploy/migration")


@pytest.fixture(scope="module")
def legacy_names():
    # The legacy install used the same names as the base, in namespace ml-engine.
    return {(d["kind"], d["metadata"].get("namespace", ""), d["metadata"]["name"]) for d in render("k8s-manifests/base")}


def ident(d):
    return d["kind"], d["metadata"].get("namespace", ""), d["metadata"]["name"]


def test_no_legacy_object_is_replaced(overlay, legacy_names):
    shared = {ident(d) for d in overlay} & legacy_names
    assert shared == {("CustomResourceDefinition", "", NEW_CRD)}, shared   # the new CRD only; the legacy CRD is untouched
    cluster_scoped = [d for d in overlay if d["kind"] in ("ClusterRole", "ClusterRoleBinding")]
    assert cluster_scoped and all(d["metadata"]["name"].startswith("pa-") for d in cluster_scoped)
    assert [d["metadata"]["name"] for d in overlay if d["kind"] == "Namespace"] == [NS]
    crds = [d["metadata"]["name"] for d in overlay if d["kind"] == "CustomResourceDefinition"]
    assert crds == [NEW_CRD]


def test_every_namespaced_object_is_in_its_own_namespace(overlay):
    namespaced = [d for d in overlay if d["kind"] not in ("Namespace", "CustomResourceDefinition", "ClusterRole",
                                                          "ClusterRoleBinding")]
    assert namespaced and {d["metadata"]["namespace"] for d in namespaced} == {NS}


def test_references_point_at_the_overlays_own_objects(overlay):
    by = {ident(d): d for d in overlay}
    crb = next(d for d in overlay if d["kind"] == "ClusterRoleBinding")
    assert crb["roleRef"]["name"] == "pa-predictive-operator-role"
    assert {(s["namespace"], s["name"]) for s in crb["subjects"]} == {(NS, "pa-predictive-operator-sa")}
    for d in overlay:
        spec = d.get("spec", {})
        pod = spec.get("template", {}).get("spec") or spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec")
        if pod:
            assert pod["serviceAccountName"] == "pa-predictive-operator-sa", d["metadata"]["name"]
            for v in pod.get("volumes", []):
                if "persistentVolumeClaim" in v:
                    assert ("PersistentVolumeClaim", NS, v["persistentVolumeClaim"]["claimName"]) in by
    op = by[("Deployment", NS, "pa-predictive-operator")]
    env = {e["name"]: e.get("value") for e in op["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["ML_API_URL"] == f"http://pa-ml-api-service.{NS}.svc.cluster.local:8000"
    assert ("Service", NS, "pa-ml-api-service") in by
    api = by[("Deployment", NS, "pa-ml-api")]
    assert "COLD_START_CRONJOB" not in {e["name"] for e in api["spec"]["template"]["spec"]["containers"][0]["env"]}
    for d in overlay:
        if d["kind"] == "VMServiceScrape" and "namespaceSelector" in d["spec"]:
            assert d["spec"]["namespaceSelector"]["matchNames"] == [NS]
    text = yaml.safe_dump_all(overlay)
    assert "ml-engine.svc" not in text and "namespace: ml-engine" not in text
