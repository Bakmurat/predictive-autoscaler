"""charts/predictive-autoscaler renders a least-privilege, non-root, isolated install (B5b, DESIGN-B5; Codex task-08
r23–r27). Rendering cannot prove permissions, storage or connectivity: the kind install smoke test does that. These
tests pin what the chart promises in its rendered objects."""

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "charts" / "predictive-autoscaler"
NS = "pa-system"
NEW_CRD = "predictiveautoscalers.autoscaling.devkuban.com"
BASE = {
    "prometheus": {"url": "http://prometheus-server.monitoring.svc:80"},
    "networkPolicy": {"prometheus": {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}},
                      "metricsFrom": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}}]},
    "training": {"targets": [{"namespace": "shop", "name": "web-pa", "schedule": "0 */6 * * *"},
                             {"namespace": "shop", "name": "api-pa", "schedule": "30 */6 * * *"}]},
}


def _merge(a, b):
    out = dict(a)
    for k, v in b.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def helm(values, tmp_path, release="pa", check=True):
    f = tmp_path / f"values-{abs(hash(json.dumps(values, sort_keys=True)))}.yaml"
    f.write_text(yaml.safe_dump(values))
    try:
        p = subprocess.run(["helm", "template", release, str(CHART), "-n", NS, "--include-crds", "--kube-version", "1.35.0",
                            "-f", str(f)], capture_output=True, text=True, timeout=60)
    except OSError as e:
        if os.environ.get("CI"):
            pytest.fail(f"helm is required in CI: {e}")
        pytest.skip(f"helm not usable here: {e}")
    if check:
        assert p.returncode == 0, p.stderr
        return [d for d in yaml.safe_load_all(p.stdout) if d]
    return p


@pytest.fixture
def render(tmp_path):
    return lambda extra=None, **kw: helm(_merge(BASE, extra or {}), tmp_path, **kw)


def by_kind(docs, kind):
    return [d for d in docs if d["kind"] == kind]


def pod_specs(docs):
    for d in docs:
        spec = d.get("spec", {})
        pod = spec.get("template", {}).get("spec") or spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec")
        if pod:
            yield d, pod


# --- RBAC: three service accounts, least privilege ------------------------------------------------------------------

def rules_of(docs, name):
    role = next(d for d in docs if d["kind"] == "ClusterRole" and d["metadata"]["name"] == name)
    return {(tuple(r.get("apiGroups", [])), tuple(r["resources"]), tuple(r.get("resourceNames", [])), tuple(sorted(r["verbs"])))
            for r in role["rules"]}


def test_rbac_matches_the_documented_least_privilege_table(render):
    docs = render()
    ro = ("get", "list", "watch")
    assert rules_of(docs, "pa-predictive-autoscaler-operator") == {
        (("autoscaling.devkuban.com",), ("predictiveautoscalers",), (), ro),
        (("autoscaling.devkuban.com",), ("predictiveautoscalers/status",), (), ("get", "patch", "update")),
        (("apps",), ("deployments",), (), ro),
        (("apps",), ("deployments/scale",), (), ("get", "update")),
        (("autoscaling",), ("horizontalpodautoscalers",), (), ro),
        (("keda.sh",), ("scaledobjects",), (), ro),
        (("autoscaling.k8s.io",), ("verticalpodautoscalers",), (), ro),
        (("autoscaler.example.com",), ("predictiveautoscalers",), (), ro),
        (("apiextensions.k8s.io",), ("customresourcedefinitions",), (NEW_CRD,), ("get",)),
        (("", "events.k8s.io"), ("events",), (), ("create", "patch")),
    }
    read_only = {(("autoscaling.devkuban.com",), ("predictiveautoscalers",), (), ("get",)),
                 (("apps",), ("deployments",), (), ("get",))}
    assert rules_of(docs, "pa-predictive-autoscaler-trainer") == read_only
    # With authentication (the default) the forecasting service also checks callers' tokens.
    assert rules_of(docs, "pa-predictive-autoscaler-forecaster") == read_only | {
        (("authentication.k8s.io",), ("tokenreviews",), (), ("create",))}
    lease = by_kind(docs, "Role")
    assert len(lease) == 1 and lease[0]["metadata"]["namespace"] == NS
    assert {tuple(r["resources"]) for r in lease[0]["rules"]} == {("leases",)}


def test_no_role_can_create_workloads_or_read_secrets(render):
    docs = render()
    for role in by_kind(docs, "ClusterRole") + by_kind(docs, "Role"):
        for r in role["rules"]:
            assert "*" not in r["verbs"] and "*" not in r["resources"] and "*" not in r.get("apiGroups", [])
            assert not {"jobs", "pods", "secrets", "cronjobs"} & set(r["resources"]), (role["metadata"]["name"], r)


def test_each_component_runs_as_its_own_service_account(render):
    docs = render()
    sas = {d["metadata"]["name"] for d in by_kind(docs, "ServiceAccount")}
    assert sas == {f"pa-predictive-autoscaler-{c}" for c in ("operator", "forecaster", "trainer")}
    for d, pod in pod_specs(docs):
        component = d["metadata"]["labels"]["app.kubernetes.io/component"]
        assert pod["serviceAccountName"] == f"pa-predictive-autoscaler-{component}"
    for b in by_kind(docs, "ClusterRoleBinding"):
        assert b["roleRef"]["name"] == b["metadata"]["name"] == b["subjects"][0]["name"]
        assert b["subjects"][0]["namespace"] == NS


# --- workloads: non-root, read-only root, explicit writable paths ---------------------------------------------------

def test_every_container_is_non_root_read_only_and_drops_all_capabilities(render):
    docs = render({"operator": {"forecastLedger": {"enabled": True}}})
    seen = set()
    for d, pod in pod_specs(docs):
        ps = pod["securityContext"]
        assert ps["runAsNonRoot"] is True and isinstance(ps["runAsUser"], int) and ps["runAsUser"] > 0
        assert ps["seccompProfile"]["type"] == "RuntimeDefault" and ps["fsGroup"] == ps["runAsUser"]
        for c in pod["containers"]:
            cs = c["securityContext"]
            assert cs == {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
            seen.add(c["name"])
    assert seen == {"operator", "forecaster", "trainer"}


def test_writable_paths_are_exactly_the_documented_ones(render):
    docs = render({"operator": {"forecastLedger": {"enabled": True}}})
    writable, read_only = {}, {}
    for d, pod in pod_specs(docs):
        for c in pod["containers"]:
            for m in c.get("volumeMounts", []):
                (read_only if m.get("readOnly") else writable).setdefault(c["name"], set()).add(m["mountPath"])
    assert writable == {"operator": {"/var/lib/predictive-autoscaler"}, "forecaster": {"/models", "/tmp"},
                        "trainer": {"/models", "/tmp"}}
    assert read_only == {"operator": {"/var/run/secrets/predictive-autoscaler/forecaster-token",
                                      "/etc/predictive-autoscaler/forecaster-ca"},
                         "forecaster": {"/etc/predictive-autoscaler/tls"}}
    for d, pod in pod_specs(docs):
        if d["metadata"]["labels"]["app.kubernetes.io/component"] != "operator":
            env = {e["name"]: e.get("value") for e in pod["containers"][0]["env"]}
            for var in ("HOME", "TMPDIR", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "KERAS_HOME"):
                assert env[var].startswith("/tmp"), (var, env[var])
            tmp = next(v for v in pod["volumes"] if v["name"] == "tmp")
            assert tmp["emptyDir"]["sizeLimit"]


def test_a_ledger_claim_runs_one_operator_replaced_not_rolled(render):
    claim = {"operator": {"forecastLedger": {"enabled": True, "persistence": {"existingClaim": "ledger"}}}}
    op = next(d for d in by_kind(render(claim), "Deployment") if d["metadata"]["name"].endswith("-operator"))
    assert op["spec"]["strategy"] == {"type": "Recreate"} and op["spec"]["replicas"] == 1
    p = render(_merge(claim, {"operator": {"replicas": 2}}), check=False)
    assert p.returncode != 0 and "needs operator.replicas: 1" in p.stderr
    plain = next(d for d in by_kind(render(), "Deployment") if d["metadata"]["name"].endswith("-operator"))
    assert "strategy" not in plain["spec"]                                # rolling update without a ledger claim


def test_the_ledger_is_off_by_default(render):
    op = next(d for d in by_kind(render(), "Deployment") if d["metadata"]["name"].endswith("-operator"))
    c = op["spec"]["template"]["spec"]["containers"][0]
    assert "FORECAST_LOG" not in {e["name"] for e in c["env"]}
    assert "/var/lib/predictive-autoscaler" not in {m["mountPath"] for m in c.get("volumeMounts", [])}


def test_no_cold_start_and_no_benchmark_experiments(render):
    for d, pod in pod_specs(render()):
        names = {e["name"] for e in pod["containers"][0]["env"]}
        assert not names & {"COLD_START_CRONJOB", "ALLOW_BENCHMARK_EXPERIMENTS", "ENSEMBLE_EXPERIMENT", "SEASONAL_EXPERIMENT"}


# --- references and names -------------------------------------------------------------------------------------------

def test_references_point_at_the_releases_own_objects(render):
    docs = render()
    ident = {(d["kind"], d["metadata"].get("namespace", ""), d["metadata"]["name"]) for d in docs}
    op = next(d for d in by_kind(docs, "Deployment") if d["metadata"]["name"] == "pa-predictive-autoscaler-operator")
    env = {e["name"]: e.get("value") for e in op["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["ML_API_URL"] == f"https://pa-predictive-autoscaler-forecaster.{NS}.svc:8443"
    assert ("Service", NS, "pa-predictive-autoscaler-forecaster") in ident
    assert env["PROMETHEUS_URL"] == BASE["prometheus"]["url"]
    for d, pod in pod_specs(docs):
        for v in pod.get("volumes", []):
            if "persistentVolumeClaim" in v:
                assert ("PersistentVolumeClaim", NS, v["persistentVolumeClaim"]["claimName"]) in ident


def test_one_training_cronjob_per_target(render):
    docs = render()
    jobs = by_kind(docs, "CronJob")
    targets = {j["metadata"]["annotations"]["autoscaling.devkuban.com/training-target"]: j for j in jobs}
    assert set(targets) == {"shop/web-pa", "shop/api-pa"}
    for target, j in targets.items():
        assert len(j["metadata"]["name"]) <= 52 and j["spec"]["concurrencyPolicy"] == "Forbid"
        pod = j["spec"]["jobTemplate"]["spec"]["template"]["spec"]
        env = {e["name"]: e.get("value") for e in pod["containers"][0]["env"]}
        assert env["TRAINING_TARGET"] == target
        aff = pod["affinity"]["podAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][0]
        assert aff["labelSelector"]["matchLabels"]["app.kubernetes.io/component"] == "forecaster"
    assert len({j["metadata"]["name"] for j in jobs}) == 2


def test_training_epochs_are_optional(render):
    env_of = lambda docs: [{e["name"]: e.get("value") for e in j["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["env"]}
                           for j in by_kind(docs, "CronJob")]
    assert all("TRAINING_EPOCHS" not in env for env in env_of(render()))
    assert all(env["TRAINING_EPOCHS"] == "3" for env in env_of(render({"training": {"epochs": 3}})))


def test_rwx_models_need_no_affinity_and_duplicate_targets_fail(render):
    docs = render({"forecaster": {"persistence": {"accessMode": "ReadWriteMany"}}})
    assert all("affinity" not in j["spec"]["jobTemplate"]["spec"]["template"]["spec"] for j in by_kind(docs, "CronJob"))
    dup = {"training": {"targets": [BASE["training"]["targets"][0]] * 2}}
    p = render(dup, check=False)
    assert p.returncode != 0 and "lists shop/web-pa twice" in p.stderr


def test_long_release_names_stay_within_kubernetes_limits(tmp_path):
    release = "a-very-long-release-name-for-predictive-autoscaling-x"     # Helm's maximum: 53 characters
    assert len(release) == 53
    docs = helm(BASE, tmp_path, release=release)
    for d in docs:
        limit = 52 if d["kind"] == "CronJob" else 63
        assert len(d["metadata"]["name"]) <= (253 if d["kind"] == "CustomResourceDefinition" else limit), d["metadata"]["name"]


def test_no_object_is_shared_with_a_legacy_install(render):
    base = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s-manifests" / "base")], capture_output=True, text=True)
    if base.returncode != 0:
        if os.environ.get("CI"):
            pytest.fail(base.stderr)
        pytest.skip("kubectl kustomize not usable here")
    legacy = {(d["kind"], d["metadata"].get("namespace", ""), d["metadata"]["name"]) for d in yaml.safe_load_all(base.stdout) if d}
    ours = {(d["kind"], d["metadata"].get("namespace", ""), d["metadata"]["name"]) for d in render()}
    assert ours & legacy == {("CustomResourceDefinition", "", NEW_CRD)}


# --- CRD and persistent data under Helm and Argo CD -------------------------------------------------------------------

def test_the_crd_is_protected_and_versioned(render):
    crd = by_kind(render(), "CustomResourceDefinition")
    assert [c["metadata"]["name"] for c in crd] == [NEW_CRD]
    meta = crd[0]["metadata"]
    assert meta["labels"]["autoscaling.devkuban.com/crd-revision"] == "1"
    assert meta["annotations"]["argocd.argoproj.io/sync-options"] == "Prune=false,Delete=false,ServerSideApply=true"
    assert meta["annotations"]["argocd.argoproj.io/sync-wave"] == "-1"
    generated = [d for d in yaml.safe_load_all((ROOT / "k8s-manifests/base/01-crd.yaml").read_text()) if d]
    assert crd == generated


def test_the_model_volume_is_retained_or_brought(render):
    pvc = by_kind(render(), "PersistentVolumeClaim")
    assert len(pvc) == 1 and pvc[0]["metadata"]["annotations"] == {
        "helm.sh/resource-policy": "keep", "argocd.argoproj.io/sync-options": "Prune=false,Delete=false"}
    docs = render({"forecaster": {"persistence": {"existingClaim": "models-v1"}}})
    assert not by_kind(docs, "PersistentVolumeClaim")
    claims = {v["persistentVolumeClaim"]["claimName"] for _, pod in pod_specs(docs) for v in pod.get("volumes", [])
              if "persistentVolumeClaim" in v}
    assert claims == {"models-v1"}


def test_the_chart_renders_no_autoscaler(render):
    assert not by_kind(render(), "PredictiveAutoscaler")


# --- network policies -------------------------------------------------------------------------------------------------

def test_network_policies_isolate_each_component(render):
    nps = {d["metadata"]["name"].rsplit("-", 1)[1]: d["spec"] for d in by_kind(render(), "NetworkPolicy")}
    assert set(nps) == {"operator", "forecaster", "trainer"}
    for spec in nps.values():
        assert spec["policyTypes"] == ["Ingress", "Egress"]
        dns, api, prom = spec["egress"][:3]
        assert {p["port"] for p in dns["ports"]} == {53} and "to" in dns
        assert {p["port"] for p in api["ports"]} == {443, 6443}
        assert prom["to"] == [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}}]
    forecaster_from = nps["forecaster"]["ingress"][0]["from"]
    assert forecaster_from[0]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "operator"
    assert nps["forecaster"]["ingress"][0]["ports"] == [{"protocol": "TCP", "port": 8443}]
    assert "ingress" not in nps["trainer"]                                   # no ingress at all
    assert nps["operator"]["egress"][3]["to"][0]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "forecaster"


def test_network_policies_need_a_prometheus_peer_and_can_be_disabled(tmp_path):
    no_peer = json.loads(json.dumps(BASE))
    no_peer["networkPolicy"]["prometheus"] = {}
    p = helm(no_peer, tmp_path, check=False)
    assert p.returncode != 0 and "networkPolicy.prometheus needs" in p.stderr
    off = helm(_merge(no_peer, {"networkPolicy": {"enabled": False}}), tmp_path)
    assert not by_kind(off, "NetworkPolicy")


def test_api_server_cidrs_narrow_the_egress(render):
    nps = by_kind(render({"networkPolicy": {"kubeAPI": {"cidrs": ["10.43.0.1/32"]}}}), "NetworkPolicy")
    assert all(np["spec"]["egress"][1]["to"] == [{"ipBlock": {"cidr": "10.43.0.1/32"}}] for np in nps)


# --- values -----------------------------------------------------------------------------------------------------------

def test_images_prefer_the_digest(render):
    digest = "sha256:" + "a" * 64
    docs = render({"images": {"operator": {"digest": digest}, "forecaster": {"tag": "v9"}}})
    images = {c["name"]: c["image"] for _, pod in pod_specs(docs) for c in pod["containers"]}
    assert images["operator"] == f"ghcr.io/bakmurat/predictive-autoscaler-operator@{digest}"
    assert images["forecaster"] == images["trainer"] == "ghcr.io/bakmurat/predictive-autoscaler-ml-api:v9"
    default = {c["name"]: c["image"] for _, pod in pod_specs(render()) for c in pod["containers"]}
    assert default["operator"].endswith(":0.1.0")                          # appVersion


@pytest.mark.parametrize("bad", [
    {"prometheus": {"url": ""}},                                             # required
    {"prometheus": {"url": "prometheus:9090"}},                              # not a URL
    {"coldStart": {"enabled": True}},                                        # not a value of this chart
    {"experiments": {"ensemble": True}},
    {"images": {"operator": {"digest": "sha256:short"}}},
    {"training": {"targets": [{"namespace": "Shop", "name": "x", "schedule": "0 * * * *"}]}},
    {"training": {"targets": [{"namespace": "shop", "name": "x"}]}},        # schedule required
    {"forecaster": {"persistence": {"accessMode": "ReadOnlyMany"}}},
    {"forecaster": {"persistence": {"accessMode": "ReadWriteOncePod"}}},    # the trainer could never mount it
    {"operator": {"replicas": 0}},
])
def test_the_values_schema_rejects(render, bad):
    p = render(bad, check=False)
    assert p.returncode != 0, bad


# --- authentication between the operator and the forecasting service (B5e) ---------------------------------------------

def deployment(docs, component):
    return next(d for d in by_kind(docs, "Deployment") if d["metadata"]["name"].endswith(f"-{component}"))


def test_authentication_is_on_by_default_with_tls(render):
    docs = render()
    fc = deployment(docs, "forecaster")["spec"]["template"]["spec"]
    env = {e["name"]: e.get("value") for e in fc["containers"][0]["env"]}
    assert env["FORECASTER_AUTH"] == "tokenreview"
    assert env["FORECASTER_AUTH_SUBJECTS"] == f"system:serviceaccount:{NS}:pa-predictive-autoscaler-operator"
    assert env["FORECASTER_AUTH_AUDIENCE"] == "predictive-autoscaler-forecaster"
    assert env["FORECASTER_TLS_CERT"].endswith("tls.crt") and "FORECASTER_AUTH_ALLOW_PLAINTEXT" not in env
    assert fc["containers"][0]["ports"] == [{"name": "http", "containerPort": 8443}]
    for probe in ("startupProbe", "livenessProbe", "readinessProbe"):
        assert fc["containers"][0][probe]["httpGet"]["scheme"] == "HTTPS"
    tls_vol = next(v for v in fc["volumes"] if v["name"] == "tls")
    assert tls_vol["secret"]["items"] == [{"key": "tls.crt", "path": "tls.crt"}, {"key": "tls.key", "path": "tls.key"}]
    svc = next(d for d in by_kind(docs, "Service") if d["metadata"]["name"].endswith("-forecaster"))
    assert svc["spec"]["ports"][0]["port"] == 8443

    op = deployment(docs, "operator")["spec"]["template"]["spec"]
    env = {e["name"]: e.get("value") for e in op["containers"][0]["env"]}
    assert env["ML_API_URL"].startswith("https://") and env["ML_API_TOKEN_FILE"].endswith("/token")
    vols = {v["name"]: v for v in op["volumes"]}
    token = vols["forecaster-token"]["projected"]["sources"][0]["serviceAccountToken"]
    assert token == {"audience": "predictive-autoscaler-forecaster", "expirationSeconds": 3600, "path": "token"}
    assert vols["forecaster-ca"]["secret"]["items"] == [{"key": "ca.crt", "path": "ca.crt"}]   # never the private key


def test_the_self_signed_certificate_covers_the_service_names(render):
    secret = next(d for d in by_kind(render(), "Secret") if d["metadata"]["name"] == "pa-predictive-autoscaler-forecaster-tls")
    assert secret["type"] == "kubernetes.io/tls" and set(secret["data"]) == {"tls.crt", "tls.key", "ca.crt"}
    import base64
    cert = base64.b64decode(secret["data"]["tls.crt"]).decode()
    assert cert.startswith("-----BEGIN CERTIFICATE-----")
    check = subprocess.run(["openssl", "x509", "-noout", "-ext", "subjectAltName"], input=cert, capture_output=True, text=True)
    if check.returncode == 0:
        assert f"pa-predictive-autoscaler-forecaster.{NS}.svc" in check.stdout


def test_cert_manager_and_existing_secret_sources(render):
    docs = render({"forecaster": {"tls": {"source": "certManager", "certManager": {"issuerRef": {"name": "ca", "kind": "ClusterIssuer"}}}}})
    cert = next(d for d in docs if d["kind"] == "Certificate")
    assert cert["spec"]["secretName"] == "pa-predictive-autoscaler-forecaster-tls"
    assert cert["spec"]["issuerRef"] == {"name": "ca", "kind": "ClusterIssuer", "group": "cert-manager.io"}
    assert f"pa-predictive-autoscaler-forecaster.{NS}.svc" in cert["spec"]["dnsNames"] and not by_kind(docs, "Secret")
    assert render({"forecaster": {"tls": {"source": "certManager"}}}, check=False).returncode != 0   # issuer required
    docs = render({"forecaster": {"tls": {"source": "existingSecret", "existingSecret": "my-tls"}}})
    assert not by_kind(docs, "Secret") and not [d for d in docs if d["kind"] == "Certificate"]
    op = deployment(docs, "operator")["spec"]["template"]["spec"]
    assert next(v for v in op["volumes"] if v["name"] == "forecaster-ca")["secret"]["secretName"] == "my-tls"
    assert render({"forecaster": {"tls": {"source": "existingSecret"}}}, check=False).returncode != 0   # name required


def test_authentication_can_be_turned_off(render):
    docs = render({"forecaster": {"auth": {"enabled": False}}})
    fc = deployment(docs, "forecaster")["spec"]["template"]["spec"]["containers"][0]
    assert "FORECASTER_AUTH" not in {e["name"] for e in fc["env"]} and fc["ports"][0]["containerPort"] == 8000
    op = deployment(docs, "operator")["spec"]["template"]["spec"]
    assert "volumes" not in op and not by_kind(docs, "Secret")
    assert rules_of(docs, "pa-predictive-autoscaler-forecaster") == {
        (("autoscaling.devkuban.com",), ("predictiveautoscalers",), (), ("get",)), (("apps",), ("deployments",), (), ("get",))}


def test_the_forecaster_is_scraped_over_tls(render):
    docs = render({"metrics": {"serviceMonitor": {"enabled": True}}})
    sm = next(d for d in docs if d["kind"] == "ServiceMonitor" and d["metadata"]["name"].endswith("-forecaster"))
    assert sm["spec"]["endpoints"][0]["scheme"] == "https"
