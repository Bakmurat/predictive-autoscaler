"""charts/predictive-autoscaler renders a least-privilege, non-root, isolated install (B5b, DESIGN-B5; Codex task-08
r23–r27). Rendering cannot prove permissions, storage or connectivity: the kind install smoke test does that. These
tests pin what the chart promises in its rendered objects."""

import json
import os
import re
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
        (("apps",), ("deployments",), (), ro),
        (("autoscaling",), ("horizontalpodautoscalers",), (), ro),
        (("keda.sh",), ("scaledobjects",), (), ro),
        (("autoscaling.k8s.io",), ("verticalpodautoscalers",), (), ro),
        (("autoscaler.example.com",), ("predictiveautoscalers",), (), ro),
        (("apiextensions.k8s.io",), ("customresourcedefinitions",), (NEW_CRD,), ("get",)),
    }
    assert rules_of(docs, "pa-predictive-autoscaler-operator-write") == {
        (("autoscaling.devkuban.com",), ("predictiveautoscalers/status",), (), ("get", "patch", "update")),
        (("apps",), ("deployments/scale",), (), ("get", "update")),
        (("", "events.k8s.io"), ("events",), (), ("create", "patch")),
    }
    # The targets' pods and ReplicaSets (startup measurement, ScaleOutBlocked): read-only, uncached lists (no watch).
    assert rules_of(docs, "pa-predictive-autoscaler-operator-targets") == {
        (("",), ("pods",), (), ("get", "list")),
        (("apps",), ("replicasets",), (), ("get", "list")),
    }
    read_only = {(("autoscaling.devkuban.com",), ("predictiveautoscalers",), (), ("get",)),
                 (("apps",), ("deployments",), (), ("get",))}
    assert rules_of(docs, "pa-predictive-autoscaler-trainer") == read_only
    # With authentication (the default) the forecasting service also checks callers' tokens.
    assert rules_of(docs, "pa-predictive-autoscaler-forecaster") == read_only | {
        (("authentication.k8s.io",), ("tokenreviews",), (), ("create",))}
    lease = by_kind(docs, "Role")
    assert len(lease) == 1 and lease[0]["metadata"]["namespace"] == NS
    assert {(tuple(r["resources"]), tuple(r.get("resourceNames", [])), tuple(sorted(r["verbs"]))) for r in lease[0]["rules"]} == {
        (("leases",), (), ("create",)),
        (("leases",), ("predictive-autoscaler.autoscaling.devkuban.com",), ("get", "update")),
        (("events",), (), ("create", "patch"))}
    main_go = (ROOT / "k8s-operator" / "main.go").read_text()
    assert 'leaderElectionID = "predictive-autoscaler.autoscaling.devkuban.com"' in main_go   # the Role's resourceName
    ev = next(r for r in lease[0]["rules"] if r["resources"] == ["events"])
    assert sorted(ev["verbs"]) == ["create", "patch"]


SCALE_RULE = (("apps",), ("deployments/scale",), ())
RBAC_KINDS = ("ClusterRole", "Role", "ClusterRoleBinding", "RoleBinding")


def _rbac(docs):
    """Every RBAC object by (kind, namespace, name), with its rules as a set of (groups, resources, names, verbs)."""
    out = {}
    for d in docs:
        if d["kind"] not in RBAC_KINDS:
            continue
        key = (d["kind"], d["metadata"].get("namespace", ""), d["metadata"]["name"])
        body = {k: v for k, v in d.items() if k not in ("metadata", "rules")}
        rules = {(tuple(r.get("apiGroups", [])), tuple(r["resources"]), tuple(r.get("resourceNames", [])),
                  tuple(sorted(r["verbs"]))) for r in d.get("rules", [])}
        out[key] = (json.dumps(body, sort_keys=True), rules)
    return out


@pytest.mark.parametrize("extra", [{}, {"operator": {"watchNamespaces": ["shop", "payments"]}}])
def test_a_shadow_install_cannot_write_replicas_and_changes_nothing_else(render, extra):
    default = render(extra)
    shadow = render(_merge(extra, {"operator": {"rbac": {"scaleWrite": False}}}))
    explicit = render(_merge(extra, {"operator": {"rbac": {"scaleWrite": True}}}))
    assert _rbac(explicit) == _rbac(default)                      # true is today's behaviour, exactly
    write = "pa-predictive-autoscaler-operator-write"
    assert {r for r in rules_of(default, write) if r[:3] == SCALE_RULE} == {SCALE_RULE + (("get", "update"),)}
    assert {r for r in rules_of(shadow, write) if r[:3] == SCALE_RULE} == {SCALE_RULE + (("get",),)}
    # No role of the shadow install may write any /scale subresource.
    for (kind, _, name), (_, rules) in _rbac(shadow).items():
        for groups, resources, _, verbs in rules:
            if any(res.endswith("/scale") for res in resources):
                assert set(verbs) <= {"get"}, (kind, name, resources, verbs)
    # Every other object and rule is identical: status, events, the Lease, the reads and every binding.
    d, s = _rbac(default), _rbac(shadow)
    assert d.keys() == s.keys()
    for key in d:
        body_d, rules_d = d[key]
        body_s, rules_s = s[key]
        assert body_d == body_s, key
        assert {r for r in rules_d if r[:3] != SCALE_RULE} == {r for r in rules_s if r[:3] != SCALE_RULE}, key
    # And the non-RBAC objects render the same (the self-signed TLS Secret is generated anew on every render).
    strip = lambda docs: [d for d in docs if d["kind"] not in RBAC_KINDS + ("Secret",)]
    assert strip(default) == strip(shadow)


def test_scale_write_must_be_a_boolean(render):
    assert render({"operator": {"rbac": {"scaleWrite": "false"}}}, check=False).returncode != 0
    assert render({"operator": {"rbac": {"scaleWrites": False}}}, check=False).returncode != 0


# --- operator.rbac.scaleTargets: the replica write limited to named Deployments ----------------------------------------

WRITE_ROLE = "pa-predictive-autoscaler-operator-write"
CI_VALUES = sorted((CHART / "ci").glob("*-values.yaml"))
LEGACY_SCALE_RULE = ('  - apiGroups: ["apps"]\n    resources: ["deployments/scale"]\n    verbs: %s\n'
                     '  - apiGroups: ["", "events.k8s.io"]\n')


def _operator_rbac_text(values_file, *sets):
    """The rendered templates/operator-rbac.yaml, byte for byte (it holds no generated material)."""
    cmd = ["helm", "template", "pa", str(CHART), "-n", NS, "--kube-version", "1.35.0", "-f", str(values_file),
           "--show-only", "templates/operator-rbac.yaml"]
    for s in sets:
        cmd += ["--set-json", s]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except OSError as e:
        if os.environ.get("CI"):
            pytest.fail(f"helm is required in CI: {e}")
        pytest.skip(f"helm not usable here: {e}")
    assert p.returncode == 0, p.stderr
    return p.stdout


@pytest.mark.parametrize("values", [f for f in CI_VALUES if f.name != "scale-targets-values.yaml"], ids=lambda f: f.name)
def test_without_scale_targets_the_rbac_renders_exactly_as_before(values):
    # Every value set that existed before scaleTargets: the single /scale rule, its text unchanged, no resourceNames
    # on it; and an explicit empty list is byte-identical to leaving the value out.
    text = _operator_rbac_text(values)
    write = (yaml.safe_load(values.read_text()).get("operator", {}).get("rbac", {}).get("scaleWrite", True))
    assert text.count('resources: ["deployments/scale"]') == 1
    assert LEGACY_SCALE_RULE % ('["get", "update"]' if write else '["get"]') in text
    assert text.count("resourceNames:") == 2          # only the operator's own CRD and its Lease, as before
    assert _operator_rbac_text(values, "operator.rbac.scaleTargets=[]") == text


def test_scale_targets_limit_the_write_verb_to_the_named_deployments(render):
    named = render({"operator": {"rbac": {"scaleWrite": True, "scaleTargets": ["web", "api"]}}})
    # Exactly the write verb granted today (update; never patch) carries the names; get stays in its own rule,
    # without names. For a subresource, resourceNames holds the parent object's name: the Deployment.
    assert {r for r in rules_of(named, WRITE_ROLE) if r[:2] == SCALE_RULE[:2]} == {
        (("apps",), ("deployments/scale",), (), ("get",)),
        (("apps",), ("deployments/scale",), ("web", "api"), ("update",))}
    role = next(d for d in named if d["kind"] == "ClusterRole" and d["metadata"]["name"] == WRITE_ROLE)
    scale = [r for r in role["rules"] if r["resources"] == ["deployments/scale"]]
    assert scale == [{"apiGroups": ["apps"], "resources": ["deployments/scale"], "verbs": ["get"]},
                     {"apiGroups": ["apps"], "resources": ["deployments/scale"], "resourceNames": ["web", "api"],
                      "verbs": ["update"]}]
    # No rule of any role writes a /scale subresource without resourceNames, and none grants patch on it.
    for (kind, _, name), (_, rules) in _rbac(named).items():
        for _groups, resources, names, verbs in rules:
            if any(res.endswith("/scale") for res in resources):
                assert set(verbs) <= {"get"} or (names == ("web", "api") and set(verbs) == {"update"}), (kind, name)
    # Nothing else changes: every other RBAC object and rule, every binding and every non-RBAC object.
    default = render()
    d, n = _rbac(default), _rbac(named)
    assert d.keys() == n.keys()
    for key in d:
        assert d[key][0] == n[key][0], key
        assert {r for r in d[key][1] if r[:2] != SCALE_RULE[:2]} == {r for r in n[key][1] if r[:2] != SCALE_RULE[:2]}, key
    strip = lambda docs: [x for x in docs if x["kind"] not in RBAC_KINDS + ("Secret",)]
    assert strip(default) == strip(named)


def test_scale_targets_apply_in_each_watched_namespace_and_nowhere_else(render):
    docs = render({"operator": {"watchNamespaces": ["shop", "payments"],
                                "rbac": {"scaleTargets": ["web"]}}})          # scaleWrite defaults to true
    assert (("apps",), ("deployments/scale",), ("web",), ("update",)) in rules_of(docs, WRITE_ROLE)
    bound = [b for b in by_kind(docs, "RoleBinding") if b["roleRef"]["name"] == WRITE_ROLE]
    assert sorted(b["metadata"]["namespace"] for b in bound) == ["payments", "shop"]
    assert all(b["roleRef"]["kind"] == "ClusterRole" for b in bound)
    # The named write role is never bound cluster-wide when namespaces are watched.
    assert not [b for b in by_kind(docs, "ClusterRoleBinding") if b["roleRef"]["name"] == WRITE_ROLE]


def test_scale_targets_need_scale_write_and_valid_distinct_names(render):
    p = render({"operator": {"rbac": {"scaleWrite": False, "scaleTargets": ["web"]}}}, check=False)
    assert p.returncode != 0 and "operator.rbac.scaleWrite is false" in p.stderr      # ambiguous intent: refused
    for bad in (["Web"], ["web_1"], [""], ["-web"], ["web", "web"], "web", [7], ["x" * 254], [f"d{i}" for i in range(65)]):
        p = render({"operator": {"rbac": {"scaleTargets": bad}}}, check=False)
        assert p.returncode != 0 and "scaleTargets" in p.stderr, bad
    assert render({"operator": {"rbac": {"scaleWrite": False, "scaleTargets": []}}}, check=False).returncode == 0


# Kubernetes' DNS-1123 subdomain, label by label: each label [a-z0-9]([-a-z0-9]*[a-z0-9])?, single dots between them.
MALFORMED_NAMES = ["web..api", "web.-api", "web-.api", ".web", "web.", "web.api.", "-web", "web-", "web.Api", "web_api",
                   "web api", "web/api", "web\n", "web\r", "web\t", "\nweb", "web.\napi", "*", "", ".", "x" * 254,
                   ".".join(["a" * 63] * 4) + "b"]
WELL_FORMED_NAMES = ["web", "web-1", "web.api", "a.b.c", "0", "web-api.v2.shop-1", "x" * 253, ".".join(["a" * 63] * 3)]


@pytest.mark.parametrize("skip_schema", [False, True], ids=["schema", "template"])
def test_scale_target_names_are_dns_1123_subdomains_label_by_label(tmp_path, skip_schema):
    # The schema refuses a malformed name, and so does the template when schema validation is skipped.
    def run(names):
        f = tmp_path / "values.yaml"
        f.write_text(yaml.safe_dump(_merge(BASE, {"operator": {"rbac": {"scaleTargets": names}}})))
        cmd = ["helm", "template", "pa", str(CHART), "-n", NS, "--kube-version", "1.35.0", "-f", str(f),
               "--show-only", "templates/operator-rbac.yaml"] + (["--skip-schema-validation"] if skip_schema else [])
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        except OSError as e:
            if os.environ.get("CI"):
                pytest.fail(f"helm is required in CI: {e}")
            pytest.skip(f"helm not usable here: {e}")
    for name in MALFORMED_NAMES:
        p = run([name])
        assert p.returncode != 0 and "scaleTargets" in p.stderr, repr(name)
        assert run(["web", name]).returncode != 0, repr(name)                 # wherever it stands in the list
    ok = run(WELL_FORMED_NAMES)
    assert ok.returncode == 0, ok.stderr
    (rule,) = [r for d in yaml.safe_load_all(ok.stdout) if d and d["kind"] == "ClusterRole"
               for r in d["rules"] if r.get("resourceNames") and r["resources"] == ["deployments/scale"]]
    assert rule["resourceNames"] == WELL_FORMED_NAMES and rule["verbs"] == ["update"]
    for bad in (["web", "web"], [f"d{i}" for i in range(65)], [7], ["web", None]):   # distinct, at most 64, strings
        p = run(bad)
        assert p.returncode != 0 and "scaleTargets" in p.stderr, bad
    for not_a_list in (None, {}, {"web": True}, "", "web", 0, False):                # null would drop the names silently
        p = run(not_a_list)
        assert p.returncode != 0 and "scaleTargets" in p.stderr, repr(not_a_list)
    assert run([f"d{i}" for i in range(64)]).returncode == 0


# --- operator.writeAllowlist: every replica write limited to reviewed Deployments, by UID -----------------------------

UID_WEB, UID_API = "6f1d2c3a-8b7e-4f5a-9c0d-1e2f3a4b5c6d", "0a9b8c7d-6e5f-4a3b-8c2d-1f0e9d8c7b6a"


def _operator_env(docs):
    (dep,) = [d for d in by_kind(docs, "Deployment") if d["metadata"]["name"].endswith("-operator")]
    (container,) = dep["spec"]["template"]["spec"]["containers"]
    return {e["name"]: e.get("value") for e in container["env"]}


def test_no_write_allowlist_by_default_and_the_listed_uids_reach_the_operator(render):
    assert "SCALE_WRITE_ALLOWLIST" not in _operator_env(render())          # unset: the operator restricts nothing by UID
    docs = render({"operator": {"watchNamespaces": ["shop"], "rbac": {"scaleTargets": ["web", "api"]},
                                "writeAllowlist": [{"namespace": "shop", "name": "web", "uid": UID_WEB},
                                                   {"namespace": "shop", "name": "api", "uid": UID_API}]}})
    assert _operator_env(docs)["SCALE_WRITE_ALLOWLIST"] == f"shop/web={UID_WEB},shop/api={UID_API}"
    # Without names or namespaces in RBAC the list still applies on its own.
    docs = render({"operator": {"writeAllowlist": [{"namespace": "shop", "name": "web", "uid": UID_WEB}]}})
    assert _operator_env(docs)["SCALE_WRITE_ALLOWLIST"] == f"shop/web={UID_WEB}"


@pytest.mark.parametrize("skip_schema", [False, True], ids=["schema", "template"])
def test_a_write_allowlist_that_cannot_be_applied_exactly_is_refused(tmp_path, skip_schema):
    def run(operator):
        f = tmp_path / "values.yaml"
        f.write_text(yaml.safe_dump(_merge(BASE, {"operator": operator})))
        cmd = ["helm", "template", "pa", str(CHART), "-n", NS, "--kube-version", "1.35.0", "-f", str(f),
               "--show-only", "templates/operator.yaml"] + (["--skip-schema-validation"] if skip_schema else [])
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        except OSError as e:
            if os.environ.get("CI"):
                pytest.fail(f"helm is required in CI: {e}")
            pytest.skip(f"helm not usable here: {e}")
    web = {"namespace": "shop", "name": "web", "uid": UID_WEB}
    assert run({"writeAllowlist": [web]}).returncode == 0
    malformed = [dict(web, uid=""), dict(web, uid="a b"), dict(web, uid="a,b"), dict(web, uid="a=b"), dict(web, uid="a/b"),
                 dict(web, uid="x" * 65), dict(web, uid=UID_WEB + "\n"), dict(web, uid=7), dict(web, namespace="Shop"),
                 dict(web, namespace="shop/x"), dict(web, namespace=""), dict(web, namespace="x" * 64),
                 dict(web, name="Web"), dict(web, name="web..api"), dict(web, name=""), dict(web, name="x" * 254),
                 dict(web, extra="x"), {"namespace": "shop", "name": "web"}, {"name": "web", "uid": UID_WEB},
                 f"shop/web={UID_WEB}", None]
    for bad in malformed:
        p = run({"writeAllowlist": [bad]})
        assert p.returncode != 0 and "writeAllowlist" in p.stderr, repr(bad)
        assert run({"writeAllowlist": [web, bad]}).returncode != 0, repr(bad)     # wherever it stands in the list
    for operator, want in (
            ({"writeAllowlist": [web, dict(web, uid=UID_API)]}, "writeAllowlist"),            # one UID per Deployment
            ({"writeAllowlist": [web, dict(web)]}, "writeAllowlist"),
            ({"writeAllowlist": [dict(web, name=f"d{i}") for i in range(65)]}, "writeAllowlist"),
            ({"writeAllowlist": [web], "rbac": {"scaleWrite": False}}, "operator.rbac.scaleWrite is false"),
            ({"writeAllowlist": [web], "watchNamespaces": ["payments"]}, "outside operator.watchNamespaces"),
            ({"writeAllowlist": [web], "rbac": {"scaleTargets": ["api"]}}, "not in operator.rbac.scaleTargets")):
        p = run(operator)
        assert p.returncode != 0 and want in p.stderr, (operator, p.stderr[-300:])
    assert run({"writeAllowlist": [dict(web, name=f"d{i}") for i in range(64)]}).returncode == 0
    assert run({"writeAllowlist": [], "rbac": {"scaleWrite": False}}).returncode == 0
    # The value itself must be a list. null removes the chart's default before the schema sees it, and a map or a
    # scalar has nothing to iterate: each would render an operator with no restriction at all.
    for not_a_list in (None, {}, {"shop/web": UID_WEB}, "", f"shop/web={UID_WEB}", 0, False):
        p = run({"writeAllowlist": not_a_list})
        assert p.returncode != 0 and "writeAllowlist" in p.stderr, repr(not_a_list)
        assert "SCALE_WRITE_ALLOWLIST" not in p.stdout
    p = run({})                                                                      # omitted: the default, no restriction
    assert p.returncode == 0 and "SCALE_WRITE_ALLOWLIST" not in p.stdout


def test_the_scale_targets_ci_value_set_lists_the_reviewed_uids(tmp_path):
    docs = helm(yaml.safe_load((CHART / "ci" / "scale-targets-values.yaml").read_text()), tmp_path)
    assert _operator_env(docs)["SCALE_WRITE_ALLOWLIST"] == f"shop/web={UID_WEB},shop/api={UID_API}"


def test_the_scale_targets_ci_value_set_renders_the_named_write():
    text = _operator_rbac_text(CHART / "ci" / "scale-targets-values.yaml")
    assert ('  - apiGroups: ["apps"]\n    resources: ["deployments/scale"]\n    verbs: ["get"]\n'
            '  - apiGroups: ["apps"]\n    resources: ["deployments/scale"]\n    resourceNames: ["web","api"]\n'
            '    verbs: ["update"]\n') in text
    assert "kind: RoleBinding" in text and "namespace: shop" in text


def test_no_role_can_create_workloads_or_read_secrets(render):
    docs = render()
    for role in by_kind(docs, "ClusterRole") + by_kind(docs, "Role"):
        for r in role["rules"]:
            assert "*" not in r["verbs"] and "*" not in r["resources"] and "*" not in r.get("apiGroups", [])
            assert not {"jobs", "secrets", "cronjobs", "pods/exec", "pods/log", "pods/attach"} & set(r["resources"]), \
                (role["metadata"]["name"], r)
            workload = {"pods", "replicasets", "deployments"} & set(r["resources"])
            # Pods and ReplicaSets only read (get/list), only by the operator's targets role.
            if workload - {"deployments"}:
                assert role["metadata"]["name"] == "pa-predictive-autoscaler-operator-targets", (role["metadata"]["name"], r)
                assert set(r["verbs"]) <= {"get", "list"}, r


def test_each_component_runs_as_its_own_service_account(render):
    docs = render()
    sas = {d["metadata"]["name"] for d in by_kind(docs, "ServiceAccount")}
    assert sas == {f"pa-predictive-autoscaler-{c}" for c in ("operator", "forecaster", "trainer")}
    for d, pod in pod_specs(docs):
        component = d["metadata"]["labels"]["app.kubernetes.io/component"]
        assert pod["serviceAccountName"] == f"pa-predictive-autoscaler-{component}"
    for b in by_kind(docs, "ClusterRoleBinding") + by_kind(docs, "RoleBinding"):
        assert b["roleRef"]["name"] == b["metadata"]["name"]
        assert len(b["subjects"]) == 1 and b["subjects"][0]["namespace"] == NS
        assert b["subjects"][0]["name"] in sas and b["metadata"]["name"].startswith(b["subjects"][0]["name"])


def _write_bindings(docs, role="write"):
    return [b for b in by_kind(docs, "ClusterRoleBinding") + by_kind(docs, "RoleBinding")
            if b["roleRef"]["name"] == f"pa-predictive-autoscaler-operator-{role}"]


def test_operator_writes_are_cluster_wide_only_when_every_namespace_is_watched(render):
    (b,) = _write_bindings(render())
    assert b["kind"] == "ClusterRoleBinding"
    (t,) = _write_bindings(render(), "targets")
    assert t["kind"] == "ClusterRoleBinding" and t["subjects"][0]["name"] == "pa-predictive-autoscaler-operator"


def test_pod_reads_follow_the_watched_namespaces(render):
    docs = render({"operator": {"watchNamespaces": ["shop", "payments", "shop"]}})
    binds = _write_bindings(docs, "targets")
    assert {b["kind"] for b in binds} == {"RoleBinding"}
    assert sorted(b["metadata"]["namespace"] for b in binds) == ["payments", "shop"]
    assert all(b["subjects"] == [{"kind": "ServiceAccount", "name": "pa-predictive-autoscaler-operator", "namespace": NS}]
               for b in binds)
    # The cluster-wide reads never include pods or ReplicaSets.
    read_rules = rules_of(docs, "pa-predictive-autoscaler-operator")
    assert not any(set(r[1]) & {"pods", "replicasets"} for r in read_rules)


def test_watched_namespaces_limit_where_the_operator_can_scale(render):
    docs = render({"operator": {"watchNamespaces": ["shop", "payments", "shop"]}})
    binds = _write_bindings(docs)
    assert {b["kind"] for b in binds} == {"RoleBinding"}
    # Only the watched namespaces: the (unwatched) release namespace gets no scale or status write, only the Lease
    # Role's leader-election events.
    assert sorted(b["metadata"]["namespace"] for b in binds) == ["payments", "shop"]
    (lease,) = by_kind(docs, "Role")
    assert lease["metadata"]["namespace"] == NS
    assert not any(set(r["resources"]) & {"deployments/scale", "predictiveautoscalers/status"} for r in lease["rules"])
    assert any(r["resources"] == ["events"] for r in lease["rules"])
    # Watching the release namespace explicitly gives it the full write role like any other watched namespace.
    docs2 = render({"operator": {"watchNamespaces": ["shop", NS]}})
    assert sorted(b["metadata"]["namespace"] for b in _write_bindings(docs2)) == sorted(["shop", NS])
    # The reads stay cluster-wide: coexistence must see replica writers outside the watched namespaces.
    reads = [b for b in by_kind(docs, "ClusterRoleBinding") if b["roleRef"]["name"] == "pa-predictive-autoscaler-operator"]
    assert len(reads) == 1
    write_rules = rules_of(docs, "pa-predictive-autoscaler-operator-write")
    read_rules = rules_of(docs, "pa-predictive-autoscaler-operator")
    assert not any(r[1] in (("deployments/scale",), ("predictiveautoscalers/status",), ("events",)) for r in read_rules)
    assert write_rules


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
    # the chart ships exactly the revision the operator requires (its CRD gate), so the two never drift apart
    required = re.search(r"RequiredCRDRevision = (\d+)", (ROOT / "k8s-operator/controllers/crd_gate.go").read_text())
    assert required and meta["labels"]["autoscaling.devkuban.com/crd-revision"] == required.group(1)
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



def test_cilium_mode_moves_the_api_allow_to_the_kube_apiserver_entity(render):
    docs = render({"networkPolicy": {"kubeAPI": {"cidrs": [], "ports": [6443], "cilium": True}}})
    nps = {np["metadata"]["name"].rsplit("-", 1)[1]: np["spec"] for np in by_kind(docs, "NetworkPolicy")}
    for spec in nps.values():                                                # no API rule, and no rule without a peer
        assert all(rule.get("to") for rule in spec["egress"]), spec["egress"]
        assert not any(p.get("port") == 6443 for rule in spec["egress"] for p in rule.get("ports", []))
    assert nps["operator"]["egress"][2]["to"][0]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "forecaster"
    [cnp] = by_kind(docs, "CiliumNetworkPolicy")
    sel = cnp["spec"]["endpointSelector"]
    assert sel["matchLabels"]["app.kubernetes.io/instance"] == "pa"
    assert sel["matchExpressions"] == [{"key": "app.kubernetes.io/component", "operator": "In",
                                        "values": ["operator", "forecaster", "trainer"]}]
    assert cnp["spec"]["egress"] == [{"toEntities": ["kube-apiserver"],
                                      "toPorts": [{"ports": [{"port": "6443", "protocol": "TCP"}]}]}]
    assert not by_kind(render(), "CiliumNetworkPolicy")                      # off by default


def test_cilium_mode_rejects_api_cidrs(tmp_path):
    p = helm(_merge(BASE, {"networkPolicy": {"kubeAPI": {"cidrs": ["10.0.0.1/32"], "cilium": True}}}), tmp_path, check=False)
    assert p.returncode != 0 and "kubeAPI.cidrs must be empty" in p.stderr


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


def test_a_forecaster_route_keeps_certificate_verification(render):
    """C2.8: the operator may dial an evaluation-owned route in front of the forecaster; its host must be a name in the
    certificate (verification stays on), or the render fails."""
    route = "c28-forecast-route.pa-eval-system.svc"
    docs = render({"operator": {"forecasterAddress": f"{route}:8443"}, "forecaster": {"tls": {"extraDNSNames": [route]}}})
    op = next(d for d in by_kind(docs, "Deployment") if d["metadata"]["name"] == "pa-predictive-autoscaler-operator")
    env = {e["name"]: e.get("value") for e in op["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["ML_API_URL"] == f"https://{route}:8443"
    secret = next(d for d in by_kind(docs, "Secret") if d["metadata"]["name"] == "pa-predictive-autoscaler-forecaster-tls")
    import base64
    cert = base64.b64decode(secret["data"]["tls.crt"]).decode()
    check = subprocess.run(["openssl", "x509", "-noout", "-ext", "subjectAltName"], input=cert, capture_output=True, text=True)
    if check.returncode == 0:
        assert route in check.stdout and f"pa-predictive-autoscaler-forecaster.{NS}.svc" in check.stdout
    bad = render({"operator": {"forecasterAddress": f"{route}:8443"}}, check=False)
    assert bad.returncode != 0 and "extraDNSNames" in bad.stderr
    assert render({"operator": {"forecasterAddress": "https://x:1"}}, check=False).returncode != 0   # schema: host:port


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


def test_alert_rules_are_opt_in_and_identical_for_both_rule_engines(render):
    assert not by_kind(render(), "PrometheusRule") and not by_kind(render(), "VMRule")
    docs = render({"metrics": {"alerts": {"prometheusRule": {"enabled": True}, "vmRule": {"enabled": True},
                                          "labels": {"team": "platform"}}}})
    (pr,), (vr,) = by_kind(docs, "PrometheusRule"), by_kind(docs, "VMRule")
    assert pr["spec"] == vr["spec"] and pr["metadata"]["namespace"] == NS
    rules = pr["spec"]["groups"][0]["rules"]
    assert {r["alert"] for r in rules} == {
        "PredictiveAutoscalerNotReady", "PredictiveAutoscalerTelemetryUnavailable",
        "PredictiveAutoscalerForecastUnavailable", "PredictiveAutoscalerConflict", "PredictiveAutoscalerModelStale",
        "PredictiveAutoscalerOperatorDown", "PredictiveAutoscalerForecasterDown",
        "PredictiveAutoscalerForecastJournalUnhealthy"}
    journal = next(r for r in rules if r["alert"] == "PredictiveAutoscalerForecastJournalUnhealthy")
    assert journal["for"] == "15m" and journal["expr"] == (
        'max(predictive_autoscaler_forecaster_journal_unhealthy_since_seconds{job="pa-predictive-autoscaler-forecaster", '
        f'pa_install_namespace="{NS}"}}) > 0')
    assert all(r["labels"]["team"] == "platform" and r["labels"]["severity"] and r["labels"]["pa_release"] == "pa"
               and r["labels"]["pa_release_namespace"] == NS for r in rules)
    # every selector is scoped to this installation
    assert all(r["expr"].count(f'pa_install_namespace="{NS}"') == r["expr"].count("{") for r in rules), rules
    # the scrape alerts name the chart's own Services (job = Service name under both scrape operators)
    services = {d["metadata"]["name"] for d in by_kind(docs, "Service")}
    jobs = {re.search(r'job="([^"]+)"', r["expr"]).group(1) for r in rules if "absent(" in r["expr"]}
    assert jobs and jobs <= services


def test_both_scrapes_keep_the_workload_namespace_label(render):
    # The operator and the forecasting service both label series with the workload's namespace, which may differ from
    # the installation's (a workload in "shop", the chart in "pa-system"): honorLabels keeps it as `namespace` instead
    # of renaming it to exported_namespace, and the installation stays in pa_install_namespace.
    docs = render({"metrics": {"serviceMonitor": {"enabled": True}, "vmServiceScrape": {"enabled": True}}})
    seen = set()
    for kind in ("ServiceMonitor", "VMServiceScrape"):
        for d in by_kind(docs, kind):
            (ep,) = d["spec"]["endpoints"]
            assert ep.get("honorLabels") is True, d
            seen.add((kind, d["metadata"]["name"].rsplit("-", 1)[-1]))
            relabel = ep["relabelings" if kind == "ServiceMonitor" else "relabelConfigs"]
            assert relabel == [{"sourceLabels": ["__meta_kubernetes_namespace"], "targetLabel": "pa_install_namespace"}]
    assert seen == {(k, c) for k in ("ServiceMonitor", "VMServiceScrape") for c in ("operator", "forecaster")}


@pytest.mark.parametrize("bad", [{"notReadyFor": "15 minutes"}, {"modelStaleHours": 0}, {"modelStaleHours": 1.5},
                                 {"journalFor": "soon"}, {"unknown": 1}])
def test_alert_values_are_validated(render, bad):
    p = render({"metrics": {"alerts": bad}}, check=False)
    assert p.returncode != 0


def _render_notes(tmp_path, values, upgrade=False):
    """NOTES.txt rendered offline: helm never renders NOTES in `helm template` and `helm install --dry-run` needs a
    cluster, so a copy of the chart serves it as an ordinary template (--show-only)."""
    import shutil
    chart = tmp_path / "chart"
    if not chart.exists():
        shutil.copytree(CHART, chart)
        shutil.copy(chart / "templates" / "NOTES.txt", chart / "notes-src.txt")
        (chart / "templates" / "notes-under-test.yaml").write_text(
            'apiVersion: v1\nkind: ConfigMap\nmetadata: {name: notes-under-test}\ndata:\n  notes: |\n'
            '{{ tpl (.Files.Get "notes-src.txt") . | indent 4 }}\n')
    f = tmp_path / "v.yaml"
    f.write_text(yaml.safe_dump(values))
    try:
        p = subprocess.run(["helm", "template", "pa", str(chart), "-n", NS, "--kube-version", "1.35.0", "-f", str(f),
                            "--show-only", "templates/notes-under-test.yaml"] + (["--is-upgrade"] if upgrade else []),
                           capture_output=True, text=True, timeout=60)
    except OSError as e:
        if os.environ.get("CI"):
            pytest.fail(f"helm is required in CI: {e}")
        pytest.skip(f"helm not usable here: {e}")
    assert p.returncode == 0, p.stderr
    return yaml.safe_load(p.stdout)["data"]["notes"]


@pytest.mark.parametrize("extra,want,unwanted", [
    ({}, ["NOT renewed automatically"], ["DELETES the model"]),
    ({"forecaster": {"persistence": {"retain": False}}}, ["DELETES the model"], []),
    ({"forecaster": {"persistence": {"retain": False, "existingClaim": "models"}}}, [], ["DELETES the model"]),
    ({"forecaster": {"tls": {"source": "existingSecret", "existingSecret": "x"}}}, [], ["NOT renewed automatically"]),
])
def test_notes_warn_about_self_signed_expiry_and_retention(tmp_path, extra, want, unwanted):
    notes = _render_notes(tmp_path, _merge(BASE, extra))
    for w in want:
        assert w in notes
    for u in unwanted:
        assert u not in notes


def test_notes_on_upgrade_explain_the_crd_and_install_notes_do_not(tmp_path):
    up = _render_notes(tmp_path, BASE, upgrade=True)
    assert "Helm never upgrades the CRD" in up and "at\nleast the required one" in up and "--kubeconfig <file> --context <context>" in up
    assert "Helm never upgrades the CRD" not in _render_notes(tmp_path, BASE)


# --- the statistical forecaster profile (C2.6) ---------------------------------------------------------------------

STAT = {"forecaster": {"engine": "statistical"}, "training": {"targets": []}}


def forecaster_of(docs):
    return next(d for d in by_kind(docs, "Deployment") if d["metadata"]["name"].endswith("-forecaster"))


def test_the_statistical_profile_runs_the_light_image_with_its_own_state(render):
    docs = render(STAT)
    dep = forecaster_of(docs)
    pod = dep["spec"]["template"]["spec"]
    (c,) = pod["containers"]
    assert c["image"].startswith("ghcr.io/bakmurat/predictive-autoscaler-forecaster-statistical:")
    assert c["command"] == ["python", "-m", "api.serve", "statistical"]
    assert c["resources"] == {"requests": {"cpu": "250m", "memory": "256Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}
    assert dep["spec"]["strategy"] == {"type": "Recreate"} and dep["spec"]["replicas"] == 1
    env = {e["name"]: e.get("value") for e in c["env"]}
    assert env["FORECAST_STATE_DIR"] == "/state" and env["FORECAST_STATE_QUOTA_BYTES"] == str(1536 << 20)
    assert env["FORECAST_MAX_AUTOSCALERS"] == "8" and env["FORECAST_AUTOSCALER_IDLE_SECONDS"] == "86400"
    assert "MODEL_DIR" not in env and not set(env) & {"COLD_START_CRONJOB", "ALLOW_BENCHMARK_EXPERIMENTS",
                                                      "ENSEMBLE_EXPERIMENT", "SEASONAL_EXPERIMENT"}
    assert env["FORECASTER_AUTH"] == "tokenreview"                              # the same caller authentication
    mounts = {m["mountPath"]: m.get("readOnly", False) for m in c["volumeMounts"]}
    assert mounts == {"/state": False, "/tmp": False, "/etc/predictive-autoscaler/tls": True}
    assert c["securityContext"] == {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]}}
    assert pod["securityContext"]["runAsUser"] == 10001 and pod["securityContext"]["runAsNonRoot"] is True
    assert dep["spec"]["template"]["metadata"]["annotations"]["autoscaling.devkuban.com/forecaster-engine"] == \
        "statistical"
    (pvc,) = by_kind(docs, "PersistentVolumeClaim")
    assert pvc["metadata"]["name"] == "pa-predictive-autoscaler-forecast-state"
    assert pvc["spec"]["accessModes"] == ["ReadWriteOnce"] and pvc["spec"]["resources"]["requests"]["storage"] == "2Gi"
    assert pvc["metadata"]["annotations"] == {"helm.sh/resource-policy": "keep",
                                              "argocd.argoproj.io/sync-options": "Prune=false,Delete=false"}
    assert not by_kind(docs, "CronJob")                                          # no trainers in this profile
    nps = {d["metadata"]["name"].rsplit("-", 1)[1] for d in by_kind(docs, "NetworkPolicy")}
    assert nps == {"operator", "forecaster", "trainer"}                          # the policies stay


def test_the_statistical_profile_refuses_training_targets(render):
    p = render({"forecaster": {"engine": "statistical"}}, check=False)          # BASE lists two targets
    assert p.returncode != 0 and "has no trainers" in p.stderr


def test_the_statistical_state_can_be_brought_or_released(render):
    docs = render(_merge(STAT, {"forecaster": {"statistical": {"persistence": {"existingClaim": "state-v1",
                                                                               "retain": False}}}}))
    assert not by_kind(docs, "PersistentVolumeClaim")
    claims = {v["persistentVolumeClaim"]["claimName"] for v in forecaster_of(docs)["spec"]["template"]["spec"]["volumes"]
              if "persistentVolumeClaim" in v}
    assert claims == {"state-v1"}
    released = by_kind(render(_merge(STAT, {"forecaster": {"statistical": {"persistence": {"retain": False}}}})),
                       "PersistentVolumeClaim")
    assert "annotations" not in released[0]["metadata"]


def test_switching_profiles_keeps_the_service_and_both_volumes(render):
    # Upgrade legacy -> statistical and roll back: the Deployment keeps its name and (immutable) selector, the Service
    # and the operator's URL stay, and each profile's retained claim survives the other profile's releases.
    legacy, stat = render(), render(STAT)
    for a, b in ((legacy, stat), (stat, legacy)):
        fa, fb = forecaster_of(a), forecaster_of(b)
        assert fa["metadata"]["name"] == fb["metadata"]["name"]
        assert fa["spec"]["selector"] == fb["spec"]["selector"]
        assert [s["spec"] for s in by_kind(a, "Service") if s["metadata"]["name"].endswith("-forecaster")] == \
               [s["spec"] for s in by_kind(b, "Service") if s["metadata"]["name"].endswith("-forecaster")]
    op_env = lambda docs: {e["name"]: e.get("value") for e in next(  # noqa: E731
        d for d in by_kind(docs, "Deployment") if d["metadata"]["name"].endswith("-operator"))
        ["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert op_env(legacy)["ML_API_URL"] == op_env(stat)["ML_API_URL"]
    (models,), (state,) = by_kind(legacy, "PersistentVolumeClaim"), by_kind(stat, "PersistentVolumeClaim")
    assert models["metadata"]["name"] != state["metadata"]["name"]
    for pvc in (models, state):                                   # helm keeps a claim the next release no longer renders
        assert pvc["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    assert forecaster_of(legacy)["spec"]["template"]["metadata"]["annotations"][
        "autoscaling.devkuban.com/forecaster-engine"] == "legacyLstm"           # the default profile is unchanged
    assert by_kind(legacy, "CronJob")


@pytest.mark.parametrize("bad", [{"forecaster": {"engine": "prophet"}},
                                 {"forecaster": {"statistical": {"persistence": {"accessMode": "ReadWriteMany"}}}},
                                 {"forecaster": {"statistical": {"stateQuota": "1.5G"}}},
                                 {"forecaster": {"statistical": {"replicas": 2}}},
                                 {"forecaster": {"statistical": {"maxAutoscalers": 0}}},
                                 {"forecaster": {"statistical": {"maxAutoscalers": 65}}},
                                 {"forecaster": {"statistical": {"maxAutoscalers": "8"}}},
                                 {"forecaster": {"statistical": {"autoscalerIdleReleaseHours": -1}}},
                                 {"forecaster": {"statistical": {"autoscalerIdleReleaseHours": "24h"}}}])
def test_the_statistical_values_are_validated(render, bad):
    assert render(_merge(STAT, bad), check=False).returncode != 0


@pytest.mark.parametrize("quota,size,ok", [("1536Mi", "2Gi", True), ("1638Mi", "2Gi", True), ("1639Mi", "2Gi", False),
                                           ("2Gi", "2Gi", False), ("3Gi", "2Gi", False), ("8Gi", "10Gi", True),
                                           ("1228Mi", "1.5Gi", True), ("1229Mi", "1.5Gi", False),
                                           ("800Mi", "1024000Ki", True), ("801Mi", "1024000Ki", False)])
def test_the_state_quota_must_leave_a_fifth_of_the_charts_volume(render, quota, size, ok):
    # R3-10 f: a quota at or above the volume's size never takes effect; the volume fills first and the journal fails.
    p = render(_merge(STAT, {"forecaster": {"statistical": {"stateQuota": quota, "persistence": {"size": size}}}}),
               check=False)
    assert (p.returncode == 0) is ok, p.stderr
    if not ok:
        assert f"stateQuota ({quota}) must be at most 80% of forecaster.statistical.persistence.size ({size})" in p.stderr


def test_the_state_quota_is_not_checked_against_a_brought_claim_or_the_other_engine(render):
    big = {"stateQuota": "50Gi"}                                   # the chart cannot see an existing claim's capacity
    docs = render(_merge(STAT, {"forecaster": {"statistical": {**big, "persistence": {"existingClaim": "state-v1"}}}}))
    env = {e["name"]: e.get("value") for e in forecaster_of(docs)["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["FORECAST_STATE_QUOTA_BYTES"] == str(50 << 30)
    render({"forecaster": {"statistical": big}})                   # legacyLstm: the statistical values are unused
    helm_doc = (ROOT / "docs" / "helm.md").read_text()
    assert "at most 80 % of" in helm_doc and "existingClaim" in helm_doc


def test_the_autoscaler_limit_and_idle_release_are_values(render):
    docs = render(_merge(STAT, {"forecaster": {"statistical": {"maxAutoscalers": 24, "autoscalerIdleReleaseHours": 0}}}))
    env = {e["name"]: e.get("value") for e in forecaster_of(docs)["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["FORECAST_MAX_AUTOSCALERS"] == "24" and env["FORECAST_AUTOSCALER_IDLE_SECONDS"] == "0"
    legacy = {e["name"] for e in forecaster_of(render())["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert not legacy & {"FORECAST_MAX_AUTOSCALERS", "FORECAST_AUTOSCALER_IDLE_SECONDS"}    # the statistical engine's only
    helm_doc = (ROOT / "docs" / "helm.md").read_text()
    assert all(v in helm_doc for v in ("forecaster.statistical.maxAutoscalers", "AutoscalerLimitReached",
                                       "forecaster.statistical.autoscalerIdleReleaseHours"))


def test_a_lab_qualification_is_opt_in_read_only_and_statistical_only(render):
    # C2.8 lab evaluation only: off by default; when set, one read-only file and get on the kube-system Namespace.
    assert not [r for r in by_kind(render(STAT), "ClusterRole") if r["metadata"]["name"].endswith("cluster-identity")]
    lab = _merge(STAT, {"forecaster": {"statistical": {"labQualification": {"configMap": "pa-lab", "key": "q.json"}}}})
    undigested = render(lab, check=False)                              # the evaluation image must be pinned by digest
    assert undigested.returncode != 0 and "needs images.statisticalForecaster.digest" in undigested.stderr
    digest = "sha256:" + "d" * 64
    lab = _merge(lab, {"images": {"statisticalForecaster": {"digest": digest}}})
    docs = render(lab)
    assert rules_of(docs, "pa-predictive-autoscaler-forecaster-cluster-identity") == {
        (("",), ("namespaces",), ("kube-system",), ("get",))}
    (binding,) = [b for b in by_kind(docs, "ClusterRoleBinding") if b["metadata"]["name"].endswith("cluster-identity")]
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": "pa-predictive-autoscaler-forecaster",
                                    "namespace": NS}]
    pod = forecaster_of(docs)["spec"]["template"]["spec"]
    (c,) = pod["containers"]
    env = {e["name"]: e.get("value") for e in c["env"]}
    assert env["FORECAST_LAB_QUALIFICATION"] == "/etc/predictive-autoscaler/lab-qualification/lab-qualification.json"
    assert env["FORECAST_IMAGE_DIGEST"] == digest and c["image"].endswith("@" + digest)
    mounts = {m["mountPath"]: m.get("readOnly", False) for m in c["volumeMounts"]}
    assert mounts["/etc/predictive-autoscaler/lab-qualification"] is True
    vol = next(v for v in pod["volumes"] if v["name"] == "lab-qualification")
    assert vol["configMap"] == {"name": "pa-lab", "items": [{"key": "q.json", "path": "lab-qualification.json"}]}
    legacy = render({"forecaster": {"statistical": {"labQualification": {"configMap": "pa-lab"}}}}, check=False)
    assert legacy.returncode != 0 and "needs forecaster.engine: statistical" in legacy.stderr
    assert render(_merge(STAT, {"forecaster": {"statistical": {"labQualification": {"configMap": "Bad_Name"}}}}),
                  check=False).returncode != 0


def test_a_demonstration_qualification_is_opt_in_read_only_statistical_only_and_never_beside_a_lab_one(render):
    # A scoped live demonstration only: off by default; when set, two read-only files from one ConfigMap (the
    # qualification and the contract it names) and get on the kube-system Namespace; never with a lab qualification.
    base = render(STAT)
    assert not [r for r in by_kind(base, "ClusterRole") if r["metadata"]["name"].endswith("cluster-identity")]
    (c0,) = forecaster_of(base)["spec"]["template"]["spec"]["containers"]
    assert not [e for e in c0["env"] if "QUALIFICATION" in e["name"] or "DEMONSTRATION" in e["name"]]
    demo = _merge(STAT, {"forecaster": {"statistical": {"demonstrationQualification": {
        "configMap": "pa-demo", "key": "q.json", "contractKey": "c.json"}}}})
    undigested = render(demo, check=False)                             # the evaluation image must be pinned by digest
    assert undigested.returncode != 0 and "needs images.statisticalForecaster.digest" in undigested.stderr
    digest = "sha256:" + "d" * 64
    demo = _merge(demo, {"images": {"statisticalForecaster": {"digest": digest}}})
    docs = render(demo)
    assert rules_of(docs, "pa-predictive-autoscaler-forecaster-cluster-identity") == {
        (("",), ("namespaces",), ("kube-system",), ("get",))}
    (binding,) = [b for b in by_kind(docs, "ClusterRoleBinding") if b["metadata"]["name"].endswith("cluster-identity")]
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": "pa-predictive-autoscaler-forecaster",
                                    "namespace": NS}]
    pod = forecaster_of(docs)["spec"]["template"]["spec"]
    (c,) = pod["containers"]
    assert len({e["name"] for e in c["env"]}) == len(c["env"])                       # no variable set twice
    env = {e["name"]: e.get("value") for e in c["env"]}
    where = "/etc/predictive-autoscaler/demonstration-qualification"
    assert env["FORECAST_DEMONSTRATION_QUALIFICATION"] == f"{where}/demonstration-qualification.json"
    assert env["FORECAST_DEMONSTRATION_CONTRACT"] == f"{where}/demonstration-contract.json"
    assert env["FORECAST_IMAGE_DIGEST"] == digest and c["image"].endswith("@" + digest)
    assert "FORECAST_LAB_QUALIFICATION" not in env
    mounts = {m["mountPath"]: m.get("readOnly", False) for m in c["volumeMounts"]}
    assert mounts[where] is True and "/etc/predictive-autoscaler/lab-qualification" not in mounts
    vol = next(v for v in pod["volumes"] if v["name"] == "demonstration-qualification")
    assert vol["configMap"] == {"name": "pa-demo", "items": [
        {"key": "q.json", "path": "demonstration-qualification.json"},
        {"key": "c.json", "path": "demonstration-contract.json"}]}
    assert not [v for v in pod["volumes"] if v["name"] == "lab-qualification"]
    # The default keys; and nothing else in the release differs from the plain statistical install with that digest.
    defaults = render(_merge(demo, {"forecaster": {"statistical": {"demonstrationQualification": {
        "configMap": "pa-demo", "key": "demonstration-qualification.json",
        "contractKey": "demonstration-contract.json"}}}}))
    vol = next(v for v in forecaster_of(defaults)["spec"]["template"]["spec"]["volumes"]
               if v["name"] == "demonstration-qualification")
    assert [i["key"] for i in vol["configMap"]["items"]] == ["demonstration-qualification.json",
                                                             "demonstration-contract.json"]
    plain = render(_merge(STAT, {"images": {"statisticalForecaster": {"digest": digest}}}))
    def other(ds):                      # everything but the forecaster Deployment, the identity read and the TLS Secret
        return [d for d in ds if d["kind"] != "Secret" and not d["metadata"]["name"].endswith("cluster-identity")
                and not (d["kind"] == "Deployment" and d["metadata"]["name"].endswith("-forecaster"))]
    assert other(docs) == other(plain) and len(other(docs)) == len(docs) - 4
    # Mutually exclusive with a lab qualification, whatever else is set.
    both = render(_merge(demo, {"forecaster": {"statistical": {"labQualification": {"configMap": "pa-lab"}}}}),
                  check=False)
    assert both.returncode != 0 and "at most one of the two documents" in both.stderr
    legacy = render({"forecaster": {"statistical": {"demonstrationQualification": {"configMap": "pa-demo"}}}},
                    check=False)
    assert legacy.returncode != 0 and "needs forecaster.engine: statistical" in legacy.stderr
    same_key = render(_merge(demo, {"forecaster": {"statistical": {"demonstrationQualification": {
        "contractKey": "q.json"}}}}), check=False)
    assert same_key.returncode != 0 and "two different keys" in same_key.stderr
    for bad in ({"configMap": "Bad_Name"}, {"key": "a/b"}, {"contractKey": ""}, {"contract": "c.json"},
                {"gates": "pass"}):
        assert render(_merge(demo, {"forecaster": {"statistical": {"demonstrationQualification": bad}}}),
                      check=False).returncode != 0, bad


def test_pod_labels_reach_the_pods_and_never_replace_the_selector(render):
    opt_out = {"sidecar.istio.io/inject": "false"}
    docs = render({"operator": {"podLabels": opt_out}, "forecaster": {"podLabels": opt_out}})
    for d in by_kind(docs, "Deployment"):
        labels = d["spec"]["template"]["metadata"]["labels"]
        assert labels["sidecar.istio.io/inject"] == "false", d["metadata"]["name"]
        assert all(labels[k] == v for k, v in d["spec"]["selector"]["matchLabels"].items())
        assert "sidecar.istio.io/inject" not in d["spec"]["selector"]["matchLabels"]
    for key in ("app.kubernetes.io/name", "app.kubernetes.io/instance", "app.kubernetes.io/component"):
        p = render({"operator": {"podLabels": {key: "x"}}}, check=False)
        assert p.returncode != 0 and "podLabels" in p.stderr, key
