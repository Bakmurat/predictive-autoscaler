"""permissions.py: the operator has no replica-write permission, from the authorizer's answers (impersonating the
ServiceAccount with its three groups) and a whole-cluster RBAC audit; errors and unclear answers are unproven."""

import copy
import json

import pytest

import permissions as P

SA_NS, SA, NS = "pa-eval-system", "pa-eval-predictive-autoscaler-operator", "demo"
T = ["c28-demo-pa-n", "c28-demo-pa-r"]


def rbac():
    role = {"kind": "ClusterRole", "metadata": {"name": "op-write"},
            "rules": [{"apiGroups": ["apps"], "resources": ["deployments/scale"], "verbs": ["get"]},
                      {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]},
                      {"apiGroups": ["autoscaling.devkuban.com"], "resources": ["predictiveautoscalers/status"],
                       "verbs": ["get", "update", "patch"]}]}
    rb = {"kind": "RoleBinding", "metadata": {"name": "op-write", "namespace": NS},
          "roleRef": {"kind": "ClusterRole", "name": "op-write"},
          "subjects": [{"kind": "ServiceAccount", "name": SA, "namespace": SA_NS}]}
    return {"clusterroles": [role], "roles": [], "clusterrolebindings": [], "rolebindings": [rb]}


def fake_kubectl(lists, overrides=None, fail=()):
    overrides = overrides or {}
    calls = []

    def run(args):
        calls.append(args)
        if args[0] == "auth":
            assert "--as=system:serviceaccount:pa-eval-system:pa-eval-predictive-autoscaler-operator" in args
            assert {"--as-group=system:serviceaccounts", f"--as-group=system:serviceaccounts:{SA_NS}",
                    "--as-group=system:authenticated"} <= set(args)
            verb, res = args[2], args[3]
            key = (verb, res, "--subresource=scale" in args)
            if "-A" in args:
                key = key + ("-A",)
            elif args[args.index("-n") + 1] != "demo":
                key = key + (args[args.index("-n") + 1],)
            word = overrides.get(key, "yes" if verb == "get" else "no")
            if word == "ERR":
                return 1, ""
            return (0 if word == "yes" else 1), word + "\n"
        kind = args[1]
        if kind in fail:
            return 1, ""
        if kind == "namespace" and args[2] == "kube-system":
            return 0, json.dumps({"metadata": {"uid": "cluster-uid-1"}})
        if kind == "namespaces":
            return 0, json.dumps({"items": [{"metadata": {"name": n}} for n in ("demo", "kube-system", "other")]})
        return 0, json.dumps({"kind": "List", "items": lists[kind]})
    run.calls = calls
    return run


def check(lists=None, overrides=None, fail=(), targets=T, cluster="cluster-uid-1"):
    k = fake_kubectl(lists or rbac(), overrides, fail)
    return P.verdict(P.collect(k, SA_NS, SA, NS, targets), SA_NS, SA, targets, cluster), k


def test_the_shadow_release_passes_and_every_question_was_asked():
    v, k = check()
    assert v["verdict"] == "pass", v
    asked = {(c[2], c[3], "--subresource=scale" in c) for c in k.calls if c[0] == "auth"}
    for name in ["deployments.apps"] + [f"deployments.apps/{t}" for t in T]:
        assert {("update", name, True), ("patch", name, True), ("get", name, True)} <= asked
    assert {("update", "deployments.apps", False), ("delete", "pods", False), ("create", "pods", False)} <= asked
    assert {c[1] for c in k.calls if c[0] == "get"} == set(P.RBAC_KINDS) | {"namespaces", "namespace"}
    ns_asked = {c[c.index("-n") + 1] for c in k.calls if c[0] == "auth" and "-n" in c and "deployments.apps" == c[3]}
    assert {"demo", "kube-system", "other"} <= ns_asked and any("-A" in c for c in k.calls if c[0] == "auth")


@pytest.mark.parametrize("overrides, text", [
    ({("update", "deployments.apps/c28-demo-pa-n", True): "yes"}, "allows update on deployments/scale of Deployment"),
    ({("patch", "deployments.apps", True): "yes"}, "allows patch on deployments/scale of any"),
    ({("update", "deployments.apps", False): "yes"}, "allows update on deployments.apps"),
    ({("create", "pods", False): "yes"}, "allows create on pods"),
    ({("update", "deployments.apps", True, "other"): "yes"}, "in namespace other"),
    ({("patch", "deployments.apps", True, "-A"): "yes"}, "cluster-wide"),
])
def test_any_write_answer_refuses(overrides, text):
    v, _ = check(overrides=overrides)
    assert v["verdict"] == "refused" and any(text in x for x in v["refused"]), v["refused"]


@pytest.mark.parametrize("edit, text", [
    (lambda r: r["clusterroles"][0]["rules"].append(
        {"apiGroups": ["apps"], "resources": ["deployments/scale"], "verbs": ["update"],
         "resourceNames": ["c28-demo-pa-n"]}), "resourceNames"),                 # name-restricted is still a grant
    (lambda r: r["clusterroles"][0]["rules"].append(
        {"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}), "authorizes"),
    (lambda r: r["rolebindings"].append(
        {"kind": "RoleBinding", "metadata": {"name": "elsewhere", "namespace": "other"},
         "roleRef": {"kind": "Role", "name": "w"}, "subjects": [{"kind": "Group", "name": "system:authenticated"}]})
     or r["roles"].append({"kind": "Role", "metadata": {"name": "w", "namespace": "other"},
                           "rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["delete"]}]}), "pods"),
])
def test_any_write_grant_reaching_the_identity_refuses(edit, text):
    r = rbac()
    edit(r)
    v, _ = check(lists=r)
    assert v["verdict"] == "refused" and any(text in x for x in v["refused"]), v["refused"]


@pytest.mark.parametrize("kw, text", [
    ({"overrides": {("update", "deployments.apps/c28-demo-pa-r", True): "ERR"}}, "no clear answer"),
    ({"overrides": {("get", "deployments.apps", True): "no"}}, "control failed"),
    ({"fail": ("rolebindings",)}, "could not be listed"),
    ({"fail": ("namespaces",)}, "namespaces could not be listed"),
    ({"targets": []}, "no target"),
])
def test_errors_and_unclear_answers_are_unproven(kw, text):
    v, _ = check(**kw)
    assert v["verdict"] == "unproven" and any(text in x for x in v["unproven"]), v["unproven"]


def test_a_binding_to_a_missing_role_is_unproven():
    r = rbac()
    r["rolebindings"][0]["roleRef"]["name"] = "gone"
    v, _ = check(lists=r)
    assert v["verdict"] == "unproven" and any("not found" in x for x in v["unproven"])


def test_another_identitys_grant_is_not_ours():
    r = rbac()
    r["clusterroles"].append({"kind": "ClusterRole", "metadata": {"name": "hpa"},
                              "rules": [{"apiGroups": ["apps"], "resources": ["deployments/scale"], "verbs": ["update"]}]})
    r["clusterrolebindings"].append({"kind": "ClusterRoleBinding", "metadata": {"name": "hpa"},
                                     "roleRef": {"kind": "ClusterRole", "name": "hpa"},
                                     "subjects": [{"kind": "ServiceAccount", "name": "hpa", "namespace": "kube-system"}]})
    assert check(lists=r)[0]["verdict"] == "pass"



def test_only_exit_0_yes_and_exit_1_no_count():
    def seven(args):
        if args[0] == "auth":
            return 7, ("yes" if args[2] == "get" else "no") + "\n"          # right words, wrong exit code
        return fake_kubectl(rbac())(args)
    v = P.verdict(P.collect(seven, SA_NS, SA, NS, T), SA_NS, SA, T, "cluster-uid-1")
    assert v["verdict"] == "unproven" and any("no clear answer (error)" in x for x in v["unproven"])

    def boom(args):
        if args[0] == "auth":
            raise TimeoutError("kubectl timed out")
        return fake_kubectl(rbac())(args)
    v = P.verdict(P.collect(boom, SA_NS, SA, NS, T), SA_NS, SA, T, "cluster-uid-1")
    assert v["verdict"] == "unproven"


def test_the_answering_cluster_must_be_the_recorded_one():
    v, _ = check(cluster="another-cluster")
    assert v["verdict"] == "unproven" and any("not the recorded cluster" in x for x in v["unproven"])
    assert v["cluster_uid"] == "cluster-uid-1" and v["recorded_cluster_uid"] == "another-cluster"


# --- all three identities against the chart's RENDERED roles (a small RBAC evaluator answers can-i) ----------------------

def rendered_rbac():
    import shutil
    import subprocess
    import os
    import yaml
    if not shutil.which("helm"):
        pytest.skip("helm not installed")
    chart = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "charts", "predictive-autoscaler")
    out = subprocess.run(["helm", "template", "pa-eval", chart, "-n", SA_NS, "--set", "operator.rbac.scaleWrite=false",
                          "--set", "operator.watchNamespaces={demo}", "--set", "prometheus.url=http://vm.invalid:8481",
                          "--set", "networkPolicy.enabled=false"],
                         capture_output=True, text=True, check=True).stdout
    docs = [d for d in yaml.safe_load_all(out) if d and d.get("kind") in
            ("ClusterRole", "Role", "ClusterRoleBinding", "RoleBinding")]
    return docs


def authorizer(docs, sa):
    def allowed(verb, res, sub, ns, name):
        group = "" if res == "pods" else "apps"
        want = res.split(".")[0] + ("/scale" if sub else "")
        roles = {(d["kind"], d["metadata"].get("namespace") or "", d["metadata"]["name"]): d for d in docs
                 if d["kind"] in ("ClusterRole", "Role")}
        for b in docs:
            if b["kind"] not in ("ClusterRoleBinding", "RoleBinding"):
                continue
            bns = b["metadata"].get("namespace") or ""
            if b["kind"] == "RoleBinding" and ns not in (bns,):
                continue
            if not any(s.get("kind") == "ServiceAccount" and s.get("name") == sa for s in b.get("subjects") or []):
                continue
            ref = b["roleRef"]
            role = roles.get((ref["kind"], bns if ref["kind"] == "Role" else "", ref["name"]))
            for r in (role or {}).get("rules") or []:
                if (group in r.get("apiGroups", []) or "*" in r.get("apiGroups", [])) and \
                        (want in r.get("resources", []) or "*" in r.get("resources", [])) and \
                        (verb in r.get("verbs", []) or "*" in r.get("verbs", [])) and \
                        (not r.get("resourceNames") or name in r["resourceNames"]):
                    return True
        return False

    def run(args):
        if args[0] == "auth":
            verb, res = args[2], args[3]
            name = res.split("/", 1)[1] if "/" in res else None
            ns = args[args.index("-n") + 1] if "-n" in args else "*"
            ok = allowed(verb, res.split("/")[0], "--subresource=scale" in args, ns, name)
            return (0, "yes\n") if ok else (1, "no\n")
        kind = args[1]
        if kind == "namespace":
            return 0, json.dumps({"metadata": {"uid": "cluster-uid-1"}})
        if kind == "namespaces":
            return 0, json.dumps({"items": [{"metadata": {"name": n}} for n in ("demo", "pa-eval-system", "other")]})
        k = {"clusterroles": "ClusterRole", "roles": "Role", "clusterrolebindings": "ClusterRoleBinding",
             "rolebindings": "RoleBinding"}[kind]
        return 0, json.dumps({"items": [d for d in docs if d["kind"] == k]})
    return run


@pytest.mark.parametrize("role", P.ROLES)
def test_each_identity_passes_against_its_rendered_roles(role):
    docs = rendered_rbac()
    sa = f"pa-eval-predictive-autoscaler-{role}"
    v = P.verdict(P.collect(authorizer(docs, sa), SA_NS, sa, NS, T, role), SA_NS, sa, T, "cluster-uid-1")
    assert v["verdict"] == "pass", (role, v["refused"], v["unproven"])
