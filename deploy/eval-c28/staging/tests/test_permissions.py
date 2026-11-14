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
            word = overrides.get(key, "yes" if verb == "get" else "no")
            if word == "ERR":
                return 1, ""
            return (0 if word == "yes" else 1), word + "\n"
        kind = args[1]
        if kind in fail:
            return 1, ""
        return 0, json.dumps({"kind": "List", "items": lists[kind]})
    run.calls = calls
    return run


def check(lists=None, overrides=None, fail=(), targets=T):
    k = fake_kubectl(lists or rbac(), overrides, fail)
    return P.verdict(P.collect(k, SA_NS, SA, NS, targets), SA_NS, SA, targets), k


def test_the_shadow_release_passes_and_every_question_was_asked():
    v, k = check()
    assert v["verdict"] == "pass", v
    asked = {(c[2], c[3], "--subresource=scale" in c) for c in k.calls if c[0] == "auth"}
    for name in ["deployments.apps"] + [f"deployments.apps/{t}" for t in T]:
        assert {("update", name, True), ("patch", name, True), ("get", name, True)} <= asked
    assert {("update", "deployments.apps", False), ("delete", "pods", False), ("create", "pods", False)} <= asked
    assert {c[1] for c in k.calls if c[0] == "get"} == set(P.RBAC_KINDS)


@pytest.mark.parametrize("overrides, text", [
    ({("update", "deployments.apps/c28-demo-pa-n", True): "yes"}, "allows update on deployments/scale of Deployment"),
    ({("patch", "deployments.apps", True): "yes"}, "allows patch on deployments/scale of any"),
    ({("update", "deployments.apps", False): "yes"}, "allows update on deployments.apps"),
    ({("create", "pods", False): "yes"}, "allows create on pods"),
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
