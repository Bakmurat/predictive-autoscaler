"""The objects that run the transition watchdog (transition_objects.py): its whole authority is two Roles."""
import os
import re

import pytest

import transition_objects as O
import transition_watchdog as W

RUN, REL, CTL, NS, ACT = "c28-demo-1", "pa-eval-predictive-autoscaler", "pa-eval-system", "demo", "reh1"
IMAGE = "harbor.devkuban.com/autoscaler/statistical-evaluation@sha256:" + "ab" * 32
SCRIPT = open(os.path.join(os.path.dirname(W.__file__), "transition_watchdog.py")).read()


def dep(**kw):
    args = dict(image=IMAGE, script=SCRIPT, deadline_epoch=1_800_000_000.5, trigger_lease_uid="uid-1",
                excluded_nodes=["kg699-2nsgd", "kg699-8vnbw"])
    args.update(kw)
    return O.deployment(RUN, REL, CTL, NS, ACT, **args)


def everything():
    return O.rbac(RUN, REL, CTL, NS, ACT) + O.leases(RUN, REL, CTL, ACT) + [
        O.configmap(RUN, REL, CTL, ACT, SCRIPT), dep()]


def test_the_authority_is_exactly_two_namespaced_roles():
    objs = O.rbac(RUN, REL, CTL, NS, ACT)
    assert [o["kind"] for o in objs] == ["ServiceAccount", "Role", "RoleBinding", "Role", "RoleBinding"]
    assert not [o for o in everything() if o["kind"].startswith("Cluster")]
    sa, target_role, target_bind, control_role, control_bind = objs
    name = "c28-transition-watchdog-reh1"
    assert sa["metadata"] == {"name": name, "namespace": CTL, "labels": O.labels(RUN, ACT)}
    # In the target namespace: get and delete on ONE RoleBinding, by name. Nothing else.
    assert target_role["metadata"]["namespace"] == NS and target_role["rules"] == [
        {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["rolebindings"],
         "resourceNames": ["pa-eval-predictive-autoscaler-operator-scale"], "verbs": ["get", "delete"]}]
    # In the control namespace: get on the heartbeat, get and update on the trigger Lease, each by name.
    assert control_role["metadata"]["namespace"] == CTL and control_role["rules"] == [
        {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"],
         "resourceNames": ["c28-transition-heartbeat-reh1"], "verbs": ["get"]},
        {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"],
         "resourceNames": ["c28-transition-trigger-reh1"], "verbs": ["get", "update"]}]
    for role, bind in ((target_role, target_bind), (control_role, control_bind)):
        assert bind["metadata"]["namespace"] == role["metadata"]["namespace"]
        assert bind["roleRef"] == {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name}
        assert bind["subjects"] == [{"kind": "ServiceAccount", "name": name, "namespace": CTL}]


def test_no_rule_can_grant_scale_or_change_an_autoscaler():
    forbidden_verbs = {"create", "patch", "deletecollection", "escalate", "bind", "impersonate", "*", "list", "watch"}
    for o in everything():
        for rule in o.get("rules", []):
            assert rule["resourceNames"], rule                     # every rule names its objects
            assert not forbidden_verbs & set(rule["verbs"]), rule
            assert "*" not in rule["resources"] and "*" not in rule["apiGroups"]
            assert not {"deployments", "deployments/scale", "predictiveautoscalers", "roles", "clusterroles",
                        "clusterrolebindings", "secrets", "pods"} & set(rule["resources"]), rule
            if "update" in rule["verbs"]:
                assert rule["resources"] == ["leases"] and rule["resourceNames"] == ["c28-transition-trigger-reh1"]
            if "delete" in rule["verbs"]:
                assert rule["resources"] == ["rolebindings"] and len(rule["resourceNames"]) == 1


def test_the_trigger_lease_starts_in_the_state_the_watchdog_arms_from():
    heartbeat, trigger = O.leases(RUN, REL, CTL, ACT)
    assert trigger["metadata"]["name"] == "c28-transition-trigger-reh1" and trigger["metadata"]["namespace"] == CTL
    holder = trigger["spec"]["holderIdentity"]
    assert W.parse_holder(holder) == ("unarmed", ACT) and W.forward(holder, "armed:abc")
    assert heartbeat["metadata"]["name"] == "c28-transition-heartbeat-reh1"
    assert W.parse_holder(heartbeat["spec"]["holderIdentity"]) is None   # the heartbeat is not a trigger state


def test_the_deployment_passes_arguments_the_watchdog_accepts():
    d = dep()
    (c,) = d["spec"]["template"]["spec"]["containers"]
    assert c["command"] == ["python", "/watchdog/transition_watchdog.py"]
    flags = dict(zip(c["args"][::2], c["args"][1::2]))
    assert len(c["args"]) % 2 == 0 and len(flags) == len(c["args"]) // 2
    declared = dict(re.findall(r'add_argument\("(--[a-z-]+)"((?:(?!add_argument).)*)', SCRIPT, flags=re.S))
    assert set(flags) <= set(declared), set(flags) - set(declared)
    required = {flag for flag, rest in declared.items() if "required=True" in rest}
    assert required and required <= set(flags), required - set(flags)
    assert flags == {
        "--activation": "reh1", "--deadline-epoch": "1800000000.5", "--binding-namespace": "demo",
        "--binding": "pa-eval-predictive-autoscaler-operator-scale", "--lease-namespace": "pa-eval-system",
        "--trigger-lease": "c28-transition-trigger-reh1", "--trigger-lease-uid": "uid-1",
        "--heartbeat-lease": "c28-transition-heartbeat-reh1", "--interval-seconds": "60.0",
        "--stale-intervals": "3", "--pass-seconds": "5.0"}
    # The names in the arguments are the ones the Roles allow.
    target_role, control_role = [o for o in O.rbac(RUN, REL, CTL, NS, ACT) if o["kind"] == "Role"]
    assert [flags["--binding"]] == target_role["rules"][0]["resourceNames"]
    assert [flags["--heartbeat-lease"]] == control_role["rules"][0]["resourceNames"]
    assert [flags["--trigger-lease"]] == control_role["rules"][1]["resourceNames"]
    # The watchdog itself accepts the timing values (it refuses non-finite ones).
    w = W.Watchdog(None, activation=flags["--activation"], deadline=float(flags["--deadline-epoch"]),
                   binding_ns=flags["--binding-namespace"], binding=flags["--binding"],
                   lease_ns=flags["--lease-namespace"], trigger_lease=flags["--trigger-lease"],
                   trigger_uid=flags["--trigger-lease-uid"], heartbeat_lease=flags["--heartbeat-lease"],
                   interval=float(flags["--interval-seconds"]), stale_intervals=int(flags["--stale-intervals"]))
    assert w.deadline == 1_800_000_000.5


def test_the_deployment_runs_one_unprivileged_pod_off_the_reserved_nodes_without_an_expiry():
    d = dep()
    spec, pod = d["spec"], d["spec"]["template"]["spec"]
    assert d["kind"] == "Deployment" and spec["replicas"] == 1 and spec["strategy"] == {"type": "Recreate"}
    assert "activeDeadlineSeconds" not in pod and pod["restartPolicy"] == "Always"
    assert spec["selector"]["matchLabels"] == spec["template"]["metadata"]["labels"] == O.labels(RUN, ACT)
    assert pod["serviceAccountName"] == "c28-transition-watchdog-reh1" and pod["automountServiceAccountToken"] is True
    assert pod["securityContext"] == {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
                                      "seccompProfile": {"type": "RuntimeDefault"}}
    (c,) = pod["containers"]
    assert c["image"] == IMAGE
    assert c["securityContext"] == {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]}}
    assert "tolerations" not in pod                                 # the reservation taint keeps it off W and S too
    (term,) = pod["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
    # By node NAME: one single-value NotIn field requirement per reserved node, in ONE term; no label is consulted.
    assert term == {"matchFields": [{"key": "metadata.name", "operator": "NotIn", "values": ["kg699-2nsgd"]},
                                    {"key": "metadata.name", "operator": "NotIn", "values": ["kg699-8vnbw"]}]}
    (vol,) = pod["volumes"]
    assert vol["configMap"]["name"] == "c28-transition-watchdog-reh1"
    assert c["volumeMounts"] == [{"name": "script", "mountPath": "/watchdog", "readOnly": True}]


def admits(deployment, node_name, node_labels):
    """Would the Deployment's REQUIRED node affinity admit this node? A small model of the scheduler's rule, written
    for this test (it is not the scheduler): terms are ORed; inside a term every matchExpressions requirement (on
    labels) and every matchFields requirement (on metadata.name, one value each) must hold."""
    affinity = deployment["spec"]["template"]["spec"]["affinity"]["nodeAffinity"]
    terms = affinity["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]

    def holds(value, requirement):
        assert requirement["operator"] in ("In", "NotIn")
        inside = value in requirement["values"]
        return inside if requirement["operator"] == "In" else not inside

    def term_ok(term):
        for r in term.get("matchFields", []):
            assert r["key"] == "metadata.name" and len(r["values"]) == 1     # what the API accepts for a field
            if not holds(node_name, r):
                return False
        return all(holds(node_labels.get(r["key"]), r) for r in term.get("matchExpressions", []))
    return any(term_ok(t) for t in terms)


def test_a_reserved_node_is_excluded_by_its_name_whatever_its_hostname_label_says():
    # The code review's finding: the exclusion compared the kubernetes.io/hostname LABEL with node NAMES. A reserved
    # node whose label differs from its name passed.
    d = dep()
    assert not admits(d, "kg699-2nsgd", {"kubernetes.io/hostname": "kg699-2nsgd"})
    assert not admits(d, "kg699-8vnbw", {"kubernetes.io/hostname": "kg699-8vnbw"})
    for label in ("some-other-host", "ip-10-0-0-7", None):                 # name and hostname differ, or no label
        labels = {} if label is None else {"kubernetes.io/hostname": label}
        assert not admits(d, "kg699-2nsgd", labels), label
        assert not admits(d, "kg699-8vnbw", labels), label
    # And the reverse: another node is admitted even if its hostname label happens to equal a reserved NAME.
    assert admits(d, "kg699-gdjn7", {"kubernetes.io/hostname": "kg699-gdjn7"})
    assert admits(d, "kg699-gdjn7", {"kubernetes.io/hostname": "kg699-2nsgd"})
    assert admits(d, "kg699-crwnv", {})
    pod = d["spec"]["template"]["spec"]
    assert "nodeSelector" not in pod and "nodeName" not in pod
    assert "kubernetes.io/hostname" not in repr(pod["affinity"])           # no label decides the exclusion


def test_the_script_is_pinned_by_its_sha256_everywhere():
    cm, d = O.configmap(RUN, REL, CTL, ACT, SCRIPT), dep()
    sha = O.script_sha256(SCRIPT)
    assert cm["data"] == {"transition_watchdog.py": SCRIPT} and cm["immutable"] is True
    assert cm["metadata"]["annotations"] == {"c28.devkuban.com/script-sha256": sha}
    assert d["metadata"]["annotations"] == {"c28.devkuban.com/script-sha256": sha}
    assert d["spec"]["template"]["metadata"]["annotations"] == {"c28.devkuban.com/script-sha256": sha}
    other = dep(script=SCRIPT + "\n# changed\n")
    assert other["spec"]["template"]["metadata"]["annotations"] != d["spec"]["template"]["metadata"]["annotations"]


def test_two_activations_share_no_object_name_and_no_allowed_lease():
    def keys(act):
        objs = O.rbac(RUN, REL, CTL, NS, act) + O.leases(RUN, REL, CTL, act) + [O.configmap(RUN, REL, CTL, act, SCRIPT)]
        named = {(o["kind"], o["metadata"]["namespace"], o["metadata"]["name"]) for o in objs}
        allowed = {n for o in objs for r in o.get("rules", []) if r["resources"] == ["leases"] for n in r["resourceNames"]}
        return named, allowed
    a, b = keys("reh1"), keys("demo-2")
    assert not a[0] & b[0] and not a[1] & b[1]


@pytest.mark.parametrize("kw", [
    {"image": "harbor.devkuban.com/autoscaler/statistical-evaluation:latest"},
    {"image": "harbor.devkuban.com/x@sha256:abc"}, {"image": ""},
    {"deadline_epoch": float("nan")}, {"deadline_epoch": float("inf")}, {"deadline_epoch": 0}, {"deadline_epoch": True},
    {"deadline_epoch": "1800000000"}, {"trigger_lease_uid": ""}, {"trigger_lease_uid": " uid"}, {"trigger_lease_uid": None},
    {"excluded_nodes": []}, {"excluded_nodes": "kg699-2nsgd"}, {"excluded_nodes": [""]},
    {"excluded_nodes": ["kg699-2nsgd", "kg699-2nsgd"]}, {"excluded_nodes": ["Not A Node"]},
    {"excluded_nodes": ["kg699-2nsgd", None]},
    {"interval_seconds": 0}, {"interval_seconds": float("nan")}, {"pass_seconds": -1}, {"stale_intervals": 0},
    {"stale_intervals": 2.5}, {"stale_intervals": True},
])
def test_a_deployment_that_could_not_bound_anything_is_refused(kw):
    with pytest.raises(ValueError):
        dep(**kw)


@pytest.mark.parametrize("act", ["", "Reh1", "reh_1", "-reh1", "reh1-", "a" * 33, "reh 1", "reh1\n", None, 7])
def test_an_activation_id_that_is_not_a_short_dns_label_is_refused(act):
    for call in (lambda: O.names(REL, act), lambda: O.leases(RUN, REL, CTL, act),
                 lambda: O.rbac(RUN, REL, CTL, NS, act), lambda: O.configmap(RUN, REL, CTL, act, SCRIPT)):
        with pytest.raises(ValueError):
            call()


def test_other_inputs_that_would_blur_the_scope_are_refused():
    with pytest.raises(ValueError):
        O.rbac(RUN, REL, CTL, CTL, ACT)                             # one namespace for both Roles
    with pytest.raises(ValueError):
        O.names("Not A Name", ACT)
    with pytest.raises(ValueError):
        O.configmap(RUN, REL, CTL, ACT, "   ")
    assert max(len(v) for v in O.names(REL, "a" * 32).values() if v.startswith("c28-")) <= 63
