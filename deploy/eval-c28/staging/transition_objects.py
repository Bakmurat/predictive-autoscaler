#!/usr/bin/env python3
"""The objects that run the transition watchdog (TRANSITION-DESIGN draft 6, section 3a). Pure: nothing here talks to a
cluster. The executor creates these through its journal and audits the live ones against the same functions.

One set per activation, every name carrying the activation id, so the watchdog of one activation can never act on, or
be cleared by, another activation's Leases:

  control namespace   ServiceAccount, ConfigMap (the script), Deployment (one replica, Recreate), the heartbeat Lease
                      and the trigger Lease (created "unarmed:<activation>"), a Role + RoleBinding: get on both Leases,
                      update on the trigger Lease only, each by resourceNames
  target namespace    a Role + RoleBinding: get and delete on ONE RoleBinding, <release>-operator-scale, by resourceNames

No ClusterRole, no ClusterRoleBinding, no other rule. The watchdog cannot create or change a binding or a role, write a
Scale or change an autoscaler.
"""
import hashlib
import math
import re

PREFIX = "c28-transition"
SCRIPT_KEY = "transition_watchdog.py"
WATCHDOG_UID = 10001                    # the forecaster image's non-root user
ACTIVATION = re.compile(r"[a-z0-9]([-a-z0-9]{0,30}[a-z0-9])?\Z")
DNS_NAME = re.compile(r"[a-z0-9]([-a-z0-9.]{0,200}[a-z0-9])?\Z")
DIGEST_IMAGE = re.compile(r"[a-z0-9][a-z0-9._/:-]*@sha256:[0-9a-f]{64}\Z")


def _require(ok, message):
    if not ok:
        raise ValueError(message)


def names(release, activation):
    _require(isinstance(activation, str) and ACTIVATION.match(activation), "the activation id must be a short DNS label")
    _require(isinstance(release, str) and DNS_NAME.match(release), "the release's full name must be a DNS name")
    watchdog = f"{PREFIX}-watchdog-{activation}"
    return {"watchdog": watchdog, "heartbeat": f"{PREFIX}-heartbeat-{activation}",
            "trigger": f"{PREFIX}-trigger-{activation}", "scale_binding": f"{release}-operator-scale"}


def labels(run, activation):
    return {"c28.devkuban.com/run": run, "c28.devkuban.com/role": "transition-watchdog",
            "c28.devkuban.com/activation": activation}


def _meta(name, namespace, run, activation, **annotations):
    meta = {"name": name, "namespace": namespace, "labels": labels(run, activation)}
    if annotations:
        meta["annotations"] = {f"c28.devkuban.com/{k.replace('_', '-')}": v for k, v in annotations.items()}
    return meta


def leases(run, release, control_ns, activation):
    """The heartbeat (renewed by the executor) and the trigger Lease in its first state."""
    n = names(release, activation)
    api = {"apiVersion": "coordination.k8s.io/v1", "kind": "Lease"}
    return [dict(api, metadata=_meta(n["heartbeat"], control_ns, run, activation), spec={"holderIdentity": "executor"}),
            dict(api, metadata=_meta(n["trigger"], control_ns, run, activation),
                 spec={"holderIdentity": f"unarmed:{activation}"})]


def rbac(run, release, control_ns, target_ns, activation):
    """The ServiceAccount and its two Roles and RoleBindings: the watchdog's whole authority."""
    n = names(release, activation)
    _require(control_ns != target_ns, "the control namespace and the target namespace must differ")
    subject = [{"kind": "ServiceAccount", "name": n["watchdog"], "namespace": control_ns}]

    def role(namespace, rules):
        return {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
                "metadata": _meta(n["watchdog"], namespace, run, activation), "rules": rules}

    def binding(namespace):
        return {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                "metadata": _meta(n["watchdog"], namespace, run, activation),
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": n["watchdog"]},
                "subjects": subject}

    lease = {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"]}
    return [
        {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": _meta(n["watchdog"], control_ns, run, activation),
         "automountServiceAccountToken": False},        # only the watchdog's own pod asks for the token
        role(target_ns, [{"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["rolebindings"],
                          "resourceNames": [n["scale_binding"]], "verbs": ["get", "delete"]}]),
        binding(target_ns),
        role(control_ns, [dict(lease, resourceNames=[n["heartbeat"]], verbs=["get"]),
                          dict(lease, resourceNames=[n["trigger"]], verbs=["get", "update"])]),
        binding(control_ns),
    ]


def script_sha256(script):
    return hashlib.sha256(script.encode()).hexdigest()


def configmap(run, release, control_ns, activation, script):
    n = names(release, activation)
    _require(isinstance(script, str) and script.strip(), "the watchdog script is empty")
    return {"apiVersion": "v1", "kind": "ConfigMap", "immutable": True,
            "metadata": _meta(n["watchdog"], control_ns, run, activation, script_sha256=script_sha256(script)),
            "data": {SCRIPT_KEY: script}}


def deployment(run, release, control_ns, target_ns, activation, *, image, script, deadline_epoch, trigger_lease_uid,
               excluded_nodes, interval_seconds=60, stale_intervals=3, pass_seconds=5):
    """One replica, Recreate, no expiry: after a trigger the watchdog must keep running until a reviewed recovery
    removes it. The absolute deadline and the trigger Lease's UID are arguments in the pod template, so they survive a
    restart of the pod (a restart is itself a trigger: the watchdog's own rule)."""
    n = names(release, activation)
    _require(isinstance(image, str) and DIGEST_IMAGE.match(image), "the image must be pinned by digest (@sha256:...)")
    _require(isinstance(deadline_epoch, (int, float)) and not isinstance(deadline_epoch, bool)
             and math.isfinite(deadline_epoch) and deadline_epoch > 0, "the deadline must be a finite time")
    _require(isinstance(trigger_lease_uid, str) and trigger_lease_uid.strip() == trigger_lease_uid and trigger_lease_uid,
             "the trigger Lease's UID is required (create the Lease first)")
    for value, what in ((interval_seconds, "interval_seconds"), (pass_seconds, "pass_seconds")):
        _require(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0,
                 f"{what} must be finite and positive")
    _require(isinstance(stale_intervals, int) and not isinstance(stale_intervals, bool) and stale_intervals > 0,
             "stale_intervals must be a positive whole number")
    _require(isinstance(excluded_nodes, (list, tuple)) and excluded_nodes
             and all(isinstance(x, str) and DNS_NAME.match(x) for x in excluded_nodes)
             and len(set(excluded_nodes)) == len(excluded_nodes),
             "name the reserved nodes the watchdog must not run on (distinct node NAMES)")
    selector = labels(run, activation)
    args = ["--activation", activation, "--deadline-epoch", repr(float(deadline_epoch)),
            "--binding-namespace", target_ns, "--binding", n["scale_binding"],
            "--lease-namespace", control_ns, "--trigger-lease", n["trigger"],
            "--trigger-lease-uid", trigger_lease_uid, "--heartbeat-lease", n["heartbeat"],
            "--interval-seconds", repr(float(interval_seconds)), "--stale-intervals", str(stale_intervals),
            "--pass-seconds", repr(float(pass_seconds))]
    pod = {
        "serviceAccountName": n["watchdog"], "automountServiceAccountToken": True, "enableServiceLinks": False,
        "restartPolicy": "Always", "terminationGracePeriodSeconds": 5,
        "securityContext": {"runAsNonRoot": True, "runAsUser": WATCHDOG_UID, "runAsGroup": WATCHDOG_UID,
                            "seccompProfile": {"type": "RuntimeDefault"}},
        # By node NAME (the field metadata.name), not by the kubernetes.io/hostname label: the label can differ from
        # the name, and a reserved node would then pass. A field requirement takes exactly one value, so there is one
        # NotIn requirement per node, all in ONE term (requirements of a term are ANDed).
        "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchFields": [{"key": "metadata.name", "operator": "NotIn", "values": [node]}
                             for node in sorted(excluded_nodes)]}]}}},
        "containers": [{
            "name": "watchdog", "image": image, "imagePullPolicy": "IfNotPresent",
            "command": ["python", f"/watchdog/{SCRIPT_KEY}"], "args": args,
            "env": [{"name": "PYTHONUNBUFFERED", "value": "1"}],
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]}},
            "resources": {"requests": {"cpu": "10m", "memory": "32Mi"}, "limits": {"memory": "96Mi"}},
            "volumeMounts": [{"name": "script", "mountPath": "/watchdog", "readOnly": True}],
        }],
        "volumes": [{"name": "script", "configMap": {"name": n["watchdog"], "defaultMode": 0o444}}],
    }
    return {"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": _meta(n["watchdog"], control_ns, run, activation, script_sha256=script_sha256(script)),
            "spec": {"replicas": 1, "strategy": {"type": "Recreate"}, "revisionHistoryLimit": 0,
                     "selector": {"matchLabels": selector},
                     "template": {"metadata": {"labels": selector, "annotations": {
                         "c28.devkuban.com/script-sha256": script_sha256(script)}}, "spec": pod}}}
