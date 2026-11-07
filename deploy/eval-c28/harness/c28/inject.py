"""The quota fault injector (EVAL-C28-MANIFEST sections 5 and 11.3): the one live mutation of a scored block.

At 16:10 it lowers the run's PriorityClass-scoped ResourceQuota to the number of fixture pods it currently counts, so new
target pods are denied while running ones are untouched; at 16:40 it restores the normal limit. Nothing else changes.

It binds its target before any mutation, from the locked run-lock's fault_scope:
  * the cluster: the kube-system namespace UID equals the run-lock's cluster UID;
  * the quota: the object in the locked namespace has the locked name and UID, the run label, and exactly the
    PriorityClass scope of the run; the patch itself carries the UID, so a replaced object is refused by the server;
  * the count: only pods of the run that use the run's PriorityClass and are not terminal (the ingress has no
    PriorityClass and is not counted).
The parameters must agree with the run-lock (namespace, run, quota name, PriorityClass). The default is a dry run.
"""

from __future__ import annotations

import json
import subprocess
from typing import Callable, Dict, List

RUN_LABEL = "c28.devkuban.com/run"


class InjectError(RuntimeError):
    pass


def kubectl(lock: Dict, *args: str) -> List[str]:
    kc = lock["kubeconfig"]
    return ["kubectl", "--kubeconfig", kc["path"], "--context", kc["context"], "--request-timeout=15s", *args]


def _bind_params(lock: Dict, params: Dict) -> Dict:
    fs = lock["fault_scope"]
    expected = {"namespace": params["namespace"], "quota_name": params["quota"]["name"],
                "run_label": f"{RUN_LABEL}={params['run_id']}", "priority_class": params["priority_class"],
                "pods_normal": params["quota"]["pods_normal"]}
    diff = {k: (fs.get(k), v) for k, v in expected.items() if fs.get(k) != v}
    if diff:
        raise InjectError(f"the parameters differ from the run-lock's fault scope: {diff}")
    return fs


def identity_check(lock: Dict, run: Callable = subprocess.run) -> None:
    out = run(kubectl(lock, "get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"),
              capture_output=True, text=True, check=True).stdout.strip()
    if out != lock["kubeconfig"]["cluster_uid"]:
        raise InjectError("the kubeconfig does not reach the run-lock's cluster; refusing")


def quota_check(lock: Dict, run: Callable = subprocess.run) -> None:
    fs = lock["fault_scope"]
    raw = run(kubectl(lock, "-n", fs["namespace"], "get", "resourcequota", fs["quota_name"], "-o", "json"),
              capture_output=True, text=True, check=True).stdout
    q = json.loads(raw)
    meta, spec = q.get("metadata", {}), q.get("spec", {})
    key, _, value = fs["run_label"].partition("=")
    want_scope = {"matchExpressions": [{"scopeName": "PriorityClass", "operator": "In", "values": [fs["priority_class"]]}]}
    problems = []
    if meta.get("uid") != fs["quota_uid"]:
        problems.append("the quota UID differs from the run-lock")
    if meta.get("namespace") != fs["namespace"] or meta.get("name") != fs["quota_name"]:
        problems.append("the quota is not the locked object")
    if meta.get("labels", {}).get(key) != value:
        problems.append("the quota lacks the run label")
    if spec.get("scopeSelector") != want_scope or spec.get("scopes"):
        problems.append("the quota is not scoped to exactly the run's PriorityClass")
    if problems:
        raise InjectError("; ".join(problems) + "; refusing")


def count_fixture_pods(lock: Dict, run: Callable = subprocess.run) -> int:
    fs = lock["fault_scope"]
    raw = run(kubectl(lock, "-n", fs["namespace"], "get", "pods", "-l", fs["run_label"], "-o", "json"),
              capture_output=True, text=True, check=True).stdout
    pods = json.loads(raw).get("items", [])
    return sum(1 for p in pods if p.get("spec", {}).get("priorityClassName") == fs["priority_class"]
               and p.get("status", {}).get("phase") not in ("Succeeded", "Failed"))


def quota_commands(lock: Dict, params: Dict, action: str, current_pods: int = 0) -> List[List[str]]:
    fs = _bind_params(lock, params)
    if action == "deny_new_pods":
        hard = current_pods
    elif action == "restore":
        hard = fs["pods_normal"]                 # only the approved original limit recorded in the run-lock
    else:
        raise InjectError(f"unknown action {action!r}")
    patch = json.dumps({"metadata": {"uid": fs["quota_uid"]}, "spec": {"hard": {"pods": str(hard)}}}, sort_keys=True)
    return [kubectl(lock, "-n", fs["namespace"], "patch", "resourcequota", fs["quota_name"], "--type", "merge", "-p", patch)]


def apply(lock: Dict, params: Dict, action: str, dry_run: bool = True, run: Callable = subprocess.run) -> List[List[str]]:
    _bind_params(lock, params)
    if dry_run:
        return quota_commands(lock, params, action, current_pods=0)
    identity_check(lock, run)
    quota_check(lock, run)
    current = count_fixture_pods(lock, run) if action == "deny_new_pods" else 0
    cmds = quota_commands(lock, params, action, current)
    for c in cmds:
        run(c, check=True)
    return cmds
