"""The quota fault injector (EVAL-C28-MANIFEST sections 5 and 11.3): the one live mutation of a scored block.

At 16:10 it lowers the run's PriorityClass-scoped ResourceQuota to the number of fixture pods that currently exist, so
new target pods are denied while running ones are untouched; at 16:40 it restores the normal limit. Nothing else is
changed. Every call carries the run-lock's kubeconfig path and context and is preceded by a cluster-identity check (the
kube-system namespace UID must equal the run-lock's cluster UID). The default is a dry run that prints the commands.
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


def identity_check(lock: Dict, run: Callable = subprocess.run) -> None:
    out = run(kubectl(lock, "get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"),
              capture_output=True, text=True, check=True).stdout.strip()
    if out != lock["kubeconfig"]["cluster_uid"]:
        raise InjectError("the kubeconfig does not reach the run-lock's cluster; refusing")


def quota_commands(lock: Dict, params: Dict, action: str, current_pods: int = 0) -> List[List[str]]:
    ns, name = params["namespace"], params["quota"]["name"]
    if action == "deny_new_pods":
        hard = current_pods
    elif action == "restore":
        hard = params["quota"]["pods_normal"]
    else:
        raise InjectError(f"unknown action {action!r}")
    patch = json.dumps({"spec": {"hard": {"pods": str(hard)}}}, sort_keys=True)
    return [kubectl(lock, "-n", ns, "patch", "resourcequota", name, "--type", "merge", "-p", patch)]


def count_fixture_pods(lock: Dict, params: Dict, run: Callable = subprocess.run) -> int:
    out = run(kubectl(lock, "-n", params["namespace"], "get", "pods", "-l", f"{RUN_LABEL}={params['run_id']}",
                      "--field-selector=status.phase!=Succeeded,status.phase!=Failed", "-o", "name"),
              capture_output=True, text=True, check=True).stdout
    return len([line for line in out.splitlines() if line.strip()])


def apply(lock: Dict, params: Dict, action: str, dry_run: bool = True, run: Callable = subprocess.run) -> List[List[str]]:
    if dry_run:
        return quota_commands(lock, params, action, current_pods=0)
    identity_check(lock, run)
    current = count_fixture_pods(lock, params, run) if action == "deny_new_pods" else 0
    cmds = quota_commands(lock, params, action, current)
    for c in cmds:
        run(c, check=True)
    return cmds
