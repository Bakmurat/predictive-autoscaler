"""The bound quota injector and the command line."""

import json
import os
import subprocess

import pytest

from c28 import inject
from c28.__main__ import main

PARAMS = os.path.join(os.path.dirname(__file__), "..", "..", "params", "kind-smoke.json")
UID = "0b6c1a52-4f0e-4c1e-9a77-3d1f0a2b9c10"
QUID = "11111111-2222-3333-4444-555555555555"
LOCK = {"kubeconfig": {"path": "/tmp/kc", "context": "kind-pa-c28-smoke", "cluster_uid": UID},
        "fault_scope": {"namespace": "demo", "quota_name": "c28-fixture-quota", "quota_uid": QUID,
                        "run_label": "c28.devkuban.com/run=c28-smoke", "priority_class": "c28-fixture"}}
QUOTA = {"metadata": {"name": "c28-fixture-quota", "namespace": "demo", "uid": QUID,
                      "labels": {"c28.devkuban.com/run": "c28-smoke"}},
         "spec": {"hard": {"pods": "64"}, "scopeSelector": {"matchExpressions": [
             {"scopeName": "PriorityClass", "operator": "In", "values": ["c28-fixture"]}]}}}
PODS = {"items": [
    {"spec": {"priorityClassName": "c28-fixture"}, "status": {"phase": "Running"}},
    {"spec": {"priorityClassName": "c28-fixture"}, "status": {"phase": "Pending"}},
    {"spec": {"priorityClassName": "c28-fixture"}, "status": {"phase": "Succeeded"}},
    {"spec": {}, "status": {"phase": "Running"}},                                   # c28-ingress: no PriorityClass
]}


def params():
    with open(PARAMS) as fh:
        return json.load(fh)


def stub(quota=QUOTA, cluster=UID, calls=None):
    def run(cmd, **kw):
        if calls is not None:
            calls.append(cmd)
        if "kube-system" in cmd:
            out = cluster
        elif "resourcequota" in cmd and "get" in cmd:
            out = json.dumps(quota)
        elif "pods" in cmd:
            out = json.dumps(PODS)
        else:
            out = ""
        return subprocess.CompletedProcess(cmd, 0, stdout=out)
    return run


def test_a_dry_run_only_prints_the_uid_bound_patch():
    (cmd,) = inject.apply(LOCK, params(), "restore")
    assert cmd[:5] == ["kubectl", "--kubeconfig", "/tmp/kc", "--context", "kind-pa-c28-smoke"]
    assert cmd[-9:-1] == ["-n", "demo", "patch", "resourcequota", "c28-fixture-quota", "--type", "merge", "-p"]
    assert json.loads(cmd[-1]) == {"metadata": {"uid": QUID}, "spec": {"hard": {"pods": "64"}}}


def test_apply_binds_cluster_quota_and_counts_only_fixture_pods():
    calls = []
    cmds = inject.apply(LOCK, params(), "deny_new_pods", dry_run=False, run=stub(calls=calls))
    assert "kube-system" in calls[0] and "resourcequota" in calls[1] and calls[-1] == cmds[0]
    assert json.loads(cmds[0][-1])["spec"]["hard"]["pods"] == "2"       # not the ingress, not the finished pod


@pytest.mark.parametrize("quota, cluster, message", [
    (QUOTA, "another-cluster", "run-lock's cluster"),
    ({**QUOTA, "metadata": {**QUOTA["metadata"], "uid": "other"}}, UID, "UID differs"),
    ({**QUOTA, "metadata": {**QUOTA["metadata"], "labels": {}}}, UID, "run label"),
    ({**QUOTA, "spec": {"hard": {"pods": "64"}}}, UID, "PriorityClass"),
    ({**QUOTA, "spec": {**QUOTA["spec"], "scopes": ["NotTerminating"]}}, UID, "PriorityClass"),
])
def test_apply_refuses_an_unbound_target(quota, cluster, message):
    calls = []
    with pytest.raises(inject.InjectError, match=message):
        inject.apply(LOCK, params(), "deny_new_pods", dry_run=False, run=stub(quota, cluster, calls))
    assert not any("patch" in c for c in calls)


def test_parameters_must_match_the_locked_scope():
    p = params()
    p["priority_class"] = "other"
    with pytest.raises(inject.InjectError, match="fault scope"):
        inject.apply(LOCK, p, "restore")


def test_cli_render_release_values_and_plan(tmp_path, capsys):
    import yaml
    out = tmp_path / "all.yaml"
    assert main(["render", "--params", PARAMS, "--out", str(out)]) == 0
    docs = list(yaml.safe_load_all(out.read_text()))
    assert sum(1 for d in docs if d["kind"] == "Deployment") == 13
    rv = tmp_path / "rv.yaml"
    assert main(["release-values", "--params", PARAMS, "--out", str(rv)]) == 0
    v = yaml.safe_load(rv.read_text())
    assert v["operator"]["forecasterAddress"] == "c28-forecast-route.pa-eval-system.svc:8443"
    assert v["forecaster"]["tls"]["extraDNSNames"] == ["c28-forecast-route.pa-eval-system.svc"]
    assert main(["plan", "--kind", "scored", "--seed", "201", "--capacity", "7", "--target", "c28-pa-n-s20",
                 "--accelerate", "24", "--out", str(tmp_path / "plan.json")]) == 0
    assert json.loads((tmp_path / "plan.json").read_text())["label"] == "accelerated-smoke"
    assert main(["order"]) == 0 and len(json.loads(capsys.readouterr().out)) == 20
