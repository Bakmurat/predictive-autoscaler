"""The guarded quota injector and the command line."""

import json
import os
import subprocess

import pytest

from c28 import inject
from c28.__main__ import main

PARAMS = os.path.join(os.path.dirname(__file__), "..", "..", "params", "kind-smoke.json")
LOCK = {"kubeconfig": {"path": "/tmp/kc", "context": "kind-pa-c28-smoke",
                       "cluster_uid": "0b6c1a52-4f0e-4c1e-9a77-3d1f0a2b9c10"}}


def params():
    with open(PARAMS) as fh:
        return json.load(fh)


def test_a_dry_run_only_prints_the_scoped_patch():
    (cmd,) = inject.apply(LOCK, params(), "restore")
    assert cmd[:5] == ["kubectl", "--kubeconfig", "/tmp/kc", "--context", "kind-pa-c28-smoke"]
    assert cmd[-9:] == ["-n", "demo", "patch", "resourcequota", "c28-fixture-quota", "--type", "merge", "-p",
                        '{"spec": {"hard": {"pods": "64"}}}']


def test_apply_checks_the_cluster_identity_first():
    calls = []

    def run(cmd, **kw):
        calls.append(cmd)
        out = "0b6c1a52-4f0e-4c1e-9a77-3d1f0a2b9c10" if "kube-system" in cmd else "pod/a\npod/b\npod/c\n"
        return subprocess.CompletedProcess(cmd, 0, stdout=out)
    cmds = inject.apply(LOCK, params(), "deny_new_pods", dry_run=False, run=run)
    assert "kube-system" in calls[0] and '"pods": "3"' in cmds[0][-1] and calls[-1] == cmds[0]

    def wrong(cmd, **kw):
        return subprocess.CompletedProcess(cmd, 0, stdout="another-cluster")
    with pytest.raises(inject.InjectError, match="refusing"):
        inject.apply(LOCK, params(), "restore", dry_run=False, run=wrong)


def test_cli_render_and_plan(tmp_path, capsys):
    out = tmp_path / "all.yaml"
    assert main(["render", "--params", PARAMS, "--out", str(out)]) == 0
    import yaml
    docs = list(yaml.safe_load_all(out.read_text()))
    assert sum(1 for d in docs if d["kind"] == "Deployment") == 13
    assert main(["plan", "--kind", "scored", "--seed", "201", "--capacity", "7", "--target", "c28-pa-n-s20",
                 "--accelerate", "24", "--out", str(tmp_path / "plan.json")]) == 0
    assert json.loads((tmp_path / "plan.json").read_text())["label"] == "accelerated-smoke"
    assert main(["order"]) == 0 and len(json.loads(capsys.readouterr().out)) == 20
