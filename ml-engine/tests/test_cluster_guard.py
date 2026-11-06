"""The test-wide cluster guard in conftest.py (Codex r23): no test, fixture or collected module reaches a cluster through a
kubectl started from Python, and a refused call fails the run even when the calling code swallows the error."""

import os
import shutil
import subprocess
import sys

import pytest

# The conftest module that patched Popen (whatever name pytest imported it under).
guard = sys.modules[subprocess.Popen.__init__.__module__]
CONFTEST = os.path.join(os.path.dirname(__file__), "conftest.py")


@pytest.mark.parametrize("args, kw", [
    (["kubectl", "get", "pods"], {}),
    (["/repo/tools/bin/kubectl", "get", "pods"], {}),                     # the pinned binary is not a stub
    (["/opt/homebrew/bin/kubectl", "-n", "ml-engine", "create", "job", "x"], {}),
    (["anything", "get", "pods"], {"executable": "/usr/local/bin/kubectl"}),
    (["kubectl", "version"], {}),                                          # plain `version` asks the server
    (["kubectl", "version", "--client=false"], {}),
    ("kubectl get pods", {"shell": True}),
    ("echo ok && kubectl delete ns demo", {"shell": True}),
    (["bash", "-c", "echo ok; /repo/tools/bin/kubectl get nodes"], {}),
    (["sh", "-c", "kubectl version --client && kubectl get ns"], {}),
    (b"kubectl get pods", {"shell": True}),
    # Codex r24: contradictory client flags and shell syntax that shlex does not parse
    (["kubectl", "version", "--client=true", "--client=false"], {}),
    (["kubectl", "version", "--client", "--client"], {}),
    (["kubectl", "version", "--client", "--server=https://x"], {}),
    ("pod=$(kubectl --kubeconfig /fixture/config get pods -o name)", {"shell": True}),
    ("true;kubectl --kubeconfig /fixture/config get pods", {"shell": True}),
    (["bash", "-c", "true;kubectl get pods"], {}),
    (["sh", "-c", "kubectl version --client && echo ok"], {}),
    (["sh", "-c", "echo `kubectl get ns`"], {}),
    (["sh", "-c", "kubectl\nget pods"], {}),
    (["sh", "-c", "kubectl kustomize . | kubectl apply -f -"], {}),
    ("echo kubectl-free", {"shell": True}),                # conservative: any mention of kubectl in a shell line
    (["sh", "-c", "kubectl kustomize $(kubectl --kubeconfig /x get cm -o name)"], {}),
    (["sh", "-c", "kubectl kustomize dir;curl x"], {}),
    (["bash", "-lc", "kubectl get pods"], {}),             # option clusters
    (["bash", "-ec", "kubectl get pods"], {}),
    (["env", "KUBECONFIG=/x", "kubectl", "get", "pods"], {}),   # wrappers
    (["timeout", "5", "/repo/tools/bin/kubectl", "get", "pods"], {}),
    (["python3", "-c", "import subprocess; subprocess.run(['kubectl', 'get', 'pods'])"], {}),
    (["kubectl get pods", "x"], {"shell": True}),          # shell=True with a sequence: the first item is the command
])
def test_cluster_calls_are_recognised(args, kw):
    assert guard.cluster_call(args, kw.get("executable"), kw.get("shell", False))


@pytest.mark.parametrize("args, kw", [
    (["kubectl", "kustomize", "k8s-manifests/base"], {}),
    (["kubectl", "version", "--client"], {}),
    (["kubectl", "version", "--client=true", "-o", "json"], {}),
    (["sh", "-c", "kubectl version --client"], {}),
    (["kubectl", "version", "--client", "-o", "json"], {}),
    (["sh", "-c", "kubectl kustomize k8s-manifests/base"], {}),
    (["bash", "script.sh"], {}),            # a script's own kubectl is outside Python (see the conftest's limits)
    (["git", "status"], {}),
    ("echo hello", {"shell": True}),
    (["sh", "-c", "kubectl version --client=true"], {}),
    (["bash", "-lc", "kubectl version --client"], {}),
    (["env", "kubectl", "kustomize", "k8s-manifests/base"], {}),
])
def test_offline_and_unrelated_commands_run(args, kw):
    assert guard.cluster_call(args, kw.get("executable"), kw.get("shell", False)) is None


def test_only_an_exact_registered_stub_runs(tmp_path):
    stub = tmp_path / "bin" / "kubectl"
    stub.parent.mkdir()
    stub.write_text("#!/bin/sh\nexit 0\n")
    guard.allow_stub(stub)
    try:
        assert guard.cluster_call([str(stub), "get", "pods"]) is None
        assert guard.cluster_call([str(tmp_path / "other" / "kubectl"), "get", "pods"])
    finally:
        guard._STUBS.discard(os.path.realpath(str(stub)))
    assert guard.cluster_call([str(stub), "get", "pods"])


def test_recorded_commands_carry_no_credentials():
    rec = guard.cluster_call(["kubectl", "--token=s3cr3t", "--server", "https://10.0.0.1:6443", "--kubeconfig",
                              "/home/u/.kube/prod", "-n", "demo", "get", "pods"])
    assert "s3cr3t" not in rec and "10.0.0.1" not in rec and "/home/u/.kube/prod" not in rec
    assert rec.startswith("kubectl --token=<redacted> --server <redacted>") and rec.endswith("get pods")


def test_a_kubectl_call_from_a_test_is_refused_and_reported():
    before = len(guard.CLUSTER_CALLS)
    with pytest.raises(RuntimeError, match="must not call kubectl"):
        subprocess.run(["kubectl", "-n", "ml-engine", "create", "job", "x"], capture_output=True)
    assert "create job x" in guard.CLUSTER_CALLS[before]
    del guard.CLUSTER_CALLS[before:]   # provoked on purpose; consumed so this test passes
    assert os.environ["KUBECONFIG"] == "/dev/null" and "KUBERNETES_SERVICE_HOST" not in os.environ
    assert "COLD_START_CRONJOB" not in os.environ


# --- the run fails when the calling code swallows the refusal --------------------------------------------------------

def _pytest_in(tmp_path, source):
    """Run pytest on one test module with this conftest, in a separate process."""
    pkg = tmp_path / "suite"
    pkg.mkdir()
    shutil.copy(CONFTEST, pkg / "conftest.py")
    (pkg / "test_probe.py").write_text(source)
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST")}
    return subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(pkg)], cwd=tmp_path,
                          env=env, capture_output=True, text=True, timeout=120)


SWALLOW = '''
import subprocess

def swallow(argv, **kw):
    try:
        subprocess.run(argv, capture_output=True, **kw)
    except Exception:
        pass
'''


def test_a_swallowed_call_inside_a_test_fails_it(tmp_path):
    p = _pytest_in(tmp_path, SWALLOW + '''
def test_swallows():
    swallow(["kubectl", "get", "pods"])

def test_unrelated():
    pass
''')
    assert p.returncode == 1, p.stdout + p.stderr
    assert "1 failed, 1 passed" in p.stdout and "tried to reach a cluster" in p.stdout


def test_a_swallowed_call_at_collection_fails_the_run(tmp_path):
    p = _pytest_in(tmp_path, SWALLOW + '''
swallow(["kubectl", "get", "nodes"])           # at import time, outside any test

def test_passes():
    pass
''')
    assert p.returncode == 1, p.stdout + p.stderr
    assert "1 passed" in p.stdout and "outside any test" in p.stderr and "get nodes" in p.stderr


def test_a_swallowed_pinned_or_executable_call_fails(tmp_path):
    p = _pytest_in(tmp_path, SWALLOW + '''
def test_pinned():
    swallow(["/repo/tools/bin/kubectl", "--kubeconfig", "/x", "get", "pods"])

def test_executable():
    swallow(["ls"], executable="/usr/local/bin/kubectl")

def test_shell():
    swallow("kubectl get pods", shell=True)
''')
    assert p.returncode == 1, p.stdout + p.stderr
    assert "3 failed" in p.stdout


def test_path_and_bytes_arguments_are_recognised(tmp_path):
    from pathlib import Path
    assert guard.cluster_call(Path("/usr/local/bin/kubectl"))
    assert guard.cluster_call([b"kubectl", b"get", b"pods"])
    assert guard.cluster_call([Path("/repo/tools/bin/kubectl"), "get", "pods"])
    assert guard.cluster_call(["ls"], executable=Path("/usr/local/bin/kubectl"))
