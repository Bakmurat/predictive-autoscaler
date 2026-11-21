"""KubectlApi.json_patch against a REAL kube-apiserver: the preconditions imageroll.py relies on are enforced by the
server in the same request, and kubectl's refusal is classified as definitive.

Needs the envtest binaries (kube-apiserver, etcd, kubectl in one directory: `make envtest` downloads them to
~/.local/share/kubebuilder-envtest/k8s/<version>). Set C28_ENVTEST_ASSETS to that directory (`make envtest-python`
does); without it the test is skipped, unless C28_ENVTEST_REQUIRED=1 (CI), where it must run. The API server and etcd are throwaway processes on 127.0.0.1 with their own data directory and a random
token; nothing here can reach a cluster."""

import json
import os
import secrets
import signal
import socket
import subprocess
import time

import pytest

import common_records as C
import execlib as X

ASSETS = os.environ.get("C28_ENVTEST_ASSETS", "")
REQUIRED = os.environ.get("C28_ENVTEST_REQUIRED") == "1"
pytestmark = pytest.mark.skipif(not ASSETS and not REQUIRED,
                                reason="C28_ENVTEST_ASSETS is not set (the envtest binaries)")
OLD = "registry.example/c28-fixture@sha256:" + "1" * 64
NEW = "registry.example/c28-fixture@sha256:" + "2" * 64
PATH = "/apis/apps/v1/namespaces/default/deployments/t"


def _port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def api(tmp_path_factory):
    assert all(os.path.isfile(os.path.join(ASSETS, b)) for b in ("etcd", "kube-apiserver", "kubectl")), \
        "C28_ENVTEST_REQUIRED=1 but C28_ENVTEST_ASSETS does not hold etcd, kube-apiserver and kubectl"
    d = tmp_path_factory.mktemp("apiserver")
    client, peer, secure, token = _port(), _port(), _port(), secrets.token_hex(16)
    subprocess.run(["openssl", "genrsa", "-out", str(d / "sa.key"), "2048"], check=True, capture_output=True)
    (d / "tokens.csv").write_text(f"{token},admin,admin,system:masters\n")
    procs = [subprocess.Popen([os.path.join(ASSETS, "etcd"), "--data-dir", str(d / "etcd"), "--unsafe-no-fsync",
                               "--listen-client-urls", f"http://127.0.0.1:{client}",
                               "--advertise-client-urls", f"http://127.0.0.1:{client}",
                               "--listen-peer-urls", f"http://127.0.0.1:{peer}"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)]
    procs.append(subprocess.Popen([
        os.path.join(ASSETS, "kube-apiserver"), f"--etcd-servers=http://127.0.0.1:{client}",
        "--bind-address=127.0.0.1", "--advertise-address=127.0.0.1", f"--secure-port={secure}",
        f"--cert-dir={d / 'certs'}", "--service-account-issuer=https://local",
        f"--service-account-key-file={d / 'sa.key'}", f"--service-account-signing-key-file={d / 'sa.key'}",
        "--service-cluster-ip-range=10.0.0.0/24", f"--token-auth-file={d / 'tokens.csv'}",
        "--authorization-mode=RBAC", "--disable-admission-plugins=ServiceAccount"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    (d / "kubeconfig").write_text(json.dumps({
        "apiVersion": "v1", "kind": "Config", "current-context": "local",
        "clusters": [{"name": "local", "cluster": {"server": f"https://127.0.0.1:{secure}",
                                                   "insecure-skip-tls-verify": True}}],
        "users": [{"name": "admin", "user": {"token": token}}],
        "contexts": [{"name": "local", "context": {"cluster": "local", "user": "admin"}}]}))

    def run(args, **kw):                              # the envtest kubectl, never one from PATH
        return subprocess.run([os.path.join(ASSETS, "kubectl")] + list(args[1:]), **kw)
    client_api = C.KubectlApi({"path": str(d / "kubeconfig"), "context": "local"}, run=run)
    try:
        deadline = time.time() + 90
        while True:
            try:
                if client_api.get("/api/v1/namespaces/default"):
                    break
            except C.ApiError:
                pass
            assert time.time() < deadline, "the throwaway API server did not come up"
            time.sleep(1)
        yield client_api
    finally:
        for p in reversed(procs):                     # the API server does not stop on SIGTERM once etcd is gone
            p.send_signal(signal.SIGKILL)
            p.wait()


def _ops(d, rv=None, uid=None, name="fixture", old=OLD, mark="ir-1-abc/4/m"):
    return [{"op": "test", "path": "/metadata/uid", "value": uid or d["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": rv or d["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/spec/template/spec/containers/0/name", "value": name},
            {"op": "test", "path": "/spec/template/spec/containers/0/image", "value": old},
            {"op": "add", "path": "/metadata/annotations/c28.devkuban.com~1image-roll", "value": mark},
            {"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": NEW}]


def test_the_api_server_enforces_the_patchs_preconditions(api):
    d = api.create("/apis/apps/v1/namespaces/default/deployments", {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "t", "namespace": "default", "annotations": {X.INTENT_ANNOTATION: "r/prepare/1"}},
        "spec": {"replicas": 1, "selector": {"matchLabels": {"a": "t"}},
                 "template": {"metadata": {"labels": {"a": "t"}},
                              "spec": {"containers": [{"name": "fixture", "image": OLD}]}}}})
    for wrong in (dict(uid="00000000-0000-4000-8000-000000000000"), dict(rv="1"), dict(name="other"), dict(old=NEW)):
        with pytest.raises(C.ApiError) as e:
            api.json_patch(PATH, _ops(d, **wrong))
        assert e.value.status == 422 and e.value.status in X.DEFINITIVE       # refused whole, and definitively
    assert api.get(PATH)["metadata"]["resourceVersion"] == d["metadata"]["resourceVersion"]
    with pytest.raises(C.NotFound):
        api.json_patch(PATH[:-1] + "nope", _ops(d))
    # the fencing write: it tests the original UID and resourceVersion and moves the resourceVersion...
    void = api.json_patch(PATH, _ops(d)[:2] + [{"op": "add", "value": "ir-1-abc/4/m",
                                               "path": "/metadata/annotations/c28.devkuban.com~1image-roll-void"}])
    assert void["metadata"]["uid"] == d["metadata"]["uid"]
    assert void["metadata"]["resourceVersion"] != d["metadata"]["resourceVersion"]
    assert void["metadata"]["generation"] == d["metadata"]["generation"] + 1  # an annotation raises the generation
    assert void["spec"] == d["spec"]                                           # ...and nothing else
    with pytest.raises(C.ApiError) as e:                                       # so the lost patch can never commit
        api.json_patch(PATH, _ops(d))
    assert e.value.status == 422
    out = api.json_patch(PATH, _ops(void))
    expected = json.loads(json.dumps(void["spec"]))
    expected["template"]["spec"]["containers"][0]["image"] = NEW
    assert out["spec"] == expected and out["metadata"]["uid"] == d["metadata"]["uid"]
    assert out["metadata"]["annotations"]["c28.devkuban.com/image-roll"] == "ir-1-abc/4/m"
    assert out["metadata"]["annotations"][X.INTENT_ANNOTATION] == "r/prepare/1"
    with pytest.raises(C.ApiError):                                            # the same request twice: refused
        api.json_patch(PATH, _ops(void))
