"""common_records: the preparation record, the explicit kubectl client, reviewed plans, write-once evidence, the
hash-chained receipt log and the sanitizer."""

import json

import pytest

import common_records as C
from fakes import CLUSTER, record


def test_a_record_is_explicit_resolved_and_credential_free(tmp_path):
    rec = record(tmp_path)
    assert C.record_problems(rec) == []
    for change, why in ((dict(kubeconfig={"path": "/home/ops/.kube/config", "context": "prodcluster"}), "default"),
                        (dict(kubeconfig={"path": "x", "context": "c", "token": "t"}), "exactly"),
                        (dict(run_id="<run id>"), "run_id"),
                        (dict(cluster_uid="kube-system"), "cluster_uid"),
                        (dict(source={"commit": "abc"}), "commit"),
                        (dict(legacy={"note": "TODO"}), "unresolved"),
                        (dict(legacy={"password": "x"}), "credential"),
                        (dict(archive={"url": "https://u:p@host/x"}), "credential-bearing")):
        bad = dict(rec, **change)
        assert any(why in p for p in C.record_problems(bad)), change
    assert any("legacy section" in p for p in C.record_problems(rec, ("legacy",)))
    f = tmp_path / "r.json"
    f.write_text(json.dumps(dict(rec, run_id="<x>")))
    with pytest.raises(C.StagingError):
        C.load_record(str(f))


class Run:
    def __init__(self, rc=0, out="{}", err=""):
        self.calls, self.rc, self.out, self.err = [], rc, out, err

    def __call__(self, args, input=None, capture_output=True, text=True):
        self.calls.append((args, input))
        return type("P", (), {"returncode": self.rc, "stdout": self.out, "stderr": self.err})()


def test_the_kubectl_client_is_explicit_and_fails_closed():
    run = Run(out='{"metadata": {"uid": "u"}}')
    api = C.KubectlApi({"path": "/k/eval.yaml", "context": "prodcluster"}, run=run)
    assert api.get("/api/v1/namespaces/kube-system") == {"metadata": {"uid": "u"}}
    args, _ = run.calls[0]
    assert args[:6] == ["kubectl", "--kubeconfig", "/k/eval.yaml", "--context", "prodcluster",
                        "--request-timeout=30s"] and args[6:] == ["get", "--raw", "/api/v1/namespaces/kube-system"]
    api.delete("/apis/keda.sh/v1alpha1/namespaces/demo/scaledobjects/x", "uid-1")
    args, body = run.calls[1]
    assert args[6:] == ["delete", "--raw", "/apis/keda.sh/v1alpha1/namespaces/demo/scaledobjects/x", "-f", "-"]
    assert json.loads(body)["preconditions"] == {"uid": "uid-1"}
    missing = C.KubectlApi({"path": "k", "context": "c"}, run=Run(rc=1, err="Error from server (NotFound)"))
    assert missing.get("/x") is None
    with pytest.raises(C.ApiError):
        C.KubectlApi({"path": "k", "context": "c"}, run=Run(rc=1, err="Forbidden")).get("/x")
    with pytest.raises(C.NotFound):
        C.KubectlApi({"path": "k", "context": "c"}, run=Run(rc=1, err="not found")).delete("/x", "u")


def test_a_mutation_needs_the_reviewed_plan(tmp_path):
    rec = record(tmp_path)
    plan = C.make_plan("legacy", rec, [{"step": 1, "op": "scale"}])
    d = C.plan_digest(plan)
    C.require_reviewed(plan, d, "legacy", rec)
    for digest, helper, r in ((None, "legacy", rec), ("0" * 64, "legacy", rec), (d, "archive", rec),
                              (d, "legacy", dict(rec, run_id="other"))):
        with pytest.raises(C.StagingError):
            C.require_reviewed(plan, digest, helper, r)
    edited = dict(plan, actions=[{"step": 1, "op": "delete"}])
    with pytest.raises(C.StagingError, match="differs"):
        C.require_reviewed(edited, d, "legacy", rec)


def test_evidence_is_written_once_and_receipts_are_chained(tmp_path):
    rec = record(tmp_path)
    p = tmp_path / "e" / "x.json"
    C.write_once(str(p), b"1")
    with pytest.raises(C.StagingError, match="written once"):
        C.write_once(str(p), b"2")
    a = C.append_receipt(rec, "legacy", "one", {"n": 1}, clock=lambda: 0)
    b = C.append_receipt(rec, "legacy", "two", {"n": 2}, clock=lambda: 60)
    assert b["prev"] == a["sha256"] and [r["event"] for r in C.read_receipts(C.receipts_path(rec, "legacy"))] == \
        ["one", "two"]
    path = C.receipts_path(rec, "legacy")
    lines = open(path).read().splitlines()
    lines[0] = lines[0].replace('"n": 1', '"n": 9')
    open(path, "w").write("\n".join(lines) + "\n")
    with pytest.raises(C.StagingError, match="hash chain"):
        C.read_receipts(path)


def test_the_cluster_identity_is_checked(tmp_path):
    from fakes import legacy_cluster
    api = legacy_cluster()
    C.require_cluster(api, CLUSTER)
    with pytest.raises(C.StagingError, match="kube-system UID"):
        C.require_cluster(api, "00000000-0000-4000-8000-000000000000")


def test_sanitizing_removes_every_secret_carrier():
    from fakes import LAST_APPLIED_MARKER, SECRET_DATA, SECRET_VALUE
    secret = C.sanitize_object({"kind": "Secret", "metadata": {"name": "s", "namespace": "demo"},
                                "data": {"k": SECRET_DATA}, "stringData": {"p": SECRET_VALUE}})
    assert secret["sanitized"] == "secret-values-removed" and SECRET_DATA not in json.dumps(secret)
    cm = C.sanitize_object({"kind": "ConfigMap", "metadata": {"name": "c"}, "data": {"script.js": SECRET_VALUE}})
    assert "data" not in cm and SECRET_VALUE not in json.dumps(cm) and set(cm["data_sha256"]) == {"script.js"}
    cj = C.sanitize_object({"kind": "CronJob", "metadata": {"name": "j", "managedFields": [{}], "annotations": {
        C.LAST_APPLIED: LAST_APPLIED_MARKER, "x-api-token": "abc", "description": "kept"}},
        "spec": {"jobTemplate": {"spec": {"template": {"spec": {"containers": [{"name": "c", "env": [
            {"name": "AWS_SECRET_ACCESS_KEY", "value": SECRET_VALUE}, {"name": "MODE", "value": "server"}],
            "command": ["run", f"--password={SECRET_VALUE}"]}]}}}}}})
    text = json.dumps(cj)
    assert SECRET_VALUE not in text and LAST_APPLIED_MARKER not in text and "managedFields" not in text
    assert cj["metadata"]["annotations"] == {"x-api-token": C.REDACTED, "description": "kept"}
    with pytest.raises(C.StagingError):
        C.assert_clean("-----BEGIN RSA PRIVATE KEY-----")
    with pytest.raises(C.StagingError):
        C.assert_clean("value: x", secrets=["value: x"])


# --- Codex r1 on #31, finding 1 ---------------------------------------------------------------------------------

@pytest.mark.parametrize("env", [{"name": "DB_PASSWORD", "value": "123456"},
                                 {"name": "API_SECRET_FILE", "value": "/run/s3cr3t-9f8e7d"},
                                 {"name": "AUTH_TOKEN", "value": "true-SECRET"},
                                 {"name": "PIN_KEY", "value": "0042"}])
def test_numeric_or_path_shaped_credentials_are_redacted(env):
    o = C.sanitize_object({"kind": "Pod", "metadata": {"name": "p"}, "spec": {"containers": [{"name": "c",
                                                                                         "env": [env]}]}})
    assert o["spec"]["containers"][0]["env"][0]["value"] == C.REDACTED


def test_split_credential_arguments_are_redacted():
    o = C.sanitize_object({"kind": "Pod", "metadata": {"name": "p"}, "spec": {"containers": [{"name": "c", "args": [
        "--password", "hunter2", "--api-key", "123", "-p", "pin", "--enable-auth", "--port=8080", "--mode", "x"]}]}})
    assert o["spec"]["containers"][0]["args"] == ["--password", C.REDACTED, "--api-key", C.REDACTED, "-p", C.REDACTED,
                                                  "--enable-auth", C.REDACTED, "--mode", "x"]   # unconditional
    dash = C.sanitize_object({"kind": "Pod", "metadata": {"name": "p"}, "spec": {"containers": [{"name": "c", "args": [
        "--password", "-starts-with-dash-SECRET", "--token", "--also-a-secret"]}]}})
    assert "SECRET" not in json.dumps(dash) and "also-a-secret" not in json.dumps(dash)


def test_kubectl_stderr_never_reaches_an_error_message():
    secret = "Bearer eyJhbGciOiJSUzI1NiJ9.SECRETPAYLOAD.sig hunter2"
    for err, status in ((f"Error from server (Forbidden): {secret}", 403),
                        (f"error: the object has been modified; body: {secret}", 409),
                        (f"weird failure {secret}", None)):
        api = C.KubectlApi({"path": "k", "context": "c"}, run=Run(rc=1, err=err))
        with pytest.raises(C.ApiError) as e:
            api.replace("/apis/apps/v1/namespaces/demo/deployments/x", {"spec": {}})
        assert "hunter2" not in str(e.value) and "SECRETPAYLOAD" not in str(e.value) and e.value.status == status


def test_the_exclusive_lock_survives_an_in_flight_mutation(tmp_path):
    rec = record(tmp_path)
    assert C.lock_path(rec, "legacy").endswith(f"{CLUSTER}-legacy.lock")
    assert C.lock_path(dict(rec, lock_dir="/srv/c28-locks"), "legacy") == f"/srv/c28-locks/{CLUSTER}-legacy.lock"
    with C.exclusive_lock(rec, "legacy"):
        with pytest.raises(C.StagingError, match="another execution"):
            with C.exclusive_lock(rec, "legacy"):
                pass
    with pytest.raises(C.ApiError):
        with C.exclusive_lock(rec, "legacy") as lock:
            lock.safe = False                    # an intent is recorded, the call's outcome is unknown
            raise C.ApiError("lost response")
    with pytest.raises(C.StagingError, match="crashed"):
        with C.exclusive_lock(rec, "legacy"):
            pass



# --- C2.8f: the helper's own exclude_secrets switch ---------------------------------------------------------------

def test_exclude_secrets_is_accepted_only_at_its_exact_path_as_a_boolean(tmp_path):
    ok = record(tmp_path, archive={"velero": {"exclude_secrets": True}})
    assert C.record_problems(ok) == []
    assert C.record_problems(record(tmp_path, archive={"velero": {"exclude_secrets": False}})) == []
    for bad in ({"archive": {"velero": {"exclude_secrets": "true"}}},          # not a boolean
                {"archive": {"velero": {"exclude_secrets": 1}}},
                {"archive": {"velero": {"exclude_secrets": {"value": True}}}},
                {"archive": {"exclude_secrets": True}},                        # another path
                {"legacy": {"velero": {"exclude_secrets": True}}},
                {"archive": {"velero": {"secret_name": "x"}}},                 # other secret-named keys
                {"archive": {"velero": {"exclude_secret": True}}},
                {"archive": {"velero": {"token": True}}}):
        assert any("credential material" in p for p in C.record_problems(record(tmp_path, **bad))), bad
