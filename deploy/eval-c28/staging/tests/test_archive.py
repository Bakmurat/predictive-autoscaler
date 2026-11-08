"""archive.py: sanitized snapshots, source revisions from the commit, allowlisted evidence verified off-cluster, the
reviewed Velero Backup and its receipt; any gap refuses the archive-complete receipt."""

import json
import os
import subprocess

import pytest

import archive as A
import common_records as C
from fakes import MARKERS, legacy_cluster, record

T0 = 1_794_000_000


def git_free():
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def scratch_repo(tmp_path):
    repo = tmp_path / "repo"
    for rel in ("deploy/prodcluster/demo/kustomization.yaml", "deploy/eks-benchmark/demo/nginx-test.yaml",
                "k8s-manifests/base/09-operator-deployment.yaml"):
        (repo / rel).parent.mkdir(parents=True, exist_ok=True)
        (repo / rel).write_text(f"# {rel}\n")
    g = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, text=True,  # noqa
                                  env=git_free())
    g("init", "-q")
    assert os.path.realpath(g("rev-parse", "--show-toplevel").stdout.strip()) == os.path.realpath(repo)
    g("add", "-A")
    g("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "x")
    return str(repo), g("rev-parse", "HEAD").stdout.strip()


def setup(tmp_path, **velero):
    repo, commit = scratch_repo(tmp_path)
    export = tmp_path / "export"
    for rel, body in (("p8/run-1/result.json", '{"ok": true}'), ("freeze/capture-v7.tar.gz", "tar"),
                      ("SHA256SUMS", "sums")):
        (export / rel).parent.mkdir(parents=True, exist_ok=True)
        (export / rel).write_text(body)
    rec = record(tmp_path, source={"commit": commit, "repo_dir": repo}, archive={
        "evidence": {"export_dir": str(export), "allowlist": ["p8/*", "freeze/*", "SHA256SUMS"],
                     "destination": str(tmp_path / "offcluster")},
        "velero": {"namespace": "velero", "backup_name": "c28-r1-legacy-20261108", "storage_location": "minio-default",
                   "ttl": "2160h0m0s", "metadata_only": True, "included_namespaces": ["demo", "ml-engine"],
                   "included_cluster_resources": ["customresourcedefinitions"], **velero}})
    return legacy_cluster(), rec


def complete_backup(api, rec, phase="Completed", errors=0, warnings=0):
    o = api.objects[("backups", "velero", rec["archive"]["velero"]["backup_name"])]
    o["status"] = {"phase": phase, "errors": errors, "warnings": warnings}


def run_all(api, rec, clock=lambda: T0):
    plan = A.build_plan(api, rec)
    d = C.plan_digest(plan)
    A.snapshot(api, rec, plan, d, clock)
    A.export(rec, plan, d, clock)
    A.velero_submit(api, rec, plan, d, clock)
    return plan, d


def test_a_complete_archive_earns_its_receipt_and_leaks_nothing(tmp_path):
    api, rec = setup(tmp_path)
    plan, d = run_all(api, rec)
    kinds = {s["kind"] for s in plan["snapshots"]}
    assert {"deployments", "cronjobs", "scaledobjects", "pa-legacy", "persistentvolumeclaims", "persistentvolumes",
            "horizontalpodautoscalers", "leases", "customresourcedefinitions"} <= kinds
    assert set(plan["source"]["blobs"]) == {"deploy/prodcluster/demo/kustomization.yaml",
                                            "deploy/eks-benchmark/demo/nginx-test.yaml",
                                            "k8s-manifests/base/09-operator-deployment.yaml"}
    backup = plan["velero"]
    assert backup["spec"]["snapshotVolumes"] is False and backup["metadata"]["labels"][
        "c28.devkuban.com/metadata-only"] == "true" and backup["spec"]["storageLocation"] == "minio-default"
    muts = api.mutations()
    assert muts == [("create", "/apis/velero.io/v1/namespaces/velero/backups")]          # the one mutation
    with pytest.raises(C.StagingError, match="Velero"):
        A.verify(rec, plan)                                                              # no status receipt yet
    complete_backup(api, rec)
    A.velero_receipt(api, rec)
    r = A.verify(rec, plan)
    assert r["event"] == "archive-complete" and r["data"]["plan_sha256"] == d
    for f in (tmp_path / "evidence").rglob("*"):
        if f.is_file():
            text = f.read_text()
            assert not any(m in text for m in MARKERS) and C.LAST_APPLIED not in text, f
    assert not any("/secrets" in p or "/configmaps" in p for _, p in api.calls)


@pytest.mark.parametrize("break_it, why", [
    (lambda api, rec, tmp: complete_backup(api, rec, "PartiallyFailed", errors=2), "PartiallyFailed"),
    (lambda api, rec, tmp: complete_backup(api, rec, warnings=1), "warnings"),
    (lambda api, rec, tmp: (complete_backup(api, rec),
                            (tmp / "offcluster" / "SHA256SUMS").write_text("tampered")), "differs at the destination"),
    (lambda api, rec, tmp: (complete_backup(api, rec),
                            next((tmp / "evidence" / "archive" / "objects" / "deployments" / "demo").iterdir())
                            .write_text("{}")), "missing or changed"),
])
def test_any_gap_refuses_the_archive(tmp_path, break_it, why):
    api, rec = setup(tmp_path)
    plan, _ = run_all(api, rec)
    break_it(api, rec, tmp_path)
    A.velero_receipt(api, rec)
    with pytest.raises(C.StagingError, match=why):
        A.verify(rec, plan)
    events = [r["event"] for r in C.read_receipts(C.receipts_path(rec, "archive"))]
    assert "archive-complete" not in events and events[-1] == "archive-refused"


def test_assessed_warnings_pass(tmp_path):
    api, rec = setup(tmp_path, warnings_assessed=["volume snapshot skipped: metadata-only by design"])
    plan, _ = run_all(api, rec)
    complete_backup(api, rec, warnings=1)
    A.velero_receipt(api, rec)
    assert A.verify(rec, plan)["event"] == "archive-complete"


@pytest.mark.parametrize("name, why", [("journal.sqlite-wal", "live database"), ("model.db", "live database"),
                                       ("notes/secret.txt", "not on the evidence allowlist")])
def test_only_allowlisted_immutable_evidence_is_exported(tmp_path, name, why):
    api, rec = setup(tmp_path)
    p = tmp_path / "export" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("x")
    with pytest.raises(C.StagingError, match=why):
        A.build_plan(api, rec)


def test_a_reused_backup_name_or_overwrite_is_refused(tmp_path):
    api, rec = setup(tmp_path)
    plan, d = run_all(api, rec)
    with pytest.raises(C.StagingError, match="exists"):
        A.build_plan(api, rec)
    with pytest.raises(C.StagingError, match="never resubmitted"):
        A.velero_submit(api, rec, plan, d)
    with pytest.raises(C.StagingError, match="never overwritten"):
        A.export(rec, plan, d)
    with pytest.raises(C.StagingError):
        A.velero_submit(api, rec, plan, "0" * 64)


def test_the_cli_plan_is_read_only(tmp_path, capsys):
    api, rec = setup(tmp_path)
    path = tmp_path / "rec.json"
    path.write_text(json.dumps(rec))
    assert A.main(["--record", str(path)], api=api) == 0
    assert api.mutations() == []
    out = capsys.readouterr().out
    assert "plan digest" in out and "velero spec" in out
