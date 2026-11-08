"""Offline tests of populate_s0.py: a fake API, fake volumes (directories) read by the REAL in-pod reader script run
locally, a scratch git repository, and a small mandatory corpus. KUBECONFIG=/dev/null (conftest)."""

import hashlib
import json
import os
import subprocess
import sys

import pytest

import common_records as C
import execlib as X
import fake2 as F
import populate_s0 as P

CUTOFF = "2026-11-02T00:00:00Z"
RUN = "c28-prod-1"
UIDS = {"forecast-log-pvc": "d7994286-9d5f-406f-a9f8-d85676f11b75",
        "evidence-archive-pvc": "ee5332fa-05c8-40c3-bfb6-f27223bf62f1",
        "load-evidence-pvc": "6751e646-d384-49d1-a7f6-e1c94ca1c8a6",
        "ml-models-pvc": "6be3115c-cc91-453d-870f-a83d13e42f8c",
        "p8-evidence-pvc": "c5201b39-9d98-463a-b402-49a2926b29aa"}
MOUNTS = {"forecast-log-pvc": "/fc", "evidence-archive-pvc": "/archive", "load-evidence-pvc": "/evidence",
          "ml-models-pvc": "/models"}
SECRET_MARK = "-----BEGIN RSA PRIVATE KEY-----"


def sha(b):
    return hashlib.sha256(b).hexdigest()


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data if isinstance(data, bytes) else data.encode())


class Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def volumes(root):
    """The four claims' campaign contents (and post-cutoff/live material that must NOT be exported)."""
    v = {p: os.path.join(root, "vol", p) for p in MOUNTS}
    fc = [{"event": "decision", "at": "2026-11-01T23:58:00Z"}, {"issued_at": "2026-11-01T23:59:00Z", "forecasts": []},
          {"event": "decision", "at": "2026-11-02T00:01:00Z"}]
    write(os.path.join(v["forecast-log-pvc"], "forecasts.jsonl"),
          "".join(json.dumps(r) + "\n" for r in fc) + '{"event": "decision", "at": "2026-11-0')   # partial last line
    a = v["evidence-archive-pvc"]
    write(os.path.join(a, "archive/k1/manifest.json"), json.dumps({"copied_at": "2026-10-20T01:00:00Z"}))
    write(os.path.join(a, "archive/k1/lstm.pkl"), b"\x00model-bytes-1")
    write(os.path.join(a, "archive/k1/meta.json"), json.dumps({"training_cutoff": "2026-10-19T18:00:00Z"}))
    write(os.path.join(a, "archive/k2/manifest.json"), json.dumps({"copied_at": "2026-11-05T01:00:00Z"}))   # later
    write(os.path.join(a, "archive/k2/lstm.pkl"), b"later")
    write(os.path.join(a, "INDEX.jsonl"), json.dumps({"event": "archived", "copied_at": "2026-10-20T01:00:00Z"}) + "\n")
    write(os.path.join(a, "logs/api-ensemble.jsonl"), json.dumps({"ts": "2026-10-30T00:00:00Z"}) + "\n")
    write(os.path.join(a, "logs/trainer-index.jsonl"), json.dumps({"at": "2026-10-30T00:00:00Z"}) + "\n")
    e = v["load-evidence-pvc"]
    write(os.path.join(e, "attempts/load-20261101T2300Z-20261101T231400Z-7.json"), json.dumps({"hour": 1}))
    write(os.path.join(e, "attempts/load-20261102T0100Z-20261102T011400Z-7.json"), json.dumps({"hour": 2}))  # later
    write(os.path.join(e, "load-20261101T2300Z.json"), json.dumps({"latest": 1}))
    write(os.path.join(e, ".lock"), b"")
    m = v["ml-models-pvc"]
    art = b"pickled-model"
    write(os.path.join(m, "lstm_nginx-test_requests.pkl"), art)
    write(os.path.join(m, "lstm_nginx-test_requests.meta.json"), json.dumps({"trained_at": "2026-11-01T18:20:00Z"}))
    write(os.path.join(m, "lstm_myapp_requests.pkl"), b"in-progress")
    write(os.path.join(m, "lstm_myapp_requests.meta.json"), json.dumps({"trained_at": "2026-11-03T00:00:00Z"}))
    return v


def git_repo(root):
    repo = os.path.join(root, "repo")
    write(os.path.join(repo, "k8s-manifests/base/09-operator-deployment.yaml"), "kind: Deployment\n")
    write(os.path.join(repo, "deploy/prodcluster/load-gate/terminations.json"), json.dumps({"pod-a": {"at": "x"}}))
    write(os.path.join(repo, "deploy/eval-c28/staging/legacy.py"), "# helper\n")
    env = dict(P.git_env(), GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@e", GIT_COMMITTER_NAME="t",
               GIT_COMMITTER_EMAIL="t@e")             # never the ambient GIT_DIR/GIT_INDEX_FILE of a hook
    for cmd in (["init", "-q"], ["add", "-A"], ["commit", "-q", "-m", "c"]):
        subprocess.run(["git", "-C", repo] + cmd, check=True, env=env, capture_output=True)
    head = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True,
                          env=P.git_env()).stdout.strip()
    return repo, head


def corpus(root):
    """Local sources: a campaign tree with a checksum chain, protocol files, an inventory."""
    camp, proto, inv = (os.path.join(root, d) for d in ("campaign-src", "protocol-src", "inventory-src"))
    write(os.path.join(camp, "final-20261102/p8-fetch-final-20261102.jsonl"), '{"receipt": {}}\n')
    write(os.path.join(camp, "final-20261102/fetch-run.log"), "ok\n")
    sums = "".join(f"{sha(open(os.path.join(camp, 'final-20261102', n), 'rb').read())}  {n}\n"
                   for n in ("p8-fetch-final-20261102.jsonl", "fetch-run.log"))
    write(os.path.join(camp, "final-20261102/SHA256SUMS-fetch"), sums)
    write(os.path.join(camp, "training-exceptions-D1106.md"), "# D-1106\n")
    write(os.path.join(proto, "DECISIONS.md"), "# decisions\n")
    inventory = {"cluster_uid": F.CLUSTER, "legacy": {"found": [
        {"kind": "deployments", "namespace": "ml-engine", "name": "predictive-operator", "role": "operator",
         "uid": "5b6e3be1-d076-4a75-862c-7a47c4fa0dfd"}]},
        "objects": {"persistentvolumes": [{"name": f"pv-{n}", "uid": f"pv-uid-{n}", "reclaim_policy": "Delete",
                                           "storage_class": "harvester", "claim": {"uid": u, "name": n}}
                                          for n, u in UIDS.items()]}}
    write(os.path.join(inv, "inventory-20261108T024922Z.json"), json.dumps(inventory))
    return camp, proto, inv


def mandatory():
    m = ["S0-SHA256SUMS", "campaign/final-20261102/SHA256SUMS-fetch", "campaign/final-20261102/fetch-run.log",
         "campaign/final-20261102/p8-fetch-final-20261102.jsonl", "campaign/training-exceptions-D1106.md",
         "protocol/DECISIONS.md", "inventory/inventory-20261108T024922Z.json",
         "source/legacy-source-manifest.json", "source/helper-source-manifest.json", "source/terminations.json",
         "restoration/legacy-selective-restore.md", "storage/pvc-retention-and-export-coverage.json"]
    m += [f"pvc-exports/{p}/{n}" for p in P.PVC_EXPORTS for n in ("campaign-export.tar", "manifest.json", "SHA256SUMS")]
    return m


@pytest.fixture
def env(tmp_path):
    root = str(tmp_path)
    vols = volumes(root)
    repo, head = git_repo(root)
    camp, proto, inv = corpus(root)
    m = mandatory()
    rec = {"schema": C.RECORD_SCHEMA, "run_id": RUN, "cluster_uid": F.CLUSTER,
           "kubeconfig": {"path": "/home/ops/.config/devkuban/c28/prodcluster-eval.kubeconfig", "context": "prodcluster"},
           "evidence_dir": os.path.join(root, "evidence"), "lock_dir": os.path.join(root, "locks"),
           "source": {"commit": "f44f7f5d25830bb37e9dba2348941a1da4419643"},
           "archive": {"unaccounted_reviewed": [{"kind": "leases", "namespace": "ml-engine",
                                                 "name": "predictive-autoscaler-leader", "role": "lease",
                                                 "reason": "observed absent"}],
                       "evidence": {"export_dir": os.path.join(root, "s0-export-source"), "allowlist": m,
                                    "mandatory": m,
                                    "checksum_manifests": ["S0-SHA256SUMS", "campaign/final-20261102/SHA256SUMS-fetch"]
                                    + [f"pvc-exports/{p}/SHA256SUMS" for p in P.PVC_EXPORTS]}},
           "populate": {
               "cutoff": CUTOFF,
               "content_sources": {"campaign/": camp, "protocol/": proto, "inventory/": inv},
               "legacy_source": {"repo_dir": repo, "commit": head[:7], "paths": ["k8s-manifests"],
                                 "operator_image": "harbor.example/autoscaler/operator@sha256:" + "b" * 64},
               "terminations": {"repo_dir": repo, "commit": head,
                                "path": "deploy/prodcluster/load-gate/terminations.json"},
               "helper_source": {"repo_dir": repo, "commit": head, "paths": ["deploy/eval-c28/staging"]},
               "inventory": "inventory-20261108T024922Z.json",
               "retained_pvcs": [{"name": n, "uid": u} for n, u in UIDS.items()],
               "p8": {"export": "campaign/final-20261102/p8-fetch-final-20261102.jsonl",
                      "sha256": sha(b'{"receipt": {}}\n')},
               "requirements": {"forecast-log-pvc": "closed campaign issuance and decision records"},
               "reader": {"image": "public.ecr.aws/docker/library/python@sha256:" + "4" * 64,
                          "deadline_seconds": 900, "exec_timeout_seconds": 600},
               "budget": {"max_total_bytes": 10_000_000},
               "pvcs": {p: {"uid": UIDS[p], "mount": MOUNTS[p], "max_bytes": 1_000_000, "rules": "default"}
                        for p in P.PVC_EXPORTS}}}
    path = os.path.join(root, "preparation.json")
    write(path, json.dumps(rec))
    api = F.FakeApi2()
    for n, u in UIDS.items():
        api.add("persistentvolumeclaims", {"kind": "PersistentVolumeClaim",
                                           "metadata": {"name": n, "namespace": "ml-engine", "uid": u},
                                           "spec": {"volumeName": f"pv-{n}", "storageClassName": "harvester",
                                                    "accessModes": ["ReadWriteOnce"]}})
        api.add("persistentvolumes", {"kind": "PersistentVolume", "metadata": {"name": f"pv-{n}"},
                                      "spec": {"persistentVolumeReclaimPolicy": "Delete"}})
    api.add("volumeattachments", {"kind": "VolumeAttachment", "metadata": {"name": "va-1"},
                                  "spec": {"nodeName": "prodworker-ml", "source": {"persistentVolumeName":
                                                                                   "pv-ml-models-pvc"}},
                                  "status": {"attached": True}})
    api.add("pods", {"kind": "Pod", "metadata": {"name": "ml-api-0", "namespace": "ml-engine"},
                     "spec": {"nodeName": "prodworker-ml", "volumes": [
                         {"name": "m", "persistentVolumeClaim": {"claimName": "ml-models-pvc"}}]},
                     "status": {"phase": "Running"}})

    def kubelet(a):
        for (k, _, _), o in list(a.objects.items()):
            if k == "pods" and P.READER_LABEL in (o["metadata"].get("labels") or {}):
                o.setdefault("status", {})["phase"] = "Running"
    api.on_poll.append(kubelet)
    record = X.load_record(path, ("populate", "archive"))
    clock = Clock()
    return {"record": record, "api": api, "kube": record.kube(api), "vols": vols, "root": root, "path": path,
            "exe": fake_exec(vols), "clock": clock}


def fake_exec(vols, mutate=None):
    """kubectl exec, offline: run the REAL reader script locally against the claim's directory."""
    def exe(pod, job, out_path):
        pvc = next(p for p in P.PVC_EXPORTS if pod.startswith(f"c28-s0-{p[:-4]}-"))
        job = dict(job, mount=vols[pvc])
        if mutate:
            mutate(pvc, job)
        with open(out_path, "wb") as fh:
            res = subprocess.run([sys.executable, "-B", "-c", P.READER], input=json.dumps(job).encode(),
                                 stdout=fh, stderr=subprocess.PIPE, timeout=60)
        return res.returncode, res.stderr.decode()
    return exe


def reviewed(record, plan, helper, name):
    saved = X.save_plan(record, helper, plan, name)
    return X.load_plan(saved["plan"]), saved["plan_digest"]


def do_probe(e):
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")
    assert p["problems"] == [], p["problems"]
    return P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)


def do_export(e):
    p, d = reviewed(e["record"], P.plan(e["record"], e["kube"]), P.HELPER, "plan")
    assert p["problems"] == [], p["problems"]
    return P.export(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)


def members(e, pvc):
    m = json.load(open(os.path.join(e["record"].doc["archive"]["evidence"]["export_dir"], "pvc-exports", pvc,
                                    "manifest.json")))
    return {f["member"]: f for f in m["files"]}


def test_full_population_and_verification(env):
    e = env
    do_probe(e)
    assert P.leftover_readers(e["kube"], e["record"]) == []
    out = do_export(e)
    assert out["event"] == "export-complete"
    v = P.verify(e["record"])
    assert v["result"] == "PASS", v["problems"]
    root = e["record"].doc["archive"]["evidence"]["export_dir"]
    assert sorted(os.path.relpath(os.path.join(d, n), root) for d, _, ns in os.walk(root) for n in ns) == \
        sorted(mandatory())
    # cutoff selection by the files' own names and record timestamps
    fc = members(e, "forecast-log-pvc")["forecasts.jsonl"]
    assert fc["selection"] == {"kind": "prefix", "lines": 2}
    assert set(members(e, "evidence-archive-pvc")) == {"archive/k1/manifest.json", "archive/k1/lstm.pkl",
                                                       "archive/k1/meta.json", "INDEX.jsonl",
                                                       "logs/api-ensemble.jsonl", "logs/trainer-index.jsonl"}
    assert set(members(e, "load-evidence-pvc")) == {"attempts/load-20261101T2300Z-20261101T231400Z-7.json",
                                                    "load-20261101T2300Z.json"}            # .lock empty, later hour out
    assert set(members(e, "ml-models-pvc")) == {"lstm_nginx-test_requests.pkl", "lstm_nginx-test_requests.meta.json"}
    # S0-SHA256SUMS covers every mandatory file except itself
    s0 = open(os.path.join(root, "S0-SHA256SUMS")).read().splitlines()
    assert sorted(ln.split("  ", 1)[1] for ln in s0) == sorted(m for m in mandatory() if m != "S0-SHA256SUMS")
    # every reader pod's cleanup was proven (4 probes + 4 exports), none left
    proven = [r for h in (P.PROBE, P.HELPER) for r in X.receipts(e["record"], h)
              if r["event"] == "reader-cleanup-proven"]
    assert len(proven) == 8 and P.leftover_readers(e["kube"], e["record"]) == []
    assert not os.path.exists(C.lock_path(e["record"].doc, P.LOCK_SCOPE))
    cov = json.load(open(os.path.join(root, "storage/pvc-retention-and-export-coverage.json")))
    assert len(cov["claims"]) == 5 and all("residual_not_backed_up" in c for c in cov["claims"])
    assert all("warning" in c for c in cov["claims"])                        # reclaim policy Delete is flagged


def test_reader_pod_is_read_only_pinned_and_tokenless(env):
    e = env
    binding = P.claim_binding(e["kube"], "ml-models-pvc", UIDS["ml-models-pvc"])
    pod = P.reader_pod(e["record"], P.section(e["record"]), "ml-models-pvc", binding, "abc")
    spec = pod["spec"]
    assert spec["automountServiceAccountToken"] is False
    assert spec["volumes"][0]["persistentVolumeClaim"]["readOnly"] is True
    assert spec["containers"][0]["volumeMounts"][0]["readOnly"] is True
    assert spec["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    assert spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0][
        "matchExpressions"][0]["values"] == ["prodworker-ml"]


def test_source_chain_tampering_blocks_the_plan(env):
    e = env
    do_probe(e)
    camp = e["record"].doc["populate"]["content_sources"]["campaign/"]
    write(os.path.join(camp, "final-20261102/fetch-run.log"), "edited\n")
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any("SHA256SUMS-fetch: fetch-run.log does not match" in p for p in probs)


def test_destination_tampering_fails_verify(env):
    e = env
    do_probe(e)
    do_export(e)
    root = e["record"].doc["archive"]["evidence"]["export_dir"]
    os.chmod(os.path.join(root, "campaign/final-20261102/fetch-run.log"), 0o640)
    write(os.path.join(root, "campaign/final-20261102/fetch-run.log"), "edited\n")
    v = P.verify(e["record"])
    assert v["result"] == "FAIL" and any("fetch-run.log" in p for p in v["problems"])


@pytest.mark.parametrize("lines, why", [
    (['{"at": "2026-11-01T00:00:00Z"}', '{"at": "2026-11-03T00:00:00Z"}', '{"at": "2026-11-01T01:00:00Z"}'],
     "not append-ordered"),
    (['{"at": "2026-11-01T00:00:00Z"}', '{"other": 1}'], "without any of"),
    (['{"at": "2026-11-05T00:00:00Z"}'], "no record of forecasts.jsonl is at or before the cutoff"),
])
def test_jsonl_prefix_selection_fails_closed(env, lines, why):
    e = env
    write(os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl"), "\n".join(lines) + "\n")
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any(why in p for p in probs), probs


def test_rule_cutoff_override(env):
    """A rule may carry its own cutoff (e.g. the export's own files up to 11-02 08:44Z)."""
    e = env
    doc = json.load(open(e["path"]))
    doc["populate"]["pvcs"]["load-evidence-pvc"]["rules"] = [
        {"type": "named", "glob": "attempts/load-*.json", "ts_regex": r"^load-(\d{8}T\d{4})Z",
         "ts_format": "%Y%m%dT%H%M", "cutoff": "2026-11-02T08:44:00Z"}]
    write(e["path"], json.dumps(doc))
    e["record"] = X.load_record(e["path"], ("populate", "archive"))
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert [it["path"] for it in p["pvcs"]["load-evidence-pvc"]["selection"]] == [
        "attempts/load-20261101T2300Z-20261101T231400Z-7.json", "attempts/load-20261102T0100Z-20261102T011400Z-7.json"]


def test_size_budget_refuses(env):
    e = env
    doc = json.load(open(e["path"]))
    doc["populate"]["pvcs"]["evidence-archive-pvc"]["max_bytes"] = 10
    write(e["path"], json.dumps(doc))
    e["record"] = X.load_record(e["path"], ("populate", "archive"))
    do_probe(e)
    p, d = reviewed(e["record"], P.plan(e["record"], e["kube"]), P.HELPER, "plan")
    assert any("over its budget 10" in x for x in p["problems"])
    with pytest.raises(X.StagingError, match="over its budget"):
        P.export(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    assert not os.path.exists(e["record"].doc["archive"]["evidence"]["export_dir"])


def test_busy_jobs_and_leftover_readers_refuse(env):
    e = env
    e["api"].add("jobs", {"kind": "Job", "metadata": {"name": "ml-training-29001", "namespace": "ml-engine"},
                          "status": {"active": 1}})
    assert any("active Jobs ['ml-training-29001']" in p for p in P.probe_plan(e["record"], e["kube"])["problems"])


def test_cleanup_must_be_proven(env):
    e = env
    real = e["api"].delete
    e["api"].delete = lambda path, uid, rv=None: {}                     # the API accepts, the pod never goes
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")
    with pytest.raises(X.StagingError, match="cleanup of .* is NOT proven"):
        P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    e["api"].delete = real


def test_member_mismatch_is_rejected(env):
    e = env
    do_probe(e)

    def grow(pvc, job):                                                 # a file changed since the probe
        if pvc == "load-evidence-pvc":
            write(os.path.join(job["mount"], "load-20261101T2300Z.json"), json.dumps({"latest": 2, "x": "y"}))
    e["exe"] = fake_exec(e["vols"], grow)
    with pytest.raises(X.StagingError, match="changed size since the probe"):
        do_export(e)
    assert P.leftover_readers(e["kube"], e["record"]) == []             # cleanup still proven after the failure


def test_secret_hits_block_without_printing(env, capsys):
    e = env
    camp = e["record"].doc["populate"]["content_sources"]["campaign/"]
    write(os.path.join(camp, "training-exceptions-D1106.md"), f"x\n{SECRET_MARK}\nMIIE...\n")
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert any("secret scan hit" in x for x in p["problems"])
    assert SECRET_MARK not in json.dumps(p)
    for h in (P.PROBE, P.HELPER):
        path = C.receipts_path(e["record"].doc, h)
        assert not os.path.exists(path) or SECRET_MARK not in open(path).read()


def test_generated_provenance_has_no_environment(env):
    e = env
    gen = P.generated_sources(e["record"], P.section(e["record"]))
    legacy = json.loads(gen["source/legacy-source-manifest.json"])
    assert legacy["blobs"][0]["path"] == "k8s-manifests/base/09-operator-deployment.yaml"
    assert legacy["observed_operator_image"].endswith("b" * 64)
    assert json.loads(gen["source/terminations.json"]) == {"pod-a": {"at": "x"}}
    assert "env" not in json.dumps(legacy).lower().replace("environment", "")
    md = P.restoration_md(e["record"], P.section(e["record"])).decode()
    assert "5b6e3be1-d076-4a75-862c-7a47c4fa0dfd" in md and "foreign writer" in md and "namespace-wide" in md


def test_existing_different_destination_is_never_overwritten(env):
    e = env
    do_probe(e)
    root = e["record"].doc["archive"]["evidence"]["export_dir"]
    write(os.path.join(root, "protocol/DECISIONS.md"), "someone else's\n")
    assert any("never overwritten" in p for p in P.plan(e["record"], e["kube"])["problems"])


def test_lost_create_response_holds_the_lock_until_recovery(env):
    e = env
    real = e["api"].create

    def lost(path, body):
        real(path, body)
        raise C.ApiError("create: failed (timeout)", 504)
    e["api"].create = lost
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")
    with pytest.raises(C.ApiError):
        P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    e["api"].create = real
    lock = C.lock_path(e["record"].doc, P.LOCK_SCOPE)
    assert os.path.exists(lock) and P.leftover_readers(e["kube"], e["record"])
    body = json.load(open(lock))
    dead = subprocess.Popen(["true"]); dead.wait()
    body["pid"] = dead.pid
    open(lock, "w").write(json.dumps(body, sort_keys=True))
    rp, rd = reviewed(e["record"], P.recover_plan(e["record"], e["kube"]), "populate-s0-recover", "recover-plan")
    assert [a["observed"] for a in rp["actions"]] == ["done"]
    P.recover(e["record"], e["kube"], rp, rd)
    assert not os.path.exists(lock) and P.leftover_readers(e["kube"], e["record"]) == []


def test_cli_plan_is_read_only(env, capsys):
    e = env
    assert P.main(["--record", e["path"], "probe-plan"], api=e["api"]) == 0
    assert e["api"].mutations() == []
    assert P.main(["--record", e["path"], "probe", "--plan-digest", "0" * 64], api=e["api"]) == 1


def test_git_ignores_an_ambient_hook_repository(env, monkeypatch, tmp_path):
    """Inside a git hook GIT_DIR/GIT_INDEX_FILE name another repository; provenance must still come from repo_dir."""
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "elsewhere.git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / "elsewhere.index"))
    gen = P.generated_sources(env["record"], P.section(env["record"]))
    assert json.loads(gen["source/terminations.json"]) == {"pod-a": {"at": "x"}}
