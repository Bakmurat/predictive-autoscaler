"""Offline tests of populate_s0.py: a fake API, fake volumes (directories) read by the REAL in-pod reader script run
locally, a scratch git repository, and a small mandatory corpus. KUBECONFIG=/dev/null (conftest)."""

import hashlib
import copy
import json
import re
import os
import subprocess
import tarfile
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


def jl(*recs):
    return "".join(json.dumps(r, sort_keys=True) + "\n" for r in recs)


def volumes(root):
    """The four claims as the campaign writers left them (deploy/eks-benchmark/instrumentation/archive.py,
    collect_load_evidence.py, the operator and trainer), plus post-cutoff and live material that must NOT be exported."""
    v = {p: os.path.join(root, "vol", p) for p in MOUNTS}
    fc = [{"event": "decision", "at": "2026-11-01T23:58:00Z", "artifact_sha256": None},
          {"issued_at": "2026-11-01T23:59:00Z", "forecasts": [], "model_name": "lstm_nginx-test_requests", "artifact_sha256": sha(b"pickled-model-v1")},
          {"event": "decision", "at": "2026-11-02T00:01:00Z"}]
    write(os.path.join(v["forecast-log-pvc"], "forecasts.jsonl"), jl(*fc) + '{"event": "decision", "at": "2026-11-0')
    a = v["evidence-archive-pvc"]
    # the model pair, trained before the cutoff, archived immutably (archive_pair: manifest + artifact + sidecar)
    art = b"pickled-model-v1"
    side = json.dumps({"trained_at": "2026-11-01T18:20:00Z", "training_cutoff": "2026-11-01T18:00:00Z",
                       "artifact_sha256": sha(art), "artifact_bytes": len(art)}, sort_keys=True).encode()
    key = f"20261101T1800Z-{sha(art)[:12]}"
    man = {"key": key, "copied_at": "2026-11-01T18:35:02Z", "started_at": "2026-11-01T18:35:00Z", "event": "archived",
           "artifact": "lstm_nginx-test_requests.pkl", "artifact_sha256": sha(art), "artifact_bytes": len(art),
           "sidecar_sha256": sha(side), "training_cutoff": "2026-11-01T18:00:00Z",
           "trained_at_per_sidecar": "2026-11-01T18:20:00Z", "archiver_version": 2, "job": "evidence-archive-1"}
    write(os.path.join(a, f"archive/{key}/manifest.json"), json.dumps(man, indent=1, sort_keys=True))
    write(os.path.join(a, f"archive/{key}/lstm_nginx-test_requests.pkl"), art)
    write(os.path.join(a, f"archive/{key}/lstm_nginx-test_requests.meta.json"), side)
    # a sidecar variant of the same artifact (artifact_key, no artifact file)
    side2 = json.dumps({"trained_at": "2026-11-01T18:20:00Z", "training_cutoff": "2026-11-01T18:00:00Z",
                        "artifact_sha256": sha(art), "note": "re-written sidecar"}, sort_keys=True).encode()
    vkey = f"{key}-sidecar-{sha(side2)[:12]}"
    write(os.path.join(a, f"archive/{vkey}/manifest.json"), json.dumps(dict(man, key=vkey, artifact_key=key,
                                                                            event="sidecar_variant",
                                                                            sidecar_sha256=sha(side2),
                                                                            copied_at="2026-11-01T20:05:00Z")))
    write(os.path.join(a, f"archive/{vkey}/lstm_nginx-test_requests.meta.json"), side2)
    # a bundle archived after the collection cutoff
    write(os.path.join(a, "archive/20261103T0000Z-later/manifest.json"),
          json.dumps({"key": "x", "copied_at": "2026-11-03T00:05:00Z", "artifact": "a.pkl"}))
    write(os.path.join(a, "archive/20261103T0000Z-later/a.pkl"), b"later")
    # INDEX with a recovered record whose OLD copied_at follows newer records (recovered_at is its append time)
    write(os.path.join(a, "INDEX.jsonl"), jl(
        dict(man, event="archived"), {"event": "sidecar_variant", "key": vkey, "copied_at": "2026-11-01T20:05:00Z"},
        dict(man, event="archived", index_recovered=True, recovered_at="2026-11-01T21:00:00Z"),
        {"event": "archived", "key": "x", "copied_at": "2026-11-03T00:05:00Z"}))
    write(os.path.join(a, "REJECTS.jsonl"), jl({"event": "rejected", "at": "2026-10-30T10:00:00Z",
                                                "reason": "artifact changed during copy", "job": "e-2"}))
    write(os.path.join(a, "RUNS.jsonl"), jl({"started_at": "2026-11-01T18:35:00Z", "finished_at": "2026-11-01T18:35:03Z",
                                             "pair": {"result": "archived"}, "errors": []},
                                            {"started_at": "2026-11-02T09:00:00Z", "finished_at": "2026-11-02T09:00:02Z",
                                             "pair": {}, "errors": []}))
    write(os.path.join(a, "logs/api-ensemble.jsonl"), jl({"ts": "2026-11-01T23:59:58Z", "collected_at": "2026-11-02T00:05:00Z",
                                                          "line_sha256": "a" * 64}))
    write(os.path.join(a, "logs/api-reload.jsonl"), jl({"ts": "2026-11-01T10:00:00Z", "collected_at": "2026-11-01T10:05:00Z",
                                                        "line": "reloaded"}))
    write(os.path.join(a, "logs/api-coverage.jsonl"), jl({"collected_at": "2026-11-01T10:05:00Z", "pod": "ml-api-0",
                                                          "lines": 3, "flags": []}))
    log = "2026-11-01T18:20:00Z trained\n"
    base = "ml-training-29001-1a2b3c4d"
    tmeta = {"collected_at": "2026-11-01T18:35:01Z", "job": "ml-training-29001", "job_uid": "j-uid",
             "pod": "ml-training-29001-x", "pod_uid": "1a2b3c4d-0000-4000-8000-000000000000", "phase": "Succeeded",
             "containers": [{"name": "trainer", "exit_code": 0, "started_at": "2026-11-01T18:00:05Z",
                             "finished_at": "2026-11-01T18:20:01Z", "log_sha256": sha(log.encode()),
                             "log_lines": 1, "previous_log_saved": False}], "archiver_version": 2}
    write(os.path.join(a, f"logs/trainer/{base}.json"), json.dumps(tmeta, indent=1, sort_keys=True))
    write(os.path.join(a, f"logs/trainer/{base}.trainer.log"), log)
    write(os.path.join(a, "logs/trainer-index.jsonl"), jl(tmeta))
    write(os.path.join(a, "logs/api-state.json"), json.dumps({"uid": {"last_ts": "x"}}))
    write(os.path.join(a, ".staging/123-abc/partial.pkl"), b"partial")
    e = v["load-evidence-pvc"]
    write(os.path.join(e, "attempts/load-20261101T2300Z-20261101T231400Z-7.json"), json.dumps({"hour": 1}))
    write(os.path.join(e, "attempts/load-20261102T0100Z-20261102T011400Z-7.json"), json.dumps({"hour": 2}))  # later
    write(os.path.join(e, "load-20261101T2300Z.json"), json.dumps({"latest": 1}))
    write(os.path.join(e, ".lock"), b"")
    m = v["ml-models-pvc"]
    write(os.path.join(m, "lstm_nginx-test_requests.pkl"), art)
    write(os.path.join(m, "lstm_nginx-test_requests.meta.json"), side)
    write(os.path.join(m, "lstm_myapp_requests.pkl"), b"in-progress")
    write(os.path.join(m, "lstm_myapp_requests.meta.json"), json.dumps({"trained_at": "2026-11-03T00:00:00Z",
                                                                         "artifact_sha256": "0" * 64}))
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
               "cutoff": CUTOFF, "collection_cutoff": "2026-11-02T08:44:00Z",
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
        argv = P.exec_argv(pod, job)
        tail = argv[argv.index("--") + 1:]                              # exactly what runs in the container
        assert tail[0] == "python3"
        with open(out_path, "wb") as fh:
            res = subprocess.run([sys.executable] + tail[1:], input=P.exec_stdin(job), stdout=fh,
                                 stderr=subprocess.PIPE, timeout=60)
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
    assert (fc["kind"], fc["lines"], fc["lines_after_cutoff"]) == ("prefix", 2, 1)
    ea = members(e, "evidence-archive-pvc")
    bundles = {p.split("/")[1] for p in ea if p.startswith("archive/")}
    assert len(bundles) == 2 and not any("later" in b for b in bundles)        # the post-cutoff bundle is out
    assert {"INDEX.jsonl", "REJECTS.jsonl", "RUNS.jsonl", "logs/api-ensemble.jsonl", "logs/api-reload.jsonl",
            "logs/api-coverage.jsonl", "logs/trainer-index.jsonl", "logs/trainer/ml-training-29001-1a2b3c4d.json",
            "logs/trainer/ml-training-29001-1a2b3c4d.trainer.log"} <= set(ea)
    assert ea["INDEX.jsonl"]["lines"] == 3 and ea["RUNS.jsonl"]["lines"] == 1    # recovered record kept, later out
    assert ea["logs/trainer/ml-training-29001-1a2b3c4d.trainer.log"]["verified"] == "equals the index's log_sha256"
    assert not any(p.startswith((".staging", "logs/api-state")) for p in ea)
    man = json.load(open(os.path.join(root, "pvc-exports/evidence-archive-pvc/manifest.json")))
    assert {x["path"] for x in man["reviewed_exclusions"]} == {"logs/api-state.json", ".staging/123-abc/partial.pkl"}
    assert set(members(e, "load-evidence-pvc")) == {"attempts/load-20261101T2300Z-20261101T231400Z-7.json",
                                                    "load-20261101T2300Z.json"}            # .lock reviewed, later out
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
    models = next(c for c in cov["claims"] if c["claim"] == "ml-models-pvc")
    assert models["protected"]["archived_copies"][0]["archived_bundle"].startswith("20261101T1800Z-")
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


D = '{"event": "decision", "at": "%s"}'
@pytest.mark.parametrize("lines, why", [
    ([D % "2026-11-01T00:00:00Z", D % "2026-11-03T00:00:00Z", D % "2026-11-01T01:00:00Z"],
     "follows a record more than 0 s after it (not append-ordered)"),
    ([D % "2026-11-01T00:00:00Z", '{"event": "decision", "other": 1}'], "has no timestamp for its type (['at'])"),
    ([D % "2026-11-05T00:00:00Z"], "no record of forecasts.jsonl is at or before the cutoff"),
    (['{"run_started": "2026-10-05T06:40:00Z", "operator_version": "v0.9", "token_hint": "abc123secret"}'],
     "at line 1 has no timestamp for its type (['issued_at']): keys [], 3 other keys (sha256 "),
    (['{"event": "brand_new_kind", "at": "2026-11-01T00:00:00Z", "value": "zzz-sensitive"}'],
     "has an unknown record type: keys ['at', 'event', 'value'], event unknown (str, sha256 "),
])
def test_jsonl_prefix_selection_fails_closed(env, lines, why):
    e = env
    write(os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl"), "\n".join(lines) + "\n")
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any(why in p for p in probs), probs
    for leaked in ("token_hint", "run_started", "operator_version", "abc123secret", "brand_new_kind", "zzz-sensitive"):
        assert not any(leaked in p for p in probs), probs


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


def test_member_mismatch_is_rejected(env):
    e = env
    do_probe(e)

    def grow(pvc, job):                                                 # a file changed since the probe
        if pvc == "load-evidence-pvc":
            write(os.path.join(job["mount"], "load-20261101T2300Z.json"), json.dumps({"latest": 2, "x": "y"}))
    e["exe"] = fake_exec(e["vols"], grow)
    with pytest.raises(X.StagingError, match="changed since the probe"):
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


def crash(record):
    lock = C.lock_path(record.doc, P.LOCK_SCOPE)
    body = json.load(open(lock))
    dead = subprocess.Popen(["true"]); dead.wait()
    body["pid"] = dead.pid
    open(lock, "w").write(json.dumps(body, sort_keys=True))
    return lock


def test_unproven_cleanup_keeps_the_lock_until_recovery_proves_it(env):
    """Codex #34 item 1: a reader whose deletion cannot be proven leaves its lifecycle unresolved and the shared lock
    held; recovery deletes the nonce's pods (durable delete intents) and proves cleanup before releasing."""
    e = env
    real = e["api"].delete
    e["api"].delete = lambda path, uid, rv=None: {}
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")
    with pytest.raises(X.StagingError, match="NOT proven"):
        P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    e["api"].delete = real
    lock = C.lock_path(e["record"].doc, P.LOCK_SCOPE)
    assert os.path.exists(lock) and P.leftover_readers(e["kube"], e["record"])
    assert P._unresolved_steps(e["record"], P.PROBE)
    assert [r for r in X.receipts(e["record"], P.PROBE) if r["event"].startswith("delete-intent-")]
    crash(e["record"])
    rp, rd = reviewed(e["record"], P.recover_plan(e["record"], e["kube"]), "populate-s0-recover", "recover-plan")
    assert rp["actions"][0]["pods"]
    P.recover(e["record"], e["kube"], rp, rd, e["clock"].sleep, e["clock"].now)
    assert not os.path.exists(lock) and P.leftover_readers(e["kube"], e["record"]) == []
    assert P._unresolved_steps(e["record"], P.PROBE) == []


def test_startup_failure_still_cleans_up_before_releasing(env):
    """Codex #34 item 1: a reader that never starts is still deleted and proven gone; only then is the lock released."""
    e = env
    e["api"].on_poll.clear()                                           # the kubelet never runs the pod
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")
    with pytest.raises(X.StagingError, match="did not start"):
        P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    assert P.leftover_readers(e["kube"], e["record"]) == []
    assert not os.path.exists(C.lock_path(e["record"].doc, P.LOCK_SCOPE))
    steps = [r for r in X.receipts(e["record"], P.PROBE) if r["event"].startswith("step-")]
    assert steps[-1]["data"]["result"] == "cleaned-after-failure"


def test_lost_create_response_is_reconciled_by_the_proven_cleanup(env):
    e = env
    real = e["api"].create

    def lost(path, body):
        real(path, body)
        raise C.ApiError("create: failed (timeout)", 504)
    e["api"].create = lost
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")
    with pytest.raises(C.ApiError):
        P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    assert P.leftover_readers(e["kube"], e["record"]) == []             # the committed pod was found and deleted


def test_legacy_and_s0_exclude_each_other(env):
    """Codex #34 item 2: one atomic lock per cluster and scope; neither side can start while the other holds it."""
    e = env
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")
    with C.exclusive_lock(e["record"].doc, "legacy", holder="legacy quiesce"):        # legacy.py's scope
        with pytest.raises(X.StagingError, match="exists"):
            P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    seen = []

    def legacy_tries(pvc, job):
        try:
            with C.exclusive_lock(e["record"].doc, "legacy", holder="legacy quiesce"):
                seen.append("legacy ran")
        except X.StagingError:
            seen.append("legacy refused")
    e["exe"] = fake_exec(e["vols"], legacy_tries)
    P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    assert set(seen) == {"legacy refused"}


def test_preconditions_are_revalidated_before_each_reader(env):
    e = env
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")

    def job_starts(pvc, job):
        if pvc == "forecast-log-pvc":
            e["api"].add("jobs", {"kind": "Job", "metadata": {"name": "evidence-archive-29002", "namespace": "ml-engine"},
                                  "status": {"active": 1}})
    e["exe"] = fake_exec(e["vols"], job_starts)
    with pytest.raises(X.StagingError, match="active Jobs"):
        P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    assert [r["data"]["pvc"] for r in X.receipts(e["record"], P.PROBE) if r["event"] == "probed"] == ["forecast-log-pvc"]
    assert P.leftover_readers(e["kube"], e["record"]) == []


def test_same_length_edit_after_the_probe_is_refused(env):
    """Codex #34 item 3: the selection is byte-bound (sha256), not size-bound."""
    e = env
    do_probe(e)

    def edit(pvc, job):
        if pvc == "load-evidence-pvc":
            write(os.path.join(job["mount"], "load-20261101T2300Z.json"), json.dumps({"latest": 7}))   # same length
    e["exe"] = fake_exec(e["vols"], edit)
    with pytest.raises(X.StagingError, match="bytes differ from the probe"):
        do_export(e)


@pytest.mark.parametrize("target, why", [
    ("bundle-artifact", "do not match the manifest's original hashes"),
    ("trainer-log", "does not match its log_sha256"),
])
def test_original_writer_hashes_are_verified(env, target, why):
    e = env
    a = e["vols"]["evidence-archive-pvc"]
    if target == "bundle-artifact":
        d = next(x for x in os.listdir(os.path.join(a, "archive")) if "sidecar" not in x and "later" not in x)
        write(os.path.join(a, "archive", d, "lstm_nginx-test_requests.pkl"), b"pickled-model-vX")
    else:
        write(os.path.join(a, "logs/trainer/ml-training-29001-1a2b3c4d.trainer.log"), "tampered\n")
    do_probe(e)
    assert any(why in x for x in P.plan(e["record"], e["kube"])["problems"])


def test_model_without_an_archived_copy_is_refused(env):
    """The mutable model directory is not historical storage: an exported pair needs its archived immutable copy."""
    e = env
    m = e["vols"]["ml-models-pvc"]
    art = b"another-model"
    write(os.path.join(m, "lstm_other_requests.pkl"), art)
    write(os.path.join(m, "lstm_other_requests.meta.json"), json.dumps({"trained_at": "2026-11-01T12:00:00Z",
                                                                          "artifact_sha256": sha(art)}))
    do_probe(e)
    assert any("lstm_other_requests.meta.json: no immutable archived copy" in x
               for x in P.plan(e["record"], e["kube"])["problems"])


def test_unreviewed_files_block_and_reviewed_exclusions_are_listed(env):
    """Codex #34 item 5: every file is selected, rule-excluded with a reason, or a reviewed exclusion."""
    e = env
    write(os.path.join(e["vols"]["evidence-archive-pvc"], "logs/new-collector.jsonl"), jl({"at": "x"}))
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert any("logs/new-collector.jsonl is neither selected" in x for x in p["problems"])
    ex = p["pvcs"]["evidence-archive-pvc"]["reviewed_exclusions"]
    assert {x["path"] for x in ex} == {"logs/api-state.json", ".staging/123-abc/partial.pkl"}
    assert all(x["reason"] for x in ex)
    rule_ex = {x["path"]: x["reason"] for x in p["pvcs"]["ml-models-pvc"]["rule_excluded"]}
    assert rule_ex["lstm_myapp_requests.meta.json"].startswith("the artifact does not match its sidecar")


def test_trainer_index_with_writer_records_and_recovered_index(env):
    """Codex #34 item 4: the writer's collected_at and nested containers; a recovered INDEX record (old copied_at
    after newer records) does not break the prefix."""
    e = env
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert p["problems"] == [], p["problems"]
    sel = {it["path"]: it for it in p["pvcs"]["evidence-archive-pvc"]["selection"]}
    assert sel["logs/trainer-index.jsonl"]["lines"] == 1 and sel["INDEX.jsonl"]["lines"] == 3


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



# --- the live probe failure (2026-11-08 04:37Z): exec transport, sanitized failures, read access -----------------------

def test_exec_command_line_is_short_and_carries_no_script():
    """The live failure: the 12 KB script on the exec command line made a ~23 KB request URL, which the proxy in
    front of the API refused; the script now travels on stdin."""
    import urllib.parse
    job = {"mode": "probe", "mount": "/data", "rules": [{"type": "jsonl_prefix", "path": "f", "ts_fields": ["at"],
                                                         "cutoff": CUTOFF}]}
    argv = P.exec_argv("c28-s0-forecast-log-abcdef0123", job)
    tail = argv[argv.index("--") + 1:]
    query = "&".join("command=" + urllib.parse.quote(a, safe="") for a in tail)
    assert len(query) < 1024 and P.READER not in argv and P.READER_SHA256 in argv and P.job_digest(job) in argv
    assert json.loads(P.exec_stdin({"mode": "probe"}))["script"] == P.READER


def test_a_script_other_than_the_reviewed_one_is_refused():
    job = {"mode": "probe", "mount": "/"}
    argv = P.exec_argv("x", job)
    tail = argv[argv.index("--") + 1:]
    stdin = (json.dumps({"script": P.READER + "\n# edited", "job": job}) + "\n").encode()
    res = subprocess.run([sys.executable] + tail[1:], input=stdin, capture_output=True, timeout=30)
    assert res.returncode == 3
    assert P.exec_failure(res.returncode, res.stdout.decode(), "") == \
        "the reader failed: DigestMismatch (integrity) at reader line 0 (exit 3)"


def test_kubectl_exec_failures_are_classified_not_echoed(env):
    e = env
    leak = "/very/secret/path?token=abc"

    def refused(pod, job, out_path):
        open(out_path, "wb").close()
        return 1, f"error: unable to upgrade connection: 414 Request-URI Too Large {leak}"
    e["exe"] = refused
    p, d = reviewed(e["record"], P.probe_plan(e["record"], e["kube"]), P.PROBE, "probe-plan")
    with pytest.raises(X.StagingError) as info:
        P.probe(e["record"], e["kube"], p, d, e["exe"], e["clock"].sleep, e["clock"].now)
    assert "kubectl exec failed (request-too-large, exit 1)" in str(info.value) and leak not in str(info.value)
    assert P.leftover_readers(e["kube"], e["record"]) == []          # the cleanup was still proven


needs_nonroot = pytest.mark.skipif(os.geteuid() == 0, reason="root reads anything; the permission cases need non-root")


@needs_nonroot
def test_unreadable_files_and_directories_are_reported_sanitized(env):
    e = env
    fc = os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl")
    tdir = os.path.join(e["vols"]["evidence-archive-pvc"], "logs/trainer")
    os.chmod(fc, 0o000); os.chmod(tdir, 0o000)
    try:
        do_probe(e)
        probs = "\n".join(P.plan(e["record"], e["kube"])["problems"])
    finally:
        os.chmod(fc, 0o644); os.chmod(tdir, 0o755)
    assert "forecast-log-pvc: rule jsonl_prefix forecasts.jsonl: PermissionError (permission) at reader line" in probs
    assert "is not readable (permission)" in probs                                   # not "missing"
    assert "evidence-archive-pvc: 1 directories are unreadable (permission)" in probs
    assert "Errno" not in probs and e["vols"]["forecast-log-pvc"] not in probs        # no message, no host path


@needs_nonroot
def test_a_reader_crash_reports_class_line_and_category_only(env):
    e = env
    do_probe(e)
    path = os.path.join(e["vols"]["load-evidence-pvc"], "load-20261101T2300Z.json")
    os.chmod(path, 0o000)
    try:
        with pytest.raises(X.StagingError) as info:
            do_export(e)
    finally:
        os.chmod(path, 0o644)
    msg = str(info.value)
    assert "the reader failed: PermissionError (permission) at reader line" in msg and "(exit 3)" in msg
    assert path not in msg and "Errno" not in msg
    assert P.leftover_readers(e["kube"], e["record"]) == []


def test_read_access_modes():
    sc, caps = P.reader_security({"read_access": "nonroot", "supplemental_groups": [1000]})
    assert (sc["runAsNonRoot"], sc["runAsUser"], sc["supplementalGroups"], caps) == (True, 65534, [1000],
                                                                                   {"drop": ["ALL"]})
    assert "fsGroup" not in sc
    sc, caps = P.reader_security({"read_access": "dac-read-search"})
    assert (sc["runAsNonRoot"], sc["runAsUser"]) == (False, 0)
    assert caps == {"drop": ["ALL"], "add": ["DAC_READ_SEARCH"]}


def test_dac_read_search_pod_keeps_every_other_restriction(env):
    e = env
    doc = json.load(open(e["path"]))
    doc["populate"]["reader"]["read_access"] = "dac-read-search"
    write(e["path"], json.dumps(doc))
    record = X.load_record(e["path"], ("populate", "archive"))
    binding = P.claim_binding(e["kube"], "ml-models-pvc", UIDS["ml-models-pvc"])
    spec = P.reader_pod(record, P.section(record), "ml-models-pvc", binding, "n1")["spec"]
    c = spec["containers"][0]
    assert spec["automountServiceAccountToken"] is False and c["securityContext"]["readOnlyRootFilesystem"] is True
    assert c["securityContext"]["allowPrivilegeEscalation"] is False
    assert c["securityContext"]["capabilities"] == {"drop": ["ALL"], "add": ["DAC_READ_SEARCH"]}
    assert spec["volumes"][0]["persistentVolumeClaim"]["readOnly"] is True
    assert P.probe_plan(record, e["kube"])["reader_security"]["capabilities"]["add"] == ["DAC_READ_SEARCH"]
    doc["populate"]["reader"]["read_access"] = "root"
    write(e["path"], json.dumps(doc))
    with pytest.raises(X.StagingError, match="read_access must be one of"):
        P.section(X.load_record(e["path"], ("populate", "archive")))



@pytest.mark.parametrize("change", [
    {"mode": "export", "selection": []},                  # a probe turned into an export
    {"mount": "/"},                                       # another mount
    {"selection": [{"path": "other.json", "kind": "file", "bytes": 1, "sha256": "0" * 64}]},
])
def test_a_job_other_than_the_reviewed_one_is_refused(env, change):
    """Codex #35: the job is bound by its own digest on the command line; an altered job never runs (no tar)."""
    reviewed_job = {"mode": "probe", "mount": env["vols"]["load-evidence-pvc"],
                    "rules": P.effective_rules(P.section(env["record"]), "load-evidence-pvc"), "selection": []}
    argv = P.exec_argv("c28-s0-load-evidence-x", reviewed_job)
    tail = argv[argv.index("--") + 1:]
    altered = dict(reviewed_job, **change)
    res = subprocess.run([sys.executable] + tail[1:], input=P.exec_stdin(altered), capture_output=True, timeout=30)
    assert res.returncode == 3 and b"ustar" not in res.stdout
    assert P.exec_failure(res.returncode, res.stdout.decode(), "") == \
        "the reader failed: DigestMismatch (integrity) at reader line 0 (exit 3)"
    ok = subprocess.run([sys.executable] + tail[1:], input=P.exec_stdin(reviewed_job), capture_output=True, timeout=30)
    assert ok.returncode == 0 and json.loads(ok.stdout)["rules"]


@pytest.mark.parametrize("err", [
    {"class": "EVIL<marker>", "line": 5, "category": "permission"},
    {"class": "PermissionError", "line": 5, "category": "see /secret/path"},
    {"class": "PermissionError", "line": 10 ** 9, "category": "permission"},
    {"class": "PermissionError", "line": True, "category": "permission"},
    {"class": "PermissionError", "line": 5, "category": "permission", "detail": "x"},
])
def test_forged_reader_errors_are_never_echoed(err):
    """Codex #35: only allowlisted classes and categories and a bounded integer line are reported."""
    msg = P.exec_failure(1, "", json.dumps({"reader_error": err}))
    assert msg == "the reader failed: unclassified error (exit 1)"
    good = P.exec_failure(3, json.dumps({"reader_error": {"class": "PermissionError", "line": 78,
                                                          "category": "permission"}}), "")
    assert good == "the reader failed: PermissionError (permission) at reader line 78 (exit 3)"



def test_every_writer_record_type_at_0472829_is_timestamped(env):
    """The operator's forecast log types (decision, issuance, sanity rejection, ledger session, attempt events) and
    interleaving within the tolerance (an issuance just after a later decision) still give a clean prefix."""
    e = env
    recs = [{"event": "forecast_ledger_session", "at": "2026-10-05T06:40:00Z", "operator_run_id": "r1"},
            {"event": "forecast_attempt_started", "at": "2026-11-01T23:55:00Z"},
            {"issued_at": "2026-11-01T23:55:01Z", "forecasts": []},
            {"event": "forecast_attempt_completed", "at": "2026-11-01T23:55:02Z"},
            {"event": "decision", "at": "2026-11-02T00:00:20Z"},
            {"issued_at": "2026-11-01T23:59:58Z", "forecasts": []},          # interleaved: 22 s out of order
            {"event": "sanity_rejected", "issued_at": "2026-11-01T23:59:58Z", "rejected_at": "2026-11-01T23:59:59Z"},
            {"event": "decision", "at": "2026-11-02T00:05:00Z"}]
    write(os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl"), jl(*recs))
    do_probe(e)
    assert any("follows a record more than 0 s after it" in x                  # strict by default
               for x in P.plan(e["record"], e["kube"])["problems"])
    set_forecast_tolerance(e, 60, "reviewed: the operator's issuance and decision writers interleave by seconds")
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    it = p["pvcs"]["forecast-log-pvc"]["selection"][0]
    assert (it["lines"], it["overshoot_lines"], it["lines_after_cutoff"]) == (7, 1, 1)
    assert "not campaign records" in it["overshoot_note"]
    assert not any("forecast-log-pvc" in x for x in p["problems"])


def set_forecast_tolerance(e, seconds, review):
    doc = json.load(open(e["path"]))
    rule = copy.deepcopy(P.DEFAULT_RULES["forecast-log-pvc"][0])
    rule.update(order_tolerance_seconds=seconds, order_tolerance_review=review)
    doc["populate"]["pvcs"]["forecast-log-pvc"]["rules"] = [rule]
    write(e["path"], json.dumps(doc))
    e["record"] = X.load_record(e["path"], ("populate", "archive"))


@pytest.mark.parametrize("rec", [
    {"header": "SECRET-VALUE-123", "event": "x y z!"},
    {"private_marker_abc": 1, "event": "pm-marker-xyz"},                  # short identifier-like names and events
    {"at": "2026-11-01T00:00:00Z", "event": {"pm-marker-xyz": 1}},
    ["pm-marker-xyz"],
])
def test_shape_reports_never_carry_values(env, rec):
    """Codex #36 item 1: only allowlisted (0472829) key names and known event names are printed; anything else is a
    count, a JSON type or a digest."""
    e = env
    write(os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl"), json.dumps(rec) + "\n")
    do_probe(e)
    text = "\n".join(P.plan(e["record"], e["kube"])["problems"])
    assert "forecasts.jsonl at line 1" in text
    for leaked in ("SECRET-VALUE-123", "header", "x y z!", "private_marker_abc", "pm-marker-xyz"):
        assert leaked not in text, text


def test_order_tolerance_is_strict_unless_reviewed_and_recorded(env):
    """Codex #36 item 4: no implicit tolerance; a tolerance needs a per-rule review, which the plan and manifest
    state, and the overhang stays distinct from campaign membership."""
    e = env
    recs = [{"event": "decision", "at": "2026-11-01T23:59:00Z"}, {"event": "decision", "at": "2026-11-02T00:00:30Z"},
            {"event": "decision", "at": "2026-11-01T23:59:50Z"}]
    write(os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl"), jl(*recs))
    do_probe(e)
    assert any("not append-ordered" in x for x in P.plan(e["record"], e["kube"])["problems"])
    doc = json.load(open(e["path"]))
    rule = dict(P.DEFAULT_RULES["forecast-log-pvc"][0], order_tolerance_seconds=60)
    doc["populate"]["pvcs"]["forecast-log-pvc"]["rules"] = [rule]
    write(e["path"], json.dumps(doc))
    with pytest.raises(X.StagingError, match="needs an order_tolerance_review"):
        P.effective_rules(P.section(X.load_record(e["path"], ("populate", "archive"))), "forecast-log-pvc")
    set_forecast_tolerance(e, 60, "reviewed 2026-11-08: interleaved writers")
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    tol = p["pvcs"]["forecast-log-pvc"]["order_tolerances"]
    assert tol == [{"path": "forecasts.jsonl", "seconds": 60, "review": "reviewed 2026-11-08: interleaved writers",
                    "overhang": "lines after the cutoff inside the prefix; not campaign records"}]
    it = p["pvcs"]["forecast-log-pvc"]["selection"][0]
    assert (it["lines"], it["overshoot_lines"]) == (3, 1) and "not campaign records" in it["overshoot_note"]
    do_export(e)
    root = e["record"].doc["archive"]["evidence"]["export_dir"]
    man = json.load(open(os.path.join(root, "pvc-exports/forecast-log-pvc/manifest.json")))
    assert man["order_tolerances"] == tol


def add_training(e, job, uid, collected, started, finished):
    a = e["vols"]["evidence-archive-pvc"]
    log = "log of %s\n" % job
    rec = {"collected_at": collected, "job": job, "pod_uid": uid,
           "containers": [{"name": "trainer", "started_at": started, "finished_at": finished,
                           "log_sha256": sha(log.encode()), "previous_log_saved": False}]}
    if started is None:
        del rec["containers"][0]["started_at"]
    with open(os.path.join(a, "logs/trainer-index.jsonl"), "a") as fh:
        fh.write(jl(rec))
    base = "logs/trainer/%s-%s" % (job, uid[:8])
    write(os.path.join(a, base + ".json"), json.dumps(rec))
    write(os.path.join(a, base + ".trainer.log"), log)
    return base


def set_trainer_reviewed(e, reviewed):
    doc = json.load(open(e["path"]))
    rules = copy.deepcopy(P.DEFAULT_RULES["evidence-archive-pvc"])
    rules[-1]["reviewed"] = reviewed
    doc["populate"]["pvcs"]["evidence-archive-pvc"]["rules"] = rules
    write(e["path"], json.dumps(doc))
    e["record"] = X.load_record(e["path"], ("populate", "archive"))


def test_trainings_are_classified_by_execution_not_collection(env):
    """Codex #36 item 3: a training that RAN in the campaign but was collected after it is selected; one that ran
    after the cutoff is excluded by the index pairing; a straddling or untimed one needs an explicit review."""
    e = env
    post = add_training(e, "ml-training-29900000", "9f9f9f9f-0000", "2026-11-02T06:35:00Z",
                        "2026-11-02T06:00:00Z", "2026-11-02T06:20:00Z")
    late = add_training(e, "ml-training-29800000", "8e8e8e8e-0000", "2026-11-03T06:35:00Z",   # collected late
                        "2026-11-01T18:00:00Z", "2026-11-01T18:20:00Z")
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert p["problems"] == [], p["problems"]
    sel = {it["path"]: it for it in p["pvcs"]["evidence-archive-pvc"]["selection"]}
    assert sel[late + ".trainer.log"]["collected_after_cutoff"] is True
    assert sel[late + ".trainer.log"]["finished_at"] == "2026-11-01T18:20:00Z" and late + ".json" in sel
    ex = {x["path"]: x["reason"] for x in p["pvcs"]["evidence-archive-pvc"]["rule_excluded"]}
    assert ex[post + ".json"] == ex[post + ".trainer.log"] == "post-campaign training (executed after the cutoff)"
    straddle = add_training(e, "ml-training-29850000", "85858585-0000", "2026-11-03T07:00:00Z",
                            "2026-11-01T23:55:00Z", "2026-11-02T00:15:00Z")
    untimed = add_training(e, "ml-training-29860000", "86868686-0000", "2026-11-03T07:01:00Z", None,
                           "2026-11-02T00:16:00Z")
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any("29850000" in x and "straddles the cutoff; needs an explicit review" in x for x in probs), probs
    assert any("29860000" in x and "execution times are missing; needs an explicit review" in x for x in probs)
    set_trainer_reviewed(e, [
        {"job": "ml-training-29850000", "pod_uid": "85858585-0000", "decision": "select", "reason": "slot 18Z ran"},
        {"job": "ml-training-29860000", "pod_uid": "86868686-0000", "decision": "exclude", "reason": "post 00Z"}])
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert p["problems"] == [], p["problems"]
    sel = {it["path"]: it for it in p["pvcs"]["evidence-archive-pvc"]["selection"]}
    assert sel[straddle + ".trainer.log"]["reviewed"] == "reviewed: slot 18Z ran"
    ex = {x["path"]: x["reason"] for x in p["pvcs"]["evidence-archive-pvc"]["rule_excluded"]}
    assert ex[untimed + ".json"] == "reviewed: post 00Z"
    set_trainer_reviewed(e, [
        {"job": "ml-training-29800000", "pod_uid": "8e8e8e8e-0000", "decision": "exclude", "reason": "no"}])
    do_probe(e)
    assert any("29800000" in x and "unambiguous" in x for x in P.plan(e["record"], e["kube"])["problems"])


def test_previous_logs_are_classified_by_their_own_execution(env):
    """Codex #36 r2 item 2: a restarted trainer whose CURRENT instance ran after the cutoff does not justify excluding
    its .previous.log (whose times the writer never records): it blocks until reviewed; a previous log of an instance
    that started at or before the cutoff is campaign evidence."""
    e = env
    a = e["vols"]["evidence-archive-pvc"]
    log, prev = "current\n", "campaign-era instance\n"
    rec = {"collected_at": "2026-11-03T06:35:00Z", "job": "ml-training-29700000", "pod_uid": "7a7a7a7a-0000",
           "containers": [{"name": "trainer", "started_at": "2026-11-02T03:00:00Z",
                           "finished_at": "2026-11-02T03:20:00Z", "restart_count": 1,
                           "log_sha256": sha(log.encode()), "previous_log_saved": True}]}
    with open(os.path.join(a, "logs/trainer-index.jsonl"), "a") as fh:
        fh.write(jl(rec))
    base = "logs/trainer/ml-training-29700000-7a7a7a7a"
    write(os.path.join(a, base + ".json"), json.dumps(rec))
    write(os.path.join(a, base + ".trainer.log"), log)
    write(os.path.join(a, base + ".trainer.previous.log"), prev)
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert any("29700000" in x and "trainer.previous.log: its previous instance's execution times are not recorded"
               in x for x in p["problems"]), p["problems"]
    assert base + ".trainer.previous.log" not in {x["path"] for x in p["pvcs"]["evidence-archive-pvc"]["rule_excluded"]}
    set_trainer_reviewed(e, [{"job": "ml-training-29700000", "pod_uid": "7a7a7a7a-0000", "decision": "select",
                              "reason": "the restart followed a campaign-era attempt"}])
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert p["problems"] == [], p["problems"]
    sel = {it["path"]: it for it in p["pvcs"]["evidence-archive-pvc"]["selection"]}
    ex = {x["path"]: x["reason"] for x in p["pvcs"]["evidence-archive-pvc"]["rule_excluded"]}
    assert sel[base + ".trainer.previous.log"]["reviewed"].startswith("reviewed: the restart")
    assert base + ".json" in sel and ex[base + ".trainer.log"] == "post-campaign training (executed after the cutoff)"
    # an instance that started before the cutoff: its previous instance ended before that, so it is selected
    rec2 = dict(rec, job="ml-training-29710000", pod_uid="7b7b7b7b-0000", collected_at="2026-11-03T06:40:00Z",
                containers=[dict(rec["containers"][0], started_at="2026-11-01T23:50:00Z",
                                 finished_at="2026-11-01T23:59:00Z")])
    with open(os.path.join(a, "logs/trainer-index.jsonl"), "a") as fh:
        fh.write(jl(rec2))
    base2 = "logs/trainer/ml-training-29710000-7b7b7b7b"
    write(os.path.join(a, base2 + ".json"), json.dumps(rec2))
    write(os.path.join(a, base2 + ".trainer.log"), log)
    write(os.path.join(a, base2 + ".trainer.previous.log"), prev)
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert p["problems"] == [], p["problems"]
    sel = {it["path"] for it in p["pvcs"]["evidence-archive-pvc"]["selection"]}
    assert {base2 + ".json", base2 + ".trainer.log", base2 + ".trainer.previous.log"} <= sel


def test_post_campaign_trainings_are_excluded_by_the_index_pairing(env):
    """Files of trainings that ran AFTER the cutoff are excluded with a reason; a file no index record references
    still blocks."""
    e = env
    a = e["vols"]["evidence-archive-pvc"]
    base = add_training(e, "ml-training-29900000", "9f9f9f9f-0000", "2026-11-03T06:35:00Z",
                        "2026-11-03T06:00:00Z", "2026-11-03T06:20:00Z")
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert not any("29900000" in x for x in p["problems"]), p["problems"]
    ex = {x["path"]: x["reason"] for x in p["pvcs"]["evidence-archive-pvc"]["rule_excluded"]}
    assert ex[base + ".json"] == ex[base + ".trainer.log"] == "post-campaign training (executed after the cutoff)"
    write(os.path.join(a, "logs/trainer/ml-training-29911111-aaaaaaaa.json"), "{}")    # referenced by no record
    do_probe(e)
    assert any("ml-training-29911111-aaaaaaaa.json is neither selected" in x
               for x in P.plan(e["record"], e["kube"])["problems"])


def models_after_cutoff_only(e):
    m = e["vols"]["ml-models-pvc"]
    for f in os.listdir(m):
        os.unlink(os.path.join(m, f))
    art = b"current-model"
    write(os.path.join(m, "lstm_nginx-test_requests.pkl"), art)
    write(os.path.join(m, "lstm_nginx-test_requests.meta.json"),
          json.dumps({"trained_at": "2026-11-08T00:20:00Z", "artifact_sha256": sha(art)}))


CAMPAIGN_ART = sha(b"pickled-model-v1")


def set_empty_by_design(e, pvc="ml-models-pvc", reason="the mutable model directory holds only post-campaign models",
                        inventory=(CAMPAIGN_ART,)):
    doc = json.load(open(e["path"]))
    doc["populate"]["pvcs"][pvc]["empty_by_design"] = reason
    if pvc == "ml-models-pvc":
        doc["populate"]["pvcs"][pvc]["campaign_artifacts"] = [
            {"artifact_sha256": d, "reason": "issued by the campaign / frozen identity"} for d in inventory]
    write(e["path"], json.dumps(doc))
    e["record"] = X.load_record(e["path"], ("populate", "archive"))


def test_ml_models_may_be_empty_by_design_when_every_pair_is_post_cutoff(env):
    e = env
    models_after_cutoff_only(e)
    do_probe(e)
    assert any("ml-models-pvc: rule pairs" in x and "selects nothing" in x
               for x in P.plan(e["record"], e["kube"])["problems"])          # not declared: fail closed
    set_empty_by_design(e)
    out = do_export(e)
    assert out["event"] == "export-complete" and P.verify(e["record"])["result"] == "PASS"
    root = e["record"].doc["archive"]["evidence"]["export_dir"]
    man = json.load(open(os.path.join(root, "pvc-exports/ml-models-pvc/manifest.json")))
    assert man["files"] == [] and man["empty_by_design"]["current_pairs_trained_after_cutoff"][0]["trained_at"] > CUTOFF
    with tarfile.open(os.path.join(root, "pvc-exports/ml-models-pvc/campaign-export.tar")) as t:
        assert t.getmembers() == []
    cov = json.load(open(os.path.join(root, "storage/pvc-retention-and-export-coverage.json")))
    mc = next(c for c in cov["claims"] if c["claim"] == "ml-models-pvc")
    assert mc["protected"]["empty_by_design"] is True and mc["protected"]["reason"].startswith("the mutable model directory")


def test_empty_by_design_refuses_anything_campaign_relevant(env):
    e = env
    set_empty_by_design(e)                         # the default fixture holds a pre-cutoff pair and an inconsistent one
    do_probe(e)
    probs = "\n".join(P.plan(e["record"], e["kube"])["problems"])
    assert "declared empty by design but the rules select 2 files" in probs
    m = e["vols"]["ml-models-pvc"]
    models_after_cutoff_only(e)
    write(os.path.join(m, "lstm_x_requests.pkl"), b"half-written")
    write(os.path.join(m, "lstm_x_requests.meta.json"), json.dumps({"trained_at": "2026-11-01T00:00:00Z",
                                                                     "artifact_sha256": "0" * 64}))
    do_probe(e)
    assert any("does not match its sidecar" in x and "declared empty by design" in x
               for x in P.plan(e["record"], e["kube"])["problems"])


def test_empty_by_design_needs_every_pair_verified_before_its_time(env):
    """Codex #36 item 2: a post-cutoff sidecar whose artifact does not match (sha256 or size) blocks the empty export
    instead of justifying it."""
    e = env
    models_after_cutoff_only(e)
    m = e["vols"]["ml-models-pvc"]
    write(os.path.join(m, "lstm_nginx-test_requests.pkl"), b"rewritten-model")       # sidecar sha now mismatches
    set_empty_by_design(e)
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any("does not match its sidecar" in x and "declared empty by design" in x for x in probs), probs
    art = b"current-model"
    write(os.path.join(m, "lstm_nginx-test_requests.pkl"), art)
    write(os.path.join(m, "lstm_nginx-test_requests.meta.json"),
          json.dumps({"trained_at": "2026-11-08T00:20:00Z", "artifact_sha256": sha(art), "artifact_bytes": 999}))
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any("does not match its sidecar" in x for x in probs), probs
    with pytest.raises(Exception):
        do_export(e)
    forged = [{"path": "lstm_a.meta.json", "reason": "trained after the cutoff", "ts": "2026-11-08T00:00:00Z"}]
    _, eprobs = P.empty_by_design("ml-models-pvc", "r", [], forged, [], [{"cutoff": CUTOFF}])
    assert eprobs                                                     # an exclusion without pair_verified never counts


def test_empty_by_design_resolves_the_reviewed_campaign_artifacts(env):
    """Codex #36 r2 item 1: the reviewed campaign artifact inventory must cover every artifact the campaign issuances
    reference, each digest must resolve to verified evidence-archive members, and the manifest and coverage file
    carry those references plus the current pairs' hashes and byte counts."""
    e = env
    models_after_cutoff_only(e)
    absent = "f" * 64
    fl = os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl")
    write(fl, jl({"issued_at": "2026-11-01T23:00:00Z", "forecasts": [], "model_name": "lstm_nginx-test_requests", "artifact_sha256": absent},
                 {"issued_at": "2026-11-01T23:59:00Z", "forecasts": [], "model_name": "lstm_nginx-test_requests", "artifact_sha256": CAMPAIGN_ART}))
    set_empty_by_design(e)
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any(f"references artifact {absent} outside the reviewed campaign artifact inventory" in x for x in probs)
    with pytest.raises(Exception):
        do_export(e)
    set_empty_by_design(e, inventory=(CAMPAIGN_ART, absent))
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any(f"campaign artifact {absent} does not resolve to a verified evidence-archive" in x for x in probs)
    set_empty_by_design(e, inventory=())
    with pytest.raises(X.StagingError, match="reviewed campaign artifact inventory"):
        P.section(e["record"])
    # the fixed state: only the archived campaign artifact is referenced
    write(fl, jl({"issued_at": "2026-11-01T23:59:00Z", "forecasts": [], "model_name": "lstm_nginx-test_requests", "artifact_sha256": CAMPAIGN_ART}))
    set_empty_by_design(e)
    do_probe(e)
    out = do_export(e)
    assert out["event"] == "export-complete" and P.verify(e["record"])["result"] == "PASS"
    root = e["record"].doc["archive"]["evidence"]["export_dir"]
    mpath = os.path.join(root, "pvc-exports/ml-models-pvc/manifest.json")
    man = json.load(open(mpath))
    ref, = man["empty_by_design"]["campaign_artifacts"]
    ea = json.load(open(os.path.join(root, "pvc-exports/evidence-archive-pvc/manifest.json")))
    members = {f["member"]: f for f in ea["files"]}
    assert ref["artifact_sha256"] == CAMPAIGN_ART and members[ref["artifact_member"]]["sha256"] == CAMPAIGN_ART
    assert ref["artifact_member"].endswith(".pkl") and members[ref["sidecar_member"]]["sha256"] == ref["sidecar_sha256"]
    assert ref["export"]["tar_sha256"] == ea["tar"]["sha256"] and ref["referenced_by"] == ["forecast log",
                                                                                           "training bundle"]
    pair, = man["empty_by_design"]["current_pairs_trained_after_cutoff"]
    assert pair["artifact_sha256"] == sha(b"current-model") and pair["artifact_bytes"] == len(b"current-model")
    assert re.match(r"^[0-9a-f]{64}$", pair["sidecar_sha256"]) and pair["sidecar_bytes"] > 0
    cov = json.load(open(os.path.join(root, "storage/pvc-retention-and-export-coverage.json")))
    mc = next(c for c in cov["claims"] if c["claim"] == "ml-models-pvc")["protected"]
    assert mc["campaign_artifacts"] == man["empty_by_design"]["campaign_artifacts"]
    assert mc["manifest_sha256"] == sha(open(mpath, "rb").read()) and mc["files"] == 0
    # a reference that no longer resolves in the exported evidence archive fails verify
    os.chmod(mpath, 0o640)
    write(mpath, json.dumps(dict(man, empty_by_design=dict(man["empty_by_design"], campaign_artifacts=[
        dict(ref, artifact_member="archive/nope/lstm.pkl")]))))
    assert any("artifact_member is not a verified member" in x for x in P.verify(e["record"])["problems"])


def test_empty_by_design_only_for_ml_models(env):
    e = env
    doc = json.load(open(e["path"]))
    doc["populate"]["pvcs"]["load-evidence-pvc"]["empty_by_design"] = "no"
    write(e["path"], json.dumps(doc))
    with pytest.raises(X.StagingError, match="allowed only for ml-models-pvc"):
        P.section(X.load_record(e["path"], ("populate", "archive")))


def reader_ns():
    ns = {"__name__": "reader_under_test"}
    exec(compile(P.READER.replace("sys.exit(run())", ""), "reader", "exec"), ns)
    return ns


@pytest.mark.parametrize("text, want", [
    ("2026-10-05T06:41:02Z", "2026-10-05T06:41:02Z"),
    ("2026-10-05T06:41:02.5Z", "2026-10-05T06:41:02.5Z"),
    ("2026-10-05T06:41:02.123456789Z", "2026-10-05T06:41:02.123456789Z"),        # Go RFC3339Nano, kept exactly
    ("2026-10-05T06:41:02.123456000Z", "2026-10-05T06:41:02.123456Z"),
    ("2026-10-05T08:41:02.1+02:00", "2026-10-05T06:41:02.1Z"),
    ("2026-10-05T01:41:02-05:00", "2026-10-05T06:41:02Z"),
    ("2026-11-01T23:59:59.999999999Z", "2026-11-01T23:59:59.999999999Z"),
    ("2026-11-02T01:00:00.000000001+01:00", "2026-11-02T00:00:00.000000001Z"),
])
def test_reader_parses_go_rfc3339nano(text, want):
    """The probe's forecast-log line 1 (forecast_ledger_session, Go time.RFC3339Nano) failed the old parser; the
    reader keeps integer nanoseconds and serialises the full fraction; the host parser agrees exactly."""
    r = reader_ns()
    n = r["utc"](text)
    assert r["iso"](n) == want and P.ts_ns(text) == n and P.ts_ns(want) == n


@pytest.mark.parametrize("text", ["2026-10-05T06:41:02", "2026-10-05 06:41:02Z", "2026-10-05T06:41:02.Z",
                                  "2026-10-05T06:41:02.1234567890Z", "2026-13-05T06:41:02Z", "2026-10-05T06:41:02+24:00",
                                  "2026-10-05T06:41:02z", "1759646462", "", None, 1759646462])
def test_reader_refuses_unparseable_times(text):
    assert reader_ns()["utc"](text) is None and P.ts_ns(text) is None


def test_nanosecond_times_never_round_into_the_cutoff(env):
    """A record a nanosecond after the cutoff is after it; Go-format records before it are selected."""
    e = env
    recs = [{"event": "forecast_ledger_session", "at": "2026-10-05T06:41:02.123456789Z", "event_seq": 1,
             "operator_run_id": "r1", "schema": "forecast-attempt-v1"},
            {"issued_at": "2026-11-01T23:59:59.999999999+00:00", "forecasts": []},
            {"event": "decision", "at": "2026-11-02T00:00:00.000000001Z"}]
    write(os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl"), jl(*recs))
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert not any("forecast-log-pvc" in x for x in p["problems"]), p["problems"]
    it = p["pvcs"]["forecast-log-pvc"]["selection"][0]
    assert (it["lines"], it["lines_after_cutoff"]) == (2, 1)


@pytest.mark.parametrize("trained_at, after", [
    ("2026-11-02T00:00:00.000000001Z", True),
    ("2026-11-02T01:00:00.000000001+01:00", True),
    ("2026-11-01T19:00:00.000000001-05:00", True),
    ("2026-11-02T00:00:00Z", False),
    ("2026-11-02T00:00:00.000000000Z", False),
    ("2026-11-02T01:00:00+01:00", False),
])
def test_cutoff_boundary_keeps_nanoseconds_end_to_end(env, trained_at, after):
    """Codex #37: a verified model trained 1 ns after the cutoff stays after it through serialisation and the
    host-side empty-by-design check (Z and offset forms); one trained exactly at the cutoff is campaign material."""
    e = env
    models_after_cutoff_only(e)
    art = b"current-model"
    write(os.path.join(e["vols"]["ml-models-pvc"], "lstm_nginx-test_requests.meta.json"),
          json.dumps({"trained_at": trained_at, "artifact_sha256": sha(art)}))
    set_empty_by_design(e)
    do_probe(e)
    if after:
        out = do_export(e)
        assert out["event"] == "export-complete" and P.verify(e["record"])["result"] == "PASS"
        root = e["record"].doc["archive"]["evidence"]["export_dir"]
        man = json.load(open(os.path.join(root, "pvc-exports/ml-models-pvc/manifest.json")))
        pair, = man["empty_by_design"]["current_pairs_trained_after_cutoff"]
        assert pair["trained_at"] == "2026-11-02T00:00:00.000000001Z"
        assert P.ts_ns(pair["trained_at"]) == P.ts_ns(CUTOFF) + 1
    else:
        probs = P.plan(e["record"], e["kube"])["problems"]
        assert any("declared empty by design but the rules select 2 files" in x for x in probs), probs


def ensemble_log(e, extra=()):
    fp1, fp2 = "e1" * 32, "e2" * 32
    recs = [{"issued_at": "2026-11-01T18:06:00Z", "forecasts": [], "model_name": "ensemble_nginx-ensemble_requests",
             "model_version": "seasonal-ensemble-q90-v1:891ca@" + fp1[:12], "artifact_sha256": fp1},
            {"event": "decision", "at": "2026-11-01T18:07:00Z", "artifact_sha256": fp1, "model_version": "x"},
            {"issued_at": "2026-11-01T23:59:00Z", "forecasts": [], "model_name": "lstm_nginx-test_requests",
             "artifact_sha256": CAMPAIGN_ART},
            {"event": "decision", "at": "2026-11-01T23:59:30Z", "artifact_sha256": CAMPAIGN_ART},
            {"issued_at": "2026-11-01T23:59:40Z", "forecasts": [], "model_name": "ensemble_nginx-ensemble-q95_requests",
             "artifact_sha256": fp2}] + list(extra)
    write(os.path.join(e["vols"]["forecast-log-pvc"], "forecasts.jsonl"), jl(*recs))
    return fp1, fp2


def test_ensemble_generation_fingerprints_are_not_campaign_artifacts(env):
    """The 2026-11-08 plan's 103 problems: ensemble_* issuances carry a generation data fingerprint as artifact_sha256
    (no model file). They are classified by model_name and listed, not resolved; lstm_* digests (persisted
    artifacts) are still checked like with like, and a missing one still blocks."""
    e = env
    models_after_cutoff_only(e)
    fp1, fp2 = ensemble_log(e)
    set_empty_by_design(e)
    do_probe(e)
    p = P.plan(e["record"], e["kube"])
    assert p["problems"] == [], p["problems"]
    it = p["pvcs"]["forecast-log-pvc"]["selection"][0]
    assert it["referenced_artifact_sha256"] == [CAMPAIGN_ART]
    assert it["referenced_generation_fingerprints"] == sorted([fp1, fp2])
    gf = p["pvcs"]["ml-models-pvc"]["empty_by_design"]["generation_fingerprints_not_artifacts"]
    assert gf["count"] == 2 and gf["sha256"] == sorted([fp1, fp2])
    out = do_export(e)
    assert out["event"] == "export-complete" and P.verify(e["record"])["result"] == "PASS"
    root = e["record"].doc["archive"]["evidence"]["export_dir"]
    cov = json.load(open(os.path.join(root, "storage/pvc-retention-and-export-coverage.json")))
    mc = next(c for c in cov["claims"] if c["claim"] == "ml-models-pvc")["protected"]
    assert mc["generation_fingerprints_not_artifacts"]["sha256"] == sorted([fp1, fp2])


def test_a_missing_lstm_artifact_still_blocks_beside_ensembles(env):
    e = env
    models_after_cutoff_only(e)
    absent = "a" * 64
    ensemble_log(e, [{"issued_at": "2026-11-01T23:59:50Z", "forecasts": [], "model_name": "lstm_nginx-seasonal_requests",
                      "artifact_sha256": absent}])
    set_empty_by_design(e)
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert [x for x in probs if "references artifact" in x] == [
        f"ml-models-pvc: the campaign forecast log references artifact {absent} outside the reviewed campaign "
        "artifact inventory"], probs


@pytest.mark.parametrize("extra, why", [
    ({"issued_at": "2026-11-01T23:59:50Z", "forecasts": [], "model_name": "pm-marker-xyz_model",
      "artifact_sha256": "b" * 64}, "carries a artifact_sha256 but its model_name is of no declared class"),
    ({"event": "decision", "at": "2026-11-01T23:59:50Z", "artifact_sha256": "c" * 64},
     "1 artifact_sha256 values of forecasts.jsonl records without model_name match no classified record"),
])
def test_unclassified_forecast_digests_fail_closed(env, extra, why):
    e = env
    ensemble_log(e, [extra])
    do_probe(e)
    probs = P.plan(e["record"], e["kube"])["problems"]
    assert any(why in x for x in probs), probs
    assert not any("pm-marker-xyz" in x or "c" * 64 in x for x in probs)


def test_ref_classes_need_disjoint_prefixes(env):
    e = env
    doc = json.load(open(e["path"]))
    rule = copy.deepcopy(P.DEFAULT_RULES["forecast-log-pvc"][0])
    rule["ref_classes"]["fingerprint_prefixes"] = ["lstm_nginx"]
    doc["populate"]["pvcs"]["forecast-log-pvc"]["rules"] = [rule]
    write(e["path"], json.dumps(doc))
    with pytest.raises(X.StagingError, match="disjoint"):
        P.effective_rules(P.section(X.load_record(e["path"], ("populate", "archive"))), "forecast-log-pvc")
