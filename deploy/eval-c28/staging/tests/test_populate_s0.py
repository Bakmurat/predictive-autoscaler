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


def jl(*recs):
    return "".join(json.dumps(r, sort_keys=True) + "\n" for r in recs)


def volumes(root):
    """The four claims as the campaign writers left them (deploy/eks-benchmark/instrumentation/archive.py,
    collect_load_evidence.py, the operator and trainer), plus post-cutoff and live material that must NOT be exported."""
    v = {p: os.path.join(root, "vol", p) for p in MOUNTS}
    fc = [{"event": "decision", "at": "2026-11-01T23:58:00Z"}, {"issued_at": "2026-11-01T23:59:00Z", "forecasts": []},
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
        tail = P.exec_argv(pod)[P.exec_argv(pod).index("--") + 1:]      # exactly what runs in the container
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


@pytest.mark.parametrize("lines, why", [
    (['{"at": "2026-11-01T00:00:00Z"}', '{"at": "2026-11-03T00:00:00Z"}', '{"at": "2026-11-01T01:00:00Z"}'],
     "a record at or before the cutoff after a later one"),
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
    assert rule_ex["lstm_myapp_requests.meta.json"] == "trained after the cutoff"


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
    argv = P.exec_argv("c28-s0-forecast-log-abcdef0123")
    tail = argv[argv.index("--") + 1:]
    query = "&".join("command=" + urllib.parse.quote(a, safe="") for a in tail)
    assert len(query) < 1024 and P.READER not in argv and P.READER_SHA256 in argv
    assert json.loads(P.exec_stdin({"mode": "probe"}))["script"] == P.READER


def test_a_script_other_than_the_reviewed_one_is_refused():
    tail = P.exec_argv("x")[P.exec_argv("x").index("--") + 1:]
    stdin = (json.dumps({"script": P.READER + "\n# edited", "job": {"mode": "probe", "mount": "/"}}) + "\n").encode()
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
