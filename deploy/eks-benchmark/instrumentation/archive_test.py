#!/usr/bin/env python3
"""Tests for archive.py: verified-pair archiving and log preservation.

Every case here is a way the evidence could be wrong without anyone noticing: a sidecar that
describes a different artifact, a pair replaced while it is being copied, a missing sidecar, a
re-run that duplicates the index, a crash between the rename and the index line, a restarted or
replaced API pod whose log lines would silently drop out. Run: python3 archive_test.py
"""
import hashlib
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import archive  # noqa: E402

ART = "lstm_nginx-test_requests.pkl"
SIDE = "lstm_nginx-test_requests.meta.json"


def sha(b):
    return hashlib.sha256(b).hexdigest()


def publish(src, payload, cutoff="2026-09-23T12:00:00Z", meta_sha=None, size=None):
    with open(os.path.join(src, ART), "wb") as fh:
        fh.write(payload)
    meta = {"artifact": ART, "artifact_sha256": meta_sha or sha(payload),
            "artifact_bytes": len(payload) if size is None else size,
            "training_cutoff": cutoff, "trained_at": "2026-09-23T12:01:06Z"}
    with open(os.path.join(src, SIDE), "w") as fh:
        json.dump(meta, fh)
    return meta


def lines(path):
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        return [json.loads(x) for x in fh if x.strip()]


class PairTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.src = os.path.join(self.tmp, "models")
        self.arc = os.path.join(self.tmp, "archive")
        os.makedirs(self.src)
        os.makedirs(self.arc)
        self.ident = {"job": "ml-archive-1", "pod": "ml-archive-1-abc"}

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_pair(self, **kw):
        return archive.archive_pair(self.src, self.arc, ART, SIDE, self.ident, **kw)

    def finals(self):
        d = os.path.join(self.arc, "archive")
        return sorted(os.listdir(d)) if os.path.isdir(d) else []

    def test_good_pair_is_finalized_and_indexed(self):
        payload = b"model-bytes-1" * 1000
        publish(self.src, payload)
        out = self.run_pair()
        self.assertEqual(out["result"], "archived", out)
        key = "20260923T120000Z-" + sha(payload)[:12]
        self.assertEqual(self.finals(), [key])
        with open(os.path.join(self.arc, "archive", key, ART), "rb") as fh:
            self.assertEqual(sha(fh.read()), sha(payload))
        idx = lines(os.path.join(self.arc, "INDEX.jsonl"))
        self.assertEqual(len(idx), 1)
        rec = idx[0]
        self.assertEqual(rec["artifact_sha256"], sha(payload))
        with open(os.path.join(self.src, SIDE), "rb") as fh:
            self.assertEqual(rec["sidecar_sha256"], sha(fh.read()))
        for f in ("copied_at", "training_cutoff", "source_mtime_artifact", "source_mtime_sidecar", "job", "pod", "key"):
            self.assertIn(f, rec)
        self.assertEqual(rec["job"], "ml-archive-1")
        self.assertFalse(os.listdir(os.path.join(self.arc, ".staging")))

    def test_repeated_run_is_idempotent(self):
        publish(self.src, b"same" * 500)
        self.assertEqual(self.run_pair()["result"], "archived")
        out = self.run_pair()
        self.assertEqual(out["result"], "already_archived", out)
        self.assertEqual(len(lines(os.path.join(self.arc, "INDEX.jsonl"))), 1)
        self.assertEqual(len(self.finals()), 1)

    def test_mismatched_pair_is_rejected_never_finalized(self):
        publish(self.src, b"new-artifact" * 100, meta_sha=sha(b"the-previous-artifact"))
        out = self.run_pair()
        self.assertEqual(out["result"], "rejected", out)
        self.assertIn("sidecar", out["reason"])
        self.assertEqual(self.finals(), [])
        self.assertEqual(lines(os.path.join(self.arc, "INDEX.jsonl")), [])
        rej = lines(os.path.join(self.arc, "REJECTS.jsonl"))
        self.assertEqual(len(rej), 1)
        self.assertEqual(rej[0]["staged_artifact_sha256"], sha(b"new-artifact" * 100))
        self.assertFalse(os.listdir(os.path.join(self.arc, ".staging")))

    def test_wrong_size_in_sidecar_is_rejected(self):
        publish(self.src, b"x" * 64, size=65)
        self.assertEqual(self.run_pair()["result"], "rejected")
        self.assertEqual(self.finals(), [])

    def test_pair_changing_mid_copy_is_rejected(self):
        publish(self.src, b"first" * 400)

        def during_copy():
            # the trainer publishes a new pair (artifact rename, then sidecar rename) mid-copy
            tmp = os.path.join(self.src, ART + ".tmp")
            with open(tmp, "wb") as fh:
                fh.write(b"second" * 400)
            os.rename(tmp, os.path.join(self.src, ART))
            publish(self.src, b"second" * 400, cutoff="2026-09-23T18:00:00Z")

        out = self.run_pair(after_copy_hook=during_copy)
        self.assertEqual(out["result"], "rejected", out)
        self.assertIn("changed during copy", out["reason"])
        self.assertEqual(self.finals(), [])
        # the next run sees a stable pair and archives the new one
        self.assertEqual(self.run_pair()["result"], "archived")
        self.assertEqual(self.finals(), ["20260923T180000Z-" + sha(b"second" * 400)[:12]])

    def test_sidecar_only_changing_mid_copy_is_rejected(self):
        publish(self.src, b"stable" * 400)

        def during_copy():
            with open(os.path.join(self.src, SIDE)) as fh:
                m = json.load(fh)
            m["trained_at"] = "later"
            with open(os.path.join(self.src, SIDE), "w") as fh:
                json.dump(m, fh)

        out = self.run_pair(after_copy_hook=during_copy)
        self.assertEqual(out["result"], "rejected", out)
        self.assertIn("changed during copy", out["reason"])

    def test_missing_sidecar_is_rejected(self):
        with open(os.path.join(self.src, ART), "wb") as fh:
            fh.write(b"orphan")
        out = self.run_pair()
        self.assertEqual(out["result"], "rejected", out)
        self.assertIn("missing", out["reason"])
        self.assertEqual(self.finals(), [])
        self.assertEqual(len(lines(os.path.join(self.arc, "REJECTS.jsonl"))), 1)

    def test_missing_artifact_is_rejected(self):
        publish(self.src, b"gone")
        os.unlink(os.path.join(self.src, ART))
        self.assertEqual(self.run_pair()["result"], "rejected")

    def test_unparsable_sidecar_is_rejected(self):
        publish(self.src, b"abc")
        with open(os.path.join(self.src, SIDE), "w") as fh:
            fh.write("{not json")
        out = self.run_pair()
        self.assertEqual(out["result"], "rejected")

    def test_crash_between_rename_and_index_is_recovered_once(self):
        publish(self.src, b"crash" * 300)
        self.run_pair()
        os.unlink(os.path.join(self.arc, "INDEX.jsonl"))
        out = self.run_pair()
        self.assertEqual(out["result"], "index_recovered", out)
        idx = lines(os.path.join(self.arc, "INDEX.jsonl"))
        self.assertEqual(len(idx), 1)
        self.assertTrue(idx[0].get("index_recovered"))
        self.assertEqual(self.run_pair()["result"], "already_archived")
        self.assertEqual(len(lines(os.path.join(self.arc, "INDEX.jsonl"))), 1)

    def test_tampered_archive_is_never_overwritten(self):
        publish(self.src, b"orig" * 300)
        self.run_pair()
        key = self.finals()[0]
        with open(os.path.join(self.arc, "archive", key, ART), "ab") as fh:
            fh.write(b"tamper")
        out = self.run_pair()
        self.assertEqual(out["result"], "rejected", out)
        self.assertIn("conflict", out["reason"])

    def test_sidecar_variant_for_archived_artifact_is_kept_separately(self):
        publish(self.src, b"art" * 300)
        self.run_pair()
        with open(os.path.join(self.src, SIDE)) as fh:
            m = json.load(fh)
        m["role"] = "benchmark-rewritten"
        with open(os.path.join(self.src, SIDE), "w") as fh:
            json.dump(m, fh)
        out = self.run_pair()
        self.assertEqual(out["result"], "sidecar_variant", out)
        idx = lines(os.path.join(self.arc, "INDEX.jsonl"))
        self.assertEqual([r["event"] for r in idx], ["archived", "sidecar_variant"])
        self.assertEqual(self.run_pair()["result"], "already_archived")
        self.assertEqual(len(lines(os.path.join(self.arc, "INDEX.jsonl"))), 2)


class FakeAPI:
    """Stands in for the Kubernetes API: pods, their logs (current and previous) and Jobs."""

    def __init__(self):
        self.pods = []
        self.logs = {}   # (pod, container, previous) -> text with RFC3339 timestamps
        self.jobs = {}
        self.calls = []

    def list_pods(self, selector):
        k, v = selector.split("=")
        return [p for p in self.pods if p["metadata"]["labels"].get(k) == v]

    def pod_log(self, name, container, since_time=None, previous=False):
        self.calls.append((name, container, since_time, previous))
        text = self.logs.get((name, container, previous))
        if text is None:
            raise archive.CollectionError(f"no log for {name}/{container} previous={previous}")
        if since_time:
            keep = [ln for ln in text.splitlines() if archive.parse_ts(ln.split(" ", 1)[0]) >= archive.parse_ts(since_time)]
            return "\n".join(keep) + ("\n" if keep else "")
        return text

    def get_job(self, name):
        return self.jobs[name]


def trainer_pod(job, uid, phase="Succeeded", exit_code=0, restarts=0):
    return {"metadata": {"name": f"{job}-x1", "uid": uid, "labels": {"app": "ml-training", "job-name": job},
                         "ownerReferences": [{"kind": "Job", "name": job, "uid": "job-" + uid}]},
            "spec": {"nodeName": "node-b"},
            "status": {"phase": phase, "containerStatuses": [{
                "name": "trainer", "restartCount": restarts, "imageID": "registry/ml-api@sha256:abc",
                "image": "registry/ml-api:tag",
                "state": ({"terminated": {"exitCode": exit_code, "reason": "Completed",
                                          "startedAt": "2026-09-23T12:00:02Z", "finishedAt": "2026-09-23T12:01:10Z"}}
                          if phase in ("Succeeded", "Failed") else {"running": {"startedAt": "2026-09-23T12:00:02Z"}})}]}}


def api_pod(name, uid, restarts=0, started="2026-09-23T00:00:00Z"):
    return {"metadata": {"name": name, "uid": uid, "labels": {"app": "ml-api"}},
            "spec": {"nodeName": "node-b"},
            "status": {"phase": "Running", "containerStatuses": [{
                "name": "api", "restartCount": restarts, "imageID": "registry/ml-api@sha256:abc",
                "state": {"running": {"startedAt": started}}}]}}


RELOAD = "INFO:api.main:Reloaded model nginx-test_requests from disk (file updated); artifact sha256={}"


class LogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.arc = self.tmp
        self.api = FakeAPI()
        self.ident = {"job": "ml-archive-1", "pod": "ml-archive-1-abc"}
        self.clock = ["2026-09-23T12:05:00Z"]

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def collect(self):
        return archive.collect_logs(self.api, self.arc, "ml-engine", "app=ml-training", "app=ml-api", "api",
                                    self.ident, now=lambda: self.clock[0])

    def test_completed_trainer_log_is_preserved_once_with_identity(self):
        self.api.pods.append(trainer_pod("ml-training-29836080", "u1"))
        self.api.jobs["ml-training-29836080"] = {"metadata": {"uid": "job-u1"},
                                                 "status": {"conditions": [{"type": "Complete", "status": "True"}],
                                                            "completionTime": "2026-09-23T12:01:18Z"}}
        self.api.logs[("ml-training-29836080-x1", "trainer", False)] = (
            "2026-09-23T12:01:07.646Z INFO:__main__:Atomic rename: a -> b\n"
            "2026-09-23T12:01:07.651Z INFO:__main__:Provenance written: m sha256=" + "a" * 64 + "\n")
        self.collect()
        d = os.path.join(self.arc, "logs", "trainer")
        names = sorted(os.listdir(d))
        self.assertEqual(len([n for n in names if n.endswith(".log")]), 1)
        with open(os.path.join(d, [n for n in names if n.endswith(".json")][0])) as fh:
            meta = json.load(fh)
        self.assertEqual(meta["job_uid"], "job-u1")
        self.assertEqual(meta["pod_uid"], "u1")
        self.assertEqual(meta["containers"][0]["exit_code"], 0)
        self.assertEqual(meta["containers"][0]["image_id"], "registry/ml-api@sha256:abc")
        self.assertEqual(meta["job_terminal"], "Complete")
        self.collect()
        self.assertEqual(len([n for n in os.listdir(d) if n.endswith(".log")]), 1)
        self.assertEqual(len(lines(os.path.join(self.arc, "logs", "trainer-index.jsonl"))), 1)

    def test_running_trainer_is_not_collected_yet(self):
        self.api.pods.append(trainer_pod("ml-training-1", "u2", phase="Running"))
        self.collect()
        self.assertFalse(os.path.exists(os.path.join(self.arc, "logs", "trainer")) and
                         os.listdir(os.path.join(self.arc, "logs", "trainer")))

    def test_api_reload_lines_are_appended_without_duplicates(self):
        self.api.pods.append(api_pod("ml-api-1", "a1"))
        self.api.logs[("ml-api-1", "api", False)] = (
            "2026-09-23T06:02:32.46Z " + RELOAD.format("c5af91a788a9") + "\n"
            "2026-09-23T06:03:00Z INFO: GET /predict 200\n")
        self.collect()
        self.clock[0] = "2026-09-23T12:20:00Z"
        self.api.logs[("ml-api-1", "api", False)] += "2026-09-23T12:02:32.81Z " + RELOAD.format("aeea5636b90e") + "\n"
        self.collect()
        rel = lines(os.path.join(self.arc, "logs", "api-reload.jsonl"))
        self.assertEqual([r["line"].rsplit("=", 1)[1] for r in rel], ["c5af91a788a9", "aeea5636b90e"])
        self.assertEqual(rel[0]["pod"], "ml-api-1")
        self.assertEqual(rel[0]["restart_count"], 0)
        cov = lines(os.path.join(self.arc, "logs", "api-coverage.jsonl"))
        self.assertEqual(len(cov), 2)
        self.assertEqual(cov[1]["window_start"], cov[0]["window_end"])
        self.assertEqual(cov[0]["flags"], [])

    def test_single_restart_recovers_the_previous_instance(self):
        self.api.pods.append(api_pod("ml-api-1", "a1"))
        self.api.logs[("ml-api-1", "api", False)] = "2026-09-23T06:02:32Z " + RELOAD.format("c5af") + "\n"
        self.collect()
        self.clock[0] = "2026-09-23T12:20:00Z"
        self.api.pods[0] = api_pod("ml-api-1", "a1", restarts=1, started="2026-09-23T12:10:00Z")
        self.api.logs[("ml-api-1", "api", True)] = (self.api.logs[("ml-api-1", "api", False)] +
                                                    "2026-09-23T12:02:32Z " + RELOAD.format("aeea") + "\n")
        self.api.logs[("ml-api-1", "api", False)] = "2026-09-23T12:10:05Z INFO:api.main:Loaded model x sha256=aeea\n"
        self.collect()
        rel = lines(os.path.join(self.arc, "logs", "api-reload.jsonl"))
        self.assertEqual([(r["instance"], r["line"][-4:]) for r in rel],
                         [("current", "c5af"), ("previous", "aeea"), ("current", "aeea")])
        cov = lines(os.path.join(self.arc, "logs", "api-coverage.jsonl"))
        self.assertTrue(any("restart" in f for f in cov[-1]["flags"]))
        self.assertFalse(any("GAP" in f for f in cov[-1]["flags"]))

    def test_multiple_restarts_between_collections_flag_a_gap(self):
        self.api.pods.append(api_pod("ml-api-1", "a1"))
        self.api.logs[("ml-api-1", "api", False)] = "2026-09-23T06:02:32Z " + RELOAD.format("c5af") + "\n"
        self.collect()
        self.api.pods[0] = api_pod("ml-api-1", "a1", restarts=2, started="2026-09-23T12:10:00Z")
        self.api.logs[("ml-api-1", "api", True)] = "2026-09-23T12:05:00Z x\n"
        self.api.logs[("ml-api-1", "api", False)] = "2026-09-23T12:10:05Z y\n"
        self.clock[0] = "2026-09-23T12:20:00Z"
        self.collect()
        cov = lines(os.path.join(self.arc, "logs", "api-coverage.jsonl"))
        self.assertTrue(any("GAP" in f for f in cov[-1]["flags"]), cov[-1])

    def test_replaced_api_pod_flags_the_lost_tail(self):
        self.api.pods.append(api_pod("ml-api-1", "a1"))
        self.api.logs[("ml-api-1", "api", False)] = "2026-09-23T06:02:32Z " + RELOAD.format("c5af") + "\n"
        self.collect()
        self.api.pods = [api_pod("ml-api-2", "a2", started="2026-09-23T12:15:00Z")]
        self.api.logs[("ml-api-2", "api", False)] = "2026-09-23T12:15:05Z INFO:api.main:Loaded model x sha256=aeea\n"
        self.clock[0] = "2026-09-23T12:20:00Z"
        self.collect()
        cov = lines(os.path.join(self.arc, "logs", "api-coverage.jsonl"))
        gone = [c for c in cov if c["pod"] == "ml-api-1" and any("GAP" in f for f in c["flags"])]
        self.assertEqual(len(gone), 1, cov)
        self.collect()   # the gone pod is reported once, not on every run
        cov = lines(os.path.join(self.arc, "logs", "api-coverage.jsonl"))
        self.assertEqual(len([c for c in cov if c["pod"] == "ml-api-1" and any("GAP" in f for f in c["flags"])]), 1)

    def test_log_collection_failure_is_recorded_not_swallowed(self):
        self.api.pods.append(api_pod("ml-api-1", "a1"))
        out = self.collect()   # no log text registered -> CollectionError
        self.assertTrue(out["errors"])
        cov = lines(os.path.join(self.arc, "logs", "api-coverage.jsonl"))
        self.assertTrue(any("COLLECTION FAILED" in f for f in cov[-1]["flags"]))


class RunTests(unittest.TestCase):
    def test_run_record_and_exit_status(self):
        tmp = tempfile.mkdtemp()
        try:
            src, arc = os.path.join(tmp, "m"), os.path.join(tmp, "a")
            os.makedirs(src)
            os.makedirs(arc)
            publish(src, b"q" * 50)
            api = FakeAPI()
            rc = archive.run(src, arc, ART, SIDE, api, "ml-engine", "app=ml-training", "app=ml-api", "api",
                             {"job": "j", "pod": "p"})
            self.assertEqual(rc, 0)
            runs = lines(os.path.join(arc, "RUNS.jsonl"))
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["pair"]["result"], "archived")
            api.pods.append(api_pod("ml-api-1", "a1"))   # log collection now fails
            rc = archive.run(src, arc, ART, SIDE, api, "ml-engine", "app=ml-training", "app=ml-api", "api",
                             {"job": "j", "pod": "p"})
            self.assertEqual(rc, 1)
        finally:
            shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main(verbosity=1)
