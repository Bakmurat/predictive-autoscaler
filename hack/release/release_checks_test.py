#!/usr/bin/env python3
"""Fixtures for release_checks.py (run by scripts/verify.sh): registry 403/timeout, foreign same-name checks, failed
newest attempts, forks, empty digests, and image defaults masked by values."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import release_checks as rc  # noqa: E402

SHA, REPO = "a" * 40, "Bakmurat/predictive-autoscaler"


def run(suite, path, sha=SHA, repo=REPO, conclusion="success", attempt=1, rid=None):
    return {"id": rid or suite, "check_suite_id": suite, "path": path, "head_sha": sha, "run_attempt": attempt,
            "head_repository": {"full_name": repo}, "status": "completed", "conclusion": conclusion}


def check(name, suite, sha=SHA, slug="github-actions", conclusion="success", started="2026-10-17T00:00:00Z"):
    return {"name": name, "head_sha": sha, "app": {"slug": slug}, "check_suite": {"id": suite},
            "status": "completed", "conclusion": conclusion, "started_at": started}


def green():
    checks, runs, suites = [], [], {}
    for name, path in rc.REQUIRED_CHECKS.items():
        s = suites.setdefault(path, 100 + len(suites))
        checks.append(check(name, s))
    runs = [run(s, p) for p, s in suites.items()]
    return checks, runs


class Lookup(unittest.TestCase):
    def test_404_is_absent(self):
        self.assertEqual(rc.classify_lookup(1, "GET https://ghcr.io/v2/x/manifests/1.0.0: MANIFEST_UNKNOWN: manifest unknown"), "absent")
        self.assertEqual(rc.classify_lookup(1, "NAME_UNKNOWN: repository name not known to registry"), "absent")

    def test_present(self):
        self.assertEqual(rc.classify_lookup(0, ""), "present")

    def test_errors_are_never_absence(self):
        for err in ("UNAUTHORIZED: authentication required", "DENIED: permission_denied", "dial tcp 1.2.3.4:443: i/o timeout",
                    "unexpected status code 503 Service Unavailable: ", "something odd"):
            with self.assertRaises(RuntimeError, msg=err):
                rc.classify_lookup(1, err)

    def test_denied_wins_over_unknown(self):
        with self.assertRaises(RuntimeError):
            rc.classify_lookup(1, "DENIED: requested access to the resource is denied; NAME_UNKNOWN")


class Ci(unittest.TestCase):
    def test_green(self):
        self.assertEqual(rc.check_ci(*green(), SHA, REPO), [])

    def test_missing_check(self):
        checks, runs = green()
        self.assertTrue(rc.check_ci(checks[1:], runs, SHA, REPO))

    def test_foreign_producer_with_the_same_name(self):
        checks, runs = green()
        checks.append(check(list(rc.REQUIRED_CHECKS)[0], 999, slug="some-other-app"))
        self.assertTrue(any("another producer" in p for p in rc.check_ci(checks, runs, SHA, REPO)))

    def test_wrong_workflow_file(self):
        checks, runs = green()
        runs = [dict(r, path=".github/workflows/evil.yml") if r["path"] == ".github/workflows/smoke.yml" else r for r in runs]
        self.assertTrue(any("smoke" in p for p in rc.check_ci(checks, runs, SHA, REPO)))

    def test_fork_run(self):
        checks, runs = green()
        runs = [dict(r, head_repository={"full_name": "someone/fork"}) for r in runs]
        self.assertTrue(rc.check_ci(checks, runs, SHA, REPO))

    def test_another_commit(self):
        checks, runs = green()
        checks = [dict(c, head_sha="b" * 40) for c in checks]
        self.assertTrue(rc.check_ci(checks, runs, SHA, REPO))

    def test_newest_attempt_failed(self):
        checks, runs = green()
        name = list(rc.REQUIRED_CHECKS)[0]
        runs.append(run(500, ".github/workflows/ci.yml", attempt=2, conclusion="failure"))
        checks.append(check(name, 500, conclusion="failure", started="2026-10-17T01:00:00Z"))
        self.assertTrue(any("newest attempt" in p for p in rc.check_ci(checks, runs, SHA, REPO)))

    def test_newest_attempt_succeeded_after_a_failure(self):
        checks, runs = green()
        name = list(rc.REQUIRED_CHECKS)[0]
        old = [c for c in checks if c["name"] == name][0]
        checks.append(dict(old, check_suite={"id": 400}, conclusion="failure", started_at="2026-10-16T23:00:00Z"))
        runs.append(run(400, ".github/workflows/ci.yml", attempt=0, conclusion="failure"))
        self.assertEqual(rc.check_ci(checks, runs, SHA, REPO), [])


def docs(op="ghcr.io/bakmurat/predictive-autoscaler-operator:1.2.3", fc="ghcr.io/bakmurat/predictive-autoscaler-ml-api:1.2.3",
         tr="ghcr.io/bakmurat/predictive-autoscaler-ml-api:1.2.3", trainers=1):
    out = [{"kind": "Deployment", "metadata": {"name": "pa-predictive-autoscaler-operator"},
            "spec": {"template": {"spec": {"containers": [{"image": op}]}}}},
           {"kind": "Deployment", "metadata": {"name": "pa-predictive-autoscaler-forecaster"},
            "spec": {"template": {"spec": {"containers": [{"image": fc}]}}}}]
    for i in range(trainers):
        out.append({"kind": "CronJob", "metadata": {"name": f"pa-predictive-autoscaler-train-{i}"},
                    "spec": {"jobTemplate": {"spec": {"template": {"spec": {"containers": [{"image": tr}]}}}}}})
    return out


class ChartImages(unittest.TestCase):
    P = "ghcr.io/bakmurat"

    def test_release_images(self):
        self.assertEqual(rc.check_chart_images(docs(), "1.2.3", self.P), [])

    def test_wrong_default_tag(self):
        self.assertTrue(rc.check_chart_images(docs(op="ghcr.io/bakmurat/predictive-autoscaler-operator:latest"), "1.2.3", self.P))

    def test_wrong_registry(self):
        self.assertTrue(rc.check_chart_images(docs(fc="docker.io/x/predictive-autoscaler-ml-api:1.2.3"), "1.2.3", self.P))

    def test_trainer_checked_separately(self):
        self.assertTrue(rc.check_chart_images(docs(tr="ghcr.io/bakmurat/predictive-autoscaler-operator:1.2.3"), "1.2.3", self.P))

    def test_no_trainer_rendered(self):
        self.assertTrue(rc.check_chart_images(docs(trainers=0), "1.2.3", self.P))


class Digest(unittest.TestCase):
    def test_digest(self):
        self.assertEqual(rc.check_digest("sha256:" + "0" * 64), [])
        for bad in ("", "sha256:", "sha256:" + "0" * 63, "sha512:" + "0" * 64, None):
            self.assertTrue(rc.check_digest(bad), bad)

    def test_semver(self):
        self.assertEqual(rc.check_semver("1.2.3"), [])
        for bad in ("01.2.3", "1.02.3", "1.2", "v1.2.3", "1.2.3-rc1", ""):
            self.assertTrue(rc.check_semver(bad), bad)


if __name__ == "__main__":
    unittest.main(verbosity=1)
