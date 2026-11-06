#!/usr/bin/env python3
"""Fixtures for release_checks.py (run by scripts/verify.sh): registry 403/timeout, foreign same-name checks, failed
newest attempts, forks, empty digests, and image defaults masked by values."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import release_checks as rc  # noqa: E402

SHA, REPO = "a" * 40, "Bakmurat/predictive-autoscaler"


def run(suite, path, sha=SHA, repo=REPO, conclusion="success", attempt=1, created="2026-10-17T00:00:00Z", number=1,
        event="push", branch="main"):
    return {"id": suite, "check_suite_id": suite, "path": path, "head_sha": sha, "run_attempt": attempt,
            "head_repository": {"full_name": repo}, "status": "completed", "conclusion": conclusion,
            "created_at": created, "run_number": number, "event": event, "head_branch": branch}


def check(name, suite, sha=SHA, slug="github-actions", conclusion="success", started="2026-10-17T00:00:00Z", cid=1):
    return {"id": cid, "name": name, "head_sha": sha, "app": {"slug": slug}, "check_suite": {"id": suite},
            "status": "completed", "conclusion": conclusion, "started_at": started}


def green():
    checks, suites = [], {}
    for name, path in rc.REQUIRED_CHECKS.items():
        s = suites.setdefault(path, 100 + len(suites))
        checks.append(check(name, s))
    runs = [run(s, p) for p, s in suites.items()]
    return checks, runs


NOT_FOUND = '{"errors":[{"code":"MANIFEST_UNKNOWN","message":"manifest unknown"}]}'


class Lookup(unittest.TestCase):
    def test_authenticated_404_is_absent(self):
        self.assertEqual(rc.classify_http(404, NOT_FOUND), "absent")
        self.assertEqual(rc.classify_http(404, '{"errors":[{"code":"NAME_UNKNOWN"}]}'), "absent")

    def test_200_is_present(self):
        self.assertEqual(rc.classify_http(200, "{}"), "present")

    def test_other_statuses_are_never_absence(self):
        for status in (401, 403, 429, 500, 503):     # even with a MANIFEST_UNKNOWN body (Codex r3 reproduction)
            with self.assertRaises(RuntimeError, msg=status):
                rc.classify_http(status, NOT_FOUND)

    def test_unrecognised_404(self):
        for body in ("", "not json", '{"errors":[]}', '{"errors":[{"code":"DENIED"}]}',
                     '{"errors":[{"code":"MANIFEST_UNKNOWN"},{"code":"DENIED"}]}'):   # mixed codes fail closed
            with self.assertRaises(RuntimeError, msg=body):
                rc.classify_http(404, body)


class NoRedirect(unittest.TestCase):
    """Neither Basic nor Bearer credentials may follow a redirect (Codex r4), including to plain HTTP elsewhere."""
    def test_redirect_is_refused_and_the_target_never_sees_credentials(self):
        import http.server
        import threading
        seen = []

        class Target(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.headers.get("Authorization"))
                self.send_response(200); self.end_headers(); self.wfile.write(b"{}")

            def log_message(self, *a):
                pass

        target = http.server.HTTPServer(("127.0.0.1", 0), Target)

        class Redirector(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{target.server_port}/stolen")
                self.end_headers()

            def log_message(self, *a):
                pass

        redirector = http.server.HTTPServer(("127.0.0.1", 0), Redirector)
        for srv in (target, redirector):
            threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            for auth in ("Basic dXNlcjpzZWNyZXQ=", "Bearer token-value"):
                with self.assertRaises(RuntimeError):
                    rc._get(f"http://127.0.0.1:{redirector.server_port}/token", {"Authorization": auth})
            self.assertEqual(seen, [])
        finally:
            for srv in (target, redirector):
                srv.shutdown()


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

    def test_wrong_event_or_branch(self):
        for kw in ({"event": "pull_request"}, {"branch": "product/x"}, {"event": "workflow_dispatch"}):
            checks, runs = green()
            runs = [dict(r, **({"event": kw["event"]} if "event" in kw else {"head_branch": kw["branch"]})) for r in runs]
            self.assertTrue(rc.check_ci(checks, runs, SHA, REPO), kw)

    def test_older_successful_run_with_a_higher_attempt_does_not_win(self):
        # Codex r3 reproduction: an older run at attempt 3 succeeded, a newer run at attempt 1 failed
        checks, runs = green()
        ci = ".github/workflows/ci.yml"
        runs = [r for r in runs if r["path"] != ci]
        runs.append(run(300, ci, attempt=3, created="2026-10-17T00:00:00Z", number=7))
        runs.append(run(301, ci, attempt=1, created="2026-10-17T02:00:00Z", number=8, conclusion="failure"))
        checks = [c for c in checks if rc.REQUIRED_CHECKS[c["name"]] != ci]
        for n, p in rc.REQUIRED_CHECKS.items():
            if p == ci:
                checks.append(check(n, 300))
                checks.append(check(n, 301, conclusion="failure", started="2026-10-17T02:01:00Z", cid=2))
        self.assertTrue(any("newest run" in p for p in rc.check_ci(checks, runs, SHA, REPO)))

    def test_rerun_success_inside_the_newest_run(self):
        checks, runs = green()
        name = list(rc.REQUIRED_CHECKS)[0]
        suite = [c for c in checks if c["name"] == name][0]["check_suite"]["id"]
        checks.append(check(name, suite, conclusion="failure", started="2026-10-16T23:00:00Z", cid=9))
        self.assertEqual(rc.check_ci(checks, runs, SHA, REPO), [])

    def test_newest_check_run_failed_inside_the_newest_run(self):
        checks, runs = green()
        name = list(rc.REQUIRED_CHECKS)[0]
        suite = [c for c in checks if c["name"] == name][0]["check_suite"]["id"]
        checks.append(check(name, suite, conclusion="failure", started="2026-10-17T05:00:00Z", cid=9))
        self.assertTrue(any("newest check run" in p for p in rc.check_ci(checks, runs, SHA, REPO)))


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
