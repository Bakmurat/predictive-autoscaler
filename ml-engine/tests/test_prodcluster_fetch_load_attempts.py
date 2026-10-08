"""deploy/prodcluster/fetch_load_attempts.sh: exit status and cleanup under injected failures (Codex r60). A stub kubectl
answers each call from the scenario in STUB_* variables; the script's clock hook puts it inside :25-:44."""
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "..", "deploy", "prodcluster", "fetch_load_attempts.sh")

STUB = r'''
import json, os, sys
args = sys.argv[1:]
d = os.environ["STUB_DIR"]
created = os.path.exists(os.path.join(d, "created"))
log = open(os.path.join(d, "calls.log"), "a"); log.write(" ".join(args) + "\n"); log.close()
def out(s, rc=0):
    sys.stdout.write(s); sys.exit(rc)
if "jobs" in args:
    out('{"items": []}')
if "pvc" in args:
    out("pv-1")
if "volumeattachments" in args:
    if created and os.environ.get("STUB_VA_FAIL"):
        out("", 1)
    out('{"items": []}')
if "create" in args:
    sys.stdin.read()
    if os.environ.get("STUB_CREATE_RC"):
        out("", int(os.environ["STUB_CREATE_RC"]))
    open(os.path.join(d, "created"), "w").close(); out("")
if "wait" in args:
    out("")
if "logs" in args:
    if os.environ.get("STUB_LOGS_FAIL"):
        out("partial", 1)
    body = json.dumps({"receipt": {"extractor": "fetch_load_attempts.sh v1", "files": []}}) + "\n"
    import hashlib
    out(body + json.dumps({"trailer": {"bytes": len(body.encode()), "sha256": hashlib.sha256(body.encode()).hexdigest()}}) + "\n")
if "delete" in args:
    out("")
if "pods" in args:
    if os.environ.get("STUB_PODS_FAIL"):
        out("", 1)
    out("pod/x\n" if os.environ.get("STUB_PODS_STAY") else "")
out("", 0)
'''


def run(tmp_path, minute="30", **scenario):
    stub_dir = tmp_path / "stub"
    stub_dir.mkdir(exist_ok=True)
    stub = stub_dir / "kubectl"
    stub.write_text(f"#!{sys.executable}\n" + STUB)
    stub.chmod(0o755)
    out = tmp_path / "attempts.jsonl"
    env = dict(os.environ, KUBE_CONTEXT="prodcluster", KUBECTL=str(stub), STUB_DIR=str(stub_dir),
               FETCH_CLOCK_MINUTE=minute, FETCH_CLEANUP_SECONDS="2", FETCH_POLL_SECONDS="0.2",
               **{f"STUB_{k.upper()}": v for k, v in scenario.items()})
    p = subprocess.run(["bash", SCRIPT, "--hours", "2026-10-07T21:00:00Z,2026-10-07T22:00:00Z", "--out", str(out)],
                       capture_output=True, text=True, env=env, timeout=60)
    return p, out


def test_happy_path_publishes_and_proves_cleanup(tmp_path):
    p, out = run(tmp_path)
    assert p.returncode == 0, p.stderr
    assert out.stat().st_size > 0 and "reader pod gone, load-evidence volume released" in p.stdout
    calls = (tmp_path / "stub" / "calls.log").read_text()
    assert "fetch-nonce=" in calls                                     # the pod is found and deleted by this run's nonce


@pytest.mark.parametrize("scenario", [{"pods_fail": "1"}, {"va_fail": "1"}, {"pods_stay": "1"}])
def test_unconfirmed_cleanup_exits_6_even_after_a_good_transfer(tmp_path, scenario):
    p, out = run(tmp_path, **scenario)
    assert p.returncode == 6, (p.stdout, p.stderr)
    assert "reader pod gone" not in p.stdout and "could not confirm" in p.stderr


def test_failed_transfer_with_unconfirmed_cleanup_is_6_and_leaves_no_reservation(tmp_path):
    p, out = run(tmp_path, logs_fail="1", pods_stay="1")
    assert p.returncode == 6 and not out.exists()
    assert [f for f in os.listdir(tmp_path) if f.startswith("attempts.jsonl.failed-")]


def test_failed_creation_still_runs_cleanup_and_fails(tmp_path):
    p, out = run(tmp_path, create_rc="1")
    assert p.returncode != 0 and not out.exists()
    assert "fetch-nonce=" in (tmp_path / "stub" / "calls.log").read_text()


def test_outside_the_window_or_an_existing_output_refuses_without_creating(tmp_path):
    p, out = run(tmp_path, minute="14")
    assert p.returncode == 4 and not (tmp_path / "stub" / "created").exists()
    out.write_text("earlier")
    p2, _ = run(tmp_path)
    assert p2.returncode == 2 and out.read_text() == "earlier"
