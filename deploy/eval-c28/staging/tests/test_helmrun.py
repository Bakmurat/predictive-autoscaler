"""helmrun.py with harmless local subprocesses (no helm, no cluster): the gate before execution, the deadline's
escalation, the group proof, a child that outlives its parent, an output flood, and output that must appear nowhere."""

import hashlib
import json
import os
import signal
import subprocess
import sys
import time

import pytest

import common_records as C
import helmrun as H

PY = sys.executable
MARKER = "tok-SECRET-marker-7f3a"


def script(code, *args):
    return [PY, "-c", code, *args]


def kill_group(pgid):
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def test_the_command_does_not_execute_before_its_identity_is_recorded(tmp_path):
    started, seen = tmp_path / "started", {}

    def record(ident):
        time.sleep(0.4)                                    # ample time for an ungated child to have started
        seen.update(ident, started_early=started.exists(), start_now=C.process_start(ident["pid"]))

    r = H.run(script("import sys; open(sys.argv[1], 'w').write('x')", str(started)), deadline_s=20,
              record_identity=record)
    assert seen["started_early"] is False and started.exists()
    assert seen["pid"] == seen["pgid"] and seen["pid_start"] == seen["start_now"] is not None
    assert r.clean_exit() and r.identity == {k: seen[k] for k in ("pid", "pgid", "pid_start", "host", "boot_id")}


def test_an_identity_that_cannot_be_recorded_means_nothing_executed(tmp_path):
    started, seen = tmp_path / "started", {}

    def record(ident):
        seen.update(ident)
        raise OSError("the receipt could not be written")

    with pytest.raises(OSError, match="receipt"):
        H.run(script("import sys; open(sys.argv[1], 'w').write('x')", str(started)), deadline_s=20,
              record_identity=record)
    time.sleep(0.3)
    assert not started.exists() and not H.group_exists(seen["pgid"])


def test_a_trampoline_whose_parent_dies_before_the_release_executes_nothing(tmp_path):
    started, ident = tmp_path / "started", tmp_path / "identity.json"
    parent = subprocess.Popen([PY, "-c", """
import json, os, sys, time
sys.path.insert(0, sys.argv[1])
import helmrun as H
def record(i):
    json.dump(i, open(sys.argv[3] + ".tmp", "w")); os.rename(sys.argv[3] + ".tmp", sys.argv[3]); time.sleep(600)
H.run([sys.executable, "-c", "import sys; open(sys.argv[1], 'w').write('x')", sys.argv[2]], deadline_s=20,
      record_identity=record)
""", os.path.dirname(H.__file__), str(started), str(ident)])
    try:
        for _ in range(200):
            if ident.exists():
                break
            time.sleep(0.05)
        recorded = json.loads(ident.read_text())
        parent.kill()                                      # the executor dies while the trampoline waits
        parent.wait()
        assert H.prove_group_empty(recorded["pgid"], 10) and not started.exists()
        assert H.group_gone(recorded) == (True, "group empty")
    finally:
        parent.kill()
        parent.wait()


@pytest.mark.parametrize("ignore_term,end,rc", [(False, "terminated", -signal.SIGTERM), (True, "killed", -signal.SIGKILL)])
def test_the_deadline_escalates_from_term_to_kill_and_the_group_is_proven_empty(tmp_path, ignore_term, end, rc):
    ready = tmp_path / "ready"
    code = ("import signal, sys, time\n" + ("signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignore_term else "")
            + "open(sys.argv[1], 'w').write('x')\ntime.sleep(600)\n")
    t0 = time.monotonic()
    r = H.run(script(code, str(ready)), deadline_s=1.5, term_grace_s=0.7)
    assert ready.exists() and (r.end, r.returncode, r.group_empty) == (end, rc, True)
    assert not r.clean_exit() and time.monotonic() - t0 < 15


def test_a_member_that_outlives_a_command_which_exited_by_itself_is_reported_not_hidden(tmp_path):
    code = "import subprocess, sys; subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(600)'])"
    r = H.run(script(code), deadline_s=20, settle_s=0.5, empty_wait_s=0.5)
    try:
        assert (r.end, r.returncode) == ("exited", 0)       # the command itself was fine
        assert r.group_empty is False and r.output_complete is False and not r.clean_exit()
        assert H.group_gone(r.identity) == (False, f"a process of group {r.identity['pgid']} still exists")
    finally:
        kill_group(r.identity["pgid"])
    assert H.prove_group_empty(r.identity["pgid"], 10) and H.group_gone(r.identity)[0]


def test_a_terminated_command_takes_its_members_with_it(tmp_path):
    code = ("import subprocess, sys, time\n"
            "subprocess.Popen([sys.executable, '-c', 'import signal, time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(600)'])\ntime.sleep(600)\n")
    r = H.run(script(code), deadline_s=1.0, term_grace_s=0.5, settle_s=0.5)
    assert r.end in ("terminated", "killed") and r.group_empty is True


def test_an_output_flood_is_counted_and_hashed_with_bounded_memory():
    n = 40 * 1024 * 1024
    code = ("import os, sys\nb = b'x' * 65536\n"
            f"for _ in range({n // 65536}):\n    os.write(1, b); os.write(2, b)\n")
    r = H.run(script(code), deadline_s=120, capture_stdout=1000)
    want = hashlib.sha256(b"x" * n).hexdigest()
    s = r.summary()
    assert r.clean_exit() and s["stdout"] == {"bytes": n, "sha256": want} and s["stderr"] == {"bytes": n, "sha256": want}
    assert r.stdout is None and r.capture_overflow is True            # more than the bound: nothing is handed over
    assert len(r._out.kept) <= 1000 and len(r._err.kept) == 0


def test_output_appears_only_in_the_bounded_capture_that_was_asked_for():
    code = f"import sys; sys.stdout.write({MARKER!r}); sys.stderr.write({MARKER!r}); sys.exit(3)"
    r = H.run(script(code), deadline_s=20, capture_stdout=4096)
    assert r.stdout == MARKER.encode() and (r.end, r.returncode) == ("exited", 3)
    assert MARKER not in json.dumps(r.summary()) and MARKER not in repr(r.__dict__["identity"])
    quiet = H.run(script(code), deadline_s=20)
    assert quiet.stdout is None and MARKER not in json.dumps(quiet.summary())
    assert quiet.summary()["stderr"] == {"bytes": len(MARKER), "sha256": hashlib.sha256(MARKER.encode()).hexdigest()}


def test_the_environment_is_a_whitelist(tmp_path, monkeypatch):
    for k, v in {"HELM_DEBUG": "true", "HELM_NAMESPACE": "other", "HELM_KUBECONTEXT": "other",
                 "KUBECONFIG": "/somewhere", "AWS_SECRET_ACCESS_KEY": MARKER, "LANG": "C.UTF-8"}.items():
        monkeypatch.setenv(k, v)
    assert set(H.environment()) <= set(H.ENV_KEEP) and H.environment()["LANG"] == "C.UTF-8"
    out = tmp_path / "env.json"
    r = H.run(script("import json, os, sys; json.dump(sorted(os.environ), open(sys.argv[1], 'w'))", str(out)),
              deadline_s=20)
    assert r.clean_exit() and set(json.loads(out.read_text())) <= set(H.ENV_KEEP) | {"LC_CTYPE"}


def test_a_relative_command_is_refused_before_anything_starts():
    with pytest.raises(C.StagingError, match="absolute"):
        H.run(["helm", "version"], deadline_s=5)


def test_the_group_of_a_dead_executors_command_is_judged_by_the_recorded_identity(tmp_path):
    ident_file = tmp_path / "identity.json"
    executor = subprocess.Popen([PY, "-c", """
import json, os, sys
sys.path.insert(0, sys.argv[1])
import helmrun as H
def record(i):
    json.dump(i, open(sys.argv[2] + ".tmp", "w")); os.rename(sys.argv[2] + ".tmp", sys.argv[2])
H.run([sys.executable, "-c", "import time; time.sleep(600)"], deadline_s=900, record_identity=record)
""", os.path.dirname(H.__file__), str(ident_file)])
    recorded = None
    try:
        for _ in range(200):
            if ident_file.exists():
                break
            time.sleep(0.05)
        recorded = json.loads(ident_file.read_text())
        time.sleep(0.3)
        executor.kill()                                    # the executor dies; its command stays alive
        executor.wait()
        assert H.group_gone(recorded) == (False, f"a process of group {recorded['pgid']} still exists")
        assert H.group_gone(dict(recorded, host="elsewhere"))[0] is False
        assert H.group_gone(dict(recorded, boot_id="another-boot"))[0] is False
        kill_group(recorded["pgid"])
        assert H.prove_group_empty(recorded["pgid"], 10) and H.group_gone(recorded) == (True, "group empty")
    finally:
        executor.kill()
        executor.wait()
        if recorded:
            kill_group(recorded["pgid"])


def test_a_reused_leader_pid_proves_the_recorded_group_emptied():
    me = H.identity(os.getpid())
    assert H.group_gone(dict(me, pgid=os.getpid(), pid_start="1"))[0] is True     # this pid, started at another time
    assert H.group_gone({"host": me["host"], "boot_id": me["boot_id"]})[0] is False   # no group named: not proven
