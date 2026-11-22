"""procgroup: one external command as a bounded, isolated process group whose identity is recorded before it can act.
Only harmless local commands run here (sh, sleep, env, cat, head, touch)."""

import hashlib
import json
import os
import signal
import subprocess
import sys
import time

import pytest

import procgroup as P

ENV = {"PATH": "/usr/bin:/bin"}
MARKER = "OUTPUT-MARKER-must-never-be-kept"
# Distinctive durations: a sleeper left behind by a failing run can be recognised as this test's and nobody else's.
LONG, LONGER = "31.4159", "61.4159"


def run(argv, record=None, **kw):
    seen = []
    kw.setdefault("deadline_s", 10)
    kw.setdefault("env", ENV)
    res = P.run(argv, cwd="/", record=record or seen.append, **kw)
    return res, seen


def sh(script, **kw):
    return run(["/bin/sh", "-c", script], **kw)


def wait_for(path, seconds=5.0):
    end = time.monotonic() + seconds
    while not os.path.exists(path) or os.path.getsize(path) == 0:
        assert time.monotonic() < end, f"{path} did not appear"
        time.sleep(0.02)


# --- the identity is recorded before the command can act -----------------------------------------------------------

def test_the_command_cannot_run_until_its_identity_is_recorded(tmp_path):
    ran = tmp_path / "ran"
    at_record = []

    def record(identity):
        time.sleep(0.4)                                   # ample time for an ungated command to have run
        at_record.append((dict(identity), ran.exists()))

    res, _ = run(["/usr/bin/touch", str(ran)], record=record)
    (identity, existed), = at_record
    assert existed is False and ran.exists()              # nothing happened before record returned; then it ran
    assert res.started and res.exit_kind == "exited" and res.exit_status == 0 and res.group_empty is True
    assert res.identity == identity and identity["pid"] == identity["pgid"] and identity["start"].isdigit()
    assert identity["host"] and identity["boot_id"] == P.boot_id()
    assert P.group_state(identity) == "empty"


@pytest.mark.parametrize("failure", [OSError("disk full"), RuntimeError("receipt chain"), KeyboardInterrupt()])
def test_if_the_identity_cannot_be_recorded_the_command_never_executes(tmp_path, failure):
    ran = tmp_path / "ran"

    def record(identity):
        raise failure

    res, _ = run(["/usr/bin/touch", str(ran)], record=record)
    assert not ran.exists()
    assert res.started is False and res.exit_kind == "never-executed" and res.exit_status is None
    assert res.runner_error == type(failure).__name__ and res.group_empty is True and res.signals_sent == []
    assert "disk full" not in json.dumps(res.as_dict())   # a class name only, never a message


def test_an_identity_that_cannot_be_read_keeps_the_gate_closed(tmp_path, monkeypatch):
    ran, seen = tmp_path / "ran", []
    monkeypatch.setattr(P, "identity_of", lambda pid: None)
    res = P.run(["/usr/bin/touch", str(ran)], env=ENV, cwd="/", deadline_s=10, record=seen.append)
    assert not ran.exists() and seen == [] and res.started is False and res.exit_kind == "never-executed"
    assert res.runner_error == "RuntimeError" and res.identity is None


def test_a_command_that_is_not_an_absolute_path_is_never_started(tmp_path):
    for argv in ([], ["touch", str(tmp_path / "ran")], ["./touch"]):
        res, seen = run(argv)
        assert res.started is False and res.runner_error == "ValueError" and seen == [] and res.identity is None
    for name in ("PWD", "OLDPWD"):                        # the trampoline removes these: asking for one is refused
        res, seen = run(["/usr/bin/touch", str(tmp_path / "ran")], env={name: "/"})
        assert res.started is False and res.runner_error == "ValueError" and seen == []
    assert not (tmp_path / "ran").exists()


def test_an_interrupted_gate_write_is_never_reported_as_never_executed(tmp_path, monkeypatch):
    """The write that opens the gate succeeds and the parent is interrupted right after it: the command runs. Even a
    command that itself exits with the trampoline's refusal status must not look unexecuted."""
    ran = tmp_path / "ran"
    real = os.write

    def write_then_interrupt(fd, data):
        n = real(fd, data)
        if bytes(data) == b"go\n":
            raise KeyboardInterrupt()
        return n

    monkeypatch.setattr(P.os, "write", write_then_interrupt)
    res, seen = sh(f"touch {ran}; exit {P.GATE_REFUSED}")
    assert ran.exists() and len(seen) == 1
    assert res.started is True and res.exit_kind == "exited" and res.exit_status == P.GATE_REFUSED
    assert res.runner_error == "KeyboardInterrupt" and res.group_empty is True


def test_a_gate_write_that_fails_outright_is_still_possibly_started(tmp_path, monkeypatch):
    real = os.write

    def fail(fd, data):
        if bytes(data) == b"go\n":
            raise OSError("injected")
        return real(fd, data)

    monkeypatch.setattr(P.os, "write", fail)
    res, _ = run(["/usr/bin/touch", str(tmp_path / "ran")])
    assert not (tmp_path / "ran").exists()                # the gate got end of file: the trampoline refused
    assert res.started is True and res.exit_kind == "exited" and res.exit_status == P.GATE_REFUSED   # never "never-executed"
    assert res.runner_error == "OSError" and res.group_empty is True


def test_an_interruption_during_the_wait_does_not_leave_the_command_running(monkeypatch):
    real_wait = subprocess.Popen.wait
    state = {"n": 0}

    def wait(self, timeout=None):
        state["n"] += 1
        if state["n"] == 1:
            raise KeyboardInterrupt()
        return real_wait(self, timeout=timeout)

    monkeypatch.setattr(subprocess.Popen, "wait", wait)
    res, seen = sh(f"sleep {LONG}", deadline_s=30, grace_s=3)
    assert res.runner_error == "KeyboardInterrupt" and res.signals_sent == ["TERM"]
    assert res.exit_kind == "signaled" and res.group_empty is True and P.group_state(seen[0]) == "empty"


def test_an_interruption_during_the_grace_time_still_ends_in_kill(monkeypatch):
    state = {"n": 0}

    def interrupted_sleep(seconds):
        state["n"] += 1
        if state["n"] == 2:                               # inside the grace wait after TERM
            raise KeyboardInterrupt()
        time.sleep(seconds)

    res, seen = sh('trap "" TERM; while :; do sleep 0.1; done', deadline_s=0.3, grace_s=30, sleep=interrupted_sleep)
    assert res.runner_error == "KeyboardInterrupt" and res.signals_sent == ["TERM", "KILL"]
    assert res.exit_kind == "signaled" and res.exit_status == signal.SIGKILL
    assert res.group_empty is True and P.group_state(seen[0]) == "empty"


def test_repeated_interruptions_end_with_an_unknown_group_never_a_claim(monkeypatch):
    def always(seconds):
        raise KeyboardInterrupt()

    res, seen = sh('trap "" TERM; while :; do sleep 0.1; done', deadline_s=0.3, grace_s=30, sleep=always)
    try:
        assert res.runner_error == "KeyboardInterrupt" and "KILL" in res.signals_sent
        assert res.group_empty is not True or P.group_state(seen[0]) == "empty"   # True only if it really is empty
    finally:
        try:
            os.killpg(seen[0]["pgid"], signal.SIGKILL)
        except OSError:
            pass


# --- bounded: the deadline, the escalation, what is left in the group ---------------------------------------------------

def test_at_the_deadline_the_group_is_terminated_reaped_and_proven_empty():
    start = time.monotonic()
    res, seen = sh(f"sleep {LONG} & sleep {LONG} & wait", deadline_s=0.4, grace_s=3)
    assert time.monotonic() - start < 3
    assert res.deadline_exceeded and res.signals_sent == ["TERM"]
    assert res.exit_kind == "signaled" and res.exit_status == signal.SIGTERM
    assert res.group_empty is True and P.members(seen[0]["pgid"]) == [] and P.group_state(seen[0]) == "empty"


def test_a_group_that_ignores_term_is_killed():
    res, seen = sh('trap "" TERM; while :; do sleep 0.1; done', deadline_s=0.4, grace_s=0.6)
    assert res.deadline_exceeded and res.signals_sent == ["TERM", "KILL"]
    assert res.exit_kind == "signaled" and res.exit_status == signal.SIGKILL and res.group_empty is True
    assert P.group_state(seen[0]) == "empty"


def test_a_leader_that_exits_and_leaves_a_process_in_its_group_is_not_the_end():
    res, seen = sh(f"sleep {LONG} & exit 0", linger_s=0.3, grace_s=3)
    assert res.exit_kind == "exited" and res.exit_status == 0 and res.deadline_exceeded is False
    assert res.survivors is True and res.signals_sent == ["TERM"] and res.group_empty is True
    assert P.members(seen[0]["pgid"]) == []


def test_survivors_that_end_within_the_linger_time_are_not_signalled():
    res, _ = sh("sleep 0.3 & exit 0", linger_s=5)
    assert res.survivors is True and res.signals_sent == [] and res.group_empty is True and res.exit_status == 0


def test_a_process_that_left_the_group_is_not_claimed_and_an_open_stream_is_reported(tmp_path):
    pidfile = tmp_path / "pid"
    res, _ = sh(f"setsid sh -c 'echo $$ > {pidfile}; exec sleep {LONG}' & exit 0", linger_s=0.3, reap_s=0.5)
    wait_for(str(pidfile))
    escaped = int(pidfile.read_text())
    try:
        assert res.group_empty is True                    # the group is empty; the escapee is in another session
        assert res.output_complete is False               # but it still holds the output streams: said, not hidden
        assert os.path.exists(f"/proc/{escaped}")
    finally:
        os.kill(escaped, signal.SIGKILL)


def test_a_group_that_cannot_be_inspected_is_unknown_not_empty(monkeypatch):
    real = P.members
    calls = {"n": 0}

    def flaky(pgid):
        calls["n"] += 1
        return None if calls["n"] > 1 else real(pgid)

    monkeypatch.setattr(P, "members", flaky)
    res, _ = sh("exit 0", reap_s=0.3)
    assert res.exit_status == 0 and res.group_empty is None


# --- output is counted and hashed, never kept ----------------------------------------------------------------------------

def test_output_is_drained_counted_and_hashed_and_appears_nowhere():
    res, _ = sh(f"echo {MARKER}; echo {MARKER}-err >&2; exit 4")
    assert res.exit_status == 4 and res.output_complete
    assert (res.stdout_bytes, res.stdout_sha256) == (len(MARKER) + 1, hashlib.sha256((MARKER + "\n").encode()).hexdigest())
    assert (res.stderr_bytes, res.stderr_sha256) == (len(MARKER) + 5, hashlib.sha256((MARKER + "-err\n").encode()).hexdigest())
    assert MARKER not in json.dumps(res.as_dict()) and MARKER not in repr(res)


def test_a_flood_on_both_streams_is_drained_without_being_stored():
    res, _ = sh("head -c 30000000 /dev/zero; head -c 20000000 /dev/zero >&2", deadline_s=60)
    assert res.exit_status == 0 and res.output_complete and res.group_empty is True
    assert res.stdout_bytes == 30_000_000 and res.stdout_sha256 == hashlib.sha256(bytes(30_000_000)).hexdigest()
    assert res.stderr_bytes == 20_000_000 and res.stderr_sha256 == hashlib.sha256(bytes(20_000_000)).hexdigest()
    assert len(json.dumps(res.as_dict())) < 1000


def test_a_command_stuck_writing_is_still_ended_at_the_deadline():
    res, _ = sh("yes", deadline_s=0.4, grace_s=3)
    assert res.deadline_exceeded and res.signals_sent == ["TERM"] and res.group_empty is True and res.stdout_bytes > 0


# --- the environment and stdin ------------------------------------------------------------------------------------------------

def test_the_environment_is_exactly_the_one_given(monkeypatch):
    monkeypatch.setenv("HELM_DEBUG", "true")
    monkeypatch.setenv("C28_INHERITED", "yes")
    res, _ = run(["/usr/bin/env"], env={"ONLY": "this"})
    assert (res.stdout_bytes, res.stdout_sha256) == (10, hashlib.sha256(b"ONLY=this\n").hexdigest())
    res, _ = run(["/usr/bin/env"], env={})                # nothing at all: not even the shell's own PWD
    assert res.exit_status == 0 and res.stdout_bytes == 0


def test_stdin_is_empty():
    res, _ = run(["/bin/cat"], deadline_s=5)
    assert res.exit_status == 0 and res.stdout_bytes == 0 and res.deadline_exceeded is False


def test_a_command_that_does_not_exist_is_an_ordinary_failed_exit(tmp_path):
    res, seen = run([str(tmp_path / "no-such-command")])
    assert res.started and res.exit_kind == "exited" and res.exit_status in (126, 127) and res.group_empty is True
    assert len(seen) == 1 and str(tmp_path) not in json.dumps(res.as_dict())


# --- the recorded identity answers later, without the runner ------------------------------------------------------------------

def test_group_state_from_a_recorded_identity():
    p = subprocess.Popen(["/bin/sleep", LONG], start_new_session=True)
    try:
        identity = P.identity_of(p.pid)
        assert identity["pgid"] == p.pid and P.group_state(identity) == "alive"
        assert P.group_state(dict(identity, host="another-host")) == "unknown"
        assert P.group_state(dict(identity, boot_id="another-boot")) == "unknown"
        assert P.group_state(dict(identity, boot_id="")) == "unknown"
        assert P.group_state({"pid": "x"}) == "unknown"
        # the recorded leader's start time differs (its PID was reused), but the group still has a live process
        assert P.group_state(dict(identity, start="1")) == "alive"
    finally:
        p.kill()
        p.wait()
    assert P.group_state(identity) == "empty" and P.members(identity["pgid"]) == []


def test_an_unreadable_proc_is_unknown(monkeypatch, tmp_path):
    p = subprocess.Popen(["/bin/sleep", LONG], start_new_session=True)
    try:
        identity = P.identity_of(p.pid)
        monkeypatch.setattr(P, "PROC", str(tmp_path / "no-proc"))
        assert P.members(identity["pgid"]) is None and P.group_state(identity) == "unknown"
        assert P.identity_of(p.pid) is None
    finally:
        p.kill()
        p.wait()


def test_one_process_that_cannot_be_examined_makes_the_scan_incomplete(monkeypatch, tmp_path):
    fake = tmp_path / "proc"
    (fake / "4242" / "stat").mkdir(parents=True)          # reading it fails, and not because the process is gone
    (fake / "4243").mkdir()                               # this one simply ended: not a failure
    monkeypatch.setattr(P, "PROC", str(fake))
    assert P.members(1) is None
    (fake / "4242" / "stat").rmdir()
    (fake / "4242" / "stat").write_text("4242 (some (odd) name) S 1 77 77 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 5 0 0\n")
    assert P.members(77) == [4242] and P.members(78) == []


def test_a_dead_group_that_cannot_be_scanned_is_unknown_not_empty(monkeypatch):
    p = subprocess.Popen(["/bin/true"], start_new_session=True)
    identity = P.identity_of(p.pid)
    p.wait()
    if identity is None:                                  # it ended before it could be read: take a synthetic one
        identity = {"pid": p.pid, "pgid": p.pid, "start": "1", "host": P.socket.gethostname(), "boot_id": P.boot_id()}
    assert P.group_state(identity) == "empty"
    monkeypatch.setattr(P, "members", lambda pgid: None)
    assert P.group_state(identity) == "unknown"


def test_without_a_boot_identity_nothing_is_judged(monkeypatch):
    p = subprocess.Popen(["/bin/sleep", LONG], start_new_session=True)
    try:
        identity = P.identity_of(p.pid)
        monkeypatch.setattr(P, "boot_id", lambda: "")     # this host cannot say which boot it is in
        assert P.group_state(dict(identity, boot_id="")) == "unknown"
        assert P.identity_of(p.pid) is None               # and no identity is produced without it
    finally:
        p.kill()
        p.wait()


def test_the_command_outlives_a_killed_executor_and_the_record_still_finds_it(tmp_path):
    """The case the shared recovery guard cannot see: the executor (the parent) is gone, the command is not."""
    record = tmp_path / "identity.json"
    executor = subprocess.Popen([sys.executable, "-c", f"""
import json, sys
sys.path.insert(0, {os.path.dirname(os.path.abspath(P.__file__))!r})
import procgroup as P
def record(identity):
    with open({str(record)!r}, "w") as fh:
        json.dump(identity, fh)
P.run(["/bin/sleep", {LONGER!r}], env={{}}, cwd="/", deadline_s=120, record=record)
"""])
    try:
        wait_for(str(record))
        identity = json.loads(record.read_text())
        time.sleep(0.3)                                   # the gate has opened: the command is running
        executor.kill()
        executor.wait()
        assert P.group_state(identity) == "alive"         # the parent is proven gone; its command is still there
        os.killpg(identity["pgid"], signal.SIGKILL)
        end = time.monotonic() + 5
        while P.group_state(identity) != "empty":
            assert time.monotonic() < end
            time.sleep(0.05)
    finally:
        if executor.poll() is None:
            executor.kill()
        try:
            os.killpg(json.loads(record.read_text())["pgid"], signal.SIGKILL)
        except (OSError, ValueError):
            pass


# --- a read's stdout sink ---------------------------------------------------------------------------------------------

def test_a_reads_stdout_goes_to_its_sink_and_stderr_and_the_result_keep_nothing():
    got = []
    res, _ = sh(f"printf '%s' answer-{MARKER}; printf '%s' err-{MARKER} >&2", stdout_sink=got.append)
    assert b"".join(got) == f"answer-{MARKER}".encode()                  # stdout only, complete, in order
    assert res.output_complete and res.stdout_bytes == len(f"answer-{MARKER}")
    assert res.stdout_sha256 == hashlib.sha256(f"answer-{MARKER}".encode()).hexdigest()
    assert MARKER not in json.dumps(res.as_dict())


def test_a_failing_sink_does_not_escape_and_the_output_is_not_called_complete():
    def sink(chunk):
        raise RuntimeError(f"a sink error quoting {MARKER}")
    res, _ = sh("head -c 300000 /dev/zero; sleep 0.2", stdout_sink=sink, deadline_s=5)
    assert res.output_complete is False and res.group_empty is True      # ended, reaped, nothing claimed about output
    assert MARKER not in json.dumps(res.as_dict())
