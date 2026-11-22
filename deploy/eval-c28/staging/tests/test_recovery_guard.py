"""common_records.recovery_guard and the owner check under it: one recoverer at a time (an flock held for the whole
recovery), a complete guard record published atomically, and a guard left behind by a dead recoverer taken over only
when that recoverer is PROVEN terminated, with the takeover journalled first."""

import json
import os
import signal
import socket
import subprocess
import sys
import time

import pytest

import common_records as C

SCOPE = "evaluation"
HERE = os.path.dirname(os.path.abspath(C.__file__))


@pytest.fixture
def rec(tmp_path):
    return {"schema": "c28-preparation/v1", "run_id": "guard-test", "cluster_uid": "cluster-1",
            "evidence_dir": str(tmp_path / "evidence"), "lock_dir": str(tmp_path / "locks")}


def dead_pid():
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    return p.pid


def stale_lock(rec):
    """A mutation lock left by an executor that has certainly ended; returns (path, sha256)."""
    lock = C.exclusive_lock(rec, SCOPE)
    lock.__enter__()
    body = json.loads(open(lock.path).read())
    body["pid"] = dead_pid()
    with open(lock.path, "w") as fh:
        fh.write(json.dumps(body, sort_keys=True))
    return lock.path, C.sha256_file(lock.path)


def guard_of(rec):
    path = C.lock_path(rec, SCOPE) + ".recovery"
    return json.loads(open(path).read()) if os.path.exists(path) else None


def recover_receipts(rec):
    path = C.receipts_path(rec, "recover")
    return C.read_receipts(path) if os.path.exists(path) else []


CHILD = """
import json, os, sys, time
sys.path.insert(0, sys.argv[1])
import common_records as C
rec = json.loads(sys.argv[2])
with C.recovery_guard(rec, "evaluation", sys.argv[3]):
    open(sys.argv[4], "w").write("inside")
    time.sleep(600)
"""


def child_inside(rec, sha, tmp_path):
    marker = tmp_path / "inside"
    p = subprocess.Popen([sys.executable, "-c", CHILD, HERE, json.dumps(rec), sha, str(marker)])
    end = time.time() + 20
    while not marker.exists():
        assert p.poll() is None and time.time() < end, "the child recoverer did not enter the guard"
        time.sleep(0.05)
    return p


# --- the guard record ---------------------------------------------------------------------------------------------------

def test_the_guard_record_says_who_recovers_which_lock_and_is_removed_on_leaving(rec):
    path, sha = stale_lock(rec)
    with C.recovery_guard(rec, SCOPE, sha) as g:
        body = guard_of(rec)
        assert body["schema"] == C.GUARD_SCHEMA and body["token"] == g.token and len(g.token) == 32
        assert (body["pid"], body["pid_start"]) == (os.getpid(), C.process_start(os.getpid()))
        assert (body["host"], body["boot_id"]) == (socket.gethostname(), C._boot_id())
        assert (body["scope"], body["run_id"], body["cluster_uid"]) == (SCOPE, "guard-test", "cluster-1")
        assert (body["lock_sha256"], body["record_sha256"]) == (sha, C.digest(rec)) and g.owns()
        assert not [n for n in os.listdir(os.path.dirname(path)) if ".tmp-" in n]
    assert guard_of(rec) is None and os.path.exists(path)              # the stale lock stays for the caller
    assert os.path.exists(path + ".recovery.flock")                    # the coordination file is never removed
    assert recover_receipts(rec) == []                                 # no takeover happened


def test_only_one_recovery_can_be_inside_at_a_time_and_the_next_one_can_follow(rec):
    _path, sha = stale_lock(rec)
    with C.recovery_guard(rec, SCOPE, sha):
        with pytest.raises(C.StagingError, match="another recovery is in progress"):
            C.recovery_guard(rec, SCOPE, sha).__enter__()
        assert guard_of(rec)["pid"] == os.getpid()                     # the refused attempt left the record alone
    with C.recovery_guard(rec, SCOPE, sha):                            # sequential recoveries are not a defect
        pass
    assert guard_of(rec) is None


def test_a_live_recoverer_in_another_process_refuses_everyone_else(rec, tmp_path):
    _path, sha = stale_lock(rec)
    p = child_inside(rec, sha, tmp_path)
    try:
        before = guard_of(rec)
        assert before["pid"] == p.pid
        with pytest.raises(C.StagingError, match="another recovery is in progress"):
            C.recovery_guard(rec, SCOPE, sha).__enter__()
        assert guard_of(rec) == before and recover_receipts(rec) == []
    finally:
        p.kill()
        p.wait()


def test_a_recoverer_killed_inside_the_guard_is_taken_over_once_it_is_proven_gone(rec, tmp_path):
    path, sha = stale_lock(rec)
    p = child_inside(rec, sha, tmp_path)
    left = guard_of(rec)
    raw_sha = C.sha256_file(path + ".recovery")
    p.send_signal(signal.SIGKILL)                                      # no cleanup of any kind
    p.wait()
    assert guard_of(rec) == left and os.path.exists(path)              # the populated guard and the stale lock remain
    with C.recovery_guard(rec, SCOPE, sha) as g:
        mine = guard_of(rec)
        assert mine["token"] == g.token != left["token"] and mine["pid"] == os.getpid()
        [r] = recover_receipts(rec)
        assert r["event"] == "recovery-guard-takeover"
        d = r["data"]
        assert d["stale_guard"]["sha256"] == raw_sha and d["lock_sha256"] == sha and d["scope"] == SCOPE
        assert d["stale_guard"]["owner"] == {k: left[k] for k in ("token", "pid", "pid_start", "host", "boot_id", "at")}
        assert d["new_guard"]["token"] == g.token and d["new_guard"]["pid"] == os.getpid()
    assert guard_of(rec) is None
    with C.recovery_guard(rec, SCOPE, sha):                            # no second takeover: nothing was left
        pass
    assert len(recover_receipts(rec)) == 1


HOLDER = """
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o640)
fcntl.flock(fd, fcntl.LOCK_EX)
open(sys.argv[2], "w").write("held")
time.sleep(600)
"""


def test_the_coordination_lock_alone_keeps_a_second_recoverer_out_even_of_a_provably_stale_guard(rec, tmp_path):
    """Two recoverers that both find a dead recoverer's guard must not both take it over: whoever holds the
    coordination lock is inside, whatever the guard record says."""
    path, sha = stale_lock(rec)
    left = _leave_guard(rec)                               # provably stale: on its own it would be taken over
    marker = tmp_path / "held"
    p = subprocess.Popen([sys.executable, "-c", HOLDER, path + ".recovery.flock", str(marker)])
    try:
        end = time.time() + 20
        while not marker.exists():
            assert p.poll() is None and time.time() < end
            time.sleep(0.05)
        with pytest.raises(C.StagingError, match="^another recovery is in progress$"):
            C.recovery_guard(rec, SCOPE, sha).__enter__()
        assert guard_of(rec) == left and recover_receipts(rec) == []
    finally:
        p.kill()
        p.wait()
    with C.recovery_guard(rec, SCOPE, sha) as g:           # the holder died: the kernel released its lock
        assert guard_of(rec)["token"] == g.token


def test_a_guard_record_that_appears_between_the_look_and_the_publication_is_not_overwritten(rec, monkeypatch):
    path, sha = stale_lock(rec)
    g = C.recovery_guard(rec, SCOPE, sha)
    foreign = {}

    def nothing_there():                                   # ...and right after this look, somebody's record appears
        foreign.update(_leave_guard(rec, pid=os.getpid(), pid_start=C.process_start(os.getpid())))
        return None
    monkeypatch.setattr(g, "_stale_guard", nothing_there)
    with pytest.raises(FileExistsError):
        g.__enter__()
    assert guard_of(rec) == foreign and not [n for n in os.listdir(os.path.dirname(path)) if ".tmp-" in n]


def _leave_guard(rec, **change):
    """A guard record as a dead recoverer of this host and boot left it, with fields changed or removed."""
    path = C.lock_path(rec, SCOPE) + ".recovery"
    body = {"schema": C.GUARD_SCHEMA, "token": "t" * 32, "pid": dead_pid(), "pid_start": "12345",
            "host": socket.gethostname(), "boot_id": C._boot_id(), "at": "2026-11-22T00:00:00Z"}
    body.update(change)
    body = {k: v for k, v in body.items() if v is not None}
    with open(path, "w") as fh:
        fh.write(json.dumps(body, sort_keys=True))
    return body


@pytest.mark.parametrize("change,text", [
    (dict(host="another-host"), "taken on host 'another-host'"),
    (dict(boot_id="another-boot"), "another boot"),
    (dict(pid=None), "owner record is incomplete"),
    (dict(pid_start=None), "owner record is incomplete"),
    (dict(pid_start=""), "owner record is incomplete"),
    (dict(pid_start="not-a-start-time"), "owner record is incomplete"),
    (dict(pid_start="12 34"), "owner record is incomplete"),
    (dict(pid=0), "owner record is incomplete"),
    (dict(pid=-5), "owner record is incomplete"),
    (dict(pid="77"), "owner record is incomplete"),
    (dict(pid=True), "owner record is incomplete"),
    (dict(host=None), "owner record is incomplete"),
    (dict(boot_id=None), "owner record is incomplete"),
])
def test_a_guard_whose_recoverer_is_not_proven_gone_is_never_taken_over(rec, change, text):
    _path, sha = stale_lock(rec)
    left = _leave_guard(rec, **change)
    with pytest.raises(C.StagingError, match=text):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    assert guard_of(rec) == left and recover_receipts(rec) == []
    with pytest.raises(C.StagingError):                                # and the refusal released the coordination lock
        C.recovery_guard(rec, SCOPE, sha).__enter__()


@pytest.mark.parametrize("content", [b"", b"not json", b"[1, 2]", b'"a string"'])
def test_an_empty_or_unreadable_guard_of_the_old_format_is_refused_as_before(rec, content):
    path, sha = stale_lock(rec)
    with open(path + ".recovery", "wb") as fh:
        fh.write(content)
    with pytest.raises(C.StagingError, match="another recovery is in progress, or left a guard record that cannot"):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    assert open(path + ".recovery", "rb").read() == content and recover_receipts(rec) == []


def test_a_guard_of_a_live_process_that_holds_no_coordination_lock_is_still_not_taken_over(rec):
    _path, sha = stale_lock(rec)
    left = _leave_guard(rec, pid=os.getpid(), pid_start=C.process_start(os.getpid()))    # e.g. an older helper
    with pytest.raises(C.StagingError, match="is still running"):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    assert guard_of(rec) == left


def test_a_proc_that_cannot_be_read_proves_nothing(rec, monkeypatch):
    _path, sha = stale_lock(rec)
    left = _leave_guard(rec)
    real = C._process_state
    monkeypatch.setattr(C, "_process_state", lambda pid: ("unknown", None) if pid == left["pid"] else real(pid))
    with pytest.raises(C.StagingError, match="could not be read from /proc"):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    assert guard_of(rec) == left and recover_receipts(rec) == []


def test_a_takeover_that_cannot_be_journalled_does_not_happen(rec, monkeypatch):
    _path, sha = stale_lock(rec)
    left = _leave_guard(rec)

    def refuse(*a, **k):
        raise OSError("the journal cannot be written")
    monkeypatch.setattr(C, "append_receipt", refuse)
    with pytest.raises(OSError, match="journal"):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    monkeypatch.undo()
    assert guard_of(rec) == left                                       # the stale record is as it was
    with C.recovery_guard(rec, SCOPE, sha) as g:                       # and a later attempt takes over properly
        assert guard_of(rec)["token"] == g.token
    assert [r["event"] for r in recover_receipts(rec)] == ["recovery-guard-takeover"]


@pytest.mark.parametrize("failing", ["link", "replace"])
def test_a_failure_while_publishing_leaves_no_half_written_guard_and_no_held_lock(rec, monkeypatch, failing):
    path, sha = stale_lock(rec)
    left = _leave_guard(rec) if failing == "replace" else None

    def refuse(*a, **k):
        raise OSError("no space left on device")
    monkeypatch.setattr(C.os, failing, refuse)
    with pytest.raises(OSError, match="no space"):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    monkeypatch.undo()
    assert guard_of(rec) == left                                       # nothing of ours, the stale one untouched
    assert not [n for n in os.listdir(os.path.dirname(path)) if ".tmp-" in n]
    with C.recovery_guard(rec, SCOPE, sha) as g:                       # safe to try again: the journal shows both
        assert guard_of(rec)["token"] == g.token
    events = [r["event"] for r in recover_receipts(rec)]
    assert events == (["recovery-guard-takeover"] * 2 if failing == "replace" else [])


@pytest.mark.parametrize("change", [dict(token="f" * 32), dict(pid=1), dict(pid_start="1")])
def test_leaving_never_removes_a_guard_that_is_not_this_recoverys(rec, change):
    path, sha = stale_lock(rec)
    g = C.recovery_guard(rec, SCOPE, sha)
    g.__enter__()
    other = dict(guard_of(rec), **change)
    with open(path + ".recovery", "w") as fh:
        fh.write(json.dumps(other, sort_keys=True))
    assert not g.owns()
    with pytest.raises(C.StagingError, match="only by the recovery that holds the guard"):
        g.release_stale()
    g.__exit__(None, None, None)
    assert guard_of(rec) == other and os.path.exists(path)             # both left exactly as they were


def test_the_recovery_that_owns_the_guard_releases_the_stale_lock(rec):
    path, sha = stale_lock(rec)
    with C.recovery_guard(rec, SCOPE, sha) as g:
        g.release_stale()
        assert not os.path.exists(path)
    assert guard_of(rec) is None


def test_the_stale_lock_is_validated_before_anything_is_journalled_or_published(rec):
    path, sha = stale_lock(rec)
    left = _leave_guard(rec)
    with pytest.raises(C.StagingError, match="sha256 differs"):
        C.recovery_guard(rec, SCOPE, "0" * 64).__enter__()
    other = dict(rec, run_id="another-run")
    with pytest.raises(C.StagingError, match="belongs to another run or record"):
        C.recovery_guard(other, SCOPE, sha).__enter__()
    assert guard_of(rec) == left and recover_receipts(rec) == [] and recover_receipts(other) == []


def test_the_coordination_descriptor_is_not_inherited_by_child_commands(rec):
    _path, sha = stale_lock(rec)
    with C.recovery_guard(rec, SCOPE, sha) as g:
        # close_fds=False: nothing but the descriptor's own close-on-exec flag keeps it from the child
        out = subprocess.run([sys.executable, "-c", f"import os; os.fstat({g._fd})"], capture_output=True, text=True,
                             close_fds=False)
        assert out.returncode != 0 and "Bad file descriptor" in out.stderr
        assert os.get_inheritable(g._fd) is False


# --- entry that fails part of the way ---------------------------------------------------------------------------------

@pytest.mark.parametrize("stale", [False, True])
def test_a_recoverer_that_cannot_describe_itself_journals_and_publishes_nothing(rec, monkeypatch, stale):
    path, sha = stale_lock(rec)
    left = _leave_guard(rec) if stale else None
    real = C.process_start
    monkeypatch.setattr(C, "process_start", lambda pid: None if pid == os.getpid() else real(pid))
    with pytest.raises(C.StagingError, match="cannot describe itself verifiably"):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    monkeypatch.undo()
    assert guard_of(rec) == left and recover_receipts(rec) == [] and os.path.exists(path)
    with C.recovery_guard(rec, SCOPE, sha):                # and the coordination lock was released
        pass


def test_a_recovery_whose_identity_can_no_longer_be_read_owns_nothing(rec, monkeypatch):
    path, sha = stale_lock(rec)
    g = C.recovery_guard(rec, SCOPE, sha)
    g.__enter__()
    mine = guard_of(rec)
    monkeypatch.setattr(C, "process_start", lambda pid: None)
    assert g.owns() is False
    nameless = dict(mine, pid_start=None)                  # and "no start time" never equals "no start time"
    with open(path + ".recovery", "w") as fh:
        fh.write(json.dumps(nameless, sort_keys=True))
    assert g.owns() is False
    with pytest.raises(C.StagingError, match="only by the recovery that holds the guard"):
        g.release_stale()
    g.__exit__(None, None, None)
    assert guard_of(rec) == nameless and os.path.exists(path)  # neither the record nor the lock was touched


def _fail_directory_sync(monkeypatch, error, before=None):
    real = C.os.fsync

    def fsync(fd):
        import stat
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            if before:
                before()
            raise error
        return real(fd)
    monkeypatch.setattr(C.os, "fsync", fsync)


@pytest.mark.parametrize("stale", [False, True])
@pytest.mark.parametrize("error", [OSError(5, "I/O error"), KeyboardInterrupt()])
def test_a_failure_after_the_record_is_published_does_not_strand_it(rec, monkeypatch, stale, error):
    path, sha = stale_lock(rec)
    if stale:
        _leave_guard(rec)
    _fail_directory_sync(monkeypatch, error)
    with pytest.raises(type(error)):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    monkeypatch.undo()
    assert guard_of(rec) is None and os.path.exists(path)  # this attempt's own record is withdrawn, the lock stays
    assert not [n for n in os.listdir(os.path.dirname(path)) if ".tmp-" in n]
    with C.recovery_guard(rec, SCOPE, sha) as g:           # a live process that never entered blocks nobody
        assert guard_of(rec)["token"] == g.token
    assert [r["event"] for r in recover_receipts(rec)] == (["recovery-guard-takeover"] if stale else [])


def test_withdrawing_after_a_failed_entry_never_removes_somebody_elses_record(rec, monkeypatch):
    path, sha = stale_lock(rec)
    foreign = {}

    def replaced_meanwhile():                              # between our publication and our failure
        foreign.update(dict(guard_of(rec), token="e" * 32))
        with open(path + ".recovery", "w") as fh:
            fh.write(json.dumps(foreign, sort_keys=True))
    _fail_directory_sync(monkeypatch, OSError(5, "I/O error"), before=replaced_meanwhile)
    with pytest.raises(OSError):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    monkeypatch.undo()
    assert guard_of(rec) == foreign and os.path.exists(path)


class Interrupted(BaseException):
    """An interruption that is not an Exception (as KeyboardInterrupt is not), without ending the test session."""


def test_an_interruption_anywhere_in_the_cleanup_still_releases_the_coordination_lock(rec, monkeypatch):
    path, sha = stale_lock(rec)
    _fail_directory_sync(monkeypatch, OSError(5, "I/O error"))

    class Attempts:                                        # interrupts the cleanup OUTSIDE its bounded attempts
        def __index__(self):
            raise Interrupted()
    monkeypatch.setattr(C, "WITHDRAW_ATTEMPTS", Attempts())
    g = C.recovery_guard(rec, SCOPE, sha)
    with pytest.raises(Interrupted):
        g.__enter__()
    monkeypatch.undo()
    assert g._fd is None and os.path.exists(path)          # released all the same
    assert guard_of(rec)["pid"] == os.getpid()             # its record could not be taken back: visible, not hidden


@pytest.mark.parametrize("interruptions,stranded", [(1, False), (C.WITHDRAW_ATTEMPTS - 1, False),
                                                    (C.WITHDRAW_ATTEMPTS, True)])
def test_an_interruption_of_the_cleanup_after_a_failed_entry_still_releases_the_lock(rec, monkeypatch, interruptions,
                                                                                      stranded):
    """An ordinary I/O failure after the publication, and then the cleanup itself is interrupted: the coordination
    lock is released in every case, the ORIGINAL failure is the one reported, and the attempt's own record is taken
    back unless every bounded attempt was interrupted."""
    path, sha = stale_lock(rec)
    _fail_directory_sync(monkeypatch, OSError(5, "I/O error"))
    real, left = C.recovery_guard._withdraw, [interruptions]

    def withdraw(self, body):
        if left[0] > 0:
            left[0] -= 1
            raise Interrupted()
        return real(self, body)
    monkeypatch.setattr(C.recovery_guard, "_withdraw", withdraw)
    g = C.recovery_guard(rec, SCOPE, sha)
    with pytest.raises(OSError, match="I/O error"):
        g.__enter__()
    monkeypatch.undo()
    assert g._fd is None and g.token is None and os.path.exists(path)
    if stranded:                                           # its record names this live process: refused, not hidden
        assert guard_of(rec)["pid"] == os.getpid()
        with pytest.raises(C.StagingError, match="is still running"):
            C.recovery_guard(rec, SCOPE, sha).__enter__()
    else:
        assert guard_of(rec) is None
        with C.recovery_guard(rec, SCOPE, sha):
            pass


def test_an_interruption_right_after_the_coordination_lock_was_taken_releases_it(rec, monkeypatch):
    _path, sha = stale_lock(rec)
    real = C.fcntl.flock

    def flock(fd, op):
        real(fd, op)                                       # acquired...
        raise KeyboardInterrupt()                          # ...and interrupted before anything else
    monkeypatch.setattr(C.fcntl, "flock", flock)
    g = C.recovery_guard(rec, SCOPE, sha)
    with pytest.raises(KeyboardInterrupt):
        g.__enter__()
    monkeypatch.undo()
    assert g._fd is None and guard_of(rec) is None
    with C.recovery_guard(rec, SCOPE, sha):                # nobody is locked out
        pass


# --- the owner check that mutation-lock recovery uses too ---------------------------------------------------------------

def _owner(**change):
    body = {"pid": os.getpid(), "pid_start": C.process_start(os.getpid()), "host": socket.gethostname(),
            "boot_id": C._boot_id()}
    body.update(change)
    return {k: v for k, v in body.items() if v is not None}


def test_an_owner_is_terminated_only_on_evidence(monkeypatch):
    assert C.owner_state(_owner()) == ("alive", f"its executor (pid {os.getpid()}) is still running")
    assert C.owner_state(_owner(pid=dead_pid())) == ("terminated", "executor gone")
    assert C.owner_state(_owner(pid_start="1"))[0] == "terminated"     # this pid, started at another time: reused
    for malformed in ("not-a-start-time", "12x", " 12", "-5", "1.5"):  # never "another start time, so terminated"
        assert C.owner_state(_owner(pid_start=malformed))[0] == "unknown", malformed
    for incomplete in (_owner(pid=None), _owner(pid_start=None), _owner(host=None), _owner(boot_id=None),
                       _owner(pid=0), _owner(pid="1"), _owner(pid_start=5), {}):
        assert C.owner_state(incomplete)[0] == "unknown" and C.owner_terminated(incomplete)[0] is False
    assert C.owner_state(_owner(host="elsewhere"))[0] == "unknown"
    assert C.owner_state(_owner(boot_id="another"))[0] == "unknown"
    monkeypatch.setattr(C, "_boot_id", lambda: "")                     # this host's boot id cannot be read
    assert C.owner_state(_owner(boot_id=""))[0] == "unknown" and C.owner_state(_owner())[0] == "unknown"


@pytest.mark.parametrize("failure,state", [(FileNotFoundError(), "absent"), (ProcessLookupError(), "absent"),
                                           (PermissionError(), "unknown"), (OSError(5, "I/O error"), "unknown")])
def test_only_absence_counts_as_absence(monkeypatch, failure, state):
    def boom(*a, **k):
        raise failure
    monkeypatch.setattr("builtins.open", boom)
    assert C._process_state(4242)[0] == state


@pytest.mark.parametrize("stat", ["", "garbage", "1 (x) S 1 2", "1 (a b) S " + " ".join(["0"] * 18) + " notanumber"])
def test_a_stat_line_that_cannot_be_parsed_is_unknown(monkeypatch, tmp_path, stat):
    import io
    monkeypatch.setattr("builtins.open", lambda *a, **k: io.StringIO(stat))
    assert C._process_state(4242) == ("unknown", None)


def test_an_unreadable_proc_no_longer_lets_a_mutation_lock_be_recovered(rec, monkeypatch):
    _path, sha = stale_lock(rec)
    monkeypatch.setattr(C, "_process_state", lambda pid: ("unknown", None))
    with pytest.raises(C.StagingError, match="refusing to recover: the state of its executor"):
        C.recovery_guard(rec, SCOPE, sha).__enter__()
    assert guard_of(rec) is None


def test_a_mutation_lock_with_an_incomplete_owner_record_is_not_recoverable(rec):
    path, _sha = stale_lock(rec)
    body = json.loads(open(path).read())
    del body["pid_start"]
    with open(path, "w") as fh:
        fh.write(json.dumps(body, sort_keys=True))
    with pytest.raises(C.StagingError, match="refusing to recover: its owner record is incomplete"):
        C.recovery_guard(rec, SCOPE, C.sha256_file(path)).__enter__()


# --- taking the mutation lock: only by an executor that can be judged later ---------------------------------------------

def test_the_mutation_lock_records_an_owner_that_reads_back_as_this_live_process(rec):
    with C.exclusive_lock(rec, SCOPE) as lock:
        body = json.loads(open(lock.path).read())
        assert C.owner_state(body) == ("alive", f"its executor (pid {os.getpid()}) is still running")
    assert not os.path.exists(lock.path)


@pytest.mark.parametrize("broken,text", [
    ("process_start", "owner record is incomplete"),       # /proc/<pid>/stat unreadable: no start time
    ("_boot_id", "owner record is incomplete"),            # no boot id
])
def test_an_executor_that_cannot_describe_itself_takes_no_lock_and_changes_nothing(rec, monkeypatch, broken, text):
    monkeypatch.setattr(C, broken, (lambda pid: None) if broken == "process_start" else (lambda: ""))
    lock = C.exclusive_lock(rec, SCOPE)
    ran = []
    with pytest.raises(C.StagingError, match="cannot describe itself verifiably") as e:
        with lock:
            ran.append("a mutation")
    assert text in str(e.value) and ran == [] and not os.path.exists(lock.path)
    assert not os.path.isdir(os.path.dirname(lock.path)) or os.listdir(os.path.dirname(lock.path)) == []
    monkeypatch.undo()
    with C.exclusive_lock(rec, SCOPE):                     # nothing was left behind: the next executor is not blocked
        pass


def test_a_start_time_that_does_not_read_back_takes_no_lock(rec, monkeypatch):
    monkeypatch.setattr(C, "process_start", lambda pid: "1")           # a wrong value, not a missing one
    lock = C.exclusive_lock(rec, SCOPE)
    with pytest.raises(C.StagingError, match="cannot describe itself verifiably .executor gone."):
        lock.__enter__()
    assert not os.path.exists(lock.path)


def test_the_executor_on_record_is_the_process_that_enters_not_the_one_that_made_the_object(rec, tmp_path):
    """A lock object made before a fork: the child that enters is the executor. Recording the parent would let a
    recovery conclude, once the parent is gone, that a still-running child had terminated."""
    lock = C.exclusive_lock(rec, SCOPE)                                # made in the parent
    made = json.loads(lock.body)
    assert made["pid"] == os.getpid()
    entered, release = tmp_path / "entered", tmp_path / "release"
    child = os.fork()
    if child == 0:                                                     # the child: enter, report, wait, leave
        code = 1
        try:
            with lock:
                entered.write_text(str(os.getpid()))
                end = time.time() + 30
                while not release.exists() and time.time() < end:
                    time.sleep(0.02)
            code = 0
        finally:
            os._exit(code)
    try:
        end = time.time() + 20
        while not entered.exists():
            assert time.time() < end, "the child did not enter the lock"
            time.sleep(0.02)
        body = json.loads(open(lock.path).read())
        assert body["pid"] == child != os.getpid()                     # the child, with ITS start time
        assert body["pid_start"] == C.process_start(child) != made["pid_start"]
        assert C.owner_state(body) == ("alive", f"its executor (pid {child}) is still running")
    finally:
        release.write_text("go")
        _pid, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 0 and not os.path.exists(lock.path)


def test_the_record_is_taken_at_entry_and_stays_what_was_written(rec, monkeypatch):
    lock = C.exclusive_lock(rec, SCOPE)
    with lock:
        written = open(lock.path).read()
        assert lock.body == written
        monkeypatch.setattr(C, "process_start", lambda pid: "1")       # later reads do not change what is on record
        assert lock.body == written
