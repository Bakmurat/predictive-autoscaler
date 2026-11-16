"""Fencing an executor (transition_fence.py), against real processes: a leader in its own group with a child, as an
executor with its Helm child would be."""
import json
import os
import signal
import socket
import subprocess
import sys
import time

import pytest

import transition_fence as F

STAGING = os.path.dirname(os.path.abspath(F.__file__))
OWNER = f"""
import json, subprocess, sys, time
sys.path.insert(0, {STAGING!r})
import transition_fence as F
F.own_group()
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
print(json.dumps(dict(F.owner(), child=child.pid)), flush=True)
time.sleep(600)
"""


def never(*_args):
    raise AssertionError("a signal was sent where none may be sent")


def alive(pid):
    """Running (not a zombie, not gone)."""
    stat = F._stat(pid)
    return stat is not None and stat[2] not in ("Z", "X")


@pytest.fixture
def executor():
    """Starts an 'executor': (Popen, its owner record incl. the child's pid). Always cleaned up by group."""
    started = []

    def start():
        p = subprocess.Popen([sys.executable, "-c", OWNER], stdout=subprocess.PIPE, text=True)
        record = json.loads(p.stdout.readline())
        started.append((p, record))
        assert record["pgid"] == p.pid != os.getpgrp() and alive(record["child"])
        return p, record
    yield start
    for p, record in started:
        if record["pgid"] != os.getpgrp():
            try:
                os.killpg(record["pgid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
        p.wait(timeout=10)
        p.stdout.close()


def record_only(record):
    return {k: record[k] for k in F.FIELDS}


def test_an_executor_records_itself_as_the_leader_of_its_own_group(executor):
    p, record = executor()
    assert set(F.FIELDS) <= set(record)
    assert record["pid"] == record["pgid"] == p.pid and record["host"] == socket.gethostname()
    assert record["boot_id"] == F.boot_id() and record["pid_start"] == F._stat(p.pid)[1]
    assert F.group_members(record["pgid"]) == sorted([p.pid, record["child"]])     # the child inherited the group
    assert os.getpid() not in F.group_members(record["pgid"])


def test_a_running_executor_and_its_child_are_killed_and_the_fence_is_proven(executor):
    p, record = executor()
    out = F.fence(record_only(record), wait=5)
    assert out == {"proven": True, "why": "no process is left in the owner's group", "signalled": True}
    assert not alive(p.pid) and not alive(record["child"]) and F.group_members(record["pgid"]) == []


def test_a_paused_executor_is_fenced(executor):
    # The case the fence exists for: stopped after its last check, it could resume later and start Helm.
    p, record = executor()
    os.kill(p.pid, signal.SIGSTOP)
    deadline = time.monotonic() + 5
    while F._stat(p.pid)[2] != "T" and time.monotonic() < deadline:
        time.sleep(0.02)
    assert F._stat(p.pid)[2] == "T"
    out = F.fence(record_only(record), wait=5)
    assert out["proven"] and out["signalled"]
    assert not alive(p.pid) and not alive(record["child"])


def test_a_child_that_outlived_its_executor_is_killed(executor):
    p, record = executor()
    os.kill(p.pid, signal.SIGKILL)
    p.wait(timeout=5)                                                   # the executor is gone and collected
    assert F._stat(p.pid) is None and alive(record["child"])
    assert F.group_members(record["pgid"]) == [record["child"]]         # the group outlived its leader
    out = F.fence(record_only(record), wait=5)
    assert out["proven"] and out["signalled"] and not alive(record["child"])


def test_an_executor_that_is_gone_with_an_empty_group_is_proven_without_a_signal(executor):
    p, record = executor()
    os.killpg(record["pgid"], signal.SIGKILL)
    p.wait(timeout=5)
    deadline = time.monotonic() + 5
    while F.group_members(record["pgid"]) and time.monotonic() < deadline:
        time.sleep(0.02)
    out = F.fence(record_only(record), kill=never)
    assert out == {"proven": True, "why": "no process is left in the owner's group", "signalled": False}


def test_a_recycled_pid_is_never_signalled_and_proves_nothing(executor):
    p, record = executor()
    out = F.fence(dict(record_only(record), pid_start=str(int(record["pid_start"]) + 1)), kill=never)
    assert out["proven"] is False and out["signalled"] is False and "recycled" in out["why"]
    assert alive(p.pid) and alive(record["child"])


def test_a_live_owner_that_is_not_the_leader_of_the_recorded_group_is_not_signalled(executor):
    p, record = executor()
    child_start = F._stat(record["child"])[1]
    for wrong in (dict(record_only(record), pid=record["child"], pid_start=child_start),      # a member, not the leader
                  dict(record_only(record), pgid=record["child"]),                            # another group id
                  # pid and group id agree, but that live process leads no group: it sits in the executor's group
                  dict(record_only(record), pid=record["child"], pgid=record["child"], pid_start=child_start)):
        out = F.fence(wrong, kill=never)
        assert out["proven"] is False and out["signalled"] is False and "leader" in out["why"], wrong
    assert alive(p.pid) and alive(record["child"])


def test_this_process_own_group_is_never_fenced():
    leader = os.getpgrp()
    stat = F._stat(leader) or F._stat(os.getpid())
    out = F.fence({"host": socket.gethostname(), "boot_id": F.boot_id(), "pid": leader, "pid_start": stat[1],
                   "pgid": leader}, kill=never)
    assert out["proven"] is False and out["signalled"] is False and "own group" in out["why"]


def test_another_boot_of_this_host_is_proven_without_a_signal(executor):
    p, record = executor()
    out = F.fence(dict(record_only(record), boot_id="00000000-0000-0000-0000-000000000000"), kill=never)
    assert out == {"proven": True, "why": "the owner belonged to another boot of this host", "signalled": False}
    assert alive(p.pid)                                                 # nothing was touched


@pytest.mark.parametrize("change", [
    {"host": "another-host"}, {"pid": 0}, {"pid": 1}, {"pid": -5}, {"pid": True}, {"pid": "12"}, {"pgid": 0},
    {"pgid": 1}, {"pgid": None}, {"pid_start": ""}, {"pid_start": None}, {"pid_start": 12345}, {"boot_id": ""},
])
def test_a_record_that_cannot_be_checked_is_unproven_and_nothing_is_signalled(executor, change):
    p, record = executor()
    out = F.fence(dict(record_only(record), **change), kill=never)
    assert out["proven"] is False and out["signalled"] is False
    assert alive(p.pid) and alive(record["child"])


@pytest.mark.parametrize("record", [None, {}, [], "x", {"host": socket.gethostname()},
                                    {"host": socket.gethostname(), "boot_id": "b", "pid": 5, "pid_start": "1"}])
def test_an_incomplete_record_is_unproven(record):
    out = F.fence(record, kill=never)
    assert out == {"proven": False, "why": "the owner record is incomplete", "signalled": False}


def test_a_member_that_does_not_die_keeps_the_fence_unproven(executor):
    p, record = executor()
    out = F.fence(record_only(record), wait=0.3, kill=lambda *_a: None)             # the signal has no effect
    assert out["proven"] is False and out["signalled"] is True and "still in the owner's group" in out["why"]
    assert alive(p.pid)


def test_a_signal_that_is_not_permitted_is_unproven(executor):
    p, record = executor()

    def denied(*_a):
        raise PermissionError
    out = F.fence(record_only(record), kill=denied)
    assert out["proven"] is False and out["signalled"] is False and "not permitted" in out["why"]
    assert alive(p.pid)


def test_a_zombie_is_not_a_member(executor):
    # Killed but not yet collected by its parent: it can run nothing, so it does not keep the fence open.
    p, record = executor()
    os.killpg(record["pgid"], signal.SIGKILL)
    deadline = time.monotonic() + 5
    while (F._stat(p.pid) or (0, "", "Z"))[2] != "Z" and time.monotonic() < deadline:
        time.sleep(0.02)
    assert F._stat(p.pid)[2] == "Z"                                     # not collected: this test has not waited for it
    assert p.pid not in F.group_members(record["pgid"])
    assert F.fence(record_only(record), wait=5, kill=never)["proven"]


# --- the code review's four findings, each as a regression ------------------------------------------------------------

def unreadable(monkeypatch, match, error=PermissionError("denied"), text=None):
    """Make reading every /proc path that `match` accepts fail (or return `text`): an observation failure."""
    real = F._read

    def read(path):
        if match(path):
            if text is not None:
                return text
            raise error
        return real(path)
    monkeypatch.setattr(F, "_read", read)


@pytest.mark.parametrize("how", ["permission", "io-error", "garbage", "truncated", "bad-start", "another-entry"])
def test_an_entry_that_cannot_be_read_is_unknown_not_absent(executor, monkeypatch, how):
    # Finding 1: an unreadable /proc entry was treated as "no such process" and the fence came out PROVEN with no
    # signal while the owner was still running.
    p, record = executor()
    mine = f"/proc/{p.pid}/stat"
    if how == "permission":
        unreadable(monkeypatch, lambda path: path == mine)
    elif how == "io-error":
        unreadable(monkeypatch, lambda path: path == mine, error=OSError(5, "Input/output error"))
    elif how == "garbage":
        unreadable(monkeypatch, lambda path: path == mine, text="not a stat line")
    elif how == "truncated":
        unreadable(monkeypatch, lambda path: path == mine, text=f"{p.pid} (python) S 1 {p.pid}")
    elif how == "bad-start":
        good = F._read(mine).rsplit(")", 1)
        fields = good[1].split()
        fields[19] = "soon"
        unreadable(monkeypatch, lambda path: path == mine, text=good[0] + ") " + " ".join(fields))
    else:                                              # some OTHER process's entry: it might be a member
        unreadable(monkeypatch, lambda path: path == f"/proc/{os.getpid()}/stat")
    out = F.fence(record_only(record), kill=never)
    assert out["proven"] is False and out["signalled"] is False and "cannot be observed" in out["why"], out
    monkeypatch.undo()
    assert alive(p.pid) and alive(record["child"])


def test_a_process_list_that_cannot_be_read_is_unproven(executor, monkeypatch):
    p, record = executor()

    def denied(_path):
        raise PermissionError("denied")
    monkeypatch.setattr(F.os, "listdir", denied)
    out = F.fence(record_only(record), kill=never)
    monkeypatch.undo()
    assert out["proven"] is False and out["signalled"] is False and "cannot be observed" in out["why"]
    assert alive(p.pid)


def test_losing_sight_after_the_signal_is_unproven_although_the_group_was_killed(executor, monkeypatch):
    p, record = executor()
    real, calls = os.listdir, {"n": 0}

    def listdir(path):
        calls["n"] += 1
        if calls["n"] > 1:                              # the first look works; after the signal /proc is unreadable
            raise OSError(5, "Input/output error")
        return real(path)
    monkeypatch.setattr(F.os, "listdir", listdir)
    out = F.fence(record_only(record), wait=2)
    monkeypatch.undo()
    assert out["proven"] is False and out["signalled"] is True and "cannot be observed" in out["why"]


def gone_pid():
    """A pid that certainly belongs to no process now (its process was just collected)."""
    q = subprocess.Popen([sys.executable, "-c", "pass"])
    q.wait()
    assert F._stat(q.pid) is None
    return q.pid


def test_a_pid_that_does_not_exist_cannot_lend_its_record_to_another_group(executor):
    # Finding 2a: a nonexistent recorded pid plus an unrelated EXISTING group id reached killpg.
    p, record = executor()
    out = F.fence(dict(record_only(record), pid=gone_pid()), kill=never)
    assert out["proven"] is False and out["signalled"] is False and "leader" in out["why"]
    assert alive(p.pid) and alive(record["child"])


def zombie_leader(p, record):
    os.kill(p.pid, signal.SIGKILL)                      # only the leader; it is NOT collected: a zombie
    deadline = time.monotonic() + 5
    while F._stat(p.pid)[2] != "Z" and time.monotonic() < deadline:
        time.sleep(0.02)
    assert F._stat(p.pid)[2] == "Z" and alive(record["child"])


def test_a_recycled_zombie_is_still_a_recycled_pid(executor):
    # Finding 2b: a zombie was dropped before its start time was compared, so a record that did not match it went
    # on to signal the group.
    p, record = executor()
    zombie_leader(p, record)
    out = F.fence(dict(record_only(record), pid_start=str(int(record["pid_start"]) + 1)), kill=never)
    assert out["proven"] is False and out["signalled"] is False and "recycled" in out["why"]
    assert alive(record["child"])


def test_the_recorded_executor_as_a_zombie_with_a_live_child_is_fenced(executor):
    p, record = executor()
    zombie_leader(p, record)
    out = F.fence(record_only(record), wait=5)
    assert out == {"proven": True, "why": "no process is left in the owner's group", "signalled": True}
    assert not alive(record["child"])


@pytest.mark.parametrize("change", [
    {"boot_id": {"bad": True}}, {"boot_id": "x"}, {"boot_id": None}, {"boot_id": 5}, {"boot_id": ["a"]},
    {"boot_id": "00000000-0000-0000-0000-00000000000"}, {"boot_id": "0000000000000000000000000000000000000"},
    {"boot_id": "ABCDEF00-0000-0000-0000-000000000000"}, {"boot_id": "00000000-0000-0000-0000-000000000000\n"},
    {"pid_start": "12a"}, {"pid_start": "-1"}, {"pid_start": " 12"}, {"pid_start": "1e3"}, {"pid_start": "1.5"},
    {"pid_start": ["12"]}, {"host": ""}, {"host": None}, {"host": 5},
])
def test_a_malformed_field_proves_nothing_not_even_a_reboot(executor, change):
    # Finding 3: boot_id={"bad": True} differed from the current boot id and so "proved" another boot.
    p, record = executor()
    out = F.fence(dict(record_only(record), **change), kill=never)
    assert out["proven"] is False and out["signalled"] is False, out
    assert alive(p.pid) and alive(record["child"])


def test_an_unreadable_current_boot_id_proves_nothing(executor, monkeypatch):
    p, record = executor()
    for value in ("", "garbage"):
        unreadable(monkeypatch, lambda path: path == F.BOOT_ID, text=value)
        assert F.boot_id() == ""
        out = F.fence(record_only(record), kill=never)
        assert out["proven"] is False and "boot id cannot be read" in out["why"]
        monkeypatch.undo()
    unreadable(monkeypatch, lambda path: path == F.BOOT_ID)
    assert F.fence(record_only(record), kill=never)["proven"] is False
    monkeypatch.undo()
    assert alive(p.pid)


@pytest.mark.parametrize("wait", [float("nan"), float("inf"), float("-inf"), -1, -0.001, True, False, "5", None, [5]])
def test_a_wait_that_does_not_bound_anything_is_refused_before_any_signal(executor, wait):
    # Finding 4: NaN made the deadline comparison false for ever if a member remained.
    p, record = executor()
    out = F.fence(record_only(record), wait=wait, kill=never)
    assert out == {"proven": False, "why": "the wait is not a finite, non-negative number", "signalled": False}
    assert alive(p.pid) and alive(record["child"])


def test_a_zero_wait_is_allowed_and_bounded(executor):
    p, record = executor()
    began = time.monotonic()
    out = F.fence(record_only(record), wait=0, kill=lambda *_a: None)
    assert out["proven"] is False and out["signalled"] is True and time.monotonic() - began < 2


@pytest.mark.parametrize("start", ["12a", "", None, 12345, "-1", " 12", ["12"]])
def test_a_malformed_start_time_proves_nothing_when_the_owner_is_gone_or_the_boot_differs(executor, start):
    # A malformed start time on a LIVE owner fails anyway (it cannot match); these are the two branches where it
    # would never be compared: the owner no longer exists (its child keeps the group alive), and another boot.
    p, record = executor()
    os.kill(p.pid, signal.SIGKILL)
    p.wait(timeout=5)
    assert F._stat(p.pid) is None and alive(record["child"])
    bad = dict(record_only(record), pid_start=start)
    out = F.fence(bad, kill=never)
    assert out["proven"] is False and out["signalled"] is False and "start time" in out["why"], out
    assert alive(record["child"])                                       # the group was not signalled
    other_boot = dict(bad, boot_id="00000000-0000-0000-0000-000000000000")
    assert F.fence(other_boot, kill=never)["proven"] is False
    # With a well-formed start time the same two records are provable: the validation, not the scenario, refused.
    assert F.fence(dict(record_only(record), boot_id="00000000-0000-0000-0000-000000000000"), kill=never)["proven"]
    assert F.fence(record_only(record), wait=5)["proven"] and not alive(record["child"])


# --- second code review: an empty listing proves nothing when /proc hides processes ---------------------------------

REAL_PROC_LINE = "28 32 0:25 / /proc rw,nosuid,nodev,noexec,relatime shared:13 - proc proc rw"
OTHER_LINES = ("27 32 0:24 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw\n"
               "32 1 259:2 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw,errors=remount-ro\n")


def mount_table(monkeypatch, *texts):
    """Serve these mount tables in turn (the last one repeats) instead of the real /proc/self/mountinfo."""
    real, served = F._read, {"n": 0}

    def read(path):
        if path == F.MOUNTINFO:
            text = texts[min(served["n"], len(texts) - 1)]
            served["n"] += 1
            if isinstance(text, Exception):
                raise text
            return text
        return real(path)
    monkeypatch.setattr(F, "_read", read)
    return served


def test_the_real_mount_table_of_this_host_is_accepted():
    assert F.proc_view() is None                                         # the ops box: procfs, no hidepid


@pytest.mark.parametrize("line", [
    REAL_PROC_LINE.replace("relatime shared", "relatime,hidepid=2 shared"),       # per-mount options
    REAL_PROC_LINE + ",hidepid=2", REAL_PROC_LINE + ",hidepid=1", REAL_PROC_LINE + ",hidepid=4",
    REAL_PROC_LINE + ",hidepid=invisible", REAL_PROC_LINE + ",hidepid=noaccess",
    REAL_PROC_LINE + ",hidepid=ptraceable", REAL_PROC_LINE + ",hidepid", REAL_PROC_LINE + ",hidepid=",
    REAL_PROC_LINE + ",hidepid=00", REAL_PROC_LINE + ",hidepid=OFF", REAL_PROC_LINE + ",hidepid=0,hidepid=2",
    REAL_PROC_LINE.replace("- proc proc", "- tmpfs tmpfs"),                        # not a procfs
    REAL_PROC_LINE.replace(" / /proc ", " /1234 /proc "),                         # not its root
    REAL_PROC_LINE + "\n" + REAL_PROC_LINE,                                        # mounted twice: ambiguous
    "",                                                                            # no /proc at all
    "28 32 0:25 / /proc rw",                                                       # malformed
    "28 32 0:25 / /proc rw - proc",                                                # truncated after the separator
    PermissionError("denied"), OSError(5, "Input/output error"),
])
def test_a_process_view_that_may_be_incomplete_proves_nothing(executor, monkeypatch, line):
    p, record = executor()
    mount_table(monkeypatch, line if isinstance(line, Exception) else OTHER_LINES + line + "\n")
    assert F.proc_view() is not None
    out = F.fence(record_only(record), kill=never)
    assert out["proven"] is False and out["signalled"] is False, out
    monkeypatch.undo()
    assert alive(p.pid) and alive(record["child"])


@pytest.mark.parametrize("line", [REAL_PROC_LINE, REAL_PROC_LINE + ",hidepid=0", REAL_PROC_LINE + ",hidepid=off",
                                  REAL_PROC_LINE.replace("relatime shared", "relatime,hidepid=0 shared")])
def test_a_complete_process_view_is_accepted(executor, monkeypatch, line):
    p, record = executor()
    mount_table(monkeypatch, OTHER_LINES + line + "\n")
    assert F.proc_view() is None
    out = F.fence(record_only(record), wait=5)
    assert out["proven"] and out["signalled"]


def test_a_filtered_listing_with_a_hidden_owner_is_not_absence(executor, monkeypatch):
    # The reviewer's reproduction: /proc lists nothing of the executor's and its own entry does not exist for this
    # viewer, exactly what hidepid does. Without the mount check this came out PROVEN with no signal.
    p, record = executor()
    hidden = {str(p.pid), str(record["child"])}
    real_listdir, real_read = os.listdir, F._read
    monkeypatch.setattr(F.os, "listdir", lambda path: [n for n in real_listdir(path) if n not in hidden])

    def read(path):
        if path == F.MOUNTINFO:
            return OTHER_LINES + REAL_PROC_LINE + ",hidepid=invisible\n"
        if path.split("/")[2] in hidden:
            raise FileNotFoundError(path)
        return real_read(path)
    monkeypatch.setattr(F, "_read", read)
    out = F.fence(record_only(record), kill=never)
    monkeypatch.undo()
    assert out["proven"] is False and out["signalled"] is False and "hides processes" in out["why"]
    assert alive(p.pid) and alive(record["child"])


def test_losing_the_complete_view_after_the_signal_is_unproven(executor, monkeypatch):
    # Complete before the signal, hidden afterwards (a remount): the empty listing that follows proves nothing.
    p, record = executor()
    served = mount_table(monkeypatch, OTHER_LINES + REAL_PROC_LINE + "\n",
                         OTHER_LINES + REAL_PROC_LINE + ",hidepid=2\n")
    out = F.fence(record_only(record), wait=5)
    monkeypatch.undo()
    assert served["n"] >= 2
    assert out["proven"] is False and out["signalled"] is True and "hides processes" in out["why"]


@pytest.mark.parametrize("table,times", [(OTHER_LINES, 0), (OTHER_LINES + REAL_PROC_LINE + "\n" + REAL_PROC_LINE + "\n", 2),
                                         (OTHER_LINES + (REAL_PROC_LINE + "\n") * 3, 3)])
def test_proc_must_be_mounted_exactly_once(executor, monkeypatch, table, times):
    # The "no /proc at all" case above is refused for its blank line, not for the count; these tables are well formed
    # and are refused for the count alone.
    p, record = executor()
    mount_table(monkeypatch, table)
    assert F.proc_view() == f"/proc appears {times} times in the mount table: the view is ambiguous"
    out = F.fence(record_only(record), kill=never)
    monkeypatch.undo()
    assert out["proven"] is False and out["signalled"] is False and "ambiguous" in out["why"]
    assert alive(p.pid)
