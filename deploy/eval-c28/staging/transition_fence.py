#!/usr/bin/env python3
"""Fencing an executor before its permission is treated as revoked (TRANSITION-DESIGN draft 6, section 3).

A grant executor that is paused after its last check can resume later and start Helm. Deleting the binding is always
safe and never waits for this; but "revoked" may only be claimed once that executor, and the Helm child it may have
started, are PROVEN gone. This module is that proof and nothing else. It never touches a cluster.

How an executor makes itself fenceable: `own_group()` at start makes the process the leader of a NEW process group
that it shares with nobody (a Helm child inherits it; nothing else is ever started in it, and no child may leave it
for another group or session), and `owner()` is what it records durably before it can grant anything: host, boot id,
pid (which IS the group id), the pid's start time.

`fence(record)` returns {"proven": bool, "why": str, "signalled": bool}. The fence is PROVEN only when the record is
well formed and of this host, and either

- it is of another boot of this host (no process of that boot exists any more), or
- it is of this boot; the recorded pid is the recorded process (same start time, running or exited) or does not exist;
  a running owner is the leader of the recorded group; the group received SIGKILL if it had members; and /proc then
  shows no process that can run in that group.

Everything else is UNPROVEN: another host; a record that is incomplete or malformed in any field (checked BEFORE any
proof branch, the boot ids included); a pid that differs from the group id; a recorded pid that now belongs to another
process, running or zombie (a recycled pid is never signalled); a running owner that is not the leader of the recorded
group; this process's own group; a signal that is not permitted; a member that does not die within the wait; an
invalid wait; and ANY failure to observe: a /proc entry or the /proc listing that cannot be read or parsed is not
"absent", it is unknown, and unknown proves nothing. Only "no such process" counts as gone.
Unproven stays unproven: no amount of waiting turns it into a proof.

The process view must be COMPLETE. An empty listing proves nothing when /proc hides processes: with hidepid (any mode
but 0/off) other users' process directories are simply not listed, without any error. So before the same-boot path,
and again before the group is declared empty, /proc must be exactly one procfs mount, at its root, without hidepid;
anything else, or mount information that cannot be read or is ambiguous, is unproven. No exemption is assumed from
this process's privileges.

Supported use: on the host itself, in the host's pid namespace (the staging tools run that way on the ops box). The
mount check does NOT establish that a different pid namespace or a container sees the executor's processes.
"""
import math
import os
import re
import signal
import socket
import time

PROC = "/proc"
BOOT_ID = "/proc/sys/kernel/random/boot_id"
MOUNTINFO = "/proc/self/mountinfo"
HIDEPID_OFF = ("0", "off")
FIELDS = ("host", "boot_id", "pid", "pid_start", "pgid")
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
TICKS = re.compile(r"[0-9]{1,20}\Z")
NOT_RUNNABLE = ("Z", "X")               # exited: a zombie waits to be collected and can run nothing


class Unobservable(Exception):
    """The state of a process, or the process list, could not be read: unknown, which is never 'absent'."""


def _read(path):
    with open(path) as f:
        return f.read()


def boot_id():
    """This boot's id, or "" when it cannot be read as a UUID."""
    try:
        value = _read(BOOT_ID).strip()
    except OSError:
        return ""
    return value if UUID.match(value) else ""


def _stat(pid):
    """(process group id, start time in clock ticks, state) of a process; None ONLY when it certainly does not
    exist; Unobservable when its entry cannot be read or parsed."""
    try:
        text = _read(f"{PROC}/{pid}/stat")
    except (FileNotFoundError, ProcessLookupError):
        return None                                          # no such process (or it ended during the read)
    except OSError as e:
        raise Unobservable(f"/proc/{pid}/stat: {type(e).__name__}") from None
    try:
        rest = text.rsplit(")", 1)[1].split()                # the command name may hold spaces and parentheses
        group, start, state = int(rest[2]), rest[19], rest[0]    # fields 5 (pgrp), 22 (starttime), 3 (state)
    except (IndexError, ValueError):
        raise Unobservable(f"/proc/{pid}/stat: not in the expected form") from None
    if not TICKS.match(start) or len(state) != 1:
        raise Unobservable(f"/proc/{pid}/stat: not in the expected form")
    return group, start, state


def proc_view():
    """None when /proc shows every process; otherwise why the view cannot be trusted as complete."""
    try:
        text = _read(MOUNTINFO)
    except OSError as e:
        return f"the mount table cannot be read ({type(e).__name__})"
    found = 0
    for line in text.splitlines():
        parts = line.split(" ")
        if "-" not in parts or parts.index("-") < 6 or len(parts) < parts.index("-") + 4:
            return "the mount table has a line that is not in the expected form"
        sep = parts.index("-")
        if parts[4] != PROC:
            continue
        found += 1
        if parts[sep + 1] != "proc" or parts[3] != "/":
            return f"{PROC} is not the root of a procfs mount"
        for options in (parts[5], parts[sep + 3]):          # per-mount options and the filesystem's own options
            for option in options.split(","):
                name, _, value = option.partition("=")
                if name == "hidepid" and value not in HIDEPID_OFF:
                    return f"{PROC} hides processes (hidepid={value or 'set'}): an empty listing proves nothing"
    if found != 1:
        return f"{PROC} appears {found} times in the mount table: the view is ambiguous"
    return None


def own_group():
    """Make this process the leader of its own process group; returns the group id. Call once, before any child."""
    if os.getpgrp() != os.getpid():
        os.setpgid(0, 0)
    return os.getpgrp()


def owner():
    """What an executor records about itself (after own_group(): pid and pgid are then the same number)."""
    me = _stat(os.getpid())
    return {"host": socket.gethostname(), "boot_id": boot_id(), "pid": os.getpid(),
            "pid_start": me[1] if me else None, "pgid": os.getpgrp()}


def group_members(pgid):
    """The pids that can still RUN in process group pgid. A process that certainly ended is not a member, and neither
    is a zombie. Raises Unobservable when the list or any entry cannot be read: an unread entry might be a member."""
    try:
        names = os.listdir(PROC)
    except OSError as e:
        raise Unobservable(f"{PROC}: {type(e).__name__}") from None
    members = []
    for name in names:
        if name.isdigit():
            stat = _stat(int(name))
            if stat is not None and stat[0] == pgid and stat[2] not in NOT_RUNNABLE:
                members.append(int(name))
    return sorted(members)


def _unproven(why, signalled=False):
    return {"proven": False, "why": why, "signalled": signalled}


def _problem(record, wait):
    """Why the inputs cannot be used, or None. Everything is checked here, before any proof branch."""
    if isinstance(wait, bool) or not isinstance(wait, (int, float)) or not math.isfinite(wait) or wait < 0:
        return "the wait is not a finite, non-negative number"
    if not isinstance(record, dict) or any(k not in record for k in FIELDS):
        return "the owner record is incomplete"
    for key in ("pid", "pgid"):
        value = record[key]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 1:
            return "the owner record names no usable pid or process group"
    if record["pid"] != record["pgid"]:
        return "the recorded pid is not the leader of the recorded group (pid and group id differ)"
    if not isinstance(record["pid_start"], str) or not TICKS.match(record["pid_start"]):
        return "the owner record's start time is not a tick count"
    if not isinstance(record["boot_id"], str) or not UUID.match(record["boot_id"]):
        return "the owner record's boot id is not a boot id"
    if not isinstance(record["host"], str) or not record["host"]:
        return "the owner record names no host"
    return None


def fence(record, *, wait=10.0, kill=os.killpg, mono=time.monotonic, sleep=time.sleep):
    problem = _problem(record, wait)
    if problem:
        return _unproven(problem)
    pid, pgid = record["pid"], record["pgid"]
    if record["host"] != socket.gethostname():
        return _unproven(f"the owner ran on host {record['host']!r}: it cannot be fenced from here")
    current = boot_id()
    if not current:
        return _unproven("this host's boot id cannot be read")
    if record["boot_id"] != current:
        return {"proven": True, "why": "the owner belonged to another boot of this host", "signalled": False}
    if pgid == os.getpgrp():
        return _unproven("the recorded group is this process's own group")
    hidden = proc_view()
    if hidden:
        return _unproven(hidden)
    signalled = False
    try:
        live = _stat(pid)
        if live is not None:
            # Identity first, whatever the state: a recycled pid, running or zombie, is never signalled.
            if live[1] != record["pid_start"]:
                return _unproven("the recorded pid now belongs to another process (recycled): nothing was signalled")
            if live[2] not in NOT_RUNNABLE and live[0] != pgid:
                return _unproven("the live owner is not the leader of the recorded process group: nothing was signalled")
        # The owner is the recorded process (running or exited), or it does not exist. A group can outlive its
        # leader (the Helm child), and the group id cannot be reused as a pid while the group has members.
        if group_members(pgid):
            try:
                kill(pgid, signal.SIGKILL)
                signalled = True
            except ProcessLookupError:
                pass                                         # the group ended between the read and the signal
            except PermissionError:
                return _unproven("not permitted to signal the recorded process group")
        deadline = mono() + wait
        while True:
            left = group_members(pgid)
            if not left:
                hidden = proc_view()                         # still complete NOW, not only before the signal
                if hidden:
                    return _unproven(hidden, signalled)
                return {"proven": True, "why": "no process is left in the owner's group", "signalled": signalled}
            if mono() >= deadline:
                return _unproven(f"processes {left} are still in the owner's group after {wait} s", signalled)
            sleep(0.05)
    except Unobservable as e:
        return _unproven(f"the processes cannot be observed ({e}): unknown is not absent", signalled)
