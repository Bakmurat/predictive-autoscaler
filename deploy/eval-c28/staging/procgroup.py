"""Run ONE external command as a bounded, isolated process group whose identity is on record before it can act.

This is the runner under the release helper (the Helm client is a writer: it must never outlive the helper that
started it, never be started twice, and nothing it prints may be kept). It knows nothing about Helm.

  run(argv, env=..., cwd=..., deadline_s=..., record=...) -> Result

What it guarantees, each covered by a test with harmless local commands:

- **Recorded before it can act.** The command is started behind a gate: a `/bin/sh` trampoline in a NEW session (so it
  leads its own process group) that waits on an inherited pipe and only then `exec`s the command. The parent learns the
  PID, the process group and the process start time, hands that identity to `record(identity)` (the caller writes it
  durably), and only when `record` has returned does it open the gate. If `record` raises, or the identity cannot be
  read, the gate is closed instead: the trampoline exits without ever executing the command (`started` is False).
- **Bounded.** At the deadline the whole GROUP gets SIGTERM, after `grace_s` SIGKILL. A command that exits by itself
  but leaves processes in its group gets `linger_s` for them to end, then the same escalation.
- **Reaped and proven empty.** The leader is reaped, and the group is then looked up in /proc: `group_empty` is True
  only when a complete scan finds no live process in the group, False when one is found, None when the scan could not
  be completed. A caller must not release a lock on anything but True.
- **Output is never kept.** stdout and stderr are drained continuously in fixed-size chunks into byte counters and
  SHA-256 digests; no chunk is stored, logged or attached to an exception. Result holds counts and digests only.
  A caller that must parse the answer of a READ passes `stdout_sink`: each stdout chunk is handed to it and to
  nothing else (bounding and dropping it is the caller's business); stderr never has a sink.
- **No surprises from the environment.** `env` is the complete environment: nothing is inherited, and the variable the
  trampoline shell would add (PWD) is removed again; an `env` that itself names PWD or OLDPWD is refused. stdin is
  /dev/null.
- **It does not raise for anything the command does, and an interruption cannot leave the command behind.**
  Failures of the runner itself, an interrupt included, come back as `Result.runner_error` (the first exception's
  class name, never a message that could quote output); termination, reaping and the final group check still run, and
  an attempt that is interrupted is followed by another, which goes on to SIGKILL. `started` is set before the gate is
  written, so an interrupted write is never reported as "never executed".

`group_state(identity)` answers later, from the recorded identity alone, whether that group can still hold a live
process: "empty", "alive" or "unknown" (another host or boot, an unreadable /proc). Recovery uses it: the shared
recovery guard proves only the PARENT gone.
"""
from __future__ import annotations

import hashlib
import os
import signal
import socket
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Dict, List, Optional

PROC = "/proc"
CHUNK = 65536
GATE_REFUSED = 97          # the trampoline's exit status when the gate was closed: the command never ran
# The trampoline: wait for one line on its stdin, the gate (EOF: the gate was closed), then become the command with
# /dev/null as stdin. `exec` keeps the PID, so the recorded identity is the command's.
# The shell would add its own PWD to the command's environment: it is unset, so the environment is exactly the caller's.
_TRAMPOLINE = 'IFS= read -r _ || exit %d; unset PWD OLDPWD; exec "$@" </dev/null' % GATE_REFUSED


@dataclass
class Result:
    started: bool = False                    # the identity was recorded and the gate MAY have been opened: the
    #                                          command may have run. False only when the gate was provably never opened
    identity: Optional[Dict] = None          # {pid, pgid, start, host, boot_id}
    exit_kind: Optional[str] = None          # "exited" | "signaled" | None (never reaped)
    exit_status: Optional[int] = None        # the exit status, or the signal number
    deadline_exceeded: bool = False
    survivors: bool = False                  # the leader exited by itself and left live processes in its group
    signals_sent: List[str] = field(default_factory=list)   # "TERM", "KILL" as sent to the group by the runner
    group_empty: Optional[bool] = None       # True only when proven
    output_complete: bool = False            # both streams reached end of file
    stdout_bytes: int = 0
    stdout_sha256: str = ""
    stderr_bytes: int = 0
    stderr_sha256: str = ""
    runner_error: Optional[str] = None       # an exception CLASS NAME from the runner itself, or None

    def as_dict(self) -> Dict:
        return asdict(self)


def boot_id() -> str:
    try:
        with open(os.path.join(PROC, "sys/kernel/random/boot_id")) as fh:
            return fh.read().strip()
    except OSError:
        return ""


def _stat(pid: int) -> Optional[List[str]]:
    """The fields of /proc/<pid>/stat after the command name (state first), or None when the process does not exist.
    Raises OSError for any other read failure."""
    try:
        with open(os.path.join(PROC, str(pid), "stat")) as fh:
            raw = fh.read()
    except (FileNotFoundError, ProcessLookupError):
        return None
    return raw.rsplit(")", 1)[1].split()


def identity_of(pid: int) -> Optional[Dict]:
    """{pid, pgid, start, host, boot_id} of a live process, or None when it cannot be established."""
    try:
        fields = _stat(pid)
    except OSError:
        return None
    if fields is None or len(fields) < 20:
        return None
    boot = boot_id()
    if not boot:
        return None
    return {"pid": pid, "pgid": int(fields[2]), "start": fields[19], "host": socket.gethostname(), "boot_id": boot}


def members(pgid: int) -> Optional[List[int]]:
    """The live (non-zombie) processes whose process group is pgid, or None when /proc could not be scanned completely.
    A process that ends during the scan is not a failure."""
    try:
        entries = [e for e in os.listdir(PROC) if e.isdigit()]
    except OSError:
        return None
    found = []
    for entry in entries:
        try:
            fields = _stat(int(entry))
        except OSError:
            return None
        if fields is None or len(fields) < 3:
            continue
        if int(fields[2]) == pgid and fields[0] not in ("Z", "X"):
            found.append(int(entry))
    return found


def group_state(identity: Dict) -> str:
    """"empty" | "alive" | "unknown" for a recorded identity. A process group id can be reused after its group has
    ended, so "alive" is claimed only while the recorded leader (same PID and start time) or a process of the group
    exists; another host or another boot cannot be judged here."""
    try:
        if identity.get("host") != socket.gethostname() or not identity.get("boot_id") or \
                identity.get("boot_id") != boot_id():
            return "unknown"
        pgid, pid = int(identity["pgid"]), int(identity["pid"])
        leader = _stat(pid)
    except (OSError, KeyError, TypeError, ValueError):
        return "unknown"
    if leader is not None and len(leader) >= 20 and leader[19] == identity.get("start") and leader[0] not in ("Z", "X"):
        return "alive"
    found = members(pgid)
    if found is None:
        return "unknown"
    return "alive" if found else "empty"


class _Drain(threading.Thread):
    """Reads one stream to its end, keeping only a byte count and a digest."""

    def __init__(self, stream, sink: Optional[Callable[[bytes], None]] = None):
        super().__init__(daemon=True)
        self.stream, self.count, self.digest, self.done, self.sink = stream, 0, hashlib.sha256(), False, sink

    def run(self):
        try:
            while True:
                chunk = self.stream.read(CHUNK)
                if not chunk:
                    self.done = True
                    return
                self.count += len(chunk)
                self.digest.update(chunk)
                if self.sink is not None:
                    self.sink(chunk)
        except Exception:                                      # a failing sink ends the drain: output_complete False
            return


def _signal_group(pgid: int, sig: int) -> None:
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError):
        pass


def _wait_empty(pgid: int, seconds: float, sleep: Callable[[float], None]) -> Optional[bool]:
    end = time.monotonic() + seconds
    while True:
        found = members(pgid)
        if found is None:
            return None
        if not found:
            return True
        if time.monotonic() >= end:
            return False
        sleep(0.05)


def run(argv: List[str], *, env: Dict[str, str], cwd: str, deadline_s: float, record: Callable[[Dict], None],
        grace_s: float = 10.0, linger_s: float = 5.0, reap_s: float = 10.0,
        sleep: Callable[[float], None] = time.sleep,
        stdout_sink: Optional[Callable[[bytes], None]] = None) -> Result:
    """Run argv as described in the module docstring. `record(identity)` must make the identity durable and return;
    if it raises, the command is never executed. argv[0] must be an absolute path (nothing is searched)."""
    res = Result()
    if not argv or not os.path.isabs(argv[0]) or "PWD" in env or "OLDPWD" in env:
        res.runner_error = "ValueError"                        # (the trampoline unsets PWD/OLDPWD: not silently dropped)
        return res
    proc, gate_r, gate_w = None, -1, -1
    drains: List[_Drain] = []
    try:
        gate_r, gate_w = os.pipe()
        # The gate is the trampoline's stdin; the command itself gets /dev/null (see _TRAMPOLINE).
        proc = subprocess.Popen(["/bin/sh", "-c", _TRAMPOLINE, "sh"] + list(argv), env=dict(env), cwd=cwd,
                                stdin=gate_r, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                start_new_session=True, close_fds=True)
        os.close(gate_r)
        gate_r = -1
        drains = [_Drain(proc.stdout, stdout_sink), _Drain(proc.stderr)]
        for d in drains:
            d.start()
        ident = identity_of(proc.pid)
        if ident is None or ident["pgid"] != proc.pid:
            raise RuntimeError("identity")                     # unreadable, or not its own group leader: never released
        res.identity = ident
        record(dict(ident))                                    # durable BEFORE the gate opens; raising keeps it closed
        res.started = True                                     # from here on the command MAY run: set before the write,
        os.write(gate_w, b"go\n")                              # so an interrupted write is never "never executed"
    except BaseException as e:                                 # before `started`: the gate stays closed, nothing ran
        res.runner_error = type(e).__name__
    finally:
        for fd in (gate_r, gate_w):                            # closing the write end unopened is the refusal (EOF)
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
    if proc is None:
        return res
    pgid = proc.pid
    escalate = False
    try:
        try:
            proc.wait(timeout=deadline_s if res.started else reap_s)
        except subprocess.TimeoutExpired:
            res.deadline_exceeded = res.started
        escalate = proc.returncode is None                     # still running at the deadline
        if not escalate and res.started:
            live = members(pgid)
            if live is None:
                escalate = True                                # cannot see the group: signal it rather than assume
            elif live:
                res.survivors = True                           # the leader is gone, something of its group is not
                escalate = _wait_empty(pgid, linger_s, sleep) is not True
    except BaseException as e:                                 # interrupted while waiting: the command must still end
        res.runner_error = res.runner_error or type(e).__name__
        escalate = True
    # Termination, reaping and the final group check are on a path that an interruption cannot skip: an interrupted
    # attempt is followed by another, which does not repeat a signal already sent (so an interruption during the grace
    # time leads straight to KILL). The first error's class is the one reported.
    for _attempt in range(4):
        try:
            _finish(res, proc, pgid, drains, escalate, grace_s, reap_s, sleep)
            break
        except BaseException as e:
            res.runner_error = res.runner_error or type(e).__name__
            res.group_empty = None
            escalate = True
    return res


def _finish(res: Result, proc, pgid: int, drains: List[_Drain], escalate: bool, grace_s: float, reap_s: float,
            sleep: Callable[[float], None]) -> None:
    """End the group if asked, reap the leader, check the group, collect the output counters. Safe to call again."""
    if escalate and "TERM" not in res.signals_sent:
        _signal_group(pgid, signal.SIGTERM)
        res.signals_sent.append("TERM")
        end = time.monotonic() + grace_s
        while time.monotonic() < end and (proc.poll() is None or members(pgid) != []):
            sleep(0.05)
    if escalate and "KILL" not in res.signals_sent and (proc.poll() is None or members(pgid) != []):
        _signal_group(pgid, signal.SIGKILL)
        res.signals_sent.append("KILL")
    try:
        proc.wait(timeout=reap_s)
    except subprocess.TimeoutExpired:
        pass
    if proc.returncode is not None:
        res.exit_kind = "signaled" if proc.returncode < 0 else "exited"
        res.exit_status = -proc.returncode if proc.returncode < 0 else proc.returncode
        if not res.started and res.exit_kind == "exited" and res.exit_status == GATE_REFUSED:
            res.exit_kind, res.exit_status = "never-executed", None       # only when the gate was provably never opened
    res.group_empty = _wait_empty(pgid, reap_s, sleep)
    for d in drains:
        d.join(timeout=reap_s if res.group_empty else 0.5)
    res.output_complete = all(d.done for d in drains)
    res.stdout_bytes, res.stdout_sha256 = drains[0].count, drains[0].digest.hexdigest()
    res.stderr_bytes, res.stderr_sha256 = drains[1].count, drains[1].digest.hexdigest()
