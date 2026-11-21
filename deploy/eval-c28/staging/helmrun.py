"""helmrun.py -- the bounded subprocess runner of release.py (no Helm knowledge, no Kubernetes access).

One external command, run once, in a session and process group of its own:

- **Nothing executes before its identity is durable.** The child is a trampoline that waits on a pipe. The caller's
  `record_identity` gets the group's identity (pid = pgid, the process start time, host, boot id) and must make it
  durable; only then is the trampoline released and executes the command. If `record_identity` raises, or this process
  dies first, the trampoline sees the pipe close and exits without executing anything.
- **A deadline, then TERM, then KILL** for the whole group; the child is reaped and the group is then PROVEN empty (no
  process with that group id). `group_empty: false` is an answer the caller must treat as "something may still act".
- **Output is counted, never kept.** stdout and stderr are drained in fixed-size chunks into byte counters and sha256;
  a caller that must parse an answer asks for a bounded capture of stdout, which is returned on the result object and
  is not part of `summary()`. No exception of this module carries output.
- **The environment is a whitelist** (PATH, HOME, LANG): no HELM_* variable, no KUBECONFIG, nothing else of the caller.

`group_gone(identity)` is the same proof for a later process (recovery): the recorded group has no process left."""

from __future__ import annotations

import hashlib
import os
import selectors
import signal
import socket
import subprocess
import sys
import time
from typing import Callable, Dict, List, Optional, Tuple

import common_records as C

CHUNK = 65536
ENV_KEEP = ("PATH", "HOME", "LANG")
NOT_RELEASED, NOT_EXECUTED = 97, 96
# argv: <fd of the gate> <absolute command> [arguments...]. It executes the command only after reading the one byte
# the parent writes once the identity is durable; a closed pipe (the parent failed or died) ends it.
TRAMPOLINE = """
import os, sys
fd = int(sys.argv[1])
try:
    go = os.read(fd, 1)
except OSError:
    go = b""
os.close(fd)
if go != b"1":
    os._exit(%d)
try:
    os.execv(sys.argv[2], sys.argv[2:])
except OSError:
    os._exit(%d)
""" % (NOT_RELEASED, NOT_EXECUTED)


def environment(source: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """The child's whole environment: the whitelist, taken from the caller's."""
    source = os.environ if source is None else source
    return {k: source[k] for k in ENV_KEEP if k in source}


def identity(pid: int) -> Dict:
    """The identity of the group a child leads (start_new_session: pgid == pid)."""
    return {"pid": pid, "pgid": pid, "pid_start": C.process_start(pid), "host": socket.gethostname(),
            "boot_id": C._boot_id()}


def group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def prove_group_empty(pgid: int, seconds: float, now: Callable[[], float] = time.monotonic,
                      sleep: Callable[[float], None] = time.sleep) -> bool:
    end = now() + seconds
    while group_exists(pgid):
        if now() >= end:
            return False
        sleep(0.05)
    return True


def group_gone(ident: Dict) -> Tuple[bool, str]:
    """For a LATER process: whether the recorded group certainly has no process left. Another host or boot cannot be
    checked from here. A process id is never reused while a group with that id has members, so a process that now has
    the leader's pid with another start time proves that the recorded group emptied."""
    if ident.get("host") != socket.gethostname():
        return False, f"the command ran on host {ident.get('host')!r}: verify there"
    if ident.get("boot_id") != C._boot_id():
        return False, "the command ran in another boot of this host: its group cannot be checked"
    try:
        pgid = int(ident["pgid"])
    except (KeyError, TypeError, ValueError):
        return False, "the recorded identity names no group"
    start = C.process_start(pgid)
    if start is not None and start != ident.get("pid_start"):
        return True, "the leader's pid belongs to a later process"
    if group_exists(pgid):
        return False, f"a process of group {pgid} still exists"
    return True, "group empty"


class _Count:
    def __init__(self, keep: int):
        self.n, self.h, self.keep, self.kept, self.overflow = 0, hashlib.sha256(), keep, bytearray(), False

    def add(self, data: bytes) -> None:
        self.n += len(data)
        self.h.update(data)
        if self.keep:
            room = self.keep - len(self.kept)
            self.kept += data[:max(room, 0)]
            self.overflow = self.overflow or len(data) > room

    def summary(self) -> Dict:
        return {"bytes": self.n, "sha256": self.h.hexdigest()}


class Result:
    """What a run established. `summary()` is safe to journal; `stdout` (the bounded capture, when asked for) is not
    part of it and must be parsed and dropped by the caller."""

    def __init__(self, ident: Dict, end: str, returncode: Optional[int], group_empty: bool, output_complete: bool,
                 out: _Count, err: _Count, seconds: float):
        self.identity, self.end, self.returncode, self.group_empty = ident, end, returncode, group_empty
        self.output_complete, self.seconds = output_complete, seconds
        self._out, self._err = out, err
        self.stdout: Optional[bytes] = bytes(out.kept) if out.keep and not out.overflow else None
        self.capture_overflow = out.overflow

    def clean_exit(self) -> bool:
        """The command ended by itself with status 0, its whole output was read and nothing of its group is left."""
        return self.end == "exited" and self.returncode == 0 and self.group_empty and self.output_complete

    def summary(self) -> Dict:
        return {"identity": dict(self.identity), "end": self.end, "returncode": self.returncode,
                "group_empty": self.group_empty, "output_complete": self.output_complete,
                "stdout": self._out.summary(), "stderr": self._err.summary(), "seconds": round(self.seconds, 3)}


def _kill(pgid: int, sig: int) -> None:
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass


def run(argv: List[str], *, deadline_s: float, record_identity: Optional[Callable[[Dict], None]] = None,
        capture_stdout: int = 0, term_grace_s: float = 10.0, settle_s: float = 5.0, empty_wait_s: float = 10.0,
        env: Optional[Dict[str, str]] = None, cwd: str = "/") -> Result:
    """Run `argv` (argv[0] absolute) once. Raises whatever `record_identity` raises, after ending the unreleased
    trampoline: then the command certainly never executed. Otherwise it returns a Result, whatever happened."""
    if not argv or not os.path.isabs(argv[0]):
        raise C.StagingError("the command must be given by an absolute path")
    gate_r, gate_w = os.pipe()
    try:
        p = subprocess.Popen([sys.executable, "-I", "-S", "-c", TRAMPOLINE, str(gate_r), *argv],
                             stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             start_new_session=True, pass_fds=(gate_r,), close_fds=True, cwd=cwd,
                             env=environment() if env is None else dict(env))
    except BaseException:
        os.close(gate_w)
        raise
    finally:
        os.close(gate_r)
    ident, released = identity(p.pid), False
    try:
        if record_identity is not None:
            record_identity(dict(ident))
        os.write(gate_w, b"1")
        released = True
    finally:
        os.close(gate_w)
        if not released:                                   # never released: nothing was executed; end the trampoline
            _kill(p.pid, signal.SIGKILL)
            p.wait()
            p.stdout.close()
            p.stderr.close()
            prove_group_empty(p.pid, empty_wait_s)
    out, err = _Count(capture_stdout), _Count(0)
    counts = {p.stdout.fileno(): out, p.stderr.fileno(): err}
    sel = selectors.DefaultSelector()
    for fd in counts:
        os.set_blocking(fd, False)
        sel.register(fd, selectors.EVENT_READ)
    start, end, term_at, exited_at, open_ = time.monotonic(), "exited", None, None, len(counts)
    try:
        while True:
            t = time.monotonic()
            alive = p.poll() is None
            if alive and end == "exited" and t - start >= deadline_s:
                _kill(p.pid, signal.SIGTERM)
                end, term_at = "terminated", t
            elif alive and end == "terminated" and t - term_at >= term_grace_s:
                _kill(p.pid, signal.SIGKILL)
                end = "killed"
            if not alive:
                exited_at = t if exited_at is None else exited_at
                if open_ == 0 or t - exited_at >= settle_s:    # all read, or someone else still holds the pipes
                    break
            if open_ == 0:
                time.sleep(0.05)
                continue
            for key, _ in sel.select(timeout=0.2):
                try:
                    data = os.read(key.fd, CHUNK)
                except BlockingIOError:
                    continue
                if data:
                    counts[key.fd].add(data)
                else:
                    sel.unregister(key.fd)
                    open_ -= 1
    finally:
        if p.poll() is None:                               # an error of this loop: never leave the group behind
            _kill(p.pid, signal.SIGKILL)
            end = "killed"
        p.wait()
        if end != "exited" and group_exists(p.pid):        # we decided to end the group: no member survives its leader
            _kill(p.pid, signal.SIGKILL)
        empty = prove_group_empty(p.pid, empty_wait_s)
        sel.close()
        p.stdout.close()
        p.stderr.close()
    return Result(ident, end, p.returncode, empty, open_ == 0, out, err, time.monotonic() - start)
