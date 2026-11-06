"""The bounded fit worker: one fit per process, a deadline and a memory limit, atomic promotion of the result.

The API owns fitting (design C2, section 3). A fit is submitted for a signal (its artifact Binding) with an allowlisted
recipe and a bounded payload; the worker runs it in a fresh spawned process, so a stuck or runaway fit can be killed
without touching the service:
  - one active or queued fit per signal: a second submit for the same signal returns the existing ticket;
  - a bounded queue (QueueFull when full, an HTTP 503 for the caller) and `max_active` concurrent fits (1 by default);
  - each fit has ONE absolute wall-clock deadline (30 s) covering the result AND the process's exit, and an
    address-space limit (RLIMIT_AS, 384 MiB) with one BLAS thread. A kill timer fires at the deadline, so neither the
    wait for the result nor the wait for the exit can outlast it. A fit whose process has not exited by the deadline
    (even after sending its result, e.g. a lingering thread) is killed, reaped and NOT promoted. The slot is released
    only after the process has been reaped: cancelled work never runs alongside the next fit;
  - shutdown is synchronized with admission, start, cancellation and publication: a child is started, and an artifact
    published, only under the worker's lock and only while it is open. close() fails every queued ticket, kills
    running children and returns only after every dequeued ticket is resolved, so no child starts and no artifact is
    published after it returns;
  - only allowlisted recipes run, by dotted name, imported inside the fit process; this module imports no NumPy, so a
    fit process applies its limits before the numerical stack loads (unless the main module already loaded it);
  - a successful fit returns artifact bytes; the parent validates and promotes them through ArtifactStore (atomic
    rename, never an older cutoff over a newer one). A failure keeps whatever was promoted before.
Failures carry a bounded reason (history, numeric, timeout, resource, storage) for fit_failures_total.
"""

from __future__ import annotations

import collections
import dataclasses
import importlib
import multiprocessing
import os
import resource
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, Optional

FITS = {"e1-product/v1": "forecasting.artifacts:fit_e1_artifact"}
FAILURE_REASONS = ("history", "numeric", "timeout", "resource", "storage")    # = statistical.FAILURE_REASONS
DEADLINE_S = 30.0
MEMORY_BYTES = 384 << 20
MAX_PENDING = 8
MAX_PAYLOAD_BYTES = 28 * 1440 * 8 + 4096                                     # 28 days at 1 minute, float64
_THREAD_ENV = ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS")


class QueueFull(Exception):
    pass


class FitRefused(Exception):
    """A submit that cannot be admitted (unknown recipe, oversized payload, worker closed)."""


@dataclass(frozen=True)
class FitResult:
    signal_key: str
    recipe: str
    status: str                       # promoted | unchanged | failed
    reason: Optional[str] = None      # one of FAILURE_REASONS when failed
    message: str = ""
    artifact_sha256: Optional[str] = None
    started: float = 0.0              # worker clock
    finished: float = 0.0
    cpu_seconds: Optional[float] = None
    max_rss_kib: Optional[int] = None


@dataclass
class Ticket:
    signal_key: str
    recipe: str
    binding: object
    payload: Dict
    done: threading.Event = field(default_factory=threading.Event)
    result: Optional[FitResult] = None


_ENV_LOCK = threading.Lock()


@contextmanager
def _one_thread_environment():
    """Force one BLAS/OpenMP thread in the environment a spawned fit process starts with (overriding any inherited
    value), then restore the parent's own values. Spawning copies the environment synchronously inside start()."""
    with _ENV_LOCK:
        saved = {name: os.environ.get(name) for name in _THREAD_ENV}
        try:
            for name in _THREAD_ENV:
                os.environ[name] = "1"
            yield
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def _child(conn, ref: str, payload: Dict, memory_bytes: int) -> None:
    """Fit process entry: limits first, then the allowlisted recipe; one message back, then exit."""
    for name in _THREAD_ENV:
        os.environ[name] = "1"
    try:
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
        module, func = ref.split(":")
        data = getattr(importlib.import_module(module), func)(payload)
        if not isinstance(data, bytes):
            raise TypeError("a fit returns artifact bytes")
        ru = resource.getrusage(resource.RUSAGE_SELF)
        msg = ("ok", data, ru.ru_utime + ru.ru_stime, ru.ru_maxrss)
    except MemoryError:
        msg = ("error", "resource", "the fit exceeded its memory limit")
    except Exception as e:                                    # a bounded reason, never the traceback's objects
        reason = getattr(e, "reason", None)
        msg = ("error", reason if reason in FAILURE_REASONS else "numeric", f"{type(e).__name__}: {e}"[:500])
    try:
        conn.send(msg)
    finally:
        conn.close()


class FitWorker:
    def __init__(self, store, *, fits: Optional[Dict[str, str]] = None, deadline_s: float = DEADLINE_S,
                 memory_bytes: int = MEMORY_BYTES, max_pending: int = MAX_PENDING, max_active: int = 1,
                 clock: Callable[[], float] = time.monotonic, promote: Optional[Callable] = None):
        self.store, self.fits = store, dict(FITS if fits is None else fits)
        self.deadline_s, self.memory_bytes, self.max_pending = deadline_s, memory_bytes, max_pending
        self.clock = clock
        self._promote = promote or self._promote_artifact
        self._ctx = multiprocessing.get_context("spawn")
        self._cv = threading.Condition()
        self._queue: Deque[Ticket] = collections.deque()
        self._by_signal: Dict[str, Ticket] = {}
        self._running: Dict[str, object] = {}         # signal key -> its live fit process
        self._closed = False
        self._hook: Optional[Callable[[str, Ticket], None]] = None    # tests: pause at "dequeued" / "publish"
        self.failures: Dict[str, int] = {r: 0 for r in FAILURE_REASONS}
        self._threads = [threading.Thread(target=self._loop, name=f"fit-worker-{i}", daemon=True)
                         for i in range(max_active)]
        for t in self._threads:
            t.start()

    # --- admission ----------------------------------------------------------------------------------------------

    def submit(self, binding, recipe: str, payload: Dict) -> Ticket:
        if recipe not in self.fits or getattr(binding, "recipe", None) != recipe:
            raise FitRefused(f"recipe {recipe!r} is not allowlisted for this binding")
        values = payload.get("values") if isinstance(payload, dict) else None
        if not isinstance(values, bytes) or len(values) > MAX_PAYLOAD_BYTES:
            raise FitRefused("the fit payload is missing or above its size bound")
        if payload.get("binding") != dataclasses.asdict(binding):
            raise FitRefused("the fit payload is bound to another signal")
        key = binding.key()
        with self._cv:
            if self._closed:
                raise FitRefused("the worker is closed")
            if key in self._by_signal:                 # one active or queued fit per signal
                return self._by_signal[key]
            if len(self._queue) >= self.max_pending:
                raise QueueFull(f"{self.max_pending} fits are already queued")
            t = Ticket(key, recipe, binding, payload)
            self._queue.append(t)
            self._by_signal[key] = t
            self._cv.notify()
            return t

    def busy(self) -> bool:
        """A fit is queued or running."""
        with self._cv:
            return bool(self._by_signal)

    def wait(self, ticket: Ticket, timeout: Optional[float] = None) -> FitResult:
        if not ticket.done.wait(timeout):
            raise TimeoutError("the fit has not finished")
        return ticket.result

    def close(self) -> None:
        """Stop: queued fits fail (reason resource), running ones are killed; when this returns, every ticket is resolved
        and nothing more is started or promoted."""
        with self._cv:
            self._closed = True
            dropped = list(self._queue)
            self._queue.clear()
            for t in dropped:
                del self._by_signal[t.signal_key]
            self._cv.notify_all()
            for p in list(self._running.values()):
                p.kill()
        for t in dropped:
            t.result = FitResult(t.signal_key, t.recipe, "failed", "resource", "the worker was closed")
            t.done.set()
        for th in self._threads:             # bounded: children are killed, publication refuses once closed
            th.join()

    # --- execution ------------------------------------------------------------------------------------------------

    def _loop(self) -> None:
        while True:
            with self._cv:
                while not self._queue and not self._closed:
                    self._cv.wait()
                if self._closed:
                    return
                t = self._queue.popleft()
            try:
                if self._hook:
                    self._hook("dequeued", t)
                result = self._run(t)
            except Exception as e:                     # never lose a ticket
                result = FitResult(t.signal_key, t.recipe, "failed", "storage", f"{type(e).__name__}: {e}")
            with self._cv:
                if result.status == "failed":
                    self.failures[result.reason] += 1
                t.result = result
                del self._by_signal[t.signal_key]      # the signal may be submitted again only now
            t.done.set()

    def _closed_result(self, t: Ticket, stage: str, **extra) -> FitResult:
        return FitResult(t.signal_key, t.recipe, "failed", "resource", f"the worker was closed {stage}", **extra)

    def _run(self, t: Ticket) -> FitResult:
        recv, send = self._ctx.Pipe(duplex=False)
        p = self._ctx.Process(target=_child, args=(send, self.fits[t.recipe], t.payload, self.memory_bytes),
                              daemon=True)
        with self._cv:                                 # start only while open, and visible to close() at once
            if self._closed:
                send.close()
                recv.close()
                return self._closed_result(t, "before the fit started")
            started = self.clock()
            deadline = time.monotonic() + self.deadline_s
            with _one_thread_environment():   # the child inherits it before importing anything (a main module
                p.start()                     # that loads NumPy would otherwise start inherited BLAS threads)
            self._running[t.signal_key] = p
        send.close()
        expired = threading.Event()

        def expire():
            expired.set()
            p.kill()                                   # ends a pending recv (EOF) and a pending join alike

        timer = threading.Timer(max(0.0, deadline - time.monotonic()), expire)
        timer.daemon = True
        timer.start()
        msg = None
        try:
            if recv.poll(max(0.0, deadline - time.monotonic())):
                try:
                    msg = recv.recv()
                except (EOFError, OSError):
                    msg = None
            p.join(max(0.0, deadline - time.monotonic()))
            if p.is_alive():                           # the result is not enough: the process must also be gone
                expire()
        finally:
            p.join()                                   # reaped (after a kill this returns promptly)
            timer.cancel()
            recv.close()
            with self._cv:
                self._running.pop(t.signal_key, None)
        finished = self.clock()
        base = dict(signal_key=t.signal_key, recipe=t.recipe, started=started, finished=finished)
        if expired.is_set() or time.monotonic() > deadline:
            return FitResult(**base, status="failed", reason="timeout",
                             message="the fit did not finish and exit within its deadline")
        if msg is None or p.exitcode != 0:
            if msg is not None and msg[0] == "error":
                return FitResult(**base, status="failed", reason=msg[1], message=msg[2])
            return FitResult(**base, status="failed", reason="resource",
                             message=f"fit process exited {p.exitcode} without a clean result")
        if msg[0] == "error":
            return FitResult(**base, status="failed", reason=msg[1], message=msg[2])
        _, data, cpu, rss = msg
        if self._hook:
            self._hook("publish", t)
        with self._cv:                                 # publish only while open; close() waits for this to finish
            if self._closed:
                return self._closed_result(t, "before publication", started=started, finished=finished)
            try:
                sha, promoted = self._promote(t, data)
            except Exception as e:
                return FitResult(**base, status="failed", reason="storage",
                                 message=f"{type(e).__name__}: {e}"[:500], cpu_seconds=cpu, max_rss_kib=rss)
        return FitResult(**base, status="promoted" if promoted else "unchanged", artifact_sha256=sha,
                         cpu_seconds=cpu, max_rss_kib=rss)

    def _promote_artifact(self, t: Ticket, data: bytes):
        from forecasting.statistical import KernelSettings
        return self.store.promote(t.binding, data, KernelSettings(**t.payload["kernel"]))
