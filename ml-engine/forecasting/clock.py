"""Receipt clocks for forecast contract v2: the one source of 'when did this arrive / when was this issued' times.

Every receipt, issue and commit time in the journal is integer epoch milliseconds from a clock here, never a value the
caller supplied. ReceiptClock reads the wall clock but never goes backwards (a stepped-back wall clock repeats the last
time instead), so journal order and receipt order agree. ManualClock is the deterministic clock of the offline replay:
it moves only when told, and only forwards.
"""

from __future__ import annotations

import threading
import time
from typing import Callable


class ClockError(Exception):
    pass


class ReceiptClock:
    """Monotone wall-clock milliseconds."""

    def __init__(self, source: Callable[[], int] = lambda: time.time_ns() // 1_000_000):
        self._source, self._last, self._lock = source, None, threading.Lock()

    def now_ms(self) -> int:
        v = self._source()
        if not isinstance(v, int) or isinstance(v, bool) or v < 0:
            raise ClockError(f"clock source returned {v!r}")
        with self._lock:
            self._last = v if self._last is None else max(self._last, v)
            return self._last


class ManualClock:
    """A replay clock: deterministic, forward-only."""

    def __init__(self, start_ms: int = 0):
        if not isinstance(start_ms, int) or isinstance(start_ms, bool) or start_ms < 0:
            raise ClockError(f"bad start {start_ms!r}")
        self._now = start_ms

    def now_ms(self) -> int:
        return self._now

    def set(self, ms: int) -> None:
        if not isinstance(ms, int) or isinstance(ms, bool) or ms < self._now:
            raise ClockError(f"the replay clock cannot move from {self._now} to {ms!r}")
        self._now = ms

    def advance(self, ms: int) -> None:
        self.set(self._now + ms)
