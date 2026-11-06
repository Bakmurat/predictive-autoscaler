"""Bounded admission for the forecasting services' request work (extracted for the statistical service, C2.6).

A request takes a slot before any Kubernetes, history or journal work and keeps it until its worker thread has really
finished: the slot is released by the submitted future's done callback, never by the awaiting coroutine, so a cancelled
or timed-out request can neither leak a slot nor free one while its work still runs. A full house is refused at once
(Busy, an HTTP 503 for the caller). The same rule as the legacy service's _run_admitted (api/main.py).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading


class Busy(Exception):
    """No admission slot is free (HTTP 503)."""


class Admission:
    def __init__(self, slots: int = 4, thread_name_prefix: str = "forecast"):
        if slots < 1:
            raise ValueError("admission needs at least one slot")
        self.slots = slots
        self._sem = threading.BoundedSemaphore(slots)
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=slots,
                                                               thread_name_prefix=thread_name_prefix)

    async def run(self, fn, *args):
        if not self._sem.acquire(blocking=False):
            raise Busy("the service is at capacity")
        try:
            fut = self._executor.submit(fn, *args)
        except BaseException:
            self._sem.release()          # never submitted, so no callback will release it
            raise
        fut.add_done_callback(lambda _f: self._sem.release())
        return await asyncio.wrap_future(fut)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)
