"""Fit functions for the worker tests (run inside spawned fit processes, so they live in an importable module)."""

import os
import signal
import time


class _Unavailable(Exception):
    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason


def sleep_fit(payload):
    time.sleep(payload.get("sleep", 60))
    return b"never promoted"


def quick_fit(payload):
    time.sleep(payload.get("sleep", 0))
    return payload["artifact"]


def hog_fit(payload):
    blocks = [bytearray(64 << 20) for _ in range(64)]      # 4 GiB, far above the limit
    return bytes(len(blocks))


def crash_fit(payload):
    os.kill(os.getpid(), signal.SIGKILL)


def history_fit(payload):
    raise _Unavailable("history", "not enough history")


def bad_reason_fit(payload):
    raise _Unavailable("whatever", "an unknown reason is reported as numeric")


def lingering_fit(payload):
    """Codex's repro: the result is ready, but a non-daemon thread keeps the process from exiting."""
    import threading
    threading.Thread(target=time.sleep, args=(60,), daemon=False).start()
    return payload["artifact"]
