"""Strict request-rate history for forecast contract v2: one signal's RPS on a parameterized grid, with receipt times.

The v1 reader (data.history) stays as it is: a fixed 600 s grid in req/min for the legacy service. This reader serves
v2 and differs where v2 needs it:
  - the grid Δ is a v2 cadence (contracts.ALLOWED_INTERVALS: 1..10 minute steps that divide a UTC day), and values
    stay in requests/second (no conversion);
  - the query is the autoscaler's compiled one, taken from a ResolvedSignal (api.identity): there is no parameter for a
    query string, another workload or caller-supplied samples;
  - only finalized samples are read: at wall time t the newest grid point is the watermark floor((t - settle)/Δ)*Δ, so
    the newest point may be settle + Δ old, and input older than settle + 2Δ is not fresh;
  - long windows (up to 28 days) are read in deterministic, contiguous, non-overlapping chunks of at most
    MAX_POINTS_PER_CHUNK points (below VictoriaMetrics' default per-series point limit). One deadline bounds the whole
    read, each chunk has a byte cap and all chunks share a total byte budget;
  - every observed point carries the time its chunk was received (receipt time, epoch ms), and gaps stay gaps: a chunk
    with no series at all is an all-missing chunk (older chunks may predate the workload), never interpolated.
Anything else in an answer (several series, partial answers, off-grid, duplicate or unordered times, points outside
the chunk, negative or non-finite values) refuses the whole read. The transport is the same bounded exchange as v1
(data.bounded_http) with the same outcome mapping: HistoryRefused (HTTP 422) or HistoryUnavailable (HTTP 503).
"""

from __future__ import annotations

import hashlib
import json
import math
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional, Tuple

from api.identity import CONTRACT, ResolvedSignal
from data.bounded_http import BodyTooLarge, TransportFailure, bounded_get
from data.history import HistoryRefused, HistoryUnavailable
from forecasting.contracts import ALLOWED_INTERVALS, MAX_HISTORY_DAYS

DEFAULT_SETTLEMENT_S = 120
DEADLINE_S = 20.0                 # the whole read, every chunk included
MAX_CHUNK_BYTES = 8 << 20
MAX_TOTAL_BYTES = 32 << 20
MAX_POINTS_PER_CHUNK = 10_080     # 7 days at 1 minute; VictoriaMetrics refuses above 30,000 points per series

Fetch = Callable[..., Tuple[int, bytes]]

__all__ = ["HistoryRefused", "HistoryUnavailable", "RpsGrid", "plan_chunks", "read_rps_history", "watermark"]


def _epoch_s(now) -> float:
    """Wall time in epoch seconds. A naive datetime is refused: v2 never guesses a timezone."""
    if isinstance(now, datetime):
        if now.tzinfo is None:
            raise ValueError("naive datetime: pass an aware datetime or epoch seconds")
        return now.astimezone(timezone.utc).timestamp()
    if isinstance(now, (int, float)) and not isinstance(now, bool) and math.isfinite(now):
        return float(now)
    raise ValueError(f"unusable wall time {now!r}")


def watermark(now, interval_s: int, settlement_s: int) -> int:
    """The newest finalized grid point at wall time `now`: floor((now - settle) / Δ) * Δ."""
    return int(math.floor((_epoch_s(now) - settlement_s) / interval_s)) * interval_s


def plan_chunks(start: int, end: int, interval_s: int, max_points: int) -> List[Tuple[int, int]]:
    """[(chunk_start, chunk_end)] covering start..end inclusive on the grid, in order, contiguous, no overlap."""
    if (end - start) % interval_s or end < start or max_points < 1:
        raise ValueError("chunk plan needs grid-aligned bounds and a positive chunk size")
    out, s = [], start
    while s <= end:
        e = min(end, s + (max_points - 1) * interval_s)
        out.append((s, e))
        s = e + interval_s
    return out


@dataclass(frozen=True)
class RpsGrid:
    """One signal's history on its grid: values[i] is the RPS at start + i*Δ, None when missing."""
    query_sha256: str
    interval_seconds: int
    start: int                                  # first grid point (inclusive)
    end: int                                    # last grid point (inclusive): the watermark at read time
    values: Tuple[Optional[float], ...]
    received_at_ms: Tuple[Optional[int], ...]   # when each observed point's answer arrived; None when missing
    read_completed_ms: int
    response_sha256: str                        # over the chunk bodies, in order
    chunks: int

    @property
    def timestamps(self) -> Tuple[int, ...]:
        return tuple(self.start + i * self.interval_seconds for i in range(len(self.values)))

    @property
    def observed_count(self) -> int:
        return sum(v is not None for v in self.values)

    @property
    def missing_count(self) -> int:
        return len(self.values) - self.observed_count

    @property
    def observed_fraction(self) -> float:
        return self.observed_count / len(self.values) if self.values else 0.0

    def latest_observed(self) -> Optional[int]:
        for i in range(len(self.values) - 1, -1, -1):
            if self.values[i] is not None:
                return self.start + i * self.interval_seconds
        return None

    def is_fresh(self, now, settlement_s: int) -> bool:
        """The newest observation is no older than settle + 2Δ at `now`. A kept fit never makes stale input fresh."""
        latest = self.latest_observed()
        return latest is not None and _epoch_s(now) - latest <= settlement_s + 2 * self.interval_seconds


def read_rps_history(base_url: str, signal: ResolvedSignal, *, now, interval_s: int, history_seconds: int,
                     settlement_s: int = DEFAULT_SETTLEMENT_S, fetch: Fetch = bounded_get,
                     deadline_s: float = DEADLINE_S, max_chunk_bytes: int = MAX_CHUNK_BYTES,
                     max_total_bytes: int = MAX_TOTAL_BYTES, max_points_per_chunk: int = MAX_POINTS_PER_CHUNK,
                     monotonic: Callable[[], float] = time.monotonic,
                     receipt_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000) -> RpsGrid:
    """The finalized history of `signal` over the last `history_seconds` ending at the watermark of `now`.

    Raises ValueError for unusable parameters, HistoryRefused for an invalid answer, HistoryUnavailable for transport
    failures, the deadline and 5xx/401/403/429 answers."""
    if not isinstance(signal, ResolvedSignal) or signal.contract != CONTRACT or not signal.query:
        raise ValueError("history needs a resolved signal under the requests-per-second/v1 contract")
    if hashlib.sha256(signal.query.encode("utf-8")).hexdigest() != signal.sha256:
        raise ValueError("the signal's query does not match its hash")
    if interval_s not in ALLOWED_INTERVALS:
        raise ValueError(f"interval {interval_s!r} is not one of {ALLOWED_INTERVALS}")
    if (not isinstance(history_seconds, int) or history_seconds < interval_s or history_seconds % interval_s
            or history_seconds > MAX_HISTORY_DAYS * 86400):
        raise ValueError(f"history {history_seconds!r} s must be a multiple of {interval_s} s up to "
                         f"{MAX_HISTORY_DAYS} days")
    if not isinstance(settlement_s, int) or settlement_s < 0:
        raise ValueError("settlement must be a non-negative integer")
    end = watermark(now, interval_s, settlement_s)
    start = end - history_seconds + interval_s
    plan = plan_chunks(start, end, interval_s, max_points_per_chunk)
    n = history_seconds // interval_s
    values: List[Optional[float]] = [None] * n
    received: List[Optional[int]] = [None] * n
    digest, used, t0, last = hashlib.sha256(), 0, monotonic(), None
    for cstart, cend in plan:
        left = deadline_s - (monotonic() - t0)
        if left <= 0:
            raise HistoryUnavailable(f"history read exceeded its {deadline_s:g} s deadline")
        budget = min(max_chunk_bytes, max_total_bytes - used)
        if budget <= 0:
            raise HistoryRefused(f"history exceeds {max_total_bytes} bytes in total")
        params = urllib.parse.urlencode({"query": signal.query, "start": cstart, "end": cend, "step": f"{interval_s}s",
                                         "deny_partial_response": "1"})
        try:
            status, body = fetch(f"{base_url.rstrip('/')}/api/v1/query_range?{params}", deadline_s=left,
                                 max_bytes=budget)
        except BodyTooLarge as e:
            raise HistoryRefused(f"history chunk exceeds its byte budget: {e}") from e
        except TransportFailure as e:
            raise HistoryUnavailable(f"history query failed: {e}") from e
        at_ms = receipt_ms()
        if status >= 500 or status in (401, 403, 429):
            raise HistoryUnavailable(f"metrics backend answered HTTP {status}")
        if status != 200:
            raise HistoryRefused(f"metrics backend refused the query: HTTP {status}")
        used += len(body)
        if used > max_total_bytes:
            raise HistoryRefused(f"history exceeds {max_total_bytes} bytes in total")
        digest.update(len(body).to_bytes(8, "big") + body)
        try:
            doc = json.loads(body)
        except ValueError as e:
            raise HistoryRefused(f"history answer is not JSON: {e}") from e
        for ts, rps in _parse_chunk(doc, cstart, cend, interval_s):
            if last is not None and ts <= last:
                raise HistoryRefused(f"duplicate or unordered sample at {ts}")
            last = ts
            i = (ts - start) // interval_s
            values[i], received[i] = rps, at_ms
    if monotonic() - t0 > deadline_s:
        raise HistoryUnavailable(f"history read exceeded its {deadline_s:g} s deadline")
    return RpsGrid(query_sha256=signal.sha256, interval_seconds=interval_s, start=start, end=end,
                   values=tuple(values), received_at_ms=tuple(received), read_completed_ms=receipt_ms(),
                   response_sha256=digest.hexdigest(), chunks=len(plan))


def _number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _parse_chunk(doc, lo: int, hi: int, interval_s: int) -> List[Tuple[int, float]]:
    if not isinstance(doc, dict) or doc.get("status") != "success":
        status = doc.get("status") if isinstance(doc, dict) else type(doc).__name__
        raise HistoryRefused(f"history query status {status!r}")
    if doc.get("isPartial"):
        raise HistoryRefused("partial history answer")
    data = doc.get("data")
    if not isinstance(data, dict) or data.get("resultType") != "matrix":
        raise HistoryRefused("the answer is not a matrix")
    result = data.get("result")
    if not isinstance(result, list) or len(result) > 1:
        raise HistoryRefused(f"{len(result) if isinstance(result, list) else 'no'} series, want at most one")
    if not result:
        return []                      # no samples in this chunk: every point in it is missing
    if not isinstance(result[0], dict) or not isinstance(result[0].get("values"), list):
        raise HistoryRefused("series without values")
    out, last = [], None
    for sample in result[0]["values"]:
        if not (isinstance(sample, list) and len(sample) == 2):
            raise HistoryRefused(f"malformed sample {sample!r}")
        ts, raw = sample
        if not _number(ts) or not math.isfinite(ts) or ts != int(ts):
            raise HistoryRefused(f"malformed sample timestamp {ts!r}")
        ts = int(ts)
        if ts % interval_s:
            raise HistoryRefused(f"sample at {ts} is off the {interval_s} s grid")
        if not lo <= ts <= hi:
            raise HistoryRefused(f"sample at {ts} is outside its chunk {lo}..{hi}")
        if last is not None and ts <= last:
            raise HistoryRefused(f"duplicate or unordered sample at {ts}")
        last = ts
        if not (isinstance(raw, str) or _number(raw)):
            raise HistoryRefused(f"non-numeric sample {raw!r}")
        try:
            rps = float(raw)
        except (TypeError, ValueError) as e:
            raise HistoryRefused(f"non-numeric sample {raw!r}") from e
        if not math.isfinite(rps) or rps < 0:
            raise HistoryRefused(f"sample {raw!r} is not a finite, non-negative rate")
        out.append((ts, rps))
    return out
