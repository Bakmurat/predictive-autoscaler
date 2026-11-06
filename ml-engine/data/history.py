"""Strict request-rate history for the forecasting service (metric contract requests-per-second/v1).

The query is the PredictiveAutoscaler's compiled query (status.metricSource), resolved by the caller; nothing here
accepts a query from a request body. The window is server-controlled: HISTORY_HOURS hours ending at the last 10-minute
boundary, sampled on that grid. The answer must be one complete series of finite, non-negative per-second rates on the
grid; anything else is refused rather than repaired. Gaps (missing samples) are kept as gaps; the downstream gap policy
(gapfill) handles them as before. The one req/s -> req/min conversion of the service happens here. The exchange is
bounded as a whole (data.bounded_http: a hard deadline over headers and body, and a body cap).
"""

from __future__ import annotations

import json
import math
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple

from data.bounded_http import BodyTooLarge, TransportFailure, bounded_get

STEP_SECONDS = 600
HISTORY_HOURS = 168
DEADLINE_S = 20.0
MAX_RESPONSE_BYTES = 8 << 20

Fetch = Callable[..., Tuple[int, bytes]]


class HistoryRefused(Exception):
    """The metrics backend answered, but not with one valid series (a signal problem: refuse, HTTP 422)."""


class HistoryUnavailable(Exception):
    """The metrics backend could not be reached or did not answer in time (transient: HTTP 503)."""


def _as_utc(now: datetime) -> datetime:
    # Naive inputs are UTC by definition here; aware inputs are converted, never relabelled.
    return now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)


def _grid_end(now: datetime) -> datetime:
    epoch = int(_as_utc(now).timestamp())
    return datetime.fromtimestamp((epoch // STEP_SECONDS) * STEP_SECONDS, tz=timezone.utc)


def query_history(base_url: str, query: str, *, now: Optional[datetime] = None, hours: int = HISTORY_HOURS,
                  fetch: Fetch = bounded_get, deadline_s: float = DEADLINE_S,
                  max_bytes: int = MAX_RESPONSE_BYTES) -> List[Dict]:
    """Return [{"timestamp": naive-UTC ISO, "value": requests per minute}, ...] on the 10-minute grid.

    Raises HistoryRefused (the answer is not one valid series) or HistoryUnavailable (transport, deadline, 5xx).
    """
    if not isinstance(query, str) or not query.strip():
        raise HistoryRefused("empty query")
    end = _grid_end(now or datetime.now(timezone.utc))
    start = end - timedelta(hours=hours)
    params = urllib.parse.urlencode({"query": query, "start": int(start.timestamp()), "end": int(end.timestamp()),
                                     "step": f"{STEP_SECONDS}s", "deny_partial_response": "1"})
    try:
        status, body = fetch(f"{base_url.rstrip('/')}/api/v1/query_range?{params}", deadline_s=deadline_s,
                             max_bytes=max_bytes)
    except BodyTooLarge as e:
        raise HistoryRefused(str(e)) from e
    except TransportFailure as e:
        raise HistoryUnavailable(f"history query failed: {e}") from e
    if status >= 500 or status in (401, 403, 429):
        raise HistoryUnavailable(f"metrics backend answered HTTP {status}")
    if status != 200:
        raise HistoryRefused(f"metrics backend refused the query: HTTP {status}")
    try:
        doc = json.loads(body)
    except ValueError as e:
        raise HistoryRefused(f"history answer is not JSON: {e}") from e
    return _parse_series(doc, start, end)


def _number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _parse_series(doc, start: datetime, end: datetime) -> List[Dict]:
    if not isinstance(doc, dict) or doc.get("status") != "success":
        status = doc.get("status") if isinstance(doc, dict) else type(doc).__name__
        raise HistoryRefused(f"history query status {status!r}")
    if doc.get("isPartial"):
        raise HistoryRefused("partial history answer")
    data = doc.get("data")
    if not isinstance(data, dict) or data.get("resultType") != "matrix":
        raise HistoryRefused("the answer is not a matrix")
    result = data.get("result")
    if not isinstance(result, list) or len(result) != 1 or not isinstance(result[0], dict):
        raise HistoryRefused(f"{len(result) if isinstance(result, list) else 'no'} series, want exactly one")
    values = result[0].get("values")
    if not isinstance(values, list):
        raise HistoryRefused("series without values")
    lo, hi = start.timestamp(), end.timestamp()
    out, last = [], None
    for sample in values:
        if not (isinstance(sample, list) and len(sample) == 2):
            raise HistoryRefused(f"malformed sample {sample!r}")
        ts, raw = sample
        if not _number(ts) or not math.isfinite(ts):
            raise HistoryRefused(f"malformed sample timestamp {ts!r}")
        if ts != int(ts) or int(ts) % STEP_SECONDS != 0:
            raise HistoryRefused(f"sample at {ts} is off the {STEP_SECONDS} s grid")
        if not (lo <= ts <= hi):
            raise HistoryRefused(f"sample at {ts} is outside the requested window")
        if last is not None and ts <= last:
            raise HistoryRefused(f"duplicate or unordered sample at {ts}")
        last = ts
        if not (isinstance(raw, str) or _number(raw)):
            raise HistoryRefused(f"non-numeric sample {raw!r}")
        try:
            rps = float(raw)
        except (TypeError, ValueError) as e:
            raise HistoryRefused(f"non-numeric sample {raw!r}") from e
        rpm = rps * 60.0  # the one req/s -> req/min conversion of the service
        if not math.isfinite(rps) or rps < 0 or not math.isfinite(rpm):
            raise HistoryRefused(f"sample {raw!r} is not a finite, non-negative rate")
        out.append({"timestamp": datetime.fromtimestamp(int(ts), tz=timezone.utc).replace(tzinfo=None).isoformat(),
                    "value": rpm})
    return out
