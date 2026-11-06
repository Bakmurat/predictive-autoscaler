"""C2.1: the v2 RPS history reader: parameterized grids, settlement, chunked bounded reads, receipts, missingness."""

import dataclasses
import inspect
import json
import os
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.bounded_http import BodyTooLarge, DeadlineExceeded, TransportFailure  # noqa: E402
from data.history import query_history  # noqa: E402
from forecasting import history as h  # noqa: E402
from forecasting.history import HistoryRefused, HistoryUnavailable, read_rps_history  # noqa: E402
from tests.b4_helpers import make_signal  # noqa: E402

SIG = make_signal(namespace="shop", name="web")
DAY = 86400
NOW = datetime(2026, 11, 6, 12, 3, 30, tzinfo=timezone.utc)
NOW_S = int(NOW.timestamp())


class SeriesFetch:
    """A fake backend answering query_range from a function of the grid time (None = no sample)."""

    def __init__(self, value=lambda ts: ts % 7 / 2, body=None, status=200, exc=None, series=1, receipt=None):
        self.value, self.body, self.status, self.exc, self.series = value, body, status, exc, series
        self.calls = []

    def __call__(self, url, **kw):
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        self.calls.append((url, q, kw))
        if self.exc:
            raise self.exc
        if self.body is not None:
            body = self.body(q) if callable(self.body) else self.body
            return self.status, body
        step = int(q["step"].rstrip("s"))
        vals = [[ts, str(v)] for ts in range(int(q["start"]), int(q["end"]) + 1, step)
                if (v := self.value(ts)) is not None]
        result = [{"metric": {}, "values": vals} for _ in range(self.series)] if vals else []
        return self.status, json.dumps({"status": "success", "data": {"resultType": "matrix", "result": result}}).encode()


def read(fetch, **kw):
    args = dict(now=NOW, interval_s=300, history_seconds=DAY, receipt_ms=lambda: 42)
    args.update(kw)
    return read_rps_history("http://vm/", SIG, fetch=fetch, **args)


# --- grids and settlement ---------------------------------------------------------------------------------------

@pytest.mark.parametrize("interval", [60, 300, 600, 480, 120, 180, 240, 360])
def test_grid_bounds_and_values_for_each_cadence(interval):
    f = SeriesFetch()
    g = read(f, interval_s=interval, history_seconds=interval * 288, settlement_s=120)
    wm = (NOW_S - 120) // interval * interval
    assert (g.end, g.start, g.interval_seconds) == (wm, wm - interval * 287, interval)
    assert len(g.values) == 288 and g.timestamps[-1] == wm and all(t % interval == 0 for t in g.timestamps)
    assert g.values == tuple(ts % 7 / 2 for ts in g.timestamps)          # RPS as answered: no req/min conversion
    url, q, kw = f.calls[0]
    assert url.startswith("http://vm/api/v1/query_range?") and q["step"] == f"{interval}s"
    assert q["query"] == SIG.query and q["deny_partial_response"] == "1"
    assert g.query_sha256 == SIG.sha256 and g.missing_count == 0 and g.observed_fraction == 1.0


@pytest.mark.parametrize("interval", [420, 540, 30, 900, 3600, 0, "300"])
def test_cadences_off_the_daily_grid_are_refused(interval):
    with pytest.raises(ValueError):
        read(SeriesFetch(), interval_s=interval, history_seconds=DAY)


@pytest.mark.parametrize("history", [0, 299, DAY + 1, 29 * DAY, "86400"])
def test_history_length_must_fit_the_grid_and_the_28_day_cap(history):
    with pytest.raises(ValueError):
        read(SeriesFetch(), history_seconds=history)


@pytest.mark.parametrize("now,settle,interval,wm", [
    ("12:02:00", 120, 300, "12:00:00"),      # exactly settled
    ("12:01:59", 120, 300, "11:55:00"),      # one second early: the 12:00 point is not final yet
    ("12:03:30", 0, 60, "12:03:00"),
    ("12:03:30", 120, 60, "12:01:00"),
    ("12:09:59", 0, 600, "12:00:00"),
])
def test_watermark_only_admits_settled_points(now, settle, interval, wm):
    t = datetime.fromisoformat(f"2026-11-06T{now}+00:00")
    assert h.watermark(t, interval, settle) == int(datetime.fromisoformat(f"2026-11-06T{wm}+00:00").timestamp())


def test_timezones_convert_and_naive_times_are_refused():
    plus2 = NOW.astimezone(timezone(timedelta(hours=2)))
    minus7 = NOW.astimezone(timezone(timedelta(hours=-7)))
    assert read(SeriesFetch(), now=plus2).end == read(SeriesFetch(), now=minus7).end == read(SeriesFetch(), now=NOW_S).end
    with pytest.raises(ValueError):
        read(SeriesFetch(), now=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError):
        h.watermark(float("nan"), 300, 120)


def test_freshness_is_settlement_plus_two_intervals():
    g = read(SeriesFetch(value=lambda ts: None if ts > NOW_S - 1000 else 1.0), settlement_s=120)
    latest = g.latest_observed()
    assert latest == (NOW_S - 1000) // 300 * 300
    assert g.is_fresh(latest + 120 + 600, 120) and not g.is_fresh(latest + 120 + 601, 120)
    empty = read(SeriesFetch(value=lambda ts: None))
    assert empty.latest_observed() is None and not empty.is_fresh(NOW, 120)


# --- missingness and receipts -----------------------------------------------------------------------------------

def test_gaps_stay_gaps_and_points_carry_their_receipt_time():
    gap = lambda ts: None if (ts // 300) % 10 == 3 else 2.0  # noqa: E731
    clock = iter(range(1000, 2000))
    g = read(SeriesFetch(value=gap), receipt_ms=lambda: next(clock))
    assert g.missing_count == sum(1 for t in g.timestamps if (t // 300) % 10 == 3) > 0
    for v, r in zip(g.values, g.received_at_ms):
        assert (v is None) == (r is None)
    assert {r for r in g.received_at_ms if r is not None} == {1000} and g.read_completed_ms == 1001


def test_an_empty_chunk_is_missing_but_an_empty_read_is_not_an_error():
    g = read(SeriesFetch(value=lambda ts: None))
    assert g.observed_count == 0 and g.values == (None,) * 288 and g.observed_fraction == 0.0


# --- bounded, chunked reads -------------------------------------------------------------------------------------

def test_28_days_at_one_minute_are_read_in_contiguous_chunks():
    f = SeriesFetch(value=lambda ts: 1.0)
    clock = iter(range(10, 100))
    g = read(f, interval_s=60, history_seconds=28 * DAY, receipt_ms=lambda: next(clock))
    spans = [(int(q["start"]), int(q["end"])) for _, q, _ in f.calls]
    assert len(spans) == 4 == g.chunks and spans[0][0] == g.start and spans[-1][1] == g.end
    assert all(e - s <= (h.MAX_POINTS_PER_CHUNK - 1) * 60 for s, e in spans)
    assert all(nxt[0] == prev[1] + 60 for prev, nxt in zip(spans, spans[1:]))     # contiguous, no overlap
    assert len(g.values) == 40320 and g.observed_count == 40320
    assert sorted(set(g.received_at_ms)) == [10, 11, 12, 13]                       # per-chunk receipt
    assert h.plan_chunks(0, 600, 60, 4) == [(0, 180), (240, 420), (480, 600)]


def test_the_chunk_plan_is_deterministic():
    a, b = SeriesFetch(), SeriesFetch()
    read(a, interval_s=60, history_seconds=10 * DAY)
    read(b, interval_s=60, history_seconds=10 * DAY)
    assert [u for u, _, _ in a.calls] == [u for u, _, _ in b.calls]


def test_a_sample_outside_its_chunk_is_refused():
    def body(q):   # every chunk also answers the point just before it: an overlap with the previous chunk
        s = int(q["start"])
        return json.dumps({"status": "success", "data": {"resultType": "matrix", "result": [
            {"values": [[s - 60, "1"], [s, "1"]]}]}}).encode()
    with pytest.raises(HistoryRefused):
        read(SeriesFetch(body=body), interval_s=60, history_seconds=28 * DAY)


def test_one_deadline_bounds_the_whole_read():
    ticks = iter([0.0, 0.0, 9.0, 18.0, 27.0, 36.0])
    f = SeriesFetch()
    with pytest.raises(HistoryUnavailable):
        read(f, interval_s=60, history_seconds=28 * DAY, deadline_s=20.0, monotonic=lambda: next(ticks))
    assert len(f.calls) == 3
    assert [kw["deadline_s"] for _, _, kw in f.calls] == [20.0, 11.0, 2.0]   # each chunk gets only what is left


def test_bytes_are_capped_per_chunk_and_in_total():
    f = SeriesFetch(value=lambda ts: 1.0)
    read(f, interval_s=60, history_seconds=28 * DAY, max_chunk_bytes=1 << 20, max_total_bytes=10 << 20)
    assert all(kw["max_bytes"] == 1 << 20 for _, _, kw in f.calls)
    size = len(SeriesFetch(value=lambda ts: 1.0)(
        "http://vm/x?" + urllib.parse.urlencode({"start": 0, "end": 604740, "step": "60s"}))[1])
    with pytest.raises(HistoryRefused):    # the total budget runs out after two chunks
        read(SeriesFetch(value=lambda ts: 1.0), interval_s=60, history_seconds=28 * DAY, max_total_bytes=2 * size + 10)
    with pytest.raises(HistoryRefused):
        read(SeriesFetch(exc=BodyTooLarge("big")))


# --- strict answers ---------------------------------------------------------------------------------------------

END = (NOW_S - 120) // 300 * 300


def doc(values, series=1, **extra):
    d = {"status": "success", "data": {"resultType": "matrix", "result": [{"values": values}] * series}}
    d.update(extra)
    return json.dumps(d).encode()


@pytest.mark.parametrize("body", [
    doc([[END, "1"]], series=2),                          # several series are never combined
    doc([[END, "1"]], isPartial=True),
    json.dumps({"status": "error"}).encode(),
    json.dumps({"status": "success", "data": {"resultType": "vector", "result": []}}).encode(),
    json.dumps({"status": "success", "data": {"resultType": "matrix", "result": [1]}}).encode(),
    json.dumps({"status": "success", "data": {"resultType": "matrix", "result": [{"values": 7}]}}).encode(),
    doc([[END, True]]), doc([[True, "1"]]), doc([[END + 0.5, "1"]]), doc([[END, None]]),
    doc([[END - 60, "1"]]),                               # off the 300 s grid
    doc([[END, "1"], [END, "2"]]),                        # duplicate
    doc([[END, "1"], [END - 300, "2"]]),                  # unordered
    doc([[END, "-1"]]), doc([[END, "NaN"]]), doc([[END, "+Inf"]]), doc([[END, "x"]]),
    doc([[END + 300, "1"]]),                              # beyond the watermark: not finalized
    doc([[END - DAY, "1"]]),                              # before the window
    doc([[END]]), b"not json", json.dumps([1]).encode(),
])
def test_anything_but_one_valid_series_is_refused(body):
    with pytest.raises(HistoryRefused):
        read(SeriesFetch(body=body))


@pytest.mark.parametrize("fetch,kind", [
    (SeriesFetch(status=503), HistoryUnavailable), (SeriesFetch(status=429), HistoryUnavailable),
    (SeriesFetch(status=401), HistoryUnavailable), (SeriesFetch(status=400), HistoryRefused),
    (SeriesFetch(exc=TransportFailure("refused")), HistoryUnavailable),
    (SeriesFetch(exc=DeadlineExceeded("slow")), HistoryUnavailable),
])
def test_transport_outcomes_map_like_v1(fetch, kind):
    with pytest.raises(kind):
        read(fetch)


# --- one signal, one query --------------------------------------------------------------------------------------

def test_only_the_resolved_signal_is_ever_queried():
    assert "query" not in inspect.signature(read_rps_history).parameters
    f = SeriesFetch()
    read(f, interval_s=60, history_seconds=28 * DAY)
    assert {q["query"] for _, q, _ in f.calls} == {SIG.query}
    # A signal whose query does not match its attested hash, or under another contract, is never read.
    for bad in (dataclasses.replace(SIG, query=SIG.query.replace("web", "api")),
                dataclasses.replace(SIG, contract="requests-per-minute/v0"), dataclasses.replace(SIG, query="")):
        g = SeriesFetch()
        with pytest.raises(ValueError):
            read_rps_history("http://vm", bad, now=NOW, interval_s=300, history_seconds=DAY, fetch=g)
        assert g.calls == []
    with pytest.raises(ValueError):
        read_rps_history("http://vm", SIG.query, now=NOW, interval_s=300, history_seconds=DAY, fetch=SeriesFetch())


# --- v1 is unchanged ----------------------------------------------------------------------------------------------

def test_v1_reader_keeps_its_grid_units_and_strictness():
    from data import history as v1
    assert (v1.STEP_SECONDS, v1.HISTORY_HOURS, v1.DEADLINE_S, v1.MAX_RESPONSE_BYTES) == (600, 168, 20.0, 8 << 20)
    v1_now = datetime(2026, 11, 6, 12, 5, tzinfo=timezone.utc)
    f = SeriesFetch(value=lambda ts: 2.5)
    out = query_history("http://vm", SIG.query, now=v1_now, hours=2, fetch=f)
    assert [r["value"] for r in out] == [150.0] * 13                  # v1 converts to req/min on the 600 s grid
    _, q, kw = f.calls[0]
    assert q["step"] == "600s" and kw == {"deadline_s": 20.0, "max_bytes": 8 << 20}
    g = read(SeriesFetch(value=lambda ts: 2.5), now=v1_now, interval_s=600, history_seconds=7200, settlement_s=0)
    assert g.values == (2.5,) * 12                                    # v2 keeps req/s
    with pytest.raises(Exception):                                    # v1 still refuses an answer without a series
        query_history("http://vm", SIG.query, now=v1_now, hours=2, fetch=SeriesFetch(value=lambda ts: None))
