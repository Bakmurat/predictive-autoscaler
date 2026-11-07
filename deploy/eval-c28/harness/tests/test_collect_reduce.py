"""Evidence collection (hashed, gap-explicit, credential-free) and its reduction to per-window metrics."""

import http.server
import json
import os
import threading
from datetime import datetime, timezone

import pytest

from c28 import collect, reduce

B = int(datetime(2026, 11, 20, tzinfo=timezone.utc).timestamp())
T = "c28-pa-n-s20"
BUCKETS = ["0.05", "0.1", "0.2", "0.3", "0.4", "0.5", "2", "+Inf"]


def scheduled():
    """The plan's scheduled arrivals matching synthetic_fetch: 8,400 a minute, 100 more in minutes 100-109 (the
    failures) and 50 more in minute 400 (the generator's drops)."""
    return [8400.0 + (100 if 100 <= k <= 109 else 0) + (50 if k == 400 else 0) for k in range(1440)]


def synthetic_fetch(base, expr, a, b, step, timeout):
    """A Prometheus-shaped answer for each default query: 140 rps offered, 5 ready, 6 pods; client failures in minutes
    100-109, slow successes in minutes 200-204, no data for minutes 300-301, 50 dropped iterations in minute 400."""
    ts = list(range(a, b + 1, step))
    def series(metric, fn):
        return {"metric": metric, "values": [[t, str(v)] for t in ts if (v := fn(t)) is not None]}
    minute = lambda t: (t - B) // 60 - 1  # noqa: E731
    gap = lambda t: 300 <= minute(t) <= 301  # noqa: E731
    result = []
    if "k6_http_reqs_total" in expr:
        result = [series({"status": "200"}, lambda t: None if gap(t) else 8400.0),
                  series({"status": "0"}, lambda t: None if gap(t) else (100.0 if 100 <= minute(t) <= 109 else 0.0))]
    elif "k6_dropped_iterations_total" in expr:
        result = [series({}, lambda t: None if gap(t) else (50.0 if minute(t) == 400 else 0.0))]
    elif "increase(c28_ingress_requests_total" in expr:
        result = [series({}, lambda t: None if gap(t) else (8400.0 + (60 if 100 <= minute(t) <= 109 else 0)))]
    elif "c28_ingress_requests_total" in expr:
        result = [series({}, lambda t: 140.0)]
    elif "c28_ingress_responses_total" in expr:
        result = [series({"code": "200"}, lambda t: None if gap(t) else 8400.0),
                  series({"code": "504"}, lambda t: None if gap(t) else (100.0 if 100 <= minute(t) <= 109 else 0.0))]
    elif "c28_ingress_duration_seconds_bucket" in expr:
        for le in BUCKETS:
            def f(t, le=le):
                if gap(t):
                    return None
                slow = 200 <= minute(t) <= 204
                if le in ("0.05",):
                    return 0.0
                if le in ("0.1", "0.2"):
                    return 7560.0 if slow else 8400.0
                return 8400.0
            result.append(series({"le": le, "outcome": "success"}, f))
    elif "replicas_ready" in expr:
        result = [series({}, lambda t: 5.0)]
    elif "status_replicas{" in expr:
        result = [series({}, lambda t: 6.0)]
    elif "spec_replicas" in expr:
        result = [series({}, lambda t: 5.0)]
    elif "resource_requests" in expr:
        result = [series({}, lambda t: 0.5)]
    return {"status": "success", "data": {"resultType": "matrix", "result": result}}


@pytest.fixture
def bundle(tmp_path):
    out = str(tmp_path / "bundle")
    ledger = tmp_path / "decisions.jsonl"
    ledger.write_text('{"event":"decision"}\n')
    idx = collect.collect(out, "http://observer.example:8428", [T], "demo", B, B + 86400, "r" * 64,
                          files={"ledgers": [str(ledger), str(tmp_path / "absent.jsonl")]}, fetch=synthetic_fetch)
    return out, idx


def test_chunks_cover_the_range_without_overlap():
    cs = collect.chunks(0, 86400, 5)
    assert cs[0][0] == 0 and cs[-1][1] == 86400
    assert all(b - a <= (collect.MAX_POINTS - 1) * 5 for a, b in cs)
    assert all(n[0] == p[1] + 5 for p, n in zip(cs, cs[1:]))


def test_the_bundle_is_hashed_and_records_gaps(bundle):
    out, idx = bundle
    assert collect.verify_bundle(out) == []
    assert [g for g in idx["gaps"] if "file" in g] == [{"file": g["file"], "status": "missing"} for g in idx["gaps"]
                                                       if "file" in g] and len(idx["gaps"]) == 1
    assert idx["runlock_sha256"] == "r" * 64 and idx["files"][0]["label"] == "ledgers"
    with open(os.path.join(out, idx["files"][0]["file"]), "a") as fh:
        fh.write("tampered\n")
    assert any(p.startswith("changed files/ledgers/") for p in collect.verify_bundle(out))
    open(os.path.join(out, "extra.txt"), "w").close()
    assert "unlisted extra.txt" in collect.verify_bundle(out)


def test_partial_and_failed_queries_are_gaps_not_empty_series(tmp_path):
    def flaky(base, expr, a, b, step, timeout):
        if "offered" in expr or "c28_ingress_requests_total" in expr:
            return {"status": "success", "isPartial": True, "data": {"result": []}}
        raise OSError("connection refused")
    idx = collect.collect(str(tmp_path / "b"), "http://o:1", [T], "demo", B, B + 600, "r" * 64,
                          queries=[{"name": "offered_rps", "step": 15, "expr": collect.DEFAULT_QUERIES[0]["expr"]},
                                   {"name": "ready_replicas", "step": 5, "expr": collect.DEFAULT_QUERIES[3]["expr"]}],
                          fetch=flaky)
    assert {g["status"].split(":")[0] for g in idx["gaps"]} == {"partial", "error"}


@pytest.mark.parametrize("url", ["http://user:pw@o:1", "ftp://o", "o:1"])
def test_credentials_and_odd_urls_are_refused(tmp_path, url):
    with pytest.raises(collect.CollectError):
        collect.collect(str(tmp_path / "b"), url, [T], "demo", B, B + 60, "r" * 64, fetch=synthetic_fetch)


def test_a_bundle_is_written_once(bundle):
    out, _ = bundle
    with pytest.raises(collect.CollectError, match="not empty"):
        collect.collect(out, "http://o:1", [T], "demo", B, B + 60, "r" * 64, fetch=synthetic_fetch)


def test_the_real_fetch_reads_a_prometheus_api(tmp_path):
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path.startswith("/api/v1/query_range?")
            body = json.dumps({"status": "success", "data": {"resultType": "matrix", "result": []}}).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
            self.wfile.write(body)
        def log_message(self, *a):
            pass
    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        idx = collect.collect(str(tmp_path / "b"), f"http://127.0.0.1:{srv.server_port}", [T], "demo", B, B + 60,
                              "r" * 64, queries=[collect.DEFAULT_QUERIES[0]])
        assert idx["gaps"] == []
    finally:
        srv.shutdown()


def test_quantiles_merge_bucket_counts():
    assert reduce.quantile(0.5, [(0.1, 50), (0.2, 100), (float("inf"), 100)]) == pytest.approx(0.1)
    assert reduce.quantile(0.95, [(0.1, 90), (0.2, 100), (float("inf"), 100)]) == pytest.approx(0.15)
    assert reduce.quantile(0.95, []) is None and reduce.quantile(0.95, [(0.1, 0), (float("inf"), 0)]) is None


def test_reduction_counts_violations_unknowns_and_capacity(bundle):
    out, _ = bundle
    bins = reduce.minute_bins(out, T, B, scheduled())
    assert bins[0]["status"] == "ok" and bins[0]["scheduled"] == 8400 and bins[0]["dispatched"] == 8400
    assert bins[100]["status"] == "violation" and bins[100]["violations"] == ["errors"]
    assert bins[400]["status"] == "violation" and bins[400]["dropped"] == 50     # drops are failures of scheduled arrivals
    assert bins[200]["status"] == "violation" and "p95" in bins[200]["violations"]
    assert bins[300]["status"] == "unknown" and bins[301]["status"] == "unknown"
    m = reduce.block_metrics(out, T, B, 35.0, scheduled())
    day = m["day"]
    assert day["slo_violation_minutes"] == 16 and day["observed_minutes"] == 1440 - 2
    assert day["surplus_replica_seconds"] == pytest.approx(86400 * 1.0, rel=1e-3)        # ready 5, n* = 4
    assert day["shortage_replica_seconds"] == 0
    assert day["provisioned_replica_seconds"] == pytest.approx(86400 * 6, rel=1e-3)
    w = m["windows"]["w00-low"]
    assert w["slo_violation_minutes"] == 0 and w["error_fraction"] == 0
    assert m["windows"]["w01-step"]["slo_violation_minutes"] == 10   # minutes 100-109; 200-204 are after 03:00
    assert m["recorder_coverage"] < 1
    step = m["windows"]["w01-step"]
    # 100 client failures in minutes 100-109 (40 never reached the ingress); nothing double-counted.
    assert step["failures"] == 1000 and step["client_failures"] == 1000 and step["pre_ingress_failures"] == 400
    assert step["requests"] == step["scheduled"] == 120 * 8400 + 1000 and step["reconciled"]
    # The two minutes without client data leave their scheduled arrivals without an outcome: failures, unreconciled.
    w = m["windows"]["w03-low"]
    assert w["unaccounted"] == 2 * 8400 and w["failures"] == 2 * 8400 and not w["reconciled"]
    assert not m["reconciled"] and m["dispatch_fraction_min"] < 0.995


def test_missing_outcomes_never_leave_minutes_healthy(tmp_path):
    """Codex repro: 996 terminal outcomes per 1,000 scheduled arrivals each minute (0.4 % missing) must not give 1,440
    healthy minutes."""
    def fetch(base, expr, a, b, step, timeout):
        ts = list(range(a, b + 1, step))
        if "k6_http_reqs_total" in expr:
            res = [{"metric": {"status": "200"}, "values": [[t, "996"] for t in ts]}]
        elif "duration_seconds_bucket" in expr:
            res = [{"metric": {"le": le, "outcome": "success"}, "values": [[t, "996"] for t in ts]} for le in ("0.1", "+Inf")]
        else:
            res = []
        return {"status": "success", "data": {"result": res}}
    out = str(tmp_path / "b")
    collect.collect(out, "http://o:1", [T], "demo", B, B + 86400, "r" * 64, fetch=fetch)
    bins = reduce.minute_bins(out, T, B, [1000.0] * 1440)
    assert sum(1 for b in bins if b["status"] == "ok") < 20
    assert all(b["status"] == "unknown" for b in bins[30:])
    m = reduce.block_metrics(out, T, B, 35.0, [1000.0] * 1440)
    assert m["day"]["error_fraction"] == pytest.approx(0.004, rel=0.01) and m["recorder_coverage"] < 0.02


def test_the_error_denominator_is_scheduled_arrivals(bundle):
    out, _ = bundle
    more = [x * 1.02 for x in scheduled()]          # 2 % of the scheduled arrivals never dispatched nor dropped
    m = reduce.block_metrics(out, T, B, 35.0, more)
    w = m["windows"]["w22-decline"]
    assert w["error_fraction"] == pytest.approx(0.02 / 1.02, rel=1e-6) and not w["reconciled"]
    with pytest.raises(ValueError):
        reduce.minute_bins(out, T, B, [1.0] * 10)
