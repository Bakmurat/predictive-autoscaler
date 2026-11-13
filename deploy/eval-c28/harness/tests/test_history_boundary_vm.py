"""The history boundary against a REAL VictoriaMetrics (the lab's version, v1.116.0, single-node binary), never a mock.

Run with C28_VM_BIN=/path/to/victoria-metrics-prod (the release tarball's binary, sha256-verified); skipped without
it. Each test imports raw samples (the shape the two scrape objects produce: the common ingress counter and the
attempt's renamed, epoch-labelled copy) and asks VM exactly what the forecaster and the operator ask: query_range of
the compiled guarded query, and raw range-vector reads for the acceptance proof."""

import json
import os
import socket
import subprocess
import time
import urllib.parse
import urllib.request

import pytest

from c28 import history_boundary as H

VM_BIN = os.environ.get("C28_VM_BIN")
pytestmark = pytest.mark.skipif(not VM_BIN, reason="C28_VM_BIN (VictoriaMetrics v1.116.0 binary) not set")
N, R = "c28-demo-pa-n", "c28-demo-pa-r"
STEP = 15


@pytest.fixture
def vm(tmp_path):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen([VM_BIN, f"-storageDataPath={tmp_path}/data", f"-httpListenAddr=127.0.0.1:{port}",
                             "-retentionPeriod=100y", "-search.latencyOffset=0s", "-search.disableCache",
                             "-loggerLevel=ERROR"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            urllib.request.urlopen(base + "/health", timeout=1)
            break
        except OSError:
            time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("VictoriaMetrics did not start")
    version = urllib.request.urlopen(base + "/metrics", timeout=5).read().decode()
    assert 'short_version="v1.116.0"' in version, "not the lab's VictoriaMetrics version"

    class VM:
        base = None

        def put(self, metric, points):
            body = json.dumps({"metric": metric, "values": [v for _, v in points],
                               "timestamps": [int(t * 1000) for t, _ in points]}).encode()
            urllib.request.urlopen(urllib.request.Request(base + "/api/v1/import", data=body), timeout=10)
            urllib.request.urlopen(base + "/internal/force_flush", timeout=10)

        def get(self, path, **params):
            url = f"{base}/api/v1/{path}?{urllib.parse.urlencode(params)}"
            doc = json.loads(urllib.request.urlopen(url, timeout=10).read())
            assert doc["status"] == "success", doc
            return doc["data"]["result"]

        def raw(self, epoch, target, lo, hi, ns="demo"):
            return self.get("query", query=f"{H.raw_selector(ns, epoch, target)}[{int(hi - lo)}s]", time=f"{hi}")

        def range(self, query, lo, hi, step=STEP):
            res = self.get("query_range", query=query, start=lo, end=hi, step=f"{step}s")
            return {float(t): float(v) for r in res for t, v in r["values"]}
    VM.base = base
    try:
        yield VM()
    finally:
        proc.kill()
        proc.wait()


def counter(t0, t1, start, per_min, step=STEP, skip=()):
    """A scraped counter: one sample per step from t0 to t1, rising per_min each minute; `skip` drops sample times."""
    out, v, t = [], float(start), t0
    while t <= t1:
        if t not in skip:
            out.append((t, v))
        t += step
        v += per_min * step / 60
    return out


RUN = "c28-demo-1"


def ep(k, run=RUN):
    return H.epoch_id(run, k)


def world(vm, attempt=1, origin=None, created_after=30, pre_rate=6000.0, post_rate=600.0, skip=(), inherited=5e6,
          run=RUN, ns="demo"):
    """The ingress counter of both targets before and after the origin O (contrasting slopes), plus the attempt's
    history copy that starts `created_after` seconds after O, carrying the counter's (large, inherited) value."""
    now = int(time.time()) // 60 * 60
    o = origin or now - 3600
    g = H.guard_s(o)
    for t in (N, R):
        pre = counter(o - 1800, o, inherited - pre_rate * 30, pre_rate)
        post = counter(o + STEP, o + 1800, pre[-1][1] + post_rate * STEP / 60, post_rate)
        vm.put({"__name__": H.SOURCE_METRIC, "namespace": ns, "target": t, "job": f"{ns}/c28-ingress"}, pre + post)
        hist = [(ts, v) for ts, v in pre + post if ts >= o + created_after and ts not in skip]
        vm.put({"__name__": H.HISTORY_METRIC, "namespace": ns, "target": t, H.EPOCH_LABEL: ep(attempt, run),
                "job": f"{ns}/{H.scrape_name(attempt)}"}, hist)
    return o, g


def accept(vm, attempt, o, g):
    return H.acceptance({t: vm.raw(ep(attempt), t, o - 1800, g) for t in (N, R)}, [N, R], "demo", ep(attempt), o, g)


def test_isolation_excluded_steps_absent_and_only_the_post_origin_slope(vm):
    o, g = world(vm)
    assert accept(vm, 1, o, g)["accepted"]
    got = vm.range(H.compiled_query("demo", ep(1), g, N), o - 1800, g + 900)
    assert got and min(got) >= g                              # every step before G is ABSENT, never zero
    assert all(abs(v - 600 / 60) < 1e-6 for v in got.values())   # only the post-origin slope (10 rps), never 100 rps
    acct = vm.range(f'sum(rate({H.SOURCE_METRIC}{{target="{N}"}}[60s]))', o - 600, o - 60)
    assert acct and all(abs(v - 6000 / 60) < 1e-6 for v in acct.values())   # accounting still sees the old slope


def test_an_inherited_nonzero_counter_never_appears_as_a_burst(vm):
    o, g = world(vm, inherited=9e7)
    got = vm.range(H.compiled_query("demo", ep(1), g, N), g, g + 600)
    assert got and max(got.values()) < 11                      # the first accepted window has a post-O predecessor


def test_accounting_queries_are_identical_with_and_without_the_history_series(vm):
    now = int(time.time()) // 60 * 60
    o = now - 3600
    for t in (N,):
        vm.put({"__name__": H.SOURCE_METRIC, "namespace": "demo", "target": t, "job": "demo/c28-ingress"},
               counter(o - 1800, o + 1800, 1000, 600))
    q = f'sum(rate({H.SOURCE_METRIC}{{target="{N}"}}[60s]))'
    before = vm.range(q, o - 1200, o + 1200)
    vm.put({"__name__": H.HISTORY_METRIC, "namespace": "demo", "target": N, H.EPOCH_LABEL: ep(1),
            "job": "demo/c28-demo-history-1"}, counter(o + 30, o + 1800, 1000, 600))
    assert vm.range(q, o - 1200, o + 1200) == before


def test_altered_pre_origin_samples_never_change_accepted_history(vm):
    o, g = world(vm)
    q = H.compiled_query("demo", ep(1), g, N)
    before = vm.range(q, g, g + 900)
    vm.put({"__name__": H.SOURCE_METRIC, "namespace": "demo", "target": N, "job": "demo/c28-ingress"},
           [(o - 7, 1.0), (o - 3, 9e9)])                       # pre-origin samples altered (a counter jump)
    assert vm.range(q, g, g + 900) == before


@pytest.mark.parametrize("skip_at, text", [
    (lambda o, g: range(g - 60, g + 1, STEP), "samples inside the first accepted window"),   # the window is empty
    (lambda o, g: range(g - 150, g - 90, STEP), "gaps above"),                               # a delayed scrape
])
def test_missing_or_delayed_scrapes_near_the_boundary_fail_the_proof(vm, skip_at, text):
    now = int(time.time()) // 60 * 60
    o = now - 3600
    g = H.guard_s(o)
    o2, g2 = world(vm, origin=o, skip=set(skip_at(o, g)))
    r = accept(vm, 1, o2, g2)
    assert not r["accepted"] and any(text in x for x in r["problems"]), r["problems"]


def test_a_history_series_started_before_the_origin_fails_the_proof(vm):
    o, g = world(vm, created_after=-60)                       # the attempt's object existed before O
    r = accept(vm, 1, o, g)
    assert not r["accepted"] and any("at or before the origin" in x for x in r["problems"])


def test_an_empty_or_missing_target_fails(vm):
    o, g = world(vm)
    r = H.acceptance({N: vm.raw(ep(1), N, o - 1800, g)}, [N, R], "demo", ep(1), o, g)
    assert not r["accepted"] and any(R in x and "no series" in x for x in r["problems"])
    assert not accept(vm, 7, o, g)["accepted"]                 # an attempt that never scraped


def test_a_failed_attempt_then_a_retry_never_mix(vm):
    now = int(time.time()) // 60 * 60
    o1 = now - 7200
    _, g1 = world(vm, attempt=1, origin=o1, skip=set(range(H.guard_s(o1) - 60, H.guard_s(o1) + 1, STEP)))
    assert not accept(vm, 1, o1, g1)["accepted"]               # attempt 1 failed (kept, never deleted)
    o2 = now - 3600
    _, g2 = world(vm, attempt=2, origin=o2, post_rate=1200.0)
    assert accept(vm, 2, o2, g2)["accepted"]
    got = vm.range(H.compiled_query("demo", ep(2), g2, N), o1, g2 + 600)
    assert got and min(got) >= g2 and all(abs(v - 20) < 1e-6 for v in got.values())   # attempt 1 never leaks in
    before = dict(got)
    vm.put({"__name__": H.HISTORY_METRIC, "namespace": "demo", "target": N, H.EPOCH_LABEL: ep(1),
            "job": "demo/c28-demo-history-1"}, [(g2 + 100, 1.0)])                                  # the failed attempt's samples changed later
    assert vm.range(H.compiled_query("demo", ep(2), g2, N), o1, g2 + 600) == before


def _ml_engine():
    import sys
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "ml-engine"))
    if root not in sys.path:
        sys.path.insert(0, root)
    from api.identity import CONTRACT, ResolvedSignal
    from forecasting import contracts, history
    return CONTRACT, ResolvedSignal, contracts, history


def _signal(query):
    import hashlib
    CONTRACT, ResolvedSignal, _, _ = _ml_engine()
    return ResolvedSignal(namespace="demo", name=N, metric="requests", query=query,
                          sha256=hashlib.sha256(query.encode()).hexdigest(), contract=CONTRACT,
                          autoscaler_uid="pa-uid-1", target_uid="dep-uid-1", generation=2)


def test_the_forecasters_history_read_sees_no_step_before_the_guard(vm):
    _, _, contracts, history = _ml_engine()
    o, g = world(vm)
    grid = history.read_rps_history(vm.base, _signal(H.compiled_query("demo", ep(1), g, N)), now=g + 900, interval_s=60,
                                    history_seconds=3600, settlement_s=0)
    pts = {grid.start + i * 60: v for i, v in enumerate(grid.values)}
    assert all(v is None for t, v in pts.items() if t < g)              # absent before G: gaps, never zeros
    after = [v for t, v in pts.items() if t >= g]
    assert after and all(v is not None and abs(v - 10) < 1e-6 for v in after)
    plain = history.read_rps_history(vm.base, _signal(f'sum(rate({H.SOURCE_METRIC}{{target="{N}"}}[60s]))'),
                                     now=g + 900, interval_s=60, history_seconds=3600, settlement_s=0)
    assert any(v is not None and v > 50 for v in plain.values)            # unguarded: admission load would enter


def test_the_guarded_query_starts_a_new_evidence_key(vm):
    _, _, contracts, _ = _ml_engine()
    o, g = world(vm)
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "ml-engine")))
    from tests import demonstration_helpers  # noqa: F401  (the ml-engine test helpers are importable)
    from tests.test_demonstration_tool import pa
    settings = contracts.resolve_settings(pa())
    a = contracts.evidence_key(_signal(H.compiled_query("demo", ep(1), g, N)), settings, 600)
    b = contracts.evidence_key(_signal(f'sum(rate({H.SOURCE_METRIC}{{target="{N}"}}[60s]))'), settings, 600)
    c = contracts.evidence_key(_signal(H.compiled_query("demo", ep(2), g + 86400, N)), settings, 600)
    assert len({a, b, c}) == 3                                            # each attempt is its own evidence epoch



def test_another_run_or_namespace_never_matches_the_epoch(vm):
    now = int(time.time()) // 60 * 60
    o = now - 3600
    _, g = world(vm, origin=o)
    q = H.compiled_query("demo", ep(1), g, N)
    before = vm.range(q, g, g + 900)
    world(vm, origin=o, run="c28-other-run", post_rate=60000.0)          # another run's attempt 1, same namespace
    world(vm, origin=o, ns="elsewhere", post_rate=60000.0)               # the same run id in another namespace
    assert vm.range(q, g, g + 900) == before
    assert accept(vm, 1, o, g)["accepted"]
    assert len(vm.raw(ep(1), N, o - 1800, g)) == 1                       # exactly our one series
