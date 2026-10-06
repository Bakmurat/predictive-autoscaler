"""Synthetic warm-start experiment (offline): the real input fails closed, the per-slot replay equals the harness."""
import importlib.util
import json
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "run_synthetic_warmstart",
    ROOT / "deploy" / "prodcluster" / "experiments" / "synthetic-warmstart" / "run_synthetic_warmstart.py")
sw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sw)
cb = sw.cb

T0 = 1_791_268_800                       # 2026-10-06T06:40:00Z, a grid instant


class _Resp:
    def __init__(self, body):
        self._b = json.dumps(body).encode()

    def read(self):
        return self._b


def _serve(monkeypatch, body):
    monkeypatch.setattr(sw.urllib.request, "urlopen", lambda url, timeout=0: _Resp(body))


def _body(points, **extra):
    return dict({"status": "success", "data": {"result": [{"metric": {}, "values": [[t, str(v)] for t, v in points]}]}},
                **extra)


def test_complete_grid_is_returned_with_its_hash(monkeypatch):
    pts = [(T0 + i * 600, 100.0 + i) for i in range(4)]
    _serve(monkeypatch, _body(pts))
    got, digest = sw.fetch_real("http://x", "nginx-test", T0, T0 + 1800)
    assert got == pts and len(digest) == 64


@pytest.mark.parametrize("mutate", [
    lambda p: p[:1] + p[2:],                       # a missing interior slot
    lambda p: p[:-1],                              # the last slot missing
    lambda p: p[:2] + [(p[2][0], float("nan"))] + p[3:],
    lambda p: p[:2] + [(p[2][0] + 1, p[2][1])] + p[3:],   # off-grid timestamp
])
def test_incomplete_real_input_fails_closed(monkeypatch, mutate):
    pts = [(T0 + i * 600, 100.0) for i in range(4)]
    _serve(monkeypatch, _body(mutate(pts)))
    with pytest.raises(SystemExit):
        sw.fetch_real("http://x", "nginx-test", T0, T0 + 1800)


def test_partial_or_multi_series_responses_fail_closed(monkeypatch):
    pts = [(T0 + i * 600, 100.0) for i in range(4)]
    _serve(monkeypatch, _body(pts, isPartial=True))
    with pytest.raises(SystemExit):
        sw.fetch_real("http://x", "nginx-test", T0, T0 + 1800)
    body = _body(pts)
    body["data"]["result"].append(body["data"]["result"][0])
    _serve(monkeypatch, body)
    with pytest.raises(SystemExit):
        sw.fetch_real("http://x", "nginx-test", T0, T0 + 1800)


def test_per_slot_replay_equals_the_harness_replay():
    rng = np.random.default_rng(7)
    y = np.concatenate([np.linspace(300, 6500, 40), np.linspace(6500, 200, 40)]) * (1 + 0.05 * rng.standard_normal(80))
    origins = list(range(1, len(y) - 2))
    for lead_of in (lambda k: float("nan"), lambda k: max(y[k + 1], y[k + 2]) * 0.9, lambda k: y[k] * 1.3):
        mine, trace = sw.replay_trace(y, origins, lead_of)
        ref = cb.replay(y, origins, lead_of, sw.PER_POD, sw.MIN_R, sw.MAX_R)
        assert (mine["shortage_replica_min"], mine["surplus_replica_min"]) == \
               (ref["shortage_replica_min"], ref["surplus_replica_min"])
        assert len(trace) == ref["slots"]
        short = sum(max(0, q - r) * 2.0 for _, _, r, q in trace.values())
        assert round(short, 1) == ref["shortage_replica_min"]
