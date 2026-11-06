"""C2.2: the chronological replay runner and dataset manifests (synthetic cases): determinism, leakage, missing/late/
out-of-order truth, crash and resume, network-free."""

import dataclasses
import json
import os
import socket
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from evaluation import datasets as ds  # noqa: E402
from evaluation.forecast_replay import (ReplayConfig, ReplayError, VisibleHistory, daily_naive, last_value,  # noqa: E402
                                        replay_keys, run_replay)
from forecasting.clock import ManualClock, ReceiptClock  # noqa: E402
from forecasting.store import ForecastStore  # noqa: E402

ML_ENGINE = Path(__file__).resolve().parent.parent
DAY = 86400
START = 1_793_923_200 - 1_793_923_200 % DAY         # a UTC midnight
DT = 600


def make(tmp_path, name="daily", days=2, **kw):
    return ds.load_dataset(ds.synthetic_dataset(str(tmp_path / "data"), name, start=START, days=days, interval=DT,
                                                seed=7, **kw))


def config(days=2, lead=1800, **kw):
    return ReplayConfig(first_origin=START + DAY, last_origin=START + days * DAY - DT, lead_seconds=lead, **kw)


def fresh_store(path):
    return ForecastStore(str(path), clock=ManualClock(0), store_id="replay-store")


def run(dataset, path, cfg=None, forecaster=daily_naive):
    s = fresh_store(path)
    try:
        return run_replay(dataset, cfg or config(), s, forecaster=forecaster), [i.bundle for i in s.issuances()], s
    finally:
        s.close()


# --- datasets ---------------------------------------------------------------------------------------------------

def test_a_dataset_round_trips_with_its_digests(tmp_path):
    d = make(tmp_path)
    m = d.manifest
    assert m["schema"] == "forecast-dataset/v1" and m["units"] == "requests/second" and m["timezone"] == "UTC"
    assert m["availability"] == {"mode": "settlement", "settlement_seconds": 120} and m["rows"] == 288
    assert m["license"]["name"] == "Apache-2.0" and m["source"]["kind"] == "synthetic"
    assert all(o.available_at_ms == (o.event_time + 120) * 1000 for o in d.observations)
    again = make(tmp_path / "again")
    assert again.manifest_sha256 == d.manifest_sha256 and again.observations == d.observations   # deterministic


def tamper(tmp_path, edit_manifest=None, edit_data=None):
    path = ds.synthetic_dataset(str(tmp_path / "t"), "t", start=START, days=1, interval=DT)
    m = json.loads(Path(path).read_text())
    if edit_manifest:
        edit_manifest(m)
        Path(path).write_text(json.dumps(m))
    if edit_data:
        data_path = Path(path).parent / "t.csv"
        data_path.write_bytes(edit_data(data_path.read_bytes()))
    return path


@pytest.mark.parametrize("edit_manifest,edit_data", [
    (None, lambda b: b.replace(b",", b",1", 1)),                                  # bytes differ from data_sha256
    (lambda m: m.update(data_file="../t.csv"), None),                             # no path traversal
    (lambda m: m.update(units="requests/minute"), None),
    (lambda m: m.update(timezone="Europe/Berlin"), None),
    (lambda m: m.update(interval_seconds=420), None),
    (lambda m: m.update(license={}), None),
    (lambda m: m.update(extra=1), None),
    (lambda m: m.pop("extraction"), None),
    (lambda m: m.update(availability={"mode": "guess", "settlement_seconds": 0}), None),
    (lambda m: m.update(exclusions=[{"start": 5, "end": 1, "reason": "x"}]), None),
    (lambda m: m.update(rows=1), None),
])
def test_a_tampered_or_incomplete_dataset_is_refused(tmp_path, edit_manifest, edit_data):
    with pytest.raises(ds.DatasetInvalid):
        ds.load_dataset(tamper(tmp_path, edit_manifest, edit_data))


@pytest.mark.parametrize("obs,availability", [
    ([ds.Observation(START + 60, 1.0, (START + 60) * 1000)], "per_sample"),     # off the grid
    ([ds.Observation(START, 1.0, START * 1000 - 1)], "per_sample"),              # available before it happened
    ([ds.Observation(START, 1.0, START * 1000)] * 2, "per_sample"),              # duplicate
    ([ds.Observation(START, -1.0, START * 1000)], "per_sample"),
    ([ds.Observation(START, float("nan"), START * 1000)], "per_sample"),
    ([ds.Observation(START, 1.0, START * 1000)], "settlement"),                  # not the declared delay
])
def test_invalid_rows_are_refused(tmp_path, obs, availability):
    with pytest.raises(ds.DatasetInvalid):
        ds.write_dataset(str(tmp_path), name="bad", version="1", description="d", source={"kind": "synthetic"},
                         interval_seconds=DT, series="s", observations=obs,
                         availability={"mode": availability, "settlement_seconds": 120},
                         extraction={"command": "test", "version": "1"})


# --- determinism ------------------------------------------------------------------------------------------------

def test_replay_is_deterministic(tmp_path):
    d = make(tmp_path, missing=[START + DAY + 7200])
    r1, b1, _ = run(d, tmp_path / "a.sqlite")
    r2, b2, _ = run(d, tmp_path / "b.sqlite")
    assert r1 == r2 and b1 == b2
    assert r1.issued == 144 and r1.scored + r1.unscorable + r1.pending == 144 and r1.unscorable == 3
    assert r1.checkpoint["unbounded"] == r1.scored and r1.checkpoint["covered"] == r1.checkpoint["missed"] == 0


def test_issuances_are_replay_labelled_and_bound_to_the_dataset(tmp_path):
    d = make(tmp_path)
    r, bundles, _ = run(d, tmp_path / "j.sqlite")
    first = json.loads(bundles[0])
    assert first["evidence_mode"] == "replay" and first["dataset_sha256"] == d.manifest_sha256
    assert first["upper_window_max_rps"] is None and first["target_timestamps"] == [START + DAY + k * DT
                                                                                   for k in (1, 2, 3)]
    assert first["input_end"] == START + DAY - DT                  # the newest point received by the origin
    assert first["input_received_through_ms"] == (START + DAY - DT + 120) * 1000
    assert (r.evidence_key, r.settings_sha256) == replay_keys(d, config())
    assert replay_keys(d, config(lead=1200))[0] != r.evidence_key   # a lead change is a new epoch


# --- leakage: mutate all future values ----------------------------------------------------------------------------

@pytest.mark.parametrize("forecaster", [daily_naive, last_value])
@pytest.mark.parametrize("cut_hours", [1, 7, 13, 23])
def test_mutating_all_future_values_leaves_past_issuances_unchanged(tmp_path, cut_hours, forecaster):
    # Same manifest identity, different future: only the runner's causality can keep the past bytes identical.
    d = make(tmp_path, days=3)
    cut = START + DAY + cut_hours * 3600
    mutated = dataclasses.replace(d, observations=tuple(
        dataclasses.replace(o, value_rps=o.value_rps * 3 + 100) if o.event_time > cut else o for o in d.observations))
    cfg = config(days=3)
    run(d, tmp_path / "a.sqlite", cfg, forecaster)
    run(mutated, tmp_path / "b.sqlite", cfg, forecaster)
    a, b = fresh_store(tmp_path / "a.sqlite"), fresh_store(tmp_path / "b.sqlite")
    ia, ib = a.issuances(), b.issuances()
    past = [x for x in ia if x.key.decision_origin <= cut]
    assert past and [x.bundle for x in past] == [x.bundle for x in ib[:len(past)]]     # byte-identical
    oa = [o for o in a.outcomes_after(ia[0].key.evidence_key, 0) if o.committed_at_ms <= cut * 1000]
    ob = [o for o in b.outcomes_after(ib[0].key.evidence_key, 0) if o.committed_at_ms <= cut * 1000]
    assert oa == ob                                                                     # outcomes settled by then too
    # Not vacuous: later issuances (whose inputs reach past the cut) and later outcomes do change.
    assert any(x.bundle != y.bundle for x, y in zip(ia[len(past):], ib[len(past):]))
    assert a.journal_digest() != b.journal_digest()
    a.close(), b.close()


def test_the_forecaster_sees_only_what_was_received(tmp_path):
    d = make(tmp_path, delays_ms={START + DAY - DT: 3_600_000})      # one point arrives an hour late
    seen = []

    def spy(view, origin, targets):
        assert all(t <= origin for t in view) and (view.latest() is None or view.latest() <= origin)
        seen.append((origin, START + DAY - DT in view))
        return last_value(view, origin, targets)

    run(d, tmp_path / "j.sqlite", forecaster=spy)
    arrival = START + DAY - DT + 120 + 3600
    assert all(present == (origin * 1000 >= arrival * 1000) for origin, present in seen)


def test_visible_history_is_read_only_and_time_bounded():
    v = VisibleHistory({1: (1.0, 10), 2: (2.0, 30)}, now_ms=20, interval=1)
    assert list(v) == [1] and v.get(2) is None and len(v) == 1 and v.latest() == 1 and v.received_through_ms() == 10
    with pytest.raises(TypeError):
        v._received[3] = (3.0, 0)


# --- missing, late and out-of-order truth -----------------------------------------------------------------------

def outcomes_by_origin(path):
    s = fresh_store(path)
    try:
        ek = s.issuances()[0].key.evidence_key
        by_id = {i.issuance_id: i.key.decision_origin for i in s.issuances()}
        return {by_id[o.issuance_id]: (o.status, o.payload(), o.committed_at_ms) for o in s.outcomes_after(ek, 0)}
    finally:
        s.close()


def test_missing_truth_closes_unscorable_and_is_never_backfilled(tmp_path):
    t = START + DAY + 6 * 3600
    late_beyond_grace = {t: 3_600_000}                              # arrives an hour after settlement (> 600 s grace)
    d = make(tmp_path, delays_ms=late_beyond_grace)
    run(d, tmp_path / "j.sqlite", config(lead=1800))
    out = outcomes_by_origin(tmp_path / "j.sqlite")
    hit = [o for o in out if o < t <= o + 1800]
    assert len(hit) == 3
    for o in hit:
        status, p, at = out[o]
        assert status == "unscorable" and p["first_missing"] == t
        assert at == -(-(o + 1800 + 120 + 600) // DT) * DT * 1000   # at the first tick once the grace ran out
    # The value did arrive later: it is in the journal as an observation, but the outcomes stay unscorable.
    s = fresh_store(tmp_path / "j.sqlite")
    assert s.observations("replay:" + d.manifest_sha256, t, t)[0][2] == (t + 120) * 1000 + 3_600_000
    s.close()


def test_late_truth_within_the_grace_is_scored_when_it_arrives(tmp_path):
    t = START + DAY + 6 * 3600
    d = make(tmp_path, delays_ms={t: 300_000})                      # five minutes late, inside the 600 s grace
    run(d, tmp_path / "j.sqlite", config(lead=1800))
    out = outcomes_by_origin(tmp_path / "j.sqlite")
    hit = [o for o in out if o < t <= o + 1800]
    assert all(out[o][0] == "scored" for o in hit)
    for o in hit:
        assert out[o][1]["completed_at_ms"] == max((t + 120) * 1000 + 300_000, (o + 1800 + 120) * 1000)


def test_out_of_order_receipts_wait_for_every_point(tmp_path):
    # The 01:00 point arrives after the 01:10 and 01:20 points: the window still waits for it and scores once.
    t = START + DAY + 3600
    d = make(tmp_path, delays_ms={t: 1_500_000})
    assert d.manifest["availability"]["mode"] == "per_sample"
    run(d, tmp_path / "j.sqlite", config(lead=1800, outcome_grace_seconds=1800))
    out = outcomes_by_origin(tmp_path / "j.sqlite")
    for o in (o for o in out if o < t <= o + 1800):
        status, p, at = out[o]
        assert status == "scored" and at >= (t + 120) * 1000 + 1_500_000


def test_zero_demand_scores_and_no_forecast_is_counted(tmp_path):
    d = make(tmp_path, kind="zero")
    r, bundles, _ = run(d, tmp_path / "j.sqlite")
    assert r.scored > 0 and all(json.loads(b)["base_window_max_rps"] == 0.0 for b in bundles)
    short = make(tmp_path / "short", days=1)
    r2, b2, _ = run(short, tmp_path / "k.sqlite", ReplayConfig(first_origin=START, last_origin=START + DAY - DT,
                                                                lead_seconds=1800))
    # Daily naive needs every lagged slot: only the last origin of the first day has a full prior window.
    assert r2.issued == 1 and r2.no_forecast == 143 and json.loads(b2[0])["decision_origin"] == START + DAY - DT


def test_invalid_trajectories_and_configs_are_refused(tmp_path):
    d = make(tmp_path)
    with pytest.raises(ReplayError):
        run(d, tmp_path / "a.sqlite", forecaster=lambda v, o, t: [float("nan")] * len(t))
    with pytest.raises(ReplayError):
        run(d, tmp_path / "b.sqlite", forecaster=lambda v, o, t: [1.0])
    with pytest.raises(ReplayError):
        run(d, tmp_path / "c.sqlite", config(lead=1000))
    wall = ForecastStore(str(tmp_path / "d.sqlite"), clock=ReceiptClock())      # a replay runs on a ManualClock
    with pytest.raises(ReplayError):
        run_replay(d, config(), wall)
    wall.close()


# --- crash and resume -------------------------------------------------------------------------------------------

RESUME = textwrap.dedent("""
    import os, sys
    from evaluation.datasets import load_dataset
    from evaluation.forecast_replay import ReplayConfig, run_replay
    from forecasting.clock import ManualClock
    from forecasting.store import ForecastStore
    manifest, path, stage, nth = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    first, last, lead = (int(x) for x in sys.argv[5:8])
    s = ForecastStore(path, clock=ManualClock(0), store_id="replay-store")
    seen = [0]
    def hook(st):
        if st == stage:
            seen[0] += 1
            if seen[0] == nth:
                os._exit(17)
    s._fault_hook = hook
    run_replay(load_dataset(manifest), ReplayConfig(first_origin=first, last_origin=last, lead_seconds=lead), s)
    os._exit(0)
""")


@pytest.mark.parametrize("stage,nth", [("before_commit", 1), ("after_commit", 2), ("before_commit", 37),
                                       ("after_commit", 120), ("before_commit", 266), ("after_commit", 401)])
def test_a_replay_killed_mid_run_resumes_to_the_same_journal(tmp_path, stage, nth):
    manifest = ds.synthetic_dataset(str(tmp_path / "data"), "daily", start=START, days=2, interval=DT, seed=7,
                                    missing=[START + DAY + 7200], delays_ms={START + DAY + 3600: 1_200_000})
    d = ds.load_dataset(manifest)
    cfg = config()
    whole, _, _ = run(d, tmp_path / "whole.sqlite")
    env = {**{k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}, "PYTHONPATH": str(ML_ENGINE),
           "KUBECONFIG": "/dev/null"}
    p = subprocess.run([sys.executable, "-c", RESUME, manifest, str(tmp_path / "crashed.sqlite"), stage, str(nth),
                        str(cfg.first_origin), str(cfg.last_origin), str(cfg.lead_seconds)], cwd=ML_ENGINE, env=env,
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 17, p.stderr[-2000:]
    s = fresh_store(tmp_path / "crashed.sqlite")
    s.verify()
    before = len(s.issuances())
    resumed = run_replay(d, cfg, s)
    s.close()
    assert resumed.journal_sha256 == whole.journal_sha256 and resumed.checkpoint == whole.checkpoint
    assert resumed.issued == whole.issued - before          # the resumed run commits only what was missing


# --- network-free -----------------------------------------------------------------------------------------------

def test_the_replay_never_touches_the_network(tmp_path, monkeypatch):
    d = make(tmp_path)

    def refuse(*a, **k):
        raise AssertionError("the replay tried to use the network")
    for name in ("create_connection", "getaddrinfo"):
        monkeypatch.setattr(socket, name, refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    r, _, _ = run(d, tmp_path / "j.sqlite")
    assert r.issued == 144
