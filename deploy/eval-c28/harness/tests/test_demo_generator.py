"""The demonstration generator's own logic, exercised on the REAL load/c28-demo.js under node (demo_sim.js: k6's
modules replaced by stubs, a virtual clock): position in the day, arrival scheduling, resume without replay, per-minute
bookkeeping, incarnations and segments. Nothing here runs k6: its event loop, http.asyncRequest and exit behaviour are
only covered by the kind smoke's full mode."""

import json

import pytest

from c28 import demo
from demo_helpers import DAY, MIN, config, simulate

ORIGIN = 1793491200000                     # 2026-11-01T00:00:00Z, the kind params' schedule origin
PEAK = ORIGIN + 10 * DAY + 18 * 3600_000   # 18:00 on day 10: the ordinary cycle's peak (9c)
N, R = "c28-demo-pa-n", "c28-demo-pa-r"


def minutes(out, target, inc=None):
    return [l for l in out["lines"] if l["schema"] == "c28-demo-minute/v1" and l["target"] == target
            and (inc is None or l["inc"] == inc)]


def events(out, event, target=None):
    return [l for l in out["lines"] if l["schema"] == "c28-demo-incarnation/v1" and l["event"] == event
            and (target is None or l["target"] == target)]


def identity(line):
    d = line["dropped"]
    assert line["due"] == line["dispatched"] - line["carried"][0] + line["carried"][1] + d["late"] + d["inflight"], line
    assert line["dispatched"] == line["ok"] + line["failed"] + line["unresolved"], line
    assert sum(line["lat"]["counts"]) == line["ok"], line


def run(tmp_path, incarnations, until, cfg=None, **extra):
    cfg = cfg or config()
    sc = dict({"incarnations": incarnations, "until_ms": until}, **extra)
    return cfg, simulate(tmp_path, cfg, sc)


def test_the_script_loads_and_declares_one_single_vu_scenario_per_target(tmp_path):
    cfg, out = run(tmp_path, [], PEAK)
    sc = out["options"]["scenarios"]
    assert sorted(sc) == ["t0", "t1"]
    for s in sc.values():
        assert (s["executor"], s["vus"], s["iterations"], s["exec"]) == ("per-vu-iterations", 1, 1, "run")
        assert s["maxDuration"] == f"{cfg['segment_minutes'] * 60 + 900}s"     # the segment, and room to finish
    assert out["options"]["discardResponseBodies"] is True
    assert out["options"]["batch"] == out["options"]["batchPerHost"] == cfg["max_inflight"]


@pytest.mark.parametrize("t, minute, day_minute", [
    (ORIGIN, 0, 0), (ORIGIN + 59_999, 0, 0), (ORIGIN + MIN, 1, 1),
    (PEAK + 4_000, 10 * 1440 + 1080, 1080),
    (ORIGIN + 14 * DAY - 1, 14 * 1440 - 1, 1439),             # the last millisecond of day 13
    (ORIGIN + 14 * DAY, 14 * 1440, 0),                        # the day loops without a reset of the minute index
    (ORIGIN - 1, -1, 1439), (ORIGIN - DAY, -1440, 0),         # before the origin the position still wraps correctly
])
def test_position_in_the_day_comes_from_wall_clock_time_and_the_one_origin(tmp_path, t, minute, day_minute):
    cfg, out = run(tmp_path, [], PEAK, probes=[{"fn": "position", "t_ms": t}])
    got = out["functions"][0]
    assert (got["minute"], got["day_minute"]) == (minute, day_minute)
    assert got["intended"] == cfg["per_minute"][day_minute]


def test_arrivals_are_evenly_spaced_inside_their_minute_and_targets_interleave(tmp_path):
    m = 10 * 1440 + 1080
    cfg, out = run(tmp_path, [], PEAK, probes=[{"fn": "arrivals", "minute": m, "target_index": 0},
                                              {"fn": "arrivals", "minute": m, "target_index": 1}])
    a, b = (f["times"] for f in out["functions"])
    n = cfg["per_minute"][1080]
    lo = ORIGIN + m * MIN
    for times in (a, b):
        assert len(times) == n                                           # identical demand for every target
        assert lo <= times[0] and times[-1] < lo + MIN                   # strictly inside the minute
        gaps = [y - x for x, y in zip(times, times[1:])]
        assert min(gaps) > 0 and max(gaps) - min(gaps) <= 1              # evenly spaced, to the millisecond
    assert not set(a) & set(b)                                           # never two targets in one millisecond


@pytest.mark.parametrize("start, end, why", [
    (PEAK, ORIGIN + 11 * DAY, "the next UTC midnight of the origin"),
    (ORIGIN + 11 * DAY, ORIGIN + 12 * DAY, "a start exactly on the boundary runs the whole next segment"),
    (ORIGIN + 11 * DAY - 200_000, ORIGIN + 12 * DAY, "closer than min_segment_ms: runs on to the following boundary"),
    (ORIGIN + 11 * DAY - 400_000, ORIGIN + 11 * DAY, "far enough: ends at the boundary"),
])
def test_segment_end(tmp_path, start, end, why):
    _, out = run(tmp_path, [], PEAK, probes=[{"fn": "position", "t_ms": start}])
    assert out["functions"][0]["segment_end_ms"] == end, why


@pytest.mark.parametrize("override, text", [
    ({"origin_ms": ORIGIN + 3_600_000}, "origin_ms must be a UTC midnight"),
    ({"minute_ms": 1000}, "minute_ms must be 60000"),
    ({"per_minute": [1] * 1439}, "per_minute must be 1440"),
    ({"per_minute": [-1] * 1440}, "per_minute must be 1440"),
    ({"targets": [N, N]}, "distinct DNS labels"),
    ({"base_url": "https://c28-ingress.demo.svc"}, "base_url must be http://"),
    ({"max_late_ms": 60_000}, "max_late_ms must be below one minute"),
    ({"schema": "c28-k6-plan/v1"}, "schema must be c28-demo-generator/v1"),
])
def test_the_script_refuses_a_bad_configuration(tmp_path, override, text):
    _, out = run(tmp_path, [], PEAK, probes=[{"fn": "check", "override": override}])
    assert out["functions"][0]["ok"] is False and text in out["functions"][0]["error"]


@pytest.mark.parametrize("late_s", [0, 4, 30, 600])
def test_a_start_any_time_after_a_minute_boundary_resumes_without_replay(tmp_path, late_s):
    """The lesson of R2-01 on the generator's side: a late start costs only what was really missed."""
    start = PEAK + late_s * 1000
    cfg, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}], PEAK + 14 * MIN,
                   probes=[{"fn": "arrivals", "minute": (start - ORIGIN) // MIN, "target_index": 0}])
    lines = minutes(out, N)
    first = lines[0]
    m0 = (start - ORIGIN) // MIN
    assert first["minute"] == m0 == 10 * 1440 + 1080 + late_s // 60       # the wall-clock position, not minute 0
    assert first["active"] == [start - (ORIGIN + m0 * MIN), MIN]
    scheduled = out["functions"][0]["times"]
    assert first["intended"] == len(scheduled)
    assert first["due"] == first["dispatched"] == sum(1 for t in scheduled if t >= start)
    if late_s % 60:
        assert first["dispatched"] < first["intended"]                    # what was missed is NOT sent later
    for l in lines:
        identity(l)
        assert l["dropped"] == {"late": 0, "inflight": 0} and l["failed"] == 0 and l["unresolved"] == 0
        assert l["dispatched"] == out["truth"][N][str(l["minute"])]["dispatched"]
    for l in lines[1:]:
        assert l["active"] == [0, MIN] and l["due"] == l["intended"] == cfg["per_minute"][l["minute"] % 1440]
    assert [l["minute"] for l in lines] == list(range(m0, m0 + len(lines)))  # one line per minute, none skipped
    assert [l["n"] for l in lines] == list(range(1, len(lines) + 1))
    st = events(out, "start", N)[0]
    assert st["active_from_ms"] == start and st["minute"] == m0 and st["origin_ms"] == ORIGIN
    assert st["config_digest"] == cfg["digest"] and st["inc"] == f"pod-a/{start}"


def test_both_targets_get_identical_demand_every_minute(tmp_path):
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], PEAK + 8 * MIN)
    a, b = minutes(out, N), minutes(out, R)
    assert len(a) == len(b) >= 5
    assert [(l["minute"], l["intended"], l["due"], l["dispatched"]) for l in a] == \
        [(l["minute"], l["intended"], l["due"], l["dispatched"]) for l in b]


def test_a_restart_resumes_the_current_phase_and_never_replays_the_gap(tmp_path):
    kill, again = PEAK + 3 * MIN + 20_000, PEAK + 5 * MIN + 10_000
    cfg, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK, "kill_ms": kill},
                              {"pod_uid": "pod-b", "start_ms": again}], PEAK + 10 * MIN)
    base = 10 * 1440 + 1080
    old, new = minutes(out, N, f"pod-a/{PEAK}"), minutes(out, N, f"pod-b/{again}")
    # The killed incarnation wrote the minutes it could still flush (a line is written 2.5 s after its minute).
    assert [l["minute"] for l in old] == [base, base + 1, base + 2]
    assert events(out, "end") == []                                       # no end record: it was killed
    assert new[0]["minute"] == base + 5 and new[0]["active"] == [10_000, MIN]   # the day is not reset
    assert new[0]["n"] == 1 and new[0]["cum"]["due"] >= new[0]["due"]     # a new incarnation counts from zero
    truth = out["truth"][N]
    assert str(base + 4) not in truth                                      # nothing at all in the gap's full minute
    assert truth[str(base + 3)]["dispatched"] < cfg["per_minute"][1083]    # the kill minute: only until the kill
    assert truth[str(base + 5)]["dispatched"] == new[0]["dispatched"] < cfg["per_minute"][1085]
    total_sent = sum(v["dispatched"] for v in truth.values())
    total_intended = sum(cfg["per_minute"][(base + k) % 1440] for k in range(10))
    assert total_sent < total_intended                                     # the missed traffic is never caught up
    for l in new[1:]:
        assert l["due"] == l["intended"] == l["dispatched"]


def test_a_segment_ends_on_its_boundary_with_an_end_record_and_the_next_incarnation_continues(tmp_path):
    cfg = config(smoke_segment_minutes=3)                                  # the kind smoke's short segments
    assert cfg["label"] == "smoke"
    start = PEAK + 20_000                                                  # PEAK is a multiple of 3 minutes from origin
    again = PEAK + 3 * MIN + 2_000                                         # the container restarts 2 s after the exit
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}, {"pod_uid": "pod-a", "start_ms": again}],
                 PEAK + 9 * MIN + 30_000, cfg=cfg)
    base = 10 * 1440 + 1080
    first = minutes(out, N, f"pod-a/{start}")
    assert [l["minute"] for l in first] == [base, base + 1, base + 2]
    assert first[-1]["active"] == [0, MIN]                                  # it ran its last minute to the boundary
    ends = events(out, "end", N)
    assert [e["inc"] for e in ends] == [f"pod-a/{start}", f"pod-a/{again}"]
    e = ends[0]
    assert e["reason"] == "segment-end" and e["segment_end_ms"] == PEAK + 3 * MIN and e["inflight"] == 0
    assert e["ended_ms"] == PEAK + 3 * MIN + cfg["timeout_ms"] + cfg["flush_grace_ms"]
    assert e["cum"]["dispatched"] == sum(l["dispatched"] for l in first) == sum(l["ok"] for l in first)
    assert e["n"] == first[-1]["n"] + 1
    second = minutes(out, N, f"pod-a/{again}")
    assert [l["minute"] for l in second] == [base + 3, base + 4, base + 5]
    assert second[0]["active"] == [2_000, MIN]
    assert events(out, "start", N)[1]["segment_end_ms"] == PEAK + 6 * MIN
    # No arrival of the first incarnation after its boundary, none of the second before its start.
    truth = out["truth"][N]
    assert truth[str(base + 3)]["dispatched"] == second[0]["dispatched"]


def test_a_failing_target_never_delays_the_other_targets_arrivals(tmp_path):
    """Independent open-loop schedules: with one target timing out on every request, the other's dispatch counts and
    records are exactly those of a healthy run, and the failing target's arrivals are still all dispatched."""
    inc = [{"pod_uid": "pod-a", "start_ms": PEAK}]
    until = PEAK + 8 * MIN
    fault = [{"target": N, "from_ms": PEAK + MIN, "to_ms": PEAK + 6 * MIN, "mode": "timeout"}]
    cfg, healthy = run(tmp_path, inc, until)
    _, faulty = run(tmp_path, inc, until, faults=fault)
    assert minutes(faulty, R) == minutes(healthy, R)                       # byte for byte: nothing moved
    assert faulty["truth"][R] == healthy["truth"][R]
    bad = minutes(faulty, N)
    assert [l["dispatched"] for l in bad] == [l["dispatched"] for l in minutes(healthy, N)]   # open loop
    for l in bad[1:5]:
        identity(l)
        assert l["failed"] == l["dispatched"] and l["ok"] == 0 and l["failed_by"] == {"0": l["failed"]}
        assert l["unresolved"] == 0                                        # every request has its terminal outcome
        assert l["dropped"] == {"late": 0, "inflight": 0}                  # the in-flight bound holds 2 s of arrivals
    assert bad[0]["failed"] == 0 and bad[6]["failed"] == 0


def test_requests_in_flight_are_bounded_and_the_excess_is_a_recorded_drop(tmp_path):
    cfg = dict(config(), max_inflight=5)
    cfg["digest"] = "test"
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], PEAK + 4 * MIN, cfg=cfg,
                 faults=[{"target": N, "from_ms": PEAK, "to_ms": PEAK + 3 * MIN, "mode": "timeout"}])
    for l in minutes(out, N)[:3]:
        identity(l)
        assert l["dropped"]["inflight"] > 0 and l["dropped"]["late"] == 0
        assert l["dispatched"] <= 5 * (MIN // cfg["timeout_ms"]) + 5       # at most 5 in flight, 2 s each
    assert all(l["dropped"]["inflight"] == 0 for l in minutes(out, R))


def test_a_stalled_process_drops_what_it_missed_instead_of_sending_a_burst(tmp_path):
    stall = {"at_ms": PEAK + MIN + 10_000, "for_ms": 5_000}
    cfg, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK, "stalls": [stall]}], PEAK + 5 * MIN)
    lines = minutes(out, N)
    hit = lines[1]
    rate = hit["intended"] / 60.0
    identity(hit)
    assert hit["dropped"]["late"] == pytest.approx(4.0 * rate, abs=2)       # 5 s stalled, 1 s of lateness tolerated
    assert hit["late_max_ms"] <= cfg["max_late_ms"]
    assert hit["due"] == hit["intended"] and hit["dispatched"] == hit["intended"] - hit["dropped"]["late"]
    assert out["truth"][N][str(hit["minute"])]["dispatched"] == hit["dispatched"]   # nothing was sent in a burst
    for l in (lines[0], lines[2], lines[3]):
        assert l["dropped"]["late"] == 0 and l["dispatched"] == l["intended"]


def test_an_arrival_late_across_a_minute_boundary_is_counted_at_its_dispatch_time(tmp_path):
    stall = {"at_ms": PEAK + 2 * MIN - 300, "for_ms": 600}                 # stalled across the boundary, < max_late
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK, "stalls": [stall]}], PEAK + 5 * MIN)
    lines = minutes(out, N)
    before, after = lines[1], lines[2]
    assert before["carried"][1] == after["carried"][0] > 0                 # due in one minute, dispatched in the next
    assert before["dispatched"] == before["due"] - before["carried"][1]
    assert after["dispatched"] == after["due"] + after["carried"][0]
    for l in lines:
        identity(l)
        assert l["dropped"] == {"late": 0, "inflight": 0}
        assert l["dispatched"] == out["truth"][N][str(l["minute"])]["dispatched"]   # the dispatch-time truth
    assert 300 <= after["late_max_ms"] <= 1000


def test_status_failures_are_counted_by_code_and_still_carry_clock_evidence(tmp_path):
    fault = [{"target": N, "from_ms": PEAK + MIN, "to_ms": PEAK + 2 * MIN, "mode": "status", "status": 503}]
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], PEAK + 4 * MIN, faults=fault)
    l = minutes(out, N)[1]
    identity(l)
    assert l["failed"] == l["dispatched"] and l["failed_by"] == {"503": l["failed"]} and l["ok"] == 0
    assert l["clock"] is not None                                          # any HTTP response carries a Date header


@pytest.mark.parametrize("text, seconds", [
    ("Sun, 06 Nov 1994 08:49:37 GMT", 784111777), ("Thu, 12 Nov 2026 00:00:00 GMT", 1794441600),
    (" Thu, 12 Nov 2026 23:59:59 GMT ", 1794527999),
    ("Thu, 12 Nov 2026 00:00:00 UTC", None), ("2026-11-12T00:00:00Z", None), ("Thu, 12 Foo 2026 00:00:00 GMT", None),
    ("", None), (None, None), (1794441600, None),
])
def test_the_http_date_is_parsed_field_by_field(tmp_path, text, seconds):
    _, out = run(tmp_path, [], PEAK, probes=[{"fn": "httpdate", "text": text}])
    assert out["functions"][0]["seconds"] == seconds


def test_every_minute_records_the_clock_evidence_without_correcting_the_schedule(tmp_path):
    skew = 7_000                                                           # the generator's clock is 7 s ahead
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK, "skew_ms": skew}], PEAK + 4 * MIN)
    lines = minutes(out, N)
    assert lines and all(l["clock"] is not None for l in lines)
    for l in lines:
        c = l["clock"]
        offset_hi = c["recv_ms"] - c["date_s"] * 1000
        assert skew <= offset_hi <= skew + 1000 + c["rt_ms"] + 5           # visible in the record...
    # ...and NOT applied: it schedules by its own clock, so in true time everything is 7 s early.
    first = lines[0]
    assert first["minute"] == (PEAK + skew - ORIGIN) // MIN and first["active"][0] == skew % MIN


def test_records_are_bounded_in_size_and_the_log_volume_is_stated(tmp_path):
    fault = [{"target": N, "from_ms": PEAK + MIN, "to_ms": PEAK + 2 * MIN, "mode": "status", "status": 503}]
    _, out = run(tmp_path, [{"pod_uid": "0f2f6a3e-7d1c-4c55-9a53-5b7a0f7c1a11", "start_ms": PEAK}], PEAK + 6 * MIN,
                 faults=fault)
    sizes = [len(json.dumps(l, separators=(",", ":"))) for l in out["lines"] if l["schema"] == "c28-demo-minute/v1"]
    assert max(sizes) < 800
    per_day = 2 * 1440 * 800                                               # two targets, one line a minute
    assert per_day < 2.4 * 2**20 and 14 * per_day < 33 * 2**20             # < 2.4 MiB a day, < 33 MiB in 14 days
    assert max(len(json.dumps(l)) for l in out["lines"]) < 1024


def test_the_prod_shaped_peak_fits_the_in_flight_bound(tmp_path):
    """At the real run's rate (17.5 rps per replica, peak about 165 arrivals a second per target) a target that times
    out on EVERY request still has every arrival dispatched: 2 s of arrivals are in flight, below max_inflight."""
    from demo_helpers import prod_like
    p = prod_like()
    cfg = demo.generator_config(p)
    assert max(cfg["per_minute"]) / 60 > 160 and cfg["max_inflight"] == demo.generator_sizing(p)["max_inflight_per_target"]
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], PEAK + 3 * MIN, cfg=cfg,
                 faults=[{"target": N, "from_ms": PEAK, "to_ms": PEAK + 3 * MIN, "mode": "timeout"}])
    for l in minutes(out, N):
        identity(l)
        assert l["dropped"] == {"late": 0, "inflight": 0} and l["dispatched"] == l["due"]
        assert l["failed"] == l["dispatched"]
