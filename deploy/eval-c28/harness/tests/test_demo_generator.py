"""The demonstration generator's own logic, exercised on the REAL load/c28-demo.js under node (demo_sim.js: k6's
modules replaced by stubs, a virtual clock): position in the day, arrival scheduling, resume without replay, per-minute
bookkeeping, incarnations and segments. Nothing here runs k6: its event loop, http.asyncRequest and exit behaviour are
only covered by the kind smoke's full mode."""

import json

import pytest

from c28 import demo
from demo_helpers import DAY, MIN, changed, config, edited, intended, params, scaled_day, simulate

ORIGIN = 1793491200000                     # 2026-11-01T00:00:00Z, the kind params' schedule origin
PEAK = ORIGIN + 10 * DAY + 18 * 3600_000   # 18:00 on day 10: the ordinary cycle's peak (9c)
N, R = "c28-demo-pa-n", "c28-demo-pa-r"


def minutes(out, target, inc=None):
    return [l for l in out["lines"] if l["schema"] == "c28-demo-minute/v3" and l["target"] == target
            and (inc is None or l["inc"] == inc)]


def events(out, event, target=None):
    return [l for l in out["lines"] if l["schema"] == "c28-demo-incarnation/v2" and l["event"] == event
            and (target is None or l["target"] == target)]


def identity(line):
    d = line["dropped"]
    assert sorted(d) == ["inflight", "late", "segment_end"], line
    assert line["due"] == line["dispatched"] - line["carried"][0] + line["carried"][1] + sum(d.values()), line
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
    assert out["options"]["throw"] is True        # a failed request rejects (recorded as status 0): no k6 warning line each
    assert out["options"]["batch"] == out["options"]["batchPerHost"] == cfg["max_inflight"]


@pytest.mark.parametrize("t, minute, day, day_minute", [
    (ORIGIN, 0, 0, 0), (ORIGIN + 59_999, 0, 0, 0), (ORIGIN + MIN, 1, 0, 1),
    (PEAK + 4_000, 10 * 1440 + 1080, 10, 1080),
    (ORIGIN + 14 * DAY - 1, 14 * 1440 - 1, 13, 1439),         # the last millisecond of day 13
    (ORIGIN + 14 * DAY, 14 * 1440, 14, 0),                    # the next day: ITS row, the minute index is not reset
    (ORIGIN + 28 * DAY - 1, 28 * 1440 - 1, 27, 1439),         # the last millisecond of the schedule
])
def test_position_comes_from_wall_clock_time_and_the_one_origin_day_by_day(tmp_path, t, minute, day, day_minute):
    cfg, out = run(tmp_path, [], PEAK, probes=[{"fn": "position", "t_ms": t}])
    got = out["functions"][0]
    assert (got["minute"], got["day"], got["day_minute"], got["scheduled"]) == (minute, day, day_minute, True)
    assert got["intended"] == cfg["per_minute_days"][day][day_minute] == intended(cfg, minute)
    # The seed of that day is the base seed plus the day: predeclared, consecutive.
    assert cfg["seeds"][day] == cfg["program"]["seed"] + day


@pytest.mark.parametrize("t, minute", [
    (ORIGIN - 1, -1), (ORIGIN - DAY, -1440),                  # before the origin
    (ORIGIN + 28 * DAY, 28 * 1440), (ORIGIN + 29 * DAY + 5 * MIN, 29 * 1440 + 5),   # after the last day
])
def test_outside_the_finite_schedule_nothing_is_intended_and_nothing_wraps(tmp_path, t, minute):
    """Intended is UNDEFINED there (null), not zero and not another day's count."""
    _, out = run(tmp_path, [], PEAK, probes=[{"fn": "position", "t_ms": t}])
    got = out["functions"][0]
    assert got["minute"] == minute and got["scheduled"] is False and got["intended"] is None


def test_arrivals_are_evenly_spaced_inside_their_minute_and_targets_interleave(tmp_path):
    m = 10 * 1440 + 1080
    cfg, out = run(tmp_path, [], PEAK, probes=[{"fn": "arrivals", "minute": m, "target_index": 0},
                                              {"fn": "arrivals", "minute": m, "target_index": 1}])
    a, b = (f["times"] for f in out["functions"])
    n = intended(cfg, m)
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
    (ORIGIN - 240_000, ORIGIN + DAY, "at most 10 min before the origin: it waits, then runs the whole first segment"),
    (ORIGIN - 600_000, ORIGIN + DAY, "exactly pre_origin_max_ms before the origin still waits"),
    (ORIGIN - 600_001, ORIGIN - 300_000, "earlier: it only idles and ends 5 min before the origin"),
    (ORIGIN - 3 * DAY, ORIGIN - 2 * DAY, "long before the origin: it idles at most one segment"),
    (ORIGIN + 28 * DAY + 3_000, ORIGIN + 29 * DAY, "after the schedule's end: it idles to its segment end"),
])
def test_segment_end(tmp_path, start, end, why):
    _, out = run(tmp_path, [], PEAK, probes=[{"fn": "position", "t_ms": start}])
    assert out["functions"][0]["segment_end_ms"] == end, why


@pytest.mark.parametrize("override, text", [
    ({"origin_ms": ORIGIN + 3_600_000}, "origin_ms must be a UTC midnight"),
    ({"origin_ms": ORIGIN + 30_000, "label": "smoke"}, "origin_ms must be a UTC midnight"),   # not even a whole minute
    ({"minute_ms": 1000}, "minute_ms must be 60000"),
    ({"targets": [N, N]}, "distinct DNS labels"),
    ({"base_url": "https://c28-ingress.demo.svc"}, "base_url must be http://"),
    ({"max_late_ms": 60_000}, "max_late_ms must be below one minute"),
    ({"schema": "c28-k6-plan/v1"}, "schema must be c28-demo-generator/v2"),
    ({"schema": "c28-demo-generator/v1"}, "schema must be c28-demo-generator/v2"),
    ({"label": "production"}, "label must be demonstration or smoke"),
    ({"days": 0}, "days must be an integer in 1..28"),
    ({"days": 29}, "days must be an integer in 1..28"),
    ({"days": 27}, "a demonstration has exactly 28 days"),
    ({"days": 27, "label": "smoke"}, "schedule_minutes and schedule_end_ms must be days x 1440"),
    ({"schedule_minutes": 1440}, "schedule_minutes and schedule_end_ms must be days x 1440"),      # a 1-day wrap
    ({"schedule_end_ms": ORIGIN + 29 * DAY}, "schedule_minutes and schedule_end_ms must be days x 1440"),
    ({"per_minute_days": [[1] * 1440] * 27}, "exactly one row per day"),
    ({"per_minute_days": [[1] * 1440] * 29}, "exactly one row per day"),
    ({"per_minute_days": [1] * 1440}, "exactly one row per day"),                                  # the v1 shape
    ({"seeds": list(range(301, 328))}, "seeds must be one seed per day"),
    ({"seeds": [301] * 28}, "seeds must be one seed per day"),
    ({"seeds": list(range(302, 330))}, "program.seed and program.days"),
    ({"pre_origin_max_ms": 900_000}, "pre_origin_max_ms must be 0..600000"),
])
def test_the_script_refuses_a_bad_configuration(tmp_path, override, text):
    _, out = run(tmp_path, [], PEAK, probes=[{"fn": "check", "override": override}])
    assert out["functions"][0]["ok"] is False and text in out["functions"][0]["error"]


@pytest.mark.parametrize("day, row, text", [
    (0, [1] * 1439, "per_minute_days[0] must be 1440"),
    (13, [1] * 1441, "per_minute_days[13] must be 1440"),          # every day's length is checked, not the first's
    (27, [1] * 1439 + [-1], "per_minute_days[27] must be 1440"),   # and every count's range, to the last minute
    (20, [1] * 1439 + [600_001], "per_minute_days[20] must be 1440"),
    (5, [1] * 1439 + [1.5], "per_minute_days[5] must be 1440"),
    (9, "ordinary", "per_minute_days[9] must be 1440"),
])
def test_the_script_checks_every_days_row(tmp_path, day, row, text):
    _, out = run(tmp_path, [], PEAK, probes=[{"fn": "check", "day_rows": {str(day): row}},
                                             {"fn": "check", "day_rows": {str(day): [7] * 1440}}])
    bad, good = out["functions"]
    assert bad["ok"] is False and text in bad["error"]
    assert good["ok"] is True


def test_only_a_configuration_labelled_smoke_may_start_off_a_utc_midnight(tmp_path):
    """The smoke-only relaxation: an origin on a whole minute that is not a midnight (so a smoke can put a day
    boundary a few minutes ahead). A configuration labelled demonstration can never use it, in Python or in the
    script."""
    p = params()
    odd = ORIGIN + 17 * 3600_000 + 23 * MIN
    smoke = demo.generator_config(p, smoke_segment_minutes=2, smoke_origin_ms=odd)
    assert smoke["label"] == "smoke" and smoke["origin_ms"] == odd and smoke["origin_utc"] == "2026-11-01T17:23:00Z"
    assert smoke["schedule_end_ms"] == odd + 28 * DAY and demo.config_problems(smoke) == []
    with pytest.raises(demo.R.ParamsError, match="smoke-only option"):
        demo.generator_config(p, smoke_origin_ms=odd)                       # no smoke segment: a demonstration
    with pytest.raises(demo.R.ParamsError, match="whole minute"):
        demo.generator_config(p, smoke_segment_minutes=2, smoke_origin_ms=odd + 1_000)
    assert demo.generator_config(p)["origin_ms"] == ORIGIN                  # the params' midnight, always
    _, out = run(tmp_path, [], PEAK, cfg=smoke, probes=[
        {"fn": "check", "override": {}},
        {"fn": "check", "override": {"label": "demonstration"}},
        {"fn": "position", "t_ms": odd + DAY + 30_000}])
    ok, relabelled, pos = out["functions"]
    assert ok["ok"] is True
    assert relabelled["ok"] is False and "origin_ms must be a UTC midnight" in relabelled["error"]
    assert (pos["minute"], pos["day"], pos["day_minute"]) == (1440, 1, 0)   # days are counted from THAT origin
    relabelled = edited(smoke, label="demonstration")                       # even with a recomputed digest
    assert any("UTC midnight" in x for x in demo.config_problems(relabelled))


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
        assert l["dropped"] == {"late": 0, "inflight": 0, "segment_end": 0} and l["failed"] == 0 and l["unresolved"] == 0
        assert l["dispatched"] == out["truth"][N][str(l["minute"])]["dispatched"]
    for l in lines[1:]:
        assert l["active"] == [0, MIN] and l["due"] == l["intended"] == intended(cfg, l["minute"])
        assert (l["day"], l["day_minute"]) == (10, l["minute"] - 10 * 1440)
    assert [l["minute"] for l in lines] == list(range(m0, m0 + len(lines)))  # one line per minute, none skipped
    assert [l["n"] for l in lines] == list(range(1, len(lines) + 1))
    st = events(out, "start", N)[0]
    assert st["active_from_ms"] == start and st["minute"] == m0 and st["origin_ms"] == ORIGIN
    assert (st["phase"], st["day"], st["send_from_ms"], st["send_to_ms"]) == ("scheduled", 10, start, ORIGIN + 11 * DAY)
    assert st["schedule_end_ms"] == ORIGIN + 28 * DAY and st["days"] == 28
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
    assert truth[str(base + 3)]["dispatched"] < intended(cfg, base + 3)    # the kill minute: only until the kill
    assert truth[str(base + 5)]["dispatched"] == new[0]["dispatched"] < intended(cfg, base + 5)
    total_sent = sum(v["dispatched"] for v in truth.values())
    total_intended = sum(intended(cfg, base + k) for k in range(10))
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
        assert l["dropped"] == {"late": 0, "inflight": 0, "segment_end": 0}                  # the in-flight bound holds 2 s of arrivals
    assert bad[0]["failed"] == 0 and bad[6]["failed"] == 0


def test_requests_in_flight_are_bounded_and_the_excess_is_a_recorded_drop(tmp_path):
    cfg = edited(config(), max_inflight=5)
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
        assert l["dropped"] == {"late": 0, "inflight": 0, "segment_end": 0}
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
    sizes = [len(json.dumps(l, separators=(",", ":"))) for l in out["lines"] if l["schema"] == "c28-demo-minute/v3"]
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
    assert max(map(max, cfg["per_minute_days"])) / 60 > 160
    assert cfg["max_inflight"] == demo.generator_sizing(p)["max_inflight_per_target"]
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], PEAK + 3 * MIN, cfg=cfg,
                 faults=[{"target": N, "from_ms": PEAK, "to_ms": PEAK + 3 * MIN, "mode": "timeout"}])
    for l in minutes(out, N):
        identity(l)
        assert l["dropped"] == {"late": 0, "inflight": 0, "segment_end": 0} and l["dispatched"] == l["due"]
        assert l["failed"] == l["dispatched"]


# --- the finite 28-day schedule: day changes, the origin, the end, clock steps -------------------------------------

D11 = ORIGIN + 11 * DAY                    # the UTC midnight between day 10 and day 11
M11 = 11 * 1440                            # its minute index


def two_day_segments():
    """A demonstration configuration whose segment spans the midnight between day 10 and day 11 (48-hour segments from
    the origin), with day 11 at half of its own counts: the two adjacent days cannot be mistaken for each other."""
    cfg = config(changed(params(), "demonstration.generator.segment_hours", 48))
    assert cfg["label"] == "demonstration" and cfg["segment_minutes"] == 2880
    return scaled_day(cfg, 11, 0.5)


def test_a_utc_day_change_inside_one_incarnation_uses_the_next_days_row(tmp_path):
    cfg = two_day_segments()
    start = D11 - 3 * MIN
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}], D11 + 4 * MIN + 10_000, cfg=cfg)
    lines = minutes(out, N)
    assert [l["minute"] for l in lines] == list(range(M11 - 3, M11 + 4))
    assert [(l["day"], l["day_minute"]) for l in lines] == [(10, 1437), (10, 1438), (10, 1439), (11, 0), (11, 1),
                                                           (11, 2), (11, 3)]
    assert len({l["inc"] for l in lines}) == 1                             # ONE incarnation across the midnight
    for l in lines:
        identity(l)
        row = cfg["per_minute_days"][l["day"]]
        assert l["intended"] == row[l["day_minute"]] == l["due"] == l["dispatched"]   # that day's own row
        assert l["dispatched"] == out["truth"][N][str(l["minute"])]["dispatched"]
        assert l["active"] == [0, MIN] and l["dropped"] == {"late": 0, "inflight": 0, "segment_end": 0}
    before, after = lines[2], lines[3]
    assert after["intended"] < 0.6 * before["intended"]                    # the counts switch AT the boundary
    assert after["intended"] != cfg["per_minute_days"][10][0]              # and not to day 10's minute 0 (no wrap)
    assert events(out, "end") == [] and events(out, "exhausted") == []


def test_adjacent_days_of_the_real_table_are_unequal_and_each_is_its_own_seeds_program(tmp_path):
    from c28 import schedule
    cfg = config()
    table = cfg["per_minute_days"]
    assert len(table) == 28 == len(cfg["seeds"]) and cfg["seeds"] == list(range(301, 329))
    for d, row in enumerate(table):
        assert row == [round(u * 2 * 60) for u in schedule.program("ordinary", 301 + d)]
    assert all(a != b for a, b in zip(table, table[1:]))                   # no two adjacent days are the same
    assert len({tuple(r) for r in table}) == 28                            # and no day repeats at all
    differing = [sum(1 for x, y in zip(a, b) if x != y) for a, b in zip(table, table[1:])]
    assert min(differing) > 1000                                           # most minutes differ day to day
    # The generator, across the real (unscaled) midnight, follows the two rows.
    seg48 = config(changed(params(), "demonstration.generator.segment_hours", 48))
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": D11 - 2 * MIN}], D11 + 3 * MIN + 10_000, cfg=seg48)
    got = [(l["day"], l["intended"]) for l in minutes(out, N)]
    assert got == [(10, table[10][1438]), (10, table[10][1439]), (11, table[11][0]), (11, table[11][1]),
                   (11, table[11][2])]


def test_a_restart_across_midnight_resumes_on_the_new_day_without_replay(tmp_path):
    """24-hour segments: the planned restart IS the midnight. The old incarnation ends on day 10's last minute; the new
    one starts 3 s into day 11, takes day 11's row, and sends neither day 10's tail nor the 3 s it missed."""
    cfg = scaled_day(config(), 11, 0.5)
    a, b = D11 - 6 * MIN, D11 + 3_000
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": a}, {"pod_uid": "pod-a", "start_ms": b}],
                 D11 + 4 * MIN + 10_000, cfg=cfg, probes=[{"fn": "arrivals", "minute": M11, "target_index": 0}])
    old, new = minutes(out, N, f"pod-a/{a}"), minutes(out, N, f"pod-a/{b}")
    assert [l["minute"] for l in old] == list(range(M11 - 6, M11)) and {l["day"] for l in old} == {10}
    end = events(out, "end", N)[0]
    assert end["inc"] == f"pod-a/{a}" and end["segment_end_ms"] == D11 and end["exhausted"] is False
    st = events(out, "start", N)[1]
    assert (st["phase"], st["day"], st["minute"], st["send_from_ms"]) == ("scheduled", 11, M11, b)
    assert st["segment_end_ms"] == D11 + DAY
    first = new[0]
    assert (first["minute"], first["day"], first["day_minute"], first["active"]) == (M11, 11, 0, [3_000, MIN])
    assert first["intended"] == cfg["per_minute_days"][11][0] != cfg["per_minute_days"][10][0]
    scheduled = out["functions"][0]["times"]
    assert len(scheduled) == first["intended"]
    assert first["due"] == first["dispatched"] == sum(1 for t in scheduled if t >= b) < first["intended"]   # no replay
    assert out["truth"][N][str(M11)]["dispatched"] == first["dispatched"]
    for l in new[1:]:
        identity(l)
        assert l["day"] == 11 and l["due"] == l["dispatched"] == l["intended"] == cfg["per_minute_days"][11][l["day_minute"]]
    # A KILLED incarnation and a late restart across the same midnight: still the clock's day, still no replay.
    kill, again = D11 - 30_000, D11 + 20_000
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": a, "kill_ms": kill},
                            {"pod_uid": "pod-b", "start_ms": again}], D11 + 3 * MIN + 10_000, cfg=cfg)
    new = minutes(out, N, f"pod-b/{again}")
    assert (new[0]["day"], new[0]["day_minute"], new[0]["active"]) == (11, 0, [20_000, MIN])
    truth = out["truth"][N]
    assert truth[str(M11 - 1)]["dispatched"] < cfg["per_minute_days"][10][1439]      # the killed tail is not sent later
    assert truth[str(M11)]["dispatched"] == new[0]["dispatched"] < cfg["per_minute_days"][11][0]
    assert out["sends"][N] == sum(v["dispatched"] for v in truth.values())


def test_a_start_shortly_before_the_origin_sends_nothing_until_the_origin_and_says_so(tmp_path):
    cfg = config()
    start = ORIGIN - 4 * MIN - 7_000
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}], ORIGIN + 3 * MIN + 10_000, cfg=cfg)
    st = events(out, "start", N)[0]
    assert st["phase"] == "pre-origin" and st["active_from_ms"] == start
    assert (st["send_from_ms"], st["send_to_ms"], st["minute"], st["day"]) == (ORIGIN, ORIGIN + DAY, 0, 0)
    assert st["segment_end_ms"] == ORIGIN + DAY                            # it runs the whole first segment
    truth = out["truth"][N]
    assert all(int(m) >= 0 for m in truth)                                 # not one request before the origin
    lines = minutes(out, N)
    assert [l["minute"] for l in lines] == [0, 1, 2]                       # no line for a minute before the origin
    for l in lines:
        identity(l)
        assert l["active"] == [0, MIN] and l["day"] == 0
        assert l["due"] == l["dispatched"] == l["intended"] == cfg["per_minute_days"][0][l["minute"]]
        assert l["dispatched"] == truth[str(l["minute"])]["dispatched"]
    assert lines[0]["n"] == 1                                              # the start record, then minute 0


def test_a_start_long_before_the_origin_only_idles_and_exits_before_it(tmp_path):
    start = ORIGIN - 2 * 3600_000
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start},
                            {"pod_uid": "pod-a", "start_ms": ORIGIN - 5 * MIN + 12_000}], ORIGIN + 2 * MIN + 10_000)
    first, second = events(out, "start", N)
    assert (first["phase"], first["send_from_ms"], first["send_to_ms"], first["minute"]) == ("pre-origin", None, None, None)
    assert first["segment_end_ms"] == ORIGIN - 5 * MIN
    end = events(out, "end", N)[0]
    assert end["inc"] == f"pod-a/{start}" and end["ended_ms"] == ORIGIN - 5 * MIN and end["cum"]["due"] == 0
    assert [l["n"] for l in out["lines"] if l["inc"] == f"pod-a/{start}" and l["target"] == N] == [0, 1]   # start, end
    # The restarted incarnation is within pre_origin_max_ms: it waits and starts with minute 0, complete.
    assert (second["phase"], second["send_from_ms"], second["minute"]) == ("pre-origin", ORIGIN, 0)
    assert all(int(m) >= 0 for m in out["truth"][N])
    assert [(l["minute"], l["active"]) for l in minutes(out, N)] == [(0, [0, MIN]), (1, [0, MIN])]


def exhausting():
    """A smoke configuration of TWO days with 7-minute segments: the schedule ends at minute 2880, three minutes into
    the segment [2877, 2884)."""
    p = changed(params(), "demonstration.program.days", 2)
    cfg = config(p, smoke_segment_minutes=7)
    assert cfg["days"] == 2 and cfg["schedule_minutes"] == 2880 and cfg["schedule_end_ms"] == ORIGIN + 2 * DAY
    assert cfg["seeds"] == [301, 302] and len(cfg["per_minute_days"]) == 2
    return cfg


def test_the_schedule_ends_inside_an_incarnation_and_nothing_wraps(tmp_path):
    cfg = exhausting()
    end, seg_end = ORIGIN + 2 * DAY, ORIGIN + 2884 * MIN
    start = ORIGIN + 2877 * MIN + 20_000
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}], seg_end + 2 * MIN, cfg=cfg)
    st = events(out, "start", N)[0]
    assert (st["phase"], st["send_to_ms"], st["segment_end_ms"], st["schedule_end_ms"]) == ("scheduled", end, seg_end, end)
    lines = minutes(out, N)
    assert [l["minute"] for l in lines] == [2877, 2878, 2879]              # the last line is the last day's last minute
    assert [(l["day"], l["day_minute"]) for l in lines] == [(1, 1437), (1, 1438), (1, 1439)]
    assert lines[-1]["active"] == [0, MIN] and lines[-1]["due"] == lines[-1]["dispatched"] == cfg["per_minute_days"][1][1439]
    for l in lines:
        identity(l)
    truth = out["truth"][N]
    assert max(int(m) for m in truth) == 2879                              # NOT ONE request after the end: no wrap to
    assert out["sends"][N] == sum(l["dispatched"] for l in lines)          # day 0, no repeat of the last day
    ex = events(out, "exhausted", N)
    assert len(ex) == 1                                                    # ONE clear record
    assert ex[0]["reason"] == "schedule-exhausted" and ex[0]["at_ms"] == end == ex[0]["schedule_end_ms"]
    assert ex[0]["last_minute"] == 2879 and ex[0]["days"] == 2
    fin = events(out, "end", N)
    assert len(fin) == 1 and fin[0]["ended_ms"] == seg_end and fin[0]["exhausted"] is True   # it idled to its segment end
    assert fin[0]["reason"] == "segment-end" and fin[0]["inflight"] == 0
    assert fin[0]["cum"]["dispatched"] == sum(l["dispatched"] for l in lines)
    ns = [l["n"] for l in out["lines"] if l["target"] == N]
    assert ns == list(range(len(ns)))                                      # start, 3 lines, exhausted, end: no gap


def test_an_incarnation_started_after_the_end_writes_the_record_sends_nothing_and_idles_to_its_segment_end(tmp_path):
    cfg = exhausting()
    start = ORIGIN + 2884 * MIN + 2_000                                    # the restart after the segment above
    seg_end = ORIGIN + 2891 * MIN
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}, {"pod_uid": "pod-a", "start_ms": seg_end + 2_000}],
                 seg_end + 10 * MIN, cfg=cfg)
    for t in (N, R):
        mine = [l for l in out["lines"] if l["target"] == t and l["inc"] == f"pod-a/{start}"]
        assert [(l["schema"], l["event"]) for l in mine] == [("c28-demo-incarnation/v2", "start"),
                                                             ("c28-demo-incarnation/v2", "exhausted"),
                                                             ("c28-demo-incarnation/v2", "end")]
        st, ex, fin = mine
        assert (st["phase"], st["send_from_ms"], st["send_to_ms"], st["minute"], st["day"]) == \
            ("schedule-exhausted", None, None, None, None)
        assert ex["reason"] == "schedule-exhausted" and ex["at_ms"] == start
        assert fin["ended_ms"] == seg_end and fin["exhausted"] is True     # no hot loop: almost a whole segment idle
        assert fin["cum"] == {"due": 0, "dispatched": 0, "dropped": 0, "ok": 0, "failed": 0, "late_completions": 0}
    assert minutes(out, N) == [] and minutes(out, R) == []                 # no per-minute line: intended is undefined
    assert out["truth"] == {N: {}, R: {}} and out["sends"] == {N: 0, R: 0}
    # The next incarnation does the same, one segment later: three records per target per segment, nothing else.
    assert len(out["lines"]) == 2 * 3 * 2
    assert [e["segment_end_ms"] for e in events(out, "end", N)] == [seg_end, seg_end + 7 * MIN]


def test_the_last_segment_of_the_28_days_ends_with_the_schedule(tmp_path):
    """The demonstration's shape: 24-hour segments, so the last incarnation's segment end IS the schedule's end."""
    cfg = config()
    end = ORIGIN + 28 * DAY
    start = end - 10 * MIN
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}, {"pod_uid": "pod-a", "start_ms": end + 4_000}],
                 end + 6 * MIN, cfg=cfg)
    lines = minutes(out, N)
    assert [l["minute"] for l in lines] == list(range(28 * 1440 - 10, 28 * 1440))
    assert {l["day"] for l in lines} == {27} and lines[-1]["day_minute"] == 1439
    assert lines[-1]["intended"] == cfg["per_minute_days"][27][1439] == lines[-1]["dispatched"]
    ex = events(out, "exhausted", N)
    assert [e["at_ms"] for e in ex] == [end, end + 4_000]                  # the ending incarnation, then the idle one
    fin = events(out, "end", N)
    assert len(fin) == 1 and fin[0]["segment_end_ms"] == end and fin[0]["exhausted"] is True
    assert max(int(m) for m in out["truth"][N]) == 28 * 1440 - 1
    idle = events(out, "start", N)[1]
    assert idle["phase"] == "schedule-exhausted" and idle["segment_end_ms"] == end + DAY


def test_a_forward_clock_jump_drops_what_was_skipped_as_late_and_sends_no_burst(tmp_path):
    cfg = config()
    step = {"at_ms": PEAK + MIN + 10_000, "by_ms": 150_000}                # the clock jumps 2.5 minutes ahead
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK, "clock_steps": [step]}], PEAK + 4 * MIN, cfg=cfg)
    base = 10 * 1440 + 1080
    lines = {l["minute"] - base: l for l in minutes(out, N)}
    for l in lines.values():
        identity(l)
        assert l["due"] == l["intended"] == intended(cfg, l["minute"])     # every arrival is accounted for
        assert l["dropped"]["inflight"] == 0 and l["late_max_ms"] <= cfg["max_late_ms"]
    rate = lines[1]["intended"] / 60.0
    assert lines[0]["dropped"]["late"] == 0 and lines[0]["dispatched"] == lines[0]["intended"]
    assert lines[1]["dispatched"] == pytest.approx(10 * rate, abs=2)       # the 10 s before the jump
    assert lines[1]["dropped"]["late"] == lines[1]["intended"] - lines[1]["dispatched"]
    assert lines[2]["dispatched"] == 0 and lines[2]["dropped"]["late"] == lines[2]["intended"]   # skipped whole
    # It lands at second 40 of minute 3: only the last max_late_ms (1 s) of arrivals may still be sent, the rest of
    # the 40 s are late. No burst: at most one second's arrivals at once.
    assert lines[3]["dropped"]["late"] == pytest.approx(39 * (lines[3]["intended"] / 60.0), abs=2)
    assert lines[3]["dispatched"] == lines[3]["intended"] - lines[3]["dropped"]["late"]
    assert lines[3]["dispatched"] == pytest.approx(21 * (lines[3]["intended"] / 60.0), abs=2)   # 1 s late + 20 s left
    for k in (4, 5):
        assert lines[k]["dispatched"] == lines[k]["intended"] and lines[k]["dropped"]["late"] == 0   # back to normal
    # Everything it sent is in a line or in the minute still running when the simulation stopped: nothing extra.
    running = out["sends"][N] - sum(l["dispatched"] for l in lines.values())
    assert 0 <= running <= intended(cfg, base + 6)


def test_a_forward_clock_jump_of_days_is_accounted_in_bulk_and_ends_the_segment(tmp_path):
    """A jump past the segment end (and across a midnight): every skipped minute of THIS segment is a line with its
    own day's intended count, all late; nothing is sent; the incarnation ends."""
    cfg = config()
    start = PEAK
    step = {"at_ms": PEAK + 30_000, "by_ms": 3 * DAY}
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start, "clock_steps": [step]}], PEAK + 2 * MIN, cfg=cfg)
    lines = minutes(out, N)
    base = 10 * 1440 + 1080
    assert [l["minute"] for l in lines] == list(range(base, 11 * 1440))    # to its segment end, not beyond
    assert sum(l["dispatched"] for l in lines) == out["sends"][N] == lines[0]["dispatched"]
    assert lines[0]["dispatched"] == pytest.approx(lines[0]["intended"] / 2, abs=2)
    for l in lines[1:]:
        assert l["dispatched"] == 0 and l["dropped"]["late"] == l["due"] == l["intended"] == intended(cfg, l["minute"])
    assert len(events(out, "end", N)) == 1


def test_a_backward_clock_step_never_sends_an_arrival_twice(tmp_path):
    cfg = config()
    back = 45_000
    step = {"at_ms": PEAK + MIN + 30_000, "by_ms": -back}                  # back into minute 0, whose line is written
    until = PEAK + 5 * MIN + back + 10_000
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK, "clock_steps": [step]}], until, cfg=cfg)
    base = 10 * 1440 + 1080
    lines = minutes(out, N)
    assert [l["minute"] for l in lines] == [base + k for k in range(5)]    # each minute written once, in order
    for l in lines:
        identity(l)
        assert l["due"] == l["dispatched"] == l["intended"] == intended(cfg, l["minute"])   # once each, never twice
        assert l["dropped"] == {"late": 0, "inflight": 0, "segment_end": 0}
    # While its clock repeated the 45 s it had already covered, it sent NOTHING: in TRUE time, minute 1 holds only
    # the 30 s before the step, and minute 2 the rest of its minute 1 (30 s) and the first 15 s of its minute 2.
    truth = out["truth"][N]
    assert truth[str(base)]["dispatched"] == lines[0]["dispatched"]
    assert truth[str(base + 1)]["dispatched"] == pytest.approx(lines[1]["intended"] / 2, abs=2)
    assert truth[str(base + 2)]["dispatched"] == pytest.approx(lines[1]["intended"] / 2 + lines[2]["intended"] / 4, abs=3)
    # In total every arrival was sent exactly once: the five written minutes plus the minute still running at the end.
    running = out["sends"][N] - sum(l["dispatched"] for l in lines)
    assert 0 <= running <= intended(cfg, base + 5)
    assert [l["n"] for l in lines] == list(range(1, 6)) and events(out, "start", N)[0]["n"] == 0


# --- the configuration that runs is the configuration that was reviewed (R3 item 3b) --------------------------------

def real_configs():
    from demo_helpers import prod_like
    return {"kind": config(), "prod-like": config(prod_like()),
            "smoke": config(smoke_segment_minutes=2, smoke_origin_ms=ORIGIN + 17 * 3600_000 + 23 * MIN),
            "two-days": config(changed(params(), "demonstration.program.days", 2), smoke_segment_minutes=7)}


@pytest.mark.parametrize("name", ["kind", "prod-like", "smoke", "two-days"])
def test_the_script_recomputes_the_digest_of_the_real_configuration_exactly_as_python_does(tmp_path, name):
    """schedule.digest in Python and configDigest in the script give the same SHA-256 for the REAL generated
    configuration (the whole table, every number and string in it); the script loaded it, so its init check passed."""
    from c28 import schedule
    cfg = real_configs()[name]
    assert demo.canonical_problems(cfg) == []                              # integers, strings, booleans only
    assert isinstance(cfg["program"]["capacity_per_replica_rps"], str)     # "2", never 2.0: one spelling in both
    _, out = run(tmp_path, [], PEAK, cfg=cfg, probes=[{"fn": "digest"}])
    got = out["functions"][0]
    assert got["digest"] == got["stated"] == cfg["digest"] == schedule.digest(cfg)
    import hashlib
    body = json.dumps({k: v for k, v in cfg.items() if k != "digest"}, sort_keys=True, separators=(",", ":"))
    assert hashlib.sha256(body.encode()).hexdigest() == got["digest"]


def test_the_canonical_form_is_pythons_byte_for_byte(tmp_path):
    values = [
        {"b": 1, "a": [3, 2, 1], "Z": None, "a10": True, "a9": False, "": 0, "nested": {"y": {"k": -7}, "x": []}},
        [0, -1, 2**53 - 1, -(2**53 - 1), 1794367800000, 600000],
        "plain", "", "quote\" backslash\\ slash/ </script>", "tab\tnewline\ncr\r bell\u0007 del\u007f nul\u0000",
        "café 中文 \U0001f600   ퟿ ￿", {"clé": "valeur", "a": "é"},
        [], {}, [[], {}, [{}]], True, False, None, 0,
    ]
    cfg = config()
    _, out = run(tmp_path, [], PEAK, cfg=cfg, probes=[{"fn": "canonical", "value": v} for v in values])
    for v, got in zip(values, out["functions"]):
        assert got.get("text") == json.dumps(v, sort_keys=True, separators=(",", ":")), (v, got)
    # A float cannot be written identically (Python 2.5e-07 / JavaScript 2.5e-7; Python 2.0 / JavaScript 2): the
    # script refuses any non-integer, and Python refuses every float before the configuration is digested.
    _, out = run(tmp_path, [], PEAK, cfg=cfg, probes=[{"fn": "canonical", "value": v} for v in (1.5, [1, 2.5e-07], {"a": 0.1})])
    assert all("is not a safe integer" in f["error"] for f in out["functions"])
    for bad in ({"a": 2.0}, {"a": [1, 1.5]}, {"a": 2**53}, {"a": {"b": 1e-7}}):
        assert demo.canonical_problems(bad), bad
    assert demo.canonical_problems({"a": [1, "x", True, None, {"b": -5}]}) == []


def test_an_altered_table_with_its_old_digest_is_refused_at_start(tmp_path):
    """The reviewer's case: one count of one day edited after rendering, the digest left as it was. Every structural
    check still passes; the recomputed digest does not."""
    from demo_helpers import run_sim
    cfg = config()
    table = [list(r) for r in cfg["per_minute_days"]]
    table[17][600] += 1
    altered = dict(cfg, per_minute_days=table)                             # the OLD digest
    rc, doc = run_sim(tmp_path, altered, {"incarnations": [{"pod_uid": "pod-a", "start_ms": PEAK}], "until_ms": PEAK + MIN})
    assert rc == 1 and "is not the one its digest names" in doc["error"] and "refusing to start" in doc["error"]
    assert cfg["digest"] in doc["error"] and "lines" not in doc            # nothing ran, nothing was sent
    rc, doc = run_sim(tmp_path, edited(cfg, per_minute_days=table), {"incarnations": [], "until_ms": PEAK})
    assert rc == 0                                                         # the same table under ITS digest runs
    probes = [
        {"fn": "check", "cells": [[17, 600, table[17][600]]], "keep_digest": True},
        {"fn": "check", "cells": [[0, 0, cfg["per_minute_days"][0][0] + 1]], "keep_digest": True},
        {"fn": "check", "cells": [[27, 1439, 0]], "keep_digest": True},
        {"fn": "check", "day_rows": {"3": cfg["per_minute_days"][4], "4": cfg["per_minute_days"][3]}, "keep_digest": True},
        {"fn": "check", "override": {"max_inflight": cfg["max_inflight"] + 1}, "keep_digest": True},
        {"fn": "check", "override": {"base_url": "http://elsewhere.demo.svc"}, "keep_digest": True},
        {"fn": "check", "override": {"targets": list(reversed(cfg["targets"]))}, "keep_digest": True},
        {"fn": "check", "override": {"origin_ms": ORIGIN + DAY, "schedule_end_ms": ORIGIN + 29 * DAY}, "keep_digest": True},
        {"fn": "check", "override": {"run_id": "c28-other"}, "keep_digest": True},
        {"fn": "check", "override": {"program": dict(cfg["program"], capacity_per_replica_rps="3")}, "keep_digest": True},
        {"fn": "check", "override": {"digest": "0" * 64}, "keep_digest": True},
        {"fn": "check", "override": {}, "keep_digest": True},                         # untouched: accepted
        {"fn": "check", "cells": [[17, 600, table[17][600]]]},                        # edited AND re-digested: accepted
        {"fn": "check", "override": {}, "no_hash": True},                             # no hash function: refused
    ]
    _, out = run(tmp_path, [], PEAK, cfg=cfg, probes=probes)
    got = out["functions"]
    for g in got[:11]:
        assert g["ok"] is False and "is not the one its digest names" in g["error"], g
    assert got[11] == {"ok": True} and got[12] == {"ok": True}
    assert got[13]["ok"] is False and "no SHA-256 function" in got[13]["error"]


def test_the_start_record_names_the_configuration_it_runs(tmp_path):
    cfg, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], PEAK + 2 * MIN)
    st = events(out, "start", N)[0]
    assert (st["config_schema"], st["config_digest"], st["origin_ms"], st["schedule_end_ms"], st["run"]) == \
        ("c28-demo-generator/v2", cfg["digest"], ORIGIN, ORIGIN + 28 * DAY, cfg["run_id"])


def test_the_clock_evidence_of_a_minute_is_its_response_with_the_shortest_round_trip(tmp_path):
    """A slow first response (1.5 s) would bound the offset only to about 2.5 s; the minute keeps the tightest
    evidence it saw instead."""
    slow = [{"from_ms": PEAK + MIN, "to_ms": PEAK + MIN + 5_000, "latency_ms": 1_500}]
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], PEAK + 4 * MIN, latencies=slow)
    lines = minutes(out, N)
    assert lines[1]["lat"]["max_ms"] == 1_500                              # the slow responses are in the minute
    for l in lines:
        c = l["clock"]
        assert c["rt_ms"] == 20                                            # ... but not its clock evidence
        lo, hi = c["recv_ms"] - c["rt_ms"] - c["date_s"] * 1000 - 1000, c["recv_ms"] - c["date_s"] * 1000
        assert -1_100 <= lo <= 0 <= hi <= 1_100


# --- nothing is sent at or after the end of the span; what is owed there is a recorded drop (R4) -------------------

def scheduled_in(cfg, target, minute, a0, a1):
    from c28 import demo_reduce
    return demo_reduce.scheduled_arrivals(cfg, target, minute, a0, a1)


@pytest.mark.parametrize("where, before_ms, for_ms", [
    ("segment-end", 100, 500),         # the reviewer's case: 2,178 sends against 2,176 recorded dispatches per target
    ("schedule-end", 100, 500),        # the same at the final schedule boundary
    ("segment-end", 600, 1_000),
    ("schedule-end", 600, 1_000),
    ("segment-end", 1_500, 2_000),     # longer than max_late_ms: the older arrivals are "late", the rest "segment-end"
    ("schedule-end", 1_500, 2_000),
    ("segment-end", 100, 70_000),      # a stall of more than a minute across the end
])
def test_a_process_delayed_across_the_end_of_its_span_sends_nothing_after_it(tmp_path, where, before_ms, for_ms):
    """A stall that begins shortly before the segment end (or the schedule's end) and ends after it. The arrivals of
    the stalled stretch are still owed when the process wakes, less than max_late_ms late, but the span is over: a
    send now would fall into a minute this incarnation never writes a line for. They are dropped, reason
    segment-end, and ACTUAL SENDS EQUAL RECORDED DISPATCHES."""
    cfg = config(smoke_segment_minutes=2)
    end = PEAK + 2 * MIN if where == "segment-end" else ORIGIN + 28 * DAY
    start = end - 2 * MIN
    stall = {"at_ms": end - before_ms, "for_ms": for_ms}
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start, "stalls": [stall]}], end + 3 * MIN, cfg=cfg)
    m_last = (end - ORIGIN) // MIN - 1
    owed_total = 0
    for t in (N, R):
        lines = minutes(out, t)
        assert [l["minute"] for l in lines] == [m_last - 1, m_last]
        for l in lines:
            identity(l)
            assert l["due"] == l["intended"] and l["active"] == [0, MIN]
        recorded = sum(l["dispatched"] for l in lines)
        assert out["sends"][t] == recorded                                 # every send is a recorded dispatch
        truth = out["truth"][t]
        assert sorted(int(m) for m in truth) == [m_last - 1, m_last]       # and lies in a minute with an emitted line
        assert max(int(m) for m in truth) < (end - ORIGIN) // MIN          # nothing at or after the end
        for l in lines:
            assert truth[str(l["minute"])]["dispatched"] == l["dispatched"]
        last = lines[-1]
        assert last["carried"] == [0, 0] and lines[0]["carried"] == [0, 0]  # nothing is carried out of the span
        stalled = scheduled_in(cfg, t, m_last, MIN - before_ms, MIN)       # the arrivals of the stalled stretch
        late_part = scheduled_in(cfg, t, m_last, MIN - before_ms, MIN - before_ms + max(0, for_ms - cfg["max_late_ms"]))
        late_part = min(late_part, stalled)
        assert last["dropped"] == {"late": late_part, "inflight": 0, "segment_end": stalled - late_part}, last
        assert last["dispatched"] == last["intended"] - stalled
        owed_total += stalled
        fin = events(out, "end", t)
        assert len(fin) == 1 and fin[0]["cum"]["dispatched"] == recorded == out["sends"][t]
        assert fin[0]["cum"]["dropped"] == stalled and fin[0]["inflight"] == 0
    assert owed_total > 0                                                  # the case really had owed arrivals
    if (where, before_ms, for_ms) == ("segment-end", 100, 500):            # the reviewer's numbers, after the fix
        assert out["sends"] == {N: 2176, R: 2176}                          # was 2,178 sent against 2,176 recorded
        assert [minutes(out, t)[-1]["dropped"]["segment_end"] for t in (N, R)] == [2, 2]
    if (where, before_ms, for_ms) == ("schedule-end", 100, 500):
        assert out["sends"] == {N: 484, R: 483}                            # was 484 sent against 484 / 483 recorded
        assert [minutes(out, t)[-1]["dropped"]["segment_end"] for t in (N, R)] == [0, 1]
        assert len(events(out, "exhausted", N)) == 1


def test_a_short_stall_inside_the_span_still_carries_into_the_next_minute_and_is_recorded_there(tmp_path):
    """The drop applies only at the END of the span: across an ordinary minute boundary a delayed arrival is still
    sent, carried out of its minute and into the next line of the same incarnation."""
    cfg = config(smoke_segment_minutes=4)
    start = PEAK                                                           # segment [PEAK, PEAK + 4 min)
    stall = {"at_ms": PEAK + 2 * MIN - 300, "for_ms": 600}
    _, out = run(tmp_path, [{"pod_uid": "pod-a", "start_ms": start, "stalls": [stall]}], PEAK + 7 * MIN, cfg=cfg)
    for t in (N, R):
        lines = minutes(out, t)
        assert len(lines) == 4
        assert [l["carried"] for l in lines] == [[0, 0], [0, lines[1]["carried"][1]], [lines[1]["carried"][1], 0], [0, 0]]
        assert lines[1]["carried"][1] > 0
        assert all(l["dropped"] == {"late": 0, "inflight": 0, "segment_end": 0} for l in lines)
        assert out["sends"][t] == sum(l["dispatched"] for l in lines) == sum(l["intended"] for l in lines)
