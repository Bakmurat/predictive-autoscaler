"""The demonstration reducer: per-minute rows binned from the RECORDED origin, with explicit states. The generator
records and the ingress samples come from the real script under the node simulation (demo_sim.js), so the reducer is
tested against what the generator actually writes, with the simulation's ground truth beside it."""

import copy
import json

import pytest

from c28 import demo_reduce as DR
from demo_helpers import DAY, MIN, changed, config, edited, intended, params, scaled_day, scrape, simulate

ORIGIN = 1793491200000
PEAK = ORIGIN + 10 * DAY + 18 * 3600_000
BASE = 10 * 1440 + 1080                    # the minute index of PEAK
N, R = "c28-demo-pa-n", "c28-demo-pa-r"


def sim(tmp_path, incarnations, minutes_after=12, cfg=None, before=3, **extra):
    cfg = cfg or config()
    lo, hi = PEAK - before * MIN, PEAK + minutes_after * MIN
    sc = dict({"incarnations": incarnations, "until_ms": hi + 10_000, "ingress_initial": 100,
               "scrape": scrape(lo - MIN, hi + 10_000)}, **extra)
    return cfg, simulate(tmp_path, cfg, sc)


def reduced(cfg, out, start=PEAK - 2 * MIN, end=PEAK + 9 * MIN, records=None, ingress=None, **kw):
    recs = out["lines"] if records is None else records
    return DR.reduce(cfg, recs, out["ingress"] if ingress is None else ingress, start, end, **kw)


def rows(red, target=N):
    return {r["minute"] - BASE: r for r in red["targets"][target]["rows"]}


def near(row):
    """The ingress count's own stated estimation error at the minute's two boundaries."""
    return row["ingress"]["received_err"] + 1.0


def check_observed(row, out, target=N, steady=False):
    """An observed row equals the simulation's ground truth: dispatch-time and ingress-receipt-time counts. steady:
    no start, stop or fault near the row, so the rate only changes at the minute boundaries and the ingress estimate
    is exact to a few requests."""
    truth = out["truth"][target][str(row["minute"])]
    assert row["state"] == "observed" and row["reasons"] == [], row
    assert row["generator"]["dispatched"] == truth["dispatched"]
    assert abs(row["ingress"]["received"] - truth["received"]) <= near(row), (row["ingress"], truth)
    assert row["ingress"]["received_bounds"][0] <= truth["received"] <= row["ingress"]["received_bounds"][1]
    assert row["reconciled"] is True
    if steady:
        assert row["ingress"]["received_err"] <= 4.0 and abs(row["ingress"]["received"] - truth["received"]) <= 3.0


@pytest.mark.parametrize("late_s", [0, 4, 30, 600])
def test_a_late_start_keeps_every_later_minute_observed(tmp_path, late_s):
    """R2-01: bins are tied to the ONE recorded origin, so a generator that starts 0, 4, 30 or 600 s after a minute
    boundary loses only what it did not cover; no later minute becomes unknown."""
    start = PEAK + late_s * 1000
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}], minutes_after=22)
    red = reduced(cfg, out, end=PEAK + 20 * MIN)
    assert red["origin_ms"] == ORIGIN and red["origin_utc"] == "2026-11-01T00:00:00Z"
    m_start = late_s // 60
    for target in (N, R):
        got = rows(red, target)
        for k, row in got.items():
            assert row["start_ms"] == ORIGIN + (BASE + k) * MIN            # binned from the origin, never the start
            assert row["intended"] == intended(cfg, BASE + k) and (row["day"], row["seed"]) == (10, 311)
            if k < m_start:
                assert row["state"] == "unknown" and row["reasons"] == ["before_first_record"]
            elif k == m_start and late_s % 60:
                assert row["state"] == "partial" and row["reasons"] == ["incarnation_boundary"]
                g = row["generator"]
                assert g["active_ms"] == MIN - (late_s % 60) * 1000
                assert g["dispatched"] == g["due"] == out["truth"][target][str(row["minute"])]["dispatched"]
                assert g["due"] < row["intended"] and row["reconciled"] is True
            else:
                check_observed(row, out, target, steady=k >= m_start + 2)
        states = red["targets"][target]["summary"]["states"]
        assert states["unknown"] == m_start + 2 and states["partial"] == (1 if late_s % 60 else 0)
        assert states["observed"] == 22 - states["unknown"] - states["partial"]


def test_a_restart_in_the_middle_of_the_peak(tmp_path):
    """A kill and a restart at the peak: the minutes the killed incarnation could not record are unknown, with their
    reason; the restart minute too (the killed one may still have been sending); everything else stays observed, and
    the ingress count stays known throughout. Nothing is back-filled."""
    kill, again = PEAK + 3 * MIN + 20_000, PEAK + 4 * MIN + 10_000
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN, "kill_ms": kill},
                              {"pod_uid": "pod-b", "start_ms": again}])
    red = reduced(cfg, out)
    got = rows(red)
    for k in (-2, -1, 0, 1, 2, 5, 6, 7, 8):
        check_observed(got[k], out)
    for k in (3, 4):
        assert got[k]["state"] == "unknown" and "unrecorded_tail" in got[k]["reasons"], got[k]
        assert got[k]["ingress_state"] == "ok"
        assert abs(got[k]["ingress"]["received"] - out["truth"][N][str(BASE + k)]["received"]) <= 0.25 * got[k]["intended"]
    assert got[4]["generator"]["incarnations"] == [f"pod-b/{again}"]          # its own record exists, as a lower bound
    assert got[4]["generator"]["dispatched"] == out["truth"][N][str(BASE + 4)]["dispatched"]
    s = red["targets"][N]["summary"]
    assert s["states"] == {"observed": 9, "partial": 0, "unknown": 2, "unscheduled": 0}
    assert s["reasons"] == {"unrecorded_tail": 2}
    assert s["observed_totals"]["dispatched"] == sum(out["truth"][N][str(BASE + k)]["dispatched"]
                                                     for k in (-2, -1, 0, 1, 2, 5, 6, 7, 8))
    incs = {i["incarnation"]: i for i in red["incarnations"] if i["target"] == N}
    assert incs[f"pod-a/{PEAK - 3 * MIN}"]["ended"] == "no end record"
    assert incs[f"pod-b/{again}"]["first_minute"] == BASE + 4


def test_a_kill_just_after_a_boundary_loses_the_unflushed_minute_too(tmp_path):
    kill, again = PEAK + 3 * MIN + 1_000, PEAK + 3 * MIN + 40_000            # before minute 2's line is written
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN, "kill_ms": kill},
                              {"pod_uid": "pod-b", "start_ms": again}])
    got = rows(reduced(cfg, out))
    assert [k for k, r in got.items() if r["state"] == "unknown"] == [2, 3]
    assert all("unrecorded_tail" in got[k]["reasons"] for k in (2, 3))
    for k in (1, 4, 5):
        check_observed(got[k], out)


def test_a_planned_segment_boundary_is_partial_and_exact_never_unknown(tmp_path):
    cfg = config(smoke_segment_minutes=3)
    start, again = PEAK - 3 * MIN, PEAK + 2_000                              # exit at the boundary, restart 2 s later
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}, {"pod_uid": "pod-a", "start_ms": again},
                              {"pod_uid": "pod-a", "start_ms": PEAK + 3 * MIN + 2_000}], cfg=cfg, minutes_after=8)
    red = reduced(cfg, out, end=PEAK + 5 * MIN)
    got = rows(red)
    assert got[0]["state"] == "partial" and got[0]["reasons"] == ["incarnation_boundary"]
    assert got[0]["generator"]["active_ms"] == MIN - 2_000
    assert got[0]["generator"]["dispatched"] == out["truth"][N][str(BASE)]["dispatched"]
    assert got[3]["state"] == "partial"
    for k in (-2, -1, 1, 2, 4):
        check_observed(got[k], out)
    assert red["targets"][N]["summary"]["states"]["unknown"] == 0


def test_a_proven_gap_between_an_end_record_and_the_next_start(tmp_path):
    cfg = config(smoke_segment_minutes=3)
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN},
                              {"pod_uid": "pod-b", "start_ms": PEAK + 3 * MIN + 5_000}], cfg=cfg, minutes_after=8)
    got = rows(reduced(cfg, out, end=PEAK + 5 * MIN))
    for k in (0, 1, 2):                                                       # ended cleanly, nothing ran: a known gap
        assert got[k]["state"] == "partial" and got[k]["reasons"] == ["generator_gap"]
        assert got[k]["generator"]["dispatched"] == 0 and got[k]["generator_state"] == "gap"
        assert got[k]["ingress"]["received"] <= 1.5
        assert got[k]["intended"] == intended(cfg, BASE + k) > 0             # intended traffic, not sent, not zeroed
    assert got[3]["state"] == "partial" and got[3]["reasons"] == ["incarnation_boundary"]
    check_observed(got[4], out)


def test_two_generators_at_once_are_detected_from_the_records(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN},
                              {"pod_uid": "pod-b", "start_ms": PEAK + 2 * MIN + 5_000}])
    got = rows(reduced(cfg, out))
    for k in (-2, -1, 0, 1):
        check_observed(got[k], out)
    for k in range(2, 9):
        assert got[k]["state"] == "unknown" and "duplicate_generators" in got[k]["reasons"]
        assert len(got[k]["generator"]["incarnations"]) == 2
    # With only ONE of the two logs, the records look clean, but the ingress received twice the dispatched count.
    one = [l for l in out["lines"] if l["inc"].startswith("pod-a/")]
    got = rows(reduced(cfg, out, records=one))
    for k in range(3, 9):
        assert got[k]["state"] == "unknown" and got[k]["reasons"] == ["unreconciled"] and got[k]["reconciled"] is False
        assert got[k]["ingress"]["received"] > 1.9 * got[k]["generator"]["dispatched"]


def test_a_scrape_gap_is_visible_and_never_a_zero(tmp_path):
    """R2-19: the ingress count comes from raw samples with their own timestamps; a gap leaves it unknown."""
    gap = {"from_ms": PEAK + 2 * MIN + 20_000, "to_ms": PEAK + 4 * MIN + 5_000}
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}],
                   scrape=dict(scrape(PEAK - 4 * MIN, PEAK + 12 * MIN), gaps=[gap]))
    got = rows(reduced(cfg, out))
    for k in (2, 3, 4):
        assert got[k]["state"] == "partial" and got[k]["reasons"] == ["ingress_gap"], got[k]
        assert got[k]["ingress_state"] == "gap" and "received" not in got[k]["ingress"]
        assert got[k]["generator_state"] == "complete" and got[k]["reconciled"] is None
        assert got[k]["generator"]["dispatched"] == out["truth"][N][str(BASE + k)]["dispatched"]
    for k in (0, 1, 5, 6):
        check_observed(got[k], out)
    # A stepped range query would have hidden it: with a wide tolerance the same samples "cover" the gap.
    assert rows(reduced(cfg, out, max_scrape_gap_s=300))[3]["state"] == "observed"


def test_an_ingress_restart_is_a_counter_reset_not_a_negative_or_zero_count(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}],
                   ingress_restarts=[PEAK + 3 * MIN + 30_000])
    got = rows(reduced(cfg, out))
    assert got[3]["state"] == "partial" and got[3]["reasons"] == ["ingress_reset"] and got[3]["ingress_state"] == "reset"
    for k in (1, 2, 4, 5, 6):
        check_observed(got[k], out)


def test_a_target_without_an_ingress_series_is_unknown_at_the_ingress_never_zero(tmp_path):
    """R2-10: the ingress exports a target's counter only after its first request."""
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    red = reduced(cfg, out, ingress={N: out["ingress"][N]})                   # no series at all for the other target
    for row in red["targets"][R]["rows"]:
        assert row["state"] == "partial" and row["reasons"] == ["ingress_absent"] and "received" not in row["ingress"]
    assert red["targets"][R]["summary"]["observed_totals"]["received"] == 0  # nothing observed, so nothing summed
    assert red["targets"][R]["summary"]["states"]["observed"] == 0
    check_observed(rows(red)[2], out)
    # The very first minutes of a series (no sample before the first request) are a gap, not zero.
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], ingress_initial=None)
    got = rows(reduced(cfg, out))
    assert got[0]["state"] == "partial" and "ingress_gap" in got[0]["reasons"]
    check_observed(got[2], out)


def test_a_clock_offset_makes_the_interval_unknown(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN, "skew_ms": 7_000}])
    red = reduced(cfg, out)
    for row in red["targets"][N]["rows"]:
        assert row["state"] == "unknown" and "clock_offset" in row["reasons"]
        lo, hi = row["clock_offset_ms"]
        assert lo <= 7_000 <= hi and hi - lo <= 1_100
    ok = reduced(*sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}]))
    lo, hi = rows(ok)[2]["clock_offset_ms"]
    assert lo <= 0 <= hi
    # A limit above the offset accepts it (the limit is the caller's, recorded in the output).
    wide = reduced(cfg, out, clock_limit_ms=10_000)
    assert wide["limits"]["clock_limit_ms"] == 10_000
    assert "clock_offset" not in rows(wide)[2]["reasons"]


def test_no_http_response_from_one_target_still_has_the_pods_clock_evidence(tmp_path):
    fault = [{"target": N, "from_ms": PEAK - MIN, "to_ms": PEAK + 20 * MIN, "mode": "unreachable"}]
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}], faults=fault)
    got = rows(reduced(cfg, out))
    for k in range(1, 8):
        row = got[k]
        assert row["state"] == "observed", row                               # the failure is data, not a missing record
        g = row["generator"]
        assert g["failed"] == g["dispatched"] == g["no_response"] > 0 and g["ok"] == 0
        assert row["ingress"]["received"] == pytest.approx(0.0, abs=1e-9)    # they never reached the ingress
        assert row["reconciled"] is True                                     # explained by the no-response failures
        assert row["clock_offset_ms"] is not None


def test_requests_in_flight_across_a_boundary_reconcile_without_matching_each_minute(tmp_path):
    """Dispatch -> ingress takes 0.9 s here: the requests of a minute's last 0.9 s are RECEIVED in the next minute. In
    the generator's first minute nothing arrives from a minute before, so received is lower than dispatched by about
    0.9 s of arrivals; that is in flight, not lost, and the running balance reconciles it."""
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK}], network_ms=900, latency_ms=950)
    got = rows(reduced(cfg, out))
    first = got[0]
    assert first["state"] == "observed" and first["generator_state"] == "complete"
    in_flight = first["generator"]["dispatched"] - first["ingress"]["received"]
    assert 10 < in_flight < 0.9 * first["intended"] / 60 + 4               # this single minute does NOT match...
    assert out["truth"][N][str(BASE)]["received"] < out["truth"][N][str(BASE)]["dispatched"]
    for k in range(0, 8):
        row = got[k]
        assert row["state"] == "observed" and row["reconciled"] is True     # ...and no minute is called a loss
        assert abs(row["balance"]) <= row["allowance"]
    assert got[7]["balance"] == pytest.approx(in_flight, abs=6)             # the balance carries what is in flight


def test_timeouts_that_reached_the_ingress_reconcile_as_received(tmp_path):
    fault = [{"target": N, "from_ms": PEAK + MIN, "to_ms": PEAK + 3 * MIN, "mode": "timeout"}]
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}], faults=fault)
    got = rows(reduced(cfg, out))
    for k in (1, 2):
        g = got[k]["generator"]
        assert got[k]["state"] == "observed" and g["failed"] == g["dispatched"] and g["latency"] is None
        assert abs(got[k]["ingress"]["received"] - g["dispatched"]) <= near(got[k])


def strip(out, pred):
    return [l for l in out["lines"] if not pred(l)]


def test_a_lost_line_is_unknown_for_its_minute_only(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    recs = strip(out, lambda l: l.get("minute") == BASE + 2 and l["target"] == N)
    red = reduced(cfg, out, records=recs)
    got = rows(red)
    assert got[2]["state"] == "unknown" and got[2]["reasons"] == ["generator_record_missing"]
    assert got[2]["ingress_state"] == "ok"
    for k in (1, 3, 4):
        check_observed(got[k], out)
    inc = [i for i in red["incarnations"] if i["target"] == N][0]
    assert inc["lost_lines"] == 1
    assert rows(red, R)[2]["state"] == "observed"


def test_the_same_log_read_twice_changes_nothing(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    once = reduced(cfg, out)
    twice = reduced(cfg, out, records=out["lines"] + out["lines"][5:25])
    assert twice["records"]["duplicate_lines"] == 20 and once["records"]["duplicate_lines"] == 0
    assert twice["targets"] == once["targets"]


@pytest.mark.parametrize("edit, reason", [
    (lambda l: l.update(dispatched=l["dispatched"] + 5), "inconsistent_line"),
    (lambda l: l.update(ok=l["ok"] - 3), "inconsistent_line"),
    (lambda l: l.update(intended=l["intended"] + 1), "plan_mismatch"),
    (lambda l: l.update(day_minute=0), "plan_mismatch"),
    (lambda l: l.update(day=9), "plan_mismatch"),                            # the right minute of day, the wrong day
    (lambda l: l.update(active=[0, 70_000]), "inconsistent_line"),
    (lambda l: l.update(unresolved=2, ok=l["ok"] - 2, lat={"counts": [0, 0, l["ok"] - 2] + [0] * 10, "max_ms": 20}),
     "unresolved_outcomes"),
])
def test_a_record_that_is_not_self_consistent_or_not_the_plan_is_unknown(tmp_path, edit, reason):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    recs = copy.deepcopy(out["lines"])
    for l in recs:
        if l.get("minute") == BASE + 2 and l["target"] == N:
            edit(l)
    got = rows(reduced(cfg, out, records=recs))
    assert got[2]["state"] == "unknown" and reason in got[2]["reasons"], got[2]["reasons"]
    assert got[2]["intended"] == intended(cfg, BASE + 2)                     # the row keeps the table's count
    check_observed(got[1], out)


def test_a_line_without_its_day_is_malformed_and_taints_its_incarnation(tmp_path):
    """A v1-shaped minute line (no `day`) cannot be checked against the table: it is not accepted, and the incarnation
    that wrote it is not trusted for any minute."""
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    recs = copy.deepcopy(out["lines"])
    for l in recs:
        if l.get("minute") == BASE + 2 and l["target"] == N:
            del l["day"]
    red = reduced(cfg, out, records=recs)
    got = rows(red)
    assert got[2]["state"] == "unknown" and got[2]["reasons"] == ["generator_record_missing"]
    assert got[1]["state"] == "unknown" and "malformed_line" in got[1]["reasons"]
    assert [i["flags"] for i in red["incarnations"] if i["target"] == N] == [["malformed_line"]]
    assert rows(red, R)[2]["state"] == "observed"


def test_conflicting_copies_of_a_line_and_a_counter_reset_are_flagged(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    recs = copy.deepcopy(out["lines"])
    forged = copy.deepcopy([l for l in recs if l.get("minute") == BASE + 2 and l["target"] == N][0])
    forged["cum"]["due"] += 1
    red = reduced(cfg, out, records=recs + [forged])
    assert all("conflicting_lines" in r["reasons"] and r["state"] == "unknown" for r in red["targets"][N]["rows"]
               if r["generator"]["incarnations"])
    recs = copy.deepcopy(out["lines"])
    for l in recs:
        if l.get("minute", 0) >= BASE + 4 and l["target"] == N:
            l["cum"] = {k: 0 for k in l["cum"]}
    red = reduced(cfg, out, records=recs)
    assert "generator_counter_reset" in rows(red)[5]["reasons"] and rows(red)[5]["state"] == "unknown"


def test_records_of_another_configuration_or_run_are_not_accepted(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    other = dict(cfg, digest="0" * 64)
    red = reduced(other, out)
    assert all(r["state"] == "unknown" and "config_mismatch" in r["reasons"] for r in red["targets"][N]["rows"])
    foreign = dict(cfg, run_id="c28-other")
    red = reduced(foreign, out)
    assert red["records"]["foreign_run"] == len(out["lines"])
    assert all(r["state"] == "unknown" for r in red["targets"][N]["rows"])


def test_the_running_incarnations_latest_minutes_are_unknown_until_their_lines_exist(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}], minutes_after=6)
    got = rows(reduced(cfg, out, end=PEAK + 9 * MIN))
    assert got[4]["state"] == "observed"
    assert got[7]["state"] == "unknown" and "unrecorded_tail" in got[7]["reasons"]


def test_latency_percentiles_come_from_merged_buckets(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}], latency_ms=180)
    lat = rows(reduced(cfg, out))[2]["generator"]["latency"]
    assert 100 < lat["p95_ms"] <= 200 and 100 < lat["p99_ms"] <= 200 and lat["max_ms"] == 180
    assert lat["ok"] == rows(reduced(cfg, out))[2]["generator"]["ok"]


def test_the_range_must_be_on_the_origins_minute_grid(tmp_path):
    cfg = config()
    for start, end in ((PEAK + 1, PEAK + MIN), (PEAK, PEAK + MIN + 1000), (PEAK, PEAK)):
        with pytest.raises(ValueError, match="minute boundaries of the recorded origin"):
            DR.reduce(cfg, [], {}, start, end)


def test_summary_counts_only_observed_minutes(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK + 30_000}])
    s = reduced(cfg, out)["targets"][N]["summary"]
    assert sum(s["states"].values()) == s["minutes"] == 11
    assert s["states"] == {"observed": 8, "partial": 1, "unknown": 2, "unscheduled": 0}
    assert s["observed_totals"]["intended"] == sum(intended(cfg, BASE + k) for k in range(1, 9))
    assert "never zero-filled" in s["note"]
    # What was not observed keeps its intended arrivals: the table's, never zero.
    assert s["scheduled_intended"] == sum(intended(cfg, BASE + k) for k in range(-2, 9))
    assert s["unobserved_intended"] == sum(intended(cfg, BASE + k) for k in (-2, -1, 0)) > 0


# --- reading the pod log --------------------------------------------------------------------------------------------

def test_parse_lines_reads_the_records_in_every_k6_log_format():
    rec = {"schema": "c28-demo-minute/v2", "run": "r", "inc": "p/1", "n": 1, "target": N, "minute": 3, "day": 0,
           "failed_by": {"503": 2}}
    raw = json.dumps(rec)
    lines = [
        raw,                                                                              # --log-format raw
        "2026-11-12T00:01:02.500000000Z " + raw,                                          # kubectl logs --timestamps
        json.dumps({"level": "info", "msg": raw, "source": "console", "time": "2026-11-12T00:01:02Z"}),   # json
        'time="2026-11-12T00:01:02Z" level=info msg=' + json.dumps(raw) + " source=console",              # text
        'time="2026-11-12T00:01:07Z" level=error msg="Failed to send the time series data to the endpoint"',
        "", "not json at all", '{"schema": "c28-demo-minute/v1", "minute": 3}', "{broken",   # the old schema is not read
        json.dumps({"level": "info", "msg": "plain message"}),
        "x" * 20_000,
    ]
    recs, stats = DR.parse_lines(lines)
    assert recs == [rec, rec, rec, rec]
    assert stats == {"lines": 11, "records": 4, "other": 6, "oversize": 1}


def test_parse_then_reduce_end_to_end_from_log_text(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    text = "\n".join(["some k6 banner"] + [json.dumps(l) for l in out["lines"]]).splitlines()
    recs, stats = DR.parse_lines(text)
    assert stats["records"] == len(out["lines"]) and stats["other"] == 1
    assert DR.reduce(cfg, recs, out["ingress"], PEAK - 2 * MIN, PEAK + 9 * MIN)["targets"] == reduced(cfg, out)["targets"]


def test_cli_demo_reduce(tmp_path, capsys):
    from c28.__main__ import main
    from c28 import render
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    (tmp_path / "cfg.json").write_text(json.dumps(cfg))
    (tmp_path / "pod.log").write_text("k6 banner\n" + "\n".join(json.dumps(l) for l in out["lines"][:20]) + "\n")
    (tmp_path / "pod2.log").write_text("\n".join(json.dumps(l) for l in out["lines"][15:]) + "\n")
    (tmp_path / "ingress.json").write_text(json.dumps(out["ingress"]))
    argv = ["demo-reduce", "--config", str(tmp_path / "cfg.json"), "--log", str(tmp_path / "pod.log"), "--log",
            str(tmp_path / "pod2.log"), "--ingress", str(tmp_path / "ingress.json"),
            "--start", "2026-11-11T17:58:00Z", "--end", "2026-11-11T18:09:00Z"]
    assert main(argv) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["targets"] == reduced(cfg, out)["targets"]
    assert doc["log"] == {"lines": 38, "records": 37, "other": 1, "oversize": 0}
    assert doc["records"]["duplicate_lines"] == 5 and doc["schema"] == "c28-demo-reduction/v2"
    assert doc["schedule"] == {"days": 28, "seeds": list(range(301, 329)), "minutes": 28 * 1440,
                               "end_ms": ORIGIN + 28 * DAY}
    table = copy.deepcopy(cfg["per_minute_days"])
    table[17][600] += 1                                                    # ONE count of ONE day edited, old digest
    for bad, why in ((dict(cfg, per_minute_days=table), "the digest differs"),
                     (dict(cfg, seeds=[301] + list(range(303, 330))), "seeds must be one seed per day"),
                     (dict(cfg, origin_ms=cfg["origin_ms"] + DAY), "schedule_minutes and schedule_end_ms"),
                     (edited(cfg, per_minute_days=table[:27]), "one row of 1440 arrival counts per day"),
                     (edited(cfg, schema="c28-demo-generator/v1"), "not a c28-demo-generator/v2 configuration")):
        (tmp_path / "cfg.json").write_text(json.dumps(bad))
        with pytest.raises(render.ParamsError, match="not an intact c28-demo-generator/v2") as e:
            main(argv)
        assert why in str(e.value)


# --- the finite schedule: every record against ITS day's row, the schedule's end, overlapping incarnations ---------

D11 = ORIGIN + 11 * DAY                    # the UTC midnight between day 10 and day 11
M11 = 11 * 1440


def around_midnight(tmp_path, incarnations, cfg, before=6, after=8, **extra):
    lo, hi = D11 - before * MIN, D11 + after * MIN
    sc = dict({"incarnations": incarnations, "until_ms": hi + 10_000, "ingress_initial": 100,
               "scrape": scrape(lo - MIN, hi + 10_000)}, **extra)
    return simulate(tmp_path, cfg, sc)


def by_minute(red, target=N):
    return {r["minute"] - M11: r for r in red["targets"][target]["rows"]}


def test_every_record_is_checked_against_its_own_days_row_across_a_midnight(tmp_path):
    """48-hour segments, day 11 at half its counts: one incarnation crosses the midnight. Each row's intended is the
    table entry of that ABSOLUTE minute (day and minute of day), and the rows stay observed across the day change."""
    cfg = scaled_day(config(changed(params(), "demonstration.generator.segment_hours", 48)), 11, 0.5)
    out = around_midnight(tmp_path, [{"pod_uid": "pod-a", "start_ms": D11 - 7 * MIN}], cfg)
    red = DR.reduce(cfg, out["lines"], out["ingress"], D11 - 5 * MIN, D11 + 6 * MIN)
    got = by_minute(red)
    for k, row in got.items():
        day, dm = (10, 1440 + k) if k < 0 else (11, k)
        assert (row["day"], row["day_minute"], row["seed"]) == (day, dm, 301 + day)
        assert row["intended"] == cfg["per_minute_days"][day][dm]
        assert row["generator"]["dispatched"] == row["intended"] == out["truth"][N][str(row["minute"])]["dispatched"]
        assert row["generator_state"] == "complete" and "plan_mismatch" not in row["reasons"], row
    assert got[0]["intended"] < 0.6 * got[-1]["intended"]                    # the switch at the boundary
    assert all(got[k]["state"] == "observed" for k in list(range(-5, -1)) + list(range(2, 6)))
    # The SAME records against a table that repeats day 10 (what a one-day program would have assumed): every line of
    # day 11 is a plan mismatch, so an "intended" from the wrong day can never pass.
    wrong = copy.deepcopy(cfg["per_minute_days"])
    wrong[11] = list(wrong[10])
    other = dict(cfg, per_minute_days=wrong)                                 # same digest: only the table check is left
    bad = by_minute(DR.reduce(other, out["lines"], out["ingress"], D11 - 5 * MIN, D11 + 6 * MIN))
    assert all("plan_mismatch" not in bad[k]["reasons"] for k in range(-5, 0))
    assert all(bad[k]["state"] == "unknown" and "plan_mismatch" in bad[k]["reasons"] for k in range(0, 6))


def test_a_line_that_carries_another_days_count_for_the_same_minute_of_day_is_a_plan_mismatch(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    recs = copy.deepcopy(out["lines"])
    yesterday = cfg["per_minute_days"][9][1080 + 2]
    assert yesterday != cfg["per_minute_days"][10][1080 + 2]                 # the days really differ at this minute
    for l in recs:
        if l.get("minute") == BASE + 2 and l["target"] == N:
            l["intended"] = yesterday                                        # a valid count of the table, the wrong day
    got = rows(reduced(cfg, out, records=recs))
    assert got[2]["state"] == "unknown" and "plan_mismatch" in got[2]["reasons"]
    assert got[2]["intended"] == cfg["per_minute_days"][10][1080 + 2]        # the row keeps the table's own count


def test_a_restart_across_midnight_is_binned_on_the_new_day_and_the_gap_is_missing_never_zero(tmp_path):
    """24-hour segments: the old incarnation ends at the midnight, the new one starts 70 s later. The gap's whole
    minute keeps day 11's intended count with nothing delivered (partial: generator_gap); nothing is zero-filled."""
    cfg = scaled_day(config(), 11, 0.5)
    again = D11 + 70_000
    out = around_midnight(tmp_path, [{"pod_uid": "pod-a", "start_ms": D11 - 7 * MIN},
                                     {"pod_uid": "pod-a", "start_ms": again}], cfg)
    red = DR.reduce(cfg, out["lines"], out["ingress"], D11 - 5 * MIN, D11 + 6 * MIN)
    got = by_minute(red)
    for k in range(-5, 0):
        assert got[k]["state"] == "observed" and got[k]["day"] == 10
    gap = got[0]
    assert (gap["state"], gap["reasons"], gap["generator_state"]) == ("partial", ["generator_gap"], "gap")
    assert gap["day"] == 11 and gap["intended"] == cfg["per_minute_days"][11][0] > 0     # intended, not delivered
    assert gap["generator"]["dispatched"] == 0
    first = got[1]
    assert first["state"] == "partial" and first["reasons"] == ["incarnation_boundary"]
    assert first["generator"]["active_ms"] == 50_000 and first["intended"] == cfg["per_minute_days"][11][1]
    assert first["generator"]["dispatched"] == out["truth"][N][str(M11 + 1)]["dispatched"] < first["intended"]
    for k in range(2, 6):
        assert got[k]["day"] == 11 and got[k]["generator_state"] == "complete"
        assert got[k]["generator"]["dispatched"] == cfg["per_minute_days"][11][k]
    assert all(got[k]["state"] == "observed" for k in range(3, 6))
    s = red["targets"][N]["summary"]
    assert s["unobserved_intended"] >= gap["intended"] + first["intended"]   # what was not observed is still intended
    assert s["scheduled_intended"] == sum(r["intended"] for r in got.values())
    incs = [i for i in red["incarnations"] if i["target"] == N]
    assert [(i["phase"], i["send_to_ms"]) for i in incs] == [("scheduled", D11), ("scheduled", D11 + DAY)]
    # A lost log line: the EVIDENCE is missing (unknown), the intended count is still the table's.
    recs = [l for l in out["lines"] if not (l.get("minute") == M11 + 4 and l["target"] == N)]
    lost = by_minute(DR.reduce(cfg, recs, out["ingress"], D11 - 5 * MIN, D11 + 6 * MIN))[4]
    assert lost["state"] == "unknown" and lost["reasons"] == ["generator_record_missing"]
    assert lost["intended"] == cfg["per_minute_days"][11][4] > 0 and lost["ingress_state"] == "ok"
    # A KILLED incarnation across the midnight: its unrecorded tail and the restart minute are unknown, never zero.
    out = around_midnight(tmp_path, [{"pod_uid": "pod-a", "start_ms": D11 - 7 * MIN, "kill_ms": D11 - 20_000},
                                     {"pod_uid": "pod-b", "start_ms": D11 + 15_000}], cfg)
    got = by_minute(DR.reduce(cfg, out["lines"], out["ingress"], D11 - 5 * MIN, D11 + 6 * MIN))
    assert got[-2]["state"] == "observed"                                    # its line was written before the kill
    assert got[-1]["state"] == "unknown" and got[-1]["reasons"] == ["unrecorded_tail"]
    assert (got[-1]["day"], got[-1]["intended"]) == (10, cfg["per_minute_days"][10][1439])
    # The killed incarnation's declared span ended AT the midnight (its start record), so day 11's first minute is
    # only the new incarnation's: partial from its start, with day 11's count.
    assert got[0]["state"] == "partial" and got[0]["reasons"] == ["incarnation_boundary"]
    assert (got[0]["day"], got[0]["intended"]) == (11, cfg["per_minute_days"][11][0])
    assert got[0]["generator"]["incarnations"] == [f"pod-b/{D11 + 15_000}"]
    assert all(got[k]["state"] == "observed" for k in range(3, 6))


def test_overlapping_active_spans_of_two_incarnations_are_flagged_and_listed_never_summed_silently(tmp_path):
    """Two generators for the same target across the midnight: the second starts 20 s into day 10's last minute while
    the first keeps running (48-hour segments). Every minute in which both were active is unknown, reason
    duplicate_generators, and listed; minutes with one incarnation only stay observed."""
    cfg = scaled_day(config(changed(params(), "demonstration.generator.segment_hours", 48)), 11, 0.5)
    second = D11 - MIN + 20_000
    out = around_midnight(tmp_path, [{"pod_uid": "pod-a", "start_ms": D11 - 7 * MIN},
                                     {"pod_uid": "pod-b", "start_ms": second}], cfg)
    red = DR.reduce(cfg, out["lines"], out["ingress"], D11 - 5 * MIN, D11 + 6 * MIN)
    got = by_minute(red)
    for k in range(-5, -1):
        assert got[k]["state"] == "observed"
    for k in range(-1, 6):
        row = got[k]
        assert row["state"] == "unknown" and "duplicate_generators" in row["reasons"], row
        assert sorted(row["generator"]["incarnations"]) == [f"pod-a/{D11 - 7 * MIN}", f"pod-b/{second}"]
        assert row["intended"] == intended(cfg, M11 + k)                     # the table's count, not twice it
    dup = red["duplicate_generators"]
    assert [d["minute"] - M11 for d in dup[N]] == list(range(-1, 6)) == [d["minute"] - M11 for d in dup[R]]
    assert all(len(d["incarnations"]) == 2 for d in dup[N])
    s = red["targets"][N]["summary"]
    assert s["reasons"]["duplicate_generators"] == 7 and s["states"]["observed"] == 4
    assert s["observed_totals"]["dispatched"] == sum(got[k]["generator"]["dispatched"] for k in range(-5, -1))
    # Spans that only TOUCH are not an overlap: a planned restart (one ends at the midnight, the next starts after it).
    clean = scaled_day(config(), 11, 0.5)
    out = around_midnight(tmp_path, [{"pod_uid": "pod-a", "start_ms": D11 - 7 * MIN},
                                     {"pod_uid": "pod-a", "start_ms": D11 + 2_000}], clean)
    red = DR.reduce(clean, out["lines"], out["ingress"], D11 - 5 * MIN, D11 + 6 * MIN)
    assert red["duplicate_generators"] == {N: [], R: []}
    assert "duplicate_generators" not in red["targets"][N]["summary"]["reasons"]
    # A second incarnation that re-covers a minute an earlier one already covered completely (a clock stepped back
    # and a restart): flagged the same way, even though each record is consistent by itself.
    recs = copy.deepcopy(out["lines"])
    again = copy.deepcopy([l for l in recs if l.get("minute") == M11 - 2 and l["target"] == N][0])
    again.update(inc="pod-z/1", n=1, active=[30_000, MIN])
    red = DR.reduce(clean, recs + [again], out["ingress"], D11 - 5 * MIN, D11 + 6 * MIN)
    assert [d["minute"] - M11 for d in red["duplicate_generators"][N]] == [-2]
    assert by_minute(red)[-2]["state"] == "unknown"


def two_days():
    cfg = config(changed(params(), "demonstration.program.days", 2), smoke_segment_minutes=7)
    assert cfg["schedule_end_ms"] == ORIGIN + 2 * DAY
    return cfg


def test_the_reducer_knows_the_schedules_end(tmp_path):
    """A two-day smoke schedule ending three minutes into a segment: rows up to the last scheduled minute are binned
    and judged; rows after it are `unscheduled` with intended null (never zero, never day 0's count again)."""
    cfg = two_days()
    end = ORIGIN + 2 * DAY
    start = ORIGIN + 2870 * MIN + 2_000                                    # the segment [2870, 2877)
    lo, hi = end - 6 * MIN, end + 9 * MIN
    sc = {"incarnations": [{"pod_uid": "pod-a", "start_ms": start},
                           {"pod_uid": "pod-a", "start_ms": ORIGIN + 2877 * MIN + 2_000},
                           {"pod_uid": "pod-a", "start_ms": ORIGIN + 2884 * MIN + 2_000}],
          "until_ms": hi + 3 * MIN, "ingress_initial": 100, "scrape": scrape(lo - MIN, hi + 3 * MIN)}
    out = simulate(tmp_path, cfg, sc)
    red = DR.reduce(cfg, out["lines"], out["ingress"], lo, hi)
    got = {r["minute"] - 2880: r for r in red["targets"][N]["rows"]}
    for k in (-6, -5, -4, -2, -1):
        assert got[k]["state"] == "observed", got[k]
        assert (got[k]["day"], got[k]["day_minute"], got[k]["intended"]) == (1, 1440 + k, cfg["per_minute_days"][1][1440 + k])
    assert got[-3]["state"] == "partial" and got[-3]["reasons"] == ["incarnation_boundary"]   # the planned restart
    for k in range(0, 9):
        row = got[k]
        assert row["state"] == "unscheduled" and row["reasons"] == ["schedule_exhausted"], row
        assert row["intended"] is None and row["day"] is None and row["day_minute"] is None and row["seed"] is None
        assert row["generator"]["dispatched"] == 0 and row["generator"]["incarnations"] == []
        assert row["reconciled"] is None
    s = red["targets"][N]["summary"]
    assert s["states"] == {"observed": 5, "partial": 1, "unknown": 0, "unscheduled": 9}
    assert s["scheduled_intended"] == sum(cfg["per_minute_days"][1][1434:1440])     # nothing intended after the end
    assert red["schedule"] == {"days": 2, "seeds": [301, 302], "minutes": 2880, "end_ms": end}
    incs = [i for i in red["incarnations"] if i["target"] == N]
    assert [i["phase"] for i in incs] == ["scheduled", "scheduled", "schedule-exhausted"]
    assert [i["schedule_exhausted"] for i in incs] == [False, True, True]
    assert incs[1]["last_minute"] == 2879 and incs[2]["minute_records"] == 0 and incs[2]["send_from_ms"] is None
    # A forged line for a minute after the end (as a wrapping generator would write: day 0's row again) is refused.
    forged = copy.deepcopy([l for l in out["lines"] if l.get("minute") == 2879 and l["target"] == N][0])
    forged.update(minute=2880, day=0, day_minute=0, intended=cfg["per_minute_days"][0][0], n=99)
    bad = DR.reduce(cfg, out["lines"] + [forged], out["ingress"], lo, hi)
    row = {r["minute"] - 2880: r for r in bad["targets"][N]["rows"]}[0]
    assert row["state"] == "unknown" and "plan_mismatch" in row["reasons"] and row["intended"] is None


def test_minutes_before_the_origin_are_unscheduled(tmp_path):
    cfg = config()
    lo, hi = ORIGIN - 4 * MIN, ORIGIN + 5 * MIN
    sc = {"incarnations": [{"pod_uid": "pod-a", "start_ms": ORIGIN - 6 * MIN}], "until_ms": hi + 2 * MIN,
          "ingress_initial": 100, "scrape": scrape(lo - MIN, hi + 2 * MIN)}
    out = simulate(tmp_path, cfg, sc)
    red = DR.reduce(cfg, out["lines"], out["ingress"], lo, hi)
    got = {r["minute"]: r for r in red["targets"][N]["rows"]}
    for m in range(-4, 0):
        assert got[m]["state"] == "unscheduled" and got[m]["reasons"] == ["before_origin"] and got[m]["intended"] is None
    for m in range(0, 5):
        assert got[m]["generator_state"] == "complete", got[m]               # minute 0 is COMPLETE: it waited for it
        assert (got[m]["day"], got[m]["intended"]) == (0, cfg["per_minute_days"][0][m])
        assert got[m]["generator"]["dispatched"] == got[m]["intended"]
    assert all(got[m]["state"] == "observed" for m in range(2, 5))
    inc = [i for i in red["incarnations"] if i["target"] == N][0]
    assert (inc["phase"], inc["send_from_ms"], inc["first_minute"]) == ("pre-origin", ORIGIN, 0)


@pytest.mark.parametrize("edit, why", [
    (lambda c: c.update(per_minute_days=c["per_minute_days"][:27]), "one row of 1440"),
    (lambda c: c["per_minute_days"][3].pop(), "one row of 1440"),
    (lambda c: c.update(seeds=c["seeds"][:5]), "one seed per day"),
    (lambda c: c.update(schedule_minutes=1440), "days x 1440"),
    (lambda c: c.update(schedule_end_ms=c["schedule_end_ms"] + DAY), "days x 1440"),
    (lambda c: c.update(schema="c28-demo-generator/v1"), "schema"),
    (lambda c: c.update(days=29), "days 1..28"),
])
def test_a_configuration_whose_schedule_cannot_be_read_is_refused(edit, why):
    cfg = copy.deepcopy(config())
    edit(cfg)
    with pytest.raises(ValueError, match=why):
        DR.reduce(cfg, [], {}, PEAK, PEAK + MIN)
