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
    rec = {"schema": "c28-demo-minute/v3", "run": "r", "inc": "p/1", "n": 1, "target": N, "minute": 3, "day": 0,
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
    assert doc["log"] == {"lines": 40, "records": 39, "other": 1, "oversize": 0}   # incl. one first_send per target
    assert doc["records"]["duplicate_lines"] == 5 and doc["schema"] == "c28-demo-reduction/v3"
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


# --- provenance: no minute is observed without its incarnation's valid start record (R3 item 3a) -------------------

def test_without_any_incarnation_record_no_minute_is_observed(tmp_path):
    """The reviewer's case: every incarnation record removed, the minute lines left. Nothing binds those lines to
    this configuration (run, digest, origin, schema, segment end), so none of their minutes is observed."""
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    full = reduced(cfg, out)
    assert full["targets"][N]["summary"]["states"]["observed"] == 11
    only_lines = [l for l in out["lines"] if l["schema"] == DR.MINUTE]
    assert len(only_lines) < len(out["lines"])
    red = reduced(cfg, out, records=only_lines)
    for t in (N, R):
        s = red["targets"][t]["summary"]
        assert s["states"]["observed"] == 0 and s["states"]["unknown"] == 11, s
        assert s["reasons"]["start_record_missing"] == 11
        assert s["observed_totals"]["dispatched"] == 0 and s["unobserved_intended"] == s["scheduled_intended"] > 0
    assert {i["start_record"] for i in red["incarnations"]} == {"no start record"}
    # Only the START record matters for the binding: without the end record the minutes stay observed.
    no_end = [l for l in out["lines"] if l.get("event") != "end"]
    assert reduced(cfg, out, records=no_end)["targets"][N]["summary"]["states"]["observed"] == 11
    # One target's start record lost: that target's minutes only.
    lost = [l for l in out["lines"] if not (l.get("event") == "start" and l["target"] == N)]
    red = reduced(cfg, out, records=lost)
    assert red["targets"][N]["summary"]["states"]["observed"] == 0
    assert red["targets"][R]["summary"]["states"]["observed"] == 11


@pytest.mark.parametrize("edit, why", [
    (lambda st: st.update(config_digest="0" * 64), "config_digest is not the configuration's"),
    (lambda st: st.update(config_schema="c28-demo-generator/v1"), "config_schema is not the configuration's"),
    (lambda st: st.pop("config_schema"), "config_schema is not the configuration's"),
    (lambda st: st.update(origin_ms=st["origin_ms"] + DAY), "origin_ms is not the configuration's"),
    (lambda st: st.update(schedule_end_ms=st["schedule_end_ms"] + DAY), "schedule_end_ms is not the configuration's"),
    (lambda st: st.update(days=27), "days is not the configuration's"),
    (lambda st: st.update(segment_end_ms=st["segment_end_ms"] + DAY, send_to_ms=st["send_to_ms"] + DAY),
     "segment_end_ms is not the segment end of its start time"),
    (lambda st: st.update(segment_end_ms=None), "must be integers"),
    (lambda st: st.update(started_ms=st["started_ms"] - 5 * MIN), "the incarnation id is not"),
    (lambda st: st.update(send_from_ms=st["send_from_ms"] - 90_000), "the declared span or phase does not follow"),
    (lambda st: st.update(send_to_ms=st["send_to_ms"] + MIN), "the declared span or phase does not follow"),
    (lambda st: st.update(phase="pre-origin"), "the declared span or phase does not follow"),
    (lambda st: st.update(schema="c28-demo-incarnation/v1"), None),          # not a record at all: missing
    (lambda st: st.update(run="c28-other-run"), None),                       # another run's record: missing
])
def test_a_start_record_that_is_not_this_configurations_does_not_bind_its_lines(tmp_path, edit, why):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN + 4_000}])
    recs = copy.deepcopy(out["lines"])
    for l in recs:
        if l.get("event") == "start" and l["target"] == N:
            edit(l)
    recs = [l for l in recs if l.get("schema") in (DR.MINUTE, DR.INCARNATION)]
    red = reduced(cfg, out, records=recs)
    s = red["targets"][N]["summary"]
    assert s["states"]["observed"] == 0, s
    flag = "start_record_invalid" if why else "start_record_missing"
    assert s["reasons"][flag] == 11
    inc = [i for i in red["incarnations"] if i["target"] == N][0]
    assert (why or "no start record") in inc["start_record"], inc["start_record"]
    assert red["targets"][R]["summary"]["states"]["observed"] >= 9          # the other target's binding is intact
    assert [i["start_record"] for i in red["incarnations"] if i["target"] == R] == ["valid"]


def test_a_line_must_lie_in_the_span_its_start_record_declared(tmp_path):
    start = PEAK - 3 * MIN + 4_000
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}])
    assert rows(reduced(cfg, out))[-2]["state"] == "observed"
    # The start minute's line claims the WHOLE minute, though the incarnation started 4 s into it.
    recs = copy.deepcopy(out["lines"])
    first = [l for l in recs if l["schema"] == DR.MINUTE and l["minute"] == BASE - 3 and l["target"] == N][0]
    assert first["active"] == [4_000, MIN]
    first["active"] = [0, MIN]
    red = DR.reduce(cfg, recs, out["ingress"], PEAK - 3 * MIN, PEAK + 9 * MIN)
    row = {r["minute"] - BASE: r for r in red["targets"][N]["rows"]}[-3]
    assert row["state"] == "unknown" and "inconsistent_line" in row["reasons"]
    # A line for a minute BEFORE the incarnation started (copied from another minute) is outside its span.
    recs = copy.deepcopy(out["lines"])
    ghost = copy.deepcopy([l for l in recs if l["schema"] == DR.MINUTE and l["minute"] == BASE and l["target"] == N][0])
    m = BASE - 5
    ghost.update(minute=m, day_minute=m % 1440, intended=cfg["per_minute_days"][10][m % 1440], n=500)
    ghost["due"] = ghost["dispatched"] = ghost["ok"] = ghost["intended"]
    ghost["lat"]["counts"] = [0, 0, ghost["ok"]] + [0] * 10
    red = DR.reduce(cfg, recs + [ghost], out["ingress"], PEAK - 6 * MIN, PEAK + 9 * MIN)
    row = {r["minute"] - BASE: r for r in red["targets"][N]["rows"]}[-5]
    assert row["state"] == "unknown" and "inconsistent_line" in row["reasons"]


# --- accounting: every arrival the schedule places in the active span is owed (R3 item 4) --------------------------

def test_scheduled_arrivals_is_the_scripts_own_count_for_any_span(tmp_path):
    cfg = config()
    m = BASE + 2
    from demo_helpers import simulate as run_probe
    out = run_probe(tmp_path, cfg, {"incarnations": [], "until_ms": PEAK,
                                    "probes": [{"fn": "arrivals", "minute": m, "target_index": i} for i in (0, 1)]})
    lo = ORIGIN + m * MIN
    n = cfg["per_minute_days"][10][1082]
    for target, f in zip((N, R), out["functions"]):
        offsets = [t - lo for t in f["times"]]
        assert len(offsets) == n == DR.scheduled_arrivals(cfg, target, m, 0, MIN)
        for a0, a1 in ((0, 1), (0, 30_000), (4_000, MIN), (59_999, MIN), (12_345, 54_321), (30_000, 30_000),
                       (offsets[7], offsets[8]), (offsets[7] + 1, offsets[9]), (0, offsets[0]), (0, offsets[0] + 1)):
            assert DR.scheduled_arrivals(cfg, target, m, a0, a1) == sum(1 for o in offsets if a0 <= o < a1), (a0, a1)
    assert DR.scheduled_arrivals(cfg, N, -1, 0, MIN) is None and DR.scheduled_arrivals(cfg, N, 28 * 1440, 0, MIN) is None
    assert DR.scheduled_arrivals(cfg, "c28-other", m, 0, MIN) is None


def test_a_full_minute_that_sent_nothing_of_its_intended_arrivals_is_not_observed(tmp_path):
    """The reviewer's case: a complete minute with intended > 1,000 and due = dispatched = dropped = 0, the ingress
    flat. The record is self-consistent (0 = 0 - 0 + 0 + 0) and nothing contradicts it at the ingress; but the
    schedule places `intended` arrivals in that span and none is accounted for."""
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    recs = copy.deepcopy(out["lines"])
    for l in recs:
        if l["schema"] == DR.MINUTE and l["target"] == N:
            assert l["intended"] > 1_000 and l["active"] == [0, MIN]
            l.update(due=0, dispatched=0, ok=0, failed=0, unresolved=0, carried=[0, 0],
                     dropped={"late": 0, "inflight": 0, "segment_end": 0}, late_max_ms=0)
            l["lat"] = {"counts": [0] * 13, "max_ms": 0}
            l["cum"] = {k: 0 for k in l["cum"]}
    flat = dict(out["ingress"], **{N: [[t, 100] for t, _ in out["ingress"][N]]})      # the ingress received nothing
    red = reduced(cfg, out, records=recs, ingress=flat)
    s = red["targets"][N]["summary"]
    assert s["states"] == {"observed": 0, "partial": 0, "unknown": 11, "unscheduled": 0}, s
    assert s["reasons"]["due_mismatch"] == 11
    for row in red["targets"][N]["rows"]:
        assert row["state"] == "unknown" and row["reasons"] == ["due_mismatch"], row
        assert row["intended"] > 1_000 and row["generator"]["due"] == 0
    assert s["observed_totals"]["intended"] == 0 and s["unobserved_intended"] == s["scheduled_intended"]
    assert red["targets"][R]["summary"]["states"]["observed"] == 11


@pytest.mark.parametrize("delta", [-1, 1, -200])
def test_due_must_be_exactly_the_scheduled_arrivals_of_the_active_span(tmp_path, delta):
    """One arrival silently omitted (or invented) while the line's own identity still holds."""
    start = PEAK - 3 * MIN + 4_000
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": start}])
    for k in (-3, 2):                                                       # the partial start minute and a full one
        recs = copy.deepcopy(out["lines"])
        line = [l for l in recs if l["schema"] == DR.MINUTE and l["minute"] == BASE + k and l["target"] == N][0]
        span = line["active"]
        assert line["due"] == DR.scheduled_arrivals(cfg, N, BASE + k, *span)
        line["due"] += delta
        line["dispatched"] += delta
        line["ok"] += delta
        line["lat"]["counts"][2] += delta
        assert line["due"] == line["dispatched"] == line["ok"] == sum(line["lat"]["counts"])
        red = DR.reduce(cfg, recs, out["ingress"], PEAK - 3 * MIN, PEAK + 9 * MIN)
        row = {r["minute"] - BASE: r for r in red["targets"][N]["rows"]}[k]
        assert row["state"] == "unknown" and "due_mismatch" in row["reasons"], (k, row["reasons"])


def test_dropped_arrivals_are_accounted_and_keep_the_minute_observed(tmp_path):
    """Accounting is not delivery: a minute whose arrivals were all DUE and partly dropped (recorded) is observed."""
    cfg = edited(config(), max_inflight=5)
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}], cfg=cfg, latency_ms=900)
    got = rows(reduced(cfg, out))
    for k in (1, 2, 3):
        g = got[k]["generator"]
        assert got[k]["state"] == "observed", got[k]
        assert g["dropped"]["inflight"] > 0 and g["due"] == got[k]["intended"] == g["dispatched"] + g["dropped"]["inflight"]


# --- clock evidence: the WHOLE interval inside the limit, for every contributing incarnation (R3 item 5) -----------

def with_clock(lines, lo, hi, pred=lambda l: True):
    """The minute lines with clock evidence that bounds (generator clock - ingress clock) to [lo, hi] ms."""
    out = copy.deepcopy(lines)
    for l in out:
        if l["schema"] == DR.MINUTE and pred(l):
            rt = hi - lo - 1000
            assert rt >= 0
            date_s = (ORIGIN + l["minute"] * MIN) // 1000 + 30
            l["clock"] = {"date_s": date_s, "recv_ms": date_s * 1000 + hi, "rt_ms": rt}
    return out


@pytest.mark.parametrize("lo, hi, reason", [
    (-2500, 500, "clock_unverified"),      # the reviewer's case: it only OVERLAPS the limit of +-2000
    (-500, 2500, "clock_unverified"),
    (-2001, 0, "clock_unverified"),
    (0, 2001, "clock_unverified"),
    (-4000, 4000, "clock_unverified"),
    (2001, 3100, "clock_offset"),          # entirely outside: a proven offset
    (-9000, -2001, "clock_offset"),
    (-2000, -1000, None),                  # entirely inside, at the limit
    (1000, 2000, None),
    (-600, 500, None),
])
def test_the_whole_clock_interval_must_lie_inside_the_limit(tmp_path, lo, hi, reason):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    red = reduced(cfg, out, records=with_clock(out["lines"], lo, hi))
    for row in red["targets"][N]["rows"]:
        assert row["clock_offset_ms"] == [lo, hi]
        if reason is None:
            assert row["state"] == "observed", row
        else:
            assert row["state"] == "unknown" and row["reasons"] == [reason], row
    assert DR._clock_minute({"i": {5: (lo, hi)}}, ["i"], 5, 2000)[1] == reason


def test_every_contributing_incarnation_needs_its_own_clock_evidence(tmp_path):
    """Evidence of one incarnation says nothing about another's clock (another process, possibly another pod): an
    incarnation without evidence of its own is unverified, however good its predecessor's was."""
    cfg = config(smoke_segment_minutes=3)
    a, b = PEAK - 3 * MIN, PEAK + 2_000
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": a}, {"pod_uid": "pod-b", "start_ms": b}], cfg=cfg,
                   minutes_after=8)
    red = reduced(cfg, out, end=PEAK + 3 * MIN)
    assert rows(red)[0]["state"] == "partial" and rows(red)[0]["reasons"] == ["incarnation_boundary"]
    assert rows(red)[-1]["state"] == "observed" and rows(red)[1]["state"] == "observed"
    # The second incarnation without any clock evidence: its minutes are unverified (the first's last sample is one
    # minute away, but it is another incarnation's), the first's stay observed.
    recs = copy.deepcopy(out["lines"])
    for l in recs:
        if l["schema"] == DR.MINUTE and l["inc"] == f"pod-b/{b}":
            l["clock"] = None
    got = rows(reduced(cfg, out, records=recs, end=PEAK + 3 * MIN))
    assert got[-1]["state"] == "observed" and got[-2]["state"] == "observed"
    for k in (0, 1, 2):
        assert got[k]["generator"]["incarnations"] == [f"pod-b/{b}"]
        assert got[k]["state"] == "unknown" and "clock_unverified" in got[k]["reasons"], got[k]
    # Two incarnations in one minute (a duplicate): BOTH need evidence inside the limit.
    # Unit: missing for one, contradictory, neighbours.
    ok, far = (-600, 500), (5_000, 6_100)
    assert DR._clock_minute({"a": {7: ok}}, ["a", "b"], 7, 2000) == ([-600, 500], "clock_unverified")
    assert DR._clock_minute({"a": {7: ok}, "b": {7: ok}}, ["a", "b"], 7, 2000) == ([-600, 500], None)
    assert DR._clock_minute({"a": {7: ok}, "b": {7: far}}, ["a", "b"], 7, 2000) == ([-600, 6_100], "clock_offset")
    assert DR._clock_minute({"a": {7: (400, -200)}}, ["a"], 7, 2000) == ([400, -200], "clock_unverified")   # empty
    assert DR._clock_minute({"a": {5: ok}}, ["a"], 7, 2000) == ([-600, 500], None)           # a neighbour, 2 min away
    assert DR._clock_minute({"a": {1: ok}}, ["a"], 7, 2000) == (None, "clock_unverified")    # 6 minutes: too far
    assert DR._clock_minute({"a": {6: ok, 8: (-2_400, -1_300)}}, ["a"], 7, 2000) == ([-2_400, 500], "clock_unverified")
    assert DR._clock_minute({}, [], 7, 2000) == (None, "clock_unverified")


def test_contradictory_clock_evidence_within_one_minute_is_unverified(tmp_path):
    """The two targets' samples of one minute are the same pod's clock: when they cannot both be true (the clock
    stepped inside the minute), the minute is unverified, whichever sample looks fine."""
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    recs = with_clock(out["lines"], 800, 1_900, lambda l: l["target"] == R and l["minute"] == BASE + 2)
    recs = with_clock(recs, -1_900, -800, lambda l: l["target"] == N and l["minute"] == BASE + 2)
    red = reduced(cfg, out, records=recs)
    for t in (N, R):
        row = rows(red, t)[2]
        assert row["state"] == "unknown" and row["reasons"] == ["clock_unverified"], row
        assert row["clock_offset_ms"][0] > row["clock_offset_ms"][1]         # an empty interval
        assert rows(red, t)[1]["state"] == "observed" and rows(red, t)[3]["state"] == "observed"


def test_a_slow_response_does_not_cost_the_minute_its_clock_evidence(tmp_path):
    """With the strict rule a 1.5 s first response alone would leave the minute unverified ([-2500, ...]); the
    generator records the shortest round trip of the minute, so the evidence stays tight."""
    slow = [{"from_ms": PEAK + 2 * MIN, "to_ms": PEAK + 2 * MIN + 5_000, "latency_ms": 1_500}]
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}], latencies=slow)
    got = rows(reduced(cfg, out))
    assert got[2]["state"] == "observed" and got[2]["generator"]["latency"]["max_ms"] == 1_500
    lo, hi = got[2]["clock_offset_ms"]
    assert -1_100 <= lo <= 0 <= hi <= 1_100


# --- pod replacement under load: two incarnations at once are detected, never summed silently (R3 item 1) ----------

def test_a_pod_replaced_under_load_while_the_old_one_still_runs_is_flagged(tmp_path):
    """What a Deployment allows on ordinary Pod replacement (not an update): the old pod is deleted and keeps
    running for its 15 s termination grace period while the ReplicaSet's new pod already sends, at the daily peak.
    (The rendered StatefulSet cannot do this: test_the_render_never_allows_two_generator_pods; prepare.py's verify
    counts a terminating pod.) If it happens anyway, the records show it: every minute in which both incarnations
    were active is unknown, reason duplicate_generators, and listed."""
    old, new = PEAK - 3 * MIN, PEAK + 2 * MIN + 20_000
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-old", "start_ms": old, "kill_ms": new + 15_000},
                              {"pod_uid": "pod-new", "start_ms": new}])
    # Under load both really sent: in the overlap minute the ingress received about a quarter more than one schedule.
    truth = out["truth"][N][str(BASE + 2)]
    assert truth["dispatched"] > intended(cfg, BASE + 2) * 1.15
    red = reduced(cfg, out)
    got = rows(red)
    for k in (-2, -1, 0):
        check_observed(got[k], out)
    # Minute 1: the old pod's last written line. Minute 2: both active (the old one's line was never written: it
    # was killed first), so the old pod's unrecorded tail makes it unknown; with BOTH lines it is a duplicate.
    assert got[2]["state"] == "unknown" and "unrecorded_tail" in got[2]["reasons"]
    for k in (3, 4, 5, 6):
        check_observed(got[k], out)
    # The same replacement where the old pod lived long enough to write its line (a longer grace period).
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-old", "start_ms": old, "kill_ms": new + 50_000},
                              {"pod_uid": "pod-new", "start_ms": new}])
    red = reduced(cfg, out)
    got = rows(red)
    assert got[2]["state"] == "unknown" and "duplicate_generators" in got[2]["reasons"], got[2]
    assert sorted(got[2]["generator"]["incarnations"]) == [f"pod-new/{new}", f"pod-old/{old}"]
    assert got[2]["generator"]["dispatched"] == out["truth"][N][str(BASE + 2)]["dispatched"] > 1.5 * intended(cfg, BASE + 2)
    assert [d["minute"] - BASE for d in red["duplicate_generators"][N]] == [2]
    assert red["duplicate_generators"][N][0]["incarnations"] == got[2]["generator"]["incarnations"]
    assert got[3]["state"] == "unknown"                                    # the old pod's tail up to its kill
    for k in (-2, -1, 0, 1, 4, 5, 6):
        check_observed(got[k], out)
    s = red["targets"][N]["summary"]
    assert s["observed_totals"]["dispatched"] == sum(got[k]["generator"]["dispatched"] for k in (-2, -1, 0, 1, 4, 5, 6, 7, 8))
    # With the OLD pod's log lost entirely, the surplus still shows: the ingress received more than was dispatched.
    only_new = [l for l in out["lines"] if l["inc"].startswith("pod-new/")]
    got = rows(reduced(cfg, out, records=only_new))
    assert got[2]["state"] != "observed" and got[3]["state"] == "unknown" and got[3]["reasons"] == ["unreconciled"]


# --- conservation: a carried arrival never leaves its incarnation (R4) ---------------------------------------------

def stalled_across_the_segment_end(tmp_path):
    """The reviewer's case on the real script: a two-minute segment [PEAK, PEAK + 2 min), a stall that starts 100 ms
    before its end and lasts 500 ms."""
    cfg = config(smoke_segment_minutes=2)
    end = PEAK + 2 * MIN
    sc = {"incarnations": [{"pod_uid": "pod-a", "start_ms": PEAK, "stalls": [{"at_ms": end - 100, "for_ms": 500}]}],
          "until_ms": end + 3 * MIN, "ingress_initial": 100, "scrape": scrape(PEAK - 2 * MIN, end + 3 * MIN)}
    return cfg, simulate(tmp_path, cfg, sc)


def test_arrivals_owed_at_the_segment_end_are_recorded_drops_and_the_minute_stays_accounted(tmp_path):
    cfg, out = stalled_across_the_segment_end(tmp_path)
    red = DR.reduce(cfg, out["lines"], out["ingress"], PEAK, PEAK + 2 * MIN)
    for t in (N, R):
        first, last = red["targets"][t]["rows"]
        g = last["generator"]
        assert g["dropped"] == {"late": 0, "inflight": 0, "segment_end": 2}
        assert g["due"] == last["intended"] == g["dispatched"] + 2
        assert g["dispatched"] == out["truth"][t][str(last["minute"])]["dispatched"]
        assert "carry_unmatched" not in last["reasons"] and last["generator_state"] == "complete"
        assert last["state"] == "observed" and last["reconciled"] is True, last
        assert out["sends"][t] == first["generator"]["dispatched"] + g["dispatched"] == 2176
        assert red["targets"][t]["summary"]["observed_totals"]["dropped"] == 2


def test_the_old_scripts_records_of_that_case_are_not_observed(tmp_path):
    """What the script wrote for the same case BEFORE the fix: it dispatched the two delayed arrivals after the
    segment end, so its final row says carried = [0, 2] with no drop, and the minute those two were dispatched in has
    no line (2,178 requests sent, 2,176 recorded). The row is self-consistent and was "observed". A carry out of an
    incarnation's last line has no counterpart: carry_unmatched, not observed."""
    cfg, out = stalled_across_the_segment_end(tmp_path)
    recs = copy.deepcopy(out["lines"])
    for l in recs:
        if l["schema"] == DR.MINUTE and l["minute"] == BASE + 1:
            assert l["dropped"]["segment_end"] == 2 and l["carried"] == [0, 0]
            l["dropped"]["segment_end"] = 0
            l["carried"] = [0, 2]                                          # due here, dispatched after the end
            l["cum"]["dropped"] -= 2
            l["cum"]["dispatched"] += 2
        if l.get("event") == "end":
            l["cum"]["dropped"] -= 2
            l["cum"]["dispatched"] += 2
    # The ingress as the old script left it: the two late requests arrived in the minute after the segment.
    ingress = {t: [[ts, v + (2 if ts * 1000 >= PEAK + 2 * MIN + 400 else 0)] for ts, v in out["ingress"][t]]
               for t in out["ingress"]}
    red = DR.reduce(cfg, recs, ingress, PEAK, PEAK + 3 * MIN)
    for t in (N, R):
        first, last, after = red["targets"][t]["rows"]
        assert DR._line_problems(cfg, [l for l in recs if l["schema"] == DR.MINUTE and l["minute"] == BASE + 1
                                       and l["target"] == t][0]) == []      # the line alone looks fine
        assert last["state"] == "unknown" and last["reasons"] == ["carry_unmatched"], last
        assert first["state"] == "observed"
        assert after["state"] != "observed"
        s = red["targets"][t]["summary"]
        assert s["reasons"]["carry_unmatched"] == 1 and s["states"]["observed"] == 1


def test_a_carry_must_be_matched_by_the_next_line_of_the_same_incarnation(tmp_path):
    stall = {"at_ms": PEAK + 2 * MIN - 300, "for_ms": 600}                 # across an ordinary minute boundary
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN, "stalls": [stall]}])
    line = lambda recs, k, t=N: [l for l in recs if l["schema"] == DR.MINUTE and l["minute"] == BASE + k  # noqa: E731
                                 and l["target"] == t][0]
    carried = line(out["lines"], 1)["carried"][1]
    assert carried > 0 and line(out["lines"], 2)["carried"] == [carried, 0]
    got = rows(reduced(cfg, out))
    assert got[1]["state"] == "observed" and got[2]["state"] == "observed"  # a matched carry is fine
    # The next line says it carried in fewer than this one carried out (its own identity kept intact).
    recs = copy.deepcopy(out["lines"])
    nxt = line(recs, 2)
    nxt["carried"][0] -= 1
    nxt["dispatched"] -= 1
    nxt["ok"] -= 1
    nxt["lat"]["counts"][2] -= 1
    assert DR._line_problems(cfg, nxt) == []
    got = rows(reduced(cfg, out, records=recs))
    for k in (1, 2):
        assert got[k]["state"] == "unknown" and "carry_unmatched" in got[k]["reasons"], got[k]
    assert got[0]["state"] == "observed" and got[3]["state"] == "observed"
    # The next line is lost: the carry-out has no counterpart at all.
    recs = [l for l in out["lines"] if not (l["schema"] == DR.MINUTE and l["minute"] == BASE + 2 and l["target"] == N)]
    got = rows(reduced(cfg, out, records=recs))
    assert got[1]["state"] == "unknown" and got[1]["reasons"] == ["carry_unmatched"]
    assert got[2]["state"] == "unknown" and got[2]["reasons"] == ["generator_record_missing"]
    # The previous line is lost: the carry-in has no counterpart.
    recs = [l for l in out["lines"] if not (l["schema"] == DR.MINUTE and l["minute"] == BASE + 1 and l["target"] == N)]
    got = rows(reduced(cfg, out, records=recs))
    assert got[2]["state"] == "unknown" and got[2]["reasons"] == ["carry_unmatched"]
    # A carry-in on an incarnation's FIRST line, or a carry-out on its LAST, can never be matched.
    cfg2, out2 = stalled_across_the_segment_end(tmp_path)
    for k, idx in ((0, 0), (1, 1)):
        recs = copy.deepcopy(out2["lines"])
        l = line(recs, k)
        l["carried"][idx] += 3
        l["dispatched"] += 3 if idx == 0 else -3
        l["ok"] += 3 if idx == 0 else -3
        l["lat"]["counts"][2] += 3 if idx == 0 else -3
        assert DR._line_problems(cfg2, l) == []
        red = DR.reduce(cfg2, recs, out2["ingress"], PEAK, PEAK + 2 * MIN)
        row = red["targets"][N]["rows"][k]
        assert row["state"] == "unknown" and "carry_unmatched" in row["reasons"], (k, row["reasons"])
    # The other target is untouched throughout.
    assert rows(reduced(cfg, out), R)[1]["state"] == "observed"


def test_a_line_without_the_segment_end_drop_field_is_malformed(tmp_path):
    cfg, out = sim(tmp_path, [{"pod_uid": "pod-a", "start_ms": PEAK - 3 * MIN}])
    recs = copy.deepcopy(out["lines"])
    for l in recs:
        if l["schema"] == DR.MINUTE and l["minute"] == BASE + 2 and l["target"] == N:
            del l["dropped"]["segment_end"]                                 # a v2-shaped line
    red = reduced(cfg, out, records=recs)
    assert [i["flags"] for i in red["incarnations"] if i["target"] == N] == [["malformed_line"]]
    assert rows(red)[1]["state"] == "unknown"


def test_a_first_send_record_binds_only_its_own_incarnations_first_send():
    from c28.demo_reduce import _Stream, first_send_problems
    cfg = {"run_id": "r", "origin_ms": 0, "minute_ms": 60_000}
    st = {"started_ms": 900_000, "send_from_ms": 900_100, "send_to_ms": 1_800_000}

    def fs(**over):
        return {"schema": "c28-demo-incarnation/v2", "event": "first_send", "run": "r", "inc": "i2", "target": "a",
                "n": None, "sent_ms": 904_000, "minute": 15, **over}
    assert first_send_problems(cfg, fs(), st, "i2", "a") == []
    for bad, why in ((fs(run="x"), "another run"), (fs(target="b"), "another run"), (fs(inc="i3"), "another run"),
                     (fs(sent_ms="904000"), "integer"), (fs(sent_ms=896_000, minute=14), "before the incarnation"),
                     (fs(sent_ms=1_800_000, minute=30), "sending span"), (fs(minute=16), "not the minute")):
        assert any(why in p for p in first_send_problems(cfg, bad, st, "i2", "a")), (bad, why)
    assert first_send_problems(cfg, fs(), None, "i2", "a") == ["first_send without a start record"]
    s = _Stream("i2", "a")
    s.add(cfg, fs())
    s.add(cfg, fs())                                                   # an identical reread is the same record
    assert s.first_send == fs() and "conflicting_first_send" not in s.flags
    s.add(cfg, fs(sent_ms=905_000))                                    # a different one: the first send is unknown
    assert "conflicting_first_send" in s.flags and s.first_send == fs()
