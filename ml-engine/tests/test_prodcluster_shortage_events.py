"""deploy/prodcluster/scoring/shortage_events.py v4: events, timelines and evidence tags (Codex r42-r44 cases)."""
import hashlib
import importlib.util
import json
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "..", "..", "deploy", "prodcluster", "scoring", "shortage_events.py")
SPEC = importlib.util.spec_from_file_location("shortage_events", PATH)
se = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(se)

D0 = 1_791_331_200_000             # 2026-10-07T00:00:00Z
STEP = se.STEP
taus = [D0 + i * STEP for i in range(20)]


def iso(t):
    return se.cap.iso(t)


def test_events_censoring_recovery_cause_and_need_at_onset():
    need = {t: 2 for t in taus}
    for t in taus[4:8]:
        need[t] = 3
    need[taus[6]] = need[taus[7]] = 4                                 # the requirement rises inside the event
    ready = {t: 2.0 for t in taus}
    ready[taus[0]] = 1.0                                               # left-censored at the window start
    ready[taus[12]] = 1.0
    ready[taus[13]] = None                                             # right-censored before a gap
    for t in taus[16:18]:
        need[t] = 3                                                    # recovers because demand falls back
    ev = se.events(taus, ready, need)
    assert [(e["start"], e["left_censored"], e["right_censored"]) for e in ev] == \
        [(taus[0], True, False), (taus[4], False, False), (taus[12], False, True), (taus[16], False, False)]
    mid = ev[1]
    assert mid["need_at_onset"] == 3 and mid["max_deficit"] == 2 and mid["demand_step"]
    assert ev[3]["recovery_cause"] == "demand_fell"


def decision(at, **kw):
    d = {"at": iso(at), "forecast_status": "used", "raw_predicted_replicas": 3, "confidence_adjusted_replicas": 3,
         "predicted_after_clamp": 3, "predicted_replicas": 3, "desired_replicas": 3, "applied_replicas": 2,
         "max_replicas": 12, "action": "scale_up", "desired_source": "prediction", "returned_issuance_id": None,
         "safeguards": []}
    d.update(kw)
    return d


def every_minute(until, **kw):
    """Decisions every 60 s from 30 min before `until` through it (full coverage)."""
    return [decision(t, **kw) for t in range(until - 30 * 60_000, until + 1, 60_000)]


def timeline(decs, need_at_onset=3, issuances=None, rate=lambda s: 1300, truth=None, need=None, app="nginx-test"):
    e = {"start": taus[10], "need_at_onset": need_at_onset, "recovered": taus[12], "end": taus[11],
         "recovery_min": 1.0, "recovery_cause": "ready_rose"}
    times = [se.ts(d["at"]) for d in decs]
    return se.operator_timeline(e, decs, times, issuances or {}, app, rate, truth or {}, need or {t: 3 for t in taus})


def test_onset_tags_use_the_requirement_at_the_onset_not_the_event_maximum():
    decs = every_minute(taus[10], raw_predicted_replicas=3, applied_replicas=3)
    tags, *_ = timeline(decs, need_at_onset=3)
    assert "lead_peak_low" not in tags and "applied_ahead_ready_late" in tags


def test_r43_applied_ahead_needs_decision_coverage_and_a_fresh_onset_decision():
    sparse = [decision(taus[10] - 10 * 60_000, applied_replicas=3)]          # one decision 10 min earlier, nothing since
    tags, *_ = timeline(sparse)
    assert tags == ["decision_coverage_gap"] or "applied_ahead_ready_late" not in tags
    gap = every_minute(taus[10], applied_replicas=3)
    gap = [d for d in gap if not (taus[10] - 10 * 60_000 < se.ts(d["at"]) < taus[10] - 6 * 60_000)]   # a 4-min hole
    tags, *_ = timeline(gap)
    assert "decision_coverage_gap" in tags and "applied_ahead_ready_late" not in tags


def test_chain_tags_and_timeline_delays():
    decs = every_minute(taus[10], raw_predicted_replicas=2, confidence_adjusted_replicas=2, predicted_after_clamp=2,
                        predicted_replicas=2, desired_replicas=2)
    decs.append(decision(taus[11] + 3_000, desired_replicas=3, applied_replicas=3))
    tags, at, fc, tl = timeline(decs)
    assert "lead_peak_low" in tags
    assert tl["first_observed_sufficient_decision_min"] == tl["first_observed_sufficient_apply_min"] == 0.55
    assert tl["pre_onset"]["desired"] == 2 and tl["decision_gaps_over_120s"] == 0


def test_r43_plan_is_a_capacity_reference_truth_is_the_forecast_error():
    used = {"u1": {"issuance_id": "u1", "application": "nginx-test",
                   "forecasts": [[1, iso(taus[10]), 1100.0], [2, iso(taus[10] + 600_000), 1500.0]]}}
    decs = every_minute(taus[10], returned_issuance_id="u1")
    tags, at, fc, _ = timeline(decs, issuances=used, rate=lambda s: 1300, truth={taus[10]: 1050.0})
    assert "forecast_below_plan" in tags and "forecast_below_truth" not in tags      # below plan, above truth
    assert fc["forecast_minus_truth"] == 50.0
    tags, *_ = timeline(decs, issuances=used, rate=lambda s: 1000, truth={taus[10]: 1200.0})
    assert "forecast_below_plan" not in tags and "forecast_below_truth" in tags


def test_r43_an_issuance_of_another_application_is_flagged_not_used():
    used = {"u1": {"issuance_id": "u1", "application": "nginx-seasonal", "forecasts": [[1, iso(taus[10]), 900.0]]}}
    tags, at, fc, _ = timeline(every_minute(taus[10], returned_issuance_id="u1"), issuances=used)
    assert "issuance_join_mismatch" in tags and fc is None


def test_keda_is_judged_over_the_whole_event_against_the_contemporaneous_requirement():
    e = {"instants": taus[3:7]}
    need = {t: 2 for t in taus}
    need[taus[5]] = need[taus[6]] = 3
    hpa = {taus[3]: 2.0, taus[4]: 2.0, taus[5]: 2.0, taus[6]: 3.0}            # below at one instant
    assert se.keda_tags(e, hpa, need) == (["hpa_desired_below_need"], {"met": 3, "below": 1, "unobserved": 0})
    met = {taus[3]: 2.0, taus[4]: 2.0, taus[5]: 3.0}                          # one instant unobserved, none below
    assert se.keda_tags(e, met, need) == (["hpa_partially_unobserved", "hpa_desired_met_ready_late"],
                                          {"met": 3, "below": 0, "unobserved": 1})
    assert se.keda_tags(e, {}, need)[0] == ["hpa_unobserved"]


def with_trailer(lines):
    body = ("\n".join(lines) + "\n").encode()
    return body.decode() + json.dumps({"trailer": {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}}) + "\n"


def test_r43_r44_extraction_rows_are_validated_against_trailer_and_receipt(tmp_path):
    K, L = ["at", "application"], ["returned_issuance_id"]
    receipt = {"receipt": {"decisions": 1, "issuances": 1, "decision_fields": K, "lookup_fields": L, "malformed_lines": 0,
                           "extractor": "extract_decisions.sh v2", "extractor_sha256": "x" * 64, "sha256": "y" * 64,
                           "lines": 3, "source": {"context": "c", "pvc": "forecast-log-pvc", "pod": "p", "node": "n"}}}
    good = [json.dumps(receipt), json.dumps(["D", "2026-10-07T00:00:00Z", "nginx-test", None]),
            json.dumps(["I", "u1", "2026-10-07T00:00:00Z", "nginx-test", None, "v", []])]
    p = tmp_path / "rows.jsonl"
    p.write_text(with_trailer(good))
    r, decs, iss, sha = se.read_rows(str(p))
    assert len(decs) == 1 and "u1" in iss and len(sha) == 64
    bad_receipt = json.dumps({"receipt": dict(receipt["receipt"], malformed_lines=1)})
    no_count = json.dumps({"receipt": {k: v for k, v in receipt["receipt"].items() if k != "malformed_lines"}})
    for bad in (with_trailer(good + [json.dumps(["X", 1])]),
                with_trailer(good + [json.dumps(["I", "u1", "2026-10-07T00:00:00Z", "nginx-seasonal", None, "v", []])]),
                with_trailer(good[:2]),
                with_trailer([bad_receipt] + good[1:]),                       # malformed source lines are rejected
                with_trailer([no_count] + good[1:]),                          # the count must be explicit
                "\n".join(good) + "\n",                                      # no trailer
                with_trailer(good).replace("nginx-test", "nginx-tesT", 1)):   # bytes changed in transfer
        p.write_text(bad)
        with pytest.raises(ValueError):
            se.read_rows(str(p))
