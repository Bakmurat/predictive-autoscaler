"""The frozen scorer on synthetic studies: bootstrap, Holm and every classification (manifest sections 7 and 8)."""

import copy

import pytest

from c28 import frozen as F
from c28 import score as S


def window_metrics(minutes, viol=0, err=0.0, surplus=1.0, pods=6.0, observed=None):
    observed = minutes if observed is None else observed
    req = 1000.0 * minutes
    return {"minutes": minutes, "observed_minutes": observed, "slo_violation_minutes": viol,
            "slo_ok_minutes": observed - viol, "requests": req, "failures": err * req, "error_fraction": err,
            "surplus_replica_seconds": surplus * minutes * 60, "shortage_replica_seconds": 0.0,
            "ready_replica_seconds": 5.0 * minutes * 60, "provisioned_replica_seconds": pods * minutes * 60,
            "capacity_coverage": 1.0}


def block(arm, profile, seed, viol=None, err=0.0, surplus=1.0, pods=6.0, eligible=0.5, e1=0.3, **extra):
    viol = viol or {}
    ws = {wid: window_metrics((e - s) * 60, viol.get(wid, 0), err, surplus, pods) for wid, s, e, _ in F.WINDOWS}
    day = window_metrics(1440, sum(w["slo_violation_minutes"] for w in ws.values()), err, surplus, pods)
    b = {"arm": arm, "profile": profile, "seed": seed, "invalid_cause": None, "windows": ws, "day": day,
         "validity": {"recorder_coverage": 0.999, "max_transition_gap_s": 10, "dispatch_fraction_min": 0.999,
                      "timestamp_skew_s": 0.2},
         "eligible_fraction": eligible if arm in F.CANDIDATE_ARMS else None,
         "e1_selected_fraction": e1 if arm == "PA-E" else None}
    b.update(extra)
    return b


def study(blocks_fn=None):
    """A complete, valid study in which PA-N halves the ramp-window violations of every comparator and PA-E never
    selects E1."""
    blocks = []
    for p in F.PROFILES:
        for s in F.CONFIRMATION_SEEDS:
            jitter = (s - 203)  # -2..2: varies the paired days
            for arm in F.ARMS:
                ramp = 10 + jitter
                if arm == "PA-N":
                    ramp = 5 + jitter
                blocks.append(block(arm, p, s, viol={"w06-ramp": max(ramp, 0)}, e1=0.0 if arm == "PA-E" else 0.3))
    st = {"schema": "c28-study/v1", "blocks": blocks, "safety_breaches": [], "contract_failures": [],
          "resources": {"g3_pass": True},
          "prerequisites": {"pinned_tree": True, "replay_simulator": True, "lab_qualification": True,
                            "family_restriction": True, "policy_cost_binding": True, "envelope_and_soak": True,
                            "recorder_frozen": True},
          "artifacts_complete": True, "cleanup_verified": True,
          "gates": {a: {"fallback": True, "blocked_slow_startup": True, "quota_guards": True, "quota_observable": True,
                        "quota_resumed": True, "g4_qualified": True} for a in F.CANDIDATE_ARMS}}
    if blocks_fn:
        blocks_fn(st)
    return st


def edit_blocks(st, arm, fn, profile=None):
    for b in st["blocks"]:
        if b["arm"] == arm and (profile is None or b["profile"] == profile):
            fn(b)


def test_bootstrap_is_deterministic_and_resamples_whole_days():
    r1, r2 = S.bootstrap([1.0, 2.0, 3.0, 4.0, 5.0]), S.bootstrap([1.0, 2.0, 3.0, 4.0, 5.0])
    assert r1["ci95"] == r2["ci95"] and r1["mean"] == 3.0 and 1.0 <= r1["ci95"][0] < 3 < r1["ci95"][1] <= 5.0
    const = S.bootstrap([-2.0] * 5)
    assert const["ci95"] == [-2.0, -2.0] and const["p"] == pytest.approx(2 / 10001)
    assert len(S.resample_indices(5)) == F.BOOTSTRAP_RESAMPLES and S.resample_indices(5) is S.resample_indices(5)


def test_holm_step_down():
    h = S.holm({"a": 0.01, "b": 0.04, "c": 0.03})
    assert h["a"]["reject"] and not h["c"]["reject"] and not h["b"]["reject"]
    assert h["a"]["p_holm"] == pytest.approx(0.03) and h["c"]["p_holm"] == pytest.approx(0.06)
    assert h["b"]["p_holm"] == pytest.approx(0.06)
    assert all(v["reject"] for v in S.holm({"x": 0.001, "y": 0.02}).values())


def test_a_complete_benefit_passes_only_the_family_that_earned_it():
    out = S.score(study())
    assert out["outcome"] == "PASS" and out["claims"] == ["PA-N"]
    assert out["candidates"]["PA-N"]["outcome"] == "PASS"
    pae = out["candidates"]["PA-E"]
    assert pae["outcome"] == "INCONCLUSIVE" and any("no eligible use" in r for r in pae["reasons"]["INCONCLUSIVE"])
    benefit = [c for c in out["candidates"]["PA-N"]["comparisons"] if c["gate"] == "useful benefit"]
    assert len(benefit) == 4 and all(c["route"] == "A" and c["reduction"] >= 0.2 for c in benefit)
    assert any(c["holm_reject"] for c in benefit)


def test_an_observed_safety_breach_fails_regardless_of_performance():
    st = study()
    st["safety_breaches"] = ["Recommend write on c28-pa-n-s20"]
    out = S.score(st)
    assert out["outcome"] == "FAIL" and out["candidates"]["PA-N"]["outcome"] == "FAIL"


def test_unmet_prerequisites_are_not_ready():
    st = study()
    st["prerequisites"]["lab_qualification"] = False
    assert S.score(st)["outcome"] == "NOT READY"
    st = study()
    st["prerequisites"]["family_restriction"] = False
    out = S.score(st)
    assert out["candidates"]["PA-N"]["outcome"] == "NOT READY"


def test_a_missing_seed_or_an_external_failure_is_incomplete():
    st = study(lambda s: s["blocks"].remove(next(b for b in s["blocks"] if b["arm"] == "H-CPU" and b["seed"] == 205)))
    assert S.score(st)["outcome"] == "INCOMPLETE"
    st = study(lambda s: edit_blocks(s, "PA-R", lambda b: b.update(invalid_cause="external") if b["seed"] == 201 else None))
    out = S.score(st)
    assert out["candidates"]["PA-N"]["outcome"] == "INCOMPLETE"
    st = study(lambda s: edit_blocks(s, "PA-N", lambda b: b["validity"].update(dispatch_fraction_min=0.99)
                                     if b["seed"] == 202 else None))
    assert S.score(st)["candidates"]["PA-N"]["outcome"] == "INCOMPLETE"
    st = study()
    st["cleanup_verified"] = False
    assert S.score(st)["outcome"] == "INCOMPLETE"


def test_product_failures_and_failed_gates_fail():
    st = study(lambda s: edit_blocks(s, "PA-N", lambda b: b.update(invalid_cause="product") if b["seed"] == 203 else None))
    assert S.score(st)["outcome"] == "FAIL"
    st = study()
    st["gates"]["PA-N"]["fallback"] = False
    assert S.score(st)["candidates"]["PA-N"]["outcome"] == "FAIL"
    st = study()
    st["gates"]["PA-N"]["quota_resumed"] = None
    assert S.score(st)["candidates"]["PA-N"]["outcome"] == "INCOMPLETE"
    st = study()
    st["resources"]["g3_pass"] = False
    assert S.score(st)["outcome"] == "FAIL"


def test_a_statistically_supported_margin_violation_fails():
    # PA-N adds 30 violation minutes a day in the ordinary 18:00-20:00 window on every paired day.
    st = study(lambda s: edit_blocks(s, "PA-N", lambda b: b["windows"]["w18-high"].update(
        slo_violation_minutes=30, slo_ok_minutes=90) or b["day"].update(
        slo_violation_minutes=b["day"]["slo_violation_minutes"] + 30)))
    out = S.score(st)["candidates"]["PA-N"]
    assert out["outcome"] == "FAIL"
    assert any("G5 slo_violation_minutes" in r for r in out["reasons"]["FAIL-STAT"])
    assert any("absolute SLO" in r for r in out["reasons"]["FAIL-STAT"])


def test_an_interval_crossing_a_margin_is_inconclusive():
    def one_bad_day(s):
        for b in s["blocks"]:
            if b["arm"] == "PA-N" and b["seed"] == 205:
                b["windows"]["w01-step"]["slo_violation_minutes"] += 6       # robustness margin: 2 min for 2 h
    out = S.score(study(one_bad_day))["candidates"]["PA-N"]
    assert out["outcome"] == "INCONCLUSIVE"
    assert any("robustness w01-step slo_violation_minutes" in r for r in out["reasons"]["INCONCLUSIVE"])


def test_unmatched_surplus_is_inconclusive():
    st = study(lambda s: edit_blocks(s, "PA-N", lambda b: [w.update(surplus_replica_seconds=w["minutes"] * 60 * 2.0)
                                                          for w in list(b["windows"].values()) + [b["day"]]]))
    out = S.score(st)["candidates"]["PA-N"]
    assert out["outcome"] == "INCONCLUSIVE" and any("surplus not matched" in r for r in out["reasons"]["INCONCLUSIVE"])


def test_route_b_applies_when_the_comparator_has_no_violation_time():
    def clean(s):
        for b in s["blocks"]:
            b["windows"]["w06-ramp"].update(slo_violation_minutes=0, slo_ok_minutes=180)
            b["day"]["slo_violation_minutes"] = 0
            if b["arm"] == "PA-N":
                for w in list(b["windows"].values()) + [b["day"]]:
                    w["provisioned_replica_seconds"] *= 0.8 - 0.01 * (b["seed"] - 203)
    out = S.score(study(clean))
    benefit = [c for c in out["candidates"]["PA-N"]["comparisons"] if c["gate"] == "useful benefit"]
    assert all(c["route"] == "B" for c in benefit) and out["candidates"]["PA-N"]["outcome"] == "PASS"
    # Without the 10 % reduction route B does not pass, and nothing else can.
    out = S.score(study(lambda s: [b["windows"]["w06-ramp"].update(slo_violation_minutes=0, slo_ok_minutes=180)
                                   or b["day"].update(slo_violation_minutes=0) for b in s["blocks"]]))
    assert out["candidates"]["PA-N"]["outcome"] == "INCONCLUSIVE"


def test_the_study_schema_is_checked():
    with pytest.raises(ValueError):
        S.score({"schema": "other"})


def test_pa_e_needs_g4_evidence_and_a_qualified_e1():
    def e1_used(st):
        edit_blocks(st, "PA-E", lambda b: b.update(e1_selected_fraction=0.4))
        edit_blocks(st, "PA-E", lambda b: b["windows"]["w06-ramp"].update(slo_violation_minutes=4, slo_ok_minutes=176))
    st = study(e1_used)
    assert S.score(st)["candidates"]["PA-E"]["outcome"] == "PASS"
    st["gates"]["PA-E"]["g4_qualified"] = False
    out = S.score(st)["candidates"]["PA-E"]
    assert out["outcome"] == "INCONCLUSIVE" and any("G4" in r for r in out["reasons"]["INCONCLUSIVE"])
    st["gates"]["PA-E"].pop("g4_qualified")
    assert S.score(st)["candidates"]["PA-E"]["outcome"] == "INCOMPLETE"
    assert S.score(st)["candidates"]["PA-N"]["outcome"] == "PASS"     # E1's evidence never invalidates naive
