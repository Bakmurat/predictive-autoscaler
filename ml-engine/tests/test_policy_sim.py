"""C2.8a: the same-policy cost simulator. Its replay is held decision for decision to the operator's replayV2 by the
Go-generated parity fixture; its cost integrals are hand-checked; its paired comparison is bound and matched."""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting import policy_sim as P  # noqa: E402

FIXTURE = Path(__file__).resolve().parents[2] / "k8s-operator" / "controllers" / "testdata" / "v2-policy-parity.json"


def epoch(text):
    return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").timestamp() - datetime(1970, 1, 1).timestamp())


def cases():
    return json.loads(FIXTURE.read_text())["cases"]


def test_the_fixture_is_large_and_exercises_the_policy():
    cs = cases()
    decisions = [d for c in cs for d in c["decisions"]]
    assert len(cs) == 48 and len(decisions) > 3000
    assert sum(d["applied"] < d["current"] for d in decisions) >= 50
    assert sum(d["cap_binding"] for d in decisions) >= 20


@pytest.mark.parametrize("i", range(48))
def test_the_python_replay_matches_the_operators_decisions(i):
    c = cases()[i]
    inp = c["input"]
    params = P.PolicyParams(inp["Min"], inp["Max"], inp["capacity_rpm"], inp["headroom_percent"],
                            inp["max_extra_replicas"], inp["max_extra_percent"])
    steps = [P.Step(epoch(s["t"]), s["rpm"], s["upper_rps"]) for s in inp["Steps"]]
    got = P.replay(params, steps, inp["Min"])
    for d, want in zip(got, c["decisions"]):
        assert (d.current, d.reactive, d.predictive, d.cap_binding, d.desired, d.applied) == (
            want["current"], want["reactive"], want["predictive"], want["cap_binding"], want["desired"],
            want["applied"]), (want["t"], d)
    assert len(got) == len(c["decisions"])


# --- cost integrals -------------------------------------------------------------------------------------------------

PARAMS = P.PolicyParams(min_replicas=1, max_replicas=12, capacity_rpm=600, headroom_percent=0, max_extra_replicas=2,
                        max_extra_percent=50, startup_delay_seconds=120, decision_interval_seconds=60)


def test_costs_are_exact_piecewise_integrals():
    # One decision at 0 raising 1 -> 4 replicas, startup delay 120 s, 300 s window, demand 3 replicas throughout.
    decisions = [P.Decision(0, 1, 4, None, False, 4, 4)]
    truth = {t: 30.0 for t in range(0, 300, 60)}                      # 30 rps / 10 rps per replica = 3
    c = P.window_costs(decisions, 1, truth, 60, 10.0, 120, 0, 300)
    assert c["replica_time"] == 4 * 300                               # requested, ready or not
    assert c["shortage"] == pytest.approx((3 - 1) * 120)             # only one ready replica until 120 s
    assert c["unknown_truth_seconds"] == 0
    gap = P.window_costs(decisions, 1, {0: 30.0, 60: 30.0}, 60, 10.0, 120, 0, 300)
    assert gap["unknown_truth_seconds"] == 180                        # missing truth is unknown, never zero shortage


def test_a_scale_down_is_ready_immediately():
    decisions = [P.Decision(0, 6, 2, None, False, 2, 2)]
    c = P.window_costs(decisions, 6, {0: 20.0, 60: 20.0}, 60, 10.0, 300, 0, 120)
    assert c["replica_time"] == 2 * 120 and c["shortage"] == 0


def test_paired_costs_differ_only_where_both_forecasts_existed():
    truth = {t: 25.0 + (40 if 1800 <= t < 3600 else 0) for t in range(0, 7200, 60)}
    early = {t: 70.0 for t in range(0, 7200, 60)}                      # anticipates the step
    late = {t: 25.0 for t in range(0, 7200, 60)}                       # does not
    res = P.paired_costs(epoch="e" * 64, challenger=("statistical", "e1"), baseline=("seasonal_naive", "naive"),
                         params=PARAMS, truth_rps=truth, challenger_upper=early, baseline_upper=late, start=0,
                         end=7140, interval=60)
    shortage_c, shortage_b = res["shortage"]
    time_c, time_b = res["replica_time"]
    assert shortage_c < shortage_b and time_c > time_b                 # less shortage for more replica-time
    assert res["paired_decisions"] == res["decisions"] == 120
    assert res["binding"] == {"epoch": "e" * 64, "challenger": {"model": "statistical", "recipe": "e1"},
                              "baseline": {"model": "seasonal_naive", "recipe": "naive"},
                              "policy_sha256": PARAMS.fingerprint(), "window": [0, 7140]}
    half = {t: v for t, v in early.items() if t % 120 == 0}            # the challenger missing every other step
    res2 = P.paired_costs(epoch="e" * 64, challenger=("statistical", "e1"), baseline=("seasonal_naive", "naive"),
                          params=PARAMS, truth_rps=truth, challenger_upper=half, baseline_upper=late, start=0,
                          end=7140, interval=60)
    assert res2["paired_decisions"] == 60


def test_identical_forecasts_cost_the_same():
    truth = {t: 30.0 for t in range(0, 3600, 60)}
    same = {t: 35.0 for t in range(0, 3600, 60)}
    res = P.paired_costs(epoch="e" * 64, challenger=("statistical", "e1"), baseline=("seasonal_naive", "n"),
                         params=PARAMS, truth_rps=truth, challenger_upper=same, baseline_upper=dict(same), start=0,
                         end=3540, interval=60)
    assert res["shortage"][0] == res["shortage"][1] and res["replica_time"][0] == res["replica_time"][1]


def test_the_policy_fingerprint_names_every_parameter():
    import dataclasses
    base = PARAMS.fingerprint()
    for change in ({"min_replicas": 2}, {"max_replicas": 11}, {"capacity_rpm": 601}, {"headroom_percent": 10},
                   {"max_extra_replicas": 3}, {"max_extra_percent": 40}, {"startup_delay_seconds": 90},
                   {"decision_interval_seconds": 120}):
        assert dataclasses.replace(PARAMS, **change).fingerprint() != base, change


def test_the_binding_window_is_in_window_end_coordinates():
    truth = {t: 30.0 for t in range(0, 3600, 60)}
    res = P.paired_costs(epoch="e" * 64, challenger=("statistical", "e1"), baseline=("seasonal_naive", "n"),
                         params=PARAMS, truth_rps=truth, challenger_upper=truth, baseline_upper=truth, start=600,
                         end=3000, interval=60, window_offset=1200)
    assert res["binding"]["window"] == [1800, 4200]                   # the issued window ends of those decisions


# --- the policy of a live autoscaler (mirrors the operator's spec reading) ------------------------------------------

class Lead:
    startup_delay_seconds = 90


def spec(**over):
    s = {"minReplicas": 1, "maxReplicas": 12, "metrics": {"requests": {"capacityPerReplica": "10"}},
         "prediction": {"updateIntervalSeconds": 60}}
    s.update(over)
    return s


def settings(**pred):
    from forecasting import contracts as c
    return c.resolve_settings({"spec": {"prediction": {"sampleIntervalSeconds": 60, **pred}}})


@pytest.mark.parametrize("text,want", [("10", 10.0), (10, 10.0), ("2500m", 2.5), ("1k", 1000.0), ("1.5", 1.5),
                                       ("1e3", 1000.0), ("x", None), ("", None), (True, None), (None, None)])
def test_quantities(text, want):
    assert P.quantity(text) == want


@pytest.mark.parametrize("req,want", [({"capacityPerReplica": "10"}, 600.0), ({"capacityPerReplica": "500m"}, 30.0),
                                      ({"targetRPS": 7}, 420.0), ({"capacityPerReplica": "10", "targetRPS": 7}, None),
                                      ({"capacityPerReplica": "0"}, None), ({"capacityPerReplica": "2e6"}, None),
                                      ({}, None), ({"targetRPS": True}, None)])
def test_capacity_follows_the_operator(req, want):
    assert P.capacity_rpm({"metrics": {"requests": req}}) == want


@pytest.mark.parametrize("update,dt,want", [(None, 60, 60), (60, 60, 60), (10, 60, 60), (90, 60, 120), (45, 15, 45),
                                            (61, 300, 300), (None, 15, 60)])
def test_the_decision_interval_follows_the_operator(update, dt, want):
    s = {"prediction": {} if update is None else {"updateIntervalSeconds": update}}
    assert P.decision_interval(s, dt) == want


def test_resolve_policy_reads_the_autoscalers_own_policy():
    p, why = P.resolve_policy(spec(behavior={"scaleDown": {"headroomPercent": 25}}),
                              settings(predictiveExtra={"maxReplicas": 3, "maxPercent": 40}), Lead())
    assert why == "" and p == P.PolicyParams(1, 12, 600.0, 25, 3, 40, 90, 60)
    default, _ = P.resolve_policy(spec(), settings(), Lead())
    assert default.headroom_percent == 10 and (default.max_extra_replicas, default.max_extra_percent) == (2, 50)
    assert P.resolve_policy(spec(behavior={"scaleDown": {"headroomPercent": None}}), settings(), Lead())[0] == default


@pytest.mark.parametrize("over,lead,why", [({"metrics": {}}, Lead(), "capacity"), ({}, None, "lead"),
                                           ({"minReplicas": 0}, Lead(), "bounds"),
                                           ({"minReplicas": 5, "maxReplicas": 4}, Lead(), "bounds"),
                                           ({"behavior": {"scaleDown": {"headroomPercent": 100}}}, Lead(), "headroom")])
def test_resolve_policy_refuses_what_the_operator_would_not_run(over, lead, why):
    p, reason = P.resolve_policy(spec(**over), settings(), lead)
    assert p is None and why in reason
