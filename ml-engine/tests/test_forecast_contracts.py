"""C2.1: forecast contract v2 (window-max-rps/v1): settings, fingerprint, identity binding, window and response."""

import copy
import dataclasses
import hashlib
import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.identity import CONTRACT, LookupFailed, ProvenanceRefused  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting.contracts import ContractRefused  # noqa: E402

QUERY = 'sum(rate(istio_requests_total{destination_workload="web",destination_workload_namespace="shop"}[5m]))'
SHA = hashlib.sha256(QUERY.encode()).hexdigest()
ORIGIN = int(datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc).timestamp())
NOW_MS = ORIGIN * 1000 + 4_321
LEAD = {"seconds": 1200, "startupDelaySeconds": 420, "actuationAllowanceSeconds": 30, "method": "configured",
        "sampleCount": 0, "observedGeneration": 3}


def pa_object(prediction=None, lead=LEAD, generation=3):
    pa = {"metadata": {"uid": "pa-uid", "generation": generation},
          "spec": {"targetDeployment": {"name": "web", "namespace": "shop"}, "mode": "Recommend",
                   "prediction": prediction if prediction is not None else {"horizonMinutes": 60}},
          "status": {"metricSource": {"query": QUERY, "sha256": SHA, "observedGeneration": generation,
                                      "targetUID": "dep-uid", "contract": CONTRACT}}}
    if lead is not None:
        pa["status"]["prediction"] = {"lead": dict(lead)}
    return pa


class FakeReader:
    def __init__(self, pa=None, dep=None, fail=None):
        self.pa = pa if pa is not None else pa_object()
        self.dep = dep if dep is not None else {"metadata": {"uid": "dep-uid"}}
        self.fail, self.paths = fail, []

    def get(self, path):
        self.paths.append(path)
        if self.fail:
            raise self.fail
        return self.pa if "predictiveautoscalers" in path else self.dep


def settings_sha(pa=None):
    return c.resolve_settings(pa or pa_object()).fingerprint()


def request(**over):
    r = {"autoscaler_namespace": "shop", "autoscaler_name": "web-pa", "autoscaler_uid": "pa-uid",
         "autoscaler_generation": 3, "target_uid": "dep-uid", "metric_query_sha256": SHA, "contract": CONTRACT,
         "application": "web", "namespace": "shop", "metric_type": "requests",
         "forecast_contract": c.FORECAST_CONTRACT, "decision_origin": c.format_ts(ORIGIN),
         "settings_sha256": settings_sha()}
    r.update(over)
    return {k: v for k, v in r.items() if v is not None}


# --- settings and fingerprint -----------------------------------------------------------------------------------

def test_defaults_resolve_and_the_fingerprint_is_stable():
    s = c.resolve_settings(pa_object(prediction={}))
    assert s == c.ForecastSettings()
    assert (s.sample_interval_seconds, s.horizon_seconds, s.steps, s.history_days) == (300, 3600, 12, 14)
    assert s.freshness_seconds == 120 + 600
    # Field order and dict identity do not matter; every resolved value does.
    assert s.fingerprint() == c.resolve_settings(copy.deepcopy(pa_object(prediction={}))).fingerprint()
    assert len(s.fingerprint()) == 64


@pytest.mark.parametrize("change", [
    {"sampleIntervalSeconds": 60}, {"historyDays": 7}, {"horizonMinutes": 30}, {"settlementSeconds": 60},
    {"outcomeGraceSeconds": 300}, {"risk": {"miscoverageBps": 500}}, {"calibration": {"minWindows": 160}},
    {"trust": {"minShadowHours": 72}}, {"predictiveExtra": {"maxReplicas": 0}}, {"engine": "legacyLstm",
                                                                                "sampleIntervalSeconds": 600},
])
def test_every_setting_changes_the_fingerprint(change):
    assert c.resolve_settings(pa_object(prediction=change)).fingerprint() != settings_sha(pa_object(prediction={}))


def test_mode_and_legacy_lead_do_not_change_the_fingerprint():
    a = pa_object(prediction={"leadTimeMinutes": 15, "updateIntervalSeconds": 300})
    b = pa_object(prediction={"leadTimeMinutes": 20, "updateIntervalSeconds": 60})
    b["spec"]["mode"] = "Active"
    assert settings_sha(a) == settings_sha(b)


@pytest.mark.parametrize("interval,horizon,steps", [
    (60, 60, 60), (300, 60, 12), (600, 60, 6),       # 1/5/10-minute grids
    (480, 56, 7),                                    # 8 minutes divides a day but not an hour: horizon 56 min
    (120, 60, 30), (180, 60, 20), (240, 60, 15), (360, 60, 10),
])
def test_admitted_cadences(interval, horizon, steps):
    s = c.resolve_settings(pa_object(prediction={"sampleIntervalSeconds": interval, "horizonMinutes": horizon}))
    assert s.steps == steps and 86400 % interval == 0


@pytest.mark.parametrize("prediction", [
    {"sampleIntervalSeconds": 420},                  # 7 minutes: a one-day lag misses the grid
    {"sampleIntervalSeconds": 540},                  # 9 minutes
    {"sampleIntervalSeconds": 30}, {"sampleIntervalSeconds": 900}, {"sampleIntervalSeconds": "300"},
    {"sampleIntervalSeconds": True},
    {"sampleIntervalSeconds": 480, "horizonMinutes": 60},   # not a multiple of 8 minutes
    {"horizonMinutes": 120},                                 # beyond the first v2 profile
    {"horizonMinutes": 0},
    {"engine": "legacyLstm", "sampleIntervalSeconds": 300},  # the LSTM keeps its 600 s grid
    {"engine": "prophet"}, {"seasonality": "dailyWeekly"}, {"seasonality": "weekly"},
    {"historyDays": 0}, {"historyDays": 29}, {"settlementSeconds": -1}, {"outcomeGraceSeconds": 7200},
    {"risk": {"miscoverageBps": 50}}, {"risk": {"miscoverageBps": 3000}}, {"risk": {"miscoverageBps": 1000.0}},
    {"risk": {"confidence": 0.9}},                           # unknown keys are refused, not ignored
    {"history": [[1, 2]]}, {"query": "up"}, {"model": "lstm"},
    {"calibration": {"minMiscoverageBps": 50}},              # rank ceil(129 * 0.995) = 129 > 128: infeasible
    {"calibration": {"maxScores": 100}},                     # buffer smaller than the minimum count
    {"calibration": {"retentionDays": 5}},                   # 120 hourly cohort windows < 128
    {"risk": {"miscoverageBps": 2500}, "calibration": {"maxMiscoverageBps": 2000}},
    {"trust": {"minShadowHours": 24 * 8}},                   # longer than the 7-day retention
    {"predictiveExtra": {"maxReplicas": -1}}, {"enabled": False}, {"risk": "high"},
])
def test_refused_settings(prediction):
    with pytest.raises(ContractRefused):
        c.resolve_settings(pa_object(prediction=prediction))


def test_feasible_rank_boundary():
    # n = 128, a_min = 0.01: k = ceil(129 * 0.99) = 128 <= 128 (admitted); 0.0077 is the last feasible bps above 76.
    assert c._rank(128, 100) == 128
    c.resolve_settings(pa_object(prediction={"calibration": {"minMiscoverageBps": 78}, "risk": {"miscoverageBps": 100}}))
    with pytest.raises(ContractRefused):
        c.resolve_settings(pa_object(prediction={"calibration": {"minMiscoverageBps": 77},
                                                 "risk": {"miscoverageBps": 100}}))


# --- timestamps -------------------------------------------------------------------------------------------------

def test_timestamps_are_utc_z_only():
    assert c.format_ts(ORIGIN) == "2026-11-06T12:00:00Z" and c.parse_ts("2026-11-06T12:00:00Z") == ORIGIN
    assert c.format_ms(ORIGIN * 1000 + 7) == "2026-11-06T12:00:00.007Z"
    assert c.parse_ms("2026-11-06T12:00:00.007Z") == ORIGIN * 1000 + 7
    for bad in ("2026-11-06T14:00:00+02:00", "2026-11-06T12:00:00", "2026-11-06 12:00:00Z", "2026-13-01T00:00:00Z",
                ORIGIN, "2026-11-06T12:00:00.5Z"):
        with pytest.raises(ContractRefused):
            c.parse_ts(bad)
    with pytest.raises(ContractRefused):
        c.parse_ts("2026-11-06T12:00:00.000Z")       # a grid time carries no fraction
    with pytest.raises(ContractRefused):
        c.parse_ms("2026-11-06T12:00:00Z")


# --- request binding --------------------------------------------------------------------------------------------

def test_a_request_binds_to_the_live_autoscaler():
    reader = FakeReader()
    b = c.bind_request(request(), reader, now_ms=NOW_MS)
    assert (b.signal.query, b.signal.autoscaler_uid, b.signal.target_uid, b.signal.generation) == (
        QUERY, "pa-uid", "dep-uid", 3)
    assert b.settings_sha256 == settings_sha() and b.decision_origin == ORIGIN and b.lead_problem == ""
    assert b.lead == c.LeadSelection(1200, 420, 30, "configured", 0, 3)
    # One read of each object: the settings come from the very object identity was checked against.
    assert reader.paths == ["/apis/autoscaling.devkuban.com/v1alpha1/namespaces/shop/predictiveautoscalers/web-pa",
                            "/apis/apps/v1/namespaces/shop/deployments/web"]
    w = b.window
    assert (w.window_start, w.window_end) == (ORIGIN, ORIGIN + 1200)
    assert w.target_timestamps == (ORIGIN + 300, ORIGIN + 600, ORIGIN + 900, ORIGIN + 1200)
    # valid_until = min(t + Δ - A, t + L - D - C - A) = min(t + 270, t + 1200 - 420 - 300 - 30)
    assert w.valid_until(b.lead) == ORIGIN + 270
    assert w.actionable_at(NOW_MS, b.lead) and not w.actionable_at((ORIGIN + 270) * 1000, b.lead)


@pytest.mark.parametrize("req,reader,err", [
    (request(autoscaler_uid="other"), FakeReader(), ProvenanceRefused),          # wrong PA UID
    (request(target_uid="new-dep"), FakeReader(), ProvenanceRefused),            # wrong target UID
    (request(), FakeReader(dep={"metadata": {"uid": "new-dep"}}), ProvenanceRefused),
    (request(autoscaler_generation=2), FakeReader(), ProvenanceRefused),         # wrong generation
    (request(), FakeReader(pa=pa_object(generation=4)), ProvenanceRefused),
    (request(metric_query_sha256="0" * 64), FakeReader(), ProvenanceRefused),    # wrong query hash
    (request(application="api"), FakeReader(), ProvenanceRefused),               # another workload's name
    (request(settings_sha256="0" * 64), FakeReader(), ContractRefused),          # wrong settings hash
    (request(settings_sha256=settings_sha(pa_object(prediction={"sampleIntervalSeconds": 60}))), FakeReader(),
     ContractRefused),
    (request(), FakeReader(pa=pa_object(prediction={"risk": {"miscoverageBps": 2000}})), ContractRefused),
    (request(contract="requests-per-minute/v0"), FakeReader(), ContractRefused),  # wrong units
    (request(forecast_contract="window-max-rpm/v1"), FakeReader(), ContractRefused),
    (request(forecast_contract=None), FakeReader(), ContractRefused),
    (request(decision_origin=c.format_ts(ORIGIN + 60)), FakeReader(), ContractRefused),   # off the 300 s grid
    (request(decision_origin=c.format_ts(ORIGIN - 300)), FakeReader(), ContractRefused),  # a stale origin
    (request(decision_origin=c.format_ts(ORIGIN + 300)), FakeReader(), ContractRefused),  # a future origin
    (request(decision_origin="2026-11-06T13:00:00+01:00"), FakeReader(), ContractRefused),
    (request(history=[[ORIGIN, 1.0]]), FakeReader(), ContractRefused),           # no caller samples
    (request(query="up"), FakeReader(), ContractRefused),                        # no caller query
    (request(horizon_minutes=120), FakeReader(), ContractRefused),
    (request(miscoverage_bps=2500), FakeReader(), ContractRefused),
    (request(model="lstm"), FakeReader(), ContractRefused),
])
def test_requests_that_do_not_match_are_refused(req, reader, err):
    with pytest.raises(err):
        c.bind_request(req, reader, now_ms=NOW_MS)


def test_schema_refusals_happen_before_any_lookup():
    reader = FakeReader()
    with pytest.raises(ContractRefused):
        c.bind_request(request(history=[]), reader, now_ms=NOW_MS)
    assert reader.paths == []


def test_lookup_failures_stay_unavailable():
    with pytest.raises(LookupFailed):
        c.bind_request(request(), FakeReader(fail=LookupFailed("api down")), now_ms=NOW_MS)


@pytest.mark.parametrize("lead,why", [
    (None, "no lead"),
    ({**LEAD, "observedGeneration": 2}, "generation 2"),
    ({**LEAD, "seconds": 1000}, "multiple of the cadence"),
    ({**LEAD, "seconds": 4200}, "within the horizon"),
    ({**LEAD, "seconds": 600}, "does not cover"),                # 600 < 420 + 300 + 30
    ({**LEAD, "method": "guessed"}, "unknown"),
    ({**LEAD, "extra": 1}, "malformed"),
    ({**LEAD, "sampleCount": -1}, "non-negative"),
])
def test_an_invalid_lead_leaves_no_window(lead, why):
    b = c.bind_request(request(), FakeReader(pa=pa_object(lead=lead)), now_ms=NOW_MS)
    assert b.lead is None and why in b.lead_problem and b.window is None and b.evidence_key is None


def test_automatic_lead_rounds_up_with_origin_slack():
    # D 420 + C 300 + A 30 + Δ 300 = 1050 -> 1200 on a 300 s grid; on 60 s: 1080.
    assert c.automatic_lead_seconds(420, 300, 30, 300) == 1200
    assert c.automatic_lead_seconds(420, 60, 30, 60) == 600
    assert c.automatic_lead_seconds(0, 300, 0, 300) == 600


def test_evidence_key_resets_on_identity_and_policy_but_not_on_mode():
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    base = b.evidence_key
    sig = b.signal
    s = b.settings
    assert c.evidence_key(sig, s, 1200) == base
    assert c.evidence_key(dataclasses.replace(sig, generation=9), s, 1200) == base      # a mode-only generation
    assert c.evidence_key(dataclasses.replace(sig, target_uid="x"), s, 1200) != base
    assert c.evidence_key(dataclasses.replace(sig, autoscaler_uid="x"), s, 1200) != base
    assert c.evidence_key(dataclasses.replace(sig, sha256="1" * 64), s, 1200) != base
    assert c.evidence_key(sig, s, 900) != base                                            # lead change
    assert c.evidence_key(sig, dataclasses.replace(s, risk=c.RiskSettings(500)), 1200) != base
    assert c.evidence_key(sig, dataclasses.replace(s, predictive_extra=c.PredictiveExtraSettings(0, 0)), 1200) == base


# --- response ---------------------------------------------------------------------------------------------------

def make_response(bound, **over):
    w, lead = bound.window, bound.lead
    points = (1.5, 2.25, 0.0, 2.0)
    r = c.V2Response(
        autoscaler_namespace="shop", autoscaler_name="web-pa", autoscaler_uid="pa-uid", autoscaler_generation=3,
        target_namespace="shop", target_name="web", target_uid="dep-uid", metric_query_sha256=SHA, contract=CONTRACT,
        settings_sha256=bound.settings_sha256, forecast_contract=c.FORECAST_CONTRACT, unit=c.UNIT,
        issuance_id="iss-1", issued_at_ms=NOW_MS, input_end=ORIGIN - 300, input_received_through_ms=NOW_MS - 1000,
        fit_cutoff=ORIGIN - 3600, model_family="seasonal_naive", model_recipe="seasonal-naive/daily/v1",
        artifact_sha256=None, calibration_epoch=bound.evidence_key, checkpoint_sha256=None,
        decision_origin=ORIGIN, window_start=w.window_start, window_end=w.window_end,
        target_timestamps=w.target_timestamps, sample_interval_seconds=300, lead_seconds=lead.lead_seconds,
        startup_delay_seconds=lead.startup_delay_seconds, actuation_allowance_seconds=lead.actuation_allowance_seconds,
        valid_until=w.valid_until(lead), point_rps=points, base_window_max_rps=max(points), upper_window_max_rps=None,
        nominal_miscoverage_bps=1000, working_miscoverage_ratio=0.1, calibration_windows=0, prospective_windows=0,
        gate_coverage_ratio=None, missing_outcome_ratio=None, selected_candidate="seasonal_naive",
        trust_state="Collecting", reason="CalibrationInsufficient", predictive_eligible=False, evidence_cutoff=None)
    return dataclasses.replace(r, **over)


def test_a_shadow_response_round_trips_and_matches_its_request():
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    r = make_response(b)
    wire = r.to_wire()
    assert wire["issued_at"] == c.format_ms(NOW_MS) and wire["upper_window_max_rps"] is None
    assert wire["target_timestamps"][0] == "2026-11-06T12:05:00Z" and wire["unit"] == "requests/second"
    back = c.response_from_wire(wire)
    assert back == r
    c.check_response(back, b)


@pytest.mark.parametrize("over", [
    {"unit": "requests/minute"}, {"forecast_contract": "window-max-rpm/v1"}, {"contract": "requests-per-minute/v0"},
    {"point_rps": (1.0, 2.0, float("nan"), 0.5)}, {"point_rps": (1.0, -2.0, 0.0, 0.5)}, {"point_rps": (1.0, 2.0)},
    {"base_window_max_rps": 2.0},                                         # not the max of the points
    {"target_timestamps": (ORIGIN + 300, ORIGIN + 600, ORIGIN + 900, ORIGIN + 1500)},
    {"window_end": ORIGIN + 1500}, {"valid_until": ORIGIN + 300}, {"decision_origin": ORIGIN + 60},
    {"upper_window_max_rps": float("inf")}, {"upper_window_max_rps": -1.0},
    {"predictive_eligible": True},                                        # no upper bound, not trusted
    {"predictive_eligible": True, "upper_window_max_rps": 3.0, "trust_state": "Shadow"},
    {"gate_coverage_ratio": 1.0},                                         # coverage from zero windows
    {"working_miscoverage_ratio": 1.5}, {"trust_state": "Ok"}, {"reason": "Fine"}, {"model_family": "prophet"},
    {"input_end": ORIGIN + 300}, {"fit_cutoff": ORIGIN}, {"input_received_through_ms": NOW_MS + 1},
    {"issued_at_ms": ORIGIN * 1000 - 1}, {"target_namespace": "other"}, {"autoscaler_generation": True},
])
def test_incoherent_responses_are_refused(over):
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    with pytest.raises(ContractRefused):
        c.check_response(make_response(b, **over))


@pytest.mark.parametrize("over", [
    {"autoscaler_uid": "other"}, {"autoscaler_generation": 4}, {"target_uid": "x"}, {"metric_query_sha256": "1" * 64},
    {"settings_sha256": "2" * 64}, {"autoscaler_name": "api-pa"}, {"target_name": "api"}, {"lead_seconds": 1500,
                                                                                            "window_end": ORIGIN + 1500},
])
def test_responses_for_another_request_are_refused(over):
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    r = make_response(b, **over)
    if "lead_seconds" in over:
        w = c.ForecastWindow(ORIGIN, 300, 1500)
        r = dataclasses.replace(r, target_timestamps=w.target_timestamps, point_rps=(1.0,) * 5,
                                base_window_max_rps=1.0, valid_until=w.valid_until(b.lead))
    with pytest.raises(ContractRefused):
        c.check_response(r, b)


ELIGIBLE = dict(predictive_eligible=True, upper_window_max_rps=3.0, trust_state="Trusted", reason="Ready",
                calibration_windows=128, prospective_windows=64, gate_coverage_ratio=0.9)


@pytest.mark.parametrize("over", [
    {"issued_at_ms": (ORIGIN + 300) * 1000},                       # issued at the first target: a hindcast
    {"issued_at_ms": (ORIGIN + 900) * 1000, "input_received_through_ms": (ORIGIN + 900) * 1000},
    {**ELIGIBLE, "issued_at_ms": (ORIGIN + 270) * 1000},             # eligible but issued at valid_until
    {**ELIGIBLE, "issued_at_ms": (ORIGIN + 280) * 1000},             # after valid_until (and inside now + A)
    {**ELIGIBLE, "issued_at_ms": (ORIGIN + 270) * 1000 + 1},
])
def test_issuance_timing_is_enforced(over):
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    with pytest.raises(ContractRefused):
        c.check_response(make_response(b, **over))


def test_a_late_shadow_response_is_coherent_but_never_eligible():
    # After valid_until but before the first target: a shadow issuance may still be recorded; it cannot be eligible.
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    c.check_response(make_response(b, issued_at_ms=(ORIGIN + 280) * 1000), b)


def test_the_calibration_epoch_must_be_the_requests_evidence_key():
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    for epoch in ("f" * 64, "", c.evidence_key(b.signal, b.settings, 900)):
        with pytest.raises(ContractRefused):
            c.check_response(make_response(b, calibration_epoch=epoch), b)


def test_an_eligible_response_is_coherent_only_when_trusted():
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    c.check_response(make_response(b, **ELIGIBLE), b)


def test_wire_decoding_is_strict():
    b = c.bind_request(request(), FakeReader(), now_ms=NOW_MS)
    wire = make_response(b).to_wire()
    for mutate in (lambda w: w.pop("valid_until"), lambda w: w.update(confidence=0.9),
                   lambda w: w.update(issued_at=c.format_ts(ORIGIN)), lambda w: w.update(point_rps="1,2")):
        bad = copy.deepcopy(wire)
        mutate(bad)
        with pytest.raises(ContractRefused):
            c.response_from_wire(bad)
