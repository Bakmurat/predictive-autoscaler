#!/usr/bin/env python3
"""Shared Go/Python fixtures for forecast contract v2 (window-max-rps/v1), generated from ml-engine/forecasting/contracts.py.

The operator re-implements the parts of the contract it must check itself (settings fingerprint, evidence key, lead,
window, strict response decoding, the request it sends). These fixtures are computed by the Python module and asserted
by the Go tests (controllers/forecast_v2_contract_test.go), so the two sides cannot drift apart unnoticed: every case
here is first checked against contracts.py while generating (a valid response must decode, an invalid one must be
refused), and `check` fails when the committed file differs from what contracts.py generates now.

  k8s-operator/hack/gen_v2_fixtures.py write   regenerate k8s-operator/controllers/testdata/v2-contract-fixtures.json
  k8s-operator/hack/gen_v2_fixtures.py check   fail when the committed fixtures are stale (run by scripts/verify.sh)
"""

import copy
import dataclasses
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "ml-engine"))

from api.identity import CONTRACT, ResolvedSignal  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting.contracts import ContractRefused  # noqa: E402

OUT = os.path.join(ROOT, "k8s-operator", "controllers", "testdata", "v2-contract-fixtures.json")
SHA = "b" * 64
ORIGIN = int(datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc).timestamp())

# spec.prediction objects, as the API server stores them (the CRD adds enabled/horizonMinutes/leadTimeMinutes/
# updateIntervalSeconds defaults whenever spec.prediction is present).
CRD_DEFAULTS = {"enabled": True, "horizonMinutes": 60, "leadTimeMinutes": 15, "updateIntervalSeconds": 300}
SETTINGS_CASES = [
    ("absent", None),
    ("crd-defaults", {}),
    ("statistical", {"engine": "statistical"}),
    ("one-minute-grid", {"engine": "statistical", "sampleIntervalSeconds": 60}),
    ("ten-minute-grid-50min", {"engine": "statistical", "sampleIntervalSeconds": 600, "horizonMinutes": 50}),
    ("eight-minute-grid-56min", {"engine": "statistical", "sampleIntervalSeconds": 480, "horizonMinutes": 56}),
    ("legacy-lstm", {"engine": "legacyLstm", "sampleIntervalSeconds": 600}),
    ("legacy-lstm-history-one-day", {"engine": "legacyLstm", "sampleIntervalSeconds": 600, "historyDays": 1}),
    ("everything", {"engine": "statistical", "sampleIntervalSeconds": 120, "seasonality": "daily", "historyDays": 21,
                    "settlementSeconds": 90, "outcomeGraceSeconds": 900, "horizonMinutes": 40,
                    "risk": {"miscoverageBps": 500},
                    "calibration": {"minWindows": 200, "maxScores": 600, "retentionDays": 10, "etaBps": 25,
                                    "minMiscoverageBps": 50, "maxMiscoverageBps": 2000},
                    "trust": {"minShadowHours": 72, "minProspectiveWindows": 80, "coverageToleranceBps": 300,
                              "worstDayMinOutcomes": 30, "minAvailabilityBps": 9000, "acuteConsecutiveMisses": 4,
                              "acuteRecentWindows": 40, "repromotionHours": 12, "repromotionWindows": 20,
                              "challengerMinPairs": 70, "challengerMinHours": 60, "challengerLossRatioBps": 9000,
                              "challengerExitLossRatioBps": 10500,
                              "challengerCostRatioBps": 10100, "maxFitAgeHours": 6},
                    "predictiveExtra": {"maxReplicas": 0, "maxPercent": 25}}),
    ("naive-only", {"engine": "statistical", "families": ["seasonalNaive"]}),
    # the validated profile (trust-repair protocol, amendment 2): 60 s grid, a 600 s minimum lead, seasonal-naive only
    ("min-lead-600", {"engine": "statistical", "sampleIntervalSeconds": 60, "families": ["seasonalNaive"],
                      "minLeadSeconds": 600}),
    ("min-lead-rounded", {"engine": "statistical", "minLeadSeconds": 541}),
    ("min-lead-equals-horizon", {"engine": "statistical", "sampleIntervalSeconds": 60, "horizonMinutes": 10,
                                 "minLeadSeconds": 600}),
    ("families-reordered", {"engine": "statistical", "families": ["statistical", "seasonalNaive"]}),
    # refused
    ("disabled", {"engine": "statistical", "enabled": False}),
    ("families-without-naive", {"engine": "statistical", "families": ["statistical"]}),
    ("families-duplicate", {"engine": "statistical", "families": ["seasonalNaive", "seasonalNaive"]}),
    ("families-unknown", {"engine": "statistical", "families": ["seasonalNaive", "lstm"]}),
    ("families-empty", {"engine": "statistical", "families": []}),
    ("seven-minute-grid", {"engine": "statistical", "sampleIntervalSeconds": 420}),
    ("horizon-too-long", {"engine": "statistical", "horizonMinutes": 70}),
    ("horizon-not-multiple", {"engine": "statistical", "horizonMinutes": 7}),
    ("lstm-off-grid", {"engine": "legacyLstm", "sampleIntervalSeconds": 300}),
    ("risk-too-high", {"engine": "statistical", "risk": {"miscoverageBps": 3000}}),
    ("rank-infeasible", {"engine": "statistical", "calibration": {"minWindows": 16, "maxScores": 16,
                                                                  "minMiscoverageBps": 100}}),
    ("retention-too-short", {"engine": "statistical", "calibration": {"retentionDays": 1}}),
    ("scores-below-min", {"engine": "statistical", "calibration": {"minWindows": 300, "maxScores": 200}}),
    ("nominal-outside-adaptive", {"engine": "statistical", "risk": {"miscoverageBps": 2400},
                                  "calibration": {"maxMiscoverageBps": 2000}}),
    ("adaptive-bounds-inverted", {"engine": "statistical", "risk": {"miscoverageBps": 1000},
                                  "calibration": {"minMiscoverageBps": 1000, "maxMiscoverageBps": 1000}}),
    ("acute-exceeds-buffer", {"engine": "statistical", "trust": {"acuteRecentWindows": 600}}),
    ("shadow-exceeds-retention", {"engine": "statistical", "trust": {"minShadowHours": 200}}),
    ("exit-below-entry", {"engine": "statistical", "trust": {"challengerLossRatioBps": 9500,
                                                             "challengerExitLossRatioBps": 9000}}),
    ("history-too-long", {"engine": "statistical", "historyDays": 29}),
    ("settlement-too-long", {"engine": "statistical", "settlementSeconds": 901}),
    ("history-one-day", {"engine": "statistical", "historyDays": 1}),
    ("min-lead-out-of-range", {"engine": "statistical", "minLeadSeconds": 3601}),
    ("min-lead-rounds-beyond-horizon", {"engine": "statistical", "sampleIntervalSeconds": 60, "horizonMinutes": 10,
                                        "minLeadSeconds": 601}),
    ("prospective-below-acute", {"engine": "statistical", "trust": {"minProspectiveWindows": 32}}),
]

# The identity of the calibration method BEFORE normalized-window-score/two-part-scale/v1 (the additive score Y - b),
# frozen from the fixtures of commit 44f65cf: the fingerprint of the default statistical settings and the evidence key
# of the fixture binding. An operator or a forecaster of that revision presents these; the current side must refuse.
LEGACY_ADDITIVE = {"settings_sha256": "a57283c74f3167299d2dbf72eb0193aa1157a1b79691570e68ebed5561848bcf",
                   "evidence_key": "58daefce56ef9b2b7244d2d565538b5edc5ce0bf78d09cc9a51c882cc4478c00"}
_ADDED_CALIBRATION_KEYS = ("method", "scale_bins", "scale_floor_bps", "scale_min_season_bps")
# The identity of the calibration change BEFORE trust policy budgeted-coverage-tests/v1 (the last 32 gate windows at
# least 85 % covered, the worst-day rule on partial days), frozen from the fixtures of that
# change: an operator or a forecaster one revision back presents these.
PRE_TRUST_POLICY = {"settings_sha256": "c8e281dc189bde4582db5085810598484abc9789a1c32a29b9f0388342936b9c",
                    "evidence_key": "e147997ab6f5181c96583aae60d6c9d477368a0a81ceb000ed450b59401905ad"}
_ADDED_TRUST_KEYS = ("policy", "rolling_alarm_ppm", "consecutive_alarm_ppm", "day_alarm_ppm", "seven_day_alarm_ppm",
                     "miss_dependence_bps")
# The identity of trust policy budgeted-coverage-tests/v1 (three budgeted rules for independent misses, the seven-day
# gate an unbudgeted ratio, no minimum lead), frozen from the fixtures of that policy (commit a774a68).
TRUST_POLICY_V1 = {"settings_sha256": "294957d5a0d715cf320838f6ef3a6b60525564b6370581581597759ba4592215",
                   "evidence_key": "41563d86004f1ebdf435ff704b9cb6bd6ad541dbfd78eac9b657c4ae910e1fea"}
_V1_TRUST = {"policy": "budgeted-coverage-tests/v1", "rolling_alarm_ppm": 35_000}
_ADDED_BY_V2 = ("seven_day_alarm_ppm", "miss_dependence_bps")
_EARLIER_TRUST_DEFAULTS = {"acute_recent_windows": 32}

SIGNAL = ResolvedSignal(namespace="shop", name="web", metric="requests", query="q", sha256=SHA, contract=CONTRACT,
                        autoscaler_uid="pa-uid", target_uid="dep-uid", generation=3)


def pa_with(prediction, lead=None, generation=3):
    pa = {"metadata": {"uid": "pa-uid", "generation": generation}, "spec": {}}
    if prediction is not None:
        pa["spec"]["prediction"] = {**CRD_DEFAULTS, **prediction}
    if lead is not None:
        pa["status"] = {"prediction": {"lead": lead}}
    return pa


def settings_cases():
    out = []
    for name, pred in SETTINGS_CASES:
        pa = pa_with(pred)
        case = {"name": name, "prediction": pa["spec"].get("prediction")}
        try:
            s = c.resolve_settings(pa)
        except ContractRefused as e:
            case["refused"] = str(e)
        else:
            case.update(canonical=c.canonical_json(s.canonical()).decode("ascii"), fingerprint=s.fingerprint(),
                        steps=s.steps)
        out.append(case)
    return out


def evidence_cases():
    out = []
    for name, pred in SETTINGS_CASES[:8]:
        s = c.resolve_settings(pa_with(pred))
        for lead in sorted({s.sample_interval_seconds * 2, s.horizon_seconds}):
            out.append({"settings_case": name, "lead_seconds": lead,
                        "signal": {"autoscaler_uid": SIGNAL.autoscaler_uid, "target_uid": SIGNAL.target_uid,
                                   "query_sha256": SIGNAL.sha256},
                        "evidence_key": c.evidence_key(SIGNAL, s, lead)})
    return out


def legacy_identity():
    """The frozen pre-change identity, re-derived: the current canonical settings and evidence-key body WITHOUT the
    keys this method added hash to exactly the frozen values, so nothing else moved the identity."""
    s = c.resolve_settings(pa_with({"engine": "statistical"}))
    # Trust policy v1: today's identity without what v2 added (two trust keys, the minimum lead), under v1's name
    # and rolling budget.
    canon = s.canonical()
    for k in _ADDED_BY_V2:
        canon["settings"]["trust"].pop(k)
    canon["settings"]["trust"].update(_V1_TRUST)
    canon["settings"].pop("min_lead_seconds")
    v1_body = {k: v for k, v in _evidence_body(s).items() if k != "min_lead_seconds"}
    v1_body["trust_policy"] = _V1_TRUST["policy"]
    v1 = {"settings_sha256": c.sha256_hex(c.canonical_json(canon)),
          "evidence_key": c.sha256_hex(c.canonical_json(v1_body))}
    if v1 != TRUST_POLICY_V1:
        raise SystemExit(f"the identity of trust policy v1 no longer re-derives: {v1}")
    if s.fingerprint() == v1["settings_sha256"] or c.evidence_key(SIGNAL, s, 1200) == v1["evidence_key"]:
        raise SystemExit("the trust policy revision does not change the settings fingerprint and the evidence key")
    canon = s.canonical()
    for k in _ADDED_TRUST_KEYS:
        canon["settings"]["trust"].pop(k)
    canon["settings"]["trust"].update(_EARLIER_TRUST_DEFAULTS)
    canon["settings"].pop("min_lead_seconds")
    pre = {"settings_sha256": c.sha256_hex(c.canonical_json(canon)),
           "evidence_key": c.sha256_hex(c.canonical_json({k: v for k, v in _evidence_body(s).items()
                                                          if k not in ("trust_policy", "min_lead_seconds")}))}
    if pre != PRE_TRUST_POLICY:
        raise SystemExit(f"the pre-trust-policy identity no longer re-derives: {pre}")
    for k in _ADDED_CALIBRATION_KEYS:
        canon["settings"]["calibration"].pop(k)
    cal = dict(canon["settings"]["calibration"])
    body = {"schema": c.EVIDENCE_SCHEMA, "autoscaler_uid": SIGNAL.autoscaler_uid, "target_uid": SIGNAL.target_uid,
            "query_sha256": SIGNAL.sha256, "signal_contract": SIGNAL.contract,
            "forecast_contract": c.FORECAST_CONTRACT, "interval_seconds": s.sample_interval_seconds,
            "lead_seconds": 1200, "engine": s.engine, "seasonality": s.seasonality, "history_days": s.history_days,
            "settlement_seconds": s.settlement_seconds, "outcome_grace_seconds": s.outcome_grace_seconds,
            "risk": dataclasses.asdict(s.risk), "calibration": cal}
    got = {"settings_sha256": c.sha256_hex(c.canonical_json(canon)),
           "evidence_key": c.sha256_hex(c.canonical_json(body))}
    if got != LEGACY_ADDITIVE:
        raise SystemExit(f"the legacy additive identity no longer re-derives: {got}")
    if s.fingerprint() == got["settings_sha256"] or c.evidence_key(SIGNAL, s, 1200) == got["evidence_key"]:
        raise SystemExit("the calibration method does not change the settings fingerprint and the evidence key")
    return {**LEGACY_ADDITIVE, "method": "additive-window-score (revision 1, unnamed)"}


def _evidence_body(s):
    """contracts.evidence_key's body for the fixture signal at lead 1200 s."""
    return {"schema": c.EVIDENCE_SCHEMA, "autoscaler_uid": SIGNAL.autoscaler_uid, "target_uid": SIGNAL.target_uid,
            "query_sha256": SIGNAL.sha256, "signal_contract": SIGNAL.contract,
            "forecast_contract": c.FORECAST_CONTRACT, "interval_seconds": s.sample_interval_seconds,
            "lead_seconds": 1200, "min_lead_seconds": s.min_lead_seconds, "engine": s.engine,
            "seasonality": s.seasonality, "history_days": s.history_days,
            "settlement_seconds": s.settlement_seconds, "outcome_grace_seconds": s.outcome_grace_seconds,
            "risk": dataclasses.asdict(s.risk), "calibration": dataclasses.asdict(s.calibration),
            "trust_policy": s.trust.policy}


def lead_cases():
    auto = [{"startup": d, "decision_interval": dt, "allowance": a, "interval": dt, "min_lead": m,
             "lead": c.automatic_lead_seconds(d, dt, a, dt, m)}
            for d in (0, 1, 59, 60, 420, 899) for dt in (60, 300, 600) for a in (0, 15, 30) for m in (0, 541, 600, 3600)]
    settings = c.resolve_settings(pa_with({"engine": "statistical"}))
    checks = []
    base = {"seconds": 1200, "startupDelaySeconds": 420, "actuationAllowanceSeconds": 30, "method": "measured",
            "sampleCount": 25, "observedGeneration": 3}
    variants = [("valid", {}), ("other-generation", {"observedGeneration": 2}),
                ("off-grid", {"seconds": 1250}), ("beyond-horizon", {"seconds": 3900}),
                ("does-not-cover", {"seconds": 600}), ("unknown-method", {"method": "guessed"}),
                ("negative", {"sampleCount": -1}), ("configured", {"method": "configured", "sampleCount": 0}),
                ("exactly-covers", {"seconds": 900, "startupDelaySeconds": 570})]
    for name, upd in variants:
        lead = {**base, **upd}
        sel, why = c.resolve_lead(pa_with({"engine": "statistical"}, lead), settings, 3)
        checks.append({"name": name, "settings": "statistical", "lead": lead, "valid": sel is not None, "why": why})
    # The validated profile's settings: a published lead below the configured minimum is refused.
    profile = dict(next(p for n, p in SETTINGS_CASES if n == "min-lead-600"))
    profile_settings = c.resolve_settings(pa_with(profile))
    short = {"seconds": 180, "startupDelaySeconds": 30, "actuationAllowanceSeconds": 15, "method": "measured",
             "sampleCount": 25, "observedGeneration": 3}
    for name, upd in (("below-minimum", {}), ("at-minimum", {"seconds": 600}), ("above-minimum", {"seconds": 660})):
        lead = {**short, **upd}
        sel, why = c.resolve_lead(pa_with(profile, lead), profile_settings, 3)
        checks.append({"name": name, "settings": "min-lead-600", "lead": lead, "valid": sel is not None, "why": why})
    missing = dict(base)
    missing.pop("sampleCount")
    sel, why = c.resolve_lead(pa_with({"engine": "statistical"}, missing), settings, 3)
    checks.append({"name": "missing-key", "settings": "statistical", "lead": missing, "valid": sel is not None,
                   "why": why})
    windows = []
    for dt, lead_s, d, a in ((300, 1200, 420, 30), (60, 600, 120, 15), (600, 3600, 900, 0), (300, 900, 570, 30)):
        sel = c.LeadSelection(lead_s, d, a, "configured", 0, 3)
        w = c.ForecastWindow(ORIGIN, dt, lead_s)
        windows.append({"origin": c.format_ts(ORIGIN), "interval": dt, "lead": lead_s, "startup": d, "allowance": a,
                        "targets": [c.format_ts(t) for t in w.target_timestamps],
                        "valid_until": c.format_ts(w.valid_until(sel))})
    return {"automatic": auto, "resolve": checks, "windows": windows}


def binding_and_response(eligible=True):
    settings = c.resolve_settings(pa_with({"engine": "statistical"}))
    lead = c.LeadSelection(1200, 420, 30, "measured", 25, 3)
    bound = c.BoundRequest(autoscaler_namespace="shop", autoscaler_name="web-pa", signal=SIGNAL, settings=settings,
                           settings_sha256=settings.fingerprint(), decision_origin=ORIGIN, lead=lead, lead_problem="")
    w = bound.window
    r = c.V2Response(
        autoscaler_namespace="shop", autoscaler_name="web-pa", autoscaler_uid="pa-uid", autoscaler_generation=3,
        target_namespace="shop", target_name="web", target_uid="dep-uid", metric_query_sha256=SHA, contract=CONTRACT,
        settings_sha256=settings.fingerprint(), forecast_contract=c.FORECAST_CONTRACT, unit=c.UNIT,
        issuance_id="iss-0001", issued_at_ms=ORIGIN * 1000 + 4321, input_end=ORIGIN - 300,
        input_received_through_ms=ORIGIN * 1000 + 1000, fit_cutoff=ORIGIN - 3600, model_family="seasonal_naive",
        model_recipe="seasonal-naive/daily@1", artifact_sha256="c" * 64, calibration_epoch=bound.evidence_key,
        checkpoint_sha256="d" * 64, decision_origin=ORIGIN, window_start=w.window_start, window_end=w.window_end,
        target_timestamps=w.target_timestamps, sample_interval_seconds=300, lead_seconds=1200,
        startup_delay_seconds=420, actuation_allowance_seconds=30, valid_until=w.valid_until(lead),
        point_rps=(10.5, 12.25, 11.0, 9.75), base_window_max_rps=12.25,
        upper_window_max_rps=15.5 if eligible else None, nominal_miscoverage_bps=1000,
        working_miscoverage_ratio=0.0975, calibration_windows=140, prospective_windows=70, gate_coverage_ratio=0.9,
        missing_outcome_ratio=0.01, selected_candidate="seasonal_naive",
        trust_state="Trusted" if eligible else "Shadow", reason="Ready" if eligible else "ShadowInsufficient",
        predictive_eligible=eligible, evidence_cutoff=ORIGIN - 600)
    c.check_response(r, bound)
    binding = {"autoscaler_namespace": "shop", "autoscaler_name": "web-pa", "autoscaler_uid": "pa-uid",
               "autoscaler_generation": 3, "target_namespace": "shop", "target_name": "web", "target_uid": "dep-uid",
               "metric_query_sha256": SHA, "settings_sha256": settings.fingerprint(), "decision_origin": ORIGIN,
               "sample_interval_seconds": 300, "nominal_miscoverage_bps": 1000,
               "lead": {"seconds": 1200, "startupDelaySeconds": 420, "actuationAllowanceSeconds": 30,
                        "method": "measured", "sampleCount": 25, "observedGeneration": 3},
               "evidence_key": bound.evidence_key}
    return bound, r, binding


def _mutate(wire, **upd):
    w = copy.deepcopy(wire)
    for k, v in upd.items():
        if v is _DROP:
            w.pop(k)
        else:
            w[k] = v
    return w


_DROP = object()


def response_cases():
    bound, r, binding = binding_and_response(True)
    _, shadow, _ = binding_and_response(False)
    wire = r.to_wire()
    valid = [{"name": "eligible", "wire": wire}, {"name": "shadow", "wire": shadow.to_wire()},
             {"name": "zero-forecast", "wire": _mutate(wire, point_rps=[0, 0, 0, 0], base_window_max_rps=0,
                                                         upper_window_max_rps=0.0)},
             {"name": "integer-rates", "wire": _mutate(wire, point_rps=[10, 12, 11, 9], base_window_max_rps=12,
                                                         upper_window_max_rps=15)}]
    t = wire["target_timestamps"]
    decode_invalid = {
        "extra-field": _mutate(wire, confidence=0.9),
        "missing-field": _mutate(wire, valid_until=_DROP),
        "wrong-contract": _mutate(wire, forecast_contract="window-max-rps/v2"),
        "rpm-unit": _mutate(wire, unit="requests/minute"),
        "negative-rate": _mutate(wire, point_rps=[10.5, -1, 11.0, 9.75]),
        "base-not-max": _mutate(wire, base_window_max_rps=11.0),
        "short-targets": _mutate(wire, target_timestamps=t[:3], point_rps=[10.5, 12.25, 11.0]),
        "slid-window": _mutate(wire, target_timestamps=t[1:] + [c.format_ts(c.parse_ts(t[-1]) + 300)]),
        "valid-until-moved": _mutate(wire, valid_until=c.format_ts(c.parse_ts(wire["valid_until"]) + 60)),
        "origin-off-grid": _mutate(wire, decision_origin=c.format_ts(ORIGIN + 60)),
        "grid-time-fraction": _mutate(wire, decision_origin=c.format_ts(ORIGIN)[:-1] + ".000Z"),
        "issued-without-ms": _mutate(wire, issued_at=c.format_ts(ORIGIN + 4)),
        "issued-at-first-target": _mutate(wire, issued_at=c.format_ms(c.parse_ts(t[0]) * 1000)),
        "eligible-without-bound": _mutate(wire, upper_window_max_rps=None),
        "eligible-not-trusted": _mutate(wire, trust_state="Shadow"),
        "eligible-not-ready": _mutate(wire, reason="CoverageLow"),
        "eligible-after-deadline": _mutate(wire, issued_at=c.format_ms(c.parse_ts(wire["valid_until"]) * 1000)),
        "unknown-state": _mutate(wire, trust_state="Confident"),
        "unknown-family": _mutate(wire, model_family="prophet"),
        "coverage-above-one": _mutate(wire, gate_coverage_ratio=1.5),
        "input-after-origin": _mutate(wire, input_end=c.format_ts(ORIGIN + 300)),
        "receipt-after-issue": _mutate(wire, input_received_through=c.format_ms(ORIGIN * 1000 + 9000)),
        "cross-namespace-target": _mutate(wire, target_namespace="other"),
        "eligible-string": _mutate(wire, predictive_eligible="true"),
        "bad-settings-sha": _mutate(wire, settings_sha256="X" * 64),
        "lead-off-grid": _mutate(wire, lead_seconds=1250),
        "boolean-generation": _mutate(wire, autoscaler_generation=True),
    }
    invalid = []
    for name, w in decode_invalid.items():
        try:
            c.response_from_wire(json.loads(json.dumps(w)))
        except ContractRefused as e:
            invalid.append({"name": name, "wire": w, "stage": "decode", "why": str(e)})
        else:
            raise SystemExit(f"fixture {name}: contracts.py accepted a response meant to be invalid")
    bound_invalid = {
        "other-uid": _mutate(wire, autoscaler_uid="pa-other"),
        "other-generation": _mutate(wire, autoscaler_generation=4),
        "other-target-uid": _mutate(wire, target_uid="dep-other"),
        "other-query": _mutate(wire, metric_query_sha256="e" * 64),
        "other-settings": _mutate(wire, settings_sha256="f" * 64),
        "other-risk": _mutate(wire, nominal_miscoverage_bps=500),
        "other-epoch": _mutate(wire, calibration_epoch="0" * 64),
        # A forecaster of the additive method answering this operator: its fingerprint, or its evidence epoch.
        "legacy-additive-settings": _mutate(wire, settings_sha256=LEGACY_ADDITIVE["settings_sha256"]),
        "legacy-additive-epoch": _mutate(wire, calibration_epoch=LEGACY_ADDITIVE["evidence_key"]),
        # A forecaster of the calibration change without this trust policy.
        "pre-trust-policy-settings": _mutate(wire, settings_sha256=PRE_TRUST_POLICY["settings_sha256"]),
        "pre-trust-policy-epoch": _mutate(wire, calibration_epoch=PRE_TRUST_POLICY["evidence_key"]),
        # An operator or forecaster of trust policy budgeted-coverage-tests/v1.
        "trust-policy-v1-settings": _mutate(wire, settings_sha256=TRUST_POLICY_V1["settings_sha256"]),
        "trust-policy-v1-epoch": _mutate(wire, calibration_epoch=TRUST_POLICY_V1["evidence_key"]),
    }
    # A different lead and origin must be internally coherent to reach the binding check.
    shifted = dataclasses.replace(r, decision_origin=ORIGIN + 300, window_start=ORIGIN + 300,
                                  window_end=ORIGIN + 1500,
                                  target_timestamps=tuple(ORIGIN + 300 + 300 * k for k in range(1, 5)),
                                  valid_until=ORIGIN + 570, issued_at_ms=(ORIGIN + 300) * 1000 + 10)
    bound_invalid["other-origin"] = shifted.to_wire()
    other_lead = dataclasses.replace(r, startup_delay_seconds=300, valid_until=ORIGIN + 270)
    bound_invalid["other-startup"] = other_lead.to_wire()
    for name, w in bound_invalid.items():
        dec = c.response_from_wire(json.loads(json.dumps(w)))
        try:
            c.check_response(dec, bound)
        except ContractRefused as e:
            invalid.append({"name": name, "wire": w, "stage": "bound", "why": str(e)})
        else:
            raise SystemExit(f"fixture {name}: contracts.py accepted a response bound to another request")
    for v in valid:
        c.check_response(c.response_from_wire(json.loads(json.dumps(v["wire"]))), bound)
    request = {"autoscaler_namespace": "shop", "autoscaler_name": "web-pa", "autoscaler_uid": "pa-uid",
               "autoscaler_generation": 3, "target_uid": "dep-uid", "metric_query_sha256": SHA, "contract": CONTRACT,
               "application": "web", "namespace": "shop", "metric_type": "requests",
               "forecast_contract": c.FORECAST_CONTRACT, "decision_origin": c.format_ts(ORIGIN),
               "settings_sha256": binding["settings_sha256"]}
    parsed = c.parse_request(request)
    assert parsed.decision_origin == ORIGIN and parsed.settings_sha256 == binding["settings_sha256"]
    return {"binding": binding, "request": request, "valid": valid, "invalid": invalid}


def generate() -> str:
    doc = {"generated_by": "k8s-operator/hack/gen_v2_fixtures.py from ml-engine/forecasting/contracts.py",
           "forecast_contract": c.FORECAST_CONTRACT, "settings_schema": c.SETTINGS_SCHEMA,
           "evidence_schema": c.EVIDENCE_SCHEMA, "unit": c.UNIT, "allowed_intervals": list(c.ALLOWED_INTERVALS),
           "calibration_method": c.CALIBRATION_POLICY_REVISION, "legacy_additive": legacy_identity(),
           "trust_policy": c.TRUST_POLICY_REVISION, "pre_trust_policy": dict(PRE_TRUST_POLICY),
           "trust_policy_v1": dict(TRUST_POLICY_V1),
           "phases": list(c.PHASES), "reasons": list(c.REASONS), "model_families": list(c.MODEL_FAMILIES),
           "settings": settings_cases(), "evidence": evidence_cases(), "lead": lead_cases(),
           "responses": response_cases()}
    return json.dumps(doc, indent=1, sort_keys=True) + "\n"


def main(argv):
    mode = argv[1] if len(argv) == 2 else ""
    if mode not in ("write", "check"):
        print(__doc__, file=sys.stderr)
        return 2
    text = generate()
    if mode == "write":
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        with open(OUT, "w") as fh:
            fh.write(text)
        print(f"gen_v2_fixtures: wrote {os.path.relpath(OUT, ROOT)}")
        return 0
    try:
        with open(OUT) as fh:
            current = fh.read()
    except OSError:
        current = ""
    if current != text:
        print(f"gen_v2_fixtures: {os.path.relpath(OUT, ROOT)} is stale: run k8s-operator/hack/gen_v2_fixtures.py write",
              file=sys.stderr)
        return 1
    print("gen_v2_fixtures: fixtures are current")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
