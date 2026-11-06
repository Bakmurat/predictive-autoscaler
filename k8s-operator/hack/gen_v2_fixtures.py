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
    # refused
    ("disabled", {"engine": "statistical", "enabled": False}),
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
]

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


def lead_cases():
    auto = [{"startup": d, "decision_interval": dt, "allowance": a, "interval": dt,
             "lead": c.automatic_lead_seconds(d, dt, a, dt)}
            for d in (0, 1, 59, 60, 420, 899) for dt in (60, 300, 600) for a in (0, 15, 30)]
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
        checks.append({"name": name, "lead": lead, "valid": sel is not None, "why": why})
    missing = dict(base)
    missing.pop("sampleCount")
    sel, why = c.resolve_lead(pa_with({"engine": "statistical"}, missing), settings, 3)
    checks.append({"name": "missing-key", "lead": missing, "valid": sel is not None, "why": why})
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
