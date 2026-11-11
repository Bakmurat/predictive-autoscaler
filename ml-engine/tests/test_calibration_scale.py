"""Calibration method normalized-window-score/two-part-scale/v1 (trust-repair protocol, T1): the two-part scale and its
fit from strictly prior data, the scale recorded with each issuance, the method as part of the bound identity (old
buffers, old trust and an old peer are refused, never reinterpreted), and the acceptance simulation against the real
service."""

import dataclasses
import json
import math
import os
import random
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests.test_statistical_service as svc_helpers  # noqa: E402
from evaluation import calibration_acceptance as acceptance  # noqa: E402
from forecasting import calibration as cal  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting.calibration import CalibrationError, CalibrationPolicy, CalibrationRecord, Scale  # noqa: E402
from forecasting.clock import ManualClock  # noqa: E402
from forecasting.service import ForecastService, ServiceConfig, ServiceError  # noqa: E402

L = 600
EPOCH = "e" * 64
DAY = 86400
ADDED_KEYS = ("method", "scale_bins", "scale_floor_bps", "scale_min_season_bps")
# The default statistical settings and the fixture binding's evidence key under the additive method, frozen from
# k8s-operator/controllers/testdata/v2-contract-fixtures.json at commit 44f65cf (the Go tests use the same constants).
LEGACY_FINGERPRINT = "a57283c74f3167299d2dbf72eb0193aa1157a1b79691570e68ebed5561848bcf"


def policy(**kw):
    base = dict(lead_seconds=L, nominal_bps=1000, eta_bps=50, min_bps=100, max_bps=2500, min_windows=4,
                max_scores=50, retention_seconds=7 * DAY)
    base.update(kw)
    return CalibrationPolicy(**base)


def rec(seq, y, base, scale, upper=None):
    return CalibrationRecord(seq, EPOCH, "m", "r", f"iss-{seq}", seq * L, seq * L + L, base, upper, "scored", y, scale)


# --- the score and the bound ----------------------------------------------------------------------------------------

def test_the_score_and_the_bound_use_the_two_part_scale():
    scale = Scale(s_min=0.5, a=1.0, beta=0.1)
    assert (scale.at(0.0), scale.at(10.0), scale.at(40.0)) == (1.0, 2.0, 5.0)
    assert Scale(s_min=3.0, a=1.0, beta=0.1).at(10.0) == 3.0                    # the floor
    p = policy()
    s = cal.initial(p, EPOCH, "m", "r")
    # residuals 4, -2, 6, 1, 10 at levels 10, 10, 40, 0, 40: sigma 2, 2, 5, 1, 5 -> scores 2, -1, 1.2, 1, 2
    for i, (y, b) in enumerate(((14.0, 10.0), (8.0, 10.0), (46.0, 40.0), (1.0, 0.0), (50.0, 40.0)), 1):
        s = cal.apply(s, rec(i, y, b, scale), p)
    assert [x[1] for x in s["scores"]] == [2.0, -1.0, 1.2, 1.0, 2.0]
    p25 = policy(nominal_bps=2500)                                             # k = ceil(6 x 0.75) = 5: q = 2.0
    s25 = dict(s, a_e8=25_000_000)
    u, info = cal.upper_bound(s25, p25, 20.0, 100 * L, scale)                   # sigma(20) = 3: U = 20 + 2 x 3
    assert (u, info["quantile"], info["sigma_rps"], info["scale"]) == (26.0, 2.0, 3.0, scale.to_wire())
    assert cal.upper_bound(s25, p25, 0.0, 100 * L, scale)[0] == 2.0             # sigma(0) = 1
    neg = cal.initial(p25, EPOCH, "m", "r")
    for i in range(1, 6):
        neg = cal.apply(neg, rec(i, 0.0, 10.0, scale), p25)                    # scores -5: U = max(0, 1 - 5 x 1.1)
    assert cal.upper_bound(neg, p25, 1.0, 100 * L, scale)[0] == 0.0
    for bad in (dict(s_min=0.0, a=1.0, beta=0.0), dict(s_min=1.0, a=-0.1, beta=0.0), dict(s_min=1.0, a=0.0, beta=-1.0),
                dict(s_min=math.inf, a=0.0, beta=0.0), dict(s_min=1.0, a=math.nan, beta=0.0)):
        with pytest.raises(CalibrationError):
            Scale(**bad)
    with pytest.raises(CalibrationError):
        Scale.from_wire({"s_min": 1.0, "a": 1.0})
    assert Scale.from_wire(scale.to_wire()) == scale


def test_a_score_keeps_the_scale_recorded_at_its_issuance():
    """A later, different scale never reinterprets a window that was issued under an earlier one: the buffer holds
    the score computed with the issuance's own scale, and the bound multiplies by the scale in force now."""
    p = policy(nominal_bps=2500)
    early, late = Scale(1.0, 2.0, 0.0), Scale(1.0, 4.0, 0.0)
    s = cal.initial(p, EPOCH, "m", "r")
    for i in range(1, 6):
        s = cal.apply(s, rec(i, 16.0, 10.0, early), p)                         # residual 6 at sigma 2: score 3
    assert [x[1] for x in s["scores"]] == [3.0] * 5
    s = cal.apply(s, rec(6, 16.0, 10.0, late), p)                              # the same residual at sigma 4: 1.5
    assert [x[1] for x in s["scores"]] == [3.0] * 5 + [1.5]                    # the five early scores did not move
    assert cal.upper_bound(s, p, 10.0, 100 * L, late)[0] == 10.0 + 3.0 * 4.0


def test_the_unit_scale_is_the_additive_method_number_for_number():
    """sigma = 1 gives exactly the earlier method (score Y - b, U = max(0, b + q)), recomputed here from its
    definition: the comparison column of the acceptance table is that method."""
    rng = random.Random(7)
    p = policy(min_windows=16, max_scores=64)
    s = cal.initial(p, EPOCH, "m", "r")
    scores, a_e8, pending = [], 10_000_000, None
    for i in range(1, 400):
        if pending is not None:
            y, b, u = pending
            s = cal.apply(s, rec(i - 1, y, b, cal.UNIT_SCALE, u), p)
            scores = (scores + [y - b])[-64:]
            if u is not None:
                a_e8 = min(25_000_000, max(1_000_000, a_e8 + 50 * (1000 - int(y > u) * 10_000)))
        b, y = rng.uniform(0, 50), rng.uniform(0, 60)
        n, k = len(scores), cal.rank(len(scores), a_e8)
        want = None if n < 16 or k > n else max(0.0, b + sorted(scores)[k - 1])
        got = cal.upper_bound(s, p, b, i * L, cal.UNIT_SCALE)[0]
        assert got == want and s["a_e8"] == a_e8
        pending = (y, b, got)


# --- the fit --------------------------------------------------------------------------------------------------------

def pairs_for(sd, n=4000, seed=3):
    """Day-lag pairs of a level sweeping 5..65 RPS with Gaussian noise of standard deviation sd(level)."""
    rng = random.Random(seed)
    today, yesterday = [], []
    for i in range(n):
        level = 5.0 + 60.0 * i / n
        yesterday.append(level + sd(level) * rng.gauss(0, 1))
        today.append(level + sd(level) * rng.gauss(0, 1))
    return cal.season_pairs(yesterday + today, n)


@pytest.mark.parametrize("name, sd, a, beta", [
    ("additive", lambda v: 2.0, 2.0, 0.0),
    ("proportional", lambda v: 0.05 * v, 0.0, 0.05),
    ("two-part", lambda v: 1.0 + 0.03 * v, 1.0, 0.03),
])
def test_fit_scale_recovers_the_shape(name, sd, a, beta):
    levels, residuals = pairs_for(sd)
    p = policy()
    fit = cal.fit_scale(levels, residuals, p, 1000)
    # The residual of a pair has the noise of two samples: sqrt(2) x sd.
    for level in (10.0, 35.0, 60.0):
        assert fit.at(level) == pytest.approx(math.sqrt(2) * (a + beta * level), rel=0.12), (name, fit)
    assert fit.a >= 0 and fit.beta >= 0 and 0 < fit.s_min < fit.at(35.0)
    pooled = 1.4826 * cal._mad(residuals)[1]
    assert fit.s_min == pytest.approx(0.05 * pooled, rel=1e-9)                 # a share of the data's scale


def test_fit_scale_is_deterministic_constrained_and_needs_a_season_of_pairs():
    levels, residuals = pairs_for(lambda v: 3.0 - 0.03 * v)                    # a scale FALLING with the level
    p = policy()
    fit = cal.fit_scale(levels, residuals, p, 1000)
    assert fit.beta == 0.0 and fit.a > 0                                       # beta >= 0: a constant, no negative slope
    assert cal.fit_scale(levels, residuals, p, 1000) == fit
    assert json.loads(json.dumps(fit.to_wire())) == fit.to_wire()              # survives the journal's JSON exactly
    steep = cal.fit_scale(*pairs_for(lambda v: max(0.0, 0.1 * (v - 20.0))), p, 1000)
    assert steep.a == 0.0 and steep.beta > 0                                   # a >= 0: through the origin instead
    need = p.min_pairs(1000)
    assert need == 800 and policy().min_pairs(24) == 32                        # 80 % of a season, at least 4 per bin
    assert cal.fit_scale(levels[:need - 1], residuals[:need - 1], p, 1000) is None
    assert cal.fit_scale(levels[:need], residuals[:need], p, 1000) is not None
    with pytest.raises(CalibrationError):
        cal.fit_scale(levels, residuals[:-1], p, 1000)
    with pytest.raises(CalibrationError):
        cal.fit_scale([math.nan] + levels[1:], residuals, p, 1000)


def test_season_pairs_skip_missing_samples_and_use_the_pair_mean_as_level():
    values = [1.0, None, 3.0, math.nan, 5.0, 7.0, 4.0, 9.0]                    # season of 4 slots
    levels, residuals = cal.season_pairs(values, 4)
    assert (levels, residuals) == ([3.0, 3.5], [4.0, 1.0])                     # (5, 1) and (4, 3); the others lack a side
    assert cal.season_pairs(values[:4], 4) == ([], [])


def test_fit_scale_on_degenerate_histories():
    p = policy()
    n = 1000
    flat = [float(i % 50) for i in range(n)]
    assert cal.fit_scale(flat, [0.0] * n, p, 1000) == cal.UNIT_SCALE           # no variation at all: scores are 0
    # Zero-inflated: most residuals exactly zero, a few not. Every bin's robust scale is 0; the scale is the pooled
    # mean absolute deviation, constant and positive.
    rare = [1.0 if i % 10 == 0 else 0.0 for i in range(n)]
    fit = cal.fit_scale(flat, rare, p, 1000)
    assert fit.beta == 0.0 and fit.a == pytest.approx(1.2533 * 0.1) and fit.s_min == pytest.approx(0.05 * fit.a)
    # One level only: the slope is not identified and the scale is that level's.
    rng = random.Random(1)
    one = cal.fit_scale([20.0] * n, [rng.gauss(0, 2.0) for _ in range(n)], p, 1000)
    assert one.beta == 0.0 and one.a == pytest.approx(2.0, rel=0.15)


# --- the method is part of the state and of the bound identity --------------------------------------------------------

def test_a_buffer_of_the_additive_method_is_never_read_as_normalized_scores():
    p = policy()
    old = {"schema": "window-calibration/v1", "epoch": EPOCH, "model": "m", "recipe": "r", "a_e8": 10_000_000,
           "scores": [[i * L, 5.0] for i in range(1, 30)], "last_seq": 29, "newest_window_end": 29 * L,
           "all": {"scored": 29, "unscorable": 0, "bounded": 0, "covered": 0, "abs_error_sum_rps": 0.0},
           "gate": {"scored": 29, "unscorable": 0, "bounded": 0, "covered": 0, "pinball_sum_rps": 0.0},
           "level_updates": 0, "at_bound": 0}
    with pytest.raises(CalibrationError, match="another method"):
        cal.upper_bound(old, p, 10.0, 100 * L, cal.UNIT_SCALE)
    with pytest.raises(CalibrationError, match="another method"):
        cal.apply(old, rec(30, 12.0, 10.0, cal.UNIT_SCALE), p)
    relabelled = dict(cal.initial(p, EPOCH, "m", "r"), method="additive")       # the schema alone is not enough
    with pytest.raises(CalibrationError, match="another method"):
        cal.upper_bound(relabelled, p, 10.0, 100 * L, cal.UNIT_SCALE)
    with pytest.raises(ValueError):
        policy(method="additive")


def test_an_issuance_without_a_recorded_scale_is_not_given_one():
    """An issuance of the additive method records no scale. With a bound it is refused outright; without one it leaves
    no score (it is never divided by a scale it was not issued with)."""
    from tests.test_forecast_calibration import journal_pair
    iss, out = journal_pair()
    bundle = json.loads(iss.bundle)
    del bundle["calibration"]
    data = c.canonical_json(bundle)
    legacy = dataclasses.replace(iss, bundle=data, bundle_sha256=c.sha256_hex(data))
    with pytest.raises(CalibrationError, match="no scale"):
        CalibrationRecord.from_journal(legacy, out)
    iss, out = journal_pair(upper=None)
    bundle = json.loads(iss.bundle)
    del bundle["calibration"]
    data = c.canonical_json(bundle)
    r = CalibrationRecord.from_journal(dataclasses.replace(iss, bundle=data, bundle_sha256=c.sha256_hex(data)), out)
    assert r.scale is None and r.decision_origin % L == 0
    p = policy()
    s = cal.apply(cal.initial(p, EPOCH, "m", "r"), dataclasses.replace(r, epoch=EPOCH), p)
    assert s["scores"] == [] and s["gate"]["unscaled"] == 1 and s["gate"]["scored"] == 1
    assert cal.upper_bound(s, p, 1.0, 10 ** 6, None) == (None, {"n": 0, "rank": 1, "working_miscoverage": 0.1,
                                                              "scale": None, "unavailable": "ScaleUnavailable"})


def legacy_identity(pa, lead_seconds):
    """(settings fingerprint, evidence key) an operator or forecaster of the additive method computes for `pa`: the
    current canonical forms without the keys this method added."""
    s = c.resolve_settings(pa)
    canon = s.canonical()
    for k in ADDED_KEYS:
        canon["settings"]["calibration"].pop(k)
    body = {"schema": c.EVIDENCE_SCHEMA, "autoscaler_uid": pa["metadata"]["uid"],
            "target_uid": pa["status"]["metricSource"]["targetUID"],
            "query_sha256": pa["status"]["metricSource"]["sha256"], "signal_contract": svc_helpers.CONTRACT,
            "forecast_contract": c.FORECAST_CONTRACT, "interval_seconds": s.sample_interval_seconds,
            "lead_seconds": lead_seconds, "engine": s.engine, "seasonality": s.seasonality,
            "history_days": s.history_days, "settlement_seconds": s.settlement_seconds,
            "outcome_grace_seconds": s.outcome_grace_seconds, "risk": dataclasses.asdict(s.risk),
            "calibration": dict(canon["settings"]["calibration"])}
    return c.sha256_hex(c.canonical_json(canon)), c.sha256_hex(c.canonical_json(body))


def test_the_method_is_in_the_fingerprint_and_the_evidence_key():
    default = c.resolve_settings({"spec": {"prediction": {"engine": "statistical", "enabled": True,
                                                          "horizonMinutes": 60, "leadTimeMinutes": 15,
                                                          "updateIntervalSeconds": 300}}})
    canon = default.canonical()
    assert canon["settings"]["calibration"]["method"] == c.CALIBRATION_POLICY_REVISION == \
        "normalized-window-score/two-part-scale/v1"
    assert default.calibration.method == cal.CALIBRATION_POLICY_REVISION       # the resolved-settings field
    assert {k: canon["settings"]["calibration"][k] for k in ADDED_KEYS[1:]} == {
        "scale_bins": 8, "scale_floor_bps": 500, "scale_min_season_bps": 8000}
    for k in ADDED_KEYS:
        canon["settings"]["calibration"].pop(k)
    assert c.sha256_hex(c.canonical_json(canon)) == LEGACY_FINGERPRINT         # nothing else moved the fingerprint
    assert default.fingerprint() != LEGACY_FINGERPRINT
    pa = svc_helpers.pa_object()
    old_sha, old_ek = legacy_identity(pa, svc_helpers.LEAD)
    bound = c.bind_request(svc_helpers.body(svc_helpers.T0), svc_helpers.Reader(),
                           now_ms=svc_helpers.T0 * 1000 + 10_000)
    assert bound.settings_sha256 != old_sha and bound.evidence_key != old_ek
    # The spec cannot choose the method or the constants of its fit.
    for key in ("method", "scaleBins", "scaleFloorBps", "scaleMinSeasonBps"):
        with pytest.raises(c.ContractRefused, match="unknown"):
            c.resolve_settings({"spec": {"prediction": {"calibration": {key: 1}}}})


def test_a_one_day_history_is_refused_for_the_statistical_engine_only():
    """The scale fit needs residuals one day apart, so the statistical engine refuses historyDays 1; the legacy
    engine has no such fit and keeps it. The CRD's admission rule and the operator's resolver decide the same
    (k8s-operator: TestEnvtestV2HistoryDaysAdmissionMatchesResolver, and the shared settings fixtures)."""
    for explicit in ({}, {"engine": "statistical"}):
        with pytest.raises(c.ContractRefused, match="historyDays must be at least 2 with engine statistical"):
            c.resolve_settings({"spec": {"prediction": {**explicit, "historyDays": 1}}})
    assert c.resolve_settings({"spec": {"prediction": {"historyDays": 2}}}).history_days == 2
    legacy = c.resolve_settings({"spec": {"prediction": {"engine": "legacyLstm", "sampleIntervalSeconds": 600,
                                                         "historyDays": 1}}})
    assert (legacy.engine, legacy.history_days) == ("legacyLstm", 1)


def test_a_peer_of_the_additive_method_gets_no_forecast(tmp_path):
    """Old operator, new forecaster: the request carries the additive method's fingerprint and is refused (422), with
    nothing issued, so that operator can never be handed predictive eligibility. (New operator, old forecaster: the
    old forecaster refuses the new fingerprint the same way; the Go test covers the operator refusing its answers.)"""
    svc, clock = svc_helpers.make(tmp_path)
    old_sha, _ = legacy_identity(svc_helpers.pa_object(), svc_helpers.LEAD)
    clock.set(svc_helpers.T0 * 1000 + 10_000)
    with pytest.raises(ServiceError) as e:
        svc.predict(svc_helpers.body(svc_helpers.T0, settings=old_sha))
    assert (e.value.status, e.value.reason) == (422, "IdentityMismatch")
    assert svc.store.issuances() == []


def test_an_old_trusted_state_does_not_survive_the_method_change(tmp_path):
    """A journal that holds a Trusted checkpoint of the additive method (its own evidence key): the service of this
    method starts a new epoch with no scores and no trust, and leaves the old evidence where it is."""
    svc, clock = svc_helpers.make(tmp_path)
    _, old_ek = legacy_identity(svc_helpers.pa_object(), svc_helpers.LEAD)
    trusted = {"schema": "statistical-service-state/v1", "epoch": old_ek, "last_outcome_seq": 0,
               "last_issuance_seq": 0, "models": {},
               "trust": {"seasonal_naive": {"epoch": old_ek, "recipe": "seasonal-naive/daily/v1", "trusted": True,
                                            "ever_trusted": True, "demoted_at_s": None, "bounds_at_demotion": 0,
                                            "gates_ok_since_s": svc_helpers.T0 - 3 * DAY}},
               "pairs": {}}
    before = svc.store.write_checkpoint(old_ek, trusted, 0)
    r = c.response_from_wire(svc_helpers.ask(svc, clock, svc_helpers.T0))
    assert r.calibration_epoch != old_ek
    assert r.trust_state != "Trusted" and r.predictive_eligible is False
    assert (r.calibration_windows, r.prospective_windows, r.upper_window_max_rps) == (0, 0, None)
    state = svc.store.latest_checkpoint(r.calibration_epoch).state
    assert state["schema"] == "statistical-service-state/v2"
    h = state["trust"]["seasonal_naive"]
    assert h["trusted"] is False and h["ever_trusted"] is False
    assert svc.store.latest_checkpoint(old_ek).payload_sha256 == before.payload_sha256      # not migrated, not touched


def test_a_checkpoint_of_another_state_schema_is_refused(tmp_path):
    svc, clock = svc_helpers.make(tmp_path)
    ek = c.bind_request(svc_helpers.body(svc_helpers.T0), svc_helpers.Reader(),
                        now_ms=svc_helpers.T0 * 1000 + 10_000).evidence_key
    svc.store.write_checkpoint(ek, {"schema": "statistical-service-state/v1", "epoch": ek, "last_outcome_seq": 0,
                                    "last_issuance_seq": 0, "models": {}, "trust": {}, "pairs": {}}, 0)
    with pytest.raises(ServiceError) as e:
        svc_helpers.ask(svc, clock, svc_helpers.T0)
    assert (e.value.status, e.value.reason) == (503, "StateUnavailable")
    assert svc.store.issuances() == []


# --- the service records the scale; strictly prior data; defined fit points -------------------------------------------

def scale_of(iss):
    return iss.payload()["candidates"]["seasonal_naive"]["calibration"]["scale"]


def test_the_service_records_the_scale_and_refits_only_at_the_first_origin_of_a_utc_day(tmp_path):
    T0, DT = svc_helpers.T0, svc_helpers.DT
    svc, clock = svc_helpers.make(tmp_path)
    start = T0 + DAY - 6 * DT                                                  # half an hour before a UTC midnight
    svc_helpers.run(svc, clock, 12, start=start)
    issued = svc.store.issuances()
    scales = [scale_of(i) for i in issued]
    assert all(s is not None and set(s) == {"s_min", "a", "beta"} for s in scales)
    assert len({json.dumps(s, sort_keys=True) for s in scales[:6]}) == 1       # one scale until midnight
    assert len({json.dumps(s, sort_keys=True) for s in scales[6:]}) == 1       # one from midnight on
    assert scales[5] != scales[6]                                              # refitted at the day's first origin
    fit = [i.payload()["calibration_scale"] for i in issued]
    assert [f["fit_origin"] for f in fit] == [start] * 6 + [T0 + DAY] * 6
    assert fit[0]["pairs"] >= 231                                              # 80 % of the 288 slots of a day
    # The recorded scale is the fit of the history that was final at the fit origin, recomputed here from the signal.
    ends = {start: start - DT, T0 + DAY: T0 + DAY - DT}                        # watermark: origin + 10 s - 120 s
    for origin, end in ends.items():
        values = [svc_helpers.rps(t) for t in range(end - 2 * DAY + DT, end + 1, DT)]
        want = cal.fit_scale(*cal.season_pairs(values, DAY // DT), CalibrationPolicy(svc_helpers.LEAD, min_windows=16,
                                                                                  max_scores=64, min_bps=600), 288)
        assert scale_of(next(i for i in issued if i.key.decision_origin == origin)) == want.to_wire()
    # A restart in the middle of a day keeps the scale in force: no refit, the same recorded scale.
    svc.close()
    again, _ = svc_helpers.make(tmp_path, clock=clock)
    more = svc_helpers.run(again, clock, 3, start=start + 12 * DT)
    assert more and [scale_of(i) for i in again.store.issuances()[-3:]] == [scales[-1]] * 3


def test_the_scale_uses_no_data_at_or_after_the_decision_origin(tmp_path):
    """Two signals that agree up to the newest finalized sample and differ from there on (the decision origin and
    everything after it included) record the same scale and issue the same bound."""
    T0, DT = svc_helpers.T0, svc_helpers.DT
    cut = T0 - DT                                                              # the newest finalized sample at T0
    other = lambda t: svc_helpers.rps(t) if t <= cut else 5.0 * svc_helpers.rps(t) + 40.0   # noqa: E731
    a, ca = svc_helpers.make(tmp_path / "a")
    b, cb = svc_helpers.make(tmp_path / "b", backend=svc_helpers.Backend(other))
    svc_helpers.ask(a, ca, T0)
    svc_helpers.ask(b, cb, T0)
    ia, ib = a.store.issuances()[0], b.store.issuances()[0]
    assert scale_of(ia) == scale_of(ib) and scale_of(ia) is not None
    assert ia.payload()["candidates"] == ib.payload()["candidates"]


def test_without_a_season_of_pairs_there_is_no_scale_no_bound_and_no_score(tmp_path):
    """History that starts just over a day before the first request: forecasts are issued, but with no scale, so with
    no bound, and their outcomes leave no score to be reinterpreted later. The scale arrives with enough pairs."""
    T0, DT = svc_helpers.T0, svc_helpers.DT
    began = T0 - DAY - 2 * DT

    class Young(svc_helpers.Backend):
        def __call__(self, url, **kw):
            status, raw = super().__call__(url, **kw)
            doc = json.loads(raw)
            for series in doc["data"]["result"]:
                series["values"] = [v for v in series["values"] if v[0] >= began]
            return status, json.dumps(doc).encode()

    svc, clock = svc_helpers.make(tmp_path, backend=Young())
    out = [c.response_from_wire(w) for w in svc_helpers.run(svc, clock, 24)]   # two hours
    assert all(r.upper_window_max_rps is None and r.calibration_windows == 0 for r in out)
    issued = svc.store.issuances()
    assert all(scale_of(i) is None for i in issued)
    assert all(i.payload()["candidates"]["seasonal_naive"]["calibration"]["unavailable"] == "ScaleUnavailable"
               for i in issued)
    ek = out[-1].calibration_epoch
    state = svc.store.latest_checkpoint(ek).state
    calib = state["models"]["seasonal_naive"]["calibration"]
    assert calib["scores"] == [] and calib["gate"]["unscaled"] >= 1 and calib["gate"]["scored"] >= 1
    assert state["scale"]["params"] is None and state["scale"]["fit_origin"] is None
    # Enough pairs (80 % of a day) arrive 0.8 day after `began` + one day; the first origin from then on fits.
    later = svc_helpers.run(svc, clock, 2, start=T0 + DAY)
    assert later and scale_of(svc.store.issuances()[-1]) is not None
    assert svc.store.issuances()[-1].payload()["calibration_scale"]["fit_origin"] == T0 + DAY


# --- the acceptance simulation ----------------------------------------------------------------------------------------

@pytest.mark.parametrize("model, lead", [("mixed", 180), ("proportional_ar1", 300), ("discrete_zero", 180)])
def test_the_fast_kernel_equals_the_product_calibration(model, lead):
    x = acceptance.signal(acceptance.MODEL[model], 5, 5)
    exact = acceptance.simulate(x, lead, kernel="exact")
    fast = acceptance.simulate(x, lead, kernel="fast")
    assert np.isfinite(exact.upper).sum() > 500 and (exact.y > exact.upper).sum() > 10
    for col in ("origin", "base", "upper", "y", "working", "sigma"):
        assert np.array_equal(getattr(exact, col), getattr(fast, col), equal_nan=True), col
    assert exact.scales == fast.scales and len(exact.scales) >= 3


def test_the_acceptance_simulation_issues_the_bounds_the_service_issues(tmp_path):
    """The same signal through the real service (journal, maturation, calibration, the scale schedule) and through
    the acceptance simulation: every cohort window gets the same forecast level and the same bound."""
    T0, DT, LEAD = svc_helpers.T0, svc_helpers.DT, svc_helpers.LEAD
    began = T0 - 3 * DAY
    n = 3 * 288 + 2 * 288 + 12
    rng = np.random.default_rng(11)
    level = 20 + 10 * np.sin(2 * np.pi * (np.arange(n) % 288) / 288)
    x = np.round(np.maximum(0.0, level * (1 + 0.05 * rng.standard_normal(n)) + rng.standard_normal(n)), 6)
    backend = svc_helpers.Backend(lambda t: float(x[(t - began) // DT]))
    svc, clock = svc_helpers.make(tmp_path, backend=backend)
    wires = svc_helpers.run(svc, clock, 2 * 288 - 12)                           # two days of requests
    got = {c.response_from_wire(w).decision_origin: c.response_from_wire(w) for w in wires}
    p = CalibrationPolicy(LEAD, min_windows=16, max_scores=64, min_bps=600)
    timing = acceptance.Timing(grid=DT, settlement=120, history_days=2, start=began, request_offset=10)
    sim = acceptance.simulate(x, LEAD, p=p, timing=timing, first_origin=T0)
    compared = bounded = 0
    for origin, base, upper in zip(sim.origin, sim.base, sim.upper):
        r = got.get(int(origin))
        if r is None:
            continue
        compared += 1
        assert r.base_window_max_rps == base
        if math.isnan(upper):
            assert r.upper_window_max_rps is None
        else:
            bounded += 1
            assert r.upper_window_max_rps == upper
    assert compared >= 140 and bounded >= 100
    fits = [(i.payload()["calibration_scale"]["fit_origin"], scale_of(i)) for i in svc.store.issuances()]
    served = max(got)                                                          # the simulation runs on to the end of x
    assert sorted(set((o, json.dumps(s, sort_keys=True)) for o, s in fits)) == \
        sorted((o, json.dumps(s, sort_keys=True)) for o, s in sim.scales if o <= served)
    assert len(set(o for o, _ in fits)) == 2                                   # the first fit and one UTC midnight


def test_the_acceptance_declarations_are_fixed():
    """The declared models, tolerances and seed sets (protocol A): a change here is a change of the protocol."""
    assert [m.name for m in acceptance.MODELS] == ["additive", "proportional", "mixed", "additive_ar1",
                                                   "proportional_ar1", "mixed_ar1", "near_zero", "discrete_zero"]
    assert [m.name for m in acceptance.MODELS if m.criterion] == [m.name for m in acceptance.MODELS[:6]]
    assert (acceptance.MARGINAL_TOLERANCE, acceptance.TERCILE_TOLERANCE, acceptance.NOMINAL_BPS) == (0.02, 0.03, 1000)
    assert acceptance.CONFIGS == ((60, 180, True), (60, 300, True), (300, 1200, False))
    assert acceptance.DEVELOPMENT_SEEDS == tuple(range(101, 111))
    assert acceptance.VALIDATION_SEEDS == tuple(range(900_001, 900_041))
    assert not set(acceptance.DEVELOPMENT_SEEDS) & set(acceptance.VALIDATION_SEEDS)
    assert acceptance.TERCILES["mixed"] == pytest.approx((18.3333, 50.6667), abs=1e-3)
    ci = acceptance.interval([0.90, 0.91, 0.89, 0.90])
    assert ci["mean"] == pytest.approx(0.90) and ci["lo"] < 0.90 < ci["hi"]
    assert acceptance.interval([0.90, 0.91, 0.89, 0.90], ratio=True) == ci      # inside [0, 1]: unchanged


def test_sparse_coverage_is_reported_with_counts_and_an_interval_inside_zero_to_one():
    """A group that few replications have (windows with a zero forecast) is reported with its counts, and a coverage
    interval never leaves [0, 1]: the plain t interval of three sparse replications does."""
    sparse = [1.0, 1.0, 0.5]
    plain, cut = acceptance.interval(sparse), acceptance.interval(sparse, ratio=True)
    assert plain["hi"] > 1.0 and cut["hi"] == 1.0 and 0.0 <= cut["lo"] == plain["lo"]
    assert acceptance.interval([0.0, 0.0, 0.4], ratio=True)["lo"] == 0.0
    assert acceptance.interval([0.97], ratio=True) == {"mean": 0.97, "lo": 0.0, "hi": 1.0, "n": 1}
    assert acceptance.interval([None, None], ratio=True) is None
    rows = [{f"windows_{g}": 0, f"coverage_{g}": None} for g in acceptance.GROUPS]
    rows = [dict(kv for r in rows for kv in r.items()) for _ in range(4)]
    rows[0].update(windows_zero_forecast=2, coverage_zero_forecast=1.0)
    rows[2].update(windows_zero_forecast=4, coverage_zero_forecast=0.5)
    pooled = acceptance.pooled_counts(rows)
    assert pooled["zero_forecast"] == {"windows": 6, "covered": 4, "replications": 2}
    assert pooled["low"] == {"windows": 0, "covered": 0, "replications": 0}
    # The acceptance run carries the counts, and its table shows them for the zero-forecast windows.
    doc = acceptance.run([101], configs=((60, 180, True),), models=("discrete_zero",), days=5, kernel="fast")
    cell = next(c for c in doc["cells"] if c["method"] == "two_part")
    z = cell["pooled"]["zero_forecast"]
    assert doc["schema"] == "calibration-acceptance/v2" and z["windows"] > 0 and 0 <= z["covered"] <= z["windows"]
    s = cell["summary"]["coverage_zero_forecast"]
    assert 0.0 <= s["lo"] <= s["mean"] <= s["hi"] <= 1.0
    assert f"{z['covered']} / {z['windows']} windows in 1 replications" in acceptance.table(doc)
    assert acceptance.reading({"mean": 0.90, "lo": 0.89, "hi": 0.91, "n": 4}, 0.02) == "MET"
    assert acceptance.reading({"mean": 0.90, "lo": 0.87, "hi": 0.93, "n": 4}, 0.02) == "INCONCLUSIVE"
    assert acceptance.reading({"mean": 0.86, "lo": 0.85, "hi": 0.87, "n": 4}, 0.02) == "NOT MET"
    assert acceptance.reading(None, 0.02) == "NOT MEASURED"
