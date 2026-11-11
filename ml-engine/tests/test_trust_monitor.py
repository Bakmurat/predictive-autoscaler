"""Trust policy budgeted-coverage-tests/v1 (trust-repair protocol, T2-T5): the coverage rules as budgeted tests with
exact thresholds, statistical looks only on new evidence, alarm accounting that survives restart and replay, the
completed-day rule, the policy as part of the bound identity, and the PERMANENT guard of the operating characteristics
(exact arithmetic plus a small fixed-seed simulation; the expensive qualification is
evaluation/trust_qualification.py)."""

import dataclasses
import functools
import json
import os
import sys
from fractions import Fraction
from math import comb, gcd

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests.test_statistical_service as svc_helpers  # noqa: E402
from evaluation import calibration_acceptance as acceptance  # noqa: E402
from evaluation import trust_qualification as Q  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting import trust as T  # noqa: E402
from forecasting.calibration import UNIT_SCALE, CalibrationRecord  # noqa: E402
from forecasting.service import ServiceError  # noqa: E402

DAY, H = 86400, 3600
L = 600
EPOCH = "e" * 64
START = 1_793_923_200 - 1_793_923_200 % DAY
TS = c.TrustSettings()
A = Fraction(1, 10)
QM = Fraction(7, 40)               # the model's P(miss | previous miss): 0.175
# The settings fingerprint of the default statistical settings BEFORE this trust policy (calibration method
# normalized-window-score/two-part-scale/v1 with "the last 32 gate windows at least 85 % covered"), frozen from the
# fixtures of the calibration change.
PRE_POLICY_ADDED = ("policy", "rolling_alarm_ppm", "consecutive_alarm_ppm", "day_alarm_ppm", "seven_day_alarm_ppm",
                    "miss_dependence_bps")
# The identity of trust policy budgeted-coverage-tests/v1 for the service tests' autoscaler, frozen from the code of
# that policy (commit a774a68): what an operator or a forecaster one revision back presents.
POLICY_V1_IDENTITY = ("fd30e6a0d857054071bfffa53d1d2db229b22d39e937180470a7e453bf6eb87b",
                      "53b55a6d085491bf1ab92912a04cb5efc961ce6698825320286ca44248c5d796")


def tail(n, k, a=A):
    return sum(comb(n, j) * a ** j * (1 - a) ** (n - j) for j in range(max(k, 0), n + 1))


def limits(lead=L, families=2, t=TS):
    return T.coverage_limits(t, lead, 1000, families)


class Feed:
    """Gate evidence for one candidate at lead = cadence: every origin is a cohort window with a bound."""

    def __init__(self, lim=None, lead=L):
        self.lead, self.lim, self.seq = lead, lim or limits(lead), 0
        self.ledger = T.new_ledger(EPOCH, "seasonal_naive", "r", lead)

    def window(self, origin, covered=True, *, mode="prospective", bounded=True, status="scored"):
        self.seq += 1
        self.ledger = T.note_issuance(self.ledger, seq=self.seq, issuance_id=f"iss-{self.seq}", epoch=EPOCH,
                                      model="seasonal_naive", recipe="r", origin=origin,
                                      window_end=origin + self.lead, deadline_s=origin + self.lead + 720,
                                      lead_s=self.lead, mode=mode, bounded=bounded)
        y = 5.0
        upper = (y + 1.0 if covered else y - 1.0) if bounded else None
        rec = CalibrationRecord(self.seq, EPOCH, "seasonal_naive", "r", f"iss-{self.seq}", origin, origin + self.lead,
                                4.0, upper, status, y if status == "scored" else None, UNIT_SCALE)
        r = T.EvidenceRecord(rec, mode, (origin + self.lead) * 1000, (1.0, 2.0, 3.0) if status == "scored" else None)
        self.ledger = T.note_outcome(self.ledger, r, lead_s=self.lead, nominal_miscoverage=0.1, limits=self.lim)
        return r

    def run(self, hours, start=START, covered=True):
        for t in range(start, start + hours * H, self.lead):
            self.window(t, covered(t) if callable(covered) else covered)
        return self


def inputs(feed):
    return T.CandidateInputs("seasonal_naive", feed.ledger, True, 200, 128, 30 * DAY, DAY, True)


def glob(now, **kw):
    return T.Global(now_s=now, lead_seconds=L, nominal_miscoverage=0.1,
                    **{"replay_suite_passed": True, "startup_budget_valid": True, "configuration_validated": True, **kw})


def evaluate(feed, now, hyst=None, **kw):
    return T.evaluate([inputs(feed)], hyst or {}, glob(now, **kw), TS)


def spread(t, every=20):
    return (t // L) % every != 0


# --- thresholds under the stated transition model ---------------------------------------------------------------------------

def test_binomial_tails_are_exact_and_match_the_protocols_reference_values():
    for n, k in ((32, 5), (32, 8), (32, 9), (32, 12), (64, 15), (24, 4), (480, 72)):
        assert Fraction(T.binomial_tail(n, 1000, k), T.BPS ** n) == tail(n, k), (n, k)
    assert float(tail(32, 5)) == pytest.approx(0.2115, abs=5e-5)           # the old rule: 5 misses of 32
    assert float(tail(32, 8)) == pytest.approx(0.011685, abs=5e-7)
    assert float(tail(32, 9)) == pytest.approx(0.003295, abs=5e-7)
    assert float(tail(32, 12)) == pytest.approx(0.000033, abs=5e-7)
    assert Fraction(T.binomial_tail(40, 250, 3), T.BPS ** 40) == tail(40, 3, Fraction(1, 40))   # another nominal rate
    assert T.binomial_tail(10, 1000, 0) == T.BPS ** 10 and T.binomial_tail(10, 1000, 11) == 0


@functools.lru_cache(maxsize=None)
def model_counts(m, a=A, q=QM, start="stationary"):
    """The stated model, recomputed here from its definition (exact): [covered, missed] with [j] = the numerator of
    P(S_m = j, last outcome) over den. start: "stationary", or "covered" (one covered outcome BEFORE the m counted)."""
    p01 = a * (1 - q) / (1 - a)
    d = (p01.denominator * q.denominator) // gcd(p01.denominator, q.denominator)
    w01, w11 = int(p01 * d), int(q * d)
    w00, w10 = d - w01, d - w11
    scale = a.denominator
    cov, mis = [0] * (m + 2), [0] * (m + 2)
    if start == "stationary":
        cov[0], mis[1] = int((1 - a) * scale), int(a * scale)
        steps, den = m - 1, scale * d ** (m - 1)
    else:
        cov[0] = int((1 - a) * scale)
        steps, den = m, scale * d ** m
    for _ in range(steps):
        cov, mis = ([w00 * cov[j] + w10 * mis[j] for j in range(m + 2)],
                    [0] + [w01 * cov[j] + w11 * mis[j] for j in range(m + 1)])
    return cov, mis, den


def model_tail(m, k, a=A, q=QM):
    cov, mis, den = model_counts(m, a, q)
    return Fraction(sum(cov[max(k, 0):]) + sum(mis[max(k, 0):]), den)


def test_the_model_is_a_stationary_chain_with_the_stated_rates():
    """What the arithmetic assumes (trust module docstring): P(miss) = a at every position, P(miss | miss) = q, and
    q = a is independence. Nothing about other miss processes follows from it."""
    w00, w01, w10, w11, d = T._transitions(1000, TS.miss_dependence_bps)
    assert (Fraction(w01, d), Fraction(w11, d)) == (A * (1 - QM) / (1 - A), QM) == (Fraction(11, 120), Fraction(7, 40))
    assert w00 + w01 == w10 + w11 == d
    for m in (1, 2, 7, 64):                                # stationary: the last outcome is a miss with probability a
        cov, mis, den = model_counts(m)
        assert Fraction(sum(mis), den) == A and Fraction(sum(cov) + sum(mis), den) == 1
    assert model_tail(64, 9, q=A) == tail(64, 9)           # q = a: the binomial
    assert T.model_dependence_bps(1000, 1750) == 1750 and T.model_dependence_bps(2500, 1750) == 2500   # never below a
    assert model_tail(64, 19) > tail(64, 19)               # dependence fattens the tail: a higher threshold follows


@pytest.mark.parametrize("n, looks, ppm, families, q", [(64, 1008, 33000, 1, 1750), (64, 3360, 33000, 2, 1750),
                                                        (64, 2016, 30000, 1, 1000), (48, 504, 20000, 2, 3000),
                                                        (16, 168, 5000, 1, 2000)])
def test_the_rolling_limit_is_the_smallest_count_whose_onsets_fit_the_budget(n, looks, ppm, families, q):
    """First-passage form: the rule starts failing only when the outcome that left was covered, the n - 1 between
    hold exactly k - 1 misses and the newest is a miss. The expected number of onsets in 7 days is within the budget
    from k on, and not at k - 1."""
    k = T.rolling_limit(n, 1000, q, looks, ppm, families)
    qq = Fraction(q, T.BPS)
    cov, mis, den = model_counts(n - 1, q=qq, start="covered")
    p01 = A * (1 - qq) / (1 - A)
    onset = lambda kk: (p01 * cov[kk - 1] + qq * mis[kk - 1]) / den                 # noqa: E731
    budget = Fraction(ppm, T.PPM * families)
    assert all(looks * onset(kk) <= budget for kk in range(k, n + 1))
    assert k == 1 or looks * onset(k - 1) > budget
    assert 1 <= k <= n + 1
    if q == 1000:                                          # independence: a (1 - a) x the binomial point probability
        assert onset(k) == A * (1 - A) * comb(n - 1, k - 1) * A ** (k - 1) * (1 - A) ** (n - k)


def test_the_day_and_seven_day_limits_are_the_models_tails_and_far_from_a_rounding_decision():
    """count_limits works in double precision; at the allowlisted profile every threshold it gives is the exact
    model's (recomputed here in integers), and no decision is closer than one part in a million to its boundary."""
    lim = limits(600, 1)
    day_alpha = Fraction(TS.day_alarm_ppm, T.PPM * 7)
    for m in (24, 72, 143, 144, 145):
        k = lim.day_misses(m)
        assert model_tail(m, k) < day_alpha * (1 - Fraction(1, 10 ** 6)), m
        assert model_tail(m, k - 1) > day_alpha * (1 + Fraction(1, 10 ** 6)), m
    week_alpha = Fraction(TS.seven_day_alarm_ppm, T.PPM * (lim.looks + 168))
    for m in (64, 288, 1008):
        k = lim.seven_day_misses(m)
        assert model_tail(m, k) < week_alpha * (1 - Fraction(1, 10 ** 6)), m
        assert model_tail(m, k - 1) > week_alpha * (1 + Fraction(1, 10 ** 6)), m
    # Count-dependent: never the old flat 85 % on a small day (15 % of 24 outcomes is 4 misses).
    assert limits(3600, 1).day_misses(24) > 4 and lim.day_misses(144) / 144 > 0.15
    assert T.count_limits(3, 1000, 1750, 1, 10 ** 9) == (1, 2, 3, 4)       # no count is unlikely enough: m + 1
    # Beyond the table every further outcome counts as a possible miss, which keeps the bound valid.
    table = T.count_limits(10, 1000, 1750, 1, 100)
    assert T._limit_at(table, 10) == table[10] and T._limit_at(table, 15) == table[10] + 5


def test_the_consecutive_rule_is_inside_the_budget():
    # With q = a the bound is the earlier (N (1 - a) + 1) a^c: 3.0 % for five misses at a 180 s lead (protocol T2).
    assert T.consecutive_bound_ppm(5, 1000, 1000, 3360) == pytest.approx(30250, rel=1e-6)
    assert T.consecutive_bound_ppm(6, 1000, 1000, 3360) == pytest.approx(3025, rel=1e-6)
    for lead, families, want in ((600, 1, 7), (600, 2, 7), (180, 2, 8), (1200, 1, 6), (3600, 1, 6)):
        looks = 7 * DAY // lead
        cc = T.consecutive_limit(5, 1000, 1750, looks, TS.consecutive_alarm_ppm, families)
        assert cc == want == limits(lead, families).consecutive, (lead, families)
        assert families * T.consecutive_bound_ppm(cc, 1000, 1750, looks) <= TS.consecutive_alarm_ppm
        assert families * T.consecutive_bound_ppm(cc - 1, 1000, 1750, looks) > TS.consecutive_alarm_ppm
    assert T.consecutive_limit(12, 1000, 1750, 1008, TS.consecutive_alarm_ppm, 1) == 12   # never below the least
    # The bound against the exact probability of a run of c misses in N outcomes of the model.
    p01, q = 0.1 * 0.825 / 0.9, 0.175
    for cc, n in ((7, 1008), (8, 3360)):
        p = [0.0] * (cc + 1)                                # p[j]: P(no run yet, current streak j)
        p[0], p[1] = 0.9, 0.1
        for _ in range(n - 1):
            nxt = [0.0] * (cc + 1)
            nxt[0] = (1 - p01) * p[0] + (1 - q) * sum(p[1:cc])
            nxt[1] = p01 * p[0]
            for j in range(2, cc):
                nxt[j] = q * p[j - 1]
            nxt[cc] = p[cc] + q * p[cc - 1]
            p = nxt
        assert p[cc] * T.PPM <= T.consecutive_bound_ppm(cc, 1000, 1750, n)
        assert p[cc] * T.PPM == pytest.approx(T.consecutive_bound_ppm(cc, 1000, 1750, n), rel=0.05)


def test_the_frozen_thresholds():
    """Frozen for the trust-repair protocol, amendment 2, before any validation seed was read: the budgets, the
    model's dependence and the thresholds they give at the allowlisted profile (and, for reference, elsewhere)."""
    assert (TS.policy, TS.acute_recent_windows, TS.acute_consecutive_misses) == \
        (c.TRUST_POLICY_REVISION, 64, 5) and c.TRUST_POLICY_REVISION == "budgeted-coverage-tests/v2"
    assert (TS.rolling_alarm_ppm, TS.consecutive_alarm_ppm, TS.day_alarm_ppm, TS.seven_day_alarm_ppm,
            TS.miss_dependence_bps) == (33_000, 7_000, 8_000, 2_000, 1_750)
    assert TS.rolling_alarm_ppm + TS.consecutive_alarm_ppm + TS.day_alarm_ppm + TS.seven_day_alarm_ppm == \
        c.COVERAGE_ALARM_BUDGET_PPM == 50_000
    over = dataclasses.replace(c.ForecastSettings(), trust=dataclasses.replace(TS, seven_day_alarm_ppm=2_001))
    with pytest.raises(c.ContractRefused, match="budgets exceed"):
        c._check_feasible(over)
    short = dataclasses.replace(c.ForecastSettings(), trust=dataclasses.replace(TS, min_prospective_windows=63))
    with pytest.raises(c.ContractRefused, match="minProspectiveWindows must be at least acuteRecentWindows"):
        c._check_feasible(short)                         # the rolling threshold is computed for a full window
    assert (TS.coverage_tolerance_bps, TS.worst_day_min_outcomes, TS.repromotion_hours, TS.repromotion_windows) == \
        (500, 24, 24, 32)
    lim = limits(600, 1)                                 # the profile: 60 s grid, 600 s lead, one family
    assert (lim.recent_n, lim.rolling_misses, lim.consecutive, lim.looks, lim.dependence_bps) == (64, 18, 7, 1008, 1750)
    assert lim.day_misses(144) == 29
    assert [lim.seven_day_misses(m) for m in (64, 288, 1008)] == [23, 59, 153]
    rep = Q.limits_report()
    assert (rep["rolling_misses"], rep["consecutive"], rep["day_outcomes"], rep["day_misses"]) == (18, 7, 144, 29)
    assert rep["seven_day"][-1] == {"outcomes": 1008, "count_condition": 153, "coverage_condition": 152,
                                    "misses_to_fail": 153}
    assert rep["seven_day"][2] == {"outcomes": 288, "count_condition": 59, "coverage_condition": 44,
                                   "misses_to_fail": 59}
    # Elsewhere (never eligible: not on the allowlist), for reference: (lead, families) -> (k of 64, run).
    for lead, families, k, run in ((180, 2, 20, 8), (300, 1, 19, 7), (1200, 1, 18, 6), (3600, 1, 16, 6)):
        other = limits(lead, families)
        assert (other.rolling_misses, other.consecutive) == (k, run), (lead, families)
    with pytest.raises(T.TrustError):
        T.coverage_limits(dataclasses.replace(TS, policy="budgeted-coverage-tests/v1"), 600, 1000, 1)


# --- the allowlist of validated configurations and the minimum lead -------------------------------------------------------

def profile_settings(**pred):
    spec = {"engine": "statistical", "sampleIntervalSeconds": 60, "families": ["seasonalNaive"], "minLeadSeconds": 600}
    spec.update(pred)
    return c.resolve_settings({"spec": {"prediction": spec}})


def test_only_the_validated_profile_is_on_the_allowlist():
    """Amendment 2: grid 60 s, lead 600 s, risk 10 %, seasonal-naive only, with the default calibration, trust and
    timing settings. Anything else is not validated, whatever its evidence."""
    assert c.VALIDATED_PROFILES == (c.ValidatedProfile(60, 600, 1000, ("seasonalNaive",),
                                                       c.VALIDATED_PROFILES[0].evidence),)
    assert c.validated_profile(profile_settings(), 600) is c.VALIDATED_PROFILES[0]
    assert c.validated_profile(profile_settings(minLeadSeconds=0), 600) is c.VALIDATED_PROFILES[0]   # the lead decides
    assert c.validated_profile(profile_settings(historyDays=7, horizonMinutes=30), 600) is not None
    for lead in (60, 180, 300, 540, 660, 1200, 3600):
        if lead != 600:
            assert c.validated_profile(profile_settings(), lead) is None, lead
    for other in ({"sampleIntervalSeconds": 300}, {"sampleIntervalSeconds": 120},
                  {"families": ["seasonalNaive", "statistical"]}, {"risk": {"miscoverageBps": 500}},
                  {"trust": {"acuteConsecutiveMisses": 6}}, {"trust": {"minShadowHours": 24}},
                  {"calibration": {"etaBps": 25}}, {"settlementSeconds": 60}, {"outcomeGraceSeconds": 300}):
        assert c.validated_profile(profile_settings(**other), 600) is None, other
    default = c.resolve_settings({"spec": {"prediction": {}}})               # 300 s grid, both families
    assert all(c.validated_profile(default, lead) is None for lead in (600, 1200))
    assert (Q.GRID, Q.LEAD, Q.NOMINAL_BPS, Q.FAMILIES) == (60, 600, 1000, 1)  # what the qualification job measures


def test_an_unvalidated_configuration_is_trusted_and_reported_but_never_ready():
    f = Feed().run(72, covered=spread)
    now = START + 72 * H
    ready, _ = evaluate(f, now)
    assert (ready.selected, ready.reason, ready.prediction_ready) == ("seasonal_naive", "Ready", True)
    v, h = evaluate(f, now, configuration_validated=False)
    assert (v.selected, v.phase, v.reason, v.prediction_ready) == \
        ("seasonal_naive", "Trusted", "ConfigurationNotValidated", False)
    assert v.candidates["seasonal_naive"].trusted and v.candidates["seasonal_naive"].reason == "Ready"
    assert h["seasonal_naive"]["trusted"]                 # the evidence, the gates and the hysteresis are the same
    assert T.Global(now_s=now, lead_seconds=L, nominal_miscoverage=0.1).configuration_validated is False   # default
    # It is named before the release gate and the lead: neither of them can stand in for a validation.
    v, _ = evaluate(f, now, configuration_validated=False, replay_suite_passed=False, startup_budget_valid=False)
    assert v.reason == "ConfigurationNotValidated"
    assert "ConfigurationNotValidated" in c.REASONS and "ConfigurationNotValidated" in T.REASON_ORDER


def test_the_lead_is_never_below_the_configured_minimum():
    """lead = grid-rounded max(configured minimum, startup + decision + actuation requirement). The startup budget is
    an input only and is not altered; a minimum beyond the horizon is refused with the settings."""
    assert c.automatic_lead_seconds(30, 60, 15, 60) == 180 == c.automatic_lead_seconds(30, 60, 15, 60, 0)
    assert c.automatic_lead_seconds(30, 60, 15, 60, 600) == 600
    assert c.automatic_lead_seconds(30, 60, 15, 60, 541) == 600            # rounded up to the grid
    assert c.automatic_lead_seconds(30, 60, 15, 60, 120) == 180            # the requirement, when it is the larger
    assert c.automatic_lead_seconds(700, 60, 15, 60, 600) == 840
    assert c.automatic_lead_seconds(30, 300, 15, 300, 700) == 900
    s = profile_settings()
    assert (s.min_lead_seconds, c.minimum_lead_seconds(s)) == (600, 600)
    assert c.minimum_lead_seconds(profile_settings(minLeadSeconds=541)) == 600
    assert c.resolve_settings({"spec": {"prediction": {}}}).min_lead_seconds == 0
    for bad in (-1, 3601, 1.5, "600", True):
        with pytest.raises(c.ContractRefused, match="minLeadSeconds"):
            profile_settings(minLeadSeconds=bad)
    with pytest.raises(c.ContractRefused, match="exceeds the horizon"):
        profile_settings(minLeadSeconds=601, horizonMinutes=10)
    assert profile_settings(minLeadSeconds=600, horizonMinutes=10).horizon_seconds == 600
    # The forecaster refuses a published lead below the minimum; the startup delay it records is the operator's.
    lead = {"seconds": 180, "startupDelaySeconds": 30, "actuationAllowanceSeconds": 15, "method": "measured",
            "sampleCount": 25, "observedGeneration": 3}
    pa = {"spec": {"prediction": {"engine": "statistical", "sampleIntervalSeconds": 60, "minLeadSeconds": 600}},
          "status": {"prediction": {"lead": lead}}}
    sel, why = c.resolve_lead(pa, c.resolve_settings(pa), 3)
    assert sel is None and "below the configured minimum lead" in why
    pa["status"]["prediction"]["lead"] = {**lead, "seconds": 600}
    sel, why = c.resolve_lead(pa, c.resolve_settings(pa), 3)
    assert why == "" and (sel.lead_seconds, sel.startup_delay_seconds) == (600, 30)
    # The setting is part of the identity: the fingerprint and the evidence key.
    a, b = profile_settings(), profile_settings(minLeadSeconds=540)
    assert a.fingerprint() != b.fingerprint() and a.canonical()["settings"]["min_lead_seconds"] == 600
    sig = c.bind_request(svc_helpers.body(svc_helpers.T0), svc_helpers.Reader(),
                         now_ms=svc_helpers.T0 * 1000 + 10_000).signal
    assert c.evidence_key(sig, a, 600) != c.evidence_key(sig, b, 600)


# --- the permanent guard of the operating characteristics (T5) ----------------------------------------------------------

def test_the_qualification_declarations_are_fixed():
    """Frozen before any validation seed was read: a change here is a change of the protocol."""
    assert Q.DEVELOPMENT_SEEDS == tuple(range(310_000, 311_000))
    assert Q.VALIDATION_SEEDS == tuple(range(1_260_000, 1_264_000))
    assert not set(Q.DEVELOPMENT_SEEDS) & set(Q.VALIDATION_SEEDS)
    assert (Q.FALSE_ALARM_TARGET, Q.DETECTION_TARGET, Q.DETECTION_OUTCOMES) == (0.05, 0.95, 64)
    assert Q.SOURCES == ("bernoulli", "additive", "proportional", "mixed", "additive_ar1", "proportional_ar1",
                         "mixed_ar1")
    assert Q.CRITERION_CASES == ("independent 40 %", "clustered 40 %")
    assert Q.INDEPENDENT_CASES == (("independent 40 %", 0.4), ("independent 50 %", 0.5), ("all-miss step", 1.0))
    assert set(Q.CLUSTERED_SCORE) == set(Q.SOURCES[1:]) and Q.CLUSTERED_SCORE == CLUSTERED_SCORE
    assert all(q60 > q50 for q60, q50 in Q.CLUSTERED_SCORE.values())     # 60th percentile, 50th percentile
    assert Q.wall_deadline_seconds() == 64 * 600 + 120 + 60 == 38_580      # 10 h 43 min
    assert acceptance.AR_RHO == 0.7 and (Q.BERNOULLI_STREAM, Q.CHANGE_STREAM, Q.PATH_DAYS) == (1001, 1002, 12)
    assert Q.reading_at_most(0.01, 0.02, 0.03, 0.05) == "MET" and Q.reading_at_most(0.03, 0.04, 0.06, 0.05) == \
        "INCONCLUSIVE" and Q.reading_at_most(0.05, 0.06, 0.07, 0.05) == "NOT MET"
    assert Q.reading_at_least(0.96, 0.97, 0.98, 0.95) == "MET" and Q.reading_at_least(0.93, 0.94, 0.96, 0.95) == \
        "NOT MET" and Q.reading_at_least(0.94, 0.96, 0.98, 0.95) == "INCONCLUSIVE"


CLUSTERED_SCORE = {
    "additive": (0.1464, 0.0),
    "proportional": (0.1401, 0.0003),
    "mixed": (0.1427, -0.0003),
    "additive_ar1": (0.1931, 0.0008),
    "proportional_ar1": (0.1868, 0.0005),
    "mixed_ar1": (0.1905, 0.0006),
}


def test_the_simulated_false_alarm_rate_at_nominal_stays_inside_the_budget():
    """Fixed seeds, independent nominal misses through the four rules, the shadow gate and the hysteresis
    (fast_monitor, which another test ties to the product's functions): the upper end of the 95 % interval of the
    false-alarm probability is within 5 %, at the profile and at a 3,600 s lead, where the unbudgeted seven-day
    gate of the earlier policy demoted about one path in six."""
    for lead, grid, days, paths in ((600, 60, 12, 400), (3600, 300, 14, 300)):
        alarms = trusted = 0
        for seed in range(paths):
            r = Q.fast_monitor(Q.bernoulli_outcomes(seed, grid, lead, days), TS, 1)
            trusted += r.trusted_at is not None
            alarms += r.false_alarm is not None
        assert trusted == paths
        assert Q.wilson(alarms, trusted)[1] <= 0.05, (lead, alarms, trusted)


def test_a_sustained_forty_percent_miscoverage_is_detected_within_64_outcomes():
    """The cheap guard, on a SEPARATE latent model (independent misses, then 40 %): the rolling and consecutive
    rules of the profile detect it within 64 outcomes with the interval's lower end at 95 % or more. The
    qualification job measures the complete monitor on the named models."""
    lim = limits(600, 1)
    d = Q.latent_detection(lim, 0.4, trials=60_000, seed=20261111)
    assert d["lo"] >= 0.95, d
    assert d["excluded"] <= 0.01 * d["trials"]
    assert Q.latent_detection(lim, 0.5, trials=20_000, seed=20261111)["lo"] >= 0.99
    step = Q.latent_detection(lim, 1.0, trials=2_000, seed=20261111)
    assert step["probability"] == 1.0 and step["p95_outcomes"] <= lim.consecutive == 7


def test_detection_is_conditional_on_an_eligible_state_at_the_change():
    """Protocol amendment 1: a path on which a rule failed before the change is excluded from the denominator and
    reported, never counted as a detection. With limits that often alarm at the nominal rate and (almost) no miss
    after the change, nothing can be detected on an eligible path: the earlier reading counted the paths that were
    already failing."""
    loose = dataclasses.replace(limits(600, 1), recent_n=8, rolling_misses=2, consecutive=2)
    d = Q.latent_detection(loose, 1e-9, trials=4_000, seed=20261110)
    assert d["excluded"] > 0 and d["eligible"] > 0 and d["eligible"] + d["excluded"] == d["trials"] == 4_000
    assert d["failing_before_change"] > 0.05                  # many paths fail AT the change ...
    assert d["unconditional"] >= d["failing_before_change"] * 0.5 > 0     # ... and the earlier reading counted them
    assert d["detected"] == 0 and d["probability"] == 0.0     # none of them is a detection of the change
    assert (d["lo"], d["hi"]) == Q.wilson(0, d["eligible"])   # the interval is over the eligible paths only
    step = Q.latent_detection(loose, 1.0, trials=4_000, seed=20261110)
    assert step["eligible"] == d["eligible"] and step["detected"] == step["eligible"] and step["probability"] == 1.0
    # The complete monitor uses the same notion: Trusted without interruption up to the change.
    res = Q.MonitorResult(1000, [{"at": 5000, "rules": ["rolling"], "repromoted_at": 9000, "seen": 80}], 500)
    assert Q.trusted_throughout(res, 4000) and Q.trusted_throughout(res, 9001)
    assert not Q.trusted_throughout(res, 6000) and not Q.trusted_throughout(res, 9000)
    assert not Q.trusted_throughout(Q.MonitorResult(None, [], 0), 10 ** 9)
    assert not Q.trusted_throughout(Q.MonitorResult(1000, [], 0), 1000)


def test_one_validation_path_goes_through_the_complete_monitor():
    """A path of the profile: first trust after the 48 h shadow, the change a day or two later, every case decided
    by a coverage demotion of the complete monitor; the all-miss step is caught by the run rule after at most c
    outcomes, and the wall time counts the lead, the settlement and the request."""
    got = Q.path_result("bernoulli", 5)
    assert got["trusted"] and got["false_alarm"] is None and set(got["detection"]) == {c_ for c_, _ in
                                                                                    Q.INDEPENDENT_CASES}
    step = got["detection"]["all-miss step"]              # misses just before the change count towards the run
    assert (step["state"], step["detected"], step["post_miss_rate"], step["rules"]) == \
        ("eligible", True, 1.0, ["consecutive"])
    assert 1 <= step["outcomes"] <= 7 and step["wall_seconds"] == step["outcomes"] * 600 + 120 + 1
    forty = got["detection"]["independent 40 %"]
    assert forty["state"] == "eligible" and 0.2 < forty["post_miss_rate"] < 0.6
    if forty["detected"]:
        assert 1 <= forty["outcomes"] <= 64 and forty["wall_seconds"] <= Q.wall_deadline_seconds()
    cal_path = Q.path_result("mixed_ar1", 310_000)           # a development seed: clustered cases on a calibrated path
    assert set(cal_path["detection"]) == {"independent 40 %", "independent 50 %", "all-miss step", "clustered 40 %",
                                          "clustered 50 %"}
    out = Q.gate_outcomes("mixed_ar1", 310_000)
    assert out.score is not None and len(out.score) == len(out.miss) and (out.grid, out.lead) == (60, 600)
    assert Q.fold_origin(600, 60) == 720 and Q.fold_origin(601, 60) == 720 and Q.fold_origin(541, 60) == 660


def test_the_validation_report_reads_every_source_once_and_needs_every_criterion():
    seeds = Q.DEVELOPMENT_SEEDS[:3]
    a = Q.validate(("bernoulli", "additive", "proportional", "mixed"), seeds)
    b = Q.validate(Q.DEPENDENT_MODELS, seeds)
    doc = Q.report([a, b])
    assert [r["source"] for r in doc["rows"]] == list(Q.SOURCES) and doc["parts"] == [a["sha256"], b["sha256"]]
    assert doc["qualified"] is False                       # three paths decide nothing: every interval is too wide
    assert doc["limits"]["rolling_misses"] == 18 and doc["profile"]["lead_seconds"] == 600
    for r in doc["rows"]:
        assert r["false_alarms"]["paths"] == 3 and r["false_alarms"]["reading"] in ("MET", "INCONCLUSIVE", "NOT MET")
        want = 3 if r["source"] == "bernoulli" else 5
        assert len(r["detection"]) == want
        assert [k for k, d in r["detection"].items() if d["criterion"]] == \
            (["independent 40 %"] if r["source"] == "bernoulli" else ["independent 40 %", "clustered 40 %"])
    text = Q.tables(doc)
    assert text.count("\n") == 2 + 7 + 1 + 2 + 3 + 6 * 5 - 1 and "clustered 40 %" in text
    with pytest.raises(ValueError, match="every source exactly once"):
        Q.report([a])
    with pytest.raises(ValueError, match="every source exactly once"):
        Q.report([a, b, b])
    other = Q.validate(Q.DEPENDENT_MODELS, Q.DEVELOPMENT_SEEDS[:2])
    with pytest.raises(ValueError, match="disagree on seeds"):
        Q.report([a, other])


def test_the_latent_guard_is_the_products_own_rule():
    """The vectorized latent simulation's rule states are trust.note_outcome's, miss sequence by miss sequence."""
    lim = limits(600, 1)
    rng = np.random.default_rng(7)
    for trial in range(40):
        seq = np.concatenate([rng.random(4 * lim.recent_n) < 0.1, rng.random(64) < 0.4])
        f = Feed(lim, lead=600)
        first = None
        for i, miss in enumerate(seq):
            f.window(START + i * 600, covered=not miss)
            state = T.rolling_state(f.ledger["recent"], f.ledger["consecutive_misses"], lim)
            if first is None and i >= 4 * lim.recent_n and (state["rolling"] or state["consecutive"]):
                first = i - 4 * lim.recent_n + 1
        cs = np.concatenate([[0], np.cumsum(seq)])
        want = None
        run = 0
        for i, miss in enumerate(seq):
            run = run + 1 if miss else 0
            rolling = cs[i + 1] - cs[max(0, i + 1 - lim.recent_n)]
            if want is None and i >= 4 * lim.recent_n and (rolling >= lim.rolling_misses or run >= lim.consecutive):
                want = i - 4 * lim.recent_n + 1
        assert first == want


def bernoulli_outcomes(seed, lead, grid, days, rate=0.1, burst=None):
    """Synthetic gate outcomes: independent misses at `rate`; `burst` = (from index, length, rate) raises it."""
    rng = np.random.default_rng(seed)
    n = days * DAY // lead
    miss = (rng.random(n) < rate).astype(np.int64)
    if burst:
        at, length, r = burst
        miss[at:at + length] |= (rng.random(length) < r).astype(np.int64)
    first = START + DAY
    ends = first + lead + lead * np.arange(n, dtype=np.int64)
    return Q.GateOutcomes(grid, lead, ends, miss, first, int(ends[-1]) - lead)


@pytest.mark.parametrize("lead, grid, families, seed, burst", [
    (600, 60, 1, 1, None), (600, 60, 1, 2, (320, 60, 0.5)), (1200, 300, 2, 3, (200, 30, 1.0)),
    (1200, 300, 2, 4, (150, 300, 0.3)), (3600, 300, 1, 6, (70, 40, 0.45))])
def test_the_fast_monitor_is_the_products_trust_evaluation(lead, grid, families, seed, burst):
    """The product's functions at every request (issuances, outcomes, evaluate with its hysteresis; the state
    through JSON every 97 requests, as after a restart) and the event-driven simulation give the same first trust,
    the same demotions by the same rules at the same requests and the same re-promotions: the profile, and two other
    leads for the seven-day rule's count condition (which decides at long leads)."""
    out = bernoulli_outcomes(seed, lead, grid, 9 if lead < 3600 else 12, burst=burst)
    fast = Q.fast_monitor(out, TS, families)
    real = Q.real_monitor(out, TS, families, restart_every=97)
    assert fast.trusted_at == real.trusted_at and fast.trusted_at is not None
    assert [(d["at"], d["rules"], d["repromoted_at"]) for d in fast.demotions] == \
        [(d["at"], d["rules"], d["repromoted_at"]) for d in real.demotions]
    if burst:
        assert fast.demotions and fast.false_alarm is not None


# --- looks happen on new evidence only -------------------------------------------------------------------------------------

def test_a_look_is_one_new_prospective_gate_outcome():
    f = Feed().run(2)
    looks = f.ledger["monitor"]["looks"]
    assert looks == 12
    f.window(START + 2 * H, mode="replay")                           # replay evidence: no look
    f.window(START + 2 * H + L, bounded=False)                       # no bound: no look
    f.window(START + 2 * H + 2 * L, status="unscorable")             # no truth: no look
    assert f.ledger["monitor"]["looks"] == looks
    r = f.window(START + 2 * H + 3 * L)
    assert f.ledger["monitor"]["looks"] == looks + 1
    again = T.note_outcome(f.ledger, r, lead_s=L, nominal_miscoverage=0.1, limits=f.lim)   # the same outcome again
    assert again is f.ledger
    half = Feed(lead=1200)                                           # a non-cohort window is not a gate outcome
    half.window(START + 600)
    assert half.ledger["monitor"]["looks"] == 0


def test_polling_unchanged_evidence_cannot_alarm_again_or_count_twice():
    f = Feed().run(72, covered=spread)
    v, h = evaluate(f, START + 72 * H)
    assert v.candidates["seasonal_naive"].trusted
    run = f.lim.consecutive                                           # 7 at this lead with two families
    for i in range(run):
        f.window(START + 72 * H + i * L, covered=False)
    now = START + 72 * H + run * L
    mon = f.ledger["monitor"]
    assert mon["alarms"] == {"rolling": 0, "consecutive": 1} and mon["last_alarm"]["rule"] == "consecutive"
    states = []
    for poll in range(30):                                           # thirty requests on the same evidence
        v, h = evaluate(f, now + 60 * poll, h)
        states.append((v.candidates["seasonal_naive"].trusted, h["seasonal_naive"]["demotions"],
                       h["seasonal_naive"]["demoted_at_s"], h["seasonal_naive"]["alarms_seen"]))
    assert set(states) == {(False, 1, now, 1)}                       # demoted once, at the first request, counted once
    assert f.ledger["monitor"] == mon                                # evaluating never touches the ledger's account
    why = h["seasonal_naive"]["last_demotion"]
    assert why["at_s"] == now and why["gates"] == ["coverage"] and why["coverage"]["failing"] == ["consecutive"]
    assert why["coverage"]["consecutive"] == [run, run] and run == 7 > TS.acute_consecutive_misses
    assert why["latched_alarm"]["rule"] == "consecutive"
    # More misses while the rule already fails are the same alarm, not new ones.
    f.window(now, covered=False)
    assert f.ledger["monitor"]["alarms"]["consecutive"] == 1


def test_an_alarm_between_two_evaluations_still_demotes_exactly_once():
    """Several outcomes fold in one request (after an outage): the consecutive rule fails and holds again before the
    next evaluation. The gate's state looks fine, but the alarm was counted at its look and demotes."""
    f = Feed().run(72, covered=spread)
    v, h = evaluate(f, START + 72 * H)
    assert v.candidates["seasonal_naive"].trusted
    t0 = START + 72 * H
    run = f.lim.consecutive
    for i in range(run):
        f.window(t0 + i * L, covered=False)
    f.window(t0 + run * L, covered=True)                             # the streak ends before anyone looked
    now = t0 + (run + 1) * L
    gates, info = T.candidate_gates(inputs(f), glob(now), TS)
    assert gates["coverage"] and info["coverage"]["failing"] == []   # the state alone would let it pass
    v, h = evaluate(f, now, h)
    cv = v.candidates["seasonal_naive"]
    assert not cv.trusted and cv.reason == "CoverageLow" and cv.coverage_failing == ("alarm",)
    assert h["seasonal_naive"]["demotions"] == 1 and h["seasonal_naive"]["last_demotion"]["latched_alarm"]
    v, h = evaluate(f, now + 60, h)                                  # seen once: the next request holds again
    assert v.candidates["seasonal_naive"].gates["coverage"] and h["seasonal_naive"]["demotions"] == 1
    assert h["seasonal_naive"]["gates_ok_since_s"] == now + 60       # and the 24 h dwell starts only now


def test_a_restart_in_the_middle_keeps_the_detector_state_and_the_alarm_account():
    def drive(restart_at=None):
        f, h, log = Feed(), {}, []
        rng = np.random.default_rng(5)
        for i in range(600):
            t = START + i * L
            miss = rng.random() < (0.45 if 450 <= i < 520 else 0.08)
            f.window(t, covered=not miss)
            v, h = evaluate(f, t + L + 120, h)
            log.append((v.candidates["seasonal_naive"].trusted, v.candidates["seasonal_naive"].coverage_failing))
            if restart_at == i:                                      # the checkpoint, read back after a restart
                f.ledger, h = json.loads(json.dumps(f.ledger)), json.loads(json.dumps(h))
        return f.ledger, h, log
    whole, split = drive(), drive(restart_at=480)                    # 480: in the middle of the burst
    assert whole == split
    ledger, h, log = whole
    assert ledger["monitor"]["alarms"]["rolling"] >= 1 and h["seasonal_naive"]["demotions"] == 1
    assert any(not trusted for trusted, _ in log) and log[440][0]


def test_a_ledger_of_another_trust_policy_is_refused():
    f = Feed().run(2)
    old = {k: v for k, v in f.ledger.items() if k != "monitor"}
    old["schema"] = "evidence-ledger/v2"
    with pytest.raises(T.TrustError, match="another trust policy"):
        T.candidate_gates(T.CandidateInputs("seasonal_naive", old, True, 200, 128, 30 * DAY, DAY, True),
                          glob(START + 3 * H), TS)
    rec = CalibrationRecord(99, EPOCH, "seasonal_naive", "r", "iss-99", START, START + L, 4.0, 6.0, "scored", 5.0,
                            UNIT_SCALE)
    with pytest.raises(T.TrustError, match="another trust policy"):
        T.note_outcome(old, T.EvidenceRecord(rec, "prospective", 0, (1.0, 2.0, 3.0)), lead_s=L,
                       nominal_miscoverage=0.1, limits=f.lim)


# --- the rolling rule ----------------------------------------------------------------------------------------------------

def test_the_rolling_rule_counts_misses_and_does_not_wait_for_a_full_window():
    lim = limits()
    f = Feed()
    k = lim.rolling_misses
    for i in range(2 * k - 2):                                       # k - 1 misses, never two in a row
        f.window(START + i * L, covered=i % 2 == 1)
    assert not T.rolling_state(f.ledger["recent"], f.ledger["consecutive_misses"], lim)["rolling"]
    f.window(START + (2 * k - 2) * L, covered=False)                 # the k-th miss among fewer than n outcomes
    assert len(f.ledger["recent"]) == 2 * k - 1 < lim.recent_n
    assert T.rolling_state(f.ledger["recent"], f.ledger["consecutive_misses"], lim) == {"rolling": True,
                                                                                         "consecutive": False}
    assert f.ledger["monitor"]["alarms"] == {"rolling": 1, "consecutive": 0}
    # The old rule (at least 85 % of the last 32 covered) demoted at 5 misses in 32: here that is far from an alarm.
    g = Feed()
    for i in range(64):
        g.window(START + i * L, covered=i % 6 != 0)                  # 11 misses in 64, 6 in every 32
    assert g.ledger["monitor"]["alarms"] == {"rolling": 0, "consecutive": 0}


# --- the completed-day rule ----------------------------------------------------------------------------------------------

def test_the_day_rule_tests_completed_days_only_once_their_outcomes_are_final():
    lim = limits()
    per_day = DAY // L                                               # 144 gate outcomes a day
    k = lim.day_misses(per_day)
    f = Feed().run(72, covered=spread)                               # three good days
    day3 = START + 3 * DAY
    bad = lambda t: (t - day3) // L % 4 != 0                         # noqa: E731  25 % misses, never two in a row
    f.run(12, start=day3, covered=bad)                               # half of day 3
    noon = day3 + 12 * H + L + 120
    gates, info = T.candidate_gates(inputs(f), glob(noon), TS)
    assert 18 >= k - 18 and info["coverage"]["failed_days"] == []    # the partial day is not tested, whatever it holds
    assert [d[0] for d in info["coverage"]["days"]] == [START // DAY + i for i in range(3)]
    f.run(12, start=day3 + 12 * H, covered=bad)                      # the rest of day 3: 36 misses of 144
    assert 36 >= k
    final = day3 + DAY + 720                                         # day end + settlement + grace
    for now, failed in ((final - 1, []), (final, [day3 // DAY]), (final + 3 * DAY, [day3 // DAY])):
        gates, info = T.candidate_gates(inputs(f), glob(now), TS)
        assert info["coverage"]["failed_days"] == failed, now
        assert ("day" in info["coverage"]["failing"]) == bool(failed)
    row = next(d for d in T.candidate_gates(inputs(f), glob(final), TS)[1]["coverage"]["days"] if d[0] == day3 // DAY)
    assert row == [day3 // DAY, per_day, per_day - 36, k]             # [day, bounded, covered, limit]
    # The failed day keeps the rule failed while it lies wholly inside the retained 7-day window (until the hour
    # before its start is the window's oldest bucket), and it is never judged on counts that have begun to expire.
    f.run(24 * 6 - 1, start=day3 + DAY, covered=spread)
    last = day3 + 7 * DAY - H
    gates, info = T.candidate_gates(inputs(f), glob(last), TS)
    assert "day" in info["coverage"]["failing"]
    assert next(d for d in info["coverage"]["days"] if d[0] == day3 // DAY) == row      # the same final counts
    f.run(1, start=last, covered=spread)
    info = T.candidate_gates(inputs(f), glob(day3 + 7 * DAY), TS)[1]["coverage"]
    assert "day" not in info["failing"] and day3 // DAY not in [d[0] for d in info["days"]]


def test_the_day_rule_is_count_dependent_and_skips_small_days():
    lim = limits()
    assert TS.worst_day_min_outcomes == 24
    f = Feed()
    for i in range(23):                                              # a day with 23 outcomes, most of them missed
        f.window(START + i * L, covered=i % 3 == 0 or i % 2 == 0)
    assert T.candidate_gates(inputs(f), glob(START + DAY + 720), TS)[1]["coverage"]["days"] == []
    g = Feed()
    k24 = lim.day_misses(24)
    for i in range(24):                                              # 24 outcomes, k24 - 1 misses spread out
        g.window(START + i * 3 * L, covered=not (i % 2 == 0 and i // 2 < k24 - 1))
    info = T.candidate_gates(inputs(g), glob(START + DAY + 720), TS)[1]["coverage"]
    assert info["days"] == [[START // DAY, 24, 24 - (k24 - 1), k24]] and info["failed_days"] == []
    assert k24 - 1 > 0.15 * 24                                       # the old flat 85 % would have failed this day


def test_missing_outcomes_are_an_availability_matter_not_a_miss():
    f = Feed().run(72, covered=spread)
    day3 = START + 3 * DAY
    for t in range(day3, day3 + DAY, L):
        f.window(t, status="unscorable" if (t // L) % 2 else "scored")
    gates, info = T.candidate_gates(inputs(f), glob(day3 + DAY + 720), TS)
    assert gates["coverage"] and not gates["availability"]
    [row] = [d for d in info["coverage"]["days"] if d[0] == day3 // DAY]
    assert row[1] == row[2] and 70 <= row[1] <= 74 and row[3] == limits().day_misses(row[1])   # scored ones, all covered


# --- the seven-day rule: low coverage AND a significant count; the dwell is unchanged ------------------------------------

def test_the_seven_day_rule_needs_low_coverage_and_a_significant_count_and_the_dwell_is_unchanged():
    k7 = limits().seven_day_misses(432)
    low = Feed().run(72, covered=lambda t: (t // L) % 6 != 0)        # 83.3 % < 85 %: 72 misses of 432
    gates, info = T.candidate_gates(inputs(low), glob(START + 72 * H), TS)
    assert info["coverage"]["seven_day"] == [360, 432, k7] and 72 < k7 < 108
    assert gates["coverage"] and info["coverage"]["failing"] == []   # below 85 %, within the noise of 432 outcomes
    lower = Feed().run(72, covered=lambda t: (t // L) % 4 != 0)      # 75 %: 108 misses of 432
    gates, info = T.candidate_gates(inputs(lower), glob(START + 72 * H), TS)
    assert not gates["coverage"] and "seven_day" in info["coverage"]["failing"]
    many = Feed(limits(600, 2)).run(72, covered=lambda t: (t // L) % 8 != 0)   # 87.5 %: the coverage condition holds
    gates, info = T.candidate_gates(inputs(many), glob(START + 72 * H), TS)
    assert gates["coverage"] and info["coverage"]["seven_day"][:2] == [378, 432]
    none = T.candidate_gates(inputs(Feed()), glob(START + 72 * H), TS)[1]["coverage"]
    assert "seven_day" in none["failing"] and none["seven_day"][:2] == [0, 0]   # no outcome at all: never holding
    # With a week of outcomes at a short lead the coverage condition is the binding one, as it always was.
    week = limits(180, 2)
    assert week.seven_day_misses(week.looks) < 15 * week.looks // 100 + 1
    led = T.new_ledger(EPOCH, "seasonal_naive", "r", L)
    h = T.step(T.new_hysteresis("r", EPOCH), True, led, 0, TS)
    h = T.step(h, False, led, 100, TS, {"gates": ["coverage"]})
    assert (h["trusted"], h["demotions"], h["last_demotion"]) == (False, 1, {"at_s": 100, "gates": ["coverage"]})
    led["totals"]["prospective_gate_bounded"] = 32
    h = T.step(h, True, led, 200, TS)
    assert not T.step(h, True, led, 200 + 24 * H - 1, TS)["trusted"]
    assert T.step(h, True, led, 200 + 24 * H, TS)["trusted"]


# --- the policy is part of the bound identity -----------------------------------------------------------------------------

def test_the_trust_policy_and_its_thresholds_are_in_the_fingerprint_and_the_policy_in_the_evidence_key():
    pa = svc_helpers.pa_object()
    s = c.resolve_settings(pa)
    canon = s.canonical()["settings"]["trust"]
    assert canon["policy"] == c.TRUST_POLICY_REVISION == "budgeted-coverage-tests/v2"
    assert (canon["rolling_alarm_ppm"], canon["consecutive_alarm_ppm"], canon["day_alarm_ppm"],
            canon["seven_day_alarm_ppm"], canon["miss_dependence_bps"]) == \
        (TS.rolling_alarm_ppm, TS.consecutive_alarm_ppm, TS.day_alarm_ppm, TS.seven_day_alarm_ppm,
         TS.miss_dependence_bps) == (33_000, 7_000, 8_000, 2_000, 1_750)
    assert (canon["acute_recent_windows"], canon["acute_consecutive_misses"]) == (64, 5)
    assert s.trust.policy == c.TRUST_POLICY_REVISION                 # the resolved-settings field
    for name, value in (("rolling_alarm_ppm", 1), ("consecutive_alarm_ppm", 1), ("day_alarm_ppm", 1),
                        ("seven_day_alarm_ppm", 1), ("miss_dependence_bps", 1000),
                        ("policy", "other/v9"), ("acute_recent_windows", 32), ("acute_consecutive_misses", 6)):
        other = dataclasses.replace(s, trust=dataclasses.replace(s.trust, **{name: value}))
        assert other.fingerprint() != s.fingerprint(), name
    sig = c.bind_request(svc_helpers.body(svc_helpers.T0), svc_helpers.Reader(),
                         now_ms=svc_helpers.T0 * 1000 + 10_000).signal
    key = c.evidence_key(sig, s, svc_helpers.LEAD)
    revised = dataclasses.replace(s, trust=dataclasses.replace(s.trust, policy="other/v9"))
    assert c.evidence_key(sig, revised, svc_helpers.LEAD) != key     # a policy revision is a new evidence epoch
    budget = dataclasses.replace(s, trust=dataclasses.replace(s.trust, rolling_alarm_ppm=1))
    assert c.evidence_key(sig, budget, svc_helpers.LEAD) == key      # thresholds alone keep the evidence (as before)
    for key_name in ("policy", "rollingAlarmPpm", "consecutiveAlarmPpm", "dayAlarmPpm", "sevenDayAlarmPpm",
                     "missDependenceBps"):                           # resolved, never read from the spec
        with pytest.raises(c.ContractRefused, match="unknown"):
            c.resolve_settings({"spec": {"prediction": {"trust": {key_name: 1}}}})


def pre_policy_identity(pa, lead_seconds):
    """(settings fingerprint, evidence key) of the calibration change without this trust policy: what an operator or
    a forecaster one revision back computes."""
    s = c.resolve_settings(pa)
    canon = s.canonical()
    for k in PRE_POLICY_ADDED:
        canon["settings"]["trust"].pop(k)
    canon["settings"]["trust"].update(acute_recent_windows=32)
    canon["settings"].pop("min_lead_seconds")
    body = {"schema": c.EVIDENCE_SCHEMA, "autoscaler_uid": pa["metadata"]["uid"],
            "target_uid": pa["status"]["metricSource"]["targetUID"],
            "query_sha256": pa["status"]["metricSource"]["sha256"], "signal_contract": svc_helpers.CONTRACT,
            "forecast_contract": c.FORECAST_CONTRACT, "interval_seconds": s.sample_interval_seconds,
            "lead_seconds": lead_seconds, "engine": s.engine, "seasonality": s.seasonality,
            "history_days": s.history_days, "settlement_seconds": s.settlement_seconds,
            "outcome_grace_seconds": s.outcome_grace_seconds, "risk": dataclasses.asdict(s.risk),
            "calibration": dataclasses.asdict(s.calibration)}
    return c.sha256_hex(c.canonical_json(canon)), c.sha256_hex(c.canonical_json(body))


def policy_v1_identity(pa, lead_seconds):
    """(settings fingerprint, evidence key) of trust policy budgeted-coverage-tests/v1, re-derived: today's canonical
    settings and evidence-key body without what v2 added, with v1's name and rolling budget."""
    s = c.resolve_settings(pa)
    canon = s.canonical()
    for k in ("seven_day_alarm_ppm", "miss_dependence_bps"):
        canon["settings"]["trust"].pop(k)
    canon["settings"]["trust"].update(policy="budgeted-coverage-tests/v1", rolling_alarm_ppm=35_000)
    canon["settings"].pop("min_lead_seconds")
    body = {"schema": c.EVIDENCE_SCHEMA, "autoscaler_uid": pa["metadata"]["uid"],
            "target_uid": pa["status"]["metricSource"]["targetUID"],
            "query_sha256": pa["status"]["metricSource"]["sha256"], "signal_contract": svc_helpers.CONTRACT,
            "forecast_contract": c.FORECAST_CONTRACT, "interval_seconds": s.sample_interval_seconds,
            "lead_seconds": lead_seconds, "engine": s.engine, "seasonality": s.seasonality,
            "history_days": s.history_days, "settlement_seconds": s.settlement_seconds,
            "outcome_grace_seconds": s.outcome_grace_seconds, "risk": dataclasses.asdict(s.risk),
            "calibration": dataclasses.asdict(s.calibration), "trust_policy": "budgeted-coverage-tests/v1"}
    return c.sha256_hex(c.canonical_json(canon)), c.sha256_hex(c.canonical_json(body))


def test_the_identity_of_policy_v1_is_what_its_code_computed():
    """Nothing but the policy revision, its budgets and the minimum lead moved the identity since v1."""
    assert policy_v1_identity(svc_helpers.pa_object(), svc_helpers.LEAD) == POLICY_V1_IDENTITY


@pytest.mark.parametrize("earlier", [pre_policy_identity, policy_v1_identity])
def test_a_peer_of_the_earlier_trust_policy_gets_no_forecast(tmp_path, earlier):
    svc, clock = svc_helpers.make(tmp_path)
    old_sha, old_ek = earlier(svc_helpers.pa_object(), svc_helpers.LEAD)
    bound = c.bind_request(svc_helpers.body(svc_helpers.T0), svc_helpers.Reader(),
                           now_ms=svc_helpers.T0 * 1000 + 10_000)
    assert bound.settings_sha256 != old_sha and bound.evidence_key != old_ek
    clock.set(svc_helpers.T0 * 1000 + 10_000)
    with pytest.raises(ServiceError) as e:
        svc.predict(svc_helpers.body(svc_helpers.T0, settings=old_sha))
    assert (e.value.status, e.value.reason) == (422, "IdentityMismatch") and svc.store.issuances() == []


@pytest.mark.parametrize("earlier", [pre_policy_identity, policy_v1_identity])
def test_an_old_trusted_state_does_not_survive_the_policy_change(tmp_path, earlier):
    """A journal holding a Trusted checkpoint of an earlier trust policy (its own evidence key, an earlier ledger
    schema): this policy starts a new epoch with no evidence and no trust, and leaves the old one alone."""
    svc, clock = svc_helpers.make(tmp_path)
    _, old_ek = earlier(svc_helpers.pa_object(), svc_helpers.LEAD)
    old_ledger = {k: v for k, v in T.new_ledger(old_ek, "seasonal_naive", "seasonal-naive/daily/v1", 300).items()
                  if k != "monitor"}
    old_ledger["schema"] = "evidence-ledger/v2"
    trusted = {"schema": "statistical-service-state/v2", "epoch": old_ek, "last_outcome_seq": 0,
               "last_issuance_seq": 0, "scale": None,
               "models": {"seasonal_naive": {"recipe": "seasonal-naive/daily/v1", "ledger": old_ledger}},
               "trust": {"seasonal_naive": {"epoch": old_ek, "recipe": "seasonal-naive/daily/v1", "trusted": True,
                                            "ever_trusted": True, "demoted_at_s": None, "bounds_at_demotion": 0,
                                            "gates_ok_since_s": svc_helpers.T0 - 3 * DAY}},
               "pairs": {}}
    before = svc.store.write_checkpoint(old_ek, trusted, 0)
    r = c.response_from_wire(svc_helpers.ask(svc, clock, svc_helpers.T0))
    assert r.calibration_epoch != old_ek and r.trust_state != "Trusted" and r.predictive_eligible is False
    assert (r.calibration_windows, r.prospective_windows) == (0, 0)
    state = svc.store.latest_checkpoint(r.calibration_epoch).state
    assert state["schema"] == "statistical-service-state/v3"
    h = state["trust"]["seasonal_naive"]
    assert (h["trusted"], h["ever_trusted"], h["alarms_seen"], h["demotions"]) == (False, False, 0, 0)
    assert state["models"]["seasonal_naive"]["ledger"]["schema"] == T.LEDGER_SCHEMA
    assert svc.store.latest_checkpoint(old_ek).payload_sha256 == before.payload_sha256
    # The same checkpoint under THIS epoch's key (a state of the earlier schema) is refused, never adopted.
    other, oclock = svc_helpers.make(tmp_path / "other")
    other.store.write_checkpoint(r.calibration_epoch, dict(trusted, epoch=r.calibration_epoch), 0)
    with pytest.raises(ServiceError) as e:
        svc_helpers.ask(other, oclock, svc_helpers.T0)
    assert (e.value.status, e.value.reason) == (503, "StateUnavailable")


def test_the_service_keeps_the_monitor_across_a_restart_and_reports_the_failing_rules(tmp_path):
    """End to end through the journal: the detector state and the alarm account are in the checkpoint, a restarted
    service continues with them unchanged, and the issuance names the coverage rules failing at its evaluation."""
    whole, wclock = svc_helpers.make(tmp_path / "whole", store_id="journal-1")
    expected = svc_helpers.run(whole, wclock, 130)
    first, clock = svc_helpers.make(tmp_path / "split", store_id="journal-1")
    got = svc_helpers.run(first, clock, 100)
    ek = c.response_from_wire(got[-1]).calibration_epoch
    mid = first.store.latest_checkpoint(ek).state["models"]["seasonal_naive"]["ledger"]["monitor"]
    first.close()
    second, _ = svc_helpers.make(tmp_path / "split", clock=clock, store_id="journal-1")
    got += svc_helpers.run(second, clock, 30, start=svc_helpers.T0 + 100 * svc_helpers.DT)
    assert [json.dumps(w, sort_keys=True) for w in got] == [json.dumps(w, sort_keys=True) for w in expected]
    a = second.store.latest_checkpoint(ek).state
    b = whole.store.latest_checkpoint(ek).state
    assert a == b
    mon = a["models"]["seasonal_naive"]["ledger"]["monitor"]
    assert mon["looks"] > mid["looks"] > 0 and set(mon["alarms"]) == {"rolling", "consecutive"}
    h = a["trust"]["seasonal_naive"]
    assert {"alarms_seen", "demotions", "last_demotion"} <= set(h)
    verdict = second.store.issuances(ek)[-1].payload()["verdict"]["candidates"]["seasonal_naive"]
    assert isinstance(verdict["coverage_failing"], list)
