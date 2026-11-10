"""Trust policy budgeted-coverage-tests/v1 (trust-repair protocol, T2-T5): the coverage rules as budgeted tests with
exact thresholds, statistical looks only on new evidence, alarm accounting that survives restart and replay, the
completed-day rule, the policy as part of the bound identity, and the PERMANENT guard of the operating characteristics
(exact arithmetic plus a small fixed-seed simulation; the expensive qualification is
evaluation/trust_qualification.py)."""

import dataclasses
import json
import os
import sys
from fractions import Fraction
from math import comb

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests.test_statistical_service as svc_helpers  # noqa: E402
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
# The settings fingerprint of the default statistical settings BEFORE this trust policy (calibration method
# normalized-window-score/two-part-scale/v1 with "the last 32 gate windows at least 85 % covered"), frozen from the
# fixtures of the calibration change.
PRE_POLICY_ADDED = ("policy", "rolling_alarm_ppm", "consecutive_alarm_ppm", "day_alarm_ppm")


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
    return T.Global(now_s=now, lead_seconds=L, nominal_miscoverage=0.1, replay_suite_passed=True,
                    startup_budget_valid=True, **kw)


def evaluate(feed, now, hyst=None, **kw):
    return T.evaluate([inputs(feed)], hyst or {}, glob(now, **kw), TS)


def spread(t, every=20):
    return (t // L) % every != 0


# --- exact thresholds ---------------------------------------------------------------------------------------------------

def test_binomial_tails_are_exact_and_match_the_protocols_reference_values():
    for n, k in ((32, 5), (32, 8), (32, 9), (32, 12), (63, 18), (480, 73), (1440, 185), (10, 0), (10, 11)):
        assert Fraction(T.binomial_tail(n, 1000, k), T.BPS ** n) == tail(n, k), (n, k)
    assert float(tail(32, 5)) == pytest.approx(0.2115, abs=5e-5)                 # the old rule: 5 misses in 32
    assert float(tail(32, 8)) == pytest.approx(0.011685, abs=5e-7)
    assert float(tail(32, 9)) == pytest.approx(0.003295, abs=5e-7)
    assert float(tail(32, 12)) == pytest.approx(0.000033, abs=5e-7)
    assert Fraction(T.binomial_tail(40, 250, 3), T.BPS ** 40) == tail(40, 3, Fraction(1, 40))   # another nominal rate


@pytest.mark.parametrize("n, looks, ppm, families", [(32, 3360, 15000, 2), (64, 3360, 30000, 2), (64, 2016, 30000, 1),
                                                     (48, 504, 20000, 2), (16, 168, 5000, 1)])
def test_the_rolling_limit_is_the_smallest_count_within_its_budget(n, looks, ppm, families):
    k = T.rolling_limit(n, 1000, looks, ppm, families)
    bound = lambda kk: looks * A * tail(n - 1, kk - 1) * families                # noqa: E731
    assert bound(k) <= Fraction(ppm, T.PPM)                                      # within the budget
    assert k == 1 or bound(k - 1) > Fraction(ppm, T.PPM)                         # and one less would not be
    assert 1 <= k <= n + 1


def test_the_day_limit_depends_on_the_days_count_and_stays_within_its_budget():
    budget = Fraction(TS.day_alarm_ppm, T.PPM)
    for m in (24, 72, 144, 288, 480, 1440):
        for families in (1, 2):
            k = T.day_limit(m, 1000, TS.day_alarm_ppm, families)
            assert 7 * families * tail(m, k) <= budget < 7 * families * tail(m, k - 1), (m, families)
    # Count-dependent: never the old flat 85 % (15 % of the day's outcomes), which on 24 outcomes is 4 misses.
    assert T.day_limit(24, 1000, TS.day_alarm_ppm, 2) > 4
    assert T.day_limit(480, 1000, TS.day_alarm_ppm, 2) / 480 < 0.16
    assert T.day_limit(3, 1000, TS.day_alarm_ppm, 2) == 4                        # no count is unlikely enough: m + 1


def test_the_consecutive_rule_is_inside_the_budget():
    # Protocol T2: "under independent 10 % misses and three-minute gate windows its own false-alarm probability is
    # about 2.98 % in seven days" for five misses; the bound used here is (N (1 - a) + 1) a^c.
    assert T.consecutive_bound_ppm(5, 1000, 3360) == pytest.approx(30250, rel=1e-6)
    assert T.consecutive_bound_ppm(6, 1000, 3360) == pytest.approx(3025, rel=1e-6)
    # The limit is the smallest run, not below the configured least (5), whose bound fits the budget share: it stays
    # at five only where the combined budget holds.
    for lead, families, want in ((180, 2, 6), (180, 1, 6), (300, 2, 6), (120, 2, 7), (1200, 2, 6), (1200, 1, 5),
                                 (3600, 2, 5)):
        looks = 7 * DAY // lead
        cc = T.consecutive_limit(5, 1000, looks, TS.consecutive_alarm_ppm, families)
        assert cc == want, (lead, families)
        assert families * T.consecutive_bound_ppm(cc, 1000, looks) <= TS.consecutive_alarm_ppm
        assert cc == 5 or families * T.consecutive_bound_ppm(cc - 1, 1000, looks) > TS.consecutive_alarm_ppm
    assert T.consecutive_limit(8, 1000, 3360, TS.consecutive_alarm_ppm, 2) == 8      # never below the configured least
    assert T.consecutive_limit(5, 2500, 3360, TS.consecutive_alarm_ppm, 2) == 10     # a 25 % nominal rate needs ten
    # The same bound against the exact probability of a run of c misses in N independent outcomes.
    for cc, n in ((5, 3360), (6, 3360), (6, 2016)):
        p = [0.0] * (cc + 1)                                # p[j]: P(no run yet, current streak j)
        p[0] = 1.0
        for _ in range(n):
            q = [0.0] * (cc + 1)
            q[0] = 0.9 * sum(p[:cc])
            for j in range(1, cc):
                q[j] = 0.1 * p[j - 1]
            q[cc] = p[cc] + 0.1 * p[cc - 1]
            p = q
        assert p[cc] * T.PPM <= T.consecutive_bound_ppm(cc, 1000, n)
        assert p[cc] * T.PPM == pytest.approx(T.consecutive_bound_ppm(cc, 1000, n), rel=0.05)


def test_the_frozen_thresholds():
    """The thresholds frozen by the selection procedure (evaluation/trust_qualification.py select), and the limits they
    give at the declared leads."""
    assert (TS.policy, TS.acute_recent_windows, TS.acute_consecutive_misses) == (c.TRUST_POLICY_REVISION, FROZEN["n"], 5)
    assert (TS.rolling_alarm_ppm, TS.consecutive_alarm_ppm, TS.day_alarm_ppm) == \
        (FROZEN["rolling_ppm"], FROZEN["consecutive_ppm"], FROZEN["day_ppm"])
    assert TS.rolling_alarm_ppm + TS.consecutive_alarm_ppm + TS.day_alarm_ppm <= c.COVERAGE_ALARM_BUDGET_PPM == 50_000
    over = dataclasses.replace(c.ForecastSettings(), trust=dataclasses.replace(TS, day_alarm_ppm=8_001))
    with pytest.raises(c.ContractRefused, match="budgets exceed"):
        c._check_feasible(over)
    assert (TS.coverage_tolerance_bps, TS.worst_day_min_outcomes, TS.repromotion_hours, TS.repromotion_windows) == \
        (500, 24, 24, 32)                                                        # unchanged: 7-day gate, dwell
    for lead, families, k, run in FROZEN["limits"]:
        lim = limits(lead, families)
        assert (lim.recent_n, lim.rolling_misses, lim.consecutive, lim.looks) == \
            (FROZEN["n"], k, run, 7 * DAY // lead), (lead, families)
    with pytest.raises(T.TrustError):
        T.coverage_limits(dataclasses.replace(TS, policy="last-32-at-85-percent"), 180, 1000, 2)


# (lead, families, rolling limit k of n, consecutive limit c)
FROZEN = {"n": 64, "rolling_ppm": 35000, "consecutive_ppm": 7000, "day_ppm": 8000,
          "limits": ((180, 1, 19, 6), (180, 2, 19, 6), (300, 1, 18, 6), (300, 2, 19, 6), (1200, 1, 17, 5),
                     (1200, 2, 18, 6))}


# --- the permanent guard of the operating characteristics (T5) ----------------------------------------------------------

@pytest.mark.parametrize("lead", [180, 300])
@pytest.mark.parametrize("families", [1, 2])
def test_the_exact_false_alarm_budget_of_a_target_is_within_five_percent(lead, families):
    """Under independent misses at the nominal rate, P(any rolling, consecutive or day alarm of any enabled family
    within 7 days) is at most the sum of the rules' union bounds: exact arithmetic, no simulation."""
    lim = limits(lead, families)
    looks, n, k = lim.looks, lim.recent_n, lim.rolling_misses
    rolling = looks * A * tail(n - 1, k - 1)
    consecutive = (looks * (1 - A) + 1) * A ** lim.consecutive
    per_day = DAY // lead
    day = 7 * tail(per_day, lim.day_misses(per_day))
    total = families * (rolling + consecutive + day)
    assert total <= Fraction(5, 100), float(total)
    ub = Q.union_bound_ppm(TS, lead, families)
    assert ub["total"] == pytest.approx(float(total) * T.PPM, rel=1e-9)
    assert families * rolling <= Fraction(TS.rolling_alarm_ppm, T.PPM)
    assert families * consecutive <= Fraction(TS.consecutive_alarm_ppm, T.PPM)
    assert families * day <= Fraction(TS.day_alarm_ppm, T.PPM)


@pytest.mark.parametrize("lead", [180, 300])
@pytest.mark.parametrize("families", [1, 2])
def test_a_sustained_forty_percent_miscoverage_is_detected_within_64_outcomes(lead, families):
    """Fixed seed, 60,000 independent trials: the lower end of the 95 % interval is at least 95 %. Also the 50 % case
    and the all-miss step (detected by the consecutive rule after exactly c outcomes)."""
    lim = limits(lead, families)
    d = Q.detection(lim, 0.4, trials=60_000, seed=20261110)
    assert d["lo"] >= 0.95, d
    assert d["failing_before_change"] < 0.001
    assert Q.detection(lim, 0.5, trials=20_000, seed=20261110)["lo"] >= 0.99
    step = Q.detection(lim, 1.0, trials=2_000, seed=20261110)
    assert step["probability"] == 1.0 and step["p95_outcomes"] <= lim.consecutive
    assert Q.wall_seconds(lim.consecutive, lead, 60) == lim.consecutive * lead + 180


def test_the_simulated_detection_is_the_products_own_rule():
    """The vectorized detection simulation and trust.note_outcome agree miss sequence by miss sequence."""
    lim = limits(180, 2)
    rng = np.random.default_rng(7)
    for trial in range(40):
        seq = np.concatenate([rng.random(4 * lim.recent_n) < 0.1, rng.random(64) < 0.4])
        f = Feed(lim, lead=180)
        first = None
        for i, miss in enumerate(seq):
            f.window(START + i * 180, covered=not miss)
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


def test_the_simulated_false_alarm_rate_at_nominal_stays_inside_the_budget():
    """Fixed seeds, independent nominal misses through the coverage gate and the hysteresis (fast_monitor, which the
    next test ties to the product's functions): the upper end of the 95 % interval of the TARGET's false-alarm
    probability (two families) is within 5 %."""
    alarms = trusted = 0
    for seed in range(400):
        r = Q.fast_monitor(bernoulli_outcomes(seed, 300, 60, 10), TS, 2)
        trusted += r.trusted_at is not None
        alarms += r.false_alarm is not None
    assert trusted == 400
    lo, hi = Q.wilson(alarms, trusted)
    assert 2 * hi <= 0.05, (alarms, trusted)


@pytest.mark.parametrize("seed, burst", [(1, None), (2, (160, 60, 0.5)), (3, (200, 30, 1.0)), (4, (150, 300, 0.3))])
def test_the_fast_monitor_is_the_products_trust_evaluation(seed, burst):
    """The product's functions at every request (issuances, outcomes, evaluate with its hysteresis; the state
    through JSON every 97 requests, as after a restart) and the event-driven simulation give the same first trust,
    the same demotions by the same rules at the same requests and the same re-promotions."""
    out = bernoulli_outcomes(seed, 1200, 300, 9, burst=burst)
    fast = Q.fast_monitor(out, TS, 2)
    real = Q.real_monitor(out, TS, 2, restart_every=97)
    assert fast.trusted_at == real.trusted_at == out.first_bound_origin + 48 * H
    assert [(d["at"], d["rules"], d["repromoted_at"]) for d in fast.demotions] == \
        [(d["at"], d["rules"], d["repromoted_at"]) for d in real.demotions]
    if burst:
        assert fast.demotions and fast.false_alarm is not None


def test_the_qualification_declarations_are_fixed():
    assert Q.DEVELOPMENT_SEEDS == tuple(range(200_000, 201_000))
    assert Q.VALIDATION_SEEDS == tuple(range(950_000, 951_000))
    assert (Q.PATHS_DEPENDENT, Q.PATHS_REPORTED) == (500, 300)
    assert not set(Q.DEVELOPMENT_SEEDS) & set(Q.VALIDATION_SEEDS)
    assert Q.DETECTION_DEVELOPMENT_SEED != Q.DETECTION_VALIDATION_SEED
    assert (Q.FALSE_ALARM_TARGET, Q.DETECTION_TARGET, Q.DETECTION_OUTCOMES) == (0.05, 0.95, 64)
    assert Q.CONFIGS == ((60, 180, True), (60, 300, True), (300, 1200, False))
    assert Q.NOMINAL_MODELS == ("additive", "proportional", "mixed")
    assert Q.reading_at_most(0.01, 0.02, 0.03, 0.05) == "MET" and Q.reading_at_most(0.03, 0.04, 0.06, 0.05) == \
        "INCONCLUSIVE" and Q.reading_at_most(0.05, 0.06, 0.07, 0.05) == "NOT MET"
    assert Q.reading_at_least(0.96, 0.97, 0.98, 0.95) == "MET" and Q.reading_at_least(0.93, 0.94, 0.96, 0.95) == \
        "NOT MET"


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
    run = f.lim.consecutive                                           # 6 at this lead with two families
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
    assert why["coverage"]["consecutive"] == [run, run] and run == 6 > TS.acute_consecutive_misses
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


# --- the seven-day gate and the dwell are unchanged ---------------------------------------------------------------------

def test_the_seven_day_gate_and_the_repromotion_dwell_are_unchanged():
    low = Feed().run(72, covered=lambda t: (t // L) % 6 != 0)        # 83.3 % < 85 %, 11 misses in every 64
    gates, info = T.candidate_gates(inputs(low), glob(START + 72 * H), TS)
    assert not gates["coverage"] and info["coverage"]["failing"] == ["seven_day"]
    assert info["coverage"]["seven_day"] == [360, 432]
    ok = Feed().run(72, covered=lambda t: (t // L) % 8 != 0)         # 87.5 %
    assert T.candidate_gates(inputs(ok), glob(START + 72 * H), TS)[0]["coverage"]
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
    assert canon["policy"] == c.TRUST_POLICY_REVISION == "budgeted-coverage-tests/v1"
    assert (canon["rolling_alarm_ppm"], canon["consecutive_alarm_ppm"], canon["day_alarm_ppm"]) == \
        (TS.rolling_alarm_ppm, TS.consecutive_alarm_ppm, TS.day_alarm_ppm) == (35_000, 7_000, 8_000)
    assert (canon["acute_recent_windows"], canon["acute_consecutive_misses"]) == (64, 5)
    assert s.trust.policy == c.TRUST_POLICY_REVISION                 # the resolved-settings field
    for name, value in (("rolling_alarm_ppm", 1), ("consecutive_alarm_ppm", 1), ("day_alarm_ppm", 1),
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
    for key_name in ("policy", "rollingAlarmPpm", "consecutiveAlarmPpm", "dayAlarmPpm"):   # not in the spec
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
    body = {"schema": c.EVIDENCE_SCHEMA, "autoscaler_uid": pa["metadata"]["uid"],
            "target_uid": pa["status"]["metricSource"]["targetUID"],
            "query_sha256": pa["status"]["metricSource"]["sha256"], "signal_contract": svc_helpers.CONTRACT,
            "forecast_contract": c.FORECAST_CONTRACT, "interval_seconds": s.sample_interval_seconds,
            "lead_seconds": lead_seconds, "engine": s.engine, "seasonality": s.seasonality,
            "history_days": s.history_days, "settlement_seconds": s.settlement_seconds,
            "outcome_grace_seconds": s.outcome_grace_seconds, "risk": dataclasses.asdict(s.risk),
            "calibration": dataclasses.asdict(s.calibration)}
    return c.sha256_hex(c.canonical_json(canon)), c.sha256_hex(c.canonical_json(body))


def test_a_peer_of_the_earlier_trust_policy_gets_no_forecast(tmp_path):
    svc, clock = svc_helpers.make(tmp_path)
    old_sha, old_ek = pre_policy_identity(svc_helpers.pa_object(), svc_helpers.LEAD)
    bound = c.bind_request(svc_helpers.body(svc_helpers.T0), svc_helpers.Reader(),
                           now_ms=svc_helpers.T0 * 1000 + 10_000)
    assert bound.settings_sha256 != old_sha and bound.evidence_key != old_ek
    clock.set(svc_helpers.T0 * 1000 + 10_000)
    with pytest.raises(ServiceError) as e:
        svc.predict(svc_helpers.body(svc_helpers.T0, settings=old_sha))
    assert (e.value.status, e.value.reason) == (422, "IdentityMismatch") and svc.store.issuances() == []


def test_an_old_trusted_state_does_not_survive_the_policy_change(tmp_path):
    """A journal holding a Trusted checkpoint of the earlier trust policy (its own evidence key, the earlier state and
    ledger schemas): this policy starts a new epoch with no evidence and no trust, and leaves the old one alone."""
    svc, clock = svc_helpers.make(tmp_path)
    _, old_ek = pre_policy_identity(svc_helpers.pa_object(), svc_helpers.LEAD)
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
