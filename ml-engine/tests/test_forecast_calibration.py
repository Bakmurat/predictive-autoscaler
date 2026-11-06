"""C2.4: window calibration: hand-computed order statistics, ties, rank overflow, cold start, risk/lead resets, delayed
outcomes, exactly-once level updates, no calibration from capped actions, dependence and drift (measured, no guarantee),
and the replay issuing calibrated bounds from matured outcomes only."""

import dataclasses
import json
import math
import os
import random
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from evaluation import datasets as ds  # noqa: E402
from evaluation.forecast_replay import ReplayConfig, run_replay  # noqa: E402
from forecasting import calibration as cal  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting.calibration import CalibrationError, CalibrationPolicy, CalibrationRecord  # noqa: E402
from forecasting.clock import ManualClock  # noqa: E402
from forecasting.maturation import check_window, window_outcome  # noqa: E402
from forecasting.store import ForecastStore, Issuance, IssuanceKey, Outcome  # noqa: E402
from tests.b4_helpers import make_signal  # noqa: E402

ML_ENGINE = Path(__file__).resolve().parent.parent
L = 600
EPOCH = "e" * 64


def policy(**kw):
    base = dict(lead_seconds=L, nominal_bps=1000, eta_bps=50, min_bps=100, max_bps=2500, min_windows=4,
                max_scores=50, retention_seconds=7 * 86400)
    base.update(kw)
    return CalibrationPolicy(**base)


def rec(seq, y, base=0.0, upper=None, origin=None, status="scored", epoch=EPOCH):
    origin = seq * L if origin is None else origin
    return CalibrationRecord(seq, epoch, "m", "r", f"iss-{seq}", origin, origin + L, base, upper, status,
                             y if status == "scored" else None)


def seeded(scores, p=None, **kw):
    """A state holding exactly these cohort scores (cold-start records: no bound, so the level does not move)."""
    p = p or policy(**kw)
    s = cal.initial(p, EPOCH, "m", "r")
    for i, e in enumerate(scores, 1):
        s = cal.apply(s, rec(i, y=10.0 + e, base=10.0), p)
    return s, p


NOW = 200 * L                    # every seeded window has ended and is within the 7-day retention


# --- order statistic, hand-computed --------------------------------------------------------------------------------

def test_hand_computed_positive_and_negative_residuals():
    scores = [-3.0, -1.0, 0.5, 2.0, 4.0, -2.0, 1.0, 3.0, -0.5]           # sorted: -3 -2 -1 -0.5 0.5 1 2 3 4
    s, p = seeded(scores)
    u, info = cal.upper_bound(s, p, 10.0, NOW)                          # a = 0.10: k = ceil(10 x 0.9) = 9
    assert (info["n"], info["rank"], info["quantile_rps"], u) == (9, 9, 4.0, 14.0)
    s2, p2 = seeded(scores, nominal_bps=2000)                           # a = 0.20: k = ceil(10 x 0.8) = 8
    u2, info2 = cal.upper_bound(s2, p2, 10.0, NOW)
    assert (info2["rank"], info2["quantile_rps"], u2) == (8, 3.0, 13.0)
    s3, p3 = seeded([-5.0, -4.0, -3.0, -2.0, -1.0], nominal_bps=2500)   # a = 0.25: k = ceil(6 x 0.75) = 5
    u3, info3 = cal.upper_bound(s3, p3, 3.0, NOW)
    assert (info3["quantile_rps"], u3) == (-1.0, 2.0)                    # a negative quantile lowers the bound
    assert cal.upper_bound(s3, p3, 0.5, NOW)[0] == 0.0                   # ... clipped at zero demand, never below


def test_ties_are_values_not_positions():
    scores = [1.0, 3.0, 1.0, 2.0, 1.0, 3.0, 1.0, 3.0]                   # sorted: 1 1 1 1 2 3 3 3
    for order in (scores, sorted(scores), sorted(scores, reverse=True)):
        s, p = seeded(order, nominal_bps=2500)                           # k = ceil(9 x 0.75) = 7 -> 3
        assert cal.upper_bound(s, p, 0.0, NOW)[1]["quantile_rps"] == 3.0
    s, p = seeded([2.0] * 6, nominal_bps=2500)
    assert cal.upper_bound(s, p, 1.0, NOW)[0] == 3.0


def test_rank_overflow_gives_no_bound():
    s, p = seeded([float(i) for i in range(9)], nominal_bps=500, min_bps=100)   # k = ceil(10 x 0.95) = 10 > 9
    u, info = cal.upper_bound(s, p, 1.0, NOW)
    assert u is None and info["unavailable"] == "RankOverflow" and info["rank"] == 10
    s, p = seeded([float(i) for i in range(19)], nominal_bps=500, min_bps=100)  # k = ceil(20 x 0.95) = 19
    assert cal.upper_bound(s, p, 1.0, NOW)[0] == 19.0
    assert cal.rank(128, 1_000_000) == 128 and cal.rank(128, 500_000) == 129    # a = 0.01 fits 128; 0.005 does not


def test_cold_start_seeds_scores_but_claims_nothing():
    p = policy(min_windows=5)
    s = cal.initial(p, EPOCH, "m", "r")
    for i in range(1, 5):
        s = cal.apply(s, rec(i, y=2.0, base=1.0), p)                     # issued without a bound
        assert cal.upper_bound(s, p, 1.0, NOW) == (None, {"n": i, "rank": cal.rank(i, s["a_e8"]),
                                                           "working_miscoverage": 0.1,
                                                           "unavailable": "CalibrationInsufficient"})
    assert s["level_updates"] == 0 and s["a_e8"] == 10_000_000 and cal.coverage(s)["ratio"] is None
    s = cal.apply(s, rec(5, y=2.0, base=1.0), p)
    assert cal.upper_bound(s, p, 1.0, NOW)[0] is None                    # k = ceil(6 x 0.9) = 6 > 5: overflow
    s25, p25 = seeded([1.0] * 6, min_windows=5, nominal_bps=2500)       # k = ceil(7 x 0.75) = 6 <= 6
    assert cal.upper_bound(s25, p25, 1.0, NOW)[0] == 2.0


# --- the adaptive level -------------------------------------------------------------------------------------------

def test_level_updates_are_exact_and_clipped():
    p = policy()
    s, _ = seeded([0.0] * 4, p)
    s = cal.apply(s, rec(10, y=5.0, base=1.0, upper=4.0), p)           # miss: a += 0.005 (0.1 - 1) = -0.0045
    assert s["a_e8"] == 10_000_000 - 450_000 and s["gate"] == {**s["gate"], "bounded": 1, "covered": 0}
    s = cal.apply(s, rec(11, y=3.0, base=1.0, upper=4.0), p)           # covered: a += 0.005 x 0.1 = +0.0005
    assert s["a_e8"] == 10_000_000 - 450_000 + 50_000 and s["level_updates"] == 2
    for i in range(12, 40):                                             # many misses: clipped at a_min, counted
        s = cal.apply(s, rec(i, y=5.0, base=1.0, upper=4.0), p)
    assert s["a_e8"] == 1_000_000 and s["at_bound"] > 0
    assert cal.pinball_rps(5.0, 4.0, 0.1) == pytest.approx(0.9) and cal.pinball_rps(3.0, 4.0, 0.1) == pytest.approx(0.1)
    assert cal.coverage(s)["ratio"] == pytest.approx(1 / 30)                         # 1 covered of 30 bounds


def test_each_outcome_moves_the_level_exactly_once():
    p = policy()
    s, _ = seeded([0.0] * 4, p)
    once = cal.apply(s, rec(10, y=5.0, base=1.0, upper=4.0), p)
    assert cal.apply(once, rec(10, y=5.0, base=1.0, upper=4.0), p) is once           # the same seq: no change
    assert cal.apply(once, rec(9, y=5.0, base=1.0, upper=4.0), p) is once            # an older seq: ignored
    assert once["level_updates"] == 1


def test_only_cohort_windows_calibrate():
    p = policy(lead_seconds=3 * L)
    s = cal.initial(p, EPOCH, "m", "r")
    s = cal.apply(s, rec(1, y=2.0, base=1.0, upper=1.5, origin=3 * L), p)            # cohort: origin mod L = 0
    s = cal.apply(s, rec(2, y=2.0, base=1.0, upper=1.5, origin=4 * L), p)            # overlapping: diagnostics only
    assert len(s["scores"]) == 1 and s["level_updates"] == 1
    assert s["all"]["bounded"] == 2 and s["gate"]["bounded"] == 1


def test_unscorable_windows_neither_score_nor_move_the_level():
    p = policy()
    s, _ = seeded([0.0] * 4, p)
    s2 = cal.apply(s, rec(10, y=None, status="unscorable", upper=4.0), p)
    assert s2["scores"] == s["scores"] and s2["a_e8"] == s["a_e8"] and s2["gate"]["unscorable"] == 1


def test_delayed_outcomes_apply_when_they_mature_and_never_rewrite_issued_bounds():
    p = policy(min_windows=3, nominal_bps=2500)
    s = cal.initial(p, EPOCH, "m", "r")
    s = cal.apply(s, rec(1, y=1.0, base=0.0, origin=1 * L), p)
    s = cal.apply(s, rec(2, y=3.0, base=0.0, origin=3 * L), p)
    s = cal.apply(s, rec(3, y=2.0, base=0.0, origin=5 * L), p)
    issued_before, _ = cal.upper_bound(s, p, 0.0, NOW)                                # k = ceil(4 x 0.75) = 3
    late = cal.apply(s, rec(4, y=9.0, base=0.0, origin=2 * L), p)                    # matured late, older window
    assert [x[0] for x in late["scores"]] == [2 * L, 3 * L, 4 * L, 6 * L]           # in window-end place
    assert issued_before == 3.0                                                      # the earlier bound stays
    assert cal.upper_bound(late, p, 0.0, NOW)[0] == 9.0                              # k = ceil(5 x 0.75) = 4
    # Scores leave the buffer by window end: older than the retention before now (or than the newest window).
    assert cal.buffered_scores(late, p, 4 * L + 7 * 86400 - 1) == [3.0, 2.0]        # windows ending 4L and 6L
    stale = cal.apply(late, rec(5, y=1.0, base=0.0, origin=6 * L + 8 * 86400), p)
    assert [x[0] for x in stale["scores"]] == [6 * L + 8 * 86400 + L]               # the old ones aged out


def test_scores_that_had_not_matured_by_issue_time_are_not_used():
    s, p = seeded([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])               # window ends L*2 .. L*11
    assert len(cal.buffered_scores(s, p, 6 * L)) == 5


# --- epochs: risk and lead resets ---------------------------------------------------------------------------------

def test_a_risk_or_lead_change_is_a_new_epoch_starting_at_nominal():
    sig = make_signal()
    base = c.ForecastSettings()
    keys = {c.evidence_key(sig, base, 1200), c.evidence_key(sig, dataclasses.replace(base, risk=c.RiskSettings(500)),
                                                            1200), c.evidence_key(sig, base, 900)}
    assert len(keys) == 3
    p = CalibrationPolicy.from_settings(dataclasses.replace(base, risk=c.RiskSettings(500)), 900)
    assert (p.nominal_bps, p.lead_seconds, p.min_windows) == (500, 900, 128)
    old = cal.apply(cal.initial(policy(), EPOCH, "m", "r"), rec(1, y=1.0), policy())
    fresh = cal.initial(p, "f" * 64, "m", "r")
    assert fresh["a_e8"] == 5_000_000 and fresh["scores"] == []
    with pytest.raises(CalibrationError):
        cal.apply(fresh, rec(2, y=1.0), p)                                            # a record of the old epoch
    assert old["epoch"] == EPOCH and cal.in_cohort(1800, 900) and not cal.in_cohort(1800, 1200)


# --- only issued forecasts and matured outcomes; never capped actions ------------------------------------------

def journal_pair(upper=4.0, scored_upper=None, extra=None, status="scored", y=5.0):
    origin = 100 * L
    targets = [origin + L]
    bundle = {"decision_origin": origin, "target_timestamps": targets, "sample_interval_seconds": L,
              "model_family": "m", "model_recipe": "r", "point_rps": [1.0],
              "base_window_max_rps": 1.0, "upper_window_max_rps": upper, **(extra or {})}
    data = c.canonical_json(bundle)
    key = IssuanceKey(EPOCH, origin, origin, origin + L, "5" * 64)
    iss = Issuance(1, "iss-1", key, data, c.sha256_hex(data), ("m",), 0)
    res = window_outcome(targets=targets, base_rps=1.0, upper_rps=upper if scored_upper is None else scored_upper,
                         received={targets[0]: (y, (origin + L) * 1000)}, now_ms=(origin + 3 * L) * 1000,
                         settlement_s=0, grace_s=0)
    payload = c.canonical_json(res[1])
    return iss, Outcome(7, "iss-1", EPOCH, origin + L, "m", status, payload, c.sha256_hex(payload), 0)


def test_records_come_only_from_an_issuance_and_its_matured_outcome():
    iss, out = journal_pair()
    r = CalibrationRecord.from_journal(iss, out)
    assert (r.seq, r.base_rps, r.upper_rps, r.y_rps, r.decision_origin) == (7, 1.0, 4.0, 5.0, 100 * L)
    assert set(CalibrationRecord.__dataclass_fields__) == {"seq", "epoch", "model", "recipe", "issuance_id",
                                                           "decision_origin", "window_end", "base_rps", "upper_rps",
                                                           "status", "y_rps"}
    other = dataclasses.replace(out, issuance_id="iss-2")
    with pytest.raises(CalibrationError):
        CalibrationRecord.from_journal(iss, other)


def test_a_capped_action_never_calibrates():
    # The operator capped the predictive replicas (say, an effective capacity of 2.0 RPS instead of the bound 4.0).
    # Scoring against that capped value is refused; decision-ledger fields in a bundle are ignored.
    iss, out = journal_pair(scored_upper=2.0)
    with pytest.raises(CalibrationError):
        CalibrationRecord.from_journal(iss, out)
    plain = CalibrationRecord.from_journal(*journal_pair())
    ledger = CalibrationRecord.from_journal(*journal_pair(extra={"capped_upper_rps": 2.0, "predictive_replicas": 3,
                                                                 "capped_replicas": 2, "cap_bound": True}))
    assert plain == ledger
    p = policy(lead_seconds=L)
    s, _ = seeded([0.0] * 4, p)
    assert cal.apply(s, plain, p) == cal.apply(s, ledger, p)


def test_targets_must_be_the_exact_window():
    for bad in ([], [5, 5], [3, 1], [1, 2, 4], [1.0]):
        with pytest.raises(ValueError):
            check_window(bad)
    iss, out = journal_pair()
    b = json.loads(iss.bundle)
    b["target_timestamps"] = [b["decision_origin"] + 2 * L]
    data = c.canonical_json(b)
    with pytest.raises(CalibrationError):
        CalibrationRecord.from_journal(dataclasses.replace(iss, bundle=data, bundle_sha256=c.sha256_hex(data)), out)


# --- dependence and drift: measured behaviour, no guarantee --------------------------------------------------------

def simulate(eta_bps, shift_at, n=3000, phi=0.8, shift=3.0, seed=1, measure_from=1200):
    """Every origin a cohort window (lead = Δ), outcomes one step late. Residuals are AR(1)-dependent with a level shift
    at `shift_at`. Returns the coverage of issued bounds after the shift settles."""
    rng = random.Random(seed)
    p = policy(eta_bps=eta_bps, min_windows=64, max_scores=256, nominal_bps=1000)
    s = cal.initial(p, EPOCH, "m", "r")
    pending, hits, total, z = None, 0, 0, 0.0
    for i in range(1, n + 1):
        t = i * L
        if pending is not None:                                         # the previous window matures now
            s = cal.apply(s, pending, p)
        z = phi * z + rng.gauss(0, 1)
        y = z + (shift if i >= shift_at else 0.0) + 10.0
        u, _ = cal.upper_bound(s, p, 10.0, t)
        if u is not None and i > measure_from:
            total += 1
            hits += int(y <= u)
        pending = CalibrationRecord(i, EPOCH, "m", "r", f"iss-{i}", t - L, t, 10.0, u, "scored", y)
    return hits / total, s


def test_dependence_and_drift_are_measured_not_guaranteed():
    """Under AR(1) dependence and a level shift, the adaptive level reacts and stays within its bounds; nothing here
    is a coverage guarantee (the shift dominates the buffer for a while, and dependent windows are not iid)."""
    fixed, s_fixed = simulate(eta_bps=0, shift_at=1000)
    adaptive, s_adapt = simulate(eta_bps=50, shift_at=1000)
    assert s_fixed["a_e8"] == 10_000_000                                 # eta = 0: the level never moves
    assert 1_000_000 <= s_adapt["a_e8"] <= 25_000_000
    assert abs(adaptive - 0.9) <= abs(fixed - 0.9) + 0.02                 # it does not do worse than a fixed level
    calm, _ = simulate(eta_bps=50, shift_at=10 ** 9)                      # no shift: near nominal, measured only
    assert 0.8 < calm < 0.97


# --- the replay issues bounds from matured cohort outcomes only ---------------------------------------------------

DAY = 86400
START = 1_793_923_200 - 1_793_923_200 % DAY


def replay(tmp_path, days=4, **kw):
    d = ds.load_dataset(ds.synthetic_dataset(str(tmp_path / "data"), "cal", start=START, days=days, interval=600,
                                             seed=7))
    cfg = ReplayConfig(first_origin=START + DAY, last_origin=START + days * DAY - 600, lead_seconds=1800,
                       min_windows=16, max_scores=64, **kw)
    s = ForecastStore(str(tmp_path / "j.sqlite"), clock=ManualClock(0), store_id="replay-store")
    return run_replay(d, cfg, s), s, cfg


def test_replay_bounds_are_recomputable_from_the_journal(tmp_path):
    r, s, cfg = replay(tmp_path)
    issued = s.issuances()
    by_id = {i.issuance_id: i for i in issued}
    outs = s.outcomes_after(r.evidence_key, 0)
    bounded = [i for i in issued if i.payload()["upper_window_max_rps"] is not None]
    assert bounded and bounded[0].key.decision_origin > issued[0].key.decision_origin
    for i in bounded[::7]:
        t = i.key.decision_origin
        matured = [o for o in outs if o.committed_at_ms <= t * 1000 and o.status == "scored"
                   and by_id[o.issuance_id].key.decision_origin % cfg.lead_seconds == 0]
        a = cfg.miscoverage_bps * 10_000                              # replay the level from those outcomes alone
        for o in matured:
            u = by_id[o.issuance_id].payload()["upper_window_max_rps"]
            if u is not None:
                miss = int(o.payload()["y_window_max_rps"] > u)
                a = min(cfg.max_miscoverage_bps * 10_000, max(cfg.min_miscoverage_bps * 10_000,
                                                              a + cfg.eta_bps * (cfg.miscoverage_bps - miss * 10_000)))
        recent = sorted(matured, key=lambda o: o.window_end)[-cfg.max_scores:]       # the newest 64 by window end
        scores = sorted(o.payload()["y_window_max_rps"] - o.payload()["base_window_max_rps"] for o in recent)
        k = cal.rank(len(scores), a)
        p = i.payload()
        assert p["calibration"]["n"] == len(scores) and p["calibration"]["rank"] == k
        assert p["upper_window_max_rps"] == max(0.0, p["base_window_max_rps"] + scores[k - 1])
    cp = r.checkpoint["calibration"]
    assert cp["level_updates"] == cp["gate"]["bounded"] and cp["last_seq"] == outs[-1].seq
    assert r.checkpoint["covered"] + r.checkpoint["missed"] == cp["all"]["bounded"]


RESUME = textwrap.dedent("""
    import os, sys
    from evaluation.datasets import load_dataset
    from evaluation.forecast_replay import ReplayConfig, run_replay
    from forecasting.clock import ManualClock
    from forecasting.store import ForecastStore
    manifest, path, nth, first, last = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
    s = ForecastStore(path, clock=ManualClock(0), store_id="replay-store")
    seen = [0]
    def hook(st):
        if st == "after_commit":
            seen[0] += 1
            if seen[0] == nth:
                os._exit(17)
    s._fault_hook = hook
    run_replay(load_dataset(manifest), ReplayConfig(first_origin=first, last_origin=last, lead_seconds=1800,
                                                    min_windows=16, max_scores=64), s)
    os._exit(0)
""")


@pytest.mark.parametrize("nth", [700, 1100])
def test_a_killed_calibrating_replay_resumes_with_each_level_update_once(tmp_path, nth):
    whole, s, cfg = replay(tmp_path / "whole")
    s.close()
    manifest = str(tmp_path / "whole" / "data" / "cal.manifest.json")
    env = {**{k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}, "PYTHONPATH": str(ML_ENGINE),
           "KUBECONFIG": "/dev/null"}
    p = subprocess.run([sys.executable, "-c", RESUME, manifest, str(tmp_path / "crashed.sqlite"), str(nth),
                        str(cfg.first_origin), str(cfg.last_origin)], cwd=ML_ENGINE, env=env, capture_output=True,
                       text=True, timeout=300)
    assert p.returncode == 17, p.stderr[-2000:]
    s = ForecastStore(str(tmp_path / "crashed.sqlite"), clock=ManualClock(0), store_id="replay-store")
    resumed = run_replay(ds.load_dataset(manifest), cfg, s)
    assert resumed.journal_sha256 == whole.journal_sha256 and resumed.checkpoint == whole.checkpoint
    assert resumed.checkpoint["calibration"]["level_updates"] > 0


# --- per-model calibration and the issued grid (Codex review on #23) --------------------------------------------

from forecasting.maturation import candidate, issued_outcome, targets_sha256  # noqa: E402

TWO = ("seasonal_naive", "statistical")


def two_model_pair(i, y, uppers, seq0):
    """An issuance with two candidates on the same window, and each model's outcome matured from the bundle."""
    origin = i * L
    targets = [origin + L]
    cands = {m: {"recipe": f"{m}/v1", "point_rps": [1.0], "base_window_max_rps": 1.0, "upper_window_max_rps": u}
             for m, u in zip(TWO, uppers)}
    bundle = {"decision_origin": origin, "target_timestamps": targets, "sample_interval_seconds": L,
              "candidates": cands}
    data = c.canonical_json(bundle)
    iss = Issuance(i, f"iss-{i}", IssuanceKey(EPOCH, origin, origin, origin + L, "5" * 64), data, c.sha256_hex(data),
                   TWO, 0)
    outs = []
    for k, m in enumerate(TWO):
        status, payload = issued_outcome(bundle, m, received={targets[0]: (y, (origin + L) * 1000)},
                                         now_ms=(origin + 3 * L) * 1000, settlement_s=0, grace_s=0)
        pb = c.canonical_json(payload)
        outs.append(Outcome(seq0 + k, iss.issuance_id, EPOCH, origin + L, m, status, pb, c.sha256_hex(pb), 0))
    return iss, outs


def test_two_models_never_pool_their_windows():
    # Codex repro: 64 windows x 2 models made 128 scores and a bound. Each model keeps its own 64.
    p = policy(min_windows=128, max_scores=512)
    states = {m: cal.initial(p, EPOCH, m, f"{m}/v1") for m in TWO}
    seq = 1
    for i in range(1, 65):
        iss, outs = two_model_pair(i, y=5.0, uppers=(None, None), seq0=seq)
        seq += 2
        for o in outs:
            r = CalibrationRecord.from_journal(iss, o)
            states[o.model] = cal.apply(states[o.model], r, p)
            other = next(m for m in TWO if m != o.model)
            with pytest.raises(CalibrationError):                 # never across models
                cal.apply(states[other], r, p)
    for m in TWO:
        assert len(states[m]["scores"]) == 64 and cal.upper_bound(states[m], p, 1.0, 100 * L)[0] is None


def test_each_model_has_its_own_level_and_bound():
    p = policy(min_windows=4)
    states = {m: cal.initial(p, EPOCH, m, f"{m}/v1") for m in TWO}
    seq = 1
    for i in range(1, 9):                                         # naive's bound 2.0 always missed, statistical's 9.0 held
        iss, outs = two_model_pair(i, y=5.0 + (i % 3), uppers=(2.0, 9.0), seq0=seq)
        seq += 2
        for o in outs:
            r = CalibrationRecord.from_journal(iss, o)
            assert (r.model, r.recipe, r.upper_rps) == (o.model, f"{o.model}/v1", 2.0 if o.model == TWO[0] else 9.0)
            states[o.model] = cal.apply(states[o.model], r, p)
    naive, stat = states[TWO[0]], states[TWO[1]]
    assert naive["a_e8"] == 10_000_000 - 8 * 450_000 and stat["a_e8"] == 10_000_000 + 8 * 50_000
    assert cal.coverage(naive)["ratio"] == 0.0 and cal.coverage(stat)["ratio"] == 1.0
    assert naive["level_updates"] == stat["level_updates"] == 8


def test_an_outcome_selects_only_its_own_candidate():
    iss, outs = two_model_pair(1, y=5.0, uppers=(2.0, 9.0), seq0=1)
    b = json.loads(iss.bundle)
    with pytest.raises(ValueError):
        candidate(b, "lstm")
    stranger = dataclasses.replace(outs[0], model="lstm")
    with pytest.raises(CalibrationError):
        CalibrationRecord.from_journal(iss, stranger)
    swapped = dataclasses.replace(outs[0], model=TWO[1])           # naive's outcome relabelled as statistical's
    with pytest.raises(CalibrationError):
        CalibrationRecord.from_journal(iss, swapped)
    single = {"model_family": "m", "model_recipe": "r", "point_rps": [1.0], "base_window_max_rps": 1.0,
              "upper_window_max_rps": None}
    with pytest.raises(ValueError):
        candidate(single, "other")


def grid_pair(scored_targets, truth=(100.0, 1.0), upper=4.0):
    """Two targets issued (truth 100 then 1); the outcome is matured on `scored_targets` instead."""
    origin = 100 * L
    targets = [origin + L, origin + 2 * L]
    bundle = {"decision_origin": origin, "target_timestamps": targets, "sample_interval_seconds": L,
              "model_family": "m", "model_recipe": "r", "point_rps": [1.0, 1.0], "base_window_max_rps": 1.0,
              "upper_window_max_rps": upper}
    data = c.canonical_json(bundle)
    iss = Issuance(1, "iss-1", IssuanceKey(EPOCH, origin, origin, origin + 2 * L, "5" * 64), data, c.sha256_hex(data),
                   ("m",), 0)
    received = {t: (v, (origin + 2 * L) * 1000) for t, v in zip(targets, truth)}
    received.update({origin + 3 * L: (1.0, 0), origin + 4 * L: (1.0, 0)})
    status, payload = window_outcome(targets=scored_targets, base_rps=1.0, upper_rps=upper, received=received,
                                     now_ms=(origin + 9 * L) * 1000, settlement_s=0, grace_s=0)
    pb = c.canonical_json(payload)
    return iss, Outcome(5, "iss-1", EPOCH, origin + 2 * L, "m", status, pb, c.sha256_hex(pb), 0), bundle, received


@pytest.mark.parametrize("shape", ["last_only", "shifted", "wrong_cadence"])
def test_a_window_scored_on_another_grid_is_refused(shape):
    origin = 100 * L
    scored = {"last_only": [origin + 2 * L],                       # Codex repro: Y = 1, "covered", true max 100
              "shifted": [origin + 2 * L, origin + 3 * L],
              "wrong_cadence": [origin + 2 * L, origin + 4 * L]}[shape]
    iss, out, _, _ = grid_pair(scored)
    if shape == "last_only":
        assert out.payload()["covered"] is True and out.payload()["y_window_max_rps"] == 1.0
    with pytest.raises(CalibrationError):
        CalibrationRecord.from_journal(iss, out)


def test_maturing_from_the_bundle_scores_the_whole_issued_window():
    iss, _, bundle, received = grid_pair([100 * L + 2 * L])
    status, payload = issued_outcome(bundle, "m", received=received, now_ms=(100 * L + 9 * L) * 1000,
                                     settlement_s=0, grace_s=0)
    assert payload["y_window_max_rps"] == 100.0 and payload["covered"] is False
    assert payload["targets_sha256"] == targets_sha256(bundle["target_timestamps"]) and payload["targets"] == 2
    pb = c.canonical_json(payload)
    r = CalibrationRecord.from_journal(iss, Outcome(5, "iss-1", EPOCH, 100 * L + 2 * L, "m", status, pb,
                                                    c.sha256_hex(pb), 0))
    assert (r.y_rps, r.upper_rps) == (100.0, 4.0)
