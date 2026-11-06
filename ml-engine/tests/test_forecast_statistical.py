"""C2.3: the statistical kernels: E1 numerical comparison, cadence equivalence, lag support and gaps, constant and zero
series, unstable AR, causality and determinism."""

import math
import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting import statistical as S  # noqa: E402
from forecasting.statistical import KernelSettings, KernelUnavailable, Series  # noqa: E402

DAY = 86400
T0 = 1_792_022_400                       # a UTC midnight


def daily(n_days, dt, noise=3.0, seed=3, weekly=5.0, base=100.0, amp=40.0):
    m = DAY // dt
    t = np.arange(int(n_days * m))
    rng = np.random.default_rng(seed)
    y = base + amp * np.sin(2 * np.pi * t / m) + weekly * np.sin(2 * np.pi * t / (7 * m)) + rng.normal(0, noise, len(t))
    return Series(T0, dt, np.maximum(y, 0.0))


def targets_after(e, dt, k=6):
    return [e + j * dt for j in range(1, k + 1)]


# --- E1: the benchmark's ensemble under its compatible fixed settings ----------------------------------------------

@pytest.fixture(scope="module")
def e1():
    from models import seasonal_ensemble as E
    return E


@pytest.mark.parametrize("age_slots", [0, 1, 15, 35])
def test_e1_product_reproduces_e1_on_complete_ten_minute_data(e1, age_slots):
    """Δ = 600 s, daily season of 144, six steps, 14-day HW window, 7-day profile and AR(3), refit on the 6 h
    boundary, complete data: E1-product's components equal E1's."""
    s = daily(16, 600)
    grid = e1.Grid.from_points([(s.start + i * 600, float(v)) for i, v in enumerate(s.values)])
    boundary = s.end - s.end % e1.REFIT_SECONDS - e1.REFIT_SECONDS
    origin_ts = boundary + age_slots * 600
    ref = e1.components_at(grid, grid.index(origin_ts))
    assert ref["generation"].boundary_ts == boundary and not ref["stale_generation"]
    ks = KernelSettings(600)
    fit = S.fit_e1(s, boundary, ks)
    got = S.forecast_e1(fit, s, origin_ts, targets_after(origin_ts, 600), ks)
    np.testing.assert_allclose(got["profile_ar"], ref["profile_ar"], rtol=0, atol=1e-9)
    np.testing.assert_allclose(got["hw"], ref["hw"], rtol=1e-6, atol=1e-5)          # same optimizer, same starts
    np.testing.assert_allclose(got["points"], ref["raw"], rtol=1e-6, atol=1e-5)
    np.testing.assert_allclose(fit.ar_coef, ref["generation"].ar_coef, rtol=0, atol=1e-12)
    assert fit.ar_rows == ref["generation"].ar_rows and fit.hw.n_train == ref["generation"].hw.n_train
    np.testing.assert_allclose([fit.hw.alpha, fit.hw.beta, fit.hw.gamma],
                               [ref["generation"].hw.alpha, ref["generation"].hw.beta, ref["generation"].hw.gamma],
                               atol=1e-6)


def test_heuristic_init_matches_e1_without_pandas(e1):
    y = daily(14, 600).values
    lv, tr, se = S.heuristic_init(y, 144)
    lv2, tr2, se2 = e1.heuristic_init(y)
    assert abs(lv - lv2) < 1e-9 and abs(tr - tr2) < 1e-12 and np.max(np.abs(se - se2)) < 1e-9
    odd = daily(3, 600).values[:3 * 135]
    a, b, c = S.heuristic_init(odd, 135)                          # odd seasons: a plain centred mean
    assert math.isfinite(a) and math.isfinite(b) and len(c) == 135 and abs(np.sum(c)) < 1e-9


def test_the_benchmark_module_is_unchanged(e1):
    assert e1.VERSION == "seasonal-ensemble-1.2.0" and e1.SLOT_SECONDS == 600 and e1.SEASON == 144


# --- cadence equivalence ------------------------------------------------------------------------------------------

def periodic(dt, days=9):
    m = DAY // dt
    t = np.arange(days * m)
    return Series(T0, dt, 50.0 + 20.0 * np.sin(2 * np.pi * t / m) + 5.0 * np.cos(4 * np.pi * t / m))


def truth(t):
    x = 2 * np.pi * ((t - T0) % DAY) / DAY
    return 50.0 + 20.0 * np.sin(x) + 5.0 * np.cos(2 * x)


@pytest.mark.parametrize("dt", [60, 300, 600])
def test_a_noiseless_daily_pattern_is_forecast_at_every_cadence(dt):
    s = periodic(dt)
    ks = KernelSettings(dt)
    e = s.end
    targets = targets_after(e, dt, 3600 // dt)
    naive = S.seasonal_naive(s, e, targets, ks)
    np.testing.assert_allclose(naive, [truth(t) for t in targets], atol=1e-9)
    fit = S.fit_e1(s, e - 6 * 3600, ks)                            # a 6-hour-old fit advanced to the input end
    got = S.forecast_e1(fit, s, e, targets, ks)
    np.testing.assert_allclose(got["profile_ar"], [truth(t) for t in targets], atol=1e-6)
    np.testing.assert_allclose(got["points"], [truth(t) for t in targets], atol=0.05)
    assert len(fit.hw.season) == DAY // dt


def test_common_targets_agree_across_cadences():
    fine, coarse = periodic(300), periodic(600)
    e = coarse.end
    assert fine.at(e) == coarse.at(e)
    common = [e + 600 * j for j in range(1, 7)]
    for s, dt in ((fine, 300), (coarse, 600)):
        ks = KernelSettings(dt)
        assert S.seasonal_naive(s, e, common, ks) == [coarse.at(t - DAY) for t in common]     # the same samples
        np.testing.assert_allclose(S.seasonal_naive(s, e, common, ks), [truth(t) for t in common], atol=1e-9)
        assert [S.profile(s, t, ks) for t in common] == [S.profile(coarse, t, KernelSettings(600)) for t in common]


# --- lag support and gaps -----------------------------------------------------------------------------------------

def with_gap(s, lo, hi):
    v = s.values.copy()
    v[s.index(lo):s.index(hi) + 1] = np.nan
    return Series(s.start, s.interval, v)


def test_seasonal_naive_needs_every_lagged_slot():
    s = daily(3, 300)
    ks = KernelSettings(300)
    e = s.end
    targets = targets_after(e, 300)
    gap = with_gap(s, targets[2] - DAY, targets[2] - DAY)
    with pytest.raises(KernelUnavailable) as err:
        S.seasonal_naive(gap, e, targets, ks)
    assert err.value.reason == "history"
    with pytest.raises(KernelUnavailable):               # a lag after the input end is never used
        S.seasonal_naive(s, e - DAY, [e + 300], ks)
    young = Series(s.start, 300, s.values[: DAY // 300 - 1])
    with pytest.raises(KernelUnavailable):               # under one day of history
        S.seasonal_naive(young, young.end, targets_after(young.end, 300), ks)


def test_profile_support_needs_three_prior_days():
    s = daily(10, 600)
    ks = KernelSettings(600)
    t = s.end
    for d in range(1, 6):                                # leave only days 6 and 7 back
        s = with_gap(s, t - d * DAY, t - d * DAY)
    assert math.isnan(S.profile(s, t, ks))
    s2 = Series(s.start, 600, s.values.copy())
    s2.values[s2.index(t - DAY)] = 1.0
    assert S.profile(s2, t, ks) == pytest.approx(np.mean([1.0, s2.at(t - 6 * DAY), s2.at(t - 7 * DAY)]))


@pytest.mark.parametrize("case", ["young", "sparse", "short_segment", "cutoff_missing", "off_grid"])
def test_fits_refuse_insufficient_history(case):
    s, ks = daily(15, 600), KernelSettings(600)
    c = s.end
    if case == "young":
        s = Series(s.start, 600, s.values[: 7 * 144 - 1])
        c = s.end
    elif case == "sparse":                              # 6% of the last 7 days missing, spread out
        v = s.values.copy()
        v[-7 * 144::16] = np.nan
        v[-1] = 1.0
        s = Series(s.start, 600, v)
    elif case == "short_segment":                       # a gap 1.5 days before the cutoff: HW segment < 2 days
        s = with_gap(s, c - int(1.5 * DAY), c - int(1.5 * DAY))
    elif case == "cutoff_missing":
        s = with_gap(s, c, c)
    else:
        c -= 60
    with pytest.raises(KernelUnavailable) as err:
        S.fit_e1(s, c, ks)
    assert err.value.reason == "history"


def test_a_gap_shortens_the_holt_winters_segment_instead_of_being_filled():
    s, ks = daily(15, 600), KernelSettings(600)
    c = s.end
    g = with_gap(s, c - 3 * DAY, c - 3 * DAY + 3000)    # a 1-hour gap three days back (0.7% of the window)
    fit = S.fit_e1(g, c, ks)
    assert fit.hw.segment_start == c - 3 * DAY + 3600 and fit.hw.n_train == (c - fit.hw.segment_start) // 600 + 1
    assert S.fit_e1(s, c, ks).hw.n_train == 14 * 144                # complete: the 14-day cap


def test_forecasts_refuse_missing_recent_input():
    s, ks = daily(15, 600), KernelSettings(600)
    e = s.end
    fit = S.fit_e1(s, e - 6 * 600, ks)
    for gap in (e - 3 * 600, e):                        # between the cutoff and the input end, or the input end
        with pytest.raises(KernelUnavailable):
            S.forecast_e1(fit, with_gap(s, gap, gap), e, targets_after(e, 600), ks)
    fit_now = S.fit_e1(s, e, ks)
    with pytest.raises(KernelUnavailable):              # an AR lag (e - 2Δ) missing
        S.forecast_e1(fit_now, with_gap(s, e - 1200, e - 1200), e, targets_after(e, 600), ks)
    with pytest.raises(KernelUnavailable):              # a fit from the future
        S.forecast_e1(fit_now, s, e - 600, targets_after(e - 600, 600), ks)
    with pytest.raises(KernelUnavailable):              # beyond one season: its profile would read past e
        S.forecast_e1(fit_now, s, e, [e + DAY + 600], ks)
    sparse = with_gap(with_gap(with_gap(with_gap(with_gap(s, e + 600 - 3 * DAY, e + 600 - 3 * DAY),
                                                 e + 600 - 4 * DAY, e + 600 - 4 * DAY), e + 600 - 5 * DAY,
                                        e + 600 - 5 * DAY), e + 600 - 6 * DAY, e + 600 - 6 * DAY),
                      e + 600 - 7 * DAY, e + 600 - 7 * DAY)
    with pytest.raises(KernelUnavailable) as err:       # the first target keeps only 2 prior days
        S.forecast_e1(fit_now, sparse, e, targets_after(e, 600), ks)
    assert "profile" in err.value.message


def test_ar_needs_enough_rows():
    s = daily(15, 600)
    with pytest.raises(KernelUnavailable) as err:
        S.fit_e1(s, s.end, KernelSettings(600, ar_min_rows=5000))
    assert err.value.reason == "history"


# --- constant, zero, unstable -------------------------------------------------------------------------------------

@pytest.mark.parametrize("level", [0.0, 42.5])
@pytest.mark.parametrize("dt", [300, 600])
def test_constant_and_zero_series(level, dt):
    s = Series(T0, dt, np.full(9 * DAY // dt, level))
    ks = KernelSettings(dt)
    e = s.end
    targets = targets_after(e, dt)
    assert S.seasonal_naive(s, e, targets, ks) == [level] * 6
    got = S.forecast_e1(S.fit_e1(s, e, ks), s, e, targets, ks)
    np.testing.assert_allclose(got["points"], [level] * 6, atol=1e-9)
    assert all(p >= 0 for p in got["points"])


def test_ar_stability_is_checked_not_repaired():
    assert S.ar_roots_max([0.0, 0.5]) == pytest.approx(0.5)
    assert S.ar_roots_max([0.0, 1.0]) == pytest.approx(1.0)
    assert S.ar_roots_max([3.0, 0.0, 0.0, 0.0]) == 0.0
    assert S.ar_roots_max([0.0, 0.5, 0.6]) > 1.0          # z^2 - 0.5 z - 0.6: a root at about 1.07


def test_an_explosive_residual_refuses_the_fit():
    dt, m = 600, 144
    n = 15 * m
    rng = np.random.default_rng(5)
    z = np.zeros(n)
    for i in range(8 * m, n):                             # an explosive AR(1) for the last 7 days
        z[i] = 1.02 * z[i - 1] + rng.normal(0, 1.0)
    s = Series(T0, dt, 1000.0 + z + 50 * np.sin(2 * np.pi * np.arange(n) / m) - min(0.0, float(np.min(z))))
    with pytest.raises(KernelUnavailable) as err:
        S.fit_e1(s, s.end, KernelSettings(dt))
    assert err.value.reason == "numeric" and "stable" in err.value.message


# --- causality, determinism, shadow candidates ------------------------------------------------------------------

def test_a_fit_reads_nothing_after_its_cutoff():
    s, ks = daily(15, 600), KernelSettings(600)
    c = s.end - 20 * 600
    v = s.values.copy()
    v[s.index(c) + 1:] = v[s.index(c) + 1:] * 7 + 1000
    a, b = S.fit_e1(s, c, ks), S.fit_e1(Series(s.start, 600, v), c, ks)
    assert a == b


def test_fits_are_deterministic():
    s, ks = daily(15, 300), KernelSettings(300)
    assert S.fit_e1(s, s.end, ks) == S.fit_e1(s, s.end, ks)


def test_shadow_candidates_never_substitute():
    s, ks = daily(15, 600), KernelSettings(600)
    e = s.end
    targets = targets_after(e, 600)
    ok = S.shadow_candidates(s, e, targets, ks, S.fit_e1(s, e, ks))
    assert set(ok) == {"seasonal_naive", "statistical"} and "points" in ok["statistical"]
    gap = with_gap(s, e - 1200, e - 1200)
    out = S.shadow_candidates(gap, e, targets, ks, S.fit_e1(s, e, ks))
    assert out["statistical"] == {"recipe": S.RECIPE_E1, "unavailable": "history",
                                  "message": "the recent AR lags are incomplete"}
    assert out["seasonal_naive"]["points"] == S.seasonal_naive(s, e, targets, ks)
    assert S.shadow_candidates(s, e, targets, ks, None)["statistical"]["unavailable"] == "history"
