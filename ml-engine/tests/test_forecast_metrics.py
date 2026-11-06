"""C2.5: forecast-quality metrics: bounded names and labels, absent values instead of fake zeros, cardinality from the
registry (<= 256 series per autoscaler, <= 8 autoscalers), and cleanup on deletion and recreation."""

import os
import re
import sys

import pytest
from prometheus_client import CollectorRegistry, generate_latest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting import metrics as M  # noqa: E402
from forecasting import trust as T  # noqa: E402
from forecasting.calibration import CalibrationPolicy, initial  # noqa: E402
from tests.test_forecast_trust import EPOCH, NOW, Feed, good  # noqa: E402

MODELS = ("seasonal_naive", "statistical", "lstm")


def snapshot(uid="uid-1", feeds=None, empty=False):
    models = {}
    for m in MODELS:
        f = Feed(m) if empty else (feeds or {}).get(m) or good(m)
        cal = initial(CalibrationPolicy(600), EPOCH, m, "r")
        models[m] = M.model_samples(ledger=f.ledger, calibration=cal, nominal_miscoverage=0.1, trusted=not empty,
                                    history_seconds=3 * 86400, now_s=NOW,
                                    latest=None if empty else {"upper_window_rps": 6.0, "base_window_rps": 4.0},
                                    fit_failures={"timeout": 2})
    return M.Snapshot(uid, models)


def registry(m):
    r = CollectorRegistry()
    r.register(m)
    return r


def samples(reg, autoscaler=None):
    out = []
    for fam in reg.collect():
        for s in fam.samples:
            if autoscaler is None or s.labels.get("autoscaler") == autoscaler:
                out.append(s)
    return out


def test_names_labels_and_meaning_are_bounded():
    m = M.ForecastMetrics()
    m.update("shop", "web", snapshot())
    names = {s.name for s in samples(registry(m))}
    assert names and all(n.startswith("predictive_autoscaler_forecast_") for n in names)
    assert not any("prediction_error_percent" in n or "accuracy" in n for n in names)
    for s in samples(registry(m)):
        extra = set(s.labels) - {"namespace", "autoscaler", "model"}
        assert set(s.labels) >= {"namespace", "autoscaler", "model"} and len(extra) <= 1
        assert s.labels["model"] in MODELS
        for label in extra:
            assert s.labels[label] in M._EXTRA[label]
    assert all(n.startswith(M.PREFIX) for n in M.METRIC_NAMES) and len(M.METRIC_NAMES) == len(M.METRICS)


def test_missing_denominators_remove_series_instead_of_exporting_zero():
    m = M.ForecastMetrics()
    m.update("shop", "web", snapshot(empty=True))
    names = {s.name for s in samples(registry(m))}
    for absent in ("window_coverage_ratio", "outcome_availability_ratio", "window_mae_rps", "window_bias_rps",
                   "window_pinball_loss_rps", "step_mae_rps", "evidence_age_seconds", "upper_window_rps",
                   "base_window_rps"):
        assert M.PREFIX + absent not in names, absent
    scored = [s for s in samples(registry(m)) if s.name == M.PREFIX + "scored_windows"]
    assert scored and all(s.value == 0 for s in scored)                 # a count of zero is a real zero
    trusted = [s.value for s in samples(registry(m)) if s.name == M.PREFIX + "trusted"]
    assert trusted == [0.0, 0.0, 0.0]


def test_values_follow_the_ledger():
    f = good()
    s = dict(((n, e), v) for n, e, v in M.model_samples(ledger=f.ledger, calibration=None, nominal_miscoverage=0.1,
                                                       trusted=True, history_seconds=1, now_s=NOW))
    w7 = T.window_sums(f.ledger, NOW, 7 * 86400)
    assert s[("window_coverage_ratio", ("gate",))] == w7["g_cv"] / w7["g_bd"] == pytest.approx(0.95, abs=0.01)
    assert s[("issued_total", ())] == 432 and s[("outcomes_total", ("missed",))] == f.ledger["totals"]["missed"]
    assert s[("window_bias_rps", ())] == pytest.approx(-1.0)          # b 4.0 against Y 5.0: under-forecast
    assert s[("step_mae_rps", ("last",))] == pytest.approx(3.0)
    assert ("working_miscoverage_ratio", ()) not in s                  # no calibration state given


def test_cardinality_is_counted_from_the_registry():
    m = M.ForecastMetrics()
    for i in range(M.MAX_AUTOSCALERS):
        m.update("shop", f"web-{i}", snapshot(uid=f"u{i}"))
    reg = registry(m)
    per = {f"web-{i}": len(samples(reg, f"web-{i}")) for i in range(M.MAX_AUTOSCALERS)}
    assert all(0 < n <= M.MAX_SERIES_PER_AUTOSCALER for n in per.values()), per
    assert sum(per.values()) <= M.MAX_AUTOSCALERS * M.MAX_SERIES_PER_AUTOSCALER
    with pytest.raises(M.RegistryFull):
        m.update("shop", "web-extra", snapshot())
    m.update("shop", "web-0", snapshot(uid="u0b"))                    # an existing one may still update
    huge = M.Snapshot("x", {"seasonal_naive": [("trusted", (), 1.0)] * (M.MAX_SERIES_PER_AUTOSCALER + 1)})
    with pytest.raises(M.CardinalityExceeded):
        m.update("shop", "web-1", huge)


@pytest.mark.parametrize("bad", [
    {"prophet": []}, {"statistical": [("outcomes_total", ("ok",), 1.0)]},
    {"statistical": [("step_mae_rps", (), 1.0)]}, {"statistical": [("trusted", ("x",), 1.0)]},
])
def test_labels_outside_the_fixed_enums_are_refused(bad):
    with pytest.raises(ValueError):
        M.ForecastMetrics().update("shop", "web", M.Snapshot("u", bad))


def test_deletion_removes_series_and_recreation_starts_clean():
    m = M.ForecastMetrics()
    m.update("shop", "web", snapshot(uid="old"))
    reg = registry(m)
    assert samples(reg, "web")
    m.remove("shop", "web")
    assert samples(reg, "web") == [] and m.registered() == []
    fresh = {k: Feed(k) for k in MODELS}
    fresh["seasonal_naive"].window(NOW - 1200)
    m.update("shop", "web", snapshot(uid="new", feeds=fresh))        # recreated: a new UID, a new ledger
    issued = [s for s in samples(reg, "web") if s.name == M.PREFIX + "issued_total" and
              s.labels["model"] == "seasonal_naive"]
    assert [s.value for s in issued] == [1.0]                         # restarted, not added to the old 432


def test_the_exposition_renders_counters_and_gauges():
    m = M.ForecastMetrics()
    m.update("shop", "web", snapshot())
    text = generate_latest(registry(m)).decode()
    assert re.search(r"^# TYPE predictive_autoscaler_forecast_issued(_total)? counter$", text, re.M)
    assert 'predictive_autoscaler_forecast_issued_total{autoscaler="web",model="statistical",namespace="shop"}' in text
    assert 'predictive_autoscaler_forecast_fit_failures_total{autoscaler="web",model="lstm",namespace="shop",' \
           'reason="timeout"} 2.0' in text


def test_an_admitted_snapshot_cannot_be_changed_by_its_caller():
    # Codex repro: appending to the caller's list after update() bypassed the series limit.
    samples = [("trusted", (), 1.0)]
    models = {"seasonal_naive": samples}
    snap = M.Snapshot("u", models)
    m = M.ForecastMetrics(max_series_per_autoscaler=2)
    m.update("shop", "web", snap)
    samples.extend([("trusted", (), 1.0)] * 10)
    models["statistical"] = [("trusted", (), 1.0)] * 10
    assert len(samples) == 11                                         # the caller's list did change ...
    reg = registry(m)
    assert len([x for fam in reg.collect() for x in fam.samples]) == 1  # ... the admitted copy did not
    with pytest.raises((TypeError, AttributeError)):
        snap.models["lstm"] = []


def test_the_namespace_label_is_the_workloads_not_the_installations():
    # A workload in "shop" served by an installation in "pa-system": the series carry namespace="shop"; the scrape
    # keeps it (honorLabels, tests/test_chart.py) and adds pa_install_namespace="pa-system" itself.
    m = M.ForecastMetrics()
    m.update("shop", "web", snapshot())
    labels = {s.labels["namespace"] for fam in registry(m).collect() for s in fam.samples}
    assert labels == {"shop"}
    assert all("pa_install_namespace" not in s.labels for fam in registry(m).collect() for s in fam.samples)


def test_reservation_is_atomic_and_counts_against_the_limit():
    m = M.ForecastMetrics(max_autoscalers=1)
    m.reserve("shop", "web")
    m.reserve("shop", "web")                           # idempotent for the same autoscaler
    with pytest.raises(M.RegistryFull):
        m.reserve("shop", "api")
    assert m.registered() == [("shop", "web")] and list(registry(m).collect())
