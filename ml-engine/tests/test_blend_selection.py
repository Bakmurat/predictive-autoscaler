"""Validation-selected blend weight (training/blend_selection.py): the network keeps weight only when the
served blend it produces beats the pattern alone on the held-out partition."""
import os
import sys
from datetime import datetime, timedelta

import joblib
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from models.lstm_model import STEPS_AHEAD, LSTMForecastModel  # noqa: E402
from training import blend_selection as bs  # noqa: E402

GRID = timedelta(minutes=10)
PER_DAY = 144


def daily_series(days, end, amplitude=400.0, base=1000.0):
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    vals = [base + amplitude * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / (24 * 60.0)) for t in idx]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


class _Stub:
    """Stands in for the Keras network: a constant (or NaN) at every step, batch-shaped."""
    def __init__(self, value_scaled):
        self.value_scaled = value_scaled
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        return np.full((len(x), STEPS_AHEAD), self.value_scaled, dtype=float)


def _model(series, network_rpm):
    from sklearn.preprocessing import RobustScaler
    m = LSTMForecastModel(sequence_length=PER_DAY)
    m.scaler = RobustScaler().fit(series.values.reshape(-1, 1))
    m.model = _Stub(float(m.scaler.transform(np.array([[network_rpm]]))[0][0]) if np.isfinite(network_rpm) else float("nan"))
    m.is_trained = True
    return m


@pytest.fixture
def data():
    end = datetime(2026, 9, 28, 12, 0)
    s = daily_series(9, end)
    test = s.iloc[-int(0.2 * len(s)):].to_frame("value")
    return s, test


def test_a_useless_network_loses_all_weight_and_the_choice_is_recorded(data):
    s, test = data
    m = _model(s, network_rpm=6000.0)          # far above the series everywhere
    bs.attach_history(m, s)
    rec = bs.select_blend_weight(m, test)
    assert rec["chosen"] == "pattern_only" and m.pattern_weight_override == 1.0
    assert [c["candidate"] for c in rec["candidates"]] == ["deployed_ramp", "pattern_0.85", "pattern_0.95", "pattern_only"]
    assert all(c["scored"] == "served_blend" for c in rec["candidates"])
    maes = [c["mae"] for c in rec["candidates"]]
    assert maes == sorted(maes, reverse=True) and maes[-1] < maes[0] / 5   # the more pattern, the better, by a wide margin
    assert m.evaluate(test)["mae"] == pytest.approx(rec["mae"])


def test_a_nan_network_never_poisons_the_selection(data):
    s, test = data
    m = _model(s, network_rpm=float("nan"))
    bs.attach_history(m, s)
    rec = bs.select_blend_weight(m, test)
    assert rec["chosen"] == "pattern_only"
    assert np.isfinite(rec["mae"]) and all(c["mae"] is None or not np.isfinite(c["mae"]) for c in rec["candidates"][:3])


def test_without_history_the_deployed_ramp_is_kept_and_the_reason_recorded(data):
    s, test = data
    m = _model(s, network_rpm=6000.0)
    rec = bs.select_blend_weight(m, test)
    assert rec["chosen"] == "deployed_ramp" and m.pattern_weight_override is None
    assert "no seasonal history" in rec["reason"]


def test_the_chosen_weight_survives_the_artifact_round_trip(tmp_path, data):
    s, test = data
    m = _model(s, network_rpm=6000.0)
    bs.attach_history(m, s)
    bs.select_blend_weight(m, test)
    joblib.dump(m, tmp_path / "m.pkl")
    loaded = joblib.load(tmp_path / "m.pkl")
    assert loaded.pattern_weight_override == 1.0


def test_equal_candidates_resolve_to_the_pattern_the_network_must_earn_its_share():
    """Rule margin-lb-pooled-hysteresis-1: pattern_only is the default; a tie never hands weight to the network
    (the argmin rule this replaced kept the deployed ramp on ties)."""
    class M:
        seasonal_history = pd.Series([1.0, 2.0])
        pattern_weight_override = None
        def evaluate(self, td, target_column="value", imputed=None, return_origin_errors=False):
            return {"mae": 10.0, "rmse": 12.0, "scored": "served_blend", "network_only": {"mae": 10.0},
                    "origin_abs_errors": [10.0] * 30}
    m = M()
    rec = bs.select_blend_weight(m, pd.DataFrame({"value": [1.0]}, index=pd.DatetimeIndex([datetime(2026, 1, 1)])))
    assert rec["chosen"] == "pattern_only" and m.pattern_weight_override == 1.0
    assert rec["rule_id"] == bs.RULE_ID and rec["stability"]["decision_path"] == "no network candidate beat pattern_only"


def test_a_clearly_better_network_blend_is_chosen_and_small_gains_are_not():
    def stub(errs):
        class M:
            seasonal_history = pd.Series([1.0, 2.0])
            pattern_weight_override = None
            def evaluate(self, td, target_column="value", imputed=None, return_origin_errors=False):
                e = errs[self.pattern_weight_override]
                return {"mae": float(np.mean(e)), "rmse": 1.0, "scored": "served_blend",
                        "network_only": {"mae": 99.0}, "origin_abs_errors": list(e)}
        return M()
    td = pd.DataFrame({"value": [1.0]}, index=pd.DatetimeIndex([datetime(2026, 1, 1)]))
    rng = np.random.RandomState(1)
    base = 100 + rng.normal(0, 5, 60)
    big = stub({None: base * 0.70, 0.85: base * 0.80, 0.95: base * 0.90, 1.0: base})      # 30 % better, every origin
    assert bs.select_blend_weight(big, td)["chosen"] == "deployed_ramp"
    small = stub({None: base * 0.97, 0.85: base * 0.98, 0.95: base * 0.99, 1.0: base})    # 3 % < min_gain 5 %
    assert bs.select_blend_weight(small, td)["chosen"] == "pattern_only"


# ------------------------------------------------------------------------------------------------------------------
# Parity with the model lab's reference (2026-09-30-blend-and-network/stable_blend_selection.py) and its vectors
# ------------------------------------------------------------------------------------------------------------------

import json  # noqa: E402
import math  # noqa: E402

V = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "stable_blend_selection_vectors.json")))


def _parts(case):
    out = []
    for part in case["partitions"]:
        a = np.array(part["actual"], dtype=float)
        out.append({k: bs.origin_errors(np.abs(np.array(v, dtype=float) - a)) for k, v in part["predictions"].items()})
    return out


def test_vector_file_matches_the_port():
    assert V["rule_id"] == bs.RULE_ID and V["defaults"] == bs.DEFAULTS
    assert [tuple(c) for c in V["candidates"]] == [tuple(c) for c in bs.CANDIDATES]


@pytest.mark.parametrize("case", V["cases"], ids=[c["name"] for c in V["cases"]])
def test_lab_vectors_reproduced(case):
    d = bs.decide(_parts(case), previous=case["previous"], params=case["params"])
    e, tol = case["expected"], V["tolerance"]
    for k in ("chosen", "weight", "decision_path", "incumbent", "pooled_partitions", "pooled_origins"):
        assert d[k] == e[k], k
    assert set(d["pooled_mae"]) == set(e["pooled_mae"])
    for c, v in e["pooled_mae"].items():
        assert abs(d["pooled_mae"][c] - v) < tol
    for block in ("vs_pattern_only", "vs_incumbent"):
        assert set(d[block]) == set(e[block])
        for c, st in e[block].items():
            assert d[block][c]["beats"] == st["beats"]
            for k in ("gain", "lower_bound", "mae_ref", "mae_cand"):
                assert abs(d[block][c][k] - st[k]) < tol, (block, c, k)


def test_splitmix64_reference_and_pinned_draws():
    r = bs.SplitMix64(1234567)   # published reference outputs of SplitMix64 for seed 1234567
    assert [r.next() for _ in range(5)] == [6457827717110365317, 3203168211198807973, 9817491932198370423,
                                            4593380528125082431, 16408922859458223821]
    r = bs.SplitMix64(V["splitmix64"]["seed"])
    assert [r.next() for _ in range(5)] == V["splitmix64"]["first_outputs"]
    b = V["block_resample"]
    assert bs.block_indices(b["K"], b["L"], bs.SplitMix64(b["seed"])) == b["indices"]
    assert bs.block_indices(5, 12, bs.SplitMix64(1)) == [0, 1, 2, 3, 4]


def test_degenerate_parameters_reproduce_the_argmin_rule():
    rng = np.random.RandomState(3)
    params = {"min_gain": 0.0, "lb_threshold": -1e300, "pool_partitions": 1, "resamples": 20}
    for trial in range(60):
        K = rng.randint(5, 40)
        part = {c: np.abs(rng.normal(100, 40 * (1 + j), K)).tolist() for j, (c, _) in enumerate(bs.CANDIDATES)}
        if trial % 5 == 0:
            part["pattern_0.95"] = list(part["pattern_only"])
        if trial % 7 == 0:
            part["pattern_0.85"] = list(part["deployed_ramp"])
        rows = [(c, float(np.mean(part[c]))) for c, _ in bs.CANDIDATES]
        assert bs.decide([part], params=params)["chosen"] == min(rows, key=lambda r: r[1])[0]


def test_pooling_depth_hysteresis_and_validation():
    d = {x["name"]: x for x in V["cases"]}
    g = d["G_pooled_veto"]; parts = _parts(g)
    extra = [{k: [1e6] * len(v) for k, v in parts[0].items()}] * 2
    assert bs.decide(parts, params=g["params"])["pooled_mae"] == bs.decide(parts + extra, params=g["params"])["pooled_mae"]
    assert bs.decide(parts, params=dict(g["params"], pool_partitions=1))["chosen"] != "pattern_only"
    h = d["D_hold"]; hp = _parts(h)
    assert bs.decide(hp, previous="pattern_0.95", params=h["params"])["chosen"] == "pattern_0.95"
    assert bs.decide(hp, previous=None, params=h["params"])["chosen"] == "pattern_only"
    assert bs.decide(hp, previous="pattern_0.95", params=dict(h["params"], hold_gain=0.05))["chosen"] == "pattern_only"
    assert bs.decide(hp, previous="weight_0.5", params=h["params"])["incumbent"] is None
    part = {"pattern_only": [10.0, 12.0, 11.0], "deployed_ramp": [float("nan"), 1.0, 1.0], "pattern_0.95": [5.0, 5.0, 5.0]}
    dd = bs.decide([part], params={"resamples": 50, "block_origins": 2, "min_gain": 0.0})
    assert "deployed_ramp" not in dd["vs_pattern_only"] and dd["chosen"] == "pattern_0.95"
    for bad in ([{"deployed_ramp": [1.0]}],):
        with pytest.raises(ValueError):
            bs.decide(bad)
    with pytest.raises(ValueError):
        bs.decide([part], params={"no_such": 1})
    with pytest.raises(ValueError):
        bs.decide([part], params={"lower_bound_quantile": 1.0})


# ------------------------------------------------------------------------------------------------------------------
# evaluate(): per-origin errors, and the origin fix after an imputed-target drop (model lab, 2026-09-30)
# ------------------------------------------------------------------------------------------------------------------

class _InputStub:
    """A network whose output depends on its input, so every cell differs."""
    def __init__(self, offsets):
        self.offsets = np.asarray(offsets, dtype=np.float32)
        self.output_shape = (None, STEPS_AHEAD)

    def predict(self, x, verbose=0):
        last = np.asarray(x)[:, -1, 0].astype(np.float32)
        return (last[:, None] * 0.9 + self.offsets[None, :]).astype(np.float32)


def _noisy_series(days=9, end=datetime(2026, 9, 28, 12, 0)):
    n = days * PER_DAY
    idx = [end - GRID * (n - 1 - i) for i in range(n)]
    rng = np.random.RandomState(5)
    vals = [1000 + 400 * np.sin(2 * np.pi * (t.hour * 60 + t.minute) / 1440.0) + 60 * (i // PER_DAY) + rng.normal(0, 25)
            for i, t in enumerate(idx)]
    return pd.Series(vals, index=pd.DatetimeIndex(idx))


def _input_model(series):
    from sklearn.preprocessing import RobustScaler
    m = LSTMForecastModel(sequence_length=PER_DAY)
    m.scaler = RobustScaler().fit(series.values.reshape(-1, 1))
    m.model = _InputStub([0.1, 0.05, 0.0, -0.05, -0.1, 0.2])
    m.is_trained = True
    return m


def test_origin_errors_average_to_the_mae_for_every_candidate():
    s = _noisy_series(); test = s.iloc[-int(0.2 * len(s)):].to_frame("value")
    m = _input_model(s); bs.attach_history(m, s)
    for _, w in bs.CANDIDATES:
        m.pattern_weight_override = w
        ev = m.evaluate(test, return_origin_errors=True)
        assert len(ev["origin_abs_errors"]) == ev["sequences_scored"] == len(ev["origins"])
        assert float(np.mean(ev["origin_abs_errors"])) == pytest.approx(ev["mae"], abs=1e-9)


def test_after_an_imputed_target_drop_the_pattern_is_looked_up_for_the_right_origin():
    """Dropping sequences must not move the others: every kept origin scores exactly as it does without the drop.
    Before the fix, the pattern for every sequence after the first drop was looked up for an earlier origin."""
    s = _noisy_series(); test = s.iloc[-int(0.2 * len(s)):].to_frame("value")
    imp = np.zeros(len(test), dtype=bool); imp[150] = imp[170] = True
    m = _input_model(s); bs.attach_history(m, s)
    m.pattern_weight_override = 1.0                                  # pattern_only: the error IS the lookup
    full = m.evaluate(test, return_origin_errors=True)
    dropped = m.evaluate(test, imputed=imp, return_origin_errors=True)
    assert dropped["sequences_dropped_imputed_target"] > 0
    by_origin = dict(zip(full["origins"], full["origin_abs_errors"]))
    for o, e in zip(dropped["origins"], dropped["origin_abs_errors"]):
        assert e == pytest.approx(by_origin[o], abs=1e-9), o


# ------------------------------------------------------------------------------------------------------------------
# Chaining through the provenance sidecar
# ------------------------------------------------------------------------------------------------------------------

def test_record_carries_the_evidence_and_the_next_training_pools_it(tmp_path):
    s = _noisy_series(); test = s.iloc[-int(0.2 * len(s)):].to_frame("value")
    m = _input_model(s); bs.attach_history(m, s)
    r1 = bs.select_blend_weight(m, test)
    st = r1["stability"]
    assert set(st["origin_errors"]) == {c for c, _ in bs.CANDIDATES} and st["pooled_history"] == []
    sidecar = tmp_path / "lstm_x_requests.meta.json"
    sidecar.write_text(json.dumps({"blend_selection": r1}, default=str))
    prev, hist = bs.history_from_sidecar(sidecar)
    assert prev == r1["chosen"] and len(hist) == 1
    r2 = bs.select_blend_weight(m, test, previous=prev, history_partitions=hist)
    assert r2["stability"]["pooled_partitions"] == 2
    sidecar.write_text(json.dumps({"blend_selection": r2}, default=str))
    prev, hist = bs.history_from_sidecar(sidecar)
    assert len(hist) == 2                                             # current + previous: pool 3 next time
    r3 = bs.select_blend_weight(m, test, previous=prev, history_partitions=hist)
    assert r3["stability"]["pooled_partitions"] == 3
    sidecar.write_text(json.dumps({"blend_selection": r3}, default=str))
    assert len(bs.history_from_sidecar(sidecar)[1]) == 2              # never deeper than pool_partitions - 1


def test_a_sidecar_from_another_rule_or_missing_contributes_nothing(tmp_path):
    old = {"blend_selection": {"chosen": "deployed_ramp", "weight": None, "rule": "lowest served-blend MAE ..."}}
    assert bs.history_from_sidecar(old) == (None, [])
    assert bs.history_from_sidecar(tmp_path / "absent.meta.json") == (None, [])
    (tmp_path / "bad.meta.json").write_text("{not json")
    assert bs.history_from_sidecar(tmp_path / "bad.meta.json") == (None, [])
