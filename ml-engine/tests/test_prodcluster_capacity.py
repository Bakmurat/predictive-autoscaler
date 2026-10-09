"""deploy/prodcluster/scoring/capacity.py v3: P9 capacity accounting (C1, the capacity part of C4); Codex r40/r41 cases."""
import importlib.util
import json
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "..", "..", "deploy", "prodcluster", "scoring", "capacity.py")
SPEC = importlib.util.spec_from_file_location("capacity", PATH)
cap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cap)
IE = cap.load("infra_events", os.path.join(cap.PROD, "infra_events.py"))

D0 = 1_791_331_200_000             # 2026-10-07T00:00:00Z
STEP = cap.STEP


def grid(n, start=D0):
    return [start + i * STEP for i in range(n)]


def test_ready_window_edges_and_largest_across_series():
    taus = grid(3)
    s1 = [(D0 - STEP, 9.0), (D0 + 5_000, 2.0), (D0 + 2 * STEP, 3.0)]    # a sample exactly at tau - 30 s is outside
    s2 = [(D0 + 2 * STEP - 1_000, 4.0)]
    r = cap.ready_at([s1, s2], taus)
    assert r[taus[0]] is None and r[taus[1]] == 2.0 and r[taus[2]] == 4.0


def test_r40_a_staleness_marker_invalidates_the_series_until_its_next_finite_sample():
    taus = [D0, D0 + STEP, D0 + 2 * STEP]
    s = [(D0 - 20_000, 12.0), (D0 - 5_000, None), (D0 + STEP + 1_000, 7.0)]
    r = cap.ready_at([s], taus)
    assert r[D0] is None                            # the marker after the finite sample wins (Codex's probe)
    assert r[D0 + STEP] is None                     # still invalidated, no finite sample since
    assert r[D0 + 2 * STEP] == 7.0                  # recovered


def test_r40_ready_must_be_a_non_negative_integer():
    for bad in (-2.0, 1.5):
        with pytest.raises(ValueError):
            cap.check_ready([[(D0, bad)]], "x")
    cap.check_ready([[(D0, 3.0), (D0 + 1, None)]], "x")


def test_shortage_and_surplus_against_the_planned_step_reference():
    taus = grid(4)
    planned = {t: 1300.0 for t in taus}             # ceil(1300/600) = 3 pods
    ready = {"e1": dict(zip(taus, [1.0, 3.0, 5.0, None])), "reactive": dict(zip(taus, [3.0, 3.0, 3.0, 3.0]))}
    rpm = {a: {t: 1300.0 for t in taus} for a in ready}
    arms, pairs = cap.score(taus, planned, ready, rpm, pairs=[("e1", "reactive")])
    (e1,) = [r for r in arms if r["arm"] == "e1"]
    assert e1["planned"] == {"shortage": 1.0, "surplus": 1.0, "minutes": 1.5}
    (p,) = pairs
    assert p["planned"]["paired"] == 3 and p["planned"]["missing"] == 1 and p["planned"]["incomplete"]


def test_observed_reference_measures_the_arm_against_its_own_traffic():
    taus = grid(2)
    planned = {t: 1200.0 for t in taus}
    ready = {"e1": {t: 1.0 for t in taus}}
    rpm = {"e1": {taus[0]: 600.0, taus[1]: None}}   # an overloaded arm may receive less: 1 pod "needed"
    (row,), _ = cap.score(taus, planned, ready, rpm, pairs=[])
    assert row["planned"]["shortage"] == 1.0 and row["observed"]["shortage"] == 0.0
    assert row["observed"]["minutes"] == 0.5


def test_p8_invalid_instants_are_excluded_for_both_arms_other_classes_are_kept_and_counted():
    taus = grid(4)
    planned = {t: 600.0 for t in taus}
    ready = {"e2": {t: 1.0 for t in taus}, "e1": {t: 1.0 for t in taus}}
    rpm = {a: {t: 600.0 for t in taus} for a in ready}
    klass = dict(zip(taus, ["invalid", "unknown", "verified_clean", "gate_fail"]))
    arms, (p,) = cap.score(taus, planned, ready, rpm, klass.get, pairs=[("e2", "e1")])
    assert p["invalid"] == 1 and p["eligible"] == 3 and p["planned"]["paired"] == 3
    assert arms[0]["classes"] == {"invalid": 1, "unknown": 1, "verified_clean": 1, "gate_fail": 1}


def rows_for(days, pair=("e1", "reactive"), a=10.0, b=30.0, minutes=720.0):
    out = []
    for d in days:
        z = {"a_shortage": a, "b_shortage": b, "a_surplus": 0.0, "b_surplus": 0.0, "paired_minutes": minutes,
             "incomplete": minutes < 1296}
        out.append({"pair": list(pair), "day": d, "planned": dict(z), "observed": dict(z, paired_minutes=0.0)})
    return out


def test_aggregate_is_a_rate_per_paired_day_and_zero_exposure_is_undefined():
    rows = rows_for(["d1"])
    pl, ob = cap.aggregate(rows, cap.make_draws(["d1"], 50, 1))
    assert pl["shortage"]["a_per_day"] == 20.0 and pl["shortage"]["difference_per_day"] == -40.0
    assert ob["shortage"]["difference_per_day"] is None and ob["shortage"]["bootstrap_undefined"] == 50
    assert ob["shortage"]["nominal_95"] is None


def test_r40_mixed_undefined_draws_are_counted_and_the_interval_is_labelled_conditional():
    rows = rows_for(["d1"]) + rows_for(["d2"], minutes=0.0)
    (pl, _) = cap.aggregate(rows, cap.make_draws(["d1", "d2"], 400, 3))
    s = pl["shortage"]
    assert s["bootstrap_undefined"] > 0 and s["bootstrap_defined"] + s["bootstrap_undefined"] == 400
    assert s["interval_note"] == "conditional on the defined draws"


def test_one_shared_draw_matrix_serves_every_comparison_and_must_match_the_days():
    days = [f"d{i}" for i in range(5)]
    rows = []
    for i, d in enumerate(days):
        rows += rows_for([d], ("e1", "reactive"), a=float(i), b=0.0, minutes=1440.0)
        rows += rows_for([d], ("e2", "e1"), a=float(i), b=0.0, minutes=1440.0)
    draws = cap.make_draws(days, 200, 7)
    g = cap.aggregate(rows, draws)
    assert g == cap.aggregate(rows, json.loads(json.dumps(draws)))           # a saved matrix reproduces the result
    ints = {tuple(x["pair"]): x["shortage"]["nominal_95"] for x in g if x["reference"] == "planned"}
    assert ints[("e1", "reactive")] == ints[("e2", "e1")]
    with pytest.raises(ValueError):
        cap.aggregate(rows, cap.make_draws(days[:3], 10, 7))


FROZEN = {"detector": "d" * 64, "identities": "i" * 64, "mask": "m" * 64}


def p8_result(start, stop, segments=(), gates=None, slots=None, **over):
    gates = {IE.iso(h): "verified" for h in range(start - IE.HOUR, stop, IE.HOUR)} if gates is None else gates
    cl = IE.Classifier([tuple(x) for x in segments], {IE.parse(h): g for h, g in gates.items()})
    slots = {IE.iso(t): cl.capacity(t, t + cap.SLOT) for t in range(start, stop, cap.SLOT)} if slots is None else slots
    res = {"detector": "infra_events.py v5", "start": IE.iso(start), "stop": IE.iso(stop),
           "run_identity": {"detector_sha256": FROZEN["detector"], "identities_sha256": FROZEN["identities"],
                            "mask_sha256": FROZEN["mask"]},
           "capacity_slots": slots, "gate_hours": gates, "segments_with_washout_ms": [list(x) for x in segments]}
    res.update(over)
    return res


def test_r40_p8_result_is_validated_before_it_is_applied():
    s, e = D0, D0 + cap.DAY
    ok = cap.p8_check(p8_result(s, e), s, e, FROZEN, IE)
    assert ok["summary"]["verified_clean"] == 144 and not ok["summary"]["compromised"]
    v6 = cap.p8_check(p8_result(s, e, detector="infra_events.py v6"), s, e, FROZEN, IE)   # D-1105: joined workers
    assert v6["summary"]["verified_clean"] == 144
    for bad in (p8_result(s, e, detector="infra_events.py v4"), p8_result(s, e, detector="infra_events.py v7"),
                p8_result(s, e, detector="infra_events.py v6-dev"),
                p8_result(s, e, run_identity=dict(p8_result(s, e)["run_identity"], mask_sha256="x")),
                p8_result(s, e - cap.SLOT),                                      # does not cover the window
                p8_result(s, e, segments=[(s, e, "clean-ish")]),
                p8_result(s, e, gates={IE.iso(s + 1): "verified"})):              # off-hour gate key
        with pytest.raises(ValueError):
            cap.p8_check(bad, s, e, FROZEN, IE)
    unk = cap.p8_check(p8_result(s, e, segments=[(s, e, "unknown")]), s, e, FROZEN, IE)
    assert unk["summary"]["compromised"]


def test_r41_saved_slots_must_equal_the_classification_recomputed_from_segments_and_gates():
    s, e = D0, D0 + cap.DAY
    clean = {IE.iso(t): "verified_clean" for t in range(s, e, cap.SLOT)}
    with pytest.raises(ValueError):                  # segments invalidate the whole day, slots claim clean
        cap.p8_check(p8_result(s, e, segments=[(s, e, "invalid")], slots=clean), s, e, FROZEN, IE)
    with pytest.raises(ValueError):                  # gates missing, slots claim clean
        cap.p8_check(p8_result(s, e, gates={}, slots=clean), s, e, FROZEN, IE)


def test_r41_a_loaded_draw_bank_must_be_exactly_the_declared_one():
    days = ["d1", "d2", "d3"]
    good = cap.make_draws(days, 50, 9)
    cap.check_draws(good, days, 50, 9)
    for bad in (dict(good, seed=10), dict(good, n_boot=10_000), dict(good, days=days[::-1]),
                dict(good, matrix=[[-1]]), dict(good, matrix=good["matrix"][:-1]),
                dict(good, matrix=[[0, 1, 3]] + good["matrix"][1:]), dict(good, matrix=[[0.0, 1, 2]] + good["matrix"][1:]),
                dict(good, matrix=[[0, 0, 0]] * 50)):                                  # right shape, wrong contents
        with pytest.raises(ValueError):
            cap.check_draws(bad, days, 50, 9)


def fake_vm(responses):
    """responses: function(path, params) -> bytes."""
    class Resp:
        def __init__(self, data):
            self.data = data
        def read(self):
            return self.data
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
    import urllib.parse as up
    def urlopen(url, timeout=None):
        path, q = url.split("?", 1)
        return Resp(responses(path.replace("http://vm", ""), dict(up.parse_qsl(q))))
    return urlopen


def test_r40_partial_and_conflicting_responses_are_rejected(monkeypatch, tmp_path):
    def partial(path, params):
        assert params["deny_partial_response"] == "1"
        return json.dumps({"status": "success", "isPartial": True, "data": {"result": []}}).encode()
    monkeypatch.setattr(cap.urllib.request, "urlopen", fake_vm(partial))
    with pytest.raises(RuntimeError):
        cap.range_values(cap.Store("http://vm", str(tmp_path / "a")), "q", D0, D0 + cap.DAY)

    def conflicting(path, params):
        return json.dumps({"status": "success", "data": {"result": [
            {"values": [[D0 / 1000, "5"]]}, {"values": [[D0 / 1000, "6"]]}]}}).encode()
    monkeypatch.setattr(cap.urllib.request, "urlopen", fake_vm(conflicting))
    with pytest.raises(ValueError):
        cap.range_values(cap.Store("http://vm", str(tmp_path / "b")), "q", D0, D0 + cap.DAY)


def test_r40_inputs_are_archived_and_a_run_replays_offline_with_the_same_result(monkeypatch, tmp_path):
    taus_day = range(D0, D0 + cap.DAY, STEP)

    def vm(path, params):
        if path == "/api/v1/export":
            return (json.dumps({"metric": {"__name__": "kube_deployment_status_replicas_ready"},
                                "timestamps": [t - 1_000 for t in taus_day], "values": [2] * len(taus_day)}) + "\n").encode()
        if path == "/api/v1/query_range":
            return json.dumps({"status": "success", "data": {"result": [
                {"values": [[t / 1000, "700"] for t in taus_day]}]}}).encode()
        return json.dumps({"status": "success", "data": {"result": [{"value": [0, "0"]}]}}).encode()
    monkeypatch.setattr(cap.urllib.request, "urlopen", fake_vm(vm))
    args = ["--start", "2026-10-07T00:00:00Z", "--stop", "2026-10-08T00:00:00Z", "--n-boot", "20",
            "--archive", str(tmp_path / "arch"), "--draws", str(tmp_path / "draws.json")]
    assert cap.main(args + ["--prom", "http://vm", "--json", str(tmp_path / "rec.json")]) == 0
    with pytest.raises(FileExistsError):                                     # a recorded archive is never reused
        cap.main(args + ["--prom", "http://vm", "--json", str(tmp_path / "x.json")])

    def offline(url, timeout=None):
        raise AssertionError("replay must not reach the network")
    monkeypatch.setattr(cap.urllib.request, "urlopen", offline)
    assert cap.main(args + ["--replay", "--json", str(tmp_path / "rep.json")]) == 0
    rec, rep = json.load(open(tmp_path / "rec.json")), json.load(open(tmp_path / "rep.json"))
    assert rec["aggregates"] == rep["aggregates"] and rec["arms"] == rep["arms"]
    assert rec["meta"]["inputs_manifest_sha256"] == rep["meta"]["inputs_manifest_sha256"]


def test_planned_requirement_is_at_least_one_pod_and_the_family_is_declared():
    assert cap.required(0) == 1 and cap.required(600) == 1 and cap.required(601) == 2
    assert cap.C1 == [("hybrid", "reactive"), ("hybrid", "keda"), ("s1", "reactive"), ("s1", "keda"),
                      ("e1", "reactive"), ("e1", "keda"), ("e2", "reactive"), ("e2", "keda")]
    assert cap.C4 == [("e2", "e1")] and cap.ARMS["e2"] == "nginx-ensemble-q95" and cap.ARMS["keda"] == "myapptwo"
    assert cap.label("e2", "2026-10-07T00:00:00Z").startswith("E2 ensemble (absolute q95")
    assert cap.label("e2", "2026-10-08T00:00:00Z") == cap.LABELS["e2"]


def test_r41_a_scored_run_must_use_the_declared_draw_contract(tmp_path):
    with pytest.raises(SystemExit):
        cap.main(["--start", "2026-10-07T00:00:00Z", "--stop", "2026-10-08T00:00:00Z", "--prom", "http://vm",
                  "--archive", str(tmp_path / "a"), "--p8-result", str(tmp_path / "p8.json"), "--n-boot", "100",
                  "--json", str(tmp_path / "o.json")])
