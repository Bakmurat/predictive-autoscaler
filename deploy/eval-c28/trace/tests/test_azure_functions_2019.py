"""Offline tests of the trace-demo selection and transformation on tiny synthetic inputs (no dataset needed)."""

import copy
import hashlib
import json
import os
import shutil
import subprocess

import pytest

import azure_functions_2019 as T

HERE = os.path.dirname(os.path.abspath(__file__))
C28_JS = os.path.join(HERE, "..", "..", "load", "c28.js")
LOADER = os.path.join(HERE, "c28js_loader.js")

# Small rules: 4 days of 6 minutes, history days 1-2, evaluation days 3-4.
R = dict(T.SELECTION_RULES, days=4, minutes_per_day=6, history_days=[1, 2], evaluation_days=[3, 4],
         history_mean_per_minute_band=[1.0, 100.0], history_min_active_fraction=0.5, strata=2, per_stratum=1)


def hx(s):
    return hashlib.sha256(s.encode()).hexdigest()


def write_day(d, day, rows, minutes=6, header=None):
    name = T.SELECTION_RULES["invocation_file_pattern"].format(day=day)
    hdr = header or ["HashOwner", "HashApp", "HashFunction", "Trigger"] + [str(i) for i in range(1, minutes + 1)]
    lines = [",".join(hdr)] + [",".join([o, a, f, t] + [str(c) for c in counts]) for o, a, f, t, counts in rows]
    p = d / name
    p.write_text("\n".join(lines) + "\n")
    return name


def sums_file(d, names):
    p = d / "SHA256SUMS"
    p.write_text("".join(f"{hashlib.sha256((d / n).read_bytes()).hexdigest()}  {n}\n" for n in names))
    return str(p)


def build(d, per_day):
    names = [write_day(d, day, rows) for day, rows in sorted(per_day.items())]
    return sums_file(d, names)


def day_path(d, day):
    return str(d / T.SELECTION_RULES["invocation_file_pattern"].format(day=day))


def sha(p):
    return T.sha256_file(str(p))


# --- frozen rules ---------------------------------------------------------------------------------------------------
def test_selection_rules_digest_is_the_v1_digest():
    # The v1 rules object is kept verbatim so the reviewed selection reproduces byte for byte; never edit it.
    assert T.rules_digest() == "a43854aa26a18f2deb64707d354076ff6574ed5bcf71cab403b8b180df0ca169"
    assert T.TRANSFORM_RULES["selection_rules_digest"] == T.rules_digest()
    assert set(T.SELECTION_KEYS) <= set(T.SELECTION_RULES)


def test_transform_rules_digest_is_pinned():
    # Changing any v2 transformation rule must be a deliberate edit of this value AND the contract JSON.
    assert T.transform_digest() == "9cc7be1385aaef033c3a9e66be69d9998328ad611834daab449c2c64383f024e"
    assert T.TRANSFORM_RULES["u_floor"] is None


# --- parsing ----------------------------------------------------------------------------------------------------------
def test_http_functions_summed_per_app_and_other_triggers_ignored(tmp_path):
    write_day(tmp_path, 1, [("o1", "a1", "f1", "http", [1, 2, 3, 0, 0, 1]),
                            ("o1", "a1", "f2", "http", [1, 1, 1, 1, 1, 1]),
                            ("o1", "a1", "f3", "timer", [9, 9, 9, 9, 9, 9])])
    funcs = {}
    sums, bad = T.read_day(day_path(tmp_path, 1), R, functions=funcs)
    assert bad == {}
    assert sums == {("o1", "a1"): [2, 3, 4, 1, 1, 2]}
    assert funcs == {("o1", "a1"): {"f1", "f2"}}


def test_duplicate_function_row_and_bad_counts_invalidate_the_app(tmp_path):
    write_day(tmp_path, 1, [("o1", "a1", "f1", "http", [1] * 6), ("o1", "a1", "f1", "http", [1] * 6),
                            ("o2", "a2", "f1", "http", [1, 1, 1, 1, 1, "x"]),
                            ("o3", "a3", "f1", "http", [1, 1, 1, 1, 1, -1]),
                            ("o4", "a4", "f1", "http", [1] * 6)])
    sums, bad = T.read_day(day_path(tmp_path, 1), R)
    assert bad == {("o1", "a1"): "duplicate-function-row", ("o2", "a2"): "non-integer-count",
                   ("o3", "a3"): "negative-count"}
    assert list(sums) == [("o4", "a4")]


def test_bad_header_and_malformed_row_abort(tmp_path):
    write_day(tmp_path, 1, [], header=["HashOwner", "HashApp", "Trigger", "HashFunction"] + [str(i) for i in range(1, 7)])
    with pytest.raises(T.TraceError, match="header"):
        T.read_day(day_path(tmp_path, 1), R)
    write_day(tmp_path, 2, [("o1", "a1", "f1", "http", [1] * 5)])
    with pytest.raises(T.TraceError, match="fields"):
        T.read_day(day_path(tmp_path, 2), R)


def test_hash_mismatch_and_unpinned_file_refused(tmp_path):
    per = {d: [("o1", "a1", "f1", "http", [5] * 6)] for d in range(1, 5)}
    s = build(tmp_path, per)
    sums = T.read_sums(s)
    T.day_files(str(tmp_path), sums, R)
    write_day(tmp_path, 3, [("o1", "a1", "f1", "http", [6] * 6)])
    with pytest.raises(T.TraceError, match="does not match"):
        T.day_files(str(tmp_path), sums, R)
    del sums[T.SELECTION_RULES["invocation_file_pattern"].format(day=4)]
    with pytest.raises(T.TraceError, match="not pinned"):
        T.day_files(str(tmp_path), sums, R, verify=False)


def test_sums_file_rejects_paths(tmp_path):
    p = tmp_path / "S"
    p.write_text("0" * 64 + "  ../evil.csv\n")
    with pytest.raises(T.TraceError, match="plain names"):
        T.read_sums(str(p))


# --- selection (unchanged from v1) ------------------------------------------------------------------------------------
def test_eligibility_stages(tmp_path):
    full = [5] * 6
    per = {}
    for d in range(1, 5):
        rows = [("o1", "good", "f", "http", full),
                ("o2", "lowvol", "f", "http", [0, 0, 0, 0, 0, 1]),
                ("o3", "sparse", "f", "http", [30, 0, 0, 0, 0, 0]),
                ("o4", "huge", "f", "http", [1000] * 6)]
        if d != 3:
            rows.append(("o5", "gap", "f", "http", full))
        rows.append(("o6", "zeroday", "f", "http", full if d != 4 else [0] * 6))
        if d == 2:
            rows += [("o7", "dup", "f", "http", full), ("o7", "dup", "f", "http", full)]
        else:
            rows.append(("o7", "dup", "f", "http", full))
        per[d] = rows
    s = build(tmp_path, per)
    e = T.eligibility(T.day_files(str(tmp_path), T.read_sums(s), R), R)
    assert e["stages"] == {"http_apps_seen": 7, "valid_rows": 6, "complete_all_days": 4,
                           "history_volume_band": 2, "history_active_fraction": 1}
    assert e["invalid_reasons"] == {"duplicate-function-row": 1}
    assert [r["app"] for r in e["eligible"]] == ["o1:good"]


def _elig(n):
    return [{"app": f"own{i % (n // 2)}:app{i:03d}", "owner": f"own{i % (n // 2)}", "history_mean_per_minute": float(i),
             "history_active_fraction": 1.0} for i in range(n)]


def test_strata_are_contiguous_volume_ranks():
    g = T.strata(_elig(10), dict(T.SELECTION_RULES, strata=3))
    assert [len(x) for x in g] == [3, 4, 3]
    assert max(r["history_mean_per_minute"] for r in g[0]) < min(r["history_mean_per_minute"] for r in g[1])


def test_draw_is_deterministic_seeded_and_owner_distinct():
    e = _elig(30)
    a, b = T.draw(e), T.draw(list(reversed(e)))
    assert a == b
    assert len(a["chosen"]) == 6 and a["shortfall"] == {}
    assert len({c["owner"] for c in a["chosen"]}) == 6
    assert [c["stratum"] for c in a["chosen"]] == [0, 0, 1, 1, 2, 2]
    assert sorted(a["live_order"]) == sorted(c["app"] for c in a["chosen"])
    c = T.draw(e, dict(T.SELECTION_RULES, seed=T.SELECTION_RULES["seed"] + 1))
    assert [x["app"] for x in c["chosen"]] != [x["app"] for x in a["chosen"]]


def test_draw_records_shortfall_without_backfill():
    e = [{"app": f"same:app{i}", "owner": "same", "history_mean_per_minute": float(i), "history_active_fraction": 1.0}
         for i in range(6)]
    d = T.draw(e)
    assert len(d["chosen"]) == 1 and d["shortfall"] == {"0": 1, "1": 2, "2": 2}


# --- missingness (A5) -------------------------------------------------------------------------------------------------
def test_absent_function_days_imputed_zero_and_counted(tmp_path):
    per = {d: [("o1", "a", "f1", "http", [4] * 6)] + ([("o1", "a", "f2", "http", [2] * 6)] if d == 1 else [])
           for d in range(1, 5)}
    s = build(tmp_path, per)
    files = T.day_files(str(tmp_path), T.read_sums(s), R)
    series, miss = T.collect_series(files, [("o1", "a")], R)
    assert series[("o1", "a")] == [6] * 6 + [4] * 18
    assert miss[("o1", "a")] == {"http_functions": 2, "function_days": 8, "absent_function_days_imputed_zero": 3,
                                 "absent_by_partition": {"history": 1, "evaluation": 2}}


def test_http_row_census_counts_zero_total_rows(tmp_path):
    write_day(tmp_path, 1, [("o", "a", "f1", "http", [0] * 6), ("o", "a", "f2", "http", [1] * 6),
                            ("o", "a", "f3", "timer", [0] * 6)])
    assert T.http_row_census(day_path(tmp_path, 1), R) == (2, 1)


# --- transformation (A1, A3) ------------------------------------------------------------------------------------------
def test_peak_nearest_rank_uses_history_only():
    series = [1, 2, 3, 4, 5, 6] + [1] * 6 + [1000] * 12
    assert T.peak(series, R) == T.nearest_rank([1, 2, 3, 4, 5, 6] + [1] * 6, 0.995) == 6
    assert T.nearest_rank(list(range(1, 1001)), 0.995) == 995


def test_to_u_has_no_floor_and_keeps_zero_and_low_rate_minutes():
    u = T.to_u([0, 1, 10, 20, 50], 10)
    assert u == [0.0, 0.9, 9.0, 10.0, 10.0]
    assert T.to_u([0, 1], 1000) == [0.0, 0.009]
    with pytest.raises(T.TraceError, match="no floor"):
        T.to_u([1], 1, dict(T.TRANSFORM_RULES, u_floor=0.1))
    st = T.transform_stats([0] * 6 + [10] * 6 + [20] * 6 + [1] * 6, 10, R)
    assert st["history"] == {"minutes": 12, "clipped": 0, "zero_count_minutes": 6, "below_0_1c_minutes": 6}
    assert st["evaluation"] == {"minutes": 12, "clipped": 6, "zero_count_minutes": 0, "below_0_1c_minutes": 0}


def test_intended_counts_scale_with_capacity_symbol():
    u = [0.0, 0.009, 9.0, 10.0]
    assert T.intended_per_minute(u, 35.0) == [0, 19, 18900, 21000]
    assert T.intended_per_minute(u, 20.0) == [0, 11, 10800, 12000]
    for bad in (0, -1.0, float("inf"), float("nan")):
        with pytest.raises(T.TraceError):
            T.intended_per_minute(u, bad)


APP = f"{hx('owner')}:{hx('app')}"
BIND = {"prepared_sha256": "a" * 64, "selection_sha256": "b" * 64, "sums_sha256": "c" * 64}


def test_plan_conserves_every_minute_including_zero_and_jumps():
    u = [1.0, 2.0, 2.0, 0.0, 10.0, 0.5]
    p = T.k6_plan(u, 10.0, APP, 0, BIND)
    assert p["intended_per_minute"] == [600, 1200, 1200, 0, 6000, 300]
    assert p["scheduled_per_minute"] == [600, 1200, 1200, 0, 6000, 300]
    # Codex's ramp example: [600, 1200] must not integrate to [600, 1195].
    q = T.k6_plan([1.0, 2.0], 10.0, APP, 0, BIND)
    assert T.stage_minutes(q["stages"], q["start_rate"], 2) == [600.0, 1200.0]
    ramp = [{"target": 600, "duration": "1000ms"}, {"target": 600, "duration": "59000ms"},
            {"target": 1200, "duration": "1000ms"}, {"target": 1200, "duration": "59000ms"}]
    assert [round(x, 6) for x in T.stage_minutes(ramp, 600, 2)] == [600.0, 1195.0]


def test_plan_shape_binding_wording_and_jitter():
    p = T.k6_plan([1.0, 2.0, 2.0], 10.0, APP, 0, BIND)
    assert p["schema"] == "c28-k6-plan/v1" and p["kind"] == p["label"] == "trace-demo"
    assert p["target"] == "c28-trace-" + hx("app")[:12] and p["seed"] == 10 * T.SELECTION_RULES["seed"]
    assert p["accelerate"] == 1 and p["time_unit"] == "1m" and p["retries"] == 0 and p["timeout"] == "2s"
    assert p["stages"] == [{"target": 600, "duration": "60000ms"}, {"target": 1200, "duration": "0ms"},
                           {"target": 1200, "duration": "60000ms"}, {"target": 1200, "duration": "60000ms"}]
    assert "invocation counts" in p["wording"] and "real" not in p["wording"]
    assert p["binding"]["prepared_sha256"] == "a" * 64 and p["binding"]["transform_rules_digest"] == T.transform_digest()
    assert p["jitter_seed"] == T.jitter_seed(APP, 0) != T.jitter_seed(APP, 1)
    assert T.k6_plan([1.0], 10.0, APP, 1, BIND)["seed"] == 10 * T.SELECTION_RULES["seed"] + 1
    body = {k: v for k, v in p.items() if k != "digest"}
    assert p["digest"] == T.digest(body)
    with pytest.raises(T.TraceError):
        T.k6_plan([1.0], 10.0, APP, 3, BIND)


def _node():
    node = shutil.which("node")
    if node is None:
        if os.environ.get("CI"):
            pytest.fail("node is required in CI for the c28.js integration test")
        pytest.skip("node not installed: the c28.js integration test cannot run here")
    return node


def _load_c28(tmp_path, plan):
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    out = subprocess.run([_node(), LOADER, C28_JS, str(path)], capture_output=True, text=True, timeout=60,
                         env={"PATH": os.environ.get("PATH", "")})
    assert out.returncode == 0, out.stdout + out.stderr
    return json.loads(out.stdout)


def test_emitted_plan_passes_the_real_c28js_validator(tmp_path):
    u = [0.0] * 3 + [9.0] * 2 + [0.25] * 3 + [10.0]
    p = T.k6_plan(u, 35.0, APP, 0, BIND)
    res = _load_c28(tmp_path, p)
    assert res["ok"], res
    sc = res["options"]["scenarios"]["c28"]
    assert sc["executor"] == "ramping-arrival-rate" and sc["timeUnit"] == "1m"
    assert sc["startRate"] == 0 and sc["stages"] == p["stages"]
    assert sc["preAllocatedVUs"] == p["pre_allocated_vus"] <= sc["maxVUs"]
    tags = res["options"]["tags"]
    assert tags == {"c28_target": p["target"], "c28_seed": str(p["seed"]), "c28_plan": p["digest"],
                    "c28_label": "trace-demo"}
    # The v1 plan schema is refused by the same validator, which is why v1 plans could not run.
    bad = dict(p, schema="c28-trace-k6-plan/v1")
    assert _load_c28(tmp_path, bad) == {"ok": False, "error": "not a c28 k6 plan"}


def test_loader_also_accepts_a_c28_harness_plan(tmp_path):
    # Sanity: the loader is not trace-specific; the C2.8 harness's own plan loads the same way.
    import sys
    sys.path.insert(0, os.path.join(HERE, "..", "..", "harness"))
    from c28 import schedule
    res = _load_c28(tmp_path, schedule.k6_plan("ordinary", 201, 35.0, "c28-pa-n-s20"))
    assert res["ok"], res


# --- provenance and validation (A4) -----------------------------------------------------------------------------------
def _pipeline(tmp_path, monkeypatch, n_apps=8):
    data = tmp_path / "data"
    data.mkdir()
    per = {d: [(hx(f"o{i}"), hx(f"a{i}"), "f", "http", [i + 1 + (m % 3) for m in range(6)]) for i in range(n_apps)]
           for d in range(1, 5)}
    s = build(data, per)
    monkeypatch.setattr(T, "SELECTION_RULES", R)
    monkeypatch.setattr(T, "TRANSFORM_RULES", dict(T.TRANSFORM_RULES, selection_rules_digest=T.rules_digest(R)))
    out = tmp_path / "out"
    out.mkdir()
    return data, s, out


def test_cli_select_prepare_plan_end_to_end_with_bound_hashes(tmp_path, monkeypatch):
    data, s, out = _pipeline(tmp_path, monkeypatch)
    assert T.main(["select", "--data", str(data), "--sums", s, "--expect-sums", sha(s),
                   "--out", str(out / "sel.json")]) == 0
    sel = json.loads((out / "sel.json").read_text())
    assert sel["eligible_count"] == 8 and len(sel["chosen"]) == 2
    assert T.main(["prepare", "--data", str(data), "--sums", s, "--selection", str(out / "sel.json"),
                   "--expect-selection", sha(out / "sel.json"), "--out", str(out / "prep.json")]) == 0
    prep = json.loads((out / "prep.json").read_text())
    assert prep["selection_sha256"] == sha(out / "sel.json") and prep["sums_sha256"] == sha(s)
    app = prep["app_order"][0]
    assert len(prep["apps"][app]["u"]) == 24 and min(prep["apps"][app]["u"]) > 0
    assert T.main(["plan", "--prepared", str(out / "prep.json"), "--expect-prepared", sha(out / "prep.json"),
                   "--app", app, "--capacity", "30", "--out", str(out / "plan.json")]) == 0
    plan = json.loads((out / "plan.json").read_text())
    assert plan["binding"]["prepared_sha256"] == sha(out / "prep.json")
    assert plan["binding"]["selection_sha256"] == sha(out / "sel.json") and plan["binding"]["sums_sha256"] == sha(s)
    assert plan["intended_per_minute"] == plan["scheduled_per_minute"] and len(plan["intended_per_minute"]) == 24
    # Outputs are never overwritten.
    before = (out / "plan.json").read_bytes()
    assert T.main(["plan", "--prepared", str(out / "prep.json"), "--expect-prepared", sha(out / "prep.json"),
                   "--app", app, "--capacity", "31", "--out", str(out / "plan.json")]) == 2
    assert (out / "plan.json").read_bytes() == before
    assert not [f for f in os.listdir(out) if ".tmp-" in f]


def test_stage_hash_bindings_are_enforced(tmp_path, monkeypatch):
    data, s, out = _pipeline(tmp_path, monkeypatch)
    assert T.main(["select", "--data", str(data), "--sums", s, "--expect-sums", "0" * 64,
                   "--out", str(out / "x.json")]) == 2
    assert T.main(["select", "--data", str(data), "--sums", s, "--expect-sums", sha(s),
                   "--out", str(out / "sel.json")]) == 0
    selp = str(out / "sel.json")
    # Wrong reviewed hash for the selection.
    assert T.main(["prepare", "--data", str(data), "--sums", s, "--selection", selp, "--expect-selection", "1" * 64,
                   "--out", str(out / "p1.json")]) == 2
    # A sums file other than the selection's.
    s2 = tmp_path / "S2"
    s2.write_text(open(s).read() + "\n")
    assert T.main(["prepare", "--data", str(data), "--sums", str(s2), "--selection", selp,
                   "--expect-selection", sha(selp), "--out", str(out / "p2.json")]) == 2
    # A selection whose recorded input hash differs from the pinned file.
    sel = json.loads(open(selp).read())
    name = sorted(sel["inputs"])[0]
    sel["inputs"][name] = "f" * 64
    (out / "sel-bad.json").write_text(json.dumps(sel))
    assert T.main(["prepare", "--data", str(data), "--sums", s, "--selection", str(out / "sel-bad.json"),
                   "--expect-selection", sha(out / "sel-bad.json"), "--out", str(out / "p3.json")]) == 2
    # A selection made under other selection rules.
    sel = json.loads(open(selp).read())
    sel["rules"] = dict(sel["rules"], seed=1)
    (out / "sel-rules.json").write_text(json.dumps(sel))
    assert T.main(["prepare", "--data", str(data), "--sums", s, "--selection", str(out / "sel-rules.json"),
                   "--expect-selection", sha(out / "sel-rules.json"), "--out", str(out / "p4.json")]) == 2
    assert not any((out / f"p{i}.json").exists() for i in range(1, 5))


def test_codex_u999_wrong_schema_prepared_file_is_refused(tmp_path, monkeypatch):
    _, _, out = _pipeline(tmp_path, monkeypatch)
    fake = {"schema": "c28-trace-prepared/v1", "rules_digest": T.rules_digest(R), "apps": {APP: {"u": [999]}}}
    (out / "fake.json").write_text(json.dumps(fake))
    assert T.main(["plan", "--prepared", str(out / "fake.json"), "--expect-prepared", sha(out / "fake.json"),
                   "--app", APP, "--capacity", "30", "--out", str(out / "plan.json")]) == 2
    assert not (out / "plan.json").exists()


@pytest.mark.parametrize("mutate,msg", [
    (lambda p, a: p["apps"][a].__setitem__("u", [999]), "minutes"),
    (lambda p, a: p["apps"][a].__setitem__("u", [999.0] * 24), "within"),
    (lambda p, a: p["apps"][a]["u"].__setitem__(3, p["apps"][a]["u"][3] + 0.5), "transformation"),
    (lambda p, a: p["apps"][a]["counts"].__setitem__(20, 10_000), "transformation"),
    (lambda p, a: p["apps"][a].__setitem__("history_peak_p995", p["apps"][a]["history_peak_p995"] + 1), "p99.5"),
    (lambda p, a: p["apps"][a]["counts"].__setitem__(0, -1), "non-negative"),
    (lambda p, a: p["apps"][a]["counts"].__setitem__(0, 1.5), "non-negative"),
    (lambda p, a: p["apps"][a]["counts"].pop(), "minutes"),
    (lambda p, a: p.__setitem__("app_order", p["app_order"][:1]), "app_order"),
    (lambda p, a: p["transform_rules"].__setitem__("u_max", 12.0), "transformation rules"),
    (lambda p, a: p.__setitem__("schema", "c28-trace-prepared/v1"), "schema"),
])
def test_prepared_validation_rejects_tampering(tmp_path, monkeypatch, mutate, msg):
    data, s, out = _pipeline(tmp_path, monkeypatch)
    assert T.main(["select", "--data", str(data), "--sums", s, "--expect-sums", sha(s),
                   "--out", str(out / "sel.json")]) == 0
    assert T.main(["prepare", "--data", str(data), "--sums", s, "--selection", str(out / "sel.json"),
                   "--expect-selection", sha(out / "sel.json"), "--out", str(out / "prep.json")]) == 0
    prep = json.loads((out / "prep.json").read_text())
    T.validate_prepared(copy.deepcopy(prep))
    app = prep["app_order"][0]
    mutate(prep, app)
    with pytest.raises(T.TraceError, match=msg):
        T.validate_prepared(prep)
    (out / "bad.json").write_text(json.dumps(prep))
    assert T.main(["plan", "--prepared", str(out / "bad.json"), "--expect-prepared", sha(out / "bad.json"),
                   "--app", app, "--capacity", "30", "--out", str(out / "plan.json")]) == 2
    assert not (out / "plan.json").exists()


def test_plan_refuses_an_app_outside_the_prepared_set(tmp_path, monkeypatch):
    data, s, out = _pipeline(tmp_path, monkeypatch)
    T.main(["select", "--data", str(data), "--sums", s, "--expect-sums", sha(s), "--out", str(out / "sel.json")])
    T.main(["prepare", "--data", str(data), "--sums", s, "--selection", str(out / "sel.json"),
            "--expect-selection", sha(out / "sel.json"), "--out", str(out / "prep.json")])
    assert T.main(["plan", "--prepared", str(out / "prep.json"), "--expect-prepared", sha(out / "prep.json"),
                   "--app", APP, "--capacity", "30", "--out", str(out / "plan.json")]) == 2


def test_nonfinite_json_is_refused(tmp_path):
    p = tmp_path / "x.json"
    p.write_text('{"a": NaN}')
    with pytest.raises(T.TraceError, match="non-finite"):
        T._load(str(p))
