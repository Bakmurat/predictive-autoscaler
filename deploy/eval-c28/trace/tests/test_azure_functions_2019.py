"""Offline tests of the trace-demo selection and transformation on tiny synthetic inputs (no dataset needed)."""

import hashlib
import json
import os

import pytest

import azure_functions_2019 as T

# Small rules: 4 days of 6 minutes, history days 1-2, evaluation days 3-4.
R = dict(T.RULES, days=4, minutes_per_day=6, history_days=[1, 2], evaluation_days=[3, 4],
         history_mean_per_minute_band=[1.0, 100.0], history_min_active_fraction=0.5, strata=2, per_stratum=1)


def write_day(d, day, rows, minutes=6, header=None):
    name = T.RULES["invocation_file_pattern"].format(day=day)
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


def test_rules_digest_is_pinned():
    # Changing any frozen rule must be a deliberate edit of this value AND the contract JSON.
    assert T.rules_digest() == "a43854aa26a18f2deb64707d354076ff6574ed5bcf71cab403b8b180df0ca169"


def test_http_functions_summed_per_app_and_other_triggers_ignored(tmp_path):
    write_day(tmp_path, 1, [("o1", "a1", "f1", "http", [1, 2, 3, 0, 0, 1]),
                            ("o1", "a1", "f2", "http", [1, 1, 1, 1, 1, 1]),
                            ("o1", "a1", "f3", "timer", [9, 9, 9, 9, 9, 9])])
    sums, bad = T.read_day(str(tmp_path / T.RULES["invocation_file_pattern"].format(day=1)), R)
    assert bad == {}
    assert sums == {("o1", "a1"): [2, 3, 4, 1, 1, 2]}


def test_duplicate_function_row_and_bad_counts_invalidate_the_app(tmp_path):
    write_day(tmp_path, 1, [("o1", "a1", "f1", "http", [1] * 6), ("o1", "a1", "f1", "http", [1] * 6),
                            ("o2", "a2", "f1", "http", [1, 1, 1, 1, 1, "x"]),
                            ("o3", "a3", "f1", "http", [1, 1, 1, 1, 1, -1]),
                            ("o4", "a4", "f1", "http", [1] * 6)])
    sums, bad = T.read_day(str(tmp_path / T.RULES["invocation_file_pattern"].format(day=1)), R)
    assert bad == {("o1", "a1"): "duplicate-function-row", ("o2", "a2"): "non-integer-count",
                   ("o3", "a3"): "negative-count"}
    assert list(sums) == [("o4", "a4")]


def test_bad_header_and_malformed_row_abort(tmp_path):
    write_day(tmp_path, 1, [], header=["HashOwner", "HashApp", "Trigger", "HashFunction"] + [str(i) for i in range(1, 7)])
    with pytest.raises(T.TraceError, match="header"):
        T.read_day(str(tmp_path / T.RULES["invocation_file_pattern"].format(day=1)), R)
    write_day(tmp_path, 2, [("o1", "a1", "f1", "http", [1] * 5)])
    with pytest.raises(T.TraceError, match="fields"):
        T.read_day(str(tmp_path / T.RULES["invocation_file_pattern"].format(day=2)), R)


def test_hash_mismatch_and_unpinned_file_refused(tmp_path):
    per = {d: [("o1", "a1", "f1", "http", [5] * 6)] for d in range(1, 5)}
    s = build(tmp_path, per)
    sums = T.read_sums(s)
    T.day_files(str(tmp_path), sums, R)
    write_day(tmp_path, 3, [("o1", "a1", "f1", "http", [6] * 6)])
    with pytest.raises(T.TraceError, match="does not match"):
        T.day_files(str(tmp_path), sums, R)
    del sums[T.RULES["invocation_file_pattern"].format(day=4)]
    with pytest.raises(T.TraceError, match="not pinned"):
        T.day_files(str(tmp_path), sums, R, verify=False)


def test_sums_file_rejects_paths(tmp_path):
    p = tmp_path / "S"
    p.write_text("0" * 64 + "  ../evil.csv\n")
    with pytest.raises(T.TraceError, match="plain names"):
        T.read_sums(str(p))


def test_eligibility_stages(tmp_path):
    full = [5] * 6
    per = {}
    for d in range(1, 5):
        rows = [("o1", "good", "f", "http", full),
                ("o2", "lowvol", "f", "http", [0, 0, 0, 0, 0, 1]),           # mean < 1/min in history
                ("o3", "sparse", "f", "http", [30, 0, 0, 0, 0, 0]),           # mean 5 but active 1/6 < 0.5
                ("o4", "huge", "f", "http", [1000] * 6)]                      # mean > 100
        if d != 3:
            rows.append(("o5", "gap", "f", "http", full))                     # absent on day 3
        rows.append(("o6", "zeroday", "f", "http", full if d != 4 else [0] * 6))  # zero total on day 4
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


def test_missing_function_day_counts_as_zero(tmp_path):
    per = {d: [("o1", "a", "f1", "http", [4] * 6)] + ([("o1", "a", "f2", "http", [2] * 6)] if d == 1 else [])
           for d in range(1, 5)}
    s = build(tmp_path, per)
    files = T.day_files(str(tmp_path), T.read_sums(s), R)
    x = T.collect_series(files, [("o1", "a")], R)[("o1", "a")]
    assert x == [6] * 6 + [4] * 18


def _elig(n):
    return [{"app": f"own{i % (n // 2)}:app{i:03d}", "owner": f"own{i % (n // 2)}", "history_mean_per_minute": float(i),
             "history_active_fraction": 1.0} for i in range(n)]


def test_strata_are_contiguous_volume_ranks():
    g = T.strata(_elig(10), dict(T.RULES, strata=3))
    assert [len(x) for x in g] == [3, 4, 3]
    assert max(r["history_mean_per_minute"] for r in g[0]) < min(r["history_mean_per_minute"] for r in g[1])


def test_draw_is_deterministic_seeded_and_owner_distinct():
    e = _elig(30)
    a, b = T.draw(e), T.draw(list(reversed(e)))
    assert a == b                                       # input order does not matter
    assert len(a["chosen"]) == 6 and a["shortfall"] == {}
    assert len({c["owner"] for c in a["chosen"]}) == 6
    assert [c["stratum"] for c in a["chosen"]] == [0, 0, 1, 1, 2, 2]
    assert sorted(a["live_order"]) == sorted(c["app"] for c in a["chosen"])
    c = T.draw(e, dict(T.RULES, seed=T.RULES["seed"] + 1))
    assert [x["app"] for x in c["chosen"]] != [x["app"] for x in a["chosen"]]


def test_draw_records_shortfall_without_backfill():
    e = [{"app": f"same:app{i}", "owner": "same", "history_mean_per_minute": float(i), "history_active_fraction": 1.0}
         for i in range(6)]
    d = T.draw(e)
    assert len(d["chosen"]) == 1 and d["shortfall"] == {"0": 1, "1": 2, "2": 2}


def test_peak_nearest_rank_uses_history_only():
    m = 6
    series = [1, 2, 3, 4, 5, 6] + [1] * 6 + [1000] * 12      # evaluation spike must not move P
    assert T.peak(series, R) == T.nearest_rank([1, 2, 3, 4, 5, 6] + [1] * 6, 0.995) == 6
    assert T.nearest_rank(list(range(1, 1001)), 0.995) == 995
    assert m == R["minutes_per_day"]


def test_to_u_scaling_floor_and_clip():
    u = T.to_u([0, 1, 10, 20, 50], 10)
    assert u == [0.1, 0.9, 9.0, 10.0, 10.0]
    st = T.transform_stats([0] * 6 + [10] * 6 + [20] * 6 + [5] * 6, 10, R)
    assert st["history"] == {"minutes": 12, "floored": 6, "clipped": 0, "zero_count_minutes": 6}
    assert st["evaluation"] == {"minutes": 12, "floored": 0, "clipped": 6, "zero_count_minutes": 0}


def test_arrivals_scale_with_capacity_symbol():
    u = [0.1, 9.0, 10.0]
    assert T.arrivals_per_minute(u, 35.0) == [210, 18900, 21000]
    assert T.arrivals_per_minute(u, 20.0) == [120, 10800, 12000]
    with pytest.raises(T.TraceError):
        T.arrivals_per_minute(u, 0)


def test_plan_shape_wording_and_jitter():
    u = [1.0, 2.0, 2.0]
    p = T.k6_plan(u, 10.0, "o:a")
    assert p["accelerate"] == 1 and p["time_unit"] == "1m" and p["retries"] == 0
    assert len(p["stages"]) == 6 and p["stages"][2] == {"target": 1200, "duration": "1000ms"}
    assert p["scheduled_per_minute"][0] == 600 and p["scheduled_per_minute"][2] == 1200
    assert "invocation counts" in p["wording"] and "real" not in p["wording"]
    assert p["jitter_seed"] == T.jitter_seed("o:a", 0) != T.jitter_seed("o:a", 1)
    body = {k: v for k, v in p.items() if k != "digest"}
    assert p["digest"] == T.digest(body)
    with pytest.raises(T.TraceError):
        T.k6_plan(u, 10.0, "o:a", repetition=3)


def test_cli_select_prepare_plan_end_to_end(tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    per = {}
    for d in range(1, 5):
        per[d] = [(f"o{i}", f"a{i}", "f", "http", [i + 1 + (m % 3) for m in range(6)]) for i in range(8)]
    s = build(data, per)
    monkeypatch.setattr(T, "RULES", R)
    out = tmp_path / "out"
    out.mkdir()
    assert T.main(["select", "--data", str(data), "--sums", s, "--out", str(out / "sel.json")]) == 0
    sel = json.loads((out / "sel.json").read_text())
    assert sel["eligible_count"] == 8 and len(sel["chosen"]) == 2 and sel["rules_digest"] == T.rules_digest(R)
    assert T.main(["prepare", "--data", str(data), "--sums", s, "--selection", str(out / "sel.json"),
                   "--out", str(out / "prep.json")]) == 0
    prep = json.loads((out / "prep.json").read_text())
    app = sel["chosen"][0]["app"]
    assert len(prep["apps"][app]["u"]) == 24
    assert max(prep["apps"][app]["u"][:12]) == pytest.approx(9.0)
    assert T.main(["plan", "--prepared", str(out / "prep.json"), "--app", app, "--capacity", "30",
                   "--out", str(out / "plan.json")]) == 0
    plan = json.loads((out / "plan.json").read_text())
    assert len(plan["stages"]) == 48
    # A selection made under other rules is refused.
    monkeypatch.setattr(T, "RULES", dict(R, seed=1))
    assert T.main(["prepare", "--data", str(data), "--sums", s, "--selection", str(out / "sel.json"),
                   "--out", str(out / "prep2.json")]) == 2
    assert not os.path.exists(out / "prep2.json")
