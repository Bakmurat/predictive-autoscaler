"""deploy/prodcluster/scoring/forecasts.py v3: P9 forecast scoring (C2, C3, the forecast part of C4); Codex r48/r49 cases."""
import hashlib
import importlib.util
import json
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "..", "..", "deploy", "prodcluster", "scoring", "forecasts.py")
SPEC = importlib.util.spec_from_file_location("forecasts", PATH)
fc = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fc)

D0 = 1_791_331_200_000             # 2026-10-07T00:00:00Z
SLOT, HOUR, DAY = fc.SLOT, fc.HOUR, fc.DAY


def iso(t, ms=False):
    s = fc.cap.iso(t)
    return s[:-1] + ".%03dZ" % (t % 1000) if ms else s


def issuance(origin, issued_delay=60_000, iid=None, cutoff=None, trained=None, values=None, anchor="inference_input_end",
             app="nginx-test", steps=None, artifact="a" * 64, ns="demo"):
    vals = values or [1000.0 + 10 * s for s in range(1, 7)]
    return {"issuance_id": iid or f"i{origin}", "issued_at": iso(origin + issued_delay), "application": app,
            "namespace": ns, "inference_input_end": iso(origin), "target_anchor": anchor, "model_version": "v",
            "model_trained_at": trained or iso(D0 - HOUR), "training_cutoff": cutoff or iso(D0 - HOUR),
            "artifact_sha256": artifact, "step_minutes": 10,
            "forecasts": [[s, iso(origin + s * SLOT), v] for s, v in zip(steps or range(1, 7), vals)]}


def test_acceptance_rules_in_log_order():
    o = D0
    recs = [issuance(o, iid="a"),
            dict(issuance(o + SLOT, iid="m"), training_cutoff=None),             # missing provenance
            issuance(o, iid="a"),                                              # duplicate id
            issuance(o + SLOT, issued_delay=-SLOT + 1_000, iid="b"),           # issued before the previous one
            issuance(o + 2 * SLOT, iid="c", cutoff=iso(D0 - 2 * HOUR)),        # cutoff moves backwards
            issuance(o + 3 * SLOT, iid="d", anchor="issued_at"),
            issuance(o + 4 * SLOT, iid="e", steps=[1, 2, 3, 4, 5, 7]),
            issuance(o + 5 * SLOT, iid="f", values=[1.0, 2.0, float("nan"), 4.0, 5.0, 6.0]),
            issuance(o + 6 * SLOT, iid="g", cutoff=iso(o + 7 * SLOT)),         # a target not after the cutoff
            issuance(o + 7 * SLOT, iid="h", cutoff=iso(o))]
    acc, rej = fc.accept(recs)
    assert [r["issuance_id"] for r in acc] == ["a", "h"]
    assert [x["reason"] for x in rej] == ["missing_provenance", "duplicate", "out_of_order", "stale_model", "anchor",
                                          "steps", "non_finite", "target_not_after_issuance_or_cutoff"]
    assert rej[0]["issuance_id"] == "m"


def test_r48_subsecond_chronology_and_timezone_parsing():
    assert fc.ts("2026-10-07T00:00:00.123456Z") == D0 + 123
    assert fc.ts("2026-10-07T02:00:00+02:00") == D0
    assert fc.ts("2026-10-07T00:00:00") is None and fc.ts("2026-10-07T00:00:00", naive_ok=True) == D0
    a = issuance(D0, iid="x", trained="2026-10-06T23:00:00.900Z")
    b = issuance(D0 + SLOT, iid="y", trained="2026-10-06T23:00:00.100Z")      # training time back by 0.8 s
    acc, rej = fc.accept([a, b])
    assert [r["issuance_id"] for r in acc] == ["x"] and rej[0]["reason"] == "stale_model"
    ens = issuance(D0, app="nginx-ensemble", trained="2026-10-06T23:00:00")   # the ensemble's naive UTC training time
    assert fc.accept([ens])[0] and not fc.accept([issuance(D0, trained="2026-10-06T23:00:00")])[0]


def ens_record(iss, **kw):
    e = {"application": iss["application"], "namespace": iss["namespace"], "origin": iss["inference_input_end"],
         "experiment": "seasonal-ensemble-q90-v1", "margin_mode": "absolute", "margin_quantile": 0.9, "partial_rule": "refuse",
         "fingerprint": iss["artifact_sha256"], "boundary": iss["training_cutoff"], "margin": 50.0,
         "served": [x[2] + 0.004 for x in iss["forecasts"]], "raw": [x[2] - 50.0 for x in iss["forecasts"]]}
    e.update(kw)
    return e


def test_r48_raw_attaches_only_to_the_generation_that_served_it():
    iss = issuance(D0, app="nginx-ensemble", values=[100.0] * 6)
    (r,), _ = fc.accept([iss])
    good = ens_record(iss)
    assert fc.attach_raw(r, [good]) == (good, "matched")
    for bad in (ens_record(iss, namespace="other"), ens_record(iss, fingerprint="b" * 64),
                ens_record(iss, boundary=iso(D0 - 2 * HOUR)), ens_record(iss, margin_mode="relative"),
                ens_record(iss, partial_rule="finite"),                                     # r49: the whole policy
                ens_record(iss, served=[100.0] * 5 + [100.5]),
                ens_record(iss, served=[100.0] * 5 + [100.006])):                           # r49: two-decimal equality
        assert fc.attach_raw(r, [bad]) == (None, "unavailable")
    assert fc.attach_raw(r, [good, dict(good)]) == (good, "matched")                       # equivalent duplicates merge
    for other in (ens_record(iss, raw=[1.0] * 6), ens_record(iss, margin=60.0)):            # r49: any conflicting field
        assert fc.attach_raw(r, [good, other]) == (None, "ambiguous")


def test_e2_policy_follows_the_d1084_switch():
    before, after = fc.cap.E2_SWITCH - HOUR, fc.cap.E2_SWITCH + HOUR
    assert fc.expected_policy("nginx-ensemble-q95", before) == ("seasonal-ensemble-q95-v1", "absolute", 0.95)
    assert fc.expected_policy("nginx-ensemble-q95", after) == ("seasonal-ensemble-rq90-v1", "relative", 0.9)


def test_per_step_metrics_coverage_and_per_day_counts():
    cs = [(D0, 1), (D0, 2), (D0 + SLOT, 1)]
    truth = {D0 + SLOT: 1000.0, D0 + 2 * SLOT: 1000.0}
    s = fc.summarize(cs, truth, {(D0, 1): 1100.0, (D0 + SLOT, 1): 900.0})
    assert s["per_step"][1] == {"eligible": 2, "answered": 2, "mae": 100.0, "bias": 0.0}
    assert s["per_step"][2]["answered"] == 0 and s["uniform_mae"] is None and s["coverage"] == 2 / 3
    assert s["per_day"][iso(D0)] == {"expected": 0, "eligible": 3, "answered": 2, "missing": 1}
    s2 = fc.summarize([], truth, {}, [iso(D0), iso(D0 + DAY)], {iso(D0 + DAY): 5})
    assert s2["per_day"][iso(D0 + DAY)] == {"expected": 5, "eligible": 0, "answered": 0, "missing": 0}   # declared day kept


def test_relative_improvement_paired_metrics_and_wording():
    origins = [D0 + i * SLOT for i in range(6)]
    cs = [(o, s) for o in origins for s in range(1, 7)]
    truth = {o + s * SLOT: 1000.0 for o, s in cs}
    va = {c: 1050.0 for c in cs}
    vb = {c: 1100.0 for c in cs if c[0] != origins[0]}
    c = fc.compare(cs, truth, va, vb, fc.cap.make_draws([iso(D0)], 20, 1))
    assert c["intersection_cells"] == 30 and c["relative_improvement"] == pytest.approx(0.5)
    assert c["paired_per_step"][1]["bias_a"] == 50.0 and c["paired_per_step"][1]["mae_b"] == 100.0
    assert "calibration not established" in c["interval_note"] and c["superiority"].startswith("not determined")
    zero = {"d": {s: [10.0, 0.0, 0.0, 0.0, 2] for s in range(1, 7)}}
    assert fc.rel_improvement(zero, ["d"])[0] is None


def test_r49_contamination_envelopes_are_conservative_and_need_p8_coverage():
    hs = D0 - 20 * DAY
    hyb = {"_cutoff": D0 - HOUR}
    w = fc.dependency_windows("hybrid", D0, hyb, hs)
    assert w == [(hs, D0)]                                                   # state chains back to the history start
    assert fc.dependency_windows("s1", D0, hyb, D0 - DAY) == [(D0 - 7 * DAY, D0)]   # inference lookback reaches before it
    old = [(hs + DAY, hs + DAY + HOUR, "invalid")]                            # 19 days before the origin
    assert fc.contamination(w, None, None) == "not_applied"
    assert fc.contamination(w, old, (hs - HOUR, D0 + DAY)) == "contaminated"
    assert fc.contamination(w, [], (hs + HOUR, D0 + DAY)) == "unknown"        # P8 starts after the history start
    assert fc.contamination(w, [], (hs - HOUR, D0 - HOUR)) == "unknown"       # nor reaches the origin
    assert fc.contamination(w, [], (hs - HOUR, D0 + DAY)) == "clean"
    assert fc.contamination(fc.dependency_windows("e1_raw", D0, {"_cutoff": None}, hs), [], (hs, D0)) == "unknown"
    raw = fc.dependency_windows("e1_raw", D0, {"_cutoff": D0 - HOUR}, hs)
    served = fc.dependency_windows("e1_served", D0, {"_cutoff": D0 - HOUR}, hs)
    assert raw[0][0] == D0 - HOUR - 360 * HOUR and served[0][0] < raw[0][0]   # served also depends on the margin window


def test_r49_envelope_inputs_come_from_the_frozen_configuration():
    cfg = fc.frozen_envelope_config()
    assert cfg["ENSEMBLE_HISTORY_HOURS"] == 360 and cfg["TRAINING_HOURS_overridden"] is False


def trailed(lines):
    body = ("\n".join(lines) + "\n").encode()
    return body.decode() + json.dumps({"trailer": {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}}) + "\n"


def receipt(extractor, key, fields, n, count_key):
    return {"receipt": {"extractor": extractor, "malformed_lines": 0, count_key: n, key: fields, "sha256": "a" * 64,
                        "lines": 1, "source": {"context": "c", "pvc": "p", "pod": "x"}, "extractor_sha256": "e" * 64}}


def test_r48_extractor_files_need_their_declared_schema(tmp_path):
    fields = fc.ENSEMBLE_FIELDS
    row = ["E"] + [None] * len(fields)
    good = [json.dumps(receipt("extract_ensemble.sh v2", "ensemble_fields", fields, 1, "ensemble_rows")), json.dumps(row)]
    p = tmp_path / "e.jsonl"
    p.write_text(trailed(good))
    assert len(fc.read_trailed(str(p), "extract_ensemble.sh v2", "E", "ensemble_fields", fields)[1]) == 1
    for bad in (trailed([json.dumps(receipt("extract_ensemble.sh v2", "ensemble_fields", fields[:-1], 1, "ensemble_rows")),
                         json.dumps(row[:-1])]),
                trailed([json.dumps(receipt("extract_ensemble.sh v2", "ensemble_fields", fields, 2, "ensemble_rows"))] + good[1:]),
                trailed([json.dumps(receipt("extract_ensemble.sh v1", "ensemble_fields", fields, 1, "ensemble_rows"))] + good[1:]),
                trailed([json.dumps({"receipt": dict(receipt("extract_ensemble.sh v2", "ensemble_fields", fields, 1,
                                                             "ensemble_rows")["receipt"], sha256="not-a-hash")})] + good[1:]),
                trailed([json.dumps({"receipt": dict(receipt("extract_ensemble.sh v2", "ensemble_fields", fields, 1,
                                                             "ensemble_rows")["receipt"], source={})})] + good[1:]),
                trailed(good).replace("null", "0", 1), "\n".join(good) + "\n"):
        p.write_text(bad)
        with pytest.raises(ValueError):
            fc.read_trailed(str(p), "extract_ensemble.sh v2", "E", "ensemble_fields", fields)


def test_r48_c4_and_views_use_only_the_window(tmp_path, monkeypatch):
    stop = D0 + DAY
    inside = issuance(D0 + 12 * HOUR, app="nginx-ensemble", iid="in")
    outside = issuance(stop + HOUR, app="nginx-ensemble", iid="out")
    inside2 = dict(issuance(D0 + 12 * HOUR, app="nginx-ensemble-q95", iid="in2"))
    rows = [json.dumps(receipt("extract_decisions.sh v3", "issuance_fields", fc.ISSUANCE_FIELDS, 3, "issuances"))]
    for r in (inside, inside2, outside):
        rows.append(json.dumps(["I"] + [r[k] for k in fc.ISSUANCE_FIELDS]))
    (tmp_path / "rows.jsonl").write_text(trailed(rows))
    ens = [json.dumps(receipt("extract_ensemble.sh v2", "ensemble_fields", fc.ENSEMBLE_FIELDS, 2, "ensemble_rows"))]
    for r in (inside, outside):
        e = ens_record(r)
        ens.append(json.dumps(["E"] + [e.get(k) for k in fc.ENSEMBLE_FIELDS]))
    (tmp_path / "ens.jsonl").write_text(trailed(ens))

    class Store:
        def __init__(self, *a, **k):
            self.manifest = str(tmp_path / "m")
            open(self.manifest, "w").write("")
    monkeypatch.setattr(fc.cap, "Store", Store)
    monkeypatch.setattr(fc.cap, "range_values", lambda *a: {t: 1000.0 for t in range(D0, stop + 2 * DAY, SLOT)})
    out = tmp_path / "o.json"
    assert fc.main(["--start", iso(D0), "--stop", iso(stop), "--rows", str(tmp_path / "rows.jsonl"),
                    "--ensemble", str(tmp_path / "ens.jsonl"), "--truth-archive", str(tmp_path / "t"),
                    "--draws", str(tmp_path / "d.json"), "--n-boot", "10", "--json", str(out)]) == 0
    r = json.load(open(out))
    assert r["C4"]["raw_attachment"] == {"e1": {"matched": 1}, "e2": {"unavailable": 1}}
    assert r["C4"]["margin_mean_rpm"]["e1"] == 50.0 and r["C4"]["origins_with_both_raw"] == 0
    assert set(r["views"]) == {"unfiltered", "operational", "clean_input"}
    assert r["views"]["operational"]["e1_served"]["coverage"] == pytest.approx(6 / 843)   # 144 × 6 − 21 late targets


def test_r50_c4_raw_identity_is_two_decimal_equality_and_generation_fields_join_duplicates():
    assert fc.cents(200.00) != fc.cents(200.01) and fc.cents(200.004) == fc.cents(200.0)
    e1 = {"fingerprint": "f" * 64, "raw": [200.00] * 6}
    assert fc.same_raw(e1, dict(e1, raw=[200.004] * 6))
    assert not fc.same_raw(e1, dict(e1, raw=[200.00] * 5 + [200.01]))                # one cent apart: not "margin only"
    assert not fc.same_raw(e1, dict(e1, fingerprint="g" * 64))
    assert not fc.same_raw(dict(e1, fingerprint=None), dict(e1, fingerprint=None))
    iss = issuance(D0, app="nginx-ensemble", values=[100.0] * 6)
    (r,), _ = fc.accept([iss])
    good = ens_record(iss, generation={"fingerprint": "a"}, margin_samples=60, stale_generation=False)
    conflicting = dict(good, generation={"fingerprint": "b"})
    assert fc.attach_raw(r, [good, conflicting]) == (None, "ambiguous")
    assert fc.attach_raw(r, [good, dict(good, stale_generation=True)]) == (None, "ambiguous")
