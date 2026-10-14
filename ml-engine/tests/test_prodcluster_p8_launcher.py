"""deploy/prodcluster/p8_launcher.py: gate copy, run ownership and result revisions (Codex Task 03 r44)."""
import importlib.util
import json
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "..", "..", "deploy", "prodcluster", "p8_launcher.py")
SPEC = importlib.util.spec_from_file_location("p8_launcher", PATH)
pl = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pl)

START, STOP = "2026-10-07T00:00:00Z", "2026-10-07T03:00:00Z"
S, E = pl.epoch(START), pl.epoch(STOP)


def rows_for(h, status="PASS", finalized=True, arms=pl.ARMS):
    return [{"app": a, "hour_start": pl.time.strftime("%Y-%m-%dT%H:%M:%SZ", pl.time.gmtime(h)), "status": status,
             "maturity": {"finalized": finalized}} for a in arms]


def attempt_name(h, collected):
    return pl.hour_name(h)[:-len(".json")] + f"-{collected}-1.json"


def gate_dir(tmp_path, hours=None, status="PASS", finalized=True, drop_arm=False):
    """The collector's layout: one retained attempt per evaluation in attempts/, plus the latest projection."""
    d = tmp_path / "gates"
    (d / "attempts").mkdir(parents=True)
    for h in hours if hours is not None else range(S - 3600, E, 3600):
        text = json.dumps(rows_for(h, status, finalized, pl.ARMS[:-1] if drop_arm else pl.ARMS))
        (d / "attempts" / attempt_name(h, "20261007T091500Z")).write_text(text)
        (d / pl.hour_name(h)).write_text(text)
    return str(d)


def test_gates_are_copied_only_when_every_hour_has_six_finalized_rows(tmp_path):
    src = gate_dir(tmp_path)
    run = tmp_path / "run"
    run.mkdir()
    pl.copy_gates(src, str(run), S, E)
    (g,) = [p for p in run.iterdir() if p.name.startswith("gates-")]
    m = json.load(open(g / "manifest.json"))
    assert m["hours"] == 4 and len(m["files"]) == 4                     # the hour before start is needed too
    assert pl.verified_gates(str(run), S, E) == str(g)


@pytest.mark.parametrize("kw", [{"hours": range(S, E, 3600)}, {"finalized": False}, {"drop_arm": True}])
def test_missing_unfinalized_or_incomplete_hours_publish_nothing(tmp_path, kw):
    src = gate_dir(tmp_path, **kw)
    run = tmp_path / "run"
    run.mkdir()
    with pytest.raises(SystemExit):
        pl.copy_gates(src, str(run), S, E)
    assert not [p for p in run.iterdir() if p.name.startswith("gates")]


def test_finalized_fail_rows_are_admissible(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    pl.copy_gates(gate_dir(tmp_path, status="FAIL"), str(run), S, E)
    assert pl.verified_gates(str(run), S, E)


def test_a_tampered_gates_directory_is_refused(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    pl.copy_gates(gate_dir(tmp_path), str(run), S, E)
    (g,) = [p for p in run.iterdir() if p.name.startswith("gates-")]
    f = next(p for p in g.iterdir() if p.name.startswith("load-"))
    f.write_text(f.read_text().replace("PASS", "FAIL", 1))
    with pytest.raises(SystemExit):
        pl.verified_gates(str(run), S, E)


@pytest.fixture
def env(tmp_path, monkeypatch):
    code = tmp_path / "code"
    code.mkdir()
    (code / "infra_events.py").write_text("import json, sys\nopen(sys.argv[sys.argv.index('--json') + 1], 'w').write(json.dumps(sys.argv))\n")
    monkeypatch.setattr(pl, "CODE", str(code))
    for k, v in (("P8_RUN", "dry"), ("P8_START", START), ("P8_STOP", STOP), ("P8_PROM", "http://vm"),
                 ("P8_EVIDENCE", str(tmp_path / "evidence"))):
        monkeypatch.setenv(k, v)
    return tmp_path / "evidence" / "dry"


def test_one_attempt_per_run_and_results_are_never_overwritten(env, monkeypatch):
    pl.main(["export"])
    (pre,) = [p for p in env.iterdir() if p.name.startswith("preliminary-")]
    assert "--archive-root" in json.loads(pre.read_text())
    (env / "attempt-1").mkdir()
    with pytest.raises(SystemExit):                                     # a second export of the same run
        pl.main(["export"])
    (env / "attempt-2").mkdir()
    with pytest.raises(SystemExit):                                     # resume/final need exactly one attempt
        pl.main(["resume"])


def test_final_needs_verified_gates_and_writes_a_new_revision(env, tmp_path, monkeypatch):
    env.mkdir(parents=True)
    (env / "attempt-1").mkdir()
    with pytest.raises(SystemExit):                                     # no gates directory yet
        pl.main(["final"])
    pl.copy_gates(gate_dir(tmp_path), str(env), S, E)
    pl.main(["final"])
    (fin,) = [p for p in env.iterdir() if p.name.startswith("final-")]
    args = json.loads(fin.read_text())
    assert args[args.index("--reanalyze") + 1].endswith("attempt-1") and "--load-gate-dir" in args


def published(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    pl.copy_gates(gate_dir(tmp_path), str(run), S, E)
    (g,) = [p for p in run.iterdir() if p.name.startswith("gates-")]
    return run, g


def test_r45_a_wrong_window_is_refused(tmp_path):
    run, g = published(tmp_path)
    with pytest.raises(SystemExit):
        pl.verified_gates(str(run), S, E + 3600)


@pytest.mark.parametrize("edit", ["empty", "missing", "duplicate"])
def test_r45_empty_missing_or_duplicate_manifests_are_refused(tmp_path, edit):
    run, g = published(tmp_path)
    m = json.load(open(g / "manifest.json"))
    if edit == "empty":
        m.update(files=[], hours=0)
    elif edit == "missing":
        m["files"] = m["files"][1:]
    else:
        m["files"] = m["files"][:-1] + [m["files"][0]]
    (g / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(SystemExit):
        pl.verified_gates(str(run), S, E)


def test_r45_the_copied_bytes_are_validated_not_an_earlier_read(tmp_path, monkeypatch):
    src = gate_dir(tmp_path)
    real = pl.shutil.copyfile

    def swap(a, b):                     # the source is replaced between any earlier check and the copy
        real(a, b)
        text = open(b).read()           # read first, then write (Codex r46: the earlier fixture truncated the file)
        assert '"finalized": true' in text
        open(b, "w").write(text.replace('"finalized": true', '"finalized": false'))
    monkeypatch.setattr(pl.shutil, "copyfile", swap)
    run = tmp_path / "run"
    run.mkdir()
    with pytest.raises(SystemExit):
        pl.copy_gates(src, str(run), S, E)
    assert not [p for p in run.iterdir() if p.name.startswith("gates")]


def test_r45_two_finals_are_two_revisions(env, tmp_path):
    env.mkdir(parents=True)
    (env / "attempt-1").mkdir()
    pl.copy_gates(gate_dir(tmp_path), str(env), S, E)
    pl.main(["final"])
    pl.main(["final"])
    assert len([p for p in env.iterdir() if p.name.startswith("final-")]) == 2


# Codex task-03 r89: the copied gate is the hour's one retained final attempt, never the mutable projection.

def published_manifest(run):
    (g,) = [p for p in run.iterdir() if p.name.startswith("gates-")]
    return json.load(open(g / "manifest.json")), g


def test_r89_the_final_attempt_is_copied_even_when_the_projection_was_rewritten(tmp_path):
    src = gate_dir(tmp_path)
    h = S
    later = rows_for(h, status="FAIL", finalized=False)          # a later, non-final evaluation rewrote the projection
    (tmp_path / "gates" / "attempts" / attempt_name(h, "20261007T231500Z")).write_text(json.dumps(later))
    (tmp_path / "gates" / pl.hour_name(h)).write_text(json.dumps(later))
    run = tmp_path / "run"
    run.mkdir()
    pl.copy_gates(src, str(run), S, E)
    m, g = published_manifest(run)
    entry = next(f for f in m["files"] if f["file"] == pl.hour_name(h))
    assert entry["source_attempt"] == attempt_name(h, "20261007T091500Z")
    assert entry["projection_matches"] is False
    assert [a["file"] for a in entry["later_attempts"]] == [attempt_name(h, "20261007T231500Z")]
    assert len(entry["later_attempts"][0]["sha256"]) == 64
    assert '"status": "PASS"' in (g / pl.hour_name(h)).read_text()
    assert all(f["projection_matches"] and f["later_attempts"] == [] for f in m["files"] if f is not entry)


@pytest.mark.parametrize("case", ["no_final", "two_finals", "unreadable"])
def test_r89_hours_without_exactly_one_readable_final_publish_nothing(tmp_path, case):
    src = gate_dir(tmp_path)
    att = tmp_path / "gates" / "attempts"
    if case == "no_final":
        (att / attempt_name(S, "20261007T091500Z")).write_text(json.dumps(rows_for(S, finalized=False)))
    elif case == "two_finals":                                    # a manual rerun inside the final window
        (att / attempt_name(S, "20261007T093000Z")).write_text(json.dumps(rows_for(S, status="FAIL")))
    else:
        (att / attempt_name(S, "20261007T101500Z")).write_text("{truncated")
    run = tmp_path / "run"
    run.mkdir()
    with pytest.raises(SystemExit) as e:
        pl.copy_gates(src, str(run), S, E)
    assert pl.hour_name(S) in str(e.value)
    assert not [p for p in run.iterdir() if p.name.startswith("gates")]


def _mixed(h):
    rows = rows_for(h)
    rows[0]["maturity"]["finalized"] = False
    return rows


def _no_maturity(h):
    rows = rows_for(h)
    del rows[2]["maturity"]
    return rows


def _string_flag(h):
    rows = rows_for(h)
    for r in rows:
        r["maturity"]["finalized"] = "true"
    return rows


def _other_hour(h):
    return rows_for(h + 3600)


def _duplicate_arm(h):
    rows = rows_for(h)
    rows[-1]["app"] = rows[0]["app"]
    return rows


def _bad_hour_string(h):
    rows = rows_for(h)
    rows[1]["hour_start"] = "yesterday"
    return rows


@pytest.mark.parametrize("make", [_mixed, _no_maturity, _string_flag, _other_hour, _duplicate_arm, _bad_hour_string,
                                  lambda h: {"rows": rows_for(h)}])
def test_r90_a_malformed_attempt_beside_a_valid_final_publishes_nothing(tmp_path, make):
    src = gate_dir(tmp_path)                                    # every hour has a valid final attempt
    (tmp_path / "gates" / "attempts" / attempt_name(S, "20261007T121500Z")).write_text(json.dumps(make(S)))
    run = tmp_path / "run"
    run.mkdir()
    with pytest.raises(SystemExit) as e:
        pl.copy_gates(src, str(run), S, E)
    assert pl.hour_name(S) in str(e.value)
    assert not [p for p in run.iterdir() if p.name.startswith("gates")]


def test_r90_only_the_listed_pre_maturity_collector_is_exempt(tmp_path):
    src = gate_dir(tmp_path)
    old = [{k: v for k, v in r.items() if k != "maturity"} for r in rows_for(S)]
    pre = [dict(r, collector_sha256=next(iter(pl.PRE_MATURITY_COLLECTORS))) for r in old]
    (tmp_path / "gates" / "attempts" / attempt_name(S, "20261007T011004Z")).write_text(json.dumps(pre))
    run = tmp_path / "run"
    run.mkdir()
    pl.copy_gates(src, str(run), S, E)                         # listed, not blocking
    m, _ = published_manifest(run)
    entry = next(f for f in m["files"] if f["file"] == pl.hour_name(S))
    assert [a["file"] for a in entry["pre_maturity_attempts"]] == [attempt_name(S, "20261007T011004Z")]
    other = [dict(r, collector_sha256="0" * 64) for r in old]   # the same shape from any other build blocks
    (tmp_path / "gates" / "attempts" / attempt_name(S, "20261007T011422Z")).write_text(json.dumps(other))
    run2 = tmp_path / "run2"
    run2.mkdir()
    with pytest.raises(SystemExit):
        pl.copy_gates(src, str(run2), S, E)
