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


def gate_dir(tmp_path, hours=None, status="PASS", finalized=True, drop_arm=False):
    d = tmp_path / "gates"
    d.mkdir()
    for h in hours if hours is not None else range(S - 3600, E, 3600):
        arms = pl.ARMS[:-1] if drop_arm else pl.ARMS
        rows = [{"app": a, "hour_start": pl.time.strftime("%Y-%m-%dT%H:%M:%SZ", pl.time.gmtime(h)), "status": status,
                 "maturity": {"finalized": finalized}} for a in arms]
        (d / pl.hour_name(h)).write_text(json.dumps(rows))
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
