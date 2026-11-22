"""The pre-flight view of the shared lock directory (common_records.lock_dir_state, lockdir.py): every entry is
classified, the recoverers' permanent flock file is expected, and nothing is ever changed."""

import argparse
import json
import os
import socket
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import common_records as C  # noqa: E402
import lockdir  # noqa: E402

UID = "0b0c0d0e-1111-2222-3333-444455556666"
OTHER = "aaaaaaaa-1111-2222-3333-444455556666"
LOCK = f"{UID}-evaluation.lock"


@pytest.fixture
def rec(tmp_path):
    (tmp_path / "locks").mkdir()
    return {"lock_dir": str(tmp_path / "locks"), "cluster_uid": UID}


def put(rec, name, body="{}"):
    path = os.path.join(rec["lock_dir"], name)
    with open(path, "w") as fh:
        fh.write(body if isinstance(body, str) else json.dumps(body))
    return path


def owner(pid=None, start=None, **more):
    pid = os.getpid() if pid is None else pid
    return {"scope": "evaluation", "holder": "release", "run_id": "c28-prod-1", "pid": pid,
            "pid_start": C.process_start(pid) if start is None else start, "host": socket.gethostname(),
            "boot_id": C._boot_id(), "at": "2026-11-22T20:00:00Z", **more}


def snapshot(rec):
    root = rec["lock_dir"]
    return sorted((n, os.lstat(os.path.join(root, n)).st_mtime_ns, os.lstat(os.path.join(root, n)).st_size,
                   os.lstat(os.path.join(root, n)).st_ino) for n in os.listdir(root))


def test_an_empty_or_missing_directory_is_clear(rec, tmp_path):
    state = C.lock_dir_state(rec)
    assert state["clear"] and state["exists"] and state["schema"] == "c28-lock-dir/v1"
    assert all(state[k] == [] for k in ("locks", "recovery_guards", "coordination", "other_clusters", "unexpected"))
    missing = C.lock_dir_state({"lock_dir": str(tmp_path / "never"), "cluster_uid": UID})
    assert missing["clear"] and not missing["exists"] and not (tmp_path / "never").exists()      # and not created


def test_the_recoverers_flock_file_is_expected_and_is_never_removed(rec):
    path = put(rec, LOCK + ".recovery.flock", "")
    before = snapshot(rec)
    state = C.lock_dir_state(rec)
    assert state["clear"] and state["coordination"] == [LOCK + ".recovery.flock"] and state["unexpected"] == []
    assert os.path.exists(path) and snapshot(rec) == before


def test_a_lock_and_a_guard_are_reported_with_what_their_owner_record_proves(rec):
    put(rec, LOCK, owner())
    put(rec, LOCK + ".recovery", owner(pid=2 ** 22 + 12345, start="1"))
    put(rec, f"{UID}-legacy.lock", "not json")
    put(rec, LOCK + ".recovery.flock", "")
    before = snapshot(rec)
    state = C.lock_dir_state(rec)
    assert not state["clear"] and snapshot(rec) == before
    by = {e["name"]: e for e in state["locks"]}
    assert by[LOCK]["owner"] == "alive" and by[LOCK]["pid"] == os.getpid() and by[LOCK]["holder"] == "release"
    assert by[LOCK]["run_id"] == "c28-prod-1" and "pid_start" not in by[LOCK] and "boot_id" not in by[LOCK]
    assert by[f"{UID}-legacy.lock"] == {"name": f"{UID}-legacy.lock", "owner": "unknown",
                                        "why": "its owner record could not be read as one JSON object"}
    [guard] = state["recovery_guards"]
    assert (guard["name"], guard["owner"]) == (LOCK + ".recovery", "terminated")
    assert state["coordination"] == [LOCK + ".recovery.flock"]


@pytest.mark.parametrize("kind", ["lock", "guard"])
def test_one_lock_or_one_guard_alone_prevents_clear(rec, kind):
    put(rec, LOCK if kind == "lock" else LOCK + ".recovery", owner(pid=2 ** 22 + 12345, start="1"))
    state = C.lock_dir_state(rec)
    assert not state["clear"] and len(state["locks"]) + len(state["recovery_guards"]) == 1


@pytest.mark.parametrize("name,why", [
    (LOCK + ".recovery.tmp-abcdef", "a recovery guard's publication was interrupted"),
    (LOCK + ".recovery.flock.bak", "its name is not of a known form"),
    (LOCK + ".old", "its name is not of a known form"),
    ("notes.txt", "its name is not of a known form"),
    (f"{UID}-.lock", "its name is not of a known form"),
    (f"{UID}-bad scope.lock", "its name is not of a known form"),
])
def test_anything_else_is_unexpected_and_prevents_clear(rec, name, why):
    put(rec, name)
    state = C.lock_dir_state(rec)
    assert not state["clear"] and state["unexpected"] == [{"name": name, "why": why}]
    assert state["locks"] == state["recovery_guards"] == state["coordination"] == []


def test_a_directory_or_a_symbolic_link_is_unexpected_whatever_its_name(rec, tmp_path):
    os.mkdir(os.path.join(rec["lock_dir"], LOCK + ".recovery.flock"))
    target = tmp_path / "elsewhere"
    target.write_text(json.dumps(owner()))
    os.symlink(target, os.path.join(rec["lock_dir"], LOCK))
    state = C.lock_dir_state(rec)
    assert not state["clear"] and state["locks"] == [] and state["coordination"] == []
    assert state["unexpected"] == [{"name": LOCK, "why": "it is not a regular file"},
                                   {"name": LOCK + ".recovery.flock", "why": "it is not a regular file"}]


def test_another_clusters_entries_are_named_and_not_judged(rec):
    put(rec, f"{OTHER}-evaluation.lock", owner())
    put(rec, f"{OTHER}-evaluation.lock.recovery.flock", "")
    state = C.lock_dir_state(rec)
    assert state["clear"] and state["other_clusters"] == [f"{OTHER}-evaluation.lock",
                                                          f"{OTHER}-evaluation.lock.recovery.flock"]
    put(rec, LOCK + ".recovery.flock", "")
    mine = C.lock_dir_state(rec)
    assert mine["coordination"] == [LOCK + ".recovery.flock"] and mine["other_clusters"] == state["other_clusters"]
    spelled = C.lock_dir_state({**rec, "cluster_uid": UID.upper()})      # a name is compared as lock_path() writes it
    assert spelled["coordination"] == [] and LOCK + ".recovery.flock" in spelled["other_clusters"]


def test_what_cannot_be_read_is_never_clear(rec, monkeypatch):
    put(rec, LOCK + ".recovery.flock", "")
    real = os.listdir
    monkeypatch.setattr(os, "listdir", lambda p: (_ for _ in ()).throw(PermissionError(13, "x")))
    state = C.lock_dir_state(rec)
    monkeypatch.setattr(os, "listdir", real)
    assert not state["clear"] and state["problems"] == ["the lock directory could not be listed (PermissionError)"]
    lstat = os.lstat
    monkeypatch.setattr(os, "lstat", lambda p: (_ for _ in ()).throw(PermissionError(13, "x")))
    state = C.lock_dir_state(rec)
    monkeypatch.setattr(os, "lstat", lstat)
    assert not state["clear"] and state["unexpected"] == [
        {"name": LOCK + ".recovery.flock", "why": "it could not be examined (PermissionError)"}]
    put(rec, LOCK, "x" * (C.LOCK_DIR_BODY_BYTES + 10))                   # a body beyond the bound is not parsed
    assert C.lock_dir_state(rec)["locks"][0]["owner"] == "unknown"
    put(rec, LOCK, json.dumps([1]))
    assert C.lock_dir_state(rec)["locks"][0]["owner"] == "unknown"
    put(rec, LOCK, owner(holder="h" * 500, at=True))
    said = C.lock_dir_state(rec)["locks"][0]
    assert len(said["holder"]) == 120 and "at" not in said


def test_a_record_without_the_directory_or_the_cluster_is_refused(rec):
    for bad in ({}, {"lock_dir": "relative", "cluster_uid": UID}, {"lock_dir": rec["lock_dir"]},
                {"lock_dir": rec["lock_dir"], "cluster_uid": ""}):
        with pytest.raises(C.StagingError, match="lock_dir"):
            C.lock_dir_state(bad)


def test_the_command_prints_the_state_and_its_exit_status_says_whether_it_is_clear(rec, monkeypatch, capsys):
    monkeypatch.setattr(lockdir.X, "load_record", lambda path, sections: argparse.Namespace(doc=rec))
    put(rec, LOCK + ".recovery.flock", "")
    assert lockdir.main(["--record", "r.json", "status"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["clear"] and out["coordination"] == [LOCK + ".recovery.flock"]
    put(rec, LOCK, owner())
    before = snapshot(rec)
    assert lockdir.main(["--record", "r.json", "status"]) == 3
    assert json.loads(capsys.readouterr().out)["locks"][0]["owner"] == "alive" and snapshot(rec) == before
    monkeypatch.setattr(lockdir.X, "load_record", lambda path, sections: argparse.Namespace(doc={}))
    assert lockdir.main(["--record", "r.json", "status"]) == 2 and "lock_dir" in capsys.readouterr().err


def test_the_helper_has_no_call_that_could_change_the_directory():
    src = open(os.path.join(HERE, "..", "lockdir.py")).read().split('"""')[2]
    fn = open(os.path.join(HERE, "..", "common_records.py")).read().split("def lock_dir_state")[1].split("\nclass ")[0]
    code = fn.split('"""')[2]                                            # without its docstring
    assert "os.listdir(root)" in code and "O_RDONLY | os.O_NOFOLLOW" in code
    for word in ("unlink", "os.remove", "rename", "os.replace", "fcntl", "flock(", "O_WRONLY", "O_RDWR", "O_CREAT",
                 "O_TRUNC", "O_APPEND", "makedirs", "mkdir", "rmdir", "truncate", "open(", "chmod", "utime"):
        assert word not in code.replace("os.open(path, os.O_RDONLY", "") and word not in src, word
