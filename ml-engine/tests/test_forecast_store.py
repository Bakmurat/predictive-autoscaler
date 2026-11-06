"""C2.2: the forecast journal (SQLite WAL): immutability, retries, crashes, disk full, corruption, recovery, retention."""

import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting.clock import ClockError, ManualClock, ReceiptClock  # noqa: E402
from forecasting.contracts import sha256_hex  # noqa: E402
from forecasting.maturation import window_outcome  # noqa: E402
from forecasting.store import (ForecastStore, IntegrityConflict, IssuanceKey, StoreCorrupt, StoreError,  # noqa: E402
                               StoreFull, StoreUnavailable)

ML_ENGINE = Path(__file__).resolve().parent.parent
EK = "e" * 64
SETTINGS = "5" * 64
SRC = "c" * 64
T0 = 1_793_966_400            # 2026-11-06T12:00:00Z
DT = 300


def key(origin=T0, lead=1200, ek=EK):
    return IssuanceKey(ek, origin, origin, origin + lead, SETTINGS)


def bundle(origin=T0, issued_at_ms=None, points=(1.0, 2.0, 1.5, 0.5)):
    return {"decision_origin": origin, "point_rps": list(points), "base_window_max_rps": max(points),
            "upper_window_max_rps": None, "issued_at_ms": issued_at_ms if issued_at_ms is not None else origin * 1000}


def open_store(path, clock=None, **kw):
    return ForecastStore(str(path), clock=clock or ManualClock(T0 * 1000), store_id=kw.pop("store_id", "test-store"),
                         **kw)


@pytest.fixture
def store(tmp_path):
    s = open_store(tmp_path / "j.sqlite")
    yield s
    s.close()


# --- clocks -----------------------------------------------------------------------------------------------------

def test_receipt_clock_never_goes_backwards():
    ticks = iter([1000, 1005, 990, 1010])
    c = ReceiptClock(source=lambda: next(ticks))
    assert [c.now_ms() for _ in range(4)] == [1000, 1005, 1005, 1010]
    with pytest.raises(ClockError):
        ReceiptClock(source=lambda: 1.5).now_ms()


def test_manual_clock_only_moves_forward():
    c = ManualClock(10)
    c.advance(5)
    c.set(20)
    assert c.now_ms() == 20
    for bad in (19, -1, 21.0, True):
        with pytest.raises(ClockError):
            c.set(bad)


# --- issuances: uniqueness, retries, immutability ---------------------------------------------------------------

def test_wal_mode_and_identity(store, tmp_path):
    assert store._db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert store._db.execute("PRAGMA synchronous").fetchone()[0] == 2         # FULL
    assert store.store_id == "test-store" and store.healthy
    store.close()
    with pytest.raises(StoreCorrupt):
        open_store(tmp_path / "j.sqlite", store_id="another-store")
    reopened = ForecastStore(str(tmp_path / "j.sqlite"), clock=ManualClock(0))   # identity read from the file
    assert reopened.store_id == "test-store"
    reopened.close()


def test_a_duplicate_retry_returns_the_committed_issuance(store):
    rec, created = store.commit_issuance(key(), bundle())
    assert created and rec.issuance_id == store.issuance_id_for(key()) and rec.payload() == bundle()
    again, created = store.commit_issuance(key(), bundle())
    assert not created and again == rec
    assert len(store.issuances()) == 1


def test_different_bytes_for_the_same_key_are_an_integrity_error(store):
    store.commit_issuance(key(), bundle())
    with pytest.raises(IntegrityConflict):
        store.commit_issuance(key(), bundle(points=(9.0, 9.0, 9.0, 9.0)))
    with pytest.raises(IntegrityConflict):
        store.commit_issuance(key(), bundle(issued_at_ms=T0 * 1000 + 1))
    assert store.issuances()[0].payload() == bundle() and store.healthy


def test_get_or_issue_never_recomputes_a_committed_issuance(store):
    calls = []

    def build(iid):
        calls.append(iid)
        return bundle(issued_at_ms=T0 * 1000 + len(calls))    # a recomputation would differ (later issued_at)

    first, created = store.get_or_issue(key(), build)
    second, created2 = store.get_or_issue(key(), build)
    assert created and not created2 and first == second and len(calls) == 1
    assert calls[0] == first.issuance_id


def test_issuance_ids_bind_the_store_and_every_key_field(tmp_path):
    a = open_store(tmp_path / "a.sqlite", store_id="store-a")
    b = open_store(tmp_path / "b.sqlite", store_id="store-b")
    assert a.issuance_id_for(key()) != b.issuance_id_for(key())
    ids = {a.issuance_id_for(k) for k in (key(), key(origin=T0 + DT), key(lead=900), key(ek="f" * 64),
                                         IssuanceKey(EK, T0, T0, T0 + 1200, "6" * 64))}
    assert len(ids) == 5
    for bad in ((EK, T0, T0 + 1, T0 + 1200, SETTINGS), (EK, T0, T0, T0, SETTINGS), ("x", T0, T0, T0 + 1, SETTINGS),
                (EK, -1, -1, 5, SETTINGS), (EK, True, True, 5, SETTINGS)):
        with pytest.raises(StoreError):
            IssuanceKey(*bad)


def test_journal_rows_cannot_be_edited_or_deleted(store, tmp_path):
    rec, _ = store.commit_issuance(key(), bundle())
    store.clock.set((T0 + 1200) * 1000)
    store.commit_outcome(rec.issuance_id, "seasonal_naive", "unscorable", {"reason": "missing_truth"})
    store.record_observations("s", [(T0, 1.0, T0 * 1000)], SRC)
    raw = sqlite3.connect(str(tmp_path / "j.sqlite"))
    for sql in ("UPDATE issuances SET bundle = x'00'", "DELETE FROM issuances", "UPDATE outcomes SET status = 'scored'",
                "DELETE FROM outcomes", "UPDATE observations SET value = 5", "DELETE FROM observations",
                "UPDATE meta SET value = 'x' WHERE key = 'store_id'", "DELETE FROM meta",
                "UPDATE meta SET value = '-1' WHERE key = 'retention_horizon'"):
        with pytest.raises(sqlite3.IntegrityError):
            raw.execute(sql)
    raw.close()


def test_bundles_are_canonical_and_capped(store):
    with pytest.raises(StoreError):
        store.commit_issuance(key(), {"point_rps": [float("nan")]})
    with pytest.raises(StoreError):
        store.commit_issuance(key(), {"blob": "x" * (8 << 10)})
    with pytest.raises(StoreError):
        store.commit_issuance(key(), ["not", "an", "object"])
    a, _ = store.commit_issuance(key(), {"b": 1, "a": [1, 2]})
    assert a.bundle == b'{"a":[1,2],"b":1}' and store.healthy


# --- outcomes ---------------------------------------------------------------------------------------------------

def test_one_terminal_outcome_per_issuance_and_model(store):
    rec, _ = store.commit_issuance(key(), bundle())
    with pytest.raises(StoreError):            # the window has not ended yet
        store.commit_outcome(rec.issuance_id, "seasonal_naive", "scored", {"y": 1})
    store.clock.set((T0 + 1200) * 1000)
    o, created = store.commit_outcome(rec.issuance_id, "seasonal_naive", "scored", {"y": 1})
    again, created2 = store.commit_outcome(rec.issuance_id, "seasonal_naive", "scored", {"y": 1})
    assert created and not created2 and o == again
    with pytest.raises(IntegrityConflict):
        store.commit_outcome(rec.issuance_id, "seasonal_naive", "unscorable", {"reason": "late"})
    with pytest.raises(IntegrityConflict):
        store.commit_outcome(rec.issuance_id, "seasonal_naive", "scored", {"y": 2})
    store.commit_outcome(rec.issuance_id, "statistical", "scored", {"y": 1})   # a shadow model's own outcome
    with pytest.raises(StoreError):
        store.commit_outcome("iss-unknown", "seasonal_naive", "scored", {})
    with pytest.raises(StoreError):
        store.commit_outcome(rec.issuance_id, "lstm", "covered", {})
    assert [x.model for x in store.outcomes_after(EK, 0)] == ["seasonal_naive", "statistical"]


def test_open_issuances_are_in_window_end_order(store):
    for origin in (T0 + 2 * DT, T0, T0 + DT):
        store.commit_issuance(key(origin), bundle(origin))
    store.clock.set((T0 + 3000) * 1000)
    assert [i.key.decision_origin for i in store.open_issuances(EK, "m", T0 + 1500)] == [T0, T0 + DT]
    store.commit_outcome(store.issuances()[1].issuance_id, "m", "scored", {})
    assert [i.key.decision_origin for i in store.open_issuances(EK, "m", T0 + 3000)] == [T0 + DT, T0 + 2 * DT]


# --- observations -----------------------------------------------------------------------------------------------

def test_the_first_receipt_is_kept_and_corrections_are_audit_only(store):
    r1 = store.record_observations("s", [(T0, 1.0, 100), (T0 + DT, 2.0, 100)], SRC)
    r2 = store.record_observations("s", [(T0, 1.0, 200), (T0 + DT, 7.0, 200), (T0 + 2 * DT, 3.0, 200)], SRC)
    assert (r1.new, r2.new, r2.duplicate, r2.corrected) == (2, 1, 1, 1)
    assert store.observations("s", T0, T0 + 2 * DT) == [(T0, 1.0, 100), (T0 + DT, 2.0, 100), (T0 + 2 * DT, 3.0, 200)]
    assert store.corrections("s") == [(T0 + DT, 7.0, 200)]
    for bad in ([(T0, -1.0, 1)], [(T0, float("nan"), 1)], [(T0, float("inf"), 1)], [(1.5, 1.0, 1)],
                [(T0, True, 1)], [(T0, 1.0, -1)], [(T0, 1.0, 1.5)]):
        with pytest.raises(StoreError):
            store.record_observations("s", bad, SRC)


# --- checkpoints and exactly-once recovery ----------------------------------------------------------------------

def count_fold(state, o):
    return {"n": state["n"] + 1, "ids": state["ids"] + [o.issuance_id]}


def initial():
    return {"n": 0, "ids": []}


def three_outcomes(store):
    ids = []
    for k in range(3):
        rec, _ = store.commit_issuance(key(T0 + k * DT), bundle(T0 + k * DT))
        ids.append(rec.issuance_id)
    store.clock.set((T0 + 3000) * 1000)
    for iid in ids:
        store.commit_outcome(iid, "m", "scored", {"iid": iid})
    return ids


def test_recovery_applies_each_outcome_once(store):
    ids = three_outcomes(store)
    outs = store.outcomes_after(EK, 0)
    store.write_checkpoint(EK, {"n": 2, "ids": ids[:2]}, outs[1].seq)
    state, last, applied = store.recover(EK, count_fold, initial)
    assert (state, last, applied) == ({"n": 3, "ids": ids}, outs[2].seq, 1)
    assert store.recover(EK, count_fold, initial) == (state, last, applied)      # recover writes nothing
    store.write_checkpoint(EK, state, last)
    assert store.recover(EK, count_fold, initial) == (state, last, 0)
    with pytest.raises(StoreError):
        store.write_checkpoint(EK, state, last - 1)                               # never backwards
    cp = store.latest_checkpoint(EK)
    assert store.write_checkpoint(EK, state, last) == cp                          # identical: nothing written


def test_only_the_latest_checkpoints_are_kept(store):
    for k in range(6):
        store.write_checkpoint(EK, {"k": k}, 0)
    rows = store._db.execute("SELECT count(*) FROM checkpoints").fetchone()[0]
    assert rows == 3 and store.latest_checkpoint(EK).state == {"k": 5}


def test_a_tampered_checkpoint_is_corrupt(store, tmp_path):
    store.write_checkpoint(EK, {"n": 1}, 0)
    raw = sqlite3.connect(str(tmp_path / "j.sqlite"))
    raw.execute("DROP TRIGGER checkpoints_immutable")
    raw.execute("UPDATE checkpoints SET payload = ?", (b'{"n":100}',))
    raw.commit()
    raw.close()
    with pytest.raises(StoreCorrupt):
        store.latest_checkpoint(EK)


# --- process crashes before and after commit --------------------------------------------------------------------

CRASH = textwrap.dedent("""
    import json, os, sys
    from forecasting.clock import ManualClock
    from forecasting.store import ForecastStore, IssuanceKey
    path, stage, op = sys.argv[1], sys.argv[2], sys.argv[3]
    T0 = 1_793_966_400
    s = ForecastStore(path, clock=ManualClock((T0 + 3000) * 1000), store_id="test-store")
    k = IssuanceKey("e" * 64, T0, T0, T0 + 1200, "5" * 64)
    if op == "outcome":
        rec = s.get_issuance(k)
    s._fault_hook = lambda st: os._exit(17) if st == stage else None
    if op == "issuance":
        s.commit_issuance(k, {"decision_origin": T0, "point_rps": [1.0], "issued_at_ms": 1})
    elif op == "outcome":
        s.commit_outcome(rec.issuance_id, "m", "scored", {"y": 3.0})
    os._exit(0)
""")


def crash(path, stage, op):
    env = {**{k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}, "PYTHONPATH": str(ML_ENGINE),
           "KUBECONFIG": "/dev/null"}
    p = subprocess.run([sys.executable, "-c", CRASH, str(path), stage, op], cwd=ML_ENGINE, env=env,
                       capture_output=True, text=True, timeout=60)
    assert p.returncode == 17, p.stderr[-2000:]


def test_a_crash_before_commit_leaves_nothing_and_the_retry_commits(tmp_path):
    path = tmp_path / "j.sqlite"
    open_store(path).close()
    crash(path, "before_commit", "issuance")
    s = open_store(path, clock=ManualClock((T0 + 3000) * 1000))
    assert s.issuances() == [] and s.healthy
    s.verify()
    rec, created = s.commit_issuance(key(), {"decision_origin": T0, "point_rps": [1.0], "issued_at_ms": 1})
    assert created and len(s.issuances()) == 1


def test_a_crash_after_commit_keeps_the_issuance_and_the_retry_returns_it(tmp_path):
    path = tmp_path / "j.sqlite"
    open_store(path).close()
    crash(path, "after_commit", "issuance")
    s = open_store(path, clock=ManualClock((T0 + 3000) * 1000))
    s.verify()
    [rec] = s.issuances()
    again, created = s.commit_issuance(key(), {"decision_origin": T0, "point_rps": [1.0], "issued_at_ms": 1})
    assert not created and again == rec                  # a retry after a lost response counts once
    with pytest.raises(IntegrityConflict):              # and a recomputation cannot replace it
        s.commit_issuance(key(), {"decision_origin": T0, "point_rps": [2.0], "issued_at_ms": 1})


@pytest.mark.parametrize("stage,durable", [("before_commit", False), ("after_commit", True)])
def test_outcome_crashes_recover_exactly_once(tmp_path, stage, durable):
    path = tmp_path / "j.sqlite"
    s = open_store(path)
    s.commit_issuance(key(), bundle())
    s.close()
    crash(path, stage, "outcome")
    s = open_store(path, clock=ManualClock((T0 + 3000) * 1000))
    s.verify()
    assert len(s.outcomes_after(EK, 0)) == int(durable)
    state, last, applied = s.recover(EK, count_fold, initial)
    assert (state["n"], applied) == (int(durable), int(durable))
    s.write_checkpoint(EK, state, last)
    s.close()
    s = open_store(path, clock=ManualClock((T0 + 3000) * 1000))      # a second restart applies nothing again
    assert s.recover(EK, count_fold, initial)[2] == 0 and s.recover(EK, count_fold, initial)[0]["n"] == int(durable)


# --- disk full and quota ----------------------------------------------------------------------------------------

def fill(store, n=10_000):
    for k in range(n):
        store.commit_issuance(key(T0 + k * DT), {"pad": "x" * 6000, "k": k})


def test_the_quota_check_refuses_before_the_disk_fills(tmp_path):
    s = open_store(tmp_path / "j.sqlite", quota_bytes=256 << 10)
    with pytest.raises(StoreFull):
        fill(s)
    assert not s.healthy and "quota" in s.problem
    with pytest.raises(StoreUnavailable):          # sticky: no further writes from this handle
        s.record_observations("s", [(T0, 1.0, 1)], SRC)
    s.close()


def test_a_real_sqlite_full_marks_the_store_unhealthy_without_partial_records(tmp_path, monkeypatch):
    path = tmp_path / "j.sqlite"
    s = open_store(path, quota_bytes=256 << 10)
    monkeypatch.setattr(s, "_check_quota", lambda incoming: None)     # only SQLite's max_page_count remains
    with pytest.raises(StoreFull) as e:
        fill(s)
    assert "full" in str(e.value) and not s.healthy
    committed = len(s.issuances())
    s.close()
    big = open_store(path, quota_bytes=64 << 20)
    big.verify()
    recs = big.issuances()
    assert len(recs) == committed > 0 and [r.payload()["k"] for r in recs] == list(range(committed))
    big.commit_issuance(key(T0 + committed * DT), {"k": committed})         # space again: writes resume
    big.close()


# --- corruption -------------------------------------------------------------------------------------------------

def test_a_file_that_is_not_a_journal_is_corrupt(tmp_path):
    p = tmp_path / "j.sqlite"
    p.write_bytes(b"this is not sqlite" * 400)
    with pytest.raises(StoreCorrupt):
        open_store(p)


def test_a_damaged_header_is_corrupt(tmp_path):
    p = tmp_path / "j.sqlite"
    s = open_store(p)
    s.commit_issuance(key(), bundle())
    s._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    s.close()
    data = bytearray(p.read_bytes())
    data[0:16] = b"\x00" * 16
    p.write_bytes(bytes(data))
    with pytest.raises(StoreCorrupt):
        open_store(p)


def test_damaged_pages_are_detected(tmp_path):
    p = tmp_path / "j.sqlite"
    s = open_store(p)
    for k in range(40):
        s.commit_issuance(key(T0 + k * DT), {"pad": "y" * 3000, "k": k})
    s._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    s.close()
    data = bytearray(p.read_bytes())
    page = 4096
    for off in range(2 * page, len(data), page):        # scribble over every page after the schema
        data[off + 8:off + 64] = b"\xff" * 56
    p.write_bytes(bytes(data))
    with pytest.raises(StoreUnavailable):
        s = open_store(p)
        s.verify()


def test_a_tampered_bundle_is_corrupt_and_disables_the_store(store, tmp_path):
    store.commit_issuance(key(), bundle())
    raw = sqlite3.connect(str(tmp_path / "j.sqlite"))
    raw.execute("DROP TRIGGER issuances_immutable")
    raw.execute("UPDATE issuances SET bundle = ?", (json.dumps(bundle(points=(9.0,))).encode(),))
    raw.commit()
    raw.close()
    with pytest.raises(StoreCorrupt):
        store.get_issuance(key())
    assert not store.healthy
    with pytest.raises(StoreUnavailable):
        store.commit_issuance(key(T0 + DT), bundle(T0 + DT))


def test_an_unknown_schema_version_is_refused(tmp_path):
    p = tmp_path / "j.sqlite"
    open_store(p).close()
    raw = sqlite3.connect(str(p))
    raw.execute("DROP TRIGGER meta_fixed")
    raw.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
    raw.commit()
    raw.close()
    with pytest.raises(StoreCorrupt):
        open_store(p)


# --- retention --------------------------------------------------------------------------------------------------

def test_retention_removes_only_old_windows_and_keeps_the_denominators(tmp_path):
    s = open_store(tmp_path / "j.sqlite", retention_seconds=86400, observation_retention_seconds=2 * 86400)
    ids = []
    for k in range(10):
        rec, _ = s.commit_issuance(key(T0 + k * 3600), bundle(T0 + k * 3600))
        ids.append(rec)
    s.record_observations("s", [(T0 - 2 * 86400 - DT, 1.0, 1), (T0, 2.0, 1)], SRC)
    s.clock.set((T0 + 12 * 3600) * 1000)
    for rec in ids[:6]:
        s.commit_outcome(rec.issuance_id, "m", "scored" if rec.key.decision_origin % 7200 else "unscorable", {})
    removed = s.compact(T0 + 86400 + 4 * 3600)          # horizon T0 + 4 h: windows k = 0..3 end before it
    assert removed == {"issuances": 4, "outcomes": 4, "observations": 1}
    assert [r.key.decision_origin for r in s.issuances()] == [T0 + k * 3600 for k in range(4, 10)]
    summary = s.retention_summary(EK)
    assert (summary["issued"], summary["scored"], summary["unscorable"]) == (4, 2, 2)
    assert summary["through_window_end"] == T0 + 4 * 3600 and len(summary["digest"]) == 64
    assert s.compact(T0 + 86400) == {"issuances": 0, "outcomes": 0, "observations": 0}     # never backwards
    raw = sqlite3.connect(str(tmp_path / "j.sqlite"))
    with pytest.raises(sqlite3.IntegrityError):                  # newer windows are still protected
        raw.execute("DELETE FROM issuances")
    raw.close()
    s.verify()
    assert s.export_manifest()["counts"]["issuances"] == 6


def test_the_journal_digest_ignores_row_numbers_but_not_content(tmp_path):
    a, b = open_store(tmp_path / "a.sqlite"), open_store(tmp_path / "b.sqlite")
    for s in (a, b):
        s.commit_issuance(key(), bundle())
    assert a.journal_digest() == b.journal_digest()
    b.record_observations("s", [(T0, 1.0, 1)], SRC)
    assert a.journal_digest() != b.journal_digest()


# --- maturation -------------------------------------------------------------------------------------------------

TARGETS = [T0 + DT, T0 + 2 * DT, T0 + 3 * DT]
END = TARGETS[-1]


def outcome(received, now_s, upper=None, settle=120, grace=600):
    return window_outcome(targets=TARGETS, base_rps=2.0, upper_rps=upper, received=received, now_ms=int(now_s * 1000),
                          settlement_s=settle, grace_s=grace)


def full(at_s=END + 60):
    return {t: (v, at_s * 1000) for t, v in zip(TARGETS, (1.0, 3.5, 2.0))}


def test_a_window_scores_only_when_every_point_has_arrived():
    assert outcome(full(), END + 119) is None                                  # not settled yet
    status, p = outcome(full(), END + 120, upper=3.0)
    assert status == "scored" and p["y_window_max_rps"] == 3.5 and p["covered"] is False
    assert p["residual_rps"] == 1.5 and p["abs_error_rps"] == 1.5 and p["completed_at_ms"] == (END + 60) * 1000
    assert outcome(full(), END + 120, upper=3.5)[1]["covered"] is True
    assert outcome(full(), END + 120)[1]["covered"] is None                    # no bound issued: no coverage claim
    assert outcome(full(), END + 120)[1]["values_sha256"] == sha256_hex(b"[1.0,3.5,2.0]")


def test_missing_and_late_truth():
    gap = {t: v for t, v in full().items() if t != TARGETS[1]}
    assert outcome(gap, END + 120) is None and outcome(gap, END + 719) is None    # waiting out the grace
    status, p = outcome(gap, END + 720)
    assert status == "unscorable" and p["missing_targets"] == 1 and p["first_missing"] == TARGETS[1]
    late = {**gap, TARGETS[1]: (3.5, (END + 700) * 1000)}                       # arrives within the grace
    assert outcome(late, END + 699) is None
    assert outcome(late, END + 700)[0] == "scored"
    future = {**gap, TARGETS[1]: (3.5, (END + 2000) * 1000)}                    # received after now: not there yet
    assert outcome(future, END + 720)[0] == "unscorable"
