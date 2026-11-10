"""The forecast journal recovers in the same process (review R3-07): a write that does not fit first lets retention
free what it may, and a failed journal is reopened (ForecastStore.reopen: the start-up checks, every record's checksum,
room for the write that did not fit, one durable commit) instead of staying failed until someone restarts the pod. A
full or corrupt file stays failed and says why; nothing in retention is ever deleted to make room."""

import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting.clock import ManualClock  # noqa: E402
from forecasting.store import ForecastStore, StoreCorrupt, StoreFull, StoreUnavailable  # noqa: E402
from tests.test_forecast_store import DT, EK, MODELS, SRC, T0, bundle, key  # noqa: E402

DAY = 86400
ROWS = 2000                                # one series of the review's scenario: 2000 samples, 128 kB by the estimate


def open_store(path, clock=None, **kw):
    kw.setdefault("retention_seconds", DAY)
    kw.setdefault("observation_retention_seconds", DAY)
    return ForecastStore(str(path), clock=clock or ManualClock(T0 * 1000), store_id="test-store", **kw)


def series(n, start=T0 - 20 * 3600):
    return f"signal:{n:064d}", [(start + i * 30, 1.0, T0 * 1000) for i in range(ROWS)], SRC


def fill(s):
    """Record whole series until one does not fit; the number recorded."""
    n = 0
    with pytest.raises(StoreFull):
        while True:
            s.record_observations(*series(n))
            n += 1
    return n


def rows(s):
    return s.export_manifest()["counts"]["observations"]


def raw(path):
    return sqlite3.connect(str(path))


# --- the quota: retention first, then the refusal -------------------------------------------------------------------

def test_a_full_journal_compacts_before_it_refuses_a_write(tmp_path):
    probe = open_store(tmp_path / "probe.sqlite", quota_bytes=2 << 20)
    n = fill(probe)                                                # how many series fit this quota
    probe.close()
    s = open_store(tmp_path / "j.sqlite", quota_bytes=2 << 20)
    for k in range(n):
        s.record_observations(*series(k))
    full = s.usage_bytes()
    assert n >= 2 and s.healthy and rows(s) == n * ROWS            # at its quota: one more series does not fit
    s.clock.advance(2 * DAY * 1000)                                # every recorded sample is now out of retention
    now = s.clock.now_ms() // 1000
    receipt = s.record_observations("signal:" + "f" * 64, [(now - i * 30, 2.0, now * 1000) for i in range(ROWS)], SRC)
    assert receipt.new == ROWS and s.healthy                       # it fitted after compaction: no failure at all
    assert rows(s) == ROWS and s.usage_bytes() < full              # the expired samples went, the new ones stayed
    s.verify()


def test_the_quota_check_refuses_before_sqlite_does(tmp_path):
    # The check counts what a write really takes (rows, indexes, page rounding), so it refuses first, with the size it
    # needs, whether the write-ahead log happens to be large or was just truncated.
    for name, write in (("observations", lambda s, k: s.record_observations(*series(k))),
                        ("issuances", lambda s, k: s.commit_issuance(key(T0 + k * DT), {"pad": "x" * 6000, "k": k},
                                                                     models=MODELS))):
        s = open_store(tmp_path / f"{name}.sqlite", quota_bytes=2 << 20)
        with pytest.raises(StoreFull) as e:
            for k in range(10_000):
                write(s, k)
                s._db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        assert "quota" in str(e.value) and "quota" in s.problem, name
        s.close()


def test_in_retention_evidence_is_never_deleted_to_make_room(tmp_path):
    path = tmp_path / "j.sqlite"
    s = open_store(path, quota_bytes=2 << 20)
    rec, _ = s.commit_issuance(key(), bundle(), models=MODELS)
    n = fill(s)
    assert not s.healthy and "quota" in s.problem
    assert rows(s) == n * ROWS and s.get_issuance(key()).bundle == rec.bundle      # all of it is still there
    with pytest.raises(StoreUnavailable):                          # failed: no further writes from this handle
        s.record_observations("s", [(T0, 1.0, 1)], SRC)
    db = raw(path)
    for table in ("observations", "issuances"):                    # the retention triggers still guard every row
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(f"DELETE FROM {table}")
    db.close()
    s.close()


# --- reopen: the review's wedge --------------------------------------------------------------------------------------

def test_a_journal_at_its_quota_stays_failed_until_space_is_freed_then_recovers_in_process(tmp_path):
    s = open_store(tmp_path / "j.sqlite", quota_bytes=2 << 20)
    n = fill(s)
    digest = s.journal_digest()
    for _ in range(3):                                             # nothing was freed: every attempt says so
        with pytest.raises(StoreFull):
            s.reopen()
        assert not s.healthy and "quota" in s.problem
    assert s.journal_digest() == digest and rows(s) == n * ROWS
    s.quota_bytes = 4 << 20                                        # the operator enlarges the budget ...
    s.reopen()
    assert s.healthy and s.problem is None and s.journal_digest() == digest      # ... and the journal is whole
    assert s.record_observations(*series(n)).new == ROWS           # the write that did not fit
    s.verify()
    s.close()


def test_a_journal_at_its_quota_recovers_when_retention_frees_space(tmp_path):
    s = open_store(tmp_path / "j.sqlite", quota_bytes=2 << 20)
    fill(s)
    with pytest.raises(StoreFull):
        s.reopen()
    s.clock.advance(2 * DAY * 1000)                                # the samples leave retention
    s.reopen()
    assert s.healthy and rows(s) == 0
    assert s.record_observations("s", [(s.clock.now_ms() // 1000, 1.0, 1)], SRC).new == 1
    s.close()


# --- reopen: an operational error ------------------------------------------------------------------------------------

def io_error(stage):
    if stage == "before_commit":
        raise sqlite3.OperationalError("disk I/O error")


def test_an_io_error_recovers_on_a_later_attempt_with_nothing_partial(tmp_path):
    s = open_store(tmp_path / "j.sqlite")
    rec, _ = s.commit_issuance(key(), bundle(), models=MODELS)
    digest = s.journal_digest()
    s._fault_hook = io_error                                       # an fsync that fails at commit
    with pytest.raises(StoreUnavailable):
        s.commit_issuance(key(T0 + DT), bundle(T0 + DT), models=MODELS)
    assert not s.healthy and "disk I/O error" in s.problem
    with pytest.raises(StoreUnavailable):                          # the disk still fails: the durable-commit check
        s.reopen()                                                 # refuses to call the journal healthy
    assert not s.healthy and "disk I/O error" in s.problem
    s._fault_hook = None                                           # the disk is back
    s.reopen()
    assert s.healthy and s.journal_digest() == digest              # the failed commit left nothing behind
    assert [i.issuance_id for i in s.issuances()] == [rec.issuance_id]
    assert s.commit_issuance(key(T0 + DT), bundle(T0 + DT), models=MODELS)[1] is True
    s.close()


# --- reopen: corruption stays corruption -----------------------------------------------------------------------------

def test_a_tampered_record_keeps_the_journal_failed_across_reopen(tmp_path):
    path = tmp_path / "j.sqlite"
    s = open_store(path)
    s.commit_issuance(key(), bundle(), models=MODELS)
    s.commit_issuance(key(T0 + DT), bundle(T0 + DT), models=MODELS)
    db = raw(path)
    db.execute("DROP TRIGGER issuances_immutable")
    db.execute("UPDATE issuances SET bundle = ? WHERE decision_origin = ?",
               (json.dumps(bundle(points=(9.0,))).encode(), T0))
    db.commit()
    db.close()
    with pytest.raises(StoreCorrupt):
        s.get_issuance(key())
    for _ in range(3):
        with pytest.raises(StoreCorrupt):                          # quick_check passes; the record checksums do not
            s.reopen()
        assert not s.healthy and "checksum" in s.problem
    with pytest.raises(StoreUnavailable):
        s.commit_issuance(key(T0 + 2 * DT), bundle(T0 + 2 * DT), models=MODELS)
    s.close()


def test_a_file_that_stops_being_a_journal_keeps_the_store_failed(tmp_path):
    path = tmp_path / "j.sqlite"
    s = open_store(path)
    s.commit_issuance(key(), bundle(), models=MODELS)
    s._fault_hook = io_error
    with pytest.raises(StoreUnavailable):
        s.commit_issuance(key(T0 + DT), bundle(T0 + DT), models=MODELS)
    s._fault_hook = None
    s.close()
    data = bytearray(path.read_bytes())
    data[0:16] = b"\0" * 16                                        # the header is gone
    path.write_bytes(bytes(data))
    for _ in range(2):
        with pytest.raises(StoreUnavailable):
            s.reopen()
        assert not s.healthy
    with pytest.raises(StoreUnavailable):
        s.issuances()


def test_another_stores_journal_is_refused_on_reopen(tmp_path):
    path = tmp_path / "j.sqlite"
    s = open_store(path)
    s._fault_hook = io_error
    with pytest.raises(StoreUnavailable):
        s.commit_issuance(key(), bundle(), models=MODELS)
    s._fault_hook = None
    s.close()
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(str(path) + suffix):
            os.unlink(str(path) + suffix)
    ForecastStore(str(path), clock=ManualClock(T0 * 1000), store_id="another-store").close()
    with pytest.raises(StoreCorrupt):
        s.reopen()
    assert not s.healthy and "another store identity" in s.problem


# --- what was sound stays sound after a reopen -----------------------------------------------------------------------

def test_a_reopened_journal_keeps_its_durability_properties(tmp_path):
    path = tmp_path / "j.sqlite"
    s = open_store(path)
    rec, _ = s.commit_issuance(key(), bundle(), models=MODELS)
    s._fault_hook = io_error
    with pytest.raises(StoreUnavailable):
        s.commit_issuance(key(T0 + DT), bundle(T0 + DT), models=MODELS)
    s._fault_hook = None
    horizon = s._horizon()
    digest = s.journal_digest()
    s.reopen()
    assert s.journal_digest() == digest and s._horizon() == horizon         # the commit check changed nothing
    assert s.store_id == "test-store" and s.get_issuance(key()) == rec
    pragma = lambda name: s._db.execute(f"PRAGMA {name}").fetchone()[0]     # noqa: E731
    assert pragma("journal_mode") == "wal" and pragma("synchronous") == 2 and pragma("foreign_keys") == 1
    seen = []
    s._db.set_trace_callback(seen.append)
    s.commit_issuance(key(T0 + DT), bundle(T0 + DT), models=MODELS)
    s._db.set_trace_callback(None)
    begins = [q for q in seen if q.upper().startswith("BEGIN")]
    assert begins == ["BEGIN IMMEDIATE"]                           # every write still in an IMMEDIATE transaction
    again, created = s.commit_issuance(key(), bundle(), models=MODELS)       # a retry still returns the record
    assert created is False and again == rec
    db = raw(path)
    for stmt in ("UPDATE issuances SET bundle = x'00'", "DELETE FROM issuances",
                 "UPDATE meta SET value = 'x' WHERE key = 'store_id'"):
        with pytest.raises(sqlite3.IntegrityError):                # the triggers are the file's own
            db.execute(stmt)
    db.execute("DROP TRIGGER issuances_immutable")
    db.execute("UPDATE issuances SET bundle = x'7b7d' WHERE decision_origin = ?", (T0,))
    db.commit()
    db.close()
    with pytest.raises(StoreCorrupt):                              # ... and every read still checks the checksum
        s.get_issuance(key())
    s.close()


def test_verify_reads_every_record_without_loading_them_at_once(tmp_path):
    path = tmp_path / "j.sqlite"
    s = open_store(path)
    for k in range(5):
        rec, _ = s.commit_issuance(key(T0 + k * DT), bundle(T0 + k * DT), models=MODELS)
    s.clock.set((T0 + 3 * 3600) * 1000)
    s.commit_outcome(rec.issuance_id, "m", "scored", {"y": 1.0})
    s.write_checkpoint(EK, {"n": 1}, 1)
    s.verify()
    db = raw(path)
    db.execute("DROP TRIGGER outcomes_immutable")
    db.execute("UPDATE outcomes SET payload = x'7b7d'")
    db.commit()
    db.close()
    with pytest.raises(StoreCorrupt):
        s.verify()
    assert not s.healthy


# --- the service: recovery on a bounded schedule, visible meanwhile --------------------------------------------------

from forecasting import contracts as c  # noqa: E402
from forecasting import service as service_module  # noqa: E402
from forecasting.metrics import SERVICE_PREFIX  # noqa: E402
from forecasting.service import ServiceError  # noqa: E402
from tests import test_statistical_service as S  # noqa: E402

BACKOFF, SLOW = service_module.RECOVERY_BACKOFF_SECONDS, service_module.RECOVERY_SLOW_SECONDS
STABLE = service_module.RECOVERY_STABLE_SECONDS


def broken(svc):
    """Fail every journal commit from now on (an fsync error); the returned function repairs the disk."""
    svc.store._fault_hook = io_error
    return lambda: setattr(svc.store, "_fault_hook", None)


def unavailable(svc, clock, origin):
    with pytest.raises(ServiceError) as e:
        S.ask(svc, clock, origin)
    assert (e.value.status, e.value.reason) == (503, "StateUnavailable")
    return e.value


def tick(svc, clock, seconds, step=1):
    """Let `seconds` pass with a maintenance pass every `step` s; the reopen attempts made meanwhile."""
    before = svc.journal_status()["attempts"]
    for _ in range(0, seconds, step):
        clock.advance(step * 1000)
        svc.maintain()
    return svc.journal_status()["attempts"] - before


def service_series(svc):
    from prometheus_client import CollectorRegistry
    reg = CollectorRegistry()
    reg.register(svc.service_metrics)
    return {s.name[len(SERVICE_PREFIX):]: s.value for fam in reg.collect() for s in fam.samples if not s.labels}


def test_the_schedule_is_bounded_then_slow():
    assert BACKOFF == (5, 15, 60, 300, 900) and SLOW == 1800 and STABLE == 3600
    assert STABLE > max(BACKOFF)                                   # a recovery must outlast every step to reset them


def test_the_service_recovers_in_process_after_an_io_error(tmp_path):
    svc, clock = S.make(tmp_path, store_id="journal-1")
    S.run(svc, clock, 12)
    assert svc.ready() and service_series(svc)["journal_permanent_failure"] == 0.0
    assert "journal_unhealthy_since_seconds" not in service_series(svc)        # absent while healthy
    repair = broken(svc)
    origin = S.T0 + 12 * S.DT
    e = unavailable(svc, clock, origin)                            # at origin + 10 s
    assert "disk I/O error" in e.message
    failed_at = clock.now_ms()
    h = svc.health()
    assert not svc.ready() and h["status"] == "healthy" and h["state"] == "unavailable"     # alive, not ready
    assert "disk I/O error" in h["state_problem"]
    assert h["journal"] == {"unhealthy_since": c.format_ms(failed_at), "recovery_attempts": 0,
                            "next_attempt": c.format_ms(failed_at + 5000), "permanent_failure": False}
    assert service_series(svc)["journal_unhealthy_since_seconds"] == failed_at / 1000
    assert tick(svc, clock, 4) == 0                                # nothing before the first step
    unavailable(svc, clock, origin)                                # ... and requests are refused without a retry
    assert tick(svc, clock, 1) == 1 and not svc.ready()            # 5 s: one attempt; the disk still fails
    assert "disk I/O error" in svc.health()["state_problem"]
    repair()
    assert tick(svc, clock, 14) == 0 and not svc.ready()           # the next one is 15 s after the failed one
    assert tick(svc, clock, 1) == 1 and svc.ready()                # recovered, in the same process
    assert svc.health()["state"] == "healthy" and svc.health()["state_problem"] is None
    assert svc.health()["journal"]["unhealthy_since"] is None
    series = service_series(svc)
    assert "journal_unhealthy_since_seconds" not in series
    assert (series["journal_failures_total"], series["journal_recovery_attempts_total"],
            series["journal_recoveries_total"]) == (1.0, 2.0, 1.0)
    # The failed request left nothing partial: the same origin is issued now, and the journal verifies.
    r = c.response_from_wire(S.ask(svc, clock, origin))
    assert r.decision_origin == origin and len(svc.store.issuances()) == 13
    svc.store.verify()


def test_a_recovered_service_continues_exactly_like_a_restarted_one(tmp_path):
    import shutil
    svc, clock = S.make(tmp_path / "a", store_id="journal-1")
    S.run(svc, clock, 40)                                          # calibrated: there is state worth losing
    real = svc.store.commit_issuance

    def after_commit(stage):
        if stage == "after_commit":
            raise sqlite3.OperationalError("disk I/O error")

    def committing(*a, **k):                                       # the issuance is committed (durably), then the
        svc.store._fault_hook = after_commit                       # journal fails: nothing was answered or folded in
        return real(*a, **k)
    svc.store.commit_issuance = committing
    unavailable(svc, clock, S.T0 + 40 * S.DT)
    del svc.store.commit_issuance
    svc.store._fault_hook = None
    assert not svc.ready() and len(svc.store.issuances()) == 41
    assert tick(svc, clock, 5) == 1 and svc.ready()
    svc.store._db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
    shutil.copytree(tmp_path / "a", tmp_path / "b")                # the same volume, read by a new process
    from forecasting.clock import ManualClock as Clock
    restarted, rclock = S.make(tmp_path / "b", clock=Clock(clock.now_ms()), store_id="journal-1")
    start = S.T0 + 40 * S.DT                                       # the failed origin is retried first
    got = S.run(svc, clock, 20, start=start)
    assert got == S.run(restarted, rclock, 20, start=start)
    ek = c.response_from_wire(got[-1]).calibration_epoch
    assert svc.store.latest_checkpoint(ek).state == restarted.store.latest_checkpoint(ek).state
    assert c.response_from_wire(got[-1]).calibration_windows >= 12         # 60 origins of evidence: kept, not rebuilt


def test_a_volume_that_stays_broken_is_a_visible_permanent_failure_without_a_hot_loop(tmp_path):
    svc, clock = S.make(tmp_path)
    S.run(svc, clock, 3)
    broken(svc)
    unavailable(svc, clock, S.T0 + 3 * S.DT)
    t0 = clock.now_ms()
    due = [sum(BACKOFF[:k + 1]) for k in range(len(BACKOFF))]      # 5, 20, 80, 380 and 1280 s after the failure
    seen = []
    for second in range(1, due[-1] + 1):                           # a maintenance pass every second
        if tick(svc, clock, 1):
            seen.append(second)
        assert svc.health()["journal"]["permanent_failure"] is (second >= due[-1])
    assert seen == due
    h = svc.health()
    assert not svc.ready() and h["status"] == "healthy"            # still alive: a restart would repair nothing
    assert h["journal"]["recovery_attempts"] == 5 and h["journal"]["unhealthy_since"] == c.format_ms(t0)
    assert h["journal"]["next_attempt"] == c.format_ms(clock.now_ms() + SLOW * 1000)
    series = service_series(svc)
    assert series["journal_permanent_failure"] == 1.0 and series["journal_unhealthy_since_seconds"] == t0 / 1000
    assert tick(svc, clock, 4 * 3600, step=5) == 4 * 3600 // SLOW  # then one attempt per half hour, never more
    assert not svc.ready() and service_series(svc)["journal_recoveries_total"] == 0.0
    unavailable(svc, clock, S.T0 + 3 * S.DT + 5 * 3600 - 5 * 3600 % S.DT)       # and every request still gets its 503


def test_a_permanent_failure_still_recovers_once_the_volume_is_repaired(tmp_path):
    svc, clock = S.make(tmp_path)
    S.run(svc, clock, 3)
    repair = broken(svc)
    unavailable(svc, clock, S.T0 + 3 * S.DT)
    tick(svc, clock, sum(BACKOFF), step=5)
    assert svc.health()["journal"]["permanent_failure"] is True
    repair()                                                       # the operator's action
    assert tick(svc, clock, SLOW, step=5) == 1 and svc.ready()
    assert svc.health()["journal"]["permanent_failure"] is False
    origin = clock.now_ms() // 1000 // S.DT * S.DT
    assert c.response_from_wire(S.ask(svc, clock, origin)).decision_origin == origin


def test_a_recovery_that_does_not_last_keeps_its_place_in_the_schedule(tmp_path):
    svc, clock = S.make(tmp_path)
    S.run(svc, clock, 3)

    def fail_now():
        broken(svc)()                                              # one failed commit, then the disk works again
        svc.store._fail("commit: disk I/O error")
        svc.maintain()                                             # noted at once (as a refused request would)

    fail_now()
    assert tick(svc, clock, BACKOFF[0]) == 1 and svc.ready()       # recovered on the first attempt
    fail_now()                                                     # ... but it fails again a moment later
    assert tick(svc, clock, BACKOFF[1] - 1) == 0 and tick(svc, clock, 1) == 1 and svc.ready()   # 15 s, not 5 s
    fail_now()
    assert tick(svc, clock, BACKOFF[2] - 1) == 0 and tick(svc, clock, 1) == 1 and svc.ready()   # 1 min
    assert svc.journal_status()["recovery_attempts"] == 3
    tick(svc, clock, STABLE, step=60)                              # healthy for an hour: the schedule starts over
    assert svc.journal_status()["recovery_attempts"] == 0
    fail_now()
    assert tick(svc, clock, BACKOFF[0]) == 1 and svc.ready()


def test_a_full_journal_recovers_in_the_service_when_the_quota_allows_it(tmp_path):
    svc, clock = S.make(tmp_path, quota_bytes=400 << 10)           # room for a few requests only
    with pytest.raises(ServiceError) as e:
        S.run(svc, clock, 200)
    assert (e.value.status, e.value.reason) == (503, "StateUnavailable") and "quota" in e.value.message
    issued = len(svc.store.issuances())
    assert issued > 0 and not svc.ready() and "quota" in svc.health()["state_problem"]
    assert tick(svc, clock, BACKOFF[0]) == 1 and not svc.ready()   # still full: the attempt fails and says so
    assert "still at its" in svc.health()["state_problem"]
    svc.store.quota_bytes = 64 << 20                               # (a larger stateQuota; in a pod: with its restart)
    assert tick(svc, clock, BACKOFF[1]) == 1 and svc.ready()
    origin = clock.now_ms() // 1000 // S.DT * S.DT
    S.ask(svc, clock, origin)
    assert len(svc.store.issuances()) == issued + 1


def test_a_journal_that_is_corrupt_at_start_stays_failed_until_it_is_replaced(tmp_path):
    svc, clock = S.make(tmp_path)
    S.run(svc, clock, 5)
    svc.close()
    path = tmp_path / "journal.sqlite"
    data = bytearray(path.read_bytes())
    data[0:16] = b"\0" * 16
    path.write_bytes(bytes(data))
    svc, _ = S.make(tmp_path, clock=clock)
    assert svc.store is None and not svc.ready() and svc.health()["journal"]["unhealthy_since"] is not None
    attempts = tick(svc, clock, sum(BACKOFF) + 2 * SLOW, step=5)
    assert attempts == len(BACKOFF) + 2 and svc.store is None and not svc.ready()
    assert svc.health()["journal"]["permanent_failure"] is True and "not a database" in svc.health()["state_problem"]
    unavailable(svc, clock, clock.now_ms() // 1000 // S.DT * S.DT)
    path.rename(tmp_path / "journal.sqlite.corrupt")               # the operator moves the corrupt file away
    assert tick(svc, clock, SLOW, step=5) == 1 and svc.ready() and svc.store is not None
    origin = clock.now_ms() // 1000 // S.DT * S.DT
    r = c.response_from_wire(S.ask(svc, clock, origin))            # an empty journal: no evidence carried over
    assert (r.calibration_windows, r.upper_window_max_rps) == (0, None) and len(svc.store.issuances()) == 1


# --- R3-10 b: an issuance that no longer fits its request is refused, never a 500 ------------------------------------

def test_a_retry_after_the_published_lead_changed_is_refused_with_422(tmp_path):
    reader = S.Reader()
    svc, clock = S.make(tmp_path, reader=reader)
    S.ask(svc, clock, S.T0)
    lead = reader.pa["status"]["prediction"]["lead"]
    lead["startupDelaySeconds"] = 480                              # the same lead (1200 s), another startup delay
    clock.advance(20_000)
    with pytest.raises(ServiceError) as e:                         # the same origin again: its committed issuance
        svc.predict(S.body(S.T0))                                  # carries the old delay
    assert (e.value.status, e.value.reason) == (422, "IdentityMismatch")
    assert "does not fit the request" in e.value.message and len(svc.store.issuances()) == 1
    assert svc.ready()                                             # not a journal problem
    r = c.response_from_wire(S.ask(svc, clock, S.T0 + S.DT))       # the next origin is issued for the new lead
    assert r.startup_delay_seconds == 480


# --- the app: liveness stays green, readiness says why, maintenance runs ---------------------------------------------

def test_the_app_keeps_liveness_green_and_readiness_names_the_reason(tmp_path):
    import time
    from fastapi.testclient import TestClient
    from api import statistical
    svc, clock = S.make(tmp_path)
    S.run(svc, clock, 2)
    repair = broken(svc)
    unavailable(svc, clock, S.T0 + 2 * S.DT)
    with TestClient(statistical.create_app({}, service_factory=lambda: svc, maintenance_seconds=0.02)) as client:
        ready = client.get("/ready")
        assert ready.status_code == 503 and ready.json()["detail"]["error"] == "StateUnavailable"
        assert "disk I/O error" in ready.json()["detail"]["message"]
        health = client.get("/health")
        assert health.status_code == 200 and health.json()["status"] == "healthy"       # liveness never fails for it
        assert health.json()["state"] == "unavailable" and health.json()["journal"]["unhealthy_since"]
        text = client.get("/metrics").text
        assert "\npredictive_autoscaler_forecaster_journal_unhealthy_since_seconds 1." in text   # a sample: unix time
        assert "\npredictive_autoscaler_forecaster_journal_failures_total 1.0" in text
        repair()
        clock.advance(BACKOFF[0] * 1000)                           # the first attempt is due: the app's own task
        deadline = time.monotonic() + 10                           # makes it, nobody calls maintain() here
        while client.get("/ready").status_code != 200 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert client.get("/ready").status_code == 200 and client.get("/health").json()["state"] == "healthy"
        text = client.get("/metrics").text
        assert "\npredictive_autoscaler_forecaster_journal_recoveries_total 1.0" in text
        assert "\npredictive_autoscaler_forecaster_journal_unhealthy_since_seconds " not in text   # absent when healthy
        clock.advance(sum(BACKOFF) * 1000)
        svc.store._fault_hook = io_error                           # permanent this time
        svc.store._fail("commit: disk I/O error")
        deadline = time.monotonic() + 10
        while not svc.health()["journal"]["permanent_failure"] and time.monotonic() < deadline:
            clock.advance(60_000)
            time.sleep(0.03)
        message = client.get("/ready").json()["detail"]["message"]
        assert "permanent failure" in message and "operator action required" in message
        assert client.get("/health").status_code == 200


def test_the_apps_maintenance_survives_a_failing_pass(tmp_path):
    import time
    from fastapi.testclient import TestClient
    from api import statistical
    from tests.test_statistical_app import FakeService

    class Flaky(FakeService):
        passes = 0

        def maintain(self):
            self.passes += 1
            if self.passes == 1:
                raise RuntimeError("boom")

    svc = Flaky()
    with TestClient(statistical.create_app({}, service_factory=lambda: svc, maintenance_seconds=0.01)) as client:
        deadline = time.monotonic() + 10
        while svc.passes < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert svc.passes >= 3 and client.get("/health").status_code == 200
    assert svc.closed
