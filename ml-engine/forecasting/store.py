"""The forecast journal: immutable issuances, outcomes and observations, plus replaceable checkpoints, in SQLite (WAL).

What it guarantees (design C2, section 6):
  - an issuance is committed before any answer is built from it, and is never changed afterwards. Its uniqueness key
    is (evidence key, decision origin, window, settings hash); its ID is derived from that key and the store identity.
    A retry with the same bytes returns the committed record (created=False, so it is never counted twice); different
    bytes for the same key are an IntegrityConflict, never an overwrite. get_or_issue() returns a committed issuance
    before building a new one, so a recomputation (a later issued_at, a refit) cannot replace what was answered;
  - every issuance records the set of models it expects outcomes for (the selected and shadow candidates issued at
    that origin); an outcome is accepted only for one of them, exactly one terminal outcome per issuance and model,
    under the same rules;
  - observations keep their first receipt; a different value for the same event time is kept in a separate
    corrections table (audit only) and never changes what was scored online;
  - UPDATE is refused by triggers on every journal table, and DELETE is refused except below the retention horizon,
    which only moves forward. Bundles carry their sha256 and every read checks it;
  - checkpoints are derived state (counters, levels, last processed outcome) with a checksum. recover() replays the
    outcomes after the checkpoint's last sequence number, so each outcome is folded in exactly once after a restart;
  - a full disk, the quota, an I/O error or corruption raises StoreUnavailable/StoreCorrupt and marks the store
    unhealthy until reopen() has proven it again: predictive eligibility needs a healthy store, reactive scaling does
    not. reopen() is a restart of the store inside the process: a new connection, the same start-up checks, every
    record read against its checksum, room for the write the quota refused, and one durable commit. A journal that
    is still full or corrupt stays failed with the new reason; the caller spaces the attempts (forecasting.service).
    A fresh or lost store starts a new epoch; it never presents itself as evidence it does not hold. Recovery
    accepts only the journal this process opened: the identity is remembered from the first successful open, also
    when none was configured, and a journal with another identity is refused. reopen() never CREATES a journal
    either: a file that is gone or empty keeps the store failed until the service is restarted, which is the one
    place where a new journal (a new identity, evidence from nothing) begins. (A service whose journal could not be
    opened at all at start has no store yet; its retries are first opens, which may initialize an empty file: that
    is the start-up path repeated, not a recovery. See ForecastService._reopen_store.) A recovery that ends in any error,
    expected or not, leaves the store failed.
The quota is enforced twice: SQLite's max_page_count (a hard SQLITE_FULL) and a check of database + WAL size plus
the write's own size (its payload and an allowance for rows, indexes and page rounding: conservative accounting
from measurements, not an exact guarantee of the physical size of a write) before each write, so
the check refuses first and knows how much room the refused write needs. A write that does not fit is refused only
after retention has freed what it may (compaction up to now, the freed pages and the WAL given back); nothing still in
retention is ever removed to make room. Retention removes an issuance whose window ended more than `retention_seconds`
ago only together with its
outcomes, and only when EVERY expected model has a terminal outcome covered by the epoch's latest verified checkpoint, so recover()
gives the same state before and after compaction; per-epoch aggregate counts and a digest chain over what was removed
are kept. Uncovered or still-open old issuances stay. Keys below the retention horizon are retired: committing one is
refused (RetiredKey, and a trigger), so a removed issuance ID can never be reissued with other bytes.

One writer process per file (Recreate, one replica): two independent writers sharing a volume are not supported.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

from forecasting.contracts import canonical_json, sha256_hex

SCHEMA_VERSION = 1
DEFAULT_QUOTA_BYTES = 1536 << 20          # 1.5 GiB of a 2 GiB volume
MAX_BUNDLE_BYTES = 8 << 10
MAX_OUTCOME_BYTES = 4 << 10
MAX_CHECKPOINT_BYTES = 1 << 20            # calibration buffers and 7-day trust ledgers of up to three models
RETENTION_SECONDS = 7 * 86400
OBSERVATION_RETENTION_SECONDS = 28 * 86400
WAL_LIMIT_BYTES = 64 << 20
OBSERVATION_ROW_BYTES = 128               # the quota check's size of a stored sample besides its series name
RECORD_SLACK_BYTES = 8 << 10              # ... and of any write besides its payload: columns, indexes, page rounding
CHECKPOINTS_KEPT = 3
OUTCOME_STATUSES = ("scored", "unscorable")
MAX_MODELS = 3                            # naive, statistical, optional LSTM: one chosen variant per family
_MODEL = re.compile(r"^[a-z0-9_]{1,32}$")

_SHA256 = re.compile(r"^[0-9a-f]{64}$")

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE issuances (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  issuance_id TEXT NOT NULL UNIQUE,
  key_sha256 TEXT NOT NULL UNIQUE,
  evidence_key TEXT NOT NULL,
  decision_origin INTEGER NOT NULL,
  window_start INTEGER NOT NULL,
  window_end INTEGER NOT NULL,
  settings_sha256 TEXT NOT NULL,
  bundle BLOB NOT NULL,
  bundle_sha256 TEXT NOT NULL,
  models TEXT NOT NULL,
  committed_at_ms INTEGER NOT NULL);
CREATE INDEX issuances_by_window ON issuances (evidence_key, window_end, seq);
CREATE TABLE outcomes (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  issuance_id TEXT NOT NULL REFERENCES issuances (issuance_id),
  evidence_key TEXT NOT NULL,
  window_end INTEGER NOT NULL,
  model TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('scored', 'unscorable')),
  payload BLOB NOT NULL,
  payload_sha256 TEXT NOT NULL,
  committed_at_ms INTEGER NOT NULL,
  UNIQUE (issuance_id, model));
CREATE INDEX outcomes_by_epoch ON outcomes (evidence_key, seq);
CREATE TABLE observations (
  series TEXT NOT NULL,
  event_time INTEGER NOT NULL,
  value REAL NOT NULL CHECK (value >= 0),
  first_received_ms INTEGER NOT NULL,
  source_sha256 TEXT NOT NULL,
  PRIMARY KEY (series, event_time)) WITHOUT ROWID;
CREATE TABLE observation_corrections (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  series TEXT NOT NULL,
  event_time INTEGER NOT NULL,
  value REAL NOT NULL,
  received_ms INTEGER NOT NULL,
  source_sha256 TEXT NOT NULL);
CREATE TABLE checkpoints (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  evidence_key TEXT NOT NULL,
  last_outcome_seq INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  payload BLOB NOT NULL,
  payload_sha256 TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL);
CREATE INDEX checkpoints_by_epoch ON checkpoints (evidence_key, seq);
CREATE TABLE retention (
  evidence_key TEXT PRIMARY KEY,
  through_window_end INTEGER NOT NULL,
  issued INTEGER NOT NULL,
  scored INTEGER NOT NULL,
  unscorable INTEGER NOT NULL,
  digest TEXT NOT NULL) WITHOUT ROWID;
CREATE TRIGGER meta_fixed BEFORE UPDATE ON meta WHEN OLD.key <> 'retention_horizon'
  BEGIN SELECT RAISE(ABORT, 'meta is immutable'); END;
CREATE TRIGGER meta_horizon_forward BEFORE UPDATE ON meta
  WHEN OLD.key = 'retention_horizon' AND CAST(NEW.value AS INTEGER) < CAST(OLD.value AS INTEGER)
  BEGIN SELECT RAISE(ABORT, 'the retention horizon only moves forward'); END;
CREATE TRIGGER meta_kept BEFORE DELETE ON meta BEGIN SELECT RAISE(ABORT, 'meta rows are permanent'); END;
CREATE TRIGGER issuances_not_retired BEFORE INSERT ON issuances
  WHEN NEW.window_end < (SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'retention_horizon')
  BEGIN SELECT RAISE(ABORT, 'the issuance key is below the retention horizon (retired)'); END;
CREATE TRIGGER issuances_immutable BEFORE UPDATE ON issuances
  BEGIN SELECT RAISE(ABORT, 'issuances are immutable'); END;
CREATE TRIGGER issuances_retention_only BEFORE DELETE ON issuances
  WHEN OLD.window_end >= (SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'retention_horizon')
  BEGIN SELECT RAISE(ABORT, 'issuances are removed by retention only'); END;
CREATE TRIGGER outcomes_immutable BEFORE UPDATE ON outcomes
  BEGIN SELECT RAISE(ABORT, 'outcomes are immutable'); END;
CREATE TRIGGER outcomes_retention_only BEFORE DELETE ON outcomes
  WHEN OLD.window_end >= (SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'retention_horizon')
  BEGIN SELECT RAISE(ABORT, 'outcomes are removed by retention only'); END;
CREATE TRIGGER observations_immutable BEFORE UPDATE ON observations
  BEGIN SELECT RAISE(ABORT, 'observations are immutable'); END;
CREATE TRIGGER observations_retention_only BEFORE DELETE ON observations
  WHEN OLD.event_time >= (SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'retention_horizon')
     - (SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'observation_extra_seconds')
  BEGIN SELECT RAISE(ABORT, 'observations are removed by retention only'); END;
CREATE TRIGGER corrections_immutable BEFORE UPDATE ON observation_corrections
  BEGIN SELECT RAISE(ABORT, 'corrections are immutable'); END;
CREATE TRIGGER checkpoints_immutable BEFORE UPDATE ON checkpoints
  BEGIN SELECT RAISE(ABORT, 'checkpoints are replaced, never edited'); END;
"""


class StoreError(Exception):
    """A journal operation was refused (programming or integrity error); the store stays usable."""


class IntegrityConflict(StoreError):
    """Different bytes for an existing immutable record (same issuance key, or same issuance and model)."""


class RetiredKey(StoreError):
    """An issuance key whose window ended below the retention horizon: it can never be (re)issued, so a removed ID
    can never come back with different bytes."""


class StoreUnavailable(StoreError):
    """The journal cannot be written or trusted (disk full, quota, I/O, lock). Predictive eligibility is off."""


class StoreFull(StoreUnavailable):
    pass


class StoreCorrupt(StoreUnavailable):
    """The file is not a valid journal, or a record does not match its checksum."""


@dataclass(frozen=True)
class IssuanceKey:
    evidence_key: str
    decision_origin: int
    window_start: int
    window_end: int
    settings_sha256: str

    def __post_init__(self):
        for name in ("evidence_key", "settings_sha256"):
            if not (isinstance(getattr(self, name), str) and _SHA256.match(getattr(self, name))):
                raise StoreError(f"{name} must be a sha256")
        for name in ("decision_origin", "window_start", "window_end"):
            v = getattr(self, name)
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise StoreError(f"{name} must be a non-negative integer")
        if self.window_start != self.decision_origin or self.window_end <= self.window_start:
            raise StoreError("a window starts at its decision origin and ends after it")

    @property
    def sha256(self) -> str:
        return sha256_hex(canonical_json({"schema": "issuance-key/v1", "evidence_key": self.evidence_key,
                                          "decision_origin": self.decision_origin, "window_start": self.window_start,
                                          "window_end": self.window_end, "settings_sha256": self.settings_sha256}))


@dataclass(frozen=True)
class Issuance:
    seq: int
    issuance_id: str
    key: IssuanceKey
    bundle: bytes
    bundle_sha256: str
    models: Tuple[str, ...]
    committed_at_ms: int

    def payload(self) -> dict:
        return json.loads(self.bundle)


@dataclass(frozen=True)
class Outcome:
    seq: int
    issuance_id: str
    evidence_key: str
    window_end: int
    model: str
    status: str
    payload_bytes: bytes
    payload_sha256: str
    committed_at_ms: int

    def payload(self) -> dict:
        return json.loads(self.payload_bytes)


@dataclass(frozen=True)
class Checkpoint:
    seq: int
    evidence_key: str
    last_outcome_seq: int
    state: dict
    payload_sha256: str
    created_at_ms: int


@dataclass(frozen=True)
class ObservationReceipt:
    new: int
    duplicate: int
    corrected: int


class ForecastStore:
    """One SQLite journal file. Thread-safe within the process; one writer process per file."""

    def __init__(self, path: str, *, clock, quota_bytes: int = DEFAULT_QUOTA_BYTES,
                 retention_seconds: int = RETENTION_SECONDS,
                 observation_retention_seconds: int = OBSERVATION_RETENTION_SECONDS,
                 store_id: Optional[str] = None, max_bundle_bytes: int = MAX_BUNDLE_BYTES):
        self.path, self.clock, self.quota_bytes = path, clock, quota_bytes
        self.retention_seconds, self.max_bundle_bytes = retention_seconds, max_bundle_bytes
        self._obs_extra = observation_retention_seconds - retention_seconds
        if self._obs_extra < 0:
            raise StoreError("observations must be kept at least as long as issuances")
        self._lock = threading.RLock()
        self._problem: Optional[str] = None
        self._recovering = False                                    # inside reopen(): not healthy until it has passed
        self._shortfall = 0                                         # bytes of the write the quota last refused
        self._requested_id = store_id                               # then: the identity the first open found
        self._fault_hook: Optional[Callable[[str], None]] = None    # tests: simulate a crash at a named point
        self._connect()
        try:
            self._open(store_id)
        except StoreError:
            self._db.close()
            raise
        self._requested_id = self.store_id      # recovery must find THIS journal, whether an identity was configured

    # --- lifecycle --------------------------------------------------------------------------------------------------

    def _connect(self) -> None:
        try:
            self._db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False, timeout=5.0)
        except sqlite3.Error as e:
            raise StoreUnavailable(f"cannot open the journal: {e}") from e

    def _open(self, store_id: Optional[str], create: bool = True) -> None:
        with self._guard("open"):
            db = self._db
            fresh = db.execute("SELECT count(*) FROM sqlite_master").fetchone()[0] == 0
            if fresh and not create:
                raise StoreCorrupt("the journal file is gone or empty; recovery never starts a new journal (restart "
                                   "the service to begin one: its evidence starts from nothing)")
            if fresh:
                db.execute("PRAGMA auto_vacuum = INCREMENTAL")      # only effective before the first table
            if db.execute("PRAGMA journal_mode = WAL").fetchone()[0].lower() != "wal":
                raise StoreUnavailable("the journal cannot use WAL mode")
            db.execute("PRAGMA synchronous = FULL")
            db.execute("PRAGMA foreign_keys = ON")
            db.execute(f"PRAGMA journal_size_limit = {WAL_LIMIT_BYTES}")
            page = db.execute("PRAGMA page_size").fetchone()[0]
            db.execute(f"PRAGMA max_page_count = {max(1, self.quota_bytes // page)}")
            check = db.execute("PRAGMA quick_check").fetchall()
            if check != [("ok",)]:
                raise StoreCorrupt(f"quick_check: {check[:3]}")
            if fresh:
                now = self.clock.now_ms()
                sid = store_id or os.urandom(16).hex()
                with self._tx():
                    for stmt in _SCHEMA.split(";\n"):
                        if stmt.strip():
                            db.execute(stmt)
                    db.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", [
                        ("schema_version", str(SCHEMA_VERSION)), ("store_id", sid), ("created_at_ms", str(now)),
                        ("retention_horizon", "0"), ("observation_extra_seconds", str(self._obs_extra))])
            meta = dict(db.execute("SELECT key, value FROM meta").fetchall())
            if meta.get("schema_version") != str(SCHEMA_VERSION):
                raise StoreCorrupt(f"unsupported journal schema {meta.get('schema_version')!r}")
            sid = meta.get("store_id")
            if not isinstance(sid, str) or not sid:
                raise StoreCorrupt("the journal has no store identity")
            if store_id is not None and sid != store_id:
                raise StoreCorrupt("the journal belongs to another store identity")
            # Every number the journal's retention is computed from must be what this code writes: a plain
            # non-negative decimal integer. A negative observation retention would make compaction delete
            # observations still inside their retention; a horizon that is not a number disables the triggers.
            numbers = {}
            for name in ("observation_extra_seconds", "created_at_ms", "retention_horizon"):
                v = meta.get(name)
                if not isinstance(v, str) or not v.isascii() or not v.isdigit():
                    raise StoreCorrupt(f"the journal's metadata is malformed: {name} is {v!r}, not a non-negative "
                                       "integer")
                numbers[name] = int(v)
            obs_extra, created = numbers["observation_extra_seconds"], numbers["created_at_ms"]
            self.store_id = sid
            self._obs_extra = obs_extra                                  # the file's own, fixed at creation
            self.created_at_ms = created

    def close(self) -> None:
        with self._lock:
            try:
                self._db.close()
            except sqlite3.Error:
                pass

    def reopen(self) -> None:
        """Recover in place after a failure, as a restart would, and prove more than a restart does. A new connection
        passes the start-up checks (WAL mode, quick_check, schema, identity); every record is read against its checksum
        (verify); the write the quota refused must fit now, after retention has freed what it may; and one commit must
        be durable (the retention horizon rewritten with its own value: a WAL append and an fsync, no logical change).
        Only then is the store healthy again. Otherwise it stays failed with the new reason and the error is raised:
        a StoreError, also for an error nothing here expected (never healthy by omission). The journal must be the
        one this process opened (its identity), and it must exist: recovery creates nothing."""
        with self._lock:
            self._recovering = True
            try:
                try:
                    self._db.close()
                except sqlite3.Error:
                    pass
                self._problem = None
                try:
                    with self._guard("reopen"):
                        self._connect()
                        self._open(self._requested_id, create=False)
                        self.verify()
                        if self._shortfall and self.usage_bytes() + self._shortfall > self.quota_bytes:
                            self._reclaim()
                            if self.usage_bytes() + self._shortfall > self.quota_bytes:
                                raise StoreFull(f"the journal is still at its {self.quota_bytes} byte quota (a "
                                                f"{self._shortfall} byte write does not fit)")
                        with self._tx():
                            self._db.execute("UPDATE meta SET value = value WHERE key = 'retention_horizon'")
                except StoreError as e:
                    if self._problem is None:
                        self._problem = f"reopen: {e}"
                    raise
                except Exception as e:                  # not foreseen: still a failed recovery, with a StoreError
                    if self._problem is None:
                        self._problem = f"reopen: unexpected {type(e).__name__}: {e}"
                    raise StoreUnavailable(self._problem) from e
                self._shortfall = 0
            finally:
                self._recovering = False

    @property
    def healthy(self) -> bool:
        return self._problem is None and not self._recovering

    @property
    def problem(self) -> Optional[str]:
        return self._problem

    def _fail(self, why: str) -> None:
        if self._problem is None:
            self._problem = why

    def _require_healthy(self) -> None:
        if self._problem is not None:
            raise StoreUnavailable(f"the journal is unavailable since an earlier failure: {self._problem}")

    def _fault(self, stage: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage)

    @contextmanager
    def _guard(self, what: str) -> Iterator[None]:
        try:
            yield
        except StoreUnavailable as e:
            self._fail(f"{what}: {e}")
            raise
        except StoreError:
            raise
        except sqlite3.IntegrityError as e:
            raise StoreError(f"{what}: refused by the journal: {e}") from e
        except sqlite3.OperationalError as e:
            msg = str(e)
            self._fail(f"{what}: {msg}")
            if "full" in msg.lower():
                raise StoreFull(f"{what}: {msg}") from e
            if "malformed" in msg.lower():
                raise StoreCorrupt(f"{what}: {msg}") from e
            raise StoreUnavailable(f"{what}: {msg}") from e
        except sqlite3.DatabaseError as e:      # not a database, malformed image
            self._fail(f"{what}: {e}")
            raise StoreCorrupt(f"{what}: {e}") from e

    @contextmanager
    def _tx(self) -> Iterator[None]:
        db = self._db
        db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._fault("before_commit")
            db.execute("COMMIT")
        except BaseException:
            try:
                db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        self._fault("after_commit")

    # --- quota ------------------------------------------------------------------------------------------------------

    def usage_bytes(self) -> int:
        with self._lock, self._guard("usage"):
            pages = self._db.execute("PRAGMA page_count").fetchone()[0]
            size = self._db.execute("PRAGMA page_size").fetchone()[0]
        try:
            wal = os.path.getsize(self.path + "-wal")
        except OSError:
            wal = 0
        return pages * size + wal

    def _check_quota(self, incoming: int) -> None:
        if self.usage_bytes() + incoming <= self.quota_bytes:
            return
        self._reclaim()                       # before refusing: retention may free the room
        if self.usage_bytes() + incoming > self.quota_bytes:
            self._shortfall = incoming
            raise StoreFull(f"the journal would exceed its {self.quota_bytes} byte quota")

    def _reclaim(self) -> None:
        """Give back what the retention rules allow: compaction up to now (expired, checkpoint-covered windows and
        expired observations; never a row still in retention, which the triggers refuse anyway), then the freed pages
        and the WAL."""
        self.compact(self.clock.now_ms() // 1000)
        with self._guard("reclaim"):
            self._db.execute("PRAGMA incremental_vacuum").fetchall()
            self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()

    # --- issuances --------------------------------------------------------------------------------------------------

    def issuance_id_for(self, key: IssuanceKey) -> str:
        return "iss-" + sha256_hex(f"{self.store_id}\0{key.sha256}".encode("ascii"))[:32]

    def _row_to_issuance(self, row) -> Issuance:
        seq, iid, ek, origin, ws, we, ss, bundle, bsha, models, at = row
        bundle = bytes(bundle)
        if sha256_hex(bundle) != bsha:
            self._fail(f"issuance {iid} does not match its checksum")
            raise StoreCorrupt(f"issuance {iid} does not match its checksum")
        try:
            expected = self._models(json.loads(models))
        except (ValueError, StoreError) as e:
            self._fail(f"issuance {iid} has a malformed model set")
            raise StoreCorrupt(f"issuance {iid} has a malformed model set") from e
        return Issuance(seq=seq, issuance_id=iid, key=IssuanceKey(ek, origin, ws, we, ss), bundle=bundle,
                        bundle_sha256=bsha, models=expected, committed_at_ms=at)

    @staticmethod
    def _models(models) -> Tuple[str, ...]:
        """The expected model set: 1..MAX_MODELS distinct names, kept sorted."""
        if isinstance(models, (str, bytes)) or not isinstance(models, (list, tuple)):
            raise StoreError("models must be a list of model names")
        out = tuple(sorted(set(models)))
        if len(out) != len(models) or not 1 <= len(out) <= MAX_MODELS or \
                not all(isinstance(m, str) and _MODEL.match(m) for m in out):
            raise StoreError(f"models must be 1..{MAX_MODELS} distinct names, got {models!r}")
        return out

    _ISSUANCE_COLS = ("seq, issuance_id, evidence_key, decision_origin, window_start, window_end, settings_sha256, "
                      "bundle, bundle_sha256, models, committed_at_ms")

    def get_issuance(self, key: IssuanceKey) -> Optional[Issuance]:
        with self._lock, self._guard("read issuance"):
            row = self._db.execute(f"SELECT {self._ISSUANCE_COLS} FROM issuances WHERE key_sha256 = ?",
                                   (key.sha256,)).fetchone()
            return self._row_to_issuance(row) if row else None

    def get_issuance_by_id(self, issuance_id: str) -> Optional[Issuance]:
        with self._lock, self._guard("read issuance"):
            row = self._db.execute(f"SELECT {self._ISSUANCE_COLS} FROM issuances WHERE issuance_id = ?",
                                   (issuance_id,)).fetchone()
            return self._row_to_issuance(row) if row else None

    def commit_issuance(self, key: IssuanceKey, payload: Mapping, *, models) -> Tuple[Issuance, bool]:
        """Commit an immutable issuance bundle expecting outcomes for `models`; (record, created). The same bytes and
        model set again: (committed record, False); anything else for the same key is an IntegrityConflict."""
        bundle = self._encode(payload, self.max_bundle_bytes, "issuance bundle")
        expected = self._models(models)
        iid = self.issuance_id_for(key)
        with self._lock, self._guard("commit issuance"):
            self._require_healthy()
            existing = self.get_issuance(key)
            if existing is not None:
                return self._same(existing, bundle, expected), False
            self._refuse_retired(key)
            self._check_quota(len(bundle) + RECORD_SLACK_BYTES)
            now = self.clock.now_ms()
            with self._tx():
                self._db.execute(
                    "INSERT INTO issuances (issuance_id, key_sha256, evidence_key, decision_origin, window_start, "
                    "window_end, settings_sha256, bundle, bundle_sha256, models, committed_at_ms) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (iid, key.sha256, key.evidence_key, key.decision_origin, key.window_start, key.window_end,
                     key.settings_sha256, bundle, sha256_hex(bundle), canonical_json(list(expected)).decode(), now))
            return self.get_issuance(key), True

    def get_or_issue(self, key: IssuanceKey, build: Callable[[str], Mapping], *, models) -> Tuple[Issuance, bool]:
        """The committed issuance for `key`, or build(issuance_id) committed now. A retry never recomputes.
        An unhealthy store answers neither path: an issuance it returns may be acted on. (Audit reads stay.)"""
        with self._lock, self._guard("issue"):
            self._require_healthy()
            existing = self.get_issuance(key)
            if existing is not None:
                if existing.models != self._models(models):
                    raise IntegrityConflict(f"issuance {existing.issuance_id} expects models {existing.models}")
                return existing, False
            self._refuse_retired(key)
            return self.commit_issuance(key, build(self.issuance_id_for(key)), models=models)

    def _horizon(self) -> int:
        return int(self._db.execute("SELECT value FROM meta WHERE key = 'retention_horizon'").fetchone()[0])

    def _refuse_retired(self, key: IssuanceKey) -> None:
        if key.window_end < self._horizon():
            raise RetiredKey(f"issuance key with window end {key.window_end} is below the retention horizon")

    @staticmethod
    def _same(existing: Issuance, bundle: bytes, models: Tuple[str, ...]) -> Issuance:
        if existing.bundle != bundle or existing.models != models:
            raise IntegrityConflict(f"issuance {existing.issuance_id} is already committed with different bytes "
                                    "or models")
        return existing

    @staticmethod
    def _encode(payload: Mapping, cap: int, what: str) -> bytes:
        if not isinstance(payload, Mapping):
            raise StoreError(f"{what} must be an object")
        try:
            data = canonical_json(dict(payload))
        except (TypeError, ValueError) as e:
            raise StoreError(f"{what} is not canonical JSON: {e}") from e
        if len(data) > cap:
            raise StoreError(f"{what} is {len(data)} bytes, above the {cap} byte cap")
        return data

    def open_issuances(self, evidence_key: str, model: str, window_end_at_most: int) -> List[Issuance]:
        """Issuances of an epoch with no terminal outcome for `model` whose window ended by the given time,
        in window-end order (the order outcomes are processed in)."""
        with self._lock, self._guard("read open issuances"):
            rows = self._db.execute(
                f"SELECT {', '.join('i.' + c.strip() for c in self._ISSUANCE_COLS.split(','))} FROM issuances i "
                "WHERE i.evidence_key = ? AND i.window_end <= ? AND NOT EXISTS "
                "(SELECT 1 FROM outcomes o WHERE o.issuance_id = i.issuance_id AND o.model = ?) "
                "ORDER BY i.window_end, i.seq", (evidence_key, window_end_at_most, model)).fetchall()
            return [i for i in map(self._row_to_issuance, rows) if model in i.models]

    def issuances_after(self, evidence_key: str, after_seq: int) -> List[Issuance]:
        """An epoch's issuances committed after `after_seq`, in commit order (for exactly-once folding)."""
        with self._lock, self._guard("read issuances"):
            rows = self._db.execute(f"SELECT {self._ISSUANCE_COLS} FROM issuances WHERE evidence_key = ? AND seq > ? "
                                    "ORDER BY seq", (evidence_key, after_seq)).fetchall()
            return [self._row_to_issuance(r) for r in rows]

    def issuance_seqs(self, evidence_key: str, origin_lo: int, origin_hi: int) -> Dict[int, int]:
        """{decision origin: seq} of an epoch's retained issuances with origins in [origin_lo, origin_hi] (no payloads:
        a cheap check of what the journal still holds)."""
        with self._lock, self._guard("read issuance seqs"):
            rows = self._db.execute("SELECT decision_origin, seq FROM issuances WHERE evidence_key = ? AND "
                                    "decision_origin BETWEEN ? AND ?", (evidence_key, origin_lo, origin_hi)).fetchall()
            return {int(o): int(q) for o, q in rows}

    def issuances(self, evidence_key: Optional[str] = None) -> List[Issuance]:
        with self._lock, self._guard("read issuances"):
            q = f"SELECT {self._ISSUANCE_COLS} FROM issuances"
            rows = (self._db.execute(q + " WHERE evidence_key = ? ORDER BY seq", (evidence_key,)) if evidence_key
                    else self._db.execute(q + " ORDER BY seq")).fetchall()
            return [self._row_to_issuance(r) for r in rows]

    # --- outcomes ---------------------------------------------------------------------------------------------------

    _OUTCOME_COLS = "seq, issuance_id, evidence_key, window_end, model, status, payload, payload_sha256, committed_at_ms"

    def _row_to_outcome(self, row) -> Outcome:
        seq, iid, ek, we, model, status, payload, psha, at = row
        payload = bytes(payload)
        if sha256_hex(payload) != psha:
            self._fail(f"outcome of {iid} does not match its checksum")
            raise StoreCorrupt(f"outcome of {iid} does not match its checksum")
        return Outcome(seq, iid, ek, we, model, status, payload, psha, at)

    def commit_outcome(self, issuance_id: str, model: str, status: str, payload: Mapping) -> Tuple[Outcome, bool]:
        """The one terminal outcome of (issuance, model); same bytes again returns it, different bytes conflict."""
        if status not in OUTCOME_STATUSES:
            raise StoreError(f"outcome status {status!r} is not one of {OUTCOME_STATUSES}")
        if not isinstance(model, str) or not model:
            raise StoreError("an outcome names its model")
        data = self._encode(payload, MAX_OUTCOME_BYTES, "outcome")
        with self._lock, self._guard("commit outcome"):
            self._require_healthy()
            row = self._db.execute(f"SELECT {self._OUTCOME_COLS} FROM outcomes WHERE issuance_id = ? AND model = ?",
                                   (issuance_id, model)).fetchone()
            if row is not None:
                o = self._row_to_outcome(row)
                if (o.status, o.payload_bytes) != (status, data):
                    raise IntegrityConflict(f"the outcome of {issuance_id}/{model} is already committed")
                return o, False
            iss = self.get_issuance_by_id(issuance_id)
            if iss is None:
                raise StoreError(f"no issuance {issuance_id}")
            if model not in iss.models:
                raise StoreError(f"issuance {issuance_id} expects outcomes for {iss.models}, not {model!r}")
            self._check_quota(len(data) + RECORD_SLACK_BYTES)
            now = self.clock.now_ms()
            if now < (iss.key.window_end) * 1000:
                raise StoreError("an outcome cannot be committed before its window has ended")
            with self._tx():
                self._db.execute(
                    "INSERT INTO outcomes (issuance_id, evidence_key, window_end, model, status, payload, "
                    "payload_sha256, committed_at_ms) VALUES (?,?,?,?,?,?,?,?)",
                    (issuance_id, iss.key.evidence_key, iss.key.window_end, model, status, data, sha256_hex(data),
                     now))
            row = self._db.execute(f"SELECT {self._OUTCOME_COLS} FROM outcomes WHERE issuance_id = ? AND model = ?",
                                   (issuance_id, model)).fetchone()
            return self._row_to_outcome(row), True

    def outcomes_after(self, evidence_key: str, after_seq: int) -> List[Outcome]:
        with self._lock, self._guard("read outcomes"):
            rows = self._db.execute(f"SELECT {self._OUTCOME_COLS} FROM outcomes WHERE evidence_key = ? AND seq > ? "
                                    "ORDER BY seq", (evidence_key, after_seq)).fetchall()
            return [self._row_to_outcome(r) for r in rows]

    # --- observations -----------------------------------------------------------------------------------------------

    def record_observations(self, series: str, samples: Iterable[Tuple[int, float, int]],
                            source_sha256: str) -> ObservationReceipt:
        """Record received samples (event time, RPS, receipt ms) in one transaction. The first receipt of an event
        time is kept; a different later value goes to the corrections table and never replaces it."""
        if not (isinstance(source_sha256, str) and _SHA256.match(source_sha256)):
            raise StoreError("observations name the sha256 of their source")
        rows = []
        for ts, v, at in samples:
            if not isinstance(ts, int) or isinstance(ts, bool) or not isinstance(v, (int, float)) or \
                    isinstance(v, bool) or not v >= 0 or v == float("inf") or not isinstance(at, int) or \
                    isinstance(at, bool) or at < 0:
                raise StoreError(f"invalid observation ({ts!r}, {v!r}, {at!r})")
            rows.append((ts, float(v), at))
        new = dup = corrected = 0
        with self._lock, self._guard("record observations"):
            self._require_healthy()
            self._check_quota((len(series.encode()) + OBSERVATION_ROW_BYTES) * len(rows) + RECORD_SLACK_BYTES)
            with self._tx():
                for ts, v, received_ms in rows:
                    have = self._db.execute("SELECT value FROM observations WHERE series = ? AND event_time = ?",
                                            (series, ts)).fetchone()
                    if have is None:
                        self._db.execute("INSERT INTO observations (series, event_time, value, first_received_ms, "
                                         "source_sha256) VALUES (?,?,?,?,?)", (series, ts, v, received_ms,
                                                                               source_sha256))
                        new += 1
                    elif have[0] == v:
                        dup += 1
                    else:
                        self._db.execute("INSERT INTO observation_corrections (series, event_time, value, "
                                         "received_ms, source_sha256) VALUES (?,?,?,?,?)",
                                         (series, ts, v, received_ms, source_sha256))
                        corrected += 1
        return ObservationReceipt(new, dup, corrected)

    def observations(self, series: str, lo: int, hi: int) -> List[Tuple[int, float, int]]:
        """[(event_time, value, first_received_ms)] for lo <= event_time <= hi, first receipts only."""
        with self._lock, self._guard("read observations"):
            return [tuple(r) for r in self._db.execute(
                "SELECT event_time, value, first_received_ms FROM observations WHERE series = ? AND event_time "
                "BETWEEN ? AND ? ORDER BY event_time", (series, lo, hi)).fetchall()]

    def corrections(self, series: str) -> List[Tuple[int, float, int]]:
        with self._lock, self._guard("read corrections"):
            return [tuple(r) for r in self._db.execute(
                "SELECT event_time, value, received_ms FROM observation_corrections WHERE series = ? ORDER BY seq",
                (series,)).fetchall()]

    # --- checkpoints ------------------------------------------------------------------------------------------------

    def write_checkpoint(self, evidence_key: str, state: Mapping, last_outcome_seq: int) -> Checkpoint:
        """A new checkpoint; identical to the latest one: nothing is written. Older ones beyond CHECKPOINTS_KEPT go."""
        data = self._encode(state, MAX_CHECKPOINT_BYTES, "checkpoint")
        with self._lock, self._guard("write checkpoint"):
            self._require_healthy()
            latest = self.latest_checkpoint(evidence_key)
            if latest is not None:
                if last_outcome_seq < latest.last_outcome_seq:
                    raise StoreError("a checkpoint cannot go back before the latest one")
                if (latest.last_outcome_seq, latest.payload_sha256) == (last_outcome_seq, sha256_hex(data)):
                    return latest
            self._check_quota(len(data) + len(data) // 512 + RECORD_SLACK_BYTES)   # and overflow chains
            now = self.clock.now_ms()
            with self._tx():
                self._db.execute("INSERT INTO checkpoints (evidence_key, last_outcome_seq, schema_version, payload, "
                                 "payload_sha256, created_at_ms) VALUES (?,?,?,?,?,?)",
                                 (evidence_key, last_outcome_seq, SCHEMA_VERSION, data, sha256_hex(data), now))
                self._db.execute("DELETE FROM checkpoints WHERE evidence_key = ? AND seq NOT IN (SELECT seq FROM "
                                 "checkpoints WHERE evidence_key = ? ORDER BY seq DESC LIMIT ?)",
                                 (evidence_key, evidence_key, CHECKPOINTS_KEPT))
            return self.latest_checkpoint(evidence_key)

    def latest_checkpoint(self, evidence_key: str) -> Optional[Checkpoint]:
        with self._lock, self._guard("read checkpoint"):
            row = self._db.execute("SELECT seq, last_outcome_seq, schema_version, payload, payload_sha256, "
                                   "created_at_ms FROM checkpoints WHERE evidence_key = ? ORDER BY seq DESC LIMIT 1",
                                   (evidence_key,)).fetchone()
            if row is None:
                return None
            seq, last, version, payload, psha, at = row
            payload = bytes(payload)
            if version != SCHEMA_VERSION or sha256_hex(payload) != psha:
                raise StoreCorrupt(f"checkpoint {seq} does not match its checksum or schema")
            return Checkpoint(seq, evidence_key, last, json.loads(payload), psha, at)

    def recover(self, evidence_key: str, fold: Callable[[dict, Outcome], dict],
                initial: Callable[[], dict]) -> Tuple[dict, int, int]:
        """(state, last outcome seq, outcomes applied): the latest checkpoint plus every later outcome folded in once,
        in sequence order. Nothing is written; the caller checkpoints the result."""
        cp = self.latest_checkpoint(evidence_key)
        state, last = (dict(cp.state), cp.last_outcome_seq) if cp else (initial(), 0)
        applied = 0
        for o in self.outcomes_after(evidence_key, last):
            state, last, applied = fold(state, o), o.seq, applied + 1
        return state, last, applied

    # --- retention --------------------------------------------------------------------------------------------------

    def compact(self, now_s: int) -> Dict[str, int]:
        """Retire windows that ended before now - retention: remove each such issuance with its outcomes when all of
        them are covered by the epoch's latest verified checkpoint (seq <= its last_outcome_seq), and observations
        older than their own retention. Keeps per-epoch counts and a digest chain of what was removed. Returns removal
        counts and how many old issuances had to stay (uncovered or still open)."""
        horizon = now_s - self.retention_seconds
        with self._lock, self._guard("compact"):
            self._require_healthy()
            if horizon <= self._horizon():
                return {"issuances": 0, "outcomes": 0, "observations": 0, "kept": 0}
            covered = {}                               # epoch -> last outcome seq of its latest verified checkpoint
            for (ek,) in self._db.execute("SELECT DISTINCT evidence_key FROM issuances WHERE window_end < ?",
                                          (horizon,)).fetchall():
                cp = self.latest_checkpoint(ek)        # raises StoreCorrupt for a checkpoint that fails its checksum
                covered[ek] = cp.last_outcome_seq if cp else 0
            with self._tx():
                # Raise the horizon first: the delete triggers admit only rows below it, and keys below it are retired.
                self._db.execute("UPDATE meta SET value = ? WHERE key = 'retention_horizon'", (str(horizon),))
                gone = self._db.execute(f"SELECT {self._ISSUANCE_COLS} FROM issuances WHERE window_end < ? "
                                        "ORDER BY seq", (horizon,)).fetchall()
                epochs: Dict[str, List] = {}
                kept = 0
                for row in gone:
                    iss = self._row_to_issuance(row)
                    rows = self._db.execute("SELECT seq, status, payload_sha256, model FROM outcomes WHERE "
                                            "issuance_id = ? ORDER BY seq", (iss.issuance_id,)).fetchall()
                    outs = [r[:3] for r in rows]
                    # Every expected model must have its terminal outcome, each folded into the checkpoint.
                    if {r[3] for r in rows} != set(iss.models) or \
                            any(seq > covered[iss.key.evidence_key] for seq, _, _ in outs):
                        kept += 1
                        continue
                    epochs.setdefault(iss.key.evidence_key, []).append((iss, outs))
                n_iss = n_out = 0
                for ek, items in epochs.items():
                    prev = self._db.execute("SELECT issued, scored, unscorable, digest FROM retention WHERE "
                                            "evidence_key = ?", (ek,)).fetchone() or (0, 0, 0, "0" * 64)
                    issued, scored, unscorable, digest = prev
                    for iss, outs in items:
                        issued += 1
                        scored += sum(1 for _, st, _ in outs if st == "scored")
                        unscorable += sum(1 for _, st, _ in outs if st == "unscorable")
                        digest = sha256_hex(canonical_json([digest, iss.issuance_id, iss.bundle_sha256,
                                                            list(iss.models), [p for _, _, p in outs]]))
                        n_out += self._db.execute("DELETE FROM outcomes WHERE issuance_id = ?",
                                                  (iss.issuance_id,)).rowcount
                        n_iss += self._db.execute("DELETE FROM issuances WHERE issuance_id = ?",
                                                  (iss.issuance_id,)).rowcount
                    self._db.execute("INSERT OR REPLACE INTO retention (evidence_key, through_window_end, issued, "
                                     "scored, unscorable, digest) VALUES (?,?,?,?,?,?)",
                                     (ek, horizon, issued, scored, unscorable, digest))
                n_obs = self._db.execute("DELETE FROM observations WHERE event_time < ?",
                                         (horizon - self._obs_extra,)).rowcount
            self._db.execute("PRAGMA incremental_vacuum")
            self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            return {"issuances": n_iss, "outcomes": n_out, "observations": n_obs, "kept": kept}

    def retention_summary(self, evidence_key: str) -> Optional[dict]:
        with self._lock, self._guard("read retention"):
            row = self._db.execute("SELECT through_window_end, issued, scored, unscorable, digest FROM retention "
                                   "WHERE evidence_key = ?", (evidence_key,)).fetchone()
        if row is None:
            return None
        return dict(zip(("through_window_end", "issued", "scored", "unscorable", "digest"), row))

    # --- verification and export ------------------------------------------------------------------------------------

    def verify(self) -> None:
        """Full check: SQLite's integrity_check plus every record's checksum. Raises StoreCorrupt."""
        with self._lock, self._guard("verify"):
            check = self._db.execute("PRAGMA integrity_check").fetchall()
            if check != [("ok",)]:
                raise StoreCorrupt(f"integrity_check: {check[:3]}")
            for row in self._db.execute(f"SELECT {self._ISSUANCE_COLS} FROM issuances ORDER BY seq"):
                self._row_to_issuance(row)             # row by row: a recovery must not load the journal at once
            for row in self._db.execute(f"SELECT {self._OUTCOME_COLS} FROM outcomes ORDER BY seq"):
                self._row_to_outcome(row)
            for (ek,) in self._db.execute("SELECT DISTINCT evidence_key FROM checkpoints").fetchall():
                self.latest_checkpoint(ek)

    def journal_digest(self) -> str:
        """A digest of the whole journal's logical content, in commit order (row numbers themselves excluded)."""
        h = hashlib.sha256()
        with self._lock, self._guard("digest"):
            db = self._db
            for table, cols in (
                    ("issuances", "issuance_id, key_sha256, bundle_sha256, models, committed_at_ms"),
                    ("outcomes", "issuance_id, model, status, payload_sha256, committed_at_ms"),
                    ("checkpoints", "evidence_key, last_outcome_seq, payload_sha256, created_at_ms"),
                    ("observation_corrections", "series, event_time, value, received_ms, source_sha256")):
                h.update(table.encode())
                for row in db.execute(f"SELECT {cols} FROM {table} ORDER BY seq"):
                    h.update(canonical_json(list(row)))
            h.update(b"observations")
            for row in db.execute("SELECT series, event_time, value, first_received_ms, source_sha256 FROM "
                                  "observations ORDER BY series, event_time"):
                h.update(canonical_json(list(row)))
            h.update(b"retention")
            for row in db.execute("SELECT * FROM retention ORDER BY evidence_key"):
                h.update(canonical_json(list(row)))
        return h.hexdigest()

    def export_manifest(self) -> dict:
        """Counts and the journal digest: the hook an evaluation export pins."""
        with self._lock, self._guard("manifest"):
            count = {t: self._db.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                     for t in ("issuances", "outcomes", "observations", "observation_corrections", "checkpoints")}
        return {"schema": "forecast-journal-manifest/v1", "store_id": self.store_id,
                "schema_version": SCHEMA_VERSION, "counts": count, "journal_sha256": self.journal_digest()}
