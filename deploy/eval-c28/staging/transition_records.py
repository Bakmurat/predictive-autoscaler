#!/usr/bin/env python3
"""The durable records of the Active transition (TRANSITION-DESIGN draft 6, section 3). Files only: no cluster.

Two kinds of record, kept apart because they are written under different conditions:

1. OWNER records, in the transition helper's hash-chained receipt log.
   - "executor-started": written by a transition executor about ITSELF (its fence record, taken from the running
     process, never passed in), bound to its operation and activation, BEFORE it starts any child or sends any
     mutation. It must hold the shared evaluation lock: the log has one chain and one writer.
   - "executor-finished": written by that SAME process (same host, boot, pid, start time, group), only after every
     child that could grant has been collected and no grant action is possible any more.
   - "executor-fenced": the proof-based completion for an executor that did not finish. It is written by a recovery
     that THIS process holds (transition_records.recovery: the shared recovery guard plus this process's own token
     in it), which runs the fence itself on the recorded owner and records the result only when it is proven. A
     recovery never writes "executor-finished" for someone else.
   The log is read as a sequence: ids are unique and in the expected form, a completion refers to an earlier start
   that is not yet completed. Anything else is a problem. An emergency fences EVERY outstanding executor.

2. EMERGENCY records, one small file each, in their own directory. An emergency runs WITHOUT the shared lock (it must
   work while a hung executor holds it), so it never appends to a chained log. A file appears under its final name
   only with its whole content (written aside, then linked exclusively). "<seq>-requested.json" opens an emergency;
   only a VALID "<seq>-revoked.json" (the tombstone: binding removed, fence proven, writes denied, each literally
   true) closes it. The CONTENT of every file is validated on every read (schema, run, sequence, event, facts): a
   name alone means nothing, and an empty, partial, unreadable or mismatched file is unresolved. A new request takes
   a sequence number above EVERY existing file's, also an orphan outcome's, so it can never attach itself to an old
   outcome. While any emergency is open, or anything cannot be read with certainty, every transition operation except
   emergency and recover refuses; and a grant plan made before an emergency is dead.

Unknown is never "nothing there". Deleting the binding does not depend on any of this; but a failure to write or read
these records can never produce a claim that the permission was revoked.
"""
import hashlib
import json
import os
import re
import secrets

import common_records as C
import execlib as X
import transition_fence as F

HELPER = "transition"
EMERGENCY_DIR = "transition-emergency"
SCHEMA = "c28-transition-emergency/v1"
OPERATIONS = ("grant", "activate", "deactivate", "revoke", "recover")
OUTCOMES = ("revoked", "fence-unproven", "not-revoked")
TOMBSTONE_FACTS = ("binding_removed", "fence_proven", "writes_denied")
ACTIVATION = re.compile(r"[a-z0-9]([-a-z0-9]{0,30}[a-z0-9])?\Z")
EVENT_FILE = re.compile(r"(\d{6})-(requested|revoked|fence-unproven|not-revoked)\.json\Z")
CLAIMS_SEQ = re.compile(r"(\d{6})(?!\d)")            # any name that starts with a sequence number
OWNER_EVENTS = ("executor-started", "executor-finished", "executor-fenced")
MAX_SEQ = 999_999
IDENTITY_FILE = "transition-identity.json"
IDENTITY_SCHEMA = "c28-transition-identity/v1"
IDENTITY_FIELDS = ("release", "control_namespace", "target_namespace", "targets", "operator_identity",
                   "decision_interval_seconds", "cadence_source")
MAX_DECISION_INTERVAL_SECONDS = 3600
K8S_NAME = re.compile(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?\Z")
CHART_DIR = "transition-chart"
CHART_SCHEMA = "c28-transition-chart/v1"
CHART_FILE = re.compile(r"(\d{6})-restore-pending\.json\Z")
PROOF_DIR = "transition-proofs"


# --- durability --------------------------------------------------------------------------------------------------------

def _sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _durable_entry(path, root):
    """Make the directory entries durable from `path`'s directory up to the directory that HOLDS `root`: a synced file
    whose name, or whose new directory's name, is not on disk yet is not durable."""
    stop = os.path.dirname(os.path.abspath(root))
    current = os.path.dirname(os.path.abspath(path))
    if os.path.commonpath([current, stop]) != stop:
        raise X.StagingError(f"{path} is not under the record's evidence directory")
    while True:
        _sync_dir(current)
        if current == stop:
            return
        current = os.path.dirname(current)


# --- owner records (one writer: the holder of the shared evaluation lock, or a recovery of it) --------------------------

def _holds_lock(record, lock):
    """Why this process is not the holder of the shared evaluation lock, or None."""
    if not isinstance(lock, C.exclusive_lock) or lock.path != X.lock_file(record):
        return "not the shared evaluation lock of this record"
    try:
        with open(lock.path) as fh:
            body = json.load(fh)
    except (OSError, ValueError) as e:
        return f"the lock file cannot be read ({type(e).__name__})"
    if not isinstance(body, dict) or body.get("pid") != os.getpid() \
            or body.get("pid_start") != C.process_start(os.getpid()):
        return "the lock file names another process"
    return None


def _owner_log(record):
    """({"starts": {id: data}, "completed": {id: event}, "order": [id]}, problem). The owner events of the log read
    in sequence; other receipts of the helper (its journal) are not owner events and are passed over."""
    path = C.receipts_path(record.doc, HELPER)
    try:
        # Absence must be CONFIRMED: the shared reader asks os.path.exists(), which also answers "no" when the path
        # cannot be reached (no permission to enter a directory), and would then return an empty log.
        try:
            size = os.stat(path).st_size
        except FileNotFoundError:
            size = None                                   # no such file, and every directory on the way was readable
        entries = X.receipts(record, HELPER) if size is not None else []
        if size and not entries:
            return None, "the transition log cannot be read (it is not empty, but it read as empty)"
    except Exception as e:                                # StagingError (chain), ValueError (torn line), OSError
        return None, f"the transition log cannot be read ({type(e).__name__})"
    starts, completed, order = {}, {}, []
    for e in entries:
        event = e.get("event")
        if event not in OWNER_EVENTS:
            continue
        data = e.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("id"), str):
            return None, f"the transition log holds an {event} record that is not in the expected form"
        executor = data["id"]
        if event == "executor-started":
            operation, activation, owner = data.get("operation"), data.get("activation"), data.get("owner")
            if operation not in OPERATIONS or not isinstance(activation, str) or not ACTIVATION.match(activation) \
                    or not isinstance(owner, dict) or set(owner) != set(F.FIELDS) or F._problem(owner, 0.0):
                return None, f"the transition log holds an executor-started record that is not valid ({executor!r})"
            if executor != f"{operation}-{activation}-{len(order) + 1}" or executor in starts:
                return None, f"the transition log holds a start with an unexpected or repeated id ({executor!r})"
            starts[executor] = data
            order.append(executor)
        else:
            if executor not in starts:
                return None, f"the transition log completes {executor!r} before, or without, its start"
            if executor in completed:
                return None, f"the transition log completes {executor!r} more than once"
            if event == "executor-fenced" and not (isinstance(data.get("fence"), dict)
                                                   and data["fence"].get("proven") is True):
                return None, f"the transition log holds an executor-fenced record without a proven fence ({executor!r})"
            completed[executor] = event
    return {"starts": starts, "completed": completed, "order": order}, None


def _append(record, event, data, clock):
    X.receipt(record, HELPER, event, data, clock)
    _durable_entry(C.receipts_path(record.doc, HELPER), record.evidence_dir)


class recovery:
    """`with R.recovery(record, lock_sha256) as held:` — the recovery of this record's stale evaluation lock, HELD BY
    THIS PROCESS. It enters common_records.recovery_guard (one recoverer; the lock's sha256 as given; the lock's
    executor proven terminated), whose guard record names this process and a fresh token. `problem()` says whether
    that record is still this object's, now.
    An object that was never entered, or whose guard another recovery holds, owns nothing: it cannot record a fence,
    cannot release the stale lock, and on leaving does not remove a guard that is not its own."""

    def __init__(self, record, lock_sha256):
        self.record, self.lock_sha256 = record, lock_sha256
        self._guard = C.recovery_guard(record.doc, X.LOCK_SCOPE, lock_sha256)
        self._token = None
        # What THIS recovery saw, itself, about the revoked permission: {"binding": "absent", "proof_sha256": ...}.
        # It lives in this object only: a release that needs it cannot borrow it from an earlier, interrupted run.
        self.fresh_revocation = None

    def __enter__(self):
        self._guard.__enter__()                           # raises unless this process may recover this lock
        self._token = self._guard.token                   # the shared guard record carries this recovery's identity
        return self

    def __exit__(self, exc_type, exc, tb):
        """Leave the recovery. The guard file is removed ONLY if it is still this object's (its token, this process):
        a guard that is another recovery's, unreadable or gone is left exactly as it is. This check is about the
        guard alone: a successful release_stale() has removed the stale lock by now, legitimately."""
        problem = self._guard_problem()
        self._token = None
        self.fresh_revocation = None
        # Always leave the shared guard: that releases the coordination lock, and it removes the guard record only if
        # the record is still this recovery's (the shared guard checks the token itself).
        self._guard.__exit__(exc_type, exc, tb)
        if problem is not None and exc_type is None:
            raise X.StagingError(f"the recovery guard was not removed: {problem}")
        return False                                      # an error is already on its way: do not hide it

    def release_stale(self):
        """Release the stale evaluation lock, only while this process holds this recovery of THAT lock."""
        problem = self.problem()
        if problem:
            raise X.StagingError(f"the stale lock is released only by the recovery that holds it: {problem}")
        self._guard.release_stale()

    def _guard_problem(self):
        """Why the guard file is not this object's right now, or None."""
        if self._token is None:
            return "this recovery was not entered"
        try:
            with open(self._guard.guard) as fh:
                body = json.load(fh)
        except (OSError, ValueError) as e:
            return f"the recovery guard cannot be read ({type(e).__name__})"
        if not isinstance(body, dict) or body.get("token") != self._token or body.get("pid") != os.getpid() \
                or body.get("pid_start") != C.process_start(os.getpid()):
            return "the recovery guard is held by another recovery"
        return None

    def problem(self):
        """Why this process does not hold this recovery of the stale lock right now, or None."""
        problem = self._guard_problem()
        if problem:
            return problem
        try:
            if C.sha256_file(self._guard.path) != self.lock_sha256:
                return "the stale lock is no longer the one this recovery was entered for"
        except OSError as e:
            return f"the stale lock cannot be read ({type(e).__name__})"
        return None


def started(record, operation, activation, lock, clock=None, detail=None):
    """Register THIS process as a transition executor, before it can do anything. Returns the executor id. `detail`
    (a plain object) is kept in the same record: what the operation must have on record from its first moment (a
    revocation: its protocol and the emergency sequence it started at). The owner
    record is taken from the running process; it refuses one that could not be fenced later (in practice: call
    transition_fence.own_group() first), one that does not hold the shared lock, and a log that is not in order."""
    problem = _holds_lock(record, lock)
    if problem:
        raise X.StagingError(f"an executor registers under the shared evaluation lock: {problem}")
    if operation not in OPERATIONS:
        raise X.StagingError(f"unknown transition operation {operation!r}")
    if not isinstance(activation, str) or not ACTIVATION.match(activation):
        raise X.StagingError("the activation id must be a short DNS label")
    owner = F.owner()
    problem = F._problem(owner, 0.0)
    if problem:
        raise X.StagingError(f"this executor cannot be fenced ({problem}): call transition_fence.own_group() first")
    log, problem = _owner_log(record)
    if problem:
        raise X.StagingError(f"{problem}: nothing may be registered on top of it")
    executor = f"{operation}-{activation}-{len(log['order']) + 1}"
    data = {"id": executor, "operation": operation, "activation": activation,
            "owner": {k: owner[k] for k in F.FIELDS}}
    if detail is not None:
        if not isinstance(detail, dict):
            raise X.StagingError("an executor's detail is a plain object")
        data["detail"] = json.loads(json.dumps(detail))
    _append(record, "executor-started", data, clock)
    return executor


def finished(record, executor, lock, *, children_collected, clock=None):
    """Close THIS process's own registration. children_collected must be True: the caller states that every child
    that could grant has been collected (waited for) and that it can take no grant action after this call. Only the
    process that started the registration can finish it; a recovery uses fenced()."""
    problem = _holds_lock(record, lock)
    if problem:
        raise X.StagingError(f"an executor finishes under the shared evaluation lock: {problem}")
    if children_collected is not True:
        raise X.StagingError("an executor is finished only after its grant-capable children were collected")
    log, problem = _owner_log(record)
    if problem:
        raise X.StagingError(f"{problem}: refusing")
    if executor not in log["starts"]:
        raise X.StagingError(f"executor {executor!r} was never started")
    if executor in log["completed"]:
        raise X.StagingError(f"executor {executor!r} is already completed ({log['completed'][executor]})")
    me = F.owner()
    if {k: me[k] for k in F.FIELDS} != log["starts"][executor]["owner"]:
        raise X.StagingError(f"executor {executor!r} was started by another process: only that process finishes it; "
                             "a recovery completes it with a fence proof (fenced)")
    _append(record, "executor-finished", {"id": executor}, clock)


def _held(record, guard):
    """Why `guard` is not an ENTERED recovery of this record's evaluation lock held by this process, or None."""
    if not isinstance(guard, recovery) or guard._guard.path != X.lock_file(record) \
            or guard.record.sha256 != record.sha256:
        return "not a recovery of this record's evaluation lock"
    return guard.problem()


def fenced(record, executor, guard, *, fence=F.fence, clock=None):
    """The proof-based completion, for a recovery: run the fence on the executor's RECORDED owner and, only when it
    is proven, record it. `guard` is an ENTERED transition_records.recovery of this record, held by this process:
    that is checked before the fence and again immediately before the append, because the owner log has one chain
    and one writer. Returns the fence's result; raises (and writes nothing) when anything does not hold."""
    def held():
        return _held(record, guard)
    problem = held()
    if problem:
        raise X.StagingError(f"an executor is fenced under a recovery held by this process: {problem}")
    log, problem = _owner_log(record)
    if problem:
        raise X.StagingError(f"{problem}: refusing")
    if executor not in log["starts"]:
        raise X.StagingError(f"executor {executor!r} was never started")
    if executor in log["completed"]:
        raise X.StagingError(f"executor {executor!r} is already completed ({log['completed'][executor]})")
    result = fence(dict(log["starts"][executor]["owner"]))
    if not (isinstance(result, dict) and result.get("proven") is True):
        why = result.get("why", "no reason given") if isinstance(result, dict) else "no result"
        raise X.StagingError(f"executor {executor!r} is NOT fenced: {why}")
    problem = held()                                      # still ours NOW: the fence took time
    if problem:
        raise X.StagingError(f"the recovery was lost while fencing ({problem}): nothing was recorded")
    _append(record, "executor-fenced", {"id": executor, "fence": json.loads(json.dumps(result))}, clock)
    return result


def owner_summary(record):
    """({"outstanding": [started records, in order], "completed": {id: event}}, problem). With a problem: (None,
    problem), and nothing is known."""
    log, problem = _owner_log(record)
    if problem:
        return None, problem
    return {"outstanding": [log["starts"][i] for i in log["order"] if i not in log["completed"]],
            "completed": dict(log["completed"])}, None


def outstanding(record):
    """(every executor that started and is neither finished nor fenced, in order; problem). With a problem the list
    is empty and means UNKNOWN: the caller must treat it as unfenced."""
    log, problem = _owner_log(record)
    if problem:
        return [], problem
    return [log["starts"][i] for i in log["order"] if i not in log["completed"]], None


# --- emergency records (no lock: one exclusively linked, validated file per event) ------------------------------------

def _dir(record):
    return os.path.join(record.evidence_dir, "receipts", EMERGENCY_DIR)


def _create(record, name, obj):
    """Put the file in place under its final name with its WHOLE content, exclusively, and durably: written and
    synced under a temporary name, then linked (the link fails if the name is taken). FileExistsError if taken."""
    text = json.dumps(obj, indent=1, sort_keys=True) + "\n"
    C.assert_clean(text)
    _publish(record, _dir(record), name, text.encode())


def _publish(record, directory, name, data):
    """Put `data` in place as <directory>/<name>, exclusively and durably (see _create). FileExistsError if taken."""
    os.makedirs(directory, exist_ok=True)
    tmp = os.path.join(directory, f".tmp-{os.getpid()}-{secrets.token_hex(8)}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.link(tmp, os.path.join(directory, name))
    finally:
        os.unlink(tmp)
    _durable_entry(os.path.join(directory, name), record.evidence_dir)


def _valid(record, name, seq, event):
    """Why this emergency file's CONTENT does not say what its name says, or None."""
    try:
        with open(os.path.join(_dir(record), name)) as fh:
            doc = json.load(fh)
    except (OSError, ValueError) as e:
        return f"{name} cannot be read ({type(e).__name__})"
    if not isinstance(doc, dict) or doc.get("schema") != SCHEMA or doc.get("run_id") != record.run_id \
            or isinstance(doc.get("seq"), bool) or doc.get("seq") != seq or doc.get("event") != event \
            or not isinstance(doc.get("at"), str) or not doc["at"]:
        return f"{name} does not hold a {event} record of emergency {seq} of this run"
    if event == "requested":
        if not isinstance(doc.get("reason"), str):
            return f"{name} holds no reason"
    else:
        facts = doc.get("facts")
        if not isinstance(facts, dict):
            return f"{name} holds no facts"
        if event == "revoked" and any(facts.get(k) is not True for k in TOMBSTONE_FACTS):
            return f"{name} is not a tombstone: it does not state all of {', '.join(TOMBSTONE_FACTS)} as true"
    return None


def emergency_state(record):
    """{"requests": [seq], "open": [seq], "last": highest seq any file claims, "tombstones": [seq], "problem": ...}.
    A request is open until a VALID tombstone of the same sequence exists. Any file that is unexpected, unreadable or
    whose content does not match its name is a problem, and it closes nothing."""
    state = {"requests": [], "open": [], "last": 0, "tombstones": [], "problem": None}
    try:
        names = sorted(os.listdir(_dir(record)))
    except FileNotFoundError:
        return state                                      # no emergency was ever requested
    except OSError as e:
        return dict(state, problem=f"the emergency records cannot be listed ({type(e).__name__})")
    seen, problems = {}, []
    for name in names:
        claim = CLAIMS_SEQ.match(name)
        if claim:
            state["last"] = max(state["last"], int(claim.group(1)))
        m = EVENT_FILE.match(name)
        if not m:
            problems.append(f"an unexpected file is among the emergency records: {name!r}")
            continue
        seq, event = int(m.group(1)), m.group(2)
        bad = _valid(record, name, seq, event)
        if bad:
            problems.append(bad)
            continue
        seen.setdefault(seq, set()).add(event)
    for seq, kinds in sorted(seen.items()):
        if "requested" not in kinds:
            problems.append(f"emergency {seq} has an outcome without a valid request")
            continue
        state["requests"].append(seq)
        (state["tombstones"] if "revoked" in kinds else state["open"]).append(seq)
    state["problem"] = "; ".join(problems) or None
    return state


def emergency_request(record, reason, clock=None):
    """Open an emergency. Needs no lock. The sequence number is above EVERY number any existing file claims (an
    orphan outcome included), so a new request can never attach itself to an old outcome. Returns the sequence.
    Raises when the records cannot be listed or written: the caller still deletes the binding, and reports that the
    request was not recorded."""
    for _ in range(64):
        try:
            names = os.listdir(_dir(record))
        except FileNotFoundError:
            names = []
        except OSError as e:
            raise X.StagingError(f"the emergency records cannot be listed ({type(e).__name__}): "
                                 "the request was NOT recorded") from None
        seq = max([int(m.group(1)) for m in map(CLAIMS_SEQ.match, names) if m] or [0]) + 1
        if seq > MAX_SEQ:
            break
        try:
            _create(record, f"{seq:06d}-requested.json",
                    {"schema": SCHEMA, "run_id": record.run_id, "seq": seq, "event": "requested",
                     "at": C.utc_now(clock), "reason": str(reason)})
            return seq
        except FileExistsError:
            continue                                      # another emergency took it: look again
    raise X.StagingError("no free emergency sequence number: the request was NOT recorded")


def emergency_close(record, seq, outcome, facts, clock=None):
    """Record an emergency's outcome. Only "revoked" (the tombstone) closes it; it requires the binding removed, the
    fence proven and the writes proven denied, each stated as true in `facts`. The request must exist AND be valid."""
    if outcome not in OUTCOMES:
        raise X.StagingError(f"unknown emergency outcome {outcome!r}")
    if isinstance(seq, bool) or not isinstance(seq, int) or not 1 <= seq <= MAX_SEQ:
        raise X.StagingError("an emergency outcome needs its sequence number")
    request = f"{seq:06d}-requested.json"
    if not os.path.exists(os.path.join(_dir(record), request)):
        raise X.StagingError(f"emergency {seq} was never requested")
    bad = _valid(record, request, seq, "requested")
    if bad:
        raise X.StagingError(f"emergency {seq}'s request is not valid ({bad}): no outcome is attached to it")
    if not isinstance(facts, dict):
        raise X.StagingError("an emergency outcome needs its facts")
    if outcome == "revoked" and any(facts.get(k) is not True for k in TOMBSTONE_FACTS):
        raise X.StagingError("a tombstone needs the binding removed, the fence proven and the writes proven denied")
    try:
        _create(record, f"{seq:06d}-{outcome}.json",
                {"schema": SCHEMA, "run_id": record.run_id, "seq": seq, "event": outcome, "at": C.utc_now(clock),
                 "facts": json.loads(json.dumps(facts))})
    except FileExistsError:
        raise X.StagingError(f"emergency {seq} already has a {outcome!r} record") from None


def refuse_if_emergency_open(record, state=None):
    """For every transition operation except emergency and recover. Decides from ONE reading of the records."""
    state = emergency_state(record) if state is None else state
    if state["problem"]:
        raise X.StagingError(f"{state['problem']}: refusing (unknown is not 'no emergency')")
    if state["open"]:
        raise X.StagingError(f"emergency {state['open']} is not closed by a tombstone: only emergency and recover run")


def refuse_dead_grant(record, plan_last_seq):
    """A grant plan embeds emergency_state()["last"] at plan time. It is dead if any emergency record is newer, if
    one is open, or if the records cannot be read. Call under the lock, immediately before starting Helm; it narrows
    the window, and the fence, not this check, is the proof."""
    if isinstance(plan_last_seq, bool) or not isinstance(plan_last_seq, int) or plan_last_seq < 0:
        raise X.StagingError("the grant plan does not name the emergency sequence it was made at")
    state = emergency_state(record)                       # ONE snapshot: problem, open and last are judged together
    refuse_if_emergency_open(record, state)
    if state["last"] != plan_last_seq:
        raise X.StagingError(f"the grant plan was made at emergency sequence {plan_last_seq}, now {state['last']}: "
                             "a plan reviewed before an emergency is dead")


# --- the transition's identity: written once, BEFORE any action that can grant ------------------------------------------

def operator_identity(release, control_namespace):
    """The user name of the operator's ServiceAccount, as permissions.py reports it."""
    return f"system:serviceaccount:{control_namespace}:{release}-predictive-autoscaler-operator"


def _cadence_ok(seconds, source):
    return not isinstance(seconds, bool) and isinstance(seconds, int) \
        and 0 < seconds <= MAX_DECISION_INTERVAL_SECONDS and isinstance(source, str) and bool(source.strip())


def identity_write(record, *, release, control_namespace, target_namespace, targets, decision_interval_seconds,
                   cadence_source):
    """Fix, for this run on this cluster, what a later emergency must be able to name without the params file: the
    release, both namespaces, the operator's identity and the targets; and the decision interval with where it was
    read from (a revocation's /scale observation spans two of them). Written once; writing the SAME identity again
    is fine, a different one is refused (the params may change later; the grant they described does not)."""
    for value, what in ((release, "release"), (control_namespace, "control namespace"),
                        (target_namespace, "target namespace")):
        if not isinstance(value, str) or not K8S_NAME.match(value):
            raise X.StagingError(f"the transition identity needs the {what}")
    if not isinstance(targets, (list, tuple)) or not targets or len(set(map(str, targets))) != len(targets) \
            or not all(isinstance(t, str) and K8S_NAME.match(t) for t in targets):
        raise X.StagingError("the transition identity needs the targets (distinct names)")
    if not _cadence_ok(decision_interval_seconds, cadence_source):
        raise X.StagingError("the transition identity needs the decision interval (whole seconds) and its source")
    doc = {"schema": IDENTITY_SCHEMA, "run_id": record.run_id, "cluster_uid": record.cluster_uid, "release": release,
           "control_namespace": control_namespace, "target_namespace": target_namespace, "targets": sorted(targets),
           "operator_identity": operator_identity(release, control_namespace),
           "decision_interval_seconds": decision_interval_seconds, "cadence_source": cadence_source}
    data = (json.dumps(doc, indent=1, sort_keys=True) + "\n").encode()
    path = os.path.join(record.evidence_dir, IDENTITY_FILE)
    try:
        _publish(record, record.evidence_dir, IDENTITY_FILE, data)
    except FileExistsError:
        with open(path, "rb") as fh:
            if fh.read() != data:
                raise X.StagingError("this run already has a DIFFERENT transition identity: it is written once") from None
        _durable_entry(path, record.evidence_dir)
    return doc


def identity_read(record):
    """(fields, problems). `fields` always has every key of IDENTITY_FIELDS; a value that cannot be trusted is None.
    Plain JSON and per-field checks only, so that an emergency can use whatever IS readable: with the release and the
    target namespace it can name the binding even when the targets are damaged. A file of another run or cluster
    gives nothing."""
    fields = dict.fromkeys(IDENTITY_FIELDS)
    try:
        with open(os.path.join(record.evidence_dir, IDENTITY_FILE)) as fh:
            doc = json.load(fh)
    except (OSError, ValueError) as e:
        return fields, [f"the transition identity cannot be read ({type(e).__name__})"]
    if not isinstance(doc, dict) or doc.get("schema") != IDENTITY_SCHEMA or doc.get("run_id") != record.run_id \
            or doc.get("cluster_uid") != record.cluster_uid:
        return fields, ["the transition identity is not this run's on this cluster"]
    problems = []
    for key in ("release", "control_namespace", "target_namespace"):
        value = doc.get(key)
        if isinstance(value, str) and K8S_NAME.match(value):
            fields[key] = value
        else:
            problems.append(f"the transition identity holds no usable {key}")
    targets = doc.get("targets")
    if isinstance(targets, list) and targets and all(isinstance(t, str) and K8S_NAME.match(t) for t in targets) \
            and len(set(targets)) == len(targets):
        fields["targets"] = sorted(targets)
    else:
        problems.append("the transition identity holds no usable targets")
    if fields["release"] and fields["control_namespace"] \
            and doc.get("operator_identity") == operator_identity(fields["release"], fields["control_namespace"]):
        fields["operator_identity"] = doc["operator_identity"]
    else:
        problems.append("the transition identity holds no usable operator identity")
    if _cadence_ok(doc.get("decision_interval_seconds"), doc.get("cadence_source")):
        fields["decision_interval_seconds"], fields["cadence_source"] = \
            doc["decision_interval_seconds"], doc["cadence_source"]
    else:
        problems.append("the transition identity holds no usable decision interval")
    return fields, problems


# --- the chart-restoration obligation (a revocation leaves the chart granting; restoring it is a later step) ----------

def _chart_dir(record):
    return os.path.join(record.evidence_dir, "receipts", CHART_DIR)


def chart_state(record):
    """{"pending": [seq] or None, "last": highest sequence, "problem": ...}. An obligation is pending from its
    restore-pending file on. NOTHING closes one yet: the chart restore, its record and the evidence that record must
    carry are not defined, so a file that claims a restoration is not accepted as one (it is a problem like any
    other unexpected file). With a problem, pending is None: UNKNOWN, never "none"."""
    try:
        names = sorted(os.listdir(_chart_dir(record)))
    except FileNotFoundError:
        return {"pending": [], "last": 0, "problem": None}
    except OSError as e:
        return {"pending": None, "last": None, "problem": f"the chart records cannot be listed ({type(e).__name__})"}
    opened, problems = set(), []
    for name in names:
        m = CHART_FILE.match(name)
        if not m:
            problems.append(f"an unexpected file is among the chart records: {name!r}")
            continue
        seq = int(m.group(1))
        try:
            with open(os.path.join(_chart_dir(record), name)) as fh:
                doc = json.load(fh)
        except (OSError, ValueError) as e:
            problems.append(f"{name} cannot be read ({type(e).__name__})")
            continue
        if not isinstance(doc, dict) or doc.get("schema") != CHART_SCHEMA or doc.get("run_id") != record.run_id \
                or isinstance(doc.get("seq"), bool) or doc.get("seq") != seq or doc.get("event") != "restore-pending" \
                or not isinstance(doc.get("at"), str) or not doc["at"]:
            problems.append(f"{name} does not hold a restore-pending record {seq} of this run")
            continue
        opened.add(seq)
    if opened and opened != set(range(1, max(opened) + 1)):
        problems.append("the chart obligations are not numbered without a gap")
    if problems:
        return {"pending": None, "last": None, "problem": "; ".join(problems)}
    return {"pending": sorted(opened), "last": max(opened, default=0), "problem": None}


def chart_pending(record, reason, clock=None):
    """Establish, durably, that the chart still has to be restored to its shadow values. Call it BEFORE the first
    deletion of a revocation. Returns the pending sequence: the one already pending if there is one, else a new one.
    Nothing closes it yet (see chart_state), and finishing an executor does not."""
    if not isinstance(reason, str) or not reason.strip():
        raise X.StagingError("a chart obligation needs its reason")
    state = chart_state(record)
    if state["problem"]:
        raise X.StagingError(f"the chart records cannot be read with certainty ({state['problem']})")
    if state["pending"]:
        return state["pending"][-1]
    seq = state["last"] + 1
    if seq > MAX_SEQ:
        raise X.StagingError("the chart obligations are exhausted")
    text = json.dumps({"schema": CHART_SCHEMA, "run_id": record.run_id, "seq": seq, "event": "restore-pending",
                       "at": C.utc_now(clock), "reason": reason}, indent=1, sort_keys=True) + "\n"
    C.assert_clean(text)
    try:
        _publish(record, _chart_dir(record), f"{seq:06d}-restore-pending.json", text.encode())
    except FileExistsError:
        raise X.StagingError(f"chart obligation {seq} was taken by another writer: read the records again") from None
    return seq


def refuse_if_chart_pending(record):
    """For grant and activate: no new permission and no activation while the chart still has to be restored, or
    while that cannot be known."""
    state = chart_state(record)
    if state["problem"]:
        raise X.StagingError(f"the chart records cannot be read with certainty ({state['problem']}): refusing")
    if state["pending"]:
        raise X.StagingError(f"chart restoration {state['pending']} is pending: restore the chart to its shadow "
                             f"values first")


# --- the permission proof's durable copy -------------------------------------------------------------------------------

def store_proof(record, proof):
    """Store the full permission proof under its own digest (common_records.digest), durably, and read it back.
    Returns the digest. Call it BEFORE the proof is used: any failure here means there is no proof."""
    if not isinstance(proof, dict):
        raise X.StagingError("a permission proof is a result object")
    data = C.canonical(proof)
    sha = hashlib.sha256(data).hexdigest()
    C.assert_clean(data.decode())
    directory = os.path.join(record.evidence_dir, PROOF_DIR)
    path = os.path.join(directory, f"{sha}.json")
    try:
        _publish(record, directory, f"{sha}.json", data)
    except FileExistsError:
        _durable_entry(path, record.evidence_dir)
    with open(path, "rb") as fh:
        if hashlib.sha256(fh.read()).hexdigest() != sha:
            raise X.StagingError("the stored permission proof is not the proof that was read")
    return sha


# --- what a recovery of the stale lock must respect ---------------------------------------------------------------------

SETTLED_OUTCOMES = ("revoked", "not-started")
PROTOCOL = 2          # revocations whose registration carries {"protocol": 2, "emergency_last": n} in its detail


def proof_problems(proof, record, identity, targets):
    """Why this permission proof does not establish that the operator's scale write is denied. Empty = it does.
    Every field must be PRESENT and of the expected type: a missing list is not an empty list."""
    if not isinstance(proof, dict):
        return ["the permission proof is not a result"]
    why = []
    if proof.get("verdict") != "pass":
        why.append(f"the verdict is {proof.get('verdict')!r}, not 'pass'")
    for key in ("refused", "unproven"):
        if key not in proof or not isinstance(proof[key], list):
            why.append(f"the proof does not carry its {key!r} list")
        elif proof[key]:
            why.append(f"the proof lists {len(proof[key])} {key} finding(s)")
    if proof.get("cluster_uid") != record.cluster_uid or proof.get("recorded_cluster_uid") != record.cluster_uid:
        why.append("the proof is not of the recorded cluster")
    if proof.get("identity") != identity or proof.get("role") != "operator":
        why.append("the proof is not about the operator's identity")
    asked = proof.get("targets")
    if not isinstance(asked, list) or not asked or sorted(map(str, asked)) != sorted(targets):
        why.append("the proof is not about the expected targets")
    return why


def revocations(record):
    """(every revoke executor in order, problem). Each: {"executor", "protocol", "emergency_last" (both from the
    registration's detail, None for a record of an earlier form), "outcome" (of its own "revocation" receipt),
    "intents" (journal intents written while it was the latest executor), "settlement" (the data of a
    "revocation-settled" receipt, unchecked)}. With a problem the list means UNKNOWN."""
    try:
        entries = X.receipts(record, HELPER)
    except Exception as e:
        return [], f"the transition receipts cannot be read ({type(e).__name__})"
    found, current = {}, None
    for e in entries:
        event, data = (e.get("event"), e.get("data")) if isinstance(e, dict) else (None, None)
        if not isinstance(event, str) or not isinstance(data, dict):
            return [], "the transition receipts hold an entry that is not in the expected form"
        if event == "executor-started":
            current = data.get("id")
            if data.get("operation") == "revoke":
                detail = data.get("detail") if isinstance(data.get("detail"), dict) else {}
                last = detail.get("emergency_last")
                found[current] = {"executor": current, "protocol": detail.get("protocol"),
                                  "emergency_last": last if isinstance(last, int) and not isinstance(last, bool)
                                  and last >= 0 else None, "outcome": None, "intents": 0, "settlement": None}
        elif event.startswith("intent-") and current in found:
            found[current]["intents"] += 1
        elif event == "revocation" and data.get("executor") in found:
            found[data["executor"]]["outcome"] = data.get("outcome")
        elif event == "revocation-settled" and data.get("executor") in found:
            found[data["executor"]]["settlement"] = data
    return list(found.values()), None


def _tombstone(record, seq):
    """(the tombstone document of emergency `seq`, the sha256 of its file), or (None, why)."""
    state = emergency_state(record)
    if state["problem"]:
        return None, f"the emergency records cannot be read with certainty ({state['problem']})"
    if isinstance(seq, bool) or not isinstance(seq, int) or seq not in state["tombstones"]:
        return None, f"emergency {seq!r} has no tombstone"
    path = os.path.join(_dir(record), f"{seq:06d}-revoked.json")
    try:
        with open(path, "rb") as fh:
            data = fh.read()
        return json.loads(data), hashlib.sha256(data).hexdigest()
    except (OSError, ValueError) as e:
        return None, f"the tombstone of emergency {seq} cannot be read ({type(e).__name__})"


def covers(tombstone, executor):
    """Whether this emergency's tombstone shows that it fenced `executor` (a proven fence) or found it already
    completed on record when it looked. Only then did the emergency's revocation come after that executor's last
    possible action."""
    fence = (tombstone.get("facts") or {}).get("fence") if isinstance(tombstone, dict) else None
    if not isinstance(fence, dict) or fence.get("problem") is not None:
        return False
    fenced_ = any(isinstance(e, dict) and e.get("id") == executor and e.get("proven") is True
                  for e in fence.get("executors") or [])
    return fenced_ or (isinstance(fence.get("completed"), list) and executor in fence["completed"])


def settlement_problem(record, revocation, settlement, completed):
    """Why this settlement does not settle this revocation, or None. `completed`: the ids the owner log shows as
    finished or fenced. The word "settled" settles nothing: every reference is opened and checked, each time.
      by "not-started": a registration of the current protocol, completed, with no journal intent after it and no
                        result of its own: it did nothing.
      by "tombstone":   an emergency tombstone LATER than the sequence the revocation started at, which covers this
                        executor (covers()), named with the hash of its file; the binding seen absent by the
                        recovery; and a stored permission proof, named by its digest, that holds for this run's
                        operator identity and targets."""
    if not isinstance(settlement, dict) or settlement.get("executor") != revocation["executor"]:
        return "the settlement is not about this revocation"
    if revocation["protocol"] != PROTOCOL:
        return "the revocation's registration is not of the current protocol: what it did cannot be inferred"
    if revocation["executor"] not in completed:
        return "the executor is neither finished nor fenced"
    if settlement.get("by") == "not-started":
        if revocation["intents"] or revocation["outcome"] is not None:
            return "the revocation has a journal intent or a result of its own: it started"
        return None
    if settlement.get("by") != "tombstone":
        return "the settlement names no ground"
    last, seq = revocation["emergency_last"], settlement.get("tombstone")
    if last is None or isinstance(seq, bool) or not isinstance(seq, int) or seq <= last:
        return "the tombstone is not later than the emergency sequence the revocation started at"
    tombstone, sha = _tombstone(record, seq)
    if tombstone is None:
        return sha
    if settlement.get("tombstone_sha256") != sha:
        return "the tombstone is not the file the settlement names (sha256 differs)"
    if not covers(tombstone, revocation["executor"]):
        return "the emergency neither fenced this executor nor found it completed"
    if settlement.get("binding") != "absent":
        return "the settlement does not state that the recovery saw the binding absent"
    return stored_proof_problem(record, settlement.get("proof_sha256"))


def stored_proof_problem(record, digest):
    """Why the stored permission proof named by `digest` does not hold for this run's identity, or None."""
    identity, problems = identity_read(record)
    if problems:
        return "the transition identity is incomplete: the proof cannot be checked"
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        return "the settlement names no stored permission proof"
    try:
        with open(os.path.join(record.evidence_dir, PROOF_DIR, f"{digest}.json"), "rb") as fh:
            data = fh.read()
        proof = json.loads(data)
    except (OSError, ValueError) as e:
        return f"the stored permission proof cannot be read ({type(e).__name__})"
    if hashlib.sha256(data).hexdigest() != digest:
        return "the stored permission proof is not the one the settlement names"
    why = proof_problems(proof, record, identity["operator_identity"], identity["targets"])
    return ("the stored permission proof does not hold: " + "; ".join(why)) if why else None


def unresolved_revocations(record):
    """(the revoke executors that are not settled, in order; problem). Settled: its own "revocation" receipt says
    revoked or not-started, or a "revocation-settled" receipt whose every reference checks out NOW
    (settlement_problem). With a problem the list means UNKNOWN."""
    found, problem = revocations(record)
    if problem:
        return [], problem
    summary, problem = owner_summary(record)
    if problem:
        return [], problem
    return [r["executor"] for r in found if r["outcome"] not in SETTLED_OUTCOMES
            and (r["settlement"] is None
                 or settlement_problem(record, r, r["settlement"], summary["completed"]) is not None)], None


def settle(record, guard, settlement, clock=None):
    """Record that a revocation is settled, under an ENTERED recovery held by this process, only when the settlement
    checks out (settlement_problem) and no emergency is open. The record names the stale lock it was made under:
    being settled is durable, being allowed to release THAT lock is not (release_problem). Raises, and writes
    nothing, otherwise."""
    problem = _held(record, guard)
    if problem:
        raise X.StagingError(f"a revocation is settled under a recovery held by this process: {problem}")
    emergencies = emergency_state(record)
    if emergencies["problem"] or emergencies["open"]:
        raise X.StagingError(f"nothing is settled while an emergency is open or its records are unreadable "
                             f"({emergencies['problem'] or emergencies['open']})")
    found, problem = revocations(record)
    summary, problem2 = owner_summary(record)
    if problem or problem2:
        raise X.StagingError(f"{problem or problem2}: refusing")
    match = [r for r in found if isinstance(settlement, dict) and r["executor"] == settlement.get("executor")]
    if not match:
        raise X.StagingError("the settlement names no revocation of this run")
    problem = settlement_problem(record, match[0], settlement, summary["completed"])
    if problem:
        raise X.StagingError(f"revocation {match[0]['executor']!r} is NOT settled: {problem}")
    problem = _held(record, guard)
    if problem:
        raise X.StagingError(f"the recovery was lost ({problem}): nothing was recorded")
    _append(record, "revocation-settled", dict(json.loads(json.dumps(settlement)), lock_sha256=guard.lock_sha256),
            clock)


def release_needs_proof(record, lock_sha256):
    """(the revocations settled by a tombstone UNDER THIS stale lock, in order; problem). Releasing this lock then
    rests on the permission being revoked, and that has to be seen by the recovery that releases it. With a problem
    the list means UNKNOWN: receipts that cannot be read do not establish that no proof is needed."""
    found, problem = revocations(record)
    if problem:
        return [], problem
    if not isinstance(lock_sha256, str) or not lock_sha256:
        return [], "the stale lock is not identified"
    return [r["executor"] for r in found if isinstance(r["settlement"], dict)
            and r["settlement"].get("by") == "tombstone" and r["settlement"].get("lock_sha256") == lock_sha256], None


def release_problem(record, guard=None):
    """Why a recovery must NOT release this record's stale evaluation lock on the transition's account, or None.
    Every recovery asks this last (execlib.recover, with its entered guard): resolved intents alone say nothing
    about an executor that is neither finished nor fenced, a revocation that proved nothing, an open emergency, or
    chart records that cannot be read. And when a revocation was settled by a tombstone under THIS lock, the release
    needs what the releasing recovery saw ITSELF (guard.fresh_revocation: the binding absent, a stored proof that
    holds): a settlement recorded by an earlier, interrupted recovery does not carry that over, and a recovery
    without the transition's guard cannot have it."""
    log, problem = _owner_log(record)
    if problem:
        return problem
    waiting = [i for i in log["order"] if i not in log["completed"]]
    if waiting:
        return f"transition executors {waiting} are neither finished nor fenced"
    unresolved, problem = unresolved_revocations(record)
    if problem:
        return problem
    if unresolved:
        return f"revocations {unresolved} did not end in a proven, recorded revocation"
    emergencies, chart = emergency_state(record), chart_state(record)
    if emergencies["problem"]:
        return f"the emergency records cannot be read with certainty ({emergencies['problem']})"
    if emergencies["open"]:
        return f"emergencies {emergencies['open']} are open"
    if chart["problem"]:
        return f"the chart records cannot be read with certainty ({chart['problem']})"
    try:
        lock_sha256 = C.sha256_file(X.lock_file(record))
    except FileNotFoundError:
        if guard is not None:
            return "the stale lock is gone while its recovery is in progress: what is being released is unknown"
        return None                                       # asked outside a recovery, and there is no lock to release
    except OSError as e:
        return (f"the stale lock cannot be read ({type(e).__name__}): whether its release needs a fresh proof is "
                f"unknown")
    needing, problem = release_needs_proof(record, lock_sha256)
    if problem:
        return f"{problem}: whether the release needs a fresh proof is unknown"
    if needing:
        fresh = guard.fresh_revocation if isinstance(guard, recovery) and _held(record, guard) is None else None
        if not isinstance(fresh, dict):
            return (f"revocations {needing} were settled under this stale lock: releasing it needs the binding seen "
                    f"absent and a fresh permission proof in the SAME recovery (transition.py recover)")
        if fresh.get("binding") != "absent":
            return "this recovery did not see the scale binding absent"
        problem = stored_proof_problem(record, fresh.get("proof_sha256"))
        if problem:
            return f"this recovery's permission proof: {problem}"
    return None
