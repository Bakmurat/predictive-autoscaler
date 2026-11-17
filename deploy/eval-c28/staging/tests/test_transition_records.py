"""The durable records of the Active transition (transition_records.py): owner records and emergency files.

Owner records are written by an executor about itself, so those tests run the scenario in a CHILD process that makes
itself a group leader and takes the shared lock, as an executor does; the test process only reads the result.
"""
import json
import os
import socket
import subprocess
import sys
import threading

import pytest

import common_records as C
import execlib as X
import transition_fence as F
import transition_records as R

STAGING = os.path.dirname(os.path.abspath(R.__file__))
CLUSTER = "19b8af73-08bc-4c8e-a233-2a104671f7c3"
BOOT = "11111111-2222-3333-4444-555555555555"
TRUE_FACTS = {"binding_removed": True, "fence_proven": True, "writes_denied": True}
CHILD = """
import json, os, sys
sys.path.insert(0, {staging!r})
import common_records as C, execlib as X, transition_fence as F, transition_records as R
if {leader!r}:
    F.own_group()
record = X.Record(sys.argv[1], json.load(open(sys.argv[1])))
out = {{"pid": os.getpid(), "owner": F.owner()}}
def attempt(name, fn):
    try:
        out[name] = {{"ok": fn()}}
    except X.StagingError as e:
        out[name] = {{"error": str(e)}}
def held():
    return C.exclusive_lock(record.doc, X.LOCK_SCOPE, holder="transition")
def done():
    print(json.dumps(out), flush=True)
{code}
done()
"""


@pytest.fixture
def record(tmp_path):
    doc = {"schema": C.RECORD_SCHEMA, "run_id": "c28-demo-1", "cluster_uid": CLUSTER,
           "kubeconfig": {"path": "/home/ops/.kube/eval-prod.yaml", "context": "prodcluster"},
           "evidence_dir": "evidence", "lock_dir": str(tmp_path / "shared-locks"), "source": {"commit": "a" * 40}}
    path = tmp_path / "preparation.json"
    path.write_text(json.dumps(doc))
    return X.Record(str(path), doc)


def child(record, code, leader=True):
    """Run `code` in a child process set up like an executor; returns what it reported."""
    script = CHILD.format(staging=STAGING, leader=leader, code=code)
    p = subprocess.run([sys.executable, "-c", script, record.path], capture_output=True, text=True, timeout=60)
    assert p.stdout.strip(), p.stderr[-2000:]
    return json.loads(p.stdout.strip().splitlines()[-1])


def owner(pid=4242, start="987654", **over):
    return dict({"host": socket.gethostname(), "boot_id": BOOT, "pid": pid, "pid_start": start, "pgid": pid}, **over)


def start_record(n, operation="grant", activation="reh1", **kw):
    return {"id": f"{operation}-{activation}-{n}", "operation": operation, "activation": activation,
            "owner": owner(pid=1000 + n, start=str(n), **kw)}


def emergency_dir(record):
    return os.path.join(record.evidence_dir, "receipts", R.EMERGENCY_DIR)


def events(record):
    return [e["event"] for e in X.receipts(record, R.HELPER)]


# --- owner records: an executor registers ITSELF ---------------------------------------------------------------------

def test_an_executor_registers_itself_and_only_it_finishes(record):
    out = child(record, """
with held() as lock:
    attempt("started", lambda: R.started(record, "grant", "reh1", lock))
    out["outstanding"] = R.outstanding(record)
    attempt("finished", lambda: R.finished(record, out["started"]["ok"], lock, children_collected=True))
    out["after"] = R.outstanding(record)
""")
    assert out["started"] == {"ok": "grant-reh1-1"} and out["finished"] == {"ok": None}
    (left,), problem = out["outstanding"]
    assert problem is None and left["id"] == "grant-reh1-1" and left["operation"] == "grant"
    # The recorded owner IS the process that ran: its pid leads its own group, and it can be fenced as recorded.
    assert left["owner"] == {k: out["owner"][k] for k in F.FIELDS}
    assert left["owner"]["pid"] == left["owner"]["pgid"] == out["pid"] != os.getpid()
    assert F._problem(left["owner"], 0.0) is None
    assert out["after"] == [[], None] and events(record) == ["executor-started", "executor-finished"]


def test_a_process_that_leads_no_group_cannot_register(record):
    out = child(record, """
with held() as lock:
    attempt("started", lambda: R.started(record, "grant", "reh1", lock))
""", leader=False)
    assert "cannot be fenced" in out["started"]["error"] and events(record) == []


def test_registration_needs_the_shared_lock_held_by_this_process(record):
    out = child(record, """
attempt("none", lambda: R.started(record, "grant", "reh1", None))
released = held()
with released:
    pass
attempt("released", lambda: R.started(record, "grant", "reh1", released))
other = C.exclusive_lock(record.doc, "legacy")
with other:
    attempt("other-scope", lambda: R.started(record, "grant", "reh1", other))
with held() as lock:
    body = json.load(open(lock.path))
    open(lock.path, "w").write(json.dumps(dict(body, pid=body["pid"] + 1)))
    attempt("another-pid", lambda: R.started(record, "grant", "reh1", lock))
    open(lock.path, "w").write(json.dumps(dict(body, pid_start="1")))
    attempt("another-start", lambda: R.started(record, "grant", "reh1", lock))
    open(lock.path, "w").write("[]")
    attempt("not-a-lock-body", lambda: R.started(record, "grant", "reh1", lock))
    open(lock.path, "w").write(json.dumps(body))
    attempt("ok", lambda: R.started(record, "grant", "reh1", lock))
    open(lock.path, "w").write(json.dumps(dict(body, pid=body["pid"] + 1)))
    attempt("finish-another-pid", lambda: R.finished(record, "grant-reh1-1", lock, children_collected=True))
    open(lock.path, "w").write(json.dumps(body))
""")
    assert "not the shared evaluation lock" in out["none"]["error"]
    assert "cannot be read" in out["released"]["error"]
    assert "not the shared evaluation lock" in out["other-scope"]["error"]
    for key in ("another-pid", "another-start", "not-a-lock-body", "finish-another-pid"):
        assert "names another process" in out[key]["error"], key
    assert out["ok"] == {"ok": "grant-reh1-1"} and events(record) == ["executor-started"]


def test_unknown_operations_and_malformed_activations_are_refused(record):
    out = child(record, """
with held() as lock:
    for i, (op, act) in enumerate([("install", "reh1"), ("", "reh1"), ("grant", "Reh1"), ("grant", ""),
                                   ("grant", "reh 1"), ("grant", None)]):
        attempt(str(i), lambda: R.started(record, op, act, lock))
""")
    assert all("error" in out[str(i)] for i in range(6)) and events(record) == []


def test_finished_needs_the_children_collected_and_its_own_open_registration(record):
    out = child(record, """
with held() as lock:
    eid = R.started(record, "grant", "reh1", lock)
    for i, value in enumerate([False, None, 1, "yes"]):
        attempt(f"not-collected-{i}", lambda: R.finished(record, eid, lock, children_collected=value))
    attempt("unknown", lambda: R.finished(record, "grant-reh1-9", lock, children_collected=True))
    attempt("ok", lambda: R.finished(record, eid, lock, children_collected=True))
    attempt("twice", lambda: R.finished(record, eid, lock, children_collected=True))
""")
    assert all("collected" in out[f"not-collected-{i}"]["error"] for i in range(4))
    assert "never started" in out["unknown"]["error"] and out["ok"] == {"ok": None}
    assert "already completed" in out["twice"]["error"]
    assert events(record) == ["executor-started", "executor-finished"]


def test_another_process_cannot_finish_an_executors_registration(record):
    # Finding 3: a lock holder could register an owner that was not itself, and finish someone else's registration.
    first = child(record, """
with held() as lock:
    attempt("started", lambda: R.started(record, "grant", "reh1", lock))
""")                                                    # exits cleanly WITHOUT finishing
    second = child(record, """
with held() as lock:
    attempt("finish-other", lambda: R.finished(record, "grant-reh1-1", lock, children_collected=True))
    attempt("started", lambda: R.started(record, "activate", "reh1", lock))
    out["outstanding"] = [d["id"] for d in R.outstanding(record)[0]]
""")
    assert first["pid"] != second["pid"]
    assert "started by another process" in second["finish-other"]["error"]
    assert second["started"] == {"ok": "activate-reh1-2"}                 # ids never repeat
    assert second["outstanding"] == ["grant-reh1-1", "activate-reh1-2"]   # the OLDER one is still outstanding
    owners = [e["data"]["owner"]["pid"] for e in X.receipts(record, R.HELPER)]
    assert owners == [first["pid"], second["pid"]]                        # each recorded ITSELF
    assert "owner" not in R.started.__code__.co_varnames[:R.started.__code__.co_argcount]   # nothing to pass in


def dead_holder(record):
    """An executor that registered and then died while holding the lock: (its report, the stale lock's sha256)."""
    out = child(record, """
lock = held()
lock.__enter__()
attempt("started", lambda: R.started(record, "grant", "reh1", lock))
done()
os._exit(0)
""")
    assert out["started"] == {"ok": "grant-reh1-1"} and os.path.exists(X.lock_file(record))
    return out, C.sha256_file(X.lock_file(record))


def test_a_recovery_completes_a_dead_executor_only_with_a_fence_proof(record):
    out, sha = dead_holder(record)
    assert [d["id"] for d in R.outstanding(record)[0]] == ["grant-reh1-1"]
    for not_ours in (None, "guard", C.exclusive_lock(record.doc, X.LOCK_SCOPE),
                     C.recovery_guard(record.doc, X.LOCK_SCOPE, sha)):
        with pytest.raises(X.StagingError, match="not a recovery of this record"):
            R.fenced(record, "grant-reh1-1", not_ours)
    with pytest.raises(X.StagingError, match="was not entered"):
        R.fenced(record, "grant-reh1-1", R.recovery(record, sha))         # constructed, never entered
    with R.recovery(record, sha) as guard:
        with pytest.raises(X.StagingError, match="never started"):
            R.fenced(record, "grant-reh1-7", guard)
        seen = []

        def unproven(owner_record):
            seen.append(owner_record)
            return {"proven": False, "why": "the processes cannot be observed", "signalled": False}
        with pytest.raises(X.StagingError, match="NOT fenced: the processes cannot be observed"):
            R.fenced(record, "grant-reh1-1", guard, fence=unproven)
        assert seen == [{k: out["owner"][k] for k in F.FIELDS}]           # the RECORDED owner was fenced
        for junk in (None, {}, {"proven": "yes"}, {"proven": 1}):
            with pytest.raises(X.StagingError, match="NOT fenced"):
                R.fenced(record, "grant-reh1-1", guard, fence=lambda _o: junk)
        assert events(record) == ["executor-started"]                     # nothing was written by the refusals
        result = R.fenced(record, "grant-reh1-1", guard)                  # the real fence: the process is gone
        assert result["proven"] is True
        with pytest.raises(X.StagingError, match="already completed"):
            R.fenced(record, "grant-reh1-1", guard)
    assert events(record) == ["executor-started", "executor-fenced"]
    assert X.receipts(record, R.HELPER)[-1]["data"] == {"id": "grant-reh1-1", "fence": result}
    assert R.outstanding(record) == ([], None)


# --- the owner log is read as a sequence ---------------------------------------------------------------------------

def write(record, *entries):
    for event, data in entries:
        X.receipt(record, R.HELPER, event, data)


def test_every_outstanding_executor_is_returned_in_order_and_other_receipts_are_passed_over(record):
    write(record, ("executor-started", start_record(1)), ("intent-1", {"op": "delete"}),
          ("executor-started", start_record(2, "activate")), ("step-1", {"result": "applied"}),
          ("executor-started", start_record(3)), ("executor-finished", {"id": "activate-reh1-2"}))
    left, problem = R.outstanding(record)
    assert problem is None and [d["id"] for d in left] == ["grant-reh1-1", "grant-reh1-3"]
    write(record, ("executor-fenced", {"id": "grant-reh1-1", "fence": {"proven": True, "why": "x", "signalled": True}}))
    assert [d["id"] for d in R.outstanding(record)[0]] == ["grant-reh1-3"]


@pytest.mark.parametrize("name,entries", [
    ("a finish before its start", [("executor-finished", {"id": "grant-reh1-1"}),
                                   ("executor-started", start_record(1))]),
    ("a finish without any start", [("executor-finished", {"id": "grant-reh1-1"})]),
    ("a fence before its start", [("executor-fenced", {"id": "grant-reh1-1", "fence": {"proven": True}}),
                                  ("executor-started", start_record(1))]),
    ("two completions", [("executor-started", start_record(1)), ("executor-finished", {"id": "grant-reh1-1"}),
                         ("executor-finished", {"id": "grant-reh1-1"})]),
    ("finished then fenced", [("executor-started", start_record(1)), ("executor-finished", {"id": "grant-reh1-1"}),
                              ("executor-fenced", {"id": "grant-reh1-1", "fence": {"proven": True}})]),
    ("a repeated start id", [("executor-started", start_record(1)), ("executor-started", start_record(1))]),
    ("an id that is not the next one", [("executor-started", start_record(2))]),
    ("an id that does not match its operation", [("executor-started", dict(start_record(1), operation="revoke"))]),
    ("an unknown operation", [("executor-started", dict(start_record(1, "install")))]),
    ("a malformed activation", [("executor-started", dict(start_record(1), activation="Reh 1"))]),
    ("no owner", [("executor-started", {k: v for k, v in start_record(1).items() if k != "owner"})]),
    ("an owner that cannot be fenced", [("executor-started", start_record(1, boot_id="x"))]),
    ("an owner whose pid leads no group", [("executor-started", start_record(1, pgid=77))]),
    ("an owner with an extra field", [("executor-started", dict(start_record(1), owner=owner(extra=1)))]),
    ("no id", [("executor-started", {"operation": "grant"})]),
    ("data that is not an object", [("executor-finished", ["grant-reh1-1"])]),
    ("a fence that is not proven", [("executor-started", start_record(1)),
                                    ("executor-fenced", {"id": "grant-reh1-1", "fence": {"proven": False}})]),
    ("a fence without its result", [("executor-started", start_record(1)), ("executor-fenced", {"id": "grant-reh1-1"})]),
])
def test_an_owner_log_out_of_order_or_out_of_form_is_a_problem_not_an_empty_list(record, name, entries):
    # Finding 4: a correctly chained finish-before-start log returned "no outstanding executor".
    write(record, *entries)
    left, problem = R.outstanding(record)
    assert left == [] and problem, name
    out = child(record, """
with held() as lock:
    attempt("started", lambda: R.started(record, "grant", "reh1", lock))
""")
    assert "error" in out["started"], name                                # and nothing registers on top of it


def test_a_log_that_cannot_be_read_in_full_is_unknown_not_empty(record):
    write(record, ("executor-started", start_record(1)))
    path = C.receipts_path(record.doc, R.HELPER)
    good = open(path).read()
    for name, text in (("a torn last line", good + '{"event": "executor-fin'),
                       ("a broken chain", good.replace('"grant-reh1-1"', '"grant-reh1-7"')),
                       ("not JSON", "garbage\n")):
        open(path, "w").write(text)
        left, problem = R.outstanding(record)
        assert left == [] and problem and "cannot be read" in problem, name
    open(path, "w").write(good)
    assert R.outstanding(record)[1] is None


# --- emergency records -------------------------------------------------------------------------------------------------

def test_no_emergency_is_a_clean_state(record):
    assert R.emergency_state(record) == {"requests": [], "open": [], "last": 0, "tombstones": [], "problem": None}
    R.refuse_if_emergency_open(record)
    R.refuse_dead_grant(record, 0)


def test_an_emergency_is_open_until_its_tombstone_and_only_a_tombstone_closes_it(record):
    seq = R.emergency_request(record, "operator asked")
    assert seq == 1 and os.listdir(emergency_dir(record)) == ["000001-requested.json"]
    assert R.emergency_state(record) == {"requests": [1], "open": [1], "last": 1, "tombstones": [], "problem": None}
    with pytest.raises(X.StagingError, match="not closed by a tombstone"):
        R.refuse_if_emergency_open(record)
    R.emergency_close(record, seq, "fence-unproven", {"binding_removed": True, "fence_proven": False})
    R.emergency_close(record, seq, "not-revoked", {"foreign_grant": "demo/other"})
    assert R.emergency_state(record)["open"] == [1]                       # still open: neither is a tombstone
    R.emergency_close(record, seq, "revoked", TRUE_FACTS)
    assert R.emergency_state(record) == {"requests": [1], "open": [], "last": 1, "tombstones": [1], "problem": None}
    R.refuse_if_emergency_open(record)
    doc = json.load(open(os.path.join(emergency_dir(record), "000001-requested.json")))
    assert set(doc) == {"schema", "run_id", "seq", "event", "at", "reason"} and doc["seq"] == 1


def put(record, name, content):
    os.makedirs(emergency_dir(record), exist_ok=True)
    with open(os.path.join(emergency_dir(record), name), "w") as fh:
        fh.write(content if isinstance(content, str) else json.dumps(content))


def tombstone(seq=1, **over):
    return dict({"schema": R.SCHEMA, "run_id": "c28-demo-1", "seq": seq, "event": "revoked",
                 "at": "2026-11-16T23:00:00Z", "facts": dict(TRUE_FACTS)}, **over)


@pytest.mark.parametrize("name,content", [
    ("empty (created, not yet written)", ""),
    ("partial", '{"schema": "c28-transition-emergency/v1", "run_id": "c28-de'),
    ("not an object", "[]"),
    ("another schema", tombstone(schema="something/v1")),
    ("another run", tombstone(run_id="c28-demo-2")),
    ("another sequence", tombstone(seq=2)),
    ("a boolean sequence", tombstone(seq=True)),
    ("another event", tombstone(event="fence-unproven")),
    ("no time", tombstone(at="")),
    ("no facts", {k: v for k, v in tombstone().items() if k != "facts"}),
    ("facts not an object", tombstone(facts=[True, True, True])),
    ("a fact missing", tombstone(facts={"binding_removed": True, "fence_proven": True})),
    ("a fact false", tombstone(facts=dict(TRUE_FACTS, fence_proven=False))),
    ("a fact merely truthy", tombstone(facts=dict(TRUE_FACTS, writes_denied="yes"))),
])
def test_a_tombstone_file_closes_nothing_unless_its_content_is_a_tombstone(record, name, content):
    # Finding 1: an empty 000001-revoked.json gave open=[] and problem=None, and operations were permitted.
    assert R.emergency_request(record, "stop") == 1
    put(record, "000001-revoked.json", content)
    state = R.emergency_state(record)
    assert state["open"] == [1] and state["tombstones"] == [] and state["problem"], name
    with pytest.raises(X.StagingError, match="refusing"):
        R.refuse_if_emergency_open(record)
    with pytest.raises(X.StagingError, match="refusing"):
        R.refuse_dead_grant(record, 1)


@pytest.mark.parametrize("content", ["", "{", "[]", {"schema": "x"},
                                     {"schema": R.SCHEMA, "run_id": "c28-demo-2", "seq": 1, "event": "requested",
                                      "at": "t", "reason": "r"},
                                     {"schema": R.SCHEMA, "run_id": "c28-demo-1", "seq": 1, "event": "requested",
                                      "at": "t"}])
def test_no_outcome_is_attached_to_a_request_that_is_not_valid(record, content):
    put(record, "000001-requested.json", content)
    with pytest.raises(X.StagingError, match="request is not valid"):
        R.emergency_close(record, 1, "revoked", TRUE_FACTS)
    state = R.emergency_state(record)
    assert state["requests"] == [] and state["tombstones"] == [] and state["problem"]
    assert sorted(os.listdir(emergency_dir(record))) == ["000001-requested.json"]


@pytest.mark.parametrize("orphan", ["000001-revoked.json", "000003-fence-unproven.json", "000007-notes.txt",
                                    "000005-requested.json.bak"])
def test_a_new_request_never_attaches_itself_to_an_existing_outcome(record, orphan):
    # Finding 2: with an orphan 000001-revoked.json, the next request became emergency 1 and was at once "closed".
    put(record, orphan, tombstone(seq=int(orphan[:6])) if "revoked" in orphan else "{}")
    claimed = int(orphan[:6])
    seq = R.emergency_request(record, "stop")
    assert seq == claimed + 1                                             # above EVERY number any file claims
    state = R.emergency_state(record)
    assert seq in state["open"] and seq not in state["tombstones"] and state["problem"]
    with pytest.raises(X.StagingError, match="refusing"):
        R.refuse_if_emergency_open(record)


@pytest.mark.parametrize("facts", [
    {}, None, {"binding_removed": True, "fence_proven": True}, {"binding_removed": True, "writes_denied": True},
    {"fence_proven": True, "writes_denied": True}, dict(TRUE_FACTS, fence_proven=False),
    dict(TRUE_FACTS, fence_proven="yes"), dict(TRUE_FACTS, binding_removed=1),
])
def test_a_tombstone_needs_all_three_facts_stated_true(record, facts):
    seq = R.emergency_request(record, "x")
    with pytest.raises(X.StagingError, match="tombstone needs|needs its facts"):
        R.emergency_close(record, seq, "revoked", facts)
    assert R.emergency_state(record)["open"] == [seq]


def test_outcomes_are_written_once_for_a_requested_emergency_only(record):
    with pytest.raises(X.StagingError, match="never requested"):
        R.emergency_close(record, 1, "fence-unproven", {})
    seq = R.emergency_request(record, "x")
    R.emergency_close(record, seq, "fence-unproven", {})
    with pytest.raises(X.StagingError, match="already has"):
        R.emergency_close(record, seq, "fence-unproven", {})
    for bad_seq in (0, -1, True, "1", None, 1_000_000):
        with pytest.raises(X.StagingError):
            R.emergency_close(record, bad_seq, "fence-unproven", {})
    with pytest.raises(X.StagingError, match="unknown emergency outcome"):
        R.emergency_close(record, seq, "done", {})


def test_sequence_numbers_are_taken_exclusively_also_by_many_at_once(record):
    assert [R.emergency_request(record, "a"), R.emergency_request(record, "b")] == [1, 2]
    got, errors = [], []

    def ask(i):
        try:
            got.append(R.emergency_request(record, f"parallel {i}"))
        except Exception as e:                                           # noqa: BLE001
            errors.append(e)
    threads = [threading.Thread(target=ask, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [] and sorted(got) == list(range(3, 19))
    state = R.emergency_state(record)
    assert state["requests"] == list(range(1, 19)) and state["problem"] is None
    assert not [n for n in os.listdir(emergency_dir(record)) if n.startswith(".tmp-")]   # nothing left aside


def test_many_processes_requesting_at_once_each_get_their_own_number(record):
    procs = [subprocess.Popen([sys.executable, "-c", CHILD.format(staging=STAGING, leader=False, code="""
out["seq"] = [R.emergency_request(record, "process") for _ in range(5)]
"""), record.path], stdout=subprocess.PIPE, text=True) for _ in range(6)]
    seqs = []
    for p in procs:
        text, _ = p.communicate(timeout=60)
        seqs += json.loads(text.strip().splitlines()[-1])["seq"]
    assert sorted(seqs) == list(range(1, 31))
    state = R.emergency_state(record)
    assert state["requests"] == list(range(1, 31)) and state["problem"] is None


def test_a_file_appears_under_its_name_only_with_its_whole_content(record, monkeypatch):
    # The link is the last step: while the content is being written the final name does not exist.
    seen = {}
    real_link = os.link

    def link(src, dst):
        seen["names_before"] = sorted(os.listdir(emergency_dir(record)))
        seen["content"] = json.load(open(src))
        return real_link(src, dst)
    monkeypatch.setattr(R.os, "link", link)
    R.emergency_request(record, "stop")
    monkeypatch.undo()
    assert len(seen["names_before"]) == 1 and seen["names_before"][0].startswith(".tmp-")
    assert seen["content"]["event"] == "requested" and seen["content"]["seq"] == 1
    assert os.listdir(emergency_dir(record)) == ["000001-requested.json"]


def test_a_write_left_aside_is_a_problem_and_closes_nothing(record):
    R.emergency_request(record, "stop")
    put(record, ".tmp-123-abcdef", "{")                                   # an interrupted write
    state = R.emergency_state(record)
    assert state["open"] == [1] and "unexpected file" in state["problem"]
    assert R.emergency_request(record, "again") == 2                      # a request is still possible


@pytest.mark.parametrize("make", ["stray-file", "outcome-without-request", "directory-is-a-file"])
def test_records_that_cannot_be_read_with_certainty_refuse_every_operation(record, make):
    os.makedirs(os.path.dirname(emergency_dir(record)), exist_ok=True)
    if make == "directory-is-a-file":
        open(emergency_dir(record), "w").write("x")
    else:
        put(record, "readme.md" if make == "stray-file" else "000001-revoked.json",
            "{}" if make == "stray-file" else tombstone())
    state = R.emergency_state(record)
    assert state["problem"] and state["tombstones"] == []
    with pytest.raises(X.StagingError, match="refusing"):
        R.refuse_if_emergency_open(record)
    with pytest.raises(X.StagingError, match="refusing"):
        R.refuse_dead_grant(record, 0)
    if make == "directory-is-a-file":
        with pytest.raises(X.StagingError, match="NOT recorded"):         # it says so; it does not pretend
            R.emergency_request(record, "stop")


def test_a_grant_plan_made_before_an_emergency_is_dead(record):
    plan_seq = R.emergency_state(record)["last"]
    R.refuse_dead_grant(record, plan_seq)                                 # nothing happened: the plan lives
    seq = R.emergency_request(record, "stop")
    with pytest.raises(X.StagingError, match="not closed"):
        R.refuse_dead_grant(record, plan_seq)                             # an emergency is open
    R.emergency_close(record, seq, "revoked", TRUE_FACTS)
    with pytest.raises(X.StagingError, match="dead"):
        R.refuse_dead_grant(record, plan_seq)                             # closed, but the plan is older than it
    R.refuse_dead_grant(record, R.emergency_state(record)["last"])        # a plan made after the tombstone lives
    for bad in (None, -1, True, "0"):
        with pytest.raises(X.StagingError, match="does not name"):
            R.refuse_dead_grant(record, bad)
    with pytest.raises(X.StagingError, match="dead"):
        R.refuse_dead_grant(record, 99)                                   # a plan naming a sequence that never was


# --- durability of the directory entries (finding 5) -----------------------------------------------------------------

def test_new_directories_and_files_are_linked_durably_up_to_the_evidence_directorys_parent(record, monkeypatch):
    synced = []
    monkeypatch.setattr(R, "_sync_dir", lambda path: synced.append(path))
    evidence = record.evidence_dir
    R.emergency_request(record, "stop")                                   # creates evidence/receipts/transition-emergency
    assert synced == [emergency_dir(record), os.path.join(evidence, "receipts"), evidence, os.path.dirname(evidence)]
    del synced[:]
    R._append(record, "executor-started", start_record(1), None)          # creates the owner log file
    assert synced == [os.path.join(evidence, "receipts"), evidence, os.path.dirname(evidence)]
    with pytest.raises(X.StagingError, match="not under"):
        R._durable_entry("/tmp/elsewhere/file", evidence)


def test_the_directory_sync_really_syncs_a_directory(tmp_path, monkeypatch):
    calls = []
    real = os.fsync
    monkeypatch.setattr(R.os, "fsync", lambda fd: (calls.append(os.fstat(fd).st_ino), real(fd))[1])
    R._sync_dir(str(tmp_path))
    monkeypatch.undo()
    assert calls == [os.stat(tmp_path).st_ino]


# --- second code review: three places where uncertainty still read as "all clear" -----------------------------------

def test_a_guard_file_that_exists_is_not_a_recovery_this_process_holds(record):
    # Finding 1: while a real recovery held the guard, a second, UNENTERED guard object with a wrong lock digest was
    # accepted, because only the guard FILE's existence was checked.
    out, sha = dead_holder(record)
    with R.recovery(record, sha) as real:
        assert real.problem() is None
        for impostor in (R.recovery(record, "0" * 64), R.recovery(record, sha),
                         C.recovery_guard(record.doc, X.LOCK_SCOPE, "0" * 64)):
            with pytest.raises(X.StagingError, match="was not entered|not a recovery of this record"):
                R.fenced(record, "grant-reh1-1", impostor)
        with pytest.raises(X.StagingError, match="another recovery is in progress"):
            R.recovery(record, sha).__enter__()                           # a second recoverer cannot enter at all
        assert events(record) == ["executor-started"]
        assert json.load(open(X.lock_file(record) + ".recovery"))["pid"] == os.getpid()
    with pytest.raises(X.StagingError, match="was not entered"):
        R.fenced(record, "grant-reh1-1", real)                            # left: it holds nothing any more
    assert events(record) == ["executor-started"]


def test_a_recovery_of_another_record_or_another_lock_is_refused(record, tmp_path):
    out, sha = dead_holder(record)
    other_doc = dict(json.load(open(record.path)), run_id="c28-demo-2")
    other_path = tmp_path / "other.json"
    other_path.write_text(json.dumps(other_doc))
    other = X.Record(str(other_path), other_doc)
    with R.recovery(record, sha) as real:
        real.record = other                                               # an object bound to another record
        with pytest.raises(X.StagingError, match="not a recovery of this record"):
            R.fenced(record, "grant-reh1-1", real)
        real.record = record
        assert R.fenced(record, "grant-reh1-1", real, fence=lambda _o: {"proven": True, "why": "t", "signalled": False})


@pytest.mark.parametrize("loss", ["another-token", "guard-gone", "guard-garbage", "another-pid", "lock-changed",
                                  "lock-gone"])
def test_a_recovery_lost_while_fencing_records_nothing(record, loss):
    # The fence takes time. What was true before it must still be true at the append: one chain, one writer.
    out, sha = dead_holder(record)
    lock_path = X.lock_file(record)
    guard_path = lock_path + ".recovery"
    with R.recovery(record, sha) as real:
        saved_guard, saved_lock = open(guard_path).read(), open(lock_path).read()

        def fence_then_lose(_owner):
            body = json.loads(saved_guard)
            if loss == "another-token":
                open(guard_path, "w").write(json.dumps(dict(body, token="f" * 32)))
            elif loss == "guard-gone":
                os.unlink(guard_path)
            elif loss == "guard-garbage":
                open(guard_path, "w").write("{")
            elif loss == "another-pid":
                open(guard_path, "w").write(json.dumps(dict(body, pid=body["pid"] + 1)))
            elif loss == "lock-changed":
                open(lock_path, "w").write(saved_lock + " ")
            else:
                os.unlink(lock_path)
            return {"proven": True, "why": "no process is left in the owner's group", "signalled": False}
        with pytest.raises(X.StagingError, match="lost while fencing"):
            R.fenced(record, "grant-reh1-1", real, fence=fence_then_lose)
        assert events(record) == ["executor-started"]                     # the proven result was NOT recorded
        open(guard_path, "w").write(saved_guard)                          # put back so the guard can leave cleanly
        open(lock_path, "w").write(saved_lock)
        assert real.problem() is None


def test_an_owner_log_that_cannot_be_reached_is_unknown_not_empty(record):
    # Finding 2: the shared reader asks os.path.exists(), which says "no" for a path it may not enter; with the
    # receipts directory unreadable the log came back as ([], None) although an executor was outstanding.
    write(record, ("executor-started", start_record(1)))
    receipts_dir = os.path.dirname(C.receipts_path(record.doc, R.HELPER))
    assert [d["id"] for d in R.outstanding(record)[0]] == ["grant-reh1-1"]
    if os.geteuid() == 0:
        pytest.skip("permissions are not enforced for root; the next test covers the same path without them")
    os.chmod(receipts_dir, 0)
    try:
        assert not os.path.exists(C.receipts_path(record.doc, R.HELPER))  # what the shared reader would conclude
        left, problem = R.outstanding(record)
        assert left == [] and problem and "cannot be read (PermissionError)" in problem
    finally:
        os.chmod(receipts_dir, 0o750)
    assert [d["id"] for d in R.outstanding(record)[0]] == ["grant-reh1-1"]


@pytest.mark.parametrize("error", [PermissionError(13, "denied"), OSError(5, "Input/output error"),
                                   NotADirectoryError(20, "not a directory")])
def test_any_failure_to_look_at_the_owner_log_is_a_problem(record, monkeypatch, error):
    write(record, ("executor-started", start_record(1)))
    real = os.stat

    def stat(path, *a, **k):
        if str(path).endswith(f"{R.HELPER}.jsonl"):
            raise error
        return real(path, *a, **k)
    monkeypatch.setattr(R.os, "stat", stat)
    left, problem = R.outstanding(record)
    monkeypatch.undo()
    assert left == [] and problem and "cannot be read" in problem


def test_a_log_that_is_not_empty_but_reads_as_empty_is_a_problem(record, monkeypatch):
    write(record, ("executor-started", start_record(1)))
    monkeypatch.setattr(X, "receipts", lambda _record, _helper: [])      # what the shared reader returned in finding 2
    left, problem = R.outstanding(record)
    assert left == [] and "read as empty" in problem


def test_confirmed_absence_of_the_owner_log_is_an_empty_log(record):
    assert not os.path.exists(record.evidence_dir)                        # nothing was ever written
    assert R.outstanding(record) == ([], None)
    os.makedirs(os.path.dirname(C.receipts_path(record.doc, R.HELPER)))
    assert R.outstanding(record) == ([], None)                            # the directory is there, the file is not


def snapshot(**over):
    return dict({"requests": [], "open": [], "last": 0, "tombstones": [], "problem": None}, **over)


def test_a_grant_is_judged_from_one_reading_of_the_emergency_records(record, monkeypatch):
    # Finding 3: a clean first read followed by an unreadable second read (last=0) let a plan made at 0 through.
    for states in ([snapshot(problem="the emergency records cannot be listed (PermissionError)")],
                   [snapshot(open=[1], requests=[1], last=1)],
                   [snapshot(last=3)],
                   [snapshot(problem="x", last=0), snapshot()],           # whatever a later read would say
                   [snapshot(open=[2], last=0), snapshot()]):
        served = []

        def state(_record, states=states, served=served):
            served.append(1)
            return states[min(len(served), len(states)) - 1]
        monkeypatch.setattr(R, "emergency_state", state)
        with pytest.raises(X.StagingError):
            R.refuse_dead_grant(record, 0)
        assert len(served) == 1, states                                   # ONE reading decided
    served = []
    monkeypatch.setattr(R, "emergency_state", lambda _r: (served.append(1), snapshot(last=4, tombstones=[4]))[1])
    R.refuse_dead_grant(record, 4)
    assert len(served) == 1


def test_nothing_is_fenced_for_a_caller_that_does_not_hold_the_recovery(record):
    # The check before the fence matters on its own: the fence SIGKILLs a process group. Without it an impostor was
    # still refused at the append (so the tests above passed), but only AFTER the fence had run.
    out, sha = dead_holder(record)

    def must_not_run(_owner):
        raise AssertionError("the fence ran for a caller that does not hold the recovery")
    with R.recovery(record, sha) as real:
        for impostor in (None, R.recovery(record, sha), R.recovery(record, "0" * 64),
                         C.recovery_guard(record.doc, X.LOCK_SCOPE, sha)):
            with pytest.raises(X.StagingError, match="under a recovery held by this process"):
                R.fenced(record, "grant-reh1-1", impostor, fence=must_not_run)
    with pytest.raises(X.StagingError, match="under a recovery held by this process"):
        R.fenced(record, "grant-reh1-1", real, fence=must_not_run)        # left: no longer held
    assert events(record) == ["executor-started"]


# --- third code review: release and cleanup obey the same ownership rule as fenced() ---------------------------------

def both_files(record):
    lock = X.lock_file(record)
    return open(lock).read(), open(lock + ".recovery").read()


def test_only_the_recovery_that_holds_the_lock_may_release_it(record):
    # Finding 1: release_stale() on a separate, unentered object deleted the stale evaluation lock.
    out, sha = dead_holder(record)
    earlier = R.recovery(record, sha)
    with earlier:
        pass                                                              # entered and left: holds nothing now
    with R.recovery(record, sha) as real:
        before = both_files(record)
        for name, impostor in (("unentered", R.recovery(record, sha)), ("unentered, wrong digest",
                               R.recovery(record, "0" * 64)), ("exited", earlier)):
            with pytest.raises(X.StagingError, match="released only by the recovery that holds it"):
                impostor.release_stale()
            assert both_files(record) == before, name                     # both files intact
        # Ownership lost by the entered object itself: another token, another pid, the lock changed.
        lock, guard = X.lock_file(record), X.lock_file(record) + ".recovery"
        body = json.loads(before[1])
        for name, path, text in (("another token", guard, json.dumps(dict(body, token="f" * 32))),
                                 ("another pid", guard, json.dumps(dict(body, pid=body["pid"] + 1))),
                                 ("garbage", guard, "{"),
                                 ("the lock changed", lock, before[0] + " ")):
            open(path, "w").write(text)
            with pytest.raises(X.StagingError, match="released only by the recovery that holds it"):
                real.release_stale()
            assert os.path.exists(lock) and os.path.exists(guard), name   # nothing was deleted
            open(lock, "w").write(before[0])
            open(guard, "w").write(before[1])
        assert real.problem() is None and both_files(record) == before


def test_the_holder_releases_the_lock_and_then_removes_its_own_guard(record):
    out, sha = dead_holder(record)
    lock, guard = X.lock_file(record), X.lock_file(record) + ".recovery"
    with R.recovery(record, sha) as real:
        assert R.fenced(record, "grant-reh1-1", real)["proven"] is True
        real.release_stale()
        assert not os.path.exists(lock) and os.path.exists(guard)         # the lock is gone, legitimately
        assert real.problem() is not None                                 # ...so it no longer holds a stale lock
    assert not os.path.exists(guard)                                      # and its own guard was still removed
    assert events(record) == ["executor-started", "executor-fenced"]


@pytest.mark.parametrize("change", ["another-token", "another-pid", "another-start", "garbage", "empty"])
def test_leaving_does_not_remove_a_guard_that_is_not_its_own(record, change):
    # Finding 2: after the guard's token was replaced, problem() reported another owner, yet __exit__ unlinked it.
    out, sha = dead_holder(record)
    guard = X.lock_file(record) + ".recovery"
    real = R.recovery(record, sha)
    real.__enter__()
    body = json.loads(open(guard).read())
    foreign = {"another-token": json.dumps(dict(body, token="f" * 32)),
               "another-pid": json.dumps(dict(body, pid=body["pid"] + 1)),
               "another-start": json.dumps(dict(body, pid_start="1")), "garbage": "{", "empty": ""}[change]
    open(guard, "w").write(foreign)
    with pytest.raises(X.StagingError, match="recovery guard was not removed"):
        real.__exit__(None, None, None)
    assert open(guard).read() == foreign                                  # preserved exactly
    assert os.path.exists(X.lock_file(record))
    with pytest.raises(X.StagingError, match="was not entered"):
        R.fenced(record, "grant-reh1-1", real)


def test_leaving_with_an_error_in_flight_keeps_the_error_and_the_foreign_guard(record):
    out, sha = dead_holder(record)
    guard = X.lock_file(record) + ".recovery"
    foreign = json.dumps({"pid": 1, "pid_start": "1", "token": "f" * 32})
    with pytest.raises(RuntimeError, match="the original failure"):
        with R.recovery(record, sha):
            open(guard, "w").write(foreign)
            raise RuntimeError("the original failure")
    assert open(guard).read() == foreign


def test_leaving_when_the_guard_is_gone_or_was_never_entered(record):
    out, sha = dead_holder(record)
    guard = X.lock_file(record) + ".recovery"
    real = R.recovery(record, sha)
    real.__enter__()
    os.unlink(guard)                                                      # removed by someone else
    with pytest.raises(X.StagingError, match="recovery guard was not removed"):
        real.__exit__(None, None, None)
    never = R.recovery(record, sha)
    with pytest.raises(X.StagingError, match="was not entered"):
        never.__exit__(None, None, None)
    assert os.path.exists(X.lock_file(record)) and not os.path.exists(guard)
    with R.recovery(record, sha):                                         # a real recovery can still be entered
        assert os.path.exists(guard)
    assert not os.path.exists(guard)


# --- the transition's identity ---------------------------------------------------------------------------------------

IDENT = {"release": "pa-eval", "control_namespace": "pa-eval-system", "target_namespace": "demo",
         "targets": ["c28-demo-pa-r", "c28-demo-pa-n"], "decision_interval_seconds": 60,
         "cadence_source": "params.demonstration.cadence.update_interval_seconds"}
OPERATOR = "system:serviceaccount:pa-eval-system:pa-eval-predictive-autoscaler-operator"


def identity_path(record):
    return os.path.join(record.evidence_dir, R.IDENTITY_FILE)


def test_the_identity_is_written_once_bound_to_the_run_and_read_back(record):
    doc = R.identity_write(record, **IDENT)
    assert doc == {"schema": R.IDENTITY_SCHEMA, "run_id": "c28-demo-1", "cluster_uid": CLUSTER, "release": "pa-eval",
                   "control_namespace": "pa-eval-system", "target_namespace": "demo",
                   "targets": ["c28-demo-pa-n", "c28-demo-pa-r"], "operator_identity": OPERATOR,
                   "decision_interval_seconds": 60, "cadence_source": IDENT["cadence_source"]}
    assert json.load(open(identity_path(record))) == doc
    assert R.identity_read(record) == ({"release": "pa-eval", "control_namespace": "pa-eval-system",
                                        "target_namespace": "demo", "targets": ["c28-demo-pa-n", "c28-demo-pa-r"],
                                        "operator_identity": OPERATOR, "decision_interval_seconds": 60,
                                        "cadence_source": IDENT["cadence_source"]}, [])
    before = open(identity_path(record), "rb").read()
    assert R.identity_write(record, **dict(IDENT, targets=["c28-demo-pa-n", "c28-demo-pa-r"])) == doc   # the same again
    for change in ({"release": "other"}, {"control_namespace": "x"}, {"target_namespace": "y"},
                   {"targets": ["c28-demo-pa-n"]}, {"decision_interval_seconds": 30}, {"cadence_source": "other"}):
        with pytest.raises(X.StagingError, match="DIFFERENT transition identity"):
            R.identity_write(record, **dict(IDENT, **change))
    assert open(identity_path(record), "rb").read() == before
    assert not [n for n in os.listdir(record.evidence_dir) if n.startswith(".tmp-")]


@pytest.mark.parametrize("change", [{"release": ""}, {"release": None}, {"release": "a/b"}, {"release": "Upper"},
                                    {"control_namespace": " x"}, {"target_namespace": 3}, {"targets": []},
                                    {"targets": None}, {"targets": "c28-demo-pa-n"}, {"targets": ["a", "a"]},
                                    {"targets": ["a", ""]}, {"targets": ["a", 1]},
                                    {"decision_interval_seconds": 0}, {"decision_interval_seconds": -60},
                                    {"decision_interval_seconds": 60.0}, {"decision_interval_seconds": True},
                                    {"decision_interval_seconds": "60"}, {"decision_interval_seconds": None},
                                    {"decision_interval_seconds": 3601}, {"cadence_source": ""},
                                    {"cadence_source": None}])
def test_an_incomplete_identity_is_not_written(record, change):
    with pytest.raises(X.StagingError, match="transition identity needs"):
        R.identity_write(record, **dict(IDENT, **change))
    assert not os.path.exists(identity_path(record))


def test_no_identity_reads_as_nothing_known(record):
    fields, problems = R.identity_read(record)
    assert fields == dict.fromkeys(R.IDENTITY_FIELDS) and problems == [
        "the transition identity cannot be read (FileNotFoundError)"]


@pytest.mark.parametrize("change", [{"run_id": "another-run"}, {"cluster_uid": "another"}, {"schema": "x"}])
def test_an_identity_of_another_run_or_cluster_gives_nothing(record, change):
    doc = R.identity_write(record, **IDENT)
    os.chmod(identity_path(record), 0o640)
    open(identity_path(record), "w").write(json.dumps(dict(doc, **change)))
    fields, problems = R.identity_read(record)
    assert fields == dict.fromkeys(R.IDENTITY_FIELDS) and problems == [
        "the transition identity is not this run's on this cluster"]


@pytest.mark.parametrize("text", ["", "{", "[]", "null", '"x"'])
def test_an_unreadable_identity_gives_nothing(record, text):
    R.identity_write(record, **IDENT)
    open(identity_path(record), "w").write(text)
    fields, problems = R.identity_read(record)
    assert fields == dict.fromkeys(R.IDENTITY_FIELDS) and len(problems) == 1


@pytest.mark.parametrize("change,lost", [
    ({"targets": None}, {"targets"}), ({"targets": []}, {"targets"}), ({"targets": ["a", "a"]}, {"targets"}),
    ({"targets": "a"}, {"targets"}), ({"targets": ["a", 2]}, {"targets"}),
    ({"operator_identity": "system:serviceaccount:pa-eval-system:other"}, {"operator_identity"}),
    ({"control_namespace": None}, {"control_namespace", "operator_identity"}),
    ({"release": "A B"}, {"release", "operator_identity"}), ({"target_namespace": ""}, {"target_namespace"}),
    ({"decision_interval_seconds": 0}, {"decision_interval_seconds", "cadence_source"}),
    ({"decision_interval_seconds": 0.5}, {"decision_interval_seconds", "cadence_source"}),
    ({"cadence_source": ""}, {"decision_interval_seconds", "cadence_source"}),
])
def test_a_damaged_identity_still_gives_every_field_that_is_sound(record, change, lost):
    # An emergency must be able to name the binding (release + target namespace) even when the targets are damaged.
    doc = R.identity_write(record, **IDENT)
    open(identity_path(record), "w").write(json.dumps(dict(doc, **change)))
    fields, problems = R.identity_read(record)
    assert {k for k, v in fields.items() if v is None} == lost and problems
    assert len(problems) == len(lost - {"cadence_source"})                 # the interval and its source are one finding
    for key in set(R.IDENTITY_FIELDS) - lost:
        assert fields[key] == (sorted(doc[key]) if key == "targets" else doc[key])


# --- the chart-restoration obligation --------------------------------------------------------------------------------

def chart_dir(record):
    return os.path.join(record.evidence_dir, "receipts", R.CHART_DIR)


def test_a_chart_obligation_is_durable_single_and_blocks(record):
    assert R.chart_state(record) == {"pending": [], "last": 0, "problem": None}
    R.refuse_if_chart_pending(record)                                      # nothing pending: no refusal
    assert R.chart_pending(record, "revoke reh1") == 1
    assert R.chart_pending(record, "again") == 1                           # the pending one, not a second one
    assert os.listdir(chart_dir(record)) == ["000001-restore-pending.json"]
    doc = json.load(open(os.path.join(chart_dir(record), "000001-restore-pending.json")))
    assert doc["reason"] == "revoke reh1" and doc["event"] == "restore-pending" and doc["seq"] == 1 and doc["at"]
    assert R.chart_state(record) == {"pending": [1], "last": 1, "problem": None}
    with pytest.raises(X.StagingError, match=r"chart restoration \[1\] is pending"):
        R.refuse_if_chart_pending(record)


def test_nothing_closes_a_chart_obligation_yet_not_even_a_file_that_says_restored(record):
    # The reviewer's reproduction: a header-only "restored" file made refuse_if_chart_pending pass. No restoration,
    # no record of one and no evidence for one are defined yet, so such a file is a problem, never a closure.
    R.chart_pending(record, "first")
    closing = {"schema": R.CHART_SCHEMA, "run_id": "c28-demo-1", "seq": 1, "event": "restored", "at": "t"}
    open(os.path.join(chart_dir(record), "000001-restored.json"), "w").write(json.dumps(closing))
    state = R.chart_state(record)
    assert state["pending"] is None and "000001-restored.json" in state["problem"]
    with pytest.raises(X.StagingError, match="cannot be read with certainty"):
        R.refuse_if_chart_pending(record)
    os.unlink(os.path.join(chart_dir(record), "000001-restored.json"))
    assert R.chart_state(record) == {"pending": [1], "last": 1, "problem": None}
    assert not [n for n in dir(R) if "restored" in n.lower()]              # and nothing here writes one


@pytest.mark.parametrize("name,content", [
    ("notes.txt", "x"), (".tmp-1-aa", "x"), ("000001-restored.json", "{"),
    ("000001-restored.json", json.dumps({"schema": "c28-transition-chart/v1", "run_id": "other", "seq": 1,
                                         "event": "restored", "at": "t"})),
    ("000001-restored.json", json.dumps({"schema": "c28-transition-chart/v1", "run_id": "c28-demo-1", "seq": 2,
                                         "event": "restored", "at": "t"})),
    ("000002-restored.json", json.dumps({"schema": "c28-transition-chart/v1", "run_id": "c28-demo-1", "seq": 2,
                                         "event": "restored", "at": "t"})),
    ("000003-restore-pending.json", json.dumps({"schema": "c28-transition-chart/v1", "run_id": "c28-demo-1",
                                                "seq": 3, "event": "restore-pending", "at": "t"})),
])
def test_chart_records_in_disorder_are_unknown_and_block(record, name, content):
    R.chart_pending(record, "first")
    open(os.path.join(chart_dir(record), name), "w").write(content)
    state = R.chart_state(record)
    assert state["pending"] is None and state["last"] is None and state["problem"]   # unknown, never "none pending"
    with pytest.raises(X.StagingError, match="cannot be read with certainty"):
        R.refuse_if_chart_pending(record)
    with pytest.raises(X.StagingError, match="cannot be read with certainty"):
        R.chart_pending(record, "another")


@pytest.mark.parametrize("reason", ["", "  ", None, 3])
def test_a_chart_obligation_needs_a_reason(record, reason):
    with pytest.raises(X.StagingError, match="needs its reason"):
        R.chart_pending(record, reason)
    assert R.chart_state(record)["pending"] == []


# --- the permission proof's durable copy -----------------------------------------------------------------------------

def test_a_proof_is_stored_under_its_digest_and_read_back(record):
    proof = {"verdict": "pass", "refused": [], "unproven": [], "answers": {"-": {"update": "no"}}}
    sha = R.store_proof(record, proof)
    path = os.path.join(record.evidence_dir, R.PROOF_DIR, sha + ".json")
    assert sha == C.digest(proof) == C.sha256_file(path) and json.load(open(path)) == proof
    assert R.store_proof(record, dict(proof)) == sha                       # the same proof again
    assert os.listdir(os.path.dirname(path)) == [sha + ".json"]
    assert R.store_proof(record, dict(proof, verdict="refused")) != sha


def test_a_stored_proof_that_does_not_match_its_name_is_no_proof(record):
    proof = {"verdict": "pass", "refused": [], "unproven": []}
    sha = R.store_proof(record, proof)
    path = os.path.join(record.evidence_dir, R.PROOF_DIR, sha + ".json")
    open(path, "w").write(json.dumps(dict(proof, verdict="refused")))      # someone changed the stored copy
    with pytest.raises(X.StagingError, match="not the proof that was read"):
        R.store_proof(record, proof)


@pytest.mark.parametrize("fail", ["fsync", "link"])
def test_a_proof_that_cannot_be_stored_is_an_error(record, monkeypatch, fail):
    def broken(*_a, **_k):
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(os, fail, broken)
    with pytest.raises(OSError):
        R.store_proof(record, {"verdict": "pass", "refused": [], "unproven": []})


@pytest.mark.parametrize("proof", [None, "pass", [], 1])
def test_only_a_result_object_is_a_proof(record, proof):
    with pytest.raises(X.StagingError, match="result object"):
        R.store_proof(record, proof)


# --- what a recovery of the stale lock must respect ------------------------------------------------------------------

def start(record, executor, operation):
    X.receipt(record, R.HELPER, "executor-started", {"id": executor, "operation": operation, "activation": "reh1",
                                                     "owner": {"host": socket.gethostname(), "boot_id": BOOT,
                                                               "pid": 4001, "pid_start": "1", "pgid": 4001}})


def test_a_run_without_a_transition_holds_nothing(record):
    assert R.unresolved_revocations(record) == ([], None) and R.release_problem(record) is None


def test_an_executor_that_is_neither_finished_nor_fenced_holds_the_lock(record):
    start(record, "grant-reh1-1", "grant")
    assert "grant-reh1-1" in R.release_problem(record) and "neither finished nor fenced" in R.release_problem(record)
    X.receipt(record, R.HELPER, "executor-finished", {"id": "grant-reh1-1"})
    assert R.release_problem(record) is None


@pytest.mark.parametrize("outcome,held", [("revoked", False), ("not-started", False), ("not-revoked", True),
                                          (None, True), ("REVOKED", True), (True, True)])
def test_only_a_proven_revocation_or_one_that_never_started_lets_the_lock_go(record, outcome, held):
    start(record, "revoke-reh1-1", "revoke")
    X.receipt(record, R.HELPER, "revocation", {"executor": "revoke-reh1-1", "outcome": outcome})
    X.receipt(record, R.HELPER, "executor-finished", {"id": "revoke-reh1-1"})
    assert R.unresolved_revocations(record) == (["revoke-reh1-1"] if held else [], None)
    assert (R.release_problem(record) is not None) is held


def test_a_finished_revoke_without_any_result_holds_the_lock(record):
    start(record, "revoke-reh1-1", "revoke")
    X.receipt(record, R.HELPER, "executor-finished", {"id": "revoke-reh1-1"})
    assert "revoke-reh1-1" in R.release_problem(record)
    X.receipt(record, R.HELPER, "revocation", {"executor": "another", "outcome": "revoked"})   # not its result
    assert "revoke-reh1-1" in R.release_problem(record)


def test_receipts_that_cannot_be_read_hold_the_lock(record, monkeypatch):
    def broken(_record, _helper):
        raise X.StagingError("the receipt chain is broken")
    monkeypatch.setattr(X, "receipts", broken)
    assert R.unresolved_revocations(record)[1] and R.release_problem(record)
