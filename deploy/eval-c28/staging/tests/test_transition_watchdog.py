"""The in-cluster watchdog of the Active transition (transition_watchdog.py), against an in-memory API.

Every rule of TRANSITION-DESIGN draft 6, section 3a, and the three items of Review 6 (a lost arming response, renewals
under conflicts and overlapping processes, the failed-trigger-write / crash / recreation sequence).
"""
import copy

import pytest

import transition_watchdog as W

NS, CTL = "demo", "pa-eval-system"
BINDING, TRIGGER, HEARTBEAT = "pa-eval-operator-scale", "c28-trigger-act1", "c28-heartbeat-act1"
UID = "lease-uid-1"


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def now(self):
        return self.t

    def advance(self, s):
        self.t += s


class FakeApi:
    """Leases and one RoleBinding with resourceVersions; `fail` injects faults per call name."""

    def __init__(self, holder="unarmed:act1", uid=UID):
        self.rv = 10
        self.beats = 0
        self.leases = {}
        if holder is not None:
            self.leases[TRIGGER] = self._lease(TRIGGER, uid, holder)
        self.leases[HEARTBEAT] = self._lease(HEARTBEAT, "hb-uid", "executor")
        self.binding = {"metadata": {"name": BINDING, "uid": "b-1"}}
        self.calls = []                 # (name, detail) in order
        self.fail = {}                  # name -> callable(api, *args) run BEFORE the call; may raise or mutate
        self.after = {}                 # name -> callable run AFTER the call applied; may raise (a lost response)

    def _lease(self, name, uid, holder):
        self.rv += 1
        return {"metadata": {"name": name, "uid": uid, "resourceVersion": str(self.rv)},
                "spec": {"holderIdentity": holder, "renewTime": W.micro_time(1_000_000)}}

    def _hook(self, table, name, *args):
        if name in table:
            table[name](self, *args)

    def beat(self, renew_time=None, touch_only=False):
        """The executor renews its heartbeat: renewTime advances. touch_only changes the resourceVersion alone."""
        self.rv += 1
        self.leases[HEARTBEAT]["metadata"]["resourceVersion"] = str(self.rv)
        if touch_only:
            return
        self.beats += 1
        self.leases[HEARTBEAT]["spec"]["renewTime"] = (
            W.micro_time(1_000_000 + self.beats) if renew_time is None else renew_time)

    def holder(self):
        return self.leases[TRIGGER]["spec"]["holderIdentity"]

    def recreate_binding(self, uid):    # a delayed grant
        self.binding = {"metadata": {"name": BINDING, "uid": uid}}

    def get_lease(self, ns, name):
        self.calls.append(("get_lease", name))
        self._hook(self.fail, "get_lease", name)
        return copy.deepcopy(self.leases.get(name))

    def update_lease(self, ns, name, obj):
        self.calls.append(("update_lease", obj["spec"]["holderIdentity"]))
        self._hook(self.fail, "update_lease", obj)
        live = self.leases.get(name)
        if live is None:
            raise W.ApiError("gone")
        if obj["metadata"]["resourceVersion"] != live["metadata"]["resourceVersion"]:
            raise W.Conflict("409")
        self.rv += 1
        new = copy.deepcopy(obj)
        new["metadata"]["resourceVersion"] = str(self.rv)
        self.leases[name] = new
        self._hook(self.after, "update_lease", obj)
        return copy.deepcopy(new)

    def get_binding(self, ns, name):
        self.calls.append(("get_binding", name))
        self._hook(self.fail, "get_binding", name)
        return copy.deepcopy(self.binding)

    def delete_binding(self, ns, name, uid):
        self.calls.append(("delete_binding", uid))
        self._hook(self.fail, "delete_binding", uid)
        if self.binding is None or self.binding["metadata"]["uid"] != uid:
            raise W.Conflict("precondition")
        self.binding = None


def dog(api, clock, nonce="n1", deadline=None, mono=None, **kw):
    logs = []
    args = dict(interval=60, stale_intervals=3)
    args.update(kw)
    w = W.Watchdog(api, activation="act1", deadline=clock.t + 1800 if deadline is None else deadline,
                   binding_ns=NS, binding=BINDING, lease_ns=CTL, trigger_lease=TRIGGER, trigger_uid=UID,
                   heartbeat_lease=HEARTBEAT, nonce=nonce, now=clock.now, mono=mono or clock.now,
                   log=logs.append, **args)
    w.logs = logs
    return w


def deny(_api, *_a):
    raise W.ApiError("HTTP 403")


def names(api):
    return [c[0] for c in api.calls]


# --- the state machine itself ------------------------------------------------------------------------------------

@pytest.mark.parametrize("current,new,ok", [
    ("unarmed:act1", "armed:n1", True), ("unarmed:act1", "triggered:deadline", True),
    ("armed:n1", "triggered:deadline", True), ("armed:n1", "armed:n1", True),
    ("triggered:deadline", "triggered:deadline", True),
    ("armed:n1", "armed:n2", False), ("armed:n1", "unarmed:act1", False),
    ("triggered:deadline", "armed:n1", False), ("triggered:deadline", "unarmed:act1", False),
    ("triggered:deadline", "triggered:stale-heartbeat", False),          # the first reason stays
    ("garbage", "armed:n1", False), ("armed:n1", "garbage", False), (None, "armed:n1", False),
    ("armed:", "triggered:x", False), ("armed:n 1", "triggered:x", False),
])
def test_the_holder_only_moves_forward(current, new, ok):
    assert W.forward(current, new) is ok


# --- start ---------------------------------------------------------------------------------------------------------

def test_an_unarmed_lease_is_armed_confirmed_and_monitored():
    api, clock = FakeApi(), Clock()
    w = dog(api, clock)
    assert w.step() == W.MONITORING
    assert api.holder() == "armed:n1" and api.binding is not None
    # armed, then read again before monitoring: the confirmation is a read AFTER the update
    assert names(api)[:3] == ["get_lease", "update_lease", "get_lease"]
    for _ in range(10):                  # a renewed heartbeat keeps it monitoring, well past the stale limit
        clock.advance(60)
        api.beat()
        assert w.step() == W.MONITORING
    assert api.holder() == "armed:n1" and api.binding is not None and w.deletions == 0


def test_a_lost_arming_response_is_settled_by_the_reread():
    # Review 6: the update applied but its response was lost: the re-read shows this process's nonce.
    api, clock = FakeApi(), Clock()

    def lost(_api, _obj):
        del _api.after["update_lease"]
        raise W.ApiError("timeout")
    api.after["update_lease"] = lost
    w = dog(api, clock)
    assert w.step() == W.MONITORING and api.holder() == "armed:n1"


def test_an_arming_that_did_not_apply_is_revocation_not_a_retry():
    api, clock = FakeApi(), Clock()

    def once(_api, _obj):
        del _api.fail["update_lease"]
        raise W.ApiError("timeout")
    api.fail["update_lease"] = once
    w = dog(api, clock)
    assert w.step() == W.REVOCATION and w.reason == "arming-uncertain"
    assert api.holder() == "triggered:arming-uncertain" and api.binding is None


def test_an_arming_whose_confirmation_cannot_be_read_is_revocation():
    api, clock = FakeApi(), Clock()
    reads = {"n": 0}

    def second_read_fails(_api, name):
        if name == TRIGGER:
            reads["n"] += 1
            if reads["n"] == 2:
                raise W.ApiError("timeout")
    api.fail["get_lease"] = second_read_fails
    w = dog(api, clock)
    assert w.step() == W.REVOCATION and w.reason == "arming-uncertain"
    assert api.binding is None and api.holder() == "triggered:arming-uncertain"   # recorded once readable again


def test_a_triggered_lease_means_revocation_whatever_the_heartbeat_says():
    api, clock = FakeApi(holder="triggered:deadline"), Clock()
    w = dog(api, clock, nonce="n2")
    api.beat()
    assert w.step() == W.REVOCATION and w.recorded and w.reason == "deadline"
    assert api.binding is None and api.holder() == "triggered:deadline"


def test_a_restart_never_resumes_monitoring():
    # Another nonce holds the arming: an earlier process was interrupted. The heartbeat is fresh. Still a trigger.
    api, clock = FakeApi(holder="armed:old"), Clock()
    api.beat()
    w = dog(api, clock, nonce="new")
    assert w.step() == W.REVOCATION and w.reason == "watchdog-restart"
    assert api.holder() == "triggered:watchdog-restart" and api.binding is None
    i = names(api)
    assert i.index("update_lease") < i.index("delete_binding")           # recorded BEFORE the deletion


@pytest.mark.parametrize("make,reason", [
    (lambda: FakeApi(holder=None), "trigger-state-missing"),
    (lambda: FakeApi(uid="another-uid"), "trigger-state-replaced"),
    (lambda: FakeApi(holder="something else"), "trigger-state-unexpected"),
    (lambda: FakeApi(holder="unarmed:act2"), "trigger-state-other-activation"),
])
def test_a_lease_that_cannot_be_trusted_is_revocation_and_is_never_written(make, reason):
    api, clock = make(), Clock()
    before = copy.deepcopy(api.leases.get(TRIGGER))
    w = dog(api, clock)
    assert w.step() == W.REVOCATION and w.reason == reason and not w.recorded
    assert api.binding is None
    for _ in range(3):
        api.recreate_binding("b-again")
        assert w.step() == W.REVOCATION and api.binding is None           # keeps revoking
    assert api.leases.get(TRIGGER) == before and "update_lease" not in names(api)


def test_an_unreadable_lease_at_start_is_revocation_and_recorded_when_it_reads_again():
    api, clock = FakeApi(), Clock()
    api.fail["get_lease"] = deny
    w = dog(api, clock)
    assert w.step() == W.REVOCATION and w.reason == "trigger-state-unreadable" and not w.recorded
    assert api.binding is None                                           # the deletion did not wait for the Lease
    # Bounded: READ_ATTEMPTS reads at start, one more for the single record attempt, then the deletion.
    assert names(api)[:W.READ_ATTEMPTS + 2] == ["get_lease"] * (W.READ_ATTEMPTS + 1) + ["get_binding"]
    del api.fail["get_lease"]
    api.beat()                                                            # a fresh heartbeat changes nothing
    assert w.step() == W.REVOCATION and w.recorded
    assert api.holder() == "triggered:trigger-state-unreadable"


# --- triggers ------------------------------------------------------------------------------------------------------

def test_the_deadline_records_then_deletes_and_never_ends():
    api, clock = FakeApi(), Clock()
    w = dog(api, clock, deadline=clock.t + 300)
    w.step()
    for _ in range(4):
        clock.advance(60)
        api.beat()
        assert w.step() == W.MONITORING
    clock.advance(60)
    api.beat()                                                            # a live executor does not postpone it
    api.calls.clear()
    assert w.step() == W.REVOCATION and w.reason == "deadline" and w.recorded
    assert api.holder() == "triggered:deadline" and api.binding is None
    i = names(api)
    assert i.index("update_lease") < i.index("delete_binding")
    # A delayed grant recreates the binding, repeatedly: deleted every time, counted, and the run never returns.
    for k in range(3):
        api.recreate_binding(f"b-late-{k}")
        assert w.step() == W.REVOCATION and api.binding is None
    assert w.deletions == 4 and api.leases[TRIGGER]["spec"]["leaseTransitions"] == 4
    assert w.run(pass_seconds=0, sleep=lambda _s: None, passes=5) == W.REVOCATION
    last = w.logs[-1]
    assert last["binding_observation"] == "absent" and "not a completed revocation" in last["note"]


def test_a_stale_heartbeat_triggers_and_a_resumed_one_does_not_clear_it():
    api, clock = FakeApi(), Clock()
    w = dog(api, clock)
    w.step()
    assert w.step() == W.MONITORING        # the first look at the heartbeat only remembers it: it proves no renewal
    clock.advance(60)
    api.beat()
    assert w.step() == W.MONITORING        # a change this process saw: the stale limit counts from here
    clock.advance(180)                                                    # exactly the limit: not yet
    assert w.step() == W.MONITORING
    clock.advance(1)
    assert w.step() == W.REVOCATION and w.reason == "stale-heartbeat"
    assert api.holder() == "triggered:stale-heartbeat" and api.binding is None
    for _ in range(5):                                                    # the executor comes back
        clock.advance(60)
        api.beat()
        api.recreate_binding("b-back")
        assert w.step() == W.REVOCATION and api.binding is None
    assert api.holder() == "triggered:stale-heartbeat"


def test_a_heartbeat_that_is_present_but_never_changes_is_stale():
    api, clock = FakeApi(), Clock()
    w = dog(api, clock)
    w.step()
    for _ in range(3):
        clock.advance(60)
        assert w.step() == W.MONITORING
    clock.advance(1)
    assert w.step() == W.REVOCATION and w.reason == "stale-heartbeat"


def test_an_unreadable_or_missing_heartbeat_counts_as_no_heartbeat():
    for breakage in ("unreadable", "missing"):
        api, clock = FakeApi(), Clock()
        w = dog(api, clock)
        w.step()
        clock.advance(60)
        api.beat()
        w.step()
        if breakage == "missing":
            del api.leases[HEARTBEAT]
        else:
            api.fail["get_lease"] = lambda _api, name: deny(_api) if name == HEARTBEAT else None
        clock.advance(181)
        assert w.step() == W.REVOCATION and w.reason == "stale-heartbeat", breakage


# --- Review 5: the exact reproduction ------------------------------------------------------------------------------

def test_failed_record_then_crash_then_resumed_heartbeat_and_recreated_binding():
    api, clock = FakeApi(), Clock()
    first = dog(api, clock, nonce="first")
    first.step()
    clock.advance(60)
    api.beat()
    assert first.step() == W.MONITORING
    # The heartbeat goes stale and the watchdog cannot write its Lease (its update rule is gone).
    api.fail["update_lease"] = deny
    clock.advance(181)
    api.calls.clear()
    assert first.step() == W.REVOCATION and not first.recorded
    # Bounded: exactly ONE try to record the trigger (the pass's own renewal attempt, also denied, is not it),
    # BEFORE the deletion, which then happens anyway. Later passes retry the record.
    tries = [i for i, c in enumerate(api.calls) if c == ("update_lease", "triggered:stale-heartbeat")]
    assert len(tries) == 1 and tries[0] < names(api).index("delete_binding")
    assert api.binding is None and api.holder() == "armed:first"         # the record failed
    # The process is killed here. The heartbeat resumes. A stopped grant process resumes and recreates the binding.
    del first
    del api.fail["update_lease"]
    api.beat()
    api.recreate_binding("b-recreated")
    second = dog(api, clock, nonce="second")
    api.beat()
    assert second.step() == W.REVOCATION and second.reason == "watchdog-restart"
    assert api.binding is None and api.holder() == "triggered:watchdog-restart"
    for _ in range(3):                                                    # and it stays so, heartbeat or not
        clock.advance(60)
        api.beat()
        api.recreate_binding("b-again")
        assert second.step() == W.REVOCATION and api.binding is None
    assert second.mode != W.MONITORING


def test_a_failed_record_is_retried_on_every_pass():
    api, clock = FakeApi(), Clock()
    w = dog(api, clock, deadline=clock.t + 60)
    w.step()
    api.fail["update_lease"] = deny
    clock.advance(61)
    assert w.step() == W.REVOCATION and not w.recorded and api.binding is None
    assert w.step() == W.REVOCATION and not w.recorded
    del api.fail["update_lease"]
    assert w.step() == W.REVOCATION and w.recorded and api.holder() == "triggered:deadline"


# --- Review 6: renewals under conflicts and overlapping processes ----------------------------------------------------

def test_overlapping_processes_cannot_undo_a_trigger():
    api, clock = FakeApi(), Clock()
    a = dog(api, clock, nonce="a")
    a.step()
    api.beat()
    b = dog(api, clock, nonce="b")                                        # a second process starts while A monitors
    assert b.step() == W.REVOCATION and api.holder() == "triggered:watchdog-restart"
    api.recreate_binding("b-2")
    clock.advance(5)
    api.beat()
    assert a.step() == W.REVOCATION and a.recorded                         # A sees the trigger and follows it
    assert a.reason == "watchdog-restart" and api.binding is None
    assert api.holder() == "triggered:watchdog-restart"                   # A never wrote armed:a over it


def test_a_renewal_that_races_a_trigger_loses():
    # A read the Lease as armed:a; before A's renewal lands, another process records the trigger. A's write is
    # conditional, so it gets a conflict and the holder stays triggered.
    api, clock = FakeApi(), Clock()
    a = dog(api, clock, nonce="a")
    a.step()

    def trigger_in_between(_api, obj):
        if obj["spec"]["holderIdentity"] == "armed:a":
            del _api.fail["update_lease"]
            _api.rv += 1
            _api.leases[TRIGGER]["spec"]["holderIdentity"] = "triggered:deadline"
            _api.leases[TRIGGER]["metadata"]["resourceVersion"] = str(_api.rv)
    api.fail["update_lease"] = trigger_in_between
    clock.advance(5)
    api.beat()
    assert a.step() == W.MONITORING                                       # the renewal failed; nothing was overwritten
    assert api.holder() == "triggered:deadline"
    clock.advance(5)
    assert a.step() == W.REVOCATION and a.reason == "deadline" and api.binding is None
    assert api.holder() == "triggered:deadline"


def test_the_lease_changing_under_a_monitoring_process_is_revocation():
    for change, reason in (("deleted", "trigger-state-changed"), ("replaced", "trigger-state-changed"),
                           ("garbage", "trigger-state-changed"), ("unarmed", "watchdog-overlap")):
        api, clock = FakeApi(), Clock()
        w = dog(api, clock)
        w.step()
        if change == "deleted":
            del api.leases[TRIGGER]
        elif change == "replaced":
            api.leases[TRIGGER]["metadata"]["uid"] = "another"
        elif change == "garbage":
            api.leases[TRIGGER]["spec"]["holderIdentity"] = "x"
        else:
            api.leases[TRIGGER]["spec"]["holderIdentity"] = "unarmed:act1"
        clock.advance(5)
        api.beat()
        assert w.step() == W.REVOCATION and w.reason == reason, change
        assert api.binding is None


# --- failure is explicit ---------------------------------------------------------------------------------------------

def test_a_denied_deletion_is_reported_and_never_reads_as_absent():
    api, clock = FakeApi(holder="triggered:deadline"), Clock()
    api.fail["delete_binding"] = deny
    w = dog(api, clock)
    for _ in range(3):
        assert w.step() == W.REVOCATION
        assert w.last_binding == "present-undeleted" and api.binding is not None
    assert w.deletions == 0 and any(l.get("event") == "deletion-failed" for l in w.logs)
    assert w.logs[-1]["binding_observation"] == "present-undeleted"
    api.fail["get_binding"] = deny
    w.step()
    assert w.last_binding == "unknown"
    del api.fail["delete_binding"], api.fail["get_binding"]
    assert w.step() == W.REVOCATION and api.binding is None and w.last_binding == "deleted"


def test_a_binding_replaced_between_the_read_and_the_delete_is_deleted_on_the_next_pass():
    api, clock = FakeApi(holder="triggered:deadline"), Clock()

    def swap(_api, _uid):
        del _api.fail["delete_binding"]
        _api.recreate_binding("b-swapped")
    api.fail["delete_binding"] = swap
    w = dog(api, clock)
    assert w.step() == W.REVOCATION and api.binding is not None          # the precondition refused the old UID
    assert w.step() == W.REVOCATION and api.binding is None


def test_main_has_no_successful_exit():
    assert W.Watchdog.run.__doc__ and "no successful exit" in W.Watchdog.run.__doc__
    src = open(W.__file__).read()
    assert "return 1" in src.split("def main")[1]                         # a return from run() is a failure


# --- the real client, against a loopback HTTP server (no cluster) ----------------------------------------------------

@pytest.fixture
def kube(tmp_path):
    import http.server
    import json
    import ssl
    import threading

    seen, answers = [], {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def _serve(self):
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n)) if n else None
            seen.append((self.command, self.path, self.headers.get("Authorization"), body))
            code, payload = answers.get((self.command, self.path), (404, {"kind": "Status"}))
            if code == "hang":
                import time
                time.sleep(1.0)
                code = 200
            raw = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        do_GET = do_PUT = do_DELETE = _serve

        def log_message(self, *_a):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    token = tmp_path / "token"
    token.write_text("tok-1\n")
    ca = ssl.get_default_verify_paths().cafile or ssl.get_default_verify_paths().openssl_cafile
    api = W.KubeApi(f"http://127.0.0.1:{server.server_address[1]}", token_file=str(token), ca_file=ca, timeout=0.3)
    yield api, seen, answers, token
    server.shutdown()
    server.server_close()


def test_the_real_client_maps_answers_and_sends_what_the_roles_allow(kube):
    api, seen, answers, token = kube
    lease = "/apis/coordination.k8s.io/v1/namespaces/pa-eval-system/leases/c28-trigger-act1"
    binding = "/apis/rbac.authorization.k8s.io/v1/namespaces/demo/rolebindings/pa-eval-operator-scale"
    assert api.get_lease(CTL, TRIGGER) is None and api.get_binding(NS, BINDING) is None       # 404: absent
    answers[("GET", lease)] = (200, {"metadata": {"uid": UID}})
    assert api.get_lease(CTL, TRIGGER) == {"metadata": {"uid": UID}}
    assert seen[-1][:3] == ("GET", lease, "Bearer tok-1")
    token.write_text("tok-2\n")                                           # a rotated projected token is read again
    obj = {"metadata": {"resourceVersion": "7"}, "spec": {"holderIdentity": "armed:n1"}}
    answers[("PUT", lease)] = (200, obj)
    assert api.update_lease(CTL, TRIGGER, obj) == obj
    assert seen[-1] == ("PUT", lease, "Bearer tok-2", obj)                # the resourceVersion travels: conditional
    answers[("PUT", lease)] = (409, {})
    with pytest.raises(W.Conflict):
        api.update_lease(CTL, TRIGGER, obj)
    answers[("PUT", lease)] = (404, {})
    with pytest.raises(W.ApiError):                                       # a Lease that vanished is not a success
        api.update_lease(CTL, TRIGGER, obj)
    answers[("DELETE", binding)] = (200, {})
    api.delete_binding(NS, BINDING, "b-1")
    assert seen[-1] == ("DELETE", binding, "Bearer tok-2",
                        {"kind": "DeleteOptions", "apiVersion": "v1", "preconditions": {"uid": "b-1"}})
    answers[("DELETE", binding)] = (409, {})
    with pytest.raises(W.Conflict):                                       # the UID precondition did not hold
        api.delete_binding(NS, BINDING, "b-1")
    for code in (401, 403, 500, 503):
        answers[("GET", binding)] = (code, {})
        with pytest.raises(W.ApiError):
            api.get_binding(NS, BINDING)
    answers[("GET", binding)] = ("hang", {})
    with pytest.raises(W.ApiError):                                       # bounded: a slow answer is a failed call
        api.get_binding(NS, BINDING)


def test_the_real_client_reports_an_unreachable_api(tmp_path):
    import ssl
    token = tmp_path / "token"
    token.write_text("t")
    ca = ssl.get_default_verify_paths().cafile or ssl.get_default_verify_paths().openssl_cafile
    api = W.KubeApi("http://127.0.0.1:9", token_file=str(token), ca_file=ca, timeout=0.3)     # nothing listens
    with pytest.raises(W.ApiError):
        api.get_lease(CTL, TRIGGER)


# --- the code review's five findings, each as a regression ------------------------------------------------------------

def test_a_trickled_answer_cannot_outlast_the_total_deadline(tmp_path):
    # Finding 1: the socket timeout bounds each read, not the call. A server that sends one byte every 50 ms never
    # trips a 0.2 s read timeout; the call must still fail at its total deadline.
    import socket
    import ssl
    import threading
    import time

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    stop = threading.Event()

    def serve():
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            try:
                conn.recv(65536)
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 400\r\n\r\n")
                for _ in range(400):
                    if stop.is_set():
                        break
                    conn.sendall(b" ")
                    time.sleep(0.05)
            except OSError:
                pass
            finally:
                conn.close()
    threading.Thread(target=serve, daemon=True).start()
    token = tmp_path / "token"
    token.write_text("t")
    ca = ssl.get_default_verify_paths().cafile or ssl.get_default_verify_paths().openssl_cafile
    api = W.KubeApi(f"http://127.0.0.1:{listener.getsockname()[1]}", token_file=str(token), ca_file=ca, timeout=0.2)
    began = time.monotonic()
    with pytest.raises(W.ApiError):
        api.get_binding(NS, BINDING)
    elapsed = time.monotonic() - began
    stop.set()
    listener.close()
    assert elapsed < 0.6, elapsed                      # the 400 bytes would take 20 s; the call ended at its deadline


def test_an_unreadable_trigger_lease_while_monitoring_is_revocation():
    # Finding 2: trigger reads denied, heartbeats continuing. It must not keep monitoring: the deletion is attempted.
    api, clock = FakeApi(), Clock()
    w = dog(api, clock)
    w.step()
    clock.advance(5)
    api.beat()
    assert w.step() == W.MONITORING
    api.fail["get_lease"] = lambda _api, name: deny(_api) if name == TRIGGER else None
    clock.advance(5)
    api.beat()
    api.calls.clear()
    assert w.step() == W.REVOCATION and w.reason == "trigger-state-unreadable" and not w.recorded
    assert api.binding is None
    assert names(api).count("get_lease") == W.READ_ATTEMPTS + 1          # bounded reads, one record attempt
    for _ in range(3):                                                    # fresh heartbeats never bring it back
        clock.advance(60)
        api.beat()
        api.recreate_binding("b-x")
        assert w.step() == W.REVOCATION and api.binding is None
    del api.fail["get_lease"]
    assert w.step() == W.REVOCATION and w.recorded
    assert api.holder() == "triggered:trigger-state-unreadable"


def test_a_lease_of_another_activation_is_never_written_on_any_path():
    # Finding 3a: unreadable at start, then readable as unarmed:act2. It was overwritten; it must be left alone.
    api, clock = FakeApi(holder="unarmed:act2"), Clock()
    api.fail["get_lease"] = deny
    w = dog(api, clock)
    assert w.step() == W.REVOCATION and w.reason == "trigger-state-unreadable"
    del api.fail["get_lease"]
    before = copy.deepcopy(api.leases[TRIGGER])
    for _ in range(4):
        api.recreate_binding("b-y")
        assert w.step() == W.REVOCATION and not w.recorded and api.binding is None
    assert api.leases[TRIGGER] == before and "update_lease" not in names(api)
    # And the single write function refuses it whatever the caller intends.
    with pytest.raises(W.ApiError):
        w._write_holder(copy.deepcopy(before), "triggered:x")
    with pytest.raises(W.ApiError):
        w._write_holder({"metadata": {"uid": "another"}, "spec": {"holderIdentity": "armed:n1"}}, "triggered:x")


def test_a_trigger_that_is_undone_is_never_renewed_as_armed_and_is_recorded_again():
    # Finding 3b: after the trigger, the holder is reset to armed:n1. The revocation loop renewed that armed state and
    # still said recorded=True. It must report the loss and move the Lease forward again.
    api, clock = FakeApi(), Clock()
    w = dog(api, clock, deadline=clock.t + 60)
    w.step()
    clock.advance(61)
    assert w.step() == W.REVOCATION and w.recorded and api.holder() == "triggered:deadline"
    blocked = {"on": True}

    def no_writes(_api, _obj):
        if blocked["on"]:
            raise W.ApiError("HTTP 403")
    api.fail["update_lease"] = no_writes
    for back in ("armed:n1", "unarmed:act1"):
        api.rv += 1
        api.leases[TRIGGER]["spec"]["holderIdentity"] = back
        api.leases[TRIGGER]["metadata"]["resourceVersion"] = str(api.rv)
        blocked["on"] = True
        api.calls.clear()
        assert w.step() == W.REVOCATION and not w.recorded               # it says so while the Lease does not show it
        assert ("update_lease", back) not in api.calls                   # never renewed as it stands
        assert w.logs[-1]["recorded"] is False
        blocked["on"] = False
        assert w.step() == W.REVOCATION and w.recorded and api.holder() == "triggered:deadline"


def test_timing_values_that_would_remove_the_limit_are_refused():
    # Finding 4: deadline=NaN never expired.
    api, clock = FakeApi(), Clock()
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError):
            dog(api, clock, deadline=bad)
    for bad in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            dog(api, clock, interval=bad)
    for bad in (0, -3, 1.5, True, None):
        with pytest.raises(ValueError):
            dog(api, clock, stale_intervals=bad)
    w = dog(api, clock)
    for bad in (float("nan"), float("inf"), -1):
        with pytest.raises(ValueError):
            w.run(pass_seconds=bad, sleep=lambda _s: None, passes=1)
    assert "update_lease" not in names(api)                               # none of them armed anything


def test_a_wall_clock_stepped_backwards_does_not_extend_the_limit():
    # Finding 4: with healthy heartbeats, a backward jump of the wall clock extended the activation.
    api, wall, mono = FakeApi(), Clock(), Clock()
    w = dog(api, wall, deadline=wall.t + 300, mono=mono.now)
    w.step()
    for _ in range(4):
        wall.advance(60)
        mono.advance(60)
        api.beat()
        assert w.step() == W.MONITORING
    wall.advance(-3600)                                                   # the node's clock is stepped back an hour
    mono.advance(59)
    api.beat()
    assert w.step() == W.MONITORING                                       # 299 s of the 300 have really passed
    mono.advance(1)
    api.beat()
    assert w.step() == W.REVOCATION and w.reason == "deadline" and api.binding is None
    # A forward jump still triggers on the absolute deadline.
    api2, wall2, mono2 = FakeApi(), Clock(), Clock()
    w2 = dog(api2, wall2, deadline=wall2.t + 300, mono=mono2.now)
    w2.step()
    wall2.advance(301)
    assert w2.step() == W.REVOCATION and w2.reason == "deadline"


def test_only_an_advancing_valid_renew_time_is_a_heartbeat():
    # Finding 5: changing only the resourceVersion kept it monitoring for 600 s with renewTime unchanged.
    for kind in ("metadata-only", "not-a-timestamp", "missing-field", "naive-timestamp", "backwards", "same"):
        api, clock = FakeApi(), Clock()
        w = dog(api, clock)
        w.step()
        clock.advance(5)
        api.beat()
        assert w.step() == W.MONITORING                                   # the first look only remembers
        clock.advance(5)
        api.beat()
        assert w.step() == W.MONITORING                                   # a real heartbeat: the limit counts from here
        good = api.leases[HEARTBEAT]["spec"]["renewTime"]
        outcome = None
        for _ in range(40):                                               # 200 s of churn, limit 180 s
            clock.advance(5)
            if kind == "metadata-only":
                api.beat(touch_only=True)
            elif kind == "not-a-timestamp":
                api.beat(renew_time=f"t{api.rv}")
            elif kind == "missing-field":
                api.beat(touch_only=True)
                api.leases[HEARTBEAT]["spec"].pop("renewTime", None)
            elif kind == "naive-timestamp":
                api.beat(renew_time="2026-11-16T20:00:00.000000")
            elif kind == "backwards":
                api.beat(renew_time=W.micro_time(999_000))
            else:
                api.beat(renew_time=good)
            outcome = w.step()
            if outcome == W.REVOCATION:
                break
        assert outcome == W.REVOCATION and w.reason == "stale-heartbeat", kind
        assert api.binding is None


def test_the_deletion_count_in_the_lease_survives_a_restart():
    api, clock = FakeApi(holder="triggered:deadline"), Clock()
    api.leases[TRIGGER]["spec"]["leaseTransitions"] = 5                   # an earlier process deleted five times
    w = dog(api, clock, nonce="later")
    assert w.step() == W.REVOCATION and w.deletions == 1                 # the first pass records before it deletes
    assert api.leases[TRIGGER]["spec"]["leaseTransitions"] == 5
    api.recreate_binding("b-z")
    assert w.step() == W.REVOCATION and w.deletions == 2
    assert api.leases[TRIGGER]["spec"]["leaseTransitions"] == 7           # 5 + this process's two, counted once each
    assert w.step() == W.REVOCATION and api.leases[TRIGGER]["spec"]["leaseTransitions"] == 7


# --- second code review: nothing may outlive a call; a stalled Lease request must not block the deletion --------------

class RawServer:
    """A loopback server whose behaviour per request is a function (conn, method, path) run in its own thread.
    It counts the connections the CLIENT closed, so a test can see that a timed-out call released its socket."""

    def __init__(self, behave):
        import socket
        import threading
        self.behave, self.requests, self.client_closed = behave, [], 0
        self.lock, self.stop = threading.Lock(), threading.Event()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(64)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        import threading
        while not self.stop.is_set():
            try:
                conn, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        try:
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                head += chunk
            method, path = head.split(b"\r\n", 1)[0].decode().split(" ")[:2]
            with self.lock:
                self.requests.append((method, path))
            self.behave(self, conn, method, path)
        except OSError:                                # the client went away while we were sending
            with self.lock:
                self.client_closed += 1
        finally:
            conn.close()

    def trickle(self, conn, prefix):
        """Send a prefix, then one byte every 20 ms until the client closes (raises OSError) or the test stops."""
        import time
        conn.sendall(prefix)
        while not self.stop.is_set():
            conn.sendall(b"a")
            time.sleep(0.02)

    def stall(self, conn):
        """Say nothing until the client closes."""
        conn.settimeout(0.05)
        while not self.stop.is_set():
            try:
                if conn.recv(1) == b"":
                    raise OSError("client closed")
            except TimeoutError:
                continue

    def close(self):
        self.stop.set()
        self.listener.close()


def real_api(server, tmp_path, timeout=0.2):
    token = tmp_path / "token"
    token.write_text("t")
    return W.KubeApi(f"http://127.0.0.1:{server.port}", token_file=str(token), ca_file=None, timeout=timeout)


def reply(conn, status, obj=None, extra=b""):
    import json
    body = b"" if obj is None else json.dumps(obj).encode()
    conn.sendall(b"HTTP/1.1 " + status + b"\r\nContent-Length: " + str(len(body)).encode() + b"\r\n" + extra + b"\r\n" + body)


@pytest.mark.parametrize("how", ["header-trickle", "stalled", "body-trickle-no-length"])
def test_repeated_timed_out_calls_leave_nothing_behind(tmp_path, how):
    # 10 calls that can never finish. Each must fail at its total deadline, on its own socket, closed; no thread of
    # the client may exist afterwards (there are none to begin with), and the server must see every socket closed.
    import threading
    import time

    def behave(server, conn, _method, _path):
        if how == "header-trickle":
            server.trickle(conn, b"HTTP/1.1 200 OK\r\nX-Pad: ")        # the headers never end
        elif how == "stalled":
            server.stall(conn)
        else:
            server.trickle(conn, b"HTTP/1.1 200 OK\r\n\r\n")          # a body without a length that never ends
    server = RawServer(behave)
    try:
        api = real_api(server, tmp_path)
        before = {t.ident for t in threading.enumerate()}
        for _ in range(10):
            began = time.monotonic()
            with pytest.raises(W.ApiError):
                api.get_lease(CTL, TRIGGER)
            assert time.monotonic() - began < 0.5                         # the deadline is 0.2 s
        deadline = time.monotonic() + 3
        while server.client_closed < 10 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert server.client_closed == 10                                 # every socket was released by the client
        mine = [t for t in threading.enumerate() if t.ident not in before and "_serve" not in repr(t._target)]
        assert mine == []                                                 # the client started no thread at all
    finally:
        server.close()


def test_stalled_lease_requests_do_not_keep_the_binding_from_being_deleted(tmp_path):
    # The whole watchdog on the real client: every Lease request trickles headers for ever; the RoleBinding answers.
    # Start must end in revocation and the DELETE must reach the server, within a bound that follows from the number
    # of Lease calls made before the deletion: READ_ATTEMPTS reads + 1 for the single record attempt.
    import time
    binding = {"metadata": {"name": BINDING, "uid": "b-1"}}
    deleted = []

    def behave(server, conn, method, path):
        if "/leases/" in path:
            server.trickle(conn, b"HTTP/1.1 200 OK\r\nX-Pad: ")
        elif method == "GET":
            reply(conn, b"200 OK", binding) if not deleted else reply(conn, b"404 Not Found", {})
        else:
            deleted.append(path)
            reply(conn, b"200 OK", {})
    server = RawServer(behave)
    try:
        logs = []
        w = W.Watchdog(real_api(server, tmp_path), activation="act1", deadline=time.time() + 600, binding_ns=NS,
                       binding=BINDING, lease_ns=CTL, trigger_lease=TRIGGER, trigger_uid=UID,
                       heartbeat_lease=HEARTBEAT, nonce="n1", log=logs.append)
        began = time.monotonic()
        assert w.step() == W.REVOCATION and w.reason == "trigger-state-unreadable" and not w.recorded
        elapsed = time.monotonic() - began
        assert deleted and w.last_binding == "deleted" and w.deletions == 1
        lease_calls = [r for r in server.requests if "/leases/" in r[1]]
        assert len(lease_calls) == W.READ_ATTEMPTS + 1
        assert elapsed < (W.READ_ATTEMPTS + 1) * 0.2 + 0.6, elapsed       # 4 Lease deadlines of 0.2 s, plus slack
        # And it keeps working: the next passes delete a recreated binding while the Lease still cannot be read.
        deleted.clear()
        del server.requests[:]
        began = time.monotonic()
        assert w.step() == W.REVOCATION and deleted and not w.recorded
        assert time.monotonic() - began < 0.2 + 0.6                       # deletion first, then ONE Lease deadline
        kinds = ["lease" if "/leases/" in path else "binding" for _method, path in server.requests]
        assert kinds == ["binding", "binding", "lease"], kinds            # get + delete BEFORE the Lease is touched
    finally:
        server.close()


def test_a_complete_answer_does_not_wait_for_the_server_to_close(tmp_path):
    # Keep-alive servers: the call ends when the answer is whole (by length or by the last chunk), not at the deadline.
    import time

    def behave(server, conn, _method, path):
        if path.endswith("chunked"):
            conn.sendall(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\n{\"a\": 1\r\n1\r\n}\r\n0\r\n\r\n")
        else:
            reply(conn, b"200 OK", {"a": 1})
        server.stall(conn)                                                # never closes
    server = RawServer(behave)
    try:
        api = real_api(server, tmp_path, timeout=2.0)
        for name in ("plain", "chunked"):
            began = time.monotonic()
            assert api.get_lease(CTL, name) == {"a": 1}
            assert time.monotonic() - began < 0.5, name
    finally:
        server.close()


def test_a_cut_malformed_or_oversized_answer_is_a_failed_call(tmp_path, monkeypatch):
    def behave(_server, conn, _method, path):
        if path.endswith("cut"):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 50\r\n\r\n{\"a\":")       # then closes
        elif path.endswith("garbage"):
            conn.sendall(b"not http at all\r\n\r\n")
        elif path.endswith("notjson"):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
        elif path.endswith("empty"):
            pass                                                           # closes without a byte
        else:
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n" + b"x" * 100000)
    server = RawServer(behave)
    try:
        api = real_api(server, tmp_path, timeout=1.0)
        monkeypatch.setattr(W, "MAX_ANSWER_BYTES", 1000)
        for name in ("cut", "garbage", "notjson", "empty", "big"):
            with pytest.raises(W.ApiError):
                api.get_lease(CTL, name)
    finally:
        server.close()


def test_the_client_refuses_an_address_or_timeout_it_cannot_bound(tmp_path):
    token = tmp_path / "token"
    token.write_text("t")
    for base in ("ftp://127.0.0.1", "127.0.0.1:6443", "https://"):
        with pytest.raises(ValueError):
            W.KubeApi(base, token_file=str(token), ca_file=None)
    for bad in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            W.KubeApi("http://127.0.0.1:1", token_file=str(token), ca_file=None, timeout=bad)


# --- third code review: no name resolution, real chunk framing, TLS --------------------------------------------------

def test_the_client_accepts_only_a_numeric_address(tmp_path):
    # A name would be resolved by a blocking call that no socket timeout bounds: refused before anything is armed.
    token = tmp_path / "token"
    token.write_text("t")
    for base in ("http://localhost:6443", "https://kubernetes.default.svc", "https://kubernetes.default.svc:443",
                 "http://example.invalid", "https://[fe80::1%25eth0]:443"):
        with pytest.raises(ValueError):
            W.KubeApi(base, token_file=str(token), ca_file=None)
    import socket
    v4 = W.KubeApi("http://10.43.0.1:443", token_file=str(token), ca_file=None)
    v6 = W.KubeApi("http://[fd00::1]:6443", token_file=str(token), ca_file=None)
    assert (v4.family, v4.address) == (socket.AF_INET, ("10.43.0.1", 443))
    assert (v6.family, v6.address) == (socket.AF_INET6, ("fd00::1", 6443, 0, 0))


def chunk(data):
    """One chunk, its size computed (hand-counted sizes were wrong twice in the first version of these tests)."""
    return b"%x\r\n" % len(data) + data + b"\r\n"


ZERO_INSIDE = b"0\r\n\r\n"            # five bytes of chunk DATA that look like the end of a chunked body


@pytest.mark.parametrize("body,whole", [
    (b"5\r\nhello\r\n0\r\n\r\n", True),
    (b"5\r\nhello\r\n0\r\n", False),                                  # the zero chunk's trailer section not ended
    (b"5\r\nhello\r\n", False),
    (b"5\r\nhel", False),
    (b"", False),
    (chunk(ZERO_INSIDE), False),                                      # "0 CR LF CR LF" is DATA of this chunk
    (chunk(ZERO_INSIDE)[:-2], False),                                 # ...also when the bytes stop right after it
    (chunk(ZERO_INSIDE) + b"0\r\n\r\n", True),
    (b"5\r\nhello\r\n0\r\nX-Trailer: v\r\n\r\n", True),               # trailers, then the empty line
    (b"5\r\nhello\r\n0\r\nX-Trailer: v\r\n", False),
    (b"5;ext=1\r\nhello\r\n0\r\n\r\n", True),                         # a chunk extension
    (b"A\r\n0123456789\r\n0\r\n\r\n", True),                          # hexadecimal size
])
def test_chunk_framing_is_parsed_not_guessed(body, whole):
    assert W._chunked_complete(body) is whole


@pytest.mark.parametrize("body", [b"zz\r\nhello\r\n0\r\n\r\n", b"-1\r\n\r\n", b"0x5\r\nhello\r\n", b"+5\r\nhello\r\n",
                                  b"\r\n", b"5\r\nhelloXX0\r\n\r\n"])
def test_malformed_chunk_framing_is_a_failed_call(body):
    with pytest.raises(W.ApiError):
        W._chunked_complete(body)


def test_chunked_answers_end_exactly_where_the_framing_says(tmp_path):
    import time

    def behave(server, conn, _method, path):
        head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        if path.endswith("early"):
            # The first chunk's DATA ends with "0 CR LF CR LF" (JSON whitespace after the number 10) and the answer
            # pauses right there. Ending the read here cut the answer short (IncompleteRead).
            first = chunk(b"{\"a\": 10\r\n\r\n")
            assert first.endswith(b"0\r\n\r\n\r\n")
            conn.sendall(head + first[:-2])                                # stops right after "...0 CR LF CR LF"
            time.sleep(0.15)
            conn.sendall(first[-2:] + chunk(b"}") + b"0\r\n\r\n")
        elif path.endswith("trailers"):
            # Whole, with trailers, on a connection the server keeps open: this waited for the deadline.
            conn.sendall(head + chunk(b"{\"a\": 1}") + b"0\r\nX-Trailer: v\r\n\r\n")
        elif path.endswith("both"):
            # Content-Length listed BEFORE Transfer-Encoding: chunked decides.
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n"
                         + chunk(b"{\"a\": 1}") + b"0\r\n\r\n")
        else:
            conn.sendall(head + b"zz\r\n{\"a\": 1}\r\n0\r\n\r\n")       # malformed size
        server.stall(conn)                                                # the connection stays open
    server = RawServer(behave)
    try:
        api = real_api(server, tmp_path, timeout=2.0)
        assert api.get_lease(CTL, "early") == {"a": 10}
        for name in ("trailers", "both"):
            began = time.monotonic()
            assert api.get_lease(CTL, name) == {"a": 1}
            assert time.monotonic() - began < 0.5, name
        began = time.monotonic()
        with pytest.raises(W.ApiError):
            api.get_lease(CTL, "malformed")
        assert time.monotonic() - began < 0.5                             # refused at once, not at the deadline
    finally:
        server.close()


@pytest.fixture(scope="module")
def certs(tmp_path_factory):
    """A CA, a server certificate for IP 127.0.0.1, one for another IP, and one from another CA (openssl CLI)."""
    import os
    import shutil
    import subprocess
    if shutil.which("openssl") is None:
        if os.environ.get("CI"):
            pytest.fail("openssl is required in CI: the TLS checks of the watchdog's client must not be skipped")
        pytest.skip("openssl not available to make throwaway certificates")
    d = tmp_path_factory.mktemp("tls")

    def run(*args):
        subprocess.run(["openssl", *args], check=True, capture_output=True, cwd=d, timeout=60)

    def ca(name):
        run("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-keyout",
            f"{name}.key", "-out", f"{name}.crt", "-subj", f"/CN={name}", "-days", "2",
            "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign")

    def leaf(name, issuer, san):
        (d / f"{name}.ext").write_text(f"subjectAltName={san}\nbasicConstraints=CA:FALSE\n")
        run("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-keyout", f"{name}.key",
            "-out", f"{name}.csr", "-subj", f"/CN={name}")
        run("x509", "-req", "-in", f"{name}.csr", "-CA", f"{issuer}.crt", "-CAkey", f"{issuer}.key", "-CAcreateserial",
            "-out", f"{name}.crt", "-days", "2", "-extfile", f"{name}.ext")
    ca("ca")
    ca("other-ca")
    leaf("good", "ca", "IP:127.0.0.1")
    leaf("wrong-ip", "ca", "IP:127.0.0.2")
    leaf("name-only", "ca", "DNS:kubernetes.default.svc")
    leaf("foreign", "other-ca", "IP:127.0.0.1")
    return d


def tls_server(certs, name):
    """An HTTPS server on 127.0.0.1 answering every request with {"ok": true}; returns (port, requests, close)."""
    import socket
    import ssl
    import threading
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(certs / f"{name}.crt"), str(certs / f"{name}.key"))
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    requests, stop = [], threading.Event()

    def serve():
        while not stop.is_set():
            try:
                raw, _ = listener.accept()
            except OSError:
                return
            try:
                raw.settimeout(2)
                conn = ctx.wrap_socket(raw, server_side=True)
                head = b""
                while b"\r\n\r\n" not in head:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    head += chunk
                requests.append(head.split(b"\r\n", 1)[0].decode())
                body = b'{"ok": true}'
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
                conn.close()
            except (OSError, ssl.SSLError):                               # the client refused the certificate
                raw.close()
    threading.Thread(target=serve, daemon=True).start()

    def close():
        stop.set()
        listener.close()
    return listener.getsockname()[1], requests, close


def test_tls_a_trusted_certificate_for_the_address_is_accepted(certs, tmp_path):
    token = tmp_path / "token"
    token.write_text("tok")
    port, requests, close = tls_server(certs, "good")
    try:
        api = W.KubeApi(f"https://127.0.0.1:{port}", token_file=str(token), ca_file=str(certs / "ca.crt"), timeout=2.0)
        assert api.get_binding(NS, BINDING) == {"ok": True}
        assert requests and requests[0].startswith("GET /apis/rbac.authorization.k8s.io/v1/namespaces/demo/")
    finally:
        close()


@pytest.mark.parametrize("cert,why", [("wrong-ip", "the certificate is for another address"),
                                      ("name-only", "the certificate names no address at all"),
                                      ("foreign", "the certificate is from another CA")])
def test_tls_a_certificate_that_does_not_prove_the_address_is_refused(certs, tmp_path, cert, why):
    token = tmp_path / "token"
    token.write_text("tok")
    port, requests, close = tls_server(certs, cert)
    try:
        api = W.KubeApi(f"https://127.0.0.1:{port}", token_file=str(token), ca_file=str(certs / "ca.crt"), timeout=2.0)
        with pytest.raises(W.ApiError):
            api.get_binding(NS, BINDING)
        assert requests == [], why                                        # no request, so no token, was ever sent
    finally:
        close()


def test_tls_a_stalled_handshake_ends_at_the_total_deadline(certs, tmp_path):
    import socket
    import threading
    import time
    token = tmp_path / "token"
    token.write_text("tok")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    held = []

    def accept():                                                         # accepts, never speaks TLS
        while True:
            try:
                held.append(listener.accept()[0])
            except OSError:
                return
    threading.Thread(target=accept, daemon=True).start()
    try:
        api = W.KubeApi(f"https://127.0.0.1:{listener.getsockname()[1]}", token_file=str(token),
                        ca_file=str(certs / "ca.crt"), timeout=0.2)
        for _ in range(5):
            began = time.monotonic()
            with pytest.raises(W.ApiError):
                api.get_lease(CTL, TRIGGER)
            assert time.monotonic() - began < 0.5
    finally:
        listener.close()
        for c in held:
            c.close()


def test_tls_needs_a_ca_file(tmp_path):
    token = tmp_path / "token"
    token.write_text("t")
    with pytest.raises((ValueError, OSError)):                            # no CA: never an unverified connection
        W.KubeApi("https://127.0.0.1:6443", token_file=str(token), ca_file=str(tmp_path / "absent.crt"))
