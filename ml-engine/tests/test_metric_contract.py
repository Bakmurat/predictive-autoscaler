"""B4b-1: the forecasting service's side of the metric contract (strict history, identity resolution, model records)."""

import dataclasses
import hashlib
import json
import os
import socket
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.identity import CONTRACT, KubeReader, LookupFailed, ProvenanceRefused, ResolvedSignal, resolve_signal  # noqa: E402
from api.registry import ModelIncompatible, ModelRegistry, load_record, model_key  # noqa: E402
from data.bounded_http import BodyTooLarge, DeadlineExceeded, TransportFailure, bounded_get  # noqa: E402
from data.history import HistoryRefused, HistoryUnavailable, query_history  # noqa: E402

NOW = datetime(2026, 10, 9, 12, 5, tzinfo=timezone.utc)   # grid end: 12:00
END = int(datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc).timestamp())
QUERY = 'sum(rate(istio_requests_total{destination_workload="web"}[1m]))'
SHA = hashlib.sha256(QUERY.encode()).hexdigest()


# --- strict history ---------------------------------------------------------------------------------------------

class FakeFetch:
    def __init__(self, status=200, body=b"", exc=None):
        self.status, self.body, self.exc, self.calls = status, body, exc, []

    def __call__(self, url, **kw):
        self.calls.append((url, kw))
        if self.exc:
            raise self.exc
        return self.status, self.body


def matrix(values, series=1, **extra):
    doc = {"status": "success", "data": {"resultType": "matrix",
                                         "result": [{"values": values} for _ in range(series)]}}
    doc.update(extra)
    return json.dumps(doc).encode()


def test_history_converts_once_and_keeps_gaps():
    f = FakeFetch(body=matrix([[END - 1200, "5"], [END, "0.5"]]))  # END-600 missing: a gap
    out = query_history("http://vm", QUERY, now=NOW, fetch=f)
    assert out == [{"timestamp": "2026-10-09T11:40:00", "value": 300.0},
                   {"timestamp": "2026-10-09T12:00:00", "value": 30.0}]
    url, kw = f.calls[0]
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
    assert url.startswith("http://vm/api/v1/query_range?") and q["query"] == QUERY and q["step"] == "600s"
    assert int(q["end"]) == END and int(q["start"]) == END - 168 * 3600 and q["deny_partial_response"] == "1"
    assert kw == {"deadline_s": 20.0, "max_bytes": 8 << 20}


def test_history_converts_an_aware_non_utc_now():
    plus2 = timezone(timedelta(hours=2))
    f = FakeFetch(body=matrix([[END, "1"]]))
    query_history("http://vm", QUERY, now=datetime(2026, 10, 9, 14, 5, tzinfo=plus2), fetch=f)  # = 12:05 UTC
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(f.calls[0][0]).query))
    assert int(q["end"]) == END


@pytest.mark.parametrize("body", [
    matrix([[END, "1"]], series=2),                       # several series are never averaged
    json.dumps({"status": "success", "data": {"resultType": "matrix", "result": []}}).encode(),
    matrix([[END, "1"]], isPartial=True),
    json.dumps({"status": "error"}).encode(),
    json.dumps({"status": "success", "data": {"resultType": "vector", "result": []}}).encode(),
    json.dumps({"status": "success", "data": [1]}).encode(),                                   # malformed shapes
    json.dumps({"status": "success", "data": {"resultType": "matrix", "result": [1]}}).encode(),
    json.dumps({"status": "success", "data": {"resultType": "matrix", "result": [{"values": 7}]}}).encode(),
    json.dumps([1]).encode(),
    matrix([[END, True]]), matrix([[True, "1"]]),         # booleans are not numbers
    matrix([[END - 300, "1"]]),                           # off the 10-minute grid
    matrix([[END, "1"], [END, "2"]]),                     # duplicate timestamp
    matrix([[END, "1"], [END - 600, "2"]]),               # unordered
    matrix([[END, "-1"]]), matrix([[END, "NaN"]]), matrix([[END, "+Inf"]]), matrix([[END, "x"]]),
    matrix([[END, "1e308"]]),                             # finite per second, infinite per minute
    matrix([[END + 600, "1"]]),                           # outside the window
    b"not json",
])
def test_history_refuses_anything_but_one_valid_series(body):
    with pytest.raises(HistoryRefused):
        query_history("http://vm", QUERY, now=NOW, fetch=FakeFetch(body=body))


@pytest.mark.parametrize("fetch,kind", [
    (FakeFetch(status=503), HistoryUnavailable),
    (FakeFetch(status=403), HistoryUnavailable),
    (FakeFetch(status=400), HistoryRefused),
    (FakeFetch(exc=TransportFailure("refused")), HistoryUnavailable),
    (FakeFetch(exc=DeadlineExceeded("slow")), HistoryUnavailable),
    (FakeFetch(exc=BodyTooLarge("big")), HistoryRefused),
])
def test_history_maps_transport_outcomes(fetch, kind):
    with pytest.raises(kind):
        query_history("http://vm", QUERY, now=NOW, fetch=fetch)


# --- the bounded exchange against real sockets (Codex r15 BLOCKER: the deadline bounds the whole exchange) ---------

def serve(handler):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(4)
    stop = threading.Event()

    def loop():
        srv.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
            except OSError:
                continue
            threading.Thread(target=handler, args=(conn, stop), daemon=True).start()

    threading.Thread(target=loop, daemon=True).start()
    return f"http://127.0.0.1:{srv.getsockname()[1]}", lambda: (stop.set(), srv.close())


def _drain_request(conn):
    conn.settimeout(2)
    try:
        conn.recv(65536)
    except OSError:
        pass


def test_bounded_get_returns_a_normal_answer():
    def h(conn, stop):
        _drain_request(conn)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        conn.close()
    url, close = serve(h)
    try:
        assert bounded_get(url + "/x", deadline_s=2, max_bytes=100) == (200, b"ok")
    finally:
        close()


@pytest.mark.parametrize("mode", ["silent", "trickle_body", "trickle_body_close", "trickle_headers"])
def test_bounded_get_ends_at_the_deadline(mode):
    def h(conn, stop):
        _drain_request(conn)
        try:
            if mode == "trickle_body":
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n")
            if mode == "trickle_body_close":  # http.client hands the socket to the response (Codex r16)
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n")
            if mode == "trickle_headers":
                conn.sendall(b"HTTP/1.1 200 OK\r\n")
            give_up = time.monotonic() + 3  # bounded, so a broken client deadline fails the timing assertion, not hangs
            while not stop.is_set() and time.monotonic() < give_up:
                if mode != "silent":
                    conn.sendall(b"X-Slow: 1\r\n" if mode == "trickle_headers" else b"x")
                time.sleep(0.05)
        except OSError:
            pass
        finally:
            conn.close()
    url, close = serve(h)
    try:
        t0 = time.monotonic()
        with pytest.raises(DeadlineExceeded):
            bounded_get(url + "/x", deadline_s=0.4, max_bytes=10 << 20)
        assert time.monotonic() - t0 < 1.5, "the deadline must bound the whole exchange"
    finally:
        close()


def test_bounded_get_caps_the_body():
    def h(conn, stop):
        _drain_request(conn)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5000\r\nConnection: close\r\n\r\n" + b"y" * 5000)
        conn.close()
    url, close = serve(h)
    try:
        with pytest.raises(BodyTooLarge):
            bounded_get(url + "/x", deadline_s=2, max_bytes=1000)
    finally:
        close()


# --- identity resolution ----------------------------------------------------------------------------------------

def pa_object(**over):
    obj = {"metadata": {"uid": "pa-uid", "generation": 3},
           "spec": {"targetDeployment": {"name": "web", "namespace": "shop"}},
           "status": {"metricSource": {"query": QUERY, "sha256": SHA, "observedGeneration": 3,
                                       "targetUID": "dep-uid", "contract": CONTRACT}}}
    for path, value in over.items():
        node, *rest = path.split(".")
        cur = obj[node]
        for p in rest[:-1]:
            cur = cur[p]
        if value is None:
            cur.pop(rest[-1], None)
        else:
            cur[rest[-1]] = value
    return obj


class FakeReader:
    def __init__(self, pa=None, dep=None, fail=None):
        self.objs = {"pa": pa if pa is not None else pa_object(), "dep": dep if dep is not None else {"metadata": {"uid": "dep-uid"}}}
        self.fail, self.paths = fail, []

    def get(self, path):
        self.paths.append(path)
        if self.fail:
            raise self.fail
        kind = "pa" if "predictiveautoscalers" in path else "dep"
        return self.objs[kind] if self.objs[kind] != "missing" else None


def request(**over):
    r = {"application": "web", "namespace": "shop", "metric_type": "requests", "autoscaler_name": "web-pa",
         "autoscaler_namespace": "shop", "autoscaler_uid": "pa-uid", "autoscaler_generation": 3,
         "target_uid": "dep-uid", "metric_query_sha256": SHA, "contract": CONTRACT}
    r.update(over)
    return {k: v for k, v in r.items() if v is not None}


def test_identity_resolves_the_compiled_query():
    reader = FakeReader()
    sig = resolve_signal(request(), reader)
    assert sig == ResolvedSignal(namespace="shop", name="web", metric="requests", query=QUERY, sha256=SHA,
                                 contract=CONTRACT, autoscaler_uid="pa-uid", target_uid="dep-uid", generation=3)
    assert reader.paths == ["/apis/autoscaler.example.com/v1alpha1/namespaces/shop/predictiveautoscalers/web-pa",
                            "/apis/apps/v1/namespaces/shop/deployments/web"]


@pytest.mark.parametrize("req,reader", [
    (request(metric_query_sha256=None), FakeReader()),                       # no reference: raw history is gone
    (request(contract="requests-per-minute/v0"), FakeReader()),
    (request(autoscaler_generation=2), FakeReader()),                        # request behind the object
    (request(), FakeReader(pa=pa_object(**{"status.metricSource.observedGeneration": 2}))),  # status behind
    (request(), FakeReader(pa=pa_object(**{"metadata.generation": 4}))),     # object ahead of status and request
    (request(autoscaler_uid="other"), FakeReader()),                         # the autoscaler was re-created
    (request(), FakeReader(pa=pa_object(**{"status.metricSource": None}))),
    (request(), FakeReader(pa=pa_object(**{"status.metricSource.sha256": "0" * 64}))),  # status hash != its query
    (request(metric_query_sha256="0" * 64), FakeReader()),
    (request(), FakeReader(pa=pa_object(**{"status.metricSource.contract": "x"}))),
    (request(), FakeReader(dep={"metadata": {"uid": "new-dep"}})),           # target re-created: live UID differs
    (request(target_uid="new-dep"), FakeReader(dep={"metadata": {"uid": "new-dep"}})),  # status still old
    (request(), FakeReader(pa=pa_object(**{"spec.targetDeployment.namespace": "other"}))),
    (request(application="api"), FakeReader()),
    (request(namespace="other"), FakeReader()),
    (request(autoscaler_namespace="Bad_NS"), FakeReader()),
    (request(), FakeReader(pa="missing")),
    (request(), FakeReader(dep="missing")),
])
def test_identity_refuses_every_mismatch(req, reader):
    with pytest.raises(ProvenanceRefused):
        resolve_signal(req, reader)


def test_lookup_failures_are_not_refusals():
    with pytest.raises(LookupFailed):
        resolve_signal(request(), FakeReader(fail=LookupFailed("api down")))


def test_kube_reader_rereads_the_token_and_maps_statuses(tmp_path):
    (tmp_path / "token").write_text("t1")
    seen, answer = [], {"status": 200, "body": b'{"ok": true}'}

    def fetch(url, headers=None, deadline_s=None, max_bytes=None, ssl_context=None):
        seen.append((headers["Authorization"], deadline_s, max_bytes))
        if isinstance(answer["status"], Exception):
            raise answer["status"]
        return answer["status"], answer["body"]

    kr = KubeReader(host="https://k8s:443", sa_dir=str(tmp_path), fetch=fetch)
    assert kr.get("/x") == {"ok": True}
    (tmp_path / "token").write_text("t2")  # rotation
    kr.get("/x")
    assert [s[0] for s in seen] == ["Bearer t1", "Bearer t2"] and seen[0][1:] == (8.0, 1 << 20)
    answer["status"] = 404
    assert kr.get("/x") is None
    for status in (401, 403, 500, TransportFailure("down"), DeadlineExceeded("slow"), BodyTooLarge("big")):
        answer["status"] = status
        with pytest.raises(LookupFailed):
            kr.get("/x")
    answer["status"], answer["body"] = 200, b"[1]"
    with pytest.raises(LookupFailed):
        kr.get("/x")


@pytest.mark.parametrize("pa,dep", [
    ({"metadata": [1]}, None),
    (pa_object(**{"status.metricSource": "x"}), None),
    (pa_object(**{"spec.targetDeployment": "web"}), None),
    (None, {"metadata": "x"}),
    (None, {"metadata": {}}),          # a Deployment without a UID
])
def test_malformed_objects_are_refused(pa, dep):
    with pytest.raises(ProvenanceRefused):
        resolve_signal(request(), FakeReader(pa=pa, dep=dep))


# --- model records ----------------------------------------------------------------------------------------------

def write_artifact(d, payload=b"model-bytes", **meta_over):
    key = model_key("shop", "web", "requests")
    meta = {"namespace": "shop", "name": "web", "metric": "requests", "metric_query_sha256": SHA,
            "contract": CONTRACT, "pa_uid": "pa-uid", "target_uid": "dep-uid",
            "artifact_sha256": hashlib.sha256(payload).hexdigest(), "trained_at": "2026-10-09T06:00:00"}
    meta.update(meta_over)
    meta = {k: v for k, v in meta.items() if v is not None}
    (d / f"lstm_{key}.pkl").write_bytes(payload)
    (d / f"lstm_{key}.meta.json").write_text(json.dumps(meta))
    return d / f"lstm_{key}.pkl"


SIGNAL = ResolvedSignal(namespace="shop", name="web", metric="requests", query=QUERY, sha256=SHA, contract=CONTRACT,
                        autoscaler_uid="pa-uid", target_uid="dep-uid", generation=3)


def test_a_complete_record_loads_and_matches_its_signal(tmp_path):
    rec = load_record(write_artifact(tmp_path), loader=lambda b: ("model", b))
    assert rec.model == ("model", b"model-bytes") and rec.artifact_sha256 == hashlib.sha256(b"model-bytes").hexdigest()
    assert rec.incompatibility(SIGNAL) is None
    for field, value in (("sha256", "0" * 64), ("target_uid", "new-dep"), ("autoscaler_uid", "other"),
                         ("contract", "x")):
        assert rec.incompatibility(SIGNAL.__class__(**{**SIGNAL.__dict__, field: value})) is not None


@pytest.mark.parametrize("over", [
    {"metric_query_sha256": None}, {"contract": None}, {"pa_uid": None}, {"target_uid": None},
    {"artifact_sha256": hashlib.sha256(b"other").hexdigest()},   # replaced during loading
    {"name": "api"},                                               # sidecar identity != file key
    {"contract": "requests-per-minute/v0"}, {"metric_query_sha256": "abc"},
])
def test_incomplete_or_inconsistent_artifacts_are_not_loaded(tmp_path, over):
    with pytest.raises(ModelIncompatible):
        load_record(write_artifact(tmp_path, **over), loader=lambda b: b)


def test_legacy_artifacts_are_not_loaded(tmp_path):
    (tmp_path / "lstm_web_requests.pkl").write_bytes(b"x")
    with pytest.raises(ModelIncompatible):
        load_record(tmp_path / "lstm_web_requests.pkl", loader=lambda b: b)


def test_a_reload_never_cleans_up_a_record_in_use(tmp_path):
    cleaned = []
    reg = ModelRegistry(cleanup=cleaned.append)
    a = load_record(write_artifact(tmp_path, b"a"), loader=lambda b: b)
    reg.install(a)
    with reg.pin(a.key) as pinned:
        assert pinned is a
        b = load_record(write_artifact(tmp_path, b"b"), loader=lambda b: b)
        reg.install(b)                 # a reload while the inference runs
        assert cleaned == [] and pinned.model == b"a" and pinned.artifact_sha256 == a.artifact_sha256
        with reg.pin(a.key) as newer:
            assert newer is b          # new requests see the new record
    assert cleaned == [a]              # cleaned once, after its last user
    reg.remove(b.key)
    assert cleaned == [a, b]


def test_pins_from_many_threads_clean_up_exactly_once(tmp_path):
    cleaned = []
    reg = ModelRegistry(cleanup=cleaned.append)
    a = load_record(write_artifact(tmp_path, b"a"), loader=lambda b: b)
    reg.install(a)
    started, release = threading.Barrier(9, timeout=10), threading.Event()

    def user():
        with reg.pin(a.key):
            started.wait()
            release.wait()

    threads = [threading.Thread(target=user, daemon=True) for _ in range(8)]
    for t in threads:
        t.start()
    try:
        started.wait(timeout=10)
        reg.remove(a.key)
        assert cleaned == []
    finally:
        release.set()  # never leave the users blocked, even when an assertion fails
        for t in threads:
            t.join(timeout=10)
    assert cleaned == [a]


def test_a_retired_record_can_never_be_installed_again(tmp_path):
    # Codex r15: install A, pin A, install B (A retired), reinstall A, release the pin, remove A.
    cleaned = []
    reg = ModelRegistry(cleanup=cleaned.append)
    a = load_record(write_artifact(tmp_path, b"a"), loader=lambda b: b)
    reg.install(a)
    with reg.pin(a.key):
        b = load_record(write_artifact(tmp_path, b"b"), loader=lambda b: b)
        reg.install(b)
        with pytest.raises(ValueError):
            reg.install(a)      # retirement is terminal: reload the artifact into a fresh record instead
        with pytest.raises(ValueError):
            reg.install(b)      # an installed record is installed once
    assert cleaned == [a]
    fresh_a = load_record(write_artifact(tmp_path, b"a"), loader=lambda b: b)
    reg.install(fresh_a)
    assert cleaned == [a, b]
    reg.remove(fresh_a.key)
    assert cleaned == [a, b, fresh_a]


def test_provenance_is_immutable(tmp_path):
    rec = load_record(write_artifact(tmp_path), loader=lambda b: b)
    with pytest.raises(TypeError):
        rec.meta["metric_query_sha256"] = "0" * 64
    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.namespace = "other"
    assert rec.incompatibility(SIGNAL) is None


def _one_shot(payload: bytes):
    def h(conn, stop):
        _drain_request(conn)
        try:
            conn.sendall(payload)
        finally:
            conn.close()
    return serve(h)


@pytest.mark.parametrize("payload", [
    # Codex r16: a declared length not delivered (a valid JSON prefix, then EOF) is not a complete answer
    b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\nConnection: close\r\n\r\n" + matrix([[END, "1"]]),
    b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n" + matrix([[END, "1"]]),           # unframed
    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel",                    # chunk cut short
])
def test_incomplete_framing_is_a_transport_failure(payload):
    url, close = _one_shot(payload)
    try:
        with pytest.raises(TransportFailure):
            bounded_get(url + "/x", deadline_s=2, max_bytes=1 << 20)
        with pytest.raises(HistoryUnavailable):
            query_history(url, QUERY, now=NOW)
    finally:
        close()


def test_a_complete_chunked_answer_is_accepted():
    body = matrix([[END, "2"]])
    payload = (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
               + f"{len(body):x}".encode() + b"\r\n" + body + b"\r\n0\r\n\r\n")
    url, close = _one_shot(payload)
    try:
        assert query_history(url, QUERY, now=NOW) == [{"timestamp": "2026-10-09T12:00:00", "value": 120.0}]
    finally:
        close()


def test_slow_name_resolution_ends_at_the_deadline(monkeypatch):
    import data.bounded_http as bh

    def slow(*a, **kw):
        time.sleep(2)
        return socket.getaddrinfo(*a, **kw)

    monkeypatch.setattr(bh, "_getaddrinfo", slow)
    t0 = time.monotonic()
    with pytest.raises(DeadlineExceeded):
        bounded_get("http://slow.invalid:80/x", deadline_s=0.3, max_bytes=100)
    assert time.monotonic() - t0 < 1.0


def test_the_deadline_interrupts_a_tls_header_trickle(tmp_path):
    # Over TLS, wrap_socket detaches the connected socket, so only the watchdog's duplicate descriptor can still shut
    # the connection down while getresponse() blocks across many small reads.
    import shutil
    import ssl
    import subprocess
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available to make a throwaway certificate")
    key, cert = tmp_path / "k.pem", tmp_path / "c.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
                    "-keyout", str(key), "-out", str(cert), "-days", "1"], check=True, capture_output=True)
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(str(cert), str(key))

    def h(conn, stop):
        try:
            tls = server_ctx.wrap_socket(conn, server_side=True)
            tls.settimeout(2)
            tls.recv(65536)
            tls.sendall(b"HTTP/1.1 200 OK\r\n")
            give_up = time.monotonic() + 3
            while not stop.is_set() and time.monotonic() < give_up:
                tls.sendall(b"X-Slow: 1\r\n")
                time.sleep(0.05)
            tls.close()
        except (OSError, ssl.SSLError):
            pass

    url, close = serve(h)
    client_ctx = ssl.create_default_context(cafile=str(cert))
    client_ctx.check_hostname = False
    try:
        t0 = time.monotonic()
        with pytest.raises(DeadlineExceeded):
            bounded_get(url.replace("http://", "https://") + "/x", deadline_s=0.5, max_bytes=1 << 20,
                        ssl_context=client_ctx)
        assert time.monotonic() - t0 < 1.5, "the deadline must interrupt a TLS read in progress"
    finally:
        close()


def test_abandoned_lookups_never_queue_up(monkeypatch):
    # Codex r17: lookups abandoned at the deadline keep their slot until they finish; when every slot is taken the
    # call fails at once instead of queueing another lookup.
    import data.bounded_http as bh
    release, executed = threading.Event(), []

    def stuck(*a, **kw):
        executed.append(a[0])
        release.wait(10)
        return socket.getaddrinfo("127.0.0.1", 80, 0, socket.SOCK_STREAM)

    wait_for_free_resolver_slots(bh)   # an earlier test's slow lookup may still hold one
    monkeypatch.setattr(bh, "_getaddrinfo", stuck)
    try:
        for _ in range(bh.RESOLVER_SLOTS):
            with pytest.raises(DeadlineExceeded):
                bounded_get("http://stuck.invalid/x", deadline_s=0.05, max_bytes=100)
        for _ in range(5):   # repeated timeouts: refused at once, nothing queued behind the stuck lookups
            t0 = time.monotonic()
            with pytest.raises(TransportFailure) as exc:
                bounded_get("http://stuck.invalid/x", deadline_s=0.5, max_bytes=100)
            assert not isinstance(exc.value, DeadlineExceeded) and time.monotonic() - t0 < 0.2
        assert len(executed) == bh.RESOLVER_SLOTS
    finally:
        release.set()
    wait_for_free_resolver_slots(bh)   # slots return only when the stuck lookups really end
    assert len(executed) == bh.RESOLVER_SLOTS


def wait_for_free_resolver_slots(bh, timeout=10.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        taken = []
        while bh._resolver_slots.acquire(blocking=False):
            taken.append(1)
        for _ in taken:
            bh._resolver_slots.release()
        if len(taken) == bh.RESOLVER_SLOTS:
            return
        time.sleep(0.02)
    raise AssertionError("resolver slots did not come back")


def test_the_next_address_is_tried_when_the_first_is_unreachable(monkeypatch):
    import data.bounded_http as bh

    def h(conn, stop):
        _drain_request(conn)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        conn.close()
    url, close = serve(h)
    port = int(url.rsplit(":", 1)[1])
    probe = socket.socket()         # a free port with nothing bound: connections are refused at once
    probe.bind(("127.0.0.1", 0))
    first = ("127.0.0.1", probe.getsockname()[1])
    probe.close()
    monkeypatch.setattr(bh, "_getaddrinfo", lambda *a, **kw: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", first),
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))])
    try:
        assert bounded_get(f"http://twohomes.invalid:{port}/x", deadline_s=2, max_bytes=100) == (200, b"ok")
    finally:
        close()
