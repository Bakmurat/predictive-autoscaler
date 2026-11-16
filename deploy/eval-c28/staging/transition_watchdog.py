#!/usr/bin/env python3
"""The in-cluster watchdog of the Active transition (C2.8 demonstration, TRANSITION-DESIGN draft 6, section 3a).

It bounds an Active period when the executor's host is gone. It can do exactly two things: delete ONE RoleBinding (the
release's <release>-operator-scale, the chart's grant of the replica write) and move ONE Lease forward. It cannot grant
anything, write a Scale or change an autoscaler.

Standard library only: it runs from the forecaster image's Python with this file mounted from a ConfigMap.

The trigger Lease is a one-way state machine in spec.holderIdentity:

    unarmed:<activation id>   written by the executor when it creates the Lease
    armed:<nonce>             claimed by ONE watchdog process (a fresh nonce per process start)
    triggered:<reason>        final; nothing clears it

Rules this file enforces (each has a test in tests/test_transition_watchdog.py):

- Start fails closed. Only "unarmed:<my activation>" on the expected Lease UID is armed and monitored, and only after
  the arming is CONFIRMED by a re-read (a lost response is settled by that re-read; anything uncertain is revocation).
  "armed:<another nonce>" means an earlier process was interrupted, possibly in the middle of a trigger whose record
  failed: this process records triggered:watchdog-restart and revokes, however fresh the heartbeat is. A missing,
  replaced, unreadable or unexpected Lease, or one of another activation, is revocation at once and is never written.
- ONE validation decides whether a Lease is the trusted one of this activation, and every read AND every write uses
  it. Every write is conditional (resourceVersion) and refuses to move the state backwards or sideways, so conflicts
  and overlapping processes cannot undo a trigger.
- The trigger is recorded BEFORE the deletion with ONE bounded attempt; a failed record never holds back the deletion
  and is retried on every pass afterwards. "Recorded" is what the Lease shows on the current pass, never a memory of
  an earlier write: a Lease that no longer reads triggered is recorded again, and is never renewed as it stands.
- A trigger Lease that cannot be read while monitoring is revocation too: a failed Lease read says nothing about
  whether the RoleBinding can be deleted.
- Every API call has a TOTAL deadline (elapsed time for the whole call, not a per-read socket timeout) and runs on one
  socket in the calling thread: an answer that trickles in, headers or body, cannot stretch a call, and nothing is
  left running after one. Calls that time out cannot pile up or take anything the deletion needs. The API is reached
  by a numeric address only (a name would be resolved by a blocking call that no timeout bounds).
- The duration limit is the absolute deadline AND the same remaining time on this process's monotonic clock, so a wall
  clock stepped backwards cannot extend it. Non-finite or non-positive timing values are refused at start.
- The heartbeat is a spec.renewTime that is a valid timestamp and ADVANCED since this process last looked, timed on
  this process's own monotonic clock (no comparison of two hosts' clocks). A changed resourceVersion alone, an
  unparseable renewTime or one that went backwards is no heartbeat. A heartbeat that resumes after a trigger is ignored.
- Revocation mode has no successful end. The process keeps deleting a binding that reappears and reports "absent" as an
  observation. Only a reviewed recovery removes the watchdog.
"""
import argparse
import datetime
import http.client
import io
import ipaddress
import json
import math
import os
import re
import secrets
import socket
import ssl
import sys
import time
import urllib.parse

MONITORING, REVOCATION = "monitoring", "revocation"
RANK = {"unarmed": 0, "armed": 1, "triggered": 2}
READ_ATTEMPTS = 3              # bounded reads of the trigger Lease before it counts as unreadable
CALL_TIMEOUT_SECONDS = 2.0     # the TOTAL time one API call may take
MAX_ANSWER_BYTES = 4 * 1024 * 1024
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"


class ApiError(Exception):
    """A call that failed or whose outcome is unknown (deadline, connection, 5xx, 403)."""


class Conflict(ApiError):
    """409: the object changed since it was read (or a delete precondition did not hold)."""


def parse_holder(holder):
    """("unarmed"|"armed"|"triggered", value) or None for anything else."""
    if not isinstance(holder, str) or ":" not in holder:
        return None
    state, value = holder.split(":", 1)
    if state not in RANK or not value or any(c.isspace() for c in value):
        return None
    return state, value


def forward(current, new):
    """True when holderIdentity may move from current to new: never backwards, never sideways."""
    c, n = parse_holder(current), parse_holder(new)
    if c is None or n is None:
        return False
    if c == n:
        return True                                   # a renewal keeps the state exactly
    return RANK[n[0]] > RANK[c[0]]                    # armed -> armed:<other> and triggered -> triggered:<other> refused


def renew_stamp(lease):
    """spec.renewTime as an aware datetime, or None when it is absent or not a timestamp."""
    value = ((lease or {}).get("spec") or {}).get("renewTime")
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp if stamp.tzinfo is not None else None


def micro_time(epoch):
    t = time.gmtime(int(epoch))
    return time.strftime("%Y-%m-%dT%H:%M:%S", t) + ".%06dZ" % int((epoch - int(epoch)) * 1_000_000)


class _Buffered:
    """A received answer, shaped like a socket for http.client's parser."""

    def __init__(self, raw):
        self._file = io.BytesIO(raw)

    def makefile(self, *_args, **_kwargs):
        return self._file


HEX, DIGITS = re.compile(rb"[0-9A-Fa-f]+\Z"), re.compile(r"[0-9]+\Z")


def _chunked_complete(body):
    """True when a chunked body is whole: every chunk by its size line, the zero chunk, and the trailer section up
    to its empty line. The framing is parsed; the bytes "0 CR LF CR LF" inside a chunk's data do not end anything, and
    trailers after the zero chunk do not hide the end."""
    pos = 0
    while True:
        eol = body.find(b"\r\n", pos)
        if eol < 0:
            return False
        size_token = body[pos:eol].split(b";", 1)[0].strip()
        if not HEX.match(size_token):
            raise ApiError("malformed chunked answer (chunk size)")
        size, pos = int(size_token, 16), eol + 2
        if size == 0:
            while True:                              # trailer lines, ended by an empty line
                eol = body.find(b"\r\n", pos)
                if eol < 0:
                    return False
                if eol == pos:
                    return True
                pos = eol + 2
        if len(body) < pos + size + 2:
            return False
        if body[pos + size:pos + size + 2] != b"\r\n":
            raise ApiError("malformed chunked answer (chunk end)")
        pos += size + 2


def _answer_complete(raw):
    """True when the bytes hold a whole HTTP answer, so the call need not wait for the server to close."""
    end = raw.find(b"\r\n\r\n")
    if end < 0:
        return False
    headers, body = {}, bytes(raw[end + 4:])
    for line in bytes(raw[:end]).decode("latin-1").split("\r\n")[1:]:
        name, _, value = line.partition(":")
        headers.setdefault(name.strip().lower(), value.strip().lower())
    if "chunked" in headers.get("transfer-encoding", ""):       # takes precedence over a Content-Length
        return _chunked_complete(body)
    length = headers.get("content-length")
    if length is None:
        return False                                 # no length: complete only when the server closes
    if not DIGITS.match(length):
        raise ApiError("malformed answer (Content-Length)")
    return len(body) >= int(length)


class KubeApi:
    """The four calls the watchdog's two Roles allow.

    Each call is done on ONE socket in the calling thread, with a TOTAL deadline: before every blocking step (connect,
    TLS handshake, each send, each receive) the socket's timeout is set to the time that is LEFT, and the socket is
    closed in every case. There is no worker thread and nothing outlives a call, so calls that time out cannot pile up
    and a stalled Lease request cannot use up anything the deletion needs. (urllib's timeout bounds each read, also
    while the answer's HEADERS are still arriving; a call abandoned in a thread stayed alive as long as bytes trickled.)
    """

    def __init__(self, base, token_file=SA_DIR + "/token", ca_file=SA_DIR + "/ca.crt", timeout=CALL_TIMEOUT_SECONDS):
        url = urllib.parse.urlsplit(base)
        if url.scheme not in ("http", "https") or not url.hostname:
            raise ValueError("the API address must be http(s)://<IP address>[:port]")
        # A numeric address only: resolving a name is a blocking call that no socket timeout bounds. In a pod the API
        # is reached by KUBERNETES_SERVICE_HOST, which is an address.
        try:
            address = ipaddress.ip_address(url.hostname)
        except ValueError:
            raise ValueError("the API address must be a numeric IPv4 or IPv6 address, not a name") from None
        if "%" in url.hostname:
            raise ValueError("an IPv6 address with a zone is not accepted")
        if not (math.isfinite(timeout) and timeout > 0):
            raise ValueError("the call timeout must be finite and positive")
        self.tls, self.host, self.netloc = url.scheme == "https", url.hostname, url.netloc
        self.port = url.port or (443 if self.tls else 80)
        self.family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
        self.address = (self.host, self.port, 0, 0) if address.version == 6 else (self.host, self.port)
        self.token_file, self.timeout = token_file, float(timeout)
        self.context = ssl.create_default_context(cafile=ca_file) if self.tls else None

    def _call(self, method, path, body=None):
        deadline = time.monotonic() + self.timeout

        def left():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ApiError(f"{method} {path}: no answer within {self.timeout} s (outcome unknown)")
            return remaining

        sock, raw = None, bytearray()
        try:
            with open(self.token_file) as f:         # read each time: projected tokens rotate
                token = f.read().strip()
            data = b"" if body is None else json.dumps(body).encode()
            request = (f"{method} {path} HTTP/1.1\r\nHost: {self.netloc}\r\nAuthorization: Bearer {token}\r\n"
                       "Accept: application/json\r\nAccept-Encoding: identity\r\nContent-Type: application/json\r\n"
                       f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n").encode() + data
            sock = socket.socket(self.family, socket.SOCK_STREAM)   # no name resolution anywhere in a call
            sock.settimeout(left())
            sock.connect(self.address)
            if self.tls:
                sock = self.context.wrap_socket(sock, server_hostname=self.host, do_handshake_on_connect=False)
                sock.settimeout(left())
                sock.do_handshake()
            view = memoryview(request)
            while view:
                sock.settimeout(left())
                view = view[sock.send(view):]
            while not _answer_complete(raw):
                sock.settimeout(left())
                chunk = sock.recv(65536)
                if not chunk:
                    break
                raw += chunk
                if len(raw) > MAX_ANSWER_BYTES:
                    raise ApiError(f"{method} {path}: the answer is larger than {MAX_ANSWER_BYTES} bytes")
            left()                                   # an answer that completed after the deadline is not used
        except ApiError:
            raise
        except Exception as e:                       # OSError (timeout, refused, reset), ssl errors, anything else
            raise ApiError(f"{method} {path}: {type(e).__name__}") from None
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
        try:
            response = http.client.HTTPResponse(_Buffered(bytes(raw)), method=method)
            response.begin()
            status, payload = response.status, response.read()
        except Exception as e:                       # a cut or malformed answer
            raise ApiError(f"{method} {path}: unusable answer ({type(e).__name__})") from None
        if status == 404:
            return None
        if status == 409:
            raise Conflict(f"{method} {path}: 409")
        if not 200 <= status < 300:
            raise ApiError(f"{method} {path}: HTTP {status}")
        try:
            return json.loads(payload.decode() or "{}")
        except ValueError:
            raise ApiError(f"{method} {path}: the answer is not JSON") from None

    @staticmethod
    def _lease(ns, name):
        return f"/apis/coordination.k8s.io/v1/namespaces/{ns}/leases/{name}"

    @staticmethod
    def _binding(ns, name):
        return f"/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings/{name}"

    def get_lease(self, ns, name):
        return self._call("GET", self._lease(ns, name))

    def update_lease(self, ns, name, obj):
        out = self._call("PUT", self._lease(ns, name), obj)
        if out is None:
            raise ApiError("the Lease disappeared during the update")
        return out

    def get_binding(self, ns, name):
        return self._call("GET", self._binding(ns, name))

    def delete_binding(self, ns, name, uid):
        self._call("DELETE", self._binding(ns, name), {
            "kind": "DeleteOptions", "apiVersion": "v1", "preconditions": {"uid": uid}})


class Watchdog:
    def __init__(self, api, *, activation, deadline, binding_ns, binding, lease_ns, trigger_lease, trigger_uid,
                 heartbeat_lease, interval=60.0, stale_intervals=3, nonce=None, now=time.time, mono=time.monotonic,
                 log=None):
        self.api, self.activation, self.deadline = api, activation, float(deadline)
        self.binding_ns, self.binding = binding_ns, binding
        self.lease_ns, self.trigger_lease, self.trigger_uid = lease_ns, trigger_lease, trigger_uid
        self.heartbeat_lease, self.interval, self.stale_intervals = heartbeat_lease, float(interval), stale_intervals
        if not math.isfinite(self.deadline):
            raise ValueError("the deadline must be a finite time")
        if not (math.isfinite(self.interval) and self.interval > 0):
            raise ValueError("the decision interval must be finite and positive")
        if isinstance(stale_intervals, bool) or not isinstance(stale_intervals, int) or stale_intervals <= 0:
            raise ValueError("stale_intervals must be a positive whole number")
        self.now, self.mono = now, mono
        # The same limit on this process's monotonic clock: a wall clock stepped backwards cannot extend it.
        self.mono_deadline = self.mono() + (self.deadline - self.now())
        self.nonce = nonce or secrets.token_hex(16)
        self.log = log or (lambda record: print(json.dumps(record, sort_keys=True), flush=True))
        self.mode = None                # None until start() has decided
        self.reason = None              # why revocation began
        self.recorded = False           # the Lease SHOWED triggered on the last pass
        self.deletions = 0              # by this process
        self.reported = 0               # of those, already added to the Lease's leaseTransitions
        self.last_binding = "unknown"   # absent | deleted | present-undeleted | unknown: an observation, never a verdict
        self.heartbeat_seen = None      # the heartbeat's last valid renewTime seen by this process
        self.heartbeat_changed = None   # this process's monotonic time of the last observed ADVANCE of it

    def expired(self):
        return self.now() >= self.deadline or self.mono() >= self.mono_deadline

    # --- the Lease -----------------------------------------------------------------------------------------------

    def _validate(self, lease):
        """Why this object is not the trusted trigger Lease of MY activation, or None. Every read and write uses it."""
        if lease is None:
            return "missing"
        if (lease.get("metadata") or {}).get("uid") != self.trigger_uid:
            return "replaced (another UID)"
        parsed = parse_holder((lease.get("spec") or {}).get("holderIdentity"))
        if parsed is None:
            return "unexpected content"
        if parsed[0] == "unarmed" and parsed[1] != self.activation:
            return "other-activation"
        return None

    def _read_trigger(self, attempts=1):
        """(lease, problem). The trusted Lease, or why it is not; "unreadable" only after `attempts` failed reads."""
        problem = "unreadable"
        for _ in range(attempts):
            try:
                lease = self.api.get_lease(self.lease_ns, self.trigger_lease)
            except ApiError as e:
                problem = f"unreadable: {e}"
                continue
            problem = self._validate(lease)
            return (None, problem) if problem else (lease, None)
        return None, problem

    def _write_holder(self, lease, holder, **spec):
        """The ONLY Lease write: conditional on the read's resourceVersion, never backwards, and only on the trusted
        Lease of this activation (checked here again, whatever path the object came from)."""
        problem = self._validate(lease)
        if problem:
            raise ApiError(f"refused: the Lease is not writable ({problem})")
        current = lease["spec"]["holderIdentity"]
        if not forward(current, holder):
            raise ApiError(f"refused: {current!r} -> {holder!r} is not a forward transition")
        new = json.loads(json.dumps(lease))
        new["spec"] = dict(new["spec"], holderIdentity=holder, renewTime=micro_time(self.now()), **spec)
        return self.api.update_lease(self.lease_ns, self.trigger_lease, new)   # resourceVersion travels in metadata

    def _settle_record(self):
        """ONE bounded round on the trigger Lease in revocation mode: read it; renew it only when it SHOWS triggered;
        otherwise move it to triggered:<reason>. Returns whether the Lease shows (or the write answered) triggered."""
        lease, problem = self._read_trigger()
        if problem:                                  # never write a Lease that is not provably the trusted one
            self.log({"event": "trigger-not-recorded", "problem": problem})
            return False
        holder = lease["spec"]["holderIdentity"]
        triggered = parse_holder(holder)[0] == "triggered"
        # leaseTransitions counts deletions across processes: the Lease's value plus this process's unreported ones
        # (a write whose answer was lost can count them twice; the count is evidence of activity, not an exact tally).
        seen = lease["spec"].get("leaseTransitions")
        total = (seen if isinstance(seen, int) and not isinstance(seen, bool) and seen > 0 else 0) \
            + self.deletions - self.reported
        try:
            if triggered:                            # sign of life + the count, on a CONFIRMED triggered state only
                self._write_holder(lease, holder, leaseTransitions=total)
            else:
                self._write_holder(lease, f"triggered:{self.reason}", acquireTime=micro_time(self.now()),
                                   leaseTransitions=total)
            self.reported = self.deletions
            return True
        except ApiError as e:                        # Conflict included: read again on the next pass
            self.log({"event": "renewal-failed" if triggered else "trigger-not-recorded", "problem": str(e)})
            return triggered

    # --- start: fail closed --------------------------------------------------------------------------------------

    def start(self):
        lease, problem = self._read_trigger(READ_ATTEMPTS)
        if problem:
            return self._revoke(f"trigger-state-{problem.split(':')[0].split(' ')[0]}")
        state, value = parse_holder(lease["spec"]["holderIdentity"])
        if state == "triggered":
            return self._revoke(value)
        if state == "armed":                         # another process was interrupted: never resume monitoring
            return self._revoke("watchdog-restart")
        try:
            self._write_holder(lease, f"armed:{self.nonce}")
        except ApiError as e:
            self.log({"event": "arming-response-not-clean", "problem": str(e)})
        # Confirm, whatever the update answered: only a re-read that shows MY nonce on the trusted Lease is armed.
        confirmed, problem = self._read_trigger()
        if problem or confirmed["spec"]["holderIdentity"] != f"armed:{self.nonce}":
            return self._revoke("arming-uncertain")
        self.mode = MONITORING
        self.heartbeat_changed = self.mono()         # a heartbeat must be seen to advance within the stale limit
        self.log({"event": "armed", "nonce": self.nonce, "deadline": self.deadline})
        return self.mode

    # --- one pass ------------------------------------------------------------------------------------------------

    def step(self):
        if self.mode is None:
            return self.start()
        return self._monitor() if self.mode == MONITORING else self._revocation_pass()

    def _monitor(self):
        if self.expired():
            return self._revoke("deadline")
        lease, problem = self._read_trigger(READ_ATTEMPTS)
        if problem:
            # Missing, replaced, unexpected, another activation, or unreadable: all revocation. A Lease that cannot be
            # read says nothing about whether the RoleBinding can be deleted, so the deletion is attempted.
            unreadable = problem.startswith("unreadable")
            return self._revoke("trigger-state-unreadable" if unreadable else "trigger-state-changed")
        holder = lease["spec"]["holderIdentity"]
        if holder != f"armed:{self.nonce}":          # triggered by another process, or armed by one: not mine any more
            return self._revoke(holder.split(":", 1)[1] if holder.startswith("triggered:") else "watchdog-overlap")
        try:
            self._write_holder(lease, holder)        # the renewal: same holder, conditional
        except ApiError as e:
            self.log({"event": "renewal-failed", "problem": str(e)})   # read again on the next pass
        try:
            stamp = renew_stamp(self.api.get_lease(self.lease_ns, self.heartbeat_lease))
            if stamp is not None:                    # missing or not a timestamp: no heartbeat, nothing remembered
                if self.heartbeat_seen is not None and stamp > self.heartbeat_seen:
                    self.heartbeat_changed = self.mono()   # only an ADVANCE of renewTime this process saw counts
                if self.heartbeat_seen is None or stamp > self.heartbeat_seen:
                    self.heartbeat_seen = stamp
        except ApiError as e:
            self.log({"event": "heartbeat-unreadable", "problem": str(e)})   # counts as no heartbeat
        if self.mono() - self.heartbeat_changed > self.stale_intervals * self.interval:
            return self._revoke("stale-heartbeat")
        return self.mode

    def _revoke(self, reason):
        """Enter revocation mode: ONE bounded attempt to record first, then the deletion whether or not it was
        recorded. The record refuses a Lease that is not the trusted one of this activation."""
        self.mode, self.reason = REVOCATION, reason
        self.recorded = self._settle_record()
        self.log({"event": "triggered", "reason": reason, "recorded": self.recorded})
        self._delete_binding()
        return self.mode

    def _delete_binding(self):
        binding = None
        try:
            binding = self.api.get_binding(self.binding_ns, self.binding)
            if binding is None:
                self.last_binding = "absent"
                return
            self.api.delete_binding(self.binding_ns, self.binding, binding["metadata"]["uid"])
            self.deletions += 1
            self.last_binding = "deleted"
        except (ApiError, KeyError, TypeError) as e:
            self.last_binding = "present-undeleted" if binding else "unknown"
            self.log({"event": "deletion-failed", "problem": str(e)})

    def _revocation_pass(self):
        self._delete_binding()                       # the deletion first, every pass
        self.recorded = self._settle_record()        # what the Lease shows NOW, never a memory of an earlier write
        self.log({"event": "revocation-pass", "reason": self.reason, "recorded": self.recorded,
                  "binding_observation": self.last_binding, "deletions": self.deletions,
                  "note": "an observation, not a completed revocation: only a reviewed recovery ends this"})
        return self.mode

    def run(self, pass_seconds=5.0, sleep=time.sleep, passes=None):
        """Forever (passes is for tests). There is no successful exit: revocation mode never returns."""
        if not (math.isfinite(pass_seconds) and pass_seconds >= 0):
            raise ValueError("pass_seconds must be finite and not negative")
        n = 0
        while passes is None or n < passes:
            self.step()
            n += 1
            sleep(pass_seconds if self.mode == REVOCATION else min(pass_seconds, self.interval))
        return self.mode


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--activation", required=True)
    p.add_argument("--deadline-epoch", required=True, type=float, help="absolute UTC deadline, seconds since the epoch")
    p.add_argument("--binding-namespace", required=True)
    p.add_argument("--binding", required=True)
    p.add_argument("--lease-namespace", required=True)
    p.add_argument("--trigger-lease", required=True)
    p.add_argument("--trigger-lease-uid", required=True)
    p.add_argument("--heartbeat-lease", required=True)
    p.add_argument("--interval-seconds", type=float, default=60.0)
    p.add_argument("--stale-intervals", type=int, default=3)
    p.add_argument("--pass-seconds", type=float, default=5.0)
    a = p.parse_args(argv)
    host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    if ":" in host:
        host = f"[{host}]"
    w = Watchdog(KubeApi(f"https://{host}:{port}"), activation=a.activation, deadline=a.deadline_epoch,
                 binding_ns=a.binding_namespace, binding=a.binding, lease_ns=a.lease_namespace,
                 trigger_lease=a.trigger_lease, trigger_uid=a.trigger_lease_uid, heartbeat_lease=a.heartbeat_lease,
                 interval=a.interval_seconds, stale_intervals=a.stale_intervals)
    w.run(pass_seconds=a.pass_seconds)
    return 1                                         # unreachable by design; a return is a failure


if __name__ == "__main__":
    sys.exit(main())
