"""HTTP GET with a hard wall-clock deadline, complete framing and a body cap, for the forecasting service's outbound reads.

requests' timeouts bound connecting and each period of inactivity, not the whole exchange: a server that trickles a
byte at a time can hold a worker indefinitely. Here the deadline covers every phase:
  - name resolution runs in a helper thread and is abandoned at the deadline (getaddrinfo cannot be interrupted). A
    lookup holds one of RESOLVER_SLOTS permits until it actually finishes, so abandoned lookups can never queue up: when
    all slots are taken the call fails at once (TransportFailure, a 503 for the caller);
  - each resolved address is tried in turn, with the remaining time as the connect timeout;
  - a timer shuts the connection down when the deadline passes, through a duplicate of the socket's descriptor held
    here: shutdown acts on the connection, not the descriptor, so it still works after http.client closes its socket
    object and hands the descriptor to the response (Connection: close), and during the TLS handshake. That unblocks
    the handshake, the header read or the body read;
  - the body is read in bounded chunks against a byte cap.
A body counts only when its framing is complete: Content-Length fully read, or the chunked encoding terminated. A short
or unframed body is a TransportFailure, never a shorter answer. The response and the connection are closed on every path.
"""

from __future__ import annotations

import concurrent.futures
import http.client
import socket
import ssl
import threading
import time
import urllib.parse
from typing import Dict, Optional, Tuple

RESOLVER_SLOTS = 4
_resolver = concurrent.futures.ThreadPoolExecutor(max_workers=RESOLVER_SLOTS, thread_name_prefix="bounded-http-dns")
_resolver_slots = threading.BoundedSemaphore(RESOLVER_SLOTS)   # held from submission until the lookup has finished
_getaddrinfo = socket.getaddrinfo   # replaceable in tests


def _resolve(host: str, port: int, timeout: float):
    if not _resolver_slots.acquire(blocking=False):
        raise TransportFailure("name resolution saturated (earlier lookups still running)")
    try:
        fut = _resolver.submit(_getaddrinfo, host, port, 0, socket.SOCK_STREAM)
    except BaseException:
        _resolver_slots.release()
        raise
    fut.add_done_callback(lambda _f: _resolver_slots.release())   # only when the lookup really ends
    try:
        return fut.result(timeout=timeout)
    except concurrent.futures.TimeoutError as e:
        raise DeadlineExceeded(f"name resolution of {host} exceeded the deadline") from e
    except OSError as e:
        raise TransportFailure(f"name resolution of {host} failed: {e}") from e


class TransportFailure(Exception):
    """The exchange could not be completed: resolution, connection, TLS, protocol, framing or deadline."""


class DeadlineExceeded(TransportFailure):
    pass


class BodyTooLarge(Exception):
    pass


def _try_timeout(sock, seconds: float) -> None:
    try:
        sock.settimeout(seconds)
    except OSError:
        pass


def bounded_get(url: str, *, headers: Optional[Dict[str, str]] = None, deadline_s: float, max_bytes: int,
                connect_timeout_s: float = 3.0, ssl_context: Optional[ssl.SSLContext] = None) -> Tuple[int, bytes]:
    """GET url; return (status, complete body). Raises DeadlineExceeded, TransportFailure or BodyTooLarge."""
    return bounded_request("GET", url, headers=headers, deadline_s=deadline_s, max_bytes=max_bytes,
                           connect_timeout_s=connect_timeout_s, ssl_context=ssl_context)


def bounded_request(method: str, url: str, *, body: Optional[bytes] = None, headers: Optional[Dict[str, str]] = None,
                    deadline_s: float, max_bytes: int, connect_timeout_s: float = 3.0,
                    ssl_context: Optional[ssl.SSLContext] = None) -> Tuple[int, bytes]:
    """method (GET or POST) url with an optional body; the same deadline, framing and size guarantees as bounded_get."""
    if method not in ("GET", "POST"):
        raise TransportFailure(f"unsupported method {method!r}")
    if deadline_s <= 0:
        raise DeadlineExceeded("deadline already passed")
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ("http", "https") or not u.hostname:
        raise TransportFailure(f"unsupported URL {url!r}")
    host, port = u.hostname, u.port or (443 if u.scheme == "https" else 80)
    deadline = time.monotonic() + deadline_s

    def remaining() -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise DeadlineExceeded(f"exchange exceeded its {deadline_s:g} s deadline")
        return left

    live = {"sock": None}   # the watchdog's own duplicate of the connected socket
    expired = threading.Event()
    lock = threading.Lock()

    def kill():
        with lock:
            expired.set()
            sock = live["sock"]
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    timer = threading.Timer(deadline_s, kill)
    timer.daemon = True
    timer.start()
    sock, conn, resp = None, None, None
    try:
        try:
            addrs = _resolve(host, port, remaining())
            if not addrs:
                raise TransportFailure(f"no address for {host}")
            errors = []
            for family, stype, proto, _, addr in addrs:   # each address in turn, within the one deadline
                candidate = socket.socket(family, stype, proto)
                with lock:
                    live["sock"] = candidate     # interrupts a connect in progress
                if expired.is_set():
                    candidate.close()
                    raise DeadlineExceeded("deadline passed before connecting")
                try:
                    candidate.settimeout(min(connect_timeout_s, remaining()))
                    candidate.connect(addr)
                except DeadlineExceeded:
                    candidate.close()
                    raise
                except OSError as e:
                    candidate.close()
                    errors.append(f"{addr}: {e}")
                    if expired.is_set() or time.monotonic() >= deadline:
                        raise DeadlineExceeded(f"connecting to {host} exceeded the deadline") from e
                    continue
                sock = candidate
                break
            if sock is None:
                raise TransportFailure(f"could not connect to {host}: {'; '.join(errors)}")
            with lock:
                live["sock"] = sock.dup()    # survives wrap_socket and http.client closing `sock`
            if expired.is_set():
                raise DeadlineExceeded("deadline passed after connecting")
            sock.settimeout(remaining())
            if u.scheme == "https":
                ctx = ssl_context or ssl.create_default_context()
                sock = ctx.wrap_socket(sock, server_hostname=host, do_handshake_on_connect=False)
                sock.do_handshake()
            conn = (http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection)(host, port)
            conn.sock = sock   # use our connected, watched socket; http.client never opens another
            path = (u.path or "/") + (f"?{u.query}" if u.query else "")
            conn.request(method, path, body=body, headers=headers or {})
            _try_timeout(sock, remaining())
            resp = conn.getresponse()
            chunks, size = [], 0
            while True:
                _try_timeout(sock, remaining())   # best effort: http.client may have closed `sock`; the timer bounds
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    raise BodyTooLarge(f"response exceeds {max_bytes} bytes")
                chunks.append(chunk)
            if expired.is_set():
                raise DeadlineExceeded(f"exchange exceeded its {deadline_s:g} s deadline")
            if not resp.chunked:
                if resp.length is None:
                    raise TransportFailure("unframed response (neither Content-Length nor chunked)")
                if resp.length > 0:
                    raise TransportFailure(f"truncated response: {resp.length} bytes missing")
            return resp.status, b"".join(chunks)
        except (BodyTooLarge, TransportFailure):
            raise
        except (OSError, http.client.HTTPException, ssl.SSLError, ValueError) as e:
            if expired.is_set() or time.monotonic() >= deadline:
                raise DeadlineExceeded(f"exchange exceeded its {deadline_s:g} s deadline") from e
            raise TransportFailure(f"{type(e).__name__}: {e}") from e
    finally:
        timer.cancel()
        with lock:
            watch, live["sock"] = live["sock"], None
        for closable in (resp, conn, sock, watch):
            if closable is not None:
                try:
                    closable.close()
                except Exception:
                    pass
