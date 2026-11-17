#!/usr/bin/env python3
"""The drain interval of a revocation (TRANSITION-DESIGN draft 6, section 3, and its Amendment 1). Pure: it judges
objects that the caller read; it reads nothing itself.

After the scale binding is deleted and every executor is fenced, a write that the API server ADMITTED earlier can
still complete. The longest such request is bounded by the API server's own limit for a non-long-running request,
`--request-timeout`. The drain interval is that limit plus a margin, and "revoked" may be claimed only after it has
elapsed. It is NOT Helm's timeout or any client's.

`verdict(...)` is verified only when ALL of this holds; anything else is unverified, with the reason:

- the control-plane node inventory (name and UID of each node) was established first and is not empty; exactly one
  kube-apiserver pod runs on each of those nodes and none anywhere else (an empty or incomplete selection proves
  nothing);
- every such pod is Running and not being deleted, and its kube-apiserver container is RUNNING and ready, with a
  runtime identity: the image's digest (imageID), the container ID, the restart count and the start time. These are
  recorded: a container replaced inside the same pod is another server;
- the container is started DIRECTLY: `command[0]` is the kube-apiserver executable and the flags follow as separate
  arguments in `command` and `args`. A shell or any other wrapper, no command at all (the image's entrypoint is not
  known here), or any argument with a `$(VAR)` reference (Kubernetes expands those from the environment; this module
  does not) is an unknown invocation: the flag could be anywhere, so nothing is concluded from not seeing it;
- the flag is read in the two forms kube-apiserver (pflag) accepts for a long flag: `--request-timeout=1m0s` and
  `--request-timeout 1m0s`. A single-dash form, a value that is malformed, zero or negative, a flag given more than
  once, or a flag with a missing or unsupported value is refused, never guessed;
- every server agrees: all give the same value, or none gives the flag AND for each server's actual image DIGEST the
  reviewed plan carries an attestation of that binary's default (seconds, version, reference). A version read from
  an image tag is not evidence about a binary. Some with and some without, or different values: unverified.

An unverified drain blocks the tombstone. It never blocks the deletion or the fence: those come first.
The verdict records the nodes and each server's identity, invocation and setting, so that the execution can check
before the drain and again at its end that nothing changed since the plan (`same_servers`).
"""
import math
import os
import re

FLAG = "request-timeout"
MARGIN_SECONDS = 30.0
CONTAINER = "kube-apiserver"
UNITS = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 0.001}
DURATION_PART = re.compile(r"([0-9]+(?:\.[0-9]+)?|\.[0-9]+)(ms|h|m|s)")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
SERVER_FIELDS = ("node", "pod", "uid", "image", "image_digest", "container_id", "restart_count", "started_at",
                 "invocation", "seconds", "default")


class Unverified(Exception):
    """Why the drain interval cannot be established."""


def parse_duration(text):
    """Seconds of a Go duration as kube-apiserver takes it ("1m0s", "90s", "1.5m", "2h"), strictly: the whole string
    must be a sequence of number+unit parts with the units h, m, s, ms; it must be finite and positive."""
    if not isinstance(text, str) or not text:
        raise Unverified("the request timeout has a missing or unsupported value")
    pos, total = 0, 0.0
    for m in DURATION_PART.finditer(text):
        if m.start() != pos:
            break
        total += float(m.group(1)) * UNITS[m.group(2)]
        pos = m.end()
    if pos != len(text):
        raise Unverified(f"the request timeout {text!r} is not a duration")
    if not math.isfinite(total) or total <= 0:
        raise Unverified(f"the request timeout {text!r} is not positive")
    return total


def invocation(container):
    """The complete argument list of a DIRECT kube-apiserver invocation: [executable, flag, ...]. Raises Unverified
    for anything else: no command (the image's entrypoint is unknown here), a wrapper, or lists that are not lists
    of strings."""
    parts = []
    for key in ("command", "args"):
        part = container.get(key)
        if part is None:
            part = []
        if not isinstance(part, list) or not all(isinstance(t, str) for t in part):
            raise Unverified(f"the kube-apiserver container's {key} is not a list of strings")
        parts.append(part)
    command, args = parts
    if not command:
        raise Unverified("the container has no command: the image's entrypoint and its arguments are not known here")
    if os.path.basename(command[0]) != CONTAINER or command[0].endswith("/"):
        raise Unverified(f"the container is not started directly as {CONTAINER} (it runs {command[0]!r}): "
                         "the flags of a wrapped invocation cannot be read")
    for token in command + args:
        # Kubernetes expands $(VAR) in command and args from the container's environment before the process starts.
        # What such a token becomes is not known here: it could BE the flag, or be part of its name or value. The
        # environment is not resolved by this module, so any token with the syntax (its escaped form included) makes
        # the invocation unknown.
        if "$(" in token:
            raise Unverified(f"the invocation contains an unexpanded variable reference ({token!r}): its effective "
                             "arguments are not known here")
    return command + args


def request_timeout(container):
    """The flag's value in seconds, or None when a direct invocation does not give the flag. Raises Unverified for an
    unknown invocation, a single-dash form, a repeated flag, or a missing or unsupported value."""
    tokens = invocation(container)[1:]
    values = []
    for i, token in enumerate(tokens):
        name, eq, value = token.partition("=")
        if name == f"-{FLAG}":
            raise Unverified("the request timeout is given in a single-dash form, which kube-apiserver does not accept")
        if name != f"--{FLAG}":
            continue
        if eq:
            values.append(value)
        elif i + 1 < len(tokens) and not tokens[i + 1].startswith("-"):
            values.append(tokens[i + 1])
        else:
            raise Unverified("the request timeout flag has a missing or unsupported value")
    if len(values) > 1:
        raise Unverified("the request timeout flag is given more than once")
    return parse_duration(values[0]) if values else None


def _text(value):
    return isinstance(value, str) and bool(value) and value == value.strip()


def _server(pod):
    """One kube-apiserver pod's record (SERVER_FIELDS, without the default), or Unverified."""
    meta, spec, status = pod.get("metadata") or {}, pod.get("spec") or {}, pod.get("status") or {}
    name = f"{meta.get('namespace')}/{meta.get('name')}"
    node, uid = spec.get("nodeName"), meta.get("uid")
    if not _text(meta.get("namespace")) or not _text(meta.get("name")) or not _text(node) or not _text(uid):
        raise Unverified(f"{name} has no namespace, name, node or UID")
    if meta.get("deletionTimestamp") or status.get("phase") != "Running":
        raise Unverified(f"{name} is not Running")
    containers = [c for c in spec.get("containers") or [] if isinstance(c, dict) and c.get("name") == CONTAINER]
    if len(containers) != 1:
        raise Unverified(f"{name} does not have exactly one {CONTAINER} container")
    statuses = [s for s in status.get("containerStatuses") or [] if isinstance(s, dict) and s.get("name") == CONTAINER]
    if len(statuses) != 1:
        raise Unverified(f"{name}: the {CONTAINER} container has no single status")
    st = statuses[0]
    running = (st.get("state") or {}).get("running") if isinstance(st.get("state"), dict) else None
    if st.get("ready") is not True or not isinstance(running, dict) or not _text(running.get("startedAt")):
        raise Unverified(f"{name}: the {CONTAINER} container is not running and ready")
    digest = DIGEST.search(st["imageID"]) if isinstance(st.get("imageID"), str) else None
    restarts = st.get("restartCount")
    if not digest or not _text(st.get("containerID")) or isinstance(restarts, bool) or not isinstance(restarts, int) \
            or restarts < 0:
        raise Unverified(f"{name}: the {CONTAINER} container has no runtime identity "
                         "(image digest, container ID, restart count)")
    image = containers[0].get("image")
    if not _text(image):
        raise Unverified(f"{name}: the {CONTAINER} container names no image")
    try:
        tokens = invocation(containers[0])
        seconds = request_timeout(containers[0])
    except Unverified as e:
        raise Unverified(f"{name}: {e}") from None
    return {"node": node, "pod": name, "uid": uid, "image": image, "image_digest": digest.group(0),
            "container_id": st["containerID"], "restart_count": restarts, "started_at": running["startedAt"],
            "invocation": tokens, "seconds": seconds, "default": None}


def _attested(server, attestations):
    """The reviewed default of this server's actual binary: {"seconds", "version", "reference"}, or Unverified."""
    a = attestations.get(server["image_digest"]) if isinstance(attestations, dict) else None
    if not isinstance(a, dict):
        raise Unverified(f"{server['pod']}: the flag is absent and no default is attested for its image "
                         f"{server['image_digest']}")
    seconds = a.get("seconds")
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0 \
            or not _text(a.get("version")) or not _text(a.get("reference")):
        raise Unverified(f"{server['pod']}: the default attested for {server['image_digest']} is incomplete "
                         "(it needs positive seconds, the version and the reference it rests on)")
    return {"seconds": float(seconds), "version": a["version"], "reference": a["reference"]}


def _nodes(control_plane_nodes):
    ok = isinstance(control_plane_nodes, (list, tuple)) and len(control_plane_nodes) > 0 and all(
        isinstance(n, dict) and set(n) == {"name", "uid"} and _text(n["name"]) and _text(n["uid"])
        for n in control_plane_nodes)
    if not ok or len({n["name"] for n in control_plane_nodes}) != len(control_plane_nodes) \
            or len({n["uid"] for n in control_plane_nodes}) != len(control_plane_nodes):
        raise Unverified("the control-plane node inventory is empty or malformed: nothing can be compared with it")
    return sorted(({"name": n["name"], "uid": n["uid"]} for n in control_plane_nodes), key=lambda n: n["name"])


def verdict(pods, control_plane_nodes, default_attestations, observed_at):
    """{"verified", "why", "seconds", "request_timeout", "nodes", "servers", "observed_at"}.
    pods: the kube-apiserver pods the caller selected; control_plane_nodes: [{"name", "uid"}] established FIRST;
    default_attestations: {"sha256:<image digest>": {"seconds", "version", "reference"}} from the reviewed plan;
    observed_at: when the caller read the objects (recorded, never compared)."""
    out = {"verified": False, "why": "", "seconds": None, "request_timeout": None, "nodes": [], "servers": [],
           "observed_at": observed_at}
    try:
        if not _text(observed_at):
            raise Unverified("the observation time is missing")
        out["nodes"] = _nodes(control_plane_nodes)
        if not isinstance(pods, list) or not pods:
            raise Unverified("no kube-apiserver pod was selected: an empty selection proves nothing")
        if not all(isinstance(p, dict) for p in pods):
            raise Unverified("a selected pod is not an object")
        servers = sorted((_server(p) for p in pods), key=lambda s: (s["node"], s["uid"]))
        out["servers"] = servers
        nodes, expected = [s["node"] for s in servers], {n["name"] for n in out["nodes"]}
        if len(nodes) != len(set(nodes)):
            raise Unverified("more than one kube-apiserver pod runs on one node")
        if set(nodes) != expected:
            missing, extra = sorted(expected - set(nodes)), sorted(set(nodes) - expected)
            raise Unverified(f"the kube-apiserver pods do not match the control-plane nodes "
                             f"(no pod on {missing}, pods outside the inventory on {extra})")
        given = {s["seconds"] for s in servers}
        if None in given:
            if given != {None}:
                raise Unverified("some API servers set the request timeout and some do not: conflicting")
            for s in servers:
                s["default"] = _attested(s, default_attestations)
            defaults = {s["default"]["seconds"] for s in servers}
            if len(defaults) != 1:
                raise Unverified("the attested defaults of the API servers differ")
            timeout = defaults.pop()
        else:
            if len(given) != 1:
                raise Unverified(f"the API servers give different request timeouts {sorted(given)}: conflicting")
            timeout = given.pop()
        out.update(verified=True, request_timeout=timeout, seconds=timeout + MARGIN_SECONDS,
                   why=f"request timeout {timeout:g} s on {len(servers)} API server(s), plus {MARGIN_SECONDS:g} s")
    except Unverified as e:
        out["why"] = str(e)
    return out


def same_servers(planned, live):
    """Whether the live verdict describes exactly what the plan recorded: both verified, the same nodes (name and
    UID), and for every server the same pod, UID, image, image digest, container ID, restart count, start time,
    invocation, setting and attested default, and the same interval. A restarted container, a replaced pod, an
    upgraded or reconfigured API server, or a changed node differs. The observation times are not compared."""
    if not (isinstance(planned, dict) and isinstance(live, dict)):
        return False
    if planned.get("verified") is not True or live.get("verified") is not True:
        return False
    for key in ("nodes", "servers"):
        if not isinstance(live.get(key), list) or not live[key] or planned.get(key) != live[key]:
            return False
    if any(not isinstance(s, dict) or set(s) != set(SERVER_FIELDS) for s in live["servers"]):
        return False
    return planned.get("seconds") == live.get("seconds") and live.get("seconds") is not None \
        and planned.get("request_timeout") == live.get("request_timeout")
