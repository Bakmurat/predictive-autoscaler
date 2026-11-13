#!/usr/bin/env python3
"""Read the operator's decision ledger from its retained claim (live demonstration, INSTALL-PLAN section 7 and its
review): a short read-only reader pod, never the operator's own container (distroless) and never docker exec.

  ledger_read.py --record R render                     offline: print the reader's Pod and NetworkPolicy
  ledger_read.py --record R read --out F [--offset N] [--max-bytes N]

The reader pod runs on the node where the operator's pod (and so the ReadWriteOnce claim) is, tolerates exactly the
reservation owner's taint, carries the demonstration's run label (an authorized occupant, counted in residency by
params.residency.support_extra), runs the reviewed statistical image (digest-pinned, python, non-root uid 10001) with no
service-account token, no service links, a read-only root filesystem, every capability dropped and NO fsGroup (the claim's
ownership is never changed), mounts the claim readOnly, and a deny-all NetworkPolicy selects it. It is created, used
and deleted under the shared evaluation lock; every create and delete is journalled (intent before, outcome after). The
pod is deleted first and its disappearance is proven as a journalled step (ledger-reader-gone, with a reconciler); only
then is its NetworkPolicy deleted. An unproven disappearance leaves that step unresolved, so the lock stays and no other
reader (or other helper) starts until recover finds the pod gone.

Inside the pod a fixed script opens forecasts.jsonl, takes its size at open (fstat), reads at most --max-bytes from
--offset up to that size in two streaming passes (memory: one 1 MiB chunk plus the longest line; the file only grows
by appends), and keeps the COMPLETE-LINE prefix: an incomplete trailing line (an append in progress) is
reported as partial_tail_bytes and never captured. It prints a receipt (size at open, offset, captured bytes, lines,
malformed lines, partial tail, cap hit, sha256 of the captured bytes, its uid and the file mode), the captured bytes,
and a trailer (bytes, sha256). The local side accepts the transfer only when the receipt, the bytes and the trailer
agree. The received bytes are written to <out>.failed-<UTC> as soon as the exec returns or times out, before any
cleanup; that file is removed only after a verified transfer, so an interrupted or altered one stays kept and refused.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from typing import Callable, Dict, List, Optional, Tuple

import execlib as X
import prepare

HELPER = "ledger_read"
LEDGER_FILE = "forecasts.jsonl"
MOUNT = "/ledger"
READER_UID = 10001                      # the statistical image's USER (ml-engine/Dockerfile.statistical)
DEFAULT_MAX_BYTES = 64 * 1024 * 1024
MAX_BYTES_LIMIT = 128 * 1024 * 1024     # the reader's memory limit is 256Mi
ROLE = "ledger-reader"
RESOURCES = {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"cpu": "200m", "memory": "256Mi"}}
LIFETIME_S = 840                        # the container sleeps this long; activeDeadlineSeconds stops it at 900 s

READER_SCRIPT = r'''
import hashlib, json, os, sys
p, off, cap = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
CH = 1 << 20
out = sys.stdout.buffer
fd = os.open(p, os.O_RDONLY)
st = os.fstat(fd)
size = st.st_size
if off > size:
    raise SystemExit("offset beyond the file's size at open (replaced or truncated)")
want = min(size - off, cap)
# Pass 1, streaming (memory: one chunk plus the longest line): the complete-line cut, the hash, the line counts.
h, pos, cut, lines, bad, carry = hashlib.sha256(), 0, 0, 0, 0, b""
while pos < want:
    b = os.pread(fd, min(CH, want - pos), off + pos)
    if not b:
        break
    pos += len(b)
    buf = carry + b
    k = buf.rfind(b"\n") + 1
    for line in buf[:k].split(b"\n")[:-1]:
        lines += 1
        try:
            json.loads(line)
        except ValueError:
            bad += 1
    h.update(buf[:k])
    cut += k
    carry = buf[k:]
    del buf, b
digest = h.hexdigest()
rec = {"schema": "c28-ledger-read/v1", "path": p, "inode": st.st_ino, "size_at_open": size, "offset": off,
       "read_bytes": pos, "captured_bytes": cut, "partial_tail_bytes": pos - cut, "cap_hit": size - off > cap,
       "lines": lines, "malformed": bad, "sha256": digest, "uid": os.getuid(), "mode": oct(st.st_mode & 0o777)}
out.write(json.dumps(rec, sort_keys=True).encode() + b"\n")
# Pass 2: stream exactly the captured bytes again (the file only grows by appends) and re-hash what was sent.
h2, sent = hashlib.sha256(), 0
while sent < cut:
    b = os.pread(fd, min(CH, cut - sent), off + sent)
    if not b:
        break
    h2.update(b)
    out.write(b)
    sent += len(b)
os.close(fd)
tr = {"bytes": sent, "sha256": h2.hexdigest()}
out.write(json.dumps({"trailer": tr}, sort_keys=True).encode() + b"\n")
out.flush()
'''


class TransferRefused(X.StagingError):
    pass


def parse_stream(raw: bytes) -> Tuple[Dict, bytes]:
    """(receipt, captured bytes) when the receipt, the bytes and the trailer agree; TransferRefused otherwise."""
    nl = raw.find(b"\n")
    if nl < 0:
        raise TransferRefused("no receipt line")
    try:
        rec = json.loads(raw[:nl])
        n = rec["captured_bytes"]
    except (ValueError, KeyError, TypeError) as e:
        raise TransferRefused(f"unreadable receipt: {type(e).__name__}") from e
    if not isinstance(n, int) or n < 0 or rec.get("schema") != "c28-ledger-read/v1":
        raise TransferRefused("not a c28-ledger-read/v1 receipt")
    body = raw[nl + 1:nl + 1 + n]
    rest = raw[nl + 1 + n:]
    if len(body) != n:
        raise TransferRefused(f"interrupted: {len(body)} of {n} captured bytes received")
    if not rest.endswith(b"\n") or rest.count(b"\n") != 1:
        raise TransferRefused("no single trailer line after the captured bytes")
    try:
        tr = json.loads(rest)["trailer"]
    except (ValueError, KeyError, TypeError) as e:
        raise TransferRefused(f"unreadable trailer: {type(e).__name__}") from e
    h = hashlib.sha256(body).hexdigest()
    if not (tr.get("bytes") == n and tr.get("sha256") == h == rec.get("sha256")):
        raise TransferRefused("the receipt, the bytes and the trailer disagree")
    if n and not body.endswith(b"\n"):
        raise TransferRefused("the captured bytes do not end on a complete line")
    return rec, body


# --- the objects ----------------------------------------------------------------------------------------------------

def _labels(params: Dict) -> Dict[str, str]:
    return {"c28.devkuban.com/run": params["demonstration"]["run_id"], "c28.devkuban.com/role": ROLE,
            "app.kubernetes.io/name": "c28-ledger-reader"}


def render(params: Dict, section: Dict, name: str, node: str) -> List[Dict]:
    """The reader's NetworkPolicy (deny all, both directions) and Pod. `node` is the operator pod's node."""
    c28 = X.import_harness()
    ns = params["control_namespace"]
    r = params["reservation"]
    claim = section["storage"]["ledger_claim"]["name"]
    labels = _labels(params)
    sidecar = {c28.render.SIDECAR_ANNOTATION: "false"}
    np_ = {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
           "metadata": {"name": name, "namespace": ns, "labels": dict(labels)},
           "spec": {"podSelector": {"matchLabels": {"c28.devkuban.com/role": ROLE,
                                                    "c28.devkuban.com/run": labels["c28.devkuban.com/run"]}},
                    "policyTypes": ["Ingress", "Egress"]}}
    pod = {"apiVersion": "v1", "kind": "Pod",
           "metadata": {"name": name, "namespace": ns, "labels": {**labels, **sidecar}, "annotations": dict(sidecar)},
           "spec": {
               "automountServiceAccountToken": False, "enableServiceLinks": False, "restartPolicy": "Never",
               "activeDeadlineSeconds": LIFETIME_S + 60, "terminationGracePeriodSeconds": 5,
               "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                   {"matchExpressions": [{"key": r["role_label"], "operator": "In", "values": ["support"]},
                                         {"key": "kubernetes.io/hostname", "operator": "In", "values": [node]}]}]}}},
               "tolerations": [{"key": r["taint_key"], "operator": "Equal", "value": r["taint_value"],
                                "effect": "NoSchedule"}],
               "securityContext": {"runAsNonRoot": True, "runAsUser": READER_UID, "runAsGroup": READER_UID,
                                   "seccompProfile": {"type": "RuntimeDefault"}},
               "containers": [{
                   "name": "reader", "image": section["release"]["statistical_image"], "imagePullPolicy": "IfNotPresent",
                   "command": ["python3", "-c", f"import time; time.sleep({LIFETIME_S})"],
                   "resources": RESOURCES,
                   "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                       "capabilities": {"drop": ["ALL"]}},
                   "volumeMounts": [{"name": "ledger", "mountPath": MOUNT, "readOnly": True}]}],
               "volumes": [{"name": "ledger", "persistentVolumeClaim": {"claimName": claim, "readOnly": True}}]}}
    return [np_, pod]


def residency_entry() -> Dict:
    """The reader as params.residency.support_extra counts it."""
    return {"name": "c28-ledger-reader", "replicas": 1, "cpu_request": RESOURCES["requests"]["cpu"],
            "memory_request": RESOURCES["requests"]["memory"]}


def residency_problems(params: Dict) -> List[str]:
    extra = params.get("residency", {}).get("support_extra") or []
    want = residency_entry()
    if not any(all(e.get(k) == v for k, v in want.items()) for e in extra):
        return [f"params.residency.support_extra must count the ledger reader as {want}"]
    return []


# --- the read -------------------------------------------------------------------------------------------------------

def operator_node(kube: X.Kube, params: Dict, section: Dict) -> Tuple[str, List[str]]:
    """The node of the ONE running operator pod (the claim is attached there); problems otherwise."""
    ns = params["control_namespace"]
    pods = [p for p in kube.list("Pod", ns, labels={"app.kubernetes.io/component": "operator"}, strict=True)
            if (p.get("status") or {}).get("phase") == "Running"]
    claim = section["storage"]["ledger_claim"]["name"]
    pods = [p for p in pods if any((v.get("persistentVolumeClaim") or {}).get("claimName") == claim
                                   for v in (p.get("spec") or {}).get("volumes") or [])]
    if len(pods) != 1:
        return "", [f"expected exactly one running operator pod with claim {claim}, found {len(pods)}"]
    node = pods[0]["spec"].get("nodeName", "")
    if node != params["nodes"]["support"]:
        return node, [f"the operator pod runs on {node!r}, not on S {params['nodes']['support']!r}"]
    return node, []


def _wait(fn: Callable[[], bool], seconds: float, sleep: Callable[[float], None],
          clock: Callable[[], float] = time.monotonic) -> bool:
    """Poll every 2 s until fn() or the elapsed-time budget (monotonic: slow polls count too) is exhausted."""
    end = clock() + seconds
    while True:
        if fn():
            return True
        if clock() >= end:
            return False
        sleep(2)


def _ready(kube: X.Kube, ns: str, name: str) -> bool:
    p = kube.get("Pod", name, ns)
    st = (p or {}).get("status") or {}
    return st.get("phase") == "Running" and any(c.get("type") == "Ready" and c.get("status") == "True"
                                                for c in st.get("conditions") or [])



GONE_OP = "ledger-reader-gone"


def _prove_gone(kube: X.Kube, ns: str, name: str, uid: str, sleep: Callable[[float], None],
                clock: Callable[[], float] = time.monotonic) -> Dict:
    def gone() -> bool:
        o = kube.get("Pod", name, ns)
        return o is None or o["metadata"].get("uid") != uid
    if not _wait(gone, 120, sleep, clock):
        raise X.StagingError(f"the reader pod {name} is still present 120 s after its delete")
    return {"metadata": {"uid": uid}}


def _reconcile_gone(kube: X.Kube, d: Dict, record) -> Dict:
    """recover: the reader pod of this intent is gone (absent, or the name holds another object): done; else unresolved
    (the lock stays until it is gone)."""
    o = kube.get("Pod", d["name"], d["namespace"])
    if o is None or o["metadata"].get("uid") != d["uid"]:
        return {"observed": "done", "uid": d["uid"]}
    return {"observed": "unresolved", "why": "the reader pod still exists"}


X.RECONCILERS[GONE_OP] = _reconcile_gone

def exec_argv(record: X.Record, ns: str, name: str, offset: int, max_bytes: int) -> List[str]:
    k = record.doc["kubeconfig"]
    return ["kubectl", "--kubeconfig", k["path"], "--context", k["context"], "-n", ns, "exec", name, "-c", "reader",
            "--", "python3", "-c", READER_SCRIPT, f"{MOUNT}/{LEDGER_FILE}", str(offset), str(max_bytes)]


def read(record: X.Record, kube: X.Kube, out: str, offset: int = 0, max_bytes: int = DEFAULT_MAX_BYTES,
         run: Callable = subprocess.run, sleep: Callable[[float], None] = time.sleep,
         now: Callable[[], str] = X.utcnow, clock: Callable[[], float] = time.monotonic) -> Dict:
    if os.path.exists(out):
        raise X.StagingError(f"{out} exists; refusing to overwrite")
    if not (0 < max_bytes <= MAX_BYTES_LIMIT) or offset < 0:
        raise X.StagingError(f"--max-bytes must be 1..{MAX_BYTES_LIMIT} and --offset >= 0")
    s = prepare._section(record)
    if prepare.stage_of(s) != "demonstration":
        raise X.StagingError("the ledger reader serves the demonstration stage only")
    params = prepare.load_params(record, "demonstration")
    probs = residency_problems(params)
    node, more = operator_node(kube, params, s)
    if probs + more:
        raise X.StagingError("; ".join(probs + more))
    stamp = now().replace("-", "").replace(":", "").lower()
    name = f"c28-ledger-reader-{stamp}"[:63].rstrip("-")
    ns = params["control_namespace"]
    np_, pod = render(params, s, name, node)
    for o in (np_, pod):
        if kube.get(o["kind"], name, ns) is not None:
            raise X.StagingError(f"{o['kind']}/{name} exists; refusing")
    raw, failure = b"", None
    kept = f"{out}.failed-{stamp}"                  # the transfer as received, written before any cleanup
    with X.mutating(record, kube, HELPER, {"read": name}):
        made = [kube.create(np_)]
        try:
            made.append(kube.create(pod))
            if not _wait(lambda: _ready(kube, ns, name), 180, sleep, clock):
                raise X.StagingError(f"the reader pod {name} did not become Ready within 180 s")
            try:
                p = run(exec_argv(record, ns, name, offset, max_bytes), capture_output=True, timeout=600)
                raw = p.stdout or b""
                if p.returncode != 0:
                    failure = f"kubectl exec exit {p.returncode}"  # stderr is never echoed (it may quote anything)
            except subprocess.TimeoutExpired as e:
                raw, failure = (e.output or b""), "kubectl exec timed out after 600 s"
            finally:
                with open(kept, "xb") as fh:
                    fh.write(raw)
        finally:
            if len(made) > 1:                       # the pod: deleted, then its disappearance proven as a journalled
                pod_uid = made[1]["metadata"]["uid"]   # step (an unproven one keeps the lock: no other reader starts)
                kube.delete("Pod", name, ns, pod_uid)
                kube.step(GONE_OP, "Pod", ns, name, pod_uid, lambda mark: _prove_gone(kube, ns, name, pod_uid, sleep, clock))
            kube.delete("NetworkPolicy", name, ns, made[0]["metadata"]["uid"])
    try:
        if failure:
            raise TransferRefused(failure)
        rec, body = parse_stream(raw)
    except TransferRefused as e:
        X.receipt(record, HELPER, "read-refused", {"pod": name, "why": str(e), "kept": kept,
                                                   "raw_bytes": len(raw), "raw_sha256": X.sha256_bytes(raw)})
        raise
    tmp = f"{out}.partial-{stamp}"
    with open(tmp, "xb") as fh:
        fh.write(body)
    os.replace(tmp, out)
    os.unlink(kept)
    data = {"pod": name, "node": node, "out": out, **rec}
    X.receipt(record, HELPER, "read", data)
    return data


def main(argv: Optional[List[str]] = None, api=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--record", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("render")
    r = sub.add_parser("read")
    r.add_argument("--out", required=True)
    r.add_argument("--offset", type=int, default=0)
    r.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    sub.add_parser("recover-plan", help="read-only: the held lock and every open intent, decided against the live state")
    rc = sub.add_parser("recover", help="apply the reviewed recover plan (the lock's executor proven terminated)")
    rc.add_argument("--plan", required=True)
    rc.add_argument("--plan-digest", required=True)
    a = ap.parse_args(argv)
    record = X.load_record(a.record, () if a.cmd.startswith("recover") else ("prepare",))
    if a.cmd == "render":
        s = prepare._section(record)
        params = prepare.load_params(record, prepare.stage_of(s))
        X.emit({"objects": render(params, s, "c28-ledger-reader-render", params["nodes"]["support"]),
                "residency_problems": residency_problems(params)})
        return 0
    kube = record.kube(api)
    if a.cmd == "recover-plan":
        X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
    elif a.cmd == "recover":
        X.emit(X.recover(record, kube, X.load_plan(a.plan), a.plan_digest))
    else:
        X.emit(read(record, kube, a.out, a.offset, a.max_bytes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
