#!/usr/bin/env python3
"""Kind check of the ledger reader (staging/ledger_read.py) on a real PersistentVolumeClaim, never a lab cluster:

  ledger_reader_kind.py --image <statistical image @sha256> --work <new dir> [--keep]

A throwaway kind cluster (own kubeconfig in --work) with one node labelled role=support and tainted like the
reservation. A writer pod with the operator's identity (uid, gid and fsGroup 65532) appends JSON lines to
forecasts.jsonl on a ReadWriteOnce claim the way the operator does (O_APPEND, mode 0644, each line in two writes,
continuously). The reader pod is rendered by ledger_read.render itself. Checks:
  R1 the reader runs as uid 10001 and reads the file (mode 0644, owner 65532) without any ownership change: the
     claim directory's and the file's uid, gid and mode are the same before and after the reader started;
  R2 the mount is read-only: a write from the reader fails;
  R3 ten reads during the appends: every transfer verifies (receipt = bytes = trailer), ends on a complete line, has
     no malformed line, and each read's bytes are a prefix of the next read's;
  R4 an interrupted transfer (kubectl exec killed mid-stream on a large read) is refused by parse_stream;
  R5 the reader pod has no service-account token mounted and no network (the NetworkPolicy object exists; kind's
     default CNI does not enforce it, which is recorded, not claimed).
"""

import argparse
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "staging"))
import ledger_read as L  # noqa: E402

CLUSTER, NS, NODE_ROLE = "pa-c28-ledger", "pa-eval-system", "support"
TAINT = ("c28.devkuban.com/reserved", "c28-kind-owner")
WRITER = r'''
import json, os, time
p = "/var/lib/predictive-autoscaler/forecasts.jsonl"
i = 0
while True:
    fd = os.open(p, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
    for _ in range(50):
        line = json.dumps({"at": i, "action": "recommend", "pad": "x" * 200}).encode() + b"\n"
        os.write(fd, line[:9]); os.write(fd, line[9:])
        i += 1
    os.close(fd)
    time.sleep(0.05)
'''
STAT = "import os,sys,json; print(json.dumps({p: [os.stat(p).st_uid, os.stat(p).st_gid, oct(os.stat(p).st_mode)] for p in sys.argv[1:]}))"


def log(msg):
    print(f"{time.strftime('%H:%M:%SZ', time.gmtime())}  {msg}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    if "@sha256:" not in a.image:
        sys.exit("--image must be pinned by digest")
    os.makedirs(a.work)
    kcfg = os.path.join(a.work, "kubeconfig")
    env = dict(os.environ, KUBECONFIG=kcfg)

    def sh(*argv, **kw):
        return subprocess.run(list(argv), env=env, check=kw.pop("check", True), capture_output=True, **kw)

    def k(*argv, **kw):
        return sh("kubectl", *argv, **kw)

    def apply(obj):
        k("apply", "-f", "-", input=json.dumps(obj).encode())

    if CLUSTER in sh("kind", "get", "clusters").stdout.decode().split():
        sys.exit(f"a kind cluster {CLUSTER} exists; refusing")
    results = {}
    try:
        sh("kind", "create", "cluster", "--name", CLUSTER, "--kubeconfig", kcfg, "--wait", "120s")
        # the node pulls the image by digest itself (kind load of a digest-only reference leaves a broken image record)
        node = k("get", "nodes", "-o", "jsonpath={.items[0].metadata.name}").stdout.decode()
        k("label", "node", node, f"c28.devkuban.com/role={NODE_ROLE}")
        k("create", "namespace", NS)
        apply({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
               "metadata": {"name": "c28-demo-operator-ledger", "namespace": NS},
               "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}})
        tol = [{"key": TAINT[0], "operator": "Equal", "value": TAINT[1], "effect": "NoSchedule"}]
        apply({"apiVersion": "v1", "kind": "Pod",
               "metadata": {"name": "writer", "namespace": NS, "labels": {"app.kubernetes.io/component": "operator"}},
               "spec": {"tolerations": tol, "automountServiceAccountToken": False,
                        "securityContext": {"runAsNonRoot": True, "runAsUser": 65532, "runAsGroup": 65532,
                                            "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch"},
                        "containers": [{"name": "w", "image": a.image, "command": ["python3", "-c", WRITER],
                                        "volumeMounts": [{"name": "l", "mountPath": "/var/lib/predictive-autoscaler"}]}],
                        "volumes": [{"name": "l", "persistentVolumeClaim": {"claimName": "c28-demo-operator-ledger"}}]}})
        k("-n", NS, "wait", "--for=condition=Ready", "pod/writer", "--timeout=180s")
        # tainted only now: kind's local-path helper pod (it creates the volume directory) tolerates no taint
        k("taint", "node", node, f"{TAINT[0]}={TAINT[1]}:NoSchedule")
        time.sleep(3)
        stat_args = ["/var/lib/predictive-autoscaler", "/var/lib/predictive-autoscaler/forecasts.jsonl"]
        before = json.loads(k("-n", NS, "exec", "writer", "--", "python3", "-c", STAT, *stat_args).stdout)
        params = {"control_namespace": NS, "nodes": {"support": node},
                  "reservation": {"taint_key": TAINT[0], "taint_value": TAINT[1], "role_label": "c28.devkuban.com/role"},
                  "demonstration": {"run_id": "c28-kind-demo"}}
        section = {"storage": {"ledger_claim": {"name": "c28-demo-operator-ledger"}},
                   "release": {"statistical_image": a.image}}
        name = "c28-ledger-reader-kind"
        for obj in L.render(params, section, name, node):
            apply(obj)
        k("-n", NS, "wait", "--for=condition=Ready", f"pod/{name}", "--timeout=180s")
        after = json.loads(k("-n", NS, "exec", "writer", "--", "python3", "-c", STAT, *stat_args).stdout)
        uid = k("-n", NS, "exec", name, "--", "python3", "-c", "import os; print(os.getuid())").stdout.decode().strip()
        dir_before, dir_after = before[stat_args[0]], after[stat_args[0]]
        f_before, f_after = before[stat_args[1]], after[stat_args[1]]
        ok1 = uid == "10001" and dir_before == dir_after and f_before[:2] == f_after[:2] == [65532, 65532] \
            and f_before[2] == f_after[2] and f_after[2].endswith("644")
        results["R1"] = {"pass": ok1, "reader_uid": uid, "dir": [dir_before, dir_after], "file": [f_before, f_after]}
        log(f"R1 {'PASS' if ok1 else 'FAIL'} {results['R1']}")
        w = k("-n", NS, "exec", name, "--", "python3", "-c", "open('/ledger/x', 'w')", check=False)
        results["R2"] = {"pass": w.returncode != 0, "exit": w.returncode,
                         "read_only_error": b"Read-only file system" in w.stderr}
        log(f"R2 {'PASS' if results['R2']['pass'] else 'FAIL'} {results['R2']}")
        prev, reads, ok3 = b"", [], True
        for i in range(10):
            p = k("-n", NS, "exec", name, "-c", "reader", "--", "python3", "-c", L.READER_SCRIPT,
                  f"{L.MOUNT}/{L.LEDGER_FILE}", "0", str(L.DEFAULT_MAX_BYTES), timeout=120)
            rec, body = L.parse_stream(p.stdout)
            good = rec["malformed"] == 0 and body.endswith(b"\n") and body.startswith(prev) and len(body) > len(prev)
            ok3 &= good
            reads.append({k_: rec[k_] for k_ in ("captured_bytes", "lines", "partial_tail_bytes", "malformed", "uid",
                                                 "mode", "sha256")})
            prev = body
            time.sleep(1)
        results["R3"] = {"pass": ok3, "reads": reads}
        log(f"R3 {'PASS' if ok3 else 'FAIL'} 10 reads, {reads[0]['lines']} .. {reads[-1]['lines']} lines, "
            f"partial tails {[r['partial_tail_bytes'] for r in reads]}")
        proc = subprocess.Popen(["kubectl", "-n", NS, "exec", name, "-c", "reader", "--", "python3", "-c",
                                 L.READER_SCRIPT, f"{L.MOUNT}/{L.LEDGER_FILE}", "0", str(L.DEFAULT_MAX_BYTES)],
                                env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        part = proc.stdout.read(len(prev) // 3)
        proc.kill()
        proc.wait()
        try:
            L.parse_stream(part)
            refused = False
        except L.TransferRefused as e:
            refused, why = True, str(e)
        results["R4"] = {"pass": refused, "received": len(part), "why": why if refused else ""}
        log(f"R4 {'PASS' if refused else 'FAIL'} {results['R4']}")
        spec = json.loads(k("-n", NS, "get", "pod", name, "-o", "json").stdout)["spec"]
        tok = any(m.get("mountPath", "").startswith("/var/run/secrets/kubernetes.io") for c in spec["containers"]
                  for m in c.get("volumeMounts", []))
        np_ok = k("-n", NS, "get", "networkpolicy", name, check=False).returncode == 0
        results["R5"] = {"pass": not tok and np_ok, "token_mounted": tok, "networkpolicy_present": np_ok,
                         "enforced": "not by kind's default CNI (kindnet); recorded, not claimed"}
        log(f"R5 {'PASS' if results['R5']['pass'] else 'FAIL'} {results['R5']}")
    finally:
        with open(os.path.join(a.work, "results.json"), "w") as fh:
            json.dump(results, fh, indent=1)
        if not a.keep:
            sh("kind", "delete", "cluster", "--name", CLUSTER, "--kubeconfig", kcfg, check=False)
    ok = len(results) == 5 and all(r["pass"] for r in results.values())
    log(f"ledger reader kind check: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
