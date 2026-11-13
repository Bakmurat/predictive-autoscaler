"""ledger_read.py: the reader script (run here as the pod would run it), the transfer check, the rendered objects and
the journalled read against the in-memory API."""

import copy
import hashlib
import json
import os
import subprocess
import sys
import threading
import time

import pytest

import execlib as X
import fake2 as F
import ledger_read as L
import part2_fixtures as PF
import prepare
from part2_fixtures import api, make_record  # noqa: F401
from test_prepare_demo import DEMO, OWNER, demo_params


def fast():
    """A simulated clock that the no-op sleep advances: waits end by their budget without wall time."""
    now = [0.0]

    def sleep(s):
        now[0] += s
    return {"sleep": sleep, "clock": lambda: now[0]}


def run_script(path, offset=0, cap=L.DEFAULT_MAX_BYTES):
    p = subprocess.run([sys.executable, "-I", "-c", L.READER_SCRIPT, str(path), str(offset), str(cap)],
                       capture_output=True, timeout=60)
    assert p.returncode == 0, p.stderr
    return p.stdout


def lines(n, start=0):
    return b"".join(json.dumps({"at": i, "action": "recommend"}).encode() + b"\n" for i in range(start, start + n))


def test_the_script_captures_complete_lines_and_reports_the_rest(tmp_path):
    f = tmp_path / "forecasts.jsonl"
    f.write_bytes(lines(5) + b"not json\n" + b'{"partial": ')
    rec, body = L.parse_stream(run_script(f))
    assert body == lines(5) + b"not json\n"
    assert rec["lines"] == 6 and rec["malformed"] == 1 and rec["partial_tail_bytes"] == len(b'{"partial": ')
    assert rec["sha256"] == hashlib.sha256(body).hexdigest() and not rec["cap_hit"]
    assert rec["size_at_open"] == f.stat().st_size


def test_offset_and_cap(tmp_path):
    f = tmp_path / "forecasts.jsonl"
    f.write_bytes(lines(100))
    first = lines(10)
    rec, body = L.parse_stream(run_script(f, 0, len(first) + 5))     # cut inside line 11: only complete lines
    assert body == first and rec["cap_hit"] and rec["partial_tail_bytes"] == 5
    rec2, body2 = L.parse_stream(run_script(f, len(first)))          # the next read continues at the offset
    assert body + body2 == lines(100) and rec2["offset"] == len(first)
    p = subprocess.run([sys.executable, "-I", "-c", L.READER_SCRIPT, str(f), str(10 ** 9), "10"],
                       capture_output=True, timeout=60)
    assert p.returncode != 0                                           # an offset beyond the file is refused


def test_a_concurrent_append_never_yields_a_torn_line(tmp_path):
    f = tmp_path / "forecasts.jsonl"
    f.write_bytes(lines(1000))
    stop = threading.Event()

    def writer():
        i = 1000
        with open(f, "ab", buffering=0) as fh:
            while not stop.is_set():
                ln = lines(1, i)
                fh.write(ln[:7])                                       # a line arrives in two writes
                fh.write(ln[7:])
                i += 1
    t = threading.Thread(target=writer)
    t.start()
    try:
        for _ in range(20):
            rec, body = L.parse_stream(run_script(f))
            assert body.endswith(b"\n") and rec["malformed"] == 0
            assert f.read_bytes().startswith(body)                     # a prefix of the file
    finally:
        stop.set()
        t.join()


@pytest.mark.parametrize("mangle, text", [
    (lambda raw: raw[:len(raw) // 2], "interrupted"),
    (lambda raw: raw[:-1], "trailer"),
    (lambda raw: raw.replace(b'"at": 3', b'"at": 4'), "disagree"),
    (lambda raw: raw + b"extra\n", "trailer"),
    (lambda raw: b"", "no receipt"),
    (lambda raw: b'{"schema": "x"}\n', "receipt"),
])
def test_an_interrupted_or_altered_transfer_is_refused(tmp_path, mangle, text):
    f = tmp_path / "forecasts.jsonl"
    f.write_bytes(lines(10))
    with pytest.raises(L.TransferRefused) as e:
        L.parse_stream(mangle(run_script(f)))
    assert text in str(e.value)


# --- the objects and the journalled read ----------------------------------------------------------------------------

def demo_record(make_record, extra_reader=True):  # noqa: F811
    p = demo_params()
    p["control_namespace"] = "pa-eval-system"
    if extra_reader:
        p["residency"]["support_extra"] = [L.residency_entry()]
    s = PF.prepare_section()
    s["stage"] = "demonstration"
    s["reservation"] = {"record": "owner.json", "sha256": "0" * 64}
    rec = X.load_record(make_record({"prepare": s, "run_id": DEMO}, params=p), ("prepare",))
    return rec, prepare.load_params(rec, "demonstration"), s


def test_the_reader_pod_is_read_only_tokenless_and_placed_on_the_operator_node(make_record):  # noqa: F811
    rec, params, s = demo_record(make_record)
    np_, pod = L.render(params, s, "c28-ledger-reader-x", "worker-s")
    spec = pod["spec"]
    assert np_["spec"]["policyTypes"] == ["Ingress", "Egress"] and "ingress" not in np_["spec"] \
        and "egress" not in np_["spec"]
    assert np_["spec"]["podSelector"]["matchLabels"].items() <= pod["metadata"]["labels"].items()
    assert spec["automountServiceAccountToken"] is False and spec["enableServiceLinks"] is False
    assert "fsGroup" not in spec["securityContext"] and spec["securityContext"]["runAsUser"] == 10001
    c = spec["containers"][0]
    assert c["image"] == s["release"]["statistical_image"] and "@sha256:" in c["image"]
    assert c["volumeMounts"] == [{"name": "ledger", "mountPath": "/ledger", "readOnly": True}]
    assert spec["volumes"][0]["persistentVolumeClaim"] == {"claimName": s["storage"]["ledger_claim"]["name"],
                                                           "readOnly": True}
    assert c["securityContext"]["readOnlyRootFilesystem"] and c["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert spec["tolerations"] == [{"key": "c28.devkuban.com/reserved", "operator": "Equal", "value": OWNER,
                                    "effect": "NoSchedule"}]
    terms = spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
    assert {"key": "kubernetes.io/hostname", "operator": "In", "values": ["worker-s"]} in terms[0]["matchExpressions"]
    assert pod["metadata"]["labels"]["c28.devkuban.com/run"] == DEMO
    assert c["resources"]["limits"]["memory"] == "256Mi" and spec["activeDeadlineSeconds"] == 900


def operator_pod(api, s, node="worker-s"):  # noqa: F811
    o = F.pod("pa-eval-operator-1", "pa-eval-system", node,
              labels={"app.kubernetes.io/component": "operator", "app.kubernetes.io/instance": "pa-eval"})
    o["spec"]["volumes"] = [{"name": "ledger", "persistentVolumeClaim": {"claimName": s["storage"]["ledger_claim"]["name"]}}]
    api.put(o)


class ReadyApi(F.FakeApi2):
    """Created reader pods become Running and Ready; deleted objects disappear."""

    def create(self, path, body):
        out = super().create(path, body)
        if body.get("kind") == "Pod":
            o = self.obj("Pod", body["metadata"]["name"], body["metadata"]["namespace"])
            o["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
        return out


def fake_exec(stdout, code=0, seen=None):
    def run(argv, **kw):
        if seen is not None:
            seen.append(argv)
        return subprocess.CompletedProcess(argv, code, stdout=stdout, stderr=b"secret-ish text")
    return run


def test_a_read_creates_uses_and_deletes_the_reader_under_the_journal(make_record, tmp_path):  # noqa: F811
    rec, params, s = demo_record(make_record)
    a = ReadyApi()
    operator_pod(a, s)
    kube = rec.kube(api=a)
    f = tmp_path / "src.jsonl"
    f.write_bytes(lines(7) + b'{"x"')
    seen = []
    out = tmp_path / "ledger.jsonl"
    data = L.read(rec, kube, str(out), run=fake_exec(run_script(f), seen=seen), **fast())
    assert out.read_bytes() == lines(7) and data["lines"] == 7 and data["partial_tail_bytes"] == 4
    assert seen[0][seen[0].index("exec") + 1].startswith("c28-ledger-reader-") and seen[0][-3] == "/ledger/forecasts.jsonl"
    ops = [(m["op"], m["kind"]) for m in kube.mutations]
    assert ops == [("create", "NetworkPolicy"), ("create", "Pod"), ("delete", "Pod"), (L.GONE_OP, "Pod"),
                   ("delete", "NetworkPolicy")]                        # the pod proven gone before its policy goes
    assert not [p for p in os.listdir(tmp_path) if ".failed-" in p]  # the kept transfer is removed once verified


    assert not X.open_intents(X.receipts(rec, L.HELPER))
    assert not os.path.exists(X.lock_file(rec))
    assert X.last(X.receipts(rec, L.HELPER), "read")["data"]["sha256"] == hashlib.sha256(lines(7)).hexdigest()


def test_a_failed_exec_or_transfer_still_deletes_the_reader_and_keeps_the_raw_bytes(make_record, tmp_path):  # noqa: F811
    rec, params, s = demo_record(make_record)
    a = ReadyApi()
    operator_pod(a, s)
    kube = rec.kube(api=a)
    f = tmp_path / "src.jsonl"
    f.write_bytes(lines(50))
    out = tmp_path / "ledger.jsonl"
    raw = run_script(f)
    with pytest.raises(L.TransferRefused):
        L.read(rec, kube, str(out), run=fake_exec(raw[:len(raw) // 2]), **fast())
    assert not out.exists()
    kept = [p for p in os.listdir(tmp_path) if p.startswith("ledger.jsonl.failed-")]
    assert len(kept) == 1 and (tmp_path / kept[0]).read_bytes() == raw[:len(raw) // 2]
    assert [m["op"] for m in kube.mutations] == ["create", "create", "delete", L.GONE_OP, "delete"]
    kube.mutations.clear()
    out2 = tmp_path / "ledger2.jsonl"
    with pytest.raises(L.TransferRefused) as e:
        L.read(rec, kube, str(out2), run=fake_exec(raw, code=1), **fast())
    assert "secret-ish" not in str(e.value) and [m["op"] for m in kube.mutations][-3:] == ["delete", L.GONE_OP, "delete"]


def test_a_timed_out_transfer_keeps_what_was_received(make_record, tmp_path):  # noqa: F811
    rec, params, s = demo_record(make_record)
    a = ReadyApi()
    operator_pod(a, s)
    kube = rec.kube(api=a)

    def run(argv, **kw):
        raise subprocess.TimeoutExpired(argv, 600, output=b"partial-transfer")
    with pytest.raises(L.TransferRefused, match="timed out"):
        L.read(rec, kube, str(tmp_path / "l.jsonl"), run=run, **fast())
    kept = [p for p in os.listdir(tmp_path) if p.startswith("l.jsonl.failed-")]
    assert len(kept) == 1 and (tmp_path / kept[0]).read_bytes() == b"partial-transfer"
    assert X.last(X.receipts(rec, L.HELPER), "read-refused")["data"]["raw_bytes"] == len(b"partial-transfer")


class StickyApi(ReadyApi):
    """A DELETE of a reader pod is accepted but the pod never goes away (a stuck kubelet, a finalizer)."""

    def delete(self, path, uid, resource_version=None):
        if "/pods/" in path:
            self.calls.append(("delete", path))
            return {}
        return super().delete(path, uid, resource_version)


def test_an_unproven_pod_deletion_keeps_the_lock_and_the_policy(make_record, tmp_path):  # noqa: F811
    rec, params, s = demo_record(make_record)
    a = StickyApi()
    operator_pod(a, s)
    kube = rec.kube(api=a)
    f = tmp_path / "src.jsonl"
    f.write_bytes(lines(3))
    with pytest.raises(X.StagingError, match="still present"):
        L.read(rec, kube, str(tmp_path / "l.jsonl"), run=fake_exec(run_script(f)), **fast())
    assert [m["op"] for m in kube.mutations] == ["create", "create", "delete"]     # no policy delete, no gone step
    assert os.path.exists(X.lock_file(rec))                                         # the fence stays
    assert [it["data"]["op"] for it in X.open_intents(X.receipts(rec, L.HELPER))] == [L.GONE_OP]
    assert [p for p in os.listdir(tmp_path) if p.startswith("l.jsonl.failed-")]    # the transfer is kept
    pod = [o for o in a.objects.values() if o["metadata"]["name"].startswith("c28-ledger-reader-")
           and o.get("kind", "Pod") == "Pod" and "containers" in (o.get("spec") or {})]
    assert X.reconcile(kube, X.open_intents(X.receipts(rec, L.HELPER))[0]["data"], rec)["observed"] == "unresolved"
    a.objects = {k: v for k, v in a.objects.items() if not (v["metadata"]["name"].startswith("c28-ledger-reader-")
                                                              and "containers" in (v.get("spec") or {}))}
    assert X.reconcile(kube, X.open_intents(X.receipts(rec, L.HELPER))[0]["data"], rec)["observed"] == "done"


def test_a_maximum_read_stays_within_the_reader_memory_limit(tmp_path):
    f = tmp_path / "forecasts.jsonl"
    line = json.dumps({"at": 0, "action": "recommend", "pad": "x" * 400}).encode() + b"\n"
    with open(f, "wb") as fh:
        for _ in range(L.MAX_BYTES_LIMIT // (len(line) * 1000) + 1):
            fh.write(line * 1000)
    probe = ("import resource, subprocess, sys; "
             "subprocess.run([sys.executable, '-I', '-c', sys.argv[1], sys.argv[2], '0', sys.argv[3]], "
             "stdout=subprocess.DEVNULL, check=True); "
             "print(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)")
    p = subprocess.run([sys.executable, "-I", "-c", probe, L.READER_SCRIPT, str(f), str(L.MAX_BYTES_LIMIT)],
                       capture_output=True, timeout=300, check=True)
    kib = int(p.stdout)
    assert kib < 64 * 1024, f"{kib} KiB for a {L.MAX_BYTES_LIMIT}-byte read"     # far below the 256Mi limit


def test_a_read_is_refused_before_any_write(make_record, tmp_path):  # noqa: F811
    rec, params, s = demo_record(make_record)
    a = ReadyApi()
    kube = rec.kube(api=a)
    out = str(tmp_path / "x.jsonl")
    with pytest.raises(X.StagingError, match="exactly one running operator pod"):
        L.read(rec, kube, out, run=fake_exec(b""), **fast())
    operator_pod(a, s, node="worker-w")
    with pytest.raises(X.StagingError, match="not on S"):
        L.read(rec, kube, out, run=fake_exec(b""), **fast())
    rec2, _, s2 = demo_record(make_record, extra_reader=False)
    with pytest.raises(X.StagingError, match="support_extra must count the ledger reader"):
        L.read(rec2, rec2.kube(api=a), out, run=fake_exec(b""), **fast())
    assert kube.mutations == []
    with pytest.raises(X.StagingError, match="max-bytes"):
        L.read(rec, kube, out, max_bytes=L.MAX_BYTES_LIMIT + 1, run=fake_exec(b""), **fast())


FRESH = r'''
import json, sys
sys.path[:0] = [sys.argv[1], sys.argv[1] + "/tests"]
import execlib as X          # a fresh process of ANOTHER helper: ledger_read is never imported here
import fake2 as F
import prepare  # noqa: F401
assert "ledger_read" not in sys.modules
api = F.FakeApi2()
for o in json.load(open(sys.argv[3])):
    api.put(o)
rec = X.load_record(sys.argv[2])
plan = X.recover_plan(rec, rec.kube(api=api))
print(json.dumps([[a["op"], a["observed"]] for a in plan["actions"]]))
'''


def _stuck(make_record, tmp_path):  # noqa: F811
    rec, params, s = demo_record(make_record)
    a = StickyApi()
    operator_pod(a, s)
    kube = rec.kube(api=a)
    f = tmp_path / "src.jsonl"
    f.write_bytes(lines(3))
    with pytest.raises(X.StagingError, match="still present"):
        L.read(rec, kube, str(tmp_path / "l.jsonl"), run=fake_exec(run_script(f)), **fast())
    return rec, a


def _reader_pods(a):
    return [k for k, v in a.objects.items() if v["metadata"]["name"].startswith("c28-ledger-reader-")
            and "containers" in (v.get("spec") or {})]


def test_a_fresh_process_of_another_helper_decides_the_gone_intent(make_record, tmp_path):  # noqa: F811
    rec, a = _stuck(make_record, tmp_path)

    def fresh():
        objs = tmp_path / "objects.json"
        objs.write_text(json.dumps([v for v in a.objects.values() if v.get("kind")]))
        p = subprocess.run([sys.executable, "-I", "-c", FRESH, os.path.dirname(os.path.dirname(__file__)) or ".",
                            rec.path, str(objs)], capture_output=True, text=True, timeout=120,
                           env=dict(os.environ, KUBECONFIG="/dev/null"))
        assert p.returncode == 0, p.stderr[-2000:]
        return json.loads(p.stdout.strip().splitlines()[-1])
    assert [L.GONE_OP, "unresolved"] in fresh()               # blocked while the reader pod exists
    for k in _reader_pods(a):
        del a.objects[k]
    assert [L.GONE_OP, "done"] in fresh()                     # decided once it is gone


def test_the_cli_recovers_the_lock_only_after_the_pod_is_gone(make_record, tmp_path, capsys):  # noqa: F811
    rec, a = _stuck(make_record, tmp_path)
    PF.crash_owner(rec)
    assert L.main(["--record", rec.path, "recover-plan"], api=a) == 0
    saved = json.loads(capsys.readouterr().out)
    with pytest.raises(X.StagingError, match="unresolved"):
        L.main(["--record", rec.path, "recover", "--plan", saved["plan"], "--plan-digest", saved["plan_digest"]], api=a)
    assert os.path.exists(X.lock_file(rec))
    for k in _reader_pods(a):
        del a.objects[k]
    assert L.main(["--record", rec.path, "recover-plan"], api=a) == 0
    saved = json.loads(capsys.readouterr().out)
    assert L.main(["--record", rec.path, "recover", "--plan", saved["plan"], "--plan-digest", saved["plan_digest"]],
                  api=a) == 0
    assert not os.path.exists(X.lock_file(rec)) and not X.open_intents(X.receipts(rec, L.HELPER))


def test_a_slow_poll_counts_against_the_wait_budget():
    now, polls = [0.0], []

    def slow_poll():                       # every poll (an API read) takes 30 s
        polls.append(now[0])
        now[0] += 30
        return False

    def sleep(s):
        now[0] += s
    assert L._wait(slow_poll, 120, sleep, lambda: now[0]) is False
    assert len(polls) <= 5 and now[0] <= 120 + 30 + 2, (len(polls), now[0])
