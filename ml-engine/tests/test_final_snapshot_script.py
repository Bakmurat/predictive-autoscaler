"""deploy/prodcluster/demo/final-snapshot.sh: the arm pods' preStop final-counter snapshot (Codex Task 03 r18/r19).

The script runs under stub `curl`, `sleep` and `date` that emulate Envoy's admin API and the vminsert import endpoint.
A receipt may be pushed only after the drained, quiescent, settled counters were uploaded; every other path fails
closed (exit 1, no receipt).
"""
import json
import os
import stat
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "..", "deploy", "prodcluster", "demo", "final-snapshot.sh")
LINES = ['istio_requests_total{reporter="destination",response_code="200"} 721',
         'istio_request_duration_milliseconds_bucket{reporter="destination",le="5"} 700']

CURL = r'''#!PYTHON
import json, os, re, sys, urllib.parse
st_path = os.environ["STUB_STATE"]
st = json.load(open(st_path))
a = sys.argv[1:]
url = [x for x in a if x.startswith("http")][0]
method = "POST" if ("-X" in a and a[a.index("-X") + 1] == "POST") or "--data-binary" in a else "GET"
def save(): json.dump(st, open(st_path, "w"))
def out(s): sys.stdout.write(s); save(); sys.exit(0)
def err(): save(); sys.exit(22)
st["calls"].append(url.split("?")[0].rsplit("/", 1)[-1] + ":" + method)
if url.startswith(os.environ["SNAPSHOT_URL"]):
    body = open(a[a.index("--data-binary") + 1][1:]).read()
    kind = "receipt" if body.startswith("bench_final_snapshot_receipt") else "payload"
    if st["fail"].get(kind): err()
    st["pushes"].append({"kind": kind, "query": url.split("?", 1)[1], "body": body})
    out("")
if "/drain_listeners" in url:
    if st["fail"].get("drain"): err()
    st["drained"] = True
    st["stats"]["listener_manager.listener_stopped"] += st["stopped_on_drain"]
    out("OK\n")
if url.endswith("/stats/prometheus"):
    i = st["prom_reads"]
    text = st["prom"][min(i, len(st["prom"]) - 1)]
    st["prom_reads"] = i + 1
    out(text)
if url.endswith("/stats"):
    filt = urllib.parse.unquote_plus([x for x in a if x.startswith("filter=")][0][len("filter="):])
    if st["drained"]:
        for k in ("listener.0.0.0.0_15006.downstream_cx_active", "listener.0.0.0.0_15006.downstream_pre_cx_active",
                  "http.inbound_0.0.0.0_80;.downstream_rq_active"):
            if k in st["stats"] and st["stats"][k] > 0 and not st.get("stuck") and k not in st.get("stuck_keys", []):
                st["stats"][k] -= 1
    out("".join(f"{k}: {v}\n" for k, v in st["stats"].items() if re.search(filt, k)))
err()
'''


def run(tmp_path, **over):
    stub = tmp_path / "bin"; stub.mkdir()
    (stub / "curl").write_text(CURL.replace("#!PYTHON", "#!" + sys.executable))
    (stub / "sleep").write_text("#!/bin/sh\nexit 0\n")
    # a clock that advances 1 s per reading, so the wall-clock deadline is reachable under a no-op sleep
    (stub / "date").write_text("#!/bin/sh\nc=$(cat \"$STUB_CLOCK\" 2>/dev/null || echo 0); echo $((c + 1)) > \"$STUB_CLOCK\"\n"
                               "case \"$*\" in *%s*) echo $((1791180000 + c));; *) echo 2026-10-05T07:00:00Z;; esac\n")
    for f in stub.iterdir():
        f.chmod(f.stat().st_mode | stat.S_IEXEC)
    state = {"stats": {"listener_manager.listener_stopped": 3, "listener.0.0.0.0_15006.downstream_cx_active": 2,
                       "listener.0.0.0.0_15006.downstream_pre_cx_active": 1,
                       "http.inbound_0.0.0.0_80;.downstream_rq_active": 1, "server.uptime": 900,
                       "server.hot_restart_epoch": 0},
             "stopped_on_drain": 2, "drained": False, "prom": ["\n".join(LINES) + "\n# TYPE x counter\n"],
             "prom_reads": 0, "fail": {}, "pushes": [], "calls": []}
    for k, v in over.items():
        if k == "drop_stats":
            for s in v:
                state["stats"].pop(s)
        else:
            state[k] = v
    sp = tmp_path / "state.json"; sp.write_text(json.dumps(state))
    log = tmp_path / "log.txt"
    env = dict(os.environ, PATH=f"{stub}:{os.environ['PATH']}", STUB_STATE=str(sp), FINAL_SNAPSHOT_LOG=str(log),
               FINAL_SNAPSHOT_TMP=str(tmp_path), STUB_CLOCK=str(tmp_path / "clock"), SNAPSHOT_URL="http://vminsert.test/api/v1/import/prometheus",
               POD_NAME="nginx-test-abc-1", POD_UID="uid-1", POD_NAMESPACE="demo")
    p = subprocess.run(["sh", SCRIPT], env=env, capture_output=True, text=True, timeout=60)
    return p.returncode, json.loads(sp.read_text()), (log.read_text() if log.exists() else "")


def test_happy_path_pushes_settled_counters_then_the_receipt(tmp_path):
    rc, st, log = run(tmp_path)
    assert rc == 0, log
    assert [p["kind"] for p in st["pushes"]] == ["payload", "receipt"]
    payload = st["pushes"][0]["body"].splitlines()
    ts = int(payload[0].rsplit(" ", 1)[1]) // 1000
    assert payload == [f"bench_final_{line} {ts}000" for line in LINES]             # renamed, capture-time stamped
    q = st["pushes"][0]["query"]
    assert "extra_label=pod=nginx-test-abc-1" in q and "extra_label=pod_uid=uid-1" in q and "snapshot_version=1" in q
    receipt = st["pushes"][1]["body"]
    assert 'series="2"' in receipt and 'proxy_uptime_s="900"' in receipt and receipt.endswith(f" {ts} {ts}000\n")
    import hashlib
    assert hashlib.sha256(st["pushes"][0]["body"].encode()).hexdigest() in receipt
    assert st["calls"].index("drain_listeners:POST") < st["calls"].index("prometheus:GET")
    assert "ok series=2" in log


@pytest.mark.parametrize("case,over,msg", [
    ("drain refused", {"fail": {"drain": True}}, "drain_listeners request failed"),
    ("listeners not stopped", {"stopped_on_drain": 0}, "not quiescent"),
    ("connections stay open", {"stuck": True}, "not quiescent"),
    ("idle socket never sends a byte", {"stuck_keys": ["listener.0.0.0.0_15006.downstream_pre_cx_active"]}, "not quiescent"),
    ("request still in flight", {"stuck_keys": ["http.inbound_0.0.0.0_80;.downstream_rq_active"]}, "not quiescent"),
    ("connection gauge missing", {"drop_stats": ["listener.0.0.0.0_15006.downstream_cx_active"]}, "gauges missing"),
    ("request gauge missing", {"drop_stats": ["http.inbound_0.0.0.0_80;.downstream_rq_active"]}, "gauges missing"),
    ("pre-connection gauge missing", {"drop_stats": ["listener.0.0.0.0_15006.downstream_pre_cx_active"]}, "gauges missing"),
    ("stopped counter missing", {"drop_stats": ["listener_manager.listener_stopped"]}, "listener_stopped not readable"),
    ("counters keep changing", {"prom": [LINES[0] + "\n", LINES[0].replace("721", "722") + "\n"] * 4}, "still changing"),
    ("no istio counters", {"prom": ["# nothing\n"]}, "counter read failed"),
    ("payload upload fails", {"fail": {"payload": True}}, "payload upload failed"),
    ("receipt upload fails", {"fail": {"receipt": True}}, "receipt upload failed"),
    ("uptime missing", {"drop_stats": ["server.uptime"]}, "server.uptime not readable"),
])
def test_every_failure_is_closed_and_leaves_no_receipt(tmp_path, case, over, msg):
    rc, st, log = run(tmp_path, **over)
    assert rc == 1 and msg in log, (case, log)
    assert not [p for p in st["pushes"] if p["kind"] == "receipt"], case
    if case != "receipt upload fails":
        assert not st["pushes"], case


def test_identity_is_required(tmp_path):
    env = dict(os.environ, SNAPSHOT_URL="http://v", POD_NAME="p", POD_UID="", POD_NAMESPACE="demo",
               FINAL_SNAPSHOT_LOG=str(tmp_path / "log"))
    p = subprocess.run(["sh", SCRIPT], env=env, capture_output=True, text=True, timeout=30)
    assert p.returncode == 1 and "pod identity" in (tmp_path / "log").read_text()
