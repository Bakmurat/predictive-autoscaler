"""Offline regressions of the image-roll smoke (smoke/roll-kind.sh, smoke/roll_kind.py): the ways its driver could
misreport. No cluster, no docker: the driver's cluster access is replaced, the shell script runs against stub tools."""

import argparse
import copy
import json
import os
import signal
import stat
import subprocess
import sys
import textwrap
import time

import pytest

SMOKE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "smoke")
sys.path.insert(0, SMOKE)
import roll_kind as RK  # noqa: E402

OLD = "registry.example/c28-fixture@sha256:" + "a" * 64
NEW = "registry.example/c28-fixture@sha256:" + "b" * 64


def smoke(tmp_path, stages=("bedcheck",), uid="original-cluster"):
    images = tmp_path / "images.json"
    images.write_text(json.dumps({"schema": "c28-images/v1", "verified": True, "images": {
        r: {"ref": OLD} for r in ("fixture", "operator", "statistical", "generator")}}))
    a = argparse.Namespace(work=str(tmp_path / "work"), kubeconfig=str(tmp_path / "kubeconfig"), context="kind-x",
                           images=str(images), new_fixture=NEW, query_api="http://127.0.0.1:1/prometheus", replicas=4,
                           kill_after=2, complete_budget=1, interrupt_budget=5, stages=list(stages))
    s = RK.Smoke(a)
    s.live_uid = uid
    s.run = lambda args, check=True, **kw: argparse.Namespace(returncode=0, stdout="https://127.0.0.1:6443", stderr="")
    s.kj = lambda *args: {"metadata": {"uid": s.live_uid}}
    return s


# --- 1. the cluster identity ------------------------------------------------------------------------------------------

def test_only_the_install_stage_may_record_a_cluster_and_every_later_invocation_requires_it(tmp_path):
    with pytest.raises(RK.Failed, match="records no cluster identity"):
        smoke(tmp_path, ("churn",)).guard()                     # a fresh directory, not installing: nothing adopted
    first = smoke(tmp_path, ("install",))
    first.guard()
    assert open(os.path.join(first.work, "cluster-uid")).read().strip() == "original-cluster"
    first.guard()                                               # the same cluster again: fine
    later = smoke(tmp_path, ("churn",))
    later.guard()
    other = smoke(tmp_path, ("churn",), uid="different-cluster")   # a separate invocation that reaches another cluster
    with pytest.raises(RK.Failed, match=r"different-cluster.*original-cluster"):
        other.guard()
    again = smoke(tmp_path, ("install",), uid="different-cluster")  # not even a second install adopts it
    with pytest.raises(RK.Failed, match="is not the one this work directory recorded"):
        again.guard()


def test_the_records_cluster_counts_when_the_identity_file_is_missing(tmp_path):
    s = smoke(tmp_path, ("churn",), uid="different-cluster")
    with open(os.path.join(s.work, "demo.json"), "w") as fh:
        json.dump({"cluster_uid": "original-cluster"}, fh)
    with pytest.raises(RK.Failed, match="original-cluster"):
        s.guard()
    with open(os.path.join(s.work, "cluster-uid"), "w") as fh:
        fh.write("third-cluster\n")
    with pytest.raises(RK.Failed, match="more than one cluster"):
        s.guard()


def test_a_guard_refuses_what_is_not_a_local_kind_cluster(tmp_path):
    s = smoke(tmp_path, ("install",))
    s.run = lambda args, check=True, **kw: argparse.Namespace(returncode=0, stdout="https://10.0.0.5:6443", stderr="")
    with pytest.raises(RK.Failed, match="not a local kind API server"):
        s.guard()


# --- 2. the interruption ----------------------------------------------------------------------------------------------

def _alive(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False


def _sleeper(s, pids):
    def executor(saved):                    # a harmless executor with a child of its own, as the helper has
        return [sys.executable, "-c", "import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', "
                "'import time; time.sleep(600)']); time.sleep(600)"]
    s.executor = executor
    real = subprocess.Popen

    def popen(*a, **kw):
        p = real(*a, **kw)
        pids.append(p.pid)
        return p
    return popen


@pytest.mark.parametrize("fault", ["journal-error", "never-reached", "executor-exits"])
def test_whatever_goes_wrong_while_waiting_the_executors_group_does_not_survive(tmp_path, monkeypatch, fault):
    s, pids = smoke(tmp_path), []
    s.demo = os.path.join(s.work, "demo.json")
    monkeypatch.setattr(RK.subprocess, "Popen", _sleeper(s, pids))
    if fault == "journal-error":
        s.journal = lambda: (_ for _ in ()).throw(ValueError("a corrupt receipt"))
        expected = ValueError
    elif fault == "never-reached":
        s.a.interrupt_budget = 1
        expected = RK.Failed
    else:
        s.executor = lambda saved: [sys.executable, "-c", "raise SystemExit(3)"]
        expected = RK.Failed
    def too_long(signum, frame):            # an unbounded wait must fail this test, not hang the whole run
        raise AssertionError("roll_killed did not return within 30 s")
    signal.signal(signal.SIGALRM, too_long)
    signal.alarm(30)
    try:
        with pytest.raises(expected):
            s.roll_killed({"plan": "p", "plan_digest": "d"}, after=1)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        for pid in pids:                    # whatever the driver left: never leave a sleeper behind a failed test
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue
            raise AssertionError("the executor's process group survived")
    assert pids


def test_the_journal_reader_waits_for_an_unfinished_line_and_refuses_a_corrupt_one(tmp_path):
    s = smoke(tmp_path)
    os.makedirs(os.path.dirname(s.journal_path()))
    done = json.dumps({"event": "member-rolled", "data": {}})
    with open(s.journal_path(), "w") as fh:
        fh.write(done + "\n" + done[:20])                       # the second receipt is being written
    assert [e["event"] for e in s.journal()] == ["member-rolled"]
    with open(s.journal_path(), "w") as fh:
        fh.write(done + "\n" + done + "\n")
    assert len(s.journal()) == 2
    with open(s.journal_path(), "w") as fh:
        fh.write(done + "\n" + "{not a receipt\n" + done + "\n")   # a COMPLETE line that is not JSON is corruption
    with pytest.raises(ValueError):
        s.journal()


def test_a_reached_interruption_point_ends_with_an_empty_group_and_a_record(tmp_path, monkeypatch):
    s, pids = smoke(tmp_path), []
    s.demo = os.path.join(s.work, "demo.json")
    monkeypatch.setattr(RK.subprocess, "Popen", _sleeper(s, pids))
    s.journal = lambda: [{"event": "member-rolled"}, {"event": "member-rolled"}]
    s.roll_killed({"plan": "p", "plan_digest": "d"}, after=2)
    rec = json.load(open(os.path.join(s.work, "roll-killed.json")))
    assert rec["process_group"] == pids[0] and rec["exit"] == -signal.SIGKILL and not _alive(pids[0])


# --- 4. the reverse check accepts exactly the cold-counter refusals ---------------------------------------------------

def _reversecheck(tmp_path, monkeypatch, problems):
    # the poll's clock and its sleep are replaced for THIS test only (monkeypatch restores the time module)
    monkeypatch.setattr(RK.time, "time", iter(range(0, 100000, 500)).__next__)   # the poll's deadline passes at once
    monkeypatch.setattr(RK.time, "sleep", lambda seconds: None)
    s = smoke(tmp_path, ("reversecheck",))
    with open(os.path.join(s.work, "cluster-before.json"), "w") as fh:
        json.dump(_cluster(OLD), fh)
    s.guard = lambda: None
    s.snapshot = lambda name: _cluster(OLD, ingress_uid="ingress-new")
    s.telemetry = lambda name: (4 if problems else 0, list(problems), {})
    s.helper = lambda *a, **k: (0, {"fence": None, "members": []}, "")
    return s


@pytest.mark.parametrize("problems", [
    ["telemetry steal_raw/w: no series"],                                   # unrelated missing telemetry alone
    sorted(RK.COLD_GAP)[:3],                                                # only part of the cold gap
    sorted(RK.COLD_GAP) + ["telemetry queue/c28-demo-pa-n: no series"],     # the gap plus something else
    [],                                                                     # no refusal at all
])
def test_after_a_reverse_only_exactly_the_four_cold_counter_refusals_count(tmp_path, monkeypatch, problems):
    s = _reversecheck(tmp_path, monkeypatch, problems)
    with pytest.raises(RK.Failed, match="should refuse for exactly that reason"):
        s.reversecheck()


def test_the_four_cold_counter_refusals_are_the_expected_limitation(tmp_path, monkeypatch):
    s = _reversecheck(tmp_path, monkeypatch, sorted(RK.COLD_GAP))
    s.reversecheck()
    assert "limitation" in s.checks[-1]["check"]


def test_this_module_leaves_the_time_module_as_it_found_it():
    import time as real                     # a replaced sleep once leaked into every later test of the run
    assert real.sleep.__name__ == "sleep" and real.time.__name__ == "time"


# --- 5. the independent cluster check ---------------------------------------------------------------------------------

def _cluster(image, pods="old", ingress_uid=None):
    doc = {f"{ns}/{kind}": [] for ns in ("demo", "pa-eval-system")
           for kind in ("deployments", "replicasets", "pods", "endpointslices")}
    for ns, name in RK.MEMBERS:
        ctr = {"pa-eval-system": "proxy"}.get(ns, "ingress" if name == "c28-ingress" else "fixture")
        n = RK.argparse and (4 if name.startswith("c28-demo-pa-") else 1)
        dep, rs = f"dep-{name}", f"rs-{name}-{pods}"
        doc[f"{ns}/deployments"].append({"metadata": {"name": name, "uid": dep}, "spec": {"replicas": n, "template": {
            "spec": {"containers": [{"name": ctr, "image": image, "env": [{"name": "UPSTREAM", "value": "x"}]}]}}}})
        doc[f"{ns}/replicasets"].append({"metadata": {"uid": rs, "ownerReferences": [{"uid": dep}]}})
        uids = [ingress_uid if (ingress_uid and name == "c28-ingress") else f"{name}-{pods}-{i}" for i in range(n)]
        for u in uids:
            doc[f"{ns}/pods"].append({"metadata": {"name": f"{name}-{u[-9:]}", "uid": u, "ownerReferences": [{"uid": rs}]},
                                      "status": {"containerStatuses": [{"name": ctr, "imageID": image,
                                                                        "restartCount": 0}]}})
        doc[f"{ns}/endpointslices"].append({"metadata": {"labels": {"kubernetes.io/service-name": name}},
                                            "endpoints": [{"conditions": {"ready": True}, "targetRef": {"uid": u}}
                                                          for u in uids]})
    doc["pa-eval-system/pods"].append({"metadata": {"name": "pa-eval-operator-1", "uid": "op"},
                                       "status": {"containerStatuses": [{"name": "operator", "restartCount": 0}]}})
    return doc


def _member(doc, name, kind="deployments"):
    ns = next(ns for ns, n in RK.MEMBERS if n == name)
    return doc[f"{ns}/{kind}"], ns


def test_the_cluster_check_passes_when_only_the_image_changed_and_every_pod_is_new(tmp_path):
    RK.Smoke.compare_cluster(_cluster(OLD), _cluster(NEW, "new"), NEW, OLD)


def _env_reference(after):                  # an unrelated value that happened to hold the old reference changes too
    deps, _ns = _member(after, "c28-demo-pa-n")
    dep = next(d for d in deps if d["metadata"]["name"] == "c28-demo-pa-n")
    dep["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = NEW


def _env_reference_before(before):
    deps, _ns = _member(before, "c28-demo-pa-n")
    dep = next(d for d in deps if d["metadata"]["name"] == "c28-demo-pa-n")
    dep["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = OLD


def _no_statuses(after):
    pods, _ns = _member(after, "c28-ingress", "pods")
    for p in pods:
        if p["metadata"]["uid"].startswith("c28-ingress"):
            p["status"]["containerStatuses"] = []


def _other_container_status(after):
    pods, _ns = _member(after, "c28-ingress", "pods")
    for p in pods:
        if p["metadata"]["uid"].startswith("c28-ingress"):
            p["status"]["containerStatuses"][0]["name"] = "sidecar"


def _pod_of(after, name):
    pods, _ns = _member(after, name, "pods")
    return next(p for p in pods if p["metadata"]["uid"].startswith(name + "-"))


def _empty_image_id(after):
    _pod_of(after, "c28-demo-pa-r")["status"]["containerStatuses"][0]["imageID"] = ""


def _old_pod_kept(after):                   # one pod of the target is still a pod from before the roll
    pod = _pod_of(after, "c28-demo-pa-r")
    slices, _ns = _member(after, "c28-demo-pa-r", "endpointslices")
    for s in slices:
        for e in s["endpoints"]:
            if e["targetRef"]["uid"] == pod["metadata"]["uid"]:
                e["targetRef"]["uid"] = "c28-demo-pa-r-old-0"
    pod["metadata"]["uid"] = "c28-demo-pa-r-old-0"


def _endpoint_missing(after):
    slices, _ns = _member(after, "c28-forecast-route", "endpointslices")
    next(s for s in slices if s["metadata"]["labels"]["kubernetes.io/service-name"] == "c28-forecast-route")[
        "endpoints"] = []


def _replicas(after):
    deps, _ns = _member(after, "c28-demo-pa-n")
    next(d for d in deps if d["metadata"]["name"] == "c28-demo-pa-n")["spec"]["replicas"] = 3


@pytest.mark.parametrize("damage,both,text", [
    (_env_reference, _env_reference_before, "changed in more than that container's image"),
    (_replicas, None, "changed in more than that container's image"),
    (_no_statuses, None, "each with a status of container ingress"),
    (_other_container_status, None, "each with a status of container ingress"),
    (_empty_image_id, None, "each with a status of container fixture"),
    (_old_pod_kept, None, "c28-demo-pa-r: its pods are not exactly the new ones"),
    (_endpoint_missing, None, "ready endpoints are not exactly the new pods"),
])
def test_the_cluster_check_refuses_what_only_looks_like_a_roll(damage, both, text):
    before, after = _cluster(OLD), _cluster(NEW, "new")
    if both:
        both(before)
    damage(after)
    with pytest.raises(RK.Failed, match=text):
        RK.Smoke.compare_cluster(before, after, NEW, OLD)


def test_a_release_pod_that_restarted_or_changed_is_not_untouched():
    before, after = _cluster(OLD), _cluster(NEW, "new")
    after["pa-eval-system/pods"][-1]["status"]["containerStatuses"][0]["restartCount"] = 1
    with pytest.raises(RK.Failed, match="release's pods"):
        RK.Smoke.compare_cluster(before, after, NEW, OLD)


# --- 3. the shell script's result comes after its cleanup -------------------------------------------------------------

STUBS = {
    "kind": """#!/bin/sh
    case "$1 $2" in
      "get clusters") exit 0 ;;
      "create cluster") while [ $# -gt 0 ]; do [ "$1" = --kubeconfig ] && : > "$2"; shift; done; exit 0 ;;
      "delete cluster") exit "${STUB_DELETE_RC:-0}" ;;
    esac
    """,
    "kubectl": """#!/bin/sh
    case "$*" in
      *"config current-context"*) echo "kind-$STUB_CLUSTER" ;;
      *"config view"*) printf 'https://127.0.0.1:6443' ;;
      *"get pods -o json"*) echo '{"items": []}' ;;
      *port-forward*) exec sleep 600 ;;
    esac
    exit 0
    """,
    "docker": "#!/bin/sh\nexit 0\n",
    "helm": """#!/bin/sh
    case "$*" in *" pull "*|pull*) printf chart > "victoria-metrics-k8s-stack-$STUB_CHART_VERSION.tgz" ;; esac
    exit 0
    """,
    "python-stub": """#!/bin/sh
    case "$1" in *roll_kind.py) exit "${STUB_DRIVER_RC:-0}" ;; esac
    exec "$STUB_REAL_PYTHON" "$@"
    """,
}


@pytest.fixture
def stubbed(tmp_path):
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    for name, body in STUBS.items():
        path = bin_ / name
        path.write_text(textwrap.dedent(body))
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    images = tmp_path / "images.json"
    images.write_text(json.dumps({"schema": "c28-images/v1", "verified": True, "images": {
        r: {"ref": OLD} for r in ("fixture", "operator", "statistical", "generator")}}))

    def run(out, **env):
        import hashlib
        full = {**os.environ, "PATH": f"{bin_}{os.pathsep}{os.environ['PATH']}", "PYTHON": str(bin_ / "python-stub"),
                "STUB_REAL_PYTHON": sys.executable, "CLUSTER": "pa-c28-roll-selftest",
                "STUB_CLUSTER": "pa-c28-roll-selftest", "STUB_CHART_VERSION": "0.0.0", "VM_CHART_VERSION": "0.0.0",
                "VM_CHART_SHA256": hashlib.sha256(b"chart").hexdigest(), "KUBECONFIG": "/dev/null", **env}
        return subprocess.run(["bash", os.path.join(SMOKE, "roll-kind.sh"), "--images", str(images), "--new-fixture",
                               NEW, "--out", str(tmp_path / out), "--scenario", "reverse"], env=full,
                              capture_output=True, text=True, timeout=120)
    return run, tmp_path


def test_the_result_is_published_only_after_a_successful_cleanup(stubbed):
    run, tmp = stubbed
    ok = run("ok")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "PASS: scenario reverse, 4 replicas per target" in ok.stdout.strip().splitlines()[-1]
    assert (tmp / "ok" / "cleanup.txt").read_text().startswith("deleted ")
    stuck = run("stuck", STUB_DELETE_RC="1")                    # every stage passed, the cluster could not be deleted
    assert stuck.returncode != 0 and "PASS" not in stuck.stdout and "could not delete" in stuck.stdout
    assert (tmp / "stuck" / "cleanup.txt").read_text().startswith("COULD NOT delete")
    kept = run("kept", KEEP="1", STUB_DELETE_RC="1")            # an explicit KEEP=1: a pass that says what was kept
    assert kept.returncode == 0 and "the cluster was KEPT (KEEP=1)" in kept.stdout.strip().splitlines()[-1]
    failed = run("failed", STUB_DRIVER_RC="1")                  # a failed driver is never a PASS, cleanup or not
    assert failed.returncode != 0 and "PASS" not in failed.stdout
    assert (tmp / "failed" / "cleanup.txt").read_text().startswith("deleted ")
