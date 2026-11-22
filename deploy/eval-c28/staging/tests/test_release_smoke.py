"""Offline regressions of the release smoke (smoke/release-kind.sh, smoke/release_kind.py): the ways its driver could
misreport. No cluster, no docker, no Helm: the driver's cluster access is replaced, the shell script runs against stub
tools, and the processes it must end are harmless local ones."""

import argparse
import copy
import hashlib
import json
import os
import signal
import stat
import subprocess
import sys
import textwrap
import time

import pytest
import yaml

SMOKE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "smoke")
sys.path.insert(0, SMOKE)
import release_kind as RS  # noqa: E402

OLD = "registry.example/statistical@sha256:" + "a" * 64
NEW = "registry.example/statistical@sha256:" + "b" * 64
FC = RS.FORECASTER
ENTRY = {"name": "WATCH_NAMESPACES", "value": "demo"}


def smoke(tmp_path, scenario="A", stages=("A",)):
    images = tmp_path / "images.json"
    images.write_text(json.dumps({"schema": "c28-images/v1", "verified": True, "images": {
        r: {"ref": OLD} for r in ("fixture", "operator", "statistical", "generator")}}))
    for tool in ("helm", "secretmeta"):
        (tmp_path / tool).write_text(f"#!/bin/sh\necho {tool}\n")
        (tmp_path / tool).chmod(0o755)
    a = argparse.Namespace(work=str(tmp_path / "work"), kubeconfig=str(tmp_path / "kubeconfig"), context="kind-x",
                           images=str(images), new_statistical=NEW, helm=str(tmp_path / "helm"),
                           secretmeta=str(tmp_path / "secretmeta"), from_commit="f" * 40, replicas=2,
                           helm_timeout="5m", scenario=scenario, stages=list(stages), query_api="")
    return RS.Scenarios(a)


# --- what must and must not have changed --------------------------------------------------------------------------------

def cluster(image, generation=1, pods="one", env=()):
    """A release as the snapshot holds it: the forecaster with its ReplicaSet, pod and EndpointSlice, the operator with
    its pod, and a ConfigMap."""
    def meta(name, uid, rv="10", **extra):
        return {"name": name, "uid": uid, "resourceVersion": rv, **extra}
    dep = {"metadata": meta(FC, "dep-fc", str(generation), generation=generation),
           "spec": {"replicas": 1, "template": {"spec": {"containers": [
               {"name": "forecaster", "image": image, "env": [{"name": "LOG", "value": "info"}, *env]}]}}}}
    rs = {"metadata": meta(f"{FC}-{pods}", f"rs-{pods}", ownerReferences=[{"uid": "dep-fc"}])}
    pod = {"metadata": meta(f"{FC}-{pods}-x", f"pod-{pods}", ownerReferences=[{"uid": f"rs-{pods}"}]),
           "status": {"containerStatuses": [{"name": "forecaster", "containerID": f"c-{pods}", "restartCount": 0}]}}
    es = {"metadata": meta(f"{FC}-abc", "es", pods, labels={"kubernetes.io/service-name": FC})}
    op = {"metadata": meta("operator", "dep-op", generation=1), "spec": {"replicas": 1}}
    op_pod = {"metadata": meta("operator-1", "pod-op"),
              "status": {"containerStatuses": [{"name": "operator", "containerID": "c-op", "restartCount": 0}]}}
    cm = {"metadata": meta("config", "cm")}
    return {f"deployments/{FC}": dep, f"replicasets/{rs['metadata']['name']}": rs,
            f"pods/{pod['metadata']['name']}": pod, f"endpointslices/{FC}-abc": es, "deployments/operator": op,
            "pods/operator-1": op_pod, "configmaps/config": cm}


def upgraded():
    return cluster(OLD), cluster(NEW, 2, "two", env=(ENTRY,))


def test_an_upgrade_and_its_rollback_are_recognised_as_exactly_that():
    before, after = upgraded()
    out = RS.Smoke.compare(before, after, OLD, NEW, ENTRY)
    assert out == {"unchanged_objects": 3, "forecaster_family": 5, "generation": [1, 2]}
    back = cluster(OLD, 3, "three")
    assert RS.Smoke.compare(after, back, NEW, OLD, ENTRY, env="removed")["generation"] == [2, 3]


def _touch(key, **meta):
    def change(before, after):
        after[key]["metadata"].update(meta)
    return change


def _ctr(doc):
    return doc[f"deployments/{FC}"]["spec"]["template"]["spec"]["containers"][0]


@pytest.mark.parametrize("damage,text", [
    (_touch("configmaps/config", resourceVersion="11"), "objects other than the forecaster's changed"),
    (_touch("deployments/operator", uid="another"), "objects other than the forecaster's changed"),
    (_touch("pods/operator-1", resourceVersion="99"), "objects other than the forecaster's changed"),
    (lambda b, a: a.update({"rolebindings/extra": {"metadata": {"uid": "x", "resourceVersion": "1"}}}),
     "objects appeared or vanished"),
    (lambda b, a: a.pop("configmaps/config"), "objects appeared or vanished"),
    (lambda b, a: _ctr(a)["env"].append(dict(ENTRY)), "was not added exactly once"),
    (lambda b, a: _ctr(a)["env"].remove(ENTRY), "was not added exactly once"),
    (lambda b, a: _ctr(a).update(command=["sh"]), "changed in more than its image"),
    (lambda b, a: _ctr(a).update(image=OLD), "changed in more than its image"),
    (lambda b, a: a[f"deployments/{FC}"]["spec"].update(replicas=2), "changed in more than its image"),
    (lambda b, a: _ctr(a)["env"].append({"name": "EXTRA", "value": "1"}), "changed in more than its image"),
    (lambda b, a: a[f"deployments/{FC}"]["metadata"].update(uid="replaced"), "objects appeared or vanished"),
    (lambda b, a: _ctr(b).update(image=NEW), "did not run the expected image before"),
])
def test_anything_else_is_not_an_upgrade(damage, text):
    before, after = upgraded()
    damage(before, after)
    with pytest.raises(RS.Failed, match=text):
        RS.Smoke.compare(before, after, OLD, NEW, ENTRY)


@pytest.mark.parametrize("change", [dict(restartCount=1), dict(containerID="c-other")])
def test_an_untouched_pod_whose_container_restarted_or_changed_is_found(change):
    before, after = upgraded()
    after["pods/operator-1"]["status"]["containerStatuses"][0].update(change)
    with pytest.raises(RS.Failed, match="untouched pod restarted or was replaced"):
        RS.Smoke.compare(before, after, OLD, NEW, ENTRY)
    before, after = upgraded()
    after["pods/operator-1"]["status"]["containerStatuses"] = []          # no status is not "untouched"
    before["pods/operator-1"]["status"]["containerStatuses"] = []
    with pytest.raises(RS.Failed, match="untouched pod restarted or was replaced"):
        RS.Smoke.compare(before, after, OLD, NEW, ENTRY)


def test_the_forecasters_family_is_found_by_ownership_and_service_name_not_by_name():
    doc = cluster(OLD)
    doc["replicasets/looks-like-" + FC] = {"metadata": {"uid": "x", "resourceVersion": "1"}}       # not owned
    assert RS.Smoke.forecaster_family(doc) == {f"replicasets/{FC}-one", f"pods/{FC}-one-x", f"endpointslices/{FC}-abc"}


def test_tls_preservation_needs_a_new_pod_and_three_equal_facts(tmp_path):
    s = smoke(tmp_path)
    a = {"secret_uid": "u", "secret_resourceVersion": "7", "served_certificate_sha256": "c", "pod": {"name": "p1", "uid": "1"}}
    s.same_tls(a, dict(a, pod={"name": "p2", "uid": "2"}), "the upgrade")
    for change in (dict(secret_uid="v"), dict(secret_resourceVersion="8"), dict(served_certificate_sha256="d")):
        with pytest.raises(RS.Failed, match="changed across"):
            s.same_tls(a, dict(a, pod={"name": "p2", "uid": "2"}, **change), "the upgrade")
    with pytest.raises(RS.Failed, match="would prove nothing"):         # the same pod: an old certificate in memory
        s.same_tls(a, dict(a), "the upgrade")


# --- boundaries, processes, the journal ---------------------------------------------------------------------------------

def test_a_boundary_that_is_not_reached_is_a_failed_setup_not_a_pass(tmp_path):
    s = smoke(tmp_path)
    t0 = time.time()
    with pytest.raises(RS.Failed, match="scenario setup failed: the pending revision was not reached within 1 s"):
        s.wait_for("the pending revision", lambda: None, 1, every=0.1)
    assert time.time() - t0 < 5 and s.wait_for("x", lambda: "got", 1) == "got"


_KILLPG = os.killpg                                                     # the real one, whatever a test replaces


@pytest.fixture
def spawn():
    """Harmless local processes, each in a session of its own, ended when the test ends HOWEVER it ends: `group()`
    gives a process group with a member of its own and its recorded identity, `sleeper()` a single process."""
    made = []

    def start(code):
        made.append(subprocess.Popen([sys.executable, "-c", code], start_new_session=True))
        return made[-1]

    class Spawn:
        @staticmethod
        def sleeper():
            return start("import time; time.sleep(600)")

        @staticmethod
        def group():
            p = start("import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', "
                      "'import time; time.sleep(600)']); time.sleep(600)")
            import procgroup
            for _ in range(100):
                ident = procgroup.identity_of(p.pid)
                if ident:
                    return p, ident
                time.sleep(0.05)
            raise AssertionError("no identity")
    yield Spawn
    for p in made:
        try:
            _KILLPG(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        p.wait()


def _journal(s, *identities):
    path = os.path.join(s.work, "evidence-demo", "receipts")
    os.makedirs(path)
    with open(os.path.join(path, "release.jsonl"), "w") as fh:
        for i, ident in enumerate(identities, 1):
            fh.write(json.dumps({"event": "helm-launch", "data": {"step": str(i), "identity": ident}}) + "\n")
        fh.write('{"event": "unfinished')                               # a line being written: not a receipt yet


def test_a_signal_goes_only_to_a_recorded_group_that_is_alive_and_never_to_the_drivers_own(tmp_path, spawn, monkeypatch):
    s = smoke(tmp_path)
    p, ident = spawn.group()
    s.signal_group(ident, signal.SIGSTOP)
    assert s.group(ident) == "alive"                                    # stopped is alive
    s.signal_group(ident, signal.SIGKILL)
    p.wait(timeout=10)
    for _ in range(100):
        if s.group(ident) == "empty":
            break
        time.sleep(0.05)
    with pytest.raises(RS.Failed, match="scenario setup failed: Helm's recorded group is empty"):
        s.signal_group(ident, signal.SIGKILL)
    sent = []                                                           # from here on no signal is really sent
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: sent.append(pgid))
    s.group = lambda i: "alive"
    for pgid in (os.getpgid(0), os.getpid(), 0, 1):
        with pytest.raises(RS.Failed, match="scenario setup failed"):
            s.signal_group(dict(ident, pgid=pgid), signal.SIGKILL)
    assert sent == []


def test_every_error_path_ends_the_helper_processes_and_the_helm_groups_it_knows(tmp_path, spawn):
    s = smoke(tmp_path)
    p, ident = spawn.group()
    os.killpg(ident["pgid"], signal.SIGSTOP)                            # as a scenario's barrier leaves it
    helper = spawn.sleeper()
    s.procs.append(helper)
    _journal(s, ident)
    assert s.launch(1) == ident and s.launch(2) is None and s.launches() == 1 and s.events() == ["helm-launch"]
    s.reap()
    p.wait(timeout=10)                                                  # a group left stopped fails here, it does not hang
    assert helper.poll() is not None
    for _ in range(100):
        if s.group(ident) == "empty":
            break
        time.sleep(0.05)
    assert s.group(ident) == "empty"


def test_the_driver_reaps_even_when_a_scenario_fails_and_refuses_a_stage_of_another_scenario(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(RS.Scenarios, "reap", lambda self: seen.append("reaped"))
    monkeypatch.setattr(RS.Scenarios, "B", lambda self: (_ for _ in ()).throw(RS.Failed("boom")))
    monkeypatch.setattr(RS.Scenarios, "C", lambda self: seen.append("C ran"))
    base = smoke(tmp_path)
    argv = ["--work", base.work, "--kubeconfig", base.kcfg, "--context", "kind-x", "--images", base.a.images,
            "--new-statistical", NEW, "--helm", base.helm, "--secretmeta", base.meta, "--from-commit", "f" * 40]
    assert RS.main(argv + ["--scenario", "B", "B"]) == 1 and seen == ["reaped"]
    assert RS.main(argv + ["--scenario", "B", "C"]) == 1 and seen == ["reaped", "reaped"]     # C is not B's stage
    assert json.load(open(os.path.join(base.work, "checks.json"))) == []


# --- the candidate record and its variants ----------------------------------------------------------------------------

def candidate(tmp_path, scenario):
    s = smoke(tmp_path, scenario, (scenario,))
    os.makedirs(os.path.join(s.work, "evidence-demo"))
    values = os.path.join(s.work, "evidence-demo", "release-staging.yaml")
    repo, digest = OLD.rsplit("@", 1)
    with open(values, "w") as fh:
        yaml.safe_dump({"images": {"statisticalForecaster": {"repository": repo, "tag": "", "digest": digest},
                                   "operator": {"repository": "x", "digest": "sha256:" + "1" * 64}}}, fh)
    s.write("demo.json", {"prepare": {"release": {"statistical_image": OLD}}, "cluster_uid": "u"})
    s.run = lambda args, check=True, **kw: argparse.Namespace(returncode=0, stdout="v3.19.0\n", stderr="")
    s.kube_version = lambda: "1.35.9"
    s.release = lambda *a, **k: (0, {"history": [{"revision": 1}], "lock_held": False, "open_intents": []}, "")
    s.candidate(values)
    return s, json.load(open(s.demo)), values


def test_the_candidate_record_names_the_new_image_and_pins_both_executables(tmp_path):
    s, rec, values = candidate(tmp_path, "A")
    up = rec["release_upgrade"]
    assert rec["prepare"]["release"]["statistical_image"] == NEW
    assert up["from"] == {"revision": 1, "commit": "f" * 40, "values": values, "values_sha256": s.sha(values)}
    assert up["helm"] == {"binary": s.helm, "sha256": s.sha(s.helm), "version": "v3.19.0", "timeout": "5m",
                          "kube_version": "1.35.9", "api_versions": []}
    assert up["metadata_reader"] == {"binary": s.meta, "sha256": s.sha(s.meta)}
    new = yaml.safe_load(open(up["to"]["values"]))
    assert new["images"]["statisticalForecaster"] == {"repository": NEW.rsplit("@", 1)[0], "tag": "",
                                                      "digest": NEW.rsplit("@", 1)[1]}
    assert new["images"]["operator"] == {"repository": "x", "digest": "sha256:" + "1" * 64}      # nothing else moved
    assert up["to"]["values"] != values and up["to"]["values_sha256"] == s.sha(up["to"]["values"])
    assert json.load(open(os.path.join(s.work, "demo.installed.json")))["prepare"]["release"]["statistical_image"] == OLD
    with pytest.raises(FileExistsError):                                 # the new values file is written once
        s.candidate(values)


@pytest.mark.parametrize("scenario,timeout", [("B", "60s"), ("C", "5m"), ("F", "60s"), ("H", "60s")])
def test_a_failing_scenarios_record_names_a_digest_that_does_not_exist(tmp_path, scenario, timeout):
    _s, rec, _values = candidate(tmp_path, scenario)
    assert rec["prepare"]["release"]["statistical_image"] == NEW.rsplit("@", 1)[0] + "@sha256:" + RS.MISSING
    assert rec["release_upgrade"]["helm"]["timeout"] == timeout


@pytest.mark.parametrize("scenario,fails", [("G", "upgrade"), ("K", "rollback")])
def test_the_shim_is_the_records_declared_helm_and_is_archived_as_a_shim(tmp_path, scenario, fails):
    s, rec, _values = candidate(tmp_path, scenario)
    shim = rec["release_upgrade"]["helm"]["binary"]
    doc = json.load(open(os.path.join(s.work, "shim.json")))
    assert shim != s.helm and rec["release_upgrade"]["helm"]["sha256"] == s.sha(shim) == doc["sha256"]
    assert (doc["real_helm"], doc["real_helm_sha256"], doc["fails"]) == (s.helm, s.sha(s.helm), fails)
    assert "synthetic refusal test, not transport-loss evidence" in doc["what"] and "NOT HELM" in open(shim).read()
    run = lambda *args: subprocess.run([shim, *args], capture_output=True, text=True)  # noqa: E731
    assert run("history", "x").stdout == "helm\n" and run(fails, "x").stdout == "helm\n"     # no flag: the real one
    open(os.path.join(s.flags, f"fail-{fails}"), "w").close()
    refused = run(fails, "x")
    assert (refused.returncode, refused.stdout) == (1, "")               # exits 1 and the real Helm never ran
    other = "rollback" if fails == "upgrade" else "upgrade"
    assert run(other, "x").stdout == "helm\n" and run("history", "x").stdout == "helm\n"


def test_a_candidate_is_refused_when_the_values_do_not_name_the_installed_image(tmp_path):
    s = smoke(tmp_path)
    os.makedirs(os.path.join(s.work, "evidence-demo"))
    values = os.path.join(s.work, "evidence-demo", "release-staging.yaml")
    with open(values, "w") as fh:
        yaml.safe_dump({"images": {"statisticalForecaster": {"repository": "another", "digest": "sha256:" + "9" * 64}}}, fh)
    with pytest.raises(RS.Failed, match="do not name the installed statistical image"):
        s.candidate(values)


def test_the_finalize_child_dies_right_before_the_first_reconciliation_receipt(tmp_path):
    stub = tmp_path / "stub"
    stub.mkdir()
    (stub / "execlib.py").write_text(
        "def receipt(record, helper, event, data, clock=None):\n    open(record, 'a').write(event + '\\n')\n")
    (stub / "release.py").write_text(
        "import execlib as X\ndef main(argv):\n"
        "    for e in ('intent-2', 'step-2', 'reconciled-1', 'never'):\n        X.receipt(argv[0], 'release', e, {})\n"
        "    return 0\n")
    log = tmp_path / "events"
    p = subprocess.run([sys.executable, "-c", RS.FINALIZE_CHILD, str(stub), str(log)])
    assert p.returncode == -signal.SIGKILL and log.read_text().split() == ["intent-2", "step-2"]


def test_a_journal_being_appended_to_is_read_up_to_its_last_complete_line_and_a_corrupt_line_raises(tmp_path):
    s = smoke(tmp_path)
    assert s.journal() == [] and s.launches() == 0 and s.takeovers() == []
    path = os.path.join(s.work, "evidence-demo", "receipts")
    os.makedirs(path)
    done = json.dumps({"event": "helm-launch", "data": {"step": "1", "identity": {}}})
    with open(os.path.join(path, "release.jsonl"), "w") as fh:
        fh.write(done + "\n" + done[:25])
    assert s.launches() == 1
    with open(os.path.join(path, "release.jsonl"), "w") as fh:
        fh.write(done + "\n{not a receipt\n")
    with pytest.raises(ValueError):
        s.journal()
    with open(os.path.join(path, "recover.jsonl"), "w") as fh:
        fh.write(json.dumps({"event": "recovery-guard-takeover", "data": {}}) + "\n" +
                 json.dumps({"event": "lock-recovered", "data": {}}) + "\n")
    assert [e["event"] for e in s.takeovers()] == ["recovery-guard-takeover"]


def test_helms_output_in_a_file_is_found(tmp_path):
    s = smoke(tmp_path)
    s.no_helm_output()
    with open(os.path.join(s.work, "some.log"), "w") as fh:
        fh.write('Release "pa-eval" has been upgraded. Happy Helming!\n')
    with pytest.raises(RS.Failed, match="Helm's output reached a file"):
        s.no_helm_output()


# --- the shell script's result comes after its cleanup ------------------------------------------------------------------

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
    esac
    exit 0
    """,
    "docker": "#!/bin/sh\nexit 0\n",
    "helm-pinned": """#!/bin/sh
    case "$1" in
      pull) printf chart > "victoria-metrics-k8s-stack-$STUB_CHART_VERSION.tgz" ;;
      template) printf 'kind: CustomResourceDefinition\\nmetadata: {name: a.example}\\n---\\nkind: Deployment\\nmetadata: {name: operator}\\n' ;;
    esac
    exit 0
    """,
    "secretmeta-pinned": "#!/bin/sh\nexit 0\n",
    "python-stub": """#!/bin/sh
    case "$1" in *release_kind.py) exit "${STUB_DRIVER_RC:-0}" ;; esac
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

    def run(out, args=None, **env):
        full = {**os.environ, "PATH": f"{bin_}{os.pathsep}{os.environ['PATH']}", "PYTHON": str(bin_ / "python-stub"),
                "STUB_REAL_PYTHON": sys.executable, "CLUSTER": "pa-c28-release-selftest",
                "STUB_CLUSTER": "pa-c28-release-selftest", "STUB_CHART_VERSION": "0.0.0", "VM_CHART_VERSION": "0.0.0",
                "VM_CHART_SHA256": hashlib.sha256(b"chart").hexdigest(), "KUBECONFIG": "/dev/null", **env}
        argv = args or ["--images", str(images), "--new-statistical", NEW, "--helm", str(bin_ / "helm-pinned"),
                        "--secretmeta", str(bin_ / "secretmeta-pinned"), "--out", str(tmp_path / out),
                        "--scenario", "B"]
        return subprocess.run(["bash", os.path.join(SMOKE, "release-kind.sh"), *argv], env=full, capture_output=True,
                              text=True, timeout=120)
    return run, tmp_path, images, bin_


def test_the_result_is_published_only_after_a_successful_cleanup(stubbed):
    run, tmp, _images, _bin = stubbed
    ok = run("ok")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "PASS: scenario B: install B" in ok.stdout.strip().splitlines()[-1]
    assert (tmp / "ok" / "cleanup.txt").read_text().startswith("deleted ")
    crds = list(yaml.safe_load_all((tmp / "ok" / "scrape-crds.yaml").read_text()))
    assert [d["kind"] for d in crds] == ["CustomResourceDefinition"]    # of the pinned chart, the definitions only
    stuck = run("stuck", STUB_DELETE_RC="1")
    assert stuck.returncode != 0 and "PASS" not in stuck.stdout and "could not delete" in stuck.stdout
    kept = run("kept", KEEP="1", STUB_DELETE_RC="1")
    assert kept.returncode == 0 and "the cluster was KEPT (KEEP=1)" in kept.stdout.strip().splitlines()[-1]
    failed = run("failed", STUB_DRIVER_RC="1")
    assert failed.returncode != 0 and "PASS" not in failed.stdout
    assert (tmp / "failed" / "cleanup.txt").read_text().startswith("deleted ")


def test_the_bed_refuses_what_is_not_pinned_or_not_named(stubbed):
    run, tmp, images, bin_ = stubbed
    base = ["--images", str(images), "--new-statistical", NEW, "--secretmeta", str(bin_ / "secretmeta-pinned"),
            "--out", str(tmp / "x"), "--scenario", "B"]
    relative = run("x", base + ["--helm", "helm-pinned"])
    assert relative.returncode != 0 and "named by absolute path" in relative.stdout and not (tmp / "x").exists()
    assert run("x", base + ["--helm", str(bin_ / "helm-pinned"), "--scenario", "Z"]).returncode == 2
    same = run("y", ["--images", str(images), "--new-statistical", OLD, "--helm", str(bin_ / "helm-pinned"),
                     "--secretmeta", str(bin_ / "secretmeta-pinned"), "--out", str(tmp / "y"), "--scenario", "B"])
    assert same.returncode != 0 and "is the installed statistical image" in same.stdout
    wrong = run("z", STUB_CHART_VERSION="0.0.0", VM_CHART_SHA256="0" * 64)
    assert wrong.returncode != 0 and "not the pinned file" in wrong.stdout and "PASS" not in wrong.stdout


def test_a_copy_of_the_expected_state_is_not_shared_between_cases():
    before, after = upgraded()                                           # the builders return fresh objects each time
    assert copy.deepcopy(before) == cluster(OLD) and before is not cluster(OLD)
