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

def key(kind, name, ns=RS.CONTROL_NS):
    return f"{kind}/{ns or '-'}/{name}"


DEP = RS.DEPLOYMENT


def cluster(image, generation=1, pods="one", env=()):
    """A release as a snapshot returns it: the forecaster with its ReplicaSet, pod and EndpointSlice, the operator with
    its pod, a ConfigMap, the TLS Secret (metadata only), a Role of ANOTHER namespace, a ClusterRole, a
    ClusterRoleBinding and a scrape object."""
    def obj(kind, name, uid, rv="10", ns=RS.CONTROL_NS, **extra):
        meta = {"name": name, "uid": uid, "resourceVersion": rv, **extra}
        if ns:
            meta["namespace"] = ns
        return {"kind": kind, "metadata": meta}
    dep = obj("Deployment", FC, "dep-fc", str(generation), generation=generation)
    dep["spec"] = {"replicas": 1, "template": {"spec": {"containers": [
        {"name": "forecaster", "image": image, "env": [{"name": "LOG", "value": "info"}, *env]}]}}}
    rs = obj("ReplicaSet", f"{FC}-{pods}", f"rs-{pods}", ownerReferences=[{"uid": "dep-fc"}])
    pod = obj("Pod", f"{FC}-{pods}-x", f"pod-{pods}", ownerReferences=[{"uid": f"rs-{pods}"}])
    pod["status"] = {"containerStatuses": [{"name": "forecaster", "containerID": f"c-{pods}", "restartCount": 0}]}
    es = obj("EndpointSlice", f"{FC}-abc", "es", pods, labels={"kubernetes.io/service-name": FC})
    op = obj("Deployment", "operator", "dep-op", generation=1)
    op["spec"] = {"replicas": 1}
    op_pod = obj("Pod", "operator-1", "pod-op")
    op_pod["status"] = {"containerStatuses": [{"name": "operator", "containerID": "c-op", "restartCount": 0}]}
    rest = [obj("ConfigMap", "config", "cm"), obj("Secret", f"{FC}-tls", "tls"),
            obj("Role", "watch", "role-demo", ns="demo"), obj("ClusterRole", "pa-eval-operator", "cr", ns=None),
            obj("ClusterRoleBinding", "pa-eval-operator", "crb", ns=None),
            obj("VMServiceScrape", "pa-eval-forecaster", "scrape")]
    return {RS.okey(o): o for o in (dep, rs, pod, es, op, op_pod, *rest)}


def upgraded():
    return cluster(OLD), cluster(NEW, 2, "two", env=(ENTRY,))


def test_an_upgrade_and_its_rollback_are_recognised_as_exactly_that():
    before, after = upgraded()
    assert key("ClusterRole", "pa-eval-operator", None) == "ClusterRole/-/pa-eval-operator" in before
    assert key("Role", "watch", "demo") in before and DEP in before
    out = RS.Smoke.compare(before, after, OLD, NEW, ENTRY)
    assert out == {"unchanged_objects": 8, "forecaster_family": 5, "generation": [1, 2]}
    back = cluster(OLD, 3, "three")
    assert RS.Smoke.compare(after, back, NEW, OLD, ENTRY, env="removed")["generation"] == [2, 3]


def _touch(k, **meta):
    def change(before, after):
        after[k]["metadata"].update(meta)
    return change


def _ctr(doc):
    return doc[DEP]["spec"]["template"]["spec"]["containers"][0]


OTHERS = [key("ConfigMap", "config"), key("Deployment", "operator"), key("Pod", "operator-1"),
          key("Secret", f"{FC}-tls"), key("Role", "watch", "demo"), key("ClusterRole", "pa-eval-operator", None),
          key("ClusterRoleBinding", "pa-eval-operator", None), key("VMServiceScrape", "pa-eval-forecaster")]


@pytest.mark.parametrize("k", OTHERS)
def test_every_other_object_whatever_its_scope_or_namespace_must_be_the_same_object_unchanged(k):
    for damage, text in ((_touch(k, resourceVersion="11"), "objects other than the forecaster's changed"),
                         (_touch(k, uid="replaced"), "objects other than the forecaster's changed"),
                         (lambda b, a: a.pop(k), "objects appeared or vanished")):
        before, after = upgraded()
        damage(before, after)
        with pytest.raises(RS.Failed, match=text):
            RS.Smoke.compare(before, after, OLD, NEW, ENTRY)


@pytest.mark.parametrize("damage,text", [
    (lambda b, a: a.update({key("RoleBinding", "extra"): {"kind": "RoleBinding", "metadata": {
        "uid": "x", "resourceVersion": "1"}}}), "objects appeared or vanished"),
    (lambda b, a: a.update({key("ClusterRole", "extra", None): {"kind": "ClusterRole", "metadata": {
        "uid": "x", "resourceVersion": "1"}}}), "objects appeared or vanished"),
    (lambda b, a: _ctr(a)["env"].append(dict(ENTRY)), "was not added exactly once"),
    (lambda b, a: _ctr(a)["env"].remove(ENTRY), "was not added exactly once"),
    (lambda b, a: _ctr(a).update(command=["sh"]), "changed in more than its image"),
    (lambda b, a: _ctr(a).update(image=OLD), "changed in more than its image"),
    (lambda b, a: a[DEP]["spec"].update(replicas=2), "changed in more than its image"),
    (lambda b, a: _ctr(a)["env"].append({"name": "EXTRA", "value": "1"}), "changed in more than its image"),
    (lambda b, a: a[DEP]["metadata"].update(uid="replaced"), "objects appeared or vanished"),
    (lambda b, a: _ctr(b).update(image=NEW), "did not run the expected image before"),
])
def test_anything_else_is_not_an_upgrade(damage, text):
    before, after = upgraded()
    damage(before, after)
    with pytest.raises(RS.Failed, match=text):
        RS.Smoke.compare(before, after, OLD, NEW, ENTRY)


@pytest.mark.parametrize("change", [dict(restartCount=1), dict(containerID="c-other")])
def test_an_untouched_pod_whose_container_restarted_or_changed_is_found(change):
    k = key("Pod", "operator-1")
    before, after = upgraded()
    after[k]["status"]["containerStatuses"][0].update(change)
    with pytest.raises(RS.Failed, match="untouched pod restarted or was replaced"):
        RS.Smoke.compare(before, after, OLD, NEW, ENTRY)
    before, after = upgraded()
    after[k]["status"]["containerStatuses"] = []                         # no status is not "untouched"
    before[k]["status"]["containerStatuses"] = []
    with pytest.raises(RS.Failed, match="untouched pod restarted or was replaced"):
        RS.Smoke.compare(before, after, OLD, NEW, ENTRY)


def test_the_forecasters_family_is_found_by_ownership_and_service_name_not_by_name():
    doc = cluster(OLD)
    doc[key("ReplicaSet", "looks-like-" + FC)] = {"kind": "ReplicaSet", "metadata": {"uid": "x", "resourceVersion": "1"}}
    assert RS.Smoke.forecaster_family(doc) == {key("ReplicaSet", f"{FC}-one"), key("Pod", f"{FC}-one-x"),
                                               key("EndpointSlice", f"{FC}-abc")}


# --- the snapshot: the whole rendered inventory, and nothing of value in a file ----------------------------------------

MARKER = "SYNTHETIC-CREDENTIAL-MARKER"
IDS = [{"kind": "ClusterRole", "group": "rbac.authorization.k8s.io", "namespace": None, "name": "pa-eval-operator"},
       {"kind": "ClusterRoleBinding", "group": "rbac.authorization.k8s.io", "namespace": None,
        "name": "pa-eval-operator"},
       {"kind": "Deployment", "group": "apps", "namespace": None, "name": "operator"},
       {"kind": "ConfigMap", "group": "", "namespace": None, "name": "config"},
       {"kind": "Deployment", "group": "apps", "namespace": None, "name": FC},
       {"kind": "Role", "group": "rbac.authorization.k8s.io", "namespace": "demo", "name": "watch"},
       {"kind": "Secret", "group": "", "namespace": None, "name": f"{FC}-tls"},
       {"kind": "VMServiceScrape", "group": "operator.victoriametrics.com", "namespace": None,
        "name": "pa-eval-forecaster"}]


def snapshotting(tmp_path, missing=None):
    """A driver whose cluster is `cluster(OLD)` with a credential marker in every place an object can carry one."""
    s = smoke(tmp_path)
    os.makedirs(s.work, exist_ok=True)
    live = cluster(OLD)
    _ctr(live)["env"].append({"name": "AUTH_TOKEN", "value": MARKER})
    live[key("ConfigMap", "config")]["data"] = {"password": MARKER}
    for o in live.values():
        o["metadata"]["annotations"] = {"note": MARKER}
        o["metadata"].setdefault("labels", {})["x"] = MARKER
    live[key("Deployment", "stray", "elsewhere")] = {"kind": "Deployment", "metadata": {
        "name": "stray", "namespace": "elsewhere", "uid": "s", "resourceVersion": "1"}}       # labelled, not rendered
    s._ids, calls = IDS, []
    singular = {"clusterrole.rbac.authorization.k8s.io": "ClusterRole", "configmap": "ConfigMap",
                "clusterrolebinding.rbac.authorization.k8s.io": "ClusterRoleBinding",
                "deployment.apps": "Deployment", "role.rbac.authorization.k8s.io": "Role",
                "vmservicescrape.operator.victoriametrics.com": "VMServiceScrape", "replicaset.apps": "ReplicaSet",
                "pod": "Pod", "endpointslice.discovery.k8s.io": "EndpointSlice"}

    def k(*args, check=True, stdin=None):
        calls.append(args)
        assert "secret" not in " ".join(args).lower().replace("secretmeta", "")
        if "-A" in args:                                                 # the label sweep
            kind = singular[args[args.index("get") + 1]]
            items = [o for o in live.values() if o["kind"] == kind and o["metadata"]["name"] in ("stray", FC)]
        elif args[-2:] == ("-o", "json") and len(args) == 7:             # one rendered object
            ns, kind, name = args[1], singular[args[3]], args[4]
            found = [o for o in live.values() if (o["kind"], o["metadata"]["name"]) == (kind, name) and
                     o["metadata"].get("namespace") in (ns, None) and name != missing]
            return argparse.Namespace(returncode=0 if found else 1, stdout=json.dumps(found[0]) if found else "")
        else:                                                            # the generated kinds of the control namespace
            items = [o for o in live.values() if o["kind"] == singular[args[3]]]
        return argparse.Namespace(returncode=0, stdout=json.dumps({"items": items}))
    s.k = k
    s.secret_meta = lambda name, *a, namespace=RS.CONTROL_NS: (
        calls.append(("secretmeta", namespace, name)) or {"found": True, "uid": "tls", "resourceVersion": "10"})
    return s, live, calls


def test_a_snapshot_holds_every_rendered_object_of_any_scope_and_its_file_holds_no_value(tmp_path):
    s, live, calls = snapshotting(tmp_path)
    doc = s.snapshot("captured.json")
    assert set(doc) == set(live)                                         # cluster-scoped, other namespaces, generated
    assert doc[key("Secret", f"{FC}-tls")] == {"kind": "Secret", "metadata": {
        "name": f"{FC}-tls", "namespace": RS.CONTROL_NS, "uid": "tls", "resourceVersion": "10"}}
    assert ("secretmeta", RS.CONTROL_NS, f"{FC}-tls") in calls           # the only way a Secret is observed
    assert ("-n", "demo", "get", "role.rbac.authorization.k8s.io", "watch", "-o", "json") in calls
    assert MARKER in json.dumps(doc)                                     # in memory, for the exact comparison
    text = open(os.path.join(s.work, "captured.json")).read()
    assert MARKER not in text and "AUTH_TOKEN" in text                   # names, never values
    filed = json.loads(text)
    assert set(filed) == set(live) and filed[key("ClusterRole", "pa-eval-operator", None)] == {
        "uid": "cr", "resourceVersion": "10", "generation": None, "owners": []}
    assert filed[DEP]["containers"] == [{"name": "forecaster", "image": OLD, "env_names": ["LOG", "AUTH_TOKEN"]}]
    assert filed[key("Pod", "operator-1")]["containers"] == [["operator", "c-op", 0]]
    assert all(set(v) <= {"uid", "resourceVersion", "generation", "owners", "containers"} for v in filed.values())


@pytest.mark.parametrize("name", ["pa-eval-operator", "watch", "pa-eval-forecaster"])
def test_a_rendered_object_that_is_not_in_the_cluster_fails_the_snapshot(tmp_path, name):
    s, _live, _calls = snapshotting(tmp_path, missing=name)
    with pytest.raises(RS.Failed, match=f"/{name} is not in the cluster"):
        s.snapshot("captured.json")


def test_a_rendered_secret_the_reader_does_not_find_fails_the_snapshot(tmp_path):
    s, _live, _calls = snapshotting(tmp_path)
    s.secret_meta = lambda name, *a, namespace=None: {"found": False}
    with pytest.raises(RS.Failed, match="was not found by the metadata reader"):
        s.snapshot("captured.json")


def test_the_inventory_is_the_installed_charts_rendering_and_its_output_is_never_shown(tmp_path):
    s = smoke(tmp_path)
    os.makedirs(os.path.join(s.work, "evidence-demo"))
    s.write("chart-installed.json", {"dir": "/chart"})
    s.kube_version = lambda: "1.35.9"
    rendering = ("apiVersion: v1\nkind: Secret\nmetadata: {name: tls}\nstringData: {k: " + MARKER + "}\n---\n"
                 "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata: {name: op}\n---\n"
                 "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: d, namespace: demo}\n")
    seen = []

    def run(args, check=True, **kw):
        seen.append(args)
        return argparse.Namespace(returncode=0, stdout=rendering, stderr="")
    s.run = run
    ids = s.identities()
    assert seen == [[s.helm, "template", RS.RELEASE, "/chart", "--namespace", RS.CONTROL_NS, "-f",
                     os.path.join(s.work, "evidence-demo", "release-staging.yaml"), "--kube-version", "1.35.9"]]
    assert ids == [{"kind": "ClusterRole", "group": "rbac.authorization.k8s.io", "namespace": None, "name": "op"},
                   {"kind": "Deployment", "group": "apps", "namespace": "demo", "name": "d"},
                   {"kind": "Secret", "group": "", "namespace": None, "name": "tls"}]
    assert s.identities() is ids and len(seen) == 1                      # rendered once
    assert MARKER not in open(os.path.join(s.work, "release-identities.json")).read()
    for bad, text in ((argparse.Namespace(returncode=1, stdout=MARKER, stderr=MARKER), "exit 1"),
                      (argparse.Namespace(returncode=0, stdout="kind: X\n", stderr=""), "without an identity"),
                      (argparse.Namespace(returncode=0, stdout=": :\n  - " + MARKER + ": [", stderr=""), "without an"),
                      (argparse.Namespace(returncode=0, stdout=rendering + "---\n" + rendering, stderr=""), "twice"),
                      (argparse.Namespace(returncode=0, stdout="", stderr=""), "is empty")):
        s._ids, s.run = None, (lambda args, check=True, bad=bad, **kw: bad)
        with pytest.raises(RS.Failed, match=text) as e:
            s.identities()
        assert MARKER not in str(e.value)


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


def test_a_signal_goes_only_to_a_group_proven_to_be_the_recorded_one_and_never_to_the_drivers_own(tmp_path, spawn,
                                                                                               monkeypatch):
    import procgroup
    s = smoke(tmp_path)
    p, ident = spawn.group()
    for _ in range(100):                                                 # until the group has its second member
        if len(procgroup.members(ident["pgid"])) == 2:
            break
        time.sleep(0.05)
    for wrong in (dict(ident, start=str(int(ident["start"]) + 1)), dict(ident, boot_id="another"),
                  dict(ident, host="another"), dict(ident, pid=os.getpid()), {"pgid": ident["pgid"]}, None, "x"):
        assert s.owned(wrong) is None                                    # alive is not ownership
        with pytest.raises(RS.Failed, match="scenario setup failed: Helm's recorded group is"):
            s.signal_group(wrong, signal.SIGKILL)
    assert s.group(dict(ident, start=str(int(ident["start"]) + 1))) == "alive" and p.poll() is None
    child = next(m for m in procgroup.members(ident["pgid"]) if m != ident["pid"])
    assert s.owned(procgroup.identity_of(child)) is None                # a member is not the recorded leader
    assert s.group(None) == "unknown"
    s.signal_group(ident, signal.SIGSTOP)
    assert s.group(ident) == "alive" and s.owned(ident) == ident["pgid"]     # stopped is alive, and still ours
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
    monkeypatch.setattr(procgroup, "identity_of", lambda pid: dict(ident, pid=pid, pgid=pid))    # "it is that one"
    s.signal_group(dict(ident, pid=4242, pgid=4242), signal.SIGTERM)
    assert sent == [4242]                                               # the stand-in works: only `mine` refuses below
    for own in (os.getpid(), os.getpgid(0), os.getsid(0), os.getppid(), os.getpgid(os.getppid()), 1, 0):
        with pytest.raises(RS.Failed, match="scenario setup failed"):
            s.signal_group(dict(ident, pid=own, pgid=own), signal.SIGKILL)
        s.known = [dict(ident, pid=own, pgid=own)]
        s.group = lambda i: "alive"
        monkeypatch.setattr(RS, "REAP_S", 0.2)
        assert any("not proven empty" in x for x in s.reap())           # the cleanup refuses it too, and says so
    assert sent == [4242]


def test_every_error_path_ends_the_helper_processes_and_the_helm_groups_it_knows(tmp_path, spawn):
    s = smoke(tmp_path)
    p, ident = spawn.group()
    os.killpg(ident["pgid"], signal.SIGSTOP)                            # as a scenario's barrier leaves it
    helper = spawn.sleeper()
    s.procs.append(helper)
    _journal(s, ident)
    assert s.launch(1) == ident and s.launch(2) is None and s.launches() == 1 and s.events() == ["helm-launch"]
    assert s.reap() == []                                               # the proof: nothing is left
    p.wait(timeout=10)
    assert helper.poll() is not None and s.group(ident) == "empty"


def test_the_cleanup_ends_the_helpers_first_and_then_reads_their_last_launches(tmp_path, spawn):
    """A helper that is still alive can launch Helm and publish the receipt while the cleanup runs."""
    s = smoke(tmp_path)
    late = []

    class Helper:
        pid = 1

        def poll(self):
            return None

        def kill(self):                                                  # its last act: one more launch, receipted
            late.append(spawn.group())
            _journal(s, late[0][1])

        def wait(self, timeout=None):
            return 0
    s.procs = [Helper()]
    assert s.reap() == []
    late[0][0].wait(timeout=10)
    assert s.group(late[0][1]) == "empty"


def test_a_journal_that_cannot_be_read_does_not_stop_the_cleanup_and_prevents_a_pass(tmp_path, spawn):
    s = smoke(tmp_path)
    p, ident = spawn.group()
    helper = spawn.sleeper()
    s.procs.append(helper)
    _journal(s, ident)
    assert s.launch(1) == ident                                          # known before the journal breaks
    with open(os.path.join(s.work, "evidence-demo", "receipts", "release.jsonl"), "a") as fh:
        fh.write("\n{a complete line that is not a receipt\n")
    problems = s.reap()
    assert len(problems) == 1 and "journal could not be read" in problems[0]
    p.wait(timeout=10)
    assert helper.poll() is not None and s.group(ident) == "empty"


def test_a_helper_that_cannot_be_ended_does_not_stop_the_cleanup(tmp_path, spawn):
    s = smoke(tmp_path)
    p, ident = spawn.group()
    _journal(s, ident)

    class Stuck:
        pid = 77

        def poll(self):
            raise OSError("no")
    helper = spawn.sleeper()
    s.procs = [Stuck(), helper]
    problems = s.reap()
    assert problems == ["helper process 77 could not be ended (OSError)"]
    p.wait(timeout=10)
    assert helper.poll() is not None and s.group(ident) == "empty"


def test_a_group_that_is_not_proven_empty_is_a_problem_within_a_bound(tmp_path, spawn, monkeypatch):
    s = smoke(tmp_path)
    p, ident = spawn.group()
    monkeypatch.setattr(RS, "REAP_S", 0.3)
    s.known = [dict(ident, start=str(int(ident["start"]) + 1))]         # occupied, but not provably ours
    t0 = time.time()
    problems = s.reap()
    assert problems == [f"Helm's recorded group {ident['pgid']} is alive after the cleanup: not proven empty"]
    assert time.time() - t0 < 5 and p.poll() is None                     # and it was NOT signalled


def test_a_process_naming_this_runs_kubeconfig_is_reported_and_never_signalled(tmp_path, spawn, monkeypatch):
    """A command line is not ownership: the search only reports, and what it reports prevents a pass."""
    s = smoke(tmp_path)
    assert s.strays() == []
    made = [subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)", *args], start_new_session=True)
            for args in (["--kubeconfig", s.kcfg], ["--kubeconfig", s.kcfg + ".another-run"])]
    try:
        spawn.sleeper()                                                  # names nothing of this run
        sent = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: sent.append(pid))
        monkeypatch.setattr(os, "killpg", lambda pgid, sig: sent.append(pgid))
        problems = s.reap()
        assert problems == [f"processes naming this run's kubeconfig are still running (not signalled: a command "
                            f"line is not ownership): {sorted(p.pid for p in made)}"]
        assert sent == [] and all(p.poll() is None for p in made)
    finally:
        monkeypatch.undo()
        for p in made:
            p.kill()
            p.wait()


@pytest.mark.parametrize("error,problem", [
    (PermissionError(13, "denied"), "1 process(es) could not be read: the search is incomplete and proves no absence"),
    (OSError(5, "EIO"), "1 process(es) could not be read: the search is incomplete and proves no absence"),
    (FileNotFoundError(2, "gone"), None), (ProcessLookupError(3, "gone"), None)])
def test_a_process_that_cannot_be_read_makes_the_search_incomplete_and_one_that_ended_does_not(tmp_path, spawn,
                                                                                               monkeypatch, error,
                                                                                               problem):
    s = smoke(tmp_path)
    target = spawn.sleeper()
    real = open

    def opening(path, *a, **kw):
        if path == f"/proc/{target.pid}/cmdline":
            raise error
        return real(path, *a, **kw)
    monkeypatch.setattr(RS, "open", opening, raising=False)
    assert s.strays() == ([problem] if problem else [])
    monkeypatch.undo()
    monkeypatch.setattr(os, "listdir", lambda path: (_ for _ in ()).throw(PermissionError(13, "denied")))
    [only] = s.strays()
    monkeypatch.undo()
    assert only.startswith("the process table could not be listed (PermissionError)")


def test_one_groups_signalling_error_does_not_stop_the_cleanup_of_the_others(tmp_path, spawn, monkeypatch):
    s = smoke(tmp_path)
    (p1, first), (p2, second) = spawn.group(), spawn.group()
    s.known = [first, "not an identity", second]
    monkeypatch.setattr(RS, "REAP_S", 0.3)
    s.strays = lambda: ["the search ran"]

    def killpg(pgid, sig):
        if pgid == first["pgid"]:
            raise PermissionError(1, "not permitted")
        return _KILLPG(pgid, sig)
    monkeypatch.setattr(os, "killpg", killpg)
    problems = s.reap()
    monkeypatch.undo()
    assert problems == [f"Helm's recorded group {first['pgid']} could not be signalled (PermissionError)"] * 2 + [
        f"Helm's recorded group {first['pgid']} is alive after the cleanup: not proven empty",
        "Helm's recorded group ? is unknown after the cleanup: not proven empty", "the search ran"]
    p2.wait(timeout=10)
    assert s.group(second) == "empty" and p1.poll() is None             # the second WAS cleaned up
    s.known, s.strays = [second], (lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert s.reap() == ["the search for processes of this run failed (RuntimeError)"]
    s.owned = lambda ident: (_ for _ in ()).throw(RuntimeError("x"))
    s.known, s.strays = [first, second], (lambda: [])
    assert s.reap() == [f"Helm's recorded group {i['pgid']} could not be cleaned up (RuntimeError)"
                        for i in (first, second)]


@pytest.mark.parametrize("scenario", ["executor_dies", "interrupt_rollback"])
def test_a_scenarios_resume_signal_needs_ownership_again(tmp_path, monkeypatch, scenario):
    """The barrier's SIGCONT goes through the same proof as its SIGSTOP: ownership lost in between fails the scenario."""
    s = smoke(tmp_path)
    ident, live, sent = {"pgid": 424242, "pid": 424242}, [True], []
    helper = argparse.Namespace(pid=999, kill=lambda: None, wait=lambda: live.__setitem__(0, False))
    s.start = lambda *a, **kw: helper
    s.plan = lambda cmd: ({"plan": "p"}, {"problems": []})
    s.release = lambda *a, **kw: (0, {"plan": "p"}, "")
    s.load = lambda path: {"actions": [{"observed": "unresolved", "why": "it is alive"}]}
    s.apply = lambda *a, **kw: (1, {}, "")
    s.status = lambda *a, **kw: {"lock_held": True}
    s.wait_for = lambda what, cond, *a, **kw: ident
    s.owned = lambda i: 424242 if live[0] else None                      # the proof holds until the executor is gone
    s.group = lambda i: "alive"
    s.wait_rolled_out = lambda: None
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: sent.append((pgid, sig)))
    with pytest.raises(RS.Failed, match="scenario setup failed: Helm's recorded group is alive, not a live group"):
        s.executor_dies("D") if scenario == "executor_dies" else s.interrupt_rollback({}, "E", 2, 3)
    assert sent == [(424242, signal.SIGSTOP)]


def test_only_two_places_send_a_signal_to_a_group_and_none_to_a_process():
    src = open(os.path.join(SMOKE, "release_kind.py")).read()
    driver = src.replace(RS.FINALIZE_CHILD, "")                          # the child's own SIGKILL to ITSELF apart
    assert driver.count("os.killpg(") == 2 and driver.count("os.kill(") == 0
    assert "pgid = self.owned(ident)" in driver.split("def signal_group")[1].split("os.killpg(")[0]
    assert "pgid = self.owned(ident)" in driver.split("def reap")[1].split("os.killpg(")[0]


def test_the_driver_reaps_even_when_a_scenario_fails_and_refuses_a_stage_of_another_scenario(tmp_path, monkeypatch):
    seen = []
    problems = []
    monkeypatch.setattr(RS.Scenarios, "reap", lambda self: seen.append("reaped") or list(problems))
    monkeypatch.setattr(RS.Scenarios, "no_helm_output", lambda self: seen.append("leak check"))
    monkeypatch.setattr(RS.Scenarios, "D", lambda self: seen.append("D ran"))
    monkeypatch.setattr(RS.Scenarios, "B", lambda self: (_ for _ in ()).throw(RS.Failed("boom")))
    monkeypatch.setattr(RS.Scenarios, "C", lambda self: seen.append("C ran"))
    base = smoke(tmp_path)
    argv = ["--work", base.work, "--kubeconfig", base.kcfg, "--context", "kind-x", "--images", base.a.images,
            "--new-statistical", NEW, "--helm", base.helm, "--secretmeta", base.meta, "--from-commit", "f" * 40]
    assert RS.main(argv + ["--scenario", "B", "B"]) == 1 and seen == ["reaped"]
    assert RS.main(argv + ["--scenario", "B", "C"]) == 1 and seen == ["reaped", "reaped"]     # C is not B's stage
    checks = lambda: [c["check"][:8] for c in json.load(open(os.path.join(base.work, "checks.json")))]  # noqa: E731
    assert checks() == ["cleanup:"] * 2                                  # one per run; the runs share the directory
    del seen[:]
    assert RS.main(argv + ["--scenario", "D", "D"]) == 0                 # the leak check follows EVERY scenario
    assert seen == ["D ran", "leak check", "reaped"] and checks() == ["cleanup:"] * 3
    problems.append("Helm's recorded group 5 is alive after the cleanup: not proven empty")
    assert RS.main(argv + ["--scenario", "D", "D"]) == 1                 # a scenario that passed, a cleanup that did not
    assert checks() == ["cleanup:"] * 3                                  # and no cleanup check is recorded for it
    monkeypatch.setattr(RS.Scenarios, "reap", lambda self: (_ for _ in ()).throw(RuntimeError("x")))
    assert RS.main(argv + ["--scenario", "D", "D"]) == 1 and checks() == ["cleanup:"] * 3


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
