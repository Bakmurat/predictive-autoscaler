"""release.py against a fake Helm state machine and the in-memory API: no test runs helm (the process runner has its
own tests, test_procgroup.py). The fake models Helm 3.19's history (pending and failure descriptions), the storage
Secrets' metadata, lost answers, foreign revisions and an interruption during a rollback; every byte it "prints"
carries a marker that must appear nowhere."""

import copy
import hashlib
import json
import os

import pytest
import yaml

import common_records as C
import execlib as X
import part2_fixtures as PF
import procgroup as P
import release as R
from part2_fixtures import api  # noqa: F401
from test_imageroll import PASS, W, served
from test_prepare_demo import demo_params, world  # noqa: F401

MARKER = "tok-SECRET-marker-91c2"
HELM, META = "helm-bin", "secretmeta-bin"
OLD_IMAGE = "harbor.example/autoscaler/statistical@sha256:" + "c" * 64


IDENT = {"pid": 4242, "pgid": 4242, "start": "777", "host": "test", "boot_id": "b"}


def result(**over):
    """What procgroup.run returns for a command that ran once and ended cleanly, unless told otherwise."""
    h = hashlib.sha256(MARKER.encode()).hexdigest()
    base = dict(started=True, identity=dict(IDENT), exit_kind="exited", exit_status=0, group_empty=True,
                output_complete=True, stdout_bytes=len(MARKER), stdout_sha256=h, stderr_bytes=len(MARKER),
                stderr_sha256=h)
    base.update(over)
    return P.Result(**base)


class FakeHelm:
    """procgroup.run's interface (argv, env=, cwd=, deadline_s=, record=, stdout_sink=) for helm and for
    c28-secretmeta. upgrade / rollback behave as `self.mode` says:
      ok            the new revision deployed (and the forecaster changed in the cluster)
      fail          the new revision `failed`, its description an error text
      pending       the new revision `pending-upgrade`, then helm is killed at the deadline
      lost          helm exits 1 and nothing is recorded      hang   helm is terminated, nothing is recorded
      foreign       a revision appears that does not carry the operation's label
      leftover      deployed, but the group could not be proven empty
      unrecorded    the gate is never opened (the identity could not be made durable)
      storage-swap  deployed, but the installed revision's storage Secret was replaced meanwhile
      storage-gone  deployed, but the installed revision's storage Secret is gone"""

    def __init__(self, w, from_docs, to_docs):
        self.w, self.docs, self.mode, self.calls, self.identity_seen = w, {"from": from_docs, "to": to_docs}, "ok", [], []
        self.version, self.fail_reads, self.uids, self.at_launch = "v3.19.0", set(), 0, None
        self.dirty_reads = {}                                   # command -> what was wrong with an answer it DID give
        self.raw_template = None                                # bytes to print instead of a rendering
        self.revisions = [self.rev(1, "deployed", "Install complete")]
        self.tls = {"uid": "tls-uid-1", "resourceVersion": "41"}

    def rev(self, n, status, description, labels=None):
        self.uids += 1
        return {"revision": n, "status": status, "chart": "predictive-autoscaler-0.9.0", "app_version": "0.9.0",
                "description": description, "labels": dict(labels or {}), "uid": f"storage-uid-{self.uids}"}

    def mutating(self):
        return [c for c in self.calls if c[0].endswith(HELM) and c[1] in ("upgrade", "rollback")]

    def __call__(self, argv, *, env, cwd, deadline_s, record, stdout_sink=None):
        self.calls.append(list(argv))
        assert cwd == "/" and set(env) <= set(R.ENV_KEEP) and os.path.isabs(argv[0])
        tool, cmd = os.path.basename(argv[0]), argv[1]
        if tool == META:
            return self.meta(argv, stdout_sink)
        if cmd in ("version", "history", "template"):
            if cmd in self.fail_reads:
                return result(exit_status=1)
            if cmd == "version":
                out = self.version.encode()
            elif cmd == "history":
                assert argv[-2:] == ["-o", "json"]
                out = json.dumps([{k: r[k] for k in ("revision", "status", "chart", "app_version", "description")}
                                  | {"updated": "now"} for r in self.revisions]).encode()
            else:
                assert "--kubeconfig" not in argv and "--kube-context" not in argv      # offline
                out = self.raw_template or \
                    yaml.safe_dump_all(self.docs["from" if "/from-tree/" in argv[3] else "to"]).encode()
            stdout_sink(out)
            return result(**self.dirty_reads.get(cmd, {}))
        assert stdout_sink is None                               # a mutating call's output goes nowhere
        if self.mode == "unrecorded":
            return P.Result(started=False, runner_error="OSError", exit_kind="never-executed")
        record(dict(IDENT))
        self.identity_seen.append(dict(IDENT))
        if self.at_launch:
            self.at_launch()
        return {"upgrade": self.upgrade, "rollback": self.rollback}[cmd](argv)

    def meta(self, argv, sink):
        name, keys = argv[argv.index("--name") + 1], [argv[i + 1] for i, a in enumerate(argv) if a == "--label-key"]
        if "meta" in self.fail_reads:
            return result(exit_status=3)
        doc = {"schema": "c28-secret-metadata/v1", "namespace": self.w.control_ns, "name": name, "found": False}
        if name.endswith("-tls"):
            doc.update(found=True, **self.tls)
        for r in self.revisions:
            if name == f"sh.helm.release.v1.{self.w.release}.v{r['revision']}" and not r.get("gone"):
                labels = {"name": self.w.release, "owner": "helm", "version": str(r["revision"]),
                          "status": r["status"], **r["labels"]}
                doc.update(found=True, uid=r["uid"], resourceVersion="7",
                           labels={k: v for k, v in labels.items() if k in keys})
        sink(json.dumps(doc).encode())
        return result()

    def label(self, argv):
        if "--labels" not in argv:
            return {}
        k, v = argv[argv.index("--labels") + 1].split("=", 1)
        return {k: v}

    def upgrade(self, argv):
        n, mode = self.revisions[-1]["revision"] + 1, self.mode
        if mode == "lost":
            return result(exit_status=1)
        if mode == "hang":
            return result(exit_kind="signaled", exit_status=15, deadline_exceeded=True, signals_sent=["TERM"])
        if mode == "foreign":
            self.revisions[-1]["status"] = "superseded"
            self.revisions.append(self.rev(n, "deployed", "Upgrade complete"))
            return result(exit_status=1)
        status, text = {"fail": ("failed", f"timed out waiting: {MARKER}"),
                        "pending": ("pending-upgrade", "Preparing upgrade")}.get(mode, ("deployed", "Upgrade complete"))
        if status == "deployed":
            self.revisions[-1]["status"] = "superseded"
            self.w.set_forecaster("to")
        if mode == "storage-swap":
            self.revisions[0]["uid"] = "storage-uid-swapped"
        if mode == "storage-gone":
            self.revisions[0]["gone"] = True
        self.revisions.append(self.rev(n, status, text, self.label(argv)))
        if mode == "pending":
            return result(exit_kind="signaled", exit_status=9, deadline_exceeded=True, signals_sent=["TERM", "KILL"])
        if mode == "leftover":
            return result(group_empty=None, survivors=True)
        return result(exit_status=0 if status == "deployed" else 1)

    def rollback(self, argv):
        dest, n, mode = int(argv[3]), self.revisions[-1]["revision"] + 1, self.mode
        if mode == "lost":
            return result(exit_status=1)
        if mode == "hang":
            return result(exit_kind="signaled", exit_status=15, deadline_exceeded=True, signals_sent=["TERM"])
        source = next(r for r in self.revisions if r["revision"] == dest)
        for r in self.revisions:
            if r["status"] == "deployed":
                r["status"] = "superseded"
        text = "Upgrade complete" if mode == "foreign" else f"Rollback to {dest}"
        self.revisions.append(self.rev(n, "failed" if mode == "fail" else "deployed", text, source["labels"]))
        if mode != "fail":
            self.w.set_forecaster("from")
        if mode == "pending":                                  # the revision is written, then helm is killed
            return result(exit_kind="signaled", exit_status=9, deadline_exceeded=True, signals_sent=["TERM", "KILL"])
        return result(exit_status=1 if mode == "fail" else 0)


def renderings(release, control_ns, ns, to_image):
    full = f"{release}-predictive-autoscaler"
    meta = lambda name, **lab: {"name": name, "namespace": control_ns,  # noqa: E731
                                "labels": {"app.kubernetes.io/instance": release, **lab}}
    dep = {"apiVersion": "apps/v1", "kind": "Deployment",
           "metadata": meta(f"{full}-forecaster", **{"app.kubernetes.io/component": "forecaster"}),
           "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "forecaster"}},
                    "template": {"metadata": {"labels": {"app": "forecaster"}},
                                 "spec": {"serviceAccountName": f"{full}-forecaster",
                                          "containers": [{"name": "forecaster", "image": OLD_IMAGE,
                                                          "env": [{"name": "LOG_LEVEL", "value": "info"}],
                                                          "ports": [{"containerPort": 8443}]}]}}}}
    frm = [{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": meta(f"{full}-forecaster")},
           {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": meta(f"{full}-operator"),
            "rules": [{"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get", "list", "watch"]}]},
           {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta(f"{full}-config"), "data": {"a": "1"}},
           {"apiVersion": "v1", "kind": "Secret", "metadata": meta(f"{full}-forecaster-tls"),
            "type": "kubernetes.io/tls", "data": {"tls.key": MARKER}},
           dep]
    to = copy.deepcopy(frm)
    ctr = to[-1]["spec"]["template"]["spec"]["containers"][0]
    ctr["image"] = to_image
    ctr["env"].append({"name": "WATCH_NAMESPACES", "value": ns})
    return frm, to


class RW(W):
    """The installed demonstration plus the release as Helm installed it, and the record's release_upgrade section."""

    def __init__(self, world):  # noqa: F811
        tmp = world.tmp
        files = {}
        for name, body in (("values-1.yaml", "a: 1\n"), ("values-2.yaml", "a: 2\n"), (HELM, "helm"), (META, "meta")):
            (tmp / name).write_text(body)
            files[name] = (str(tmp / name), hashlib.sha256(body.encode()).hexdigest())
        cfg = {"from": {"revision": 1, "commit": "f" * 40, "values": files["values-1.yaml"][0],
                        "values_sha256": files["values-1.yaml"][1]},
               "to": {"values": files["values-2.yaml"][0], "values_sha256": files["values-2.yaml"][1]},
               "helm": {"binary": files[HELM][0], "sha256": files[HELM][1], "version": "v3.19.0", "timeout": "5m",
                        "kube_version": "1.35.9"},
               "metadata_reader": {"binary": files[META][0], "sha256": files[META][1]}}

        def build():
            return world(release_upgrade=cfg)
        build.api, build.tmp = world.api, world.tmp
        super().__init__(build)
        p = demo_params()
        self.release, self.control_ns, self.ns = p["release"], p["control_namespace"], p["namespace"]
        import prepare
        self.to_image = prepare._section(X.load_record(self.path, ("prepare",)))["release"]["statistical_image"]
        self.from_docs, self.to_docs = renderings(self.release, self.control_ns, self.ns, self.to_image)
        self.helm = FakeHelm(self, self.from_docs, self.to_docs)
        self.api.put({"apiVersion": "apps/v1", "kind": "Deployment",
                      "metadata": copy.deepcopy(self.from_docs[-1]["metadata"]),
                      "spec": served(self.from_docs[-1]["spec"])})
        self.api.put({"apiVersion": "v1", "kind": "Pod", "metadata": {
            "name": f"{self.release}-forecaster-0", "namespace": self.control_ns,
            "labels": {"app.kubernetes.io/instance": self.release}},
            "spec": {"nodeName": "worker-s", "containers": [{"name": "forecaster"}]},
            "status": {"phase": "Running", "containerStatuses": [{"name": "forecaster", "restartCount": 0}]}})
        self.checkout, self.role_perm = [], {}

    def set_forecaster(self, which):
        docs = self.from_docs if which == "from" else self.to_docs
        self.api.obj("Deployment", docs[-1]["metadata"]["name"], self.control_ns)["spec"] = served(docs[-1]["spec"])

    def ctx(self):
        if not hasattr(self, "helm"):                      # W's own constructor: the image-roll context
            return super().ctx()
        record = X.load_record(self.path, ("prepare",))

        def permission_check():
            self.perm_calls += 1
            return {role: self.role_perm.get(role, self.perm(self.perm_calls)) for role in R.ROLES}

        def charts(commit):
            which = "from" if commit == "f" * 40 else "to"
            return {"commit": commit, "tree": which[0] * 40, "archive_sha256": which[0] * 64,
                    "dir": f"/evidence/{which}-tree/charts/predictive-autoscaler", "files": 12}
        return R.Ctx(record, record.kube(self.api), runner=self.helm, charts=charts,
                     permission_check=permission_check, checkout=lambda: list(self.checkout))

    def plan(self, fn=R.plan_upgrade, name="plan-upgrade"):
        c = self.ctx()
        plan = fn(c)
        return plan, X.save_plan(c.record, R.HELPER, plan, name)

    def upgrade(self):
        _plan, saved = self.plan()
        return R.apply_upgrade(self.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])

    def rollback(self):
        _plan, saved = self.plan(R.plan_rollback, "plan-rollback")
        return R.apply_rollback(self.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])

    def events(self):
        return [e["event"] for e in X.receipts(self.ctx().record, R.HELPER)]

    def everything_written(self):
        """Every byte under the test's directory (plans, receipts, records, the lock)."""
        out = []
        for root, _dirs, names in os.walk(self.tmp):
            for n in names:
                with open(os.path.join(root, n), "rb") as fh:
                    out.append(fh.read())
        return b"\n".join(out)


@pytest.fixture
def w(world):  # noqa: F811
    return RW(world)


# --- the plan ---------------------------------------------------------------------------------------------------------

def test_the_upgrade_plan_binds_what_was_reviewed_and_is_deterministic(w):
    plan, saved = w.plan()
    assert plan["problems"] == [] and plan["kind"] == "upgrade"
    again, saved2 = w.plan()
    assert saved2["plan_digest"] == saved["plan_digest"]
    a = plan["actions"][0]
    op = plan["operation"]
    assert a["argv"] == [w.helm_path(), "upgrade", w.release, "/evidence/to-tree/charts/predictive-autoscaler",
                         "--namespace", w.control_ns, "--kubeconfig", "/home/ops/.kube/eval-prod.yaml",
                         "--kube-context", "prodcluster", "-f", str(w.tmp / "values-2.yaml"), "--wait", "--timeout",
                         "5m", "--labels", f"c28.devkuban.com/release-op={op}"]
    assert not [x for x in a["argv"] if x.split("=")[0] in R.FORBIDDEN] and a["deadline_s"] == 390
    assert R.argv_problems(w.ctx(), a["argv"], "upgrade", R.UPGRADE_OPTIONS) == []
    assert (a["from_revision"], a["expected_revision"], a["rollback_to"]) == (1, 2, 1)
    assert plan["history"] == [{"revision": 1, "status": "deployed", "chart": "predictive-autoscaler-0.9.0",
                                "app_version": "0.9.0", "description_class": "Install complete",
                                "description_sha256": hashlib.sha256(b"Install complete").hexdigest()}]
    assert plan["storage"] == {"revision": 1, "name": f"sh.helm.release.v1.{w.release}.v1", "uid": "storage-uid-1"}
    assert plan["tls"] == {"name": f"{w.release}-predictive-autoscaler-forecaster-tls", "uid": "tls-uid-1",
                           "resourceVersion": "41"}
    d = plan["difference"]
    assert (d["added"], d["removed"], d["changed"]) == ([], [], [d["forecaster"]])
    assert (d["from_image"], d["to_image"]) == (OLD_IMAGE, w.to_image)
    assert d["added_env_names"] == ["WATCH_NAMESPACES"]
    tls = f"Secret/{w.control_ns}/{w.release}-predictive-autoscaler-forecaster-tls"
    assert d["tls_exception"] == tls and plan["renders"]["from"]["objects"] == 4
    assert plan["renders"]["from"]["secrets"] == plan["renders"]["to"]["secrets"] == [
        {"key": tls, "type": "kubernetes.io/tls", "data_keys": ["tls.key"]}]
    assert plan["live_dependence"] == {"from": {"lookup": {}, "capabilities": {}, "lookup_sha256": {}},
                                       "to": {"lookup": {}, "capabilities": {}, "lookup_sha256": {}}}
    assert plan["charts"]["from"]["tree"] == "f" * 40 and plan["charts"]["to"]["tree"] == "t" * 40
    assert [p["name"] for p in plan["pods"]] == [f"{w.release}-forecaster-0"]
    assert not w.helm.mutating() and not w.lock()                    # planning ran no mutating command
    assert MARKER.encode() not in w.everything_written()


RW.helm_path = lambda self: str(self.tmp / HELM)


def test_the_operation_identifier_follows_what_it_binds(w):
    c = w.ctx()
    frm, to = c.charts("f" * 40), c.charts(c.record.commit)
    op = R.operation_id(c, 1, frm, to)
    assert len(op) == 20 and op == R.operation_id(w.ctx(), 1, frm, to)
    assert op != R.operation_id(c, 2, frm, to) and op != R.operation_id(c, 1, dict(frm, tree="0" * 40), to)
    assert op != R.operation_id(c, 1, frm, dict(to, tree="0" * 40))


def _to_dep(w):
    return w.to_docs[-1]


@pytest.mark.parametrize("change,text", [
    (lambda w: w.to_docs.append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                                 "metadata": {"name": "extra", "namespace": w.control_ns}}), "adds RoleBinding"),
    (lambda w: w.to_docs.pop(0), "removes ServiceAccount"),
    (lambda w: w.to_docs[1]["rules"][0]["verbs"].append("patch"), "Role/"),
    (lambda w: w.to_docs[2]["data"].update(a="2"), "ConfigMap/"),
    (lambda w: w.to_docs.insert(0, {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                                    "metadata": {"name": "open", "namespace": w.control_ns}}), "adds NetworkPolicy"),
    (lambda w: _to_dep(w)["spec"]["template"]["spec"]["containers"][0].update(command=["sh"]),
     "differs in more than its image"),
    (lambda w: _to_dep(w)["spec"].update(replicas=2), "differs in more than its image"),
    (lambda w: _to_dep(w)["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": "X", "value": "y"}),
     "does not add exactly the one environment entry"),
    (lambda w: _to_dep(w)["spec"]["template"]["spec"]["containers"][0]["env"].pop(),
     "does not add exactly the one environment entry"),
    (lambda w: _to_dep(w)["spec"]["template"]["spec"]["containers"][0]["env"].__setitem__(
        1, {"name": "WATCH_NAMESPACES", "value": "kube-system"}), "does not add exactly the one environment entry"),
    (lambda w: _to_dep(w)["spec"]["template"]["spec"]["containers"][0].update(
        image="harbor.example/autoscaler/statistical@sha256:" + "0" * 64), "not the record's statistical image"),
    (lambda w: _to_dep(w)["spec"]["template"]["spec"]["containers"][0].update(image=OLD_IMAGE),
     "not the record's statistical image"),
    (lambda w: _to_dep(w)["metadata"]["labels"].update({"helm.sh/chart": "predictive-autoscaler-1.0.0"}),
     "differs in more than its image"),
])
def test_any_difference_but_the_two_allowed_ones_refuses_the_plan(w, change, text):
    change(w)
    plan, _saved = w.plan()
    assert any(text in p for p in plan["problems"]), plan["problems"]


@pytest.mark.parametrize("damage,text", [
    (lambda w: w.api.obj("Deployment", w.from_docs[-1]["metadata"]["name"], w.control_ns)["spec"]["template"]["spec"][
        "containers"][0].update(command=["sleep", "1"]), "live only"),
    (lambda w: w.api.obj("Deployment", w.from_docs[-1]["metadata"]["name"], w.control_ns)["spec"]["template"]["spec"]
     .update(nodeSelector={"x": "y"}), "live only"),
    (lambda w: w.api.obj("Deployment", w.from_docs[-1]["metadata"]["name"], w.control_ns)["spec"]["template"]["spec"][
        "containers"][0].update(image=w.to_image), "the live forecaster differs"),
    (lambda w: w.api.obj("Deployment", w.from_docs[-1]["metadata"]["name"], w.control_ns)["spec"]["template"][
        "metadata"].update(annotations={"a": "b"}), "live only"),
    (lambda w: w.helm.revisions.append(w.helm.rev(2, "deployed", "Upgrade complete")), "the newest revision is 2"),
    (lambda w: w.helm.revisions[0].update(status="failed"), "not the one deployed revision"),
    (lambda w: w.helm.revisions[0].update(revision=3), "the newest revision is 3"),
    (lambda w: setattr(w.helm, "version", "v3.18.0"), "helm is v3.18.0"),
    (lambda w: w.helm.revisions[0]["labels"].update(owner="someone"), "was not found as Helm's object"),
    (lambda w: w.helm.revisions[0]["labels"].update(name="another-release"), "was not found as Helm's object"),
    (lambda w: w.helm.tls.clear(), None),
    (lambda w: (w.tmp / "values-2.yaml").write_text("a: 3\n"), "does not have the recorded sha256"),
    (lambda w: w.checkout.append("the executing checkout is not clean"), "not clean"),
    (lambda w: setattr(w, "perm", lambda n: {"verdict": "fail", "write_grants": ["x"]}),
     "replica-write permission is not proven absent"),
])
def test_every_precondition_refuses_the_plan(w, damage, text):
    damage(w)
    if text is None:                                       # a reader's answer without identity is not an answer
        with pytest.raises(R.ReadError, match="not the expected document"):
            w.plan()
        return
    plan, saved = w.plan()
    assert any(text in p for p in plan["problems"]), plan["problems"]
    with pytest.raises(X.StagingError, match="problems"):
        R.apply_upgrade(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert not w.helm.mutating() and not w.lock() and "intent-1" not in w.events()


@pytest.mark.parametrize("read", ["history", "template", "version", "meta"])
def test_a_failed_read_refuses_to_plan_without_echoing_anything(w, read):
    w.helm.fail_reads.add(read)
    with pytest.raises(R.ReadError) as e:
        w.plan()
    assert MARKER not in str(e.value) and "bytes" in str(e.value)


@pytest.mark.parametrize("read", ["history", "template", "version"])
@pytest.mark.parametrize("wrong", [{"exit_status": 1}, {"exit_kind": "signaled", "exit_status": 15},
                                   {"group_empty": False}, {"group_empty": None}, {"survivors": True},
                                   {"deadline_exceeded": True}, {"output_complete": False},
                                   {"runner_error": "KeyboardInterrupt"}, {"started": False}])
def test_an_answer_from_a_command_that_did_not_end_cleanly_is_not_used(w, read, wrong):
    w.helm.dirty_reads[read] = wrong                        # the answer is complete and well-formed all the same
    with pytest.raises(R.ReadError, match="gave no usable answer"):
        w.plan()


def test_an_answer_larger_than_its_bound_is_refused_not_truncated(w):
    w.helm.version = "v3.19.0" + " " * 5000                  # helm version's answer is bounded at 256 bytes
    with pytest.raises(R.ReadError, match="over the bound True"):
        w.plan()


# --- apply: the outcome table -----------------------------------------------------------------------------------------

def test_an_applied_upgrade_is_bound_by_its_own_revision_and_releases_the_lock(w):
    seen = {}
    w.helm.at_launch = lambda: seen.update(events=w.events(), lock=w.lock())
    r = w.upgrade()
    assert seen == {"events": ["intent-1", "helm-launch"], "lock": True}          # durable before helm could act
    assert w.events() == ["intent-1", "helm-launch", "helm-storage", "step-1"] and not w.lock()
    d = r["data"]
    assert d["result"] == "applied" and d["post"] == {"revision": 2, "uid": "storage-uid-2",
                                                       "storage": f"sh.helm.release.v1.{w.release}.v2"}
    assert d["op"] == "helm-upgrade" and d["context"]["release_op"] and d["pre"] == {"revision": 1,
                                                                                      "storage_uid": "storage-uid-1"}
    assert len(w.helm.mutating()) == 1 and MARKER.encode() not in w.everything_written()
    assert X.open_intents(X.receipts(w.ctx().record, R.HELPER)) == []
    plan, _ = w.plan()                                                              # the installed revision moved on
    assert any("the newest revision is 2" in p for p in plan["problems"])


@pytest.mark.parametrize("mode,classification", [
    ("fail", "failed"), ("pending", "unresolved"), ("lost", "unresolved"), ("hang", "unresolved"),
    ("foreign", "foreign"), ("leftover", "unknown"), ("storage-swap", "foreign"), ("storage-gone", "unknown"),
])
def test_everything_but_applied_keeps_the_lock_and_is_never_retried(w, mode, classification):
    w.helm.mode = mode
    with pytest.raises(X.StagingError, match=f"is {classification}"):
        w.upgrade()
    assert w.lock() and len(w.helm.mutating()) == 1
    assert [e for e in w.events() if e.startswith(("intent-", "step-", "helm-o"))] == ["intent-1", "helm-outcome"]
    out = X.last(X.receipts(w.ctx().record, R.HELPER), "helm-outcome")["data"]
    assert out["classification"] == classification and out["step"] == "1"
    assert MARKER.encode() not in w.everything_written()             # not even the failure's description
    with pytest.raises(X.StagingError, match="exists"):              # nothing can start under the kept lock
        w.upgrade()
    assert len(w.helm.mutating()) == 1


def test_a_read_that_fails_after_the_launch_is_unknown_not_a_guess(w):
    w.helm.at_launch = lambda: w.helm.fail_reads.add("history")
    with pytest.raises(X.StagingError, match="is unknown"):
        w.upgrade()
    assert w.lock() and "step-1" not in w.events()


def test_a_command_that_was_never_released_did_not_execute_and_may_be_planned_again(w):
    w.helm.mode = "unrecorded"
    with pytest.raises(X.StagingError, match="never released .OSError.: nothing was executed"):
        w.upgrade()
    assert w.events() == ["intent-1", "step-1"] and not w.lock()
    step = X.last(X.receipts(w.ctx().record, R.HELPER), "step-1")["data"]
    assert step["result"] == "not-applied" and len(w.helm.revisions) == 1
    w.helm.mode = "ok"
    assert w.upgrade()["data"]["result"] == "applied" and w.events()[-1] == "step-2"


def test_the_same_operation_is_never_submitted_twice(w):
    w.helm.mode = "lost"
    with pytest.raises(X.StagingError, match="is unresolved"):
        w.upgrade()
    os.unlink(PF.lock_file(w.ctx().record))                     # even if somebody removed the lock by hand
    X.receipt(w.ctx().record, R.HELPER, "reconciled-1", {"observed": "unresolved-by-hand"})
    plan, saved = w.plan()
    assert any("already submitted" in p for p in plan["problems"])
    with pytest.raises(X.StagingError, match="problems"):
        R.apply_upgrade(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert len(w.helm.mutating()) == 1


def test_a_plan_that_no_longer_matches_the_live_state_is_refused_before_anything_is_journalled(w):
    _plan, saved = w.plan()
    w.api.obj("Pod", f"{w.release}-forecaster-0", w.control_ns)["status"]["containerStatuses"][0]["restartCount"] = 1
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        R.apply_upgrade(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert w.events() == [] and not w.lock() and not w.helm.mutating()


# --- rollback after an applied upgrade --------------------------------------------------------------------------------

def test_a_rollback_after_an_applied_upgrade_is_a_new_revision_bound_by_our_own_clean_run(w):
    w.upgrade()
    plan, _saved = w.plan(R.plan_rollback, "plan-rollback")
    assert plan["problems"] == [] and (plan["kind"], plan["form"], plan["lock_sha256"]) == ("rollback", "applied", "")
    a = plan["actions"][0]
    assert a["argv"] == [w.helm_path(), "rollback", w.release, "1", "--namespace", w.control_ns, "--kubeconfig",
                         "/home/ops/.kube/eval-prod.yaml", "--kube-context", "prodcluster", "--wait", "--timeout", "5m"]
    assert (a["from_revision"], a["expected_revision"], a["destination"]) == (2, 3, 1)
    assert any("restores the unfixed forecaster" in n for n in plan["notes"])
    r = w.rollback()
    assert r["data"]["result"] == "applied" and r["data"]["post"]["revision"] == 3 and not w.lock()
    assert w.events()[-4:] == ["intent-2", "helm-launch", "helm-storage", "step-2"]
    assert [x["description"] for x in w.helm.revisions] == ["Install complete", "Upgrade complete", "Rollback to 1"]
    assert len(w.helm.mutating()) == 2 and MARKER.encode() not in w.everything_written()
    up, _ = w.plan()                                       # revision 3 is not the record's installed revision
    assert any("the newest revision is 3" in p for p in up["problems"])


@pytest.mark.parametrize("mode,classification", [("fail", "failed"), ("lost", "unresolved"), ("hang", "unresolved"),
                                                 ("foreign", "foreign"), ("pending", "unresolved")])
def test_a_rollback_that_is_not_proven_ours_keeps_the_lock(w, mode, classification):
    w.upgrade()
    w.helm.mode = mode
    with pytest.raises(X.StagingError, match=f"is {classification}"):
        w.rollback()
    assert w.lock() and len(w.helm.mutating()) == 2 and w.events()[-1] == "helm-outcome"
    out = X.last(X.receipts(w.ctx().record, R.HELPER), "helm-outcome")["data"]
    assert (out["op"], out["classification"]) == ("helm-rollback", classification)
    if mode == "pending":                                  # the revision exists, deployed, as a rollback: not enough
        assert "provenance is ambiguous" in out["why"] and w.helm.revisions[-1]["status"] == "deployed"


def test_there_is_nothing_to_roll_back_before_an_upgrade_of_this_record(w):
    plan, saved = w.plan(R.plan_rollback, "plan-rollback")
    assert any("the newest revision is 1, not the upgrade's revision 2" in p for p in plan["problems"])
    with pytest.raises(X.StagingError, match="problems"):
        R.apply_rollback(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert not w.helm.mutating() and w.events() == []


def test_a_rollback_plan_refuses_a_revision_or_a_forecaster_that_is_not_this_records_upgrade(w):
    w.upgrade()
    w.set_forecaster("from")                               # the live forecaster is not what the upgrade installed
    assert any("the live forecaster differs" in p for p in w.plan(R.plan_rollback, "plan-rollback")[0]["problems"])
    w.set_forecaster("to")
    w.helm.revisions[-1]["labels"] = {R.OP_LABEL: "0" * 20}     # revision 2 belongs to another operation
    assert any("does not carry this record's operation identifier" in p
               for p in w.plan(R.plan_rollback, "plan-rollback")[0]["problems"])


# --- recovery ---------------------------------------------------------------------------------------------------------

class Died(BaseException):
    """The executor process ends here (nothing after this line of the helper runs)."""


@pytest.fixture
def rec(w, monkeypatch):
    """Recovery runs in a fresh process: the reconciler builds its own context from the module's defaults."""
    w.group = "empty"
    monkeypatch.setattr(R, "RUNNER", w.helm)
    monkeypatch.setattr(R, "PERMISSIONS", lambda: {role: PASS for role in R.ROLES})
    monkeypatch.setattr(R.procgroup, "group_state", lambda ident: w.group)
    return w


def die_after(w, mode="ok"):
    """Helm does what `mode` says and the executor dies before reading the outcome."""
    w.helm.mode, real = mode, w.helm.upgrade

    def upgrade(argv):
        real(argv)
        raise Died()
    w.helm.upgrade = upgrade
    with pytest.raises(Died):
        w.upgrade()
    w.helm.upgrade = real
    assert w.lock() and w.events()[-1] == "helm-launch"


def recover_plan(w):
    c = w.ctx()
    return X.recover_plan(c.record, c.kube)


def recover(w):
    c = w.ctx()
    PF.crash_owner(c.record)
    saved = X.save_plan(c.record, "recover", X.recover_plan(c.record, c.kube), "recover-plan")
    return X.recover(c.record, c.kube, X.load_plan(saved["plan"]), saved["plan_digest"])


def test_an_upgrade_whose_executor_died_is_done_only_when_its_own_revision_is_deployed(rec):
    w = rec
    die_after(w)
    [a] = recover_plan(w)["actions"]
    assert (a["helper"], a["op"], a["observed"]) == ("release", "helm-upgrade", "unresolved")
    assert "executor is not proven gone" in a["why"]       # this process still holds the lock
    recover(w)
    assert not w.lock() and w.events()[-2:] == ["helm-storage", "reconciled-1"]
    e = X.last(X.receipts(w.ctx().record, R.HELPER), "reconciled-1")["data"]
    assert e["observed"] == "done" and e["uid"] == "storage-uid-2"
    assert len(w.helm.mutating()) == 1                     # recovery never ran helm upgrade again


@pytest.mark.parametrize("mode,text", [("fail", "the upgrade is failed"), ("pending", "the upgrade is unresolved"),
                                       ("lost", "the upgrade is unresolved"), ("foreign", "the upgrade is foreign")])
def test_recovery_never_decides_an_upgrade_that_is_not_proven_applied(rec, mode, text):
    w = rec
    die_after(w, mode)
    with pytest.raises(X.StagingError, match="unresolved outcomes"):
        recover(w)
    [a] = recover_plan(w)["actions"]
    assert a["observed"] == "unresolved" and text in a["why"] and w.lock()
    assert "reconciled-1" not in w.events() and len(w.helm.mutating()) == 1


def test_recovery_waits_while_a_process_of_the_helm_group_exists(rec):
    w = rec
    die_after(w)
    for state in ("alive", "unknown"):                     # unknown: another host or boot, an unreadable /proc
        w.group = state
        with pytest.raises(X.StagingError, match="unresolved outcomes"):
            recover(w)
        [a] = recover_plan(w)["actions"]
        assert f"the Helm process group is not proven gone: it is {state}" in a["why"] and w.lock()
    w.group = "empty"
    recover(w)
    assert not w.lock()


def test_an_intent_without_a_launch_receipt_never_executed(rec, monkeypatch):
    w, real = rec, X.receipt

    def receipt(record, helper, event, data, clock=None):
        if event.startswith("step-"):
            raise Died()                                   # the executor dies before it can record the refusal
        return real(record, helper, event, data, clock)
    w.helm.mode = "unrecorded"
    monkeypatch.setattr(X, "receipt", receipt)
    with pytest.raises(Died):
        w.upgrade()
    monkeypatch.setattr(X, "receipt", real)
    assert w.events() == ["intent-1"] and w.lock() and not w.helm.identity_seen
    recover(w)
    e = X.last(X.receipts(w.ctx().record, R.HELPER), "reconciled-1")["data"]
    assert (e["observed"], e["state"]) == ("not-done", "not-applied") and not w.lock()
    w.helm.mode = "ok"
    assert w.upgrade()["data"]["result"] == "applied"      # it was never submitted: it may be planned again


def test_a_storage_secret_that_was_replaced_between_two_observations_is_foreign(rec):
    w = rec
    die_after(w, "fail")
    with pytest.raises(X.StagingError):
        recover(w)                                         # first observation: revision 2's storage UID is recorded
    w.helm.revisions[-1].update(status="deployed", uid="storage-uid-recreated")
    [a] = recover_plan(w)["actions"]
    assert a["observed"] == "unresolved" and "storage Secret was replaced" in a["why"]


def test_an_interrupted_rollback_stays_unresolved_whatever_the_history_shows(rec):
    w = rec
    w.upgrade()
    real = w.helm.rollback

    def rollback(argv):
        real(argv)                                         # the rollback revision is written, deployed
        raise Died()
    w.helm.rollback = rollback
    with pytest.raises(Died):
        w.rollback()
    with pytest.raises(X.StagingError, match="unresolved outcomes"):
        recover(w)
    [a] = recover_plan(w)["actions"]
    assert (a["op"], a["observed"]) == ("helm-rollback", "unresolved") and "ambiguous provenance" in a["why"]
    assert w.lock()


# --- the rollback of an unresolved upgrade, under the recovery guard --------------------------------------------------

@pytest.mark.parametrize("mode", ["fail", "pending"])
def test_a_failed_or_pending_upgrade_is_rolled_back_under_the_retained_lock(rec, mode):
    w = rec
    w.helm.mode = mode
    with pytest.raises(X.StagingError):
        w.upgrade()
    w.helm.mode = "ok"
    plan, saved = w.plan(R.plan_rollback, "plan-rollback")
    assert plan["problems"] == [] and plan["form"] == "unresolved" and plan["lock_sha256"]
    with pytest.raises(X.StagingError, match="refusing to recover"):       # its executor is not proven gone
        R.apply_rollback(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert len(w.helm.mutating()) == 1
    PF.crash_owner(w.ctx().record)
    seen = {}
    w.helm.at_launch = lambda: seen.update(lock=w.lock(), guard=os.path.exists(PF.lock_file(w.ctx().record)
                                                                               + ".recovery"))
    r = w.rollback()
    assert seen == {"lock": True, "guard": True}           # the stale lock stayed for the whole rollback
    assert r["data"]["result"] == "applied" and r["data"]["post"]["revision"] == 3 and not w.lock()
    assert w.events()[-5:] == ["intent-2", "helm-launch", "helm-storage", "step-2", "reconciled-1"]
    e = X.last(X.receipts(w.ctx().record, R.HELPER), "reconciled-1")["data"]
    assert e["observed"] == "rolled-back" and e["by"] == "step-2"
    assert X.last(X.receipts(w.ctx().record, "recover"), "lock-recovered")["data"]["by"] == "release rollback"
    assert C.unresolved_intents(w.ctx().record.doc) == {}


def test_a_guarded_rollback_that_is_not_proven_keeps_the_lock_and_needs_a_person(rec):
    w = rec
    w.helm.mode = "fail"
    with pytest.raises(X.StagingError):
        w.upgrade()
    PF.crash_owner(w.ctx().record)
    w.helm.mode = "pending"                                # the rollback's helm is killed after writing its revision
    with pytest.raises(X.StagingError, match="is unresolved"):
        w.rollback()
    assert w.lock() and not os.path.exists(PF.lock_file(w.ctx().record) + ".recovery")
    assert C.unresolved_intents(w.ctx().record.doc) == {"release": ["1", "2"]}
    plan, _ = w.plan(R.plan_rollback, "plan-rollback")     # no second attempt is planned by the helper
    assert any("are not exactly this operation's upgrade" in p for p in plan["problems"])


def test_a_guarded_rollback_needs_the_upgrades_helm_group_gone_and_a_revision_to_roll_back(rec):
    w = rec
    w.helm.mode = "fail"
    with pytest.raises(X.StagingError):
        w.upgrade()
    w.group = "unknown"
    plan, _ = w.plan(R.plan_rollback, "plan-rollback")
    assert any("Helm process group is not proven gone: it is unknown" in p for p in plan["problems"])


def test_an_applied_upgrade_with_an_open_intent_is_for_recovery_not_for_a_guarded_rollback(rec):
    w = rec
    die_after(w)                                           # deployed; only the executor's answer is missing
    plan, _ = w.plan(R.plan_rollback, "plan-rollback")
    assert plan["form"] == "unresolved"
    assert any("only failed or pending-upgrade is rolled back under the recovery guard" in p for p in plan["problems"])


def test_an_upgrade_without_revision_evidence_cannot_be_rolled_back_by_the_helper(rec):
    w = rec
    w.helm.mode = "lost"
    with pytest.raises(X.StagingError, match="is unresolved"):
        w.upgrade()
    plan, _ = w.plan(R.plan_rollback, "plan-rollback")
    assert any("the newest revision is 1, not the upgrade's revision 2" in p for p in plan["problems"])


# --- the chart tree, with a real git repository -----------------------------------------------------------------------

@pytest.fixture
def repo(tmp_path):
    import subprocess
    root = tmp_path / "repo"
    chart = root / "charts" / "predictive-autoscaler"
    (chart / "templates").mkdir(parents=True)
    (chart / "Chart.yaml").write_text("name: predictive-autoscaler\nversion: 0.9.0\n")
    (chart / "templates" / "forecaster.yaml").write_text("kind: Deployment\n")
    (root / "other.txt").write_text("not part of the chart\n")

    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}     # the suite also runs inside a hook

    def git(*args):
        return subprocess.run(["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@example.invalid",
                               "-c", "commit.gpgsign=false", *args], check=True, capture_output=True, text=True,
                              env=env).stdout.strip()
    git("init", "-q")
    # Never add or commit anywhere but in this temporary repository (inside a hook, an inherited GIT_DIR once made
    # these commands act on the repository being committed to).
    assert os.path.realpath(git("rev-parse", "--show-toplevel")) == os.path.realpath(root)
    assert os.path.realpath(git("rev-parse", "--absolute-git-dir")) == os.path.realpath(root / ".git")
    git("add", "-A")
    git("commit", "-q", "-m", "one")

    class Rec:
        evidence_dir = str(tmp_path / "evidence")
    return root, git, Rec()


def test_the_chart_tree_is_extracted_once_and_verified_against_gits_own_listing(repo):
    root, git, record = repo
    head = git("rev-parse", "HEAD")
    a = R.chart_artifact(record, head, str(root))
    assert a["commit"] == head and a["tree"] == git("rev-parse", f"{head}:{R.CHART}") and a["files"] == 2
    assert a["dir"] == os.path.join(record.evidence_dir, "release", f"chart-{a['tree']}", R.CHART)
    assert sorted(os.listdir(a["dir"])) == ["Chart.yaml", "templates"]
    assert not os.path.exists(os.path.join(os.path.dirname(os.path.dirname(a["dir"])), "other.txt"))
    assert R.chart_artifact(record, head, str(root)) == a                  # the same tree: the same directory
    with open(os.path.join(a["dir"], "templates", "forecaster.yaml"), "a") as fh:
        fh.write("# edited after extraction\n")
    with pytest.raises(X.StagingError, match="does not equal the chart tree"):
        R.chart_artifact(record, head, str(root))


def test_an_extra_file_in_the_extracted_chart_or_a_link_in_the_tree_is_refused(repo):
    root, git, record = repo
    head = git("rev-parse", "HEAD")
    a = R.chart_artifact(record, head, str(root))
    with open(os.path.join(a["dir"], "templates", "extra.yaml"), "w") as fh:
        fh.write("kind: ClusterRoleBinding\n")
    with pytest.raises(X.StagingError, match="does not equal the chart tree"):
        R.chart_artifact(record, head, str(root))
    os.symlink("/etc/passwd", root / "charts" / "predictive-autoscaler" / "templates" / "link.yaml")
    git("add", "-A")
    git("commit", "-q", "-m", "two")
    with pytest.raises(X.StagingError, match="not a regular file"):
        R.chart_artifact(record, git("rev-parse", "HEAD"), str(root))
    with pytest.raises(X.StagingError):
        R.chart_artifact(record, "0" * 40, str(root))                     # no such commit


def test_git_is_asked_about_the_named_repository_even_inside_a_hook(repo, monkeypatch, tmp_path):
    root, git, record = repo
    head = git("rev-parse", "HEAD")
    for k, v in {"GIT_DIR": str(tmp_path / "elsewhere.git"), "GIT_INDEX_FILE": str(tmp_path / "index"),
                 "GIT_WORK_TREE": str(tmp_path)}.items():
        monkeypatch.setenv(k, v)                            # what a pre-commit hook's environment carries
    assert R.checkout_problems(str(root), head) == []
    assert R.chart_artifact(record, head, str(root))["commit"] == head


def test_the_executing_checkout_must_be_the_records_commit_and_clean(repo):
    root, git, _record = repo
    head = git("rev-parse", "HEAD")
    assert R.checkout_problems(str(root), head) == []
    assert R.checkout_problems(str(root), "1" * 40) == ["the executing checkout is not at the record's source.commit"]
    (root / "untracked.txt").write_text("x")
    assert R.checkout_problems(str(root), head) == ["the executing checkout is not clean"]


# --- the command line and the report ----------------------------------------------------------------------------------

def test_the_command_line_plans_applies_and_reports(rec, monkeypatch, capsys):
    w = rec
    monkeypatch.setattr(R, "CHARTS", w.ctx().charts)
    monkeypatch.setattr(R, "checkout_problems", lambda repo, commit: [])
    assert R.main(["--record", w.path, "plan-upgrade"], api=w.api) == 0
    saved = json.loads(capsys.readouterr().out)
    assert saved["problems"] == [] and os.path.basename(saved["plan"]).startswith("plan-upgrade-")
    assert R.main(["--record", w.path, "apply-upgrade", "--plan", saved["plan"], "--plan-digest",
                   saved["plan_digest"]], api=w.api) == 0
    assert json.loads(capsys.readouterr().out)["data"]["result"] == "applied"
    assert R.main(["--record", w.path, "status"], api=w.api) == 0
    st = json.loads(capsys.readouterr().out)
    assert st["lock_held"] is False and st["open_intents"] == [] and [h["revision"] for h in st["history"]] == [1, 2]
    assert R.main(["--record", w.path, "plan-rollback"], api=w.api) == 0
    saved = json.loads(capsys.readouterr().out)
    assert R.main(["--record", w.path, "apply-rollback", "--plan", saved["plan"], "--plan-digest",
                   saved["plan_digest"]], api=w.api) == 0
    capsys.readouterr()
    assert R.main(["--record", w.path, "recover-plan"], api=w.api) == 0
    assert json.loads(capsys.readouterr().out)["problems"] == []


def test_the_report_shows_an_unresolved_outcome_without_its_text(rec):
    w = rec
    w.helm.mode = "fail"
    with pytest.raises(X.StagingError):
        w.upgrade()
    st = R.status(w.ctx())
    assert st["lock_held"] and st["open_intents"] == [{"step": "1", "op": "helm-upgrade"}]
    assert st["last_outcome"]["classification"] == "failed" and MARKER not in json.dumps(st)
    assert st["history"][-1]["description_class"] == "other"
    w.helm.fail_reads.add("history")
    assert "history_error" in R.status(w.ctx())


def test_the_offline_renderings_get_the_records_capabilities(w, tmp_path):
    doc = json.loads(open(w.path).read())
    doc["release_upgrade"]["helm"]["api_versions"] = ["operator.victoriametrics.com/v1beta1"]
    with open(w.path, "w") as fh:
        json.dump(doc, fh)
    plan, _ = w.plan()
    templates = [c for c in w.helm.calls if c[1] == "template"]
    assert len(templates) == 2 and all(c[-2:] == ["--api-versions", "operator.victoriametrics.com/v1beta1"] and
                                       c[c.index("--kube-version") + 1] == "1.35.9" for c in templates)
    assert any("offline" in n for n in plan["notes"])


def test_descriptions_are_kept_as_a_class_and_a_hash():
    for text, cls in (("Install complete", "Install complete"), ("Upgrade complete", "Upgrade complete"),
                      ("Preparing upgrade", "Preparing upgrade"), ("Rollback to 12", "Rollback to 12"),
                      ("Rollback to 1 failed: " + MARKER, "other"), ("upgrade failed: " + MARKER, "other"),
                      (None, "other")):
        d = R.describe(text)
        assert d["description_class"] == cls and MARKER not in json.dumps(d)
        assert d["description_sha256"] == hashlib.sha256(("" if text is None else text).encode()).hexdigest()


def test_no_forbidden_flag_can_ride_in_a_mutating_call():
    assert R._forbidden(["helm", "upgrade", "r", "c", "--wait", "--timeout", "5m", "--labels", "a=b"]) == []
    for flag in ("--atomic", "--force", "--install", "--cleanup-on-fail", "--take-ownership", "--reuse-values",
                 "--debug", "--dry-run=server", "-o", "--set", "--no-hooks", "--description"):
        assert R._forbidden(["helm", "upgrade", "r", "c", flag]), flag


# --- the first review's findings (PR #109) ----------------------------------------------------------------------------

def _secret(w, name, typ="Opaque", **data):
    return {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": w.control_ns}, "type": typ,
            "data": data}


def _tls(docs):
    return next(d for d in docs if d["kind"] == "Secret" and d["metadata"]["name"].endswith("-tls"))


def _ctr(docs):
    return docs[-1]["spec"]["template"]["spec"]["containers"][0]


def _other_deployment(w, token):
    return {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "operator", "namespace": w.control_ns},
            "spec": {"template": {"spec": {"containers": [{"name": "operator", "image": "x@sha256:" + "1" * 64,
                                                           "env": [{"name": "AUTH_TOKEN", "value": token}]}]}}}}


def _both(w, fn):
    fn(w.from_docs)
    fn(w.to_docs)


@pytest.mark.parametrize("change,text", [
    (lambda w: w.to_docs.append(_secret(w, "extra", token=MARKER)), "the new rendering adds Secret/"),
    (lambda w: w.to_docs.remove(_tls(w.to_docs)), "the new rendering removes Secret/"),
    (lambda w: _tls(w.to_docs)["metadata"].update(name="renamed-tls"), "the new rendering adds Secret/"),
    (lambda w: (w.from_docs.append(_secret(w, "api", token="one-" + MARKER)),
                w.to_docs.append(_secret(w, "api", token="two-" + MARKER))), "/api differs between the renderings"),
    (lambda w: _tls(w.to_docs)["data"].update({"ca.crt": MARKER}), "-forecaster-tls differs between the renderings"),
    (lambda w: _tls(w.to_docs).update(type="Opaque"), "-forecaster-tls differs between the renderings"),
    (lambda w: (_ctr(w.from_docs)["env"].insert(0, {"name": "AUTH_TOKEN", "value": "one-" + MARKER}),
                _ctr(w.to_docs)["env"].insert(0, {"name": "AUTH_TOKEN", "value": "two-" + MARKER})),
     "does not add exactly the one environment entry"),
    (lambda w: (w.from_docs.insert(0, _other_deployment(w, "one-" + MARKER)),
                w.to_docs.insert(0, _other_deployment(w, "two-" + MARKER))), "/operator differs between the renderings"),
])
def test_secrets_and_literal_credentials_take_part_in_the_difference_and_are_never_quoted(w, change, text):
    change(w)
    plan, _saved = w.plan()
    assert any(text in p for p in plan["problems"]), plan["problems"]
    assert MARKER not in json.dumps(plan) and MARKER.encode() not in w.everything_written()


def test_the_generated_tls_secret_is_the_one_object_compared_by_shape_only(w):
    _tls(w.to_docs)["data"]["tls.key"] = "another-" + MARKER              # offline, each rendering makes a new key
    assert w.plan()[0]["problems"] == []
    _both(w, lambda docs: docs.append(_secret(w, "api", "kubernetes.io/tls", **{"tls.key": "k"})))
    next(d for d in w.to_docs if d["metadata"]["name"] == "api")["data"]["tls.key"] = "other"
    assert any("/api differs" in p for p in w.plan()[0]["problems"])      # another TLS Secret is not the exception


def test_a_live_literal_that_differs_from_the_rendering_is_found_and_not_quoted(w):
    _both(w, lambda docs: _ctr(docs)["env"].insert(0, {"name": "AUTH_TOKEN", "value": "rendered-" + MARKER}))
    w.set_forecaster("from")
    assert w.plan()[0]["problems"] == []
    live = w.api.obj("Deployment", w.from_docs[-1]["metadata"]["name"], w.control_ns)
    live["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = "live-" + MARKER
    plan, _ = w.plan()
    assert any("the live forecaster differs" in p and "the values differ" in p for p in plan["problems"])
    assert MARKER not in json.dumps(plan)


@pytest.mark.parametrize("value", [f"3 rendered entries, 4 live {MARKER}", f"x: rendered, absent live {MARKER}",
                                   f"live only ({MARKER})", f"{MARKER}: rendered, absent live"])
def test_a_live_value_that_imitates_a_finding_is_still_never_quoted(w, value):
    _both(w, lambda docs: _ctr(docs)["env"].insert(0, {"name": "AUTH_TOKEN", "value": "rendered"}))
    w.set_forecaster("from")
    live = w.api.obj("Deployment", w.from_docs[-1]["metadata"]["name"], w.control_ns)
    live["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = value
    live["spec"]["template"]["metadata"]["labels"][f"odd key: {MARKER}"] = "x"     # a key only the live object has
    plan, saved = w.plan()
    found = [p for p in plan["problems"] if "the live forecaster differs" in p]
    assert sorted(p.rsplit(": ", 1)[1] for p in found) == ["live only", "the values differ"]
    assert any("spec.template.metadata.labels.<a key the rendering does not have>: live only" in p for p in found)
    assert MARKER not in json.dumps(plan) and MARKER not in open(saved["plan"]).read()
    assert MARKER.encode() not in w.everything_written()


def test_a_path_shows_rendered_keys_only_in_the_characters_of_names(w):
    _both(w, lambda docs: docs[-1]["spec"]["template"]["metadata"]["labels"].update({"a rendered: key": "v"}))
    w.set_forecaster("from")
    live = w.api.obj("Deployment", w.from_docs[-1]["metadata"]["name"], w.control_ns)
    live["spec"]["template"]["metadata"]["labels"]["a rendered: key"] = "other"
    live["spec"]["template"]["spec"].pop("serviceAccountName")
    found = [p for p in w.plan()[0]["problems"] if "the live forecaster differs" in p]
    assert any(p.endswith("spec.template.metadata.labels.a?rendered??key: the values differ") for p in found)
    assert any(p.endswith("spec.template.spec.serviceAccountName: rendered, absent live") for p in found)


@pytest.mark.parametrize("tool", [HELM, META])
def test_an_executable_that_is_not_the_records_is_never_run(rec, tool):
    w = rec
    (w.tmp / tool).write_text("something else")
    for fn, name in ((R.plan_upgrade, "plan-upgrade"), (R.plan_rollback, "plan-rollback")):
        with pytest.raises(R.ToolError, match="it was not run"):
            w.plan(fn, name)
    assert w.helm.calls == []                               # not version, not a rendering, not history, not the reader
    if tool == HELM:
        assert "it was not run" in R.status(w.ctx())["history_error"] and w.helm.calls == []


def test_an_executable_swapped_between_the_plan_and_the_launch_is_refused_before_anything_is_journalled(w):
    def perm(n):
        if n >= 2:                                         # during apply's own re-plan, after its reads
            (w.tmp / HELM).write_text("swapped")
        return PASS
    w.perm = perm
    _plan, saved = w.plan()
    with pytest.raises(R.ToolError, match="it was not run"):
        R.apply_upgrade(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert w.events() == [] and not w.lock() and not w.helm.mutating()


def test_recovery_does_not_run_a_wrong_executable_either(rec):
    w = rec
    die_after(w)
    PF.crash_owner(w.ctx().record)
    calls = len(w.helm.calls)
    (w.tmp / HELM).write_text("something else")
    [a] = recover_plan(w)["actions"]
    assert a["observed"] == "unresolved" and "it was not run" in a["why"] and len(w.helm.calls) == calls


def test_an_installed_revision_whose_storage_is_gone_is_not_continuity(rec):
    w = rec
    die_after(w, "storage-gone")                            # revision 2 deployed and ours; revision 1's storage vanished
    PF.crash_owner(w.ctx().record)
    [a] = recover_plan(w)["actions"]
    assert a["observed"] == "unresolved" and "storage Secret is missing" in a["why"]


@pytest.mark.parametrize("role", R.ROLES)
@pytest.mark.parametrize("answer", [{"verdict": "unproven", "write_grants": []}, {"verdict": "refused"},
                                    {"verdict": "pass", "write_grants": [{"binding": "b"}]}, {}])
def test_each_of_the_three_identities_needs_its_own_permission_proof(w, role, answer):
    w.role_perm[role] = answer
    plan, saved = w.plan()
    assert [p for p in plan["problems"] if "permission is not proven absent" in p] == [
        f"the {role}'s replica-write permission is not proven absent (verdict {answer.get('verdict')})"]
    with pytest.raises(X.StagingError, match="problems"):
        R.apply_upgrade(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert not w.helm.mutating()


def test_the_default_permission_proof_asks_for_the_three_service_accounts(w, monkeypatch):
    import permissions
    seen = []

    def collect(kubectl, sa_ns, sa, ns, targets, role):
        seen.append((sa_ns, sa, ns, role))
        return {"role": role}
    monkeypatch.setattr(permissions, "collect", collect)
    monkeypatch.setattr(permissions, "verdict", lambda collected, sa_ns, sa, targets, uid: {
        "verdict": "pass", "write_grants": [], "identity": sa})
    out = R._permissions(w.ctx())
    assert list(out) == list(R.ROLES) == ["operator", "forecaster", "trainer"]
    assert seen == [(w.control_ns, f"{w.release}-predictive-autoscaler-{r}", w.ns, r) for r in R.ROLES]


@pytest.mark.parametrize("output,text", [
    (f"kind: Secret\nmetadata: {{name: x}}\ndata:\n  tls.key: {MARKER}\n  broken: [unclosed\n".encode(),
     r"not valid YAML \(line \d+, column \d+\)"),
    (b"kind: ConfigMap\nmetadata: {name: x}\ndata: {a: \xff\xfe" + MARKER.encode() + b"}\n", "not UTF-8"),
    (f"data: {MARKER}\n".encode(), "without a kind or metadata"),
    (f"kind: Widget\nmetadata: {{name: x}}\nspec: {MARKER}\n".encode(), "needs secret review"),
])
def test_a_rendering_that_cannot_be_used_is_reported_without_any_of_its_text(w, output, text):
    w.helm.raw_template = output
    with pytest.raises(X.StagingError, match=text) as e:
        w.plan()
    assert MARKER not in str(e.value) and MARKER.encode() not in w.everything_written()


def test_the_sanitizers_own_yaml_error_names_a_position_only():
    with pytest.raises(C.StagingError, match=r"line \d+, column \d+") as e:
        R._sanitizer().sanitize_stream(f"kind: Secret\ndata:\n  a: {MARKER}\n  b: [unclosed\n")
    assert MARKER not in str(e.value)


TLS_TEMPLATE = '{{- $e := lookup "v1" "Secret" .Release.Namespace (include "x.tls" .) }}\nkind: Secret\n'
HELPERS = '{{- define "x.tls" -}}x-tls{{- end -}}\n'


@pytest.fixture
def chart(w, tmp_path, monkeypatch):
    """A chart whose TLS template and helpers are the reviewed content (for this test)."""
    root = tmp_path / "chart"
    (root / "templates").mkdir(parents=True)
    (root / "templates" / "forecaster-tls.yaml").write_text(TLS_TEMPLATE)
    (root / "templates" / "_helpers.tpl").write_text(HELPERS)
    (root / "templates" / "plain.yaml").write_text("kind: ConfigMap\n")
    (root / "values.yaml").write_text("lookup: a value, not a template\n")
    sha = lambda text: hashlib.sha256(text.encode()).hexdigest()  # noqa: E731
    monkeypatch.setattr(R, "REVIEWED_LOOKUP", {"templates/forecaster-tls.yaml": {
        "sha256": (sha(TLS_TEMPLATE),), "with": {"templates/_helpers.tpl": (sha(HELPERS),)}}})
    return root


def test_templates_that_read_the_live_cluster_are_listed_and_the_reviewed_tls_template_is_accepted(w, chart):
    c = w.ctx()
    found, problems = R.live_dependence(c, str(chart))
    assert problems == [] and found["lookup"] == {"templates/forecaster-tls.yaml": 1} and found["capabilities"] == {}
    assert found["lookup_sha256"] == {"templates/forecaster-tls.yaml": hashlib.sha256(TLS_TEMPLATE.encode()).hexdigest()}
    (chart / "templates" / "rbac.yaml").write_text(
        '{{ if (lookup "v1" "Namespace" "" "x") }}kind: ClusterRoleBinding{{ end }}\n{{ lookup "v1" "Pod" "" "" }}\n')
    (chart / "templates" / "metrics.yaml").write_text('{{ if .Capabilities.APIVersions.Has "x/v1" }}kind: VMRule{{ end }}\n')
    found, problems = R.live_dependence(c, str(chart))
    assert found["lookup"] == {"templates/forecaster-tls.yaml": 1, "templates/rbac.yaml": 2}
    assert found["capabilities"] == {"templates/metrics.yaml": 1}
    assert [p for p in problems if "lookup in" in p] == [p for p in problems if "lookup in templates/rbac.yaml" in p] != []
    assert any("must bind helm.api_versions" in p for p in problems)
    c.cfg["helm"]["api_versions"] = []                      # bound, even when empty: the capability question is answered
    assert [p for p in R.live_dependence(c, str(chart))[1] if "api_versions" in p] == []


@pytest.mark.parametrize("change", [
    # another lookup under the reviewed FILE NAME: a Namespace lookup guarding a ClusterRoleBinding
    lambda chart: (chart / "templates" / "forecaster-tls.yaml").write_text(
        TLS_TEMPLATE + '{{ if (lookup "v1" "Namespace" "" "x") }}kind: ClusterRoleBinding{{ end }}\n'),
    lambda chart: (chart / "templates" / "forecaster-tls.yaml").write_text(TLS_TEMPLATE.replace(".Release.Namespace",
                                                                                                 '"kube-system"')),
    lambda chart: (chart / "templates" / "_helpers.tpl").write_text('{{- define "x.tls" -}}another-secret{{- end -}}\n'),
    lambda chart: (chart / "templates" / "_helpers.tpl").unlink(),
])
def test_the_lookup_exception_is_the_reviewed_content_not_a_file_name(w, chart, change):
    change(chart)
    _found, problems = R.live_dependence(w.ctx(), str(chart))
    assert any("lookup in templates/forecaster-tls.yaml" in p and "not the reviewed content" in p for p in problems)


def test_the_charts_own_tls_template_and_helpers_are_the_reviewed_content(w):
    """Pins REVIEWED_LOOKUP to the chart of this commit: changing the template or its helpers fails here until the
    change is reviewed and the constant updated."""
    found, problems = R.live_dependence(w.ctx(), os.path.join(R.repo_root(), R.CHART))
    assert problems == [] and list(found["lookup"]) == ["templates/forecaster-tls.yaml"] and found["capabilities"] == {}


def test_the_mutating_calls_shape_is_an_exact_allowlist(w):
    c = w.ctx()
    argv = w.plan()[0]["actions"][0]["argv"]
    assert R.argv_problems(c, argv, "upgrade", R.UPGRADE_OPTIONS) == []
    for bad in (argv + ["--skip-crds"], argv[:-2], argv[:4] + ["--history-max", "3"] + argv[4:],
                [argv[0], "install"] + argv[2:], argv[:2] + ["another-release"] + argv[3:],
                argv[:4] + argv[6:] + argv[4:6], argv + ["extra-positional"]):
        assert R.argv_problems(c, bad, "upgrade", R.UPGRADE_OPTIONS), bad
    rb = [argv[0], "rollback", w.release, "1", *c.conn(), "--wait", "--timeout", "5m"]
    assert R.argv_problems(c, rb, "rollback", R.ROLLBACK_OPTIONS) == []
    assert R.argv_problems(c, rb + ["--force"], "rollback", R.ROLLBACK_OPTIONS)


# --- finishing after an interruption, and the reviewed disposition ----------------------------------------------------

def test_an_interruption_after_the_rollbacks_applied_receipt_is_finished_without_running_helm_again(rec, monkeypatch):
    w, real = rec, X.receipt
    w.helm.mode = "fail"
    with pytest.raises(X.StagingError):
        w.upgrade()
    w.helm.mode = "ok"
    PF.crash_owner(w.ctx().record)

    def receipt(record, helper, event, data, clock=None):
        if event.startswith("reconciled-"):
            raise Died()                                   # after step-2 applied is durable, before the reconciliation
        return real(record, helper, event, data, clock)
    monkeypatch.setattr(X, "receipt", receipt)
    with pytest.raises(Died):
        w.rollback()
    monkeypatch.setattr(X, "receipt", real)
    lock = PF.lock_file(w.ctx().record)
    assert w.lock() and not os.path.exists(lock + ".recovery") and w.events()[-1] == "step-2"
    assert C.unresolved_intents(w.ctx().record.doc) == {"release": ["1"]}
    plan, saved = w.plan(R.plan_rollback, "plan-rollback")
    assert (plan["form"], plan["problems"]) == ("finalize", [])
    assert plan["actions"] == [{"op": "finalize-rollback", "upgrade_step": "1", "rollback_step": "2", "revision": 3}]
    r = R.apply_rollback(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert r["event"] == "reconciled-1" and (r["data"]["observed"], r["data"]["by"]) == ("rolled-back", "step-2")
    assert not w.lock() and len(w.helm.mutating()) == 2    # the upgrade and the one rollback: no second rollback
    assert C.unresolved_intents(w.ctx().record.doc) == {}


def test_finishing_is_refused_when_the_release_is_no_longer_the_applied_rollback(rec, monkeypatch):
    w, real = rec, X.receipt
    w.helm.mode = "pending"
    with pytest.raises(X.StagingError):
        w.upgrade()
    w.helm.mode = "ok"
    PF.crash_owner(w.ctx().record)
    monkeypatch.setattr(X, "receipt", lambda record, helper, event, data, clock=None: (_ for _ in ()).throw(Died())
                        if event.startswith("reconciled-") else real(record, helper, event, data, clock))
    with pytest.raises(Died):
        w.rollback()
    monkeypatch.setattr(X, "receipt", real)
    w.helm.revisions[-1]["uid"] = "storage-uid-recreated"
    plan, saved = w.plan(R.plan_rollback, "plan-rollback")
    assert plan["form"] == "finalize" and any("is not the one its receipt recorded" in p for p in plan["problems"])
    w.helm.revisions[-1]["status"] = "superseded"
    w.helm.revisions.append(w.helm.rev(4, "deployed", "Upgrade complete"))
    plan, saved = w.plan(R.plan_rollback, "plan-rollback")
    assert any("the newest revision is not the applied rollback's revision 3" in p for p in plan["problems"])
    with pytest.raises(X.StagingError, match="problems"):
        R.apply_rollback(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert w.lock()


def interrupted_rollback(w):
    w.upgrade()
    real = w.helm.rollback

    def rollback(argv):
        real(argv)
        raise Died()
    w.helm.rollback = rollback
    with pytest.raises(Died):
        w.rollback()
    w.helm.rollback = real


def do_dispose(w):
    _plan, saved = w.plan(R.plan_dispose, "plan-dispose")
    return R.dispose(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])


def test_an_interrupted_rollback_is_settled_by_a_reviewed_disposition_with_its_evidence(rec):
    w = rec
    interrupted_rollback(w)
    plan, saved = w.plan(R.plan_dispose, "plan-dispose")
    assert plan["problems"] == [] and (plan["kind"], plan["state"]) == ("dispose", "installed")
    assert plan["actions"] == [{"op": "dispose", "step": "2", "intent_op": "helm-rollback", "state": "installed"}]
    assert plan["groups"] == {"2": "empty"} and plan["storage"]["revision"] == 3
    assert plan["evidence"]["differs_from_installed"] == 0 and plan["evidence"]["differs_from_new"] > 0
    with pytest.raises(X.StagingError, match="refusing to recover"):       # the executor is not proven gone
        R.dispose(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    PF.crash_owner(w.ctx().record)
    plan, saved = w.plan(R.plan_dispose, "plan-dispose")                   # the lock file changed: a new plan
    r = R.dispose(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert r["event"] == "reconciled-2" and (r["data"]["observed"], r["data"]["state"]) == ("disposed", "installed")
    assert not w.lock() and w.events()[-2:] == ["disposition", "reconciled-2"] and len(w.helm.mutating()) == 2
    assert X.last(X.receipts(w.ctx().record, "recover"), "lock-recovered")["data"]["by"] == "release dispose"


def test_a_killed_guarded_rollback_leaves_two_intents_and_one_disposition_settles_both(rec):
    w = rec
    w.helm.mode = "fail"
    with pytest.raises(X.StagingError):
        w.upgrade()
    PF.crash_owner(w.ctx().record)
    w.helm.mode = "pending"                                # its helm wrote the rollback revision and was killed
    with pytest.raises(X.StagingError, match="is unresolved"):
        w.rollback()
    plan, _ = w.plan(R.plan_dispose, "plan-dispose")
    assert plan["problems"] == [] and plan["state"] == "installed" and [a["step"] for a in plan["actions"]] == ["1", "2"]
    do_dispose(w)
    assert not w.lock() and C.unresolved_intents(w.ctx().record.doc) == {}


def test_an_upgrade_without_revision_evidence_is_not_disposable_because_its_request_can_still_commit(rec):
    w = rec
    w.helm.mode = "lost"                                   # the revision-create was accepted; its answer was lost
    with pytest.raises(X.StagingError):
        w.upgrade()
    PF.crash_owner(w.ctx().record)
    plan, saved = w.plan(R.plan_dispose, "plan-dispose")
    again, _ = w.plan(R.plan_dispose, "plan-dispose")      # two observations that agree prove nothing here
    assert again["history"] == plan["history"]
    assert [p for p in plan["problems"] if "step 1" in p] == [
        "step 1: Helm executed and revision 2 does not exist. A request the API server accepted can still commit, and "
        "nothing read here can show that it will not: this case is not disposable and stays unresolved"]
    with pytest.raises(X.StagingError, match="problems"):
        R.dispose(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert w.lock() and "disposition" not in w.events()
    # ...and now the queued request completes, with no second client submission: the lock was still held
    w.helm.revisions.append(w.helm.rev(2, "pending-upgrade", "Preparing upgrade"))
    assert w.lock() and len(w.helm.mutating()) == 1
    assert C.unresolved_intents(w.ctx().record.doc) == {"release": ["1"]}


def test_a_rollback_without_revision_evidence_is_not_disposable_either(rec):
    w = rec
    w.upgrade()
    w.helm.mode = "lost"
    with pytest.raises(X.StagingError, match="is unresolved"):
        w.rollback()
    PF.crash_owner(w.ctx().record)
    plan, _ = w.plan(R.plan_dispose, "plan-dispose")       # the live state is exactly "upgraded", and still: no
    assert any("step 2: Helm executed and revision 3 does not exist" in p for p in plan["problems"])


def test_an_applied_upgrade_can_also_be_disposed_as_upgraded(rec):
    w = rec
    die_after(w)
    PF.crash_owner(w.ctx().record)
    plan, _ = w.plan(R.plan_dispose, "plan-dispose")
    assert plan["problems"] == [] and plan["state"] == "upgraded"
    assert do_dispose(w)["data"]["state"] == "upgraded" and not w.lock()


@pytest.mark.parametrize("damage,text", [
    (lambda w: setattr(w, "group", "alive"), "its Helm process group is not proven gone (alive)"),
    (lambda w: w.set_forecaster("to"), "is in neither reviewed state"),            # a rollback revision, new forecaster
    (lambda w: w.api.obj("Deployment", w.from_docs[-1]["metadata"]["name"], w.control_ns)["spec"]["template"]["spec"][
        "containers"][0].update(command=["x"]), "is in neither reviewed state"),
    (lambda w: w.helm.revisions[-1].update(status="failed"), "not the one deployed revision"),
    (lambda w: w.helm.revisions[-1].update(description="Upgrade complete"), "is in neither reviewed state"),
    (lambda w: w.helm.revisions[-1].update(gone=True), "was not found as Helm's object"),
    (lambda w: X.receipt(w.ctx().record, "bounded", "intent-9", {"op": "create", "kind": "Job", "namespace": "demo",
                                                                  "name": "x", "context": {}}),
     "only this helper's Helm intents may be open"),
    (lambda w: w.role_perm.update(trainer={"verdict": "unproven"}), "the trainer's replica-write permission"),
])
def test_a_disposition_is_refused_unless_every_piece_of_evidence_holds(rec, damage, text):
    w = rec
    interrupted_rollback(w)
    PF.crash_owner(w.ctx().record)
    damage(w)
    plan, saved = w.plan(R.plan_dispose, "plan-dispose")
    assert any(text in p for p in plan["problems"]), plan["problems"]
    with pytest.raises(X.StagingError, match="problems"):
        R.dispose(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert w.lock() and "disposition" not in w.events()


def test_a_state_that_moved_during_the_review_voids_the_disposition(rec):
    w = rec
    interrupted_rollback(w)
    PF.crash_owner(w.ctx().record)
    _plan, saved = w.plan(R.plan_dispose, "plan-dispose")
    w.api.obj("Pod", f"{w.release}-forecaster-0", w.control_ns)["status"]["containerStatuses"][0]["restartCount"] = 2
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        R.dispose(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert w.lock() and "disposition" not in w.events()


def test_there_is_nothing_to_dispose_of_without_an_open_intent(rec):
    w = rec
    w.upgrade()
    plan, saved = w.plan(R.plan_dispose, "plan-dispose")
    assert any("there is nothing to dispose of" in p for p in plan["problems"])
