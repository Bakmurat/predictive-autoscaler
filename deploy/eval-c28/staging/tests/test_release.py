"""release.py against a fake Helm state machine and the in-memory API: no test runs helm (the process runner has its
own tests, test_helmrun.py). The fake models Helm 3.19's history (pending and failure descriptions), the storage
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
import release as R
from part2_fixtures import api  # noqa: F401
from test_imageroll import PASS, W, served
from test_prepare_demo import demo_params, world  # noqa: F401

MARKER = "tok-SECRET-marker-91c2"
HELM, META = "helm-bin", "secretmeta-bin"
OLD_IMAGE = "harbor.example/autoscaler/statistical@sha256:" + "c" * 64


class Result:
    def __init__(self, end="exited", returncode=0, group_empty=True, stdout=None, ident=None):
        self.end, self.returncode, self.group_empty, self.output_complete = end, returncode, group_empty, True
        self.stdout, self.identity = stdout, ident or {}

    def clean_exit(self):
        return self.end == "exited" and self.returncode == 0 and self.group_empty

    def summary(self):
        h = hashlib.sha256(MARKER.encode()).hexdigest()
        return {"identity": dict(self.identity), "end": self.end, "returncode": self.returncode,
                "group_empty": self.group_empty, "output_complete": True,
                "stdout": {"bytes": len(MARKER), "sha256": h}, "stderr": {"bytes": len(MARKER), "sha256": h},
                "seconds": 1.0}


class FakeHelm:
    """`runner(argv, deadline_s=, record_identity=, capture_stdout=)` for helm and for c28-secretmeta. upgrade /
    rollback behave as `self.mode` says:
      ok            the new revision deployed (and the forecaster changed in the cluster)
      fail          the new revision `failed`, its description an error text
      pending       the new revision `pending-upgrade`, then helm is killed at the deadline
      lost          helm exits 1 and nothing is recorded      hang   helm is terminated, nothing is recorded
      foreign       a revision appears that does not carry the operation's label
      leftover      deployed, but a process of helm's group survives
      unrecorded    the identity cannot be made durable (the receipt write fails)
      storage-swap  deployed, but the installed revision's storage Secret was replaced meanwhile"""

    def __init__(self, w, from_docs, to_docs):
        self.w, self.docs, self.mode, self.calls, self.identity_seen = w, {"from": from_docs, "to": to_docs}, "ok", [], []
        self.version, self.fail_reads, self.uids, self.at_launch = "v3.19.0", set(), 0, None
        self.dirty_reads = {}                                   # command -> what was wrong with an answer it DID give
        self.revisions = [self.rev(1, "deployed", "Install complete")]
        self.tls = {"uid": "tls-uid-1", "resourceVersion": "41"}

    def rev(self, n, status, description, labels=None):
        self.uids += 1
        return {"revision": n, "status": status, "chart": "predictive-autoscaler-0.9.0", "app_version": "0.9.0",
                "description": description, "labels": dict(labels or {}), "uid": f"storage-uid-{self.uids}"}

    def mutating(self):
        return [c for c in self.calls if c[0].endswith(HELM) and c[1] in ("upgrade", "rollback")]

    def __call__(self, argv, deadline_s, record_identity=None, capture_stdout=0):
        self.calls.append(list(argv))
        tool, cmd = os.path.basename(argv[0]), argv[1]
        if tool == META:
            return self.meta(argv)
        if cmd in self.fail_reads:
            return Result(returncode=1)
        if cmd in ("version", "history", "template"):
            if cmd == "version":
                out = self.version.encode()
            elif cmd == "history":
                assert argv[-2:] == ["-o", "json"] and capture_stdout
                out = json.dumps([{k: r[k] for k in ("revision", "status", "chart", "app_version", "description")}
                                  | {"updated": "now"} for r in self.revisions]).encode()
            else:
                assert "--kubeconfig" not in argv and "--kube-context" not in argv      # offline
                out = yaml.safe_dump_all(self.docs["from" if "/from-tree/" in argv[3] else "to"]).encode()
            return Result(stdout=out, **self.dirty_reads.get(cmd, {}))
        ident = {"pid": 4242, "pgid": 4242, "pid_start": "777", "host": "test", "boot_id": "b"}
        if self.mode == "unrecorded":
            raise OSError("the launch receipt could not be written")
        record_identity(ident)
        self.identity_seen.append(ident)
        if self.at_launch:
            self.at_launch()
        return {"upgrade": self.upgrade, "rollback": self.rollback}[cmd](argv, ident)

    def meta(self, argv):
        name, keys = argv[argv.index("--name") + 1], [argv[i + 1] for i, a in enumerate(argv) if a == "--label-key"]
        if "meta" in self.fail_reads:
            return Result(returncode=3)
        doc = {"schema": "c28-secret-metadata/v1", "namespace": self.w.control_ns, "name": name, "found": False}
        if name.endswith("-tls"):
            doc.update(found=True, **self.tls)
        for r in self.revisions:
            if name == f"sh.helm.release.v1.{self.w.release}.v{r['revision']}":
                labels = {"name": self.w.release, "owner": "helm", "version": str(r["revision"]),
                          "status": r["status"], **r["labels"]}
                doc.update(found=True, uid=r["uid"], resourceVersion="7",
                           labels={k: v for k, v in labels.items() if k in keys})
        return Result(stdout=json.dumps(doc).encode())

    def label(self, argv):
        if "--labels" not in argv:
            return {}
        k, v = argv[argv.index("--labels") + 1].split("=", 1)
        return {k: v}

    def upgrade(self, argv, ident):
        n, mode = self.revisions[-1]["revision"] + 1, self.mode
        if mode in ("lost", "hang"):
            return Result(end="exited" if mode == "lost" else "terminated", returncode=1 if mode == "lost" else -15,
                          ident=ident)
        if mode == "foreign":
            self.revisions[-1]["status"] = "superseded"
            self.revisions.append(self.rev(n, "deployed", "Upgrade complete"))
            return Result(returncode=1, ident=ident)
        status, text = {"fail": ("failed", f"timed out waiting: {MARKER}"),
                        "pending": ("pending-upgrade", "Preparing upgrade")}.get(mode, ("deployed", "Upgrade complete"))
        if status == "deployed":
            self.revisions[-1]["status"] = "superseded"
            self.w.set_forecaster("to")
        if mode == "storage-swap":
            self.revisions[-2 if len(self.revisions) > 1 else -1]["uid"] = "storage-uid-swapped"
        self.revisions.append(self.rev(n, status, text, self.label(argv)))
        if mode == "storage-swap":
            self.revisions[0]["uid"] = "storage-uid-swapped"
        if mode == "pending":
            return Result(end="killed", returncode=-9, ident=ident)
        return Result(returncode=0 if status == "deployed" else 1, group_empty=mode != "leftover", ident=ident)

    def rollback(self, argv, ident):
        dest, n, mode = int(argv[3]), self.revisions[-1]["revision"] + 1, self.mode
        if mode in ("lost", "hang"):
            return Result(end="exited" if mode == "lost" else "terminated", returncode=1 if mode == "lost" else -15,
                          ident=ident)
        source = next(r for r in self.revisions if r["revision"] == dest)
        for r in self.revisions:
            if r["status"] == "deployed":
                r["status"] = "superseded"
        text = "Upgrade complete" if mode == "foreign" else f"Rollback to {dest}"
        self.revisions.append(self.rev(n, "failed" if mode == "fail" else "deployed", text, source["labels"]))
        if mode != "fail":
            self.w.set_forecaster("from")
        if mode == "pending":                                  # the revision is written, then helm is killed
            return Result(end="killed", returncode=-9, ident=ident)
        return Result(returncode=1 if mode == "fail" else 0, ident=ident)


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
        self.checkout = []

    def set_forecaster(self, which):
        docs = self.from_docs if which == "from" else self.to_docs
        self.api.obj("Deployment", docs[-1]["metadata"]["name"], self.control_ns)["spec"] = served(docs[-1]["spec"])

    def ctx(self):
        if not hasattr(self, "helm"):                      # W's own constructor: the image-roll context
            return super().ctx()
        record = X.load_record(self.path, ("prepare",))

        def permission_check():
            self.perm_calls += 1
            return self.perm(self.perm_calls)

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
    assert d["added_env"] == [{"name": "WATCH_NAMESPACES", "value": w.ns}]
    assert plan["renders"]["from"]["secrets_discarded"] == 1 and plan["renders"]["from"]["objects"] == 4
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
    (lambda w: (w.tmp / HELM).write_text("another helm"), "the helm binary"),
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
@pytest.mark.parametrize("wrong", [{"returncode": 1}, {"end": "terminated", "returncode": -15},
                                   {"group_empty": False}])
def test_an_answer_from_a_command_that_did_not_end_cleanly_is_not_used(w, read, wrong):
    w.helm.dirty_reads[read] = wrong                        # the answer is complete and well-formed all the same
    with pytest.raises(R.ReadError, match="gave no usable answer"):
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
    ("foreign", "foreign"), ("leftover", "unknown"), ("storage-swap", "foreign"),
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
    with pytest.raises(OSError, match="launch receipt"):
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
    w.group = (True, "group empty")
    monkeypatch.setattr(R, "RUNNER", w.helm)
    monkeypatch.setattr(R, "PERMISSIONS", lambda: PASS)
    monkeypatch.setattr(R.helmrun, "group_gone", lambda ident: w.group)
    return w


def die_after(w, mode="ok"):
    """Helm does what `mode` says and the executor dies before reading the outcome."""
    w.helm.mode, real = mode, w.helm.upgrade

    def upgrade(argv, ident):
        real(argv, ident)
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
    w.group = (False, "a process of group 4242 still exists")
    with pytest.raises(X.StagingError, match="unresolved outcomes"):
        recover(w)
    [a] = recover_plan(w)["actions"]
    assert "the Helm process group is not proven gone" in a["why"] and w.lock()
    w.group = (True, "group empty")
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

    def rollback(argv, ident):
        real(argv, ident)                                  # the rollback revision is written, deployed
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
    w.group = (False, "a process of group 4242 still exists")
    plan, _ = w.plan(R.plan_rollback, "plan-rollback")
    assert any("Helm process group is not proven gone" in p for p in plan["problems"])


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
