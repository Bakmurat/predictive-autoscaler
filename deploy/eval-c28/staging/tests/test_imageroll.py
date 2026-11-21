"""imageroll.py on the in-memory API: the spec comparison, the journal-first
opening and its crash recovery, the roll with a fake Deployment controller (surge, terminating overlap, a stalled
rollout), the safety conditions before every patch, the three patch outcomes with both commit orders of the fencing
write, forward completion with the zero-counter evidence, and a reverse as a successor transition."""

import copy
import json
import os
import re
import signal
import urllib.parse

import pytest

import bounded as B
import common_records as C
import execlib as X
import imageroll as I
import part2_fixtures as PF
import prepare
from part2_fixtures import api  # noqa: F401
from test_prepare_demo import DEMO, N, R, demo_params, run_until, world  # noqa: F401

OLD = demo_params()["image"]
NEW = OLD.rsplit(":", 1)[0] + ":" + "b" * 64
PASS = {"verdict": "pass", "write_grants": []}


def served(spec):
    """A rendered Deployment spec as the API server stores it: the enumerated defaults filled in."""
    s = copy.deepcopy(spec)
    s.setdefault("progressDeadlineSeconds", 600)
    s.setdefault("revisionHistoryLimit", 10)
    s.setdefault("strategy", {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": "25%", "maxUnavailable": "25%"}})
    pod = s["template"]["spec"]
    for k, v in (("dnsPolicy", "ClusterFirst"), ("restartPolicy", "Always"), ("schedulerName", "default-scheduler"),
                 ("terminationGracePeriodSeconds", 30), ("securityContext", {})):
        pod.setdefault(k, v)
    if "serviceAccountName" in pod:
        pod["serviceAccount"] = pod["serviceAccountName"]
    for ctr in pod["containers"]:
        for k, v in (("imagePullPolicy", "IfNotPresent"), ("terminationMessagePath", "/dev/termination-log"),
                     ("terminationMessagePolicy", "File")):
            ctr.setdefault(k, v)
        for port in ctr.get("ports") or []:
            port.setdefault("protocol", "TCP")
        for probe in ("readinessProbe", "livenessProbe", "startupProbe"):
            if probe in ctr:
                for k, v in (("timeoutSeconds", 1), ("periodSeconds", 10), ("successThreshold", 1),
                             ("failureThreshold", 3)):
                    ctr[probe].setdefault(k, v)
                if "httpGet" in ctr[probe]:
                    ctr[probe]["httpGet"].setdefault("scheme", "HTTP")
    return s


class Controller:
    """A fake Deployment controller: when a Deployment's template differs from its active ReplicaSet's, it makes a new
    ReplicaSet with new pods (their imageID is the template's image) and moves the Service's endpoints. stalled: names
    it does not act on; linger: how many polls the old pods stay, terminating, beside their replacements."""

    def __init__(self, names):
        self.names, self.stalled, self.linger, self.left, self.image_id = set(names), set(), 0, {}, {}

    def _owned(self, api, kind, ns, uid):
        return [(k, o) for k, o in list(api.objects.items()) if k[0] == kind and k[1] == ns and
                any(r.get("uid") == uid for r in o["metadata"].get("ownerReferences") or [])]

    def __call__(self, api):
        for (kind, ns, name), dep in list(api.objects.items()):
            if kind == "deployments" and name in self.names:
                self.sync(api, ns, name, dep)

    def sync(self, api, ns, name, dep):
        m, tmpl, want = dep["metadata"], dep["spec"]["template"], int(dep["spec"].get("replicas") or 0)
        rss = self._owned(api, "replicasets", ns, m["uid"])
        cur = [o for _k, o in rss if o["spec"]["replicas"] > 0]
        if self.left.get(name, 0) > 0:
            self.left[name] -= 1
            if self.left[name] == 0:
                for k, o in list(api.objects.items()):
                    if k[0] == "pods" and k[1] == ns and o["metadata"].get("deletionTimestamp") and \
                            o["metadata"]["labels"].get("fake/deployment") == name:
                        del api.objects[k]
        if cur and cur[0]["spec"]["template"] == tmpl:
            dep["status"]["observedGeneration"] = m.get("generation", 1)   # e.g. an annotation: observed, no rollout
            return
        if name in self.stalled:
            return
        for _k, o in rss:
            o["spec"]["replicas"] = 0
            for pk, p in self._owned(api, "pods", ns, o["metadata"]["uid"]):
                if self.linger:
                    p["metadata"]["deletionTimestamp"] = "2026-11-11T00:00:05Z"
                    self.left[name] = self.linger
                else:
                    del api.objects[pk]
        gen = m.get("generation", 1)
        rs = api.add("replicasets", {"kind": "ReplicaSet", "metadata": {
            "name": f"{name}-rs{gen}-{len(rss)}", "namespace": ns,
            "ownerReferences": [{"kind": "Deployment", "name": name, "uid": m["uid"], "controller": True}]},
            "spec": {"replicas": want, "template": copy.deepcopy(tmpl)}})
        ctr = tmpl["spec"]["containers"][0]
        uids = []
        for i in range(want):
            p = api.add("pods", {"kind": "Pod", "metadata": {
                "name": f"{rs['metadata']['name']}-{i:02d}", "namespace": ns,
                "labels": {**(tmpl["metadata"].get("labels") or {}), X.RUN_LABEL: DEMO, "fake/deployment": name},
                "ownerReferences": [{"kind": "ReplicaSet", "name": rs["metadata"]["name"], "uid": rs["metadata"]["uid"],
                                     "controller": True}]},
                "spec": {"nodeName": "worker-w" if ns == "demo" else "worker-s", "tolerations": [],
                         "containers": [{"name": ctr["name"], "resources": copy.deepcopy(ctr.get("resources") or {})}]},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}],
                           "containerStatuses": [{"name": ctr["name"], "restartCount": 0,
                                                  "imageID": self.image_id.get(name, ctr["image"])}]}})
            uids.append(p["metadata"]["uid"])
        api.objects[("endpointslices", ns, f"{name}-es")] = {
            "kind": "EndpointSlice", "metadata": {"name": f"{name}-es", "namespace": ns, "uid": F_uid(),
                                                  "resourceVersion": "1", "labels": {I.SERVICE_LABEL: name}},
            "endpoints": [{"conditions": {"ready": True}, "targetRef": {"kind": "Pod", "uid": u}} for u in uids]}
        dep["status"] = {"observedGeneration": gen, "replicas": want, "updatedReplicas": want, "readyReplicas": want,
                         "availableReplicas": want}


def F_uid():
    import uuid
    return str(uuid.uuid4())


class W:
    """The installed demonstration: the fake cluster, the canonical record's path, a clock and the controller."""

    def __init__(self, world):  # noqa: F811
        record, kube = world()
        run_until(record, kube, "pas")
        self.api, self.tmp, self.path = world.api, world.tmp, record.path
        self.clock = PF.Clock(1_800_000_000)
        self.perm_calls, self.perm = 0, lambda n: PASS
        for n in ("worker-w", "worker-s"):
            node = self.api.obj("Node", n)
            node["status"]["allocatable"] = {"cpu": "8", "memory": "16Gi", "pods": "110"}
            node["status"]["conditions"].append({"type": "PIDPressure", "status": "False"})
        for (kind, _ns, name), claim in list(self.api.objects.items()):       # the fake binds no claim by itself
            if kind == "persistentvolumeclaims":
                claim["spec"]["volumeName"], claim["status"] = f"pv-{name}", {"phase": "Bound"}
                self.api.put({"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": f"pv-{name}"},
                              "spec": {"persistentVolumeReclaimPolicy": "Retain"}})
            if kind == "resourcequotas":                                       # ...and runs no quota controller
                claim["status"] = {"hard": dict(claim["spec"]["hard"]), "used": {"pods": "25"}}
        c = self.ctx()
        self.members = [k for k in I._rendered(c.params)]
        for ns, name in self.members:
            dep = self.api.obj("Deployment", name, ns)
            dep["spec"] = served(dep["spec"])
        self.ctl = Controller(name for _ns, name in self.members)
        self.ctl(self.api)                                  # the first rollout: ReplicaSets, pods, endpoints, status
        self.api.on_poll.append(self.ctl)
        (self.tmp / "demo-params-new.json").write_text(json.dumps(dict(demo_params(), image=NEW)))

    def ctx(self):
        record = X.load_record(self.path, ("prepare",))

        def permission_check():
            self.perm_calls += 1
            return self.perm(self.perm_calls)
        return I.Ctx(record, record.kube(self.api), now=self.clock.now, sleep=self.clock.sleep,
                     permission_check=permission_check)

    def run(self, plan_fn, apply_fn, *args, name="plan"):
        c = self.ctx()
        plan = plan_fn(c)
        saved = X.save_plan(c.record, I.HELPER, plan, name)
        return apply_fn(c, X.load_plan(saved["plan"]), saved["plan_digest"], *args)

    def open(self, new="demo-params-new.json", supersedes=None):
        return self.run(lambda c: I.plan_open(c, new, supersedes=supersedes), I.open_, name="plan-open")

    def dep(self, name):
        ns = next(ns for ns, n in self.members if n == name)
        return self.api.obj("Deployment", name, ns)

    def states(self):
        return {m["name"]: m["state"] for m in I.status(self.ctx())["members"]}

    def events(self):
        return [e["event"] for e in X.receipts(self.ctx().record, I.HELPER)]

    def lock(self):
        return os.path.exists(PF.lock_file(self.ctx().record))


@pytest.fixture
def w(world):  # noqa: F811
    return W(world)


def image_of(dep):
    return dep["spec"]["template"]["spec"]["containers"][0]["image"]


ORDER = ["c28-forecast-route", "c28-metrics-route", "c28-ingress", N, R]


# --- the spec comparison ---------------------------------------------------------------------------------------------

def test_only_enumerated_defaults_the_render_does_not_set_are_ignored(w):
    c = w.ctx()
    for key, r in I._rendered(c.params).items():
        want, live = r["object"]["spec"], w.api.obj("Deployment", key[1], key[0])["spec"]
        assert live != want and I.spec_diff(want, live) == []          # the server's defaults are not differences
        planted = copy.deepcopy(live)
        planted["template"]["spec"]["hostNetwork"] = True               # a field nobody rendered stays visible
        assert I.spec_diff(want, planted) == ["spec.template.spec.hostNetwork: live only (true)"]
        other = copy.deepcopy(live)
        other["template"]["spec"]["dnsPolicy"] = "Default"              # a default field with another value too
        assert "spec.template.spec.dnsPolicy" in I.spec_diff(want, other)[0]
        gone = copy.deepcopy(live)
        del gone["template"]["spec"]["containers"][0]["env"]
        assert "rendered, absent live" in I.spec_diff(want, gone)[0]
    target = I._rendered(c.params)[("demo", N)]["object"]["spec"]       # the targets render their strategy explicitly
    live = copy.deepcopy(w.dep(N)["spec"])
    live["strategy"] = {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": "25%", "maxUnavailable": "25%"}}
    assert any(d.startswith("spec.strategy.rollingUpdate.maxSurge") for d in I.spec_diff(target, live))
    assert I.spec_diff({"a": 1}, {"a": True}) and I.spec_diff({"a": [1]}, {"a": [1, 2]})


# --- open ------------------------------------------------------------------------------------------------------------

def test_the_open_plan_binds_the_members_in_order_and_reads_only(w):
    c = w.ctx()
    w.api.calls.clear()
    p = I.plan_open(c, "demo-params-new.json")
    assert p["problems"] == [] and PF.mutations(w.api) == []
    b = p["binding"]
    assert [m["name"] for m in b["members"]] == ORDER and [m["member"] for m in b["members"]] == [1, 2, 3, 4, 5]
    assert {m["start"] for m in b["members"]} == {"from"} and (b["from_image"], b["to_image"]) == (OLD, NEW)
    assert b["kind"] == "forward" and b["record_path"] == os.path.realpath(w.path)
    assert b["members"][0]["nodes"] == ["worker-s"] and b["members"][4]["nodes"] == ["worker-w"]
    assert [m["container"] for m in b["members"]] == ["proxy", "proxy", "ingress", "fixture", "fixture"]
    assert re.fullmatch(r"ir-1-[0-9a-f]{12}", p["id"]) and p["candidate"]["image_transition"]["id"] == p["id"]
    assert p["candidate"]["params"] == "demo-params-new.json"
    again = I.plan_open(c, "demo-params-new.json", nonce=p["nonce"])
    assert X.digest(again) == X.digest(p)                               # the reviewed nonce reproduces the plan
    assert I.plan_open(c, "demo-params-new.json")["id"] != p["id"]      # another attempt: another id


@pytest.mark.parametrize("edit,text", [
    (lambda w: (w.tmp / "demo-params-new.json").write_text(json.dumps(dict(demo_params(), image=NEW, namespace="x"))),
     "more than `image`"),
    (lambda w: (w.tmp / "demo-params-new.json").write_text(json.dumps(demo_params())), "already current"),
    (lambda w: w.dep(N)["spec"]["template"]["spec"].update(hostNetwork=True), "neither render"),
    (lambda w: w.dep("c28-ingress")["status"].update(readyReplicas=0), "c28-ingress: wants 1"),
    (lambda w: X.receipt(w.ctx().record, "prepare", "intent-99", {"op": "create", "kind": "ConfigMap", "name": "x",
                                                                   "namespace": "demo"}), "unknown outcomes"),
])
def test_the_open_plan_refuses(w, edit, text):
    edit(w)
    problems = I.plan_open(w.ctx(), "demo-params-new.json")["problems"]
    assert any(text in p for p in problems), problems
    with pytest.raises(X.StagingError):
        w.open()
    assert "transition-prepared" not in w.events()


def test_open_journals_the_transition_then_publishes_the_record(w):
    before = open(w.path, "rb").read()
    out = w.open()
    tid = out["data"]["id"]
    assert w.events() == ["transition-prepared", "transition-open"] and not w.lock()
    doc = json.load(open(w.path))
    assert doc["params"] == "demo-params-new.json" and doc["image_transition"]["id"] == tid
    assert open(f"{w.path}.before-{tid}", "rb").read() == before and out["data"]["record_sha256"] == C.digest(doc)
    c = w.ctx()
    assert c.params["image"] == NEW and X.image_fence(c.record) == tid
    assert set(w.states().values()) == {"from"}
    v = prepare.verify(c.record, c.kube)
    assert v["result"] == "IN_TRANSITION" and v["image_transition"] == tid and v["problems"] == []
    assert any(tid in p for p in B.fence_problems(B.Ctx(c.record, c.kube)))
    assert any(tid in p for p in prepare.plan(c.record, c.kube, "pas")["problems"])
    with pytest.raises(X.StagingError, match="is open"):
        w.open()                                                        # a second transition needs --supersedes


@pytest.mark.parametrize("crash", ["before-publish", "after-publish"])
def test_a_crash_inside_open_leaves_the_fence_and_open_recover_decides(w, crash):
    old = json.load(open(w.path))

    def publish(path, tid, doc):
        if crash == "after-publish":
            I._publish(path, tid, doc)
        raise RuntimeError("killed")
    with pytest.raises(RuntimeError):
        w.run(lambda c: I.plan_open(c, "demo-params-new.json"), lambda c, p, d: I.open_(c, p, d, publish))
    assert w.events() == ["transition-prepared"] and not w.lock()
    tid = X.receipts(w.ctx().record, I.HELPER)[0]["data"]["id"]
    archived = X.Record(w.path, old)                                    # whichever record is loaded: fenced
    assert X.image_fence(w.ctx().record) == tid and X.image_fence(archived) == tid
    with pytest.raises(X.StagingError, match="is open"), X.mutating(archived, archived.kube(w.api), "bounded"):
        pass
    assert I.plan_roll(w.ctx())["problems"] == ["no image transition is open"]
    p = I.plan_open_recover(w.ctx())
    want = "transition-open" if crash == "after-publish" else "transition-abandoned"
    assert p["problems"] == [] and [a["journal"] for a in p["actions"]] == [want]
    w.run(I.plan_open_recover, I.open_recover)
    assert w.events() == ["transition-prepared", want]
    if crash == "after-publish":
        assert X.image_fence(w.ctx().record) == tid and set(w.states().values()) == {"from"}
    else:
        assert X.image_fence(w.ctx().record) is None and json.load(open(w.path)) == old
        assert w.open()["data"]["id"].startswith("ir-2-")              # a new attempt has a new id


# --- roll ------------------------------------------------------------------------------------------------------------

def test_a_roll_patches_one_member_at_a_time_and_verifies_each(w):
    tid = w.open()["data"]["id"]
    plan = I.plan_roll(w.ctx())
    assert plan["problems"] == [] and [(a["name"], a["do"]) for a in plan["actions"]] == [(n, "patch") for n in ORDER]
    w.perm_calls = 0
    out = w.run(I.plan_roll, I.roll)
    assert {m["name"]: m["state"] for m in out["members"]} == {n: "rolled" for n in ORDER} and not w.lock()
    ev = w.events()[2:]
    assert ev == [x for n in range(1, 6) for x in (f"intent-{n}", f"step-{n}", "member-rolled")]
    assert w.perm_calls >= 1 + 5                                        # the plan, and again before EVERY patch
    entries = X.receipts(w.ctx().record, I.HELPER)
    for i, name in enumerate(ORDER, 1):
        dep = w.dep(name)
        step = next(e for e in entries if e["event"] == f"step-{i}")["data"]
        assert image_of(dep) == NEW and step["name"] == name and step["result"] == "applied"
        assert dep["metadata"]["annotations"][I.ROLL_ANNOTATION] == f"{tid}/{i}/{DEMO}/imageroll/{i}"
        assert step["pre"]["transition"] == tid and step["pre"]["member"] == i
    rolled = [e["data"] for e in entries if e["event"] == "member-rolled"]
    assert [len(r["pods"]) for r in rolled] == [1, 1, 1, 12, 12]
    assert all(p["imageID"] == NEW for r in rolled for p in r["pods"])
    assert X.image_fence(w.ctx().record) == tid                         # five patches do not complete the transition
    again = I.plan_roll(w.ctx())
    assert again["problems"] == [] and {a["do"] for a in again["actions"]} == {"verify"}


def test_a_new_image_without_this_transitions_patch_is_divergent(w):
    w.open()
    w.dep("c28-ingress")["spec"]["template"]["spec"]["containers"][0]["image"] = NEW      # somebody else's edit
    assert w.states()["c28-ingress"] == "divergent"
    p = I.plan_roll(w.ctx())
    assert any("c28-ingress is divergent" in x for x in p["problems"])
    assert any(f"{N}: an earlier member blocks the order" in x for x in p["problems"])
    with pytest.raises(X.StagingError):
        w.run(I.plan_roll, I.roll)
    assert image_of(w.dep("c28-forecast-route")) == OLD and "intent-1" not in w.events()


@pytest.mark.parametrize("break_it,text", [
    (lambda w: setattr(w, "perm", lambda n: PASS if n <= 4 else {"verdict": "refused", "write_grants": [{}]}),
     "replica-write permission"),
    (lambda w: w.ctl.__setattr__("after", ("c28-metrics-route", lambda a: a.obj("PredictiveAutoscaler", N, "demo")[
        "spec"].update(mode="Active"))), "not Recommend"),
])
def test_the_safety_conditions_are_checked_again_before_every_patch(w, break_it, text):
    w.open()
    w.perm_calls = 0
    break_it(w)
    hook = getattr(w.ctl, "after", None)
    if hook:                                    # the condition breaks right after the named member has rolled
        real = w.ctl.sync

        def sync(api, ns, name, dep):
            real(api, ns, name, dep)
            if name == hook[0] and image_of(dep) == NEW:
                hook[1](api)
        w.ctl.sync = sync
    with pytest.raises(X.StagingError, match=text):
        w.run(I.plan_roll, I.roll)
    s = w.states()
    assert s["c28-forecast-route"] == s["c28-metrics-route"] == "rolled" and s[N] == s[R] == "from" and not w.lock()
    assert s["c28-ingress"] == "from" and image_of(w.dep("c28-ingress")) == OLD
    w.perm = lambda n: PASS
    if hook:
        w.ctl.sync = real
    w.api.obj("PredictiveAutoscaler", N, "demo")["spec"]["mode"] = "Recommend"
    plan = I.plan_roll(w.ctx())
    assert [a["do"] for a in plan["actions"]] == ["verify", "verify", "patch", "patch", "patch"]
    w.run(I.plan_roll, I.roll)
    assert set(w.states().values()) == {"rolled"}


def test_a_rollout_that_does_not_converge_stops_and_a_resume_only_waits(w):
    tid = w.open()["data"]["id"]
    w.ctl.stalled.add("c28-ingress")
    t0 = w.clock.now()
    with pytest.raises(X.StagingError, match="c28-ingress: the rollout did not verify within 300 s"):
        w.run(I.plan_roll, I.roll)
    assert w.clock.now() - t0 >= I.WAIT_SINGLE_S and not w.lock()       # the patch's outcome is known: lock released
    assert X.image_fence(w.ctx().record) == tid and w.states()["c28-ingress"] == "patched" and w.states()[N] == "from"
    assert image_of(w.dep("c28-ingress")) == NEW and image_of(w.dep(N)) == OLD     # no rollback, nothing further
    plan = I.plan_roll(w.ctx())
    assert [a["do"] for a in plan["actions"]] == ["verify", "verify", "wait", "patch", "patch"]
    w.ctl.stalled.clear()
    n = len([e for e in w.events() if e.startswith("intent-")])
    w.run(I.plan_roll, I.roll)
    assert set(w.states().values()) == {"rolled"}
    assert len([e for e in w.events() if e.startswith("intent-")]) == n + 2      # the ingress was not patched again


def test_terminating_pods_overlap_their_replacements_and_block_until_gone(w):
    w.open()
    w.ctl.linger = 40
    seen = []
    real = I.rollout_problems

    def spy(c, m, image, nodes, fresh, dep=None):
        probs, pods = real(c, m, image, nodes, fresh, dep)
        seen.append((m["name"], [p for p in probs if "terminating" in p]))
        return probs, pods
    I.rollout_problems, restore = spy, real
    try:
        w.run(I.plan_roll, I.roll)
    finally:
        I.rollout_problems = restore
    assert set(w.states().values()) == {"rolled"}
    blocked = [n for n, t in seen if t]
    assert N in blocked and "c28-ingress" in blocked                    # old pods existed beside the new ones
    assert any(len(t) == 12 for n, t in seen if n == N)                 # all twelve at once, despite maxSurge 0


@pytest.mark.parametrize("edit,text", [
    (lambda w: w.api.put({"apiVersion": "v1", "kind": "ResourceQuota", "metadata": {"name": "q", "namespace": "demo"},
                          "status": {"hard": {"pods": "30"}, "used": {"pods": "25"}}}), "quota q has pods 25/30, 12"),
    (lambda w: w.api.obj("Node", "worker-w")["status"]["allocatable"].update(pods="30"), "node worker-w has 25 pods"),
    (lambda w: w.api.obj("Node", "worker-w")["status"]["allocatable"].update(cpu="100m"), "lacks unrequested CPU"),
    (lambda w: w.api.obj("Node", "worker-w")["status"]["allocatable"].update(memory="lots"), "cannot be evaluated"),
])
def test_headroom_counts_the_worst_case_of_existing_pods(w, edit, text):
    w.open()
    assert I.plan_roll(w.ctx())["problems"] == []
    edit(w)
    problems = I.plan_roll(w.ctx())["problems"]
    assert any(text in p for p in problems), problems


# --- the three outcomes of a patch -----------------------------------------------------------------------------------

def test_a_refused_patch_is_recorded_as_not_applied_and_a_resume_patches_again(w):
    w.open()
    real, calls = w.api.json_patch, []

    def refuse_once(path, ops):
        calls.append(path)
        if len(calls) == 1:
            raise C.ApiError("patch: failed (invalid, exit 1)", 422)
        return real(path, ops)
    w.api.json_patch = refuse_once
    with pytest.raises(C.ApiError):
        w.run(I.plan_roll, I.roll)
    assert w.events()[2:] == ["intent-1", "step-1"] and not w.lock()
    assert X.receipts(w.ctx().record, I.HELPER)[-1]["data"]["result"] == "not-applied"
    assert set(w.states().values()) == {"from"}
    w.run(I.plan_roll, I.roll)
    assert set(w.states().values()) == {"rolled"}
    assert w.dep("c28-forecast-route")["metadata"]["annotations"][I.ROLL_ANNOTATION].endswith("/imageroll/2")


def lose(w, commit, at=1):
    """The `at`-th patch gets no answer; commit: whether the API server applied it. Returns what was sent."""
    real, sent, n = w.api.json_patch, {}, [0]

    def flaky(path, ops):
        n[0] += 1
        if n[0] != at:
            return real(path, ops)
        sent.update(path=path, ops=ops)
        if commit:
            real(path, ops)
        raise C.ApiError("patch: failed (timeout, exit 1)", 504)
    w.api.json_patch = flaky
    with pytest.raises(X.StagingError, match="outcome of the image-roll patch is unknown"):
        w.run(I.plan_roll, I.roll)
    w.api.json_patch = real
    assert w.lock() and w.events()[-1].startswith("intent-")
    return sent, real


def recover(w):
    c = w.ctx()
    PF.crash_owner(c.record)
    rp = X.recover_plan(c.record, c.kube)
    saved = X.save_plan(c.record, "recover", rp, "recover-plan")
    return X.recover(c.record, c.kube, X.load_plan(saved["plan"]), saved["plan_digest"],
                     image_transition=X.image_fence(c.record))


def test_a_lost_patch_that_committed_is_reconciled_as_done(w):
    w.open()
    lose(w, commit=True, at=3)                                          # the ingress
    assert w.states()["c28-ingress"] == "unresolved"
    c = w.ctx()
    [a] = X.recover_plan(c.record, c.kube)["actions"]
    assert (a["helper"], a["name"], a["observed"]) == ("imageroll", "c28-ingress", "done")
    assert any("unknown outcomes" in p for p in I.plan_roll(c)["problems"])
    recover(w)
    assert not w.lock() and w.states()["c28-ingress"] == "patched"
    assert [a["do"] for a in I.plan_roll(w.ctx())["actions"]] == ["verify", "verify", "wait", "patch", "patch"]
    w.run(I.plan_roll, I.roll)
    assert set(w.states().values()) == {"rolled"}


def test_a_lost_patch_that_did_not_commit_is_fenced_by_the_void_write(w):
    w.open()
    sent, real = lose(w, commit=False)
    c = w.ctx()
    [a] = X.recover_plan(c.record, c.kube)["actions"]
    assert a["observed"] == "unresolved" and a["fence"] == "possible"   # it can still commit: never "not applied"
    with pytest.raises(X.StagingError, match="unresolved outcomes"):
        recover(w)
    assert w.lock()
    plan = I.plan_void(w.ctx())
    assert plan["problems"] == [] and [(v["name"], v["void"]) for v in plan["actions"]] == [("c28-forecast-route", "1")]
    out = w.run(I.plan_void, I.void)
    dep = w.dep("c28-forecast-route")
    op = out["voided"][0]["operation"]
    assert dep["metadata"]["annotations"][I.VOID_ANNOTATION] == op and image_of(dep) == OLD and w.lock()
    assert w.events()[-2:] == ["intent-2", "step-2"]
    with pytest.raises(C.ApiError) as e:                                # the delayed request arrives at last: refused
        real(sent["path"], sent["ops"])
    assert e.value.status == 422 and image_of(dep) == OLD
    c = w.ctx()
    [a] = X.recover_plan(c.record, c.kube)["actions"]
    assert a["observed"] == "not-done" and "fenced" in a["why"]
    recover(w)
    assert not w.lock() and w.states()["c28-forecast-route"] == "from"
    w.run(I.plan_roll, I.roll)                                          # a fresh intent with the new resourceVersion
    assert set(w.states().values()) == {"rolled"}
    assert w.dep("c28-forecast-route")["metadata"]["annotations"][I.ROLL_ANNOTATION].endswith("/imageroll/3")


def test_the_original_patch_can_commit_before_the_void_write(w):
    w.open()
    sent, real = lose(w, commit=False)
    plan_before = I.plan_void(w.ctx())
    assert plan_before["problems"] == []

    def original_first(path, ops):                                      # the delayed request wins the race
        real(sent["path"], sent["ops"])
        return real(path, ops)
    w.api.json_patch = original_first
    c = w.ctx()
    saved = X.save_plan(c.record, I.HELPER, plan_before, "plan-void")
    PF.crash_owner(c.record)
    plan_now = I.plan_void(w.ctx())                                     # the lock's sha changed with crash_owner
    saved = X.save_plan(c.record, I.HELPER, plan_now, "plan-void")
    with pytest.raises(C.ApiError):
        I.void(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    w.api.json_patch = real
    entries = X.receipts(w.ctx().record, I.HELPER)
    assert entries[-1]["event"] == "step-2" and entries[-1]["data"]["result"] == "not-applied"
    dep = w.dep("c28-forecast-route")
    assert image_of(dep) == NEW and I.VOID_ANNOTATION not in dep["metadata"]["annotations"]
    assert any("not a case for the fencing write" in p for p in I.plan_void(w.ctx())["problems"])
    c = w.ctx()
    [a] = X.recover_plan(c.record, c.kube)["actions"]
    assert a["observed"] == "done"
    recover(w)
    assert w.states()["c28-forecast-route"] == "patched" and not w.lock()


@pytest.mark.parametrize("committed", [True, False])
def test_a_lost_answer_to_the_void_write_is_decided_by_its_own_reconciler(w, committed):
    w.open()
    sent, real = lose(w, commit=False)

    def flaky(path, ops):
        if committed:
            real(path, ops)
        raise C.ApiError("patch: failed (timeout, exit 1)", 504)
    c = w.ctx()
    PF.crash_owner(c.record)
    w.api.json_patch = flaky
    with pytest.raises(X.StagingError, match="outcome of the image-roll-void patch is unknown"):
        w.run(I.plan_void, I.void)
    w.api.json_patch = real
    c = w.ctx()
    acts = {a["op"]: a for a in X.recover_plan(c.record, c.kube)["actions"]}
    if committed:
        assert acts[I.VOID_OP]["observed"] == "done" and acts[I.PATCH_OP]["observed"] == "not-done"
        recover(w)
        assert not w.lock() and w.states()["c28-forecast-route"] == "from"
    else:
        assert acts[I.VOID_OP]["observed"] == "unresolved" and acts[I.PATCH_OP]["observed"] == "unresolved"
        assert any("already unresolved" in p for p in I.plan_void(c)["problems"])
        with pytest.raises(X.StagingError, match="unresolved outcomes"):
            recover(w)
        assert w.lock()


def test_an_answer_that_does_not_show_the_expected_state_is_not_an_outcome(w):
    w.open()
    real = w.api.json_patch

    def meddled(path, ops):
        out = real(path, ops)
        out["spec"]["template"]["spec"]["hostNetwork"] = True           # not the spec this patch could produce
        return out
    w.api.json_patch = meddled
    with pytest.raises(X.StagingError, match="does not show the expected post-state"):
        w.run(I.plan_roll, I.roll)
    assert w.lock() and w.events()[-1] == "intent-1"


# --- completion ------------------------------------------------------------------------------------------------------

class VM:
    """The observer for the zero-counter evidence: every counter of the live pods at zero unless `edit` says else."""

    def __init__(self, w):
        self.w, self.edit = w, None

    def pods(self, name):
        return sorted(k[2] for k, p in self.w.api.objects.items() if k[0] == "pods" and k[1] == "demo" and
                      p["metadata"]["labels"].get("fake/deployment") == name and
                      not p["metadata"].get("deletionTimestamp"))

    def __call__(self, url):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        metric, target = re.fullmatch(r'(\w+)\{namespace="demo",target="([^"]+)"\}\[\d+s\]', q["query"][0]).groups()
        t, ing = float(q["time"][0]), self.pods("c28-ingress")[0]
        vals = [[t - 30, "0"], [t - 15, "0"], [t, "0"]]
        if metric == "c28_ingress_requests_total":
            series = [{"metric": {"pod": ing, "target": target}, "values": vals}]
        elif metric == "c28_ingress_dispatched_total":
            series = [{"metric": {"pod": ing, "target": target, "endpoint_pod": p}, "values": vals}
                      for p in self.pods(target)]
        else:
            series = [{"metric": {"pod": p, "target": target, "code": "200"}, "values": vals}
                      for p in self.pods(target)]
        if self.edit:
            series = self.edit(metric, target, copy.deepcopy(series))
        return {"status": "success", "data": {"result": series}}


@pytest.fixture
def rolled(w, monkeypatch):
    monkeypatch.setattr(B, "existing_telemetry", lambda *a, **k: [])
    w.open()
    w.run(I.plan_roll, I.roll)
    return w


def test_forward_completion_needs_the_zero_counters_of_the_replacement_processes(rolled):
    w, vm = rolled, VM(rolled)
    tid = X.image_fence(w.ctx().record)
    assert "needs --query-api" in I.plan_complete(w.ctx())["problems"][0]
    plan = I.plan_complete(w.ctx(), vm, "http://vm")
    assert plan["problems"] == [] and plan["kind"] == "forward"
    out = w.run(lambda c: I.plan_complete(c, vm, "http://vm"), lambda c, p, d: I.complete(c, p, d, vm, "http://vm"))
    assert out["event"] == "transition-complete" and out["data"]["id"] == tid and not w.lock()
    assert w.events()[-2:] == ["zero-counters", "transition-complete"]
    path = os.path.join(w.ctx().record.evidence_dir, "imageroll", f"zero-counters-{tid}-01.json")
    ev = json.load(open(path))
    assert out["data"]["zero_counters"] == {"file": os.path.basename(path), "sha256": C.sha256_file(path)}
    assert ev["series"] == 2 * (1 + 12 + 12) and len(ev["checks"]) == 6
    assert all(k["selector"].startswith("c28_") and 'namespace="demo"' in k["selector"] for k in ev["checks"])
    samples = [x for k in ev["checks"] for s in k["series"] for x in s["samples"]]
    assert {v for _t, v in samples} == {"0"} and len(samples) == 3 * ev["series"]     # every sample, with its time
    assert sorted(ev["replacements"]) == sorted(ORDER) and len(ev["replacements"][N]) == 12
    live = {p["metadata"]["name"]: p["metadata"]["uid"] for k, p in w.api.objects.items() if k[0] == "pods"}
    assert all(live[p["name"]] == p["uid"] for pods in ev["replacements"].values() for p in pods)
    c = w.ctx()
    assert X.image_fence(c.record) is None and prepare.verify(c.record, c.kube)["result"] == "PASS"
    assert not any("image transition" in p for p in B.fence_problems(B.Ctx(c.record, c.kube)))
    with X.mutating(c.record, c.kube, "bounded"):                       # the installation is open to the others again
        pass


def _drop(metric, n=1):
    return lambda m, t, s: s[n:] if m == metric and t == N else s


def _value(metric, v):
    def edit(m, t, s):
        if m == metric and t == R:
            s[0]["values"][-1][1] = v
        return s
    return edit


def _foreign(m, t, s):
    if m == "c28_requests_total" and t == N:
        s.append({"metric": {"pod": "c28-demo-pa-n-old-00", "target": t, "code": "200"}, "values": [[1, "0"]]})
    return s


def _twice(m, t, s):
    return s + s[:1] if m == "c28_ingress_requests_total" and t == N else s


def _samples(metric, make):
    def edit(m, t, s):
        if m == metric:
            for x in s:
                x["values"] = make(x["values"])
        return s
    return edit


def _code(m, t, s):
    if m == "c28_requests_total" and t == N:
        s[0]["metric"]["code"] = "503"
    return s


@pytest.mark.parametrize("edit,text", [
    (_samples("c28_ingress_dispatched_total", lambda v: [[v[-1][0] - 179, "0"]]), "not at least 3 fresh samples"),
    (_samples("c28_requests_total", lambda v: v[:2]), "not at least 3 fresh samples"),
    (_samples("c28_ingress_requests_total", lambda v: [[t - 100, x] for t, x in v]), "not at least 3 fresh samples"),
    (_samples("c28_requests_total", lambda v: [[v[0][0] - 90, "0"]] + v[1:]), "without a gap"),
    (_samples("c28_ingress_requests_total", lambda v: v[:2] + [[v[2][0], "NaN"]]), "finite numbers"),
    (_samples("c28_ingress_dispatched_total", lambda v: [v[-1]] * 3), "1 distinct samples of 3"),
    (_samples("c28_ingress_dispatched_total", lambda v: [[float("nan"), "0"] for _ in v]), "finite numbers"),
    (_samples("c28_ingress_dispatched_total", lambda v: [[t, False] for t, _x in v]), "finite numbers"),
    (_samples("c28_ingress_dispatched_total", lambda v: [[t, x, "extra"] for t, x in v]), "finite numbers"),
    (_samples("c28_requests_total", lambda v: [v[0], v[1], v[1], v[2]]), "3 distinct samples of 4"),
    (_code, "a series with code '503'"),
    (_drop("c28_ingress_requests_total"), f"ingress-arrivals/{N}: series for []"),
    (_drop("c28_ingress_dispatched_total"), f"ingress-dispatch/{N}: series for"),
    (_drop("c28_requests_total", 3), f"fixture-requests/{N}: series for"),
    (_value("c28_ingress_dispatched_total", "1"), f"ingress-dispatch/{R}"),
    (_value("c28_requests_total", "7"), "not exactly zero in every sample"),
    (_foreign, "which is not a replacement pod"),
    (_twice, f"ingress-arrivals/{N}: series for"),
])
def test_completion_refuses_without_exact_zero_counter_evidence(rolled, edit, text):
    w, vm = rolled, VM(rolled)
    vm.edit = edit
    problems = I.plan_complete(w.ctx(), vm, "http://vm")["problems"]
    assert any(text in p for p in problems), problems
    with pytest.raises(X.StagingError):
        w.run(lambda c: I.plan_complete(c, vm, "http://vm"), lambda c, p, d: I.complete(c, p, d, vm, "http://vm"))
    assert "transition-complete" not in w.events() and X.image_fence(w.ctx().record) and not w.lock()


def test_completion_needs_every_member_rolled_and_the_telemetry_gate(w, monkeypatch):
    monkeypatch.setattr(B, "existing_telemetry", lambda *a, **k: ["queue/c28-demo-pa-n: no series"])
    w.open()
    vm = VM(w)
    assert any("is from" in p for p in I.plan_complete(w.ctx(), vm, "http://vm")["problems"])
    w.run(I.plan_roll, I.roll)
    assert I.plan_complete(w.ctx(), vm, "http://vm")["problems"] == ["queue/c28-demo-pa-n: no series"]
    w.api.obj("PredictiveAutoscaler", R, "demo")["spec"]["mode"] = "Active"
    assert any("not Recommend" in p for p in I.plan_complete(w.ctx(), vm, "http://vm")["problems"])


# --- a reverse is a successor transition -----------------------------------------------------------------------------

def test_a_reverse_supersedes_the_open_transition_and_completes_with_its_limitation(w):
    first = w.open()["data"]["id"]
    w.perm_calls, w.perm = 0, lambda n: PASS if n <= 5 else {"verdict": "unproven"}
    with pytest.raises(X.StagingError):
        w.run(I.plan_roll, I.roll)                                      # three members rolled, the targets untouched
    w.perm = lambda n: PASS
    assert list(w.states().values()) == ["rolled", "rolled", "rolled", "from", "from"]
    plan = I.plan_open(w.ctx(), "demo-params.json", supersedes=first)
    assert plan["problems"] == [] and plan["id"].startswith("ir-2-") and plan["binding"]["supersedes"] == first
    assert plan["binding"]["kind"] == "reverse"
    assert [m["start"] for m in plan["binding"]["members"]] == ["from", "from", "from", "target", "target"]
    assert (plan["binding"]["from_image"], plan["binding"]["to_image"]) == (NEW, OLD)
    second = w.open("demo-params.json", supersedes=first)["data"]["id"]
    c = w.ctx()
    ts = X.image_transitions(X.receipts(c.record, I.HELPER))
    assert ts[first]["state"] == "superseded" and ts[second]["state"] == "open" and X.image_fence(c.record) == second
    doc = json.load(open(w.path))
    assert doc["params"] == "demo-params.json" and doc["image_transition"]["supersedes"] == first
    assert list(w.states().values()) == ["from", "from", "from", "target", "target"]
    plan = I.plan_roll(w.ctx())
    assert plan["problems"] == [] and [a["do"] for a in plan["actions"]] == ["patch", "patch", "patch", "none", "none"]
    w.run(I.plan_roll, I.roll)
    assert list(w.states().values()) == ["rolled", "rolled", "rolled", "target", "target"]
    assert {image_of(w.dep(n)) for n in ORDER} == {OLD}
    plan = I.plan_complete(w.ctx())                                     # no observer: nothing to prove at zero
    assert plan["problems"] == [] and plan["kind"] == "reverse" and "not admissible" in plan["limitation"]
    out = w.run(I.plan_complete, I.complete)
    assert out["data"]["kind"] == "reverse" and "limitation" in out["data"] and "zero-counters" not in w.events()
    assert X.image_fence(w.ctx().record) is None
    third = I.plan_open(w.ctx(), "demo-params-new.json")
    assert third["problems"] == [] and third["id"].startswith("ir-3-")   # forward again: a new id, all from


def test_a_successor_waits_for_outstanding_writes(w):
    first = w.open()["data"]["id"]
    lose(w, commit=False)
    problems = I.plan_open(w.ctx(), "demo-params.json", supersedes=first)["problems"]
    assert any("unknown outcomes" in p for p in problems)
    assert any("is not the open image transition" in p
               for p in I.plan_open(w.ctx(), "demo-params.json", supersedes="ir-9-ffffffffffff")["problems"])


def _forward(w):
    return w.run(I.plan_roll, I.roll)


def test_a_successor_to_a_third_image_is_a_forward_transition(w, monkeypatch):
    monkeypatch.setattr(B, "existing_telemetry", lambda *a, **k: [])
    first = w.open()["data"]["id"]
    _forward(w)
    third = OLD.rsplit(":", 1)[0] + ":" + "c" * 64
    (w.tmp / "third.json").write_text(json.dumps(dict(demo_params(), image=third)))
    plan = I.plan_open(w.ctx(), "third.json", supersedes=first)
    assert plan["problems"] == [] and plan["binding"]["kind"] == "forward"       # not what the parent started from
    w.open("third.json", supersedes=first)
    _forward(w)
    assert {image_of(w.dep(n)) for n in ORDER} == {third}
    assert "needs --query-api" in I.plan_complete(w.ctx())["problems"][0]       # no reverse exemption
    plan = I.plan_complete(w.ctx(), VM(w), "http://vm")
    assert plan["problems"] == [] and plan["kind"] == "forward"


def test_a_successor_that_is_not_a_reverse_may_not_leave_members_behind(w):
    first = w.open()["data"]["id"]
    w.perm_calls, w.perm = 0, lambda n: PASS if n <= 5 else {"verdict": "unproven"}
    with pytest.raises(X.StagingError):
        _forward(w)                                                     # three members on NEW, the targets on OLD
    w.perm = lambda n: PASS
    (w.tmp / "third.json").write_text(json.dumps(dict(demo_params(), image=OLD.rsplit(":", 1)[0] + ":" + "c" * 64)))
    problems = I.plan_open(w.ctx(), "third.json", supersedes=first)["problems"]
    assert any(f"{N}: the live spec is neither render" in p for p in problems)


def test_abandoning_a_successor_gives_the_parent_its_fence_back(w):
    original = copy.deepcopy(w.ctx().record.doc)
    original.pop("image_transition", None)
    first = w.open()["data"]["id"]
    _forward(w)

    def crash(path, tid, doc):
        raise RuntimeError("before publication")
    with pytest.raises(RuntimeError):
        w.run(lambda c: I.plan_open(c, "demo-params.json", supersedes=first), lambda c, p, d: I.open_(c, p, d, crash))
    ts = X.image_transitions(X.receipts(w.ctx().record, I.HELPER))
    assert ts[first]["state"] == "superseded" and X.image_fence(w.ctx().record).startswith("ir-2-")
    w.run(I.plan_open_recover, I.open_recover)
    assert w.events()[-1] == "transition-abandoned"
    c = w.ctx()
    assert X.image_transitions(X.receipts(c.record, I.HELPER))[first]["state"] == "open"
    assert X.image_fence(c.record) == first and set(w.states().values()) == {"rolled"}
    archived = X.Record(w.path, original)                               # no field, no successor: still fenced
    with pytest.raises(X.StagingError, match="is open"), X.mutating(archived, archived.kube(w.api), "bounded"):
        pass


def test_an_archived_record_decides_nothing_about_a_published_transition(w):
    def crash(path, tid, doc):
        I._publish(path, tid, doc)
        raise RuntimeError("after publication")
    with pytest.raises(RuntimeError):
        w.run(lambda c: I.plan_open(c, "demo-params-new.json"), lambda c, p, d: I.open_(c, p, d, crash))
    tid = X.image_fence(w.ctx().record)
    archived = X.load_record(f"{w.path}.before-{tid}", ("prepare",))
    c = I.Ctx(archived, archived.kube(w.api), now=w.clock.now, sleep=w.clock.sleep, permission_check=lambda: PASS)
    plan = I.plan_open_recover(c)
    assert plan["actions"] == [] and "an archived copy decides nothing" in plan["problems"][0]
    saved = X.save_plan(c.record, I.HELPER, plan, "plan-open-recover")
    with pytest.raises(X.StagingError, match="archived copy"):
        I.open_recover(c, X.load_plan(saved["plan"]), saved["plan_digest"])
    assert w.events() == ["transition-prepared"] and X.image_fence(archived) == tid
    w.run(I.plan_open_recover, I.open_recover)                          # the canonical record decides: published
    assert w.events()[-1] == "transition-open"
    assert "not the one transition" in I.plan_roll(c)["problems"][0]    # and the archived copy cannot roll either


@pytest.mark.parametrize("when", ["before-publish", "after-publish"])
def test_a_killed_executor_leaves_the_lock_and_the_fence_and_each_is_recovered_explicitly(w, when):
    c = w.ctx()
    plan = I.plan_open(c, "demo-params-new.json")
    saved = X.save_plan(c.record, I.HELPER, plan, "plan-open")
    tid, pid = plan["id"], os.fork()
    if pid == 0:                                    # the executor: killed, not unwound, inside `open`
        def publish(path, t, doc):
            if when == "after-publish":
                I._publish(path, t, doc)
            os.kill(os.getpid(), signal.SIGKILL)
        try:
            I.open_(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"], publish)
        finally:
            os._exit(99)
    _pid, status = os.waitpid(pid, 0)
    assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL
    assert w.events() == ["transition-prepared"] and w.lock()           # nothing unwound: the lock is still there
    assert (json.load(open(w.path)).get("image_transition") or {}).get("id") == (tid if when == "after-publish"
                                                                                else None)
    with pytest.raises(X.StagingError, match="exists"):
        w.run(I.plan_open_recover, I.open_recover)                      # the dead executor's lock comes first
    if when == "after-publish":                     # the lock was taken with the OLD record: the new one is refused
        new = w.ctx()
        rp = X.save_plan(new.record, "recover", X.recover_plan(new.record, new.kube), "recover-plan")
        with pytest.raises(X.StagingError, match="another run or record"):
            X.recover(new.record, new.kube, X.load_plan(rp["plan"]), rp["plan_digest"], image_transition=tid)
        assert w.lock()
    owner = X.load_record(w.path if when == "before-publish" else f"{w.path}.before-{tid}")
    rp = X.save_plan(owner, "recover", X.recover_plan(owner, owner.kube(w.api)), "recover-plan-owner")
    out = X.recover(owner, owner.kube(w.api), X.load_plan(rp["plan"]), rp["plan_digest"], image_transition=tid)
    assert out["data"]["image_transition_open"] == tid and not w.lock()
    assert X.image_fence(w.ctx().record) == tid                         # the lock is gone, the fence is not
    w.run(I.plan_open_recover, I.open_recover)
    assert w.events()[-1] == ("transition-open" if when == "after-publish" else "transition-abandoned")


def test_an_earlier_member_that_lost_readiness_blocks_the_next_patch(w, monkeypatch):
    w.open()
    real = I._wait

    def wait(c, t, m):
        out = real(c, t, m)
        if m["member"] == 1:                        # right after its receipt, the first route's pod stops being Ready
            for p in I._member_pods(c, w.dep(m["name"]))[1]:
                w.api.obj("Pod", p["metadata"]["name"], m["namespace"])["status"]["conditions"] = [
                    {"type": "Ready", "status": "False"}]
        return out
    monkeypatch.setattr(I, "_wait", wait)
    with pytest.raises(X.StagingError, match="the earlier member c28-forecast-route is patched"):
        _forward(w)
    assert [e for e in w.events() if e.startswith("intent-")] == ["intent-1"] and not w.lock()
    assert image_of(w.dep("c28-metrics-route")) == OLD


def test_the_last_read_before_a_patch_must_be_the_reviewed_object(w, monkeypatch):
    w.open()
    real = I._patch

    def patch(c, t, m, reviewed):                   # somebody edits the Deployment after the plan and the checks
        dep = w.dep(m["name"])
        dep["spec"]["template"]["spec"]["hostNetwork"] = True
        dep["metadata"]["generation"] += 1
        dep["metadata"]["resourceVersion"] = str(int(dep["metadata"]["resourceVersion"]) + 100)
        return real(c, t, m, reviewed)
    monkeypatch.setattr(I, "_patch", patch)
    with pytest.raises(X.StagingError, match="c28-forecast-route is not as reviewed"):
        _forward(w)
    assert not [e for e in w.events() if e.startswith("intent-")] and not w.lock()      # nothing was sent


def test_a_spec_changed_during_the_rollout_stops_the_wait(w):
    w.open()
    real = w.ctl.sync

    def sync(api, ns, name, dep):
        real(api, ns, name, dep)
        if name == "c28-ingress" and image_of(dep) == NEW:
            dep["spec"]["template"]["spec"]["hostNetwork"] = True
    w.ctl.sync = sync
    with pytest.raises(X.StagingError, match="c28-ingress: the object is no longer this patch's post-state"):
        _forward(w)
    assert "member-rolled" not in w.events()[-1:] and w.states()["c28-ingress"] == "divergent"


def test_completion_resumes_after_an_interrupted_attempt(rolled, monkeypatch):
    w, vm = rolled, VM(rolled)
    tid, real = X.image_fence(w.ctx().record), X.receipt

    def crash(record, helper, event, data, *a, **k):
        if event == "zero-counters":
            raise RuntimeError("after the evidence file")
        return real(record, helper, event, data, *a, **k)
    go = (lambda c: I.plan_complete(c, vm, "http://vm"), lambda c, p, d: I.complete(c, p, d, vm, "http://vm"))
    with monkeypatch.context() as mp:
        mp.setattr(X, "receipt", crash)
        with pytest.raises(RuntimeError):
            w.run(*go)
    folder = os.path.join(w.ctx().record.evidence_dir, "imageroll")
    assert [f for f in sorted(os.listdir(folder)) if f.startswith("zero-")] == [f"zero-counters-{tid}-01.json"]
    assert X.image_fence(w.ctx().record) == tid and not w.lock()
    w.clock.sleep(5)
    out = w.run(*go)
    assert out["data"]["zero_counters"]["file"] == f"zero-counters-{tid}-02.json"       # the attempt that completed
    assert os.path.exists(os.path.join(folder, f"zero-counters-{tid}-01.json"))        # the interrupted one is kept
    assert X.image_fence(w.ctx().record) is None


def _rewrite(w, name, **change):
    path = w.tmp / name
    path.write_text(json.dumps(dict(json.loads(path.read_text()), **change)))


def test_a_params_file_changed_after_the_review_stops_everything(rolled):
    w, vm = rolled, VM(rolled)
    plan = I.plan_complete(w.ctx(), vm, "http://vm")
    assert plan["problems"] == []
    saved = X.save_plan(w.ctx().record, I.HELPER, plan, "plan-complete")
    _rewrite(w, "demo-params-new.json", image=OLD)                      # the bound NEW params now say the old image
    c = w.ctx()
    assert c.params["image"] == OLD and {image_of(w.dep(n)) for n in ORDER} == {NEW}
    problems = I.plan_complete(c, vm, "http://vm")["problems"]
    assert any("is not the file the transition bound" in p for p in problems), problems
    with pytest.raises(X.StagingError):
        I.complete(c, X.load_plan(saved["plan"]), saved["plan_digest"], vm, "http://vm")
    assert "transition-complete" not in w.events() and I.plan_roll(c)["problems"]
    _rewrite(w, "demo-params-new.json", image=NEW)
    assert I.plan_complete(w.ctx(), vm, "http://vm")["problems"] == []
    _rewrite(w, "demo-params.json", release="other")                    # the bound OLD file is checked as well
    assert any("demo-params.json is not the file the transition bound" in p
               for p in I.plan_complete(w.ctx(), vm, "http://vm")["problems"])


def test_params_changed_between_the_plan_and_the_apply_send_nothing(w):
    plan = I.plan_open(w.ctx(), "demo-params-new.json")
    saved = X.save_plan(w.ctx().record, I.HELPER, plan, "plan-open")
    third = OLD.rsplit(":", 1)[0] + ":" + "c" * 64
    _rewrite(w, "demo-params-new.json", image=third)                    # after the review of the open plan
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        I.open_(w.ctx(), X.load_plan(saved["plan"]), saved["plan_digest"])
    assert w.events() == []
    _rewrite(w, "demo-params-new.json", image=NEW)
    w.open()
    c = w.ctx()                                                         # this executor loaded the bound params...
    plan = I.plan_roll(c)
    saved = X.save_plan(c.record, I.HELPER, plan, "plan-roll")
    _rewrite(w, "demo-params-new.json", image=third)                    # ...and the file changes before its apply
    assert any("not the file the transition bound" in p for p in I.plan_roll(c)["problems"])
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        I.roll(c, X.load_plan(saved["plan"]), saved["plan_digest"])
    assert not [e for e in w.events() if e.startswith("intent-")] and not w.lock()
    assert {image_of(w.dep(n)) for n in ORDER} == {OLD}


def test_a_params_file_changed_during_the_roll_stops_before_the_next_patch(w):
    w.open()
    real = w.ctl.sync

    def sync(api, ns, name, dep):
        real(api, ns, name, dep)
        if name == "c28-metrics-route" and image_of(dep) == NEW:        # while the second member rolls
            (w.tmp / "demo-params-new.json").write_text(json.dumps(dict(demo_params(), image=NEW, release="other")))
    w.ctl.sync = sync
    with pytest.raises(X.StagingError, match="c28-ingress: not safe to continue"):
        w.run(I.plan_roll, I.roll)
    assert image_of(w.dep("c28-ingress")) == OLD and len([e for e in w.events() if e.startswith("intent-")]) == 2


def test_a_changed_params_file_is_refused_where_only_the_resume_check_looks(w):
    """plan-void runs no safety conditions (it only fences a lost patch): the bound files are checked by the resume
    check alone, and status reports the same problems without being a gate."""
    w.open()
    lose(w, commit=False)
    assert I.plan_void(w.ctx())["problems"] == [] and I.status(w.ctx())["problems"] == []
    _rewrite(w, "demo-params-new.json", release="other")
    for problems in (I.plan_void(w.ctx())["problems"], I.status(w.ctx())["problems"]):
        assert any("demo-params-new.json is not the file the transition bound" in p for p in problems), problems
    assert I.plan_void(w.ctx())["actions"] == []
    with pytest.raises(X.StagingError, match="not the file the transition bound"):
        w.run(I.plan_void, I.void)
    assert w.events()[-1] == "intent-1"                                 # no fencing write was sent


# --- the command line ------------------------------------------------------------------------------------------------

def test_the_commands_plan_and_report(w, capsys, monkeypatch):
    monkeypatch.setattr(I, "_operator_permissions", lambda c: PASS)
    assert I.main(["--record", w.path, "status"], api=w.api) == 0
    assert json.loads(capsys.readouterr().out) == {"action": "status", "fence": None, "helper": "imageroll",
                                                   "members": [], "problems": [], "transition": None}
    assert I.main(["--record", w.path, "plan-open", "--new-params", "demo-params-new.json"], api=w.api) == 0
    saved = json.loads(capsys.readouterr().out)
    assert saved["problems"] == [] and os.path.basename(saved["plan"]).startswith("plan-open-")
    assert I.main(["--record", w.path, "open", "--plan", saved["plan"], "--plan-digest", saved["plan_digest"]],
                  api=w.api) == 0
    capsys.readouterr()
    assert I.main(["--record", w.path, "plan-roll"], api=w.api) == 0
    assert json.loads(capsys.readouterr().out)["problems"] == []
    assert prepare.main(["--record", w.path, "verify"], api=w.api) == 5          # IN_TRANSITION is never exit 0
    assert json.loads(capsys.readouterr().out)["result"] == "IN_TRANSITION"
    assert I.main(["--record", w.path, "recover-plan"], api=w.api) == 0
