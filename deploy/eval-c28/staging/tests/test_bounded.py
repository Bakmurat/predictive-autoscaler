"""bounded.py, the admission run's lifecycle on the in-memory API: plan refusals and the shared fence (both ways), a
complete run (journalled creates, observation, evidence written before cleanup, the verdict from the written file, the
pod proven gone before the ConfigMap goes), a stop, a VOID start and an unproven disappearance that keeps the lock.
The verdict's criteria are tested in the harness (test_demo_admission)."""

import json
import os
from datetime import datetime, timezone

import pytest

import bounded as B
import execlib as X
import fake2 as F
import prepare
from part2_fixtures import api  # noqa: F401
from test_prepare_demo import DEMO, N, R, run_until, world  # noqa: F401

ORIGIN = "2026-11-11T00:05:00Z"
O_MS = int(datetime(2026, 11, 11, 0, 5, tzinfo=timezone.utc).timestamp() * 1000)


class Clock:
    def __init__(self, ms):
        self.ms = ms

    def now(self):
        return self.ms

    def sleep(self, s):
        self.ms += s * 1000


def ready_world(world):  # noqa: F811
    record, kube = world()
    run_until(record, kube, "pas")
    a = world.api
    for n in ("worker-w", "worker-s"):                    # a real node reports all three pressure conditions
        conds = a.obj("Node", n).setdefault("status", {}).setdefault("conditions", [])
        if not any(x["type"] == "PIDPressure" for x in conds):
            conds.append({"type": "PIDPressure", "status": "False"})
    for name in (N, R):
        dep = a.obj("Deployment", name, "demo")
        dep["status"] = {"replicas": 12, "readyReplicas": 12, "updatedReplicas": 12}
        uids = []
        for i in range(12):
            p = F.pod(f"{name}-{i:02d}", "demo", "worker-w", labels={"c28.devkuban.com/target": name, X.RUN_LABEL: DEMO})
            p["status"]["conditions"] = [{"type": "Ready", "status": "True", "lastTransitionTime": "2026-11-10T00:00:00Z"}]
            p["status"]["containerStatuses"] = [{"name": "fixture", "containerID": f"c-{name}-{i}", "restartCount": 0}]
            a.put(p)
            uids.append(a.obj("Pod", p["metadata"]["name"], "demo")["metadata"]["uid"])
        a.put({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
               "metadata": {"name": f"{name}-x", "namespace": "demo", "labels": {"kubernetes.io/service-name": name}},
               "endpoints": [{"conditions": {"ready": True}, "targetRef": {"uid": u}} for u in uids]})
    return record, kube, a


class JobApi(F.FakeApi2):
    """A created Job gets one Running pod it owns; the Job completes (pod exit 0) once the clock passes its end."""

    def __init__(self, base, clock, end_ms, sticky=False, never_running=False):
        self.__dict__.update(base.__dict__)
        self.clock, self.end_ms, self.sticky, self.never_running = clock, end_ms, sticky, never_running

    def create(self, path, body):
        out = super().create(path, body)
        m = body["metadata"]
        if body["kind"] == "Job":
            job = self.obj("Job", m["name"], m["namespace"])
            job["metadata"]["creationTimestamp"] = datetime.fromtimestamp(self.clock.ms / 1000, timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            pod = F.pod(m["name"] + "-abcde", m["namespace"], "worker-s",
                        labels=dict(body["spec"]["template"]["metadata"]["labels"]),
                        phase="Pending" if self.never_running else "Running")
            pod["metadata"]["ownerReferences"] = [{"kind": "Job", "uid": job["metadata"]["uid"], "controller": True}]
            pod["status"]["containerStatuses"] = [{"name": "k6", "state": {"running": {}}}]
            self.put(pod)
        return out

    def get(self, path):
        o = super().get(path)
        if o and o.get("kind") == "Job" and self.clock.ms >= self.end_ms:
            o.setdefault("status", {})["conditions"] = [{"type": "Complete", "status": "True"}]
            for p in self.objects.values():
                if any(r.get("uid") == o["metadata"]["uid"] for r in p["metadata"].get("ownerReferences") or []):
                    p["status"]["phase"] = "Succeeded"
                    p["status"]["containerStatuses"] = [{"name": "k6", "state": {"terminated": {"exitCode": 0}}}]
        return o

    def delete(self, path, uid, resource_version=None):
        if self.sticky and "/pods/" in path:
            self.calls.append(("delete", path))
            return {}
        if "/jobs/" in path:                  # the raw API's default for a Job: its pod is ORPHANED, not collected
            self.calls.append(("delete", path))
            kind, ns, name, _ = self._parse(path)
            o = self.objects.get((kind, ns, name))
            if o is None or o["metadata"]["uid"] != uid:
                return super().delete(path, uid, resource_version)
            del self.objects[(kind, ns, name)]
            return {}
        return super().delete(path, uid, resource_version)


def fetch(url):
    return {"status": "success", "data": {"resultType": "matrix", "result": []}}


def plan_now(record, kube, clock):
    c = B.Ctx(record, kube, now=clock.now, sleep=clock.sleep)
    p = B.plan_admission(c, ORIGIN)
    saved = X.save_plan(record, B.HELPER, p, "plan-admission")
    return c, X.load_plan(saved["plan"]), saved["plan_digest"]


def test_a_plan_binds_the_objects_and_passes_on_a_ready_world(world):  # noqa: F811
    record, kube, a = ready_world(world)
    clock = Clock(O_MS - 180_000)
    c, p, d = plan_now(record, kube, clock)
    assert p["problems"] == [], p["problems"]
    assert [x["create"] for x in p["actions"]] == ["ConfigMap", "Job"] and p["deadline_s"] == 300 + 2400 + 180
    assert set(p["targets"]) == {N, R} and all(v["pa_uid"] for v in p["targets"].values())


@pytest.mark.parametrize("edit, text", [
    (lambda a: a.put({"apiVersion": "apps/v1", "kind": "StatefulSet",
                      "metadata": {"name": "c28-demo-generator", "namespace": "demo",
                                   "labels": {"app.kubernetes.io/name": "c28-demo-generator"}}, "spec": {}}),
     "one generator-like workload at a time"),
    (lambda a: a.put(dict(F.pod("c28-demo-startup-x", "demo", "worker-s",
                                labels={"app.kubernetes.io/name": "c28-demo-startup"}),
                          metadata={"name": "c28-demo-startup-x", "namespace": "demo",
                                    "labels": {"app.kubernetes.io/name": "c28-demo-startup"},
                                    "deletionTimestamp": "2026-11-11T00:00:00Z"})), "(Terminating)"),
    (lambda a: a.obj("Deployment", N, "demo")["status"].update(readyReplicas=11), "is not at 12 Ready"),
    (lambda a: a.obj("PredictiveAutoscaler", R, "demo")["spec"].update(mode="Active"), "not Recommend"),
])
def test_the_plan_refuses_a_fence_or_a_target_not_as_reviewed(world, edit, text):  # noqa: F811
    record, kube, a = ready_world(world)
    edit(a)
    _, p, _ = plan_now(record, kube, Clock(O_MS - 180_000))
    assert any(text in x for x in p["problems"]), p["problems"]


def test_the_origin_must_be_two_to_five_minutes_ahead(world):  # noqa: F811
    record, kube, a = ready_world(world)
    _, p, _ = plan_now(record, kube, Clock(O_MS - 60_000))
    assert any("must be 120..300" in x for x in p["problems"])


def test_prepares_generator_phase_refuses_while_a_bounded_workload_exists(world):  # noqa: F811
    record, kube, a = ready_world(world)
    a.put(F.pod("c28-demo-admission-x", "demo", "worker-s", labels={"app.kubernetes.io/name": "c28-demo-admission"}))
    probs = prepare.plan(record, kube, "generator", now=lambda: "2026-11-11T00:00:00Z")["problems"]
    assert any("a bounded admission workload exists" in x for x in probs)


def _run(world, **kw):  # noqa: F811
    record, kube, base = ready_world(world)
    clock = Clock(O_MS - 180_000)
    a = JobApi(base, clock, O_MS + 40 * 60_000 + 60_000, **kw)
    kube = record.kube(api=a)
    c, p, d = plan_now(record, kube, clock)
    assert p["problems"] == [], p["problems"]
    logs = lambda c_, pod: ['{"schema": "x"}']
    return record, kube, a, clock, c, p, d, logs


def test_a_complete_run_writes_evidence_before_cleanup_and_proves_the_pod_gone(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs = _run(world)
    out = B.run_admission(c, p, d, fetch, "http://vm", logs=logs)
    assert out["data"]["verdict"] in ("PASS", "INCOMPLETE", "FAIL", "LIMIT", "STOPPED") and not out["data"]["stopped"]
    ops = [(m["op"], m["kind"]) for m in kube.mutations]
    assert ops == [("create", "ConfigMap"), ("create", "Job"), ("delete", "Job"), ("delete", "Pod"),
                   (B.GONE_OP, "Pod"), ("delete", "ConfigMap")]
    ev = json.load(open(os.path.join(out["data"]["dir"], "evidence.json")))
    assert ev["job"] == {"status": "Complete", "exit_code": 0, "created_s": ev["job"]["created_s"]}
    assert len(ev["pods"]["expected"][N]) == 12 and ev["endpoints"] and ev["pods"]["watch"]
    v = json.load(open(os.path.join(out["data"]["dir"], "verdict.json")))
    assert v["verdict"] == out["data"]["verdict"]
    assert not os.path.exists(X.lock_file(record)) and not X.open_intents(X.receipts(record, B.HELPER))


def test_node_pressure_stops_the_run_and_keeps_the_partial_evidence(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs = _run(world)
    real_sleep = clock.sleep

    def sleep(s):
        real_sleep(s)
        if clock.ms > O_MS + 5 * 60_000:
            n = a.obj("Node", "worker-s")
            next(x for x in n["status"]["conditions"] if x["type"] == "MemoryPressure")["status"] = "True"
    c.sleep = sleep
    out = B.run_admission(c, p, d, fetch, "http://vm", logs=logs)
    assert out["data"]["verdict"] == "STOPPED" and "MemoryPressure" in out["data"]["stopped"]
    assert os.path.exists(os.path.join(out["data"]["dir"], "evidence.json"))
    assert ("delete", "Job") in [(m["op"], m["kind"]) for m in kube.mutations]


def test_a_pod_not_running_before_the_origin_is_void_and_cleaned_up(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs = _run(world, never_running=True)
    with pytest.raises(X.StagingError, match="VOID"):
        B.run_admission(c, p, d, fetch, "http://vm", logs=logs)
    ops = [(m["op"], m["kind"]) for m in kube.mutations]
    assert ops[:2] == [("create", "ConfigMap"), ("create", "Job")] and ("delete", "ConfigMap") == ops[-1]
    assert not os.path.exists(X.lock_file(record))


def test_an_unproven_pod_disappearance_keeps_the_lock(world):  # noqa: F811
    record, kube, a, clock, c, p, d, logs = _run(world, sticky=True)
    with pytest.raises(X.StagingError, match="still present"):
        B.run_admission(c, p, d, fetch, "http://vm", logs=logs)
    assert os.path.exists(X.lock_file(record))
    assert [it["data"]["op"] for it in X.open_intents(X.receipts(record, B.HELPER))] == [B.GONE_OP]
    assert ("delete", "ConfigMap") not in [(m["op"], m["kind"]) for m in kube.mutations]


def test_a_fresh_process_of_another_helper_decides_the_bounded_gone_step(world, tmp_path):  # noqa: F811
    import subprocess
    import sys
    from test_ledger_read import FRESH
    record, kube, a, clock, c, p, d, logs = _run(world, sticky=True)
    with pytest.raises(X.StagingError, match="still present"):
        B.run_admission(c, p, d, fetch, "http://vm", logs=logs)

    def fresh():
        objs = tmp_path / "objects.json"
        objs.write_text(json.dumps([v for v in a.objects.values() if v.get("kind")]))
        r = subprocess.run([sys.executable, "-I", "-c", FRESH.replace('assert "ledger_read" not in sys.modules',
                                                                     'assert "bounded" not in sys.modules'),
                            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), record.path, str(objs)],
                           capture_output=True, text=True, timeout=120, env=dict(os.environ, KUBECONFIG="/dev/null"))
        assert r.returncode == 0, r.stderr[-2000:]
        return json.loads(r.stdout.strip().splitlines()[-1])
    assert [B.GONE_OP, "unresolved"] in fresh()
    for k in [k for k, v in a.objects.items() if v["metadata"]["name"].startswith("c28-demo-admission-")
              and v.get("kind") == "Pod"]:
        del a.objects[k]
    assert [B.GONE_OP, "done"] in fresh()
