"""legacy.py: an exact, UID-bound S4 plan; execution only with the reviewed digest; evidence and archived objects
untouched; verify and the ten quiet minutes; selective restore."""

import copy
import json

import pytest

import common_records as C
import legacy as LG
from fakes import CLUSTER, MARKERS, legacy_cluster, record

T0 = 1_794_000_000


def ready_record(tmp_path, **legacy):
    rec = record(tmp_path, legacy={"parked_replicas": 0, "job_deadline_seconds": 600, "wait_timeout_seconds": 60,
                                   "external_launchers": ["pf-loop on the ops box"],
                                   "external_launchers_attested": True, **legacy})
    r = C.append_receipt(rec, "archive", "archive-complete", {"plan_sha256": "f" * 64}, clock=lambda: T0)
    rec["legacy"].setdefault("archive_receipt_sha256", r["sha256"])
    return rec


class Clock:
    def __init__(self, t=T0):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def snapshot_state(api):
    return copy.deepcopy(api.objects)


def test_the_plan_is_exact_ordered_and_read_only(tmp_path):
    api = legacy_cluster()
    plan = LG.build_plan(api, ready_record(tmp_path))
    assert api.mutations() == []
    ops = [(a["op"], a.get("name")) for a in plan["actions"]]
    def first(op, name=None):
        return next(i for i, (o, n) in enumerate(ops) if o == op and (name is None or n == name))
    assert ops[:3] == [("suspend", "ml-training"), ("suspend", "evidence-archive"), ("suspend", "load-evidence")]
    assert first("suspend") < first("wait_jobs") < first("scale", "k6-nginx-test") < \
        first("scale", "predictive-operator") < first("delete") < first("verify_no_writers") < \
        first("scale", "nginx-test") < first("scale", "ml-api")
    touched = {(a.get("kind"), a.get("name")) for a in plan["actions"] if a.get("kind")}
    assert not any(k in ("persistentvolumeclaims", "pa-legacy", "customresourcedefinitions", "leases")
                   for k, _ in touched)                                  # evidence, archived PAs, CRD, Lease: never
    for a in plan["actions"]:
        if a.get("kind"):
            assert a["uid"] and a["namespace"] in ("demo", "ml-engine")
    deletes = [a for a in plan["actions"] if a["op"] == "delete"]
    assert {a["name"] for a in deletes} == {"nginx-test-keda-fallback", "myapptwo-keda-fallback"}
    assert all(a["generated_hpa"]["name"] == f"keda-hpa-{a['name']}" for a in deletes)
    assert {r["name"] for r in plan["retained"] if r["role"] == "retained"} >= {"p8-evidence-pvc", "ml-models-pvc"}


@pytest.mark.parametrize("change, why", [
    (lambda rec, api: rec["legacy"].update(archive_receipt_sha256="0" * 64), "archive-complete"),
    (lambda rec, api: rec["legacy"].update(external_launchers_attested=False), "external_launchers_attested"),
    (lambda rec, api: api.objects.pop(("persistentvolumeclaims", "ml-engine", "p8-evidence-pvc")), "retained"),
    (lambda rec, api: api.add("pa-legacy", {"kind": "PredictiveAutoscaler",
                                            "metadata": {"name": "x", "namespace": "shop"},
                                            "spec": {"targetDeployment": {"name": "web"}}}), "another consumer"),
])
def test_the_plan_refuses_until_s0_and_consumers_are_settled(tmp_path, change, why):
    api, rec = legacy_cluster(), ready_record(tmp_path)
    change(rec, api)
    with pytest.raises(C.StagingError, match=why):
        LG.build_plan(api, rec)


def test_quiesce_runs_only_the_reviewed_plan_and_keeps_evidence(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    for digest in (None, "0" * 64):
        with pytest.raises(C.StagingError):
            LG.quiesce(api, rec, plan, digest, sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.mutations() == []
    keep = {k: copy.deepcopy(v) for k, v in api.objects.items()
            if k[0] in ("persistentvolumeclaims", "persistentvolumes", "pa-legacy", "customresourcedefinitions",
                        "leases", "secrets", "services")}
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    for k, v in keep.items():
        assert api.objects[k] == v, k                                   # byte-for-byte untouched
    deps = {k[2]: v for k, v in api.objects.items() if k[0] == "deployments"}
    assert all(d["spec"]["replicas"] == 0 for d in deps.values())
    assert all(v["spec"]["suspend"] for k, v in api.objects.items() if k[0] == "cronjobs")
    assert not any(k[0] in ("scaledobjects", "horizontalpodautoscalers") for k in api.objects)
    muts = api.mutations()
    assert all(v in ("replace", "delete") for v, _ in muts)
    assert not any("/persistentvolume" in p or "predictiveautoscalers" in p for _, p in muts)
    rlog = open(C.receipts_path(rec, "legacy")).read()
    assert not any(m in rlog for m in MARKERS)
    snaps = list((tmp_path / "evidence" / "legacy" / "deleted").iterdir())
    assert len(snaps) == 2 and not any(m in s.read_text() for s in snaps for m in MARKERS)


def test_execution_refuses_drift_and_another_cluster(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    d = C.plan_digest(plan)
    api.objects[("deployments", "demo", "k6-nginx-test")]["metadata"]["generation"] += 1      # edited after review
    with pytest.raises(C.StagingError, match="changed"):
        LG.quiesce(api, rec, plan, d, sleep=clock.sleep, monotonic=clock, clock=clock)
    api2, rec2 = legacy_cluster(), ready_record(tmp_path / "b")
    plan2 = LG.build_plan(api2, rec2)
    api2.objects[("namespaces", None, "kube-system")]["metadata"]["uid"] = "00000000-0000-4000-8000-000000000000"
    with pytest.raises(C.StagingError, match="kube-system UID"):
        LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api2.mutations() == []
    api3, rec3 = legacy_cluster(), ready_record(tmp_path / "c")
    plan3 = LG.build_plan(api3, rec3)
    old = api3.objects.pop(("scaledobjects", "demo", "myapptwo-keda-fallback"))      # recreated: new UID
    api3.add("scaledobjects", dict(old, metadata={k: v for k, v in old["metadata"].items() if k not in ("uid",)}))
    with pytest.raises(C.StagingError, match="another UID"):
        LG.quiesce(api3, rec3, plan3, C.plan_digest(plan3), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert ("scaledobjects", "demo", "myapptwo-keda-fallback") in api3.objects        # never deleted


def test_active_jobs_finish_first_or_quiesce_stops(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    job = api.add("jobs", {"kind": "Job", "metadata": {"name": "load-evidence-1", "namespace": "ml-engine",
                                                       "ownerReferences": [{"kind": "CronJob",
                                                                            "name": "load-evidence"}]},
                           "status": {"active": 1}})
    plan = LG.build_plan(api, rec)
    polls = {"n": 0}

    def finish(a):
        polls["n"] += 1
        if polls["n"] > 40:
            job["status"] = {"active": 0, "succeeded": 1, "completionTime": "2026-11-08T00:05:00Z"}
    api.on_poll.append(finish)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b", job_deadline_seconds=30), Clock()
    api2.add("jobs", {"kind": "Job", "metadata": {"name": "p8-x", "namespace": "ml-engine", "labels": {"p8-run": "r"}},
                      "status": {"active": 1}})
    plan2 = LG.build_plan(api2, rec2)
    with pytest.raises(C.StagingError, match="active Jobs"):
        LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock2.sleep, monotonic=clock2, clock=clock2)
    assert api2.objects[("deployments", "demo", "k6-nginx-test")]["spec"]["replicas"] == 1   # stopped before arrivals


def test_verify_declares_retirement_only_after_ten_quiet_minutes(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    assert LG.verify(api, rec, clock=clock, now=clock())["quiesced"] is False
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    clock.t += 1
    v1 = LG.verify(api, rec, clock=clock, now=clock())
    assert v1["quiesced"] and not v1["retired"] and v1["problems"] == []
    clock.t += 601
    assert LG.verify(api, rec, clock=clock, now=clock())["retired"] is True
    api.objects[("deployments", "demo", "nginx-test")]["spec"]["replicas"] = 2          # something wrote a target
    v3 = LG.verify(api, rec, clock=clock, now=clock())
    assert not v3["retired"] and any("nginx-test" in p for p in v3["problems"])


def test_restore_undoes_exactly_what_quiesce_did(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    before = {k: (v["spec"].get("replicas"), v["spec"].get("suspend")) for k, v in api.objects.items()
              if k[0] in ("deployments", "cronjobs")}
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.add("deployments", {"kind": "Deployment", "metadata": {"name": "c28-pa-n-s20", "namespace": "demo"},
                            "spec": {"replicas": 2}})                                    # the new fixture
    rplan = LG.build_restore_plan(api, rec)
    assert rplan["actions"][0]["name"] == "ml-api"                                      # reverse order
    assert not any(a.get("name", "").startswith("c28-") for a in rplan["actions"])
    with pytest.raises(C.StagingError):
        LG.restore(api, rec, rplan, "0" * 64, clock=clock)
    LG.restore(api, rec, rplan, C.plan_digest(rplan), clock=clock)
    after = {k: (v["spec"].get("replicas"), v["spec"].get("suspend")) for k, v in api.objects.items()
             if k[0] in ("deployments", "cronjobs") and not k[2].startswith("c28-")}
    assert after == before
    assert api.objects[("deployments", "demo", "c28-pa-n-s20")]["spec"]["replicas"] == 2   # never touched
    so = api.objects[("scaledobjects", "demo", "nginx-test-keda-fallback")]
    assert so["spec"]["scaleTargetRef"] == {"name": "nginx-test"}
    assert so["metadata"]["annotations"] == {"autoscaling.keda.sh/paused": "true"}


def test_restore_never_overwrites_and_refuses_recreated_targets(tmp_path):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    api.add("scaledobjects", {"kind": "ScaledObject",
                              "metadata": {"name": "myapptwo-keda-fallback", "namespace": "demo"},
                              "spec": {}})
    rplan = LG.build_restore_plan(api, rec)
    with pytest.raises(C.StagingError, match="exists"):
        LG.restore(api, rec, rplan, C.plan_digest(rplan), clock=clock)
    api2, rec2, clock2 = legacy_cluster(), ready_record(tmp_path / "b"), Clock()
    plan2 = LG.build_plan(api2, rec2)
    LG.quiesce(api2, rec2, plan2, C.plan_digest(plan2), sleep=clock2.sleep, monotonic=clock2, clock=clock2)
    old = api2.objects.pop(("deployments", "ml-engine", "ml-api"))
    api2.add("deployments", dict(old, metadata={k: v for k, v in old["metadata"].items() if k != "uid"}))
    rplan2 = LG.build_restore_plan(api2, rec2)
    with pytest.raises(C.StagingError, match="another UID"):
        LG.restore(api2, rec2, rplan2, C.plan_digest(rplan2), clock=clock2)


def test_the_cli_defaults_to_a_read_only_plan(tmp_path, capsys):
    api, rec = legacy_cluster(), ready_record(tmp_path)
    path = tmp_path / "rec.json"
    path.write_text(json.dumps(rec))
    assert LG.main(["--record", str(path)], api=api) == 0
    assert api.mutations() == []
    out = capsys.readouterr().out
    plan_file = out.split("plan: ")[1].split("\n")[0]
    digest = out.strip().split()[-1]
    assert C.plan_digest(C.load_plan(plan_file)) == digest
    assert LG.main(["--record", str(path), "quiesce", "--plan", plan_file, "--plan-digest", "1" * 64], api=api) == 1
    assert api.mutations() == []
    clock = Clock()
    assert LG.main(["--record", str(path), "quiesce", "--plan", plan_file, "--plan-digest", digest], api=api,
                   sleep=clock.sleep, monotonic=clock, clock=clock) == 0
    for f in (tmp_path / "evidence").rglob("*"):
        if f.is_file():
            assert not any(m in f.read_text() for m in MARKERS), f


@pytest.mark.parametrize("kind, ns, name", [("persistentvolumeclaims", "ml-engine", "p8-evidence-pvc"),
                                            ("pa-legacy", "demo", "nginx-test-autoscaler"),
                                            ("deployments", "demo", "c28-pa-n-s20")])
def test_even_a_reviewed_plan_cannot_touch_evidence_or_foreign_objects(tmp_path, kind, ns, name):
    api, rec, clock = legacy_cluster(), ready_record(tmp_path), Clock()
    plan = LG.build_plan(api, rec)
    plan["actions"].insert(0, {"step": 0, "op": "delete", "kind": kind, "namespace": ns, "name": name, "uid": "u"})
    with pytest.raises(C.StagingError, match="not a mutable legacy object"):
        LG.quiesce(api, rec, plan, C.plan_digest(plan), sleep=clock.sleep, monotonic=clock, clock=clock)
    assert api.mutations() == []
