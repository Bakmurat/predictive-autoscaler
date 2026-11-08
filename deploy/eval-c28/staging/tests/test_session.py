import json

import pytest

import common_records as C
import execlib as X
import freeze
import session as S
from part2_fixtures import api, make_record, sign, ssh_keys, world  # noqa: F401

GEN = "grafana/k6@sha256:" + "e" * 64
T0 = 1_796_000_000                                   # a fixed "now" for preparation sessions (2026-11-30)
BLOCK = "2026-12-10T00:00:00Z"
BLOCK_T = 1_796_860_800                              # BLOCK as an epoch


def steal_ok(url):
    return {"data": {"result": [{"metric": {"node": n}, "values": [[i, "1.0"] for i in range(5)]}
                                for n in ("worker-w", "worker-s")]}}


def steal_high(url):
    return {"data": {"result": [{"metric": {"node": "worker-w"}, "values": [[i, "9.0"] for i in range(5)]}]}}


def write_plan(world, kind, seed, target, capacity=7.0, accelerate=1, name="plan.json"):
    c28 = X.import_harness()
    (world["tmp"] / name).write_text(json.dumps(c28.schedule.k6_plan(kind, seed, capacity, target, accelerate)))
    return name


def session_record(world, **over):
    s = {"id": "s-0001", "phase": "preparation", "arm": "H-CPU", "profile": "s20", "seed": 101,
         "generator_image": GEN, "arrival_budget": {"max_rps": 200, "max_total_arrivals": 50_000_000},
         "deadline_utc": "2026-12-02T00:00:00Z",
         "stops": {"steal_query": "max by (node) (steal)", "query_api": "http://vm.example:8481/select/0/prometheus",
                   "steal_percent": 5, "steal_bins": 5}}
    if "plan" not in over:
        s["plan"] = write_plan(world, "ordinary", 101, "c28-h-cpu-s20")
    s.update(over)
    return X.load_record(world["make_record"](dict(world["extra"], session=s)), ("session",))


def ctx(world, record, now=T0):
    return S.Context(record, record.kube(world["api"]), now=lambda: now)


def review(record, plan, helper, name):
    saved = X.save_plan(record, helper, plan, name)
    return X.load_plan(saved["plan"]), saved["plan_digest"]


def start_session(world, record, now=T0):
    c = ctx(world, record, now)
    p = S.plan_start(c)
    assert p["problems"] == [], p["problems"]
    plan, digest = review(record, p, "session", "start")
    return c, digest, S.start(c, plan, digest)


def stop_session(c):
    plan, digest = review(c.record, S.plan_stop(c), "session-stop", "stop")
    return S.stop(c, plan, digest)


def events(record):
    return [e["event"] for e in X.receipts(record, "session")]


def problems(world, record, now=T0):
    return "\n".join(S.plan_start(ctx(world, record, now))["problems"])


# --- preparation ----------------------------------------------------------------------------------------------------

def test_preparation_baseline_session_lifecycle(world):
    record = session_record(world)
    api = world["api"]
    before = len(api.mutations())
    S.plan_start(ctx(world, record))
    assert len(api.mutations()) == before                         # the plan is read-only
    c, digest, started = start_session(world, record)
    assert api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo")["spec"]["scaleTargetRef"]["name"] == \
        "c28-h-cpu-s20"
    job = api.obj("Job", "c28-gen-s-0001", "demo")
    assert job["spec"]["suspend"] is False and job["spec"]["activeDeadlineSeconds"] == 86_400 + 900
    assert job["spec"]["template"]["spec"]["containers"][0]["image"] == GEN
    assert job["metadata"]["labels"][X.SESSION_LABEL] == "s-0001"
    assert S.verify(c, steal_ok)["result"] == "OK"
    sp = S.plan_stop(c)
    assert [(a["op"], a.get("kind")) for a in sp["actions"]] == [("delete", "Job"),
                                                                  ("delete", "HorizontalPodAutoscaler"),
                                                                  ("keep", "ConfigMap")]
    stop_session(c)
    assert api.obj("Job", "c28-gen-s-0001", "demo") is None
    assert api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo") is None
    assert api.obj("ConfigMap", "c28-k6-s-0001", "demo") is not None          # the plan stays as evidence
    with pytest.raises(X.StagingError, match="not open"):
        S.plan_stop(c)


def test_one_loaded_arm_at_a_time(world):
    start_session(world, session_record(world))
    other = session_record(world, id="s-0002", arm="H-RPS",
                           plan=write_plan(world, "ordinary", 102, "c28-h-rps-s20", name="p2.json"))
    probs = problems(world, other)
    assert "sessions still open" in probs and "generators of the run still active" in probs


def test_preparation_pa_session_never_activates(world):
    record = session_record(world, arm="PA-N", plan=write_plan(world, "ordinary", 101, "c28-pa-n-s20", name="pa.json"))
    c = ctx(world, record)
    assert not any(a["op"] in ("activate", "create-writer") for a in S.plan_start(c)["actions"])
    start_session(world, record)
    assert world["api"].obj("PredictiveAutoscaler", "c28-pa-n-s20", "demo")["spec"]["mode"] == "Recommend"


def test_preparation_refuses_scored_plans_and_dirty_faults(world):
    record = session_record(world, plan=write_plan(world, "scored", 201, "c28-h-cpu-s20", name="scored.json"))
    assert "ordinary plans only" in problems(world, record)
    record = session_record(world)
    world["api"].obj("ConfigMap", "c28-metrics-route-faults", "pa-eval-system")["data"]["faults.json"] = '{"x":1}'
    assert "fault-disabled" in problems(world, record)


def test_arrival_budget_and_deadline(world):
    assert "exceeds the admitted arrival budget" in problems(
        world, session_record(world, arrival_budget={"max_rps": 10, "max_total_arrivals": 1000}))
    assert "deadline does not cover the plan" in problems(world, session_record(world,
                                                                                deadline_utc="2026-11-30T12:00:00Z"))


def test_exact_writer_inventory(world):
    api = world["api"]
    record = session_record(world)
    api.put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
             "metadata": {"name": "stray", "namespace": "demo"},
             "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "c28-pa-r-s20"}}})
    api.obj("PredictiveAutoscaler", "c28-pa-e-s90", "demo")["spec"]["mode"] = "Active"
    probs = problems(world, record)
    assert "c28-pa-r-s20: writers ['HorizontalPodAutoscaler/stray', 'PredictiveAutoscaler/c28-pa-r-s20']" in probs
    assert "PredictiveAutoscaler/c28-pa-e-s90 is Active" in probs


def test_legacy_group_writer_counts(world):
    world["api"].add("pa-legacy", {"apiVersion": "autoscaler.example.com/v1alpha1", "kind": "PredictiveAutoscaler",
                                   "metadata": {"name": "old", "namespace": "demo"},
                                   "spec": {"targetDeployment": {"name": "c28-h-cpu-s20", "namespace": "demo"}}})
    assert "c28-h-cpu-s20: writers ['LegacyPredictiveAutoscaler/old']" in problems(world, session_record(world))


def test_replaced_target_is_refused(world):
    record = session_record(world)
    world["api"].obj("Deployment", "c28-h-cpu-s20", "demo")["metadata"]["uid"] = "99999999-0000-4000-8000-000000000000"
    assert "missing or replaced" in problems(world, record)


def test_start_needs_the_reviewed_plan(world):
    record = session_record(world)
    c = ctx(world, record)
    plan, digest = review(record, S.plan_start(c), "session", "start")
    with pytest.raises(X.StagingError, match="differs from the reviewed digest"):
        S.start(c, plan, "0" * 64)
    world["api"].put({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                      "metadata": {"name": "late", "namespace": "demo"},
                      "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "c28-h-cpu-s20"}}})
    with pytest.raises(X.StagingError, match="changed since review"):
        S.start(c, plan, digest)


def test_watchdog_stops_on_a_manifest_stop(world):
    record = session_record(world)
    c, digest, started = start_session(world, record)
    out = S.watch(c, digest, sleep=lambda s: None, fetch=steal_high, max_checks=3)
    assert out["ended"] == "stopped" and "guest steal" in out["stops"][0]
    assert world["api"].obj("Job", "c28-gen-s-0001", "demo") is None
    stopped = X.last(X.receipts(record, "session"), "session-stopped")
    assert stopped["data"]["reason"].startswith("watchdog: ")


def test_missing_observation_is_a_stop(world):
    record = session_record(world)
    c, digest, started = start_session(world, record)

    def broken(url):
        raise OSError("connection refused")
    v = S.verify(c, broken)
    assert v["result"] == "STOP" and "required observation missing" in v["stops"][0]


def test_watchdog_needs_the_reviewed_start_digest(world):
    record = session_record(world)
    c, digest, started = start_session(world, record)
    with pytest.raises(X.StagingError, match="digest of the reviewed start plan"):
        S.watch(c, "0" * 64, sleep=lambda s: None, fetch=steal_ok, max_checks=1)


def test_failed_start_is_undone(world):
    record = session_record(world)
    api = world["api"]
    c = ctx(world, record)
    plan, digest = review(record, S.plan_start(c), "session", "start")
    real = api.create

    def flaky(path, body):
        if body["kind"] == "Job":
            raise C.ApiError("create: Forbidden: exceeded quota")
        return real(path, body)
    api.create = flaky
    with pytest.raises(X.StagingError):
        S.start(c, plan, digest)
    assert api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo") is None
    assert events(record) == ["start-aborted", "start-undone"]
    assert S._open_sessions(X.receipts(record, "session")) == []


# --- confirmation ---------------------------------------------------------------------------------------------------

def confirmation_world(world, arm="PA-N", seed=201, block=BLOCK, sign_receipts=True):
    if not world["have_ssh"]:
        pytest.skip("ssh-keygen is not available")
    record = world["record"]
    p = freeze.plan(record)
    assert p["problems"] == [], p["problems"]
    saved = X.save_plan(record, "freeze", p)
    freeze.assemble(record, X.load_plan(saved["plan"]), saved["plan_digest"])
    if sign_receipts:
        for who in ("alice", "bob"):
            body = freeze.receipt_body(record, who, now=lambda: "2026-11-22T00:00:00Z")
            path = world["tmp"] / "receipts" / f"{who}.json"
            path.write_text(json.dumps(body, sort_keys=True))
            sign(world["keys"][who], freeze.RECEIPT_NS, path)
    target = X.import_harness().render.target_name(arm, "s20")
    rec = session_record(world, id="c-0001", phase="confirmation", arm=arm, profile="s20", seed=seed,
                         plan=f"evidence/plans/scored-{seed}-{target}.json", block_start=block,
                         runlock="evidence/runlock.json", manifest_root=".", deadline_utc="2026-12-11T00:30:00Z",
                         arrival_budget={"max_rps": 500, "max_total_arrivals": 50_000_000})
    pa = world["api"].obj("PredictiveAutoscaler", target, "demo")
    if pa is not None:
        pa["metadata"]["generation"] = 3
        pa["status"] = {"observedGeneration": 3,
                        "conditions": [{"type": "PredictionReady", "status": "True", "reason": "Ready"}],
                        "prediction": {"eligible": True, "lead": {"seconds": 180}, "frozenLeadSeconds": 180}}
    return rec, target


def test_confirmation_pa_session(world):
    record, target = confirmation_world(world)
    api = world["api"]
    c, digest, started = start_session(world, record, now=BLOCK_T - 120)
    assert api.obj("PredictiveAutoscaler", target, "demo")["spec"]["mode"] == "Active"
    assert api.obj("Job", "c28-gen-c-0001", "demo")["spec"]["suspend"] is True
    mr = json.loads(api.obj("ConfigMap", "c28-metrics-route-faults", "pa-eval-system")["data"]["faults.json"])
    assert mr["outages"][0]["start"] == "2026-12-10T12:15:00Z"
    assert json.loads(api.obj("ConfigMap", f"{target}-faults", "demo")["data"]["faults.json"])["slow_starts"]
    assert api.obj("ConfigMap", "c28-pa-e-s20-faults", "demo")["data"]["faults.json"] == "{}"
    c.now = lambda: BLOCK_T + 2
    out = S.watch(c, digest, sleep=lambda s: None, fetch=steal_ok, max_checks=1)
    assert out["ended"] == "check budget exhausted"
    assert api.obj("Job", "c28-gen-c-0001", "demo")["spec"]["suspend"] is False
    assert X.last(X.receipts(record, "session"), "generator-released")["data"]["offset_s"] == 2
    api.obj("Node", "worker-w")["status"]["conditions"][1]["status"] = "True"     # MemoryPressure
    api.obj("ResourceQuota", "c28-fixture-quota", "demo")["spec"]["hard"]["pods"] = "5"
    out = S.watch(c, digest, sleep=lambda s: None, fetch=steal_ok, max_checks=2)
    assert out["ended"] == "stopped"
    assert api.obj("Job", "c28-gen-c-0001", "demo") is None
    assert api.obj("PredictiveAutoscaler", target, "demo")["spec"]["mode"] == "Recommend"
    assert api.obj("ConfigMap", "c28-metrics-route-faults", "pa-eval-system")["data"]["faults.json"] == "{}"
    assert api.obj("ResourceQuota", "c28-fixture-quota", "demo")["spec"]["hard"]["pods"] == "64"
    ops = [st["op"] for st in X.last(X.receipts(record, "session"), "session-stopped")["data"]["steps"]]
    assert ops.index("delete") < ops.index("clear-faults") < ops.index("recommend") < ops.index("restore-quota")


def test_confirmation_needs_operator_verified_readiness(world):
    record, target = confirmation_world(world)
    pa = world["api"].obj("PredictiveAutoscaler", target, "demo")
    pa["status"]["conditions"] = [{"type": "PredictionReady", "status": "False", "reason": "DataUnavailable",
                                   "message": "ML API returned status 503"}]
    pa["status"]["prediction"]["eligible"] = False
    pa["status"]["prediction"]["lead"] = {"seconds": 120}
    probs = problems(world, record, BLOCK_T - 60)
    assert "PredictionReady is False/DataUnavailable" in probs
    assert "eligible is not true" in probs and "differs from the locked 180 s" in probs


def test_confirmation_refuses_unsigned_wrong_date_and_late_start(world):
    record, target = confirmation_world(world, sign_receipts=False, block="2026-12-11T00:00:00Z")
    probs = problems(world, record, BLOCK_T + 86_400 + 60)
    assert "reviewer receipts" in probs and "is not a permitted date" in probs and "start has passed" in probs


def test_confirmation_refuses_an_unlocked_plan(world):
    record, target = confirmation_world(world)
    alt = write_plan(world, "scored", 202, target, name="wrong-seed.json")
    record = X.load_record(world["make_record"](dict(world["extra"], session=dict(record.doc["session"], plan=alt))),
                           ("session",))
    assert "not the run-lock's locked confirmation plan" in problems(world, record, BLOCK_T - 60)


def test_confirmation_too_early_is_refused(world):
    record, target = confirmation_world(world)
    assert "too early" in problems(world, record, BLOCK_T - 3600)
