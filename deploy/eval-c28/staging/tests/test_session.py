import json
import os

import pytest

import common_records as C
import execlib as X
import part2_fixtures as PF
import fake2 as F
import freeze
import session as S
from part2_fixtures import Clock, api, make_record, sign, ssh_keys, world  # noqa: F401

GEN = "grafana/k6@sha256:" + "e" * 64
T0 = 1_796_000_000                                   # a fixed "now" for preparation sessions (2026-11-30)
BLOCK = "2026-12-10T00:00:00Z"
BLOCK_T = 1_796_860_800                              # BLOCK as an epoch
NODES = ("worker-w", "worker-s")


def steal(clock, value="1.0", nodes=NODES, stale=0):
    """A fake query API answering five fresh, consecutive one-minute samples per node."""
    def fetch(url):
        end = clock.now() - clock.now() % 60 - stale
        return {"data": {"result": [{"metric": {"node": n}, "values": [[end - 60 * i, value] for i in range(4, -1, -1)]}
                                    for n in nodes]}}
    return fetch


def observe(clock, arrivals_at=None, steal_value="1.0"):
    """A fake query API: fresh steal samples for both nodes, and the ingress counter for the target rising from 0 to
    5 at `arrivals_at` (never when None)."""
    import urllib.parse

    def fetch(url):
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        if "c28_ingress_requests_total" in q["query"][0]:
            start, end, step = int(q["start"][0]), int(q["end"][0]), int(q["step"][0])
            vals = [[t, "0" if arrivals_at is None or t < arrivals_at else "5"] for t in range(start, end + 1, step)]
            return {"data": {"result": [{"metric": {}, "values": vals}]}}
        return steal(clock, steal_value)(url)
    return fetch


def write_plan(world, kind, seed, target, capacity=7.0, accelerate=1, name="plan.json"):
    c28 = X.import_harness()
    (world["tmp"] / name).write_text(json.dumps(c28.schedule.k6_plan(kind, seed, capacity, target, accelerate)))
    return name


def session_record(world, params=None, **over):
    s = {"id": "s-0001", "phase": "preparation", "arm": "H-CPU", "profile": "s20", "seed": 101,
         "generator_image": GEN, "arrival_budget": {"max_rps": 200, "max_total_arrivals": 50_000_000},
         "deadline_utc": "2026-12-02T00:00:00Z", "quiesce_timeout_s": 60,
         "stops": {"steal_query": "max by (node) (steal)", "query_api": "http://vm.example:8481/select/0/prometheus",
                   "steal_percent": 5, "steal_bins": 5}}
    if "plan" not in over:
        s["plan"] = write_plan(world, "ordinary", 101, "c28-h-cpu-s20")
    s.update(over)
    return X.load_record(world["make_record"](dict(world["extra"], session=s), params=params), ("session",))


def ctx(world, record, now=T0):
    clock = Clock(now)
    c = S.Context(record, record.kube(world["api"]), now=clock.now, sleep=clock.sleep)
    c.clock = clock
    return c


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


def lock_path(record):
    return PF.lock_file(record)


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
    assert job["metadata"]["labels"][X.SESSION_LABEL] == "s-0001"
    assert S.verify(c, steal(c.clock))["result"] == "OK"
    sp = S.plan_stop(c)
    assert [(a["op"], a.get("kind")) for a in sp["actions"]] == [("delete", "Job"),
                                                                  ("delete", "HorizontalPodAutoscaler"),
                                                                  ("keep", "ConfigMap")]
    out = stop_session(c)
    assert out["data"]["resolved"] is True and events(record)[-1] == "session-closed"
    assert api.obj("Job", "c28-gen-s-0001", "demo") is None
    assert api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo") is None
    assert api.obj("ConfigMap", "c28-k6-s-0001", "demo") is not None          # the plan stays as evidence
    assert not os.path.exists(lock_path(record))
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
    assert not any(a["op"] in ("activate", "create-writer") for a in S.plan_start(ctx(world, record))["actions"])
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
    assert "c28-pa-r-s20: writers ['HorizontalPodAutoscaler/demo/stray#" in probs
    assert "PredictiveAutoscaler/c28-pa-e-s90 is Active" in probs


def test_foreign_namespace_pa_with_the_expected_name_is_a_writer(world):
    """Codex #32 item 3."""
    world["api"].add("pa-new", {"kind": "PredictiveAutoscaler",
                                "metadata": {"name": "c28-pa-n-s20", "namespace": "other"},
                                "spec": {"mode": "Active",
                                         "targetDeployment": {"name": "c28-pa-n-s20", "namespace": "demo"}}})
    probs = problems(world, session_record(world))
    assert "PredictiveAutoscaler/other/c28-pa-n-s20#" in probs


def test_legacy_group_writer_counts(world):
    world["api"].add("pa-legacy", {"apiVersion": "autoscaler.example.com/v1alpha1", "kind": "PredictiveAutoscaler",
                                   "metadata": {"name": "old", "namespace": "demo"},
                                   "spec": {"targetDeployment": {"name": "c28-h-cpu-s20", "namespace": "demo"}}})
    assert "c28-h-cpu-s20: writers ['LegacyPredictiveAutoscaler/demo/old#" in problems(world, session_record(world))


def test_live_arm_config_must_equal_its_rendering(world):
    """Codex #32 item 1: a live capacityPerReplica of 999 is a start problem."""
    record = session_record(world, arm="PA-N", plan=write_plan(world, "ordinary", 101, "c28-pa-n-s20", name="pa.json"))
    world["api"].obj("PredictiveAutoscaler", "c28-pa-n-s20", "demo")["spec"]["metrics"]["requests"][
        "capacityPerReplica"] = "999"
    assert "differs from its reviewed rendering at ['/spec/metrics/requests/capacityPerReplica']" in \
        problems(world, record)
    world["api"].obj("Deployment", "c28-pa-n-s20", "demo")["spec"]["template"]["spec"]["containers"][0]["image"] = \
        "evil@sha256:" + "0" * 64
    assert "Deployment/c28-pa-n-s20 differs" in problems(world, record)


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


def test_definitive_failure_is_undone_and_releases_the_lock(world):
    record = session_record(world)
    api = world["api"]
    c = ctx(world, record)
    plan, digest = review(record, S.plan_start(c), "session", "start")
    real = api.create

    def refused(path, body):
        if body["kind"] == "Job":
            raise C.ApiError("create: failed (forbidden)", 403)
        return real(path, body)
    api.create = refused
    with pytest.raises(X.StagingError):
        S.start(c, plan, digest)
    assert api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo") is None
    assert events(record)[-2:] == ["start-undone", "session-closed"]
    assert S._open_sessions(X.receipts(record, "session")) == [] and not os.path.exists(lock_path(record))


def test_lost_response_leaves_an_open_session_that_recovery_and_stop_clean(world):
    """Codex #32 item 2: a committed HPA create whose response is lost stays owned, blocks the next start, keeps the
    shared lock until recovery, and the scoped stop removes it."""
    record = session_record(world)
    api = world["api"]
    c = ctx(world, record)
    plan, digest = review(record, S.plan_start(c), "session", "start")
    real = api.create

    def lost(path, body):
        out = real(path, body)
        if body["kind"] == "HorizontalPodAutoscaler":
            raise C.ApiError("create: failed (timeout)", 504)
        return out
    api.create = lost
    with pytest.raises(X.StagingError):
        S.start(c, plan, digest)
    api.create = real
    assert api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo") is not None
    assert os.path.exists(lock_path(record))
    other = session_record(world, id="s-0002", arm="H-RPS",
                           plan=write_plan(world, "ordinary", 102, "c28-h-rps-s20", name="p2.json"))
    probs = problems(world, other)
    assert "still open or unresolved" in probs and "unknown outcomes" in probs
    with pytest.raises(X.StagingError, match="exists"), X.mutating(other, other.kube(api), "reserve"):
        pass                                         # the shared lock blocks every part-2 helper until recovery
    rp = X.recover_plan(record, c.kube)
    assert [(a["kind"], a["observed"]) for a in rp["actions"]] == [("HorizontalPodAutoscaler", "done")]
    PF.recover(record, c.kube, S.recover_follow_up(record))
    assert not os.path.exists(lock_path(record))
    sp = S.plan_stop(c)
    assert ("delete", "HorizontalPodAutoscaler") in [(a["op"], a.get("kind")) for a in sp["actions"]]
    assert stop_session(c)["data"]["resolved"] is True
    assert api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo") is None


def test_stop_waits_for_generator_pods_and_reports_unresolved(world):
    """Codex #32 item 4: a surviving (terminating) generator pod keeps the session open; the next start is refused."""
    record = session_record(world)
    api = world["api"]
    c, digest, started = start_session(world, record)
    pod = F.pod("c28-gen-s-0001-abc", "demo", "worker-s", labels={X.SESSION_LABEL: "s-0001", X.RUN_LABEL: "c28-r1"})
    pod["metadata"]["deletionTimestamp"] = "2026-11-30T00:00:00Z"
    api.put(pod)
    out = stop_session(c)
    assert out["data"]["resolved"] is False and "generator pods still present" in out["data"]["unresolved"][0]
    other = session_record(world, id="s-0002", arm="H-RPS",
                           plan=write_plan(world, "ordinary", 102, "c28-h-rps-s20", name="p2.json"))
    assert "still open or unresolved" in problems(world, other)
    del api.objects[("pods", "demo", "c28-gen-s-0001-abc")]
    assert stop_session(c)["data"]["resolved"] is True
    assert "still open or unresolved" not in problems(world, other)


def test_replaced_objects_are_unresolved_not_success(world):
    record = session_record(world)
    api = world["api"]
    c, digest, started = start_session(world, record)
    api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo")["metadata"]["uid"] = F.new_uid()
    sp = S.plan_stop(c)
    assert any("was replaced under the session" in u for u in sp["unresolved"])
    out = stop_session(c)
    assert out["data"]["resolved"] is False and events(record)[-1] == "session-stopped"


def test_pa_stop_needs_the_operator_to_confirm_recommend(world):
    record, target = confirmation_world(world)
    api = world["api"]
    c, digest, started = start_session(world, record, now=BLOCK_T - 120)
    api.on_poll.clear()                                 # the operator stops reconciling
    out = stop_session(c)
    assert out["data"]["resolved"] is False and "has not confirmed Recommend" in out["data"]["unresolved"][0]


def test_watchdog_stops_on_a_manifest_stop(world):
    record = session_record(world)
    c, digest, started = start_session(world, record)
    out = S.watch(c, digest, fetch=steal(c.clock, "9.0"), max_checks=3)
    assert out["ended"] == "stopped" and "guest steal" in out["stops"][0]
    assert world["api"].obj("Job", "c28-gen-s-0001", "demo") is None
    assert X.last(X.receipts(record, "session"), "session-stopped")["data"]["reason"].startswith("watchdog: ")


@pytest.mark.parametrize("bad, why", [
    ({"nodes": ("worker-w", "worker-x")}, "no steal series for the support node"),
    ({"value": "NaN"}, "finite samples"),
    ({"value": "not-a-number"}, "malformed steal samples"),
    ({"stale": 600}, "stale or not consecutive"),
])
def test_watchdog_requires_valid_fresh_observations(world, bad, why):
    """Codex #32 item 5."""
    record = session_record(world)
    c, digest, started = start_session(world, record)
    out = S.watch(c, digest, fetch=steal(c.clock, **bad), max_checks=2)
    assert out["ended"] == "stopped" and any(why in s for s in out["stops"]), out["stops"]


def test_watchdog_stops_on_api_failure_and_unplanned_occupant(world):
    record = session_record(world)
    api = world["api"]
    c, digest, started = start_session(world, record)
    api.put(F.pod("shop-1", "shop", "worker-w", owner=("Deployment", "shop")))
    out = S.watch(c, digest, fetch=steal(c.clock), max_checks=1)
    assert out["ended"] == "stopped" and any("unplanned" in s for s in out["stops"])
    record2 = session_record(world, id="s-0002", arm="H-RPS",
                             plan=write_plan(world, "ordinary", 102, "c28-h-rps-s20", name="p2.json"))
    del api.objects[("pods", "shop", "shop-1")]
    c2, d2, _ = start_session(world, record2)

    def broken(url):
        raise OSError("connection refused")
    out = S.watch(c2, d2, fetch=broken, max_checks=1)
    assert out["ended"] == "stopped" and "required observation missing" in out["stops"][0]


def test_watchdog_cleanup_failure_stays_unresolved(world):
    record = session_record(world)
    api = world["api"]
    c, digest, started = start_session(world, record)
    real = api.delete

    def failing(path, uid, resource_version=None):
        raise C.ApiError("delete: failed (server error)", 500)
    api.delete = failing
    out = S.watch(c, digest, fetch=steal(c.clock, "9.0"), max_checks=1)
    api.delete = real
    assert out["ended"] == "unresolved" and out["unresolved"]
    assert os.path.exists(lock_path(record))                # unknown outcomes keep the shared lock


def test_watchdog_needs_the_reviewed_start_digest(world):
    record = session_record(world)
    c, digest, started = start_session(world, record)
    with pytest.raises(X.StagingError, match="digest of the reviewed start plan"):
        S.watch(c, "0" * 64, fetch=steal(c.clock), max_checks=1)


# --- confirmation ---------------------------------------------------------------------------------------------------

def confirmation_world(world, arm="PA-N", seed=201, block=BLOCK, sign_receipts=True, params=None, **session_over):
    if not world["have_ssh"]:
        pytest.skip("ssh-keygen is not available")
    record = world["record"]
    if not os.path.exists(record.resolve("evidence/runlock.json")):
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
    over = dict(id="c-0001", phase="confirmation", arm=arm, profile="s20", seed=seed,
                plan=f"evidence/plans/scored-{seed}-{target}.json", block_start=block,
                runlock="evidence/runlock.json", manifest_root=".", deadline_utc="2026-12-11T00:30:00Z",
                arrival_budget={"max_rps": 500, "max_total_arrivals": 50_000_000})
    over.update(session_over)
    rec = session_record(world, params=params, **over)
    pa = world["api"].obj("PredictiveAutoscaler", target, "demo")
    if pa is not None:
        pa["metadata"]["generation"] = 3
        pa["status"] = {"observedGeneration": 3,
                        "conditions": [{"type": "PredictionReady", "status": "True", "reason": "Ready"}],
                        "prediction": {"eligible": True, "lead": {"seconds": 180}, "frozenLeadSeconds": 180}}
    return rec, target


def generator_pod(api, sid, statuses, job_uid=None, name=None):
    """A generator pod bound (controller reference) to the owned Job, with the given container statuses."""
    job = api.obj("Job", f"c28-gen-{sid}", "demo")
    p = F.pod(name or f"c28-gen-{sid}-x", "demo", "worker-s", labels={X.SESSION_LABEL: sid, X.RUN_LABEL: "c28-r1"},
              phase="Running")
    p["metadata"]["ownerReferences"] = [{"kind": "Job", "name": f"c28-gen-{sid}", "controller": True,
                                         "uid": job_uid or job["metadata"]["uid"]}]
    p["status"]["containerStatuses"] = statuses
    api.put(p)


K6_RUNNING = [{"name": "k6", "state": {"running": {"startedAt": "2026-12-10T00:00:03Z"}}}]


def test_confirmation_pa_session(world):
    record, target = confirmation_world(world)
    api = world["api"]
    c, digest, started = start_session(world, record, now=BLOCK_T - 120)
    assert api.obj("PredictiveAutoscaler", target, "demo")["spec"]["mode"] == "Active"
    assert api.obj("Job", "c28-gen-c-0001", "demo")["spec"]["suspend"] is True
    mr = json.loads(api.obj("ConfigMap", "c28-metrics-route-faults", "pa-eval-system")["data"]["faults.json"])
    assert mr["outages"][0]["start"] == "2026-12-10T12:15:00Z"
    assert api.obj("ConfigMap", "c28-pa-e-s20-faults", "demo")["data"]["faults.json"] == "{}"
    c.clock.t = BLOCK_T + 2
    out = S.watch(c, digest, fetch=steal(c.clock), max_checks=1)
    assert out["ended"] == "check budget exhausted"
    assert api.obj("Job", "c28-gen-c-0001", "demo")["spec"]["suspend"] is False
    generator_pod(api, "c-0001", K6_RUNNING)
    c.clock.t = BLOCK_T + 30
    S.watch(c, digest, fetch=observe(c.clock, arrivals_at=BLOCK_T + 7), max_checks=1)
    assert X.last(X.receipts(record, "session"), "block-classified")["data"] == {
        "session": "c-0001", "block_start": BLOCK, "classification": "on-time", "lateness_s": 10,
        "k6_container": "running"}
    del api.objects[("pods", "demo", "c28-gen-c-0001-x")]
    api.obj("Node", "worker-w")["status"]["conditions"][1]["status"] = "True"     # MemoryPressure
    api.obj("ResourceQuota", "c28-fixture-quota", "demo")["spec"]["hard"]["pods"] = "5"
    out = S.watch(c, digest, fetch=steal(c.clock), max_checks=2)
    assert out["ended"] == "stopped", out
    assert api.obj("Job", "c28-gen-c-0001", "demo") is None
    assert api.obj("PredictiveAutoscaler", target, "demo")["spec"]["mode"] == "Recommend"
    assert api.obj("ConfigMap", "c28-metrics-route-faults", "pa-eval-system")["data"]["faults.json"] == "{}"
    assert api.obj("ResourceQuota", "c28-fixture-quota", "demo")["spec"]["hard"]["pods"] == "64"
    ops = [st["op"] for st in X.last(X.receipts(record, "session"), "session-stopped")["data"]["steps"]]
    assert ops.index("delete") < ops.index("clear-faults") < ops.index("recommend") < ops.index("restore-quota")


def test_late_release_or_start_is_classified_and_stopped(world):
    """Codex #32 item 8: the predeclared lateness limit (120 s in the lock)."""
    record, target = confirmation_world(world)
    c, digest, started = start_session(world, record, now=BLOCK_T - 120)
    c.clock.t = BLOCK_T + 300
    out = S.watch(c, digest, fetch=steal(c.clock), max_checks=1)
    assert out["ended"] == "stopped" and "release window passed" in out["stops"][0]
    assert world["api"].obj("Job", "c28-gen-c-0001", "demo") is None
    assert X.last(X.receipts(record, "session"), "block-classified")["data"]["classification"] == \
        "late-replacement-slot"


def test_generator_starting_late_is_classified(world):
    record, target = confirmation_world(world)
    api = world["api"]
    c, digest, started = start_session(world, record, now=BLOCK_T - 60)
    c.clock.t = BLOCK_T + 1
    S.watch(c, digest, fetch=steal(c.clock), max_checks=1)                  # released
    generator_pod(api, "c-0001", K6_RUNNING)
    c.clock.t = BLOCK_T + 200
    out = S.watch(c, digest, fetch=observe(c.clock, arrivals_at=BLOCK_T + 178), max_checks=1)
    assert out["ended"] in ("stopped", "unresolved") and "180 s after the block start" in out["stops"][0]
    assert X.last(X.receipts(record, "session"), "block-classified")["data"]["lateness_s"] == 180


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


def test_itt_fallback_only_after_the_qualification_deadline(world):
    """Codex #32 item 7: the lock's qualification deadline is 2026-12-15; the fallback must be declared."""
    late_block, late_t = "2027-01-20T00:00:00Z", 1_800_403_200
    record, target = confirmation_world(world, block=BLOCK, itt_fallback=True)
    pa = world["api"].obj("PredictiveAutoscaler", target, "demo")
    pa["status"]["conditions"] = [{"type": "PredictionReady", "status": "False", "reason": "HistoryInsufficient"}]
    pa["status"]["prediction"]["eligible"] = False
    assert "only allowed after the qualification deadline" in problems(world, record, BLOCK_T - 60)
    record2, _ = confirmation_world(world, block=late_block, itt_fallback=True, id="c-0002",
                                    deadline_utc="2027-01-21T00:30:00Z", sign_receipts=False)
    pa = world["api"].obj("PredictiveAutoscaler", target, "demo")
    pa["status"]["conditions"] = [{"type": "PredictionReady", "status": "False", "reason": "HistoryInsufficient"}]
    pa["status"]["prediction"]["eligible"] = False
    p = S.plan_start(ctx(world, record2, late_t - 60))
    assert p["problems"] == [] and p["readiness"] == "itt-fallback"
    record3, _ = confirmation_world(world, block=late_block, id="c-0003", deadline_utc="2027-01-21T00:30:00Z",
                                    sign_receipts=False)
    pa = world["api"].obj("PredictiveAutoscaler", target, "demo")
    pa["status"]["prediction"]["eligible"] = False
    assert "eligible is not true" in problems(world, record3, late_t - 60)      # not declared: still required


def test_confirmation_checks_against_the_signed_lock(world):
    """Codex #32 item 1: params changed after locking, or a generator image other than the lock's."""
    record, target = confirmation_world(world, generator_image="grafana/k6@sha256:" + "f" * 64)
    assert "the generator image is not the signed run-lock's" in problems(world, record, BLOCK_T - 60)
    import part2_fixtures
    params = part2_fixtures.base_params()
    params["hpa_cpu_target_percent"] = 70
    record, target = confirmation_world(world, arm="H-CPU", sign_receipts=False, params=params)
    assert "params changed after locking" in problems(world, record, BLOCK_T - 60)


def test_quota_intervention_stays_not_ready_without_a_signed_plan(world):
    record, target = confirmation_world(world, sign_receipts=False)
    for who in ("alice", "bob"):                     # receipts that do not attest the quota plan
        body = freeze.receipt_body(record, who, now=lambda: "2026-11-22T00:00:00Z")
        body["prerequisites"] = {}
        path = world["tmp"] / "receipts" / f"{who}.json"
        path.write_text(json.dumps(body, sort_keys=True))
        sign(world["keys"][who], freeze.RECEIPT_NS, path)
    assert "quota intervention is NOT READY" in problems(world, record, BLOCK_T - 60)


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



def test_validation_happens_under_the_lock(world, monkeypatch):
    """Codex #32 r2 item 1: a start validated, then overtaken by another start before taking the lock, must refuse."""
    record_a = session_record(world)
    record_b = session_record(world, id="s-0002", arm="H-RPS",
                              plan=write_plan(world, "ordinary", 102, "c28-h-rps-s20", name="p2.json"))
    ca, cb = ctx(world, record_a), ctx(world, record_b)
    pa, da = review(record_a, S.plan_start(ca), "session", "start-a")
    pb, db = review(record_b, S.plan_start(cb), "session", "start-b")     # both validated while nothing runs
    real, raced = X.mutating, []

    def racing(record, kube, helper, context=None):
        m = real(record, kube, helper, context)
        if (context or {}).get("session") == "s-0002" and not raced:
            raced.append(True)
            S.start(ca, pa, da)                     # A runs between B's validation and B's lock
        return m
    monkeypatch.setattr(X, "mutating", racing)
    with pytest.raises(X.StagingError, match="changed since review"):
        S.start(cb, pb, db)
    assert world["api"].obj("Job", "c28-gen-s-0002", "demo") is None
    assert world["api"].obj("Job", "c28-gen-s-0001", "demo") is not None


def test_a_later_success_never_clears_an_earlier_unknown_outcome(world):
    """Codex #32 r2 item 2: a lost Job-delete response, then a successful HPA delete: the lock stays."""
    record = session_record(world)
    api = world["api"]
    c, digest, started = start_session(world, record)
    real = api.delete

    def lost_job_delete(path, uid, resource_version=None):
        out = real(path, uid, resource_version)
        if "/jobs/" in path:
            raise C.ApiError("delete: failed (timeout)", 504)
        return out
    api.delete = lost_job_delete
    out = stop_session(c)
    api.delete = real
    assert api.obj("HorizontalPodAutoscaler", "c28-h-cpu-s20", "demo") is None        # the later step succeeded
    assert out["data"]["resolved"] is False
    assert X.open_intents(X.receipts(record, "session")) and os.path.exists(lock_path(record))


def test_shutdown_needs_the_operator_to_observe_the_stop_generation(world):
    """Codex #32 r2 item 3: Recommend reported for an older generation is not a confirmation."""
    record, target = confirmation_world(world)
    api = world["api"]
    c, digest, started = start_session(world, record, now=BLOCK_T - 120)
    api.operator_paused = True
    pa = api.obj("PredictiveAutoscaler", target, "demo")
    pa["status"]["mode"] = "Recommend"                       # stale: observed before our stop
    pa["status"]["observedGeneration"] = pa["metadata"]["generation"] - 1
    out = stop_session(c)
    assert out["data"]["resolved"] is False and "for generation" in out["data"]["unresolved"][0]
    api.operator_paused = False
    assert stop_session(c)["data"]["resolved"] is True


def test_a_running_sidecar_or_container_start_is_not_an_arrival(world):
    """Codex #32 r2 item 4: a running sidecar with a waiting k6, or a pod of another Job, never classifies on-time;
    only the ingress-acknowledged first arrival does."""
    record, target = confirmation_world(world)
    api = world["api"]
    c, digest, started = start_session(world, record, now=BLOCK_T - 60)
    c.clock.t = BLOCK_T + 1
    S.watch(c, digest, fetch=observe(c.clock), max_checks=1)                       # released
    generator_pod(api, "c-0001", [{"name": "sidecar", "state": {"running": {"startedAt": "2026-12-10T00:00:02Z"}}},
                                  {"name": "k6", "state": {"waiting": {"reason": "ContainerCreating"}}}])
    generator_pod(api, "c-0001", K6_RUNNING, job_uid=F.new_uid(), name="impostor")
    c.clock.t = BLOCK_T + 60
    assert S.watch(c, digest, fetch=observe(c.clock), max_checks=1)["ended"] == "check budget exhausted"
    assert X.last(X.receipts(record, "session"), "block-classified") is None
    c.clock.t = BLOCK_T + 130
    out = S.watch(c, digest, fetch=observe(c.clock), max_checks=1)
    assert out["ended"] in ("stopped", "unresolved") and "no arrival acknowledged" in out["stops"][0]
    cls = X.last(X.receipts(record, "session"), "block-classified")["data"]
    assert cls["classification"] == "late-replacement-slot" and cls["k6_container"] == "waiting"


def test_pending_owner_moves_block_start_and_stop_sessions(world):
    """Codex #32 r2 item 5: every disallowed occupant class, movable-pending-owner-move included."""
    api = world["api"]
    extra = dict(world["extra"])
    extra["reserve"] = dict(extra["reserve"], movable=[{"namespace": "argocd", "kind": "Deployment",
                                                        "name": "argocd-repo", "uid": "u-argo"}])
    world["extra"] = extra
    api.put({"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": {
        "name": "argocd-repo-1", "namespace": "argocd",
        "ownerReferences": [{"kind": "Deployment", "name": "argocd-repo", "controller": True, "uid": "u-argo"}]}})
    api.put(F.pod("argocd-repo-1-x", "argocd", "worker-w", owner=("ReplicaSet", "argocd-repo-1")))
    record = session_record(world)
    assert "movable-pending-owner-move" in problems(world, record)
    del api.objects[("pods", "argocd", "argocd-repo-1-x")]
    c, digest, started = start_session(world, record)
    api.put(F.pod("argocd-repo-1-y", "argocd", "worker-s", owner=("ReplicaSet", "argocd-repo-1")))
    out = S.watch(c, digest, fetch=observe(c.clock), max_checks=1)
    assert out["ended"] == "stopped" and any("movable-pending-owner-move" in s for s in out["stops"])
