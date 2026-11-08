import copy
import json
import os

import pytest

import common_records as C
import execlib as X
import fake2
import calibrate as K
import cal_world as CW
import part2_fixtures as PF
import reserve
from part2_fixtures import W_UID, api, make_record  # noqa: F401


@pytest.fixture
def cal(tmp_path, make_record, api):
    def _make(contract_over=None, nodes_alloc=("4", "8Gi"), images_over=None):
        record, con = CW.setup(tmp_path, make_record, api, contract_over, images_over, nodes_alloc)
        return CW.context(record, api)
    return _make


def run(ctx):
    plan, digest = CW.reviewed(ctx)
    assert plan["problems"] == [], plan["problems"]
    return K.apply(ctx, plan, digest)


def cert(ctx):
    return json.load(open(os.path.join(ctx.dir, "certificate.json")))


def events(ctx):
    return [e["event"] for e in ctx.entries()]


def lock_held(ctx):
    return os.path.exists(PF.lock_file(ctx.record))


def live_calibration_objects(api, cal_id):
    return sorted((k, n) for (k, _, n), o in api.objects.items()
                  if (o.get("metadata", {}).get("labels") or {}).get(K.CAL_LABEL) == cal_id)


# --- the full calibration and the offline reducer ---------------------------------------------------------------------

def test_happy_path_is_ready_reproducible_and_clean(cal, api):
    ctx = cal()
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "READY", c["reasons"]
    assert c["work_selection"]["selected"] == "w0" and c["work_selection"]["work_iterations"] == 600
    assert [m["qualifies"] for m in c["work_selection"]["measurements"]] == [True, False]
    assert {n: v["outcome"] for n, v in c["per_replica_count"].items()} == {"1": "calibrated", "2": "calibrated",
                                                                           "4": "calibrated"}
    q = {n: v["q_per_min"] for n, v in c["per_replica_count"].items()}
    want = min(q[str(n)] / (60 * n) for n in (1, 2, 4))
    assert abs(c["c0_rps"] - want) < 1e-6 and c["c0_rps"] <= 20.0
    assert c["c_rps"] == X.import_harness().render.capacity_rps(c["c0_rps"])
    assert all(v >= 0.9 for v in c["linearity"]["by_repetition"].values())
    assert c["ceiling"]["delivery"] == "uncensored" and c["ceiling"]["rate_per_min"] >= 1.2 * q["4"]
    assert c["classification"]["measured_failures"] > 0          # unsuccessful trials are kept
    assert live_calibration_objects(api, ctx.cal) == []          # everything owned was deleted
    assert not lock_held(ctx)
    again = K.reduce_bundle(ctx.dir)
    assert again["reproduced"] and again["c0_rps"] == c["c0_rps"] and again["verdict"] == "READY"
    assert K.status(ctx)["state"] == "closed"
    # the plan never exceeded its bounds and every Job had a finite deadline
    jobs = [json.load(open(os.path.join(ctx.dir, "trials", t, "job.json"))) for t in os.listdir(
        os.path.join(ctx.dir, "trials"))]
    assert all(j["spec"]["activeDeadlineSeconds"] > 0 and j["spec"]["backoffLimit"] == 0 for j in jobs)
    # an ID is used once
    assert any("used once" in p for p in K.plan(ctx)["problems"])


def test_reducer_refuses_tampered_or_foreign_bundles(cal, tmp_path):
    ctx = cal()
    run(ctx)
    good = ctx.dir
    # 1. a raw observation edited after the fact
    b1 = str(tmp_path / "b1")
    import shutil
    shutil.copytree(good, b1)
    obs = os.path.join(b1, "trials", "t003", "observations.json")
    doc = json.load(open(obs))
    doc["ingress_after"]["requests"]["response"]["data"]["result"][0]["values"][-1][1] = "1"
    open(obs, "w").write(json.dumps(doc))
    with pytest.raises(X.StagingError, match="sha256 differs from the manifest"):
        K.reduce_bundle(b1)
    # 2. an edited certificate (re-indexed): the journal's closed receipt binds the original
    b2 = str(tmp_path / "b2")
    shutil.copytree(good, b2)
    cp = os.path.join(b2, "certificate.json")
    c = json.load(open(cp))
    c["c0_rps"] = 99.0
    open(cp, "w").write(json.dumps(c))
    idx = json.load(open(os.path.join(b2, "bundle-index.json")))
    idx["certificate_sha256"] = C.sha256_file(cp)
    os.chmod(os.path.join(b2, "bundle-index.json"), 0o640)
    open(os.path.join(b2, "bundle-index.json"), "w").write(json.dumps(idx))
    with pytest.raises(X.StagingError, match="does not bind this certificate"):
        K.reduce_bundle(b2)
    # 3. another candidate's contract: a different calibration, never combined
    b3 = str(tmp_path / "b3")
    shutil.copytree(good, b3)
    cpath = os.path.join(b3, "contract.json")
    con = json.load(open(cpath))
    con["candidate"]["max_queue"] = 64
    open(cpath, "w").write(json.dumps(con))
    m = json.load(open(os.path.join(b3, "manifest.json")))
    m["files"]["contract.json"] = C.sha256_file(cpath)
    open(os.path.join(b3, "manifest.json"), "w").write(json.dumps(m))
    idx = json.load(open(os.path.join(b3, "bundle-index.json")))
    idx["manifest_sha256"] = C.sha256_file(os.path.join(b3, "manifest.json"))
    open(os.path.join(b3, "bundle-index.json"), "w").write(json.dumps(idx))
    with pytest.raises(X.StagingError):
        K.reduce_bundle(b3)


def test_recomputation_detects_a_wrong_verdict(cal):
    """evaluate_trial is the only judge: the recorded result must be what the raw files give."""
    ctx = cal()
    run(ctx)
    base = os.path.join(ctx.dir, "trials", "t003")
    raw = {"trial": json.load(open(os.path.join(base, "result.json")))}
    begin = [e["data"] for e in ctx.entries() if e["event"] == "trial-begin" and e["data"]["trial"]["id"] == "t003"]
    raw = {"trial": begin[0]["trial"], "generator_pod": json.load(open(os.path.join(base, "generator-pod.json"))),
           "pods_begin": json.load(open(os.path.join(base, "pods-begin.json"))),
           "pods_end": json.load(open(os.path.join(base, "pods-end.json"))),
           "observations": json.load(open(os.path.join(base, "observations.json")))}
    assert K.evaluate_trial(ctx.con, raw) == json.load(open(os.path.join(base, "result.json")))
    assert K.evaluate_trial(ctx.con, raw)["verdict"] == "PASS"
    # the exact queue rule: an end backlog above the per-replica cap fails; bounded growth passes
    bad = copy.deepcopy(raw)
    for v in bad["observations"]["fixture_queue"]["response"]["data"]["result"][0]["values"]:
        v[1] = "4.5"
    r = K.evaluate_trial(ctx.con, bad)
    assert r["verdict"] == "FAIL" and r["criteria"]["queue_growth"]["end_over_cap"]
    grow = copy.deepcopy(raw)
    ws = r["criteria"]["window"]["start"]
    for v in grow["observations"]["fixture_queue"]["response"]["data"]["result"][0]["values"]:
        v[1] = str(min(3.9, (v[0] - ws) / 60 * 0.25))          # +0.25/min: slope > 0.2, increase > 2 -> sustained
    r = K.evaluate_trial(ctx.con, grow)
    assert r["verdict"] == "FAIL" and r["criteria"]["queue_growth"]["sustained"]
    small = copy.deepcopy(raw)
    for v in small["observations"]["fixture_queue"]["response"]["data"]["result"][0]["values"]:
        v[1] = str((v[0] - ws) / 60 * 0.1)                      # slow growth under both bounds
    assert K.evaluate_trial(ctx.con, small)["verdict"] == "PASS"
    # missing and stale observations are unknown, never zero
    miss = copy.deepcopy(raw)
    miss["observations"]["fixture_cpu_cores"] = {"error": "URLError"}
    assert K.evaluate_trial(ctx.con, miss)["verdict"] == "INCOMPLETE"
    stale = copy.deepcopy(raw)
    stale["observations"]["fixture_queue"]["response"]["data"]["result"][0]["values"] = \
        stale["observations"]["fixture_queue"]["response"]["data"]["result"][0]["values"][:10]
    assert "stale or missing" in " ".join(K.evaluate_trial(ctx.con, stale)["incomplete"])
    # a lost outcome is unknown: it can only bound the failure fraction
    lost = copy.deepcopy(raw)
    term = lost["generator_pod"]["status"]["containerStatuses"][0]["state"]["terminated"]
    summ = json.loads(term["message"])
    summ["scenarios"]["measure"]["http_reqs"]["count"] -= 50
    term["message"] = json.dumps(summ)
    r = K.evaluate_trial(ctx.con, lost)
    assert r["verdict"] == "INCOMPLETE" and r["criteria"]["failure_fraction"]["unknown_outcomes"] == 50
    # the wrong generator image
    img = copy.deepcopy(raw)
    img["generator_pod"]["status"]["containerStatuses"][0]["imageID"] = "grafana/k6@sha256:" + "f" * 64
    assert "image digest" in " ".join(K.evaluate_trial(ctx.con, img)["incomplete"])


def test_unaccounted_arrivals_make_the_trial_incomplete(cal):
    ctx = cal()
    ctx.world.extra_arrivals = 50                     # someone else sends traffic through the calibration route
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "NOT READY" and c["halted"]["category"] == "incomplete"
    first = json.load(open(os.path.join(ctx.dir, "trials", "t001", "result.json")))
    assert any("unaccounted arrivals" in w for w in first["incomplete"])
    assert c["trials"] == 1                           # never bisected across missing evidence
    assert K.reduce_bundle(ctx.dir)["verdict"] == "NOT READY"


def test_generator_drops_are_a_limit_not_a_capacity(cal):
    ctx = cal()
    ctx.world.drops = 30
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "NOT READY" and c["halted"]["category"] == "limit"
    assert c["c0_rps"] is None and c["classification"]["limit"] == 1


def test_stale_observations_make_the_trial_incomplete(cal):
    ctx = cal()
    ctx.world.truncate_windows = True                 # the window's later samples never arrive
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "NOT READY" and c["halted"]["category"] == "incomplete" and c["trials"] == 1
    r = json.load(open(os.path.join(ctx.dir, "trials", "t001", "result.json")))
    assert r["verdict"] == "INCOMPLETE" and any("stale or missing" in w for w in r["incomplete"])
    assert live_calibration_objects(ctx.world.api, ctx.cal) == []


def test_stale_observer_mid_trial_stops_it(cal, api):
    ctx = cal()

    def go_stale(world, api_):
        if world.stale_after is None and any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            world.stale_after = world.clock.now()
    ctx.world.hooks.append(go_stale)
    out = run(ctx)
    c = cert(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and c["halted"]["category"] == "incomplete"
    assert stopped and stopped[0]["category"] == "incomplete" and "observer" in stopped[0]["reason"]
    assert live_calibration_objects(api, ctx.cal) == []


def test_missing_backlog_metric_is_incomplete(cal):
    ctx = cal()
    ctx.world.missing = {"backlog"}
    out = run(ctx)
    assert out["verdict"] == "NOT READY" and cert(ctx)["halted"]["category"] == "incomplete"


def test_neither_work_value_qualifies_stops_for_a_revised_candidate(cal):
    ctx = cal()
    ctx.world.cost_ms = {600: 1.0, 900: 5.0}          # both outside [2.5, 3.0]
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "NOT READY" and c["halted"]["category"] == "work-selection"
    assert c["work_selection"]["selected"] is None and len(c["work_selection"]["measurements"]) == 2
    assert all(cv["kind"] == "selection" for cv in c["curves"])        # no informal search
    assert K.reduce_bundle(ctx.dir)["verdict"] == "NOT READY"


def test_tie_break_is_closest_to_target_then_fewer_iterations():
    ws = {"work_selection": {"values": [600, 900], "rate_rps": 8, "idle_s": 120, "settle_s": 60, "measure_s": 300,
                             "cpu_ms_per_request": {"min": 2.0, "target": 2.75, "max": 3.5},
                             "tie_break": K.TIE_BREAK}}

    def res(v, cost, verdict="PASS"):
        return {"variant": v, "trial": v, "verdict": verdict, "criteria": {"work_cost": {"cpu_ms_per_request": cost}}}
    assert K.select_work(ws, [res("w0", 2.5), res("w1", 3.0)])["selected"] == "w0"        # equal distance
    assert K.select_work(ws, [res("w0", 2.2), res("w1", 3.0)])["selected"] == "w1"        # closer
    assert K.select_work(ws, [res("w0", 2.7, "FAIL"), res("w1", 3.4)])["selected"] == "w1"  # must PASS
    assert K.select_work(ws, [res("w0", 1.0), res("w1", 9.0)])["selected"] is None


def test_linearity_below_090_is_not_ready(cal):
    ctx = cal()
    ctx.world.lin4 = 0.8
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "NOT READY" and any(r.startswith("linearity") for r in c["reasons"])
    assert c["measured_c0_rps"] is not None and c["c0_rps"] is None
    assert all(v < 0.9 for v in c["linearity"]["by_repetition"].values())


def test_normalization_takes_the_minimum_over_counts_and_repetitions():
    con = CW.contract("/nonexistent")
    con["search"].update(rate_min_rps_per_replica=10, rate_max_rps_per_replica=30, initial_rps_per_replica=20,
                         bracket_factor=1.25, resolution=0.05)

    def trials_for(cap):                               # cap: {n: per-replica capacity in rps}
        out, i = [], 0
        for k, wi in enumerate((600, 900)):
            i += 1
            out.append({"trial": f"t{i:03d}", "kind": "selection", "variant": f"w{k}", "n": 1, "rep": 0,
                        "rate_per_min": K.selection_rate(con), "verdict": "PASS",
                        "criteria": {"work_cost": {"cpu_ms_per_request": 2.7 if k == 0 else 4.0}}})
        for n in (1, 2, 4):
            hist = []
            while True:
                st = K.next_step(con, n, hist)
                if st["action"] == "done":
                    break
                ok = st["rate"] / 60 / n <= cap[n]
                vs = []
                for rep in range(3):
                    i += 1
                    v = "PASS" if ok else "FAIL"
                    out.append({"trial": f"t{i:03d}", "kind": "search", "variant": "w0", "n": n, "rep": rep,
                                "rate_per_min": st["rate"], "verdict": v})
                    vs.append(v)
                    if v != "PASS":
                        break
                hist.append({"rate": st["rate"], "verdict": "pass" if ok else "fail"})
        q4 = K.next_step(con, 4, hist)["q"]
        i += 1
        out.append({"trial": f"t{i:03d}", "kind": "ceiling", "variant": "w0", "n": 4, "rep": 0,
                    "rate_per_min": K.ceiling_rate(con, q4), "verdict": "FAIL", "delivery": "uncensored"})
        return out
    s = K.summarize(con, "c28cal-0123456789", "0" * 64, trials_for({1: 20, 2: 19, 4: 19.5}), None)
    q = {n: s["per_replica_count"][str(n)]["q_per_min"] for n in (1, 2, 4)}
    assert s["verdict"] == "READY" and s["c0_rps"] == round(min(q[n] / (60 * n) for n in (1, 2, 4)), 6)
    assert s["c0_rps"] <= 19.0 and s["c_rps"] == X.import_harness().render.capacity_rps(s["c0_rps"])
    assert s["linearity"]["by_repetition"]["0"] == round(q[4] / (4 * q[1]), 4)
    # a replica count that passes at its arrival ceiling is an unresolved lower bound, not a capacity
    s = K.summarize(con, "c28cal-0123456789", "0" * 64, trials_for({1: 20, 2: 50, 4: 19.5})[:-1], None)
    assert s["verdict"] == "NOT READY" and s["per_replica_count"]["2"]["outcome"] == "unresolved-lower-bound"
    assert any("unresolved lower bound" in r for r in s["reasons"])


def test_bracket_exhaustion_both_ways():
    con = CW.contract("/nonexistent")
    rmin, rmax, r0 = K.bounds(con, 1)
    hist = [{"rate": r0, "verdict": "pass"}]
    while True:
        st = K.next_step(con, 1, hist)
        if st["action"] == "done":
            break
        assert rmin <= st["rate"] <= rmax
        hist.append({"rate": st["rate"], "verdict": "pass"})
    assert st["outcome"] == "unresolved-lower-bound" and st["q"] is None and hist[-1]["rate"] == rmax
    hist = [{"rate": r0, "verdict": "fail"}]
    while True:
        st = K.next_step(con, 1, hist)
        if st["action"] == "done":
            break
        assert rmin <= st["rate"] <= rmax
        hist.append({"rate": st["rate"], "verdict": "fail"})
    assert st["outcome"] == "no-qualifying-rate" and hist[-1]["rate"] == rmin
    # a calibrated crossing needs a pass and a fail within the resolution
    st = K.next_step(con, 1, [{"rate": 1000, "verdict": "pass"}, {"rate": 1040, "verdict": "fail"}])
    assert st == {"action": "done", "outcome": "calibrated", "q": 1000, "lo": 1000, "hi": 1040}
    assert K.next_step(con, 1, [{"rate": 1000, "verdict": "pass"}, {"rate": 1100, "verdict": "fail"}])["rate"] == 1050


def test_ceiling_live_unresolved_lower_bound(cal):
    ctx = cal({"search.rate_max_rps_per_replica": 19})       # the fixture still passes at the arrival ceiling
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "NOT READY" and c["per_replica_count"]["1"]["outcome"] == "unresolved-lower-bound"


def test_ceiling_above_the_authorized_maximum_is_not_ready(cal):
    ctx = cal({"search.rate_max_rps_per_replica": 22})       # Q4 found, but 1.2 x Q4 exceeds Rmax
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "NOT READY" and c["halted"]["category"] == "ceiling"


# --- interruption safety ------------------------------------------------------------------------------------------

def _hpa(target):
    return {"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
            "metadata": {"name": "intruder", "namespace": "demo"},
            "spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": target},
                     "minReplicas": 1, "maxReplicas": 4}}


def test_foreign_writer_mid_trial_stops_arrivals_first(cal, api):
    ctx = cal()
    target = ctx.nm["variants"][0]

    def intrude(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects) and \
                ("horizontalpodautoscalers", "demo", "intruder") not in api_.objects:
            api_.put(_hpa(target))
    ctx.world.hooks.append(intrude)
    out = run(ctx)
    c = cert(ctx)
    assert out["verdict"] == "NOT READY" and c["halted"]["category"] == "stopped"
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert stopped and stopped[0]["trial"]["id"] == "t003" and "replica writer" in stopped[0]["reason"]
    ev = events(ctx)
    # after the stop, the trial's Job delete (arrivals) precedes every other delete
    entries = ctx.entries()
    at = next(i for i, e in enumerate(entries) if e["event"] == "trial-stopping")
    deletes = [e["data"] for e in entries[at:] if e["event"].startswith("intent-") and e["data"]["op"] == "delete"]
    assert deletes[0]["kind"] == "Job" and deletes[0]["name"].endswith("t003")
    assert all(d["kind"] != "Job" for d in deletes[1:])
    assert ev.index("trial-stopped") < ev.index("calibration-closed")
    assert live_calibration_objects(api, ctx.cal) == [] and not lock_held(ctx)
    assert K.reduce_bundle(ctx.dir)["verdict"] == "NOT READY"


def test_request_stop_reaches_the_running_executor(cal):
    ctx = cal()

    def ask(world, api_):
        if any(k == "jobs" and n.endswith("t004") for (k, _, n) in api_.objects) and \
                not os.path.exists(os.path.join(ctx.dir, "stop-request.json")):
            K.request_stop(ctx, "operator wants the node back")
    ctx.world.hooks.append(ask)
    out = run(ctx)
    assert out["verdict"] == "NOT READY" and cert(ctx)["halted"]["category"] == "stopped"


class Crash(BaseException):
    pass


def test_crash_mid_trial_is_recorded_interrupted_and_never_resumed(cal, api):
    ctx = cal()
    real_sleep = ctx.clock.sleep

    def sleep(s):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api.objects):
            raise Crash()
        real_sleep(s)
    ctx.sleep = sleep
    with pytest.raises(Crash):
        run(ctx)
    ctx.sleep = real_sleep
    assert K.status(ctx)["state"] == "open"
    assert any("is open" in p for p in K.plan(ctx)["problems"])          # no resumption, no second apply
    sp = K.plan_stop(ctx)
    assert sp["interrupted"] == ["t003"] and sp["actions"][0]["kind"] == "Job"
    assert sp["actions"][1]["op"] == "prove-gone"
    plan, digest = CW.reviewed(ctx, K.plan_stop, K.STOP, "stop-plan")
    out = K.stop(ctx, plan, digest, "executor crashed")
    assert out["outcome"] == "STOPPED" and out["verdict"] == "NOT READY"
    inter = [e["data"] for e in ctx.entries() if e["event"] == "trial-interrupted"]
    assert [d["trial"]["id"] for d in inter] == ["t003"]
    assert live_calibration_objects(api, ctx.cal) == [] and not lock_held(ctx)
    assert K.reduce_bundle(ctx.dir)["reproduced"]


def test_lost_write_response_keeps_the_lock_until_recover(cal, api):
    ctx = cal()
    real = api.create

    def create(path, body):
        out = real(path, body)
        if body["metadata"]["name"].endswith("-w1") and body["kind"] == "Deployment":
            raise C.ApiError("create: failed (timeout)", 504)          # applied, response lost
        return out
    api.create = create
    with pytest.raises(X.StagingError):
        run(ctx)
    api.create = real
    assert lock_held(ctx) and X.open_intents(ctx.entries())
    assert any("unknown outcomes" in p for p in K.plan_stop(ctx)["problems"])
    rec = PF.recover(ctx.record, ctx.kube)
    assert rec["event"] == "lock-recovered" and not lock_held(ctx)
    own = {o["name"]: o for o in K.owned_objects(ctx.entries(), ctx.cal)}
    assert ctx.nm["variants"][1] in own                                 # the lost create is still owned
    plan, digest = CW.reviewed(ctx, K.plan_stop, K.STOP, "stop-plan")
    K.stop(ctx, plan, digest, "after a lost write")
    assert live_calibration_objects(api, ctx.cal) == []


def test_lost_scale_response_is_reconciled(cal, api):
    ctx = cal()
    real = api.replace
    hit = []

    def replace(path, body):
        out = real(path, body)
        if path.endswith("/scale") and not hit:
            hit.append(path)
            raise C.ApiError("replace: failed (timeout)", 504)
        return out
    api.replace = replace
    with pytest.raises(X.StagingError):
        run(ctx)
    api.replace = real
    assert lock_held(ctx)
    rp = X.recover_plan(ctx.record, ctx.kube)
    assert [a["observed"] for a in rp["actions"]] == ["done"]            # exactly one generation step to the target
    PF.recover(ctx.record, ctx.kube)
    plan, digest = CW.reviewed(ctx, K.plan_stop, K.STOP, "stop-plan")
    K.stop(ctx, plan, digest, "after a lost scale response")
    assert not lock_held(ctx)


def test_failed_cleanup_keeps_the_lock(cal, api):
    ctx = cal()
    real = api.delete

    def delete(path, uid, resource_version=None):
        if "/jobs/" in path:                                   # the Job goes, its pod is stuck terminating
            pods = [o for (k, _, _), o in api.objects.items() if k == "pods" and
                    any(r.get("uid") == uid for r in o["metadata"].get("ownerReferences") or [])]
            out = real(path, uid, resource_version)
            for p in pods:
                p["metadata"]["deletionTimestamp"] = "2026-11-30T00:00:00Z"
                api.objects[("pods", "demo", p["metadata"]["name"])] = p
            return out
        return real(path, uid, resource_version)
    api.delete = delete
    with pytest.raises(X.StagingError, match="generator pods are not gone"):
        run(ctx)
    assert lock_held(ctx) and "cleanup-unresolved" in events(ctx)
    assert K.status(ctx)["state"] == "open"
    # the fixture was never touched while a generator pod remained
    deletes = [e["data"] for e in ctx.entries() if e["event"].startswith("intent-") and e["data"]["op"] == "delete"]
    assert {d["kind"] for d in deletes} == {"Job"}


# --- identity, admission, reservation ----------------------------------------------------------------------------

def test_wrong_cluster_and_node_uids_are_refused(cal, api, tmp_path, make_record):
    ctx = cal()
    api.objects[("namespaces", None, "kube-system")]["metadata"]["uid"] = "99999999-2222-4333-8444-555555555555"
    with pytest.raises(X.StagingError, match="recorded cluster"):
        K.plan(ctx)
    api.objects[("namespaces", None, "kube-system")]["metadata"]["uid"] = fake2.CLUSTER
    api.objects[("nodes", None, "worker-w")]["metadata"]["uid"] = "bbbbbbbb-0000-4000-8000-000000000009"
    probs = K.plan(ctx)["problems"]
    assert any("missing or replaced" in p for p in probs) and any("not the recorded" in p for p in probs)
    con = json.load(open(K.record_contract_path(ctx.record)))
    con["nodes"]["workload"]["uid"] = "bbbbbbbb-0000-4000-8000-000000000009"
    con["cluster_uid"] = "99999999-2222-4333-8444-555555555555"
    open(K.record_contract_path(ctx.record), "w").write(json.dumps(con))
    with pytest.raises(X.StagingError) as e:
        K.load_contract(ctx.record, ctx.params)
    assert "another cluster" in str(e.value) and "reserve.nodes.workload" in str(e.value)


def test_contract_validation_refuses_placeholders_and_changed_numbers(cal):
    ctx = cal()
    path = K.record_contract_path(ctx.record)
    good = json.load(open(path))
    cases = [("candidate", "cpu_request", "<calibrated>", "placeholder"),
             ("criteria", "p95_ms", 250, "manifest"), ("search", "settle_s", 120, "manifest"),
             ("observer", "step_s", 1, "scrape cadence"), ("search", "resolution", 0.1, "resolution"),
             ("search", "max_trials", True, "max_trials"), ("criteria", "ceiling_factor", 1.0, "manifest"),
             ("work_selection", "values", [600, 600], "two distinct")]
    for section, key, value, why in cases:
        bad = copy.deepcopy(good)
        bad[section][key] = value
        open(path, "w").write(json.dumps(bad))
        with pytest.raises(X.StagingError, match=why):
            K.load_contract(ctx.record, ctx.params)
    open(path, "w").write(json.dumps(good).replace('"rate_rps": 8', '"rate_rps": NaN'))
    with pytest.raises(X.StagingError, match="non-finite"):
        K.load_contract(ctx.record, ctx.params)
    bad = dict(good, extra=1)
    open(path, "w").write(json.dumps(bad))
    with pytest.raises(X.StagingError, match="unknown contract fields"):
        K.load_contract(ctx.record, ctx.params)
    bad = copy.deepcopy(good)
    bad["images"]["fixture"] = "harbor.example/autoscaler/c28-fixture@sha256:" + "b" * 64
    open(path, "w").write(json.dumps(bad))
    with pytest.raises(X.StagingError, match="images.json record"):
        K.load_contract(ctx.record, ctx.params)


def test_a_changed_candidate_is_a_new_calibration(cal):
    ctx = cal()
    path = K.record_contract_path(ctx.record)
    con = json.load(open(path))
    first = ctx.cal
    con["candidate"]["gomaxprocs"] = 1
    open(path, "w").write(json.dumps(con))
    _, sha, second = K.load_contract(ctx.record, ctx.params)
    assert second != first and second == K.calibration_id(sha)
    # a plan reviewed for one candidate never applies to another
    ctx2 = CW.context(ctx.record, ctx.world.api, ctx.clock)
    con["candidate"]["gomaxprocs"] = 2
    open(path, "w").write(json.dumps(con))
    plan, digest = CW.reviewed(ctx)
    with pytest.raises(X.StagingError):
        K.apply(ctx2, plan, digest)


def test_plan_is_read_only_and_authorizes_bounded_objects(cal, api):
    ctx = cal()
    before = list(api.mutations())
    p = K.plan(ctx)
    assert api.mutations() == before and p["problems"] == []
    kinds = sorted((a["kind"], a["name"]) for a in p["actions"])
    assert kinds == sorted([("Deployment", ctx.nm["variants"][0]), ("Service", ctx.nm["variants"][0]),
                            ("Deployment", ctx.nm["variants"][1]), ("Service", ctx.nm["variants"][1]),
                            ("Deployment", ctx.nm["ingress"]), ("Service", ctx.nm["ingress"]),
                            ("ConfigMap", ctx.nm["script"])])
    assert p["search"]["max_rate_per_min"] == 30 * 4 * 60 and p["work_selection"]["values"] == [600, 900]
    assert p["admission"]["workload"]["footprint"][0] == {"what": "fixture", "pods": 6, "cpu": "50m",
                                                          "memory": "32Mi"}          # per pod, 4 + 2 overlap


def test_admission_refuses_a_footprint_over_75_percent(cal):
    ctx = cal(nodes_alloc=("500m", "8Gi"))
    assert any(p.startswith("admission: workload CPU") for p in K.plan(ctx)["problems"])


def test_rollout_guard_refuses_a_platform_rollout_on_w(cal, api):
    ctx = cal()
    ds = api.put({"apiVersion": "apps/v1", "kind": "DaemonSet",
                  "metadata": {"name": "cilium", "namespace": "kube-system", "generation": 3},
                  "spec": {}, "status": {"observedGeneration": 3, "desiredNumberScheduled": 2,
                                         "updatedNumberScheduled": 1, "numberAvailable": 2}})
    pod = fake2.pod("cilium-w", "kube-system", "worker-w", owner=("DaemonSet", "cilium"))
    pod["metadata"]["ownerReferences"][0]["uid"] = ds["metadata"]["uid"]
    api.put(pod)
    rec = ctx.record.doc
    rec["reserve"]["allowed"]["daemonsets"] = [{"namespace": "kube-system", "name": "cilium"}]
    assert any("mid-rollout" in p for p in K.plan(ctx)["problems"])
    ds["status"]["updatedNumberScheduled"] = 2
    assert not any("mid-rollout" in p for p in K.plan(ctx)["problems"])


def test_reservation_recognizes_only_this_calibrations_verified_objects(cal, api):
    ctx = cal()
    view = {"id": ctx.cal, "namespace": "demo", "workload": [], "support": []}
    rs = api.put({"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": {
        "name": f"{ctx.cal}-w0-abc", "namespace": "demo",
        "ownerReferences": [{"kind": "Deployment", "name": f"{ctx.cal}-w0", "uid": "u-not-ours", "controller": True}]}})
    pod = fake2.pod(f"{ctx.cal}-w0-abc-xyz12", "demo", "worker-w", owner=("ReplicaSet", f"{ctx.cal}-w0-abc"),
                    labels={K.CAL_LABEL: ctx.cal})
    pod["metadata"]["ownerReferences"][0]["uid"] = rs["metadata"]["uid"]
    api.put(pod)
    # a calibration-named, calibration-labelled pod that this journal does not own is an unplanned occupant
    out = reserve.verify(ctx.record, ctx.kube, calibration=view)
    assert out["reservation"] == "FAIL" and any("unplanned" in p for p in out["reservation_problems"])
    view["workload"] = ["u-not-ours"]                      # owned Deployment UID: now verified through the RS chain
    out = reserve.verify(ctx.record, ctx.kube, calibration=view)
    assert out["reservation"] == "PASS", out["reservation_problems"]
    assert {e["class"] for e in out["occupants"]["workload"]} == {"calibration"}
    rs["metadata"]["uid"] = "rs-replaced"                  # the ReplicaSet UID must be exactly the referenced one
    assert reserve.verify(ctx.record, ctx.kube, calibration=view)["reservation"] == "FAIL"
    rs["metadata"]["uid"] = pod["metadata"]["ownerReferences"][0]["uid"]
    assert reserve.verify(ctx.record, ctx.kube)["reservation"] == "FAIL"       # no calibration context: unplanned
    other = dict(view, id="c28cal-ffffffffff")
    assert reserve.verify(ctx.record, ctx.kube, calibration=other)["reservation"] == "FAIL"


def test_fixture_keeps_the_arms_container_shape_and_is_sidecar_free(cal):
    ctx = cal()
    R = X.import_harness().render
    arm = next(o for o in R.render_arm(PF.base_params(), "H-RPS", "s20") if o["kind"] == "Deployment")
    fx = K.render_fixture(ctx.con, ctx.cal, ctx.params, 0)[0]
    a, f = arm["spec"]["template"], fx["spec"]["template"]
    ac, fc = a["spec"]["containers"][0], f["spec"]["containers"][0]
    assert set(fc) == set(ac) - {"volumeMounts"}
    for k in ("readinessProbe", "livenessProbe", "ports", "securityContext"):
        assert fc[k] == ac[k]
    assert [e["name"] for e in fc["env"]] == [e["name"] for e in ac["env"] if e["name"] != "FAULT_FILE"] + \
        ["GOMAXPROCS"]
    assert {e["name"]: e["value"] for e in fc["env"]}["GOMAXPROCS"] == "2"
    assert fc["resources"] == {"requests": {"cpu": "50m", "memory": "32Mi"}, "limits": {"cpu": "50m",
                                                                                         "memory": "32Mi"}}
    assert f["spec"]["tolerations"] == a["spec"]["tolerations"] and f["spec"]["affinity"] == a["spec"]["affinity"]
    assert "priorityClassName" not in f["spec"] and X.RUN_LABEL not in f["metadata"]["labels"]
    objs = K.render_objects(ctx.con, ctx.cal, ctx.params, "script")
    R.audit_sidecar_free(objs)
    job = K.generator_job(ctx.con, ctx.cal, ctx.params, K.trial_meta(ctx, 7, "search", "w0", 2, 1200, 1))
    R.audit_sidecar_free([job])
    assert job["spec"]["activeDeadlineSeconds"] == 300 + 900 + 120 and X.RUN_LABEL not in job["metadata"]["labels"]
    ing = next(o for o in objs if o["metadata"]["name"] == ctx.nm["ingress"])
    ups = {e["name"]: e["value"] for e in ing["spec"]["template"]["spec"]["containers"][0]["env"]}["UPSTREAMS"]
    assert ups == ",".join(f"{v}=http://{v}.demo.svc" for v in ctx.nm["variants"])
    assert not set(ctx.nm["variants"]) & {R.target_name(a_, p_) for a_ in X.import_harness().frozen.ARMS
                                           for p_ in X.import_harness().frozen.PROFILES}


def test_k6_script_matches_the_job_interface():
    script = open(K.LOAD_SCRIPT).read()
    for name in ("C28CAL_TRIAL", "C28CAL_TARGET", "C28CAL_BASE_URL", "C28CAL_RATE_PER_MIN", "C28CAL_SETTLE_S",
                 "C28CAL_MEASURE_S", "C28CAL_SEED", "C28CAL_PREALLOCATED_VUS", "C28CAL_MAX_VUS", "C28CAL_TIMEOUT_S"):
        assert f"'{name}'" in script or f"__ENV.{name}" in script
    assert "constant-arrival-rate" in script and "/dev/termination-log" in script and K.K6_SCHEMA in script


def test_cli_plan_and_reduce(cal, api, capsys):
    ctx = cal()
    assert K.main(["--record", ctx.record.path, "plan"], api=api, fetch=ctx.world.fetch, now=ctx.clock.now,
                  sleep=ctx.clock.sleep) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["problems"] == [] and os.path.exists(out["plan"])
    assert K.main(["--record", ctx.record.path, "apply", "--plan", out["plan"], "--plan-digest", out["plan_digest"]],
                  api=api, fetch=ctx.world.fetch, now=ctx.clock.now, sleep=ctx.clock.sleep) == 0
    capsys.readouterr()
    assert K.main(["reduce", "--bundle", ctx.dir]) == 0
    assert json.loads(capsys.readouterr().out)["verdict"] == "READY"
    assert K.main(["--record", ctx.record.path, "apply", "--plan", out["plan"]], api=api) == 1


def test_verify_open_and_closed(cal, api):
    ctx = cal()
    assert K.verify(ctx)["state"] == "not-begun"
    real_sleep = ctx.clock.sleep

    def sleep(s):
        if any(k == "jobs" and n.endswith("t002") for (k, _, n) in api.objects):
            raise Crash()
        real_sleep(s)
    ctx.sleep = sleep
    with pytest.raises(Crash):
        run(ctx)
    ctx.sleep = real_sleep
    v = K.verify(ctx)
    assert v["state"] == "open" and v["result"] == "OK", v["stops"]
    api.put(_hpa(ctx.nm["variants"][1]))
    v = K.verify(ctx)
    assert v["result"] == "STOP" and any("replica writer" in s for s in v["stops"])
    api.objects.pop(("horizontalpodautoscalers", "demo", "intruder"))
    plan, digest = CW.reviewed(ctx, K.plan_stop, K.STOP, "stop-plan")
    K.stop(ctx, plan, digest)
    v = K.verify(ctx)
    assert v["state"] == "closed" and v["bundle"]["reproduced"]


# --- Codex review of 0657f2b: one regression test per finding --------------------------------------------------------

def _raw(ctx, trial):
    base = os.path.join(ctx.dir, "trials", trial)
    begin = [e["data"] for e in ctx.entries() if e["event"] == "trial-begin" and e["data"]["trial"]["id"] == trial]
    return {"trial": begin[0]["trial"], "generator_pod": json.load(open(os.path.join(base, "generator-pod.json"))),
            "pods_begin": json.load(open(os.path.join(base, "pods-begin.json"))),
            "pods_end": json.load(open(os.path.join(base, "pods-end.json"))),
            "observations": json.load(open(os.path.join(base, "observations.json")))}


def _summary(raw):
    term = raw["generator_pod"]["status"]["containerStatuses"][0]["state"]["terminated"]
    return term, json.loads(term["message"])


def test_f1_fixture_changed_before_its_first_scale_is_refused(cal, api):
    """Codex repro: the fixture CPU limit changed 50m -> 500m before its first scale still produced READY."""
    ctx = cal()

    def tamper(world, api_):
        dep = api_.objects.get(("deployments", "demo", ctx.nm["variants"][0]))
        if dep and not world.__dict__.get("tampered"):
            dep["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["cpu"] = "500m"
            world.tampered = True
    ctx.world.hooks.append(tamper)
    with pytest.raises(X.StagingError, match="differs from its reviewed rendering"):
        run(ctx)
    c = cert(ctx)
    assert c["verdict"] == "NOT READY" and c["trials"] == 0
    assert api.objects.get(("deployments", "demo", ctx.nm["variants"][0])) is None     # cleaned up, never scaled
    scales = [e for e in ctx.entries() if e["event"] == "scale-write"]
    assert scales == []


def test_f1_admitted_pod_changed_mid_measurement_stops_the_trial(cal, api):
    ctx = cal()

    def tamper(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects) and not world.__dict__.get("t"):
            pod = next(o for (k, _, n), o in api_.objects.items() if k == "pods" and
                       n.startswith(ctx.nm["variants"][0] + "-"))
            for e in pod["spec"]["containers"][0]["env"]:
                if e["name"] == "WORK_ITERATIONS":
                    e["value"] = "300"
            world.t = True
    ctx.world.hooks.append(tamper)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and stopped[0]["trial"]["id"] == "t003"
    assert "differs from its reviewed template" in stopped[0]["reason"]


def test_f1_job_changed_mid_measurement_stops_the_trial(cal, api):
    ctx = cal()

    def tamper(world, api_):
        job = next((o for (k, _, n), o in api_.objects.items() if k == "jobs" and n.endswith("t003")), None)
        if job and not world.__dict__.get("t"):
            job["spec"]["template"]["spec"]["containers"][0]["args"][3] = "C28CAL_TARGET=elsewhere"
            world.t = True
    ctx.world.hooks.append(tamper)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and "differs from its reviewed rendering" in stopped[0]["reason"]


def test_f2_writer_appearing_after_the_scale_intent_is_not_submitted(cal, api, monkeypatch):
    """Codex repro: an HPA inserted after the intent; the fixture still went 0 -> 1."""
    ctx = cal()
    real = C.append_receipt

    def append(rec, helper, event, data, *a, **kw):
        out = real(rec, helper, event, data, *a, **kw)
        if helper == K.HELPER and event.startswith("intent-") and data.get("op") == "calibrate-scale" and \
                ("horizontalpodautoscalers", "demo", "intruder") not in api.objects:
            api.put(_hpa(ctx.nm["variants"][0]))
        return out
    monkeypatch.setattr(C, "append_receipt", append)
    with pytest.raises(X.StagingError, match="pre-write check refused"):
        run(ctx)
    monkeypatch.setattr(C, "append_receipt", real)
    dep = api.objects[("deployments", "demo", ctx.nm["variants"][0])]
    assert (dep["spec"].get("replicas") or 0) == 0                         # nothing was written
    writes = [e["data"]["result"] for e in ctx.entries() if e["event"] == "scale-write"]
    assert writes == ["not-submitted"]
    assert lock_held(ctx)                                                   # the step's outcome is for recover
    api.objects.pop(("horizontalpodautoscalers", "demo", "intruder"))
    rp = X.recover_plan(ctx.record, ctx.kube)
    assert [(a["observed"], a.get("state")) for a in rp["actions"]] == [("not-done", "not-applied")]
    PF.recover(ctx.record, ctx.kube)
    plan, digest = CW.reviewed(ctx, K.plan_stop, K.STOP, "stop-plan")
    K.stop(ctx, plan, digest, "writer appeared")
    assert not lock_held(ctx)


def test_f3_steal_stop_during_a_trial(cal, api):
    ctx = cal()

    def steal(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            world.steal["worker-s"] = 9.0
    ctx.world.hooks.append(steal)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and "guest steal above 5%" in stopped[0]["reason"]


def test_f3_missing_steal_observation_refuses_the_plan(cal):
    ctx = cal()
    ctx.world.missing = {"steal"}
    assert any("required observation missing" in p and "worker-w" in p and "worker-s" in p
               for p in K.plan(ctx)["problems"])
    ctx.world.missing = set()
    ctx.world.steal = {"worker-w": 6.0}
    assert any("guest steal above 5%" in p and "on worker-w" in p for p in K.plan(ctx)["problems"])
    ctx.world.steal = {"worker-w": 5.0}                     # at the bar, not above it
    assert not any("steal" in p for p in K.plan(ctx)["problems"])


def test_f3_completion_is_accepted_only_after_a_final_gate(cal, api):
    """A writer appearing just as the Job completes: the trial must not be accepted."""
    ctx = cal()

    def at_completion(world, api_):
        job = next((o for (k, _, n), o in api_.objects.items() if k == "jobs" and n.endswith("t003")), None)
        if job and job.get("status", {}).get("conditions") and \
                ("horizontalpodautoscalers", "demo", "intruder") not in api_.objects:
            api_.put(_hpa(ctx.nm["variants"][0]))
    ctx.world.hooks.append(at_completion)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and stopped[0]["reason"].startswith("after completion")
    assert not os.path.exists(os.path.join(ctx.dir, "trials", "t003", "result.json"))


def test_f3_steal_inside_the_measurement_window_is_evidence(cal):
    ctx = cal()
    run(ctx)
    raw = _raw(ctx, "t003")
    assert K.evaluate_trial(ctx.con, raw)["criteria"]["steal"]["workload"]["longest_run_over"] == 0
    bad = copy.deepcopy(raw)
    vals = next(r for r in bad["observations"]["steal"]["response"]["data"]["result"]
                if r["metric"]["node"] == "worker-w")["values"]
    for v in vals[3:8]:
        v[1] = "7.5"
    r = K.evaluate_trial(ctx.con, bad)
    assert r["verdict"] == "INCOMPLETE" and any("interference: guest steal" in w for w in r["incomplete"])
    gap = copy.deepcopy(raw)
    res = gap["observations"]["steal"]["response"]["data"]["result"]
    next(x for x in res if x["metric"]["node"] == "worker-s")["values"].pop(6)
    assert any("required observation missing" in w for w in K.evaluate_trial(ctx.con, gap)["incomplete"])


def test_f4_window_comes_from_the_actual_scenario_start(cal):
    ctx = cal()
    ctx.world.init_delay = 40                    # k6 initialization/VU allocation delays the scenarios
    out = run(ctx)
    assert out["verdict"] == "READY"
    raw = _raw(ctx, "t003")
    term, summ = _summary(raw)
    start = summ["scenarios"]["measure"]["start_ms"]["min"] // 1000
    assert start == K._utc(term["startedAt"]) + 300 + 40
    r = json.load(open(os.path.join(ctx.dir, "trials", "t003", "result.json")))
    assert r["criteria"]["window"] == {"start": start, "end": start + 900}
    assert raw["observations"]["fixture_queue"]["start"] == start
    gone = copy.deepcopy(raw)
    t, sm = _summary(gone)
    sm["scenarios"]["measure"]["start_ms"] = None
    t["message"] = json.dumps(sm)
    assert any("no actual measure-scenario start" in w for w in K.evaluate_trial(ctx.con, gone)["incomplete"])


def test_f5_one_dropped_arrival_is_censored(cal):
    """Codex repro: one drop returned PASS / uncensored."""
    ctx = cal()
    ctx.world.drops = 1
    out = run(ctx)
    r = json.load(open(os.path.join(ctx.dir, "trials", "t001", "result.json")))
    assert out["verdict"] == "NOT READY" and r["verdict"] == "LIMIT" and r["delivery"] == "censored"
    assert any("zero-drop" in w for w in r["limits"])


def test_f5_absent_drops_are_zero_only_with_exact_reconciliation(cal):
    ctx = cal()
    run(ctx)
    raw = _raw(ctx, "t003")
    t, sm = _summary(raw)
    assert sm["scenarios"]["measure"]["dropped_iterations"] is None
    sm["scenarios"]["measure"]["iterations"]["count"] -= 1         # one scheduled arrival never dispatched
    sm["scenarios"]["measure"]["http_reqs"]["count"] -= 1
    t["message"] = json.dumps(sm)
    r = K.evaluate_trial(ctx.con, raw)
    assert r["verdict"] == "INCOMPLETE" and any("k6 counts are incomplete" in w for w in r["incomplete"])


def test_f6_direct_traffic_during_idle_is_refused(cal):
    ctx = cal()
    ctx.world.direct_traffic = True
    out = run(ctx)
    r = json.load(open(os.path.join(ctx.dir, "trials", "t001", "result.json")))
    assert out["verdict"] == "NOT READY" and r["verdict"] == "INCOMPLETE"
    assert any("idle baseline not demonstrated idle" in w and "direct-to-fixture" in w for w in r["incomplete"])


def test_f6_idle_ingress_movement_and_undrained_work_are_refused(cal):
    ctx = cal()
    run(ctx)
    raw = _raw(ctx, "t001")
    assert K.evaluate_trial(ctx.con, raw)["criteria"]["idle"]["idle_fixture_inflight"] == "0 throughout"
    moved = copy.deepcopy(raw)
    moved["observations"]["ingress_idle_start"]["requests"] = moved["observations"]["ingress_after"]["requests"]
    moved["observations"]["ingress_idle_start"]["requests"] = copy.deepcopy(
        moved["observations"]["ingress_idle_start"]["requests"])
    for v in moved["observations"]["ingress_idle_start"]["requests"]["response"]["data"]["result"][0]["values"]:
        v[0] -= 10_000
    r = K.evaluate_trial(ctx.con, moved)
    assert any("idle baseline" in w for w in r["incomplete"])
    busy = copy.deepcopy(raw)
    busy["observations"]["idle_fixture_inflight"]["response"]["data"]["result"][0]["values"][2][1] = "1"
    assert any("idle_fixture_inflight is not 0" in w for w in K.evaluate_trial(ctx.con, busy)["incomplete"])


def test_observer_queries_must_select_the_live_pods(cal):
    ctx = cal()
    ctx.world.up_label = "kubernetes_pod_name"        # the queries do not see pods under the declared label
    out = run(ctx)
    c = cert(ctx)
    ns = [e["data"] for e in ctx.entries() if e["event"] == "trial-not-started"]
    assert out["verdict"] == "NOT READY" and c["trials"] == 0
    assert any("does not prove the live" in p for p in ns[0]["problems"])


def test_certificate_states_the_pending_experiment_admission(cal):
    ctx = cal()
    run(ctx)
    assert any("maximum-arrival admission stays PENDING" in s for s in cert(ctx)["scope_limits"])
