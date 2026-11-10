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
                            ("ServiceAccount", ctx.nm["ingress"]), ("Role", ctx.nm["ingress"]),
                            ("RoleBinding", ctx.nm["ingress"]),
                            ("ConfigMap", ctx.nm["script"]), ("VMPodScrape", ctx.cal)])
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
    ing = next(o for o in objs if o["metadata"]["name"] == ctx.nm["ingress"] and o["kind"] == "Deployment")
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
                if r["metric"]["nodename"] == "worker-w")["values"]
    for v in vals[3:8]:
        v[1] = "7.5"
    r = K.evaluate_trial(ctx.con, bad)
    assert r["verdict"] == "INCOMPLETE" and any("interference: guest steal" in w for w in r["incomplete"])
    gap = copy.deepcopy(raw)
    res = gap["observations"]["steal"]["response"]["data"]["result"]
    next(x for x in res if x["metric"]["nodename"] == "worker-s")["values"].pop(6)
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
    ns = [e["data"] for e in ctx.entries() if e["event"] == "observer-not-ready"]
    assert out["verdict"] == "NOT READY" and c["trials"] == 0
    assert any("does not prove the live" in p for p in ns[0]["problems"])


def test_certificate_states_the_pending_experiment_admission(cal):
    ctx = cal()
    run(ctx)
    assert any("maximum-arrival admission stays PENDING" in s for s in cert(ctx)["scope_limits"])


def _bump_dispatch(raw, delta):
    """Change the after-reading of one of THIS trial's pods (earlier pods' series are in the reading too)."""
    pods = {p["metadata"]["name"] for p in raw["pods_begin"][raw["trial"]["variant"]]}
    res = raw["observations"]["ingress_after"]["dispatched"]["response"]["data"]["result"]
    for v in next(x for x in res if x["metric"]["endpoint_pod"] in pods)["values"]:
        v[1] = str(float(v[1]) + delta)


def test_k6_boundary_iteration_is_accepted_but_never_a_missing_dispatch(cal):
    """The kind smoke showed real k6 starting ONE iteration beyond rate x duration (300 -> 301, 1000 -> 1001)."""
    ctx = cal()
    run(ctx)
    raw = _raw(ctx, "t003")
    for extra, verdict in ((1, "PASS"), (2, "INCOMPLETE")):
        r = copy.deepcopy(raw)
        t, sm = _summary(r)
        for k in ("iterations", "http_reqs"):
            sm["scenarios"]["measure"][k]["count"] += extra
        sm["scenarios"]["measure"]["http_req_failed"]["fails"] += extra
        t["message"] = json.dumps(sm)
        for v in r["observations"]["ingress_after"]["requests"]["response"]["data"]["result"][0]["values"] + \
                r["observations"]["ingress_after"]["responses"]["response"]["data"]["result"][0]["values"]:
            v[1] = str(float(v[1]) + extra)
        _bump_dispatch(r, extra)                        # the extra arrival was dispatched to a pod as well
        assert K.evaluate_trial(ctx.con, r)["verdict"] == verdict, extra
    assert K.offered_ok(300, 300) and K.offered_ok(300, 301) and not K.offered_ok(300, 299) \
        and not K.offered_ok(300, 302)


# --- Codex re-review of ef8436b ------------------------------------------------------------------------------------

def _fixture_pod_and_template(ctx, api):
    tmpl = K.render_fixture(ctx.con, ctx.cal, ctx.params, 0)[0]["spec"]["template"]
    pod = {"metadata": {"name": "p", "labels": {**tmpl["metadata"]["labels"], "pod-template-hash": "abc"},
                        "annotations": dict(tmpl["metadata"]["annotations"])},
           "spec": CW.World._admit({**copy.deepcopy(tmpl["spec"]), "nodeName": "worker-w"})}
    return tmpl, pod


def test_r1_unrendered_container_fields_are_refused(cal, api):
    """Codex repro: an added container command returned no problems."""
    ctx = cal()
    tmpl, pod = _fixture_pod_and_template(ctx, api)
    assert K.pod_problems("p", tmpl, pod) == []
    for path, value in ((("containers", 0, "command"), ["/bin/sh", "-c", "sleep 1; exec /fixture"]),
                        (("containers", 0, "workingDir"), "/tmp"),
                        (("containers", 0, "envFrom"), [{"configMapRef": {"name": "x"}}]),
                        (("containers", 0, "lifecycle"), {"preStop": {"exec": {"command": ["sleep", "5"]}}}),
                        (("containers", 0, "volumeMounts"), [{"name": "v", "mountPath": "/v"}]),
                        (("containers", 0, "securityContext", "privileged"), True),
                        (("containers", 0, "readinessProbe", "initialDelaySeconds"), 30),
                        (("containers", 0, "imagePullPolicy"), "Always"),
                        (("initContainers",), [{"name": "i", "image": "x"}]),
                        (("hostNetwork",), True), (("priorityClassName",), "high")):
        bad = copy.deepcopy(pod)
        cur = bad["spec"]
        for k in path[:-1]:
            cur = cur[k]
        cur[path[-1]] = value
        assert K.pod_problems("p", tmpl, bad), path
    extra = copy.deepcopy(pod)
    extra["metadata"]["annotations"]["prometheus.io/path"] = "/other"
    assert K.pod_problems("p", tmpl, extra)
    # workload objects too: an unrendered field on the Deployment's container is refused before the first scale
    dep = K.render_fixture(ctx.con, ctx.cal, ctx.params, 0)[0]
    live = copy.deepcopy(dep)
    live["spec"]["template"]["spec"]["containers"][0]["command"] = ["/bin/sh"]
    assert K.object_problems("Deployment", dep, live)
    live = copy.deepcopy(dep)
    live["spec"]["progressDeadlineSeconds"] = 600
    live["spec"]["template"]["spec"]["dnsPolicy"] = "ClusterFirst"
    assert K.object_problems("Deployment", dep, live) == []                # API defaults only


def test_r1_command_added_to_a_running_pod_stops_the_trial(cal, api):
    ctx = cal()

    def tamper(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects) and not world.__dict__.get("t"):
            pod = next(o for (k, _, n), o in api_.objects.items() if k == "pods" and
                       n.startswith(ctx.nm["variants"][0] + "-"))
            pod["spec"]["containers"][0]["command"] = ["/bin/sh", "-c", "exec /fixture --slow"]
            world.t = True
    ctx.world.hooks.append(tamper)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and "command" in stopped[0]["reason"]


def test_r2_admission_default_tolerations_only(cal, api):
    """Codex repro: the standard not-ready/unreachable NoExecute tolerations produced a refusal."""
    ctx = cal()
    tmpl, pod = _fixture_pod_and_template(ctx, api)
    assert [t["key"] for t in pod["spec"]["tolerations"]][-2:] == ["node.kubernetes.io/not-ready",
                                                                    "node.kubernetes.io/unreachable"]
    assert K.pod_problems("p", tmpl, pod) == []
    cases = [lambda t: t[-1].update(tolerationSeconds=600),                    # not the default
             lambda t: t.append({"operator": "Exists"}),                         # a broad toleration
             lambda t: t[0].update(value="someone-else"),                        # the reservation toleration changed
             lambda t: t.pop(0),                                                 # the reservation toleration gone
             lambda t: t.append(dict(t[-1]))]                                   # a default added twice
    for mutate in cases:
        bad = copy.deepcopy(pod)
        mutate(bad["spec"]["tolerations"])
        assert K.pod_problems("p", tmpl, bad), mutate
    dep = K.render_fixture(ctx.con, ctx.cal, ctx.params, 0)[0]
    live = copy.deepcopy(dep)
    live["spec"]["template"]["spec"]["tolerations"] += list(K.ADMISSION_TOLERATIONS)
    assert K.object_problems("Deployment", dep, live) == []


def test_r3_a_settling_drop_is_censored(cal):
    """Codex repro: settling 5,400 scheduled, 5,399 dispatched, 1 dropped returned PASS/uncensored."""
    ctx = cal()
    run(ctx)
    raw = _raw(ctx, "t003")
    t, sm = _summary(raw)
    st = sm["scenarios"]["settle"]
    st["iterations"]["count"] -= 1
    st["http_reqs"]["count"] -= 1
    st["http_req_failed"]["fails"] -= 1
    st["dropped_iterations"] = {"count": 1, "rate": 1 / 300}
    t["message"] = json.dumps(sm)
    for side in ("requests", "responses"):
        for v in raw["observations"]["ingress_after"][side]["response"]["data"]["result"][0]["values"]:
            v[1] = str(float(v[1]) - 1)
    _bump_dispatch(raw, -1)                            # the dropped arrival was never dispatched to a pod either
    r = K.evaluate_trial(ctx.con, raw)
    assert r["verdict"] == "LIMIT" and r["delivery"] == "censored" and any(w.startswith("settle:") for w in r["limits"])


# --- c2.8s: guards before the contract freeze ------------------------------------------------------------------------

def _ds(api, name, strategy, ready=True, desired=2, updated=0, gen=6):
    ds = api.put({"apiVersion": "apps/v1", "kind": "DaemonSet",
                  "metadata": {"name": name, "namespace": "velero", "generation": gen},
                  "spec": {"updateStrategy": {"type": strategy}},
                  "status": {"observedGeneration": gen, "desiredNumberScheduled": desired,
                             "currentNumberScheduled": desired, "numberReady": desired, "numberAvailable": desired,
                             **({"updatedNumberScheduled": updated} if updated else {})}})
    pods = []
    for i, node in enumerate(("worker-w", "worker-s")[:desired]):
        p = fake2.pod(f"{name}-{i}", "velero", node, owner=("DaemonSet", name))
        p["metadata"]["ownerReferences"][0]["uid"] = ds["metadata"]["uid"]
        p["metadata"]["labels"] = {"controller-revision-hash": "rev-5"}
        p["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                       "containerStatuses": [{"name": "c", "restartCount": 0}]}
        pods.append(api.put(p))
    return ds, pods


def _allow_ds(ctx, name):
    ctx.record.doc["reserve"]["allowed"]["daemonsets"].append({"namespace": "velero", "name": name})


def test_s1_ondelete_daemonset_with_held_revision_is_settled(cal, api):
    """velero/node-agent on prod: OnDelete, none of its pods on the current template, all Ready: not a rollout."""
    ctx = cal()
    ds, pods = _ds(api, "node-agent", "OnDelete")
    _allow_ds(ctx, "node-agent")
    assert not any("rollout guard" in p for p in K.plan(ctx)["problems"])
    pods[1]["status"]["conditions"][0]["status"] = "False"           # a pod not Ready
    assert any("node-agent is mid-rollout (OnDelete" in p for p in K.plan(ctx)["problems"])
    pods[1]["status"]["conditions"][0]["status"] = "True"
    pods[0]["metadata"]["deletionTimestamp"] = "2026-11-09T00:00:00Z"   # a pod terminating
    assert any("terminating" in p for p in K.plan(ctx)["problems"])
    pods[0]["metadata"].pop("deletionTimestamp")
    ds["status"]["numberReady"] = 1                                     # status short of desired
    assert any("numberReady" in p for p in K.plan(ctx)["problems"])
    ds["status"]["numberReady"] = 2
    ds["metadata"]["generation"] = 7                                    # not yet observed
    assert any("has not observed" in p for p in K.plan(ctx)["problems"])
    # a RollingUpdate DaemonSet keeps the strict rule: a revision lag is a rollout
    ds2, _ = _ds(api, "agent-ru", "RollingUpdate", updated=1)
    _allow_ds(ctx, "agent-ru")
    ds["metadata"]["generation"] = 6
    probs = K.plan(ctx)["problems"]
    assert any("agent-ru is mid-rollout (updatedNumberScheduled" in p for p in probs)
    assert not any("node-agent" in p for p in probs)


def test_s1_ondelete_statefulsets_in_extra_owners(cal, api):
    """vmselect/vmstorage/vmalertmanager: OnDelete with a lagging currentRevision, guarded through extra_owners."""
    ctx = cal({"rollout_guard.extra_owners": [{"kind": "StatefulSet", "namespace": "monitoring",
                                               "name": "vmselect-vmst"}]})
    sts = api.put({"apiVersion": "apps/v1", "kind": "StatefulSet",
                   "metadata": {"name": "vmselect-vmst", "namespace": "monitoring", "generation": 3},
                   "spec": {"replicas": 2, "updateStrategy": {"type": "OnDelete"}},
                   "status": {"observedGeneration": 3, "replicas": 2, "readyReplicas": 2, "updatedReplicas": 2,
                              "currentRevision": "vmselect-a", "updateRevision": "vmselect-b"}})
    pods = []
    for i, node in enumerate(("worker-x", "worker-y")):
        p = fake2.pod(f"vmselect-vmst-{i}", "monitoring", node, owner=("StatefulSet", "vmselect-vmst"))
        p["metadata"]["ownerReferences"][0]["uid"] = sts["metadata"]["uid"]
        p["metadata"]["labels"] = {"controller-revision-hash": "vmselect-a"}
        p["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
        pods.append(api.put(p))
    assert not any("rollout guard" in p for p in K.plan(ctx)["problems"])
    api.objects.pop(("pods", "monitoring", "vmselect-vmst-1"))
    assert any("vmselect-vmst is mid-rollout (OnDelete: 1 pods for 2" in p for p in K.plan(ctx)["problems"])
    api.put(pods[1])
    sts["spec"]["updateStrategy"]["type"] = "RollingUpdate"           # the strict rule for RollingUpdate
    assert any("currentRevision differs" in p for p in K.plan(ctx)["problems"])


def _guard_world(ctx, api):
    ds, pods = _ds(api, "node-agent", "OnDelete")
    _allow_ds(ctx, "node-agent")
    return ds, pods


@pytest.mark.parametrize("change,why", [
    ("replace", "replaced pods during the plateau"),
    ("restart", "restarted during the plateau"),
    ("revision", "changed revision during the plateau"),
    ("template", "changed its template"),
])
def test_s1_guard_drift_during_a_plateau_stops_the_trial(cal, api, change, why):
    ctx = cal()
    ds, pods = _guard_world(ctx, api)

    def drift(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects) and not world.__dict__.get("d"):
            world.d = True
            if change == "replace":
                old = api_.objects.pop(("pods", "velero", "node-agent-0"))
                new = copy.deepcopy(old)
                new["metadata"]["uid"] = fake2.new_uid()
                new["metadata"]["name"] = "node-agent-9"
                api_.objects[("pods", "velero", "node-agent-9")] = new
            elif change == "restart":
                api_.objects[("pods", "velero", "node-agent-1")]["status"]["containerStatuses"][0]["restartCount"] = 1
            elif change == "revision":
                api_.objects[("pods", "velero", "node-agent-1")]["metadata"]["labels"]["controller-revision-hash"] = \
                    "rev-6"
            else:
                d = api_.objects[("daemonsets", "velero", "node-agent")]
                d["metadata"]["generation"] = 7
                d["status"]["observedGeneration"] = 7
    ctx.world.hooks.append(drift)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and stopped and why in stopped[0]["reason"], stopped
    guards = [e["data"] for e in ctx.entries() if e["event"] == "plateau-guard"]
    assert guards and "DaemonSet/velero/node-agent" in guards[-1]["guard"]["owners"]
    assert set(guards[0]["guard"]["owners"]["DaemonSet/velero/node-agent"]["pods"].values().__iter__().__next__()) \
        == {"name", "revision", "restarts"}


def test_s2_a_missing_series_cannot_hide_in_a_sum(cal):
    ctx = cal()
    run(ctx)
    two = next(t for t in sorted(os.listdir(os.path.join(ctx.dir, "trials")))
               if json.load(open(os.path.join(ctx.dir, "trials", t, "result.json")))["n"] == 2)
    raw = _raw(ctx, two)
    r = K.evaluate_trial(ctx.con, raw)
    assert r["criteria"]["raw_freshness"]["fixture_cpu"] and \
        len(r["criteria"]["raw_freshness"]["fixture_cpu"]) == 2
    for source in ("fixture_cpu", "fixture_metrics"):
        gone = copy.deepcopy(raw)
        gone["observations"][f"raw_{source}"]["response"]["data"]["result"].pop(0)
        assert any(f"raw {source}: no raw samples for" in w for w in K.evaluate_trial(ctx.con, gone)["incomplete"])
    old = copy.deepcopy(raw)
    for v in old["observations"]["raw_ingress_cpu"]["response"]["data"]["result"][0]["values"][20:24]:
        v[1] = str(v[0] - 90)                                          # the newest raw sample 90 s old
    assert any("raw ingress_cpu" in w and "90 s old" in w for w in K.evaluate_trial(ctx.con, old)["incomplete"])
    steal = copy.deepcopy(raw)
    steal["observations"]["raw_steal"]["response"]["data"]["result"].pop(1)
    assert any("raw steal: no raw samples for" in w for w in K.evaluate_trial(ctx.con, steal)["incomplete"])


def test_s2_live_raw_freshness(cal, api):
    ctx = cal()
    ctx.world.raw_missing = {"worker-s"}
    assert any("required observation missing: raw steal" in p for p in K.plan(ctx)["problems"])
    ctx.world.raw_missing = set()

    def stale(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            world.raw_stale = {n for (k, _, n) in api_.objects if k == "pods" and "-ing-" in n}
    ctx.world.hooks.append(stale)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and "raw ingress" in stopped[0]["reason"]


def test_s3_reconcile_tolerance_must_be_zero(cal):
    ctx = cal()
    path = K.record_contract_path(ctx.record)
    good = json.load(open(path))
    for tol in ({"absolute": 2, "relative": 0}, {"absolute": 0, "relative": 0.001}, {"absolute": False, "relative": 0}):
        bad = copy.deepcopy(good)
        bad["criteria"]["reconcile_tolerance"] = tol
        open(path, "w").write(json.dumps(bad))
        with pytest.raises(X.StagingError, match="reconcile_tolerance must be"):
            K.load_contract(ctx.record, ctx.params)
    # with zero tolerance, one arrival the generator never dispatched is unaccounted, never absorbed
    open(path, "w").write(json.dumps(good))
    run(ctx)
    raw = _raw(ctx, "t003")
    for v in raw["observations"]["ingress_after"]["requests"]["response"]["data"]["result"][0]["values"] + \
            raw["observations"]["ingress_after"]["responses"]["response"]["data"]["result"][0]["values"]:
        v[1] = str(float(v[1]) + 1)
    assert any("unaccounted arrivals" in w for w in K.evaluate_trial(ctx.con, raw)["incomplete"])


def test_s4_idle_cpu_is_averaged_after_the_washout(cal):
    ctx = cal()
    path = K.record_contract_path(ctx.record)
    good = json.load(open(path))
    bad = copy.deepcopy(good)
    bad["work_selection"]["idle_s"] = 200                 # < 60 s rate window + 30 s scrape + 120 s averaging
    open(path, "w").write(json.dumps(bad))
    with pytest.raises(X.StagingError, match="idle_s must be at least the washout"):
        K.load_contract(ctx.record, ctx.params)
    open(path, "w").write(json.dumps(good))
    run(ctx)
    raw = _raw(ctx, "t001")
    obs = raw["observations"]
    assert obs["fixture_cpu_idle"]["start"] == obs["idle_window"]["start"] + 90
    assert obs["fixture_cpu_idle"]["end"] == obs["idle_window"]["end"] == obs["idle_window"]["start"] + 240
    assert K.evaluate_trial(ctx.con, raw)["verdict"] == "PASS"
    early = copy.deepcopy(raw)
    early["observations"]["fixture_cpu_idle"]["start"] = obs["idle_window"]["start"]     # contaminated beginning
    assert any("idle averaging window" in w for w in K.evaluate_trial(ctx.con, early)["incomplete"])
    gone = copy.deepcopy(raw)
    gone["observations"]["idle_raw_fixture_cpu"]["response"]["data"]["result"] = []
    assert any("raw fixture_cpu" in w for w in K.evaluate_trial(ctx.con, gone)["incomplete"])


def test_s5_example_contract_uses_prod_style_queries(cal):
    ctx = cal()
    q, fr = ctx.con["observer"]["queries"], ctx.con["observer"]["freshness"]
    for name in K.CPU_QUERIES:
        assert q[name].count('metrics_path="/metrics/cadvisor"') == q[name].count("container_"), name
    for name in ("fixture_cpu", "ingress_cpu", "generator_cpu"):
        assert 'metrics_path="/metrics/cadvisor"' in fr[name]
    assert 'job=~"__NAMESPACE__/c28cal-.+"' in q["ingress_up"] and ctx.con["stops"]["node_label"] == "nodename"
    assert ctx.con["observer"]["scrape"] == "VMPodScrape" and ctx.con["observer"]["cpu_scrape_interval_s"] == 30
    bad = copy.deepcopy(ctx.con)
    bad["observer"]["queries"]["fixture_cpu_cores"] = q["fixture_cpu_cores"].replace("[1m]", "[5m]")
    assert any("rate window" in p for p in K._observer_problems(bad["observer"], X.import_harness().render))


# --- Codex review of 2aa1bcf ------------------------------------------------------------------------------------------

def _raw_doc(points):
    return {"start": 940, "end": 1000, "step": 15,
            "response": {"data": {"result": [{"metric": {"pod": "p"}, "values": [[t, str(v)] for t, v in points]}]}}}


def test_t1_freshness_is_judged_at_the_gate_time_with_full_coverage(cal):
    """Codex repro: gate 1000, evaluations 940/955/970, raw 880/895/910, max_staleness_s 60 passed."""
    ctx = cal()
    con = ctx.con
    stale_tail = _raw_doc([(940, 880), (955, 895), (970, 910)])
    why, d = K.raw_freshness(con, stale_tail, ["p"], "pod", 940, 1000)
    assert why and "missing head or tail" in why
    gap = _raw_doc([(940, 930), (955, 945), (985, 975), (1000, 990)])
    assert "gap" in K.raw_freshness(con, gap, ["p"], "pod", 940, 1000)[0]
    good = _raw_doc([(t, t - 10) for t in range(940, 1001, 15)])
    why, d = K.raw_freshness(con, good, ["p"], "pod", 940, 1000)
    assert why == "" and d["p"]["age_at_end_s"] == 10
    # every evaluation present but the raw samples stopped long ago: old at the gate time
    frozen = _raw_doc([(t, 925) for t in range(940, 1001, 15)])
    why, d = K.raw_freshness(con, frozen, ["p"], "pod", 940, 1000)
    assert "75 s old" in why and d["p"]["age_at_end_s"] == 75


def test_t2_new_pods_get_a_bounded_observer_warmup(cal):
    ctx = cal()
    out = run(ctx)
    assert out["verdict"] == "READY"
    ready = [e["data"] for e in ctx.entries() if e["event"] == "observer-ready"]
    assert ready[0]["trial"] == "t001" and ready[0]["waited_s"] >= ctx.world.scrape_delay
    # the idle interval began only after the observer saw the new pod
    raw = _raw(ctx, "t001")
    born = min(v for k, v in ctx.world.born.items() if k.startswith(ctx.nm["variants"][0] + "-"))
    assert raw["observations"]["idle_window"]["start"] >= born + ctx.world.scrape_delay


def test_t2_observer_warmup_timeout_is_not_ready(cal, api):
    ctx = cal()
    ctx.world.scrape_delay = 100_000                  # the new pods are never scraped
    out = run(ctx)
    c = cert(ctx)
    nr = [e["data"] for e in ctx.entries() if e["event"] == "observer-not-ready"]
    assert out["verdict"] == "NOT READY" and c["trials"] == 0 and c["halted"]["category"] == "incomplete"
    assert nr and nr[0]["timeout_s"] == 300
    assert live_calibration_objects(api, ctx.cal) == []


def test_t2_safety_guards_stay_active_during_the_warmup(cal, api):
    ctx = cal()
    ctx.world.scrape_delay = 100_000

    def intrude(world, api_):                         # a writer appears while the wait is under way
        born = [v for k, v in world.born.items() if k.startswith(ctx.nm["variants"][0] + "-")]
        if born and world.clock.now() >= min(born) + 30 and \
                ("horizontalpodautoscalers", "demo", "intruder") not in api_.objects:
            api_.put(_hpa(ctx.nm["variants"][0]))
    ctx.world.hooks.append(intrude)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "observer-wait-stopped"]
    assert out["verdict"] == "NOT READY" and cert(ctx)["halted"]["category"] == "stopped"
    assert stopped and any("replica writer" in p for p in stopped[0]["problems"])
    assert not any(e["event"] == "observer-not-ready" for e in ctx.entries())


# --- c2.8t: writer API groups that discovery proves absent -----------------------------------------------------------

PA_GROUP = "autoscaling.devkuban.com"
CRON_GROUP = "autoscaling.alibabacloud.com"
VPA_GROUP = "autoscaling.k8s.io"


def _absent(api, group=PA_GROUP):
    api.groups = [g for g in api.groups if g != group]


def test_u1_absent_writer_api_is_discovery_proven_absent_and_bound(cal, api):
    """prod: the new PredictiveAutoscaler CRD is not installed yet (circular with S3)."""
    ctx = cal()
    served = K.plan(ctx)["writer_apis"]
    assert {g: served[g] for g in ("autoscaling", "keda.sh", PA_GROUP, "autoscaler.example.com")} == \
        dict.fromkeys(("autoscaling", "keda.sh", PA_GROUP, "autoscaler.example.com"), "served")
    assert served[CRON_GROUP] == served[VPA_GROUP] == K.ABSENT          # not served by the fake by default
    _absent(api)
    p = K.plan(ctx)
    assert p["problems"] == [], p["problems"]
    assert p["writer_apis"][PA_GROUP] == K.ABSENT == "API discovery-proven absent"
    assert p["writer_apis"]["autoscaling"] == "served"
    out = run(ctx)
    assert out["verdict"] == "READY"
    begin = [e["data"] for e in ctx.entries() if e["event"] == "calibration-begin"][0]
    assert begin["writer_apis"] == p["writer_apis"]
    assert K.reduce_bundle(ctx.dir)["reproduced"]


def test_u1_without_the_opt_in_other_helpers_still_refuse(cal, api):
    ctx = cal()
    _absent(api)
    with pytest.raises(X.StagingError, match="not served"):
        X.writer_inventory(record_kube(ctx), "demo", ["x"])           # the original, strict behaviour


def record_kube(ctx):
    return ctx.record.kube(ctx.world.api)


@pytest.mark.parametrize("start_absent,toggle,why", [
    (True, PA_GROUP, f"{PA_GROUP} API discovery-proven absent -> served"),
    (False, "keda.sh", "keda.sh served -> API discovery-proven absent"),
])
def test_u2_writer_api_change_mid_trial_stops(cal, api, start_absent, toggle, why):
    ctx = cal()
    if start_absent:
        _absent(api)

    def flip(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects) and not world.__dict__.get("f"):
            world.f = True
            if toggle in api_.groups:
                api_.groups = [g for g in api_.groups if g != toggle]
            else:
                api_.groups = api_.groups + [toggle]
    ctx.world.hooks.append(flip)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and stopped and why in stopped[0]["reason"], stopped


def test_u2_api_appearing_between_the_scale_intent_and_the_write(cal, api, monkeypatch):
    ctx = cal()
    _absent(api)
    real = C.append_receipt

    def append(rec, helper, event, data, *a, **kw):
        out = real(rec, helper, event, data, *a, **kw)
        if helper == K.HELPER and event.startswith("intent-") and data.get("op") == "calibrate-scale" and \
                PA_GROUP not in api.groups:
            api.groups = api.groups + [PA_GROUP]
        return out
    monkeypatch.setattr(C, "append_receipt", append)
    with pytest.raises(X.StagingError, match="pre-write check refused"):
        run(ctx)
    monkeypatch.setattr(C, "append_receipt", real)
    assert [e["data"]["result"] for e in ctx.entries() if e["event"] == "scale-write"] == ["not-submitted"]
    assert (api.objects[("deployments", "demo", ctx.nm["variants"][0])]["spec"].get("replicas") or 0) == 0
    rp = X.recover_plan(ctx.record, ctx.kube)
    assert [(a["observed"], a.get("state")) for a in rp["actions"]] == [("not-done", "not-applied")]
    PF.recover(ctx.record, ctx.kube)
    plan, digest = CW.reviewed(ctx, K.plan_stop, K.STOP, "stop-plan")
    K.stop(ctx, plan, digest, "a writer API appeared")
    assert not lock_held(ctx)


def test_u2_api_appearing_right_before_the_generator_job(cal, api, monkeypatch):
    ctx = cal()
    _absent(api)
    real = C.append_receipt

    def append(rec, helper, event, data, *a, **kw):
        out = real(rec, helper, event, data, *a, **kw)
        if event == "trial-begin" and data["trial"]["id"] == "t003" and PA_GROUP not in api.groups:
            api.groups = api.groups + [PA_GROUP]
        return out
    monkeypatch.setattr(C, "append_receipt", append)
    out = run(ctx)
    monkeypatch.setattr(C, "append_receipt", real)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and stopped[0]["reason"].startswith("before the generator Job")
    creates = [e["data"] for e in ctx.entries() if e["event"].startswith("intent-") and e["data"]["op"] == "create"
               and e["data"]["kind"] == "Job"]
    assert not any(c["name"].endswith("t003") for c in creates)            # the load was never written


def test_u3_only_successful_discovery_proves_absence(cal, api):
    ctx = cal()
    _absent(api)
    real = api.get

    for failure in (C.ApiError("get /apis: failed (forbidden, exit 1)", 403),
                    C.ApiError("get /apis: failed (timeout, exit 1)", 504), {"groups": [{"versions": []}]},
                    {"kind": "Status"}, None):
        def get(path, failure=failure):
            if path == "/apis":
                if isinstance(failure, Exception):
                    raise failure
                return failure
            return real(path)
        api.get = get
        assert any("API discovery failed" in p for p in K.plan(ctx)["problems"]), failure
    api.get = real
    # a served writer API that cannot be listed stays a refusal
    api.groups = api.groups + [PA_GROUP]

    def get(path):
        if path.startswith("/apis/autoscaling.devkuban.com/"):
            raise C.ApiError("list: failed (forbidden, exit 1)", 403)
        return real(path)
    api.get = get
    assert any("listing a served writer API failed" in p for p in K.plan(ctx)["problems"])
    api.get = real


def test_u3_availability_change_between_review_and_apply_is_refused(cal, api):
    ctx = cal()
    _absent(api)
    plan, digest = CW.reviewed(ctx)
    api.groups = api.groups + [PA_GROUP]
    with pytest.raises(X.StagingError, match="no longer produces the reviewed plan"):
        K.apply(ctx, plan, digest)


def _cronhpa(target, name="nightly"):
    return {"apiVersion": "autoscaling.alibabacloud.com/v1beta1", "kind": "CronHorizontalPodAutoscaler",
            "metadata": {"name": name, "namespace": "demo"},
            "spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": target},
                     "jobs": [{"name": "up", "schedule": "0 0 8 * * *", "targetSize": 4}]}}


def _vpa(target, mode=None, name="vpa"):
    spec = {"targetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": target}}
    if mode is not None:
        spec["updatePolicy"] = {"updateMode": mode}
    return {"apiVersion": "autoscaling.k8s.io/v1", "kind": "VerticalPodAutoscaler",
            "metadata": {"name": name, "namespace": "demo"}, "spec": spec}


def test_v1_cronhpa_is_a_replica_writer_served_or_absent(cal, api):
    ctx = cal()
    api.groups = api.groups + [CRON_GROUP]
    assert K.plan(ctx)["writer_apis"][CRON_GROUP] == "served" and K.plan(ctx)["problems"] == []
    api.put(_cronhpa(ctx.nm["variants"][0]))
    assert any("CronHorizontalPodAutoscaler/demo/nightly" in p for p in K.plan(ctx)["problems"])
    api.put(_cronhpa("some-other-app", name="other"))          # not ours: ignored
    api.objects.pop(("cronhpas", "demo", "nightly"))
    assert K.plan(ctx)["problems"] == []
    # the shared inventory (session/prepare) sees it as a writer too, once its API is served
    inv = X.writer_inventory(record_kube(ctx), "demo", ["some-other-app"])
    assert [w["kind"] for w in inv["some-other-app"]] == ["CronHorizontalPodAutoscaler"]
    # an unserved group is discovery-proven absent: its objects are not listed
    api.groups = [g for g in api.groups if g != CRON_GROUP]
    assert K.plan(ctx)["writer_apis"][CRON_GROUP] == K.ABSENT


def test_v1_cronhpa_appearing_mid_trial_stops(cal, api):
    ctx = cal()
    api.groups = api.groups + [CRON_GROUP]

    def cron(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects) and \
                ("cronhpas", "demo", "nightly") not in api_.objects:
            api_.put(_cronhpa(ctx.nm["variants"][0]))
    ctx.world.hooks.append(cron)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and "CronHorizontalPodAutoscaler" in stopped[0]["reason"]


@pytest.mark.parametrize("mode", [None, "Auto", "Recreate", "Initial", "InPlaceOrRecreate"])
def test_v2_a_writing_vpa_refuses(cal, api, mode):
    ctx = cal()
    api.groups = api.groups + [VPA_GROUP]
    api.put(_vpa(ctx.nm["variants"][1], mode))
    want = mode or "Auto"                                     # the VPA API's default updateMode
    assert any(f"updateMode {want}" in p for p in K.plan(ctx)["problems"])


def test_v2_an_off_vpa_is_recorded_and_allowed_until_its_mode_changes(cal, api):
    ctx = cal()
    api.groups = api.groups + [VPA_GROUP]
    api.put(_vpa(ctx.nm["variants"][0], "Off", name="goldilocks-fixture"))
    p = K.plan(ctx)
    assert p["problems"] == [] and p["writer_apis"][VPA_GROUP] == "served"
    assert [v["update_mode"] for v in p["resource_writers"][ctx.nm["variants"][0]]] == ["Off"]

    def flip(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            api_.objects[("vpas", "demo", "goldilocks-fixture")]["spec"]["updatePolicy"]["updateMode"] = "Auto"
    ctx.world.hooks.append(flip)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and "updateMode Auto" in stopped[0]["reason"]


def test_v2_vpa_api_absent_or_disappearing(cal, api):
    ctx = cal()
    assert K.plan(ctx)["writer_apis"][VPA_GROUP] == K.ABSENT and K.plan(ctx)["problems"] == []
    api.groups = api.groups + [VPA_GROUP]

    def gone(world, api_):
        if any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            api_.groups = [g for g in api_.groups if g != VPA_GROUP]
    ctx.world.hooks.append(gone)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and f"{VPA_GROUP} served -> API discovery-proven absent" in \
        stopped[0]["reason"]


# --- Codex review of PR #49 -------------------------------------------------------------------------------------------

def test_w1_api_appearing_at_begin_creates_nothing(cal, api, monkeypatch):
    """Codex repro: the PA API appearing at calibration-begin still let all eight objects be created."""
    ctx = cal()
    _absent(api)
    plan, digest = CW.reviewed(ctx)
    real = C.append_receipt

    def append(rec, helper, event, data, *a, **kw):
        out = real(rec, helper, event, data, *a, **kw)
        if event == "calibration-begin" and PA_GROUP not in api.groups:
            api.groups.append(PA_GROUP)
        return out
    monkeypatch.setattr(C, "append_receipt", append)
    with pytest.raises(X.StagingError, match="writer census refused creating"):
        K.apply(ctx, plan, digest)
    monkeypatch.setattr(C, "append_receipt", real)
    created = [e["data"] for e in ctx.entries() if e["event"].startswith("intent-") and e["data"].get("op") == "create"]
    assert created == [] and not lock_held(ctx)
    assert cert(ctx)["verdict"] == "NOT READY"                    # closed as stopped, nothing to clean


def test_w1_api_appearing_after_a_create_intent_is_not_submitted(cal, api, monkeypatch):
    ctx = cal()
    _absent(api)
    plan, digest = CW.reviewed(ctx)
    real = C.append_receipt

    def append(rec, helper, event, data, *a, **kw):
        out = real(rec, helper, event, data, *a, **kw)
        if helper == K.HELPER and event.startswith("intent-") and data.get("op") == "create" and \
                data.get("kind") == "Service" and PA_GROUP not in api.groups:
            api.groups.append(PA_GROUP)                           # after the first Service's intent, before its POST
        return out
    monkeypatch.setattr(C, "append_receipt", append)
    with pytest.raises(X.StagingError, match="pre-write check refused creating Service/"):
        K.apply(ctx, plan, digest)
    monkeypatch.setattr(C, "append_receipt", real)
    writes = [e["data"] for e in ctx.entries() if e["event"] == "create-write"]
    assert [w["result"] for w in writes] == ["not-submitted"] and writes[0]["what"].startswith("Service/")
    svc = writes[0]["what"].split("/", 1)[1]
    assert ("services", "demo", svc) not in api.objects                 # nothing was sent
    assert lock_held(ctx)
    rp = X.recover_plan(ctx.record, ctx.kube)
    assert [(a["kind"], a["observed"], a.get("state")) for a in rp["actions"]] == [("Service", "not-done",
                                                                                     "not-applied")]
    PF.recover(ctx.record, ctx.kube)
    plan2, digest2 = CW.reviewed(ctx, K.plan_stop, K.STOP, "stop-plan")
    K.stop(ctx, plan2, digest2, "a writer API appeared during creation")
    assert live_calibration_objects(api, ctx.cal) == [] and not lock_held(ctx)


def test_w2_a_failing_second_discovery_never_gives_a_clean_plan(cal, api):
    """Codex repro: the plan's second discovery failing gave writer_apis {"discovery": "failed"} with no problems."""
    ctx = cal()
    real, calls = api.get, []

    def get(path):
        if path == "/apis":
            calls.append(path)
            if len(calls) >= 2:
                raise C.ApiError("test discovery failure", 503)
        return real(path)
    api.get = get
    p = K.plan(ctx)
    api.get = real
    assert len(calls) == 1 and p["problems"] == [] and set(p["writer_apis"].values()) <= {"served", K.ABSENT}
    # and a failing (only) census is recorded AND refused: never a clean plan with failed evidence
    def failing(path):
        if path == "/apis":
            raise C.ApiError("test discovery failure", 503)
        return real(path)
    api.get = failing
    p = K.plan(ctx)
    api.get = real
    assert p["writer_apis"] == {"discovery": "failed"} and any("API discovery failed" in x for x in p["problems"])


@pytest.mark.parametrize("doc,why", [
    ({"groups": [{"name": "", "versions": [{"version": None}]}]}, "non-empty string name"),
    ({"groups": [{"name": "apps", "versions": [{"version": None}]}]}, "non-empty version"),
    ({"groups": [{"name": "apps", "versions": []}]}, "no versions list"),
    ({"groups": [{"name": "apps"}]}, "no versions list"),
    ({"groups": [{"name": "apps", "versions": [{"version": "v1"}]}, {"name": "apps", "versions": [{"version": "v1"}]}]},
     "listed twice"),
    ({"kind": "Status", "groups": []}, "not APIGroupList"),
    ({"groups": "apps"}, "groups is not a list"),
    ({"groups": [["apps"]]}, "non-empty string name"),
    (None, "not a JSON object"),
    ([], "not a JSON object"),
])
def test_w3_malformed_discovery_is_refused(cal, api, doc, why):
    ctx = cal()
    real = api.get
    api.get = lambda path: doc if path == "/apis" else real(path)
    with pytest.raises(C.ApiError, match=why):
        ctx.kube.discover_fresh()
    assert any("API discovery failed" in p for p in K.plan(ctx)["problems"])
    api.get = real
    good = X.validate_discovery({"kind": "APIGroupList", "groups": [
        {"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]},
        {"name": "autoscaling", "versions": [{"groupVersion": "autoscaling/v2"}]}]})
    assert good == {"": ["v1"], "apps": ["v1"], "autoscaling": ["v2"]}


@pytest.mark.parametrize("kind", ["PredictiveAutoscaler", "HorizontalPodAutoscaler", "ScaledObject",
                                  "VerticalPodAutoscaler", "CronHorizontalPodAutoscaler"])
@pytest.mark.parametrize("answer", [None, {"kind": "Status"}, {"items": "x"}])
def test_w4_a_served_writer_api_must_answer_with_a_list(cal, api, kind, answer):
    """Codex repro: a served PA endpoint answering None (KubectlApi's 404) read as an empty list."""
    ctx = cal()
    api.groups = api.groups + [g for g in (VPA_GROUP, CRON_GROUP) if g not in api.groups]
    real, missing = api.get, X.path(kind, None)
    api.get = lambda path: answer if path == missing else real(path)
    probs = K.writer_problems(ctx)
    api.get = real
    assert any("listing a served writer API failed" in p and "no list response" in p for p in probs), probs
    # other callers keep the lenient list
    api.get = lambda path: None if path == missing else real(path)
    assert ctx.kube.list(kind, None) == []
    api.get = real


def test_w5_pre_job_stop_reports_a_foreign_job_of_that_name(cal, api, monkeypatch):
    ctx = cal()
    _absent(api)
    real = C.append_receipt

    def append(rec, helper, event, data, *a, **kw):
        out = real(rec, helper, event, data, *a, **kw)
        if event == "trial-begin" and data["trial"]["id"] == "t003" and PA_GROUP not in api.groups:
            api.groups.append(PA_GROUP)
            api.put({"apiVersion": "batch/v1", "kind": "Job", "metadata": {
                "name": K.job_name(ctx.cal, 3), "namespace": "demo"}, "spec": {}})
        return out
    monkeypatch.setattr(C, "append_receipt", append)
    out = run(ctx)
    monkeypatch.setattr(C, "append_receipt", real)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and "that this calibration did not create" in stopped[0]["reason"]
    assert ("jobs", "demo", K.job_name(ctx.cal, 3)) in api.objects               # never touched


# --- the endpoints-rr ingress: per-request round-robin, least-privilege RBAC --------------------------------------------

def _ingress_pod_and_template(ctx):
    dep = next(o for o in K.render_ingress(ctx.con, ctx.cal, ctx.params) if o["kind"] == "Deployment")
    tmpl = dep["spec"]["template"]
    pod = {"metadata": {"name": "p", "labels": {**tmpl["metadata"]["labels"], "pod-template-hash": "abc"},
                        "annotations": dict(tmpl["metadata"]["annotations"])},
           "spec": CW.World._admit({**copy.deepcopy(tmpl["spec"]), "nodeName": "worker-w"})}
    return dep, tmpl, pod


def test_the_calibration_ingress_routes_per_request_with_least_privilege(cal):
    ctx = cal()
    R = X.import_harness().render
    objs = K.render_objects(ctx.con, ctx.cal, ctx.params, "script")
    order = [(o["kind"], o["metadata"]["name"]) for o in objs]
    ing = ctx.nm["ingress"]
    # the identity exists before the ingress Deployment that runs as it
    assert order.index(("RoleBinding", ing)) < order.index(("Deployment", ing))
    rbac = {o["kind"]: o for o in objs if o["kind"] in K.RBAC_KINDS}
    assert set(rbac) == {"ServiceAccount", "Role", "RoleBinding"}
    assert all(o["metadata"]["name"] == ing and o["metadata"]["namespace"] == "demo" for o in rbac.values())
    assert rbac["Role"]["rules"] == [{"apiGroups": ["discovery.k8s.io"], "resources": ["endpointslices"],
                                      "verbs": ["get", "list", "watch"]}]
    assert rbac["RoleBinding"]["subjects"] == [{"kind": "ServiceAccount", "name": ing, "namespace": "demo"}]
    assert rbac["RoleBinding"]["roleRef"] == {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": ing}
    assert rbac["ServiceAccount"]["automountServiceAccountToken"] is False
    assert not [o for o in objs if o["kind"] in ("ClusterRole", "ClusterRoleBinding")]
    assert {K._role(o, ctx.nm) for o in rbac.values()} == {"ingress-rbac"}
    dep = next(o for o in objs if o["kind"] == "Deployment" and o["metadata"]["name"] == ing)
    spec = dep["spec"]["template"]["spec"]
    env = {e["name"]: e["value"] for e in spec["containers"][0]["env"]}
    assert env["ROUTING"] == "endpoints-rr" and env["DISCOVERY_RESYNC_SECONDS"] == "30" and \
        env["DISCOVERY_STALE_SECONDS"] == "90"
    assert spec["serviceAccountName"] == ing and spec["automountServiceAccountToken"] is False
    vol, mount = R.ingress_token()
    assert spec["volumes"] == [vol] and spec["containers"][0]["volumeMounts"] == [mount]
    # the evaluation ingress has the same shape (render_common), names apart
    common = R.render_common(PF.base_params())
    cdep = next(o for o in common if o["kind"] == "Deployment" and o["metadata"]["name"] == "c28-ingress")
    cspec = cdep["spec"]["template"]["spec"]
    assert set(cspec) == set(spec) and cspec["volumes"] == spec["volumes"]
    cenv = {e["name"]: e["value"] for e in cspec["containers"][0]["env"]}
    assert set(cenv) == set(env) and all(cenv[k] == env[k] for k, _ in R.INGRESS_ROUTING_ENV)
    # a broader grant is refused at rendering
    broad = copy.deepcopy(objs)
    next(o for o in broad if o["kind"] == "Role")["rules"][0]["verbs"].append("create")
    with pytest.raises(R.ParamsError, match="RBAC"):
        R.audit_rbac(broad)


def test_rbac_objects_are_compared_strictly(cal):
    ctx = cal()
    rbac = {o["kind"]: o for o in K.render_ingress(ctx.con, ctx.cal, ctx.params) if o["kind"] in K.RBAC_KINDS}
    for kind, want in rbac.items():
        assert K.object_problems(kind, want, copy.deepcopy(want)) == [], kind
    live = copy.deepcopy(rbac["RoleBinding"])
    live["subjects"][0]["apiGroup"] = ""                                    # the API default for a ServiceAccount
    assert K.object_problems("RoleBinding", rbac["RoleBinding"], live) == []
    cases = [("Role", lambda o: o["rules"][0]["verbs"].append("delete")),
             ("Role", lambda o: o["rules"].append({"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]})),
             ("Role", lambda o: o["rules"][0]["resources"].append("endpoints")),
             ("RoleBinding", lambda o: o["subjects"].append({"kind": "ServiceAccount", "name": "default",
                                                            "namespace": "demo"})),
             ("RoleBinding", lambda o: o["subjects"][0].update(apiGroup="rbac.authorization.k8s.io")),
             ("RoleBinding", lambda o: o["roleRef"].update(kind="ClusterRole")),
             ("ServiceAccount", lambda o: o.update(automountServiceAccountToken=True)),
             ("ServiceAccount", lambda o: o.update(imagePullSecrets=[{"name": "x"}])),
             ("ServiceAccount", lambda o: o.pop("automountServiceAccountToken"))]
    for kind, mutate in cases:
        live = copy.deepcopy(rbac[kind])
        mutate(live)
        assert K.object_problems(kind, rbac[kind], live), (kind, live)


def test_the_ingress_pod_runs_as_its_service_account_and_nothing_else(cal):
    ctx = cal()
    dep, tmpl, pod = _ingress_pod_and_template(ctx)
    assert pod["spec"]["serviceAccount"] == ctx.nm["ingress"]               # the API server's deprecated mirror
    assert K.pod_problems("p", tmpl, pod) == []
    live = copy.deepcopy(dep)
    live["spec"]["template"]["spec"]["serviceAccount"] = ctx.nm["ingress"]
    assert K.object_problems("Deployment", dep, live) == []
    live["spec"]["template"]["spec"]["serviceAccount"] = "default"
    assert K.object_problems("Deployment", dep, live)
    for mutate in (lambda s: s.update(serviceAccount="default"),
                   lambda s: s.update(serviceAccountName="default", serviceAccount="default"),
                   lambda s: s.update(automountServiceAccountToken=True),
                   lambda s: s["volumes"].append({"name": "kube-api-access-x7k2p", "projected": {"sources": [
                       {"serviceAccountToken": {"path": "token", "expirationSeconds": 3607}}]}}),
                   lambda s: s["volumes"][0]["projected"]["sources"][0]["serviceAccountToken"].update(audience="x"),
                   lambda s: s["containers"][0]["env"].pop()):
        bad = copy.deepcopy(pod)
        mutate(bad["spec"])
        assert K.pod_problems("p", tmpl, bad), mutate
    # fixture pods still run as default, without a token
    ftmpl, fpod = _fixture_pod_and_template(ctx, None)
    assert fpod["spec"]["serviceAccount"] == "default" and K.pod_problems("p", ftmpl, fpod) == []


def test_cleanup_removes_the_ingress_identity_after_the_ingress(cal, api):
    ctx = cal()
    assert run(ctx)["verdict"] == "READY"
    deletes = [(e["data"]["kind"], e["data"]["name"]) for e in ctx.entries()
               if e["event"].startswith("intent") and e["data"].get("op") == "delete"
               and (e["data"].get("context") or {}).get("phase") == "cleanup"]
    ing = ctx.nm["ingress"]
    assert deletes.index(("Deployment", ing)) < min(deletes.index((k, ing)) for k in K.RBAC_KINDS)
    assert not [k for (k, _, n) in api.objects if n == ing]


# --- Codex review of PR #51, finding 3: the ingress's endpoint discovery is a gate ---------------------------------------

def _no_load(ctx):
    return not any(e["event"] == "trial-begin" for e in ctx.entries())


def test_v1_contracts_are_refused_and_discovery_queries_are_validated(cal):
    ctx = cal()
    path = K.record_contract_path(ctx.record)
    con = json.load(open(path))
    old = copy.deepcopy(con)
    old["schema"] = "c28-calibration-contract/v1"
    old["observer"].pop("discovery")
    open(path, "w").write(json.dumps(old))
    with pytest.raises(X.StagingError, match="predates the endpoints-rr ingress"):
        K.load_contract(ctx.record, ctx.params)
    cases = [(lambda o: o.pop("discovery"), "observer must be exactly"),
             (lambda o: o["discovery"].pop("dispatched"), "observer.discovery must be exactly"),
             (lambda o: o["discovery"].update(endpoints='max by (endpoint_pod) (c28_ingress_endpoint{target="__TARGET__"})'),
              "must be a timestamp"),
             (lambda o: o["discovery"].update(stale="timestamp(c28_ingress_discovery_stale{pod=~\"__PODS__\"})"),
              "must not be a timestamp"),
             (lambda o: o["discovery"].update(dispatched='sum(c28_ingress_dispatched_total{target="__TARGET__"})'),
              "endpoint_pod"),
             (lambda o: o["discovery"].update(synced="max(c28_ingress_discovery_synced)"), "using __PODS__")]
    for mutate, msg in cases:
        bad = copy.deepcopy(con)
        mutate(bad["observer"])
        open(path, "w").write(json.dumps(bad))
        with pytest.raises(X.StagingError, match=msg):
            K.load_contract(ctx.record, ctx.params)


def test_stale_discovery_before_load_refuses_the_trial(cal, api):
    ctx = cal()
    ctx.world.disc_stale_from = CW.T0                 # the ingress reports stale discovery throughout
    out = run(ctx)
    nr = [e["data"] for e in ctx.entries() if e["event"] == "observer-not-ready"]
    assert out["verdict"] == "NOT READY" and cert(ctx)["halted"]["category"] == "incomplete"
    assert nr and any("stale discovery" in p for p in nr[0]["problems"]) and _no_load(ctx)
    assert live_calibration_objects(api, ctx.cal) == []


def test_never_synced_discovery_refuses_the_trial(cal):
    ctx = cal()
    ctx.world.disc_synced_from = CW.T0 + 10 ** 8
    run(ctx)
    nr = [e["data"] for e in ctx.entries() if e["event"] == "observer-not-ready"]
    assert nr and any("never synced" in p for p in nr[0]["problems"]) and _no_load(ctx)


def test_missing_discovery_metrics_refuse_the_trial(cal):
    ctx = cal()
    ctx.world.disc_missing = True                     # e.g. a ROUTING=service image: no discovery series at all
    out = run(ctx)
    nr = [e["data"] for e in ctx.entries() if e["event"] == "observer-not-ready"]
    assert out["verdict"] == "NOT READY" and _no_load(ctx)
    assert nr and any("discovery metrics missing or old" in p for p in nr[0]["problems"])


def test_a_membership_mismatch_before_load_refuses_the_trial(cal):
    ctx = cal()

    def hide(world, api_):                           # the ingress does not route to one of the live fixture pods
        pods = world._variant_pods(ctx.nm["variants"][0])
        if pods:
            world.disc_hidden = {pods[0]}
    ctx.world.hooks.append(hide)
    run(ctx)
    nr = [e["data"] for e in ctx.entries() if e["event"] == "observer-not-ready"]
    assert nr and any("discovery membership" in p for p in nr[0]["problems"]) and _no_load(ctx)


def test_stale_discovery_mid_measurement_stops_the_trial(cal, api):
    ctx = cal()

    def go_stale(world, api_):
        if world.disc_stale_from is None and any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            world.disc_stale_from = world.clock.now() + 30
    ctx.world.hooks.append(go_stale)
    out = run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert out["verdict"] == "NOT READY" and cert(ctx)["halted"]["category"] == "incomplete"
    assert stopped and stopped[0]["category"] == "incomplete" and "stale discovery" in stopped[0]["reason"]
    assert stopped[0]["trial"]["id"] == "t003" and live_calibration_objects(api, ctx.cal) == []


def test_an_extra_endpoint_mid_measurement_stops_the_trial(cal):
    ctx = cal()

    def rogue(world, api_):
        if not world.disc_extra and any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            world.disc_extra = {"someone-elses-pod"}
    ctx.world.hooks.append(rogue)
    run(ctx)
    stopped = [e["data"] for e in ctx.entries() if e["event"] == "trial-stopped"]
    assert stopped and stopped[0]["category"] == "incomplete" and "discovery membership" in stopped[0]["reason"]


def test_a_membership_blip_between_polls_invalidates_the_trial_from_the_window(cal):
    """poll_s is 300 s: a blip between two live gates is caught by evaluate_trial over the recorded window."""
    ctx = cal()

    def blip(world, api_):
        if world.disc_extra_window is None and any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            now = world.clock.now()
            # inside the measurement window, between the live polls (the Job starts now; settle 60 s)
            world.disc_extra, world.disc_extra_window = {"someone-elses-pod"}, (now + 400, now + 430)
    ctx.world.hooks.append(blip)
    run(ctx)
    r = json.load(open(os.path.join(ctx.dir, "trials", "t003", "result.json")))
    assert r["verdict"] == "INCOMPLETE" and any(w.startswith("discovery over the window: discovery membership")
                                                for w in r["incomplete"]), r["incomplete"]
    assert r["criteria"]["discovery"]["membership"]["mismatches"]
    assert K.reduce_bundle(ctx.dir)["reproduced"]


def test_per_endpoint_dispatch_is_recorded_per_trial_and_reconciled(cal):
    ctx = cal()
    out = run(ctx)
    assert out["verdict"] == "READY"
    seen = 0
    for t in sorted(os.listdir(os.path.join(ctx.dir, "trials"))):
        r = json.load(open(os.path.join(ctx.dir, "trials", t, "result.json")))
        pods = sorted(p["metadata"]["name"] for p in json.load(open(os.path.join(
            ctx.dir, "trials", t, "pods-begin.json")))[r["variant"]])
        d = r["criteria"]["dispatch"]
        assert sorted(d["by_endpoint"]) == pods and len(pods) == r["n"]
        assert d["total"] == r["criteria"]["ingress"]["received"] and max(d["by_endpoint"].values()) - min(
            d["by_endpoint"].values()) <= 1                       # the round-robin's evenness, measurable
        assert r["criteria"]["discovery"]["membership"]["expected"] == pods
        assert not r["criteria"]["discovery"]["membership"]["mismatches"]
        seen += 1
    assert seen > 3 and K.reduce_bundle(ctx.dir)["reproduced"]


def test_a_dispatch_that_does_not_reconcile_is_incomplete(cal):
    ctx = cal()
    ctx.world.dispatch_short = 5                       # arrivals received but never dispatched to a pod
    out = run(ctx)
    r = json.load(open(os.path.join(ctx.dir, "trials", "t001", "result.json")))
    assert out["verdict"] == "NOT READY" and r["verdict"] == "INCOMPLETE"
    assert any("does not reconcile" in w for w in r["incomplete"])


def test_discovery_verdict_is_pure_and_judges_every_evaluation():
    con = CW.contract("/nonexistent")
    step = con["observer"]["step_s"]
    grid = list(range(1000, 1000 + 10 * step, step))

    def rng(metric, fn, label="pod", names=("ing-1",)):
        return {"start": grid[0], "end": grid[-1], "response": {"data": {"result": [
            {"metric": {label: n}, "values": [[t, str(fn(n, t))] for t in grid]} for n in names]}}}
    obs = {"synced": rng("s", lambda n, t: 1), "stale": rng("s", lambda n, t: 0),
           "scrape_timestamp": rng("s", lambda n, t: t - 5),
           "endpoints": rng("e", lambda n, t: t - 5, "endpoint_pod", ("fx-a", "fx-b"))}
    why, d = K.discovery_verdict(con, obs, "ing-1", ["fx-a", "fx-b"], grid[0], grid[-1])
    assert why == [] and d["membership"]["evaluations"] == 10
    # an endpoint series left from an OLDER scrape is not current membership (a pod that left the set)
    old = dict(obs, endpoints=rng("e", lambda n, t: t - 5 if n == "fx-a" or t < grid[5] else grid[4] - 5,
                                  "endpoint_pod", ("fx-a", "fx-b")))
    why, d = K.discovery_verdict(con, old, "ing-1", ["fx-a", "fx-b"], grid[0], grid[-1])
    assert why and len(d["membership"]["mismatches"]) == 5
    # the live gate looks at the newest evaluation only
    assert K.discovery_verdict(con, old, "ing-1", ["fx-a"], grid[0], grid[-1], live=True)[0] == []



# --- the S1 images.json of a rebuild (build_images.py build.attempt) ------------------------------------------------

def test_a_rebuild_images_json_of_the_same_run_is_accepted(cal):
    """A later build of the same run carries attempt and suffixed tags; the contract binds refs by digest and the run
    id, never a tag, so it is accepted exactly like the run's first build."""
    tagged = {"fixture": {"ref": CW.FIXTURE, "tag": "harbor.example/autoscaler/c28-fixture:c28-" + PF.RUN + "-r2"},
              "generator": {"ref": CW.GENERATOR}}
    ctx = cal(images_over={"attempt": "r2", "images": tagged})
    K.load_contract(ctx.record, ctx.params)
    assert json.load(open(ctx.record.resolve("evidence/images.json")))["attempt"] == "r2"


def test_an_images_json_of_another_run_id_is_refused(cal):
    """A rebuild keeps the run id: an images.json whose run_id carries the attempt (or any other run) is refused."""
    with pytest.raises(X.StagingError, match="not this run's verified c28-images/v1 record"):
        cal(images_over={"run_id": PF.RUN + "-r2"})        # the context loads (and refuses) the contract


# --- contract v3: the ingress's no-ready rejections in the dispatch identity (c28cal-614e3450df) ---------------------

def _result(ctx, trial):
    return json.load(open(os.path.join(ctx.dir, "trials", trial, "result.json")))


def _no_ready_values(raw, side):
    return raw["observations"][f"ingress_{side}"]["no_ready"]["response"]["data"]["result"]


def _con_v2(con):
    """The same contract as v2 (observer.discovery without no_ready): the pre-v3 dispatch identity."""
    old = copy.deepcopy(con)
    old["schema"] = CW.V2
    old["observer"]["discovery"].pop("no_ready")
    return old


def test_v3_an_empty_endpoint_set_at_pass_level_reconciles_and_counts_the_failures(cal):
    """k arrivals answered 503 "no ready endpoint" (received, never dispatched) at a PASS-level load: the identity
    received = sum(dispatched) + no_ready holds, the k 503s are counted as failures (within the bound), PASS."""
    ctx = cal()
    ctx.world.no_ready_trials = {"t001": 2}
    out = run(ctx)
    assert out["verdict"] == "READY"
    r = _result(ctx, "t001")
    assert r["verdict"] == "PASS" and r["incomplete"] == [], r["incomplete"]
    d, ing = r["criteria"]["dispatch"], r["criteria"]["ingress"]
    assert d["no_ready"]["rejected"] == 2 and d["no_ready"]["absent"] == ["before"]
    assert d["total"] + 2 == ing["received"] == ing["answered"]
    ff, m = r["criteria"]["failure_fraction"], r["criteria"]["arrivals"]["measure"]
    assert m["failed"] == 2 and ff["measured"] == 2 / m["scheduled"] and ff["pass"]
    # the variant never rejected anything: an absent counter on both sides of the proven fresh ingress is 0
    r2 = _result(ctx, "t002")
    assert r2["criteria"]["dispatch"]["no_ready"] == {"before": 0.0, "after": 0.0, "absent": ["before", "after"],
                                                      "rejected": 0.0}
    # later trials of the same variant read the existing counter on both sides (no new rejection)
    r3 = _result(ctx, "t003")
    assert r3["variant"] == r["variant"] and r3["criteria"]["dispatch"]["no_ready"]["rejected"] == 0.0
    assert r3["criteria"]["dispatch"]["no_ready"]["absent"] == []
    assert K.reduce_bundle(ctx.dir)["reproduced"]


def test_v3_an_empty_endpoint_set_at_overload_is_a_fail_not_incomplete(cal):
    """The t006 shape of c28cal-614e3450df: an overloaded plateau with 5 no-ready 503s is a measured FAIL."""
    ctx = cal()
    ctx.world.no_ready_trials = {"t006": 5}
    run(ctx)
    r = _result(ctx, "t006")
    assert r["verdict"] == "FAIL" and r["incomplete"] == [], r["incomplete"]
    d, ing = r["criteria"]["dispatch"], r["criteria"]["ingress"]
    assert d["no_ready"]["rejected"] == 5 and d["total"] + 5 == ing["received"]
    assert K.reduce_bundle(ctx.dir)["reproduced"]


def _reading(at, dispatched, no_ready, step=15):
    """One aligned ingress reading (quiet: every sample equal) with the per-endpoint dispatch counters and the
    no-ready counter (None: the series does not exist yet)."""
    grid = list(range(at - 4 * step, at + 1, step))

    def doc(result):
        return {"start": grid[0], "end": at, "step": step, "response": {"status": "success", "data": {
            "result": result}}}
    return {"at": at,
            "up": doc([{"metric": {"pod": "ing-1"}, "values": [[t, "1"] for t in grid]}]),
            "dispatched": doc([{"metric": {"endpoint_pod": k}, "values": [[t, str(v)] for t in grid]}
                               for k, v in sorted(dispatched.items())]),
            "no_ready": doc([] if no_ready is None else [{"metric": {}, "values": [[t, str(no_ready)] for t in grid]}])}


def test_v3_the_t006_numbers_reconcile_and_v2_keeps_its_verdict():
    """received 38401, dispatched 38396, no_ready 5 (c28cal-614e3450df t006)."""
    con = CW.contract("/nonexistent")
    pods = ["fx-a", "fx-b", "fx-c", "fx-d"]
    after = {"fx-a": 9599, "fx-b": 9599, "fx-c": 9599, "fx-d": 9599}
    obs = {"ingress_before": _reading(10_000, {}, None), "ingress_after": _reading(11_500, after, 5)}
    why, d = K.dispatch_counts(con, obs, pods, 38401.0)
    assert why == [] and d["total"] == 38396 and d["no_ready"]["rejected"] == 5
    assert K.dispatch_counts(con, obs, pods, 38402.0)[0][0].startswith("per-endpoint dispatch 38396 plus 5 no-ready")
    # v2: the same evidence still does not reconcile (its recorded verdicts stay reproducible)
    why, d2 = K.dispatch_counts(_con_v2(con), obs, pods, 38401.0)
    assert why == ["per-endpoint dispatch 38396 does not reconcile with the 38401 arrivals the ingress received "
                   "(each arrival must be dispatched exactly once)"] and "no_ready" not in d2
    # the stray-endpoint check stays
    stray = {"ingress_before": obs["ingress_before"], "ingress_after": _reading(11_500, dict(after, other=1), 5)}
    why, _ = K.dispatch_counts(con, stray, pods, 38402.0)
    assert any("outside the trial's fixture pods: ['other']" in w for w in why)


@pytest.mark.parametrize("trial,mutate,why", [
    ("t001", lambda raw: raw["observations"]["ingress_after"].update(no_ready={"error": "URLError"}),
     "no-ready rejections after the trial: the query failed (URLError)"),
    ("t001", lambda raw: [v.__setitem__(0, v[0] - 200) for v in _no_ready_values(raw, "after")[0]["values"]],
     "no-ready rejections after the trial: the last sample is stale"),
    ("t003", lambda raw: _no_ready_values(raw, "after").clear(),
     "the no-ready rejection counter disappeared during the trial"),
    ("t003", lambda raw: [v.__setitem__(1, "1") for v in _no_ready_values(raw, "after")[0]["values"]],
     "the no-ready rejection counter went backwards"),
    ("t003", lambda raw: [v.__setitem__(1, "NaN") for v in _no_ready_values(raw, "after")[0]["values"]],
     "no-ready rejections after the trial: non-finite samples"),
    ("t003", lambda raw: [v.__setitem__(1, "-2") for v in _no_ready_values(raw, "before")[0]["values"]],
     "no-ready rejections before the trial: a negative counter value"),
    ("t002", lambda raw: raw["observations"]["ingress_after"].update(up={"error": "URLError"}),
     "no-ready rejections after the trial: no series, and ingress_up"),
])
def test_v3_an_unknown_no_ready_reading_is_incomplete(cal, trial, mutate, why):
    """Absent is 0 only from a successful query of the proven fresh ingress whose counter has not appeared; a failed
    query, a stale or non-finite sample, positive-to-absent, a reset or a negative value is unknown, never 0."""
    ctx = cal()
    ctx.world.no_ready_trials = {"t001": 2}
    run(ctx)
    raw = _raw(ctx, trial)
    assert K.evaluate_trial(ctx.con, raw) == _result(ctx, trial)
    bad = copy.deepcopy(raw)
    mutate(bad)
    r = K.evaluate_trial(ctx.con, bad)
    assert r["verdict"] == "INCOMPLETE" and any(w.startswith(why) for w in r["incomplete"]), r["incomplete"]
    assert not any("does not reconcile" in w for w in r["incomplete"])        # unknown, never reconciled as 0


def test_v3_no_ready_never_waives_a_sampled_membership_failure(cal):
    ctx = cal()
    ctx.world.no_ready_trials = {"t003": 3}

    def blip(world, api_):
        if world.disc_extra_window is None and any(k == "jobs" and n.endswith("t003") for (k, _, n) in api_.objects):
            now = world.clock.now()
            world.disc_extra, world.disc_extra_window = {"someone-elses-pod"}, (now + 400, now + 430)
    ctx.world.hooks.append(blip)
    run(ctx)
    r = _result(ctx, "t003")
    assert r["verdict"] == "INCOMPLETE" and r["criteria"]["dispatch"]["no_ready"]["rejected"] == 3
    assert any(w.startswith("discovery over the window: discovery membership") for w in r["incomplete"])
    assert not any("does not reconcile" in w for w in r["incomplete"])         # the accounting itself held


def test_v2_evidence_keeps_the_old_incomplete_and_its_bundles_still_verify(cal, api, monkeypatch):
    """A v2 contract (the closed revision-3 evidence) reduces and verifies exactly as before: the same no-ready
    evidence is still INCOMPLETE under v2; a v2 contract loads (status, verify) but plan and apply refuse it."""
    ctx = cal()
    ctx.world.no_ready_trials = {"t001": 2}
    run(ctx)
    raw = _raw(ctx, "t001")
    r2 = K.evaluate_trial(_con_v2(ctx.con), raw)
    assert r2["verdict"] == "INCOMPLETE" and "no_ready" not in r2["criteria"]["dispatch"]
    assert any("does not reconcile" in w and "exactly once)" in w for w in r2["incomplete"])
    # a calibration the pre-v3 executor ran on a v2 contract (same world, same rejections)
    v2 = cal(contract_over={"schema": CW.V2})
    assert v2.con["schema"] == CW.V2 and "no_ready" not in v2.con["observer"]["discovery"]
    p = K.plan(v2)
    assert any("is read-only" in x and K.CONTRACT_SCHEMA in x for x in p["problems"]), p["problems"]
    plan, digest = CW.reviewed(v2)
    with pytest.raises(X.StagingError, match="is read-only"):
        K.apply(v2, plan, digest)
    assert live_calibration_objects(api, v2.cal) == []
    monkeypatch.setattr(K, "CONTRACT_SCHEMA", CW.V2)             # the executor as it was before contract v3
    v2.world.no_ready_trials = {"t001": 2}
    out = run(v2)
    monkeypatch.undo()
    t1 = _result(v2, "t001")
    assert out["verdict"] == "NOT READY" and cert(v2)["halted"]["category"] == "incomplete"
    assert t1["verdict"] == "INCOMPLETE" and any("does not reconcile" in w for w in t1["incomplete"])
    assert "no_ready" not in json.load(open(os.path.join(v2.dir, "trials", "t001", "observations.json")))[
        "ingress_after"]
    again = CW.context(v2.record, api)                           # today's executor reads the v2 record
    v = K.verify(again)
    assert v["state"] == "closed" and v["bundle"]["reproduced"] and v["bundle"]["verdict"] == "NOT READY"
    assert K.status(again)["state"] == "closed"


def test_v3_validation_requires_the_no_ready_query(cal):
    ctx = cal()
    path = K.record_contract_path(ctx.record)
    con = json.load(open(path))
    assert con["schema"] == K.CONTRACT_SCHEMA == "c28-calibration-contract/v3"
    cases = [(lambda c: c["observer"]["discovery"].pop("no_ready"), "observer.discovery must be exactly"),
             (lambda c: c["observer"]["discovery"].update(
                 no_ready='sum(c28_ingress_no_ready_endpoints_total{namespace="__NAMESPACE__"})'), "using __TARGET__"),
             (lambda c: c["observer"]["discovery"].update(
                 no_ready='timestamp(c28_ingress_no_ready_endpoints_total{target="__TARGET__"})'),
              "must not be a timestamp"),
             (lambda c: c["observer"]["discovery"].update(
                 no_ready='sum by (pod) (c28_ingress_no_ready_endpoints_total{target="__TARGET__"})'),
              "one series per target"),
             # a v2 contract carries exactly the v2 discovery queries
             (lambda c: c.update(schema=CW.V2), "observer.discovery must be exactly")]
    for mutate, msg in cases:
        bad = copy.deepcopy(con)
        mutate(bad)
        open(path, "w").write(json.dumps(bad))
        with pytest.raises(X.StagingError, match=msg):
            K.load_contract(ctx.record, ctx.params)
    ok = _con_v2(con)
    open(path, "w").write(json.dumps(ok))
    assert K.load_contract(ctx.record, ctx.params)[0]["schema"] == CW.V2
