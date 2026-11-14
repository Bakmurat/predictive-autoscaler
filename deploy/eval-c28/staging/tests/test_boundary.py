"""boundary.py on the in-memory API: the query update (both PAs separately, status confirmed, partial = incomplete),
the history scrape's timing and generator-start gate (VOID outside (O, G - 120 s]), the acceptance receipt and its
binding, the live hold proof, and the generator's retirement for a retry."""

import copy
import hashlib
import json
import os
from datetime import datetime, timezone

import pytest

import boundary as BY
import bounded as BD
import execlib as X
import fake2 as F
import prepare
from part2_fixtures import api  # noqa: F401
from test_prepare_demo import DEMO, N, R, demo_params, run_until, world  # noqa: F401

O_TXT = "2026-11-12T00:00:00Z"
O = int(datetime(2026, 11, 12, tzinfo=timezone.utc).timestamp())
G = O + 300


class Clock:
    def __init__(self, s):
        self.ms = s * 1000

    def now(self):
        return self.ms

    def sleep(self, s):
        self.ms += s * 1000


class OperatorApi(F.FakeApi2):
    """A PA update is answered as the operator would: status.metricSource compiled for the new generation and the
    selected lead 600 for it (unless `stuck` names the PA)."""

    def __init__(self, base, stuck=()):
        self.__dict__.update(base.__dict__)
        self.stuck = set(stuck)

    def replace(self, path, body):
        out = super().replace(path, body)
        if "/predictiveautoscalers/" in path and body["metadata"]["name"] not in self.stuck:
            o = self.obj("PredictiveAutoscaler", body["metadata"]["name"], body["metadata"]["namespace"])
            q = o["spec"]["metrics"]["requests"]["source"]["query"].replace("{{ .Name }}", o["metadata"]["name"])
            gen = o["metadata"]["generation"]
            o.setdefault("status", {})["metricSource"] = {"query": q, "sha256": hashlib.sha256(q.encode()).hexdigest(),
                                                          "observedGeneration": gen}
            o["status"]["prediction"] = {"lead": {"seconds": 600, "observedGeneration": gen}}
            o["status"]["conditions"] = [{"type": "TelemetryAvailable", "status": "False",
                                          "reason": "MetricsUnavailable"}]
        return out


def boundary_world(world, stuck=()):  # noqa: F811
    p = demo_params()
    p["demonstration"]["schedule_origin_utc"] = O_TXT
    record, kube = world(params=p)
    run_until(record, kube, "pas")
    a = OperatorApi(world.api, stuck)
    kube = record.kube(api=a)
    clock = Clock(O - 6 * 3600)
    c = BY.Ctx(record, kube, now=clock.now, sleep=clock.sleep)
    admission_pass(record, c)
    ev = os.path.join(record.evidence_dir, "startup-x")
    os.makedirs(ev)
    sha = X.write_new(os.path.join(ev, "evidence.json"), "{}\n")
    X.receipt(record, "bounded", "startup-complete", {"run": "s-x", "measured": True, "dir": ev,
                                                      "evidence_sha256": sha, "admission": "a-x"})
    return record, kube, a, clock, c


def admission_pass(record, c):
    """A bounded admission PASS bound to the world's LIVE configuration (as bounded.run_admission records it)."""
    b = c.bctx()
    targets, _ = BD._targets_state(b)
    d = os.path.join(record.evidence_dir, "admission-a-x")
    os.makedirs(d)
    ev = X.write_new(os.path.join(d, "evidence.json"), "{}\n")
    vd = X.write_new(os.path.join(d, "verdict.json"), json.dumps({"verdict": "PASS"}) + "\n")
    X.receipt(record, "bounded", "run-complete", {"run": "a-x", "label": "admission", "verdict": "PASS", "dir": d,
                                                  "evidence_sha256": ev, "verdict_sha256": vd,
                                                  "binding": BD.binding(b, targets)})


def saved(record, plan, name):
    s = X.save_plan(record, BY.HELPER, plan, name)
    return X.load_plan(s["plan"]), s["plan_digest"]


def test_plan_query_binds_both_pas_and_the_guarded_query(world):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world)
    p = BY.plan_query(c, 1)
    assert p["problems"] == [], p["problems"]
    assert [x["name"] for x in p["actions"]] == sorted([N, R]) and p["guard_s"] == G
    assert p["template"] == c.hb.guarded_query("demo", f"{DEMO}-h1", G)


@pytest.mark.parametrize("edit, text", [
    (lambda record, c, clock: clock.__setattr__("ms", (O + 1) * 1000), "not in the future"),
    (lambda record, c, clock: X.receipt(record, "bounded", "startup-complete", {"run": "s-y", "measured": False}),
     "must be measured first"),
    (lambda record, c, clock: X.receipt(record, BY.HELPER, "boundary-failed", {"attempt": 1}), "not above the last"),
])
def test_plan_query_refuses(world, edit, text):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world)
    edit(record, c, clock)
    assert any(text in x for x in BY.plan_query(c, 1)["problems"]), BY.plan_query(c, 1)["problems"]


def test_apply_query_updates_each_pa_separately_and_confirms_its_status(world):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world)
    p, d = saved(record, BY.plan_query(c, 1), "q1")
    out = BY.apply_query(c, p, d)
    assert [m["op"] for m in kube.mutations] == ["replace", "replace"]
    for t in (N, R):
        assert a.obj("PredictiveAutoscaler", t, "demo")["spec"]["metrics"]["requests"]["source"]["query"] == p["template"]
    assert len(out["data"]["pas"]) == 2 and not os.path.exists(X.lock_file(record))


def test_a_partial_query_update_is_an_incomplete_attempt(world):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world, stuck=[R])
    p, d = saved(record, BY.plan_query(c, 1), "q1")
    with pytest.raises(X.StagingError, match="did not report"):
        BY.apply_query(c, p, d, wait_s=30)
    inc = X.last(X.receipts(record, BY.HELPER), "attempt-incomplete")["data"]
    assert inc["attempt"] == 1 and [x["name"] for x in inc["done"]] == [N]
    assert not BY.plan_scrape(c, 1)["problems"] or any("query update is not complete" in x
                                                         for x in BY.plan_scrape(c, 1)["problems"])


def _query_done(world, **kw):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world, **kw)
    p, d = saved(record, BY.plan_query(c, 1), "q1")
    BY.apply_query(c, p, d)
    return record, kube, a, clock, c


def _generator(a, c, origin_ms):
    """The continuous generator pod with the params' configuration digest and its simulated log."""
    import sys
    import tempfile
    import pathlib
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                                    "harness", "tests"))
    from demo_helpers import MIN, simulate
    cfg = c.c28.demo.generator_config(c.params)
    pod = F.pod(c.c28.demo.generator_pod_name(), "demo", "worker-s",
                labels={"app.kubernetes.io/name": c.c28.demo.GENERATOR_NAME, X.RUN_LABEL: DEMO})
    pod["metadata"]["annotations"] = {"c28.devkuban.com/generator-config": cfg["digest"]}
    a.put(pod)
    out = simulate(pathlib.Path(tempfile.mkdtemp()), cfg, {"incarnations": [{"pod_uid": "g", "start_ms": origin_ms + 5_000}],
                                                           "until_ms": origin_ms + 3 * MIN})
    return [json.dumps(l) for l in out["lines"]]


def test_the_scrape_is_created_only_inside_its_window_after_a_proven_generator_start(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    p, d = saved(record, BY.plan_scrape(c, 1), "s1")                      # reviewed hours before O
    assert p["problems"] == [], p["problems"]
    lines = _generator(a, c, O * 1000)
    with pytest.raises(X.StagingError, match="VOID"):                     # before O
        BY.apply_scrape(c, p, d, logs=lambda pod: lines)
    assert X.last(X.receipts(record, BY.HELPER), "attempt-void")


def test_a_scrape_needs_the_generators_validated_first_send(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    p, d = saved(record, BY.plan_scrape(c, 1), "s1")
    _generator(a, c, O * 1000)
    clock.ms = (O + 60) * 1000
    with pytest.raises(X.StagingError, match="not proven started"):
        BY.apply_scrape(c, p, d, logs=lambda pod: [])
    assert not [k for k in a.objects if k[0] == "vmpodscrapes" and "history" in k[2]]


def test_a_scrape_inside_its_window_is_created_once(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    p, d = saved(record, BY.plan_scrape(c, 1), "s1")
    lines = _generator(a, c, O * 1000)
    clock.ms = (O + 60) * 1000
    out = BY.apply_scrape(c, p, d, logs=lambda pod: lines)
    assert out["data"]["uid"] and out["data"]["generator_config_digest"]
    assert any("created" in x or "exists" in x for x in BY.plan_scrape(c, 1)["problems"])


def _vm_series(c, start, end, step=15, value=1.0):
    def fetch(url):
        import urllib.parse as U
        q = dict(U.parse_qsl(U.urlparse(url).query))["query"]
        t = q.split('target="')[1].split('"')[0]
        return {"status": "success", "data": {"result": [{"metric": {"__name__": c.hb.HISTORY_METRIC,
                                                                      "namespace": "demo", "target": t,
                                                                      c.hb.EPOCH_LABEL: c.epoch(1)},
                                                           "values": [[x, str(value)] for x in range(start, end + 1, step)]}]}}
    return fetch


def _scrape_done(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    p, d = saved(record, BY.plan_scrape(c, 1), "s1")
    lines = _generator(a, c, O * 1000)
    clock.ms = (O + 60) * 1000
    BY.apply_scrape(c, p, d, logs=lambda pod: lines)
    clock.ms = (G + 40) * 1000
    return record, kube, a, clock, c


def test_acceptance_is_one_receipt_bound_to_the_attempt(world):  # noqa: F811
    record, kube, a, clock, c = _scrape_done(world)
    out = BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")
    assert out["event"] == "boundary-accepted", out["data"]["problems"]
    b = out["data"]["binding"]
    assert b["origin_s"] == O and b["guard_s"] == G and b["epoch"] == f"{DEMO}-h1" and b["raw_sha256"]
    assert set(b["pas"]) == {N, R} and b["scrape"]["uid"] and b["generator_config_digest"]
    assert BY.accepted_boundary(record)["attempt"] == 1
    with pytest.raises(X.StagingError, match="already decided"):
        BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")


def test_a_gap_fails_the_boundary_and_activation_never_sees_it(world):  # noqa: F811
    record, kube, a, clock, c = _scrape_done(world)
    out = BY.accept(c, 1, _vm_series(c, O + 75, G - 70), "http://vm")
    assert out["event"] == "boundary-failed" and BY.accepted_boundary(record) is None


def test_a_replaced_history_scrape_object_fails_the_boundary(world):  # noqa: F811
    record, kube, a, clock, c = _scrape_done(world)
    obj = a.obj("VMPodScrape", c.hb.scrape_name(1), "demo")
    obj["metadata"]["uid"] = "11111111-0000-4000-8000-00000000abcd"
    out = BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")
    assert out["event"] == "boundary-failed"
    assert any("not the one created" in x for x in out["data"]["problems"])


def hold_vm(mode_from, mode_to, writes="zero", step=30, mode_value="1", baseline=60, pods=("operator-a",
                                                                                         "operator-a")):
    """The observer for the hold proof: ONE operator identity's mode series (Recommend) and applied-write counter,
    sampled every `step` s over [mode_from, mode_to]; writes "zero" = constant 0, None = absent, a list = those values
    spread over the interval; mode_from None = no mode series."""
    def fetch(url):
        import urllib.parse as U
        q = dict(U.parse_qsl(U.urlparse(url).query))["query"]
        ts = [] if mode_from is None else list(range(int(mode_from) - baseline, int(mode_to) + 1, step))
        if q.startswith("predictive_autoscaler_mode"):
            res = [] if mode_from is None else [{"metric": {"pod": pods[0]},
                                                 "values": [[t, mode_value] for t in ts]}]
        elif q.startswith("predictive_autoscaler_scale_writes_total"):
            if writes is None or mode_from is None:
                res = []
            else:
                vals = ["0"] * len(ts) if writes == "zero" else \
                    [str(writes[min(i * len(writes) // len(ts), len(writes) - 1)]) for i in range(len(ts))]
                res = [{"metric": {"pod": pods[1]}, "values": [[t, v] for t, v in zip(ts, vals)]}]
        else:
            raise ValueError(q)
        return {"status": "success", "data": {"result": res}}
    return fetch


def decisions(clock, **over):
    at = datetime.fromtimestamp(clock.ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return [dict({"event": "decision", "application": t, "namespace": "demo", "at": at, "desired_source": "keep_current",
                  "applied_replicas": 12, "current_replicas": 12}, **over) for t in (N, R)]


def test_the_hold_proof_needs_real_decisions_mode_coverage_and_no_applied_write(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    since = X.last(X.receipts(record, BY.HELPER), "query-updated")["data"]["updated_s"]
    clock.sleep(600)
    now = clock.ms / 1000
    rows = decisions(clock)
    assert BY.prove_hold(c, 1, hold_vm(since, now), "http://vm", rows)["proven"]
    assert not BY.prove_hold(c, 1, hold_vm(since, now, writes=[0, 1]), "http://vm", rows)["proven"]   # a write
    assert not BY.prove_hold(c, 1, hold_vm(since, now, writes=None), "http://vm", rows)["proven"]     # no counter
    assert not BY.prove_hold(c, 1, hold_vm(since, now, writes=[3, 0]), "http://vm", rows)["proven"]   # a reset
    assert not BY.prove_hold(c, 1, hold_vm(None, None), "http://vm", rows)["proven"]          # no evidence at all
    assert not BY.prove_hold(c, 1, hold_vm(since, now, mode_value="0"), "http://vm", rows)["proven"]  # not Recommend
    assert not BY.prove_hold(c, 1, hold_vm(since + 300, now), "http://vm", rows)["proven"]    # coverage gap
    assert not BY.prove_hold(c, 1, hold_vm(since, now), "http://vm", decisions(clock, desired_source="reactive"))["proven"]
    assert not BY.prove_hold(c, 1, hold_vm(since, now), "http://vm",
                             [dict(r, application="other") for r in rows])["proven"]          # not these targets
    clock.ms = (G + 1) * 1000
    assert not BY.prove_hold(c, 1, hold_vm(since, G), "http://vm", decisions(clock))["proven"]   # only before G


def test_a_ledger_file_must_be_a_receipted_read(world, tmp_path):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    f = tmp_path / "ledger.jsonl"
    f.write_text("{}\n")
    with pytest.raises(X.StagingError, match="not a verified ledger_read"):
        BY.ledger_rows(record, str(f))
    X.receipt(record, "ledger_read", "read", {"out": str(f), "sha256": hashlib.sha256(b"{}\n").hexdigest()})
    assert BY.ledger_rows(record, str(f)) == [{}]


def test_the_generator_is_retired_only_for_a_retry_and_by_recorded_uid(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    assert any("only for a retry" in x for x in BY.plan_retire(c)["problems"])
    X.receipt(record, BY.HELPER, "boundary-failed", {"attempt": 1})
    p = BY.plan_retire(c)
    assert p["problems"] == [] and p["actions"] == []                         # nothing of this run's to retire yet


def test_a_retry_retires_the_generator_by_recorded_uid_and_proves_its_pod_gone(world):  # noqa: F811
    record, kube = world()                                   # origin in the past: prepare's generator phase runs
    run_until(record, kube, "generator")
    a = world.api
    c = BY.Ctx(record, record.kube(api=a))
    pod = F.pod(c.c28.demo.generator_pod_name(), "demo", "worker-s",
                labels={"app.kubernetes.io/name": c.c28.demo.GENERATOR_NAME, X.RUN_LABEL: DEMO})
    sts_uid = a.obj("StatefulSet", c.c28.demo.GENERATOR_NAME, "demo")["metadata"]["uid"]
    pod["metadata"]["ownerReferences"] = [{"kind": "StatefulSet", "uid": sts_uid, "controller": True}]
    a.put(pod)
    X.receipt(record, BY.HELPER, "boundary-failed", {"attempt": 1})
    p = BY.plan_retire(c)
    assert p["problems"] == [] and {x["delete"] for x in p["actions"]} == {"StatefulSet", "Service", "ConfigMap", "Pod"}
    s = X.save_plan(record, BY.HELPER, p, "retire")
    out = BY.retire(c, X.load_plan(s["plan"]), s["plan_digest"], logs=lambda name: ['{"k6": "log"}'])
    assert out["data"]["logs"] and all(out["data"]["logs"].values())
    for kind in ("StatefulSet", "Service", "ConfigMap", "Pod"):
        assert not [k for k, v in a.objects.items() if v.get("kind") == kind
                    and v["metadata"]["name"].startswith(c.c28.demo.GENERATOR_NAME)], kind
    ops = [(m["op"], m["kind"]) for m in c.kube.mutations]
    assert ops.index(("delete", "StatefulSet")) < ops.index((BY.ABSENT_OP, "StatefulSet")) \
        < ops.index((BY.ABSENT_OP, "Pod")) < ops.index(("delete", "ConfigMap"))
    assert not os.path.exists(X.lock_file(record))



# --- the review's reproductions, now refused -----------------------------------------------------------------------------

def test_acceptance_refuses_changed_live_queries_or_a_replaced_pa(world):  # noqa: F811
    record, kube, a, clock, c = _scrape_done(world)
    for t in (N, R):
        pa = a.obj("PredictiveAutoscaler", t, "demo")
        pa["spec"]["metrics"]["requests"]["source"]["query"] = "sum(rate(old_counter[60s]))"
        pa["metadata"]["generation"] += 1
    out = BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")
    assert out["event"] == "boundary-failed" and any("changed after the query update" in x for x in out["data"]["problems"])


def test_acceptance_refuses_a_replaced_pa(world):  # noqa: F811
    record, kube, a, clock, c = _scrape_done(world)
    a.obj("PredictiveAutoscaler", N, "demo")["metadata"]["uid"] = "11111111-0000-4000-8000-0000000000ff"
    out = BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")
    assert out["event"] == "boundary-failed" and any("gone or replaced" in x for x in out["data"]["problems"])


def test_a_newer_attempt_in_any_state_invalidates_an_older_acceptance(world):  # noqa: F811
    record, kube, a, clock, c = _scrape_done(world)
    BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")
    assert BY.accepted_boundary(record)["attempt"] == 1
    X.receipt(record, BY.HELPER, "attempt-incomplete", {"attempt": 2, "done": []})
    assert BY.accepted_boundary(record) is None


def test_an_altered_acceptance_file_is_no_longer_accepted(world):  # noqa: F811
    record, kube, a, clock, c = _scrape_done(world)
    out = BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")
    with open(out["data"]["proof_path"], "a") as fh:
        fh.write(" ")
    assert BY.accepted_boundary(record) is None


def test_an_unowned_generator_named_pod_is_never_retired(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    pod = F.pod(c.c28.demo.generator_pod_name(), "demo", "worker-s", labels={})
    a.put(pod)
    X.receipt(record, BY.HELPER, "boundary-failed", {"attempt": 1})
    assert any("not owned by this run's generator" in x for x in BY.plan_retire(c)["problems"])
    assert c.kube.get("Pod", pod["metadata"]["name"], "demo") is not None


def test_a_failed_log_fetch_refuses_the_retirement(world):  # noqa: F811
    record, kube = world()
    run_until(record, kube, "generator")
    a = world.api
    c = BY.Ctx(record, record.kube(api=a))
    pod = F.pod(c.c28.demo.generator_pod_name(), "demo", "worker-s", labels={})
    pod["metadata"]["ownerReferences"] = [{"kind": "StatefulSet", "controller": True,
                                           "uid": a.obj("StatefulSet", c.c28.demo.GENERATOR_NAME, "demo")["metadata"]["uid"]}]
    a.put(pod)
    X.receipt(record, BY.HELPER, "boundary-failed", {"attempt": 1})
    s_ = X.save_plan(record, BY.HELPER, BY.plan_retire(c), "retire")

    def broken(name):
        raise X.StagingError("kubectl logs failed (exit 1)")
    with pytest.raises(X.StagingError, match="logs"):
        BY.retire(c, X.load_plan(s_["plan"]), s_["plan_digest"], logs=broken)
    assert a.obj("StatefulSet", c.c28.demo.GENERATOR_NAME, "demo") is not None   # nothing deleted


def test_a_slow_check_cannot_create_the_scrape_after_the_cutoff(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    p, d = saved(record, BY.plan_scrape(c, 1), "s1")
    lines = _generator(a, c, O * 1000)
    clock.ms = (G - 121) * 1000

    def slow(pod):
        clock.sleep(120)
        return lines
    with pytest.raises(X.StagingError, match="VOID"):
        BY.apply_scrape(c, p, d, logs=slow)
    assert not [k for k in a.objects if k[0] == "vmpodscrapes" and "history" in k[2]]
    assert X.last(X.receipts(record, BY.HELPER), "attempt-void")


def test_a_changed_live_target_template_voids_the_startup_basis(world):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world)
    dep = a.obj("Deployment", R, "demo")
    dep["spec"]["template"]["spec"]["containers"][0].setdefault("resources", {}).setdefault("limits", {})["memory"] = "16Mi"
    assert any("pod_templates differs from the admission binding" in x for x in BY.plan_query(c, 1)["problems"])


def test_an_interrupted_acceptance_is_completed_by_a_rerun(world, monkeypatch):  # noqa: F811
    record, kube, a, clock, c = _scrape_done(world)
    real = X.write_new

    def broken(path, *args, **kw):
        if path.endswith("/proof.json"):
            raise OSError("simulated disk failure")
        return real(path, *args, **kw)
    with monkeypatch.context() as m:
        m.setattr(X, "write_new", broken)
        with pytest.raises(OSError):
            BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")
    out = BY.accept(c, 1, _vm_series(c, O + 75, G), "http://vm")       # the same raw samples: completes
    assert out["event"] == "boundary-accepted" and BY.accepted_boundary(record)["attempt"] == 1
    assert not os.path.exists(X.lock_file(record))



# --- the second review's reproductions, now handled ----------------------------------------------------------------------

def test_an_origin_only_params_change_keeps_the_admission_basis(world):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world)
    path = record.resolve(record.doc["params"])
    p = json.load(open(path))
    p["demonstration"]["schedule_origin_utc"] = "2026-11-13T00:00:00Z"
    open(path, "w").write(json.dumps(p))
    c = BY.Ctx(record, kube, now=clock.now, sleep=clock.sleep)
    assert BY.plan_query(c, 1)["problems"] == []
    p["proxy"]["cpu_limit"] = "250m"                                   # any OTHER params change is refused
    open(path, "w").write(json.dumps(p))
    c = BY.Ctx(record, kube, now=clock.now, sleep=clock.sleep)
    assert any("params_except_origin_sha256 differs" in x for x in BY.plan_query(c, 1)["problems"])


def test_a_retry_after_an_attempts_own_query_change_is_allowed(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    X.receipt(record, BY.HELPER, "attempt-incomplete", {"attempt": 1, "done": []})
    assert BY.plan_query(c, 2)["problems"] == []


def test_a_pa_changed_during_the_status_wait_is_never_adopted(world, monkeypatch):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world)
    p, d = saved(record, BY.plan_query(c, 1), "q")
    real = kube.update

    def change_after_write(kind, name, ns, *args, **kw):
        out = real(kind, name, ns, *args, **kw)
        pa = a.obj(kind, name, ns)
        pa["spec"]["mode"] = "Active"
        pa["metadata"]["generation"] += 1
        return out
    monkeypatch.setattr(kube, "update", change_after_write)
    with pytest.raises(X.StagingError, match="never adopted"):
        BY.apply_query(c, p, d)
    assert X.last(X.receipts(record, BY.HELPER), "attempt-incomplete")


def _retire_world(world):  # noqa: F811
    record, kube = world()
    run_until(record, kube, "generator")
    a = world.api
    c = BY.Ctx(record, record.kube(api=a))
    pod = F.pod(c.c28.demo.generator_pod_name(), "demo", "worker-s", labels={})
    pod["metadata"]["ownerReferences"] = [{"kind": "StatefulSet", "controller": True,
                                           "uid": a.obj("StatefulSet", c.c28.demo.GENERATOR_NAME, "demo")["metadata"]["uid"]}]
    a.put(pod)
    X.receipt(record, BY.HELPER, "boundary-failed", {"attempt": 1})
    return record, a, c


def test_a_retirement_retries_after_a_failed_log_fetch(world):  # noqa: F811
    record, a, c = _retire_world(world)
    p, d = saved(record, BY.plan_retire(c), "r")

    def failed(_):
        raise X.StagingError("log fetch failed")
    with pytest.raises(X.StagingError):
        BY.retire(c, p, d, logs=failed)
    BY.retire(c, p, d, logs=lambda _: ["ok"])                            # the identical plan, retried
    assert a.obj("StatefulSet", c.c28.demo.GENERATOR_NAME, "demo") is None


def test_a_surviving_pod_of_a_deleted_statefulset_is_still_ours(world, monkeypatch):  # noqa: F811
    record, a, c = _retire_world(world)
    p, d = saved(record, BY.plan_retire(c), "r")
    pod = copy.deepcopy(c.kube.get("Pod", c.c28.demo.generator_pod_name(), c.ns))

    def interrupted(*args, **kw):
        a.put(pod)                                                       # background GC has not removed it yet
        raise RuntimeError("interrupted after the StatefulSet delete")
    with monkeypatch.context() as m:
        m.setattr(c.kube, "step", interrupted)
        with pytest.raises(RuntimeError):
            BY.retire(c, p, d, logs=lambda _: ["ok"])
    p2 = BY.plan_retire(c)
    assert not any("not owned" in x for x in p2["problems"])
    assert [x["uid"] for x in p2["actions"] if x["delete"] == "Pod"] == [pod["metadata"]["uid"]]



# --- the third review's reproductions, now handled -----------------------------------------------------------------------

def test_a_genuinely_partial_update_does_not_block_a_retry(world):  # noqa: F811
    record, kube, a, clock, c = boundary_world(world, stuck=[R])
    p, d = saved(record, BY.plan_query(c, 1), "q")
    with pytest.raises(X.StagingError, match="did not report"):
        BY.apply_query(c, p, d, wait_s=30)                              # both writes happened; R never confirmed
    assert BY.plan_query(c, 2)["problems"] == []                       # proven query-only changes are accepted
    a.obj("PredictiveAutoscaler", R, "demo")["spec"]["maxReplicas"] = 7   # anything else is not
    assert any("reviewed chain" in x for x in BY.plan_query(c, 2)["problems"])


def test_the_hold_proof_needs_a_baseline_one_identity_and_coverage(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    since = X.last(X.receipts(record, BY.HELPER), "query-updated")["data"]["updated_s"]
    clock.sleep(600)
    now = clock.ms / 1000
    rows = decisions(clock)
    assert BY.prove_hold(c, 1, hold_vm(since, now), "http://vm", rows)["proven"]
    assert not BY.prove_hold(c, 1, hold_vm(since + 15, now, baseline=0, writes=[1]), "http://vm", rows)["proven"]
    assert not BY.prove_hold(c, 1, hold_vm(since, now, pods=("operator-a", "operator-b")), "http://vm", rows)["proven"]
    assert not BY.prove_hold(c, 1, hold_vm(since, since + 30, step=30, baseline=0), "http://vm", rows)["proven"]


def test_an_unreviewed_change_before_the_pre_image_is_never_a_recovery_basis(world, monkeypatch):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)                     # attempt 1 completed
    X.receipt(record, BY.HELPER, "attempt-incomplete", {"attempt": 1, "done": []})
    p, d = saved(record, BY.plan_query(c, 2), "q2")
    real_get = kube.get
    calls = {"n": 0}

    def get(kind, name, ns=None):
        o = real_get(kind, name, ns)
        if kind == "PredictiveAutoscaler" and name == N and calls["n"] == 0:
            calls["n"] += 1
            live = a.obj(kind, name, ns)
            live["spec"]["maxReplicas"] = 7                             # another writer, before the pre-image read
            live["metadata"]["generation"] += 1
            o = real_get(kind, name, ns)
        return o
    monkeypatch.setattr(kube, "get", get)
    with pytest.raises(X.StagingError):
        BY.apply_query(c, p, d)
    assert not [e for e in X.receipts(record, BY.HELPER) if e["event"] == "pa-pre-image"
                and e["data"]["attempt"] == 2]                         # nothing recorded for the changed object
    monkeypatch.undo()
    assert any("reviewed chain" in x for x in BY.plan_query(c, 3)["problems"])   # never adopted later
