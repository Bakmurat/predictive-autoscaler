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
    ev = os.path.join(record.evidence_dir, "startup-x")
    os.makedirs(ev)
    sha = X.write_new(os.path.join(ev, "evidence.json"), "{}\n")
    X.receipt(record, "bounded", "startup-complete", {"run": "s-x", "measured": True, "dir": ev,
                                                      "evidence_sha256": sha})
    clock = Clock(O - 6 * 3600)
    return record, kube, a, clock, BY.Ctx(record, kube, now=clock.now, sleep=clock.sleep)


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
    (lambda record, c, clock: X.receipt(record, BY.HELPER, "boundary-failed", {"attempt": 1}), "used before"),
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


def test_the_hold_proof_needs_keep_current_decisions_and_no_applied_write(world):  # noqa: F811
    record, kube, a, clock, c = _query_done(world)
    clock.sleep(600)
    at = datetime.fromtimestamp(clock.ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = [{"autoscaler_name": t, "at": at, "desired_source": "keep_current", "applied_replicas": 12,
             "current_replicas": 12} for t in (N, R)]

    def vm(value):
        return lambda url: {"status": "success", "data": {"result": [{"metric": {}, "value": [0, str(value)]}]}}
    assert BY.prove_hold(c, 1, vm(0), "http://vm", rows)["proven"]
    assert not BY.prove_hold(c, 1, vm(1), "http://vm", rows)["proven"]          # an applied write
    bad = copy.deepcopy(rows)
    bad[0]["desired_source"] = "reactive"
    assert not BY.prove_hold(c, 1, vm(0), "http://vm", bad)["proven"]
    assert not BY.prove_hold(c, 1, vm(0), "http://vm", [])["proven"]            # no decision seen at all


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
    a.put(pod)
    X.receipt(record, BY.HELPER, "boundary-failed", {"attempt": 1})
    p = BY.plan_retire(c)
    assert p["problems"] == [] and {x["delete"] for x in p["actions"]} == {"StatefulSet", "Service", "ConfigMap"}
    s = X.save_plan(record, BY.HELPER, p, "retire")
    out = BY.retire(c, X.load_plan(s["plan"]), s["plan_digest"], logs=lambda name: ['{"k6": "log"}'])
    assert out["data"]["log_sha256"]
    for kind in ("StatefulSet", "Service", "ConfigMap", "Pod"):
        assert not [k for k, v in a.objects.items() if v.get("kind") == kind
                    and v["metadata"]["name"].startswith(c.c28.demo.GENERATOR_NAME)], kind
    ops = [(m["op"], m["kind"]) for m in c.kube.mutations]
    assert ops.index(("delete", "StatefulSet")) < ops.index((BD.GONE_OP, "Pod")) < ops.index(("delete", "ConfigMap"))
    assert not os.path.exists(X.lock_file(record))
