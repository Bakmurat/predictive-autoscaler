"""c28.demo_admission: the admission verdict recomputed from an evidence file. The generator side is the REAL script run
under the simulator for the whole 40 minutes; the cluster-side observations are synthetic and edited one at a time."""

import copy
import json

import pytest

from c28 import demo, demo_admission as A
from demo_helpers import MIN, params, scrape, simulate

ORIGIN = 1793491200000 + 17 * 3600_000 + 23 * MIN
N, R = "c28-demo-pa-n", "c28-demo-pa-r"


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    cfg = demo.bounded_config(params(), "admission", ORIGIN)
    out = simulate(tmp_path_factory.mktemp("adm"), cfg,
                   {"incarnations": [{"pod_uid": "pod-a", "start_ms": ORIGIN - 3 * MIN}], "until_ms": ORIGIN + 45 * MIN,
                    "ingress_initial": 100, "scrape": scrape(ORIGIN - 5 * MIN, ORIGIN + 45 * MIN)})
    o = ORIGIN / 1000
    pods = {t: [f"{t}-pod-{i:02d}" for i in range(12)] for t in (N, R)}
    steps = [o + 15 * k for k in range(0, 40 * 4)]
    ev = {"schema": A.SCHEMA, "config": cfg, "lines": [json.dumps(l) for l in out["lines"]], "ingress": out["ingress"],
          "queue": {t: [[s, 2.0] for s in steps] for t in (N, R)},
          "throttled": {k: [[s, 0.01] for s in steps] for k in ("generator", "ingress", "fixture")},
          "steal": {node: [[o + 60 * k + 30, 1.0] for k in range(40)] for node in ("w", "s")},
          "endpoints": [{"t_s": o + 30 * k, "target": t, "ready": list(pods[t])} for k in range(81) for t in (N, R)],
          "pod_requests": {t: {u: 1000 for u in pods[t]} for t in (N, R)},
          "pods": {"expected": pods, "baseline_s": o - 120, "completion_s": o + 40 * 60 + 20,
                   "watch": [{"from_s": o - 200, "to_s": o + 41 * 60}], "transitions": []},
          "freshness": {"ingress": 20, "queue": 20, "throttled": 25, "steal": 50},
          "job": {"status": "Complete", "exit_code": 0, "created_s": o - 180, "pod_started_s": o - 170},
          "stops": [], "limits": copy.deepcopy(A.FROZEN)}
    return ev


def run(ev, edit=None):
    ev = copy.deepcopy(ev)
    if edit:
        edit(ev)
    return A.verdict(ev)


def test_a_clean_run_passes(base):
    v = run(base)
    assert v["verdict"] == "PASS", v["reasons"]
    assert v["criteria"][f"{N}/latency"]["outcomes"] > 0 and v["criteria"]["throttled/fixture"]["result"] == "DIAGNOSTIC"


def _first_measured_line(ev, target):
    for i, raw in enumerate(ev["lines"]):
        l = json.loads(raw)
        if l.get("schema") == "c28-demo-minute/v3" and l["target"] == target and l["minute"] == 15:
            return i, l
    raise AssertionError("no line")


def slow(ev):                                     # a measured minute whose outcomes all took over 500 ms
    i, l = _first_measured_line(ev, N)
    n = sum(l["lat"]["all"])
    l["lat"]["all"] = [0] * (len(l["lat"]["all"]) - 1) + [n]
    l["lat"]["counts"] = [0] * (len(l["lat"]["counts"]) - 1) + [l["ok"]]
    ev["lines"][i] = json.dumps(l)


def missing_line(ev):
    i, _ = _first_measured_line(ev, R)
    del ev["lines"][i]


@pytest.mark.parametrize("edit, want, key", [
    (lambda ev: ev["stops"].append({"t_s": 1, "why": "node pressure on S"}), "STOPPED", "stops"),
    (lambda ev: ev["steal"]["w"].__setitem__(slice(10, 16), [[x[0], 9.0] for x in ev["steal"]["w"][10:16]]),
     "STOPPED", "steal/w"),
    (missing_line, "INCOMPLETE", f"{R}/rows"),
    (lambda ev: ev["job"].update(exit_code=1, status="Failed"), "INCOMPLETE", "job"),
    (lambda ev: ev["freshness"].update(queue=95), "INCOMPLETE", "freshness"),
    (lambda ev: ev["queue"][N].__delitem__(slice(80, 90)), "INCOMPLETE", f"{N}/queue"),
    (lambda ev: ev["pods"]["watch"].__setitem__(0, {"from_s": ev["pods"]["baseline_s"] + 5, "to_s": 9e12}),
     "INCOMPLETE", "pods"),
    (lambda ev: ev.update(limits=dict(ev["limits"], max_failure_fraction=0.01)), "INCOMPLETE", None),
    (lambda ev: ev["throttled"]["generator"].__setitem__(slice(None), [[t, 0.2] for t, _ in ev["throttled"]["generator"]]),
     "LIMIT", "throttled/generator"),
    (slow, "FAIL", f"{N}/latency"),
    (lambda ev: ev["queue"][R].__setitem__(slice(None), [[t, 30.0] for t, _ in ev["queue"][R]]), "FAIL", f"{R}/queue"),
    (lambda ev: ev["queue"][R].__setitem__(slice(None), [[t, (t - ev["config"]["origin_ms"] / 1000) / 30]
                                                         for t, _ in ev["queue"][R]]), "FAIL", f"{R}/queue"),
    (lambda ev: ev["endpoints"][50]["ready"].pop(), "FAIL", None),
    (lambda ev: ev["pod_requests"][N].update({f"{N}-pod-00": 5000}), "FAIL", f"{N}/distribution"),
    (lambda ev: ev["pods"]["transitions"].append({"t_s": ev["pods"]["baseline_s"] + 600, "pod_uid": "x",
                                                  "what": "ready-lost"}), "FAIL", "pods"),
])
def test_each_criterion_decides_with_its_own_outcome(base, edit, want, key):
    v = run(base, edit)
    assert v["verdict"] == want, v["reasons"]
    if key:
        assert key in v["reasons"], v["reasons"]


def test_precedence_stopped_over_incomplete_over_limit_over_fail(base):
    def all_of(ev):
        slow(ev)
        ev["throttled"]["ingress"] = [[t, 0.5] for t, _ in ev["throttled"]["ingress"]]
        ev["freshness"]["queue"] = 999
    assert run(base, all_of)["verdict"] == "INCOMPLETE"
    assert run(base, lambda ev: (all_of(ev), ev["stops"].append({"t_s": 1, "why": "x"})))["verdict"] == "STOPPED"


def test_a_bad_or_foreign_evidence_file_is_never_a_pass(base):
    assert run(base, lambda ev: ev.pop("steal"))["verdict"] == "INCOMPLETE"
    assert run(base, lambda ev: ev.update(schema="x"))["verdict"] == "INCOMPLETE"
    startup = demo.bounded_config(params(), "startup", ORIGIN)
    assert run(base, lambda ev: ev.update(config=startup))["verdict"] == "INCOMPLETE"


def test_a_line_whose_lat_all_disagrees_with_its_outcomes_is_not_observed(base):
    def edit(ev):
        i, l = _first_measured_line(ev, N)
        l["lat"]["all"][0] += 1                               # one outcome too many in the all-outcomes buckets
        ev["lines"][i] = json.dumps(l)
    v = run(base, edit)
    assert v["verdict"] == "INCOMPLETE" and f"{N}/rows" in v["reasons"]
    assert any("inconsistent_line" in r for _, r in v["criteria"][f"{N}/rows"]["not_observed"])
