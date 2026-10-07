"""deploy/prodcluster/infra_events.py: P8 infrastructure-event detector (Codex r27/r30)."""
import importlib.util
import json
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location(
    "infra_events", os.path.join(HERE, "..", "..", "deploy", "prodcluster", "infra_events.py"))
ie = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ie)

S = 1_791_331_200            # 2026-10-07T00:00:00Z
E = S + 6 * 3600
STEP = 30


class FakeVM:
    """Scripted query_range: {substring: [(labels, [(from, to)])]} -> timestamps on the step grid inside the ranges."""

    def __init__(self, ksm_gaps=(), series=None, fail=None):
        self.ksm_gaps, self.series, self.fail = ksm_gaps, series or {}, fail

    def range(self, query, start, end, step):
        if self.fail and self.fail in query:
            raise RuntimeError("query failed or partial")
        grid = list(range(start - start % step, end + 1, step))
        if query.startswith('max(up{job="kube-state-metrics"})'):
            return [({}, [t for t in grid if not any(a <= t < b for a, b in self.ksm_gaps)])]
        for key, out in self.series.items():
            if key in query:
                return [(lab, [t for t in grid if any(a <= t <= b for a, b in rs)]) for lab, rs in out]
        return []


def run(vm, mask=None, gates=None, start=S, stop=E):
    ev, unk = ie.detect(vm, start, stop, step=STEP, mask=mask)
    slots, summary = ie.classify(ev, unk, start, stop, gates)
    return ev, unk, slots, summary


def test_healthy_window_is_verified_clean():
    ev, unk, slots, s = run(FakeVM())
    assert ev == [] and unk == [] and s["verified_clean"] == s["total"] == 36 and not s["compromised"]


def test_missing_source_is_unknown_never_clean():
    ev, unk, slots, s = run(FakeVM(ksm_gaps=[(S + 3600, S + 3600 + 900)]))
    assert unk and unk[0]["source"] == "kube-state-metrics"
    assert s["unknown"] == 2 and slots[S + 3600] == "unknown" and slots[S + 3600 + 600] == "unknown"
    assert slots[S + 3600 - 600] == "verified_clean"


@pytest.mark.parametrize("status", ["false", "unknown"])
def test_node_not_ready_or_unknown_invalidates_with_outward_bracketing_and_washout(status):
    vm = FakeVM(series={'condition="Ready"': [({"node": "w1", "condition": "Ready", "status": status},
                                                [(S + 3605, S + 3900)])]})
    ev, unk, slots, s = run(vm)
    assert [e["class"] for e in ev] == ["a"] and ev[0]["start"] < S + 3605 and ev[0]["end"] > S + 3900
    # rounded outward to the grid, then the declared 60-min washout
    assert slots[S + 3000] == "verified_clean" and slots[S + 3600] == "invalid"
    assert slots[S + 4200 + 3600 - 600] == "invalid" and slots[S + 4200 + 3600] == "verified_clean"


def test_scheduling_blockage_needs_more_than_120_s():
    short = FakeVM(series={'condition="false"': [({"namespace": "demo", "pod": "a"}, [(S + 600, S + 690)])]})
    assert run(short)[0] == []                               # 90 s: provisioning, part of readiness
    long = FakeVM(series={'condition="false"': [({"namespace": "demo", "pod": "a"}, [(S + 600, S + 750)])]})
    assert [e["class"] for e in run(long)[0]] == ["c"]


def test_eviction_is_an_event():
    vm = FakeVM(series={"kube_pod_status_reason": [({"namespace": "demo", "pod": "nginx-test-x", "reason": "Evicted"},
                                                     [(S + 7200, S + 7230)])]})
    assert [(e["class"], e["kind"]) for e in run(vm)[0]] == [("b", "Evicted")]


def test_generator_gap_is_explained_only_by_a_declared_mask_interval():
    gap = {'k6_vus{testid="nginx-test"}': [({}, [(S + 1830, S + 1860)])]}
    ev = run(FakeVM(series=gap))[0]
    assert [(e["class"], e["kind"]) for e in ev] == [("d", "k6 heartbeat gap")]
    mask = {"intervals": [{"start": ie.iso(S + 1700), "end": ie.iso(S + 1950), "reason": "declared restart"}]}
    ev = run(FakeVM(series=gap), mask=mask)[0]
    assert [e["class"] for e in ev] == ["e"]                 # the declared interval explains the gap


def test_load_gate_incomplete_is_unverified_and_fail_is_not_an_event():
    gates = {S: {a: "PASS" for a in ie.APPS}, S + 3600: dict({a: "PASS" for a in ie.APPS}, myapptwo="INCOMPLETE"),
             S + 7200: dict({a: "PASS" for a in ie.APPS}, myapptwo="FAIL")}
    ev, unk, slots, s = run(FakeVM(), gates=gates)
    assert ev == [] and slots[S] == "verified_clean" and slots[S + 3600] == "unverified"
    assert slots[S + 7200] == "unverified" and slots[S + 3 * 3600] == "unverified"   # no gate row = not verified


def test_compromised_when_invalid_plus_unknown_exceed_20_percent():
    vm = FakeVM(ksm_gaps=[(S, S + 3 * 3600)])
    assert run(vm)[3]["compromised"]


def test_slots_stop_at_the_stop_and_a_failed_query_raises():
    slots = run(FakeVM(), stop=S + 3600 + 300)[2]
    assert max(slots) == S + 3600 and min(slots) == S
    with pytest.raises(RuntimeError):
        run(FakeVM(fail="kube_pod_status_reason"))


def test_gate_hours_reads_latest_projections(tmp_path):
    rows = [{"app": a, "hour_start": ie.iso(S), "status": "PASS"} for a in ie.APPS]
    (tmp_path / "load-20261007T0000Z.json").write_text(json.dumps(rows))
    assert ie.gate_hours(str(tmp_path)) == {S: {a: "PASS" for a in ie.APPS}}
