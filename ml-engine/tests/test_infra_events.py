"""deploy/prodcluster/infra_events.py v5: P8 detector on raw samples (Codex r30–r35 cases)."""
import fcntl
import hashlib
import importlib.util
import io
import json
import os
import re
import urllib.parse

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PROD = os.path.join(HERE, "..", "..", "deploy", "prodcluster")
SPEC = importlib.util.spec_from_file_location("infra_events", os.path.join(PROD, "infra_events.py"))
ie = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ie)
IDENT = json.load(open(os.path.join(PROD, "infra-identities.json")))
APPS = list(IDENT["generators"])
NODES = IDENT["workers"]

MIN, HOUR, DAY = 60_000, ie.HOUR, 24 * ie.HOUR
S = 1_791_331_200_000        # 2026-10-07T00:00:00.000Z
E = S + 2 * HOUR
LO = S - ie.PRE
KSM = list(range(LO - LO % 20_000 + 7_000, E + 1, 20_000))
EXPORTER = {"job": "kube-state-metrics", "instance": "10.42.12.226:8080"}


def parse_selector(sel):
    m = re.fullmatch(r"([a-zA-Z_:][\w:]*)?\{(.*)\}", sel)
    out = [("__name__", "=", m.group(1))] if m.group(1) else []
    out += re.findall(r'(\w+)(=~|=)"((?:[^"\\]|\\.)*)"', m.group(2))
    return out


class Lab:
    """Synthetic raw samples with the label layout of kube-state-metrics and k6."""

    def __init__(self):
        self.records, self.fail = [], None

    def add(self, labels, samples):
        self.records.append((labels, samples))

    def fetch(self, selector, lo, hi):
        if self.fail and self.fail in selector:
            raise RuntimeError("export failed")
        want = parse_selector(selector)
        ok = lambda m: all((re.fullmatch(v, m.get(k, "")) if op == "=~" else m.get(k) == v) for k, op, v in want)
        out = []
        for m, s in self.records:
            xs = [(t, v) for t, v in s if lo <= t <= hi]
            if ok(m) and xs:
                out.append((m, xs))
        return out

    def fetch_chunks(self, selector, lo, hi):
        c = lo
        while c <= hi:
            yield self.fetch(selector, c, min(c + ie.CHUNK - 1, hi))
            c += ie.CHUNK

    def drop(self, **labels):
        self.records = [(m, s) for m, s in self.records if not all(m.get(k) == v for k, v in labels.items())]


def pod(lab, ns, name, a=LO, b=E, created=None, start=None, generator=False, sched=None, reason=None,
        restarts=None, running=None, stale=True, info=None, exporter=EXPORTER):
    """A pod observed at every KSM scrape in [a, b]; `sched(t)` → condition, `reason` {reason: from_t},
    `restarts`/`running` [(t, value)] replace the defaults, `info` (from, until) narrows the inventory series."""
    ts = [t for t in KSM if a <= t <= b]
    end = [t for t in KSM if t > b][:1] if stale and b < E else []
    mark = lambda xs: xs + [(t, None) for t in end]
    base = dict(exporter, namespace=ns, pod=name)
    i0, i1 = info or (a, b)
    lab.add(dict(base, __name__="kube_pod_info", node=NODES[0], uid="u-" + name),
            mark([(t, 1.0) for t in ts if i0 <= t <= i1]) if not info or i1 >= b else [(t, 1.0) for t in ts if i0 <= t <= i1])
    for c in ie.SCHEDULED:
        lab.add(dict(base, __name__="kube_pod_status_scheduled", condition=c),
                mark([(t, 1.0 if (sched(t) if sched else "true") == c else 0.0) for t in ts]))
    for r in ie.POD_REASONS:
        frm = (reason or {}).get(r)
        lab.add(dict(base, __name__="kube_pod_status_reason", reason=r),
                mark([(t, 1.0 if frm is not None and t >= frm else 0.0) for t in ts]))
    lab.add(dict(base, __name__="kube_pod_created"), mark([(t, (created if created is not None else a) / 1000) for t in ts]))
    if generator:
        st = start if start is not None else (created if created is not None else a) + 5_000
        lab.add(dict(base, __name__="kube_pod_start_time"), mark([(t, st / 1000) for t in ts if t >= st]))
        lab.add(dict(base, __name__="kube_pod_container_status_restarts_total", container="k6"),
                mark(restarts if restarts is not None else [(t, 0.0) for t in ts if t >= st]))
        lab.add(dict(base, __name__="kube_pod_container_status_running", container="k6"),
                mark(running if running is not None else [(t, 1.0) for t in ts if t >= st]))


def heartbeats(lab, app, a=LO, b=E, holes=()):
    lab.add({"__name__": "k6_vus", "testid": app},
            [(t + 400, 30.0) for t in range(a - a % 10_000, b + 1, 10_000)
             if a <= t + 400 <= b and not any(x < t + 400 < y for x, y in holes)])


def healthy(nodes=NODES, skip_gen=()):
    lab = Lab()
    for node in nodes:
        for c in ie.CONDITIONS:
            lab.add(dict(EXPORTER, __name__="kube_node_status_condition", node=node, condition=c, status="true"),
                    [(t, 1.0 if c == "Ready" else 0.0) for t in KSM])
    for app in APPS:
        pod(lab, "demo", f"{app}-77d6cb8d96-9sb2p", created=LO - DAY)
        if app not in skip_gen:
            pod(lab, "demo", f"k6-{app}-5645f9f848-fh4wl", created=LO - DAY, generator=True)
            heartbeats(lab, app)
    pod(lab, "ml-engine", "ml-api-ff49c76f9-cj6wv", created=LO - DAY)
    return lab


def gate(status="PASS", finalized=True, qualifies=True, apps=APPS):
    return {a: {"status": status, "maturity": {"finalized": finalized}, "qualification": {"qualifies": qualifies}}
            for a in apps}


GOOD = {h: gate() for h in range(S - HOUR, E, HOUR)}


def run(lab, mask=None, gates=GOOD, att=None, start=S, stop=E):
    ev, unk = ie.detect(lab, start, stop, IDENT, mask, att)
    slots, targets, summary, _ = ie.classify(ev, unk, start, stop, APPS, gates)
    return ev, unk, slots, targets, summary


def kinds(ev):
    return sorted((e["class"], e["kind"], e["status"]) for e in ev)


def sources(unk):
    return [u["source"] for u in unk]


def test_healthy_window_with_finalized_gates_is_verified_clean():
    ev, unk, slots, targets, s = run(healthy())
    assert ev == [] and unk == []
    assert s["verified_clean"] == s["total"] == 12 and not s["compromised"]
    assert set(targets.values()) == {"verified_clean"}


def test_r32_missing_pressure_series_of_one_worker_is_unknown():
    lab = healthy()
    lab.drop(node=NODES[0], condition="DiskPressure")
    ev, unk, slots, targets, s = run(lab)
    assert any(x.endswith("condition DiskPressure") for x in sources(unk))
    assert s["verified_clean"] == 0 and s["compromised"]


def test_r32_restart_split_across_export_lines_is_detected():
    lab = healthy(skip_gen=("nginx-test",))
    name = "k6-nginx-test-5645f9f848-fh4wl"
    t1 = S + 30 * MIN + 7_000
    pod(lab, "demo", name, created=LO - DAY, generator=True, restarts=[])
    labels = dict(EXPORTER, namespace="demo", pod=name, __name__="kube_pod_container_status_restarts_total", container="k6")
    lab.add(labels, [(t, 0.0) for t in KSM if t < t1])
    lab.add(labels, [(t, 1.0) for t in KSM if t >= t1])
    heartbeats(lab, "nginx-test", holes=[(t1 - 15_000, t1 + 15_000)])
    ev, unk, slots, targets, s = run(lab)
    rs = [e for e in ev if e["kind"] == "k6 container restart"]
    assert len(rs) == 1 and rs[0]["status"] == "invalid"
    assert rs[0]["start"] <= t1 - 20_000 and rs[0]["end"] >= t1          # bracketed by this incarnation's heartbeats
    assert slots[S + 10 * MIN] == "verified_clean" and slots[S + 30 * MIN] == "invalid"


def test_r32_malformed_conflicting_nonfinite_and_out_of_domain_exports_are_rejected():
    line = lambda d: (json.dumps(d) + "\n").encode()
    with pytest.raises(ValueError):
        ie.parse_lines(line({"metric": {"a": "b"}, "timestamps": [1, 2], "values": [1]}))
    with pytest.raises(ValueError):
        ie.parse_lines(b'{"metric": {}, "timestamps": [1], "values": [NaN]}\n')
    with pytest.raises(ValueError):
        ie.parse_lines(line({"metric": {}, "timestamps": [1.5], "values": [1]}))
    recs = ie.parse_lines(line({"metric": {"x": "1"}, "timestamps": [2, 1], "values": [5, None]})
                          + line({"metric": {"x": "1"}, "timestamps": [2, 3], "values": [5, 6]}))
    assert ie.normalize(recs) == [({"x": "1"}, [(1, None), (2, 5.0), (3, 6.0)])]
    with pytest.raises(ValueError):
        ie.normalize([({"x": "1"}, [(2, 5.0)]), ({"x": "1"}, [(2, 6.0)])])
    lab = healthy()
    lab.drop(node=NODES[2], condition="PIDPressure")
    lab.add(dict(EXPORTER, __name__="kube_node_status_condition", node=NODES[2], condition="PIDPressure", status="true"),
            [(t, 2.0) for t in KSM])
    with pytest.raises(ValueError):
        run(lab)


def test_r32_milliseconds_are_preserved():
    assert ie.iso(S + 123) == "2026-10-07T00:00:00.123Z" and ie.parse("2026-10-07T00:00:00.123Z") == S + 123
    lab = healthy(skip_gen=("myapptwo",))
    pod(lab, "demo", "k6-myapptwo-5645f9f848-fh4wl", created=LO - DAY, generator=True)
    heartbeats(lab, "myapptwo", holes=[(S + 10 * MIN, S + 11 * MIN)])
    ev, *_ = run(lab)
    gap = [e for e in ev if e["kind"] == "k6 heartbeat gap"]
    assert len(gap) == 1 and gap[0]["id"].endswith(".400Z") and gap[0]["start"] % 1000 == 400


def test_r32_pod_reason_transition_is_the_event_not_the_terminal_marker():
    lab = healthy()
    x = S + 10 * MIN + 7_000
    pod(lab, "demo", "nginx-reactive-77d6cb8d96-zz9zz", created=LO - DAY, reason={"Evicted": x})
    ev, unk, slots, targets, s = run(lab)
    (b,) = [e for e in ev if e["class"] == "b"]
    assert (b["start"], b["end"], b["status"]) == (x - 20_000, x, "unknown")
    assert slots[S + 10 * MIN] == "unknown" and slots[S + 80 * MIN] == "verified_clean"


def test_r32_pod_reason_without_a_preceding_observation_stays_open():
    lab = healthy()
    pod(lab, "demo", "nginx-reactive-77d6cb8d96-zz9zz", created=LO - DAY, reason={"Evicted": LO - MIN})
    ev, *_ = run(lab)
    (b,) = [e for e in ev if e["class"] == "b"]
    assert b["open_start"] and b["start"] == b["end"]


def test_r32_dropped_iterations_first_positive_sample_and_reset():
    lab = healthy()
    t1, t2 = S + 20 * MIN, S + 50 * MIN
    lab.add({"__name__": "k6_dropped_iterations_total", "testid": "nginx-seasonal", "scenario": "pattern"},
            [(t1, 3.0), (t1 + 10_000, 3.0), (t1 + 20_000, None), (t2, 2.0)])
    ev, *_ = run(lab)
    drops = sorted(e["end"] for e in ev if e["kind"] == "k6 dropped iterations")
    assert drops == [t1, t2]
    assert all(e["status"] == "unknown" for e in ev if e["kind"] == "k6 dropped iterations")


def test_r32_mask_explains_the_inside_but_keeps_the_outer_tail():
    lab = healthy(skip_gen=("nginx-ensemble",))
    pod(lab, "demo", "k6-nginx-ensemble-5645f9f848-fh4wl", created=LO - DAY, generator=True)
    m0, m1 = S + 30 * MIN, S + 31 * MIN
    heartbeats(lab, "nginx-ensemble", holes=[(m0 - 5_000, m1 + 20_000)])
    mask = {"intervals": [{"start": ie.iso(m0), "end": ie.iso(m1), "reason": "declared restart"}]}
    ev, *_ = run(lab, mask=mask)
    (g,) = [e for e in ev if e["kind"] == "k6 heartbeat gap"]
    assert g["explained_by"].startswith("e:") and g["segments"]
    assert all(b <= m0 or a >= m1 for a, b, _ in g["segments"])          # only the parts outside the mask remain
    assert any(st == "unknown" and b > m1 for a, b, st in g["segments"])


def test_r32_boundary_slot_and_target_need_every_hour_they_touch():
    gates = {h: g for h, g in GOOD.items() if h != S}
    ev, unk, slots, targets, s = run(healthy(), gates=gates)
    assert slots[S + 50 * MIN] == "unverified" and slots[S + 60 * MIN] == "verified_clean"
    assert targets[S + 60 * MIN] == "unverified" and targets[S + 70 * MIN] == "verified_clean"


@pytest.mark.parametrize("gates", [{}, {h: gate(finalized=False) for h in GOOD}, {h: gate(status="INCOMPLETE") for h in GOOD},
                                   {h: gate(apps=[a + "-x" for a in APPS]) for h in GOOD},
                                   {h: gate(qualifies=False) for h in GOOD}])
def test_r31_missing_provisional_or_wrong_gate_rows_are_never_clean(gates):
    *_, s = run(healthy(), gates=gates)
    assert s["verified_clean"] == 0 and s["unverified"] == s["total"] and s["compromised"]


def test_finalized_gate_fail_is_visible_but_outside_the_budget():
    gates = dict(GOOD)
    gates[S] = dict(gate(), **{"nginx-test": {"status": "FAIL", "maturity": {"finalized": True}}})
    *_, s = run(healthy(), gates=gates)
    assert s["gate_fail"] == 6 and s["budget_slots"] == 0


def test_r32_attribution_needs_the_schema_evidence_and_a_matching_candidate():
    lab = healthy()
    lab.drop(node=NODES[1], condition="Ready")
    lab.add(dict(EXPORTER, __name__="kube_node_status_condition", node=NODES[1], condition="Ready", status="true"),
            [(t, 0.0 if S + 10 * MIN <= t <= S + 15 * MIN else 1.0) for t in KSM])
    ev, *_ = run(lab)
    (a,) = [e for e in ev if e["class"] == "a"]
    assert a["status"] == "unknown"
    rec = {"id": a["id"], "attribution": "infrastructure", "evidence": ["THREAD.md 2026-10-07T10:20Z node reboot"],
           "recorded_by": "user", "recorded_at": "2026-10-08T00:00:00Z", "decision_ref": "U-31"}
    doc = lambda *rs: {"schema": ie.ATTRIBUTION_SCHEMA, "records": list(rs)}
    ev, *_ = run(lab, att=doc(rec))
    assert [e["status"] for e in ev if e["class"] == "a"] == ["invalid"]
    with pytest.raises(ValueError):
        run(lab, att={a["id"]: {"attribution": "system"}})
    with pytest.raises(ValueError):
        run(lab, att=doc(dict(rec, evidence=[])))
    with pytest.raises(ValueError):
        run(lab, att=doc(dict(rec, id="a:node not ready or under pressure:x:2026-10-07T00:00:00.000Z")))


def test_r31_unrelated_pods_are_ignored():
    lab = healthy()
    pod(lab, "demo", "unrelated-business-pod", created=LO - DAY, reason={"Evicted": S + MIN})
    ev, unk, *_ = run(lab)
    assert ev == [] and unk == []


def test_r31_scheduling_threshold_is_strict_and_ambiguity_is_unknown():
    def with_pending(lo_s, hi_s):
        lab = healthy()
        p0 = S + 30 * MIN + 7_000
        pod(lab, "demo", "nginx-test-77d6cb8d96-ab1cd", created=LO - DAY,
            sched=lambda t: "false" if p0 + lo_s <= t <= p0 + hi_s else "true")
        return [e for e in run(lab)[0] if e["class"] == "c"]
    assert with_pending(0, 40_000) == []                                   # 40 s observed, bracket 80 s
    (amb,) = with_pending(0, 120_000)                                      # exactly 120 s observed: not "> 120"
    assert amb["definite"] is False and amb["status"] == "unknown"
    (c,) = with_pending(0, 140_000)
    assert c["definite"] is True and c["kind"] == "unscheduled > 120 s"


def test_r33_scheduled_states_must_be_one_hot():
    lab = healthy()
    bad = S + 40 * MIN + 7_000
    pod(lab, "demo", "nginx-test-77d6cb8d96-ab1cd", created=LO - DAY)
    lab.drop(pod="nginx-test-77d6cb8d96-ab1cd", condition="unknown")
    lab.add(dict(EXPORTER, __name__="kube_pod_status_scheduled", namespace="demo", pod="nginx-test-77d6cb8d96-ab1cd",
                 condition="unknown"), [(t, 1.0 if t == bad else 0.0) for t in KSM] + [(KSM[-1] + 20_000, None)])
    ev, unk, slots, *_ = run(lab)
    assert any("not one-hot" in x for x in sources(unk)) and slots[S + 40 * MIN] == "unknown"


def test_r31_pre_window_mask_washout_is_not_clean():
    mask = {"intervals": [{"start": ie.iso(S - 40 * MIN), "end": ie.iso(S - 30 * MIN), "reason": "x"}]}
    ev, unk, slots, *_ = run(healthy(), mask=mask)
    assert slots[S] == slots[S + 30 * MIN] == "invalid" and slots[S + 40 * MIN] == "verified_clean"  # closed end


def test_r31_heartbeat_gap_is_found_from_raw_timestamps():
    lab = healthy(skip_gen=("nginx-test", "myapptwo"))
    for app, hole in (("nginx-test", 15_000), ("myapptwo", 45_000)):         # 30-s and 60-s spacing
        pod(lab, "demo", f"k6-{app}-5645f9f848-fh4wl", created=LO - DAY, generator=True)
        heartbeats(lab, app, holes=[(S + MIN, S + MIN + hole)])
    gaps = [e["subject"] for e in run(lab)[0] if e["kind"] == "k6 heartbeat gap"]
    assert gaps == ["myapptwo"]


def replaced_generator(lab, overlap=False, new_running=True):
    cut = S + 40 * MIN
    old, new = "k6-nginx-seasonal-697c8ddf44-7fqmn", "k6-nginx-seasonal-64c5d9f9f8-qrdzd"
    pod(lab, "demo", old, created=LO - DAY, generator=True, b=cut + (60_000 if overlap else -13_000))
    pod(lab, "demo", new, a=cut + 7_000, created=cut + 2_000, start=cut + 5_000, generator=True,
        running=None if new_running else [(t, 0.0) for t in KSM if t >= cut + 7_000])
    heartbeats(lab, "nginx-seasonal", holes=[] if overlap else [(cut - 10_000, cut + 15_000)])
    return cut


def test_new_generator_incarnation_is_invalid_and_a_declared_interval_explains_it():
    lab = healthy(skip_gen=("nginx-seasonal",))
    cut = replaced_generator(lab)
    ev, unk, slots, *_ = run(lab)
    assert ("d", "new generator incarnation", "invalid") in kinds(ev)
    assert slots[S + 30 * MIN] == "invalid" and slots[S + 20 * MIN] == "verified_clean"
    mask = {"intervals": [{"start": ie.iso(cut - 20_000), "end": ie.iso(cut + 40_000), "reason": "declared restart"}]}
    ev, *_ = run(lab, mask=mask)
    inc = [e for e in ev if e["kind"] == "new generator incarnation"]
    assert inc[0]["explained_by"] and inc[0]["segments"] == []


def test_r33_simultaneous_running_is_definite_object_overlap_alone_is_a_candidate():
    lab = healthy(skip_gen=("nginx-seasonal",))
    replaced_generator(lab, overlap=True)
    assert ("d", "simultaneous generators", "invalid") in kinds(run(lab)[0])
    lab = healthy(skip_gen=("nginx-seasonal",))
    replaced_generator(lab, overlap=True, new_running=False)
    k = kinds(run(lab)[0])
    assert ("d", "generator pod objects overlap", "unknown") in k
    assert ("d", "generator pod never observed running", "unknown") in k
    assert not any(x[1] in ("simultaneous generators", "new generator incarnation") for x in k)


def test_pod_family_coverage_gap_is_unknown():
    lab = healthy()
    lab.drop(pod="nginx-test-77d6cb8d96-9sb2p", reason="Shutdown")
    ev, unk, *_ = run(lab)
    assert "kube_pod_status_reason[Shutdown] demo/nginx-test-77d6cb8d96-9sb2p" in sources(unk)


def test_r33_inventory_that_stops_while_the_pod_continues_is_unknown():
    lab = healthy(skip_gen=("nginx-test",))
    pod(lab, "demo", "k6-nginx-test-5645f9f848-fh4wl", created=LO - DAY, generator=True, info=(LO, S + 10 * MIN))
    heartbeats(lab, "nginx-test")
    ev, unk, slots, targets, s = run(lab)
    assert any(x.startswith("pod series continue after kube_pod_info ends") for x in sources(unk))
    assert s["verified_clean"] == 0


def test_r33_inventory_that_starts_after_the_pods_creation_is_unknown():
    lab = healthy(skip_gen=("nginx-test",))
    pod(lab, "demo", "k6-nginx-test-5645f9f848-fh4wl", created=LO - DAY, generator=True, info=(S + 30 * MIN, E))
    heartbeats(lab, "nginx-test")
    ev, unk, slots, *_ = run(lab)
    assert any(x.startswith("kube_pod_info prefix") for x in sources(unk)) and slots[S + 20 * MIN] == "unknown"


def test_r33_restart_count_positive_at_a_late_first_observation_is_unresolved():
    lab = healthy(skip_gen=("nginx-test",))
    pod(lab, "demo", "k6-nginx-test-5645f9f848-fh4wl", created=LO - DAY, generator=True,
        restarts=[(t, 1.0) for t in KSM if t >= S + 30 * MIN])
    heartbeats(lab, "nginx-test")
    ev, unk, slots, *_ = run(lab)
    assert any(x.startswith("k6 restart count positive at first observation") for x in sources(unk))
    assert any(x.startswith("k6 container restarts") for x in sources(unk)) and slots[S + 20 * MIN] == "unknown"


def test_r33_pod_end_needs_a_marker_or_the_same_exporters_next_scrape():
    def inv(lab):
        ev = ie.Events(LO, E, {})
        return ie.inventory(lab, ev, "demo", IDENT["pod_patterns"]["demo"], LO, E), ev.unknown
    lab = healthy()
    pod(lab, "demo", "nginx-test-77d6cb8d96-ab1cd", created=S, a=S + 7_000, b=S + 30 * MIN + 7_000, stale=False)
    pods, unk = inv(lab)
    assert pods["nginx-test-77d6cb8d96-ab1cd"]["end"] == S + 30 * MIN + 27_000 and unk == []   # next scrape, same exporter
    solo = Lab()
    pod(solo, "demo", "nginx-test-77d6cb8d96-ab1cd", created=S, a=S + 7_000, b=S + 30 * MIN + 7_000, stale=False)
    pods, unk = inv(solo)
    assert pods["nginx-test-77d6cb8d96-ab1cd"]["end"] is None
    assert [u["source"] for u in unk] == ["pod end not established demo/nginx-test-77d6cb8d96-ab1cd"]


def test_r33_heartbeats_without_a_running_generator_are_unknown():
    lab = healthy(skip_gen=("myapptwo",))
    off = (S + 20 * MIN, S + 25 * MIN)
    pod(lab, "demo", "k6-myapptwo-5645f9f848-fh4wl", created=LO - DAY, generator=True,
        running=[(t, 0.0 if off[0] <= t <= off[1] else 1.0) for t in KSM])
    heartbeats(lab, "myapptwo")
    ev, unk, *_ = run(lab)
    assert any(x == "k6 heartbeats without a running generator myapptwo" for x in sources(unk))


def test_r33_coverage_washout_exposure_is_reported_separately():
    lab = healthy()
    lab.drop(node=NODES[3], condition="MemoryPressure")
    lab.add(dict(EXPORTER, __name__="kube_node_status_condition", node=NODES[3], condition="MemoryPressure", status="true"),
            [(t, 0.0) for t in KSM if not S + 10 * MIN < t < S + 13 * MIN])
    *_, s = run(lab)
    # the gap [09:47, 13:07] touches two slots; its washout (to 14:13:07) adds six more
    assert s["unknown"] == 8 and s["coverage_washout_extra_slots"] == 6


def test_failed_source_raises():
    lab = healthy()
    lab.fail = "k6_vus"
    with pytest.raises(RuntimeError):
        run(lab)


# ---------------------------------------------------------------------------------------------------- archive / main
def opener_for(lab, split=False, fail_once=None):
    state = {"failed": set()}

    def opener(url, timeout=None):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        sel = q["match[]"][0]
        if fail_once and fail_once in sel and sel not in state["failed"]:
            state["failed"].add(sel)
            raise ConnectionResetError("reset once")
        lo, hi = (int(round(float(q[k][0]) * 1000)) for k in ("start", "end"))
        out = b""
        for m, s in lab.fetch(sel, lo, hi):
            parts = [s[: len(s) // 2], s[len(s) // 2:]] if split and len(s) > 1 else [s]
            for p in parts:
                out += (json.dumps({"metric": m, "values": [v for _, v in p], "timestamps": [t for t, _ in p]}) + "\n").encode()
        return io.BytesIO(out)
    return opener


def files(tmp_path, gates=GOOD):
    gd = tmp_path / "gates"
    gd.mkdir(exist_ok=True)
    for h, rows in gates.items():
        (gd / f"load-{ie.iso(h)[:13].replace('-', '')}00Z.json").write_text(
            json.dumps([dict(r, app=a, hour_start=ie.iso(h)[:19] + "Z") for a, r in rows.items()]))
    (tmp_path / "mask.json").write_text(json.dumps({"intervals": []}))
    return ["--prom", "http://vm/select/0/prometheus", "--start", ie.iso(S), "--stop", ie.iso(E),
            "--identities", os.path.join(PROD, "infra-identities.json"), "--mask", str(tmp_path / "mask.json"),
            "--load-gate-dir", str(gd), "--archive-root", str(tmp_path / "arch"), "--json", str(tmp_path / "out.json")]


@pytest.fixture
def fast(monkeypatch):
    monkeypatch.setattr(ie, "CHUNK", HOUR)
    monkeypatch.setattr(ie, "RETRY_BACKOFF", 0.0)


def test_main_archives_chunks_with_hashes_and_binds_inputs(tmp_path, fast):
    assert ie.main(files(tmp_path), opener=opener_for(healthy(), split=True, fail_once="k6_vus")) == 0
    out = json.load(open(tmp_path / "out.json"))
    (attempt,) = (tmp_path / "arch").iterdir()
    man = json.load(open(attempt / "manifest.json"))
    assert len(man) == out["chunks"]["planned"] > len({e["selector"] for e in man})       # several chunks per selector
    for e in man:
        data = (attempt / e["file"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == e["sha256"] and len(data) == e["bytes"]
    assert out["manifest_sha256"] == hashlib.sha256((attempt / "manifest.json").read_bytes()).hexdigest()
    assert len(out["load_gate_files"]) == len(GOOD) and all(len(f["sha256"]) == 64 for f in out["load_gate_files"])
    assert out["summary"]["verified_clean"] == 12 and json.load(open(attempt / "result.json")) == out
    assert json.load(open(attempt / "run.json")) == out["run_identity"]


def test_main_failure_receipt_resume_repair_and_reanalysis(tmp_path, fast):
    lab = healthy()
    lab.fail = "k6_vus"
    args = files(tmp_path)
    with pytest.raises(RuntimeError):
        ie.main(args, opener=opener_for(lab))
    (attempt,) = (tmp_path / "arch").iterdir()
    (receipt,) = attempt.glob("failure-*.json")
    assert "export failed" in json.load(open(receipt))["error"] and not (tmp_path / "out.json").exists()
    first = sorted((attempt / "raw").glob("*.jsonl"))[0]
    first.write_bytes(first.read_bytes() + b" ")                            # a tampered chunk is kept and fetched again
    lab.fail = None
    assert ie.main(args + ["--resume", str(attempt)], opener=opener_for(lab)) == 0
    out = json.load(open(tmp_path / "out.json"))
    assert 0 < out["chunks"]["reused"] < out["chunks"]["planned"]
    (rep,) = out["chunks"]["repairs"]
    assert (attempt / rep["kept_as"]).exists() and out["summary"]["verified_clean"] == 12
    with pytest.raises(ValueError):                                         # complete: no resume
        ie.main(args + ["--resume", str(attempt)], opener=opener_for(lab))

    def offline(url, timeout=None):
        raise AssertionError("re-analysis must not export")
    assert ie.main(args + ["--reanalyze", str(attempt)], opener=offline) == 0
    assert (attempt / "result-r1.json").exists() and json.load(open(attempt / "result.json"))["revision"] == 0
    other = files(tmp_path)
    other[other.index("--stop") + 1] = ie.iso(E + HOUR)
    with pytest.raises(ValueError):                                         # another run identity
        ie.main(other + ["--reanalyze", str(attempt)], opener=offline)


def test_one_writer_per_attempt_and_new_attempts_never_reuse_a_directory(tmp_path):
    ident = {"prom": "x"}
    ie.Archive("http://vm", str(tmp_path / "a"), ident)
    holder = open(tmp_path / "a" / ".lock", "w")
    fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with pytest.raises(RuntimeError):
        ie.Archive("http://vm", str(tmp_path / "a"), ident, mode="resume")
    with pytest.raises(FileExistsError):
        ie.Archive("http://vm", str(tmp_path / "a"), ident)


def test_only_a_torn_last_manifest_line_is_tolerated(tmp_path):
    ident = {"prom": "x"}
    ie.Archive("http://vm", str(tmp_path / "a"), ident).lock.close()
    good = json.dumps({"file": "raw/x", "selector": "s", "start_ms": 1, "end_ms": 2, "bytes": 0, "sha256": "0"})
    (tmp_path / "a" / "manifest.jsonl").write_text(good + "\n" + '{"file": "raw/y", "sel')
    arch = ie.Archive("http://vm", str(tmp_path / "a"), ident, mode="resume")
    assert arch.torn and list(arch.entries) == [("s", 1, 2)]
    arch.lock.close()
    (tmp_path / "a" / "manifest.jsonl").write_text('{"broken\n' + good + "\n")
    with pytest.raises(ValueError):
        ie.Archive("http://vm", str(tmp_path / "a"), ident, mode="resume")
    (tmp_path / "a" / "manifest.jsonl").write_text(good + "\n" + '{"file": "raw/y", "sel\n')   # complete but malformed
    with pytest.raises(ValueError):
        ie.Archive("http://vm", str(tmp_path / "a"), ident, mode="resume")


def test_r35_the_deadline_also_bounds_a_trickling_download(tmp_path, monkeypatch):
    clock = {"t": 0}
    monkeypatch.setattr(ie, "now_ms", lambda: clock["t"])

    class Trickle(io.BytesIO):
        def read(self, n=-1):
            clock["t"] += 70_000                                            # every read takes 70 s
            return b"x"

    arch = ie.Archive("http://vm", str(tmp_path / "a"), {"prom": "x"}, deadline=200_000,
                      opener=lambda url, timeout: Trickle())
    with pytest.raises(TimeoutError):
        arch.fetch("k6_vus", 0, 1000)


def test_r35_missing_creation_leaves_the_earlier_lifetime_unresolved():
    lab = healthy()
    name, base = "nginx-test-77d6cb8d96-ab1cd", dict(EXPORTER, namespace="demo", pod="nginx-test-77d6cb8d96-ab1cd")
    t30 = S + 30 * MIN
    lab.add(dict(base, __name__="kube_pod_info"), [(t, 1.0) for t in KSM if t >= t30])
    lab.add(dict(base, __name__="kube_pod_status_scheduled", condition="true"), [(t, 1.0) for t in KSM if t >= t30])
    lab.add(dict(base, __name__="kube_pod_status_scheduled", condition="false"),
            [(t, 1.0 if t < S + 20 * MIN else 0.0) for t in KSM if t >= S + 10 * MIN])
    lab.add(dict(base, __name__="kube_pod_status_scheduled", condition="unknown"), [(t, 0.0) for t in KSM if t >= S + 10 * MIN])
    for r in ie.POD_REASONS:
        lab.add(dict(base, __name__="kube_pod_status_reason", reason=r), [(t, 0.0) for t in KSM if t >= S + 10 * MIN])
    ev, unk, slots, *_ = run(lab)
    assert any(x.startswith("kube_pod_created missing") for x in sources(unk))
    assert slots[S + 10 * MIN] == "unknown" and slots[S + 20 * MIN] == "unknown"


def test_r35_any_continuing_pod_series_reopens_its_lifetime_and_is_read():
    lab = healthy()
    name, t10, t30 = "nginx-reactive-77d6cb8d96-zz9zz", S + 10 * MIN, S + 30 * MIN + 7_000
    pod(lab, "demo", name, created=LO - DAY, reason={"Evicted": t30})
    for m, smp in lab.records:
        if m.get("pod") == name and (m["__name__"] == "kube_pod_info" or m.get("condition") == "true"):
            smp[:] = [(t, v) for t, v in smp if t <= t10]
    ev, unk, slots, *_ = run(lab)
    assert any(x.startswith("pod series continue after kube_pod_info ends") for x in sources(unk))
    assert [e["kind"] for e in ev if e["class"] == "b"] == ["Evicted"]


def test_r35_another_exporter_does_not_close_a_pods_lifetime():
    lab = Lab()
    other = dict(EXPORTER, instance="10.42.9.9:8080")
    pod(lab, "demo", "nginx-test-77d6cb8d96-ab1cd", created=LO - DAY, b=S + 30 * MIN, stale=False)
    pod(lab, "demo", "nginx-reactive-77d6cb8d96-zz9zz", created=LO - DAY, exporter=other)
    ev = ie.Events(LO, E, {})
    pods = ie.inventory(lab, ev, "demo", IDENT["pod_patterns"]["demo"], LO, E)
    assert pods["nginx-test-77d6cb8d96-ab1cd"]["end"] is None
    assert "pod end not established demo/nginx-test-77d6cb8d96-ab1cd" in [u["source"] for u in ev.unknown]


def test_r36_a_pod_seen_only_through_staleness_markers_is_unknown_not_a_crash():
    lab = healthy()
    lab.add(dict(EXPORTER, __name__="kube_pod_info", namespace="demo", pod="nginx-test-77d6cb8d96-qq1qq"), [(LO + 7_000, None)])
    ev, unk, *_ = run(lab)
    assert "pod seen only through staleness markers demo/nginx-test-77d6cb8d96-qq1qq" in sources(unk)


def test_r36_a_watchdog_ends_a_read_that_keeps_trickling_past_the_deadline(tmp_path):
    import threading, time

    class Endless:
        """Like http.client's read(n): one call keeps collecting bytes until n arrive or the stream closes."""
        def __init__(self):
            self.closed = threading.Event()

        def read(self, n=-1):
            while not self.closed.is_set():
                time.sleep(0.01)
            raise ValueError("I/O operation on closed file")

        def close(self):
            self.closed.set()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.close()

    arch = ie.Archive("http://vm", str(tmp_path / "a"), {"prom": "x"}, deadline=ie.now_ms() + 300,
                      opener=lambda url, timeout: Endless())
    t0 = time.time()
    with pytest.raises(TimeoutError):
        arch.fetch("k6_vus", 0, 1000)
    assert time.time() - t0 < 5


def test_r38_dropped_counter_domain_and_start_time_coverage():
    lab = healthy()
    lab.add({"__name__": "k6_dropped_iterations_total", "testid": "nginx-test"}, [(S + MIN, 1.5)])
    with pytest.raises(ValueError):
        run(lab)
    lab = healthy()
    for m, smp in lab.records:
        if m.get("pod") == "k6-nginx-test-5645f9f848-fh4wl" and m["__name__"] == "kube_pod_start_time":
            smp[:] = [(t, v) for t, v in smp if not S + 10 * MIN < t < S + 15 * MIN]
    ev, unk, *_ = run(lab)
    assert "k6 container start time demo/k6-nginx-test-5645f9f848-fh4wl" in sources(unk)
