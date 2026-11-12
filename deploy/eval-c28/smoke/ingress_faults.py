"""The verdict of demo-kind.sh --ingress-faults: the fixture's ingress and the generator on the final images.

Criteria fixed before any run (c28-staging/live-demo/INGRESS-FAULT-TEST-CRITERIA.md, Codex's 2026-11-12 decisions T1,
T2 and the phase layout, and its review of PR #83); nothing here may be loosened after a run has been looked at.

The result is PASS, FAIL or UNPROVEN. A run that is not the frozen test, or evidence that shows a criterion broken, is a
FAIL; evidence that is missing or cannot be judged makes the result UNPROVEN, never a pass.

  Setup (FAIL when not met: the run is not the frozen test)
    the six phases steady1, fault1, recovery1, steady2, fault2, recovery2 in this order, contiguous within each
    incarnation, every boundary on a schedule minute; faults and recoveries at least 300 s, steadies at least 180 s;
    fault1 makes exactly c28-demo-pa-r unreachable, fault2 both targets, direction "requests"; the ingress limit
    exactly 128 MiB; the schedule a constant-peak smoke schedule; steady1..recovery1 inside one incarnation of the
    generator pod and steady2..recovery2 inside the next one of the same pod (from the incarnations' start records and
    segment ends).
  Traffic (the reducer's rows: validated provenance, conservation and reconciliation)
    every minute of every phase except the first minute of each fault and recovery (the transition minute, where the
    blackhole is put on or taken off) must be exactly one reducer row per target, state "observed", intended equal to
    the schedule's constant peak count, everything dispatched, nothing dropped, and: an unreachable target's requests
    all failed; a reachable target's all succeeded. A missing or non-observed row is UNPROVEN; wrong counts are a FAIL.
  Ingress (fault1 start to recovery2 end, the generator's restart in between included)
    I1 identity and OOM: demo_kind.ingress_evidence_classified (classes, not text);
    I2 memory.peak over the whole run at most 64 MiB (50 % of 128 MiB);
    I3 for each fault and recovery: the largest memory.current in its last minute at most 110 % of its largest in its
       second minute, both exactly schedule minutes; memory.current readings must cover each window and the whole span
       within 5 s of each edge and with no gap over 15 s, else UNPROVEN.
  Generator (T2)
    G1 at least two consecutive planned restarts in the pod: the ended container (matched by its termination's times)
       ran more than 600 s and terminated Completed with code 0; the next incarnation started after the previous one
       ended; and its actual first send (the "first_send" record) to EACH target is 0 to 10 s after the segment end.
       This meets the restart-gap budget; it does not by itself prove the absence of kubelet back-off.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import demo_kind as K

PHASES = ("steady1", "fault1", "recovery1", "steady2", "fault2", "recovery2")
# The transition minutes: the last minute of each steady phase (the blackhole is installed in its last seconds, so
# that it is complete when the fault begins) and the first minute of each recovery (it is removed when the fault
# ends). Their accounting is judged like every other minute's; only the split between successes and failures is not.
TRANSITION_LAST = ("steady1", "steady2")
TRANSITION_FIRST = ("recovery1", "recovery2")
COMPARED = ("fault1", "recovery1", "fault2", "recovery2")
FAULTS = {"fault1": ["c28-demo-pa-r"], "fault2": ["c28-demo-pa-n", "c28-demo-pa-r"]}
LIMIT_BYTES = 128 * 2**20
PEAK_MAX_BYTES = 64 * 2**20
PLATEAU_RATIO = 1.10
MIN_FAULT_MS, MIN_STEADY_MS = 300_000, 180_000
INSTALL_LEAD_MS = 60_000           # the blackhole's installation starts within the steady phase's last minute
REMOVE_WITHIN_MS = 60_000          # and its removal completes within the recovery's first minute
# Amendment 1 (2026-11-12, after run 1): the blackhole is kept until the fault's end + the request timeout + 1 s, so
# that no request dispatched inside the fault can still complete after the removal (run 1: about 300 requests of the
# fault's last 2 s completed after it; the transport mechanism is inferred).
REMOVE_HOLD_MS = 3_000
EDGE_MS, MAX_GAP_MS = 5_000, 15_000
MIN_RUNTIME_S = 600
MAX_FIRST_SEND_MS = 10_000
MIN_RESTARTS = 2


class Verdict:
    def __init__(self):
        self.fail: List[str] = []
        self.unproven: List[str] = []

    def add(self, cls: str, text: str) -> None:
        (self.fail if cls == "fail" else self.unproven).append(text)

    @property
    def result(self) -> str:
        return "FAIL" if self.fail else ("UNPROVEN" if self.unproven else "PASS")


def coverage(samples, a: int, b: int, field: str = "current") -> Dict:
    """memory.current readings in [a, b), with the last one before and the first one after: covered when readings lie
    within EDGE_MS of both edges and no two consecutive readings are more than MAX_GAP_MS apart."""
    t = sorted(s["t_ms"] for s in samples if s.get(field) is not None)
    inside = [x for x in t if a <= x < b]
    near = [x for x in t if x < a][-1:] + inside + [x for x in t if x >= b][:1]
    gap = max([y - x for x, y in zip(near, near[1:])] or [b - a])
    ok = bool(inside) and near[0] <= a + EDGE_MS and near[-1] >= b - EDGE_MS and gap <= MAX_GAP_MS
    return {"readings": len(inside), "max_gap_ms": gap, "covered": ok}


def window_max(samples, a: int, b: int) -> Optional[int]:
    return max((s["current"] for s in samples if a <= s["t_ms"] < b and s.get("current") is not None), default=None)


def setup_problems(phases: Dict, ph: Dict, cfg: Dict, limits: List[int], incs: List[Dict]) -> List[str]:
    out = []
    if any(p not in phases for p in PHASES):
        return [f"the phases file lacks {[p for p in PHASES if p not in phases]}"]
    seq = [phases[p] for p in PHASES]
    if any(a >= b for a, b in seq) or any(seq[i][1] > seq[i + 1][0] for i in range(5)):
        out.append("the phases are not in order")
    for p, q in (("steady1", "fault1"), ("fault1", "recovery1"), ("steady2", "fault2"), ("fault2", "recovery2")):
        if phases[p][1] != phases[q][0]:
            out.append(f"{p} and {q} are not contiguous")
    origin, m = cfg["origin_ms"], cfg["minute_ms"]
    if any((x - origin) % m for p in PHASES for x in phases[p]):
        out.append("a phase boundary is not on a schedule minute")
    for p in PHASES:
        need = MIN_STEADY_MS if p.startswith("steady") else MIN_FAULT_MS
        if phases[p][1] - phases[p][0] < need:
            out.append(f"{p} lasts {(phases[p][1] - phases[p][0]) // 1000} s (at least {need // 1000} s)")
    if {k: sorted(v) for k, v in (ph.get("faults") or {}).items()} != {k: sorted(v) for k, v in FAULTS.items()}:
        out.append(f"the faults are {ph.get('faults')}, the test's are {FAULTS}")
    if (ph.get("blackhole") or {}).get("direction") != "requests":
        out.append("the blackhole direction is not requests")
    # The observed installation and removal: the fault phase is fully blackholed for its whole recorded length, and
    # the recovery has at least MIN_FAULT_MS without any rule after the removal completed.
    for fault, rec in (("fault1", "recovery1"), ("fault2", "recovery2")):
        act = (ph.get("actions") or {}).get(fault) or {}
        keys = ("install_started_ms", "install_done_ms", "remove_started_ms", "remove_done_ms")
        if not all(isinstance(act.get(k), int) for k in keys):
            out.append(f"{fault}: the observed installation and removal times are missing")
            continue
        f0, f1, r1 = phases[fault][0], phases[fault][1], phases[rec][1]
        if not (f0 - INSTALL_LEAD_MS <= act["install_started_ms"] <= act["install_done_ms"] <= f0):
            out.append(f"{fault}: the blackhole was not complete at the fault's start ({act})")
        if not (f1 + REMOVE_HOLD_MS <= act["remove_started_ms"] <= act["remove_done_ms"] <= f1 + REMOVE_WITHIN_MS):
            out.append(f"{fault}: the blackhole was not kept to the fault's end, or its removal was late ({act})")
        elif r1 - act["remove_done_ms"] < MIN_FAULT_MS:
            out.append(f"{rec}: {(r1 - act['remove_done_ms']) // 1000} s after the removal completed (at least "
                       f"{MIN_FAULT_MS // 1000} s)")
    if limits != [LIMIT_BYTES]:
        out.append(f"the ingress limits seen are {limits}, the test's is exactly {LIMIT_BYTES}")
    if (cfg.get("program") or {}).get("kind") != "constant-peak" or cfg.get("label") != "smoke":
        out.append("the schedule is not the constant-peak smoke schedule")
    # Placement: each group inside one incarnation, the second in the next incarnation of the same pod.
    def holder(a, b):
        return next((i for i, e in enumerate(incs) if e["start"]["started_ms"] <= a and b <= e["start"]["segment_end_ms"]),
                    None)
    h1, h2 = holder(phases["steady1"][0], phases["recovery1"][1]), holder(phases["steady2"][0], phases["recovery2"][1])
    if h1 is None or h2 is None or h2 != h1 + 1 or \
            incs[h1]["start"].get("pod_uid") != incs[h2]["start"].get("pod_uid"):
        out.append(f"the phase groups are not inside two consecutive incarnations of one pod (found {h1}, {h2})")
    return out


def traffic(red: Dict, phases: Dict, cfg: Dict, targets: List[str], peak: int, v: Verdict) -> Dict:
    """Every judged minute of every phase, per target, from the reducer's rows."""
    origin, m = cfg["origin_ms"], cfg["minute_ms"]
    detail = {}
    for name in PHASES:
        a, b = phases[name]
        minutes = list(range((a - origin) // m, (b - origin) // m))
        mixed = {minutes[-1]} if name in TRANSITION_LAST else {minutes[0]} if name in TRANSITION_FIRST else set()
        down = FAULTS.get(name, [])
        for t in targets:
            rows = [r for r in ((red.get("targets") or {}).get(t) or {}).get("rows", []) if r["minute"] in minutes]
            by = {}
            for r in rows:
                by.setdefault(r["minute"], []).append(r)
            bad = []
            for mm in minutes:
                got = by.get(mm, [])
                if len(got) != 1:
                    v.add("unproven", f"{name} {t} minute {mm}: {len(got)} reducer rows (exactly one needed)")
                    continue
                r, g = got[0], got[0]["generator"]
                if r["state"] != "observed":
                    v.add("unproven", f"{name} {t} minute {mm}: state {r['state']} {r.get('reasons')}")
                    continue
                drops = sum(g["dropped"].get(k, 0) for k in ("late", "inflight", "segment_end"))
                if r["intended"] != peak or g["dispatched"] != peak or drops or \
                        g["ok"] + g["failed"] != g["dispatched"]:
                    bad.append(f"minute {mm}: intended {r['intended']}, dispatched {g['dispatched']}, dropped {drops}, "
                               f"{g['ok']} ok + {g['failed']} failed (the constant peak is {peak}, every request "
                               "resolved)")
                elif mm in mixed:
                    continue                                   # a transition minute: any split of ok and failed
                elif t in down and (g["ok"] != 0 or g["failed"] != g["dispatched"]):
                    bad.append(f"minute {mm}: unreachable, yet {g['ok']} ok and {g['failed']} failed of {g['dispatched']}")
                elif t not in down and (g["ok"] != g["dispatched"] or g["failed"]):
                    bad.append(f"minute {mm}: reachable, yet {g['ok']} ok of {g['dispatched']}")
            if bad:
                v.add("fail", f"{name} {t}: " + "; ".join(bad[:4]))
            detail[f"{name}:{t}"] = {"minutes": len(minutes), "bad": len(bad)}
    return detail


def ingress_memory(samples, phases: Dict, v: Verdict, collection_end_ms: Optional[int] = None) -> Dict:
    # I2 covers every collected reading, from steady1 to the end of the collection (through the second restart);
    # the plateau and OOM spans are defined separately.
    if not isinstance(collection_end_ms, int) or collection_end_ms < phases["recovery2"][1]:
        v.add("unproven", f"I2: the collection end is unknown or before recovery2's end ({collection_end_ms})")
        collection_end_ms = phases["recovery2"][1]
    run = (phases["steady1"][0], collection_end_ms)
    span = (phases["fault1"][0], phases["recovery2"][1])
    peaks = [s["peak"] for s in samples if s.get("peak") is not None and run[0] <= s["t_ms"] <= run[1]]
    currents = [s["current"] for s in samples if s.get("current") is not None and run[0] <= s["t_ms"] <= run[1]]
    cov, pcov = coverage(samples, *span), coverage(samples, *run, field="peak")
    out = {"peak_bytes": max(peaks, default=None), "current_max_bytes": max(currents, default=None),
           "span_coverage": cov, "peak_coverage": pcov, "phases": {}}
    if not cov["covered"]:
        v.add("unproven", f"I3: memory.current does not cover the fault span ({cov})")
    high = max([x for x in (out["peak_bytes"], out["current_max_bytes"]) if x is not None], default=None)
    if high is not None and high > PEAK_MAX_BYTES:
        v.add("fail", f"I2: ingress memory reached {high} B, above {PEAK_MAX_BYTES} B")
    elif not pcov["covered"]:
        v.add("unproven", f"I2: memory.peak readings do not cover the run ({pcov}): the high-water mark is unknown")
    for name in COMPARED:
        a, b = phases[name]
        w2, wl = (a + 60_000, a + 120_000), (b - 60_000, b)
        c2, cl = coverage(samples, *w2), coverage(samples, *wl)
        m2, ml = window_max(samples, *w2), window_max(samples, *wl)
        row = {"second": {**c2, "max": m2}, "last": {**cl, "max": ml}}
        if not (c2["covered"] and cl["covered"]) or m2 is None or ml is None:
            v.add("unproven", f"I3 {name}: the compared windows are not covered ({c2}, {cl})")
        elif ml > PLATEAU_RATIO * m2:
            v.add("fail", f"I3 {name}: memory still growing: last minute {ml} B is {ml / m2:.3f} x the second minute's "
                          f"{m2} B (at most {PLATEAU_RATIO})")
        out["phases"][name] = row
    return out


def _epoch_ms(text) -> Optional[int]:
    try:
        return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except (TypeError, ValueError):
        return None


def generator_restarts(red: Dict, incs: List[Dict], exits: List[Dict], targets: List[str], v: Verdict) -> Dict:
    first = {(i["incarnation"], i["target"]): i.get("first_send_ms") for i in red.get("incarnations", [])}
    starts = {(i["incarnation"], i["target"]): i.get("start_record") for i in red.get("incarnations", [])}
    invalid = {(i["incarnation"], i["target"]): i.get("first_send_problems") for i in red.get("incarnations", [])
               if i.get("first_send_problems")}
    rows, run, best = [], 0, 0
    for a, b in zip(incs, incs[1:]):
        if a["start"].get("pod_uid") != b["start"].get("pod_uid") or a["end"] is None:
            run = 0
            continue
        ended, started = a["end"]["ended_ms"], b["start"]["started_ms"]
        seg_end = a["start"]["segment_end_ms"]
        # The ended container: its termination finished between the end record and the next start, and it started
        # before the incarnation did (container times have whole-second precision).
        term = [e for e in exits if e.get("pod_uid") == a["start"].get("pod_uid")
                and _epoch_ms(e.get("finished_at")) is not None and _epoch_ms(e.get("started_at")) is not None
                and ended - 1_000 <= _epoch_ms(e["finished_at"]) <= started + 1_000
                and _epoch_ms(e["started_at"]) <= a["start"]["started_ms"] + 1_000]
        row = {"from": a["inc"], "to": b["inc"], "segment_end_ms": seg_end, "termination": term,
               "first_send_after_segment_end_ms": {t: (None if first.get((b["inc"], t)) is None
                                                       else first[(b["inc"], t)] - seg_end) for t in targets}}
        why = []
        if len(term) != 1:
            why.append(f"{len(term)} matching container terminations")
        else:
            e = term[0]
            runtime = (_epoch_ms(e["finished_at"]) - _epoch_ms(e["started_at"])) // 1000
            row["runtime_s"] = runtime
            if e.get("reason") != "Completed" or e.get("exit_code") != 0:
                why.append(f"terminated {e.get('reason')} with code {e.get('exit_code')}")
            if runtime <= MIN_RUNTIME_S:
                why.append(f"ran {runtime} s (more than {MIN_RUNTIME_S} s needed)")
        if started < ended:
            why.append("the next incarnation started before this one ended")
        for t, d in row["first_send_after_segment_end_ms"].items():
            if starts.get((b["inc"], t)) != "valid":
                why.append(f"invalid start record of {t}: {starts.get((b['inc'], t))}")
            elif invalid.get((b["inc"], t)):
                why.append(f"invalid first_send of {t}: {invalid[(b['inc'], t)]}")
            elif d is None:
                why.append(f"no first_send record of {t}")
            elif not 0 <= d <= MAX_FIRST_SEND_MS:
                why.append(f"first send to {t} {d} ms after the segment end")
        row["meets_budget"], row["why_not"] = not why, why
        run = run + 1 if not why else 0
        best = max(best, run)
        rows.append(row)
    if best < MIN_RESTARTS:
        # Evidence that is absent makes it unproven; evidence that is present and wrong (a short or failed container,
        # a late or invalid first send, overlapping incarnations) makes it a failure.
        broken = any(not (w.startswith("no first_send") or "matching container" in w) for r in rows for w in r["why_not"])
        v.add("fail" if broken or len(rows) < MIN_RESTARTS else "unproven",
              f"G1: {best} consecutive planned restarts met the budget (at least {MIN_RESTARTS}): "
              f"{[(r['from'], r['why_not']) for r in rows]}")
    return {"restarts": rows, "consecutive_meeting_budget": best}


def judge(*, red, phases, ph, cfg, ing, identity, limits, incs, exits) -> Dict:
    v = Verdict()
    targets = list(cfg["targets"])
    for p in setup_problems(phases, ph, cfg, limits, incs):
        v.add("fail", f"setup: {p}")
    if any(p not in phases for p in PHASES):
        return {"result": v.result, "fail": v.fail, "unproven": v.unproven}
    peak = cfg["per_minute_days"][0][0]
    detail = {"traffic": traffic(red, phases, cfg, targets, peak, v),
              "ingress": ingress_memory(ing, phases, v, ph.get("collection_end_ms"))}
    for cls, text in identity:
        v.add(cls, f"I1: {text}")
    detail["generator"] = generator_restarts(red, incs, exits, targets, v)
    return {"result": v.result, "fail": v.fail, "unproven": v.unproven, "detail": detail}


def analyse(a) -> int:
    sys.path.insert(0, a.harness)
    from c28 import demo, demo_reduce as DR
    cfg = json.load(open(a.config))
    why = demo.config_problems(cfg)
    if why:
        sys.exit("analyse: the configuration is not intact: " + "; ".join(why))
    with open(os.path.join(a.dir, "generator.log"), errors="replace") as fh:
        records, log_stats = DR.parse_lines(fh.read().splitlines())
    ph = json.load(open(a.phases))
    phases = {k: tuple(v) for k, v in ph["phases"].items()}
    m = cfg["minute_ms"]
    red = DR.reduce(cfg, records, json.load(open(a.ingress)), min(v[0] for v in phases.values()) // m * m,
                    max(v[1] for v in phases.values()) // m * m + m)
    with open(os.path.join(a.dir, "reduction.json"), "w") as fh:
        json.dump({**red, "log": log_stats}, fh, indent=1, sort_keys=True)
    ing = K.read_jsonl(os.path.join(a.dir, "ingress-memory.jsonl"))
    limits = sorted({s["limit"] for s in ing if s.get("limit")})
    span = (phases["fault1"][0], phases["recovery2"][1]) if {"fault1", "recovery2"} <= set(phases) else (0, 0)
    oom = K.oom_coverage(ing, *span)
    sampled = sorted({s["container"] for s in ing if span[0] <= s["t_ms"] <= span[1]})
    imem = K.memory_summary(ing, limits[-1] if limits else None, {"timeout": list(span)})
    identity = K.ingress_evidence_classified(ph.get("ingress_final"), ph.get("ingress_before"), imem, oom, sampled)
    exits = K.container_exits(K.read_jsonl(os.path.join(a.dir, "pods-watch.jsonl")),
                              K.read_jsonl(os.path.join(a.dir, "pods-poll.jsonl")))
    incs = K.incarnations(records, cfg["targets"][0])
    out = judge(red=red, phases=phases, ph=ph, cfg=cfg, ing=ing, identity=identity, limits=limits, incs=incs, exits=exits)
    times = [s["t_ms"] for s in ing]
    out.update({"schema": "c28-ingress-fault-test/v2", "config_digest": cfg["digest"], "phases": ph["phases"],
                "ingress_observation": {"from_ms": min(times, default=None), "to_ms": max(times, default=None),
                                        "samples": len(ing)},
                "ingress_limits_seen": limits, "oom_coverage": oom, "log": log_stats,
                "pre_fault_vs_end_bytes": {               # reported, not judged (criteria, item 4)
                    "steady1_max": window_max(ing, *phases["steady1"]) if "steady1" in phases else None,
                    "recovery2_last_minute_max": window_max(ing, phases["recovery2"][1] - 60_000, phases["recovery2"][1])
                    if "recovery2" in phases else None}})
    with open(a.out, "w") as fh:
        json.dump(out, fh, indent=1, sort_keys=True)
    print(f"ingress fault test: {out['result']}")
    for p in out["fail"]:
        print(f"  FAIL {p}")
    for p in out["unproven"]:
        print(f"  UNPROVEN {p}")
    return 0 if out["result"] == "PASS" else 1


# --- selftest (offline) ------------------------------------------------------------------------------------------------

def selftest() -> int:
    A, B = "c28-demo-pa-n", "c28-demo-pa-r"
    PEAK, M = 9000, 60_000
    cfg = {"origin_ms": 0, "minute_ms": M, "targets": [A, B], "label": "smoke", "program": {"kind": "constant-peak"},
           "per_minute_days": [[PEAK] * 1440]}
    # 20-minute segments: incarnation i1 [0, 20 min), i2 [20 min + 3 s, 40 min), i3 from 40 min + 3 s.
    phases = {"steady1": (M, 4 * M), "fault1": (4 * M, 9 * M), "recovery1": (9 * M, 15 * M),
              "steady2": (21 * M, 24 * M), "fault2": (24 * M, 29 * M), "recovery2": (29 * M, 35 * M)}
    acts = {f: {"install_started_ms": phases[f][0] - 5_000, "install_done_ms": phases[f][0] - 4_000,
                "remove_started_ms": phases[f][1] + 3_200, "remove_done_ms": phases[f][1] + 3_500} for f in FAULTS}
    ph = {"faults": FAULTS, "blackhole": {"direction": "requests"}, "actions": acts, "collection_end_ms": 41 * M}

    def start(inc, started, seg_end):
        return {"inc": inc, "start": {"started_ms": started, "segment_end_ms": seg_end, "pod_uid": "p"},
                "end": None, "lines": []}
    incs = [start("i1", 0, 20 * M), start("i2", 20 * M + 3_000, 40 * M), start("i3", 40 * M + 3_000, 60 * M)]
    incs[0]["end"], incs[1]["end"] = {"ended_ms": 20 * M + 2_000}, {"ended_ms": 40 * M + 2_000}

    def iso(ms):
        return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    exits = [{"pod_uid": "p", "reason": "Completed", "exit_code": 0, "started_at": iso(0), "finished_at": iso(20 * M + 2_000)},
             {"pod_uid": "p", "reason": "Completed", "exit_code": 0, "started_at": iso(20 * M + 3_000),
              "finished_at": iso(40 * M + 2_000)}]

    def red_for(edit=None):
        rows = {A: [], B: []}
        for mm in range(0, 41):
            t0 = mm * M
            for t in (A, B):
                down = any(t in FAULTS[f] and phases[f][0] <= t0 < phases[f][1] for f in FAULTS)
                g = {"dispatched": PEAK, "ok": 0 if down else PEAK, "failed": PEAK if down else 0,
                     "dropped": {"late": 0, "inflight": 0, "segment_end": 0}}
                rows[t].append({"minute": mm, "state": "observed", "reasons": [], "intended": PEAK, "generator": g})
        red = {"targets": {t: {"rows": r} for t, r in rows.items()},
               "incarnations": [{"incarnation": i, "target": t, "first_send_ms": s, "first_send_problems": [],
                                 "start_record": "valid"}
                                for i, s in (("i2", 20 * M + 4_000), ("i3", 40 * M + 4_000)) for t in (A, B)]}
        if edit:
            edit(red)
        return red
    ing = [{"t_ms": t, "container": "c1", "current": 30_000_000, "peak": 31_000_000} for t in range(0, 41 * M, 1000)]
    kw = dict(red=red_for(), phases=phases, ph=ph, cfg=cfg, ing=ing, identity=[], limits=[LIMIT_BYTES], incs=incs,
              exits=exits)
    good = judge(**kw)
    assert good["result"] == "PASS", good

    def r(**over):
        return judge(**{**kw, **over})

    def rows_of(red, t):
        return red["targets"][t]["rows"]

    def must(out, result, needle):
        texts = out["fail"] if result == "FAIL" else out["unproven"]
        assert out["result"] == result and any(needle in p for p in texts), (result, needle, out)

    # Transition minutes: a mixed split is allowed; their accounting is not optional (Codex, second review, 1).
    out = r(red=red_for(lambda red: rows_of(red, B)[3]["generator"].update(ok=PEAK - 70, failed=70)))
    assert out["result"] == "PASS", out                                   # steady1's last minute: mixed is fine
    out = r(red=red_for(lambda red: [rows_of(red, t).pop(9) for t in (A, B)]))  # recovery1's first minute deleted
    must(out, "UNPROVEN", "minute 9: 0 reducer rows")
    must(r(red=red_for(lambda red: rows_of(red, B)[9]["generator"].update(dispatched=0, ok=0, failed=0))), "FAIL",
         "recovery1 c28-demo-pa-r")
    # Counterexamples of the first review.
    def zero_unreachable(red):
        for row in rows_of(red, B):
            if phases["fault1"][0] <= row["minute"] * M < phases["fault1"][1]:
                row["generator"].update(dispatched=0, failed=0)
    must(r(red=red_for(zero_unreachable)), "FAIL", "fault1 c28-demo-pa-r")
    must(r(red=red_for(lambda red: rows_of(red, A).pop(5))), "UNPROVEN", "minute 5: 0 reducer rows")
    must(r(red=red_for(lambda red: rows_of(red, A)[25].update(state="partial", reasons=["ingress_gap"]))), "UNPROVEN",
         "state partial")
    must(r(red=red_for(lambda red: rows_of(red, A)[11]["generator"].update(ok=PEAK - 3, failed=3))), "FAIL",
         "recovery1 c28-demo-pa-n")
    hole = [x for x in ing if not (phases["recovery1"][0] + 70_000 <= x["t_ms"] < phases["recovery1"][0] + 100_000)]
    must(r(ing=hole), "UNPROVEN", "I3 recovery1")
    grow = [dict(x, current=x["current"] + 20_000_000) if phases["fault2"][1] - M <= x["t_ms"] < phases["fault2"][1]
            else x for x in ing]
    must(r(ing=grow), "FAIL", "I3 fault2")
    must(r(limits=[256 * 2**20]), "FAIL", "limits seen")
    must(r(phases=dict(phases, fault1=(4 * M, 8 * M), recovery1=(8 * M, 15 * M)),
           ph=dict(ph, actions=dict(acts, fault1=dict(acts["fault1"], remove_started_ms=8 * M + 500,
                                                       remove_done_ms=8 * M + 1_500)))), "FAIL", "fault1 lasts 240 s")
    must(r(phases=dict(phases, steady1=(M + 30_000, 4 * M))), "FAIL", "schedule minute")
    must(r(ph=dict(ph, faults={"fault1": [A], "fault2": [A, B]})), "FAIL", "the faults are")
    must(r(incs=[incs[0], start("i2", 20 * M + 3_000, 30 * M)] + incs[2:]), "FAIL", "incarnations")
    must(r(identity=[("unproven", "x not captured")]), "UNPROVEN", "I1")
    must(r(identity=[("fail", "restarted 1 time(s)")]), "FAIL", "I1")
    # Peak (second review, 2): a stale early reading cannot hide a high current; peaks must cover the run.
    stale = [dict(x, peak=None if x["t_ms"] > 0 else 31_000_000, current=80 * 2**20) for x in ing]
    must(r(ing=stale), "FAIL", "I2")
    must(r(ing=[dict(x, peak=None) if x["t_ms"] > 2 * M else x for x in ing]), "UNPROVEN", "I2")
    must(r(ing=[dict(x, peak=70 * 2**20) for x in ing]), "FAIL", "I2")
    tail = [dict(x, current=80 * 2**20, peak=80 * 2**20) if x["t_ms"] > phases["recovery2"][1] else x for x in ing]
    must(r(ing=tail), "FAIL", "I2")                               # third review, 2: the collection tail counts
    must(r(ph=dict(ph, collection_end_ms=None)), "UNPROVEN", "collection end")
    must(r(red=red_for(lambda red: red["incarnations"][2].update(start_record="config_mismatch"))), "FAIL",
         "invalid start record")                                  # third review, 1: incarnation three's provenance
    # First sends (second review, 3): an invalid record of the third incarnation fails; a missing one is unproven.
    must(r(red=red_for(lambda red: red["incarnations"][2].update(
        first_send_ms=None, first_send_problems=["first_send before the incarnation started"]))), "FAIL", "invalid first_send")
    must(r(red=red_for(lambda red: red["incarnations"][3].update(first_send_ms=40 * M + 12_000))), "FAIL",
         "first send to c28-demo-pa-r 12000 ms")
    must(r(red=red_for(lambda red: red["incarnations"][0].update(first_send_ms=None))), "UNPROVEN", "no first_send")
    # Containers (first review, 3).
    must(r(exits=[dict(e, started_at=e["finished_at"]) for e in exits]), "UNPROVEN", "matching container terminations")
    brief = [start("i1", 15 * M, 20 * M)] + incs[1:]
    brief[0]["end"] = {"ended_ms": 20 * M + 2_000}
    out = r(incs=brief, exits=[dict(exits[0], started_at=iso(15 * M))] + exits[1:])
    assert out["result"] == "FAIL" and any("ran 302 s" in p for p in out["fail"]), out
    must(r(exits=[dict(exits[0], reason="Error", exit_code=137), exits[1]]), "FAIL", "terminated Error")
    # Observed boundaries (second review, 4).
    def act(fault, **over):
        return dict(ph, actions=dict(acts, **{fault: dict(acts[fault], **over)}))
    must(r(ph=act("fault1", install_done_ms=phases["fault1"][0] + 2_000)), "FAIL", "not complete at the fault's start")
    must(r(ph=act("fault2", remove_started_ms=phases["fault2"][1] - 1_000)), "FAIL", "not kept to the fault's end")
    must(r(ph=act("fault1", remove_started_ms=phases["fault1"][1] + 500, remove_done_ms=phases["fault1"][1] + 800)),
         "FAIL", "not kept to the fault's end")                      # amendment 1: held 3 s past the fault's end
    must(r(ph=act("fault2", remove_done_ms=phases["fault2"][1] + 61_000)), "FAIL", "not kept to the fault's end")
    must(r(ph=act("fault1", remove_started_ms=phases["fault1"][1] + 59_000,
                  remove_done_ms=phases["fault1"][1] + 59_500), phases=dict(phases, recovery1=(9 * M, 14 * M))),
         "FAIL", "recovery1: 240 s after the removal")
    must(r(ph=dict(ph, actions={})), "FAIL", "observed installation and removal times are missing")
    print("ingress_faults selftest: setup and observed blackhole times, every minute's accounting from the reducer "
          "(transition minutes included), memory coverage, plateau and peak, identity classes, and the generator's "
          "restart budget from terminations and validated first sends: OK")
    return 0


if __name__ == "__main__":
    sys.exit(selftest())
