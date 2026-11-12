"""The verdict of demo-kind.sh --ingress-faults: the fixture's ingress and the generator on the final images.

Criteria fixed before any run (c28-staging/live-demo/INGRESS-FAULT-TEST-CRITERIA.md, and Codex's 2026-11-12 decisions
T1, T2 and the phase layout); nothing here may be loosened after a run has been looked at.

Phases (epoch ms, from the phases file): steady1, fault1 (one target unreachable), recovery1, steady2, fault2 (both
unreachable), recovery2. The schedule is a SMOKE schedule held at the full schedule's peak minute count for every
minute (program.kind "constant-peak"), so the compared windows carry the same intended rate per target.

The result is PASS, FAIL or UNPROVEN. A problem that shows the criterion is broken is a FAIL; evidence that is missing
or cannot be compared makes the criterion UNPROVEN, never a pass.

  Ingress (fault1 start to recovery2 end, the generator's restart in between included)
    I1 identity: the status after the run with restart count 0, the same pod UID and container ID as before fault1,
       exactly that container in the memory samples of the span; both OOM counters read at 0 throughout
       (demo_kind.ingress_evidence_problems and oom_coverage);
    I2 peak: memory.peak over the whole run at most 50 % of the limit;
    I3 plateau, for each fault and each recovery: the largest memory.current in the phase's last 60 s at most 110 % of
       its largest in seconds 60 to 120; each window needs at least MIN_WINDOW_SAMPLES samples, else UNPROVEN;
    I4 constant traffic in those comparisons: the fully contained minutes of the two windows have, per target, equal
       intended and equal dispatched counts and no generator drop; else UNPROVEN.
  Service
    S1 steady and recovery phases: every target has an observed minute with served requests;
    S2 fault1: the unreachable target fails in its fully contained minutes with nothing dropped, the other target has
       an observed, fully served minute;
    S3 fault2: both targets fail in their fully contained minutes with nothing dropped.
  Generator (T2)
    G1 at least two consecutive planned restarts in one pod, each after more than 600 s of container runtime, exit 0
       (Completed), and the next incarnation's first dispatch to EACH target at most 10 s after the segment end.
       This meets the restart-gap budget; it does not by itself prove the absence of kubelet back-off.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Dict, List, Optional, Tuple

import demo_kind as K

PHASES = ("steady1", "fault1", "recovery1", "steady2", "fault2", "recovery2")
FAULTS = ("fault1", "fault2")
COMPARED = ("fault1", "recovery1", "fault2", "recovery2")
PEAK_FRACTION = 0.50
PLATEAU_RATIO = 1.10
MIN_WINDOW_SAMPLES = 30            # the ingress sampler reads once a second: half of a 60 s window at least
MIN_RUNTIME_MS = 600_000
MAX_FIRST_DISPATCH_MS = 10_000
MIN_RESTARTS = 2


def _window(samples, a: int, b: int) -> List[int]:
    return [s["current"] for s in samples if a <= s["t_ms"] < b and s.get("current") is not None]


def plateau(samples, phase: Tuple[int, int]) -> Dict:
    """I3 for one phase: the second minute's and the last minute's largest memory.current, and the verdict."""
    a, b = phase
    second, last = _window(samples, a + 60_000, a + 120_000), _window(samples, b - 60_000, b)
    row = {"second_minute": {"samples": len(second), "max": max(second, default=None)},
           "last_minute": {"samples": len(last), "max": max(last, default=None)}}
    if b - a < 180_000 or len(second) < MIN_WINDOW_SAMPLES or len(last) < MIN_WINDOW_SAMPLES:
        row["verdict"] = "unproven"
    else:
        row["ratio"] = round(row["last_minute"]["max"] / row["second_minute"]["max"], 4)
        row["verdict"] = "pass" if row["last_minute"]["max"] <= PLATEAU_RATIO * row["second_minute"]["max"] else "fail"
    return row


def _contained(lines, target: str, a: int, b: int, origin: int, minute: int) -> List[Dict]:
    """The target's minute lines whose whole minute lies in [a, b)."""
    return [l for l in lines if l["target"] == target and a <= origin + l["minute"] * minute
            and origin + (l["minute"] + 1) * minute <= b]


def constant_traffic(lines, cfg, phase: Tuple[int, int], targets) -> Dict:
    """I4 for one phase: per target, the fully contained minutes of the second-minute window and of the last-minute
    window carry equal intended and equal dispatched counts and no drop. Missing lines make it unproven."""
    a, b = phase
    o, m = cfg["origin_ms"], cfg["minute_ms"]
    out = {"verdict": "pass", "targets": {}}
    for t in targets:
        early = _contained(lines, t, a + 60_000 - m, a + 120_000 + m, o, m)
        late = _contained(lines, t, b - 60_000 - m, b, o, m)
        if not early or not late:
            out["targets"][t] = "no fully contained minute in a compared window"
            out["verdict"] = "unproven"
            continue
        e, l = early[0], late[-1]
        same = (e["intended"], e["dispatched"]) == (l["intended"], l["dispatched"])
        drops = sum(x["dropped"].get(k, 0) for x in (e, l) for k in ("late", "inflight", "segment_end"))
        out["targets"][t] = {"early": [e["minute"], e["intended"], e["dispatched"]],
                             "late": [l["minute"], l["intended"], l["dispatched"]], "drops": drops}
        if not same or drops:
            out["verdict"] = "unproven"
    return out


def faulted_minutes(lines, cfg, phase: Tuple[int, int], target: str) -> Dict:
    """S2/S3: the target's fully contained minutes in the fault: all failed, nothing dropped."""
    rows = _contained(lines, target, *phase, cfg["origin_ms"], cfg["minute_ms"])
    return {"minutes": len(rows), "failed": sum(r["failed"] for r in rows), "ok": sum(r["ok"] for r in rows),
            "dispatched": sum(r["dispatched"] for r in rows),
            "dropped": sum(r["dropped"].get(k, 0) for r in rows for k in ("late", "inflight", "segment_end"))}


def generator_restarts(records, cfg, exits, targets) -> Dict:
    """G1: the planned restarts of the pod, with each container's runtime and each target's first dispatch."""
    per_target = {t: K.incarnations(records, t) for t in targets}
    base = per_target[targets[0]]
    rows = []
    for i, (a, b) in enumerate(zip(base, base[1:])):
        if a["end"] is None or a["start"].get("pod_uid") != b["start"].get("pod_uid"):
            continue
        row = {"from": a["inc"], "to": b["inc"], "runtime_ms": a["end"]["ended_ms"] - a["start"]["started_ms"],
               "segment_end_ms": a["start"]["segment_end_ms"], "first_dispatch_ms": {}}
        for t in targets:
            nxt = next((e for e in per_target[t] if e["inc"] == b["inc"]), None)
            first = next((l for l in (nxt or {}).get("lines", []) if l["dispatched"] > 0), None)
            row["first_dispatch_ms"][t] = None if first is None else \
                cfg["origin_ms"] + first["minute"] * cfg["minute_ms"] + first["active"][0] - a["start"]["segment_end_ms"]
        rows.append(row)
    completed = [e for e in exits if e["reason"] == "Completed" and e["exit_code"] == 0]
    good = [r for r in rows if r["runtime_ms"] > MIN_RUNTIME_MS and
            all(v is not None and v <= MAX_FIRST_DISPATCH_MS for v in r["first_dispatch_ms"].values())]
    run = best = 0
    prev = None
    for r in rows:
        ok = r in good
        run = run + 1 if ok and prev is not None and prev["to"] == r["from"] and prev in good else (1 if ok else 0)
        best, prev = max(best, run), r
    return {"restarts": rows, "consecutive_meeting_budget": best, "completed_exits": len(completed)}


def verdict(*, phases, cfg, lines, verdicts, ing_samples, ingress_identity: List[str], limit, peak_bytes,
            restarts: Dict, faults: Dict[str, List[str]], targets) -> Dict:
    fail, unproven = [], []
    missing = [p for p in PHASES if p not in phases]
    if missing:
        unproven.append(f"phases missing from the phases file: {missing}")
        return {"result": "UNPROVEN", "fail": fail, "unproven": unproven}
    # I1 (identity, OOM coverage): demo_kind's own checks; a missing capture is unproven, a seen change a failure.
    for p in ingress_identity:
        (unproven if ("unproven" in p or "not captured" in p or "not measured" in p or "unknown" in p)
         else fail).append(p)
    # I2
    if not isinstance(limit, int) or limit <= 0 or peak_bytes is None:
        unproven.append("I2: the ingress's limit or memory.peak is unknown")
    elif peak_bytes > PEAK_FRACTION * limit:
        fail.append(f"I2: ingress memory.peak {peak_bytes} B is above {PEAK_FRACTION:.0%} of the limit {limit} B")
    # I3, I4
    detail = {}
    for name in COMPARED:
        pl, tr = plateau(ing_samples, phases[name]), constant_traffic(lines, cfg, phases[name], targets)
        detail[name] = {"plateau": pl, "traffic": tr}
        if pl["verdict"] == "fail":
            fail.append(f"I3 {name}: ingress memory still growing: last-minute max {pl['last_minute']['max']} B is "
                        f"{pl['ratio']} x the second minute's {pl['second_minute']['max']} B (at most {PLATEAU_RATIO})")
        elif pl["verdict"] == "unproven":
            unproven.append(f"I3 {name}: too few ingress samples in a compared window ({pl['second_minute']['samples']}"
                            f", {pl['last_minute']['samples']}; at least {MIN_WINDOW_SAMPLES} each) or the phase is "
                            "shorter than 180 s")
        if tr["verdict"] != "pass":
            unproven.append(f"I4 {name}: the compared windows do not carry the same traffic: {tr['targets']}")
    # S1, S2, S3
    for name in PHASES:
        down = faults.get(name, [])
        for t in targets:
            if t in down:
                f = faulted_minutes(lines, cfg, phases[name], t)
                detail.setdefault(name, {})[f"faulted:{t}"] = f
                if f["minutes"] == 0:
                    unproven.append(f"S {name}: no fully contained minute of {t}")
                elif f["ok"] or f["dropped"] or f["failed"] != f["dispatched"]:
                    fail.append(f"S {name}: {t} was unreachable but {f['ok']} requests succeeded, {f['dropped']} were "
                                f"dropped and {f['failed']} of {f['dispatched']} failed")
            elif ((verdicts.get(name) or {}).get(t) or {}).get("observed_served", 0) < 1:
                (unproven if not lines else fail).append(f"S {name}: {t} has no observed minute with served requests")
    # G1
    if restarts["consecutive_meeting_budget"] < MIN_RESTARTS:
        fail.append(f"G1: {restarts['consecutive_meeting_budget']} consecutive planned restarts met the budget "
                    f"(runtime > {MIN_RUNTIME_MS // 1000} s, first dispatch to each target within "
                    f"{MAX_FIRST_DISPATCH_MS // 1000} s); at least {MIN_RESTARTS} needed: {restarts['restarts']}")
    if restarts["completed_exits"] < MIN_RESTARTS:
        unproven.append(f"G1: {restarts['completed_exits']} container exits seen as Completed with code 0 "
                        f"(at least {MIN_RESTARTS})")
    result = "FAIL" if fail else ("UNPROVEN" if unproven else "PASS")
    return {"result": result, "fail": fail, "unproven": unproven, "detail": detail}


def analyse(a) -> int:
    sys.path.insert(0, a.harness)
    from c28 import demo, demo_reduce as DR
    cfg = json.load(open(a.config))
    why = demo.config_problems(cfg)
    if why or (cfg.get("program") or {}).get("kind") != "constant-peak":
        sys.exit("analyse: the configuration is not an intact constant-peak smoke schedule: " + "; ".join(why))
    with open(os.path.join(a.dir, "generator.log"), errors="replace") as fh:
        records, log_stats = DR.parse_lines(fh.read().splitlines())
    ph = json.load(open(a.phases))
    phases = {k: tuple(v) for k, v in ph["phases"].items()}
    ingress = json.load(open(a.ingress))
    m = cfg["minute_ms"]
    red = DR.reduce(cfg, records, ingress, min(v[0] for v in phases.values()) // m * m,
                    max(v[1] for v in phases.values()) // m * m)
    with open(os.path.join(a.dir, "reduction.json"), "w") as fh:
        json.dump({**red, "log": log_stats}, fh, indent=1, sort_keys=True)
    lines = [r for r in records if r.get("schema") == DR.MINUTE]
    verdicts = K.phase_verdicts(red, phases)
    ing = K.read_jsonl(os.path.join(a.dir, "ingress-memory.jsonl"))
    limits = sorted({s["limit"] for s in ing if s.get("limit")})
    peaks = [s["peak"] for s in ing if s.get("peak") is not None]
    span = (phases["fault1"][0], phases["recovery2"][1])
    oom = K.oom_coverage(ing, *span)
    sampled = sorted({s["container"] for s in ing if span[0] <= s["t_ms"] <= span[1]})
    imem = K.memory_summary(ing, limits[-1] if limits else None, {"timeout": [phases["fault1"][0], phases["fault2"][1]]})
    identity = K.ingress_evidence_problems(ph.get("ingress_final"), ph.get("ingress_before"), imem, oom, sampled)
    exits = K.container_exits(K.read_jsonl(os.path.join(a.dir, "pods-watch.jsonl")),
                              K.read_jsonl(os.path.join(a.dir, "pods-poll.jsonl")))
    rs = generator_restarts(records, cfg, exits, list(cfg["targets"]))
    v = verdict(phases=phases, cfg=cfg, lines=lines, verdicts=verdicts, ing_samples=ing, ingress_identity=identity,
                limit=limits[-1] if len(limits) == 1 else None, peak_bytes=max(peaks, default=None), restarts=rs,
                faults={k: list(v) for k, v in ph["faults"].items()}, targets=list(cfg["targets"]))
    times = [s["t_ms"] for s in ing]
    before = _window(ing, phases["steady1"][0], phases["steady1"][1])
    after = _window(ing, phases["recovery2"][1] - 60_000, phases["recovery2"][1])
    out = {"schema": "c28-ingress-fault-test/v1", "config_digest": cfg["digest"], "phases": ph["phases"],
           "faults": ph["faults"], "ingress_observation": {"from_ms": min(times, default=None),
                                                         "to_ms": max(times, default=None), "samples": len(ing)},
           "ingress_limit_bytes": limits, "ingress_peak_bytes": max(peaks, default=None), "oom_coverage": oom,
           "pre_fault_vs_end_bytes": {"steady1_max": max(before, default=None), "recovery2_last_minute_max":
                                      max(after, default=None)},           # reported, not judged (criterion 4)
           "restarts": rs, "log": log_stats, **v}
    with open(a.out, "w") as fh:
        json.dump(out, fh, indent=1, sort_keys=True)
    print(f"ingress fault test: {out['result']}")
    for p in v["fail"]:
        print(f"  FAIL {p}")
    for p in v["unproven"]:
        print(f"  UNPROVEN {p}")
    return 0 if out["result"] == "PASS" else 1


# --- selftest (offline): every criterion passes on a clean synthetic run and is broken or unproven as intended -------

def selftest() -> int:
    A, B = "t-a", "t-b"
    cfg = {"origin_ms": 0, "minute_ms": 60_000}
    starts = {"steady1": 0, "fault1": 180_000, "recovery1": 480_000, "steady2": 960_000, "fault2": 1_140_000,
              "recovery2": 1_440_000}
    ends = {"steady1": 180_000, "fault1": 480_000, "recovery1": 780_000, "steady2": 1_140_000, "fault2": 1_440_000,
            "recovery2": 1_740_000}
    phases = {k: (starts[k], ends[k]) for k in PHASES}
    faults = {"fault1": [B], "fault2": [A, B]}

    def line(t, minute, ok=True, n=100, dispatched=None, dropped=0):
        d = n if dispatched is None else dispatched
        return {"target": t, "minute": minute, "intended": n, "dispatched": d, "ok": d if ok else 0,
                "failed": 0 if ok else d, "dropped": {"late": dropped, "inflight": 0, "segment_end": 0}, "active": [0, 60_000]}

    def lines_for():
        out = []
        for minute in range(29):
            t0 = minute * 60_000
            for t in (A, B):
                down = any(t in faults.get(p, []) and phases[p][0] <= t0 and t0 + 60_000 <= phases[p][1] for p in faults)
                out.append(line(t, minute, ok=not down))
        return out
    samples = [{"t_ms": t, "container": "c1", "current": 30_000_000, "peak": 31_000_000} for t in range(0, 1_740_000, 1000)]
    served = {p: {A: {"observed_served": 1}, B: {"observed_served": 1}} for p in PHASES}
    rs = {"restarts": [], "consecutive_meeting_budget": 2, "completed_exits": 2}
    kw = dict(phases=phases, cfg=cfg, lines=lines_for(), verdicts=served, ing_samples=samples, ingress_identity=[],
              limit=128 * 2**20, peak_bytes=31_000_000, restarts=rs, faults=faults, targets=[A, B])
    good = verdict(**kw)
    assert good["result"] == "PASS", good

    def outcome(**over):
        return verdict(**{**kw, **over})
    r = outcome(peak_bytes=70 * 2**20)
    assert r["result"] == "FAIL" and any(p.startswith("I2") for p in r["fail"])
    grow = [dict(s, current=s["current"] + (20_000_000 if phases["fault2"][1] - 60_000 <= s["t_ms"] < phases["fault2"][1]
                                           else 0)) for s in samples]
    r = outcome(ing_samples=grow)
    assert r["result"] == "FAIL" and any(p.startswith("I3 fault2") for p in r["fail"]), r
    sparse = [s for s in samples if not (phases["recovery1"][0] + 60_000 <= s["t_ms"] < phases["recovery1"][0] + 120_000)
              or s["t_ms"] % 3000 == 0]
    r = outcome(ing_samples=sparse)
    assert r["result"] == "UNPROVEN" and any(p.startswith("I3 recovery1") for p in r["unproven"]), r
    uneven = [dict(l, intended=l["intended"] + 1) if l["target"] == A and l["minute"] == 7 else l for l in kw["lines"]]
    r = outcome(lines=uneven)
    assert r["result"] == "UNPROVEN" and any(p.startswith("I4 fault1") for p in r["unproven"]), r
    dropped = [dict(l, dropped={"late": 0, "inflight": 3, "segment_end": 0}) if l["minute"] == 20 else l
               for l in kw["lines"]]
    r = outcome(lines=dropped)
    assert r["result"] == "FAIL" and any(p.startswith("S fault2") for p in r["fail"]), r
    leaked = [dict(l, ok=1, failed=l["failed"] - 1) if l["target"] == B and l["minute"] == 5 else l for l in kw["lines"]]
    r = outcome(lines=leaked)
    assert r["result"] == "FAIL" and any(p.startswith("S fault1") for p in r["fail"]), r
    r = outcome(verdicts={**served, "fault1": {A: {"observed_served": 0}, B: {}}})
    assert r["result"] == "FAIL" and any("S fault1: t-a" in p for p in r["fail"]), r
    r = outcome(restarts=dict(rs, consecutive_meeting_budget=1))
    assert r["result"] == "FAIL" and any(p.startswith("G1") for p in r["fail"])
    r = outcome(restarts=dict(rs, completed_exits=1))
    assert r["result"] == "UNPROVEN"
    r = outcome(ingress_identity=["the ingress's oom_kill counters were not read ... is unproven"])
    assert r["result"] == "UNPROVEN"
    r = outcome(ingress_identity=["ingress pod p restarted 1 time(s): last termination OOMKilled"])
    assert r["result"] == "FAIL"
    r = outcome(phases={k: v for k, v in phases.items() if k != "steady2"})
    assert r["result"] == "UNPROVEN"
    # G1's arithmetic on records: two restarts after 15 min, first dispatch 4 s after the segment end.
    def rec(inc, started, seg_end, ended=None):
        out = [{"target": t, "event": "start", "inc": inc, "pod_uid": "p", "started_ms": started,
                "active_from_ms": started, "segment_end_ms": seg_end} for t in (A, B)]
        if ended:
            out += [{"target": t, "event": "end", "inc": inc, "ended_ms": ended} for t in (A, B)]
        return out
    recs = rec("i1", 0, 900_000, 902_000) + rec("i2", 903_500, 1_800_000, 1_802_000) + rec("i3", 1_803_000, 2_700_000)
    for inc, minute, active in (("i2", 15, 4_000), ("i3", 30, 4_000)):
        recs += [{"target": t, "inc": inc, "minute": minute, "active": [active, 60_000], "dispatched": 10} for t in (A, B)]
    ex = [{"reason": "Completed", "exit_code": 0}] * 2
    g = generator_restarts(recs, cfg, ex, [A, B])
    assert g["consecutive_meeting_budget"] == 2 and all(v == 4_000 for r in g["restarts"]
                                                        for v in r["first_dispatch_ms"].values()), g
    late = [dict(r, active=[12_000, 60_000]) if r.get("inc") == "i3" and "minute" in r and r["target"] == B else r
            for r in recs]
    assert generator_restarts(late, cfg, ex, [A, B])["consecutive_meeting_budget"] == 1
    short = rec("i1", 0, 500_000, 502_000) + recs[4:]
    assert generator_restarts(short, cfg, ex, [A, B])["consecutive_meeting_budget"] == 1
    print("ingress_faults selftest: peak, plateau, constant traffic, service in faults and recoveries, identity and "
          "the generator's restart budget: OK")
    return 0


if __name__ == "__main__":
    sys.exit(selftest())
