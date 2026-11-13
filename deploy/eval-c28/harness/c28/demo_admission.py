"""The bounded admission exercise's verdict (ADMISSION-DESIGN draft 2, section 3), recomputed from its evidence file.

    verdict(evidence) -> {"verdict": STOPPED | INCOMPLETE | LIMIT | FAIL | PASS, "criteria": {...}, "reasons": [...]}

The evidence (schema c28-admission-evidence/v1, written by the executor; every field is required):
  config         the admission Job's generator configuration (demo.bounded_config, label admission)
  lines          the generator's pod-log lines (raw strings), the final full read included
  ingress        {target: [[t_s, value], ...]} RAW samples of c28_ingress_requests_total{target}
  queue          {target: [[t_s, value], ...]} the fixture's waiting queue (sum of c28_queue), 15 s steps
  throttled      {"generator": [[t_s, f]], "ingress": [[t_s, f]], "fixture": [[t_s, f]]} throttled fraction
  steal          {node: [[t_s, percent], ...]} one-minute steps over the whole run, W and S
  endpoints      [{"t_s": t, "target": name, "ready": [pod uid, ...]}] sampled (sampled evidence, not a watch)
  pod_requests   {target: {pod uid: increase of c28_requests_total over M}}
  pods           {"expected": {target: [12 pod uids]}, "baseline_s": t, "completion_s": t (the Job's end),
                  "watch": [{"from_s": t, "to_s": t}] the covered spans of the pod watch,
                  "transitions": [{"t_s", "pod_uid", "what": ready-lost | ready-gained | restart | container-changed |
                                   deleted | oom}]}
  freshness      {series name: worst staleness in seconds over the run}
  job            {"status": Complete | Failed | ..., "exit_code": int | null, "created_s": t, "pod_started_s": t}
  stops          [{"t_s", "why"}] the executor's stop conditions (node pressure, neighbour alert, steal, OOM, ...)
  limits         the frozen criteria (FROZEN below; the evidence must carry exactly these)

Aggregation, per target, over the measured window M = [origin + 10 min, origin + 40 min) unless named otherwise:
  rows           every minute 0..39 of both targets "observed" by demo_reduce (identity, carries conserved, clock
                 verified, running balance reconciled); anything else INCOMPLETE
  drops          zero dropped arrivals over settle AND M (minutes 0..39)                              -> LIMIT
  unresolved     zero unresolved outcomes, minutes 0..39                                              -> INCOMPLETE
  failure bound  (failed + unresolved + dropped) / due over M <= 0.001 (r4: offered arrivals)          -> FAIL
  latency        from lat.all summed over M: share of outcomes above 200 ms <= 5 %, above 500 ms <= 1 % -> FAIL
  queue          r4's rule on 60 s bins of the waiting queue (E = 3, END / 12 <= 1.5, not (INCREASE > 1.5 and
                 SLOPE > 0.125 per minute)); every bin needs a sample, gaps <= 2 steps               -> FAIL
  throttling     mean generator and ingress throttled fraction over M <= 0.05 each                    -> LIMIT
  steal          > 5 % for 5 consecutive one-minute bins on W or S during the run                     -> STOPPED
  endpoints      every sample: exactly the 12 expected pods ready; samples at most 45 s apart over M  -> FAIL
  distribution   each expected pod's request increase over M within 0.5x..1.5x of the target's total / 12 -> FAIL
  pods           no ready-lost, restart, container-changed, deleted or oom transition from the baseline through
                 completion; the watch covers [baseline, completion] without a gap > 30 s             -> FAIL
  freshness      exactly the inventory of freshness_inventory(targets), each finite and <= 60 s, from RAW samples
                                                                                                     -> INCOMPLETE
  throttling     each of generator, ingress, fixture: every 60 s bin of M has a finite sample, gaps <= 2 steps of 15 s
  collection     collection_problems empty (the executor's own collection errors)                    -> INCOMPLETE
  job            Complete with exit code 0                                                            -> INCOMPLETE
Precedence: STOPPED > INCOMPLETE > LIMIT > FAIL > PASS."""

from __future__ import annotations

import math
from typing import Dict, List, Tuple

from . import demo, demo_reduce

SCHEMA = "c28-admission-evidence/v1"
SETTLE_MIN, RUN_MIN = 10, 40
REPLICAS = 12
FROZEN = {"max_failure_fraction": 0.001, "max_share_above_200ms": 0.05, "max_share_above_500ms": 0.01,
          "queue": {"edge_bins": 3, "max_increase": 1.5, "max_slope_per_min": 0.125,
                    "max_end_backlog_per_replica": 1.5, "step_s": 15, "max_gap_steps": 2},
          "max_throttled_fraction": 0.05, "steal_percent": 5, "steal_bins": 5, "endpoint_max_gap_s": 45,
          "distribution": [0.5, 1.5], "watch_max_gap_s": 30, "max_staleness_s": 60}
ORDER = ("STOPPED", "INCOMPLETE", "LIMIT", "FAIL", "PASS")
REQUIRED = ("schema", "config", "lines", "ingress", "queue", "throttled", "steal", "endpoints", "pod_requests", "pods",
            "freshness", "job", "stops", "limits", "collection_problems")
THROTTLED = ("generator", "ingress", "fixture")


def freshness_inventory(targets: List[str]) -> List[str]:
    """The RAW-sample sources whose staleness the evidence must state, every one: the ingress counter, the waiting
    queue and the per-pod request counters of each target; both cAdvisor counters (periods and throttled periods) of
    each throttling series; the steal source (node_cpu_seconds_total, mode steal) of W and S."""
    out = [f"{k}/{t}" for t in targets for k in ("ingress", "queue", "requests")]
    out += [f"{k}/{w}" for w in THROTTLED for k in ("cfs_periods", "cfs_throttled")]
    return sorted(out + ["steal_raw/w", "steal_raw/s"])


def _crit(out: Dict, name: str, ok, failing: str, **data) -> None:
    """ok: True (pass), False (the criterion's own outcome `failing`), None (missing evidence: INCOMPLETE)."""
    out[name] = {"result": "PASS" if ok is True else failing if ok is False else "INCOMPLETE", **data}


def _bins(samples: List[List[float]], lo_s: float, n: int, step_s: float, max_gap_steps: int
          ) -> Tuple[List[float], List[str]]:
    """Means of n 60 s bins from lo_s; problems for an empty bin or a gap above max_gap_steps steps of step_s."""
    pts = sorted((float(t), float(v)) for t, v in samples if lo_s <= float(t) < lo_s + 60 * n)
    probs, means = [], []
    for k in range(n):
        vals = [v for t, v in pts if lo_s + 60 * k <= t < lo_s + 60 * (k + 1)]
        if not vals:
            probs.append(f"bin {k} has no sample")
        means.append(sum(vals) / len(vals) if vals else float("nan"))
    ts = [lo_s] + [t for t, _ in pts] + [lo_s + 60 * n]
    if any(b - a > step_s * (max_gap_steps + 1) for a, b in zip(ts, ts[1:])):
        probs.append(f"a gap above {max_gap_steps} steps of {step_s:g} s")
    return means, probs


def _slope(ys: List[float]) -> float:
    n = len(ys)
    xm, ym = (n - 1) / 2, sum(ys) / n
    den = sum((x - xm) ** 2 for x in range(n))
    return sum((x - xm) * (y - ym) for x, y in zip(range(n), ys)) / den


def _mean_over(samples, lo_s: float, hi_s: float):
    vals = [float(v) for t, v in samples or [] if lo_s <= float(t) < hi_s]
    return sum(vals) / len(vals) if vals else None


def verdict(ev: Dict) -> Dict:
    missing = [k for k in REQUIRED if k not in ev]
    if missing or ev.get("schema") != SCHEMA:
        return {"verdict": "INCOMPLETE", "criteria": {}, "reasons": [f"not a {SCHEMA} evidence file: missing {missing}"]}
    if ev["limits"] != FROZEN:
        return {"verdict": "INCOMPLETE", "criteria": {}, "reasons": ["the evidence's limits are not the frozen ones"]}
    cfg = ev["config"]
    why = demo.config_problems(cfg)
    if why or cfg.get("label") != "admission":
        return {"verdict": "INCOMPLETE", "criteria": {}, "reasons": [f"not an admission configuration: {why}"]}
    origin_s = cfg["origin_ms"] / 1000
    m_lo, m_hi = origin_s + SETTLE_MIN * 60, origin_s + RUN_MIN * 60
    crit: Dict = {}
    records, _ = demo_reduce.parse_lines(ev["lines"])
    red = demo_reduce.reduce(cfg, records, ev["ingress"], cfg["origin_ms"], cfg["origin_ms"] + RUN_MIN * 60_000)
    bounds = list(cfg["latency_bounds_ms"])
    i200, i500 = bounds.index(200), bounds.index(500)
    for t in cfg["targets"]:
        rows = red["targets"][t]["rows"]
        bad = [(r["minute"], r["reasons"]) for r in rows if r["state"] != "observed"]
        _crit(crit, f"{t}/rows", None if bad else True, "INCOMPLETE", not_observed=bad[:10])
        gs = [r["generator"] for r in rows]
        drops = sum(sum(g["dropped"].values()) for g in gs)
        unresolved = sum(g["unresolved"] for g in gs)
        _crit(crit, f"{t}/drops", drops == 0, "LIMIT", value=drops)
        _crit(crit, f"{t}/unresolved", True if unresolved == 0 else None, "INCOMPLETE", value=unresolved)
        meas = [g for r, g in zip(rows, gs) if SETTLE_MIN <= r["minute"] < RUN_MIN]
        due = sum(g["due"] for g in meas)
        failures = sum(g["failed"] + g["unresolved"] + sum(g["dropped"].values()) for g in meas)
        frac = failures / due if due else None
        _crit(crit, f"{t}/failure_bound", None if frac is None else frac <= FROZEN["max_failure_fraction"], "FAIL",
              value=frac, due=due, failures=failures)
        alls = [g.get("lat_all") for g in meas]
        if not alls or any(a is None for a in alls):
            _crit(crit, f"{t}/latency", None, "FAIL", why="lat.all missing in a measured minute")
        else:
            tot = [sum(c) for c in zip(*alls)]
            n = sum(tot)
            a200, a500 = (sum(tot[i200 + 1:]) / n, sum(tot[i500 + 1:]) / n) if n else (None, None)
            ok = None if not n else (a200 <= FROZEN["max_share_above_200ms"] and a500 <= FROZEN["max_share_above_500ms"])
            _crit(crit, f"{t}/latency", ok, "FAIL", share_above_200ms=a200, share_above_500ms=a500, outcomes=n)
        q = FROZEN["queue"]
        means, probs = _bins(ev["queue"].get(t) or [], m_lo, RUN_MIN - SETTLE_MIN, q["step_s"], q["max_gap_steps"])
        if probs:
            _crit(crit, f"{t}/queue", None, "FAIL", problems=probs[:5])
        else:
            e = q["edge_bins"]
            end, start = sum(means[-e:]) / e, sum(means[:e]) / e
            inc, slope = end - start, _slope(means)
            ok = end / REPLICAS <= q["max_end_backlog_per_replica"] and not (inc > q["max_increase"]
                                                                              and slope > q["max_slope_per_min"])
            _crit(crit, f"{t}/queue", ok, "FAIL", end=end, increase=inc, slope_per_min=slope)
        expected = set(ev["pods"]["expected"].get(t) or [])
        samples = sorted((s for s in ev["endpoints"] if s.get("target") == t and m_lo <= s["t_s"] < m_hi),
                         key=lambda s: s["t_s"])
        ts = [m_lo] + [s["t_s"] for s in samples] + [m_hi]
        if len(expected) != REPLICAS or not samples or max(b - a for a, b in zip(ts, ts[1:])) > FROZEN["endpoint_max_gap_s"]:
            _crit(crit, f"{t}/endpoints", None, "FAIL", samples=len(samples), expected=len(expected))
        else:
            wrong = [s["t_s"] for s in samples if set(s["ready"]) != expected]
            _crit(crit, f"{t}/endpoints", not wrong, "FAIL", samples=len(samples), wrong_at=wrong[:5],
                  note="sampled evidence")
        reqs = ev["pod_requests"].get(t) or {}
        if set(reqs) != expected or len(expected) != REPLICAS:
            _crit(crit, f"{t}/distribution", None, "FAIL", pods=len(reqs))
        else:
            total = sum(reqs.values())
            lo, hi = FROZEN["distribution"]
            out = {u: v for u, v in reqs.items() if not lo * total / REPLICAS <= v <= hi * total / REPLICAS}
            _crit(crit, f"{t}/distribution", bool(total) and not out, "FAIL", total=total, outside=out)
    q = FROZEN["queue"]
    for who in THROTTLED:
        series = ev["throttled"].get(who) or []
        _, probs = _bins(series, m_lo, RUN_MIN - SETTLE_MIN, q["step_s"], q["max_gap_steps"])
        finite = all(isinstance(v, (int, float)) and math.isfinite(v) for t, v in series if m_lo <= t < m_hi)
        m = _mean_over(series, m_lo, m_hi) if not probs and finite else None
        if who == "fixture":
            crit["throttled/fixture"] = {"result": "DIAGNOSTIC" if m is not None else "INCOMPLETE", "mean": m,
                                         "problems": probs[:3]}
            continue
        _crit(crit, f"throttled/{who}", None if m is None else m <= FROZEN["max_throttled_fraction"], "LIMIT",
              mean=m, problems=probs[:3])
    run_lo, run_hi = origin_s, m_hi
    for node, samples in sorted(ev["steal"].items()):
        means, probs = _bins(samples, run_lo, RUN_MIN, 60, 1)        # one-minute steps, one missing step at most
        if probs:
            _crit(crit, f"steal/{node}", None, "STOPPED", problems=probs[:5])
            continue
        run = worst = 0
        for v in means:
            run = run + 1 if v > FROZEN["steal_percent"] else 0
            worst = max(worst, run)
        _crit(crit, f"steal/{node}", worst < FROZEN["steal_bins"], "STOPPED", longest_run=worst)
    if len(ev["steal"]) < 2:
        _crit(crit, "steal/nodes", None, "STOPPED", nodes=sorted(ev["steal"]))
    pods = ev["pods"]
    base, done = pods.get("baseline_s"), pods.get("completion_s")
    covered = base is not None and done is not None and base <= done
    cur = base
    for a, b in sorted((w["from_s"], w["to_s"]) for w in pods.get("watch") or []):
        if not covered:
            break
        if b <= cur:
            continue
        if a - cur > FROZEN["watch_max_gap_s"] or (cur == base and a > base):
            covered = False                       # a gap above the limit, or the watch began after the baseline
        cur = max(cur, b)
    covered = bool(covered) and cur is not None and done is not None and cur >= done
    bad = [x for x in pods.get("transitions") or [] if base is not None and x["t_s"] >= base]
    _crit(crit, "pods", None if not covered else not bad, "FAIL", transitions=bad[:10])
    want = freshness_inventory(cfg["targets"])
    fr = ev["freshness"]
    missing = sorted(set(want) - set(fr))
    stale = {k: fr[k] for k in want if k in fr and not (isinstance(fr[k], (int, float)) and math.isfinite(fr[k])
                                                        and fr[k] <= FROZEN["max_staleness_s"])}
    _crit(crit, "freshness", True if not missing and not stale else None, "INCOMPLETE", missing=missing, stale=stale)
    _crit(crit, "collection", True if not ev["collection_problems"] else None, "INCOMPLETE",
          problems=list(ev["collection_problems"])[:10])
    job = ev["job"]
    _crit(crit, "job", True if job.get("status") == "Complete" and job.get("exit_code") == 0 else None, "INCOMPLETE",
          status=job.get("status"), exit_code=job.get("exit_code"))
    _crit(crit, "stops", not ev["stops"], "STOPPED", stops=ev["stops"])
    results = {c["result"] for c in crit.values() if c["result"] != "DIAGNOSTIC"}
    final = next(o for o in ORDER if o in results or o == "PASS")
    return {"verdict": final, "criteria": crit,
            "reasons": sorted(k for k, c in crit.items() if c["result"] not in ("PASS", "DIAGNOSTIC")),
            "window": {"settle_from_s": origin_s, "measured_from_s": m_lo, "measured_to_s": m_hi}}
