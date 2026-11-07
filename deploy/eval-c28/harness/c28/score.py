"""The C2.8 scorer: paired day-block bootstrap, Holm correction and the frozen classification (EVAL-C28-MANIFEST
sections 7 and 8).

Input (one JSON study): the prerequisites of section 1, observed safety/contract/resource breaches, artifact and
cleanup status, the per-candidate fallback/startup/quota gate evidence, and the scored blocks: one per (arm, profile,
seed) with its validity measurements and per-window metrics (reduce.block_metrics). Output: one classification per
candidate arm (PA-N, PA-E) and for the study, with every comparison, interval and reason.

Statistics. Blocks are paired by (profile, seed). For a comparison of arm A with comparator B the paired daily
differences d_i = A_i - B_i are resampled as whole days: one index matrix of 10,000 resamples drawn once with
random.Random(2808) for the number of paired days, applied to every comparison (all arm data of a day move together).
The two-sided 95 % interval is the 2.5th/97.5th percentile of the resampled means (nearest rank). The bootstrap p value
of "no difference" is min(1, 2 min((#{m <= 0} + 1)/(R + 1), (#{m >= 0} + 1)/(R + 1))). Superiority claims against
more than one comparator are Holm-adjusted at family-wise 0.05 across (comparator x startup profile).

Classification precedence (a decision of this implementation, recorded here and in the report):
  1. FAIL      an observed safety/contract/resource breach, a product-caused validity failure, or a failed fallback,
               startup or quota gate (any of these is FAIL whatever else is true);
  2. NOT READY a section 1 prerequisite is not met;
  3. INCOMPLETE missing blocks, artifacts or cleanup, or an unresolved validity failure;
  4. FAIL      a statistically supported margin violation (the whole interval beyond a margin), or a missed absolute
               G5 bar on ordinary windows (with complete data);
  5. INCONCLUSIVE  an interval crossing a margin, unmatched surplus, no eligible use, or no benefit route;
  6. PASS.
None of these becomes PASS by waiting.
"""

from __future__ import annotations

import math
import random
from typing import Dict, List, Optional, Sequence, Tuple

from . import frozen as F


# --- statistics -------------------------------------------------------------------------------------------------

_INDEX_CACHE: Dict[int, List[List[int]]] = {}


def resample_indices(n: int) -> List[List[int]]:
    """The frozen day-block resampling matrix for n paired days (seed 2808, 10,000 resamples)."""
    if n not in _INDEX_CACHE:
        rng = random.Random(F.BOOTSTRAP_SEED)
        _INDEX_CACHE[n] = [[rng.randrange(n) for _ in range(n)] for _ in range(F.BOOTSTRAP_RESAMPLES)]
    return _INDEX_CACHE[n]


def bootstrap(diffs: Sequence[float]) -> Dict:
    n = len(diffs)
    if n == 0:
        raise ValueError("no paired days")
    means = sorted(sum(diffs[i] for i in idx) / n for idx in resample_indices(n))
    r = len(means)
    lo = means[max(0, math.floor(0.025 * r))]
    hi = means[min(r - 1, math.ceil(0.975 * r) - 1)]
    le0 = sum(1 for m in means if m <= 0)
    ge0 = sum(1 for m in means if m >= 0)
    p = min(1.0, 2 * min((le0 + 1) / (r + 1), (ge0 + 1) / (r + 1)))
    return {"n": n, "mean": sum(diffs) / n, "ci95": [lo, hi], "p": p, "means": means}


def holm(pvalues: Dict[str, float], alpha: float = F.FAMILY_ALPHA) -> Dict[str, Dict]:
    """Holm step-down: adjusted p values and rejections (ties keep their order by name, deterministically)."""
    order = sorted(pvalues, key=lambda k: (pvalues[k], k))
    m = len(order)
    out, running, stop = {}, 0.0, False
    for i, k in enumerate(order):
        adj = min(1.0, (m - i) * pvalues[k])
        running = max(running, adj)
        reject = not stop and pvalues[k] <= alpha / (m - i)
        stop = stop or not reject
        out[k] = {"p": pvalues[k], "p_holm": running, "reject": reject, "level": 1 - alpha / (m - i)}
    return out


def interval_at(means: List[float], level: float) -> List[float]:
    r = len(means)
    a = (1 - level) / 2
    return [means[max(0, math.floor(a * r))], means[min(r - 1, math.ceil((1 - a) * r) - 1)]]


# --- block access -----------------------------------------------------------------------------------------------

def _valid(block: Dict) -> Tuple[bool, str]:
    """(usable, reason). A block marked invalid, or whose validity measurements miss a bar, is not usable."""
    v = block.get("validity", {})
    if block.get("invalid_cause"):
        return False, f"invalid ({block['invalid_cause']})"
    misses = []
    if v.get("recorder_coverage", 0) < F.VALIDITY["recorder_coverage"]:
        misses.append("recorder coverage")
    if v.get("max_transition_gap_s", math.inf) > F.VALIDITY["max_transition_gap_s"]:
        misses.append("transition gap")
    if v.get("dispatch_fraction_min", 0) < F.VALIDITY["dispatch_fraction"]:
        misses.append("dispatch")
    if v.get("timestamp_skew_s", math.inf) > F.VALIDITY["timestamp_skew_s"]:
        misses.append("timestamp skew")
    return (not misses), ", ".join(misses)


def _metric(block: Dict, windows: Optional[Sequence[str]], key: str) -> Optional[float]:
    """Sum of a window metric over windows (or the whole day); error_fraction is recomputed from counts."""
    parts = [block["day"]] if windows is None else [block["windows"][w] for w in windows]
    if key == "error_fraction":
        req = sum(p["requests"] for p in parts)
        return sum(p["failures"] for p in parts) / req if req else None
    if key == "mean_surplus_replicas":
        secs = sum(p["minutes"] for p in parts) * 60
        return sum(p["surplus_replica_seconds"] for p in parts) / secs
    return float(sum(p[key] for p in parts))


def _pairs(blocks: List[Dict], a: str, b: str, profile: str) -> Dict[int, Tuple[Dict, Dict]]:
    by = {(x["arm"], x["profile"], x["seed"]): x for x in blocks if _valid(x)[0]}
    return {s: (by[(a, profile, s)], by[(b, profile, s)]) for s in F.CONFIRMATION_SEEDS
            if (a, profile, s) in by and (b, profile, s) in by}


def compare(blocks, a, b, profile, key, windows=None) -> Dict:
    pairs = _pairs(blocks, a, b, profile)
    seeds = sorted(pairs)
    da = [_metric(pairs[s][0], windows, key) for s in seeds]
    db = [_metric(pairs[s][1], windows, key) for s in seeds]
    if any(v is None for v in da + db):
        return {"arm": a, "comparator": b, "profile": profile, "key": key, "windows": windows, "seeds": seeds,
                "error": "a paired day has no requests for this endpoint"}
    diffs = [x - y for x, y in zip(da, db)]
    res = bootstrap(diffs)
    sa, sb = (sum(_metric(pairs[s][i], None, "mean_surplus_replicas") for s in seeds) / len(seeds) for i in (0, 1))
    matched = abs(sa - sb) <= max(F.SURPLUS_MATCH_RELATIVE * sb, F.SURPLUS_MATCH_FLOOR_REPLICAS)
    return {"arm": a, "comparator": b, "profile": profile, "key": key, "windows": list(windows) if windows else None,
            "seeds": seeds, "arm_values": da, "comparator_values": db, "diffs": diffs, "mean": res["mean"],
            "ci95": res["ci95"], "p": res["p"], "arm_mean": sum(da) / len(da), "comparator_mean": sum(db) / len(db),
            "surplus_arm": sa, "surplus_comparator": sb, "surplus_matched": matched, "_means": res["means"]}


def _margin(c: Dict, margin: float) -> str:
    """pass (upper bound within the margin), fail (lower bound beyond it) or inconclusive."""
    lo, hi = c["ci95"]
    if hi <= margin:
        return "pass"
    if lo > margin:
        return "fail"
    return "inconclusive"


# --- classification ---------------------------------------------------------------------------------------------

def classify_candidate(study: Dict, arm: str) -> Dict:
    blocks = study.get("blocks", [])
    reasons: Dict[str, List[str]] = {k: [] for k in ("FAIL", "NOT READY", "INCOMPLETE", "FAIL-STAT", "INCONCLUSIVE")}
    comparisons: List[Dict] = []

    # 1. Observed breaches and failed gates.
    for b in study.get("safety_breaches", []):
        reasons["FAIL"].append(f"safety breach: {b}")
    for b in study.get("contract_failures", []):
        reasons["FAIL"].append(f"contract failure: {b}")
    res = study.get("resources", {})
    if res.get("g3_pass") is False:
        reasons["FAIL"].append(f"G3 resource bar breached: {res.get('detail', '')}".strip())
    gates = study.get("gates", {}).get(arm, {})
    for g in ("fallback", "blocked_slow_startup", "quota_guards", "quota_observable", "quota_resumed"):
        if gates.get(g) is False:
            reasons["FAIL"].append(f"{g} gate failed")
    for x in blocks:
        if x.get("invalid_cause") == "product":
            reasons["FAIL"].append(f"product-caused invalid block {x['arm']}/{x['profile']}/{x['seed']}")

    # 2. Prerequisites.
    for name, ok in study.get("prerequisites", {}).items():
        if ok is not True:
            reasons["NOT READY"].append(f"prerequisite not met: {name}")
    if not study.get("prerequisites"):
        reasons["NOT READY"].append("prerequisites not recorded")
    if arm == "PA-N" and study.get("prerequisites", {}).get("family_restriction") is not True:
        reasons["NOT READY"].append("PA-N needs the family-restriction mechanism (section 1, prerequisite 4)")
    if arm == "PA-E" and study.get("prerequisites", {}).get("policy_cost_binding") is not True:
        reasons["NOT READY"].append("PA-E needs the same-policy cost binding for E1 selection (G4)")

    # 3. Completeness.
    if res.get("g3_pass") is None:
        reasons["INCOMPLETE"].append("G3 resource evidence missing")
    if not study.get("artifacts_complete"):
        reasons["INCOMPLETE"].append("required artifacts missing")
    if not study.get("cleanup_verified"):
        reasons["INCOMPLETE"].append("cleanup not verified")
    for g in ("fallback", "blocked_slow_startup", "quota_guards", "quota_observable", "quota_resumed"):
        if gates.get(g) is None:
            reasons["INCOMPLETE"].append(f"{g} gate evidence missing")
    if arm == "PA-E" and gates.get("g4_qualified") is None:
        reasons["INCOMPLETE"].append("G4 E1 qualification evidence missing (independent recomputation from the journal)")
    needed = set(F.ARMS)                         # the primary matrix: five blocks per arm per profile (section 5)
    for x in blocks:
        ok, why = _valid(x)
        if not ok and x.get("invalid_cause") != "product":
            reasons["INCOMPLETE"].append(f"unusable block {x['arm']}/{x['profile']}/{x['seed']}: {why}")
    for profile in F.PROFILES:
        for other in sorted(needed):
            have = {x["seed"] for x in blocks if x["arm"] == other and x["profile"] == profile and _valid(x)[0]}
            missing = sorted(set(F.CONFIRMATION_SEEDS) - have)
            if missing:
                reasons["INCOMPLETE"].append(f"{other}/{profile}: no valid block for seeds {missing}")

    complete = not reasons["INCOMPLETE"] and not reasons["NOT READY"]
    if complete and not reasons["FAIL"]:
        # 4/5. G5 noninferiority (both profiles, each NI comparator) and the absolute SLO on ordinary windows.
        for profile in F.PROFILES:
            for comp in F.NI_COMPARATORS:
                for key, margin in (("slo_violation_minutes", F.NI_SLO_MINUTES_PER_DAY),
                                    ("error_fraction", F.NI_ERROR_FRACTION)):
                    c = compare(blocks, arm, comp, profile, key)
                    c["gate"], c["margin"] = "G5 noninferiority", margin
                    comparisons.append(c)
                    if "error" in c:
                        reasons["INCONCLUSIVE"].append(f"G5 {key} vs {comp}/{profile}: {c['error']}")
                        continue
                    c["verdict"] = _margin(c, margin)
                    if not c["surplus_matched"]:
                        reasons["INCONCLUSIVE"].append(f"G5 vs {comp}/{profile}: surplus not matched "
                                                       f"({c['surplus_arm']:.3f} vs {c['surplus_comparator']:.3f})")
                    elif c["verdict"] == "fail":
                        reasons["FAIL-STAT"].append(f"G5 {key} vs {comp}/{profile}: interval {c['ci95']} beyond {margin}")
                    elif c["verdict"] == "inconclusive":
                        reasons["INCONCLUSIVE"].append(f"G5 {key} vs {comp}/{profile}: interval {c['ci95']} crosses {margin}")
            own = [x for x in blocks if x["arm"] == arm and x["profile"] == profile and _valid(x)[0]]
            ok_m = sum(x["windows"][w]["slo_ok_minutes"] for x in own for w in F.ORDINARY_WINDOWS)
            obs_m = sum(x["windows"][w]["observed_minutes"] for x in own for w in F.ORDINARY_WINDOWS)
            share = ok_m / obs_m if obs_m else 0.0
            if share < F.ORDINARY_SLO_MINUTE_SHARE:
                reasons["FAIL-STAT"].append(f"G5 absolute SLO on ordinary windows ({profile}): {share:.4f} < 0.99")

        # Robustness noninferiority versus PA-R.
        for profile in F.PROFILES:
            for w in F.ROBUSTNESS_WINDOWS:
                for key, margin in (("slo_violation_minutes", F.ROBUSTNESS_MINUTES_PER_HOUR * F.window_hours(w)),
                                    ("error_fraction", F.NI_ERROR_FRACTION)):
                    c = compare(blocks, arm, F.ROBUSTNESS_COMPARATOR, profile, key, [w])
                    c["gate"], c["margin"] = f"robustness {w}", margin
                    comparisons.append(c)
                    if "error" in c:
                        reasons["INCONCLUSIVE"].append(f"robustness {w} {key}/{profile}: {c['error']}")
                        continue
                    c["verdict"] = _margin(c, margin)
                    if c["verdict"] == "fail":
                        reasons["FAIL-STAT"].append(f"robustness {w} {key}/{profile}: interval {c['ci95']} beyond {margin}")
                    elif c["verdict"] == "inconclusive":
                        reasons["INCONCLUSIVE"].append(f"robustness {w} {key}/{profile}: interval {c['ci95']} crosses {margin}")

        # G4: an E1 claim needs a genuinely qualified E1 (paired evidence, costs, dwell); its failure does not touch PA-N.
        if arm == "PA-E" and gates.get("g4_qualified") is False:
            reasons["INCONCLUSIVE"].append("G4: E1 did not qualify as a challenger; no E1 efficacy claim")
        # Eligible use: intention-to-treat, but a family never used cannot claim efficacy.
        for profile in F.PROFILES:
            own = [x for x in blocks if x["arm"] == arm and x["profile"] == profile and _valid(x)[0]]
            key = "e1_selected_fraction" if arm == "PA-E" else "eligible_fraction"
            if not any((x.get(key) or 0) > 0 for x in own):
                reasons["INCONCLUSIVE"].append(f"{profile}: no eligible use ({key} is zero in every block)")

        # Useful benefit: route A (>= 20 % fewer violation minutes on predictable windows) or, when the comparator
        # had none, route B (>= 10 % fewer provisioned replica-seconds); Holm across comparators x profiles.
        tests, info = {}, {}
        for profile in F.PROFILES:
            for comp in F.BENEFIT_COMPARATORS:
                c = compare(blocks, arm, comp, profile, "slo_violation_minutes", F.PREDICTABLE_WINDOWS)
                route, threshold = "A", F.BENEFIT_SLO_REDUCTION
                if "error" not in c and c["comparator_mean"] == 0:
                    c = compare(blocks, arm, comp, profile, "provisioned_replica_seconds")
                    route, threshold = "B", F.BENEFIT_REPLICA_SECONDS_REDUCTION
                c["gate"], c["route"], c["threshold"] = "useful benefit", route, threshold
                comparisons.append(c)
                if "error" in c:
                    continue
                k = f"{comp}/{profile}"
                tests[k] = c["p"]
                info[k] = c
        h = holm(tests) if tests else {}
        passed = []
        for k, c in info.items():
            reduction = (c["comparator_mean"] - c["arm_mean"]) / c["comparator_mean"] if c["comparator_mean"] else 0.0
            c["reduction"] = reduction
            c.update({"p_holm": h[k]["p_holm"], "holm_reject": h[k]["reject"],
                      "ci_holm": interval_at(c["_means"], h[k]["level"])})
            ok = reduction >= c["threshold"] and h[k]["reject"] and c["mean"] < 0 and c["surplus_matched"] \
                if c["route"] == "A" else reduction >= c["threshold"] and h[k]["reject"] and c["mean"] < 0
            c["verdict"] = "pass" if ok else "not established"
            if ok:
                passed.append(f"route {c['route']} vs {k}")
        if not passed:
            reasons["INCONCLUSIVE"].append("no useful-benefit route passed")
    for c in comparisons:
        c.pop("_means", None)

    if reasons["FAIL"]:
        outcome = "FAIL"
    elif reasons["NOT READY"]:
        outcome = "NOT READY"
    elif reasons["INCOMPLETE"]:
        outcome = "INCOMPLETE"
    elif reasons["FAIL-STAT"]:
        outcome = "FAIL"
    elif reasons["INCONCLUSIVE"]:
        outcome = "INCONCLUSIVE"
    else:
        outcome = "PASS"
    return {"arm": arm, "outcome": outcome, "reasons": {k: v for k, v in reasons.items() if v},
            "comparisons": comparisons}


def score(study: Dict) -> Dict:
    """Per-candidate classifications and the study outcome. PASS claims only the passing family/configuration."""
    if study.get("schema") != "c28-study/v1":
        raise ValueError("schema must be c28-study/v1")
    cands = {arm: classify_candidate(study, arm) for arm in F.CANDIDATE_ARMS}
    outcomes = {a: c["outcome"] for a, c in cands.items()}
    if any(c["reasons"].get("FAIL") for c in cands.values()):
        overall = "FAIL"                         # an observed breach fails the study, whatever passed
    elif "PASS" in outcomes.values():
        overall = "PASS"
    else:
        for o in ("NOT READY", "INCOMPLETE", "FAIL", "INCONCLUSIVE"):
            if o in outcomes.values():
                overall = o
                break
    return {"schema": "c28-score/v1", "outcome": overall,
            "claims": [a for a, o in outcomes.items() if o == "PASS"], "candidates": cands,
            "bootstrap": {"seed": F.BOOTSTRAP_SEED, "resamples": F.BOOTSTRAP_RESAMPLES}}
