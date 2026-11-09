#!/usr/bin/env python3
"""Pre-declared selection and transformation of the Azure Functions 2019 trace (revision 2) for the C2.8 public-trace
demonstration. PROPOSAL: the contract that freezes these rules is a DRAFT pending Codex review.

This file is the executable form of `trace-contract-azure-functions-2019` (context workspace,
agent-coordination/09-real-world-research/c28-staging/trace-demo/). It is NOT part of C2.8 scoring and does not replace
the C2.8 manifest's ordinary warm-up. It never runs a forecaster and never computes forecast accuracy.

Wording rule: what this produces are "requests derived from per-minute invocation counts". The dataset records
invocation counts per function and minute AFTER execution; it holds no arrival timestamps, so nothing here is "real
traffic" or "real arrival timestamps".

Data source: Azure Functions Trace 2019 (Microsoft), licensed CC BY 4.0. Cite: Shahrad et al., "Serverless in the Wild:
Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider", USENIX ATC 2020. Modifications made
here: HTTP-trigger functions summed per application, selection of a few applications, per-application normalization,
floor and clip, conversion to an arrival-rate plan.

Standard library only, one file, so it runs under `python -I` (no script-directory imports) against untrusted data
passed by path. Subcommands:

  select   --data DIR --sums SHA256SUMS --out selection.json
  prepare  --data DIR --sums SHA256SUMS --selection selection.json --out prepared.json
  plan     --prepared prepared.json --app OWNER:APP --capacity C --out plan.json [--repetition N]
  rules    print the frozen rules and their digest
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import sys
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------------------------------------------------
# The frozen rules. The contract JSON carries the same object and its digest; tests pin the digest, so a rule change is
# a deliberate, reviewed edit of both, never a silent drift.
# ---------------------------------------------------------------------------------------------------------------------
RULES: Dict = {
    "schema": "c28-trace-rules/v1",
    "dataset": "azure-functions-2019-rev2",
    "invocation_file_pattern": "invocations_per_function_md.anon.d{day:02d}.csv",
    "days": 14,
    "minutes_per_day": 1440,
    "header_prefix": ["HashOwner", "HashApp", "HashFunction", "Trigger"],
    "trigger": "http",
    "app_key": ["HashOwner", "HashApp"],
    "history_days": [1, 2, 3, 4, 5, 6, 7],
    "evaluation_days": [8, 9, 10, 11, 12, 13, 14],
    # Missing data: a function absent from a day's file contributes zero invocations that day (the files list invoked
    # functions). An application must have at least one HTTP row and a positive HTTP total on EVERY day, else it is
    # ineligible. A duplicate (owner, app, function) HTTP row in one day, or a non-integer/negative count, makes the
    # application ineligible (never repaired). A malformed row or header aborts the run.
    "missing_function_day": "zero",
    "require_present_all_days": True,
    # Volume band and activity, measured on the history days ONLY (no look at evaluation-day values).
    "history_mean_per_minute_band": [10.0, 100000.0],
    "history_min_active_fraction": 0.9,
    # Seeded, volume-stratified draw from the eligible set.
    "strata": 3,
    "per_stratum": 2,
    "distinct_owner": True,
    "seed": 20261109,
    "live_apps": 1,                   # the first entry of the seeded live_order is replayed live; all chosen offline
    # Normalization to the fixture: u = offered_RPS / c (operational replica equivalents, as in the C2.8 manifest).
    "peak_quantile": 0.995,           # nearest-rank quantile of the history-day per-minute counts
    "u_peak": 9.0,                    # the history peak maps to 0.75 x maxReplicas(12) = 9 c
    "u_min": 0.1,                     # floor: the ingress counter advances every minute
    "u_max": 10.0,                    # clip: the manifest keeps demand <= 10 c
    "max_replicas": 12,
    "rounding": "python-round-half-even",
    # Timing.
    "intra_minute": "k6 ramping-arrival-rate: 1 s linear ramp to the minute's rate, then constant for 59 s",
    "ramp_ms": 1000,
    "jitter": "k6 per-VU 0-100 ms dispatch offset, xorshift32 seeded by jitter_seed (load/c28.js)",
    "rate_perturbation": None,
    "time_compression": 1,
    "trace_minute_to_utc": "minute m of day d starts at T0 + (d-1)*86400 s + (m-1)*60 s; T0 = 00:00:00Z of the "
                           "run-lock start date (the trace's original timezone is unspecified)",
    "repetitions": 1,
    "max_infrastructure_restarts": 2,
    "request_timeout_s": 2,
    "retries": 0,
}

WORDING = "requests derived from per-minute invocation counts (Azure Functions 2019, CC BY 4.0, modified)"
ATTRIBUTION = ("Azure Functions Trace 2019, Microsoft (github.com/Azure/AzurePublicDataset), CC BY 4.0. "
               "Shahrad et al., \"Serverless in the Wild: Characterizing and Optimizing the Serverless Workload at a "
               "Large Cloud Provider\", USENIX ATC 2020.")
MODIFICATIONS = ("HTTP-trigger functions summed per application; applications selected by the pre-declared seeded rule; "
                 "per-application normalization (history-day 99.5th-percentile minute = 9 c), floor 0.1 c, clip 10 c; "
                 "converted to an arrival-rate plan.")


class TraceError(Exception):
    """A structural problem with the input: the run stops (never a silent repair)."""


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(obj) -> str:
    return hashlib.sha256(canonical(obj)).hexdigest()


def rules_digest(rules: Optional[Dict] = None) -> str:
    rules = RULES if rules is None else rules
    return digest(rules)


# ---------------------------------------------------------------------------------------------------------------------
# Input integrity
# ---------------------------------------------------------------------------------------------------------------------
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_sums(path: str) -> Dict[str, str]:
    """Parse a `sha256sum` file: '<64 hex>  <name>' per line; names must be plain file names."""
    out: Dict[str, str] = {}
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(None, 1)
            if len(parts) != 2 or len(parts[0]) != 64 or any(c not in "0123456789abcdef" for c in parts[0]):
                raise TraceError(f"{path}:{n}: not a sha256sum line")
            name = parts[1].lstrip("*")
            if os.path.basename(name) != name or name in (".", ".."):
                raise TraceError(f"{path}:{n}: file names must be plain names, got {name!r}")
            if name in out:
                raise TraceError(f"{path}:{n}: duplicate entry {name!r}")
            out[name] = parts[0]
    return out


def day_files(data_dir: str, sums: Dict[str, str], rules: Optional[Dict] = None, verify: bool = True) -> List[Tuple[int, str]]:
    """The invocation files in day order, each verified against its pinned hash before any parsing."""
    rules = RULES if rules is None else rules
    files = []
    for day in range(1, rules["days"] + 1):
        name = rules["invocation_file_pattern"].format(day=day)
        if name not in sums:
            raise TraceError(f"{name} is not pinned in the sums file")
        path = os.path.join(data_dir, name)
        if not os.path.isfile(path) or os.path.islink(path):
            raise TraceError(f"{path} is missing or not a regular file")
        if verify:
            got = sha256_file(path)
            if got != sums[name]:
                raise TraceError(f"{name}: sha256 {got} does not match the pinned {sums[name]}")
        files.append((day, path))
    return files


# ---------------------------------------------------------------------------------------------------------------------
# Parsing and per-application aggregation
# ---------------------------------------------------------------------------------------------------------------------
AppKey = Tuple[str, str]


def app_id(key: AppKey) -> str:
    return f"{key[0]}:{key[1]}"


def parse_app_id(s: str) -> AppKey:
    owner, sep, app = s.partition(":")
    if not sep or not owner or not app:
        raise TraceError(f"bad application id {s!r} (want OWNER:APP)")
    return owner, app


def read_day(path: str, rules: Optional[Dict] = None) -> Tuple[Dict[AppKey, List[int]], Dict[AppKey, str]]:
    """One day file -> (per-application per-minute HTTP sums, per-application invalidity reason).

    Only rows with the declared trigger contribute. Applications marked invalid keep their reason; their sums are
    dropped."""
    rules = RULES if rules is None else rules
    m = rules["minutes_per_day"]
    want_header = list(rules["header_prefix"]) + [str(i) for i in range(1, m + 1)]
    sums: Dict[AppKey, List[int]] = {}
    invalid: Dict[AppKey, str] = {}
    seen_functions = set()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise TraceError(f"{path}: empty file") from None
        if header != want_header:
            raise TraceError(f"{path}: unexpected header (first fields {header[:6]!r})")
        width = len(want_header)
        for row in reader:
            if len(row) != width:
                raise TraceError(f"{path}:{reader.line_num}: {len(row)} fields, want {width}")
            if row[3] != rules["trigger"]:
                continue
            owner, app, func = row[0], row[1], row[2]
            if not owner or not app or not func:
                raise TraceError(f"{path}:{reader.line_num}: empty identifier")
            key = (owner, app)
            if key in invalid:
                continue
            fkey = (owner, app, func)
            if fkey in seen_functions:
                invalid[key] = "duplicate-function-row"
                sums.pop(key, None)
                continue
            seen_functions.add(fkey)
            try:
                counts = [int(v) for v in row[4:]]
            except ValueError:
                invalid[key] = "non-integer-count"
                sums.pop(key, None)
                continue
            if any(c < 0 for c in counts):
                invalid[key] = "negative-count"
                sums.pop(key, None)
                continue
            acc = sums.get(key)
            if acc is None:
                sums[key] = counts
            else:
                for i, c in enumerate(counts):
                    acc[i] += c
    return sums, invalid


# ---------------------------------------------------------------------------------------------------------------------
# Eligibility (history-only volume statistics) and the seeded draw
# ---------------------------------------------------------------------------------------------------------------------
def eligibility(files: Sequence[Tuple[int, str]], rules: Optional[Dict] = None) -> Dict:
    """Stream the day files once and apply the declared filters. Returns stage counts and the eligible records."""
    rules = RULES if rules is None else rules
    history = set(rules["history_days"])
    m = rules["minutes_per_day"]
    present_days: Dict[AppKey, int] = {}
    positive_days: Dict[AppKey, int] = {}
    hist_total: Dict[AppKey, int] = {}
    hist_active: Dict[AppKey, int] = {}
    invalid: Dict[AppKey, str] = {}
    for day, path in files:
        sums, bad = read_day(path, rules)
        for key, reason in bad.items():
            invalid.setdefault(key, f"{reason}@d{day:02d}")
        for key, counts in sums.items():
            present_days[key] = present_days.get(key, 0) + 1
            total = sum(counts)
            if total > 0:
                positive_days[key] = positive_days.get(key, 0) + 1
            if day in history:
                hist_total[key] = hist_total.get(key, 0) + total
                hist_active[key] = hist_active.get(key, 0) + sum(1 for c in counts if c > 0)
    n_days = len(files)
    hist_minutes = len(history) * m
    lo, hi = rules["history_mean_per_minute_band"]
    universe = set(present_days) | set(invalid)
    stages = {"http_apps_seen": len(universe)}
    s1 = [k for k in universe if k not in invalid]
    stages["valid_rows"] = len(s1)
    s2 = [k for k in s1 if present_days.get(k, 0) == n_days and positive_days.get(k, 0) == n_days] \
        if rules["require_present_all_days"] else s1
    stages["complete_all_days"] = len(s2)
    s3 = [k for k in s2 if lo <= hist_total.get(k, 0) / hist_minutes <= hi]
    stages["history_volume_band"] = len(s3)
    s4 = [k for k in s3 if hist_active.get(k, 0) / hist_minutes >= rules["history_min_active_fraction"]]
    stages["history_active_fraction"] = len(s4)
    eligible = sorted(
        ({"app": app_id(k), "owner": k[0],
          "history_mean_per_minute": hist_total[k] / hist_minutes,
          "history_active_fraction": hist_active[k] / hist_minutes} for k in s4),
        key=lambda r: r["app"])
    reasons: Dict[str, int] = {}
    for r in invalid.values():
        base = r.split("@", 1)[0]
        reasons[base] = reasons.get(base, 0) + 1
    return {"stages": stages, "invalid_reasons": dict(sorted(reasons.items())), "eligible": eligible}


def strata(eligible: Sequence[Dict], rules: Optional[Dict] = None) -> List[List[Dict]]:
    """Contiguous volume strata by rank of the history mean (ties by application id); sizes differ by at most one."""
    rules = RULES if rules is None else rules
    ordered = sorted(eligible, key=lambda r: (r["history_mean_per_minute"], r["app"]))
    k = rules["strata"]
    n = len(ordered)
    bounds = [round(i * n / k) for i in range(k + 1)]
    return [ordered[bounds[i]:bounds[i + 1]] for i in range(k)]


def draw(eligible: Sequence[Dict], rules: Optional[Dict] = None) -> Dict:
    """Seeded draw: per stratum (low to high), a seeded permutation of the stratum sorted by application id; take the
    first `per_stratum` whose owner is not already chosen (when distinct_owner). No backfill across strata."""
    rules = RULES if rules is None else rules
    chosen, owners, shortfall = [], set(), {}
    for i, group in enumerate(strata(eligible, rules)):
        pool = sorted(group, key=lambda r: r["app"])
        rng = random.Random(f"c28-trace-draw-{rules['seed']}-stratum-{i}")
        rng.shuffle(pool)
        taken = 0
        for r in pool:
            if taken == rules["per_stratum"]:
                break
            if rules["distinct_owner"] and r["owner"] in owners:
                continue
            chosen.append({"stratum": i, "draw_rank": taken, **r})
            owners.add(r["owner"])
            taken += 1
        if taken < rules["per_stratum"]:
            shortfall[str(i)] = rules["per_stratum"] - taken
    live_order = [c["app"] for c in chosen]
    random.Random(f"c28-trace-live-{rules['seed']}").shuffle(live_order)
    return {"chosen": chosen, "shortfall": shortfall, "live_order": live_order,
            "strata_sizes": [len(g) for g in strata(eligible, rules)]}


# ---------------------------------------------------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------------------------------------------------
def nearest_rank(values: Sequence[int], q: float) -> int:
    if not values:
        raise TraceError("empty series")
    s = sorted(values)
    return s[max(0, math.ceil(q * len(s)) - 1)]


def collect_series(files: Sequence[Tuple[int, str]], keys: Iterable[AppKey], rules: Optional[Dict] = None) -> Dict[AppKey, List[int]]:
    """Concatenated per-minute HTTP counts (days in order) for the given applications; absent function-days are zero."""
    rules = RULES if rules is None else rules
    want = set(keys)
    m = rules["minutes_per_day"]
    out = {k: [] for k in want}
    for day, path in files:
        sums, bad = read_day(path, rules)
        for k in want:
            if k in bad:
                raise TraceError(f"{app_id(k)} is invalid on day {day}: {bad[k]}")
            out[k].extend(sums.get(k, [0] * m))
    return out


def peak(series: Sequence[int], rules: Optional[Dict] = None) -> int:
    rules = RULES if rules is None else rules
    m = rules["minutes_per_day"]
    hist = [x for d in rules["history_days"] for x in series[(d - 1) * m:d * m]]
    p = nearest_rank(hist, rules["peak_quantile"])
    if p <= 0:
        raise TraceError("history peak is zero")
    return p


def to_u(series: Sequence[int], p: int, rules: Optional[Dict] = None) -> List[float]:
    """u_k = clip(u_peak * x_k / P, u_min, u_max): demand in units of the calibrated capacity c."""
    rules = RULES if rules is None else rules
    a, lo, hi = rules["u_peak"], rules["u_min"], rules["u_max"]
    return [min(max(a * x / p, lo), hi) for x in series]


def arrivals_per_minute(u: Sequence[float], capacity_rps: float) -> List[int]:
    """Scheduled arrivals per real minute: round(60 * c * u_k). c is the calibrated capacity, substituted later."""
    if not (capacity_rps > 0) or math.isinf(capacity_rps):
        raise TraceError("capacity must be a positive finite number")
    return [max(0, round(60.0 * capacity_rps * x)) for x in u]


def transform_stats(series: Sequence[int], p: int, rules: Optional[Dict] = None) -> Dict:
    rules = RULES if rules is None else rules
    a, lo, hi = rules["u_peak"], rules["u_min"], rules["u_max"]
    m = rules["minutes_per_day"]
    out = {}
    for part in ("history_days", "evaluation_days"):
        xs = [x for d in rules[part] for x in series[(d - 1) * m:d * m]]
        raw = [a * x / p for x in xs]
        out[part.split("_")[0]] = {"minutes": len(xs), "floored": sum(1 for r in raw if r < lo),
                                   "clipped": sum(1 for r in raw if r > hi), "zero_count_minutes": xs.count(0)}
    return out


def jitter_seed(app: str, repetition: int) -> int:
    return int(hashlib.sha256(f"c28-trace-jitter-{app}-{repetition}".encode()).hexdigest()[:8], 16)


def k6_plan(u: Sequence[float], capacity_rps: float, app: str, repetition: int = 0, rules: Optional[Dict] = None) -> Dict:
    """The k6 ramping-arrival-rate plan for one application, at 1x wall clock (each trace minute is one real minute)."""
    rules = RULES if rules is None else rules
    if repetition < 0 or repetition >= rules["repetitions"] + rules["max_infrastructure_restarts"]:
        raise TraceError("repetition out of the declared range")
    per_min = arrivals_per_minute(u, capacity_rps)
    ramp_ms, step_ms = rules["ramp_ms"], 60_000
    stages, scheduled, prev = [], [], per_min[0]
    for r in per_min:
        stages.append({"target": r, "duration": f"{ramp_ms}ms"})
        stages.append({"target": r, "duration": f"{step_ms - ramp_ms}ms"})
        scheduled.append(((prev + r) / 2 * ramp_ms + r * (step_ms - ramp_ms)) / 60_000)
        prev = r
    pre = int(math.ceil(max(per_min) / 60 * rules["request_timeout_s"] * 1.5)) + 10
    plan = {"schema": "c28-trace-k6-plan/v1", "label": "trace-demo", "wording": WORDING,
            "attribution": ATTRIBUTION, "modifications": MODIFICATIONS, "app": app, "repetition": repetition,
            "capacity_rps": capacity_rps, "rules_digest": rules_digest(rules), "accelerate": 1,
            "time_unit": "1m", "start_rate": per_min[0], "stages": stages, "scheduled_per_minute": scheduled,
            "pre_allocated_vus": pre, "max_vus": 2 * pre, "timeout": f"{rules['request_timeout_s']}s",
            "jitter_seed": jitter_seed(app, repetition), "retries": rules["retries"]}
    plan["digest"] = digest(plan)
    return plan


# ---------------------------------------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------------------------------------
def _write(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, sort_keys=True, allow_nan=False)
        f.write("\n")
    os.replace(tmp, path)


def cmd_select(a) -> int:
    sums = read_sums(a.sums)
    files = day_files(a.data, sums)
    elig = eligibility(files)
    d = draw(elig["eligible"])
    out = {"schema": "c28-trace-selection/v1", "rules": RULES, "rules_digest": rules_digest(),
           "sums_sha256": sha256_file(a.sums),
           "inputs": {os.path.basename(p): sums[os.path.basename(p)] for _, p in files},
           "stages": elig["stages"], "invalid_reasons": elig["invalid_reasons"],
           "eligible_count": len(elig["eligible"]),
           "eligible_digest": digest([r["app"] for r in elig["eligible"]]), **d,
           "note": "selection statistics only; no forecaster run, no forecast accuracy"}
    _write(a.out, out)
    print(json.dumps({"eligible_count": out["eligible_count"], "stages": out["stages"],
                      "chosen": [c["app"] for c in d["chosen"]], "shortfall": d["shortfall"]}, indent=1))
    return 0


def cmd_prepare(a) -> int:
    with open(a.selection, encoding="utf-8") as f:
        sel = json.load(f)
    if sel.get("rules_digest") != rules_digest():
        raise TraceError("the selection was made under different rules")
    sums = read_sums(a.sums)
    files = day_files(a.data, sums)
    keys = [parse_app_id(c["app"]) for c in sel["chosen"]]
    series = collect_series(files, keys)
    apps = {}
    for k in keys:
        x = series[k]
        p = peak(x)
        apps[app_id(k)] = {"history_peak_p995": p, "counts": x, "u": [round(v, 9) for v in to_u(x, p)],
                           "transform_stats": transform_stats(x, p)}
    out = {"schema": "c28-trace-prepared/v1", "rules_digest": rules_digest(), "selection_sha256": sha256_file(a.selection),
           "wording": WORDING, "attribution": ATTRIBUTION, "modifications": MODIFICATIONS, "apps": apps}
    _write(a.out, out)
    print(json.dumps({k: {"history_peak_p995": v["history_peak_p995"], "transform_stats": v["transform_stats"]}
                      for k, v in apps.items()}, indent=1))
    return 0


def cmd_plan(a) -> int:
    with open(a.prepared, encoding="utf-8") as f:
        prep = json.load(f)
    if prep.get("rules_digest") != rules_digest():
        raise TraceError("prepared series were made under different rules")
    if a.app not in prep["apps"]:
        raise TraceError(f"{a.app} is not a prepared application")
    _write(a.out, k6_plan(prep["apps"][a.app]["u"], a.capacity, a.app, a.repetition))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("select")
    s.add_argument("--data", required=True)
    s.add_argument("--sums", required=True)
    s.add_argument("--out", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--data", required=True)
    p.add_argument("--sums", required=True)
    p.add_argument("--selection", required=True)
    p.add_argument("--out", required=True)
    k = sub.add_parser("plan")
    k.add_argument("--prepared", required=True)
    k.add_argument("--app", required=True)
    k.add_argument("--capacity", required=True, type=float)
    k.add_argument("--repetition", type=int, default=0)
    k.add_argument("--out", required=True)
    sub.add_parser("rules")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "select":
            return cmd_select(a)
        if a.cmd == "prepare":
            return cmd_prepare(a)
        if a.cmd == "plan":
            return cmd_plan(a)
        print(json.dumps({"rules": RULES, "rules_digest": rules_digest()}, indent=1, sort_keys=True))
        return 0
    except TraceError as e:
        print(f"azure_functions_2019: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
