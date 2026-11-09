#!/usr/bin/env python3
"""Pre-declared selection and transformation of the Azure Functions 2019 trace (revision 2) for the C2.8 public-trace
demonstration. PROPOSAL: the contract that freezes these rules is a DRAFT pending Codex review (contract v2, with the
post-observation amendments A1-A7 of 2026-11-09).

This file is the executable form of `trace-contract-azure-functions-2019` (context workspace,
agent-coordination/09-real-world-research/c28-staging/trace-demo/). It is NOT part of C2.8 scoring and does not replace
the C2.8 manifest's ordinary warm-up. It never runs a forecaster and never computes forecast accuracy.

Wording rule: what this produces are "requests derived from per-minute invocation counts". The dataset records
invocation counts per function and minute AFTER execution; it holds no arrival timestamps, so nothing here is "real
traffic" or "real arrival timestamps".

Data source: Azure Functions Trace 2019 (Microsoft), licensed CC BY 4.0. Cite: Shahrad et al., "Serverless in the Wild:
Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider", USENIX ATC 2020. Modifications made
here: HTTP-trigger functions summed per application (absent function-days imputed as zero), selection of a few
applications, per-application normalization and a ceiling, conversion to an arrival-rate plan.

Standard library only, one file, so it runs under `python -I` (no script-directory imports) against untrusted data
passed by path. Every stage verifies the reviewed hash of its input and records the hashes it was built from
(source -> selection -> prepared -> plan); no output file is ever overwritten. Subcommands:

  select   --data DIR --sums SHA256SUMS --expect-sums SHA --out selection.json
  prepare  --data DIR --sums SHA256SUMS --selection selection.json --expect-selection SHA --out prepared.json
  plan     --prepared prepared.json --expect-prepared SHA --app OWNER:APP --capacity C --out plan.json [--repetition N]
  rules    print the frozen rules and their digests
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import re
import sys
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------------------------------------------------
# The frozen rules.
#
# SELECTION_RULES is the v1 rules object of the 2026-11-09 draft, VERBATIM (digest a43854aa...), so the selection it
# produced (selection.json, sha256 ead9658d...) stays reproducible byte for byte and is never redrawn. Only its
# SELECTION_KEYS govern anything now. Its transformation and timing keys (u_min, ramp_ms, intra_minute, ...) are
# SUPERSEDED by TRANSFORM_RULES (contract v2, amendments A1-A3); they stay in the object only to keep its digest.
# ---------------------------------------------------------------------------------------------------------------------
SELECTION_RULES: Dict = {
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
    "missing_function_day": "zero",
    "require_present_all_days": True,
    "history_mean_per_minute_band": [10.0, 100000.0],
    "history_min_active_fraction": 0.9,
    "strata": 3,
    "per_stratum": 2,
    "distinct_owner": True,
    "seed": 20261109,
    "live_apps": 1,
    "peak_quantile": 0.995,
    "u_peak": 9.0,
    "u_min": 0.1,
    "u_max": 10.0,
    "max_replicas": 12,
    "rounding": "python-round-half-even",
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
RULES = SELECTION_RULES   # the name the v1 code used; selection reads only SELECTION_KEYS
SELECTION_KEYS = ("dataset", "invocation_file_pattern", "days", "minutes_per_day", "header_prefix", "trigger", "app_key",
                  "history_days", "evaluation_days", "missing_function_day", "require_present_all_days",
                  "history_mean_per_minute_band", "history_min_active_fraction", "strata", "per_stratum",
                  "distinct_owner", "seed", "live_apps")

TRANSFORM_RULES: Dict = {
    "schema": "c28-trace-transform/v2",
    "selection_rules_digest": "a43854aa26a18f2deb64707d354076ff6574ed5bcf71cab403b8b180df0ca169",
    # Missing data (A5): a function absent from a day's file is IMPUTED as zero invocations that day; the affected
    # function-days are counted and reported per application.
    "absent_function_day": "imputed-zero",
    # Normalization: u = offered_RPS / c. History-only nearest-rank p99.5 -> 9 c; ceiling 10 c. NO floor (A1): zero and
    # low-rate minutes are preserved; zero traffic is told apart from missing telemetry by scrape timestamps and
    # initialized counters, never by fabricated requests.
    "peak_quantile": 0.995,
    "u_peak": 9.0,
    "u_floor": None,
    "u_max": 10.0,
    "max_replicas": 12,
    "rounding": "python-round-half-even",
    # Timing (A3): each minute is a constant rate held for the whole minute, entered by a zero-duration k6 stage, so the
    # scheduled count of every minute equals its intended count exactly (no ramp smoothing).
    "intra_minute": "k6 ramping-arrival-rate, piecewise constant: a 0 ms stage jumps to the minute's rate, a 60000 ms "
                    "stage holds it",
    "jitter": "k6 per-VU 0-100 ms dispatch offset, xorshift32 seeded by jitter_seed (load/c28.js)",
    "rate_perturbation": None,
    "time_compression": 1,
    "trace_minute_to_utc": "minute m of day d starts at T0 + (d-1)*86400 s + (m-1)*60 s; T0 = 00:00:00Z of the "
                           "run-lock start date (the trace's original timezone is unspecified)",
    "repetitions": 1,
    "max_infrastructure_restarts": 2,
    "request_timeout_s": 2,
    "retries": 0,
    # Execution interface (A2): exactly the plan load/c28.js accepts.
    "plan_schema": "c28-k6-plan/v1",
    "plan_kind": "trace-demo",
    "target_pattern": "c28-trace-{hashapp12}",
    "seed_rule": "seed = 10 * selection seed + repetition",
}

PREPARED_SCHEMA = "c28-trace-prepared/v2"
SELECTION_SCHEMA = "c28-trace-selection/v1"
CONTRACT_ID = "trace-contract-azure-functions-2019"

WORDING = "requests derived from per-minute invocation counts (Azure Functions 2019, CC BY 4.0, modified)"
ATTRIBUTION = ("Azure Functions Trace 2019, Microsoft (github.com/Azure/AzurePublicDataset), CC BY 4.0. "
               "Shahrad et al., \"Serverless in the Wild: Characterizing and Optimizing the Serverless Workload at a "
               "Large Cloud Provider\", USENIX ATC 2020.")
MODIFICATIONS = ("HTTP-trigger functions summed per application, absent function-days imputed as zero; applications "
                 "selected by the pre-declared seeded rule; per-application normalization (history-day "
                 "99.5th-percentile minute = 9 c) with a 10 c ceiling, no floor; converted to a piecewise-constant "
                 "per-minute arrival-rate plan.")

_HEX64 = re.compile(r"^[0-9a-f]{64}$")


class TraceError(Exception):
    """A structural, provenance or consistency problem: the run stops (never a silent repair)."""


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(obj) -> str:
    return hashlib.sha256(canonical(obj)).hexdigest()


def rules_digest(rules: Optional[Dict] = None) -> str:
    return digest(SELECTION_RULES if rules is None else rules)


def transform_digest(rules: Optional[Dict] = None) -> str:
    return digest(TRANSFORM_RULES if rules is None else rules)


# ---------------------------------------------------------------------------------------------------------------------
# Input integrity
# ---------------------------------------------------------------------------------------------------------------------
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def expect_hash(path: str, want: str, what: str) -> str:
    if not _HEX64.match(want or ""):
        raise TraceError(f"--expect for {what} must be a 64-hex sha256")
    got = sha256_file(path)
    if got != want:
        raise TraceError(f"{what} {path}: sha256 {got} is not the reviewed {want}")
    return got


def read_sums(path: str) -> Dict[str, str]:
    """Parse a `sha256sum` file: '<64 hex>  <name>' per line; names must be plain file names."""
    out: Dict[str, str] = {}
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(None, 1)
            if len(parts) != 2 or not _HEX64.match(parts[0]):
                raise TraceError(f"{path}:{n}: not a sha256sum line")
            name = parts[1].lstrip("*")
            if os.path.basename(name) != name or name in (".", ".."):
                raise TraceError(f"{path}:{n}: file names must be plain names, got {name!r}")
            if name in out:
                raise TraceError(f"{path}:{n}: duplicate entry {name!r}")
            out[name] = parts[0]
    return out


def day_files(data_dir: str, sums: Dict[str, str], rules: Optional[Dict] = None,
              verify: bool = True) -> List[Tuple[int, str]]:
    """The invocation files in day order, each verified against its pinned hash before any parsing."""
    rules = SELECTION_RULES if rules is None else rules
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
    owner, sep, app = str(s).partition(":")
    if not sep or not owner or not app:
        raise TraceError(f"bad application id {s!r} (want OWNER:APP)")
    return owner, app


def read_day(path: str, rules: Optional[Dict] = None, functions: Optional[Dict[AppKey, set]] = None
             ) -> Tuple[Dict[AppKey, List[int]], Dict[AppKey, str]]:
    """One day file -> (per-application per-minute HTTP sums, per-application invalidity reason).

    Only rows with the declared trigger contribute. Applications marked invalid keep their reason; their sums are
    dropped. When `functions` is given, the HTTP function ids seen per application are added to it."""
    rules = SELECTION_RULES if rules is None else rules
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
            if functions is not None:
                functions.setdefault(key, set()).add(func)
            acc = sums.get(key)
            if acc is None:
                sums[key] = counts
            else:
                for i, c in enumerate(counts):
                    acc[i] += c
    return sums, invalid


def http_row_census(path: str, rules: Optional[Dict] = None) -> Tuple[int, int]:
    """(HTTP rows, HTTP rows whose daily total is zero) in one day file: evidence about what an absent row means."""
    rules = SELECTION_RULES if rules is None else rules
    rows = zero = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) > 4 and row[3] == rules["trigger"]:
                rows += 1
                try:
                    if sum(int(v) for v in row[4:]) == 0:
                        zero += 1
                except ValueError:
                    pass
    return rows, zero


# ---------------------------------------------------------------------------------------------------------------------
# Eligibility (history-only volume statistics) and the seeded draw. Unchanged from v1.
# ---------------------------------------------------------------------------------------------------------------------
def eligibility(files: Sequence[Tuple[int, str]], rules: Optional[Dict] = None) -> Dict:
    """Stream the day files once and apply the declared filters. Returns stage counts and the eligible records."""
    rules = SELECTION_RULES if rules is None else rules
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
    rules = SELECTION_RULES if rules is None else rules
    ordered = sorted(eligible, key=lambda r: (r["history_mean_per_minute"], r["app"]))
    k = rules["strata"]
    n = len(ordered)
    bounds = [round(i * n / k) for i in range(k + 1)]
    return [ordered[bounds[i]:bounds[i + 1]] for i in range(k)]


def draw(eligible: Sequence[Dict], rules: Optional[Dict] = None) -> Dict:
    """Seeded draw: per stratum (low to high), a seeded permutation of the stratum sorted by application id; take the
    first `per_stratum` whose owner is not already chosen (when distinct_owner). No backfill across strata."""
    rules = SELECTION_RULES if rules is None else rules
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
# Transformation (contract v2)
# ---------------------------------------------------------------------------------------------------------------------
def nearest_rank(values: Sequence[int], q: float) -> int:
    if not values:
        raise TraceError("empty series")
    s = sorted(values)
    return s[max(0, math.ceil(q * len(s)) - 1)]


def collect_series(files: Sequence[Tuple[int, str]], keys: Iterable[AppKey], rules: Optional[Dict] = None
                   ) -> Tuple[Dict[AppKey, List[int]], Dict[AppKey, Dict]]:
    """Concatenated per-minute HTTP counts (days in order) for the given applications, and their missingness: absent
    function-days (a function seen on some day but absent from another day's file) are imputed as zero and counted."""
    rules = SELECTION_RULES if rules is None else rules
    want = set(keys)
    m = rules["minutes_per_day"]
    history = set(rules["history_days"])
    out = {k: [] for k in want}
    per_day: Dict[AppKey, List[set]] = {k: [] for k in want}
    for day, path in files:
        funcs: Dict[AppKey, set] = {}
        sums, bad = read_day(path, rules, functions=funcs)
        for k in want:
            if k in bad:
                raise TraceError(f"{app_id(k)} is invalid on day {day}: {bad[k]}")
            out[k].extend(sums.get(k, [0] * m))
            per_day[k].append(funcs.get(k, set()))
    miss = {}
    for k in want:
        all_f = set().union(*per_day[k]) if per_day[k] else set()
        absent = {"history": 0, "evaluation": 0}
        for i, fs in enumerate(per_day[k]):
            part = "history" if files[i][0] in history else "evaluation"
            absent[part] += len(all_f - fs)
        miss[k] = {"http_functions": len(all_f), "function_days": len(all_f) * len(per_day[k]),
                   "absent_function_days_imputed_zero": absent["history"] + absent["evaluation"],
                   "absent_by_partition": absent}
    return out, miss


def peak(series: Sequence[int], rules: Optional[Dict] = None) -> int:
    rules = SELECTION_RULES if rules is None else rules
    m = rules["minutes_per_day"]
    hist = [x for d in rules["history_days"] for x in series[(d - 1) * m:d * m]]
    p = nearest_rank(hist, TRANSFORM_RULES["peak_quantile"])
    if p <= 0:
        raise TraceError("history peak is zero")
    return p


def to_u(series: Sequence[int], p: int, tr: Optional[Dict] = None) -> List[float]:
    """u_k = min(u_peak * x_k / P, u_max): demand in units of the calibrated capacity c. No floor (A1)."""
    tr = TRANSFORM_RULES if tr is None else tr
    if tr["u_floor"] is not None:
        raise TraceError("contract v2 has no floor")
    a, hi = tr["u_peak"], tr["u_max"]
    return [min(a * x / p, hi) for x in series]


def intended_per_minute(u: Sequence[float], capacity_rps: float) -> List[int]:
    """Intended arrivals per real minute: round(60 * c * u_k). c is the calibrated capacity, substituted later."""
    if not isinstance(capacity_rps, (int, float)) or not math.isfinite(capacity_rps) or not capacity_rps > 0:
        raise TraceError("capacity must be a positive finite number")
    return [round(60.0 * capacity_rps * x) for x in u]


arrivals_per_minute = intended_per_minute   # v1 name


def transform_stats(series: Sequence[int], p: int, rules: Optional[Dict] = None, tr: Optional[Dict] = None) -> Dict:
    rules = SELECTION_RULES if rules is None else rules
    tr = TRANSFORM_RULES if tr is None else tr
    a, hi = tr["u_peak"], tr["u_max"]
    m = rules["minutes_per_day"]
    out = {}
    for part in ("history_days", "evaluation_days"):
        xs = [x for d in rules[part] for x in series[(d - 1) * m:d * m]]
        raw = [a * x / p for x in xs]
        out[part.split("_")[0]] = {"minutes": len(xs), "clipped": sum(1 for r in raw if r > hi),
                                   "zero_count_minutes": xs.count(0),
                                   "below_0_1c_minutes": sum(1 for r in raw if r < 0.1)}
    return out


def target_name(app: str) -> str:
    _, hashapp = parse_app_id(app)
    if not re.match(r"^[0-9a-f]{12}", hashapp):
        raise TraceError("HashApp must start with 12 lowercase hex digits")
    return TRANSFORM_RULES["target_pattern"].format(hashapp12=hashapp[:12])


def jitter_seed(app: str, repetition: int) -> int:
    return int(hashlib.sha256(f"c28-trace-jitter-{app}-{repetition}".encode()).hexdigest()[:8], 16)


def stage_minutes(stages: Sequence[Dict], start_rate: float, minutes: int) -> List[float]:
    """Integrate k6 ramping-arrival-rate stages (timeUnit 1m; each stage ramps linearly from the previous target to its
    own over its duration; a 0 ms stage is a jump) into scheduled arrivals per real minute."""
    out = [0.0] * minutes
    t, rate = 0.0, float(start_rate)
    for s in stages:
        d = s["duration"]
        if not (isinstance(d, str) and d.endswith("ms") and d[:-2].isdigit()):
            raise TraceError(f"stage duration {d!r} is not an integer millisecond string")
        dur = int(d[:-2]) / 1000.0
        tgt = float(s["target"])
        end = t + dur
        k = int(t // 60)
        while dur > 0 and t < end - 1e-12:
            seg_end = min(end, (k + 1) * 60.0)
            r0 = rate + (tgt - rate) * (t - (end - dur)) / dur
            r1 = rate + (tgt - rate) * (seg_end - (end - dur)) / dur
            if k < minutes:
                out[k] += (r0 + r1) / 2 * (seg_end - t) / 60.0
            t, k = seg_end, k + 1
        t, rate = end, tgt
    return out


def k6_plan(u: Sequence[float], capacity_rps: float, app: str, repetition: int, binding: Dict,
            rules: Optional[Dict] = None, tr: Optional[Dict] = None) -> Dict:
    """The c28.js plan for one application at 1x wall clock: one constant rate per trace minute (A2, A3)."""
    rules = SELECTION_RULES if rules is None else rules
    tr = TRANSFORM_RULES if tr is None else tr
    if not isinstance(repetition, int) or repetition < 0 \
            or repetition >= tr["repetitions"] + tr["max_infrastructure_restarts"]:
        raise TraceError("repetition out of the declared range")
    intended = intended_per_minute(u, capacity_rps)
    stages: List[Dict] = []
    prev = intended[0]
    for r in intended:
        if r != prev:
            stages.append({"target": r, "duration": "0ms"})
        stages.append({"target": r, "duration": "60000ms"})
        prev = r
    scheduled = stage_minutes(stages, intended[0], len(intended))
    if any(abs(s - i) > 1e-6 * max(1, i) for s, i in zip(scheduled, intended)):
        raise TraceError("the stages do not conserve the intended per-minute counts")
    pre = int(math.ceil(max(intended) / 60 * tr["request_timeout_s"] * 1.5)) + 10
    plan = {"schema": tr["plan_schema"], "kind": tr["plan_kind"], "label": tr["plan_kind"],
            "seed": 10 * rules["seed"] + repetition, "repetition": repetition, "target": target_name(app), "app": app,
            "capacity_rps": capacity_rps, "accelerate": 1, "time_unit": "1m", "start_rate": intended[0],
            "stages": stages, "intended_per_minute": intended, "scheduled_per_minute": [round(s, 6) for s in scheduled],
            "pre_allocated_vus": pre, "max_vus": 2 * pre, "timeout": f"{tr['request_timeout_s']}s",
            "jitter_seed": jitter_seed(app, repetition), "retries": tr["retries"],
            "wording": WORDING, "attribution": ATTRIBUTION, "modifications": MODIFICATIONS,
            "binding": dict(binding, contract=CONTRACT_ID, transform_rules_digest=transform_digest(tr),
                            selection_rules_digest=rules_digest(rules))}
    plan["digest"] = digest(plan)
    return plan


# ---------------------------------------------------------------------------------------------------------------------
# Validation of stage artifacts
# ---------------------------------------------------------------------------------------------------------------------
def _load(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        try:
            obj = json.load(f, parse_constant=lambda c: (_ for _ in ()).throw(TraceError(f"non-finite {c}")))
        except json.JSONDecodeError as e:
            raise TraceError(f"{path}: not JSON ({e})") from None
    if not isinstance(obj, dict):
        raise TraceError(f"{path}: not a JSON object")
    return obj


def validate_selection(sel: Dict, rules: Optional[Dict] = None) -> None:
    rules = SELECTION_RULES if rules is None else rules
    if sel.get("schema") != SELECTION_SCHEMA:
        raise TraceError(f"selection schema {sel.get('schema')!r}, want {SELECTION_SCHEMA}")
    if sel.get("rules_digest") != rules_digest(rules) or digest(sel.get("rules")) != rules_digest(rules):
        raise TraceError("the selection was made under different selection rules")
    chosen = sel.get("chosen")
    if not isinstance(chosen, list) or not chosen:
        raise TraceError("selection has no chosen applications")
    apps = [c.get("app") for c in chosen if isinstance(c, dict)]
    if len(apps) != len(chosen) or len(set(apps)) != len(apps):
        raise TraceError("selection chosen list is malformed or has duplicates")
    for a in apps:
        parse_app_id(a)
    if sorted(sel.get("live_order", [])) != sorted(apps):
        raise TraceError("selection live_order is not a permutation of the chosen applications")
    inputs = sel.get("inputs")
    want = {rules["invocation_file_pattern"].format(day=d) for d in range(1, rules["days"] + 1)}
    if not isinstance(inputs, dict) or set(inputs) != want or not all(_HEX64.match(str(v)) for v in inputs.values()):
        raise TraceError("selection inputs do not pin exactly the invocation files")
    if not _HEX64.match(str(sel.get("sums_sha256", ""))):
        raise TraceError("selection does not record its sums file hash")


def validate_prepared(prep: Dict, rules: Optional[Dict] = None, tr: Optional[Dict] = None) -> None:
    """Schema, rules, lengths, finite bounds and transformation consistency (u and P recomputed from the counts)."""
    rules = SELECTION_RULES if rules is None else rules
    tr = TRANSFORM_RULES if tr is None else tr
    if prep.get("schema") != PREPARED_SCHEMA:
        raise TraceError(f"prepared schema {prep.get('schema')!r}, want {PREPARED_SCHEMA}")
    if prep.get("transform_rules_digest") != transform_digest(tr) or digest(prep.get("transform_rules")) \
            != transform_digest(tr):
        raise TraceError("prepared series were made under different transformation rules")
    if prep.get("selection_rules_digest") != rules_digest(rules):
        raise TraceError("prepared series were made from a selection under different rules")
    for k in ("selection_sha256", "sums_sha256"):
        if not _HEX64.match(str(prep.get(k, ""))):
            raise TraceError(f"prepared file lacks {k}")
    apps = prep.get("apps")
    order = prep.get("app_order")
    if not isinstance(apps, dict) or not isinstance(order, list) or sorted(order) != sorted(apps) or not apps:
        raise TraceError("prepared apps do not match its app_order")
    n = rules["days"] * rules["minutes_per_day"]
    for a, rec in apps.items():
        parse_app_id(a)
        x, u, p = rec.get("counts"), rec.get("u"), rec.get("history_peak_p995")
        if not (isinstance(x, list) and isinstance(u, list) and len(x) == n and len(u) == n):
            raise TraceError(f"{a}: counts and u must each have {n} minutes")
        if not all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in x):
            raise TraceError(f"{a}: counts must be non-negative integers")
        if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
                   and 0 <= v <= tr["u_max"] for v in u):
            raise TraceError(f"{a}: u must be finite and within [0, {tr['u_max']}]")
        if p != peak(x, rules):
            raise TraceError(f"{a}: history_peak_p995 is not the history-day p99.5 of its counts")
        if any(abs(v - w) > 1e-9 for v, w in zip(u, to_u(x, p, tr))):
            raise TraceError(f"{a}: u is not the declared transformation of its counts")


def _write_new(path: str, obj) -> None:
    """Write JSON to a path that must not exist yet (never overwrite a reviewed artifact)."""
    if os.path.lexists(path):
        raise TraceError(f"{path} exists; outputs are never overwritten")
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "x", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, sort_keys=True, allow_nan=False)
        f.write("\n")
    try:
        os.link(tmp, path)
    except FileExistsError:
        raise TraceError(f"{path} exists; outputs are never overwritten") from None
    finally:
        os.unlink(tmp)


# ---------------------------------------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------------------------------------
def cmd_select(a) -> int:
    expect_hash(a.sums, a.expect_sums, "sums file")
    sums = read_sums(a.sums)
    files = day_files(a.data, sums)
    elig = eligibility(files)
    d = draw(elig["eligible"])
    out = {"schema": SELECTION_SCHEMA, "rules": SELECTION_RULES, "rules_digest": rules_digest(),
           "sums_sha256": sha256_file(a.sums),
           "inputs": {os.path.basename(p): sums[os.path.basename(p)] for _, p in files},
           "stages": elig["stages"], "invalid_reasons": elig["invalid_reasons"],
           "eligible_count": len(elig["eligible"]),
           "eligible_digest": digest([r["app"] for r in elig["eligible"]]), **d,
           "note": "selection statistics only; no forecaster run, no forecast accuracy"}
    _write_new(a.out, out)
    print(json.dumps({"eligible_count": out["eligible_count"], "stages": out["stages"],
                      "chosen": [c["app"] for c in d["chosen"]], "shortfall": d["shortfall"]}, indent=1))
    return 0


def cmd_prepare(a) -> int:
    sel_sha = expect_hash(a.selection, a.expect_selection, "selection")
    sel = _load(a.selection)
    validate_selection(sel)
    if sha256_file(a.sums) != sel["sums_sha256"]:
        raise TraceError("the sums file is not the one the selection was made from")
    sums = read_sums(a.sums)
    for name, h in sel["inputs"].items():
        if sums.get(name) != h:
            raise TraceError(f"{name}: pinned hash differs from the selection's input hash")
    files = day_files(a.data, sums)
    keys = [parse_app_id(c["app"]) for c in sel["chosen"]]
    series, miss = collect_series(files, keys)
    census = {"http_rows": 0, "http_rows_zero_daily_total": 0}
    for _, path in files:
        r, z = http_row_census(path)
        census["http_rows"] += r
        census["http_rows_zero_daily_total"] += z
    apps = {}
    for c in sel["chosen"]:
        k = parse_app_id(c["app"])
        x = series[k]
        p = peak(x)
        apps[c["app"]] = {"stratum": c["stratum"], "draw_rank": c["draw_rank"],
                          "live_rank": sel["live_order"].index(c["app"]), "target": target_name(c["app"]),
                          "history_peak_p995": p, "counts": x, "u": [round(v, 12) for v in to_u(x, p)],
                          "transform_stats": transform_stats(x, p), "missingness": miss[k]}
    out = {"schema": PREPARED_SCHEMA, "transform_rules": TRANSFORM_RULES, "transform_rules_digest": transform_digest(),
           "selection_rules_digest": rules_digest(), "selection_sha256": sel_sha, "sums_sha256": sel["sums_sha256"],
           "inputs": sel["inputs"], "app_order": [c["app"] for c in sel["chosen"]], "live_order": sel["live_order"],
           "dataset_http_row_census": census, "wording": WORDING, "attribution": ATTRIBUTION,
           "modifications": MODIFICATIONS, "apps": apps}
    validate_prepared(out)
    _write_new(a.out, out)
    print(json.dumps({"census": census, "apps": {k: {"history_peak_p995": v["history_peak_p995"],
                                                     "transform_stats": v["transform_stats"],
                                                     "missingness": v["missingness"]} for k, v in apps.items()}},
                     indent=1))
    return 0


def cmd_plan(a) -> int:
    prep_sha = expect_hash(a.prepared, a.expect_prepared, "prepared")
    prep = _load(a.prepared)
    validate_prepared(prep)
    if a.app not in prep["apps"]:
        raise TraceError(f"{a.app} is not a prepared application")
    rec = prep["apps"][a.app]
    u = to_u(rec["counts"], rec["history_peak_p995"])
    binding = {"prepared_sha256": prep_sha, "selection_sha256": prep["selection_sha256"],
               "sums_sha256": prep["sums_sha256"]}
    _write_new(a.out, k6_plan(u, a.capacity, a.app, a.repetition, binding))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("select")
    s.add_argument("--data", required=True)
    s.add_argument("--sums", required=True)
    s.add_argument("--expect-sums", required=True)
    s.add_argument("--out", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--data", required=True)
    p.add_argument("--sums", required=True)
    p.add_argument("--selection", required=True)
    p.add_argument("--expect-selection", required=True)
    p.add_argument("--out", required=True)
    k = sub.add_parser("plan")
    k.add_argument("--prepared", required=True)
    k.add_argument("--expect-prepared", required=True)
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
        print(json.dumps({"selection_rules": SELECTION_RULES, "selection_rules_digest": rules_digest(),
                          "selection_keys": list(SELECTION_KEYS), "transform_rules": TRANSFORM_RULES,
                          "transform_rules_digest": transform_digest()}, indent=1, sort_keys=True))
        return 0
    except (TraceError, OSError) as e:
        print(f"azure_functions_2019: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
