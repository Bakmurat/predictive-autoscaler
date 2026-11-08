#!/usr/bin/env python3
"""S5a capacity calibration of the C2.8 fixture on the reserved W (EVAL-C28-MANIFEST section 3; STAGING S5a).

  calibrate.py --record R [plan]                              read-only: write the plan file, print its digest
  calibrate.py --record R apply --plan F --plan-digest D      run exactly that reviewed calibration (create, select
                                                              work_iterations, search, ceiling probe, clean up, close)
  calibrate.py --record R status                              read-only: the calibration's journal (trials, decisions)
  calibrate.py --record R verify                              read-only: is the open calibration within its contract?
                                                              (a closed one: its evidence bundle is recomputed offline)
  calibrate.py --record R request-stop --reason TEXT          ask the RUNNING executor to stop (no lock needed)
  calibrate.py --record R stop-plan                           read-only: the scoped stop plan (from the journal)
  calibrate.py --record R stop --plan F --plan-digest D       stop: arrivals first, prove generator pods gone, clean up
  calibrate.py --record R recover-plan | recover --plan F --plan-digest D   reconcile open intents (shared lock)
  calibrate.py reduce --bundle DIR                            offline: recompute the capacity from an evidence bundle

What it measures. The manifest's c0: the largest per-replica offered rate at which the fixture, at FIXED replicas and
with no autoscaler, meets p95 <= 200 ms, p99 <= 500 ms, a request failure fraction <= 0.1 %, no generator drops beyond
the contract's bound and the queue rule below, for 1, 2 and 4 replicas, each plateau measured three times (15 minutes
after 5 minutes of settling), the crossing refined to <= 5 %, the minimum across repetitions and replica counts; NOT
READY when four replicas reach < 90 % of linear or a generator/network limit intervenes. Operating capacity
c = 0.875 c0 (render.capacity_rps). It qualifies nothing else: not the HPA target, not the startup profiles, not the
twelve-replica layout; burst behaviour is fixture characterization only (no claim that autoscaling covers single-pod
bursts); no prospective history or confirmation evidence starts during calibration.

The calibration contract (record section "calibrate": {"contract": "<file>"}, schema c28-calibration-contract/v1) is
frozen independently of the unfinished experiment params (only the reservation stage of the params is read). It binds
the cluster and node UIDs, the namespace, the source commit and load-script hash, the S1 images.json and its fixture and
generator digests, the candidate fixture configuration (resources, max_inflight, max_queue, queue timeout, GOMAXPROCS,
response size, startup delay), the work_iterations selection, the ingress and generator resources, the observer (query
API, the evaluation's 15 s scrape cadence, staleness, scrape kind, the exact queries), the criteria (the manifest's
numbers, which may not be changed, the queue rule, the limit bounds, the 1.2 ceiling factor), the search (bounds,
factor, resolution, maximum trials, total deadline, seed, grace and timeouts), the admission budget and the rollout
guard. Every consumed value is validated: finite, positive where a count or rate, no placeholders, no unknown keys.
The calibration ID is c28cal-<first 10 hex of the contract's canonical sha256>: changing the candidate (or anything
else) starts a NEW calibration, and an ID is used once (contract.attempt makes a fresh attempt of the same
configuration). Results are never combined across IDs.

Objects (all created by apply, journalled, bound by UID; none carries the run label, so no confirmation target, session,
quota or run-lock ever counts them, and their names c28cal-* never collide with the c28-<arm>-<profile> targets):
  * two calibration fixture variants <id>-w0 and <id>-w1 (work_iterations = work_selection.values[0] / [1]), each a
    Deployment created at 0 replicas (every count is set through /scale with the read resourceVersion and the UID) and
    a Service: the arms' container shape (render.render_arm) with the candidate's resources and queue settings and an
    explicit GOMAXPROCS env, sidecar-free (label AND annotation), pinned to W by the run taint toleration and required
    role + hostname affinity, no PriorityClass. The selection never edits a pod template;
  * the pinned calibration ingress <id>-ing and its Service on W (render.render_common's ingress shape, one route per
    variant), which counts offered demand before the fixture can reject it: every request goes through it;
  * the k6 script ConfigMap <id>-k6 (load/c28-calibrate.js) and, when the contract's scrape kind is PodMonitor or
    VMPodScrape, one scrape object for the fixture and ingress pods at the 15 s cadence;
  * one k6 Job per trial, <id>-gen-tNNN, on S, backoffLimit 0 and a finite activeDeadlineSeconds (settle + measure +
    job_slack_s), so a dead executor can never leave indefinite traffic.

Stage 1, work_iterations selection (select_work, before any c0 search). Each variant alone at ONE replica (the other at
0): idle_s of idle (its CPU is the idle baseline), then one trial at work_selection.rate_rps (declared below
saturation) with the selection's settle_s/measure_s, through the ingress. CPU cost per request = (mean fixture CPU in
the measurement window - mean idle CPU) / successful requests per second, in ms. A value qualifies when its trial
PASSes every criterion and its cost lies in cpu_ms_per_request [min, max]; among qualifying values the cost closest to
target wins, a tie goes to fewer iterations ("closest-to-target-then-fewer-iterations"). Neither qualifying: the
calibration STOPS (NOT READY, "work-selection") for a revised candidate, never an informal search. The selection
(`work-selected` receipt, both measurements retained) is frozen into the record before the c0 search, which runs on the
selected variant only.

Stage 2, the c0 search (SEARCH_ALGORITHM, pure function next_step, replayed by the reducer). Rates are total arrivals
per minute (integers). Per replica count n in [1, 2, 4]: Rmin = ceil(rate_min_rps_per_replica * n * 60), Rmax =
floor(rate_max_rps_per_replica * n * 60), start R0 = initial_rps_per_replica * n * 60 (clamped). A plateau at R runs
its repetitions in order; it passes when all three PASS and fails at its first measured FAIL (the minimum across
repetitions). Bracket: after a pass the next rate is min(Rmax, ceil(lo * bracket_factor)); after a fail max(Rmin,
floor(hi / bracket_factor)). Once a pass lo and a fail hi bracket the crossing, bisect at floor((lo + hi) / 2) until
(hi - lo) <= resolution * lo (or no integer lies between). Q_n = lo. A pass at Rmax is an UNRESOLVED LOWER BOUND, never
a calibrated capacity; a fail at Rmin means no qualifying rate. Any INCOMPLETE, LIMIT or STOPPED trial halts the whole
calibration (never bisected across); so do the trial budget (max_trials, every trial counted) and the total deadline.
Every choice is a `decision` receipt with the evidence it followed and the rate it chose; the plan authorizes this
algorithm and its limits (every search rate lies in [Rmin, Rmax]), never an arbitrary load.

Stage 3, the ceiling probe. With every count calibrated, one trial at 4 replicas at ceil(1.2 * Q_4) (the manifest's
120 %), which must lie within the authorized Rmax: its arrivals must be UNCENSORED (complete evidence, no generator
drop beyond the bound, every dispatched arrival received by the ingress, generator and ingress throttling within their
bounds); its SLO outcome is irrelevant. Censored or unknown delivery, or a ceiling above Rmax, is NOT READY: generator
drops are never attributed to the fixture.

Measurement (per trial, evaluate_trial, recomputed by the reducer from the raw files):
  * k6 (its termination message): per scenario iterations (dispatched), dropped_iterations, http_reqs (terminal
    outcomes), http_req_failed and http_req_duration p95/p99 (ALL requests, timeouts included);
  * the ingress counters before the trial and after it plus completion_grace_s, both read as quiet (three or more
    fresh, equal samples; a target's counter that does not exist yet reads 0 only while the ingress scrape is proven
    up and fresh): received arrivals and responses;
  * the observer's range queries over the measurement window [k6 start + settle, + measure] at the 15 s cadence: the
    fixture backlog query, fixture CPU and throttling, ingress and generator CPU and throttling.
  Reconciliation: scheduled = rate * duration / 60 must equal dropped + dispatched within the tolerance; dispatched -
  terminal = unknown outcomes (counted as failures for the bound; when only they push the fraction over the limit the
  result is INCOMPLETE, not a measured failure); received > dispatched + tolerance = UNACCOUNTED ARRIVALS
  (INCOMPLETE); dispatched > received + tolerance = arrivals lost before the ingress (LIMIT); ingress responses must
  equal received. Criteria are judged separately for every repetition.
  Queue rule (exact): the measurement window is cut into 60 s bins; b_m = the mean of the backlog query's samples in
  minute m (every bin needs a sample, gaps <= 2 steps). With E = edge_bins, END = mean(b over the last E bins),
  INCREASE = END - mean(b over the first E bins) and SLOPE = the least-squares slope of b over the bins (per minute),
  the repetition FAILS when END / n > max_end_backlog_per_replica, or when INCREASE > max_increase AND SLOPE >
  max_slope_per_min (sustained growth). Bounded growth below either of those numbers passes.
  Limits: dropped fraction > max_dropped_fraction, mean generator or ingress CPU throttling above its bound, or
  arrivals lost before the ingress, is a generator/network limit (LIMIT): NOT READY, never a lower capacity.
  Image identity: every fixture/ingress pod and the generator pod must report the contract's image digests.
  Verdict precedence: INCOMPLETE > LIMIT > FAIL > PASS; STOPPED for a trial the executor stopped.

Admission and monitoring. plan, apply and every trial require: reserve.verify with this calibration's
journal-verified own objects (reserve.calibration_owned: exact Deployment/Job UIDs from this journal through a
UID-checked ReplicaSet chain; no name prefix) and require_reservation_pass; the calibration footprint on W (fixture at 4
replicas + admission.overlap_pods, the ingress, declared observer pods) and on S (one generator, declared observer pods)
plus every other live pod's requests (system pods, terminating ones included) within admission.limit_fraction (0.75)
of the node's live allocatable, CPU and memory separately; no rollout in progress for any Deployment, StatefulSet or
DaemonSet with a pod on W or S, nor for the contract's extra_owners (the observer stack, Flux-managed platform
pieces): capacity is never measured during the remaining Flux rollouts; no writer of a fixture variant or the ingress
(HPA, KEDA, both PA groups); the owned objects at their UIDs; the active variant at exactly its count and generation;
no session open and no run generator active; no injected sidecar in an owned pod; fresh observer data (the ingress
scrape up within max_staleness_s); no stop request; the total deadline. During a trial the same checks run every
poll_s; a violation stops the trial (arrivals first: the Job is deleted and its pods proven gone) and the calibration.

Interruption. Everything runs under the shared evaluation lock (execlib.mutating) with intents before and outcomes
after each create, delete and /scale step; an unknown outcome keeps the lock until `recover`. Trial boundaries
(trial-begin, trial-result, trial-stopped, trial-interrupted) and every decision are receipts. Cleanup deletes the
generator Jobs first and proves their pods gone before touching the fixture; an ambiguous write or an unproven cleanup
keeps the lock. An interrupted measurement stays recorded as interrupted and is never resumed: after a crash, recover
and stop close the calibration as STOPPED; a new attempt is a new contract.attempt and a new ID.

Evidence (evidence/calibrate/<id>/): contract.json, objects/, trials/<tNNN>/ (job, pods at begin and end, the generator
pod with its termination message, the raw observer responses, the derived result), then at close manifest.json (every
file's sha256), certificate.json (verdict READY | NOT READY with every reason: measured criteria, incomplete evidence,
limit, stopped, work selection, unresolved bound, linearity, ceiling, budget; the selected work_iterations, both
selection measurements, curves with CPU and throttling, every unsuccessful trial), journal.jsonl (the hash-chained
receipts) and bundle-index.json. `reduce --bundle` verifies every hash and the receipt chain, recomputes every trial
from its raw files, replays selection, search and ceiling, and recomputes c0, c and the linearity ratios; any
difference is refused. The certificate's targets are calibration targets only: never confirmation targets, never
trust or qualification evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Callable, Dict, List, Optional, Tuple

import common_records as C
import execlib as X
import reserve

HELPER = "calibrate"
STOP = "calibrate-stop"
CONTRACT_SCHEMA = "c28-calibration-contract/v1"
CERT_SCHEMA = "c28-calibration-certificate/v1"
RESULT_SCHEMA = "c28-calibration-trial/v1"
K6_SCHEMA = "c28-calibration-k6/v1"
SEARCH_ALGORITHM = "c28-calibrate-bracket-bisect/v1"
CAL_LABEL = reserve.CALIBRATION_LABEL
CAL_RUN_LABEL = "c28.devkuban.com/calibration-run"
TRIAL_LABEL = "c28.devkuban.com/calibration-trial"
VARIANT_LABEL = "c28.devkuban.com/calibration-variant"
PART_OF = "c28-calibration"
LOAD_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "load", "c28-calibrate.js")
REQUEST_TIMEOUT_S = 2            # frozen.REQUEST_TIMEOUT_S (checked at import of the harness)

# The manifest's section 3 numbers: a contract must carry exactly these.
FROZEN = {"p95_ms": 200, "p99_ms": 500, "max_failure_fraction": 0.001, "linearity_min": 0.90,
          "replica_counts": [1, 2, 4], "repetitions": 3, "settle_s": 300, "measure_s": 900, "limit_fraction": 0.75,
          "bin_s": 60, "ceiling_factor": 1.2, "step_s": 15}
TIE_BREAK = "closest-to-target-then-fewer-iterations"
MAX_RESOLUTION = 0.05
QUERY_TOKENS = {"ingress_up": "__PODS__", "ingress_requests": "__TARGET__", "ingress_responses": "__TARGET__",
                "fixture_backlog": "__TARGET__",
                "fixture_cpu_cores": "__PODS__", "fixture_throttled_fraction": "__PODS__",
                "ingress_cpu_cores": "__PODS__", "ingress_throttled_fraction": "__PODS__",
                "generator_cpu_cores": "__PODS__", "generator_throttled_fraction": "__PODS__"}
WINDOW_QUERIES = ("fixture_backlog", "fixture_cpu_cores", "fixture_throttled_fraction", "ingress_cpu_cores",
                  "ingress_throttled_fraction", "generator_cpu_cores", "generator_throttled_fraction")
ROLLOUT_KINDS = ["DaemonSet", "Deployment", "StatefulSet"]
VERDICTS = ("PASS", "FAIL", "LIMIT", "INCOMPLETE", "STOPPED")
SCOPE_LIMITS = ["capacity calibration of this fixture candidate only (sidecar-free, fixed replicas, no autoscaler)",
                "does not qualify the CPU HPA target, the startup profiles or the twelve-replica layout",
                "calibration targets are not confirmation targets and carry no trust or qualification evidence; no "
                "prospective history or confirmation evidence starts during calibration",
                "burst behaviour, if observed, is fixture characterization only: no claim that autoscaling covers "
                "single-pod bursts"]

_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:-]*@sha256:([0-9a-f]{64})$")
_URL = re.compile(r"^https?://[^@/\s]+(/\S*)?$")
_VERSION = re.compile(r"^\d+\.\d+\.\d+$")


class Halt(Exception):
    """The search stops here (recorded, not an error): category is incomplete | limit | stopped | budget."""

    def __init__(self, category: str, reason: str):
        super().__init__(reason)
        self.category, self.reason = category, reason


def _utc(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def _ts(epoch: float) -> str:
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


# --- the calibration contract ---------------------------------------------------------------------------------------

CONTRACT_KEYS = {"schema", "run_id", "attempt", "cluster_uid", "namespace", "nodes", "source", "images", "candidate",
                 "work_selection", "ingress", "generator", "observer", "criteria", "search", "admission",
                 "rollout_guard"}


def _reject_constant(name):
    raise ValueError(f"non-finite JSON number {name}")


def read_json_strict(path: str):
    with open(path) as fh:
        return json.load(fh, parse_constant=_reject_constant)


def _resources_problems(d, where: str, R) -> List[str]:
    out = []
    if not isinstance(d, dict) or set(d) != {"cpu_request", "cpu_limit", "memory_request", "memory_limit"}:
        return [f"{where} must be exactly {{cpu_request, cpu_limit, memory_request, memory_limit}}"]
    for req, lim, fn in (("cpu_request", "cpu_limit", R.cpu_millicores),
                         ("memory_request", "memory_limit", R.memory_bytes)):
        try:
            a, b = fn(d[req]), fn(d[lim])
        except (R.ParamsError, TypeError) as e:
            out.append(f"{where}: {e}")
            continue
        if a <= 0 or a > b:
            out.append(f"{where}.{req} must be positive and at most {where}.{lim}")
    return out


def contract_problems(con, record: X.Record, params: Dict) -> List[str]:
    """Every reason the contract cannot be used (empty when it can)."""
    R = X.import_harness().render
    if not isinstance(con, dict):
        return ["the calibration contract is not a JSON object"]
    out: List[str] = []
    unknown = sorted(set(con) - CONTRACT_KEYS)
    missing = sorted(CONTRACT_KEYS - set(con))
    if unknown:
        out.append(f"unknown contract fields: {unknown}")
    if missing:
        return out + [f"missing contract fields: {missing}"]
    for path_, key, v in C._walk(con):
        if key is not None and C.SECRET_KEY.search(str(key)):
            out.append(f"{path_}: credential material is never recorded")
        if isinstance(v, str) and (C._UNRESOLVED.search(v) or C._LEAK.search(v) or v.strip().startswith("<")):
            out.append(f"{path_}: unresolved placeholder or credential-bearing value")
    if con["schema"] != CONTRACT_SCHEMA:
        out.append(f"schema must be {CONTRACT_SCHEMA}")
    if con["run_id"] != record.run_id:
        out.append("run_id differs from the record")
    if con["cluster_uid"] != record.cluster_uid:
        out.append("cluster_uid differs from the record (another cluster)")
    if not (_int(con["attempt"]) and 1 <= con["attempt"] <= 999):
        out.append("attempt must be an integer in 1..999")
    if not (isinstance(con["namespace"], str) and X._DNS.match(con["namespace"]) and len(con["namespace"]) <= 40):
        out.append("namespace must be a short DNS label")
    rs = record.doc.get("reserve") or {}
    nodes = con["nodes"]
    for role in ("workload", "support"):
        n = (nodes or {}).get(role) if isinstance(nodes, dict) else None
        if not (isinstance(n, dict) and set(n) == {"name", "uid"} and X._DNS.match(str(n.get("name", "")))
                and X._UUID.match(str(n.get("uid", "")))):
            out.append(f"nodes.{role} must be exactly {{name, uid}}")
            continue
        if ((rs.get("nodes") or {}).get(role) or {}) != n:
            out.append(f"nodes.{role} differs from the record's reserve.nodes.{role}")
        if params["nodes"][role] != n["name"]:
            out.append(f"nodes.{role} differs from params.nodes.{role}")
    src = con["source"]
    if not (isinstance(src, dict) and set(src) == {"commit", "load_script_sha256"}):
        out.append("source must be exactly {commit, load_script_sha256}")
    else:
        if src["commit"] != record.commit:
            out.append("source.commit differs from the record's source commit")
        if not os.path.isfile(LOAD_SCRIPT) or src["load_script_sha256"] != C.sha256_file(LOAD_SCRIPT):
            out.append("source.load_script_sha256 is not load/c28-calibrate.js as checked out")
    img = con["images"]
    if not (isinstance(img, dict) and set(img) == {"images_json", "sha256", "fixture", "generator"}):
        out.append("images must be exactly {images_json, sha256, fixture, generator}")
    else:
        for k in ("fixture", "generator"):
            if not _DIGEST_IMAGE.match(str(img[k])):
                out.append(f"images.{k} must be pinned by digest")
        p = record.resolve(str(img["images_json"]))
        if not os.path.isfile(p) or C.sha256_file(p) != img["sha256"]:
            out.append("images.images_json is missing or its sha256 differs (the S1 record)")
        else:
            try:
                doc = read_json_strict(p)
            except ValueError as e:
                doc = {}
                out.append(f"images.images_json is not JSON ({e})")
            refs = {k: ((doc.get("images") or {}).get(k) or {}).get("ref") for k in ("fixture", "generator")}
            if doc.get("schema") != "c28-images/v1" or doc.get("verified") is not True or \
                    doc.get("run_id") != record.run_id:
                out.append("images.images_json is not this run's verified c28-images/v1 record")
            if refs != {"fixture": img["fixture"], "generator": img["generator"]}:
                out.append("images.fixture/generator differ from the images.json record (take them from the record)")
    cand = con["candidate"]
    ckeys = {"cpu_request", "cpu_limit", "memory_request", "memory_limit", "response_bytes",
             "max_inflight", "max_queue", "queue_timeout_seconds", "gomaxprocs", "startup_delay_seconds"}
    if not (isinstance(cand, dict) and set(cand) == ckeys):
        out.append(f"candidate must be exactly {sorted(ckeys)}")
    else:
        out += _resources_problems({k: cand[k] for k in ("cpu_request", "cpu_limit", "memory_request", "memory_limit")},
                                   "candidate", R)
        for k, lo in (("max_inflight", 1), ("gomaxprocs", 1), ("response_bytes", 0), ("max_queue", 0)):
            if not (_int(cand[k]) and lo <= cand[k] <= 10_000_000):
                out.append(f"candidate.{k} must be an integer >= {lo}")
        if not (_num(cand["queue_timeout_seconds"]) and 0 < cand["queue_timeout_seconds"] <= 60):
            out.append("candidate.queue_timeout_seconds must be a finite number in (0, 60]")
        if not (_num(cand["startup_delay_seconds"]) and 0 <= cand["startup_delay_seconds"] <= 600):
            out.append("candidate.startup_delay_seconds must be a finite number in [0, 600]")
    out += _work_selection_problems(con["work_selection"], con["criteria"])
    out += _resources_problems(con["ingress"], "ingress", R)
    gen = con["generator"]
    if not (isinstance(gen, dict) and "k6_version" in gen):
        out.append("generator must carry k6_version and its resources")
    else:
        if not _VERSION.match(str(gen["k6_version"])):
            out.append("generator.k6_version must be x.y.z")
        out += _resources_problems({k: v for k, v in gen.items() if k != "k6_version"}, "generator", R)
    out += _observer_problems(con["observer"], R)
    out += _criteria_problems(con["criteria"])
    out += _search_problems(con["search"])
    adm = con["admission"]
    if not (isinstance(adm, dict) and set(adm) == {"limit_fraction", "overlap_pods"}):
        out.append("admission must be exactly {limit_fraction, overlap_pods}")
    else:
        if adm["limit_fraction"] != FROZEN["limit_fraction"]:
            out.append(f"admission.limit_fraction must be {FROZEN['limit_fraction']} (the manifest's >= 25 % headroom)")
        if not (_int(adm["overlap_pods"]) and 1 <= adm["overlap_pods"] <= 12):
            out.append("admission.overlap_pods must be an integer in 1..12 (startup/termination overlap)")
    rg = con["rollout_guard"]
    if not (isinstance(rg, dict) and set(rg) == {"kinds", "extra_owners"} and rg["kinds"] == ROLLOUT_KINDS):
        out.append(f"rollout_guard must be exactly {{kinds: {ROLLOUT_KINDS}, extra_owners: [...]}}")
    elif not isinstance(rg["extra_owners"], list) or not all(
            isinstance(o, dict) and set(o) == {"kind", "namespace", "name"} and o["kind"] in ROLLOUT_KINDS
            and X._DNS.match(str(o["namespace"])) and X._DNS.match(str(o["name"])) for o in rg["extra_owners"]):
        out.append("rollout_guard.extra_owners must be [{kind, namespace, name}] of rollout kinds")
    return out


def _work_selection_problems(ws, cr) -> List[str]:
    """The predeclared work_iterations selection on W before the c0 search (see select_work)."""
    keys = {"values", "rate_rps", "idle_s", "settle_s", "measure_s", "cpu_ms_per_request", "tie_break"}
    if not (isinstance(ws, dict) and set(ws) == keys):
        return [f"work_selection must be exactly {sorted(keys)}"]
    out = []
    v = ws["values"]
    if not (isinstance(v, list) and len(v) == 2 and all(_int(x) and 1 <= x <= 10_000_000 for x in v)
            and v[0] != v[1]):
        out.append("work_selection.values must be two distinct predeclared positive integers (work_iterations)")
    if not (_num(ws["rate_rps"]) and 0 < ws["rate_rps"] <= 100_000):
        out.append("work_selection.rate_rps must be a finite positive rate (one replica, below saturation)")
    if not (_int(ws["idle_s"]) and 60 <= ws["idle_s"] <= 900):
        out.append("work_selection.idle_s must be an integer in 60..900 (the idle-CPU window)")
    if not (_int(ws["settle_s"]) and 60 <= ws["settle_s"] <= 900):
        out.append("work_selection.settle_s must be an integer in 60..900")
    edge = ((cr or {}).get("queue_growth") or {}).get("edge_bins") if isinstance(cr, dict) else None
    if not (_int(ws["measure_s"]) and ws["measure_s"] % 60 == 0 and 300 <= ws["measure_s"] <= 3600 and
            (not _int(edge) or 2 * edge * 60 <= ws["measure_s"])):
        out.append("work_selection.measure_s must be a multiple of 60 in 300..3600, at least 2 * edge_bins minutes")
    c = ws["cpu_ms_per_request"]
    if not (isinstance(c, dict) and set(c) == {"min", "target", "max"} and all(_num(c[k]) and c[k] > 0 for k in c)
            and c["min"] <= c["target"] <= c["max"]):
        out.append("work_selection.cpu_ms_per_request must be {min <= target <= max}, finite and positive")
    if ws["tie_break"] != TIE_BREAK:
        out.append(f"work_selection.tie_break must be {TIE_BREAK!r}")
    return out


def _observer_problems(ob, R) -> List[str]:
    keys = {"query_api", "step_s", "max_staleness_s", "scrape", "queries", "pods"}
    if not (isinstance(ob, dict) and set(ob) == keys):
        return [f"observer must be exactly {sorted(keys)}"]
    out = []
    if not _URL.match(str(ob["query_api"])):
        out.append("observer.query_api must be an http(s) URL without credentials")
    if ob["step_s"] != FROZEN["step_s"] or isinstance(ob["step_s"], bool):
        out.append(f"observer.step_s must be the evaluation's scrape cadence {FROZEN['step_s']} s (never a faster "
                   "sampler)")
    elif not (_int(ob["max_staleness_s"]) and 2 * ob["step_s"] <= ob["max_staleness_s"] <= 600):
        out.append("observer.max_staleness_s must be an integer in [2 * step_s, 600]")
    if ob["scrape"] not in ("annotations", "PodMonitor", "VMPodScrape"):
        out.append("observer.scrape must be annotations, PodMonitor or VMPodScrape")
    q = ob["queries"]
    if not (isinstance(q, dict) and set(q) == set(QUERY_TOKENS)):
        out.append(f"observer.queries must be exactly {sorted(QUERY_TOKENS)}")
    else:
        for name, token in QUERY_TOKENS.items():
            if not (isinstance(q[name], str) and q[name].strip() and token in q[name]):
                out.append(f"observer.queries.{name} must be a query using {token}")
    pods = ob["pods"]
    if not isinstance(pods, list):
        out.append("observer.pods must be a list of declared observer pods (or [])")
    else:
        for i, p in enumerate(pods):
            if not (isinstance(p, dict) and set(p) == {"name", "role", "cpu_request", "memory_request"}
                    and p["role"] in ("workload", "support")):
                out.append(f"observer.pods[{i}] must be {{name, role, cpu_request, memory_request}}")
                continue
            try:
                if R.cpu_millicores(p["cpu_request"]) <= 0 or R.memory_bytes(p["memory_request"]) <= 0:
                    out.append(f"observer.pods[{i}] requests must be positive")
            except R.ParamsError as e:
                out.append(f"observer.pods[{i}]: {e}")
    return out


def _criteria_problems(cr) -> List[str]:
    keys = {"p95_ms", "p99_ms", "max_failure_fraction", "max_dropped_fraction", "linearity_min", "queue_growth",
            "max_generator_throttled_fraction", "max_ingress_throttled_fraction", "reconcile_tolerance",
            "ceiling_factor"}
    if not (isinstance(cr, dict) and set(cr) == keys):
        return [f"criteria must be exactly {sorted(keys)}"]
    out = []
    for k in ("p95_ms", "p99_ms", "max_failure_fraction", "linearity_min", "ceiling_factor"):
        if cr[k] != FROZEN[k] or isinstance(cr[k], bool):
            out.append(f"criteria.{k} must be the manifest's {FROZEN[k]}")
    if not (_num(cr["max_dropped_fraction"]) and 0 < cr["max_dropped_fraction"] <= FROZEN["max_failure_fraction"]):
        out.append("criteria.max_dropped_fraction must be a finite number in (0, 0.001]")
    for k in ("max_generator_throttled_fraction", "max_ingress_throttled_fraction"):
        if not (_num(cr[k]) and 0 < cr[k] < 1):
            out.append(f"criteria.{k} must be a finite number in (0, 1)")
    qg = cr["queue_growth"]
    qkeys = {"bin_s", "edge_bins", "max_increase", "max_slope_per_min", "max_end_backlog_per_replica"}
    if not (isinstance(qg, dict) and set(qg) == qkeys):
        out.append(f"criteria.queue_growth must be exactly {sorted(qkeys)}")
    else:
        if qg["bin_s"] != FROZEN["bin_s"] or isinstance(qg["bin_s"], bool):
            out.append("criteria.queue_growth.bin_s must be 60")
        if not (_int(qg["edge_bins"]) and 1 <= qg["edge_bins"] <= 7):
            out.append("criteria.queue_growth.edge_bins must be an integer in 1..7")
        for k in ("max_increase", "max_slope_per_min", "max_end_backlog_per_replica"):
            if not (_num(qg[k]) and qg[k] > 0):
                out.append(f"criteria.queue_growth.{k} must be a finite positive number")
    rt = cr["reconcile_tolerance"]
    if not (isinstance(rt, dict) and set(rt) == {"absolute", "relative"} and _int(rt["absolute"])
            and 0 <= rt["absolute"] <= 100 and _num(rt["relative"]) and 0 <= rt["relative"] <= 0.01):
        out.append("criteria.reconcile_tolerance must be {absolute: 0..100, relative: 0..0.01}")
    return out


def _search_problems(se) -> List[str]:
    keys = {"replica_counts", "repetitions", "settle_s", "measure_s", "rate_min_rps_per_replica",
            "rate_max_rps_per_replica", "initial_rps_per_replica", "bracket_factor", "resolution", "max_trials",
            "deadline_utc", "seed", "completion_grace_s", "job_slack_s", "converge_timeout_s", "poll_s"}
    if not (isinstance(se, dict) and set(se) == keys):
        return [f"search must be exactly {sorted(keys)}"]
    out = []
    for k in ("replica_counts", "repetitions", "settle_s", "measure_s"):
        if se[k] != FROZEN[k] or isinstance(se[k], bool):
            out.append(f"search.{k} must be the manifest's {FROZEN[k]}")
    rates = [se[k] for k in ("rate_min_rps_per_replica", "initial_rps_per_replica", "rate_max_rps_per_replica")]
    if not all(_num(r) and r > 0 for r in rates):
        out.append("search rates must be finite positive numbers")
    elif not rates[0] <= rates[1] <= rates[2] or rates[2] > 100_000:
        out.append("search rates must satisfy rate_min <= initial <= rate_max (<= 100000 per replica)")
    if not (_num(se["bracket_factor"]) and 1 < se["bracket_factor"] <= 4):
        out.append("search.bracket_factor must be a finite number in (1, 4]")
    if not (_num(se["resolution"]) and 0 < se["resolution"] <= MAX_RESOLUTION):
        out.append(f"search.resolution must be a finite number in (0, {MAX_RESOLUTION}]")
    if not (_int(se["max_trials"]) and 3 <= se["max_trials"] <= 500):
        out.append("search.max_trials must be an integer in 3..500")
    if not _TS.match(str(se["deadline_utc"])):
        out.append("search.deadline_utc must be an RFC 3339 UTC time")
    if not (_int(se["seed"]) and 1 <= se["seed"] <= 2**31 - 1):
        out.append("search.seed must be a positive 31-bit integer")
    for k, lo, hi in (("completion_grace_s", REQUEST_TIMEOUT_S + 30, 600), ("job_slack_s", 30, 900),
                      ("converge_timeout_s", 30, 3600), ("poll_s", 5, 300)):
        if not (_int(se[k]) and lo <= se[k] <= hi):
            out.append(f"search.{k} must be an integer in {lo}..{hi}")
    return out


def load_contract(record: X.Record, params: Dict) -> Tuple[Dict, str, str]:
    """(contract, its canonical sha256, the calibration id). Refuses any problem."""
    s = record.section("calibrate")
    if set(s) != {"contract"}:
        raise X.StagingError("the record's calibrate section must be exactly {contract: <file>}")
    try:
        con = read_json_strict(record.resolve(str(s["contract"])))
    except (OSError, ValueError) as e:
        raise X.StagingError(f"the calibration contract cannot be read: {e}") from e
    probs = contract_problems(con, record, params)
    if probs:
        raise X.StagingError("calibration contract: " + "; ".join(probs))
    sha = C.digest(con)
    return con, sha, calibration_id(sha)


def calibration_id(contract_sha256: str) -> str:
    return f"c28cal-{contract_sha256[:10]}"


# --- names, labels and rendering --------------------------------------------------------------------------------

def names(cal: str) -> Dict:
    """Object names. The fixture exists in two variants, one per predeclared work_iterations value (w0, w1); each is
    its own Deployment + Service (and ingress route /t/<variant>/), so the selection never edits a pod template."""
    return {"variants": [f"{cal}-w0", f"{cal}-w1"], "ingress": f"{cal}-ing", "script": f"{cal}-k6", "scrape": cal,
            "ingress_pods": f"{cal}-ing-[a-z0-9]+-[a-z0-9]+"}


def pods_re(name: str) -> str:
    """The pod-name regex of a Deployment's pods (<name>-<rs hash>-<suffix>; no other calibration name matches)."""
    return f"{name}-[a-z0-9]+-[a-z0-9]+"


def job_name(cal: str, index: int) -> str:
    return f"{cal}-gen-t{index:03d}"


def labels(con: Dict, cal: str, component: str) -> Dict[str, str]:
    return {"app.kubernetes.io/part-of": PART_OF, CAL_LABEL: cal, CAL_RUN_LABEL: con["run_id"],
            X.COMPONENT_LABEL: component}


def _placement(params: Dict, role: str) -> Dict:
    R = X.import_harness().render
    return R._placement({"reservation": params["reservation"], "nodes": params["nodes"]}, role)


def _res(d: Dict) -> Dict:
    return {"requests": {"cpu": d["cpu_request"], "memory": d["memory_request"]},
            "limits": {"cpu": d["cpu_limit"], "memory": d["memory_limit"]}}


def render_fixture(con: Dict, cal: str, params: Dict, k: int) -> List[Dict]:
    """Calibration fixture variant k (work_iterations = work_selection.values[k]): render_arm's container shape
    (probes, ports, security, sidecar opt-outs) with the candidate configuration and an explicit GOMAXPROCS; no fault
    file, no PriorityClass, no run label."""
    R = X.import_harness().render
    ns, cand = con["namespace"], con["candidate"]
    name = names(cal)["variants"][k]
    lab = {**labels(con, cal, "calibration-fixture"), VARIANT_LABEL: f"w{k}"}
    sel = {CAL_LABEL: cal, X.COMPONENT_LABEL: "calibration-fixture", VARIANT_LABEL: f"w{k}"}
    env = [{"name": "MODE", "value": "server"}, {"name": "TARGET", "value": name},
           {"name": "WORK_ITERATIONS", "value": str(con["work_selection"]["values"][k])},
           {"name": "RESPONSE_BYTES", "value": str(cand["response_bytes"])},
           {"name": "MAX_INFLIGHT", "value": str(cand["max_inflight"])},
           {"name": "MAX_QUEUE", "value": str(cand["max_queue"])},
           {"name": "QUEUE_TIMEOUT_SECONDS", "value": str(cand["queue_timeout_seconds"])},
           {"name": "STARTUP_DELAY_SECONDS", "value": str(cand["startup_delay_seconds"])},
           {"name": "GOMAXPROCS", "value": str(cand["gomaxprocs"])}]
    pod = {"metadata": {"labels": {**lab, **R.SIDECAR_FREE},
                        "annotations": {"prometheus.io/scrape": "true", "prometheus.io/port": "8080",
                                        **R.SIDECAR_FREE}},
           "spec": {"automountServiceAccountToken": False, "securityContext": R._SECURITY,
                    "terminationGracePeriodSeconds": 10, **_placement(params, "workload"),
                    "containers": [{"name": "fixture", "image": con["images"]["fixture"], "env": env,
                                    "ports": [{"name": "http", "containerPort": 8080}],
                                    "readinessProbe": {"httpGet": {"path": "/ready", "port": "http"},
                                                       "periodSeconds": 1, "failureThreshold": 1,
                                                       "successThreshold": 1, "timeoutSeconds": 1},
                                    "livenessProbe": {"httpGet": {"path": "/healthz", "port": "http"},
                                                      "periodSeconds": 10, "failureThreshold": 6},
                                    "resources": _res(cand), "securityContext": R._CONTAINER_SECURITY}]}}
    dep = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name, "namespace": ns, "labels": lab},
           "spec": {"replicas": 0, "revisionHistoryLimit": 2, "selector": {"matchLabels": sel},
                    "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 0, "maxUnavailable": 1}},
                    "template": pod}}
    svc = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": ns, "labels": lab},
           "spec": {"selector": sel, "ports": [{"name": "http", "port": 80, "targetPort": "http"}]}}
    return [dep, svc]


def render_ingress(con: Dict, cal: str, params: Dict) -> List[Dict]:
    """The pinned calibration ingress (render_common's ingress shape) whose only upstreams are the two calibration
    fixture variants (one route each)."""
    R = X.import_harness().render
    ns, nm = con["namespace"], names(cal)
    lab = labels(con, cal, "calibration-ingress")
    sel = {CAL_LABEL: cal, X.COMPONENT_LABEL: "calibration-ingress"}
    dep = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": nm["ingress"], "namespace": ns,
                                                                       "labels": lab},
           "spec": {"replicas": 1, "selector": {"matchLabels": sel}, "strategy": {"type": "Recreate"},
                    "template": {"metadata": {"labels": {**lab, **R.SIDECAR_FREE},
                                              "annotations": {"prometheus.io/scrape": "true",
                                                              "prometheus.io/port": "8080", **R.SIDECAR_FREE}},
                                 "spec": {"automountServiceAccountToken": False, "securityContext": R._SECURITY,
                                          **_placement(params, "workload"),
                                          "containers": [{"name": "ingress", "image": con["images"]["fixture"],
                                                          "env": [{"name": "MODE", "value": "ingress"},
                                                                  {"name": "UPSTREAMS", "value": ",".join(
                                                                      f"{v}=http://{v}.{ns}.svc"
                                                                      for v in nm["variants"])},
                                                                  {"name": "TIMEOUT_SECONDS",
                                                                   "value": str(REQUEST_TIMEOUT_S)}],
                                                          "ports": [{"name": "http", "containerPort": 8080}],
                                                          "readinessProbe": {"httpGet": {"path": "/ready",
                                                                                         "port": "http"},
                                                                             "periodSeconds": 5},
                                                          "resources": _res(con["ingress"]),
                                                          "securityContext": R._CONTAINER_SECURITY}]}}}}
    svc = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": nm["ingress"], "namespace": ns, "labels": lab},
           "spec": {"selector": sel, "ports": [{"name": "http", "port": 80, "targetPort": "http"}]}}
    return [dep, svc]


def render_objects(con: Dict, cal: str, params: Dict, script: str) -> List[Dict]:
    """Every object apply creates before the first trial, in creation order."""
    R = X.import_harness().render
    ns, nm = con["namespace"], names(cal)
    objs = render_fixture(con, cal, params, 0) + render_fixture(con, cal, params, 1) + \
        render_ingress(con, cal, params)
    objs.append({"apiVersion": "v1", "kind": "ConfigMap",
                 "metadata": {"name": nm["script"], "namespace": ns, "labels": labels(con, cal, "calibration-load")},
                 "data": {"c28-calibrate.js": script}})
    if con["observer"]["scrape"] != "annotations":
        group = "monitoring.coreos.com/v1" if con["observer"]["scrape"] == "PodMonitor" else \
            "operator.victoriametrics.com/v1beta1"
        objs.append({"apiVersion": group, "kind": con["observer"]["scrape"],
                     "metadata": {"name": nm["scrape"], "namespace": ns,
                                  "labels": labels(con, cal, "calibration-scrape")},
                     "spec": {"selector": {"matchExpressions": [
                         {"key": CAL_LABEL, "operator": "In", "values": [cal]},
                         {"key": X.COMPONENT_LABEL, "operator": "In",
                          "values": ["calibration-fixture", "calibration-ingress"]}]},
                         "podMetricsEndpoints": [{"port": "http", "path": "/metrics",
                                                  "interval": f"{con['observer']['step_s']}s"}]}})
    R.audit_sidecar_free(objs)
    return objs


def vus(rate_per_min: int) -> Tuple[int, int]:
    """Preallocated and maximum VUs for an open-loop rate (as schedule.k6_plan sizes them: the worst latency is the
    2 s timeout plus the 0.1 s jitter, times 1.5, plus 10)."""
    pre = int(math.ceil(rate_per_min / 60 * (REQUEST_TIMEOUT_S + 0.1) * 1.5)) + 10
    return pre, 2 * pre


def trial_seed(con_sha: str, kind: str, target: str, n: int, rate: int, rep: int) -> int:
    return int(C.digest({"contract": con_sha, "kind": kind, "target": target, "n": n, "rate": rate,
                         "rep": rep})[:8], 16) % (2**31 - 1) + 1


def durations(con: Dict, kind: str) -> Tuple[int, int]:
    """(settle_s, measure_s) of a trial kind: the work selection's own, else the manifest's 300/900."""
    src = con["work_selection"] if kind == "selection" else con["search"]
    return src["settle_s"], src["measure_s"]


def generator_job(con: Dict, cal: str, params: Dict, meta: Dict) -> Dict:
    """The trial's k6 Job (meta: id, kind, target, rate_per_min, seed) with a finite activeDeadlineSeconds."""
    R = X.import_harness().render
    ns, nm, g = con["namespace"], names(cal), con["generator"]
    settle, measure = durations(con, meta["kind"])
    trial, rate = meta["id"], meta["rate_per_min"]
    lab = {**labels(con, cal, "calibration-generator"), TRIAL_LABEL: trial}
    pre, mx = vus(rate)
    args = ["run", "--no-usage-report"]
    for k, v in (("C28CAL_TRIAL", trial), ("C28CAL_TARGET", meta["target"]),
                 ("C28CAL_BASE_URL", f"http://{nm['ingress']}.{ns}.svc"), ("C28CAL_RATE_PER_MIN", rate),
                 ("C28CAL_SETTLE_S", settle), ("C28CAL_MEASURE_S", measure),
                 ("C28CAL_SEED", meta["seed"]), ("C28CAL_PREALLOCATED_VUS", pre),
                 ("C28CAL_MAX_VUS", mx), ("C28CAL_TIMEOUT_S", REQUEST_TIMEOUT_S)):
        args += ["-e", f"{k}={v}"]
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name(cal, int(trial[1:])), "namespace": ns, "labels": lab},
            "spec": {"backoffLimit": 0, "activeDeadlineSeconds": settle + measure + con["search"]["job_slack_s"],
                     "template": {"metadata": {"labels": {**lab, **R.SIDECAR_FREE},
                                               "annotations": dict(R.SIDECAR_FREE)}, "spec": {
                         "restartPolicy": "Never", "automountServiceAccountToken": False,
                         "securityContext": {"runAsNonRoot": True, "runAsUser": 12345,
                                             "seccompProfile": {"type": "RuntimeDefault"}},
                         **_placement(params, "support"),
                         "containers": [{"name": "k6", "image": con["images"]["generator"],
                                         "args": args + ["/scripts/c28-calibrate.js"],
                                         "resources": _res({k: v for k, v in g.items() if k != "k6_version"}),
                                         "terminationMessagePolicy": "File",
                                         "securityContext": {"allowPrivilegeEscalation": False,
                                                             "capabilities": {"drop": ["ALL"]}},
                                         "volumeMounts": [{"name": "script", "mountPath": "/scripts",
                                                           "readOnly": True}]}],
                         "volumes": [{"name": "script", "configMap": {"name": nm["script"]}}]}}}}


def _load_script() -> str:
    with open(LOAD_SCRIPT) as fh:
        return fh.read()


# --- the journal: state and ownership ---------------------------------------------------------------------------

def _mine(e: Dict, cal: str) -> bool:
    return (e.get("data") or {}).get("calibration") == cal


def calibration_state(entries: List[Dict], cal: str) -> str:
    """not-begun | open | closed (an ID is used once)."""
    state = "not-begun"
    for e in entries:
        if not _mine(e, cal):
            continue
        if e["event"] == "calibration-begin":
            state = "open"
        elif e["event"] == "calibration-closed":
            state = "closed"
    return state


def owned_objects(entries: List[Dict], cal: str) -> List[Dict]:
    """Every object this calibration created (applied creates, live or recovered), in creation order, each with
    deleted True once one of its UID-bound deletes applied."""
    out: List[Dict] = []
    for it in X.applied_intents(entries):
        d = it["data"]
        if (d.get("context") or {}).get("calibration") != cal:
            continue
        if d["op"] == "create":
            out.append({"kind": d["kind"], "namespace": d["namespace"], "name": d["name"], "uid": d["result_uid"],
                        "role": (d.get("context") or {}).get("role", ""), "deleted": False})
        elif d["op"] == "delete":
            for o in out:
                if o["uid"] == d["uid"]:
                    o["deleted"] = True
    return out


def ownership_view(entries: List[Dict], cal: str, ns: str) -> Dict:
    """What reserve.calibration_owned may recognize: exactly the Deployment and Job UIDs this journal created."""
    own = owned_objects(entries, cal)
    return {"id": cal, "namespace": ns, "workload": sorted(o["uid"] for o in own if o["kind"] == "Deployment"),
            "support": sorted(o["uid"] for o in own if o["kind"] == "Job")}


def _controller_uid(obj: Dict, kind: str) -> str:
    for r in (obj.get("metadata") or {}).get("ownerReferences") or []:
        if r.get("controller") and r.get("kind") == kind:
            return r.get("uid", "")
    return ""


def owned_pods(kube: X.Kube, ns: str, own: List[Dict]) -> Dict[str, List[Dict]]:
    """{<Deployment role>: [...], "generator": [...]} (roles: w0, w1, ingress): pods controlled (through a
    UID-checked ReplicaSet) by the owned Deployments, or directly by the owned Jobs, terminating ones included."""
    dep = {o["uid"]: o["role"] for o in own if o["kind"] == "Deployment"}
    jobs = {o["uid"] for o in own if o["kind"] == "Job"}
    rs = {r["metadata"]["uid"]: dep[_controller_uid(r, "Deployment")] for r in kube.list("ReplicaSet", ns)
          if _controller_uid(r, "Deployment") in dep}
    out: Dict[str, List[Dict]] = {**{r: [] for r in dep.values()}, "generator": []}
    for p in kube.list("Pod", ns):
        ref = _controller_uid(p, "ReplicaSet")
        if ref and ref in rs:
            out[rs[ref]].append(p)
        elif _controller_uid(p, "Job") in jobs:
            out["generator"].append(p)
    return out


# --- the search (pure; replayed by the reducer) ------------------------------------------------------------------

def bounds(con: Dict, n: int) -> Tuple[int, int, int]:
    """(Rmin, Rmax, R0) in total arrivals per minute for n replicas."""
    se = con["search"]
    lo = int(math.ceil(se["rate_min_rps_per_replica"] * n * 60))
    hi = int(math.floor(se["rate_max_rps_per_replica"] * n * 60))
    r0 = min(hi, max(lo, int(round(se["initial_rps_per_replica"] * n * 60))))
    return max(1, lo), max(1, hi), max(1, r0)


def next_step(con: Dict, n: int, history: List[Dict]) -> Dict:
    """The deterministic next action for replica count n given its plateau history [{rate, verdict: pass|fail}]:
    {"action": "test", "rate", "phase", "rule"} or {"action": "done", "outcome", "q", ...}."""
    rmin, rmax, r0 = bounds(con, n)
    se = con["search"]
    f = Decimal(str(se["bracket_factor"]))
    if not history:
        return {"action": "test", "rate": r0, "phase": "bracket", "rule": "initial"}
    passes = [h["rate"] for h in history if h["verdict"] == "pass"]
    fails = [h["rate"] for h in history if h["verdict"] == "fail"]
    lo, hi = (max(passes) if passes else None), (min(fails) if fails else None)
    if lo is not None and hi is not None and lo >= hi:
        return {"action": "done", "outcome": "non-monotonic", "q": None, "lo": lo, "hi": hi}
    if hi is None:
        if lo >= rmax:
            return {"action": "done", "outcome": "unresolved-lower-bound", "q": None, "lower_bound": lo}
        return {"action": "test", "rate": min(rmax, int((Decimal(lo) * f).to_integral_value(rounding="ROUND_CEILING"))),
                "phase": "bracket", "rule": "expand after a pass"}
    if lo is None:
        if hi <= rmin:
            return {"action": "done", "outcome": "no-qualifying-rate", "q": None, "upper_bound": hi}
        return {"action": "test", "rate": max(rmin, int((Decimal(hi) / f).to_integral_value(rounding="ROUND_FLOOR"))),
                "phase": "bracket", "rule": "contract after a fail"}
    mid = (lo + hi) // 2
    if Decimal(hi - lo) <= Decimal(str(se["resolution"])) * lo or mid <= lo:
        return {"action": "done", "outcome": "calibrated", "q": lo, "lo": lo, "hi": hi}
    return {"action": "test", "rate": mid, "phase": "bisect", "rule": "bisect the bracket"}


def worst_case(con: Dict) -> Dict:
    """Upper bounds of the search: plateaus and trials per replica count, and the total duration."""
    se, plateaus = con["search"], 0
    for n in se["replica_counts"]:
        rmin, rmax, _ = bounds(con, n)
        span = max(2.0, rmax / max(1, rmin))
        plateaus += int(math.ceil(math.log(span, se["bracket_factor"]))) + 1 + \
            int(math.ceil(math.log2(max(2, rmax - rmin)))) + 1
    trials = plateaus * se["repetitions"] + len(con["work_selection"]["values"]) + 1   # + selection + ceiling
    per_trial = trial_seconds(con)
    return {"plateaus": plateaus, "trials": trials, "trial_seconds": per_trial,
            "duration_s": trials * per_trial + (len(se["replica_counts"]) + 4) * se["converge_timeout_s"] +
            len(con["work_selection"]["values"]) * con["work_selection"]["idle_s"]}


def trial_seconds(con: Dict) -> int:
    se = con["search"]
    return se["settle_s"] + se["measure_s"] + se["job_slack_s"] + se["completion_grace_s"]


def plateau_verdict(verdicts: List[str], reps: int) -> str:
    """pass | fail | pending | the halting verdict (INCOMPLETE, LIMIT, STOPPED)."""
    for v in verdicts:
        if v in ("INCOMPLETE", "LIMIT", "STOPPED"):
            return v
    if "FAIL" in verdicts:
        return "fail"
    return "pass" if len(verdicts) == reps and all(v == "PASS" for v in verdicts) else "pending"


# --- the observer ---------------------------------------------------------------------------------------------------

def _http_json(url: str) -> Dict:
    with urllib.request.urlopen(url, timeout=15) as r:      # noqa: S310 - the contract's in-cluster query API
        return json.loads(r.read())


def resolve_query(con: Dict, target: str, name: str, pods: str = "") -> str:
    q = con["observer"]["queries"][name]
    return q.replace("__NAMESPACE__", con["namespace"]).replace("__TARGET__", target).replace("__PODS__", pods)


def query_range(fetch: Callable[[str], Dict], con: Dict, query: str, start: int, end: int) -> Dict:
    """One raw range query, recorded as asked and as answered (a failure is recorded, never a value)."""
    step = con["observer"]["step_s"]
    q = urllib.parse.urlencode({"query": query, "start": start, "end": end, "step": step})
    rec = {"query": query, "start": start, "end": end, "step": step}
    try:
        doc = fetch(f"{con['observer']['query_api'].rstrip('/')}/api/v1/query_range?{q}")
        rec["response"] = json.loads(json.dumps(doc, allow_nan=False))
    except Exception as e:          # noqa: BLE001 - any failure is a missing observation
        rec["error"] = type(e).__name__
    return rec


def series(obs: Dict) -> Tuple[Optional[List[Tuple[int, float]]], str]:
    """The one series of a recorded range query, or (None, why)."""
    if not isinstance(obs, dict) or "response" not in obs:
        return None, f"the query failed ({(obs or {}).get('error', 'not recorded')})"
    try:
        result = obs["response"]["data"]["result"]
        if not isinstance(result, list):
            return None, "malformed response"
        if len(result) != 1:
            return None, f"{len(result)} series where exactly one is required"
        pts = [(int(float(t)), float(v)) for t, v in result[0]["values"]]
    except (KeyError, TypeError, ValueError, IndexError):
        return None, "malformed response"
    if not all(math.isfinite(v) for _, v in pts):
        return None, "non-finite samples"
    return sorted(pts), ""


def covered(pts: List[Tuple[int, float]], start: int, end: int, step: int) -> str:
    """'' when the samples cover [start, end] (first and last within two steps of the edges, no gap over two
    steps); otherwise why not."""
    pts = [p for p in pts if start <= p[0] <= end]
    if not pts:
        return "no samples in the window"
    ts = [t for t, _ in pts]
    if ts[0] - start > 2 * step or end - ts[-1] > 2 * step:
        return "the samples do not reach the window's edges (stale or missing)"
    if any(b - a > 2 * step for a, b in zip(ts, ts[1:])):
        return "a gap of more than two steps"
    return ""


def _up(obs: Dict, at: int, con: Dict) -> str:
    """'' when the ingress scrape is healthy: at least three samples in the range, all 1, the last fresh."""
    pts, why = series(obs)
    if pts is None:
        return f"ingress_up: {why}"
    pts = [p for p in pts if p[0] <= at]
    if len(pts) < 3 or any(v != 1 for _, v in pts[-3:]) or at - pts[-1][0] > con["observer"]["max_staleness_s"]:
        return "ingress_up: the ingress scrape is not fresh and up"
    return ""


def quiet_value(obs: Dict, at: int, con: Dict, up: Optional[Dict] = None) -> Tuple[Optional[float], str]:
    """A counter read while nothing flows: at least three samples in the recorded range, all equal, the last fresh.
    A per-target counter that does not exist yet (the ingress has seen no arrival for that target since it started)
    reads 0 only while the ingress scrape itself is proven fresh and up (`up`, the ingress_up range)."""
    if isinstance(obs, dict) and isinstance(((obs.get("response") or {}).get("data") or {}).get("result"), list) \
            and obs["response"]["data"]["result"] == []:
        why = _up(up or {}, at, con)
        return (0.0, "") if not why else (None, f"no series, and {why}")
    pts, why = series(obs)
    if pts is None:
        return None, why
    pts = [p for p in pts if p[0] <= at]
    if len(pts) < 3:
        return None, "fewer than three samples"
    if at - pts[-1][0] > con["observer"]["max_staleness_s"]:
        return None, "the last sample is stale"
    if len({v for _, v in pts[-3:]}) != 1:
        return None, "the counter is still moving (not quiet)"
    return pts[-1][1], ""


def ingress_reading(fetch, con: Dict, cal: str, target: str, at: int) -> Dict:
    span = 4 * con["observer"]["step_s"]
    out = {"at": at, **{k: query_range(fetch, con, resolve_query(con, target, f"ingress_{k}"), at - span, at)
                        for k in ("requests", "responses")}}
    out["up"] = query_range(fetch, con, resolve_query(con, target, "ingress_up", names(cal)["ingress_pods"]),
                            at - span, at)
    return out


def fresh(fetch, con: Dict, cal: str, now: int) -> str:
    """'' when the observer scrapes the calibration ingress now: its up series fresh (within max_staleness_s) and 1."""
    span = 4 * con["observer"]["step_s"]
    why = _up(query_range(fetch, con, resolve_query(con, "", "ingress_up", names(cal)["ingress_pods"]),
                          now - span, now), now, con)
    return f"observer: {why}" if why else ""


# --- trial evaluation (pure; recomputed by the reducer) ------------------------------------------------------------

def _digest_of(image_id: str) -> str:
    m = re.search(r"sha256:([0-9a-f]{64})$", str(image_id or ""))
    return m.group(1) if m else ""


def _terminated(pod: Optional[Dict], container: str) -> Optional[Dict]:
    for cs in ((pod or {}).get("status") or {}).get("containerStatuses") or []:
        if cs.get("name") == container:
            return {**((cs.get("state") or {}).get("terminated") or {}), "imageID": cs.get("imageID", "")} \
                if (cs.get("state") or {}).get("terminated") else None
    return None


def _metric(summary: Dict, scenario: str, metric: str, field: str):
    v = (((summary.get("scenarios") or {}).get(scenario) or {}).get(metric) or {})
    v = v.get(field) if isinstance(v, dict) else None
    return v if _num(v) else None


def _ols_slope(ys: List[float]) -> float:
    n = len(ys)
    mx = (n - 1) / 2
    my = sum(ys) / n
    den = sum((i - mx) ** 2 for i in range(n))
    return sum((i - mx) * (y - my) for i, y in enumerate(ys)) / den if den else 0.0


def _pod_identity(pods: List[Dict]) -> Dict[str, Tuple]:
    out = {}
    for p in pods or []:
        m = p.get("metadata") or {}
        cs = ((p.get("status") or {}).get("containerStatuses") or [{}])[0]
        ready = any(c.get("type") == "Ready" and c.get("status") == "True"
                    for c in (p.get("status") or {}).get("conditions") or [])
        out[m.get("uid")] = (m.get("name"), int(cs.get("restartCount") or 0), _digest_of(cs.get("imageID")),
                             bool(m.get("deletionTimestamp")), ready, (p.get("spec") or {}).get("nodeName"))
    return out


def evaluate_trial(con: Dict, raw: Dict) -> Dict:
    """The verdict of one trial from its raw evidence alone (raw: trial meta, generator_pod, pods_begin, pods_end,
    observations). INCOMPLETE > LIMIT > FAIL > PASS; every criterion is reported separately. `delivery` says whether
    the arrivals were uncensored (no incomplete evidence, no generator/ingress limit); a selection trial also reports
    its CPU cost per request."""
    t, cr, ob = raw["trial"], con["criteria"], con["observer"]
    step, rate, n, kind = ob["step_s"], t["rate_per_min"], t["n"], t["kind"]
    settle_s, measure_s = durations(con, kind)
    incomplete, limits, failures, crit = [], [], [], {}
    tol = lambda x: max(cr["reconcile_tolerance"]["absolute"], cr["reconcile_tolerance"]["relative"] * x)  # noqa
    fx_digest = _DIGEST_IMAGE.match(con["images"]["fixture"]).group(1)
    gen_digest = _DIGEST_IMAGE.match(con["images"]["generator"]).group(1)
    if (t.get("settle_s"), t.get("measure_s")) != (settle_s, measure_s):
        incomplete.append(f"the trial's durations are not the contract's for a {kind} trial")
    # the generator: its termination message is the k6 summary
    term = _terminated(raw.get("generator_pod"), "k6")
    summary, started = {}, None
    if term is None:
        incomplete.append("the generator container did not terminate with a recorded state")
    else:
        if term.get("exitCode") != 0:
            incomplete.append(f"k6 exited {term.get('exitCode')} ({term.get('reason')})")
        if _digest_of(term.get("imageID")) != gen_digest:
            incomplete.append("the generator pod's image digest is not the contract's")
        try:
            summary = json.loads(term.get("message") or "", parse_constant=_reject_constant)
            started = _utc(term["startedAt"])
        except (ValueError, KeyError, TypeError):
            incomplete.append("the k6 summary (termination message) or the container start is missing or malformed")
            summary = {}
        if summary and (summary.get("schema") != K6_SCHEMA or summary.get("trial") != t["id"] or
                        summary.get("rate_per_min") != rate or summary.get("settle_s") != settle_s or
                        summary.get("measure_s") != measure_s or summary.get("target") != t["target"]):
            incomplete.append("the k6 summary is not this trial's (schema, trial, target, rate or durations differ)")
            summary = {}
    sched = {"settle": rate * settle_s / 60, "measure": rate * measure_s / 60}
    counts = {}
    for s in ("settle", "measure"):
        dispatched = _metric(summary, s, "iterations", "count")
        dropped = _metric(summary, s, "dropped_iterations", "count")
        terminal = _metric(summary, s, "http_reqs", "count")
        failed = _metric(summary, s, "http_req_failed", "passes")
        if summary and dropped is None and dispatched is not None and abs(sched[s] - dispatched) <= tol(sched[s]):
            dropped = 0          # k6 emits no dropped_iterations sample without a drop; reconciled just below
        counts[s] = {"scheduled": sched[s], "dispatched": dispatched, "dropped": dropped, "terminal": terminal,
                     "failed": failed}
        if summary and None in (dispatched, dropped, terminal, failed):
            incomplete.append(f"{s}: the k6 counts are incomplete")
        elif summary:
            if abs(sched[s] - (dropped + dispatched)) > tol(sched[s]):
                incomplete.append(f"{s}: scheduled {sched[s]:g} != dropped {dropped:g} + dispatched {dispatched:g}")
            if terminal > dispatched + tol(dispatched):
                incomplete.append(f"{s}: more terminal outcomes ({terminal:g}) than dispatched ({dispatched:g})")
    crit["arrivals"] = counts
    # the ingress: received and answered between the quiet readings
    obs = raw.get("observations") or {}
    readings = {}
    for side in ("before", "after"):
        r = obs.get(f"ingress_{side}") or {}
        readings[side] = {k: quiet_value(r.get(k) or {}, int(r.get("at") or 0), con, r.get("up"))
                          for k in ("requests", "responses")}
        for k, (v, why) in readings[side].items():
            if v is None:
                incomplete.append(f"ingress {k} {side} the trial: {why}")
    ing = {}
    if not any(v is None for side in readings.values() for v, _ in side.values()) and summary and \
            None not in (counts["settle"]["dispatched"], counts["measure"]["dispatched"]):
        received = readings["after"]["requests"][0] - readings["before"]["requests"][0]
        answered = readings["after"]["responses"][0] - readings["before"]["responses"][0]
        dispatched = counts["settle"]["dispatched"] + counts["measure"]["dispatched"]
        ing = {"received": received, "answered": answered, "dispatched": dispatched,
               "pre_ingress_failures": max(0.0, dispatched - received)}
        if received < 0 or answered < 0:
            incomplete.append("an ingress counter went backwards (a restart or reset)")
        elif received > dispatched + tol(dispatched):
            incomplete.append(f"unaccounted arrivals: the ingress received {received:g} but the generator dispatched "
                              f"{dispatched:g}")
        elif abs(answered - received) > tol(received):
            incomplete.append(f"the ingress answered {answered:g} of {received:g} arrivals after the grace period")
        elif dispatched - received > tol(dispatched):
            limits.append(f"{dispatched - received:g} dispatched arrivals never reached the ingress (a network "
                          "limit; offered demand was censored)")
    crit["ingress"] = ing
    window = None
    if started is not None:
        ws = started + settle_s
        window = {"start": ws, "end": ws + measure_s}
    crit["window"] = window
    # latency and failures (measure scenario, all requests)
    m = counts["measure"]
    p95 = _metric(summary, "measure", "http_req_duration", "p(95)")
    p99 = _metric(summary, "measure", "http_req_duration", "p(99)")
    crit["p95_ms"] = {"value": p95, "limit": cr["p95_ms"], "pass": None if p95 is None else p95 <= cr["p95_ms"]}
    crit["p99_ms"] = {"value": p99, "limit": cr["p99_ms"], "pass": None if p99 is None else p99 <= cr["p99_ms"]}
    for k in ("p95_ms", "p99_ms"):
        if crit[k]["pass"] is None and summary:
            incomplete.append(f"{k}: no latency summary")
        elif crit[k]["pass"] is False:
            failures.append(f"{k} {crit[k]['value']:g} > {crit[k]['limit']}")
    succeeded = None
    if None not in (m["dispatched"], m["dropped"], m["terminal"], m["failed"]):
        unknown = max(0.0, m["dispatched"] - m["terminal"])
        measured = (m["failed"] + m["dropped"]) / m["scheduled"]
        bound = (m["failed"] + m["dropped"] + unknown) / m["scheduled"]
        dropped_fraction = m["dropped"] / m["scheduled"]
        succeeded = m["terminal"] - m["failed"]
        crit["failure_fraction"] = {"measured": measured, "upper_bound": bound, "unknown_outcomes": unknown,
                                    "limit": cr["max_failure_fraction"], "pass": bound <= cr["max_failure_fraction"]}
        crit["dropped_fraction"] = {"value": dropped_fraction, "limit": cr["max_dropped_fraction"],
                                    "pass": dropped_fraction <= cr["max_dropped_fraction"]}
        if measured > cr["max_failure_fraction"]:
            failures.append(f"failure fraction {measured:.6f} > {cr['max_failure_fraction']}")
        elif bound > cr["max_failure_fraction"]:
            incomplete.append(f"{unknown:g} outcomes are unknown: the failure fraction is only bounded by {bound:.6f}")
        if dropped_fraction > cr["max_dropped_fraction"]:
            limits.append(f"the generator dropped {dropped_fraction:.6f} of the scheduled arrivals (a generator "
                          "limit; drops are never attributed to the fixture)")
    # observer series over the window (the evaluation's 15 s scrape cadence)
    obs_stats = {}
    named = [(name, window) for name in WINDOW_QUERIES]
    if kind == "selection":
        idle = obs.get("fixture_cpu_idle") or {}
        iw = {"start": idle.get("start"), "end": idle.get("end")}
        if not (_int(iw["start"]) and _int(iw["end"]) and iw["end"] - iw["start"] == con["work_selection"]["idle_s"]
                and started is not None and iw["end"] <= started):
            incomplete.append("fixture_cpu_idle: the idle window is not the declared idle_s before the trial")
        else:
            named.append(("fixture_cpu_idle", iw))
    for name, win in named:
        o = obs.get(name) or {}
        if win is None:
            break
        pts, why = series(o)
        if pts is not None and (o.get("start"), o.get("end"), o.get("step")) != (win["start"], win["end"], step):
            pts, why = None, "the recorded query is not over its window"
        if pts is not None:
            why = covered(pts, win["start"], win["end"], step)
        if pts is None or why:
            incomplete.append(f"{name}: {why}")
            continue
        vals = [v for ts, v in pts if win["start"] <= ts <= win["end"]]
        obs_stats[name] = {"mean": sum(vals) / len(vals), "max": max(vals), "samples": len(vals), "points": pts}
    crit["observed"] = {k: {kk: vv for kk, vv in v.items() if kk != "points"} for k, v in obs_stats.items()}
    if "fixture_backlog" in obs_stats:
        qg = cr["queue_growth"]
        nb = measure_s // qg["bin_s"]
        bins = []
        for i in range(nb):
            lo_, hi_ = window["start"] + i * qg["bin_s"], window["start"] + (i + 1) * qg["bin_s"]
            vs = [v for ts, v in obs_stats["fixture_backlog"]["points"]
                  if lo_ <= ts < hi_ or (i == nb - 1 and ts == hi_)]
            bins.append(sum(vs) / len(vs) if vs else None)
        if None in bins:
            incomplete.append("fixture_backlog: a one-minute bin has no sample")
        else:
            e = qg["edge_bins"]
            end_mean = sum(bins[-e:]) / e
            increase = end_mean - sum(bins[:e]) / e
            slope = _ols_slope(bins)
            over_end = end_mean / n > qg["max_end_backlog_per_replica"]
            sustained = increase > qg["max_increase"] and slope > qg["max_slope_per_min"]
            crit["queue_growth"] = {"bins": bins, "end_mean": end_mean, "end_mean_per_replica": end_mean / n,
                                    "increase": increase, "slope_per_min": slope, "rule": dict(qg),
                                    "end_over_cap": over_end, "sustained": sustained,
                                    "pass": not (over_end or sustained)}
            if over_end:
                failures.append(f"backlog at the end {end_mean / n:.3f} per replica > "
                                f"{qg['max_end_backlog_per_replica']}")
            if sustained:
                failures.append(f"sustained queue growth (+{increase:.3f} over the window, slope {slope:.3f}/min)")
    for name, key in (("generator_throttled_fraction", "max_generator_throttled_fraction"),
                      ("ingress_throttled_fraction", "max_ingress_throttled_fraction")):
        if name in obs_stats and obs_stats[name]["mean"] > cr[key]:
            limits.append(f"{name} {obs_stats[name]['mean']:.4f} > {cr[key]} (a generator/network limit)")
    # the active fixture variant and the ingress did not change during the trial; no other variant had pods
    pb, pe = raw.get("pods_begin") or {}, raw.get("pods_end") or {}
    begin, end = _pod_identity(pb.get(t["variant"])), _pod_identity(pe.get(t["variant"]))
    ib, ie = _pod_identity(pb.get("ingress")), _pod_identity(pe.get("ingress"))
    others = [r for side in (pb, pe) for r, ps in side.items() if r not in (t["variant"], "ingress") and ps]
    w_node = con["nodes"]["workload"]["name"]
    if len(begin) != n or begin != end:
        incomplete.append(f"the fixture pods changed during the trial or are not exactly {n} (restart, replacement)")
    if len(ib) != 1 or ib != ie:
        incomplete.append("the ingress pod changed during the trial or is not exactly one")
    if others:
        incomplete.append(f"other calibration pods were present during the trial: {sorted(set(others))}")
    for ident in list(begin.values()) + list(ib.values()):
        name, _, digest, terminating, ready, node_ = ident
        if digest != fx_digest:
            incomplete.append(f"pod {name}: image digest is not the contract's fixture")
        if terminating or not ready or node_ != w_node:
            incomplete.append(f"pod {name}: not Ready on W throughout")
    out = {"schema": RESULT_SCHEMA, "trial": t["id"], "kind": kind, "variant": t["variant"],
           "work_iterations": t["work_iterations"], "n": n, "rate_per_min": rate, "rep": t["rep"],
           "rps_per_replica": rate / 60 / n}
    if kind == "selection":
        cost = None
        if "fixture_cpu_cores" in obs_stats and "fixture_cpu_idle" in obs_stats and succeeded and succeeded > 0:
            busy = obs_stats["fixture_cpu_cores"]["mean"] - obs_stats["fixture_cpu_idle"]["mean"]
            cost = busy * 1000 / (succeeded / measure_s)
        elif not incomplete:
            incomplete.append("the CPU cost per request cannot be computed (no successful requests)")
        crit["work_cost"] = {"cpu_ms_per_request": cost, "range": dict(con["work_selection"]["cpu_ms_per_request"]),
                             "idle_cpu_cores": (obs_stats.get("fixture_cpu_idle") or {}).get("mean"),
                             "busy_cpu_cores": (obs_stats.get("fixture_cpu_cores") or {}).get("mean")}
    verdict = "INCOMPLETE" if incomplete else "LIMIT" if limits else "FAIL" if failures else "PASS"
    out.update(verdict=verdict, delivery="unknown" if incomplete else "censored" if limits else "uncensored",
               incomplete=sorted(set(incomplete)), limits=limits, failures=failures, criteria=crit)
    return out


# --- summarizing (pure; the certificate) ---------------------------------------------------------------------------

def selection_rate(con: Dict) -> int:
    return max(1, int(round(con["work_selection"]["rate_rps"] * 60)))


def select_work(con: Dict, results: List[Dict]) -> Dict:
    """The predeclared choice between the two work_iterations values: a value qualifies when its selection trial
    PASSes every criterion at the declared rate and its idle-subtracted CPU cost per request lies in the declared
    range; among qualifying values the cost closest to the target wins, a tie goes to fewer iterations. None
    qualifying: no value is selected and the calibration stops for a revised candidate (never an informal search)."""
    ws = con["work_selection"]
    rng = ws["cpu_ms_per_request"]
    meas = []
    for k, value in enumerate(ws["values"]):
        r = next((x for x in results if x.get("variant") == f"w{k}"), None)
        cost = ((r or {}).get("criteria") or {}).get("work_cost", {}).get("cpu_ms_per_request") if r else None
        ok = bool(r) and r["verdict"] == "PASS" and cost is not None and rng["min"] <= cost <= rng["max"]
        meas.append({"variant": f"w{k}", "work_iterations": value, "trial": (r or {}).get("trial"),
                     "verdict": (r or {}).get("verdict"), "cpu_ms_per_request": cost, "qualifies": ok})
    q = [m for m in meas if m["qualifies"]]
    best = min(q, key=lambda m: (abs(m["cpu_ms_per_request"] - rng["target"]), m["work_iterations"])) if q else None
    return {"rule": ws["tie_break"], "range": dict(rng), "rate_per_min": selection_rate(con), "measurements": meas,
            "selected": best["variant"] if best else None, "work_iterations": best["work_iterations"] if best else None}


def _expect(tr: Dict, kind: str, variant: str, n: int, rate: int, rep: int) -> str:
    want = (kind, variant, n, rate, rep)
    got = (tr.get("kind"), tr.get("variant"), tr.get("n"), tr.get("rate_per_min"), tr.get("rep"))
    return "" if got == want else f"trial {tr.get('trial')} is {got}, the algorithm asks for {want}"


def ceiling_rate(con: Dict, q_max: int) -> int:
    return int((Decimal(q_max) * Decimal(str(con["criteria"]["ceiling_factor"]))).to_integral_value(
        rounding="ROUND_CEILING"))


def replay(con: Dict, trials: List[Dict], halt: Optional[Dict]) -> Dict:
    """Replay the whole calibration over the recorded trials [{trial, kind, variant, n, rate_per_min, rep, verdict}]
    in order: the work selection (w0 then w1 at one replica), the c0 search on the selected variant, the ceiling
    probe. Every recorded trial must be exactly the one the algorithm asks for next, and the trials must end exactly
    where the algorithm or the recorded halt ends them."""
    se = con["search"]
    reps, problems, outcomes, plateaus = se["repetitions"], [], {}, []
    i, halted, selection, ceiling = 0, None, None, None

    def ran_out(what: str) -> Dict:
        if halt and halt.get("category") in ("budget", "stopped", "incomplete", "limit"):
            return dict(halt)
        problems.append(f"the trials end before the calibration did ({what})")
        return {"category": "incomplete", "reason": "the recorded trials end early"}

    def halting(tr: Dict) -> Optional[Dict]:
        cat = {"STOPPED": tr.get("category", "stopped"), "LIMIT": "limit", "INCOMPLETE": "incomplete"}
        return {"category": cat[tr["verdict"]], "reason": f"trial {tr['trial']} is {tr['verdict']}"} \
            if tr["verdict"] in cat else None
    sel = []
    for k in range(len(con["work_selection"]["values"])):
        if i >= len(trials):
            halted = ran_out(f"work selection w{k}")
            break
        tr = trials[i]
        bad = _expect(tr, "selection", f"w{k}", 1, selection_rate(con), 0)
        if bad:
            problems.append(bad)
            halted = {"category": "incomplete", "reason": "the recorded trials do not follow the calibration"}
            break
        sel.append(tr)
        i += 1
        halted = halting(tr)
        if halted:
            break
    if not halted:
        selection = select_work(con, sel)
        if selection["selected"] is None:
            halted = {"category": "work-selection", "reason": "neither predeclared work_iterations value qualifies: "
                                                              "STOP for a revised candidate"}
    variant = (selection or {}).get("selected")
    for n in se["replica_counts"] if not halted else []:
        history: List[Dict] = []
        while halted is None:
            step = next_step(con, n, history)
            if step["action"] == "done":
                outcomes[n] = step
                break
            if i + reps > se["max_trials"] and halt and halt.get("category") == "budget" and i == len(trials):
                halted = dict(halt)
                break
            if i >= len(trials):
                halted = ran_out(f"n={n} rate {step['rate']}")
                break
            verdicts, ids = [], []
            for rep in range(reps):
                if i >= len(trials):
                    break
                tr = trials[i]
                bad = _expect(tr, "search", variant, n, step["rate"], rep)
                if bad:
                    problems.append(bad)
                    halted = {"category": "incomplete", "reason": "the recorded trials do not follow the search"}
                    break
                verdicts.append(tr["verdict"])
                ids.append(tr["trial"])
                i += 1
                if tr["verdict"] != "PASS":
                    break
            if halted:
                break
            pv = plateau_verdict(verdicts, reps)
            plateaus.append({"n": n, "rate_per_min": step["rate"], "phase": step["phase"], "rule": step["rule"],
                             "trials": ids, "verdicts": verdicts, "verdict": pv})
            if pv in ("pass", "fail"):
                history.append({"rate": step["rate"], "verdict": pv})
            elif pv == "pending":
                halted = ran_out(f"plateau n={n} rate {step['rate']} unfinished")
            else:
                halted = halting(trials[i - 1])
        if halted:
            break
    n_max = max(se["replica_counts"])
    if not halted and all((outcomes.get(n) or {}).get("outcome") == "calibrated" for n in se["replica_counts"]):
        rate = ceiling_rate(con, outcomes[n_max]["q"])
        ceiling = {"n": n_max, "rate_per_min": rate, "factor": con["criteria"]["ceiling_factor"],
                   "q_per_min": outcomes[n_max]["q"], "authorized_max_per_min": bounds(con, n_max)[1]}
        if rate > bounds(con, n_max)[1]:
            halted = {"category": "ceiling", "reason": f"the required arrival ceiling {rate}/min is above the "
                                                       f"authorized maximum {bounds(con, n_max)[1]}/min"}
        elif i >= len(trials):
            halted = ran_out("the ceiling probe")
        else:
            tr = trials[i]
            bad = _expect(tr, "ceiling", variant, n_max, rate, 0)
            if bad:
                problems.append(bad)
                halted = {"category": "incomplete", "reason": "the recorded trials do not follow the calibration"}
            else:
                i += 1
                ceiling.update(trial=tr["trial"], verdict=tr["verdict"], delivery=tr.get("delivery", "unknown"))
                if tr["verdict"] == "STOPPED":
                    halted = halting(tr)
                elif ceiling["delivery"] != "uncensored":
                    halted = {"category": "limit" if ceiling["delivery"] == "censored" else "incomplete",
                              "reason": f"arrivals at the ceiling {rate}/min are {ceiling['delivery']}"}
    if i < len(trials):
        problems.append(f"{len(trials) - i} recorded trials follow the end of the calibration")
    return {"selection": selection, "outcomes": outcomes, "plateaus": plateaus, "ceiling": ceiling,
            "halted": halted, "problems": problems}


def per_rep_q(trials: List[Dict], n: int, rep: int) -> Optional[int]:
    """The highest tested search rate at which repetition `rep` qualified with every lower rate it ran also
    qualifying."""
    runs = sorted((t["rate_per_min"], t["verdict"]) for t in trials
                  if t.get("kind") == "search" and t["n"] == n and t["rep"] == rep)
    q = None
    for rate, v in runs:
        if v != "PASS":
            break
        q = rate
    return q


def summarize(con: Dict, cal: str, con_sha: str, trials: List[Dict], halt: Optional[Dict]) -> Dict:
    """The certificate's derived content from the trial results alone (the live close and the reducer both call it)."""
    R = X.import_harness().render
    se, cr = con["search"], con["criteria"]
    rp = replay(con, trials, halt)
    reasons: List[str] = [f"evidence: {p}" for p in rp["problems"]]
    if rp["halted"]:
        reasons.append(f"{rp['halted']['category']}: {rp['halted']['reason']}")
    per_n = {}
    searched = rp["selection"] is not None and rp["selection"]["selected"] is not None
    for n in se["replica_counts"]:
        o = rp["outcomes"].get(n)
        per_n[str(n)] = {"outcome": (o or {}).get("outcome", "not reached"), "q_per_min": (o or {}).get("q"),
                         "bounds_per_min": list(bounds(con, n)),
                         "q_by_repetition": {str(k): per_rep_q(trials, n, k) for k in range(se["repetitions"])}}
        if o is None:
            if searched and not rp["halted"]:
                reasons.append(f"n={n}: not reached")
        elif o["outcome"] == "unresolved-lower-bound":
            reasons.append(f"n={n}: unresolved lower bound (the arrival ceiling {o['lower_bound']}/min passed without "
                           "a crossing): not a calibrated capacity")
        elif o["outcome"] != "calibrated":
            reasons.append(f"n={n}: {o['outcome']}")
    c0 = c = None
    linearity = {}
    if all(per_n[str(n)]["outcome"] == "calibrated" for n in se["replica_counts"]):
        qs = []
        for n in se["replica_counts"]:
            for k in range(se["repetitions"]):
                q = per_n[str(n)]["q_by_repetition"][str(k)]
                if q is None or q < per_n[str(n)]["q_per_min"]:
                    reasons.append(f"evidence: n={n} repetition {k} has no qualifying rate at Q_n")
                else:
                    qs.append(Decimal(per_n[str(n)]["q_per_min"]) / (60 * n))
        if qs:
            c0 = float(min(qs).quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN))
            c = R.capacity_rps(c0)
        lo_n, hi_n = min(se["replica_counts"]), max(se["replica_counts"])
        for k in range(se["repetitions"]):
            q1 = per_n[str(lo_n)]["q_by_repetition"][str(k)]
            q4 = per_n[str(hi_n)]["q_by_repetition"][str(k)]
            ratio = (Decimal(q4) / (Decimal(q1) * (hi_n // lo_n))) if q1 and q4 else None
            linearity[str(k)] = None if ratio is None else float(ratio.quantize(Decimal("0.0001")))
            if ratio is None or ratio < Decimal(str(cr["linearity_min"])):
                reasons.append(f"linearity: repetition {k}: {hi_n}-replica / ({hi_n // lo_n} x {lo_n}-replica) = "
                               f"{linearity[str(k)]} < {cr['linearity_min']}")
    curves = [{k: t.get(k) for k in ("trial", "kind", "variant", "work_iterations", "n", "rep", "rate_per_min",
                                     "rps_per_replica", "verdict", "delivery")} |
              {"p95_ms": ((t.get("criteria") or {}).get("p95_ms") or {}).get("value"),
               "p99_ms": ((t.get("criteria") or {}).get("p99_ms") or {}).get("value"),
               "failure_fraction": ((t.get("criteria") or {}).get("failure_fraction") or {}).get("upper_bound"),
               "queue_growth": {k: ((t.get("criteria") or {}).get("queue_growth") or {}).get(k)
                                for k in ("end_mean_per_replica", "increase", "slope_per_min", "end_over_cap",
                                          "sustained")},
               "work_cost": (t.get("criteria") or {}).get("work_cost"),
               "observed": (t.get("criteria") or {}).get("observed"),
               "why": (t.get("incomplete") or []) + (t.get("limits") or []) + (t.get("failures") or [])
               + ([t["stop_reason"]] if t.get("stop_reason") else [])}
              for t in trials]
    ready = not reasons and c0 is not None and (rp["ceiling"] or {}).get("delivery") == "uncensored"
    if not reasons and not ready:
        reasons.append("incomplete: the calibration has no uncensored ceiling proof")
    return {"schema": CERT_SCHEMA, "calibration_id": cal, "contract_sha256": con_sha, "algorithm": SEARCH_ALGORITHM,
            "verdict": "READY" if ready else "NOT READY", "reasons": reasons, "c0_rps": c0 if ready else None,
            "c_rps": c if ready else None, "measured_c0_rps": c0, "candidate": dict(con["candidate"]),
            "work_selection": rp["selection"], "per_replica_count": per_n,
            "linearity": {"min": cr["linearity_min"], "by_repetition": linearity},
            "ceiling": rp["ceiling"], "queue_growth_rule": dict(cr["queue_growth"]),
            "plateaus": rp["plateaus"], "halted": rp["halted"], "curves": curves,
            "trials": len(trials), "scope_limits": SCOPE_LIMITS,
            "classification": {"measured_failures": sum(1 for t in trials if t["verdict"] == "FAIL"),
                               "incomplete": sum(1 for t in trials if t["verdict"] == "INCOMPLETE"),
                               "limit": sum(1 for t in trials if t["verdict"] == "LIMIT"),
                               "stopped": sum(1 for t in trials if t["verdict"] == "STOPPED")}}



# --- the live context -----------------------------------------------------------------------------------------------

class Context:
    def __init__(self, record: X.Record, kube: X.Kube, fetch: Callable[[str], Dict] = _http_json,
                 now: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep):
        self.record, self.kube, self.fetch, self.sleep = record, kube, fetch, sleep
        self._now = now
        self.c28 = X.import_harness()
        if self.c28.frozen.REQUEST_TIMEOUT_S != REQUEST_TIMEOUT_S:      # pragma: no cover
            raise X.StagingError("calibrate.REQUEST_TIMEOUT_S differs from frozen.REQUEST_TIMEOUT_S")
        self.params = X.load_params_stage(record, reserve.STAGE)
        record.section("reserve")
        self.con, self.sha, self.cal = load_contract(record, self.params)
        self.ns = self.con["namespace"]
        self.nm = names(self.cal)
        self.dir = os.path.join(record.evidence_dir, "calibrate", self.cal)

    def now(self) -> int:
        return int(self._now())

    def entries(self) -> List[Dict]:
        return X.receipts(self.record, HELPER)

    def receipt(self, event: str, data: Dict) -> Dict:
        return X.receipt(self.record, HELPER, event, {"calibration": self.cal, **data})

    def own(self) -> List[Dict]:
        return owned_objects(self.entries(), self.cal)

    def view(self) -> Dict:
        return ownership_view(self.entries(), self.cal, self.ns)

    def write(self, rel: str, obj) -> Dict:
        """A new evidence file under the calibration directory; returns {path (relative), sha256}."""
        sha = X.write_new(os.path.join(self.dir, rel), json.dumps(obj, indent=1, sort_keys=True) + "\n")
        return {"path": rel, "sha256": sha}

    def stop_requested(self) -> str:
        p = os.path.join(self.dir, "stop-request.json")
        if os.path.exists(p):
            try:
                return f"stop requested: {read_json_strict(p).get('reason', '')}"
            except (OSError, ValueError):
                return "stop requested"
        return ""


# --- live checks ----------------------------------------------------------------------------------------------------

def _requests(R, pod: Dict) -> Tuple[Decimal, Decimal]:
    try:
        return R._pod_requests(pod)
    except R.ParamsError:
        return Decimal(10**9), Decimal(10**15)      # an unparseable request is never treated as free


def admission(ctx: Context) -> Tuple[Dict, List[str]]:
    """The calibration footprint plus every other live pod's requests within limit_fraction of each node's live
    allocatable, CPU and memory separately (the calibration's own verified pods are counted in the footprint)."""
    R, con = ctx.c28.render, ctx.con
    view, cache = ctx.view(), {}
    pods = ctx.kube.list("Pod")
    cand, g = con["candidate"], con["generator"]
    fx = (R.cpu_millicores(cand["cpu_request"]), R.memory_bytes(cand["memory_request"]))
    ing = (R.cpu_millicores(con["ingress"]["cpu_request"]), R.memory_bytes(con["ingress"]["memory_request"]))
    gen = (R.cpu_millicores(g["cpu_request"]), R.memory_bytes(g["memory_request"]))
    nfx = max(con["search"]["replica_counts"]) + con["admission"]["overlap_pods"]
    foot = {"workload": [("fixture", nfx, fx), ("ingress", 1, ing)], "support": [("generator", 1, gen)]}
    for p in con["observer"]["pods"]:
        foot[p["role"]].append((f"observer {p['name']}", 1, (R.cpu_millicores(p["cpu_request"]),
                                                             R.memory_bytes(p["memory_request"]))))
    report, problems = {}, []
    limit = Decimal(str(con["admission"]["limit_fraction"]))
    for role in ("workload", "support"):
        want = con["nodes"][role]
        node = ctx.kube.get("Node", want["name"])
        if node is None or node["metadata"].get("uid") != want["uid"]:
            problems.append(f"{role} node {want['name']} is missing or replaced (UID differs from the contract)")
            continue
        alloc = (node.get("status") or {}).get("allocatable") or {}
        try:
            acpu, amem = R.cpu_millicores(alloc["cpu"]), R.memory_bytes(alloc["memory"])
        except (KeyError, R.ParamsError):
            problems.append(f"{role} node {want['name']}: allocatable CPU/memory unreadable")
            continue
        existing = [(p["metadata"].get("namespace"), p["metadata"]["name"], *_requests(R, p)) for p in pods
                    if (p.get("spec") or {}).get("nodeName") == want["name"]
                    and (p.get("status") or {}).get("phase") not in ("Succeeded", "Failed")
                    and not reserve.calibration_owned(ctx.kube, p, view, None, cache)]
        ecpu = sum((e[2] for e in existing), Decimal(0))
        emem = sum((e[3] for e in existing), Decimal(0))
        fcpu = sum((k * r[0] for _, k, r in foot[role]), Decimal(0))
        fmem = sum((k * r[1] for _, k, r in foot[role]), Decimal(0))
        tcpu, tmem = ecpu + fcpu, emem + fmem
        report[role] = {"node": want["name"], "allocatable": {"cpu": R._m(acpu), "memory": R._mi(amem)},
                        "existing_pods": len(existing), "existing": {"cpu": R._m(ecpu), "memory": R._mi(emem)},
                        "footprint": [{"what": w, "pods": k, "cpu": R._m(r[0]), "memory": R._mi(r[1])}
                                      for w, k, r in foot[role]],
                        "total": {"cpu": R._m(tcpu), "memory": R._mi(tmem)},
                        "limit": {"cpu": R._m(acpu * limit), "memory": R._mi(amem * limit)}}
        if tcpu > acpu * limit:
            problems.append(f"admission: {role} CPU {R._m(tcpu)} exceeds {limit} of allocatable {R._m(acpu)}")
        if tmem > amem * limit:
            problems.append(f"admission: {role} memory {R._mi(tmem)} exceeds {limit} of allocatable {R._mi(amem)}")
    return report, problems


def _settled(kind: str, o: Dict) -> str:
    """'' when a Deployment/StatefulSet/DaemonSet has no rollout in progress; otherwise why."""
    st, spec, gen = o.get("status") or {}, o.get("spec") or {}, (o.get("metadata") or {}).get("generation", 0)
    if int(st.get("observedGeneration") or 0) < int(gen or 0):
        return "its controller has not observed the current generation"
    if kind == "Deployment":
        want = int(spec.get("replicas", 1) if spec.get("replicas") is not None else 1)
        bad = [k for k in ("replicas", "updatedReplicas", "availableReplicas") if int(st.get(k) or 0) != want]
        return f"{', '.join(bad)} differ from {want}" if bad else ""
    if kind == "StatefulSet":
        want = int(spec.get("replicas", 1) if spec.get("replicas") is not None else 1)
        if st.get("updateRevision") and st.get("currentRevision") != st.get("updateRevision"):
            return "currentRevision differs from updateRevision"
        bad = [k for k in ("readyReplicas", "updatedReplicas") if int(st.get(k) or 0) != want]
        return f"{', '.join(bad)} differ from {want}" if bad else ""
    want = int(st.get("desiredNumberScheduled") or 0)
    bad = [k for k in ("updatedNumberScheduled", "numberAvailable") if int(st.get(k) or 0) != want]
    if int(st.get("numberUnavailable") or 0):
        bad.append("numberUnavailable")
    return f"{', '.join(bad)} differ from desired {want}" if bad else ""


def rollout_problems(ctx: Context) -> List[str]:
    """No rollout in progress for any workload with a pod on W or S (the calibration's own excepted) nor for the
    contract's extra_owners (e.g. the observer stack): capacity is never measured during platform rollouts."""
    view, cache, rs_cache, out, seen = ctx.view(), {}, {}, [], set()
    nodes = {ctx.con["nodes"][r]["name"] for r in ("workload", "support")}
    owners = []
    for p in ctx.kube.list("Pod"):
        if (p.get("spec") or {}).get("nodeName") not in nodes or \
                (p.get("status") or {}).get("phase") in ("Succeeded", "Failed") or \
                reserve.calibration_owned(ctx.kube, p, view, None, cache):
            continue
        o = reserve._owner(ctx.kube, p, rs_cache)
        if o["kind"] in ROLLOUT_KINDS:
            owners.append((o["kind"], o["namespace"], o["name"], o["uid"]))
    owners += [(o["kind"], o["namespace"], o["name"], None) for o in ctx.con["rollout_guard"]["extra_owners"]]
    for kind, ns, name, uid in owners:
        if (kind, ns, name) in seen:
            continue
        seen.add((kind, ns, name))
        live = ctx.kube.get(kind, name, ns)
        if live is None or (uid and live["metadata"].get("uid") != uid):
            out.append(f"rollout guard: {kind} {ns}/{name} cannot be read (its rollout state is unknown)")
            continue
        why = _settled(kind, live)
        if why:
            out.append(f"rollout guard: {kind} {ns}/{name} is mid-rollout ({why})")
    return out


def reservation_problems(ctx: Context) -> List[str]:
    try:
        out = reserve.verify(ctx.record, ctx.kube, calibration=ctx.view())
    except X.StagingError as e:                       # e.g. a replaced node: a problem, never a pass
        return [f"reservation: {e}"]
    try:
        reserve.require_reservation_pass(out)
    except X.StagingError as e:
        return [str(e)]
    return []


def activity_problems(ctx: Context) -> List[str]:
    """No session of the run open and no run generator active: calibration load never overlaps another load."""
    import session
    out = []
    opened = session._open_sessions(X.receipts(ctx.record, "session"))
    if opened:
        out.append(f"sessions of the run are open: {opened}")
    jobs = [j for ns in sorted({ctx.ns, ctx.params["control_namespace"]})
            for j in ctx.kube.list("Job", ns, labels={X.RUN_LABEL: ctx.record.run_id,
                                                      X.COMPONENT_LABEL: session.GENERATOR})
            if not any(c.get("type") in ("Complete", "Failed") and c.get("status") == "True"
                       for c in (j.get("status") or {}).get("conditions") or [])]
    if jobs:
        out.append(f"generators of the run are active: {sorted(j['metadata']['name'] for j in jobs)}")
    return out


def writer_problems(ctx: Context) -> List[str]:
    inv = X.writer_inventory(ctx.kube, ctx.ns, ctx.nm["variants"] + [ctx.nm["ingress"]])
    return [f"{name}: replica writer(s) {[X.ident_str(w) + '#' + str(w['uid']) for w in ws]}: the calibration runs "
            "at fixed replicas with no writer" for name, ws in sorted(inv.items()) if ws]


def ownership_problems(ctx: Context) -> List[str]:
    out = []
    for o in ctx.own():
        if o["deleted"]:
            continue
        live = ctx.kube.get(o["kind"], o["name"], o["namespace"])
        if live is None or live["metadata"].get("uid") != o["uid"]:
            out.append(f"owned {o['kind']}/{o['name']} is gone or replaced (UID differs from the journal)")
    return out


def node_problems(ctx: Context) -> List[str]:
    out = []
    for role in ("workload", "support"):
        n = ctx.kube.get("Node", ctx.con["nodes"][role]["name"])
        conds = {c["type"]: c["status"] for c in ((n or {}).get("status") or {}).get("conditions", [])}
        for k in ("MemoryPressure", "DiskPressure"):
            if conds.get(k) != "False":
                out.append(f"{role} node: {k} is {conds.get(k, 'unknown')}")
    return out


def injection(ctx: Context) -> List[str]:
    pods = owned_pods(ctx.kube, ctx.ns, ctx.own())
    return ctx.c28.render.unexpected_injection([p for ps in pods.values() for p in ps])


def converged(ctx: Context, name: str, uid: str, n: int) -> List[str]:
    """The Deployment at exactly n desired, its status converged, exactly n owned pods (none terminating), all Running
    and Ready on W."""
    import session
    dep = ctx.kube.get("Deployment", name, ctx.ns)
    if dep is None or dep["metadata"].get("uid") != uid:
        return [f"{name} is gone or replaced"]
    if int(dep["spec"].get("replicas") or 0) != n:
        return [f"{name} has {dep['spec'].get('replicas')} desired replicas, not {n}"]
    rs, pods = ctx.kube.list("ReplicaSet", ctx.ns), ctx.kube.list("Pod", ctx.ns)
    out = session.idle_residency_problems(dep, rs, pods, n)
    rs_uids = {r["metadata"]["uid"] for r in rs if _controller_uid(r, "Deployment") == uid}
    off = [p["metadata"]["name"] for p in pods if _controller_uid(p, "ReplicaSet") in rs_uids
           and (p.get("spec") or {}).get("nodeName") != ctx.con["nodes"]["workload"]["name"]]
    if off:
        out.append(f"{name} has pods off W: {sorted(off)}")
    return out


def gate_problems(ctx: Context, fixture_state: Optional[Dict] = None, reservation: bool = True) -> List[str]:
    """Everything that must hold before a trial and throughout it (fixture_state: the replicas and generation the
    fixture must still have)."""
    out = writer_problems(ctx) + ownership_problems(ctx) + node_problems(ctx) + rollout_problems(ctx) + \
        activity_problems(ctx) + injection(ctx)
    if reservation:
        out += reservation_problems(ctx)
    _, adm = admission(ctx)
    out += adm
    if fixture_state:
        dep = ctx.kube.get("Deployment", fixture_state["name"], ctx.ns)
        if dep is None or dep["metadata"].get("uid") != fixture_state["uid"] or \
                int(dep["spec"].get("replicas") or 0) != fixture_state["replicas"] or \
                dep["metadata"].get("generation") != fixture_state["generation"]:
            out.append("the calibration fixture's replicas or generation changed (another writer)")
    stop = ctx.stop_requested()
    if stop:
        out.append(stop)
    if ctx.now() >= _utc(ctx.con["search"]["deadline_utc"]):
        out.append("the calibration's total deadline has passed")
    return out


# --- plan -----------------------------------------------------------------------------------------------------------

def plan(ctx: Context) -> Dict:
    problems: List[str] = []
    entries = ctx.entries()
    if ctx.kube.identity() != ctx.record.cluster_uid:
        raise X.StagingError("the kubeconfig does not reach the recorded cluster (kube-system UID differs); refusing")
    state = calibration_state(entries, ctx.cal)
    if state != "not-begun":
        problems.append(f"calibration {ctx.cal} is {state}: an ID is used once (stop/close it; a new attempt is a "
                        "new contract.attempt)")
    if X.open_intents(entries):
        problems.append("the calibrate journal has intents with unknown outcomes: run recover first")
    script = _load_script()
    objs = render_objects(ctx.con, ctx.cal, ctx.params, script)
    for o in objs:
        if ctx.kube.get(o["kind"], o["metadata"]["name"], o["metadata"].get("namespace")) is not None:
            problems.append(f"{o['kind']}/{o['metadata']['name']} already exists")
    for role in ("workload", "support"):
        want = ctx.con["nodes"][role]
        n = ctx.kube.get("Node", want["name"])
        if n is None or n["metadata"].get("uid") != want["uid"]:
            problems.append(f"{role} node {want['name']} is missing or replaced (UID differs from the contract)")
    problems += writer_problems(ctx) + node_problems(ctx) + rollout_problems(ctx) + activity_problems(ctx) + \
        reservation_problems(ctx)
    adm, adm_problems = admission(ctx)
    problems += adm_problems
    probe = query_range(ctx.fetch, ctx.con, resolve_query(ctx.con, ctx.nm["variants"][0], "ingress_requests"),
                        ctx.now() - 60, ctx.now())
    if "response" not in probe:
        problems.append(f"the observer's query API does not answer ({probe.get('error')})")
    wc = worst_case(ctx.con)
    if _utc(ctx.con["search"]["deadline_utc"]) - ctx.now() < ctx.con["search"]["repetitions"] * trial_seconds(ctx.con):
        problems.append("the total deadline leaves no room for one plateau")
    rates = {str(n): dict(zip(("min", "max", "initial"), bounds(ctx.con, n)))
             for n in ctx.con["search"]["replica_counts"]}
    return X.new_plan(HELPER, ctx.record, [{"op": "create", "kind": o["kind"], "name": o["metadata"]["name"],
                                            "sha256": X.json_digest(o)} for o in objs],
                      calibration_id=ctx.cal, contract_sha256=ctx.sha, contract=ctx.con,
                      load_script_sha256=X.sha256_bytes(script.encode()),
                      work_selection={"values": ctx.con["work_selection"]["values"],
                                      "variants": ctx.nm["variants"], "rate_per_min": selection_rate(ctx.con),
                                      "rule": TIE_BREAK, "cpu_ms_per_request": ctx.con["work_selection"][
                                          "cpu_ms_per_request"]},
                      ceiling={"factor": ctx.con["criteria"]["ceiling_factor"],
                               "authorized_max_per_min": bounds(ctx.con, max(ctx.con["search"]["replica_counts"]))[1]},
                      search={"algorithm": SEARCH_ALGORITHM, "rates_per_min": rates,
                              "max_rate_per_min": max(r["max"] for r in rates.values()),
                              "max_trials": ctx.con["search"]["max_trials"],
                              "deadline_utc": ctx.con["search"]["deadline_utc"], "worst_case": wc,
                              "generator_job_deadline_s": ctx.con["search"]["settle_s"] +
                              ctx.con["search"]["measure_s"] + ctx.con["search"]["job_slack_s"]},
                      admission=adm, problems=problems,
                      never=["scale anything but the calibration fixture", "write a writer", "touch a confirmation "
                             "target, the quota or a fault schedule", "resume an interrupted measurement",
                             "a rate outside the plan's bounds"])


# --- apply: create, search, clean up, close ----------------------------------------------------------------------

def _wait(ctx: Context, what: str, check: Callable[[], List[str]], timeout: int) -> None:
    deadline = ctx.now() + timeout
    while True:
        left = check()
        if not left:
            return
        if ctx.now() >= deadline:
            raise X.StagingError(f"{what} within {timeout} s: {'; '.join(left)}")
        ctx.sleep(5)


def _variant(ctx: Context, role: str) -> Dict:
    o = next((o for o in ctx.own() if o["role"] == role and o["kind"] == "Deployment" and not o["deleted"]), None)
    if o is None:
        raise X.StagingError(f"the calibration fixture variant {role} is not an owned live Deployment")
    return o


def scale_to(ctx: Context, role: str, n: int) -> Dict:
    """Set a fixture variant's count through /scale (read resourceVersion + UID) as one journalled step that includes
    the wait for convergence; any failure leaves the step unresolved for recover."""
    fx = _variant(ctx, role)
    dep = ctx.kube.get("Deployment", fx["name"], ctx.ns)
    if dep is None or dep["metadata"]["uid"] != fx["uid"]:
        raise X.StagingError(f"the calibration fixture {fx['name']} is gone or replaced")
    m = dep["metadata"]
    cur = int(dep["spec"].get("replicas") or 0)
    if writer_problems(ctx):
        raise X.StagingError("; ".join(writer_problems(ctx)))
    pre = {"resourceVersion": m["resourceVersion"], "generation": m.get("generation"), "replicas": cur}

    def fn(mark):
        base = {"calibration": ctx.cal, "mark": mark, "name": fx["name"]}
        if cur != n:
            X.receipt(ctx.record, HELPER, "scale-write", dict(base, result="submitting"))
            try:
                ctx.kube.scale("Deployment", fx["name"], ctx.ns, fx["uid"], m["resourceVersion"], n)
            except C.ApiError as e:
                X.receipt(ctx.record, HELPER, "scale-write", dict(base, result="refused" if getattr(
                    e, "status", None) in X.DEFINITIVE else "unknown", status=getattr(e, "status", None)))
                raise
            X.receipt(ctx.record, HELPER, "scale-write", dict(base, result="applied",
                                                               generation=int(m.get("generation") or 0) + 1))
        _wait(ctx, f"{fx['name']} did not converge at {n}",
              lambda: converged(ctx, fx["name"], fx["uid"], n) + writer_problems(ctx),
              ctx.con["search"]["converge_timeout_s"])
        return ctx.kube.get("Deployment", fx["name"], ctx.ns)
    ctx.kube.journal.context = {"calibration": ctx.cal, "phase": "scale", "role": role}
    return ctx.kube.step("calibrate-scale", "Deployment", ctx.ns, fx["name"], fx["uid"], fn, pre=pre,
                         expect=[["/spec/replicas", n]])


def reconcile_scale(kube: X.Kube, d: Dict, record: X.Record) -> Dict:
    """recover's view of a calibrate-scale step: done when the Deployment (same UID, no writer) is at the target at
    exactly the generation the write explains and converged; not-done when nothing was written; else unresolved."""
    live = kube.get("Deployment", d["name"], d["namespace"])
    if live is None or live["metadata"].get("uid") != d["uid"]:
        return {"observed": "unresolved", "why": "the Deployment is gone or replaced"}
    if X.writer_inventory(kube, d["namespace"], [d["name"]]).get(d["name"]):
        return {"observed": "unresolved", "why": "a writer targets the calibration fixture"}
    to, pre_gen, pre_rep = d["expect"][0][1], d["pre"].get("generation"), d["pre"].get("replicas")
    gen, cur = live["metadata"].get("generation"), int(live["spec"].get("replicas") or 0)
    writes = [e["data"] for e in X.receipts(record, HELPER) if e["event"] == "scale-write"
              and e["data"].get("mark") == d["mark"]]
    submitted = any(w["result"] == "submitting" for w in writes)
    final = next((w for w in reversed(writes) if w["result"] != "submitting"), None)
    if pre_rep == to:
        want_gen = pre_gen
    elif final and final["result"] == "applied":
        want_gen = final["generation"]
    elif gen == pre_gen and cur == pre_rep:
        return {"observed": "not-done", "state": "not-applied", "uid": d["uid"]}
    elif submitted and (final is None or final["result"] == "unknown") and isinstance(pre_gen, int) and \
            gen == pre_gen + 1 and cur == to:
        want_gen = pre_gen + 1                   # a lost response: exactly our one step, nothing since
    else:
        return {"observed": "unresolved", "why": f"at {cur} replicas, generation {gen}: not explained by the intent"}
    if gen != want_gen or cur != to:
        return {"observed": "unresolved", "why": f"at {cur} replicas, generation {gen}: changed after the write"}
    import session
    left = session.idle_residency_problems(live, kube.list("ReplicaSet", d["namespace"]),
                                           kube.list("Pod", d["namespace"]), to)
    return {"observed": "done", "uid": d["uid"]} if not left else \
        {"observed": "not-done", "state": "applied-convergence-pending", "uid": d["uid"]}


X.RECONCILERS["calibrate-scale"] = reconcile_scale


def _role(o: Dict, nm: Dict) -> str:
    name, kind = o["metadata"]["name"], o["kind"]
    if kind in ("Deployment", "Service") and name in nm["variants"]:
        return f"w{nm['variants'].index(name)}"
    if kind in ("Deployment", "Service") and name == nm["ingress"]:
        return "ingress"
    return {"ConfigMap": "script"}.get(kind, "scrape")


def _snapshot(pods: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
    return {k: [C.sanitize_object(p) for p in sorted(v, key=lambda p: p["metadata"]["name"])]
            for k, v in pods.items() if k != "generator"}


def trial_meta(ctx: Context, index: int, kind: str, role: str, n: int, rate: int, rep: int) -> Dict:
    target = ctx.nm["variants"][int(role[1:])]
    settle, measure = durations(ctx.con, kind)
    return {"id": f"t{index:03d}", "kind": kind, "variant": role, "target": target,
            "work_iterations": ctx.con["work_selection"]["values"][int(role[1:])], "n": n, "rate_per_min": rate,
            "rep": rep, "settle_s": settle, "measure_s": measure,
            "seed": trial_seed(ctx.sha, kind, target, n, rate, rep)}


def run_trial(ctx: Context, meta: Dict, decision_sha: str) -> Dict:
    """One trial: gate, (selection: the idle-CPU window), a quiet ingress reading, the Job, monitoring every poll_s,
    completion, the after reading past the grace period, raw observations, the Job's deletion with its pods proven
    gone, then the verdict (evaluate_trial on the raw files). A gate violation stops the trial and raises Halt."""
    se, con = ctx.con["search"], ctx.con
    trial, n, rate = meta["id"], meta["n"], meta["rate_per_min"]
    if meta["kind"] != "selection":
        rmin, rmax = bounds(con, n)[:2]
        if not rmin <= rate <= rmax:                       # pragma: no cover - next_step never leaves the bounds
            raise X.StagingError(f"rate {rate} is outside the plan's bounds [{rmin}, {rmax}] for n={n}")
    fx = _variant(ctx, meta["variant"])
    dep = ctx.kube.get("Deployment", fx["name"], ctx.ns)
    fstate = {"name": fx["name"], "uid": fx["uid"], "replicas": n, "generation": dep["metadata"].get("generation")}
    tdir = f"trials/{trial}"
    observations: Dict = {}
    problems = gate_problems(ctx, fstate) + converged(ctx, fx["name"], fx["uid"], n)
    stale = fresh(ctx.fetch, con, ctx.cal, ctx.now())
    if not problems and not stale and meta["kind"] == "selection":
        idle_s = con["work_selection"]["idle_s"]
        t0 = ctx.now()
        ctx.sleep(idle_s)
        problems = gate_problems(ctx, fstate) + converged(ctx, fx["name"], fx["uid"], n)
        observations["fixture_cpu_idle"] = query_range(ctx.fetch, con, resolve_query(
            con, meta["target"], "fixture_cpu_cores", pods_re(fx["name"])), t0, t0 + idle_s)
        stale = fresh(ctx.fetch, con, ctx.cal, ctx.now())
    if problems or stale:
        ctx.receipt("trial-not-started", {"trial": meta, "problems": problems + ([stale] if stale else [])})
        raise Halt("incomplete" if stale and not problems else "stopped", "; ".join(problems + [stale]).strip("; "))
    before = ingress_reading(ctx.fetch, con, ctx.cal, meta["target"], ctx.now())
    if any(quiet_value(before[k], before["at"], con, before["up"])[0] is None for k in ("requests", "responses")):
        ctx.receipt("trial-not-started", {"trial": meta, "problems": ["the ingress is not quiet and fresh before the "
                                                                      "trial"]})
        raise Halt("incomplete", "the ingress counters are not quiet and fresh before the trial")
    pods_begin = _snapshot(owned_pods(ctx.kube, ctx.ns, ctx.own()))
    job = generator_job(con, ctx.cal, ctx.params, meta)
    files = [ctx.write(f"{tdir}/job.json", job), ctx.write(f"{tdir}/pods-begin.json", pods_begin)]
    ctx.receipt("trial-begin", {"trial": meta, "decision_sha256": decision_sha, "job_sha256": X.json_digest(job),
                                "files": files})
    ctx.kube.journal.context = {"calibration": ctx.cal, "phase": "trial", "trial": trial, "role": "generator"}
    created = ctx.kube.create(job)
    juid = created["metadata"]["uid"]
    hard = ctx.now() + job["spec"]["activeDeadlineSeconds"] + 120
    while True:
        j = ctx.kube.get("Job", job["metadata"]["name"], ctx.ns)
        if j is None or j["metadata"].get("uid") != juid:
            stop_trial(ctx, meta, juid, "the generator Job is gone or replaced", "stopped", files)
        conds = {c.get("type"): c.get("status") for c in (j.get("status") or {}).get("conditions") or []}
        if conds.get("Complete") == "True" or conds.get("Failed") == "True":
            break
        if ctx.now() >= hard:
            stop_trial(ctx, meta, juid, "the generator Job outlived its deadline", "stopped", files)
        problems = gate_problems(ctx, fstate)
        stale = fresh(ctx.fetch, con, ctx.cal, ctx.now())
        if problems:
            stop_trial(ctx, meta, juid, "; ".join(problems), "stopped", files)
        if stale:
            stop_trial(ctx, meta, juid, stale, "incomplete", files)
        ctx.sleep(se["poll_s"])
    gpod = next((p for p in owned_pods(ctx.kube, ctx.ns, ctx.own())["generator"]
                 if _controller_uid(p, "Job") == juid), None)
    gsnap = C.sanitize_object(gpod) if gpod else None
    ctx.sleep(se["completion_grace_s"])
    observations.update(ingress_before=before,
                        ingress_after=ingress_reading(ctx.fetch, con, ctx.cal, meta["target"], ctx.now()))
    term = _terminated(gpod, "k6")
    try:
        started = _utc(term["startedAt"]) if term else None
    except (KeyError, ValueError, TypeError):
        started = None
    if started:
        ws = started + meta["settle_s"]
        regex = {"fixture_backlog": "", "fixture_cpu_cores": pods_re(fx["name"]),
                 "fixture_throttled_fraction": pods_re(fx["name"]), "ingress_cpu_cores": ctx.nm["ingress_pods"],
                 "ingress_throttled_fraction": ctx.nm["ingress_pods"],
                 "generator_cpu_cores": f"{job['metadata']['name']}-[a-z0-9]+",
                 "generator_throttled_fraction": f"{job['metadata']['name']}-[a-z0-9]+"}
        for name in WINDOW_QUERIES:
            observations[name] = query_range(ctx.fetch, con, resolve_query(con, meta["target"], name, regex[name]),
                                             ws, ws + meta["measure_s"])
    pods_end = _snapshot(owned_pods(ctx.kube, ctx.ns, ctx.own()))
    remove_jobs(ctx, [{"name": job["metadata"]["name"], "uid": juid}])
    files += [ctx.write(f"{tdir}/generator-pod.json", gsnap), ctx.write(f"{tdir}/observations.json", observations),
              ctx.write(f"{tdir}/pods-end.json", pods_end)]
    result = evaluate_trial(con, {"trial": meta, "generator_pod": gsnap, "pods_begin": pods_begin,
                                  "pods_end": pods_end, "observations": observations})
    rfile = ctx.write(f"{tdir}/result.json", result)
    ctx.receipt("trial-result", {"trial": meta, "verdict": result["verdict"], "files": files, "result": rfile})
    return result


def stop_trial(ctx: Context, meta: Dict, juid: str, reason: str, category: str, files: List[Dict]) -> None:
    """Arrivals first: delete the trial's Job and prove its pods gone; the trial is recorded as STOPPED."""
    ctx.receipt("trial-stopping", {"trial": meta, "reason": reason[:500]})
    remove_jobs(ctx, [{"name": job_name(ctx.cal, int(meta["id"][1:])), "uid": juid}])
    ctx.receipt("trial-stopped", {"trial": meta, "reason": reason[:500], "category": category, "files": files})
    raise Halt(category, reason)


class CleanupUnresolved(X.StagingError):
    """A cleanup the executor could not prove: the lock stays (lock.safe False) until recover and stop."""


def remove_jobs(ctx: Context, jobs: List[Dict]) -> None:
    """Delete each live owned generator Job (UID-bound) and prove that no pod of any owned Job remains (terminating
    ones included), then let in-flight requests time out. An unproven removal raises CleanupUnresolved."""
    for j in jobs:
        live = ctx.kube.get("Job", j["name"], ctx.ns)
        if live is not None and live["metadata"].get("uid") == j["uid"]:
            ctx.kube.journal.context = {"calibration": ctx.cal, "phase": "cleanup", "role": "generator"}
            ctx.kube.delete("Job", j["name"], ctx.ns, j["uid"])
    try:
        _wait(ctx, "the generator pods are not gone", lambda: [
            f"generator pod {p['metadata']['name']} remains" for p in owned_pods(ctx.kube, ctx.ns, ctx.own())[
                "generator"]], ctx.con["search"]["converge_timeout_s"])
    except X.StagingError as e:
        raise CleanupUnresolved(str(e)) from e
    ctx.sleep(REQUEST_TIMEOUT_S + 1)


def cleanup(ctx: Context) -> List[str]:
    """Stop arrivals (delete every live owned Job, prove all generator pods gone), THEN delete the other owned objects
    (UID-bound) and wait until the fixture's and ingress's pods are gone. Returns what stayed unresolved."""
    left: List[str] = []
    own = ctx.own()
    try:
        remove_jobs(ctx, [o for o in own if o["kind"] == "Job" and not o["deleted"]])
    except X.StagingError as e:
        return [f"generator cleanup: {e}"]        # never touch the fixture while arrivals may continue
    order = {"scrape": 0, "ingress": 1, "w0": 2, "w1": 2, "script": 3}
    for o in sorted((o for o in own if o["kind"] != "Job" and not o["deleted"]),
                    key=lambda o: (order.get(o["role"], 9), o["kind"] != "Service", o["name"])):
        live = ctx.kube.get(o["kind"], o["name"], o["namespace"])
        if live is None:
            continue
        if live["metadata"].get("uid") != o["uid"]:
            left.append(f"{o['kind']}/{o['name']} was replaced (UID differs): not touched")
            continue
        try:
            ctx.kube.journal.context = {"calibration": ctx.cal, "phase": "cleanup", "role": o["role"]}
            ctx.kube.delete(o["kind"], o["name"], o["namespace"], o["uid"])
        except X.StagingError as e:
            left.append(f"delete {o['kind']}/{o['name']}: {e}")
    def remaining() -> List[str]:
        # owned through the UID chain, and (stricter) any pod still labelled with this calibration's id: a pod whose
        # ReplicaSet is already gone is no longer reachable through the chain but still occupies W
        names_ = {p["metadata"]["name"] for ps in owned_pods(ctx.kube, ctx.ns, own).values() for p in ps}
        names_ |= {p["metadata"]["name"] for p in ctx.kube.list("Pod", ctx.ns)
                   if (p["metadata"].get("labels") or {}).get(CAL_LABEL) == ctx.cal}
        return [f"pod {n} remains" for n in sorted(names_)]
    try:
        _wait(ctx, "the calibration pods are not gone", remaining, ctx.con["search"]["converge_timeout_s"])
    except X.StagingError as e:
        left.append(str(e))
    return left


def _stopped_result(t: Dict, d: Dict) -> Dict:
    return {"schema": RESULT_SCHEMA, "trial": t["id"], "kind": t["kind"], "variant": t["variant"],
            "work_iterations": t["work_iterations"], "n": t["n"], "rate_per_min": t["rate_per_min"], "rep": t["rep"],
            "rps_per_replica": t["rate_per_min"] / 60 / t["n"], "verdict": "STOPPED", "delivery": "unknown",
            "stop_reason": d.get("reason", ""), "category": d.get("category", "stopped"),
            "incomplete": [], "limits": [], "failures": [], "criteria": {}}


def _trial_records(entries: List[Dict], cal: str, base: str) -> Tuple[List[Dict], List[Dict]]:
    """(results, interrupted) from the journal: every trial-result (its result file) and trial-stopped or
    trial-interrupted, in order; a trial-begin without any of them is interrupted."""
    out, begun, ended = [], {}, set()
    for e in entries:
        if not _mine(e, cal):
            continue
        d = e["data"]
        if e["event"] == "trial-begin":
            begun[d["trial"]["id"]] = d["trial"]
        elif e["event"] == "trial-result":
            with open(os.path.join(base, d["result"]["path"])) as fh:
                out.append(json.load(fh))
            ended.add(d["trial"]["id"])
        elif e["event"] in ("trial-stopped", "trial-interrupted"):
            out.append(_stopped_result(d["trial"], d))
            ended.add(d["trial"]["id"])
    return out, [t for k, t in begun.items() if k not in ended]


def close(ctx: Context, outcome: str, halt: Optional[Dict]) -> Dict:
    """Write manifest.json, certificate.json (summarize over the recorded trials), the calibration-closed receipt,
    journal.jsonl and bundle-index.json. Interrupted trials are recorded as such first, never resumed."""
    for t in _trial_records(ctx.entries(), ctx.cal, ctx.dir)[1]:
        ctx.receipt("trial-interrupted", {"trial": t, "category": "stopped",
                                          "reason": "the executor ended before the trial finished; an interrupted "
                                                    "measurement is never resumed"})
    trials, _ = _trial_records(ctx.entries(), ctx.cal, ctx.dir)
    manifest = {}
    for root, _, fs in os.walk(ctx.dir):
        for f in sorted(fs):
            p = os.path.join(root, f)
            rel = os.path.relpath(p, ctx.dir)
            if rel not in ("manifest.json", "certificate.json", "journal.jsonl", "bundle-index.json",
                           "stop-request.json"):
                manifest[rel] = C.sha256_file(p)
    mfile = ctx.write("manifest.json", {"calibration_id": ctx.cal, "files": manifest})
    cert = summarize(ctx.con, ctx.cal, ctx.sha, trials, halt)
    cert.update(outcome=outcome, manifest_sha256=mfile["sha256"])
    cfile = ctx.write("certificate.json", cert)
    ctx.receipt("calibration-closed", {"outcome": outcome, "verdict": cert["verdict"], "halt": halt,
                                       "certificate_sha256": cfile["sha256"], "manifest_sha256": mfile["sha256"],
                                       "work_iterations": (cert["work_selection"] or {}).get("work_iterations"),
                                       "c0_rps": cert["c0_rps"], "c_rps": cert["c_rps"]})
    with open(C.receipts_path(ctx.record.doc, HELPER), "rb") as fh:
        jsha = C.write_once(os.path.join(ctx.dir, "journal.jsonl"), fh.read())
    ctx.write("bundle-index.json", {"calibration_id": ctx.cal, "journal_sha256": jsha,
                                    "manifest_sha256": mfile["sha256"], "certificate_sha256": cfile["sha256"]})
    return {"calibration_id": ctx.cal, "outcome": outcome, "verdict": cert["verdict"], "reasons": cert["reasons"],
            "work_iterations": (cert["work_selection"] or {}).get("work_iterations"),
            "c0_rps": cert["c0_rps"], "c_rps": cert["c_rps"],
            "certificate": os.path.join(ctx.dir, "certificate.json")}


def record_contract_path(record: X.Record) -> str:
    return record.resolve(str(record.section("calibrate")["contract"]))


def apply(ctx: Context, reviewed: Dict, reviewed_digest: str) -> Dict:
    with X.mutating(ctx.record, ctx.kube, HELPER, {"calibration": ctx.cal}) as lock:
        live = plan(ctx)                       # recomputed UNDER the shared lock
        X.require_live(reviewed, reviewed_digest, HELPER, ctx.record, live)
        if live["problems"]:
            raise X.StagingError("the calibration plan has problems: " + "; ".join(live["problems"]))
        ctx.receipt("calibration-begin", {"plan_sha256": reviewed_digest, "contract_sha256": ctx.sha,
                                          "algorithm": SEARCH_ALGORITHM})
        try:
            with open(record_contract_path(ctx.record), "rb") as fh:
                C.write_once(os.path.join(ctx.dir, "contract.json"), fh.read())
            for o in render_objects(ctx.con, ctx.cal, ctx.params, _load_script()):
                ctx.write(f"objects/{o['kind']}-{o['metadata']['name']}.json", o)
                ctx.kube.journal.context = {"calibration": ctx.cal, "phase": "create", "role": _role(o, ctx.nm)}
                ctx.kube.create(o)
            ing = _variant(ctx, "ingress")
            _wait(ctx, "the calibration ingress did not become ready",
                  lambda: converged(ctx, ing["name"], ing["uid"], 1), ctx.con["search"]["converge_timeout_s"])
            halt = calibrate(ctx)
        except CleanupUnresolved as e:
            ctx.receipt("cleanup-unresolved", {"error": str(e)[:500]})
            lock.safe = False                      # the lock stays: recover, then stop
            raise
        except Exception as e:
            ctx.receipt("apply-error", {"error": str(e)[:500], "outcome_known": lock.safe})
            if not lock.safe:
                raise                              # an unknown outcome: recover decides it
            left = cleanup(ctx)
            if left:
                ctx.receipt("cleanup-unresolved", {"unresolved": left})
                lock.safe = False
                raise
            close(ctx, "STOPPED", {"category": "stopped", "reason": f"executor error: {str(e)[:300]}"})
            raise
        left = cleanup(ctx)
        if left:
            ctx.receipt("cleanup-unresolved", {"unresolved": left})
            lock.safe = False
            raise X.StagingError(f"cleanup unresolved {left}: the lock stays; recover, then stop-plan / stop")
        return close(ctx, "COMPLETED" if halt is None else "HALTED", halt)


def calibrate(ctx: Context) -> Optional[Dict]:
    """The reviewed calibration: work selection, the c0 search on the selected variant, the ceiling probe. Returns
    the halt (None when it ran to its end)."""
    se, index = ctx.con["search"], 0
    # 1. the predeclared work_iterations selection: each variant alone at one replica, at the declared rate
    sel = []
    for k in range(len(ctx.con["work_selection"]["values"])):
        role = f"w{k}"
        scale_to(ctx, role, 1)
        index += 1
        dec = ctx.receipt("decision", {"stage": "work-selection", "variant": role,
                                       "step": {"action": "test", "rate": selection_rate(ctx.con)}})
        try:
            res = run_trial(ctx, trial_meta(ctx, index, "selection", role, 1, selection_rate(ctx.con), 0),
                            dec["sha256"])
        except Halt as h:
            return _halt(ctx, h.category, h.reason)
        scale_to(ctx, role, 0)
        sel.append(res)
        if res["verdict"] in ("INCOMPLETE", "LIMIT"):
            return _halt(ctx, res["verdict"].lower(), f"work selection trial {res['trial']} is {res['verdict']}")
    choice = select_work(ctx.con, sel)
    ctx.receipt("work-selected", choice)
    if choice["selected"] is None:
        return _halt(ctx, "work-selection", "neither predeclared work_iterations value qualifies: STOP for a revised "
                                            "candidate")
    role = choice["selected"]
    # 2. the c0 search on the selected variant
    prev, outcomes = "", {}
    for n in se["replica_counts"]:
        scale_to(ctx, role, n)
        history: List[Dict] = []
        while True:
            step = next_step(ctx.con, n, history)
            dec = ctx.receipt("decision", {"stage": "search", "n": n, "history": history,
                                           "preceding_plateau_sha256": prev, "step": step})
            if step["action"] == "done":
                outcomes[n] = step
                break
            if index + se["repetitions"] > se["max_trials"]:
                return _halt(ctx, "budget", f"max_trials {se['max_trials']} reached before plateau n={n} "
                                            f"rate={step['rate']}")
            if ctx.now() + se["repetitions"] * trial_seconds(ctx.con) > _utc(se["deadline_utc"]):
                return _halt(ctx, "budget", f"the total deadline leaves no room for plateau n={n} rate={step['rate']}")
            verdicts = []
            for rep in range(se["repetitions"]):
                index += 1
                try:
                    res = run_trial(ctx, trial_meta(ctx, index, "search", role, n, step["rate"], rep), dec["sha256"])
                except Halt as h:
                    return _halt(ctx, h.category, h.reason)
                verdicts.append(res["verdict"])
                if res["verdict"] != "PASS":
                    break
            pv = plateau_verdict(verdicts, se["repetitions"])
            prev = ctx.receipt("plateau", {"n": n, "rate_per_min": step["rate"], "verdicts": verdicts,
                                           "verdict": pv})["sha256"]
            if pv not in ("pass", "fail"):
                return _halt(ctx, {"LIMIT": "limit", "INCOMPLETE": "incomplete"}.get(pv, "stopped"),
                             f"plateau n={n} rate={step['rate']} is {pv}")
            history.append({"rate": step["rate"], "verdict": pv})
    # 3. the ceiling probe: arrivals uncensored at ceiling_factor x Q of the largest count
    if not all(o["outcome"] == "calibrated" for o in outcomes.values()):
        return None
    n = max(se["replica_counts"])
    rate = ceiling_rate(ctx.con, outcomes[n]["q"])
    dec = ctx.receipt("decision", {"stage": "ceiling", "n": n, "step": {"action": "test", "rate": rate}})
    if rate > bounds(ctx.con, n)[1]:
        return _halt(ctx, "ceiling", f"the required arrival ceiling {rate}/min is above the authorized maximum "
                                     f"{bounds(ctx.con, n)[1]}/min")
    if index + 1 > se["max_trials"]:
        return _halt(ctx, "budget", f"max_trials {se['max_trials']} reached before the ceiling probe")
    index += 1
    try:
        res = run_trial(ctx, trial_meta(ctx, index, "ceiling", role, n, rate, 0), dec["sha256"])
    except Halt as h:
        return _halt(ctx, h.category, h.reason)
    if res["delivery"] != "uncensored":
        return _halt(ctx, "limit" if res["delivery"] == "censored" else "incomplete",
                     f"arrivals at the ceiling {rate}/min are {res['delivery']}")
    return None


def _halt(ctx: Context, category: str, reason: str) -> Dict:
    h = {"category": category, "reason": reason[:500]}
    ctx.receipt("search-halted", h)
    return h



# --- stop, request-stop, recover ------------------------------------------------------------------------------------

def plan_stop(ctx: Context) -> Dict:
    entries = ctx.entries()
    if calibration_state(entries, ctx.cal) != "open":
        raise X.StagingError(f"calibration {ctx.cal} is not open in the journal")
    problems, steps, unresolved = [], [], []
    if X.open_intents(entries):
        problems.append("the calibrate journal has intents with unknown outcomes: run recover first")
    own = owned_objects(entries, ctx.cal)
    for o in sorted((o for o in own if not o["deleted"]), key=lambda o: o["kind"] != "Job"):
        live = ctx.kube.get(o["kind"], o["name"], o["namespace"])
        if live is None:
            continue
        if live["metadata"].get("uid") != o["uid"]:
            unresolved.append(f"{o['kind']}/{o['name']} was replaced (UID differs): not touched")
            continue
        steps.append({"op": "delete", "kind": o["kind"], "name": o["name"], "uid": o["uid"], "role": o["role"]})
    steps.insert(sum(1 for s in steps if s["kind"] == "Job"),
                 {"op": "prove-gone", "what": "every pod of every owned generator Job (terminating ones included)"})
    begun, _ = _trial_records(entries, ctx.cal, ctx.dir)
    return X.new_plan(STOP, ctx.record, steps, calibration_id=ctx.cal, unresolved=unresolved, problems=problems,
                      interrupted=[t["id"] for t in _trial_records(entries, ctx.cal, ctx.dir)[1]],
                      recorded_trials=len(begun),
                      never=["resume a measurement", "touch an object the journal did not record", "evict"])


def stop(ctx: Context, reviewed: Dict, reviewed_digest: str, reason: str = "operator stop") -> Dict:
    with X.mutating(ctx.record, ctx.kube, HELPER, {"calibration": ctx.cal, "phase": "stop"}) as lock:
        p = plan_stop(ctx)
        X.require_live(reviewed, reviewed_digest, STOP, ctx.record, p)
        if p["problems"]:
            raise X.StagingError("; ".join(p["problems"]))
        ctx.receipt("stop-begin", {"plan_sha256": reviewed_digest, "reason": reason})
        left = cleanup(ctx) + p["unresolved"]
        if left:
            ctx.receipt("cleanup-unresolved", {"unresolved": left})
            lock.safe = False
            raise X.StagingError(f"cleanup unresolved {left}: the lock stays; recover, then stop again")
        return close(ctx, "STOPPED", {"category": "stopped", "reason": reason})


def request_stop(ctx: Context, reason: str) -> Dict:
    if calibration_state(ctx.entries(), ctx.cal) != "open":
        raise X.StagingError(f"calibration {ctx.cal} is not open")
    out = ctx.write("stop-request.json", {"reason": reason[:300], "at": _ts(ctx.now())})
    return {"calibration_id": ctx.cal, "stop_request": out}


# --- status and verify ---------------------------------------------------------------------------------------------

def status(ctx: Context) -> Dict:
    entries = ctx.entries()
    trials, interrupted = _trial_records(entries, ctx.cal, ctx.dir) if os.path.isdir(ctx.dir) else ([], [])
    closed = next((e["data"] for e in reversed(entries) if _mine(e, ctx.cal) and e["event"] ==
                   "calibration-closed"), None)
    return {"helper": HELPER, "calibration_id": ctx.cal, "contract_sha256": ctx.sha,
            "state": calibration_state(entries, ctx.cal),
            "owned": owned_objects(entries, ctx.cal),
            "trials": [{k: t.get(k) for k in ("trial", "n", "rep", "rate_per_min", "verdict")} for t in trials],
            "interrupted": interrupted,
            "decisions": [e["data"]["step"] for e in entries if _mine(e, ctx.cal) and e["event"] == "decision"],
            "closed": closed, "open_intents": X.open_intents(entries)}


def verify(ctx: Context) -> Dict:
    state = calibration_state(ctx.entries(), ctx.cal)
    if state == "closed":
        return {"helper": HELPER, "state": state, "bundle": reduce_bundle(ctx.dir)}
    if state != "open":
        return {"helper": HELPER, "state": state, "stops": []}
    stops = gate_problems(ctx)
    stale = fresh(ctx.fetch, ctx.con, ctx.cal, ctx.now())
    if stale:
        stops.append(stale)
    if X.open_intents(ctx.entries()):
        stops.append("intents with unknown outcomes: recovery needed")
    return {"helper": HELPER, "state": state, "calibration_id": ctx.cal, "stops": stops,
            "result": "OK" if not stops else "STOP"}


# --- the offline reducer ---------------------------------------------------------------------------------------------

def reduce_bundle(bundle: str) -> Dict:
    """Recompute a closed calibration from its evidence bundle alone: every hash, the receipt chain, every trial's
    verdict from its raw files, the search replay, c0, c and linearity. Raises StagingError on any difference."""
    def load(rel):
        return read_json_strict(os.path.join(bundle, rel))
    idx = load("bundle-index.json")
    for rel, key in (("journal.jsonl", "journal_sha256"), ("manifest.json", "manifest_sha256"),
                     ("certificate.json", "certificate_sha256")):
        if C.sha256_file(os.path.join(bundle, rel)) != idx.get(key):
            raise X.StagingError(f"{rel}: sha256 differs from bundle-index.json")
    entries = C.read_receipts(os.path.join(bundle, "journal.jsonl"))     # verifies the hash chain
    cal = idx["calibration_id"]
    closed = [e["data"] for e in entries if _mine(e, cal) and e["event"] == "calibration-closed"]
    if len(closed) != 1 or closed[0]["certificate_sha256"] != idx["certificate_sha256"] or \
            closed[0]["manifest_sha256"] != idx["manifest_sha256"]:
        raise X.StagingError("the journal's calibration-closed receipt does not bind this certificate and manifest")
    manifest = load("manifest.json")
    for rel, sha in sorted(manifest["files"].items()):
        p = os.path.join(bundle, rel)
        if not os.path.isfile(p) or C.sha256_file(p) != sha:
            raise X.StagingError(f"{rel}: missing or its sha256 differs from the manifest")
    con = load("contract.json")
    con_sha = C.digest(con)
    begin = [e["data"] for e in entries if _mine(e, cal) and e["event"] == "calibration-begin"]
    if calibration_id(con_sha) != cal or len(begin) != 1 or begin[0]["contract_sha256"] != con_sha:
        raise X.StagingError("the bundle's contract is not this calibration's (a different candidate or contract "
                             "is a different calibration; results are never combined)")
    c28 = X.import_harness()
    confirmation = {c28.render.target_name(a, p) for a in c28.frozen.ARMS for p in c28.frozen.PROFILES}
    if set(names(cal)["variants"]) & confirmation or not cal.startswith("c28cal-"):
        raise X.StagingError("the calibration target is a confirmation target name")
    trials, trial_files = [], {}
    for e in entries:
        if not _mine(e, cal):
            continue
        d = e["data"]
        if e["event"] == "trial-begin":
            trial_files[d["trial"]["id"]] = list(d["files"])
        elif e["event"] == "trial-result":
            files = {os.path.basename(f["path"]).split(".")[0]: f for f in d["files"]}
            for f in d["files"] + [d["result"]]:
                if manifest["files"].get(f["path"]) != f["sha256"]:
                    raise X.StagingError(f"{f['path']}: the receipt's sha256 differs from the manifest")
            if d["trial"]["target"] not in names(cal)["variants"]:
                raise X.StagingError(f"trial {d['trial']['id']} targets {d['trial']['target']}, not this calibration")
            raw = {"trial": d["trial"], "generator_pod": load(files["generator-pod"]["path"]),
                   "pods_begin": load(files["pods-begin"]["path"]), "pods_end": load(files["pods-end"]["path"]),
                   "observations": load(files["observations"]["path"])}
            again = evaluate_trial(con, raw)
            if C.digest(again) != C.digest(load(d["result"]["path"])) or again["verdict"] != d["verdict"]:
                raise X.StagingError(f"trial {d['trial']['id']}: the recomputed result differs from the recorded one")
            trials.append(again)
        elif e["event"] in ("trial-stopped", "trial-interrupted"):
            trials.append(_stopped_result(d["trial"], d))
    cert = load("certificate.json")
    again = summarize(con, cal, con_sha, trials, closed[0]["halt"])
    again.update(outcome=closed[0]["outcome"], manifest_sha256=idx["manifest_sha256"])
    if C.digest(again) != C.digest(cert):
        diff = sorted(k for k in set(again) | set(cert) if again.get(k) != cert.get(k))
        raise X.StagingError(f"the recomputed certificate differs from the recorded one in {diff}")
    return {"reproduced": True, "calibration_id": cal, "verdict": again["verdict"], "reasons": again["reasons"],
            "c0_rps": again["c0_rps"], "c_rps": again["c_rps"], "linearity": again["linearity"],
            "trials": len(trials)}


# --- CLI ------------------------------------------------------------------------------------------------------------

def main(argv=None, api=None, fetch=None, now=None, sleep=None) -> int:
    ap = argparse.ArgumentParser(prog="calibrate.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", default="")
    ap.add_argument("command", nargs="?", default="plan",
                    choices=["plan", "apply", "status", "verify", "request-stop", "stop-plan", "stop",
                             "recover-plan", "recover", "reduce"])
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    ap.add_argument("--reason", default="")
    ap.add_argument("--bundle", default="")
    args = ap.parse_args(argv)
    try:
        if args.command == "reduce":
            if not args.bundle:
                raise X.StagingError("reduce needs --bundle <evidence/calibrate/<id>>")
            X.emit(reduce_bundle(args.bundle))
            return 0
        if not args.record:
            raise X.StagingError("--record is required")
        record = X.load_record(args.record, ("reserve", "calibrate"))
        kube = record.kube(api)
        kw = {k: v for k, v in (("fetch", fetch), ("now", now), ("sleep", sleep)) if v is not None}
        ctx = Context(record, kube, **kw)
        if args.command in ("apply", "stop", "recover") and not (args.plan and args.plan_digest):
            raise X.StagingError(f"{args.command} needs --plan <the reviewed plan file> and --plan-digest")
        if args.command == "plan":
            X.emit(X.cli_plan(record, HELPER, plan(ctx)))
        elif args.command == "apply":
            X.emit(apply(ctx, X.load_plan(args.plan), args.plan_digest))
        elif args.command == "status":
            X.emit(status(ctx))
        elif args.command == "verify":
            out = verify(ctx)
            X.emit(out)
            return 0 if out.get("result", "OK") == "OK" else 1
        elif args.command == "request-stop":
            if not args.reason:
                raise X.StagingError("request-stop needs --reason")
            X.emit(request_stop(ctx, args.reason))
        elif args.command == "stop-plan":
            X.emit(X.cli_plan(record, STOP, plan_stop(ctx), "stop-plan"))
        elif args.command == "stop":
            X.emit(stop(ctx, X.load_plan(args.plan), args.plan_digest, args.reason or "operator stop"))
        elif args.command == "recover-plan":
            X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
        else:
            X.emit(X.recover(record, kube, X.load_plan(args.plan), args.plan_digest))
    except X.StagingError as e:
        print(f"calibrate: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
