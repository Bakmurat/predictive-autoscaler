#!/usr/bin/env python3
"""Quiesce (and selectively restore) the legacy benchmark (STAGING-C28-PROD.md S4; "Helpers to implement": legacy.py).

S4 runs in TWO reviewed phases, each with its own read-only plan and digest:

  legacy.py --record R plan                     phase A, read-only: bind the allowlist to live UIDs, write the
                                                quiesce plan (default command)
  legacy.py --record R quiesce --plan P --plan-digest D     phase A: execute exactly that reviewed plan
  legacy.py --record R park-plan                phase B, read-only: requires phase A's completion receipt; re-reads
                                                the targets against phase A's captured state; writes the park plan
  legacy.py --record R park --plan P --plan-digest D        phase B: execute exactly that reviewed park plan
  legacy.py --record R verify [--observe-seconds N]         read-only: post-parking control state over a continuous
                                                            observation; "retired" only after >= 600 s of it, all
                                                            clean, bound to phase A's arrivals proof
  legacy.py --record R restore-plan             read-only: the selective undo of what both phases did (receipts)
  legacy.py --record R restore --plan P --plan-digest D     execute exactly that reviewed restore plan
  legacy.py --record R recover --lock-sha256 S  after an ambiguous outcome: observe each unresolved intent live,
                                                record it, release the shared lock (S = the lock file's sha256)

Phase A ("quiesce") names only the allowlisted launchers and writers (legacy_allowlist.py), each bound to its UID and
generation from a fresh read-only inventory that checks every writer's ACTUAL target and every generated HPA's owner,
in S4 order: suspend the launcher CronJobs and let their active Jobs finish (deadline); stop the k6 generators and wait
until their pods are gone; re-check live, right before it, that every legacy PA is a benchmark arm and nothing
reconciles the namespaces; stop the old operator and wait until its pods are gone; delete the benchmark ScaledObjects
with UID preconditions (their generated HPAs must go with them; the shared KEDA operator keeps running); prove no
writer remains; SETTLE for legacy.arrivals.settle_seconds (>= the rate query's window, so the one-minute rate no longer
contains requests from before the stop; a non-zero rate while settling is expected and not read); then OBSERVE the
shutdown signals continuously for observe_seconds without interruption (the old Lease is not renewed, no stopped
workload has a pod, no Job runs, the targets do not change, no writer or consumer appears, and every target's request
counters, read fresh from the metrics API through the reviewed legacy.arrivals queries, show zero requests: the
summed rate is zero, the newest sample is fresh, and EVERY expected reporter (each target pod running when the
observation starts, grouped by arrivals.pod_label) has its own fresh series; a missing observation blocks, it is never
read as zero); finally CAPTURE the stable target state (UID, generation, resourceVersion, replicas). Phase A never
parks a target and never binds a target's generation or replicas (the old operator may change them until it has
stopped): it binds the target UIDs only, and its completion receipt (quiesce-complete) carries the captured state and
the hash of the arrivals-observation receipt.

Phase B ("park") is planned only from phase A's completion receipt: the latest quiesce-complete of this run in the
verified hash chain, bound into the park plan by its sha256 together with the arrivals proof. park-plan re-reads every
target and refuses one whose UID or generation changed after the captured state (re-run phase A to capture a new stable
state), re-checks that the phase A post-conditions still hold, and plans: a live re-check right before parking (the
stopped workloads, writers, consumers, Lease and one more fresh zero-arrivals sample from every running reporter); the
arm Deployments to the parked count (bound to the captured generation); their pods gone; active Jobs finished; the old
API stopped. Legacy PAs, PVCs, the legacy CRD and the Lease are never touched, even by an edited plan; a phase A plan
can only touch launchers and writers, a phase B plan only targets and the API.

Retirement (S4 step 6) is the preserved PRE-parking traffic proof followed by continuous POST-parking control-state
verification. Parking the targets (and stopping the generators) removes their Istio reporters, so their series cannot
establish fresh zero traffic after parking; verify therefore does NOT query metrics and never reads an absent series as
zero. It requires (i) phase A's completed arrivals observation (>= 600 s of fresh per-target, per-reporter zero
rates; its receipt is bound into phase B's plan and park-complete receipt and re-verified through the hash chain) and
(ii) a park-complete receipt bound to that phase A, and then observes for >= 600 s that the targets stay at the parked
count with no pods and their captured UIDs, no generator/operator/API pod exists, the launcher CronJobs stay suspended
and no Job of theirs appears or runs, no ScaledObject or generated HPA, HPA, new-group PA, legacy PA consumer or
reconciler appears, the old Lease is not renewed or created, and nothing is recreated or rescaled.

Quiesce, park and restore share ONE mutation lock per cluster (common_records.lock_path, scope "legacy") and one
receipt journal ("legacy"; step numbers continue across phases and attempts, so every intent is unique). Execution
re-checks the cluster UID before every mutation, refuses an object whose UID or generation changed since the plan,
scales through /scale with the read resourceVersion and the UID, deletes with a UID precondition, and records each
mutation's INTENT and pre-state (a ScaledObject's sanitized snapshot included) durably BEFORE the call and its outcome
after; an interrupted call leaves the lock and an intent that recover reconciles against the live object. Restore
undoes only what was done, newest first across both phases, bound to the post-quiesce/post-park state (generation and
value: a later change is drift and refused), checks that no foreign writer owns a target before it restores a writer,
and that each target is ready with a single writer before it restarts the generators. It never overwrites an existing
object and never restores a whole namespace.

legacy.arrivals (record): query_api; rate_query, age_query and series_query, each with {target} and the SAME label
selector, which must include destination_workload="{target}" and destination_workload_namespace="demo" (the targets'
namespace); series_query returns one age per reporter pod, grouped "by (<pod_label>)"; pod_label (required; Istio's
destination pod label on prodcluster is "pod"); max_age_seconds (1..600); settle_seconds (default 90, >= the largest
range window in rate_query and >= 60, <= 600).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import sys
import time
from typing import Callable, Dict, List, Mapping, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common_records as C  # noqa: E402
import inventory as I  # noqa: E402
import legacy_allowlist as L  # noqa: E402

HELPER = "legacy"                                   # phase A plans, and the ONE receipt journal of both phases
PARK = "legacy-park"                                # phase B plans (a quiesce plan can never run as park, or back)
RESTORE = "legacy-restore"
LOCK_SCOPE = "legacy"                               # ONE mutation lock for quiesce, park and restore on a cluster
OP_ANNOTATION = "c28.devkuban.com/restore-operation"  # identity of a recreation, for recovery
QUIET_SECONDS = 600
SAMPLE_SECONDS = 10.0
NEVER = ("persistentvolumeclaims", "persistentvolumes", "pa-legacy", "customresourcedefinitions", "namespaces",
         "leases")                                  # evidence, models, archived PAs, CRDs, namespaces: never mutated
ROLES = {(k, ns, n): role for k, ns, n, role in L.ALLOWLIST}
ALLOWED = {(k, ns, n) for k, ns, n, role in L.ALLOWLIST if role not in ("retained", "pa", "lease")}
QUIESCE_ROLES = ("launcher", "generator", "operator", "keda")
PARK_ROLES = ("target", "api")
MUTATING = ("suspend", "scale", "delete", "recreate")
TARGET_NS = "demo"
_LABEL = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MATCHER = re.compile(r'\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"\s*')
_SENTINEL = "__c28_target__"
_WINDOW = re.compile(r"\[(\d+)([smh])\]")


def _selector(query: str, field: str) -> frozenset:
    """The one label selector of a query as a set of (label, op, value) matchers ({target} kept as a sentinel)."""
    sels = re.findall(r"\{([^{}]*)\}", query.replace("{target}", _SENTINEL))
    if len(sels) != 1:
        raise C.StagingError(f"legacy.arrivals.{field} must carry exactly one label selector")
    raw = sels[0]
    out = [m.groups() for m in _MATCHER.finditer(raw)]
    if re.sub(r"[\s,]", "", _MATCHER.sub("", raw)):
        raise C.StagingError(f"legacy.arrivals.{field}: cannot parse its label selector")
    if len(set(out)) != len(out):
        raise C.StagingError(f"legacy.arrivals.{field} repeats a matcher")
    return frozenset(out)


def _arrivals(arr) -> Dict:
    """Validate the reviewed legacy.arrivals block (Codex: the age and series queries must observe exactly what the
    rate query observes, in the targets' namespace; every expected reporter must be fresh; a bounded settle period)."""
    keys = ("query_api", "rate_query", "age_query", "series_query", "pod_label")
    if not isinstance(arr, dict) or not all(isinstance(arr.get(k), str) and arr[k] for k in keys) or \
            not all("{target}" in arr[k] for k in ("rate_query", "age_query", "series_query")) or \
            not isinstance(arr.get("max_age_seconds"), int) or isinstance(arr.get("max_age_seconds"), bool) or \
            not 0 < arr["max_age_seconds"] <= 600:
        raise C.StagingError("legacy.arrivals needs query_api, rate_query, age_query and series_query (each with "
                             "{target}), pod_label and max_age_seconds (1..600): request counters are observed per "
                             "target and per reporter while quiet")
    if not _LABEL.match(arr["pod_label"]):
        raise C.StagingError("legacy.arrivals.pod_label must be a label name")
    sels = {k: _selector(arr[k], k) for k in ("rate_query", "age_query", "series_query")}
    for k, sel in sels.items():
        ns = [m for m in sel if m[0] == "destination_workload_namespace"]
        if ns != [("destination_workload_namespace", "=", TARGET_NS)]:
            raise C.StagingError(f'legacy.arrivals.{k} must select destination_workload_namespace="{TARGET_NS}" '
                                 f"exactly (the rate, age and series queries observe the same namespace)")
        if ("destination_workload", "=", _SENTINEL) not in sel:
            raise C.StagingError(f'legacy.arrivals.{k} must select destination_workload="{{target}}"')
    if not sels["rate_query"] == sels["age_query"] == sels["series_query"]:
        raise C.StagingError("legacy.arrivals: rate_query, age_query and series_query must carry the same label "
                             "selector (the age and series must belong to the rated series)")
    if not re.search(r"\bby\s*\(\s*" + re.escape(arr["pod_label"]) + r"\s*\)", arr["series_query"]):
        raise C.StagingError("legacy.arrivals.series_query must group by (pod_label): one series per reporter pod")
    windows = [int(n) * {"s": 1, "m": 60, "h": 3600}[u] for n, u in _WINDOW.findall(arr["rate_query"])]
    if not windows:
        raise C.StagingError("legacy.arrivals.rate_query needs a range window (e.g. [1m])")
    settle = arr.get("settle_seconds", 90)
    if not isinstance(settle, int) or isinstance(settle, bool) or not max(60, max(windows)) <= settle <= 600:
        raise C.StagingError(f"legacy.arrivals.settle_seconds must be an integer from max(60, the rate window "
                             f"{max(windows)} s) to 600")
    return dict(arr, settle_seconds=settle)


def _settings(rec: Mapping) -> Dict:
    s = rec["legacy"]
    out = {"parked_replicas": s.get("parked_replicas", 0), "job_deadline_seconds": s.get("job_deadline_seconds", 3600),
           "wait_timeout_seconds": s.get("wait_timeout_seconds", 600), "observe_seconds": s.get("observe_seconds", 120)}
    if not all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in out.values()):
        raise C.StagingError("legacy settings must be non-negative integers")
    if out["observe_seconds"] < 60:
        raise C.StagingError("legacy.observe_seconds must be at least 60 (a bounded continuous observation)")
    out["arrivals"] = _arrivals(s.get("arrivals"))
    return out


def _finite(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


class PromClient:
    """Instant queries against the Prometheus-compatible API (VictoriaMetrics vmselect). __call__: exactly one finite
    value whose evaluation timestamp is within max_skew_seconds of now, or None (no observation). vector(expr, label):
    {label value: finite value} for every series, each keyed by a distinct label value and evaluated now, or None."""

    def __init__(self, base: str, opener=None, timeout: float = 10.0, now: Callable = time.time,
                 max_skew_seconds: float = 30.0):
        import urllib.request
        self.base, self.timeout, self.now, self.max_skew = base.rstrip("/"), timeout, now, max_skew_seconds
        self.opener = opener or urllib.request.urlopen

    def _result(self, expr: str):
        import urllib.parse
        t = self.now()
        with self.opener(f"{self.base}/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": f"{t:.3f}"}),
                         timeout=self.timeout) as r:
            doc = json.loads(r.read(1 << 20))
        if doc.get("status") != "success" or doc["data"].get("resultType", "vector") != "vector":
            return None, t
        return doc["data"]["result"], t

    def _value(self, sample: Mapping, t: float) -> Optional[float]:
        ts, value = float(sample["value"][0]), float(sample["value"][1])
        if not (math.isfinite(ts) and math.isfinite(value)) or abs(ts - t) > self.max_skew:
            return None                          # not an observation of now
        return value

    def __call__(self, expr: str) -> Optional[float]:
        try:
            res, t = self._result(expr)
            if res is None or len(res) != 1:
                return None
            return self._value(res[0], t)
        except Exception:                        # unreachable, malformed or ambiguous: no observation
            return None

    def vector(self, expr: str, label: str) -> Optional[Dict[str, float]]:
        try:
            res, t = self._result(expr)
            if res is None:
                return None
            out: Dict[str, float] = {}
            for s in res:
                key = (s.get("metric") or {}).get(label)
                v = self._value(s, t)
                if not key or key in out or v is None:
                    return None                  # unlabelled, duplicated or invalid: ambiguous, no observation
                out[key] = v
            return out
        except Exception:
            return None


def arrival_problems(metrics, arrivals: Mapping, reporters: Mapping[str, List[str]]) -> List[str]:
    """Fresh, target-bound request-counter observations. For each target: its summed request rate must be observed
    (exactly one sample) and zero, its newest series no older than max_age_seconds, and EVERY expected reporter (the
    target's pods running when the observation started, `reporters[target]`) must have its own series no older than
    max_age_seconds (the newest series alone does not prove the others are fresh). A missing observation is a
    problem, never a zero; a target without an expected reporter cannot be observed and is a problem too."""
    out = []
    for t, pods in reporters.items():
        rate = metrics(arrivals["rate_query"].replace("{target}", t))
        age = metrics(arrivals["age_query"].replace("{target}", t))
        if not (_finite(rate) and _finite(age)):
            out.append(f"no fresh, finite request-counter observation for {t}")
            continue
        if not 0 <= age <= arrivals["max_age_seconds"]:
            out.append(f"the request counters of {t} are stale or from the future (age {age:g} s)")
            continue
        if rate != 0:
            out.append(f"{t} still receives requests or reports an invalid rate ({rate:g}/s)")
            continue
        if not pods:
            out.append(f"{t} has no running pod to report its requests (no expected reporter)")
            continue
        series = metrics.vector(arrivals["series_query"].replace("{target}", t), arrivals["pod_label"])
        if not isinstance(series, Mapping):
            out.append(f"no per-reporter request-counter observation for {t}")
            continue
        bad = sorted(p for p in pods
                     if not (_finite(series.get(p)) and 0 <= series.get(p) <= arrivals["max_age_seconds"]))
        if bad:
            out.append(f"the request series of {t}'s reporter pods {bad} are missing, stale or from the future")
    return out


def _archive_complete(rec: Mapping) -> Optional[str]:
    want = rec["legacy"].get("archive_receipt_sha256")
    receipts = C.read_receipts(C.receipts_path(rec, "archive"))
    done = [r["sha256"] for r in receipts if r["event"] == "archive-complete"]
    return want if want and want in done else None


def _ts(text: str) -> float:
    return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()


def _journal(rec: Mapping) -> List[Dict]:
    return C.read_receipts(C.receipts_path(rec, HELPER))       # the hash chain is verified on every read


def _step_numbers(receipts: List[Dict]) -> List[int]:
    """Every step number the journal has used: mutations, and the whole range of each executed plan."""
    out = [int(r["event"].split("-", 1)[1]) for r in receipts
           if r["event"].startswith(("intent-", "step-", "reconciled-"))]
    return out + [r["data"]["last_step"] for r in receipts
                  if r["event"] in ("quiesce-start", "park-start") and "last_step" in r["data"]]


def _next_step(rec: Mapping) -> int:
    """Step numbers continue across phases and attempts in the one journal, so every intent is unique."""
    return max(_step_numbers(_journal(rec)) or [0]) + 1


def _require_fresh_steps(rec: Mapping, plan: Mapping) -> None:
    used = _step_numbers(_journal(rec))
    if used and min(a["step"] for a in plan["actions"]) <= max(used):
        raise C.StagingError("the plan's step numbers are already recorded (an executed or stale plan); re-plan")


# --- phase A plan ----------------------------------------------------------------------------------------------------

def _jobs_spec() -> Dict:
    return dict(cronjobs=[n for _, _, n, _ in L.entries("launcher")], namespace="ml-engine", labels=["p8-run"])


def build_plan(api, rec: Mapping) -> Dict:
    """Phase A (quiesce): launchers and writers only, ending with the settle, the uninterrupted quiet observation and
    the capture of the stable target state. Targets are bound by UID only (no generation, no replicas)."""
    s = _settings(rec)
    inv = I.collect(api, rec)
    leg = inv["legacy"]
    refusals = list(leg["blockers"])
    if not _archive_complete(rec):
        refusals.append("no archive-complete receipt matching legacy.archive_receipt_sha256 (S0 must finish first)")
    if rec["legacy"].get("external_launchers_attested") is not True:
        refusals.append("legacy.external_launchers_attested must be true (detached generators/watchdogs stopped and "
                        "listed in legacy.external_launchers)")
    missing_retained = [m for m in leg["missing"] if m["role"] == "retained"]
    if missing_retained:
        refusals.append(f"retained evidence/model objects are missing: {missing_retained}; investigate before S4")
    if C.unresolved_intents(rec):
        refusals.append(f"unresolved intents {C.unresolved_intents(rec)}: run recover first")
    if refusals:
        raise C.StagingError("plan refused: " + "; ".join(refusals))
    actions: List[Dict] = []
    first = _next_step(rec)

    def add(op, kind=None, ns=None, name=None, **kw):
        a = {"step": first + len(actions), "op": op}
        if kind:
            o = I.find(inv, kind, ns, name)
            a.update(kind=kind, namespace=ns, name=name, uid=o["uid"], generation=o.get("generation"))
        a.update(kw)
        actions.append(a)

    def deployments(role):
        return [(ns, n, I.find(inv, "deployments", ns, n)) for _, ns, n, _r in L.entries(role)
                if I.find(inv, "deployments", ns, n)]

    lease = I.find(inv, "leases", "ml-engine", L.LEGACY_LEASE)
    jobs = _jobs_spec()
    for _, ns, n, _r in L.entries("launcher"):
        o = I.find(inv, "cronjobs", ns, n)
        if o:
            add("suspend", "cronjobs", ns, n, before=o["suspend"], after=True)
    add("wait_jobs", timeout_seconds=s["job_deadline_seconds"], **jobs)
    gens = deployments("generator")
    for ns, n, o in gens:
        add("scale", "deployments", ns, n, before=o["replicas"], after=0)
    add("wait_pods_gone", timeout_seconds=s["wait_timeout_seconds"],
        deployments=[[ns, n, o["uid"]] for ns, n, o in gens])
    add("recheck_consumers")
    ops = deployments("operator")
    for ns, n, o in ops:
        add("scale", "deployments", ns, n, before=o["replicas"], after=0)
    add("wait_pods_gone", timeout_seconds=s["wait_timeout_seconds"],
        deployments=[[ns, n, o["uid"]] for ns, n, o in ops])
    for _, ns, n, _r in L.entries("keda"):
        o = I.find(inv, "scaledobjects", ns, n)
        if o is None:
            continue
        hpa = next((h for h in inv["writers"]["horizontalpodautoscalers"] or []
                    if any(r.get("uid") == o["uid"] and r.get("controller") for r in h["ownerReferences"])), None)
        add("delete", "scaledobjects", ns, n, restore_to_original=o["restore_to_original"],
            timeout_seconds=s["wait_timeout_seconds"],
            generated_hpa=None if hpa is None else {"name": hpa["name"], "uid": hpa["uid"]})
    add("verify_no_writers", targets=[[TARGET_NS, a] for a in L.ARMS], timeout_seconds=s["wait_timeout_seconds"])
    add("settle", seconds=s["arrivals"]["settle_seconds"])
    stopped = [[ns, n, o["uid"]] for ns, n, o in gens + ops]
    targets = [[ns, n, o["uid"]] for ns, n, o in deployments("target")]          # UIDs only: no generation
    add("observe_quiet", seconds=s["observe_seconds"], stopped=stopped, lease=None if lease is None else
        {"uid": lease["uid"], "renew": lease.get("renew"), "holder": lease.get("holder")}, targets=targets, jobs=jobs)
    add("capture_targets", targets=targets)
    retained = [dict(m) for m in leg["found"] if m["role"] in ("retained", "pa", "lease")]
    skipped = [m for m in leg["missing"] if m["role"] not in ("retained",)]
    return C.make_plan(HELPER, rec, actions, phase="quiesce", inventory_sha256=C.digest(inv), retained=retained,
                       skipped=skipped, unclassified=leg["unclassified"], archive_receipt_sha256=_archive_complete(rec),
                       external_launchers=rec["legacy"].get("external_launchers", []))


def _guard(plan: Mapping, roles=None) -> None:
    """Defence in depth: a plan (even a reviewed one) may only touch mutable allowlisted objects, and a phase plan
    only the roles of its phase (phase A: launchers and writers; phase B: targets and the API)."""
    for a in plan["actions"]:
        if "kind" not in a:
            continue
        key = (a["kind"], a.get("namespace"), a.get("name"))
        if a["kind"] in NEVER or key not in ALLOWED:
            raise C.StagingError(f"the plan touches {a['kind']} {a.get('namespace')}/{a.get('name')}, which is not "
                                 f"a mutable legacy object; refusing")
        if roles is not None and ROLES[key] not in roles:
            raise C.StagingError(f"the plan touches {a['kind']} {a.get('namespace')}/{a.get('name')} (role "
                                 f"{ROLES[key]}), which this phase may not touch; refusing")


def _quiesce_shape(plan: Mapping, s: Mapping) -> None:
    """A phase A plan ends with verify_no_writers, the settle, the quiet observation and the capture, in that order."""
    ops = [a["op"] for a in plan["actions"]]
    if plan.get("phase") != "quiesce" or ops[-4:] != ["verify_no_writers", "settle", "observe_quiet",
                                                      "capture_targets"] or ops.count("observe_quiet") != 1:
        raise C.StagingError("not a phase A plan: it must end with verify_no_writers, settle, observe_quiet and "
                             "capture_targets")
    settle, obs = plan["actions"][-3], plan["actions"][-2]
    if settle["seconds"] < s["arrivals"]["settle_seconds"] or obs["seconds"] < 60:
        raise C.StagingError("the plan's settle or observation is shorter than the record requires")
    if any(len(t) != 3 for t in obs["targets"]) or plan["actions"][-1]["targets"] != obs["targets"]:
        raise C.StagingError("phase A binds the targets by UID only, the same in the observation and the capture")


# --- live reads -------------------------------------------------------------------------------------------------

def _bind(api, a: Mapping, generation: Optional[int] = None) -> Dict:
    o = api.get(C.path_of(a["kind"], a["namespace"], a["name"]))
    if o is None:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} disappeared; refusing")
    m = o.get("metadata") or {}
    if m.get("uid") != a["uid"]:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} has another UID than the plan; refusing")
    want = a.get("generation") if generation is None else generation
    if want is not None and m.get("generation") != want:
        raise C.StagingError(f"{a['kind']}/{a['namespace']}/{a['name']} changed (generation) since it was bound; "
                             f"refusing")
    return o


def _state(o: Mapping) -> Dict:
    m, sp = o.get("metadata") or {}, o.get("spec") or {}
    return {"generation": m.get("generation"), "replicas": sp.get("replicas"), "suspend": sp.get("suspend")}


def _owned_pod_objects(api, ns: str, deploy_uid: str) -> List[Dict]:
    rs = {r["metadata"]["uid"] for r in C.list_items(api, "replicasets", ns)
          if any(o.get("uid") == deploy_uid for o in r["metadata"].get("ownerReferences") or [])}
    return [p for p in C.list_items(api, "pods", ns)
            if any(o.get("uid") in rs for o in p["metadata"].get("ownerReferences") or [])]


def owned_pods(api, ns: str, deploy_uid: str) -> List[str]:
    """Pods (terminating ones included) of a Deployment, through its ReplicaSets' owner references."""
    return sorted(p["metadata"]["name"] for p in _owned_pod_objects(api, ns, deploy_uid))


def running_pods(api, ns: str, deploy_uid: str) -> List[str]:
    """The Deployment's pods that are Running and not terminating: the expected request reporters."""
    return sorted(p["metadata"]["name"] for p in _owned_pod_objects(api, ns, deploy_uid)
                  if (p.get("status") or {}).get("phase") == "Running" and not p["metadata"].get("deletionTimestamp"))


def _legacy_jobs(api, j: Mapping) -> List[Dict]:
    out = []
    for job in C.list_items(api, "jobs", j["namespace"]):
        m = job.get("metadata") or {}
        owned = any(r.get("kind") == "CronJob" and r.get("name") in j["cronjobs"]
                    for r in m.get("ownerReferences") or [])
        if owned or any(k in (m.get("labels") or {}) for k in j["labels"]):
            out.append(job)
    return out


def _active_jobs(api, j: Mapping) -> List[str]:
    return sorted(job["metadata"].get("name") for job in _legacy_jobs(api, j)
                  if (job.get("status") or {}).get("active", 0) and not (job.get("status") or {}).get("completionTime"))


def _writers_of_targets(api, targets) -> List[str]:
    keys = {(ns, n) for ns, n in targets}
    found = []
    for kind in ("horizontalpodautoscalers", "scaledobjects", "pa-new"):
        for w in C.list_items(api, kind):
            m, sp = w.get("metadata") or {}, w.get("spec") or {}
            t = sp.get("scaleTargetRef") or sp.get("targetDeployment") or {}
            if (t.get("namespace") or m.get("namespace"), t.get("name")) in keys:
                found.append(f"{kind} {m.get('namespace')}/{m.get('name')}")
    return found


def _lease(api) -> Optional[Dict]:
    o = api.get(C.path_of("leases", "ml-engine", L.LEGACY_LEASE))
    if o is None:
        return None
    sp = o.get("spec") or {}
    return {"uid": o["metadata"].get("uid"), "renew": sp.get("renewTime"), "holder": sp.get("holderIdentity")}


def _wait(cond: Callable[[], Optional[str]], timeout: int, sleep: Callable, monotonic: Callable, what: str) -> None:
    """Poll cond() (None when satisfied, else the reason) until timeout; fail closed."""
    end = monotonic() + timeout
    while True:
        why = cond()
        if why is None:
            return
        if monotonic() >= end:
            raise C.StagingError(f"{what} did not happen within {timeout} s: {why}")
        sleep(min(SAMPLE_SECONDS, max(0.1, end - monotonic())))


def _pause(seconds: int, sleep: Callable, monotonic: Callable) -> None:
    end = monotonic() + seconds
    while monotonic() < end:
        sleep(min(SAMPLE_SECONDS, max(0.1, end - monotonic())))


def _observe(sample: Callable[[], List[str]], seconds: int, sleep: Callable, monotonic: Callable, what: str) -> int:
    """Sample continuously for `seconds`; any problem in any sample fails (no averaging, no retry). Returns samples."""
    end, n = monotonic() + seconds, 0
    while True:
        problems = sample()
        n += 1
        if problems:
            raise C.StagingError(f"{what}: {'; '.join(problems)}")
        if monotonic() >= end:
            return n
        sleep(min(SAMPLE_SECONDS, max(0.1, end - monotonic())))


def _lease_problems(api, baseline_lease) -> List[str]:
    lease = _lease(api)
    if baseline_lease is None and lease is not None:
        return ["an old-operator leader Lease appeared (a live writer)"]       # observed-if-present, never new
    if lease != baseline_lease:
        return ["the old operator's Lease was renewed or changed"]
    return []


def _stopped_problems(api, rec: Mapping, stopped, jobs: Mapping, targets) -> List[str]:
    out = []
    for ns, n, uid in stopped:
        pods = owned_pods(api, ns, uid)
        if pods:
            out.append(f"{n} still has pods {pods}")
    active = _active_jobs(api, jobs)
    if active:
        out.append(f"active Jobs {active}")
    out += [f"writer {w}" for w in _writers_of_targets(api, targets)]
    out += I.live_blockers(api, rec)
    return out


def quiet_problems(api, rec: Mapping, obs: Mapping, baseline: Mapping, metrics) -> List[str]:
    """The shutdown signals at one instant against their baseline (request counters of every expected reporter
    included)."""
    out = arrival_problems(metrics, _settings(rec)["arrivals"], baseline["reporters"])
    out += _lease_problems(api, baseline.get("lease"))
    for ns, n, uid in obs["targets"]:
        o = api.get(C.path_of("deployments", ns, n))
        if o is None or o["metadata"].get("uid") != uid or _state(o) != baseline["targets"].get(n):
            out.append(f"target {n} changed (a writer or arrivals are still active)")
    return out + _stopped_problems(api, rec, obs["stopped"], obs["jobs"], [(ns, n) for ns, n, _ in obs["targets"]])


# --- mutations --------------------------------------------------------------------------------------------------

def _mutate(api, rec: Mapping, helper: str, lock, a: Mapping, clock, pre: Mapping,
            call: Callable[[Dict], tuple], extra=None) -> Dict:
    """Intent (with the VALIDATED pre-state) durably first, then the call, then the outcome with the post-state taken
    from the mutation's own response (never a later read)."""
    intent = {"action": a, "pre": _state(pre), **(extra or {})}
    return C.mutation_step(rec, helper, lock, a["step"], intent, lambda: call(pre), clock)


def _scale_call(api, a: Mapping, replicas: int):
    """PUT /scale guarded by the resourceVersion of the object just validated (a Deployment and its scale share it):
    any change after validation, even a status update, is a 409 and nothing is written."""
    def call(pre):
        m = pre["metadata"]
        before = (pre.get("spec") or {}).get("replicas")
        resp = api.replace(C.path_of("deployments", a["namespace"], a["name"], "scale"),
                           {"apiVersion": "autoscaling/v1", "kind": "Scale",
                            "metadata": {"name": a["name"], "namespace": a["namespace"], "uid": m["uid"],
                                         "resourceVersion": m["resourceVersion"]},
                            "spec": {"replicas": replicas}})
        got = ((resp or {}).get("spec") or {}).get("replicas")
        if got != replicas or ((resp or {}).get("metadata") or {}).get("uid") not in (None, m["uid"]):
            raise C.StagingError(f"the scale response of {a['namespace']}/{a['name']} does not confirm {replicas}")
        # The Scale response has no generation: a spec change raises it by exactly one.
        post = {"generation": m.get("generation") + (1 if before != replicas else 0), "replicas": got,
                "suspend": None, "resourceVersion": resp["metadata"].get("resourceVersion")}
        return {"from": before, "to": replicas}, post
    return call


def _suspend_call(api, a: Mapping, value: bool):
    def call(pre):
        before = bool((pre.get("spec") or {}).get("suspend"))
        pre["spec"]["suspend"] = value           # the validated object: its resourceVersion guards the update
        resp = api.replace(C.path_of("cronjobs", a["namespace"], a["name"]), pre)
        if ((resp or {}).get("spec") or {}).get("suspend") is not value:
            raise C.StagingError(f"the update response of {a['namespace']}/{a['name']} does not confirm suspend")
        return {"from": before, "to": value}, dict(_state(resp), resourceVersion=resp["metadata"].get(
            "resourceVersion"))
    return call


def _run_common(api, rec: Mapping, a: Mapping, lock, clock, sleep, monotonic) -> Optional[Dict]:
    """The steps both phases share; returns the mutation receipt, or None for a wait."""
    op = a["op"]
    if op == "suspend":
        return _mutate(api, rec, HELPER, lock, a, clock, _bind(api, a), _suspend_call(api, a, True))
    if op == "scale":
        return _mutate(api, rec, HELPER, lock, a, clock, _bind(api, a), _scale_call(api, a, a["after"]))
    if op == "wait_jobs":
        _wait(lambda: (lambda j: f"active Jobs {j}" if j else None)(_active_jobs(api, a)),
              a["timeout_seconds"], sleep, monotonic, "active Jobs finishing")
    elif op == "wait_pods_gone":
        def gone():
            left = {n: owned_pods(api, ns, uid) for ns, n, uid in a["deployments"]}
            left = {k: v for k, v in left.items() if v}
            return f"pods remain {left}" if left else None
        _wait(gone, a["timeout_seconds"], sleep, monotonic, "pod termination")
    else:
        raise C.StagingError(f"unknown plan action {op!r}")
    return None


# --- phase A: quiesce ---------------------------------------------------------------------------------------------

def quiesce(api, rec: Mapping, plan: Mapping, reviewed: str, sleep: Callable = time.sleep,
            monotonic: Callable = time.monotonic, clock=None, metrics=None) -> List[Dict]:
    s = _settings(rec)
    metrics = metrics or PromClient(s["arrivals"]["query_api"])
    C.require_reviewed(plan, reviewed, HELPER, rec)
    _guard(plan, QUIESCE_ROLES)
    _quiesce_shape(plan, s)
    if plan.get("archive_receipt_sha256") != _archive_complete(rec):
        raise C.StagingError("the archive-complete receipt no longer matches the plan; refusing")
    done, observed, captured = [], None, None
    with C.exclusive_lock(rec, LOCK_SCOPE, clock, holder="legacy quiesce") as lock:
        _require_fresh_steps(rec, plan)
        C.append_receipt(rec, HELPER, "quiesce-start", {"plan_sha256": reviewed,
                                                        "last_step": plan["actions"][-1]["step"]}, clock)
        for a in plan["actions"]:
            C.require_cluster(api, rec["cluster_uid"])
            op = a["op"]
            if op in ("suspend", "scale", "wait_jobs", "wait_pods_gone"):
                r = _run_common(api, rec, a, lock, clock, sleep, monotonic)
                if r is not None:
                    done.append(r)
            elif op == "delete":
                o = _bind(api, a)
                spath = os.path.join(rec["evidence_dir"], "legacy", "deleted",
                                     f"{a['kind']}-{a['namespace']}-{a['name']}-{a['uid']}.json")
                snap = {"path": spath, "sha256": C.write_json_once(spath, C.sanitize_object(o))}

                def call(pre, a=a):
                    api.delete(C.path_of(a["kind"], a["namespace"], a["name"]), a["uid"],
                               pre["metadata"].get("resourceVersion"))
                    return {"deleted": a["uid"]}, None
                done.append(_mutate(api, rec, HELPER, lock, a, clock, o, call, {"snapshot": snap}))
                hpa = a.get("generated_hpa")
                if hpa:
                    hp = C.path_of("horizontalpodautoscalers", a["namespace"], hpa["name"])
                    _wait(lambda hp=hp: None if api.get(hp) is None else "still present", a["timeout_seconds"],
                          sleep, monotonic, f"removal of the generated HPA {hpa['name']}")
            elif op == "recheck_consumers":
                blockers = I.live_blockers(api, rec)
                if blockers:
                    raise C.StagingError("consumers/reconcilers changed since the plan: " + "; ".join(blockers))
            elif op == "verify_no_writers":
                _wait(lambda a=a: (lambda w: None if not w else f"writers {w}")(_writers_of_targets(api, a["targets"])),
                      a["timeout_seconds"], sleep, monotonic, "no writer on the legacy targets")
            elif op == "settle":
                # The rate window still holds requests from before the stop: wait it out, read nothing.
                t0 = monotonic()
                _pause(a["seconds"], sleep, monotonic)
                C.append_receipt(rec, HELPER, f"settled-{a['step']}", {"seconds": a["seconds"],
                                                                       "waited": monotonic() - t0}, clock)
            elif op == "observe_quiet":
                observed = _observe_quiet(api, rec, a, metrics, sleep, monotonic, clock)
            elif op == "capture_targets":
                if observed is None:
                    raise C.StagingError("capture_targets needs the quiet observation of this execution; refusing")
                captured = _capture(api, rec, a, observed, clock)
            else:
                raise C.StagingError(f"unknown plan action {op!r}")
        C.append_receipt(rec, HELPER, "quiesce-complete", {
            "phase": "quiesce", "plan_sha256": reviewed, "steps": len(done),
            "observed_receipt_sha256": observed["receipt"]["sha256"], "observed_seconds": observed["seconds"],
            "samples": observed["samples"], "captured_receipt_sha256": captured["sha256"],
            "lease": observed["baseline"]["lease"], "targets": captured["data"]["targets"]}, clock)
    return done


def _observe_quiet(api, rec: Mapping, a: Mapping, metrics, sleep, monotonic, clock) -> Dict:
    lease = _lease(api)
    if a.get("lease") and lease and lease["uid"] != a["lease"]["uid"]:
        raise C.StagingError("the old operator's Lease was recreated; refusing")
    if a.get("lease") is not None and lease is None:
        raise C.StagingError("the old operator's Lease planned present has disappeared before the observation; "
                             "refusing")
    if a.get("lease") is None and lease is not None:
        raise C.StagingError("an old-operator leader Lease appeared since the plan (a live writer); refusing")
    targets, reporters = {}, {}
    for ns, n, uid in a["targets"]:
        o = api.get(C.path_of("deployments", ns, n))
        if o is None or o["metadata"].get("uid") != uid:
            raise C.StagingError(f"target {ns}/{n} disappeared or has another UID than the plan; refusing")
        targets[n] = _state(o)
        reporters[n] = running_pods(api, ns, uid)        # the expected reporters, fixed at the observation start
    baseline = {"lease": lease, "targets": targets, "reporters": reporters}
    n = _observe(lambda: quiet_problems(api, rec, a, baseline, metrics), a["seconds"], sleep, monotonic,
                 "the shutdown signals are not quiet")
    r = C.append_receipt(rec, HELPER, f"observed-{a['step']}", {
        "seconds": a["seconds"], "samples": n, "baseline": baseline,
        "arrivals": {k: _settings(rec)["arrivals"][k] for k in ("rate_query", "age_query", "series_query",
                                                                 "pod_label", "max_age_seconds", "settle_seconds")}},
        clock)
    return {"receipt": r, "baseline": baseline, "seconds": a["seconds"], "samples": n}


def _capture(api, rec: Mapping, a: Mapping, observed: Mapping, clock) -> Dict:
    """The stable target state after the writers are proven gone: each target exactly as it was throughout the
    observation (same UID, generation and replicas), with its resourceVersion."""
    out = []
    for ns, n, uid in a["targets"]:
        o = api.get(C.path_of("deployments", ns, n))
        if o is None or o["metadata"].get("uid") != uid or _state(o) != observed["baseline"]["targets"][n]:
            raise C.StagingError(f"target {ns}/{n} changed after the quiet observation; refusing to capture")
        m = o["metadata"]
        out.append({"namespace": ns, "name": n, "uid": uid, "generation": m.get("generation"),
                    "resourceVersion": m.get("resourceVersion"), "replicas": (o.get("spec") or {}).get("replicas")})
    return C.append_receipt(rec, HELPER, f"captured-{a['step']}", {
        "targets": out, "observed_receipt_sha256": observed["receipt"]["sha256"]}, clock)


# --- phase B: park ------------------------------------------------------------------------------------------------

def _phase_a(rec: Mapping, receipts: Optional[List[Dict]] = None) -> Dict:
    """Phase A's completion as the verified journal states it: the LATEST quiesce-complete of this run, its arrivals
    observation and its capture (each found by hash in the same chain), and whatever this journal recorded after it
    (the park steps). Refuses a foreign, broken or incomplete chain."""
    receipts = _journal(rec) if receipts is None else receipts       # read_receipts verifies the hash chain
    idx = [i for i, r in enumerate(receipts) if r["event"] == "quiesce-complete"]
    if not idx:
        raise C.StagingError("no phase A completion receipt (quiesce-complete): run plan and quiesce first")
    i = idx[-1]
    q = receipts[i]
    by_sha = {r["sha256"]: r for r in receipts[:i]}
    d = q.get("data") or {}
    obs, cap = by_sha.get(d.get("observed_receipt_sha256")), by_sha.get(d.get("captured_receipt_sha256"))
    if any(r.get("run_id") != rec["run_id"] or r.get("helper") != HELPER for r in receipts):
        raise C.StagingError("the legacy journal holds receipts of another run or helper; refusing")
    if d.get("phase") != "quiesce" or obs is None or not obs["event"].startswith("observed-") or cap is None or \
            not cap["event"].startswith("captured-") or cap["data"].get("targets") != d.get("targets") or \
            cap["data"].get("observed_receipt_sha256") != obs["sha256"]:
        raise C.StagingError("the phase A completion receipt does not bind its observation and capture; refusing")
    return {"complete": q, "observed": obs, "captured": cap, "after": receipts[i + 1:]}


def _restore_since(rec: Mapping, since: str) -> bool:
    return any(r["event"].startswith("intent-") and _ts(r["at"]) >= _ts(since)
               for r in C.read_receipts(C.receipts_path(rec, RESTORE)))


def _parked_by(after: List[Dict], target: Mapping, parked: int) -> Optional[Dict]:
    """The completed park step (recorded after this phase A) that left `target` at the parked count, if any."""
    for r in after:
        if r["event"].startswith("step-"):
            a = r["data"]["action"]
            if (a.get("op"), a.get("namespace"), a.get("name"), a.get("uid"), a.get("after")) == \
                    ("scale", target["namespace"], target["name"], target["uid"], parked):
                return r["data"]["post"]
    return None


def _quiesced_problems(api, rec: Mapping, lease) -> List[str]:
    """Phase A's post-conditions, live: launchers suspended, generators/operator at 0 without pods, benchmark
    ScaledObjects and their HPAs gone, no active Job, no writer, consumer or reconciler, the Lease unchanged."""
    out = []
    for _, ns, n, role in L.ALLOWLIST:
        if role == "launcher":
            o = api.get(C.path_of("cronjobs", ns, n))
            if o and not (o.get("spec") or {}).get("suspend"):
                out.append(f"CronJob {n} is not suspended")
        elif role in ("generator", "operator"):
            o = api.get(C.path_of("deployments", ns, n))
            if o and ((o.get("spec") or {}).get("replicas") or owned_pods(api, ns, o["metadata"]["uid"])):
                out.append(f"{ns}/{n} is not stopped")
        elif role == "keda" and api.get(C.path_of("scaledobjects", ns, n)) is not None:
            out.append(f"ScaledObject {n} exists")
    out += _lease_problems(api, lease)
    return out + _stopped_problems(api, rec, [], _jobs_spec(), [(TARGET_NS, a) for a in L.ARMS])


def build_park_plan(api, rec: Mapping) -> Dict:
    """Phase B (park), read-only: only from phase A's completion receipt (same run, verified chain, bound by hash);
    every target must still be exactly the captured state (UID and generation) or already parked by this park."""
    C.require_cluster(api, rec["cluster_uid"])
    s = _settings(rec)
    pa = _phase_a(rec)
    q = pa["complete"]
    refusals = []
    if any(r["event"] == "park-complete" for r in pa["after"]):
        refusals.append("phase B already completed for the latest phase A")
    if C.unresolved_intents(rec):
        refusals.append(f"unresolved intents {C.unresolved_intents(rec)}: run recover first")
    if _restore_since(rec, q["at"]):
        refusals.append("a restore ran after phase A: re-run phase A")
    if not _archive_complete(rec):
        refusals.append("no archive-complete receipt matching legacy.archive_receipt_sha256")
    refusals += _quiesced_problems(api, rec, q["data"]["lease"])
    plan_targets, already = [], []
    for t in q["data"]["targets"]:
        o = api.get(C.path_of("deployments", t["namespace"], t["name"]))
        m = (o or {}).get("metadata") or {}
        if o is None or m.get("uid") != t["uid"]:
            refusals.append(f"target {t['name']} disappeared or was recreated after phase A")
            continue
        post = _parked_by(pa["after"], t, s["parked_replicas"])
        if post and m.get("generation") == post["generation"] and o["spec"].get("replicas") == s["parked_replicas"]:
            already.append(t["name"])
            continue
        if m.get("generation") != t["generation"] or (o.get("spec") or {}).get("replicas") != t["replicas"]:
            refusals.append(f"target {t['name']} changed after phase A's captured state (generation "
                            f"{t['generation']} -> {m.get('generation')}): re-run phase A to capture a stable state")
            continue
        plan_targets.append(t)
    api_dep = [(ns, n, api.get(C.path_of("deployments", ns, n))) for _, ns, n, _r in L.entries("api")]
    api_dep = [(ns, n, o) for ns, n, o in api_dep if o]
    if refusals:
        raise C.StagingError("park plan refused: " + "; ".join(refusals))
    actions: List[Dict] = []
    first = _next_step(rec)

    def add(op, **kw):
        actions.append(dict({"step": first + len(actions), "op": op}, **kw))

    jobs = _jobs_spec()
    add("recheck_quiesced", targets=[[t["namespace"], t["name"], t["uid"]] for t in plan_targets],
        lease=q["data"]["lease"], jobs=jobs)
    for t in plan_targets:
        add("scale", kind="deployments", namespace=t["namespace"], name=t["name"], uid=t["uid"],
            generation=t["generation"], before=t["replicas"], after=s["parked_replicas"])
    if s["parked_replicas"] == 0 and plan_targets:
        add("wait_pods_gone", timeout_seconds=s["wait_timeout_seconds"],
            deployments=[[t["namespace"], t["name"], t["uid"]] for t in plan_targets])
    add("wait_jobs", timeout_seconds=s["job_deadline_seconds"], **jobs)
    for ns, n, o in api_dep:
        add("scale", kind="deployments", namespace=ns, name=n, uid=o["metadata"]["uid"],
            generation=o["metadata"].get("generation"), before=(o.get("spec") or {}).get("replicas"), after=0)
    add("wait_pods_gone", timeout_seconds=s["wait_timeout_seconds"],
        deployments=[[ns, n, o["metadata"]["uid"]] for ns, n, o in api_dep])
    return C.make_plan(PARK, rec, actions, phase="park", quiesce_receipt_sha256=q["sha256"],
                       quiesce_plan_sha256=q["data"]["plan_sha256"],
                       observed_receipt_sha256=pa["observed"]["sha256"],
                       captured_receipt_sha256=pa["captured"]["sha256"], captured_targets=q["data"]["targets"],
                       already_parked=already, archive_receipt_sha256=_archive_complete(rec))


def park(api, rec: Mapping, plan: Mapping, reviewed: str, sleep: Callable = time.sleep,
         monotonic: Callable = time.monotonic, clock=None, metrics=None) -> List[Dict]:
    s = _settings(rec)
    metrics = metrics or PromClient(s["arrivals"]["query_api"])
    C.require_reviewed(plan, reviewed, PARK, rec)
    _guard(plan, PARK_ROLES)
    if plan.get("phase") != "park" or plan.get("archive_receipt_sha256") != _archive_complete(rec):
        raise C.StagingError("not a park plan, or the archive-complete receipt no longer matches it; refusing")
    done = []
    with C.exclusive_lock(rec, LOCK_SCOPE, clock, holder="legacy park") as lock:
        pa = _phase_a(rec)
        if (pa["complete"]["sha256"], pa["observed"]["sha256"]) != (plan.get("quiesce_receipt_sha256"),
                                                                    plan.get("observed_receipt_sha256")):
            raise C.StagingError("the park plan is not bound to the latest phase A completion; refusing")
        if any(r["event"] == "park-complete" for r in pa["after"]) or _restore_since(rec, pa["complete"]["at"]):
            raise C.StagingError("phase B already completed, or a restore ran after phase A; refusing")
        _require_fresh_steps(rec, plan)
        C.append_receipt(rec, HELPER, "park-start", {"plan_sha256": reviewed, "last_step": plan["actions"][-1]["step"],
                                                     "quiesce_receipt_sha256": pa["complete"]["sha256"]}, clock)
        for a in plan["actions"]:
            C.require_cluster(api, rec["cluster_uid"])
            if a["op"] == "recheck_quiesced":
                problems = _quiesced_problems(api, rec, a["lease"])
                reporters = {}
                for ns, n, uid in a["targets"]:
                    o = api.get(C.path_of("deployments", ns, n))
                    if o is None or o["metadata"].get("uid") != uid:
                        problems.append(f"target {n} disappeared or was recreated")
                    else:
                        reporters[n] = running_pods(api, ns, uid)
                problems += arrival_problems(metrics, s["arrivals"], reporters)    # one fresh sample, pre-parking
                if problems:
                    raise C.StagingError("not quiesced right before parking: " + "; ".join(problems))
                C.append_receipt(rec, HELPER, f"rechecked-{a['step']}", {"reporters": reporters}, clock)
                continue
            r = _run_common(api, rec, a, lock, clock, sleep, monotonic)
            if r is not None:
                done.append(r)
        C.append_receipt(rec, HELPER, "park-complete", {
            "phase": "park", "plan_sha256": reviewed, "steps": len(done),
            "quiesce_receipt_sha256": pa["complete"]["sha256"], "observed_receipt_sha256": pa["observed"]["sha256"],
            "targets": [[t["namespace"], t["name"], t["uid"]] for t in pa["complete"]["data"]["targets"]]}, clock)
    return done


# --- verify --------------------------------------------------------------------------------------------------------

def _snapshot_problems(api, rec: Mapping, s: Mapping, uids: Optional[Mapping] = None) -> (List[str], Dict):
    problems, state = [], {}
    for _, ns, n, role in L.ALLOWLIST:
        if role == "launcher":
            o = api.get(C.path_of("cronjobs", ns, n))
            if o and not (o.get("spec") or {}).get("suspend"):
                problems.append(f"CronJob {n} is not suspended")
        elif role in ("generator", "operator", "api", "target"):
            o = api.get(C.path_of("deployments", ns, n))
            if o:
                want = s["parked_replicas"] if role == "target" else 0
                if (o.get("spec") or {}).get("replicas") != want:
                    problems.append(f"Deployment {ns}/{n} is not at {want} replicas")
                pods = owned_pods(api, ns, o["metadata"]["uid"])
                if want == 0 and pods:
                    problems.append(f"{n} still has pods {pods} (terminating ones included)")
                state[f"{ns}/{n}"] = dict(_state(o), uid=o["metadata"]["uid"])
            if role == "target" and uids is not None and (o or {}).get("metadata", {}).get("uid") != uids.get(n):
                problems.append(f"target {ns}/{n} is not the Deployment phase A captured (missing or recreated)")
        elif role == "keda":
            if api.get(C.path_of("scaledobjects", ns, n)) is not None:
                problems.append(f"ScaledObject {n} still exists")
        elif role == "retained":
            kind = "customresourcedefinitions" if ns is None else "persistentvolumeclaims"
            o = api.get(C.path_of(kind, ns, n))
            if o is None:
                problems.append(f"retained {kind} {n} is missing")
            elif kind == "persistentvolumeclaims" and (o.get("status") or {}).get("phase") != "Bound":
                problems.append(f"retained claim {n} is not Bound")
    for h in C.list_items(api, "horizontalpodautoscalers", TARGET_NS):
        if any(r.get("kind") == "ScaledObject" and r.get("name") in L.KEDA_OBJECTS
               for r in h["metadata"].get("ownerReferences") or []):
            problems.append(f"generated HPA {h['metadata']['name']} still exists")
    problems += [f"active Job {j}" for j in _active_jobs(api, _jobs_spec())]
    problems += [f"writer remains: {w}" for w in _writers_of_targets(api, [(TARGET_NS, a) for a in L.ARMS])]
    problems += I.live_blockers(api, rec)
    state["lease"] = _lease(api)
    return problems, state


def _retirement_chain(rec: Mapping) -> Dict:
    """Phase A's arrivals proof and the park bound to it, from the verified journal (empty fields when absent)."""
    try:
        pa = _phase_a(rec)
    except C.StagingError as e:
        return {"problem": str(e)}
    parks = [r for r in pa["after"] if r["event"] == "park-complete"]
    p = parks[-1] if parks else None
    if p and (p["data"].get("quiesce_receipt_sha256"), p["data"].get("observed_receipt_sha256")) != \
            (pa["complete"]["sha256"], pa["observed"]["sha256"]):
        return {"problem": "the park-complete receipt is not bound to the latest phase A"}
    return {"quiesce_receipt_sha256": pa["complete"]["sha256"], "observed_receipt_sha256": pa["observed"]["sha256"],
            "observed_seconds": pa["observed"]["data"]["seconds"], "park_receipt_sha256": p and p["sha256"],
            "uids": {t["name"]: t["uid"] for t in pa["complete"]["data"]["targets"]}}


def verify(api, rec: Mapping, observe_seconds: int = 0, sleep: Callable = time.sleep,
           monotonic: Callable = time.monotonic, clock=None) -> Dict:
    """Retirement = phase A's preserved pre-parking arrivals proof + this post-parking control-state observation,
    sampled continuously for observe_seconds: every sample clean and identical (targets parked without pods and with
    their captured UIDs, no pod of a stopped workload, launchers suspended and no Job of theirs appearing or running,
    no writer, consumer or reconciler, the Lease neither renewed nor created, nothing rescaled or recreated). No
    metric is read after parking: the reporters are gone, and an absent series is never a zero. Retired only after a
    completed phase A with >= QUIET_SECONDS of arrivals observation, a park bound to it, and >= QUIET_SECONDS here."""
    C.require_cluster(api, rec["cluster_uid"])
    s = _settings(rec)
    chain = _retirement_chain(rec)
    first_problems, baseline = _snapshot_problems(api, rec, s, chain.get("uids"))
    jobs0 = {j["metadata"].get("uid") for j in _legacy_jobs(api, _jobs_spec())}
    problems, samples = list(first_problems), 1
    if not problems and observe_seconds > 0:
        def sample():
            p, st = _snapshot_problems(api, rec, s, chain.get("uids"))
            new = sorted(j["metadata"].get("name") for j in _legacy_jobs(api, _jobs_spec())
                         if j["metadata"].get("uid") not in jobs0)
            if new:
                p.append(f"legacy Jobs appeared {new} (a launcher or reconciler is active)")
            if baseline.get("lease") is None and st.get("lease") is not None:
                p.append("an old-operator leader Lease appeared (a live writer)")
            return p + (["the state changed during the observation (Lease renewal, a scale or a recreated object)"]
                        if st != baseline else [])
        try:
            samples = _observe(sample, observe_seconds, sleep, monotonic, "not quiet")
        except C.StagingError as e:
            problems.append(str(e))
    parked = bool(chain.get("park_receipt_sha256"))
    proof = parked and chain["observed_seconds"] >= QUIET_SECONDS
    result = {"problems": problems, "state": baseline, "observed_seconds": observe_seconds if not problems else 0,
              "samples": samples, "phase_a": chain.get("quiesce_receipt_sha256"), "parked": parked,
              "chain_problem": chain.get("problem"),
              "arrivals_proof": None if not parked else {
                  "observed_receipt_sha256": chain["observed_receipt_sha256"],
                  "observed_seconds": chain["observed_seconds"], "park_receipt_sha256": chain["park_receipt_sha256"]},
              "metrics_after_parking": "not read (the reporters are gone; an absent series is never a zero)",
              "quiesced": parked and not problems,
              "retired": bool(proof and not problems and observe_seconds >= QUIET_SECONDS)}
    C.append_receipt(rec, HELPER, "verify", result, clock)
    return result


# --- restore -------------------------------------------------------------------------------------------------------

def _observe_outcome(api, d: Mapping) -> Dict:
    """The live outcome of an intent whose call was interrupted: {"observed": "done"|"not-done"|"unresolved", ...}.
    Only an unambiguous observation decides: a recreation is done only if the object carries this operation's identity
    annotation; an update only if the generation advanced by exactly one with the intended value (or did not move at all
    for not-done); a deletion only if the object is gone. Anything else (drift, another owner's object) is
    unresolved."""
    a = d["action"]
    o = api.get(C.path_of(a["kind"], a["namespace"], a["name"]))
    base = {"action": a, "pre": d.get("pre"), "post": None}
    if a["op"] == "delete":
        if o is None:
            return dict(base, observed="done", result={"deleted": a["uid"]}, snapshot=d.get("snapshot"))
        if o["metadata"].get("uid") == a["uid"]:
            return dict(base, observed="not-done")
        return dict(base, observed="unresolved", why="a same-named object with another UID exists")
    if a["op"] == "recreate":
        if o is None:
            return dict(base, observed="not-done")
        if ((o["metadata"].get("annotations") or {}).get(OP_ANNOTATION)) == d.get("operation"):
            return dict(base, observed="done", result={"recreated_uid": o["metadata"].get("uid")})
        return dict(base, observed="unresolved", why="a same-named object without this operation's identity exists")
    if o is None or o["metadata"].get("uid") != a["uid"]:
        return dict(base, observed="unresolved", why="the object vanished or was recreated")
    st, key = _state(o), ("replicas" if a["op"] == "scale" else "suspend")
    pre, target = d["pre"], a["after"]
    if pre[key] == target and st[key] == target and st["generation"] == pre["generation"]:
        return dict(base, observed="done", result={"from": pre[key], "to": target}, post=st)     # a no-op
    if st[key] == target and st["generation"] == pre["generation"] + 1:
        return dict(base, observed="done", result={"from": pre[key], "to": target}, post=st)
    if st[key] == pre[key] and st["generation"] == pre["generation"]:
        return dict(base, observed="not-done", post=st)
    return dict(base, observed="unresolved", why="the object changed in another way (drift)", post=st)


def _outcomes(receipts: List[Dict]) -> (Dict[str, Dict], List[str]):
    """{step: outcome data} of completed or reconciled-done steps, and the intents still unresolved."""
    out, ambiguous = {}, []
    for r in receipts:
        ev = r["event"]
        if ev.startswith("step-"):
            out[ev[5:]] = r["data"]
        elif ev.startswith("reconciled-") and r["data"]["observed"] == "done":
            out[ev[11:]] = r["data"]
    resolved = set(out) | {r["event"][11:] for r in receipts if r["event"].startswith("reconciled-")}
    ambiguous = [r["event"][7:] for r in receipts
                 if r["event"].startswith("intent-") and r["event"][7:] not in resolved]
    return out, ambiguous


def recover(api, rec: Mapping, lock_sha: str, clock=None) -> Dict:
    """The recovery procedure for an ambiguous outcome. With the exact lock file's sha256 it takes the recovery guard
    (one recoverer; the lock's executor proven terminated: same host and boot, PID gone or reused), observes every
    unresolved intent of quiesce, park and restore against the live object and records decided outcomes as
    reconciled-<n>. The stale lock is released only when every intent is decided; an unresolved one keeps it (fail closed)."""
    with C.recovery_guard(rec, LOCK_SCOPE, lock_sha) as guard:
        C.require_cluster(api, rec["cluster_uid"])
        report, unresolved = {}, {}
        for helper in (HELPER, RESTORE):
            receipts = C.read_receipts(C.receipts_path(rec, helper))
            _, ambiguous = _outcomes(receipts)
            intents = {r["event"][7:]: r["data"] for r in receipts if r["event"].startswith("intent-")}
            for n in ambiguous:
                obs = _observe_outcome(api, intents[n])
                if obs["observed"] == "unresolved":
                    unresolved[f"{helper}/{n}"] = obs["why"]
                    continue
                C.append_receipt(rec, helper, f"reconciled-{n}", obs, clock)
                report[f"{helper}/{n}"] = obs["observed"]
        if unresolved:
            C.append_receipt(rec, HELPER, "recovery-unresolved", {"lock_sha256": lock_sha, "decided": report,
                                                                  "unresolved": unresolved}, clock)
            raise C.StagingError(f"unresolved outcomes {unresolved}: the lock stays; reconcile them by hand")
        guard.release_stale()
        C.append_receipt(rec, HELPER, "lock-recovered", {"lock_sha256": lock_sha, "reconciled": report}, clock)
    return report


def build_restore_plan(api, rec: Mapping) -> Dict:
    """The exact undo of the recorded steps of BOTH phases (one journal: park steps after quiesce steps) not yet
    undone, newest first, each bound to its post-quiesce/post-park state. Read-only; refuses while an intent is
    unresolved (run recover first)."""
    C.require_cluster(api, rec["cluster_uid"])
    receipts = C.read_receipts(C.receipts_path(rec, HELPER))
    outcomes, ambiguous = _outcomes(receipts)
    rreceipts = C.read_receipts(C.receipts_path(rec, RESTORE))
    routcomes, rambiguous = _outcomes(rreceipts)
    if ambiguous or rambiguous:
        raise C.StagingError(f"unresolved intents (quiesce {ambiguous}, restore {rambiguous}): run legacy.py "
                             f"recover first")
    undone = {d["action"].get("undoes") for d in routcomes.values()}
    order = [r["event"][7:] for r in receipts if r["event"].startswith("intent-")]
    actions = []
    for n in reversed(order):
        d = outcomes.get(n)
        if d is None or n in undone:
            continue
        a, res, post = d["action"], d["result"], d.get("post")
        base = {k: a[k] for k in ("kind", "namespace", "name", "uid")}
        base["undoes"] = n
        if a["op"] == "scale":
            actions.append(dict(base, op="scale", after=res["from"], expect={"replicas": res["to"],
                                                                             "generation": post["generation"]}))
        elif a["op"] == "suspend":
            actions.append(dict(base, op="suspend", after=res["from"], expect={"suspend": True,
                                                                               "generation": post["generation"]}))
        elif a["op"] == "delete":
            actions.append(dict(base, op="recreate", snapshot=d["snapshot"]))
    if not actions:
        raise C.StagingError("no recorded quiesce or park step left to restore")
    roles = {(k, ns, n): role for k, ns, n, role in L.ALLOWLIST}
    ordered, checked_writers, checked_ready = [], False, False
    for a in actions:
        role = roles.get((a["kind"], a["namespace"], a["name"]))
        if role in ("keda", "operator") and not checked_writers:
            ordered.append({"op": "check_no_foreign_writer", "targets": [["demo", t] for t in L.ARMS]})
            checked_writers = True
        if role == "generator" and not checked_ready:
            ordered.append({"op": "check_ready_single_writer", "targets": [["demo", t] for t in L.ARMS]})
            checked_ready = True
        ordered.append(a)
        if role == "api" and a["op"] == "scale" and a["after"]:
            ordered.append({"op": "wait_ready", "deployments": [[a["namespace"], a["name"]]]})
    first = (max(int(k) for k in [r["event"].split("-", 1)[1] for r in rreceipts
                                  if r["event"].startswith(("intent-", "step-", "reconciled-"))] or [0]) + 1)
    for i, a in enumerate(ordered, first):
        a["step"] = i
    return C.make_plan(RESTORE, rec, ordered, quiesce_receipts=[r["sha256"] for r in receipts],
                       restore_receipts=[r["sha256"] for r in rreceipts])


def _recreate_body(snapshot: Mapping) -> Dict:
    m = snapshot.get("metadata") or {}
    meta = {k: m[k] for k in ("name", "namespace", "labels", "annotations") if k in m}
    body = {"apiVersion": snapshot.get("apiVersion"), "kind": snapshot.get("kind"), "metadata": meta,
            "spec": snapshot.get("spec")}
    if C.REDACTED in json.dumps(body):
        raise C.StagingError("the snapshot has redacted fields; recreate it from the protected backup instead")
    return body


def writer_census(api) -> Dict[str, List[str]]:
    """Active writers per legacy target: legacy PAs while the old operator runs, ScaledObjects that are not paused,
    any new-group PA or unowned HPA (generated HPAs count as their ScaledObject)."""
    op = api.get(C.path_of("deployments", "ml-engine", "predictive-operator"))
    operator_on = bool(op and (op.get("spec") or {}).get("replicas"))
    out = {t: [] for t in L.ARMS}
    for p in C.list_items(api, "pa-legacy"):
        t = (p.get("spec") or {}).get("targetDeployment") or {}
        if operator_on and t.get("name") in out and (t.get("namespace") or p["metadata"]["namespace"]) == "demo":
            out[t["name"]].append(f"legacy PA {p['metadata']['name']}")
    # Only ScaledObjects that are counted (not paused) absorb their generated HPA: a paused one does not prove that
    # its HPA stopped writing, so that HPA counts on its own.
    sos = {o["metadata"]["uid"]: o for o in C.list_items(api, "scaledobjects", "demo")
           if (o["metadata"].get("annotations") or {}).get("autoscaling.keda.sh/paused") != "true"}
    for kind in ("scaledobjects", "pa-new", "horizontalpodautoscalers"):
        for w in C.list_items(api, kind, "demo"):
            m, sp = w["metadata"], w.get("spec") or {}
            t = (sp.get("scaleTargetRef") or sp.get("targetDeployment") or {}).get("name")
            if t not in out:
                continue
            if kind == "scaledobjects" and (m.get("annotations") or {}).get("autoscaling.keda.sh/paused") == "true":
                continue
            if kind == "horizontalpodautoscalers" and _verified_generated(w, sos):
                continue                         # the same writer as its (existing, same-UID, same-target) owner
            out[t].append(f"{kind} {m['name']}")
    return out


def _verified_generated(hpa: Mapping, sos: Mapping) -> bool:
    """An HPA is a ScaledObject's generated HPA only if its controller owner exists with that UID and name and both
    target the same Deployment; anything else (an orphan, a stale owner reference) is an independent writer."""
    owner = next((r for r in hpa["metadata"].get("ownerReferences") or [] if r.get("controller")), None)
    so = sos.get((owner or {}).get("uid")) if owner and owner.get("kind") == "ScaledObject" else None
    if so is None or so["metadata"].get("name") != owner.get("name"):
        return False
    return ((so.get("spec") or {}).get("scaleTargetRef") or {}).get("name") == \
        ((hpa.get("spec") or {}).get("scaleTargetRef") or {}).get("name")


def restore(api, rec: Mapping, plan: Mapping, reviewed: str, sleep: Callable = time.sleep,
            monotonic: Callable = time.monotonic, clock=None) -> List[Dict]:
    C.require_reviewed(plan, reviewed, RESTORE, rec)
    _guard(plan)
    s, done = _settings(rec), []
    with C.exclusive_lock(rec, LOCK_SCOPE, clock, holder="legacy restore") as lock:
        for a in plan["actions"]:
            C.require_cluster(api, rec["cluster_uid"])
            if a["op"] in ("scale", "suspend"):
                o = _bind(api, a, generation=a["expect"]["generation"])        # drift since quiesce: refused
                key = "replicas" if a["op"] == "scale" else "suspend"
                if _state(o)[key] != a["expect"][key]:
                    raise C.StagingError(f"{a['namespace']}/{a['name']} drifted from its post-quiesce state; refusing")
                call = _scale_call(api, a, a["after"]) if a["op"] == "scale" else _suspend_call(api, a, a["after"])
                done.append(_mutate(api, rec, RESTORE, lock, a, clock, o, call))   # PUT guarded by o's version
                continue
            elif a["op"] == "recreate":
                snap = a["snapshot"]
                if C.sha256_file(snap["path"]) != snap["sha256"]:
                    raise C.StagingError(f"the snapshot {snap['path']} changed; refusing")
                path = C.path_of(a["kind"], a["namespace"], a["name"])
                if api.get(path) is not None:
                    raise C.StagingError(f"{a['kind']} {a['namespace']}/{a['name']} exists; never overwritten")
                with open(snap["path"]) as fh:
                    body = _recreate_body(json.load(fh))
                operation = f"{rec['run_id']}-{reviewed[:12]}-{a['step']}-{os.urandom(4).hex()}"
                body["metadata"].setdefault("annotations", {})[OP_ANNOTATION] = operation

                def create(body=body, a=a):
                    created = api.create(C.path_of(a["kind"], a["namespace"]), body)
                    m = created.get("metadata") or {}
                    return {"recreated_uid": m.get("uid")}, {"uid": m.get("uid"), "operation": operation}
                done.append(C.mutation_step(rec, RESTORE, lock, a["step"], {"action": a, "operation": operation},
                                            create, clock))
                continue
            elif a["op"] == "check_no_foreign_writer":
                legacy_names = {f"legacy PA {n}" for _, _, n, r in L.entries("pa")} | \
                    {f"scaledobjects {n}" for n in L.KEDA_OBJECTS}
                foreign = {t: [x for x in w if x not in legacy_names] for t, w in writer_census(api).items()}
                foreign = {t: w for t, w in foreign.items() if w}
                if foreign:
                    raise C.StagingError(f"legacy targets already have writers {foreign}; refusing to add another")
                res = {"foreign": {}}
            elif a["op"] == "check_ready_single_writer":
                def ready_single(a=a):
                    census, bad = writer_census(api), []
                    for ns, t in a["targets"]:
                        d = api.get(C.path_of("deployments", ns, t))
                        if d and (d.get("status") or {}).get("readyReplicas", 0) < (d.get("spec") or {}).get(
                                "replicas", 0):
                            bad.append(f"{t} not ready")
                        if len(census.get(t, [])) > 1:
                            bad.append(f"{t} has writers {census[t]}")
                    return "; ".join(bad) or None
                _wait(ready_single, s["wait_timeout_seconds"], sleep, monotonic, "ready targets with one writer each")
                res = {"census": writer_census(api)}
            elif a["op"] == "wait_ready":
                def ready(a=a):
                    for ns, n in a["deployments"]:
                        d = api.get(C.path_of("deployments", ns, n)) or {}
                        if (d.get("status") or {}).get("readyReplicas", 0) < (d.get("spec") or {}).get("replicas", 0):
                            return f"{n} not ready"
                    return None
                _wait(ready, s["wait_timeout_seconds"], sleep, monotonic, "readiness")
                res = {}
            else:
                raise C.StagingError(f"unknown restore action {a['op']!r}")
            done.append(C.append_receipt(rec, RESTORE, f"step-{a['step']}", {"action": a, "result": res}, clock))
            lock.safe = True
        C.append_receipt(rec, RESTORE, "restore-complete", {"plan_sha256": reviewed, "steps": len(done)}, clock)
    return done


# --- CLI ------------------------------------------------------------------------------------------------------------

def main(argv=None, api=None, sleep=time.sleep, monotonic=time.monotonic, clock=None) -> int:
    ap = argparse.ArgumentParser(prog="legacy.py", description=__doc__.splitlines()[0])
    C.add_common_args(ap)
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("plan")
    sub.add_parser("park-plan")
    for name in ("quiesce", "park", "restore"):
        p = sub.add_parser(name)
        p.add_argument("--plan", required=True)
        p.add_argument("--plan-digest", required=True)
    rc = sub.add_parser("recover")
    rc.add_argument("--lock-sha256", required=True)
    v = sub.add_parser("verify")
    v.add_argument("--observe-seconds", type=int, default=QUIET_SECONDS)
    sub.add_parser("restore-plan")
    a = ap.parse_args(argv)
    cmd = a.cmd or "plan"
    try:
        rec = C.load_record(a.record, ("legacy",))
        api = api or C.api_for(rec)
        if cmd in ("plan", "park-plan", "restore-plan"):
            plan = {"plan": build_plan, "park-plan": build_park_plan, "restore-plan": build_restore_plan}[cmd](api, rec)
            path = os.path.join(rec["evidence_dir"], "legacy", f"{cmd}-{C.digest(plan)[:12]}.json")
            C.write_json_once(path, plan)
            print(f"{cmd}: {path}\nplan digest (review, then pass --plan-digest): {C.plan_digest(plan)}")
        elif cmd == "quiesce":
            quiesce(api, rec, C.load_plan(a.plan), a.plan_digest, sleep, monotonic, clock)
            print("phase A (quiesce) complete; run park-plan, review it, then park")
        elif cmd == "park":
            park(api, rec, C.load_plan(a.plan), a.plan_digest, sleep, monotonic, clock)
            print("phase B (park) complete; run verify (600 s of continuous observation) to declare retirement")
        elif cmd == "recover":
            print(json.dumps(recover(api, rec, a.lock_sha256, clock), indent=1, sort_keys=True))
        elif cmd == "restore":
            restore(api, rec, C.load_plan(a.plan), a.plan_digest, sleep, monotonic, clock)
            print("restore complete")
        else:
            print(json.dumps(verify(api, rec, a.observe_seconds, sleep, monotonic, clock), indent=1, sort_keys=True))
    except C.StagingError as e:
        print(f"legacy: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
