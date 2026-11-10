"""The demonstration stage: "predictive scaling on live synthetic diurnal traffic" (LIVE-DEMO-PLAN sections 5-7).

A real stage of the tools with no experiment-only field and no dummy value. It renders, under its OWN run id:

  * the common objects (PriorityClass, scoped quota, ingress, the two routes, their EMPTY fault schedules, route
    NetworkPolicies, the scrape object), with the ingress routing only to the stage's two targets;
  * two fixture targets with ONE startup profile, each rendered exactly as the comparison arms are (render.
    target_objects: the calibrated fixture resources, probes and placement), at a fixed shadow replica count;
  * two PredictiveAutoscalers in Recommend (render.pa_object, the arms' own rendering): the forecast path
    (prediction.families from the params) and the reactive reference, the same object with the predictive extra an
    explicit zero, exactly as the PA-R arm is rendered;
  * ONE continuous generator (a Deployment with one replica and strategy Recreate, the reviewed k6 image by digest, the
    script load/c28-demo.js and its configuration from a ConfigMap) on the support node, feeding both targets the same
    ordinary diurnal program from ONE immutable schedule origin.

What the stage consumes is render.STAGE_FIELDS["demonstration"]; it never reads c0_rps, hpa_cpu_target_percent or
keda.* (render.EXPERIMENT_ONLY): its per-replica capacity is its own reviewed field, labelled provisional and naming
the calibration it came from.

The reservation is NOT the demonstration's: the workload and support nodes stay reserved by another run (its taint
value is that run's id). The params carry an explicit reference to that reservation (owner run id, taint value, both
node names and UIDs); nodes are never retagged, and staging/prepare.py checks the reference against the owner's
reservation record and its live verdict.

Nothing here is the Active transition: every PredictiveAutoscaler is rendered in Recommend, and the release overlay
that prepare.py writes withholds the scale write. active_values_problems() only VALIDATES the Active configuration's
chart values when another tool presents them.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Dict, List, Optional, Tuple

from . import frozen as F
from . import render as R
from . import schedule

STAGE = "demonstration"
DEMO_ROLE_LABEL = "c28.devkuban.com/demo-role"
COMPONENT_LABEL = "app.kubernetes.io/component"
ROLES = ("forecast", "reactive")
# The comparison arm each role is rendered as (render.pa_object / render._labels): the forecast path as PA-N, the
# reactive reference as PA-R (predictive extra zero).
ROLE_ARM = {"forecast": "PA-N", "reactive": "PA-R"}
PROFILE = "s20"                                   # ONE startup profile (LIVE-DEMO-PLAN section 5)
FAMILIES = ("seasonalNaive",)                     # the only families the demonstration qualification would admit
CADENCE = {"grid_seconds": 60, "update_interval_seconds": 60}
BOUNDS = {"min": F.MIN_REPLICAS, "max": F.MAX_REPLICAS}
CAPACITY_LABEL = "provisional"
PEAK_MULTIPLE = max(u for _, u in F.DAILY_SHAPE)  # 9: the ordinary cycle's peak in units of c

# The already reviewed generator image (c28-images/v1 "generator", build attempt r2): k6 0.57.0 by digest. The
# repository may be a mirror; the digest may not differ.
GENERATOR_DIGEST = "sha256:70af91f86cd8e142e0544a4edaf79835a80033f71974b92edd5ac36fd4442a7b"
GENERATOR_K6_VERSION = "0.57.0"
GENERATOR_NAME = "c28-demo-generator"
GENERATOR_SCRIPT = "c28-demo.js"
GENERATOR_CONFIG = "config.json"
GENERATOR_CONFIG_SCHEMA = "c28-demo-generator/v1"
GENERATOR_USER = 12345                            # the k6 image's user
K6_OUTPUT = "experimental-prometheus-rw"          # as staging/session.py's generator Job
SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "load", GENERATOR_SCRIPT)

# Generator behaviour, fixed here and written into its configuration (so the record says what ran).
MAX_LATE_MS = 1_000          # an arrival more than this late is DROPPED (reason "late"), never replayed in a burst
FLUSH_GRACE_MS = 500         # a minute's line is written this long after its last request can have timed out
MIN_SEGMENT_MS = 300_000     # an incarnation starting closer than this to its segment boundary runs to the next one
LATENCY_BOUNDS_MS = (5, 10, 25, 50, 100, 200, 300, 500, 750, 1000, 1500, 2000)
SEGMENT_HOURS = tuple(24 * n for n in range(1, 29))      # whole days, so every planned restart falls on a UTC
#                                                          midnight of the origin: the ordinary cycle's lowest demand
# R2-18 arithmetic (generator_sizing): assumptions for the memory ESTIMATE, to be replaced by the measured values of the
# kind smoke's full mode. They are deliberately generous; nothing was measured when they were written.
ASSUMED_BASE_BYTES = 128 * 2**20         # the k6 process with its outputs, before any VU
ASSUMED_VU_BYTES = 16 * 2**20            # one JavaScript VU (the script, its parsed configuration, its runtime)
ASSUMED_INFLIGHT_BYTES = 128 * 2**10     # one request in flight (goroutine, connection buffers, promise)
SETUP_VUS = 1                            # k6 runs setup() in its own VU

_DNS = re.compile(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_CALIBRATION = re.compile(r"c28cal-[0-9a-f]{10}")
_IMAGE = re.compile(r"([a-z0-9][a-z0-9./:-]*)@(sha256:[0-9a-f]{64})")
_ORIGIN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00Z")
_REMOTE_WRITE = re.compile(r"https?://[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:[0-9]{1,5})?(/[A-Za-z0-9._~/-]*)?")

BLOCK_KEYS = ("run_id", "reservation_ref", "targets", "profile", "shadow_replicas", "bounds",
              "capacity_per_replica_rps", "capacity_label", "capacity_source_calibration", "cadence",
              "schedule_origin_utc", "peak_multiple", "program", "generator")
GENERATOR_KEYS = ("image", "k6_version", "resources", "segment_hours", "remote_write_url")
RESOURCE_KEYS = ("cpu_request", "cpu_limit", "memory_request", "memory_limit")


# --- the params stage -------------------------------------------------------------------------------------------

def _keys(d, want: Tuple[str, ...], where: str, out: List[str]) -> bool:
    """`d` is an object with exactly the keys `want` (an unknown key is refused: the stage has no free-form field)."""
    if not isinstance(d, dict):
        out.append(f"{where} must be an object with {', '.join(want)}")
        return False
    missing, extra = [k for k in want if k not in d], sorted(k for k in d if k not in want)
    if missing:
        out.append(f"{where} is missing {', '.join(missing)}")
    if extra:
        out.append(f"{where} has unknown fields {', '.join(extra)}")
    return not missing


def _int(v, lo: int, hi: int) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and lo <= v <= hi


def _reserved_names(p: Dict) -> set:
    """Names the demonstration's targets may never take: the comparison arms' targets and the run-wide objects."""
    arms = {R.target_name(a, pr) for pr in F.PROFILES for a in F.ARMS}
    return arms | {"c28-ingress", "c28-metrics-route", "c28-forecast-route", GENERATOR_NAME}


def block_problems(p: Dict) -> List[str]:
    """Every problem of the params' "demonstration" block and of its bindings to the rest of the params (run id,
    nodes, reservation taint). Never raises."""
    out: List[str] = []
    d = p.get("demonstration")
    if not _keys(d, BLOCK_KEYS, "demonstration", out):
        return out
    left = R.placeholder_fields(d, "demonstration")
    if left:
        return out + [f"still placeholders: {', '.join(left)}"]
    if d["run_id"] != p.get("run_id"):
        out.append("demonstration.run_id must be the params' run_id (its objects, records and journals use it)")

    # The reservation is another run's: an explicit reference, bound to the params' nodes and taint.
    ref = d["reservation_ref"]
    if _keys(ref, ("run_id", "taint_value", "nodes"), "demonstration.reservation_ref", out):
        if not (isinstance(ref["run_id"], str) and _DNS.fullmatch(ref["run_id"]) and len(ref["run_id"]) <= 40):
            out.append("demonstration.reservation_ref.run_id must be the reservation owner's run id")
        if ref["taint_value"] != ref["run_id"]:
            out.append("demonstration.reservation_ref.taint_value must be the owner's run id (reserve.py taints with "
                       "the run id)")
        if (p.get("reservation") or {}).get("taint_value") != ref["taint_value"]:
            out.append("reservation.taint_value must be demonstration.reservation_ref.taint_value: the demonstration's "
                       "pods tolerate the owner's taint, nodes are never retagged")
        nodes = ref["nodes"]
        if _keys(nodes, ("workload", "support"), "demonstration.reservation_ref.nodes", out):
            for role in ("workload", "support"):
                n = nodes[role]
                if not _keys(n, ("name", "uid"), f"demonstration.reservation_ref.nodes.{role}", out):
                    continue
                if n["name"] != (p.get("nodes") or {}).get(role):
                    out.append(f"demonstration.reservation_ref.nodes.{role}.name must be nodes.{role}")
                if not (isinstance(n["uid"], str) and _UUID.fullmatch(n["uid"])):
                    out.append(f"demonstration.reservation_ref.nodes.{role}.uid must be the reserved node's UID")
            uids = [(nodes.get(r) or {}).get("uid") for r in ("workload", "support")] \
                if all(isinstance(nodes.get(r), dict) for r in ("workload", "support")) else []
            if len(uids) == 2 and uids[0] is not None and uids[0] == uids[1]:
                out.append("demonstration.reservation_ref.nodes: the workload and support UIDs must differ")

    # Exactly two targets, one per role.
    ts = d["targets"]
    if not (isinstance(ts, list) and len(ts) == 2 and all(isinstance(t, dict) for t in ts)):
        out.append("demonstration.targets must be exactly two targets: one forecast, one reactive")
    else:
        if sorted(str(t.get("role")) for t in ts) != sorted(ROLES):
            out.append("demonstration.targets must have exactly the roles forecast and reactive, one each")
        names = [t.get("name") for t in ts]
        for i, t in enumerate(ts):
            where = f"demonstration.targets[{i}]"
            want = ("name", "role", "families") if t.get("role") == "forecast" else ("name", "role")
            if not _keys(t, want, where, out):
                continue
            n = t["name"]
            if not (isinstance(n, str) and _DNS.fullmatch(n) and n.startswith("c28-") and len(n) <= 40):
                out.append(f"{where}.name must be a c28-* DNS label of at most 40 characters")
            elif n in _reserved_names(p):
                out.append(f"{where}.name {n} is a comparison arm's or a run-wide object's name")
            if t.get("role") == "forecast" and t["families"] != list(FAMILIES):
                out.append(f"{where}.families must be exactly {list(FAMILIES)} (the families the demonstration "
                           "qualification admits)")
        if len(set(map(str, names))) != 2:
            out.append("demonstration.targets must have two different names")

    if d["profile"] != PROFILE:
        out.append(f"demonstration.profile must be the one startup profile {PROFILE}")
    if d["bounds"] != BOUNDS:
        out.append(f"demonstration.bounds must be exactly {BOUNDS}")
    if not _int(d["shadow_replicas"], BOUNDS["min"], BOUNDS["max"]):
        out.append(f"demonstration.shadow_replicas must be an integer within the bounds {BOUNDS['min']}.."
                   f"{BOUNDS['max']}")
    c = d["capacity_per_replica_rps"]
    if isinstance(c, bool) or not isinstance(c, (int, float)) or not math.isfinite(c) or c <= 0:
        out.append("demonstration.capacity_per_replica_rps must be a positive number")
    if d["capacity_label"] != CAPACITY_LABEL:
        out.append(f'demonstration.capacity_label must be the literal "{CAPACITY_LABEL}"')
    if not (isinstance(d["capacity_source_calibration"], str) and _CALIBRATION.fullmatch(d["capacity_source_calibration"])):
        out.append("demonstration.capacity_source_calibration must name the source calibration id (c28cal-<10 hex>)")
    cad = d["cadence"]
    if not isinstance(cad, dict) or cad != CADENCE or any(isinstance(v, bool) for v in cad.values()):
        out.append(f"demonstration.cadence must be exactly {CADENCE}: one issuance per grid step; any other "
                   "combination is refused")
    if origin_ms(d["schedule_origin_utc"]) is None:
        out.append("demonstration.schedule_origin_utc must be a UTC midnight (YYYY-MM-DDT00:00:00Z), recorded once")
    if not _int(d["peak_multiple"], PEAK_MULTIPLE, PEAK_MULTIPLE):
        out.append(f"demonstration.peak_multiple must be {PEAK_MULTIPLE}, the ordinary program's peak in units of c")
    prog = d["program"]
    if _keys(prog, ("kind", "seed"), "demonstration.program", out):
        if prog["kind"] != "ordinary":
            out.append("demonstration.program.kind must be ordinary")
        if not _int(prog["seed"], 1, 2**31 - 1):
            out.append("demonstration.program.seed must be a positive integer")
        elif prog["seed"] in F.TUNING_SEEDS + F.CONFIRMATION_SEEDS:
            out.append("demonstration.program.seed must not be a tuning or confirmation seed of the comparison")
    out += _generator_problems(d["generator"])
    return out


def _generator_problems(g) -> List[str]:
    out: List[str] = []
    if not _keys(g, GENERATOR_KEYS, "demonstration.generator", out):
        return out
    m = _IMAGE.fullmatch(g["image"]) if isinstance(g["image"], str) else None
    if not m:
        out.append("demonstration.generator.image must be pinned by digest")
    elif m.group(2) != GENERATOR_DIGEST:
        out.append(f"demonstration.generator.image must be the reviewed k6 image ({GENERATOR_DIGEST})")
    if g["k6_version"] != GENERATOR_K6_VERSION:
        out.append(f"demonstration.generator.k6_version must be {GENERATOR_K6_VERSION}")
    res = g["resources"]
    if _keys(res, RESOURCE_KEYS, "demonstration.generator.resources", out):
        try:
            for req, lim, fn in (("cpu_request", "cpu_limit", R.cpu_millicores),
                                 ("memory_request", "memory_limit", R.memory_bytes)):
                if fn(res[req]) <= 0 or fn(res[req]) > fn(res[lim]):
                    out.append(f"demonstration.generator.resources.{req} must be positive and at most {lim}")
        except R.ParamsError as e:
            out.append(f"demonstration.generator.resources: {e}")
    if not (_int(g["segment_hours"], 1, 10**6) and g["segment_hours"] in SEGMENT_HOURS):
        out.append("demonstration.generator.segment_hours must be a whole number of days in hours (24, 48, ... 672)")
    if not (isinstance(g["remote_write_url"], str) and _REMOTE_WRITE.fullmatch(g["remote_write_url"])):
        out.append("demonstration.generator.remote_write_url must be an http(s) URL without user-info or query")
    return out


def problems(p: Dict) -> List[str]:
    """Every problem of the fields the demonstration stage consumes (render.stage_problems(p, "demonstration")); never
    raises. Empty means only that this stage may render and prepare its own objects: it says nothing about the
    comparison experiment's parameters."""
    if not isinstance(p, dict):
        return ["the params must be a JSON object"]
    out: List[str] = []
    try:
        R._validate(p, experiment=False)
    except R.ParamsError as e:
        out.append(str(e))
    except (KeyError, TypeError, AttributeError, ValueError) as e:
        out.append(f"malformed params ({type(e).__name__}: {e})")
    out += [x for x in R._reservation_problems(p, run_owned=False) if x not in out]
    if "residency" not in p:
        out.append("residency is required: the stage's admission counts W and S (allocatable and system requests)")
    out += block_problems(p)
    if not out:
        try:
            out += sizing_problems(p)
        except (R.ParamsError, ValueError) as e:
            out.append(str(e))
    return out


def validate(p: Dict) -> None:
    R.validate_stage(p, STAGE)


def origin_ms(text) -> Optional[int]:
    """The schedule origin in epoch milliseconds, or None unless `text` is a real UTC midnight."""
    if not (isinstance(text, str) and _ORIGIN.fullmatch(text)):
        return None
    try:
        t = datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return int(t.timestamp()) * 1000


def targets(p: Dict) -> List[Dict]:
    """The two targets in a fixed order (forecast, reactive): name, role, the arm they are rendered as, families (the
    reactive reference runs the forecast path's families; only its predictive extra differs)."""
    d = p["demonstration"]
    by_role = {t["role"]: t for t in d["targets"]}
    fam = list(by_role["forecast"]["families"])
    return [{"name": by_role[r]["name"], "role": r, "arm": ROLE_ARM[r], "families": fam} for r in ROLES]


def target_names(p: Dict) -> List[str]:
    return [t["name"] for t in targets(p)]


def capacity(p: Dict) -> str:
    """capacityPerReplica as the decimal string the PredictiveAutoscaler carries (three places, like render.
    capacity_rps, but WITHOUT the 0.875 ratio: the demonstration's field already is the per-replica capacity)."""
    c = Decimal(str(p["demonstration"]["capacity_per_replica_rps"])).quantize(Decimal("0.001"), rounding=ROUND_HALF_EVEN)
    if c <= 0:
        raise R.ParamsError("demonstration.capacity_per_replica_rps rounds to zero")
    return format(c.normalize(), "f")


# --- the generator's program and configuration --------------------------------------------------------------------

def day_plan(p: Dict) -> List[int]:
    """Intended arrivals of each of the 1,440 minutes of the day, for EACH target (identical demand): the ordinary
    program of the declared seed in units of c, times the per-replica capacity, per minute. The same conversion as
    schedule.k6_plan (max(0, round(u * c * 60)))."""
    c = float(capacity(p))
    prog = p["demonstration"]["program"]
    return [max(0, round(u * c * 60)) for u in schedule.program(prog["kind"], prog["seed"])]


def generator_sizing(p: Dict) -> Dict:
    """The generator's limits against the peak rate and the response timeout (R2-18), with the arithmetic.

    The script runs ONE virtual user per target (an event loop that dispatches asynchronous requests), so the number
    of VUs does not grow with the rate; what grows is the number of requests in flight, bounded per target by
    max_inflight = ceil(peak arrivals per second x timeout x 1.5) + 10 (schedule.k6_plan's rule for its VUs). Memory
    is ESTIMATED from the ASSUMED_* constants, not measured."""
    plan = day_plan(p)
    peak = max(plan)
    peak_rps = peak / 60.0
    inflight = int(math.ceil(peak_rps * F.REQUEST_TIMEOUT_S * 1.5)) + 10
    n = len(target_names(p))
    vus = n + SETUP_VUS
    estimate = ASSUMED_BASE_BYTES + vus * ASSUMED_VU_BYTES + n * inflight * ASSUMED_INFLIGHT_BYTES
    return {"targets": n, "peak_per_minute_per_target": peak, "peak_rps_per_target": round(peak_rps, 3),
            "peak_rps_total": round(peak_rps * n, 3), "timeout_s": F.REQUEST_TIMEOUT_S,
            "max_inflight_per_target": inflight, "vus": vus,
            "memory_estimate_bytes": estimate,
            "memory_estimate": f"{ASSUMED_BASE_BYTES} + {vus} x {ASSUMED_VU_BYTES} + {n} x {inflight} x "
                               f"{ASSUMED_INFLIGHT_BYTES} = {estimate} bytes (assumed constants, not measured)",
            "memory_limit_bytes": int(R.memory_bytes(p["demonstration"]["generator"]["resources"]["memory_limit"]))}


def sizing_problems(p: Dict) -> List[str]:
    s = generator_sizing(p)
    if s["memory_limit_bytes"] < s["memory_estimate_bytes"]:
        return [f"demonstration.generator.resources.memory_limit is below the estimated need at the peak rate "
                f"({s['memory_estimate']})"]
    return []


def generator_config(p: Dict, smoke_segment_minutes: Optional[int] = None) -> Dict:
    """The configuration load/c28-demo.js reads (schema c28-demo-generator/v1), with its digest.

    smoke_segment_minutes (the kind smoke only): a short segment so a planned restart happens within minutes; the
    configuration is then labelled "smoke" and is never a demonstration."""
    validate(p)
    d = p["demonstration"]
    plan = day_plan(p)
    seg = d["generator"]["segment_hours"] * 60
    label = "demonstration"
    if smoke_segment_minutes is not None:
        if not _int(smoke_segment_minutes, 2, 1_440):
            raise R.ParamsError("smoke_segment_minutes must be an integer in 2..1440")
        seg, label = smoke_segment_minutes, "smoke"
    cfg = {"schema": GENERATOR_CONFIG_SCHEMA, "label": label, "run_id": p["run_id"],
           "origin_utc": d["schedule_origin_utc"], "origin_ms": origin_ms(d["schedule_origin_utc"]),
           "minute_ms": 60_000, "day_minutes": schedule.MINUTES, "segment_minutes": seg,
           "min_segment_ms": MIN_SEGMENT_MS if label == "demonstration" else 30_000,
           "base_url": f"http://c28-ingress.{p['namespace']}.svc", "targets": target_names(p),
           "timeout_ms": F.REQUEST_TIMEOUT_S * 1000, "max_late_ms": MAX_LATE_MS, "flush_grace_ms": FLUSH_GRACE_MS,
           "max_inflight": generator_sizing(p)["max_inflight_per_target"],
           "latency_bounds_ms": list(LATENCY_BOUNDS_MS),
           "program": {"kind": d["program"]["kind"], "seed": d["program"]["seed"],
                       "capacity_per_replica_rps": float(capacity(p)), "capacity_label": d["capacity_label"],
                       "capacity_source_calibration": d["capacity_source_calibration"]},
           "per_minute": plan}
    cfg["digest"] = schedule.digest(cfg)
    return cfg


def script_text() -> str:
    with open(SCRIPT_PATH) as fh:
        return fh.read()


def _generator_labels(p: Dict) -> Dict[str, str]:
    return {**R._labels(p), "app.kubernetes.io/name": GENERATOR_NAME, COMPONENT_LABEL: "generator"}


def generator_objects(p: Dict, smoke_segment_minutes: Optional[int] = None) -> List[Dict]:
    """The generator: a ConfigMap (the script and its configuration) and ONE Deployment (replicas 1, strategy Recreate:
    the old pod is gone before a new one is created) on the support node. The container runs `k6 run` once per
    SEGMENT: the script resumes the program at the wall-clock position, ends at the next segment boundary and k6 exits
    0, so the kubelet starts the next incarnation (restartPolicy Always)."""
    cfg = generator_config(p, smoke_segment_minutes)
    g = p["demonstration"]["generator"]
    ns, labels = p["namespace"], _generator_labels(p)
    selector = {"app.kubernetes.io/name": GENERATOR_NAME, R.RUN_LABEL: p["run_id"]}
    script = script_text()
    cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": GENERATOR_NAME, "namespace": ns, "labels": labels},
          "data": {GENERATOR_SCRIPT: script, GENERATOR_CONFIG: json.dumps(cfg, sort_keys=True, separators=(",", ":"))}}
    args = ["run", "--quiet", "--no-color", "--no-usage-report", "--no-summary", "--no-thresholds",
            "--log-format", "raw", "--out", K6_OUTPUT, "--tag", f"c28_run={p['run_id']}",
            "-e", f"C28D_CONFIG=/plan/{GENERATOR_CONFIG}", f"/plan/{GENERATOR_SCRIPT}"]
    env = [{"name": "POD_UID", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.uid"}}},
           {"name": "POD_NAME", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.name"}}},
           {"name": "K6_PROMETHEUS_RW_SERVER_URL", "value": g["remote_write_url"]},
           {"name": "K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM", "value": "true"},
           {"name": "K6_PROMETHEUS_RW_PUSH_INTERVAL", "value": "5s"}]
    pod = {"metadata": {"labels": {**labels, **R.SIDECAR_FREE},
                        "annotations": {**R.SIDECAR_FREE, "c28.devkuban.com/generator-config": cfg["digest"],
                                        "c28.devkuban.com/generator-script":
                                            hashlib.sha256(script.encode()).hexdigest()}},
           "spec": {"automountServiceAccountToken": False, "terminationGracePeriodSeconds": 15,
                    "securityContext": {"runAsNonRoot": True, "runAsUser": GENERATOR_USER,
                                        "seccompProfile": {"type": "RuntimeDefault"}},
                    **R._placement(p, "support"),
                    "containers": [{"name": "k6", "image": g["image"], "args": args, "env": env,
                                    "resources": R._resources(g["resources"]),
                                    "securityContext": {"allowPrivilegeEscalation": False,
                                                        "capabilities": {"drop": ["ALL"]}},
                                    "volumeMounts": [{"name": "plan", "mountPath": "/plan", "readOnly": True}]}],
                    "volumes": [{"name": "plan", "configMap": {"name": GENERATOR_NAME}}]}}
    dep = {"apiVersion": "apps/v1", "kind": "Deployment",
           "metadata": {"name": GENERATOR_NAME, "namespace": ns, "labels": labels},
           "spec": {"replicas": 1, "revisionHistoryLimit": 2, "strategy": {"type": "Recreate"},
                    "selector": {"matchLabels": selector}, "template": pod}}
    return [cm, dep]


# --- rendering ------------------------------------------------------------------------------------------------------

def render(p: Dict, smoke_segment_minutes: Optional[int] = None) -> Dict[str, List[Dict]]:
    """The stage's objects by preparation phase: common, targets (Deployment, Service, EMPTY faults ConfigMap), pas
    (both in Recommend), writers (always empty: the stage has no HPA or KEDA writer) and generator. Audited: every
    target has exactly its PredictiveAutoscaler, every pod template is sidecar-free and least-privilege, every object
    carries the stage's run label, every name is c28-*, and the rendered cadence and bounds are the params'."""
    validate(p)
    d = p["demonstration"]
    ts = targets(p)
    out: Dict[str, List[Dict]] = {"common": R.common_objects(p, [t["name"] for t in ts]), "targets": [], "pas": [],
                                  "writers": [], "generator": generator_objects(p, smoke_segment_minutes)}
    for t in ts:
        labels = {**R._labels(p, t["arm"], d["profile"], t["name"]), DEMO_ROLE_LABEL: t["role"]}
        out["targets"] += R.target_objects(p, t["name"], labels, d["profile"], d["shadow_replicas"])
        out["pas"].append(R.pa_object(p, t["name"], copy.deepcopy(labels), d["profile"], capacity(p),
                                      reactive_only=t["role"] == "reactive", families=t["families"]))
    objs = flat(out)
    R.audit_rendered(objs)
    R.audit_sidecar_free(objs)
    R.audit_rbac(objs)
    audit(p, out)
    return out


def flat(parts: Dict[str, List[Dict]]) -> List[Dict]:
    return [o for k in ("common", "targets", "pas", "writers", "generator") for o in parts.get(k, [])]


def render_all(p: Dict) -> List[Dict]:
    """Every object of the stage, audited, within the residency budget (the stage's admission audit)."""
    parts = render(p)
    objs = flat(parts)
    rep = residency_report(p, objs)
    if rep["problems"]:
        raise R.ParamsError("residency budget exceeded (demonstration + system requests > 75 % of allocatable): "
                            + " | ".join(rep["problems"]))
    return objs


def audit(p: Dict, parts: Dict[str, List[Dict]]) -> None:
    """The stage's own rendering audit (ParamsError otherwise): names and run labels; two targets at the shadow count
    with the frozen bounds; both PredictiveAutoscalers in Recommend at exactly the params' cadence, with the
    forecast path's families, the reactive reference's predictive extra an explicit zero and the forecast path's the
    frozen one; one generator with one replica, strategy Recreate and the reviewed image."""
    d, bad = p["demonstration"], []
    names = target_names(p)
    for o in flat(parts):
        m = o["metadata"]
        if not m["name"].startswith("c28-"):
            bad.append(f"{o['kind']}/{m['name']} is not a c28-* name")
        if (m.get("labels") or {}).get(R.RUN_LABEL) != p["run_id"]:
            bad.append(f"{o['kind']}/{m['name']} does not carry {R.RUN_LABEL}={p['run_id']}")
        for t in R.pod_templates(o):
            if ((t.get("metadata") or {}).get("labels") or {}).get(R.RUN_LABEL) != p["run_id"]:
                bad.append(f"{o['kind']}/{m['name']}: its pods do not carry {R.RUN_LABEL}={p['run_id']}")
        if o["kind"] == "ConfigMap" and m["name"].endswith("-faults") and o["data"] != {"faults.json": "{}"}:
            bad.append(f"{m['name']}: a rendered fault schedule must be empty")
    deps = {o["metadata"]["name"]: o for o in parts["targets"] if o["kind"] == "Deployment"}
    if sorted(deps) != sorted(names):
        bad.append(f"the target Deployments {sorted(deps)} are not the two targets {sorted(names)}")
    for name, dep in deps.items():
        if dep["spec"]["replicas"] != d["shadow_replicas"]:
            bad.append(f"{name} is rendered at {dep['spec']['replicas']} replicas, not the shadow count")
    pas = {o["metadata"]["name"]: o for o in parts["pas"]}
    if sorted(pas) != sorted(names) or any(o["kind"] != "PredictiveAutoscaler" for o in parts["pas"]):
        bad.append("the PredictiveAutoscalers are not exactly one per target")
    for t in targets(p):
        spec = (pas.get(t["name"]) or {}).get("spec") or {}
        pred = spec.get("prediction") or {}
        where = f"PredictiveAutoscaler/{t['name']}"
        if spec.get("mode") != "Recommend":
            bad.append(f"{where} is not in Recommend")
        if (spec.get("minReplicas"), spec.get("maxReplicas")) != (d["bounds"]["min"], d["bounds"]["max"]):
            bad.append(f"{where}: bounds differ from the params'")
        if (pred.get("sampleIntervalSeconds"), pred.get("updateIntervalSeconds")) != \
                (d["cadence"]["grid_seconds"], d["cadence"]["update_interval_seconds"]):
            bad.append(f"{where}: the rendered cadence is not the params' grid/update interval")
        if pred.get("engine") != "statistical" or pred.get("families") != t["families"]:
            bad.append(f"{where}: engine statistical with families {t['families']} is required")
        want = {"maxReplicas": 0, "maxPercent": 0} if t["role"] == "reactive" else \
            {"maxReplicas": F.EXTRA_MAX_REPLICAS, "maxPercent": F.EXTRA_MAX_PERCENT}
        if pred.get("predictiveExtra") != want:
            bad.append(f"{where}: predictiveExtra must be exactly {want}")
        if ((spec.get("metrics") or {}).get("requests") or {}).get("capacityPerReplica") != capacity(p):
            bad.append(f"{where}: capacityPerReplica is not the stage's provisional capacity")
    if parts["writers"]:
        bad.append("the demonstration renders no HPA or KEDA writer")
    gens = [o for o in parts["generator"] if o["kind"] == "Deployment"]
    if len(gens) != 1 or gens[0]["spec"]["replicas"] != 1 or gens[0]["spec"]["strategy"] != {"type": "Recreate"}:
        bad.append("exactly one generator Deployment with one replica and strategy Recreate is required")
    if bad:
        raise R.ParamsError("demonstration rendering audit: " + "; ".join(bad))


# --- admission: residency -------------------------------------------------------------------------------------------

def release_values(p: Dict) -> Dict:
    """The release's route-binding chart values for this stage (render.eval_release_values, stage-validated)."""
    return R.eval_release_values(p, STAGE)


def residency_report(p: Dict, objs: Optional[List[Dict]] = None, chart: Optional[Dict] = None) -> Dict:
    """render.residency_report for the demonstration, against the same 75 % rule: on W BOTH targets at maxReplicas plus
    overlap_pods each (rollout and termination overlap) and the ingress; on S the two routes, the rendered generator,
    and the operator and forecaster as the release's FINAL chart values request them (`chart`; default the chart's
    values.yaml merged with release_values); each node's system requests on top."""
    validate(p)
    objs = flat(render(p)) if objs is None else objs
    chart = R.chart_values(release_values(p)) if chart is None else chart
    return R.residency_report(p, objs, chart, all_loaded=True, max_replicas=p["demonstration"]["bounds"]["max"],
                              generator_jobs=False)


# --- the Active configuration's chart values (validated here, never produced here) ---------------------------------

def active_values_problems(values: Dict, p: Dict) -> List[str]:
    """Problems of the release's chart values with respect to the scale write (LIVE-DEMO-PLAN sections 4 and 8).

    Shadow (operator.rbac.scaleWrite false): no scaleTargets may be named. Active (scaleWrite true): operator.rbac.
    scaleTargets must be exactly the stage's two target names; an empty or missing list (the chart's default, which
    keeps a namespace-wide write for existing installs) is refused, and so is any other name. Anything but a strict
    boolean scaleWrite is refused. The Active transition itself is another tool's reviewed step."""
    rbac = ((values.get("operator") or {}).get("rbac") or {}) if isinstance(values, dict) else {}
    write, named = rbac.get("scaleWrite"), rbac.get("scaleTargets")
    names = target_names(p)
    if write is False:
        return [] if named in (None, []) else \
            ["operator.rbac.scaleTargets must be empty while operator.rbac.scaleWrite is false (shadow)"]
    if write is not True:
        return ["operator.rbac.scaleWrite must be an explicit boolean"]
    if not isinstance(named, list) or not named:
        return ["operator.rbac.scaleTargets is empty: the Active configuration must name exactly the two targets "
                f"{names} (an empty list is a namespace-wide scale write)"]
    if not all(isinstance(n, str) for n in named) or len(set(named)) != len(named) or sorted(named) != sorted(names):
        return [f"operator.rbac.scaleTargets {named} must equal the two target names {names}"]
    return []
