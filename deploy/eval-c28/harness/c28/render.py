"""Kubernetes manifests for the C2.8 fixture and its five arms (EVAL-C28-MANIFEST sections 3, 4 and 11).

One Deployment per (arm, startup profile), each with a new name (and so a new UID), its own Service, fault ConfigMap
and exactly one replica writer: an HPA (H-CPU), a KEDA ScaledObject whose generated HPA is the writer (H-RPS), or a
PredictiveAutoscaler (PA-R, PA-N, PA-E). Fixture pods tolerate only the run's own NoSchedule taint and require the
workload node by role label and hostname; the ingress sits on the workload node too, the fault routes on the support
node. Every PredictiveAutoscaler is rendered in Recommend mode: switching an arm to Active is the executor's gated
step (section 1, prerequisite 3), never a rendering default.

Sidecar-free: the evaluation runs in prodcluster's `demo`, which injects Istio sidecars namespace-wide. Every Pod
template rendered for the evaluation carries `sidecar.istio.io/inject: "false"` as a pod LABEL (the injector webhook's
objectSelector excludes it, so the API server never calls the webhook) and as a pod ANNOTATION (the injector's own
per-pod opt-out); the namespace policy is never changed. audit_sidecar_free() refuses a rendering in which any Pod
template lacks either, and unexpected_injection() checks admitted pods for istio-proxy/istio-init containers. Results
therefore concern this sidecar-free fixture, not mesh-enabled workloads.

Ingress routing: the ingress sends every request to the next READY pod of its target in a strict per-request
round-robin (fixture ROUTING=endpoints-rr), discovered from the target Services' EndpointSlices, instead of through the
Service's ClusterIP (where the cluster balances per kept-alive CONNECTION, which made one of two fixture pods queue in
the S5a calibration). It therefore runs as its own ServiceAccount, bound by a namespaced Role to get/list/watch
EndpointSlices and nothing else; its token is an explicit projected volume (automountServiceAccountToken stays false
everywhere, so no admission-generated volume appears). audit_rbac() refuses any other RBAC shape: no cluster-scoped
object, exactly that rule, a binding only to a rendered ServiceAccount of the same namespace, and a token only in an
endpoints-rr ingress pod. The routing and the discovery bounds are set explicitly (INGRESS_ROUTING_ENV), never left to
the binary's default.

Residency: when the params carry a `residency` section (required by the prodcluster staging helpers, optional for the
kind smoke), residency_report() computes the evaluation's maximum resident requests on W and S from the rendered pod
templates, the evaluation release's chart values (operator and forecaster) and the declared generators and extras, and
render_all() refuses when evaluation plus node-system requests
exceed 75 % of a node's allocatable, CPU and memory separately (frozen.RESIDENCY_LIMIT_FRACTION).
"""

from __future__ import annotations

import copy
import os
import re
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Dict, Iterable, List, Optional, Tuple

import yaml

from . import frozen as F

RUN_LABEL = "c28.devkuban.com/run"
ARM_LABEL = "c28.devkuban.com/arm"
PROFILE_LABEL = "c28.devkuban.com/profile"
TARGET_LABEL = "c28.devkuban.com/target"

_DNS = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$")
_QUANTITY = re.compile(r"^[0-9]+(\.[0-9]+)?(m|Mi|Gi|Ki)?$")
_CPU = re.compile(r"^([0-9]+(?:\.[0-9]+)?)(m?)$")
_MEMORY = re.compile(r"^([0-9]+(?:\.[0-9]+)?)(Ki|Mi|Gi|Ti|k|M|G|T)?$")
_MEMORY_UNITS = {None: 1, "": 1, "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "k": 10**3, "M": 10**6,
                 "G": 10**9, "T": 10**12}

# The pod label AND annotation that keep Istio's injector away from every evaluation pod (demo injects namespace-wide;
# its webhook's objectSelector is `sidecar.istio.io/inject NotIn [false]`, a label selector).
SIDECAR_ANNOTATION = "sidecar.istio.io/inject"
SIDECAR_LABEL = SIDECAR_ANNOTATION
SIDECAR_FREE = {SIDECAR_ANNOTATION: "false"}
INJECTED_CONTAINERS = ("istio-proxy", "istio-init", "istio-validation")
# Declared budgets of the evaluation release's pods, used unless params.residency overrides them. The release's chart
# values are set FROM these (eval_release_values), and residency_report() counts what the final chart values request,
# refusing a component that requests less than its budget. The operator at the chart's default request; the forecaster
# at frozen.FORECASTER_BUDGET (a floor). One generator Job at session.py's default generator_resources.
OPERATOR_BUDGET = {"cpu_request": "100m", "memory_request": "128Mi"}
GENERATOR_BUDGET = {"concurrent": 1, "cpu_request": "500m", "memory_request": "256Mi"}
CHART_VALUES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "charts",
                            "predictive-autoscaler", "values.yaml")

# The ingress's routing (fixture ROUTING) and EndpointSlice discovery bounds, always rendered explicitly: a LIST every
# resync (a WATCH applies changes in between) and c28_ingress_discovery_stale = 1 once the last successful LIST is older
# than the stale bound (the last known endpoints stay in use; the harness gates on the metric).
INGRESS_ROUTING = "endpoints-rr"
INGRESS_ROUTING_ENV = (("ROUTING", INGRESS_ROUTING), ("DISCOVERY_RESYNC_SECONDS", "30"),
                       ("DISCOVERY_STALE_SECONDS", "90"))
RBAC_GROUP = "rbac.authorization.k8s.io"
ENDPOINTSLICE_RULES = [{"apiGroups": ["discovery.k8s.io"], "resources": ["endpointslices"],
                        "verbs": ["get", "list", "watch"]}]
SA_TOKEN_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
SA_TOKEN_VOLUME = "kube-api-access"
SA_TOKEN_SECONDS = 3600

FIXED_REPLICAS = 2          # every target's fixed replica count outside a session (the common starting count, section 5)

REQUIRED = ("schema", "run_id", "namespace", "control_namespace", "release", "image", "c0_rps", "nodes", "reservation",
            "fixture", "ingress", "proxy", "hpa_cpu_target_percent", "keda", "routes", "priority_class", "quota",
            "scrape", "release_network_policies")


class ParamsError(ValueError):
    pass


def _placeholder(v) -> bool:
    """A template placeholder: a string starting with '<' ("<W node name>") or one wrapped in double underscores with a
    non-empty body ("__METRICS_QUERY_PATH__"). An empty string or list is NOT a placeholder: a required field refuses
    it in its own check, an optional one (residency.support_extra = []) may legitimately be empty."""
    if not isinstance(v, str):
        return False
    s = v.strip()
    return s.startswith("<") or (len(s) > 4 and s.startswith("__") and s.endswith("__"))


def cpu_millicores(q) -> Decimal:
    """A CPU quantity ("250m", "1", "0.5") in millicores."""
    m = _CPU.match(str(q))
    if not m:
        raise ParamsError(f"{q!r} is not a CPU quantity (e.g. 250m or 1)")
    return Decimal(m.group(1)) * (1 if m.group(2) else 1000)


def memory_bytes(q) -> Decimal:
    """A memory quantity ("128Mi", "7.75Gi", "512M", bytes) in bytes."""
    m = _MEMORY.match(str(q))
    if not m:
        raise ParamsError(f"{q!r} is not a memory quantity (e.g. 128Mi or 7.75Gi)")
    return Decimal(m.group(1)) * _MEMORY_UNITS[m.group(2)]


def capacity_rps(c0: float) -> str:
    """c = 0.875 c0, as a decimal string with three places (the value every RPS arm and the scorer use)."""
    c = (Decimal(str(c0)) * Decimal(str(F.CAPACITY_RATIO))).quantize(Decimal("0.001"), rounding=ROUND_HALF_EVEN)
    if c <= 0:
        raise ParamsError("c0 must be positive")
    return format(c.normalize(), "f")


def validate(p: Dict) -> None:
    """The FULL c28-params/v1 validation: every field the experiment consumes is complete and well-formed, and no
    value anywhere is still a template placeholder. It includes every stage validation (validate_stage)."""
    _validate(p, experiment=True)


# The experiment-only quantities: the uncapped calibrated capacity c0, the CPU HPA's tuned target and the KEDA settings.
# A stage that runs no H-CPU or H-RPS arm and takes its capacity from its own reviewed field (the demonstration) never
# consumes them: they may be absent or still placeholders there, and no dummy value is ever filled in for them.
EXPERIMENT_ONLY = ("c0_rps", "hpa_cpu_target_percent", "keda")


def _validate(p: Dict, experiment: bool) -> None:
    """validate()'s checks, in its order. experiment=False (the demonstration stage, demo.validate) leaves out exactly
    the EXPERIMENT_ONLY fields and the reservation stage's "taint_value is the run_id" rule (the demonstration occupies
    another run's reservation by explicit reference, which demo.problems checks instead)."""
    required = REQUIRED if experiment else tuple(k for k in REQUIRED if k not in EXPERIMENT_ONLY)
    missing = [k for k in required if k not in p]
    if missing:
        raise ParamsError(f"missing parameters: {', '.join(missing)}")
    if p["schema"] != "c28-params/v1":
        raise ParamsError("schema must be c28-params/v1")
    if experiment:
        validate_stage(p, "reservation")
    for key in ("run_id", "namespace", "control_namespace", "priority_class"):
        if not _DNS.match(str(p[key])) or len(str(p[key])) > 40:
            raise ParamsError(f"{key} must be a short DNS label")
    if not _DIGEST_IMAGE.match(p["image"]):
        raise ParamsError("image must be pinned by digest (no tags, never latest)")
    if experiment and not (isinstance(p["c0_rps"], (int, float)) and p["c0_rps"] > 0):
        raise ParamsError("c0_rps must be a positive number (the calibrated per-replica capacity)")
    for role in ("workload", "support"):
        if not _DNS.match(str(p["nodes"].get(role, ""))):
            raise ParamsError(f"nodes.{role} must be a node name")
    if p["nodes"]["workload"] == p["nodes"]["support"]:
        raise ParamsError("the workload and support nodes must differ (section 11.1)")
    for res in ("fixture", "ingress", "proxy"):
        for k in ("cpu_request", "cpu_limit", "memory_request", "memory_limit"):
            if k not in p[res]:
                raise ParamsError(f"{res}.{k} is missing" + (_FIXTURE_RESOURCES if res == "fixture" else ""))
            if _placeholder(p[res][k]):
                raise ParamsError(f"{res}.{k} is still a placeholder" + (_FIXTURE_RESOURCES if res == "fixture" else ""))
            if not _QUANTITY.match(str(p[res][k])):
                raise ParamsError(f"{res}.{k} is not a quantity")
        parse = (("cpu_request", "cpu_limit", cpu_millicores), ("memory_request", "memory_limit", memory_bytes))
        for req, lim, fn in parse:
            if fn(p[res][req]) <= 0 or fn(p[res][req]) > fn(p[res][lim]):
                raise ParamsError(f"{res}.{req} must be positive and at most {res}.{lim}")
    if experiment and p["hpa_cpu_target_percent"] not in F.HPA_CPU_CANDIDATES:
        raise ParamsError(f"hpa_cpu_target_percent must be one of the tuning candidates {F.HPA_CPU_CANDIDATES}")
    if p["scrape"] not in ("PodMonitor", "VMPodScrape", "annotations"):
        raise ParamsError("scrape must be PodMonitor, VMPodScrape or annotations")
    if not _DNS.match(str(p["release"])):
        raise ParamsError("release must be the evaluation Helm release name")
    if not isinstance(p["release_network_policies"], bool):
        raise ParamsError("release_network_policies must say whether the evaluation release enforces NetworkPolicies")
    routes = p["routes"]
    if not isinstance(routes, dict):
        raise ParamsError("routes must name metrics_upstream, metrics_query_path and forecast_upstream")
    for k in ("metrics_upstream", "forecast_upstream"):
        if not isinstance(routes.get(k), str) or not routes[k].strip():
            raise ParamsError(f"routes.{k} is required (host:port)")
    url = metrics_query_url(p)
    if experiment and p["keda"]["server_address"] != url:
        raise ParamsError(f"keda.server_address must be exactly the evaluation metrics route's query URL ({url}): "
                          "the RPS outage reaches H-RPS, and KEDA queries the same API path as the operator and the "
                          "forecaster")
    if "residency" in p:
        residency_params(p)
    left = placeholder_fields(p if experiment else {k: v for k, v in p.items() if k not in EXPERIMENT_ONLY})
    if left:
        raise ParamsError(f"still placeholders: {', '.join(left)}")


# --- stage-scoped validation ------------------------------------------------------------------------------------------
# A stage that runs before the experiment parameters can be completed validates exactly the fields it consumes, with
# the full validation's strictness for those fields; every other field may still be a placeholder. Passing a stage
# validation says NOTHING about experiment readiness: that is full_validation_problems() == [] (validate() passes).
# The reservation stage (staging/reserve.py: plan, apply, rollback-plan, rollback, verify's reservation part) consumes
# run_id (the taint value and the run label of evaluation pods), nodes.workload/support (bound to the record's
# name+UID), reservation.taint_key/taint_value/role_label, and control_namespace + release (occupant classification:
# the evaluation release's pods on S count as evaluation pods). Capacity calibration on the reserved W comes after it.
# The demonstration stage (c28/demo.py; LIVE-DEMO-PLAN section 5) consumes every field its own objects are rendered
# from and nothing of the comparison experiment: no c0_rps, no hpa_cpu_target_percent, no keda.* (EXPERIMENT_ONLY).
STAGE_FIELDS = {
    "reservation": ("schema", "run_id", "nodes.workload", "nodes.support", "reservation.taint_key",
                    "reservation.taint_value", "reservation.role_label", "control_namespace", "release"),
    "demonstration": ("schema", "run_id", "namespace", "control_namespace", "release", "image", "nodes.workload",
                      "nodes.support", "reservation.taint_key", "reservation.taint_value", "reservation.role_label",
                      "fixture", "ingress", "proxy", "routes.metrics_upstream", "routes.metrics_query_path",
                      "routes.forecast_upstream", "priority_class", "quota", "scrape", "release_network_policies",
                      "residency", "demonstration"),
}
_LABEL_NAME = re.compile(r"^[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$")
_DNS_SUBDOMAIN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")


def _qualified_name(v) -> bool:
    """A Kubernetes qualified name (label/taint key): [prefix/]name, prefix a DNS subdomain of at most 253, name at
    most 63 alphanumerics, '-', '_' or '.', starting and ending alphanumeric."""
    if not isinstance(v, str) or _placeholder(v) or v.count("/") > 1:
        return False
    prefix, _, name = v.rpartition("/")
    if "/" in v and not (prefix and len(prefix) <= 253 and _DNS_SUBDOMAIN.match(prefix)):
        return False
    return bool(_LABEL_NAME.match(name))


def _short_label(p: Dict, key: str) -> Optional[str]:
    v = p.get(key)
    if _placeholder(v):
        return f"{key} is still a placeholder"
    if not (isinstance(v, str) and _DNS.match(v) and len(v) <= 40):
        return f"{key} must be a short DNS label"
    return None


def stage_problems(p: Dict, stage: str) -> List[str]:
    """Every problem of the fields `stage` consumes (STAGE_FIELDS[stage]); never raises. Empty means only that this
    stage may run, never that the experiment parameters are complete."""
    if stage not in STAGE_FIELDS:
        raise ValueError(f"unknown params stage {stage!r} (known: {', '.join(sorted(STAGE_FIELDS))})")
    if not isinstance(p, dict):
        return ["the params must be a JSON object"]
    if stage == "demonstration":
        from . import demo                     # demo imports this module: resolved at call time
        return demo.problems(p)
    return _reservation_problems(p, run_owned=True)


def _reservation_problems(p: Dict, run_owned: bool) -> List[str]:
    """The reservation stage's checks. run_owned=False (the demonstration, which occupies ANOTHER run's reservation by
    explicit reference) leaves out only "reservation.taint_value must be the run_id"; demo.problems binds the taint
    value to the referenced reservation instead."""
    out: List[str] = []
    if p.get("schema") != "c28-params/v1":
        out.append("schema must be c28-params/v1")
    for key in ("run_id", "control_namespace"):
        bad = _short_label(p, key)
        if bad:
            out.append(bad)
    rel = p.get("release")
    if _placeholder(rel):
        out.append("release is still a placeholder")
    elif not (isinstance(rel, str) and _DNS.match(rel)):
        out.append("release must be the evaluation Helm release name")
    nodes = p.get("nodes")
    if not isinstance(nodes, dict):
        out.append("nodes must name the workload and support nodes")
        nodes = {}
    for role in ("workload", "support"):
        v = nodes.get(role)
        if _placeholder(v):
            out.append(f"nodes.{role} is still a placeholder")
        elif not (isinstance(v, str) and _DNS.match(v)):
            out.append(f"nodes.{role} must be a node name")
    if nodes.get("workload") is not None and nodes.get("workload") == nodes.get("support"):
        out.append("the workload and support nodes must differ (section 11.1)")
    r = p.get("reservation")
    if not isinstance(r, dict):
        out.append("reservation must name taint_key, taint_value and role_label")
        r = {}
    for key in ("taint_key", "role_label"):
        v = r.get(key)
        if _placeholder(v):
            out.append(f"reservation.{key} is still a placeholder")
        elif not _qualified_name(v):
            out.append(f"reservation.{key} must be a Kubernetes qualified name ([prefix/]name)")
    if r.get("taint_key") is not None and r.get("taint_key") == r.get("role_label"):
        out.append("reservation.taint_key and reservation.role_label must differ")
    tv = r.get("taint_value")
    if _placeholder(tv):
        out.append("reservation.taint_value is still a placeholder")
    elif not (isinstance(tv, str) and _LABEL_NAME.match(tv)):
        out.append("reservation.taint_value must be a label value")
    elif run_owned and isinstance(p.get("run_id"), str) and tv != p["run_id"]:
        out.append("reservation.taint_value must be the run_id")
    return out


def validate_stage(p: Dict, stage: str) -> None:
    """Raise ParamsError unless every field `stage` consumes is complete and well-formed (stage_problems)."""
    problems = stage_problems(p, stage)
    if problems:
        raise ParamsError(f"{stage} stage: " + "; ".join(problems))


def placeholder_fields(p, prefix: str = "") -> List[str]:
    """The dotted paths of every value that is still a template placeholder ('<...>' or '__...__', _placeholder)."""
    if _placeholder(p):
        return [prefix or "(root)"]
    out: List[str] = []
    if isinstance(p, dict):
        for k in sorted(p):
            out += placeholder_fields(p[k], f"{prefix}.{k}" if prefix else str(k))
    elif isinstance(p, list):
        for i, v in enumerate(p):
            out += placeholder_fields(v, f"{prefix}[{i}]")
    return out


def full_validation_problems(p: Dict) -> List[str]:
    """The full validation's verdict without raising: the missing required parameters, every placeholder field, and
    validate()'s refusal. Empty exactly when validate(p) passes, i.e. the experiment parameters are complete."""
    if not isinstance(p, dict):
        return ["the params must be a JSON object"]
    out: List[str] = []
    missing = [k for k in REQUIRED if k not in p]
    if missing:
        out.append(f"missing: {', '.join(missing)}")
    left = placeholder_fields(p)
    if left:
        out.append(f"placeholders: {', '.join(left)}")
    try:
        validate(p)
    except ParamsError as e:
        if str(e) != f"still placeholders: {', '.join(left)}":       # already listed above
            out.append(str(e))
    except (KeyError, TypeError, AttributeError, ValueError) as e:     # a malformed section the checks above reached
        out.append(f"malformed params ({type(e).__name__}: {e})")
    return out


_FIXTURE_RESOURCES = (": the fixture's CPU/memory requests and limits are the calibrated, reviewed values (manifest "
                      "section 3), identical for every arm; they are never invented or lowered to fit the residency "
                      "budget")


def _req_pair(d, where: str) -> Tuple[Decimal, Decimal]:
    if not isinstance(d, dict):
        raise ParamsError(f"{where} must be an object with cpu and memory")
    for k in ("cpu", "memory"):
        if k not in d:
            raise ParamsError(f"{where}.{k} is missing")
        if _placeholder(d[k]):
            raise ParamsError(f"{where}.{k} is still a placeholder: use the value of the fresh dated node inventory")
    return cpu_millicores(d["cpu"]), memory_bytes(d["memory"])


def _component(d, where: str) -> Tuple[Decimal, Decimal]:
    if not isinstance(d, dict):
        raise ParamsError(f"{where} must be an object with cpu_request and memory_request")
    for k in ("cpu_request", "memory_request"):
        if k not in d or _placeholder(d[k]):
            raise ParamsError(f"{where}.{k} is missing or a placeholder")
    return cpu_millicores(d["cpu_request"]), memory_bytes(d["memory_request"])


def _count(v, where: str, lo: int, hi: int = 1_000) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or not lo <= v <= hi:
        raise ParamsError(f"{where} must be an integer in {lo}..{hi}")
    return v


def residency_params(p: Dict) -> Dict:
    """The validated, normalized residency section (millicores and bytes), defaults applied.

      residency.workload / residency.support: {"allocatable": {"cpu", "memory"}, "system_requests": {"cpu", "memory"}}
          allocatable from the fresh node inventory; system_requests = the summed requests of the node-level pods that
          stay on that node (DaemonSets and other allowlisted system agents).
      residency.overlap_pods (default 2 = frozen SCALE_DOWN_PODS, at least 1): extra pods of the loaded arm counted on
          W for startup/termination overlap. A scale-down step may terminate up to two pods, whose requests the
          scheduler keeps counting until they are gone, while scale-up (stabilization 0) creates their replacements.
      residency.idle_arm_replicas (default = the rendered fixed replica count, 2): the replica count of every arm that
          is not loaded. A value below the fixed starting count is idle-arm PARKING, which needs a parking mechanism
          (writers stopped, the starting count restored before each session, history preserved, the loaded arm's
          frozen bounds unchanged) that is not implemented: it is refused.
      residency.operator (default the chart's request 100m/128Mi), residency.forecaster (default and floor 1 CPU /
          512Mi): the declared budgets; the evaluation release requests exactly these (eval_release_values), and the
          accounting reads the final chart values, refusing a component that requests less than its budget,
      residency.generator ({"concurrent", "cpu_request", "memory_request"}, default one Job at 500m/256Mi; history and
          warm-up generators that may run at the same time count in "concurrent"),
      residency.support_extra (default []): [{"name", "replicas", "cpu_request", "memory_request"}] for every other
          unrendered evaluation pod on S (recorder, adapters)."""
    r = p.get("residency")
    if not isinstance(r, dict):
        raise ParamsError("residency must be an object")
    out: Dict = {"nodes": {}}
    for role in ("workload", "support"):
        n = r.get(role)
        if not isinstance(n, dict):
            raise ParamsError(f"residency.{role} needs allocatable and system_requests")
        alloc = _req_pair(n.get("allocatable"), f"residency.{role}.allocatable")
        system = _req_pair(n.get("system_requests"), f"residency.{role}.system_requests")
        if alloc[0] <= 0 or alloc[1] <= 0:
            raise ParamsError(f"residency.{role}.allocatable must be positive")
        out["nodes"][role] = {"allocatable": alloc, "system": system}
    out["overlap_pods"] = _count(r.get("overlap_pods", F.SCALE_DOWN_PODS), "residency.overlap_pods", 1, F.MAX_REPLICAS)
    fixed = FIXED_REPLICAS
    idle = _count(r.get("idle_arm_replicas", fixed), "residency.idle_arm_replicas", 0, F.MAX_REPLICAS)
    if idle < fixed:
        raise ParamsError(f"residency.idle_arm_replicas {idle} is below the fixed starting count {fixed}: parking idle "
                          "arms needs a parking mechanism (writers stopped, starting count restored before each "
                          "session, history preserved, frozen bounds unchanged) that is not implemented")
    out["idle_arm_replicas"] = idle
    out["operator"] = _component(r.get("operator", OPERATOR_BUDGET), "residency.operator")
    if out["operator"][0] <= 0 or out["operator"][1] <= 0:
        raise ParamsError("residency.operator must request CPU and memory")
    out["forecaster"] = _component(r.get("forecaster", F.FORECASTER_BUDGET), "residency.forecaster")
    floor = (cpu_millicores(F.FORECASTER_BUDGET["cpu_request"]), memory_bytes(F.FORECASTER_BUDGET["memory_request"]))
    if out["forecaster"][0] < floor[0] or out["forecaster"][1] < floor[1]:
        raise ParamsError("residency.forecaster must reserve at least 1 CPU / 512Mi (STAGING S2)")
    out["raw_operator"] = dict(r.get("operator", OPERATOR_BUDGET))
    out["raw_forecaster"] = dict(r.get("forecaster", F.FORECASTER_BUDGET))
    g = r.get("generator", GENERATOR_BUDGET)
    out["generator"] = _component(g, "residency.generator")
    out["generator_concurrent"] = _count(g.get("concurrent", 1), "residency.generator.concurrent", 1, 20)
    extra = r.get("support_extra", [])
    if _placeholder(extra) or not isinstance(extra, list):
        raise ParamsError("residency.support_extra must list every other evaluation pod on S (recorder, adapters), "
                          "or be [] when there is none")
    out["support_extra"] = []
    for i, e in enumerate(extra):
        where = f"residency.support_extra[{i}]"
        if not isinstance(e, dict) or not _DNS.match(str(e.get("name", ""))):
            raise ParamsError(f"{where} needs a name")
        out["support_extra"].append((e["name"], _count(e.get("replicas", 1), f"{where}.replicas", 1, 20),
                                     *_component(e, where)))
    return out


def route_host(p: Dict, kind: str) -> str:
    """The in-cluster name of an evaluation-owned route (also the TLS name for the forecast route)."""
    return f"c28-{kind}-route.{p['control_namespace']}.svc"


METRICS_ROUTE_PORT = 8481
_PATH_SEGMENT = re.compile(r"[A-Za-z0-9._~-]+")      # fullmatch: "$" would admit a final newline


def metrics_query_path(p: Dict) -> str:
    """routes.metrics_query_path, validated: the Prometheus-API prefix of the upstream behind the metrics route (the
    route is a plain TCP proxy, so the path travels in the URL). "/select/0/prometheus" for the cluster
    VictoriaMetrics (vmselect, tenant 0), "/" for a non-tenant backend (single-node VictoriaMetrics, Prometheus).
    Required; starts with "/", unreserved characters only, no query string, fragment, empty, "." or ".." segment, and
    no trailing slash except the root itself."""
    routes = p.get("routes") if isinstance(p, dict) else None
    v = routes.get("metrics_query_path") if isinstance(routes, dict) else None
    where = "routes.metrics_query_path"
    if _placeholder(v):
        raise ParamsError(f"{where} is still a placeholder")
    if not isinstance(v, str) or not v:
        raise ParamsError(f"{where} is required: the upstream's query API path, e.g. /select/0/prometheus, or / for "
                          "a non-tenant backend")
    if not v.startswith("/"):
        raise ParamsError(f"{where} must start with '/'")
    if "?" in v or "#" in v:
        raise ParamsError(f"{where} must not carry a query string or fragment")
    if ".." in v:
        raise ParamsError(f"{where} must not contain '..'")
    if v == "/":
        return v
    if v.endswith("/"):
        raise ParamsError(f"{where} must not end with '/' (only the root path is '/')")
    if not all(_PATH_SEGMENT.fullmatch(seg) and seg != "." for seg in v[1:].split("/")):
        raise ParamsError(f"{where} must be '/'-separated non-empty segments of unreserved characters")
    return v


def metrics_query_url(p: Dict) -> str:
    """The query API base URL through the evaluation metrics route, the ONE value for the operator's and forecaster's
    prometheus.url and KEDA's serverAddress (validate() requires keda.server_address to equal it exactly). Clients
    append /api/v1/query or /api/v1/query_range; for the root path the URL is the bare route, no trailing slash."""
    path = metrics_query_path(p)
    base = f"http://{route_host(p, 'metrics')}:{METRICS_ROUTE_PORT}"
    return base if path == "/" else base + path


def eval_release_values(p: Dict, stage: str = "experiment") -> Dict:
    """The evaluation release's chart values that bind it to the routes: metrics through the metrics route at its
    query path (metrics_query_url, the same URL KEDA uses), the forecaster through the forecast route with the route's name in the certificate (verification stays on).
    `stage`: "experiment" (the full validation, the default) or a STAGE_FIELDS stage that renders the release
    ("demonstration"): the values are the same function of the same fields."""
    validate(p) if stage == "experiment" else validate_stage(p, stage)
    fr = route_host(p, "forecast")
    out = {"prometheus": {"url": metrics_query_url(p)},
           "operator": {"watchNamespaces": [p["namespace"]], "forecasterAddress": f"{fr}:8443",
                        "podLabels": dict(SIDECAR_FREE), "podAnnotations": dict(SIDECAR_FREE)},
           "forecaster": {"engine": "statistical", "tls": {"extraDNSNames": [fr]},
                          "podLabels": dict(SIDECAR_FREE), "podAnnotations": dict(SIDECAR_FREE)}}
    if "residency" in p:
        r, chart = residency_params(p), chart_values()
        out["operator"]["resources"] = _budget_resources(r["raw_operator"], chart["operator"]["resources"])
        out["forecaster"]["statistical"] = {"resources": _budget_resources(
            r["raw_forecaster"], chart["forecaster"]["statistical"]["resources"])}
    return out


def _budget_resources(budget: Dict, default: Dict) -> Dict:
    """Chart resources requesting exactly the declared budget; each default limit is kept, raised to the request."""
    req = {"cpu": str(budget["cpu_request"]), "memory": str(budget["memory_request"])}
    lim = dict(default.get("limits") or {})
    if "cpu" in lim and cpu_millicores(lim["cpu"]) < cpu_millicores(req["cpu"]):
        lim["cpu"] = req["cpu"]
    if "memory" in lim and memory_bytes(lim["memory"]) < memory_bytes(req["memory"]):
        lim["memory"] = req["memory"]
    return {"requests": req, "limits": lim}


def _merge(a: Dict, b: Dict) -> Dict:
    """Helm's values merge: maps merge key by key, anything else replaces."""
    out = copy.deepcopy(a)
    for k, v in (b or {}).items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else copy.deepcopy(v)
    return out


def chart_values(*overlays: Dict) -> Dict:
    """The final values of the evaluation release: the chart's values.yaml merged with each overlay in order."""
    with open(CHART_VALUES) as fh:
        out = yaml.safe_load(fh)
    for o in overlays:
        out = _merge(out, o)
    return out


def chart_pods(values: Dict) -> List[Tuple[str, int, Decimal, Decimal]]:
    """(name, replicas, cpu, memory) of every long-running pod the chart renders with these final values, read from the
    same paths its templates read (operator.yaml: operator.replicas, operator.resources; forecaster.yaml: one replica,
    forecaster.statistical.resources for the statistical engine). Training CronJobs are refused: the evaluation release
    trains nothing."""
    if (values.get("training") or {}).get("targets"):
        raise ParamsError("the evaluation release must not train (training.targets must be empty)")
    stat = (values.get("forecaster") or {}).get("engine") == "statistical"
    fres = values["forecaster"]["statistical"]["resources"] if stat else values["forecaster"]["resources"]

    def req(res):
        r = (res or {}).get("requests") or {}
        return cpu_millicores(r.get("cpu", "0")), memory_bytes(r.get("memory", "0"))
    return [("operator", int(values["operator"].get("replicas", 1)), *req(values["operator"]["resources"])),
            ("forecaster", 1, *req(fres))]


def _route_policies(p: Dict) -> List[Dict]:
    """Additive NetworkPolicies for a release that enforces them: its operator and forecaster may reach the routes, and
    the forecaster accepts the forecast route (the release's own policies admit only the operator)."""
    ns, labels = p["control_namespace"], _labels(p)
    release = {"app.kubernetes.io/name": "predictive-autoscaler", "app.kubernetes.io/instance": p["release"]}
    routes = {"matchLabels": {RUN_LABEL: p["run_id"]}, "matchExpressions": [
        {"key": "app.kubernetes.io/name", "operator": "In", "values": ["c28-metrics-route", "c28-forecast-route"]}]}
    return [
        {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
         "metadata": {"name": "c28-release-to-routes", "namespace": ns, "labels": labels},
         "spec": {"podSelector": {"matchLabels": release}, "policyTypes": ["Egress"],
                  "egress": [{"to": [{"podSelector": routes}],
                              "ports": [{"protocol": "TCP", "port": 8481}, {"protocol": "TCP", "port": 8443}]}]}},
        {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
         "metadata": {"name": "c28-forecast-route-to-forecaster", "namespace": ns, "labels": labels},
         "spec": {"podSelector": {"matchLabels": {**release, "app.kubernetes.io/component": "forecaster"}},
                  "policyTypes": ["Ingress"],
                  "ingress": [{"from": [{"podSelector": {"matchLabels": {RUN_LABEL: p["run_id"],
                                                                        "app.kubernetes.io/name": "c28-forecast-route"}}}],
                               "ports": [{"protocol": "TCP", "port": 8443}]}]}},
    ]


def target_name(arm: str, profile: str) -> str:
    if arm not in F.ARMS or profile not in F.PROFILES:
        raise ParamsError(f"unknown arm/profile {arm}/{profile}")
    return f"c28-{arm.lower()}-{profile}"


def _labels(p: Dict, arm: str = "", profile: str = "", target: str = "") -> Dict[str, str]:
    out = {"app.kubernetes.io/part-of": "c28-evaluation", RUN_LABEL: p["run_id"]}
    if arm:
        out.update({ARM_LABEL: arm.lower(), PROFILE_LABEL: profile, TARGET_LABEL: target})
    return out


def _placement(p: Dict, role: str) -> Dict:
    r = p["reservation"]
    return {
        "tolerations": [{"key": r["taint_key"], "operator": "Equal", "value": r["taint_value"], "effect": "NoSchedule"}],
        "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{
            "matchExpressions": [{"key": r["role_label"], "operator": "In", "values": [role]},
                                 {"key": "kubernetes.io/hostname", "operator": "In", "values": [p["nodes"][role]]}]}]}}},
    }


def _resources(r: Dict) -> Dict:
    return {"requests": {"cpu": r["cpu_request"], "memory": r["memory_request"]},
            "limits": {"cpu": r["cpu_limit"], "memory": r["memory_limit"]}}


_SECURITY = {"runAsNonRoot": True, "runAsUser": 65532, "runAsGroup": 65532, "seccompProfile": {"type": "RuntimeDefault"}}
_CONTAINER_SECURITY = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                       "capabilities": {"drop": ["ALL"]}}


def ingress_routing_env() -> List[Dict]:
    return [{"name": k, "value": v} for k, v in INGRESS_ROUTING_ENV]


def ingress_rbac(ns: str, name: str, labels: Dict[str, str]) -> List[Dict]:
    """The ingress's identity: a ServiceAccount (no automatic token), a Role that may only get/list/watch EndpointSlices
    in its own namespace, and the RoleBinding of the one to the other (all named `name`)."""
    md = {"name": name, "namespace": ns, "labels": dict(labels)}
    return [{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": copy.deepcopy(md),
             "automountServiceAccountToken": False},
            {"apiVersion": f"{RBAC_GROUP}/v1", "kind": "Role", "metadata": copy.deepcopy(md),
             "rules": copy.deepcopy(ENDPOINTSLICE_RULES)},
            {"apiVersion": f"{RBAC_GROUP}/v1", "kind": "RoleBinding", "metadata": copy.deepcopy(md),
             "roleRef": {"apiGroup": RBAC_GROUP, "kind": "Role", "name": name},
             "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": ns}]}]


def ingress_token() -> Tuple[Dict, Dict]:
    """(the pod volume, the container mount) of the ingress's API credentials: an explicit projected volume with every
    field the API server would otherwise default (token expiry, defaultMode, fieldRef apiVersion), so the admitted pod
    equals its rendering and no admission-generated kube-api-access-<random> volume appears."""
    vol = {"name": SA_TOKEN_VOLUME, "projected": {"defaultMode": 420, "sources": [
        {"serviceAccountToken": {"path": "token", "expirationSeconds": SA_TOKEN_SECONDS}},
        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
        {"downwardAPI": {"items": [{"path": "namespace",
                                    "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}}]}}]}}
    return vol, {"name": SA_TOKEN_VOLUME, "mountPath": SA_TOKEN_DIR, "readOnly": True}


def rps_query(target: str) -> str:
    """The target's own offered-RPS query at the independent ingress (frozen 60 s range). The PredictiveAutoscaler gets
    the {{ .Name }} form, which the operator compiles to this exact string."""
    return f'sum(rate(c28_ingress_requests_total{{target="{target}"}}[{F.RATE_RANGE}]))'


def scale_down_behavior() -> Dict:
    return {"scaleUp": {"stabilizationWindowSeconds": 0, "selectPolicy": "Max",
                        "policies": [{"type": "Pods", "value": F.MAX_REPLICAS, "periodSeconds": 15},
                                     {"type": "Percent", "value": 1000, "periodSeconds": 15}]},
            "scaleDown": {"stabilizationWindowSeconds": F.SCALE_DOWN_STABILIZATION_S, "selectPolicy": "Max",
                          "policies": [{"type": "Percent", "value": F.SCALE_DOWN_PERCENT,
                                        "periodSeconds": F.SCALE_DOWN_PERIOD_S},
                                       {"type": "Pods", "value": F.SCALE_DOWN_PODS,
                                        "periodSeconds": F.SCALE_DOWN_PERIOD_S}]}}


def _writer(p: Dict, arm: str, profile: str, target: str) -> Dict:
    ns, labels = p["namespace"], _labels(p, arm, profile, target)
    ref = {"apiVersion": "apps/v1", "kind": "Deployment", "name": target}
    c = capacity_rps(p["c0_rps"])
    if arm == "H-CPU":
        return {"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                "metadata": {"name": target, "namespace": ns, "labels": labels},
                "spec": {"scaleTargetRef": ref, "minReplicas": F.MIN_REPLICAS, "maxReplicas": F.MAX_REPLICAS,
                         "metrics": [{"type": "Resource", "resource": {"name": "cpu", "target": {
                             "type": "Utilization", "averageUtilization": p["hpa_cpu_target_percent"]}}}],
                         "behavior": scale_down_behavior()}}
    if arm == "H-RPS":
        k = p["keda"]
        return {"apiVersion": "keda.sh/v1alpha1", "kind": "ScaledObject",
                "metadata": {"name": target, "namespace": ns, "labels": labels},
                "spec": {"scaleTargetRef": {"name": target}, "pollingInterval": k["polling_interval_seconds"],
                         "minReplicaCount": F.MIN_REPLICAS, "maxReplicaCount": F.MAX_REPLICAS,
                         "advanced": {"horizontalPodAutoscalerConfig": {"name": f"keda-hpa-{target}",
                                                                        "behavior": scale_down_behavior()}},
                         "triggers": [{"type": "prometheus", "metricType": "AverageValue", "metadata": {
                             "serverAddress": metrics_query_url(p), "query": rps_query(target), "threshold": c,
                             "ignoreNullValues": "false"}}]}}
    return pa_object(p, target, labels, profile, c, reactive_only=arm == "PA-R", families=F.PA_FAMILIES[arm])


def pa_object(p: Dict, target: str, labels: Dict[str, str], profile: str, capacity: str, reactive_only: bool,
              families: Iterable[str]) -> Dict:
    """One PredictiveAutoscaler in Recommend, as every PA arm is rendered (and the demonstration's two targets, which
    reuse it): reactive_only writes the predictive extra as an explicit zero (PA-R: forecasts never add replicas),
    otherwise the frozen extra; `families` is spec.prediction.families; `capacity` the capacityPerReplica string."""
    ns = p["namespace"]
    extra = (0, 0) if reactive_only else (F.EXTRA_MAX_REPLICAS, F.EXTRA_MAX_PERCENT)
    budget = F.PROFILE_STARTUP_BUDGET[profile]
    c = capacity
    pa = {"apiVersion": "autoscaling.devkuban.com/v1alpha1", "kind": "PredictiveAutoscaler",
          "metadata": {"name": target, "namespace": ns, "labels": labels},
          "spec": {"mode": "Recommend", "targetDeployment": {"name": target, "namespace": ns},
                   "minReplicas": F.MIN_REPLICAS, "maxReplicas": F.MAX_REPLICAS,
                   "metrics": {"requests": {"enabled": True, "capacityPerReplica": c, "source": {
                       "preset": "prometheus",
                       "query": f'sum(rate(c28_ingress_requests_total{{target="{{{{ .Name }}}}"}}[{F.RATE_RANGE}]))'}}},
                   "prediction": {"enabled": True, "engine": "statistical",
                                  "sampleIntervalSeconds": F.SAMPLE_INTERVAL_S, "horizonMinutes": F.HORIZON_MINUTES,
                                  "updateIntervalSeconds": F.DECISION_INTERVAL_S,
                                  "settlementSeconds": F.SETTLEMENT_S, "outcomeGraceSeconds": F.GRACE_S,
                                  "risk": {"miscoverageBps": F.MISCOVERAGE_BPS},
                                  "calibration": {"minWindows": F.CALIBRATION_MIN, "maxScores": F.CALIBRATION_MAX,
                                                  "retentionDays": F.RETENTION_DAYS},
                                  "predictiveExtra": {"maxReplicas": extra[0], "maxPercent": extra[1]},
                                  "families": list(families)},
                   "behavior": {"scaleDown": {"headroomPercent": F.PA_HEADROOM_PERCENT},
                                "startup": {"fallbackDelaySeconds": budget, "minimumDelaySeconds": budget,
                                            "actuationAllowanceSeconds": F.ACTUATION_ALLOWANCE_S}}}}
    return pa


def render_arm(p: Dict, arm: str, profile: str) -> List[Dict]:
    validate(p)
    target = target_name(arm, profile)
    labels = _labels(p, arm, profile, target)
    return [*target_objects(p, target, labels, profile, FIXED_REPLICAS), _writer(p, arm, profile, target)]


def target_objects(p: Dict, target: str, labels: Dict[str, str], profile: str, replicas: int) -> List[Dict]:
    """One fixture target without its writer: the EMPTY fault ConfigMap, the Deployment (the calibrated fixture
    resources, probes and placement, identical for every arm and for the demonstration's targets) and the Service.
    The caller has validated the params (validate, or its stage)."""
    ns = p["namespace"]
    fx = p["fixture"]
    faults_cm = {"apiVersion": "v1", "kind": "ConfigMap",
                 "metadata": {"name": f"{target}-faults", "namespace": ns, "labels": labels},
                 "data": {"faults.json": "{}"}}
    env = [{"name": "MODE", "value": "server"}, {"name": "TARGET", "value": target},
           {"name": "WORK_ITERATIONS", "value": str(fx["work_iterations"])},
           {"name": "RESPONSE_BYTES", "value": str(fx["response_bytes"])},
           {"name": "MAX_INFLIGHT", "value": str(fx["max_inflight"])},
           {"name": "MAX_QUEUE", "value": str(fx["max_queue"])},
           {"name": "QUEUE_TIMEOUT_SECONDS", "value": str(fx["queue_timeout_seconds"])},
           {"name": "STARTUP_DELAY_SECONDS", "value": str(F.PROFILES[profile])},
           {"name": "FAULT_FILE", "value": "/etc/c28/faults.json"}]
    pod = {"metadata": {"labels": {**labels, **SIDECAR_FREE},
                        "annotations": {"prometheus.io/scrape": "true", "prometheus.io/port": "8080",
                                        **SIDECAR_FREE}},
           "spec": {"priorityClassName": p["priority_class"], "automountServiceAccountToken": False,
                    "securityContext": _SECURITY, "terminationGracePeriodSeconds": 10, **_placement(p, "workload"),
                    "containers": [{"name": "fixture", "image": p["image"], "env": env,
                                    "ports": [{"name": "http", "containerPort": 8080}],
                                    "readinessProbe": {"httpGet": {"path": "/ready", "port": "http"},
                                                       "periodSeconds": 1, "failureThreshold": 1,
                                                       "successThreshold": 1, "timeoutSeconds": 1},
                                    "livenessProbe": {"httpGet": {"path": "/healthz", "port": "http"},
                                                      "periodSeconds": 10, "failureThreshold": 6},
                                    "resources": _resources(fx), "securityContext": _CONTAINER_SECURITY,
                                    "volumeMounts": [{"name": "faults", "mountPath": "/etc/c28", "readOnly": True}]}],
                    "volumes": [{"name": "faults", "configMap": {"name": f"{target}-faults", "optional": True}}]}}
    deployment = {"apiVersion": "apps/v1", "kind": "Deployment",
                  "metadata": {"name": target, "namespace": ns, "labels": labels},
                  "spec": {"replicas": replicas, "revisionHistoryLimit": 2,
                           "selector": {"matchLabels": {TARGET_LABEL: target}},
                           "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 0, "maxUnavailable": 1}},
                           "template": pod}}
    service = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": target, "namespace": ns, "labels": labels},
               "spec": {"selector": {TARGET_LABEL: target}, "ports": [{"name": "http", "port": 80, "targetPort": "http"}]}}
    return [faults_cm, deployment, service]


def _proxy(p: Dict, name: str, upstream: str, port: int) -> List[Dict]:
    ns, labels = p["control_namespace"], {**_labels(p), "app.kubernetes.io/name": name}
    cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": f"{name}-faults", "namespace": ns, "labels": labels},
          "data": {"faults.json": "{}"}}
    dep = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name, "namespace": ns, "labels": labels},
           "spec": {"replicas": 1, "selector": {"matchLabels": {"app.kubernetes.io/name": name, RUN_LABEL: p["run_id"]}},
                    "template": {"metadata": {"labels": {**labels, **SIDECAR_FREE},
                                              "annotations": dict(SIDECAR_FREE)}, "spec": {
                        "automountServiceAccountToken": False, "securityContext": _SECURITY, **_placement(p, "support"),
                        "containers": [{"name": "proxy", "image": p["image"],
                                        "env": [{"name": "MODE", "value": "tcpproxy"}, {"name": "UPSTREAM", "value": upstream},
                                                {"name": "PORT", "value": str(port)}, {"name": "METRICS_PORT", "value": "9091"},
                                                {"name": "FAULT_FILE", "value": "/etc/c28/faults.json"}],
                                        "ports": [{"name": "route", "containerPort": port}, {"name": "metrics", "containerPort": 9091}],
                                        "readinessProbe": {"httpGet": {"path": "/healthz", "port": "metrics"}, "periodSeconds": 5},
                                        "resources": _resources(p["proxy"]), "securityContext": _CONTAINER_SECURITY,
                                        "volumeMounts": [{"name": "faults", "mountPath": "/etc/c28", "readOnly": True}]}],
                        "volumes": [{"name": "faults", "configMap": {"name": f"{name}-faults", "optional": True}}]}}}}
    svc = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": ns, "labels": labels},
           "spec": {"selector": {"app.kubernetes.io/name": name, RUN_LABEL: p["run_id"]},
                    "ports": [{"name": "route", "port": port, "targetPort": "route"},
                              {"name": "metrics", "port": 9091, "targetPort": "metrics"}]}}
    return [cm, dep, svc]


def render_common(p: Dict) -> List[Dict]:
    """Run-wide objects: the PriorityClass and its scoped ResourceQuota (the quota fault), the ingress (on the workload
    node, routing /t/<target>/ to every target of the run) and the two evaluation-owned fault routes."""
    validate(p)
    return common_objects(p, [target_name(a, pr) for pr in F.PROFILES for a in F.ARMS])


def common_objects(p: Dict, targets: List[str]) -> List[Dict]:
    """render_common's objects for an explicit target list (the ingress routes /t/<target>/ to exactly these). The
    caller has validated the params (validate, or its stage)."""
    ns, labels = p["namespace"], _labels(p)
    ups = ",".join(f"{t}=http://{t}.{ns}.svc" for t in targets)
    pc = {"apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClass", "metadata": {"name": p["priority_class"],
          "labels": labels}, "value": 0, "globalDefault": False, "preemptionPolicy": "Never",
          "description": "C2.8 evaluation fixture pods only (scopes the run's ResourceQuota)."}
    quota = {"apiVersion": "v1", "kind": "ResourceQuota",
             "metadata": {"name": p["quota"]["name"], "namespace": ns, "labels": labels},
             "spec": {"hard": {"pods": str(p["quota"]["pods_normal"])},
                      "scopeSelector": {"matchExpressions": [{"scopeName": "PriorityClass", "operator": "In",
                                                              "values": [p["priority_class"]]}]}}}
    ilabels = {**labels, "app.kubernetes.io/name": "c28-ingress"}
    token_volume, token_mount = ingress_token()
    ingress = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "c28-ingress", "namespace": ns, "labels": ilabels},
               "spec": {"replicas": 1, "selector": {"matchLabels": {"app.kubernetes.io/name": "c28-ingress", RUN_LABEL: p["run_id"]}},
                        "template": {"metadata": {"labels": {**ilabels, **SIDECAR_FREE},
                                                  "annotations": {"prometheus.io/scrape": "true", "prometheus.io/port": "8080",
                                                                  **SIDECAR_FREE}},
                                     "spec": {"serviceAccountName": "c28-ingress",
                                              "automountServiceAccountToken": False, "securityContext": _SECURITY,
                                              **_placement(p, "workload"),
                                              "containers": [{"name": "ingress", "image": p["image"],
                                                              "env": [{"name": "MODE", "value": "ingress"},
                                                                      {"name": "UPSTREAMS", "value": ups},
                                                                      {"name": "TIMEOUT_SECONDS", "value": str(F.REQUEST_TIMEOUT_S)},
                                                                      *ingress_routing_env()],
                                                              "ports": [{"name": "http", "containerPort": 8080}],
                                                              "readinessProbe": {"httpGet": {"path": "/ready", "port": "http"}, "periodSeconds": 5},
                                                              "resources": _resources(p["ingress"]),
                                                              "securityContext": _CONTAINER_SECURITY,
                                                              "volumeMounts": [token_mount]}],
                                              "volumes": [token_volume]}}}}
    isvc = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "c28-ingress", "namespace": ns, "labels": ilabels},
            "spec": {"selector": {"app.kubernetes.io/name": "c28-ingress", RUN_LABEL: p["run_id"]},
                     "ports": [{"name": "http", "port": 80, "targetPort": "http"}]}}
    out = [pc, quota, *ingress_rbac(ns, "c28-ingress", ilabels), ingress, isvc]
    out += _proxy(p, "c28-metrics-route", p["routes"]["metrics_upstream"], METRICS_ROUTE_PORT)
    out += _proxy(p, "c28-forecast-route", p["routes"]["forecast_upstream"], 8443)
    if p["release_network_policies"]:
        out += _route_policies(p)
    if p["scrape"] != "annotations":
        group = "monitoring.coreos.com/v1" if p["scrape"] == "PodMonitor" else "operator.victoriametrics.com/v1beta1"
        endpoints_key = "podMetricsEndpoints"
        out.append({"apiVersion": group, "kind": p["scrape"], "metadata": {"name": "c28-ingress", "namespace": ns, "labels": labels},
                    "spec": {"selector": {"matchLabels": {"app.kubernetes.io/name": "c28-ingress", RUN_LABEL: p["run_id"]}},
                             endpoints_key: [{"port": "http", "path": "/metrics",
                                              "interval": f"{F.SCRAPE_INTERVAL_S}s"}]}})
    return out


def render_all(p: Dict) -> List[Dict]:
    objs = render_all_unchecked(p)
    audit_rendered(objs)
    audit_sidecar_free(objs)
    audit_rbac(objs)
    if "residency" in p:
        check_residency(p, objs)
    return objs


# --- sidecar-free audit ---------------------------------------------------------------------------------------------

def pod_templates(obj: Dict) -> List[Dict]:
    """The Pod templates an object carries (Deployment/StatefulSet/DaemonSet/ReplicaSet/Job .spec.template, a CronJob's
    jobTemplate, or a Pod itself)."""
    kind, spec = obj.get("kind"), obj.get("spec") or {}
    if kind == "Pod":
        return [obj]
    if kind == "CronJob":
        return [((spec.get("jobTemplate") or {}).get("spec") or {}).get("template") or {}]
    if kind in ("Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job"):
        return [spec.get("template") or {}]
    return []


def audit_sidecar_free(objs: List[Dict]) -> None:
    """Every rendered Pod template carries sidecar.istio.io/inject: "false" as a label AND an annotation on the POD
    metadata (the namespace keeps its injection policy; the workload's own metadata does not reach the pods)."""
    bad = []
    for o in objs:
        for t in pod_templates(o):
            md = t.get("metadata") or {}
            missing = [part for part in ("labels", "annotations") if (md.get(part) or {}).get(SIDECAR_LABEL) != "false"]
            if missing:
                bad.append(f"{o['kind']}/{o['metadata']['name']} ({' and '.join(missing)})")
    if bad:
        raise ParamsError(f"Pod templates without the {SIDECAR_LABEL}: \"false\" label and annotation (the "
                          f"evaluation is sidecar-free): {', '.join(bad)}")


# --- RBAC audit -------------------------------------------------------------------------------------------------------

def _env_of(container: Dict) -> Dict[str, str]:
    return {e.get("name"): e.get("value") for e in container.get("env") or []}


def audit_rbac(objs: List[Dict]) -> None:
    """Least privilege, refused otherwise: no cluster-scoped RBAC object; every Role grants exactly get/list/watch on
    EndpointSlices; every RoleBinding binds a rendered Role to exactly one rendered ServiceAccount of its own
    namespace; no ServiceAccount (and no pod) mounts a token automatically; a pod runs as a non-default ServiceAccount
    and carries a service-account token only when it is an endpoints-rr ingress, and then exactly ingress_token()'s."""
    bad = []
    by = lambda kind: {(o["metadata"].get("namespace"), o["metadata"]["name"]): o  # noqa: E731
                       for o in objs if o.get("kind") == kind}
    sas, roles = by("ServiceAccount"), by("Role")
    for o in objs:
        kind, md = o.get("kind"), o.get("metadata") or {}
        where = f"{kind}/{md.get('name')}"
        if kind in ("ClusterRole", "ClusterRoleBinding"):
            bad.append(f"{where}: no cluster-scoped RBAC")
        elif kind == "ServiceAccount" and o.get("automountServiceAccountToken") is not False:
            bad.append(f"{where}: automountServiceAccountToken must be false")
        elif kind == "Role" and o.get("rules") != ENDPOINTSLICE_RULES:
            bad.append(f"{where}: rules must be exactly get/list/watch on discovery.k8s.io endpointslices")
        elif kind == "RoleBinding":
            ns, ref, subs = md.get("namespace"), o.get("roleRef") or {}, o.get("subjects") or []
            if ref != {"apiGroup": RBAC_GROUP, "kind": "Role", "name": ref.get("name")} or (ns, ref.get("name")) not in roles:
                bad.append(f"{where}: roleRef must be a rendered Role of its namespace")
            if len(subs) != 1 or set(subs[0]) != {"kind", "name", "namespace"} or subs[0]["kind"] != "ServiceAccount" \
                    or subs[0]["namespace"] != ns or (ns, subs[0]["name"]) not in sas:
                bad.append(f"{where}: subjects must be exactly one rendered ServiceAccount of its namespace")
        for t in pod_templates(o):
            spec = t.get("spec") or {}
            if spec.get("automountServiceAccountToken") is not False:
                bad.append(f"{where}: pods must not mount a token automatically")
            cs = spec.get("containers") or []
            ingress = len(cs) == 1 and _env_of(cs[0]).get("MODE") == "ingress"
            rr = ingress and _env_of(cs[0]).get("ROUTING") == INGRESS_ROUTING
            sa = spec.get("serviceAccountName", "default")
            tokens = [v for v in spec.get("volumes") or [] if any("serviceAccountToken" in src for src in
                                                                   (v.get("projected") or {}).get("sources") or [])]
            if ingress and [_env_of(cs[0]).get(k) for k, _ in INGRESS_ROUTING_ENV] != [v for _, v in INGRESS_ROUTING_ENV]:
                bad.append(f"{where}: the ingress must set {dict(INGRESS_ROUTING_ENV)} explicitly")
            if rr:
                vol, mount = ingress_token()
                if (md.get("namespace"), sa) not in sas or tokens != [vol] or \
                        [m for m in cs[0].get("volumeMounts") or [] if m.get("name") == SA_TOKEN_VOLUME] != [mount]:
                    bad.append(f"{where}: the endpoints-rr ingress runs as its rendered ServiceAccount with exactly "
                               "the projected token")
            elif sa != "default" or tokens:
                bad.append(f"{where}: only the endpoints-rr ingress runs as a ServiceAccount with a token")
    if bad:
        raise ParamsError("RBAC audit: " + "; ".join(bad))


def unexpected_injection(pods: List[Dict]) -> List[str]:
    """Admitted evaluation pods that nonetheless carry Istio's containers (regular or init): one problem per pod."""
    out = []
    for p in pods:
        spec = p.get("spec") or {}
        names = [c.get("name") for c in (spec.get("initContainers") or []) + (spec.get("containers") or [])]
        hit = [n for n in names if n in INJECTED_CONTAINERS]
        if hit:
            m = p.get("metadata") or {}
            out.append(f"Pod {m.get('namespace')}/{m.get('name')} has injected containers {hit} (unexpected injection)")
    return out


# --- residency budget -----------------------------------------------------------------------------------------------

def _pod_requests(template: Dict) -> Tuple[Decimal, Decimal]:
    """A pod's effective requests: the sum over its containers, or the largest init container if that is larger."""
    spec = template.get("spec") or {}

    def total(cs):
        cpu = sum((cpu_millicores(((c.get("resources") or {}).get("requests") or {}).get("cpu", "0")) for c in cs),
                  Decimal(0))
        mem = sum((memory_bytes(((c.get("resources") or {}).get("requests") or {}).get("memory", "0")) for c in cs),
                  Decimal(0))
        return cpu, mem
    run = total(spec.get("containers") or [])
    inits = [total([c]) for c in spec.get("initContainers") or []]
    return (max([run[0]] + [i[0] for i in inits]), max([run[1]] + [i[1] for i in inits]))


def _role(template: Dict, p: Dict) -> str:
    """The reservation role (workload/support) a template's required node affinity pins it to."""
    role_label = p["reservation"]["role_label"]
    terms = (((template.get("spec") or {}).get("affinity") or {}).get("nodeAffinity") or {}).get(
        "requiredDuringSchedulingIgnoredDuringExecution", {}).get("nodeSelectorTerms", [])
    for t in terms:
        for e in t.get("matchExpressions", []):
            if e.get("key") == role_label and e.get("operator") == "In" and len(e.get("values", [])) == 1:
                return e["values"][0]
    raise ParamsError("a rendered pod template is not pinned to a reservation role; its residency is unknown")


def _mi(b: Decimal) -> str:
    return f"{format((Decimal(b) / 2**20).quantize(Decimal('0.1')).normalize(), 'f')}Mi"


def _m(cpu: Decimal) -> str:
    return f"{format(Decimal(cpu).normalize(), 'f')}m"


def residency_report(p: Dict, objs: List[Dict] = None, chart: Optional[Dict] = None, *, all_loaded: bool = False,
                     max_replicas: Optional[int] = None, generator_jobs: bool = True) -> Dict:
    """The evaluation's maximum resident requests on W and S against 75 % of each node's allocatable.

    The keyword options serve a stage whose targets are all loaded at once and whose generator is a rendered workload
    (the demonstration, demo.residency_report); the defaults are the comparison experiment's accounting, unchanged:
    all_loaded counts EVERY fixture target at max_replicas (default the frozen maximum) plus overlap_pods, instead of
    one loaded arm and the others idle; generator_jobs=False leaves out the budgeted generator Jobs (the rendered
    generator is counted from its own pod template, like every other rendered pod on S).

    W: the ten fixture arms, all but one at idle_arm_replicas, the loaded arm (the largest per-pod request, so the worst
       case) at maxReplicas plus overlap_pods, plus every other rendered pod pinned to W (the ingress) at its replicas.
    S: every rendered pod pinned to S (the two fault routes) at its replicas, the operator and the forecaster as the
       evaluation release's FINAL chart values request them (`chart`; default the chart's values.yaml merged with
       eval_release_values), the generator Jobs that may run at the same time, and support_extra. A chart pod that
       requests less than its declared budget is a problem: the budget never understates the deployed pods.
    Each node adds its system_requests. CPU and memory are checked separately; problems lists every exceedance."""
    r = residency_params(p)
    objs = render_all_unchecked(p) if objs is None else objs
    contrib: Dict[str, List[Dict]] = {"workload": [], "support": []}
    sums: Dict[str, List[Decimal]] = {"workload": [Decimal(0), Decimal(0)], "support": [Decimal(0), Decimal(0)]}

    def add(role, name, pods, cpu, mem, note=""):
        sums[role][0] += cpu * pods
        sums[role][1] += mem * pods
        contrib[role].append({"name": name, "pods": pods, "per_pod_cpu": _m(cpu), "per_pod_memory": _mi(mem),
                              "cpu_m": float(cpu * pods), "memory_bytes": int(mem * pods),
                              **({"note": note} if note else {})})
    arms = []
    for o in objs:
        for t in pod_templates(o):
            role = _role(t, p)
            cpu, mem = _pod_requests(t)
            name = f"{o['kind']}/{o['metadata']['name']}"
            if o["kind"] == "Deployment" and (o["metadata"].get("labels") or {}).get(ARM_LABEL):
                if role != "workload":
                    raise ParamsError(f"{name} is a fixture arm pinned to {role}, not the workload node")
                arms.append((name, cpu, mem))
            else:
                replicas = 1 if o["kind"] in ("Pod", "Job", "CronJob") else int(o["spec"].get("replicas", 1))
                add(role, name, replicas, cpu, mem)
    top = F.MAX_REPLICAS if max_replicas is None else _count(max_replicas, "max_replicas", 1, F.MAX_REPLICAS)
    if arms and all_loaded:
        for name, cpu, mem in arms:
            add("workload", name, top, cpu, mem, "loaded target at maxReplicas (every target is loaded at once)")
            add("workload", f"{name} overlap", r["overlap_pods"], cpu, mem, "rollout/termination overlap")
    elif arms:
        idle, overlap = r["idle_arm_replicas"], r["overlap_pods"]
        loaded = max(arms, key=lambda a: (a[1], a[2]))
        for name, cpu, mem in arms:
            if name == loaded[0]:
                add("workload", name, F.MAX_REPLICAS, cpu, mem, "loaded arm at maxReplicas (worst-case arm)")
                add("workload", f"{name} overlap", overlap, cpu, mem, "startup/termination overlap of the loaded arm")
            else:
                add("workload", name, idle, cpu, mem, "idle arm")
    chart = chart_values(eval_release_values(p)) if chart is None else chart
    under = []
    for name, replicas, cpu, mem in chart_pods(chart):
        add("support", f"{name} (chart)", replicas, cpu, mem)
        want = r[name]
        if cpu < want[0] or mem < want[1]:
            under.append(f"the evaluation release's {name} requests {_m(cpu)}/{_mi(mem)}, below its declared budget "
                         f"{_m(want[0])}/{_mi(want[1])} (residency.{name}): the chart values must request it")
    if generator_jobs:
        add("support", "generator Jobs", r["generator_concurrent"], *r["generator"], "concurrent generators")
    for name, replicas, cpu, mem in r["support_extra"]:
        add("support", name, replicas, cpu, mem)
    out = {"limit_fraction": F.RESIDENCY_LIMIT_FRACTION, "idle_arm_replicas": r["idle_arm_replicas"],
           "overlap_pods": r["overlap_pods"], "nodes": {}, "problems": list(under)}
    limit = Decimal(str(F.RESIDENCY_LIMIT_FRACTION))
    for role in ("workload", "support"):
        alloc, system = r["nodes"][role]["allocatable"], r["nodes"][role]["system"]
        add(role, "node system pods", 1, *system, "DaemonSets and other system agents (params)")
        cpu, mem = sums[role]
        node = {"node": p["nodes"][role], "allocatable": {"cpu_m": float(alloc[0]), "memory_bytes": int(alloc[1])},
                "total": {"cpu_m": float(cpu), "memory_bytes": int(mem)},
                "fraction": {"cpu": float(cpu / alloc[0]), "memory": float(mem / alloc[1])},
                "contributions": contrib[role]}
        out["nodes"][role] = node
        listing = "; ".join(f"{c['name']} {c['pods']}x({c['per_pod_cpu']}, {c['per_pod_memory']})"
                            for c in contrib[role])
        if cpu > alloc[0] * limit:
            out["problems"].append(f"{role} node {p['nodes'][role]}: CPU requests {_m(cpu)} are "
                                   f"{float(cpu / alloc[0]):.1%} of allocatable {_m(alloc[0])}, above "
                                   f"{F.RESIDENCY_LIMIT_FRACTION:.0%}; contributions: {listing}")
        if mem > alloc[1] * limit:
            out["problems"].append(f"{role} node {p['nodes'][role]}: memory requests {_mi(mem)} are "
                                   f"{float(mem / alloc[1]):.1%} of allocatable {_mi(alloc[1])}, above "
                                   f"{F.RESIDENCY_LIMIT_FRACTION:.0%}; contributions: {listing}")
    return out


def check_residency(p: Dict, objs: List[Dict] = None) -> Dict:
    rep = residency_report(p, objs)
    if rep["problems"]:
        raise ParamsError("residency budget exceeded (evaluation + system requests > 75 % of allocatable): "
                          + " | ".join(rep["problems"]))
    return rep


def render_all_unchecked(p: Dict) -> List[Dict]:
    """Every evaluation object, without the audits (render_all runs them)."""
    objs = render_common(p)
    for profile in F.PROFILES:
        for arm in F.ARMS:
            objs += render_arm(p, arm, profile)
    return objs


# --- exclusive-writer audit ---------------------------------------------------------------------------------------

def _writer_targets(obj: Dict) -> Iterable[Tuple[str, str]]:
    """(namespace, deployment) pairs an object writes the replica count of."""
    kind, meta, spec = obj.get("kind"), obj.get("metadata", {}), obj.get("spec", {})
    ns = meta.get("namespace", "")
    if kind == "HorizontalPodAutoscaler" and spec.get("scaleTargetRef", {}).get("kind", "Deployment") == "Deployment":
        yield ns, spec["scaleTargetRef"]["name"]
    elif kind == "ScaledObject":
        yield ns, spec.get("scaleTargetRef", {}).get("name", "")
    elif kind == "PredictiveAutoscaler":
        td = spec.get("targetDeployment", {})
        yield td.get("namespace") or ns, td.get("name", "")


def audit_rendered(objs: List[Dict]) -> None:
    """Every rendered Deployment has exactly one writer, and no writer targets anything else."""
    deps = {(o["metadata"]["namespace"], o["metadata"]["name"]) for o in objs if o["kind"] == "Deployment"
            and o["metadata"].get("labels", {}).get(ARM_LABEL)}
    writers: Dict[Tuple[str, str], List[str]] = {d: [] for d in deps}
    for o in objs:
        for t in _writer_targets(o):
            if t not in writers:
                raise ParamsError(f"{o['kind']}/{o['metadata']['name']} writes {t}, which is not a fixture target")
            writers[t].append(f"{o['kind']}/{o['metadata']['name']}")
    bad = {t: w for t, w in writers.items() if len(w) != 1}
    if bad:
        raise ParamsError(f"targets without exactly one writer: {bad}")


def audit_live(targets: Dict[Tuple[str, str], str], live: List[Dict]) -> List[str]:
    """Problems with the live writers of the fixture targets (empty when exclusive). `targets` maps (namespace, name)
    to the expected writer "Kind/name"; `live` is every PredictiveAutoscaler (both API groups), HPA and ScaledObject in
    the cluster as JSON. A KEDA-generated HPA counts as its ScaledObject (owner reference), nothing else does."""
    found: Dict[Tuple[str, str], List[str]] = {t: [] for t in targets}
    for o in live:
        meta = o.get("metadata", {})
        owners = [r for r in meta.get("ownerReferences", []) if r.get("kind") == "ScaledObject" and r.get("controller")]
        for t in _writer_targets(o):
            if t not in found:
                continue
            name = f"{o['kind']}/{meta.get('name')}"
            if o["kind"] == "HorizontalPodAutoscaler" and owners:
                name = f"ScaledObject/{owners[0]['name']}"
            if name not in found[t]:
                found[t].append(name)
    problems = []
    for t, expected in targets.items():
        if found[t] != [expected]:
            problems.append(f"{t[0]}/{t[1]}: writers {found[t] or 'none'}, expected exactly {expected}")
    return problems


def expected_writers(p: Dict) -> Dict[Tuple[str, str], str]:
    kinds = {"H-CPU": "HorizontalPodAutoscaler", "H-RPS": "ScaledObject"}
    return {(p["namespace"], target_name(a, pr)): f"{kinds.get(a, 'PredictiveAutoscaler')}/{target_name(a, pr)}"
            for pr in F.PROFILES for a in F.ARMS}


def with_faults(objs: List[Dict], faults: Dict, arm_targets: Iterable[str]) -> List[Dict]:
    """Copies of the fault ConfigMaps filled for one scored block (written before the block starts)."""
    import json
    out = []
    for o in objs:
        if o["kind"] != "ConfigMap":
            continue
        name = o["metadata"]["name"]
        data = None
        if name == "c28-metrics-route-faults":
            data = faults["metrics_route"]
        elif name == "c28-forecast-route-faults":
            data = faults["forecast_route"]
        elif name.endswith("-faults") and name[:-len("-faults")] in set(arm_targets):
            data = faults["slow_start"]
        if data is not None:
            c = copy.deepcopy(o)
            c["data"]["faults.json"] = json.dumps(data, sort_keys=True)
            out.append(c)
    return out
