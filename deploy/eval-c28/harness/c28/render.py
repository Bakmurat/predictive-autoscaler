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

FIXED_REPLICAS = 2          # every target's fixed replica count outside a session (the common starting count, section 5)

REQUIRED = ("schema", "run_id", "namespace", "control_namespace", "release", "image", "c0_rps", "nodes", "reservation",
            "fixture", "ingress", "proxy", "hpa_cpu_target_percent", "keda", "routes", "priority_class", "quota",
            "scrape", "release_network_policies")


class ParamsError(ValueError):
    pass


def _placeholder(v) -> bool:
    return isinstance(v, str) and v.strip().startswith("<")


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
    missing = [k for k in REQUIRED if k not in p]
    if missing:
        raise ParamsError(f"missing parameters: {', '.join(missing)}")
    if p["schema"] != "c28-params/v1":
        raise ParamsError("schema must be c28-params/v1")
    for key in ("run_id", "namespace", "control_namespace", "priority_class"):
        if not _DNS.match(str(p[key])) or len(str(p[key])) > 40:
            raise ParamsError(f"{key} must be a short DNS label")
    if not _DIGEST_IMAGE.match(p["image"]):
        raise ParamsError("image must be pinned by digest (no tags, never latest)")
    if not (isinstance(p["c0_rps"], (int, float)) and p["c0_rps"] > 0):
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
    if p["hpa_cpu_target_percent"] not in F.HPA_CPU_CANDIDATES:
        raise ParamsError(f"hpa_cpu_target_percent must be one of the tuning candidates {F.HPA_CPU_CANDIDATES}")
    if p["scrape"] not in ("PodMonitor", "VMPodScrape", "annotations"):
        raise ParamsError("scrape must be PodMonitor, VMPodScrape or annotations")
    if not _DNS.match(str(p["release"])):
        raise ParamsError("release must be the evaluation Helm release name")
    if not isinstance(p["release_network_policies"], bool):
        raise ParamsError("release_network_policies must say whether the evaluation release enforces NetworkPolicies")
    route = f"http://{route_host(p, 'metrics')}:8481"
    if not (str(p["keda"]["server_address"]) == route or str(p["keda"]["server_address"]).startswith(route + "/")):
        raise ParamsError(f"keda.server_address must go through the evaluation metrics route ({route}), so the RPS "
                          "outage reaches H-RPS")
    if "residency" in p:
        residency_params(p)


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


def eval_release_values(p: Dict) -> Dict:
    """The evaluation release's chart values that bind it to the routes: metrics through the metrics route, the
    forecaster through the forecast route with the route's name in the certificate (verification stays on)."""
    validate(p)
    fr = route_host(p, "forecast")
    out = {"prometheus": {"url": f"http://{route_host(p, 'metrics')}:8481"},
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
                             "serverAddress": k["server_address"], "query": rps_query(target), "threshold": c,
                             "ignoreNullValues": "false"}}]}}
    extra = (0, 0) if arm == "PA-R" else (F.EXTRA_MAX_REPLICAS, F.EXTRA_MAX_PERCENT)
    budget = F.PROFILE_STARTUP_BUDGET[profile]
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
                                  "families": list(F.PA_FAMILIES[arm])},
                   "behavior": {"scaleDown": {"headroomPercent": F.PA_HEADROOM_PERCENT},
                                "startup": {"fallbackDelaySeconds": budget, "minimumDelaySeconds": budget,
                                            "actuationAllowanceSeconds": F.ACTUATION_ALLOWANCE_S}}}}
    return pa


def render_arm(p: Dict, arm: str, profile: str) -> List[Dict]:
    validate(p)
    target = target_name(arm, profile)
    ns, labels = p["namespace"], _labels(p, arm, profile, target)
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
                  "spec": {"replicas": FIXED_REPLICAS, "revisionHistoryLimit": 2,
                           "selector": {"matchLabels": {TARGET_LABEL: target}},
                           "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 0, "maxUnavailable": 1}},
                           "template": pod}}
    service = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": target, "namespace": ns, "labels": labels},
               "spec": {"selector": {TARGET_LABEL: target}, "ports": [{"name": "http", "port": 80, "targetPort": "http"}]}}
    return [faults_cm, deployment, service, _writer(p, arm, profile, target)]


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
    ns, labels = p["namespace"], _labels(p)
    targets = [target_name(a, pr) for pr in F.PROFILES for a in F.ARMS]
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
    ingress = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "c28-ingress", "namespace": ns, "labels": ilabels},
               "spec": {"replicas": 1, "selector": {"matchLabels": {"app.kubernetes.io/name": "c28-ingress", RUN_LABEL: p["run_id"]}},
                        "template": {"metadata": {"labels": {**ilabels, **SIDECAR_FREE},
                                                  "annotations": {"prometheus.io/scrape": "true", "prometheus.io/port": "8080",
                                                                  **SIDECAR_FREE}},
                                     "spec": {"automountServiceAccountToken": False, "securityContext": _SECURITY,
                                              **_placement(p, "workload"),
                                              "containers": [{"name": "ingress", "image": p["image"],
                                                              "env": [{"name": "MODE", "value": "ingress"},
                                                                      {"name": "UPSTREAMS", "value": ups},
                                                                      {"name": "TIMEOUT_SECONDS", "value": str(F.REQUEST_TIMEOUT_S)}],
                                                              "ports": [{"name": "http", "containerPort": 8080}],
                                                              "readinessProbe": {"httpGet": {"path": "/ready", "port": "http"}, "periodSeconds": 5},
                                                              "resources": _resources(p["ingress"]),
                                                              "securityContext": _CONTAINER_SECURITY}]}}}}
    isvc = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "c28-ingress", "namespace": ns, "labels": ilabels},
            "spec": {"selector": {"app.kubernetes.io/name": "c28-ingress", RUN_LABEL: p["run_id"]},
                     "ports": [{"name": "http", "port": 80, "targetPort": "http"}]}}
    out = [pc, quota, ingress, isvc]
    out += _proxy(p, "c28-metrics-route", p["routes"]["metrics_upstream"], 8481)
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


def residency_report(p: Dict, objs: List[Dict] = None, chart: Optional[Dict] = None) -> Dict:
    """The evaluation's maximum resident requests on W and S against 75 % of each node's allocatable.

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
    if arms:
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
