"""Kubernetes manifests for the C2.8 fixture and its five arms (EVAL-C28-MANIFEST sections 3, 4 and 11).

One Deployment per (arm, startup profile), each with a new name (and so a new UID), its own Service, fault ConfigMap
and exactly one replica writer: an HPA (H-CPU), a KEDA ScaledObject whose generated HPA is the writer (H-RPS), or a
PredictiveAutoscaler (PA-R, PA-N, PA-E). Fixture pods tolerate only the run's own NoSchedule taint and require the
workload node by role label and hostname; the ingress sits on the workload node too, the fault routes on the support
node. Every PredictiveAutoscaler is rendered in Recommend mode: switching an arm to Active is the executor's gated
step (section 1, prerequisite 3), never a rendering default.
"""

from __future__ import annotations

import copy
import re
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Dict, Iterable, List, Tuple

from . import frozen as F

RUN_LABEL = "c28.devkuban.com/run"
ARM_LABEL = "c28.devkuban.com/arm"
PROFILE_LABEL = "c28.devkuban.com/profile"
TARGET_LABEL = "c28.devkuban.com/target"

_DNS = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$")
_QUANTITY = re.compile(r"^[0-9]+(\.[0-9]+)?(m|Mi|Gi|Ki)?$")

REQUIRED = ("schema", "run_id", "namespace", "control_namespace", "release", "image", "c0_rps", "nodes", "reservation",
            "fixture", "ingress", "proxy", "hpa_cpu_target_percent", "keda", "routes", "priority_class", "quota",
            "scrape", "release_network_policies")


class ParamsError(ValueError):
    pass


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
            if not _QUANTITY.match(str(p[res].get(k, ""))):
                raise ParamsError(f"{res}.{k} is not a quantity")
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


def route_host(p: Dict, kind: str) -> str:
    """The in-cluster name of an evaluation-owned route (also the TLS name for the forecast route)."""
    return f"c28-{kind}-route.{p['control_namespace']}.svc"


def eval_release_values(p: Dict) -> Dict:
    """The evaluation release's chart values that bind it to the routes: metrics through the metrics route, the
    forecaster through the forecast route with the route's name in the certificate (verification stays on)."""
    validate(p)
    fr = route_host(p, "forecast")
    return {"prometheus": {"url": f"http://{route_host(p, 'metrics')}:8481"},
            "operator": {"watchNamespaces": [p["namespace"]], "forecasterAddress": f"{fr}:8443"},
            "forecaster": {"engine": "statistical", "tls": {"extraDNSNames": [fr]}}}


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
    pod = {"metadata": {"labels": labels,
                        "annotations": {"prometheus.io/scrape": "true", "prometheus.io/port": "8080"}},
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
                  "spec": {"replicas": 2, "revisionHistoryLimit": 2,
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
                    "template": {"metadata": {"labels": labels}, "spec": {
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
                        "template": {"metadata": {"labels": ilabels,
                                                  "annotations": {"prometheus.io/scrape": "true", "prometheus.io/port": "8080"}},
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
    objs = render_common(p)
    for profile in F.PROFILES:
        for arm in F.ARMS:
            objs += render_arm(p, arm, profile)
    audit_rendered(objs)
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
