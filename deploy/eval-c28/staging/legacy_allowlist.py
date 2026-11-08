"""The exact legacy benchmark objects on prodcluster (STAGING-C28-PROD.md S0/S4), from the repository's
deploy/prodcluster overlays (k8s-manifests/base, deploy/eks-benchmark demo and experiments, the prodcluster
ml-engine additions) at the campaign's images (commit 0472829: PredictiveAutoscalers in the LEGACY API group
autoscaler.example.com/v1alpha1, the operator's Lease "predictive-autoscaler-leader").

Each entry is (kind, namespace, name, role). Roles decide what S4 may do, in this order:
  launcher   CronJobs that start training/evidence Jobs: suspend (their active Jobs finish within the deadline);
  generator  the k6 Deployments: scale to 0 (arrivals stop first);
  operator   the old operator Deployment: scale to 0 only when every legacy PA is one of these (no other consumer);
  lease      the old operator's Lease: observed only (renewals must stop);
  keda       the benchmark ScaledObjects: deleted with a UID precondition after sources are stopped (the shared KEDA
             operator keeps running); their generated HPAs (keda-hpa-<name>) must disappear with them;
  pa         the legacy PredictiveAutoscalers: kept as archived objects (their operator is stopped);
  target     the arm Deployments: parked (normally 0 replicas) only when no writer remains;
  api        the old forecasting API: scaled to 0 after the operator, the trainers and active Jobs are done;
  retained   evidence and model claims, the legacy CRD: never mutated, never deleted.
UIDs are bound live by the plan (inventory), never written here. Anything in demo/ml-engine that is not listed is
reported as unclassified, and an unlisted writer or legacy PA blocks the plan.
"""

LEGACY_GROUP = "autoscaler.example.com"
LEGACY_LEASE = "predictive-autoscaler-leader"
ARMS = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
PA_ARMS = ("nginx-test", "nginx-reactive", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
KEDA_OBJECTS = ("nginx-test-keda-fallback", "myapptwo-keda-fallback")

ALLOWLIST = (
    [("cronjobs", "ml-engine", n, "launcher") for n in ("ml-training", "evidence-archive", "load-evidence")]
    + [("deployments", "demo", f"k6-{a}", "generator") for a in ARMS]
    + [("deployments", "ml-engine", "predictive-operator", "operator"),
       ("leases", "ml-engine", LEGACY_LEASE, "lease")]
    + [("scaledobjects", "demo", n, "keda") for n in KEDA_OBJECTS]
    + [("pa-legacy", "demo", f"{a}-autoscaler", "pa") for a in PA_ARMS]
    + [("deployments", "demo", a, "target") for a in ARMS]
    + [("deployments", "ml-engine", "ml-api", "api")]
    + [("persistentvolumeclaims", "ml-engine", n, "retained")
       for n in ("ml-models-pvc", "forecast-log-pvc", "evidence-archive-pvc", "load-evidence-pvc", "p8-evidence-pvc")]
    + [("customresourcedefinitions", None, f"predictiveautoscalers.{LEGACY_GROUP}", "retained")]
)
NAMESPACES = ("demo", "ml-engine")
ROLE_ORDER = ("launcher", "generator", "operator", "keda", "target", "api")


def generated_hpa(scaled_object: str) -> str:
    """KEDA's default name for a ScaledObject's HPA (none of the benchmark objects overrides it)."""
    return f"keda-hpa-{scaled_object}"


def entries(role=None):
    return [e for e in ALLOWLIST if role is None or e[3] == role]
