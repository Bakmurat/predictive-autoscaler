#!/usr/bin/env bash
# Install or update the soak (soak/README.md) on an EXISTING local kind cluster.
#   KUBECONFIG=<own file> soak/up.sh <operator-image:tag> <forecaster-image:tag> [<state-dir>]
# The images must already be loaded into the kind nodes (kind load docker-image). Refuses any cluster that is not a
# local kind cluster reached on 127.0.0.1: the soak never touches a lab or production cluster.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OP="${1:?operator image}"; FC="${2:?forecaster image}"
STATE="${3:-${XDG_STATE_HOME:-$HOME/.local/state}/predictive-autoscaler-soak}"
VM_IMAGE=victoriametrics/victoria-metrics:v1.153.0
NS=predictive-autoscaler
RELEASE=predictive-autoscaler
log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

# --- refuse anything but a local kind cluster -----------------------------------------------------------------------
[ -n "${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ] || { echo "set KUBECONFIG to the soak cluster's own file" >&2; exit 2; }
case "$(cd "$(dirname "$KUBECONFIG")" && pwd)/$(basename "$KUBECONFIG")" in
  "$HOME/.kube/config") echo "refusing the default kubeconfig; use the soak cluster's own file" >&2; exit 2 ;;
esac
CTX="$(kubectl config current-context)"
SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$CTX" =~ ^kind- ]] && [[ "$SERVER" =~ ^https://127\.0\.0\.1:[0-9]+$ ]] || {
  echo "refusing: context $CTX at $SERVER is not a local kind cluster" >&2; exit 2; }
k() { kubectl --request-timeout=20s --context "$CTX" "$@"; }
h() { helm --kube-context "$CTX" "$@"; }
if k get nodes -o name | grep -q -v -E '^node/[a-z0-9-]+-(control-plane|worker[0-9]*)$'; then
  echo "refusing: node names are not kind's" >&2; exit 2
fi
for img in "$OP" "$FC"; do [[ "$img" == *:* && "$img" != *@* ]] || { echo "expected repository:tag: $img" >&2; exit 2; }; done

umask 077; mkdir -p "$STATE"
log "soak on $CTX ($SERVER); state $STATE"
{ echo "commit $(git -C "$ROOT" rev-parse HEAD)"; echo "operator $OP"; echo "forecaster $FC"; echo "installed_at $(date -u +%FT%TZ)"; } \
  > "$STATE/install-$(date -u +%Y%m%dT%H%M%SZ).txt"

# --- namespaces, scripts, metrics store -----------------------------------------------------------------------------
for n in monitoring pa-soak; do k create namespace "$n" --dry-run=client -o yaml | k apply -f - >/dev/null; done
k -n pa-soak create configmap soak-scripts --from-file="$ROOT/soak/app.py" --dry-run=client -o yaml | k apply -f -
k -n monitoring create configmap soak-scripts --from-file="$ROOT/soak/traffic.py" --from-file="$ROOT/hack/smoke/synth.py" \
  --dry-run=client -o yaml | k apply -f -
python3 - "$ROOT/quickstart/backend.yaml" <<'PY' | k apply -f -
import sys
docs = open(sys.argv[1]).read().split("\n---\n")
print("\n---\n".join(d for d in docs if "name: vm," in d))     # the store and its Service; not the quickstart traffic
PY

# --- workloads: active (Active), recommend (Recommend), conflict (Active; HPA injected by check C2) --------------
python3 - "$FC" <<'PY' | k apply -f -
import json, sys
image = sys.argv[1]
for name in ("active", "recommend", "conflict"):
    labels = {"app": name, "devkuban.com/scaling-owner": "predictive-autoscaler"}
    print(json.dumps({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name, "namespace": "pa-soak", "labels": labels},
        "spec": {"selector": {"matchLabels": {"app": name}},
                 "template": {"metadata": {"labels": {"app": name}}, "spec": {
                     "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "seccompProfile": {"type": "RuntimeDefault"}},
                     "containers": [{"name": "web", "image": image, "imagePullPolicy": "Never", "command": ["python3", "/soak/app.py"],
                                     "env": [{"name": "SOAK_NAMESPACE", "value": "pa-soak"}, {"name": "SOAK_SERVICE", "value": name}],
                                     "ports": [{"containerPort": 8000}], "readinessProbe": {"httpGet": {"path": "/health", "port": 8000}},
                                     "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"memory": "128Mi"}},
                                     "securityContext": {"readOnlyRootFilesystem": True, "allowPrivilegeEscalation": False,
                                                         "capabilities": {"drop": ["ALL"]}},
                                     "volumeMounts": [{"name": "scripts", "mountPath": "/soak", "readOnly": True}]}],
                     "volumes": [{"name": "scripts", "configMap": {"name": "soak-scripts"}}]}}}}))
    print("---")
    for svc, extra in ((name, {}), (name + "-metrics", {"clusterIP": "None"})):
        print(json.dumps({"apiVersion": "v1", "kind": "Service", "metadata": {"name": svc, "namespace": "pa-soak"},
                          "spec": dict({"selector": {"app": name}, "ports": [{"port": 8000}]}, **extra)}))
        print("---")
print(json.dumps({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "traffic", "namespace": "monitoring"},
    "spec": {"selector": {"matchLabels": {"app": "traffic"}}, "template": {"metadata": {"labels": {"app": "traffic"}}, "spec": {
        "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{"name": "traffic", "image": image, "imagePullPolicy": "Never", "command": ["python3", "/soak/traffic.py"],
                        "resources": {"requests": {"cpu": "100m", "memory": "48Mi"}, "limits": {"memory": "192Mi"}},
                        "securityContext": {"readOnlyRootFilesystem": True, "allowPrivilegeEscalation": False,
                                            "capabilities": {"drop": ["ALL"]}},
                        "volumeMounts": [{"name": "scripts", "mountPath": "/soak", "readOnly": True}]}],
        "volumes": [{"name": "scripts", "configMap": {"name": "soak-scripts"}}]}}}}))
PY
k -n monitoring rollout status deploy/vm --timeout=180s
for d in active recommend conflict; do k -n pa-soak rollout status "deploy/$d" --timeout=180s; done

# --- the chart -------------------------------------------------------------------------------------------------------
api_ep="$(k get endpoints kubernetes -o jsonpath='{.subsets[0].addresses[0].ip}')"
api_port="$(k get endpoints kubernetes -o jsonpath='{.subsets[0].ports[0].port}')"
python3 - "$OP" "$FC" "$api_ep" "$api_port" "${SOAK_OPERATOR_REPLICAS:-1}" > "$STATE/values.json" <<'PY'
import json, sys
def image(v):
    repository, tag = v.rsplit(":", 1)
    return {"repository": repository, "tag": tag}
print(json.dumps({
    "images": {"operator": image(sys.argv[1]), "forecaster": image(sys.argv[2]), "pullPolicy": "Never"},
    "prometheus": {"url": "http://vm.monitoring.svc:8428"},
    "operator": {"watchNamespaces": ["pa-soak"], "replicas": int(sys.argv[5])},
    "forecaster": {"resources": {"requests": {"cpu": "200m", "memory": "512Mi"}, "limits": {"memory": "3Gi"}},
                   "persistence": {"size": "2Gi"}},
    # serialized training: staggered schedules, 20 minutes apart
    "training": {"targets": [{"namespace": "pa-soak", "name": "active-pa", "schedule": "0 */6 * * *"},
                             {"namespace": "pa-soak", "name": "recommend-pa", "schedule": "20 */6 * * *"},
                             {"namespace": "pa-soak", "name": "conflict-pa", "schedule": "40 */6 * * *"}],
                 "resources": {"requests": {"cpu": "500m", "memory": "768Mi"}, "limits": {"memory": "3Gi"}}},
    "networkPolicy": {"kubeAPI": {"cidrs": [sys.argv[3] + "/32"], "ports": [int(sys.argv[4])]},
                      "prometheus": {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}},
                                     "podSelector": {"matchLabels": {"app": "vm"}}, "ports": [8428]}}
}))
PY
h upgrade --install "$RELEASE" "$ROOT/charts/predictive-autoscaler" -n "$NS" --create-namespace -f "$STATE/values.json" \
  --wait --timeout 8m

# --- the autoscalers ---------------------------------------------------------------------------------------------
for spec in "active Active" "recommend Recommend" "conflict Active"; do
  set -- $spec
  k apply -f - <<EOF
apiVersion: autoscaling.devkuban.com/v1alpha1
kind: PredictiveAutoscaler
metadata: {name: $1-pa, namespace: pa-soak}
spec:
  mode: $2
  targetDeployment: {name: $1, namespace: pa-soak}
  minReplicas: 1
  maxReplicas: 10
  metrics:
    requests:
      enabled: true
      targetRPS: 2
      source:
        preset: prometheus
        query: 'sum(rate(demo_requests_total{namespace="{{ .Namespace }}",service="{{ .Name }}",origin="soak"}[2m]))'
  prediction: {enabled: true, updateIntervalSeconds: 60, horizonMinutes: 60, leadTimeMinutes: 15}
EOF
done
k -n pa-soak get pa,deploy
log "soak installed; record: $STATE"
