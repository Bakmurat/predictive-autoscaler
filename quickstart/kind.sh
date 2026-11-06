#!/usr/bin/env bash
# Source-built local demo. Every cluster operation is bound to this run's private state.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="${PA_QUICKSTART_STATE:-${XDG_STATE_HOME:-$HOME/.local/state}/predictive-autoscaler-quickstart}"
KIND="${KIND:-kind}"
NS=predictive-autoscaler
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
VM_IMAGE=victoriametrics/victoria-metrics:v1.153.0
ACTION="${1:-help}"
[ "$#" = 0 ] || shift
usage() {
  cat <<'EOF'
Usage: quickstart/kind.sh up [--synthetic-history] | status | active | stop | down
  up      Build/load images, create a private kind cluster; verify Recommend leaves one replica.
          --synthetic-history explicitly imports generated history, trains, and verifies a forecast.
  active  Opt in to /scale writes on this demo; wait for healthy scaled replicas.
  stop    Return to Recommend; leave the workload's replica count where it is.
  down    Delete only the recorded demo cluster, including its models and metrics.
Environment: PA_QUICKSTART_STATE (private directory), KIND (binary), NODE_IMAGE,
             OPERATOR_IMAGE / FORECASTER_IMAGE (prebuilt local repository:tag).
EOF
}
case "$ACTION" in help|-h|--help) usage; exit 0 ;; up|status|active|stop|down) ;; *) usage >&2; exit 2 ;; esac
SYNTHETIC=0
if [ "$ACTION" = up ] && [ "${1:-}" = --synthetic-history ]; then SYNTHETIC=1; shift; fi
[ "$#" = 0 ] || { usage >&2; exit 2; }
for tool in python3 docker "$KIND" kubectl helm; do
  command -v "$tool" >/dev/null || { echo "Missing tool: $tool" >&2; exit 2; }
done
STATE="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$STATE")"
KCFG="$STATE/kubeconfig"
export KUBECONFIG="$KCFG"
unset KUBERNETES_SERVICE_HOST KUBERNETES_SERVICE_PORT
log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
k() { kubectl --request-timeout=15s --kubeconfig "$KCFG" --context "kind-$CLUSTER" "$@"; }
h() { helm --kubeconfig "$KCFG" --kube-context "kind-$CLUSTER" "$@"; }
guard() {
  [ -f "$STATE/owner" ] && [ "$(cat "$STATE/owner")" = predictive-autoscaler-quickstart-v1 ] || {
    echo "No quickstart ownership record in $STATE; refusing" >&2; exit 2;
  }
  CLUSTER="$(cat "$STATE/cluster")"
  [[ "$CLUSTER" =~ ^pa-quickstart-[a-f0-9]{12}$ ]] || exit 2
  recorded_node="$(cat "$STATE/node-id")"
  [[ "$recorded_node" =~ ^[a-f0-9]{64}$ ]] || { echo "Missing or invalid node identity; refusing" >&2; exit 2; }
  actual_node="$(docker inspect --type container --format '{{.Id}}' "$CLUSTER-control-plane")"
  [ "$actual_node" = "$recorded_node" ] || {
    echo "The recorded kind node was replaced; refusing" >&2; exit 2;
  }
  [ "$ACTION" != down ] || return 0
  [ "$(kubectl --kubeconfig "$KCFG" config current-context)" = "kind-$CLUSTER" ] || exit 2
  recorded_uid="$(cat "$STATE/cluster-uid")"
  [[ "$recorded_uid" =~ ^[a-f0-9-]{36}$ ]] || { echo "Missing or invalid cluster identity; refusing" >&2; exit 2; }
  actual_uid="$(k get namespace kube-system -o jsonpath='{.metadata.uid}')"
  [ "$actual_uid" = "$recorded_uid" ] || {
    echo "The kubeconfig does not identify the recorded cluster; refusing" >&2; exit 2;
  }
}
diagnose() {
  log "Demo retained in $STATE; inspect with quickstart/kind.sh status (same PA_QUICKSTART_STATE)."
  k get pods -A -o wide || true
  k -n demo get pa web-pa -o yaml || true
  k -n "$NS" logs -l app.kubernetes.io/component=trainer --tail=30 || true
}
wait_for() {
  local seconds=$1 description=$2; shift 2
  local deadline=$(( $(date +%s) + seconds ))
  until "$@" >"$STATE/last-check.log" 2>&1; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "FAIL: $description"; cat "$STATE/last-check.log"; diagnose; exit 1
    fi
    sleep 5
  done
  log "PASS: $description"
}
outcome() { k -n demo get pa/web-pa deploy/web -o json | python3 "$ROOT/quickstart/check.py" "$@"; }
status() {
  k -n demo get pa/web-pa deploy/web
  k -n demo get pa/web-pa -o jsonpath='{range .status.conditions[*]}{.type}{"="}{.status}{" ("}{.reason}{")\n"}{end}{"Last forecast: "}{.status.lastPrediction}{"\n"}'
  log "Private kubeconfig: $KCFG; context: kind-$CLUSTER"
}

if [ "$ACTION" != up ]; then
  guard
  case "$ACTION" in
    status) status ;;
    active)
      k -n demo patch pa web-pa --type merge -p '{"spec":{"mode":"Active"}}'
      phase=active-reactive
      [ "$(cat "$STATE/synthetic")" != 1 ] || phase=active
      wait_for 420 "Active mode scales this demo and all replicas become Ready" outcome "$phase" "$(cat "$STATE/started-at")"
      status ;;
    stop)
      replicas_before="$(k -n demo get deploy web -o jsonpath='{.spec.replicas}')"
      k -n demo patch pa web-pa --type merge -p '{"spec":{"mode":"Recommend"}}'
      wait_for 180 "Recommend mode has stopped writing replicas" outcome stopped
      [ "$(k -n demo get deploy web -o jsonpath='{.spec.replicas}')" = "$replicas_before" ] || { echo "Replica count changed during stop" >&2; exit 1; }
      status ;;
    down)
      log "Deleting owned local cluster $CLUSTER, including its models and metrics"
      # kind also edits its kubeconfig on delete; use a private empty config even if the demo's config is damaged.
      printf '%s\n' '{"apiVersion":"v1","kind":"Config","clusters":[],"contexts":[],"users":[]}' > "$STATE/delete-kubeconfig"
      "$KIND" delete cluster --name "$CLUSTER" --kubeconfig "$STATE/delete-kubeconfig"
      log "Cluster removed. Private state/logs retained at $STATE; choose a new state directory for another run." ;;
  esac
  exit 0
fi

# Reserve a new directory before creating any resource. Never adopt an existing cluster or state directory.
umask 077
mkdir -p "$(dirname "$STATE")"
mkdir "$STATE" || { echo "State directory already exists; use status or choose a new PA_QUICKSTART_STATE" >&2; exit 2; }
CLUSTER="pa-quickstart-$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
"$KIND" get clusters > "$STATE/existing-clusters"
if grep -Fxq "$CLUSTER" "$STATE/existing-clusters"; then echo "Cluster name already exists; refusing" >&2; exit 2; fi
printf '%s\n' predictive-autoscaler-quickstart-v1 > "$STATE/owner"
printf '%s\n' "$CLUSTER" > "$STATE/cluster"
printf '%s\n' "$SYNTHETIC" > "$STATE/synthetic"
date -u +%Y-%m-%dT%H:%M:%SZ > "$STATE/started-at"
log "State: $STATE; cluster: $CLUSTER. Failures leave resources for inspection."
OPERATOR_IMAGE="${OPERATOR_IMAGE:-pa-quickstart/operator:dev}"
FORECASTER_IMAGE="${FORECASTER_IMAGE:-pa-quickstart/ml-api:dev}"
for img in "$OPERATOR_IMAGE" "$FORECASTER_IMAGE"; do
  [[ "$img" == *:* && "$img" != *@* && "${img##*:}" != */* ]] || {
    echo "Expected local repository:tag image: $img" >&2; exit 2;
  }
done
if [ "${OPERATOR_IMAGE}" = pa-quickstart/operator:dev ]; then
  docker build -t "$OPERATOR_IMAGE" "$ROOT/k8s-operator"
fi
if [ "${FORECASTER_IMAGE}" = pa-quickstart/ml-api:dev ]; then
  docker build -t "$FORECASTER_IMAGE" "$ROOT/ml-engine"
fi
for img in "$OPERATOR_IMAGE" "$FORECASTER_IMAGE"; do docker image inspect "$img" >/dev/null; done
docker pull -q "$VM_IMAGE" >/dev/null
if ! "$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --kubeconfig "$KCFG" --wait 120s --retain; then
  docker inspect --type container --format '{{.Id}}' "$CLUSTER-control-plane" > "$STATE/node-id" 2>/dev/null || true
  log "kind creation failed; recorded any retained node for quickstart/kind.sh down. State: $STATE"
  exit 1
fi
docker inspect --type container --format '{{.Id}}' "$CLUSTER-control-plane" > "$STATE/node-id"
k get namespace kube-system -o jsonpath='{.metadata.uid}' > "$STATE/cluster-uid"
guard
for img in "$OPERATOR_IMAGE" "$FORECASTER_IMAGE" "$VM_IMAGE"; do
  "$KIND" load docker-image --name "$CLUSTER" "$img"
done
k create namespace monitoring
k create namespace demo
k -n demo create configmap demo-scripts --from-file="$ROOT/quickstart/app.py"
k -n monitoring create configmap demo-scripts --from-file="$ROOT/quickstart/traffic.py" --from-file="$ROOT/hack/smoke/synth.py"
render() {
  python3 - "$ROOT/quickstart/$1" "$FORECASTER_IMAGE" <<'PYRENDER'
import pathlib, re, sys
assert re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._:/-]*:[a-zA-Z0-9_][a-zA-Z0-9_.-]*", sys.argv[2]), "invalid image"
print(pathlib.Path(sys.argv[1]).read_text().replace("pa-quickstart/ml-api:dev", sys.argv[2]))
PYRENDER
}
render workload.yaml | k apply -f -
render backend.yaml | k apply -f -
if [ "$SYNTHETIC" = 1 ]; then
  log "SYNTHETIC HISTORY: generated eight-day series; demonstrates mechanics, not forecast accuracy."
  k -n monitoring patch deploy traffic --type strategic -p '{"spec":{"template":{"spec":{"containers":[{"name":"traffic","command":["python3","/demo/synth.py","http://vm.monitoring.svc:8428"]}]}}}}'
fi
wait_for 180 "metrics store ready" k -n monitoring rollout status deploy/vm --timeout=5s
wait_for 180 "HTTP demo ready" k -n demo rollout status deploy/web --timeout=5s
if [ "$SYNTHETIC" = 1 ]; then
  backfilled() { k -n monitoring logs deploy/traffic | grep -q 'backfill done'; }
  wait_for 300 "generated eight-day history imported" backfilled
fi
api_ep="$(k get endpoints kubernetes -o jsonpath='{.subsets[0].addresses[0].ip}')"
api_port="$(k get endpoints kubernetes -o jsonpath='{.subsets[0].ports[0].port}')"
python3 - "$OPERATOR_IMAGE" "$FORECASTER_IMAGE" "$api_ep" "$api_port" > "$STATE/values.json" <<'PY'
import json, sys
def image(value):
    repository, tag = value.rsplit(":", 1)
    return {"repository": repository, "tag": tag}
print(json.dumps({
    "images": {"operator": image(sys.argv[1]), "forecaster": image(sys.argv[2]), "pullPolicy": "Never"},
    "prometheus": {"url": "http://vm.monitoring.svc:8428"},
    "operator": {"watchNamespaces": ["demo"]},
    "forecaster": {"resources": {"requests": {"cpu": "200m", "memory": "512Mi"}, "limits": {"memory": "3Gi"}}, "persistence": {"size": "1Gi"}},
    "training": {"targets": [{"namespace": "demo", "name": "web-pa", "schedule": "0 */6 * * *"}], "epochs": 2,
                 "resources": {"requests": {"cpu": "200m", "memory": "512Mi"}, "limits": {"memory": "3Gi"}}},
    "networkPolicy": {"kubeAPI": {"cidrs": [sys.argv[3] + "/32"], "ports": [int(sys.argv[4])]},
                      "prometheus": {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}},
                                     "podSelector": {"matchLabels": {"app": "vm"}}, "ports": [8428]}}
}))
PY
h install pa "$ROOT/charts/predictive-autoscaler" -n "$NS" --create-namespace -f "$STATE/values.json" --wait --timeout 6m
origin=demo
[ "$SYNTHETIC" = 0 ] || origin=synthetic
k apply -f - <<EOF
apiVersion: autoscaling.devkuban.com/v1alpha1
kind: PredictiveAutoscaler
metadata: {name: web-pa, namespace: demo}
spec:
  mode: Recommend
  targetDeployment: {name: web, namespace: demo}
  minReplicas: 1
  maxReplicas: 6
  metrics:
    requests:
      enabled: true
      targetRPS: 4
      source:
        preset: prometheus
        query: 'sum(rate(demo_requests_total{namespace="{{ .Namespace }}",service="{{ .Name }}",origin="$origin"}[2m]))'
  prediction: {enabled: true, updateIntervalSeconds: 60, horizonMinutes: 60, leadTimeMinutes: 15}
EOF
wait_for 240 "current telemetry, reactive recommendation, one unchanged replica" outcome recommend
if [ "$SYNTHETIC" = 1 ]; then
  cj="$(k -n "$NS" get cronjob -l app.kubernetes.io/component=trainer -o jsonpath='{.items[0].metadata.name}')"
  k -n "$NS" create job --from="cronjob/$cj" first-training
  wait_for 1500 "first training completes" k -n "$NS" wait --for=condition=complete job/first-training --timeout=5s
  wait_for 420 "fresh forecast is used while Recommend leaves one replica" outcome forecast "$(cat "$STATE/started-at")"
else
  log "No model yet: reactive recommendations only. Forecasting needs about a week of history and successful training."
fi
status
log "Ready. Opt in to scaling with quickstart/kind.sh active; stop with quickstart/kind.sh stop."
