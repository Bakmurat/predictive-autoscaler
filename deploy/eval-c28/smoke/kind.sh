#!/usr/bin/env bash
# C2.8 kind smoke (EVAL-C28-MANIFEST section 2: the aspire kind bed is for functionality, not evidence). On a throwaway
# kind cluster with its own kubeconfig, it deploys the evaluation fixture, the ingress, the fault routes and the five
# arms for both startup profiles, and checks:
#   1. every fixture pod lands on its role node (workload / support) through the run taint and required affinity;
#   2. every target Deployment has exactly one writer (python -m c28 audit over the live HPAs, ScaledObjects and
#      PredictiveAutoscalers of both API groups), including KEDA's generated HPA;
#   3. the PredictiveAutoscaler arms run in Recommend mode and write nothing (replicas stay at 2);
#   4. an accelerated (labelled accelerated-smoke, never a day) open-loop k6 run reaches the ingress, which counts it;
#   5. a scheduled metrics-route outage refuses connections while the ingress counter keeps counting;
#   6. the PriorityClass-scoped quota denies new fixture pods (ReplicaFailure, then ScaleOutBlocked on a PA arm) and
#      leaves a non-fixture pod alone.
# It only ever talks to its own cluster: the kubeconfig in its work directory, checked (context and cluster UID) before
# every step. Nothing here reads or writes a lab cluster, and no evidence produced here counts toward C2.8.
#
#   deploy/eval-c28/smoke/kind.sh               build, create the cluster, run, delete it
#   KEEP=1 deploy/eval-c28/smoke/kind.sh        keep the cluster for inspection
#   deploy/eval-c28/smoke/kind.sh --render-only render and audit the manifests offline (no cluster, no docker)
# Needs: docker, kind, kubectl, helm, python3 with PyYAML. OPERATOR_IMAGE / FORECASTER_IMAGE (local tags) skip builds.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
HARNESS="$ROOT/deploy/eval-c28/harness"
PARAMS="$ROOT/deploy/eval-c28/params/kind-smoke.json"
PY="${PYTHON:-python3}"
case "$PY" in */*) PY="$(cd "$(dirname "$PY")" && pwd)/$(basename "$PY")" ;; esac
c28() { (cd "$HARNESS" && KUBECONFIG=/dev/null "$PY" -m c28 "$@"); }

if [ "${1:-}" = --render-only ]; then
  out="$(mktemp -d)"
  c28 render --params "$PARAMS" --out "$out/all.yaml"
  "$PY" - "$out/all.yaml" "$PARAMS" "$HARNESS" <<'PY'
import json, sys, yaml
sys.path.insert(0, sys.argv[3])
from c28 import render
objs = list(yaml.safe_load_all(open(sys.argv[1])))
render.audit_rendered(objs)
live = [o for o in objs if o["kind"] in ("HorizontalPodAutoscaler", "ScaledObject", "PredictiveAutoscaler")]
problems = render.audit_live(render.expected_writers(json.load(open(sys.argv[2]))), live)
assert not problems, problems
print(f"render-only: {len(objs)} objects, every target has exactly one writer")
PY
  exit 0
fi

CLUSTER="${CLUSTER:-pa-c28-smoke}"
# Ownership: only what this run created is ever removed. A supplied WORK is never deleted; a pre-existing cluster,
# registry container or image tag this run would build makes the smoke refuse to start. Image tags it builds and a
# registry image it pulls are untagged at the end (never forced, so layers another tag uses stay).
OWN_WORK=0 OWN_CLUSTER=0 OWN_REG=0
OWN_IMAGES=()
if [ -z "${WORK:-}" ]; then WORK="$(mktemp -d)"; OWN_WORK=1; fi
KCFG="$WORK/kubeconfig"
KIND="${KIND:-kind}"
NS=demo
CTRL=pa-eval-system
REG_NAME=pa-c28-registry
REG_PORT="${REG_PORT:-5001}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
REGISTRY_IMAGE=registry:2.8.3
VM_IMAGE=victoriametrics/victoria-metrics:v1.153.0
K6_IMAGE=grafana/k6:0.57.0
KEDA_CHART_VERSION="${KEDA_CHART_VERSION:-2.16.1}"
METRICS_SERVER_URL=https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
OPERATOR_IMAGE="${OPERATOR_IMAGE:-}"
FORECASTER_IMAGE="${FORECASTER_IMAGE:-}"
export KUBECONFIG="$KCFG"
unset KUBERNETES_SERVICE_HOST KUBERNETES_SERVICE_PORT

log() { printf '%s  %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
pass() { log "PASS $*"; }
die() { log "FAIL $*"; k get pods -A -o wide 2>/dev/null | tail -40 || true; exit 1; }
k() { kubectl --request-timeout=30s --kubeconfig "$KCFG" --context "kind-$CLUSTER" "$@"; }
h() { helm --kubeconfig "$KCFG" --kube-context "kind-$CLUSTER" "$@"; }
CLUSTER_UID=""
guard() {
  [ "$(kubectl --kubeconfig "$KCFG" config current-context 2>/dev/null)" = "kind-$CLUSTER" ] \
    || { echo "smoke: the kubeconfig does not point at kind-$CLUSTER; refusing" >&2; exit 2; }
  case "$(kubectl --kubeconfig "$KCFG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" in
    https://127.0.0.1:*|https://localhost:*) ;;
    *) echo "smoke: the API server is not a local kind endpoint; refusing" >&2; exit 2 ;;
  esac
  if [ -n "$CLUSTER_UID" ]; then
    [ "$(k get namespace kube-system -o jsonpath='{.metadata.uid}')" = "$CLUSTER_UID" ] \
      || { echo "smoke: the cluster identity changed; refusing" >&2; exit 2; }
  fi
}
wait_for() {
  local secs=$1 what=$2; shift 2
  local deadline=$(( $(date +%s) + secs ))
  until "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || die "$what (after ${secs}s)"
    sleep 5
  done
}
cleanup() {
  if [ "${KEEP:-0}" = 1 ]; then
    log "kept: KUBECONFIG=$KCFG kubectl get pods -A ; delete with: $KIND delete cluster --name $CLUSTER"
    return
  fi
  [ "$OWN_CLUSTER" = 1 ] && "$KIND" delete cluster --name "$CLUSTER" --kubeconfig "$KCFG" >/dev/null 2>&1 || true
  [ "$OWN_REG" = 1 ] && docker rm -f "$REG_NAME" >/dev/null 2>&1 || true
  for img in ${OWN_IMAGES[@]+"${OWN_IMAGES[@]}"}; do docker image rm "$img" >/dev/null 2>&1 || true; done
  if [ "$OWN_WORK" = 1 ] && [ -d "$WORK" ]; then rm -rf -- "${WORK:?}"; fi
}
trap cleanup EXIT
for tool in docker "$KIND" kubectl helm "$PY"; do command -v "$tool" >/dev/null || die "missing tool: $tool"; done
if docker container inspect "$REG_NAME" >/dev/null 2>&1; then die "a container named $REG_NAME already exists; refusing"; fi
if "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER"; then die "a kind cluster named $CLUSTER already exists; refusing"; fi
FIXTURE_TAG="localhost:$REG_PORT/c28-fixture:smoke"
BUILD_TAGS=("$FIXTURE_TAG")
[ -n "$OPERATOR_IMAGE" ] || BUILD_TAGS+=(pa-operator:c28-smoke)
[ -n "$FORECASTER_IMAGE" ] || BUILD_TAGS+=(pa-forecaster-statistical:c28-smoke)
for img in "${BUILD_TAGS[@]}"; do
  if docker image inspect "$img" >/dev/null 2>&1; then die "an image tagged $img already exists; refusing"; fi
done
docker image inspect "$REGISTRY_IMAGE" >/dev/null 2>&1 || OWN_IMAGES+=("$REGISTRY_IMAGE")

# --- images and a local registry (so every manifest pins a real digest) ---------------------------------------------
OWN_REG=1   # set before run: a half-started container of this name is ours (its absence was checked above)
docker run -d --restart=no --name "$REG_NAME" -p "127.0.0.1:$REG_PORT:5000" "$REGISTRY_IMAGE" >/dev/null
OWN_IMAGES+=("${BUILD_TAGS[@]}")   # absent above, so every one of these tags is this run's
docker build -q -t "$FIXTURE_TAG" "$ROOT/deploy/eval-c28/fixture" >/dev/null
docker push -q "$FIXTURE_TAG" >/dev/null
FIXTURE_DIGEST="$(docker inspect --format '{{index .RepoDigests 0}}' "$FIXTURE_TAG")"
FIXTURE_DIGEST="${FIXTURE_DIGEST#*@}"
[[ "$FIXTURE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || die "no registry digest for the fixture image"
if [ -z "$OPERATOR_IMAGE" ]; then
  OPERATOR_IMAGE=pa-operator:c28-smoke
  docker build -q -t "$OPERATOR_IMAGE" "$ROOT/k8s-operator" >/dev/null
fi
if [ -z "$FORECASTER_IMAGE" ]; then
  FORECASTER_IMAGE=pa-forecaster-statistical:c28-smoke
  docker build -q -t "$FORECASTER_IMAGE" -f "$ROOT/ml-engine/Dockerfile.statistical" "$ROOT/ml-engine" >/dev/null
fi
pass "images built; fixture pinned at $FIXTURE_DIGEST"

# --- the cluster: one control plane, a workload node and a support node ---------------------------------------------
cat > "$WORK/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:$REG_PORT"]
      endpoint = ["http://$REG_NAME:5000"]
EOF
OWN_CLUSTER=1   # set before create: a half-created cluster of this name is ours (its absence was checked above)
"$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --config "$WORK/kind.yaml" --kubeconfig "$KCFG" --wait 120s >/dev/null
docker network connect kind "$REG_NAME" 2>/dev/null || true
guard
CLUSTER_UID="$(k get namespace kube-system -o jsonpath='{.metadata.uid}')"
"$KIND" load docker-image "$OPERATOR_IMAGE" "$FORECASTER_IMAGE" --name "$CLUSTER" >/dev/null
W="$CLUSTER-worker"; S="$CLUSTER-worker2"
# Both workers carry the run taint, so the shared platform (metrics-server, KEDA, the metrics backend), which tolerates
# nothing, runs on this throwaway cluster's control plane.
k taint node "$CLUSTER-control-plane" node-role.kubernetes.io/control-plane:NoSchedule- >/dev/null
k label node "$W" c28.devkuban.com/role=workload --overwrite >/dev/null
k label node "$S" c28.devkuban.com/role=support --overwrite >/dev/null
k taint node "$W" c28.devkuban.com/reserved=c28-smoke:NoSchedule --overwrite >/dev/null
k taint node "$S" c28.devkuban.com/reserved=c28-smoke:NoSchedule --overwrite >/dev/null
k create namespace "$NS" >/dev/null
k create namespace "$CTRL" >/dev/null
k create namespace monitoring >/dev/null
pass "cluster $CLUSTER_UID with workload node $W and support node $S (run taint, role labels)"

# --- platform: metrics-server (H-CPU), KEDA (H-RPS), a metrics backend scraping the ingress ---------------------------
guard
k apply -f "$METRICS_SERVER_URL" >/dev/null
k -n kube-system patch deploy metrics-server --type json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' >/dev/null
h repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
h install keda kedacore/keda --version "$KEDA_CHART_VERSION" -n keda --create-namespace --wait --timeout 6m >/dev/null \
  || die "KEDA install"
k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ConfigMap
metadata: {name: vm-scrape, namespace: monitoring}
data:
  scrape.yaml: |
    scrape_configs:
      - job_name: c28-ingress
        scrape_interval: 15s
        static_configs: [{targets: ["c28-ingress.$NS.svc:80"]}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: vm, namespace: monitoring}
spec:
  replicas: 1
  selector: {matchLabels: {app: vm}}
  template:
    metadata: {labels: {app: vm}}
    spec:
      containers:
        - name: vm
          image: $VM_IMAGE
          args: ["-promscrape.config=/etc/vm/scrape.yaml", "-retentionPeriod=1d"]
          ports: [{containerPort: 8428}]
          volumeMounts: [{name: cfg, mountPath: /etc/vm}]
      volumes: [{name: cfg, configMap: {name: vm-scrape}}]
---
apiVersion: v1
kind: Service
metadata: {name: vm, namespace: monitoring}
spec: {selector: {app: vm}, ports: [{port: 8428}]}
EOF
wait_for 180 "the metrics backend is ready" k -n monitoring rollout status deploy/vm --timeout=5s
pass "metrics-server, KEDA $KEDA_CHART_VERSION and the metrics backend"

# --- the evaluation release (statistical engine; metrics and forecasts through the evaluation-owned routes) ----------
jq_params="$WORK/params.json"
"$PY" - "$PARAMS" "$jq_params" "localhost:$REG_PORT/c28-fixture@$FIXTURE_DIGEST" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))
p["image"] = sys.argv[3]
json.dump(p, open(sys.argv[2], "w"))
PY
c28 release-values --params "$jq_params" --out "$WORK/release-values.yaml"   # routes, and the route's TLS name
cat > "$WORK/values.yaml" <<EOF
images:
  operator: {repository: ${OPERATOR_IMAGE%:*}, tag: ${OPERATOR_IMAGE##*:}}
  statisticalForecaster: {repository: ${FORECASTER_IMAGE%:*}, tag: ${FORECASTER_IMAGE##*:}}
  pullPolicy: Never
operator:
  tolerations: [{key: c28.devkuban.com/reserved, operator: Equal, value: c28-smoke, effect: NoSchedule}]
  nodeSelector: {c28.devkuban.com/role: support}
forecaster:
  tolerations: [{key: c28.devkuban.com/reserved, operator: Equal, value: c28-smoke, effect: NoSchedule}]
  nodeSelector: {c28.devkuban.com/role: support}
networkPolicy: {enabled: false}
EOF
guard
h install pa-eval "$ROOT/charts/predictive-autoscaler" -n "$CTRL" -f "$WORK/release-values.yaml" -f "$WORK/values.yaml" \
  --wait --timeout 8m >/dev/null \
  || die "evaluation release install"
pass "evaluation release in $CTRL watching $NS only"

# --- the fixture and the five arms (both profiles) --------------------------------------------------------------------
c28 render --params "$jq_params" --out "$WORK/all.yaml"
guard
k apply -f "$WORK/all.yaml" >/dev/null
for d in $(k -n "$NS" get deploy -l c28.devkuban.com/run=c28-smoke -o name); do
  wait_for 300 "$d is available" k -n "$NS" rollout status "$d" --timeout=5s
done
wait_for 120 "the fault routes are available" k -n "$CTRL" rollout status deploy/c28-metrics-route --timeout=5s
misplaced="$(k get pods -A -l c28.devkuban.com/run=c28-smoke -o json | "$PY" -c '
import json, sys
W, S = sys.argv[1], sys.argv[2]
for p in json.load(sys.stdin)["items"]:
    want = S if p["metadata"]["name"].startswith("c28-") and "-route-" in p["metadata"]["name"] else W
    if p["spec"].get("nodeName") != want:
        print(p["metadata"]["name"], p["spec"].get("nodeName"))
' "$W" "$S")"
[ -z "$misplaced" ] || die "pods on the wrong node: $misplaced"
pass "1. every fixture pod is on its role node"

# --- 2. exclusive writers ---------------------------------------------------------------------------------------------
wait_for 120 "KEDA created its HPAs" sh -c "[ \"\$(kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n $NS get hpa -o name | grep -c keda-hpa)\" = 2 ]"
{
  echo '['
  first=1
  for kind in hpa scaledobjects.keda.sh predictiveautoscalers.autoscaling.devkuban.com; do
    k get "$kind" -A -o json | "$PY" -c 'import json,sys; [print(json.dumps(i)) for i in json.load(sys.stdin)["items"]]'
  done | while read -r line; do
    [ "$first" = 1 ] || echo ','
    first=0
    echo "$line"
  done
  echo ']'
} > "$WORK/live.json"
c28 audit --params "$jq_params" --live "$WORK/live.json" || die "exclusive-writer audit"
pass "2. every target has exactly its one writer"

# --- 3. Recommend writes nothing ----------------------------------------------------------------------------------------
sleep 90
for d in $(k -n "$NS" get pa -o jsonpath='{.items[*].spec.targetDeployment.name}'); do
  [ "$(k -n "$NS" get deploy "$d" -o jsonpath='{.spec.replicas}')" = 2 ] || die "Recommend arm $d changed its replicas"
  [ "$(k -n "$NS" get pa "$d" -o jsonpath='{.status.mode}')" = Recommend ] || die "$d is not in Recommend"
done
pass "3. the six PredictiveAutoscaler arms are in Recommend and wrote nothing"

# --- 4. accelerated open-loop load through the ingress ------------------------------------------------------------------
# accelerate 120: each program minute lasts 0.5 s, the whole scored program 12 minutes (labelled accelerated-smoke).
c28 plan --kind scored --seed 201 --capacity 7 --target c28-h-rps-s20 --accelerate 120 --out "$WORK/plan.json"
k -n "$NS" create configmap c28-k6 --from-file=plan.json="$WORK/plan.json" --from-file=c28.js="$ROOT/deploy/eval-c28/load/c28.js" >/dev/null
k apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata: {name: c28-k6-smoke, namespace: $NS}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 900
  template:
    spec:
      restartPolicy: Never
      tolerations: [{key: c28.devkuban.com/reserved, operator: Equal, value: c28-smoke, effect: NoSchedule}]
      nodeSelector: {c28.devkuban.com/role: support}
      containers:
        - name: k6
          image: $K6_IMAGE
          args: ["run", "-e", "C28_PLAN=/plan/plan.json", "-e", "C28_BASE_URL=http://c28-ingress.$NS.svc",
                 "-e", "C28_SUMMARY=/tmp/summary.json", "/plan/c28.js"]
          volumeMounts: [{name: plan, mountPath: /plan}]
      volumes: [{name: plan, configMap: {name: c28-k6}}]
EOF
# fetch_matches <namespace> <resource> <local port> <remote port> <path> <pattern>: fetch through a short
# port-forward (the images carry no tools) and match the answer.
fetch_matches() {
  local ns=$1 res=$2 lport=$3 rport=$4 path=$5 pattern=$6 out pf
  k -n "$ns" port-forward "$res" "$lport:$rport" >/dev/null 2>&1 & pf=$!
  sleep 2
  out="$("$PY" -c 'import sys, urllib.request; print(urllib.request.urlopen(sys.argv[1], timeout=5).read().decode())' \
    "http://127.0.0.1:$lport$path" 2>/dev/null || true)"
  kill "$pf" 2>/dev/null || true
  grep -Eq "$pattern" <<<"$out"
}
ingress_counted() { fetch_matches "$NS" deploy/c28-ingress 19181 8080 /metrics '^c28_ingress_requests_total\{target="c28-h-rps-s20"\} [1-9]'; }
wait_for 300 "the ingress counts the arrivals" ingress_counted
pass "4. accelerated-smoke load reaches the ingress, which counts it per target"

# --- 5. a scheduled metrics-route outage ---------------------------------------------------------------------------------
start="$(date -u -d '+90 seconds' +%Y-%m-%dT%H:%M:%SZ)"; end="$(date -u -d '+150 seconds' +%Y-%m-%dT%H:%M:%SZ)"
k -n "$CTRL" create configmap c28-metrics-route-faults --dry-run=client -o yaml \
  --from-literal=faults.json="{\"outages\":[{\"start\":\"$start\",\"end\":\"$end\"}]}" | k apply -f - >/dev/null
route_in_outage() { fetch_matches "$CTRL" deploy/c28-metrics-route 19191 9091 /metrics '^c28_proxy_outage 1'; }
wait_for 240 "the route reports its outage" route_in_outage
wait_for 60 "the ingress still counts during the outage" ingress_counted
pass "5. the metrics route refuses during its scheduled outage (the ingress counter is unaffected)"

# --- 6. the scoped quota -------------------------------------------------------------------------------------------------
guard
# A hard limit of 0 (below the pods already counted, which stay) denies every new fixture pod. A limit equal to the
# current count would not hold: the active arms keep scaling, and one scale-down frees a slot (seen on aspire: an H-CPU
# arm stepped down and the denied pod was created before the PA's next decision).
k -n "$NS" patch resourcequota c28-fixture-quota --type merge -p '{"spec":{"hard":{"pods":"0"}}}' >/dev/null
k -n "$NS" scale deploy c28-pa-r-s20 --replicas 3 >/dev/null   # a manual scale of a Recommend arm (it never writes)
wait_for 120 "the quota denies the new fixture pod" sh -c "kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n $NS get deploy c28-pa-r-s20 -o jsonpath='{.status.conditions[?(@.type==\"ReplicaFailure\")].status}' | grep -q True"
wait_for 180 "the PA arm reports ScaleOutBlocked" sh -c "kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n $NS get pa c28-pa-r-s20 -o jsonpath='{.status.conditions[?(@.type==\"ScaleOutBlocked\")].reason}' | grep -q QuotaExceeded"
k -n "$NS" run c28-not-fixture --image=registry.k8s.io/pause:3.10 --restart=Never \
  --overrides='{"spec":{"tolerations":[{"key":"c28.devkuban.com/reserved","operator":"Exists","effect":"NoSchedule"}]}}' >/dev/null \
  || die "a non-fixture pod was denied by the scoped quota"
k -n "$NS" patch resourcequota c28-fixture-quota --type merge -p '{"spec":{"hard":{"pods":"64"}}}' >/dev/null
pass "6. the scoped quota denies fixture pods only; ScaleOutBlocked=QuotaExceeded"

# --- 7. the forecast route keeps TLS verification -------------------------------------------------------------------------
# The operator dials https://c28-forecast-route...:8443 (a TCP pass-through) and verifies the forecaster's certificate
# against that name (forecaster.tls.extraDNSNames). A service answer proves the verified path: a trust reason, a 422
# refusal, or an HTTP error status from the service ("ML API returned status", built only from a response read over
# the verified connection). A TLS or dial failure is DataUnavailable with a transport error instead. The smoke's load
# reaches only the H-RPS arm, so the PA arms' series are empty and the usual answer here is the service's 503.
service_reason() {
  local r m
  r="$(k -n "$NS" get pa c28-pa-n-s20 -o jsonpath='{.status.conditions[?(@.type=="PredictionReady")].reason}')"
  m="$(k -n "$NS" get pa c28-pa-n-s20 -o jsonpath='{.status.conditions[?(@.type=="PredictionReady")].message}')"
  case "$r" in HistoryInsufficient|CalibrationInsufficient|ShadowInsufficient|CoverageLow|LeadInsufficient|RequestRefused|Ready) return 0 ;; esac
  [ "$r" = DataUnavailable ] && [[ "$m" == "ML API returned status "* ]]
}
wait_for 300 "a forecast answer through the verified route" service_reason
pass "7. forecasts reach the operator through the route with certificate verification on"
log "smoke complete (accelerated, engineering only: no C2.8 evidence)"
