#!/usr/bin/env bash
# The install smoke test (B5b-3, DESIGN-B5; Codex task-08 r23-r30): the Helm chart on a throwaway kind cluster, with a
# metrics backend holding a week of labelled synthetic history. Rendering cannot prove permissions, storage, the CRD gate
# or connectivity; this does:
#   1. a cold install without models: the operator passes the CRD gate, the autoscaler compiles its query, measures the
#      request rate and recommends reactively in Recommend mode, nothing is scaled;
#   2. the service accounts' real permissions (kubectl auth can-i);
#   3. a training run: published on the volume, reloaded by the forecasting service, used by the operator;
#   4. Active mode scales the Deployment;
#   5. the model survives a forecasting-service restart (persistent reload);
#   6. a query change refuses the old model until retrained;
#   7. the CRD gate blocks an operator on an older CRD revision and releases it;
#   8. NetworkPolicy allow/deny from each component's identity, the API server reached through its Service with the
#      API CIDRs tightened;
#   9. a helm upgrade keeps the model; helm uninstall keeps the CRD, the autoscaler, the claim and the replica count.
#
# It only ever talks to its own cluster: a kubeconfig in its work directory, and a context it checks before every step.
#   hack/smoke-kind.sh            build images, create the cluster, run, delete the cluster
#   KEEP=1 hack/smoke-kind.sh     keep the cluster for inspection
# Needs: docker, kind (v0.24+ enforces NetworkPolicy), kubectl, helm. OPERATOR_IMAGE / FORECASTER_IMAGE skip the builds.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${CLUSTER:-pa-smoke}"
WORK="${WORK:-$(mktemp -d)}"
KCFG="$WORK/kubeconfig"
KIND="${KIND:-kind}"
NS=predictive-autoscaler
OPERATOR_IMAGE="${OPERATOR_IMAGE:-}"
FORECASTER_IMAGE="${FORECASTER_IMAGE:-}"
VM_IMAGE=victoriametrics/victoria-metrics:v1.153.0
# kind v0.33.0's published node image for Kubernetes 1.35 (the version envtest and the chart's CI target), by digest.
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
QUERY='sum(rate(demo_requests_total{namespace="{{ .Namespace }}",service="{{ .Name }}",origin="synthetic"}[2m]))'
export KUBECONFIG="$KCFG"            # every kubectl/helm below, and anything they spawn, sees only this cluster

log() { printf '%s  %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
pass() { log "PASS $*"; }
die() { log "FAIL $*"; diagnose; exit 1; }
k() { kubectl --kubeconfig "$KCFG" --context "kind-$CLUSTER" "$@"; }
h() { helm --kubeconfig "$KCFG" --kube-context "kind-$CLUSTER" "$@"; }
guard() {
  [ "$(kubectl --kubeconfig "$KCFG" config current-context 2>/dev/null)" = "kind-$CLUSTER" ] \
    || { echo "smoke: the kubeconfig does not point at kind-$CLUSTER; refusing" >&2; exit 2; }
}
# wait_for <seconds> <description> <command...>: retry the command until it succeeds.
wait_for() {
  local secs=$1 what=$2; shift 2
  local deadline=$(( $(date +%s) + secs ))
  until "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || die "$what (after ${secs}s)"
    sleep 5
  done
}
jp() { k "$@"; }   # kept for readability: jp get <obj> -o jsonpath=...
cond() { k -n demo get pa web-pa -o jsonpath="{.status.conditions[?(@.type==\"$1\")].status}"; }
# "<name> <uid>" of a component's application pods: owned by its ReplicaSet (probe pods carry the same labels and no
# owner, so they are never mistaken for the application).
app_pods() {
  k -n "$NS" get pod -l "app.kubernetes.io/component=$1" -o \
    jsonpath='{range .items[?(@.metadata.ownerReferences[0].kind=="ReplicaSet")]}{.metadata.name}{" "}{.metadata.uid}{"\n"}{end}'
}
# current_app_pod <component>: "<name> <uid>" of its single application pod; fails unless exactly one exists.
current_app_pod() {
  local lines
  lines="$(app_pods "$1")" || return 1
  [ "$(printf '%s\n' "$lines" | grep -c .)" = 1 ] || return 1
  case "$lines" in *" "?*) printf '%s\n' "$lines" ;; *) return 1 ;; esac
}
# wait_replacement <component> <old uid>: waits for an application pod with another UID that is Ready, with a Ready
# time, and prints "<name> <uid> <ready time>". An empty old UID is refused (it would accept the incumbent).
wait_replacement() {
  local comp=$1 old=$2 deadline=$(( $(date +%s) + 300 )) line name t
  [ -n "$old" ] || { log "wait_replacement: no old UID given"; return 2; }
  while :; do
    line="$(app_pods "$comp" | awk -v old="$old" 'NF == 2 && $2 != old' | head -1)"
    if [ -n "$line" ]; then
      name=${line%% *}
      if [ "$(k -n "$NS" get pod "$name" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')" = True ]; then
        t="$(k -n "$NS" get pod "$name" -o jsonpath='{.status.conditions[?(@.type=="Ready")].lastTransitionTime}')"
        [ -n "$t" ] && { echo "$line $t"; return 0; }
      fi
    fi
    [ "$(date +%s)" -lt "$deadline" ] || die "no Ready replacement $comp pod (old UID $old)"
    sleep 3
  done
}
# A forecast issued AFTER <time> is in use: proves the (re)started forecasting service served it, not a cached one
# (the operator sets the issue time only on a fresh response). An empty reference time never counts.
fresh_forecast_after() {
  local since=$1 last
  [ -n "$since" ] || return 1
  last="$(k -n demo get pa web-pa -o jsonpath='{.status.lastPrediction}')"
  [ "$(cond ForecastAvailable)" = True ] && [ -n "$last" ] && [[ "$last" > "$since" ]]
}
diagnose() {
  [ -f "$KCFG" ] || return 0
  log "---- diagnostics"
  k get pods -A -o wide 2>&1 | tail -30 || true
  k -n demo get pa web-pa -o yaml 2>&1 | sed -n '/^status:/,$p' | head -60 || true
  k -n "$NS" logs deploy/pa-predictive-autoscaler-operator --tail=40 2>&1 || true
  k -n "$NS" logs deploy/pa-predictive-autoscaler-forecaster --tail=40 2>&1 || true
}
cleanup() {
  if [ "${KEEP:-0}" = 1 ]; then log "cluster kept: KUBECONFIG=$KCFG"; else "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; fi
}

# --- cluster and images ---------------------------------------------------------------------------------------------
for t in docker "$KIND" kubectl helm openssl; do command -v "$t" >/dev/null || { echo "smoke: $t not found" >&2; exit 2; }; done
if [ -z "$OPERATOR_IMAGE" ]; then OPERATOR_IMAGE=pa-smoke/operator:dev; docker build -q -t "$OPERATOR_IMAGE" "$ROOT/k8s-operator" >/dev/null; fi
if [ -z "$FORECASTER_IMAGE" ]; then FORECASTER_IMAGE=pa-smoke/ml-api:dev; docker build -q -t "$FORECASTER_IMAGE" "$ROOT/ml-engine" >/dev/null; fi
docker pull -q "$VM_IMAGE" >/dev/null
log "creating kind cluster $CLUSTER (kubeconfig $KCFG)"
"$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --kubeconfig "$KCFG" --wait 120s >/dev/null
trap cleanup EXIT
guard
for img in "$OPERATOR_IMAGE" "$FORECASTER_IMAGE" "$VM_IMAGE"; do "$KIND" load docker-image --name "$CLUSTER" "$img" >/dev/null; done
pass "cluster up ($(k version -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')), images loaded"

# --- metrics backend with a week of labelled synthetic history ----------------------------------------------------
k create namespace monitoring >/dev/null
k create namespace demo >/dev/null
k -n monitoring create configmap synth --from-file=synth.py="$ROOT/hack/smoke/synth.py" >/dev/null
# A throwaway certificate for the synth pod's HTTPS endpoint (an unlisted HTTPS destination), made for this run only.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=synth.monitoring.svc" \
  -keyout "$WORK/synth.key" -out "$WORK/synth.crt" >/dev/null 2>&1 || die "openssl could not make the test certificate"
k -n monitoring create secret tls synth-tls --cert="$WORK/synth.crt" --key="$WORK/synth.key" >/dev/null
k apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: vm, namespace: monitoring}
spec:
  selector: {matchLabels: {app: vm}}
  template:
    metadata: {labels: {app: vm}}
    spec:
      containers:
        - name: vm
          image: $VM_IMAGE
          imagePullPolicy: IfNotPresent
          args: ["-retentionPeriod=30d", "-search.latencyOffset=0s", "-search.disableCache"]
          ports: [{containerPort: 8428}]
---
apiVersion: v1
kind: Service
metadata: {name: vm, namespace: monitoring}
spec: {selector: {app: vm}, ports: [{port: 8428}]}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: synth, namespace: monitoring}
spec:
  selector: {matchLabels: {app: synth}}
  template:
    metadata: {labels: {app: synth}}
    spec:
      containers:
        - name: synth
          image: $FORECASTER_IMAGE
          imagePullPolicy: Never
          command: ["python3", "/synth/synth.py", "http://vm.monitoring.svc:8428"]
          ports: [{containerPort: 8000}, {containerPort: 6443}]
          volumeMounts: [{name: synth, mountPath: /synth}, {name: tls, mountPath: /tls}]
      volumes: [{name: synth, configMap: {name: synth}}, {name: tls, secret: {secretName: synth-tls}}]
---
apiVersion: v1
kind: Service
metadata: {name: synth, namespace: monitoring}
spec: {selector: {app: synth}, ports: [{name: http, port: 8000}, {name: https, port: 6443}]}
EOF
wait_for 180 "the metrics backend is ready" k -n monitoring rollout status deploy/vm --timeout=5s
wait_for 300 "the synthetic week is backfilled" sh -c "k() { kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' \"\$@\"; }; k -n monitoring logs deploy/synth | grep -q 'backfill done'"
pass "metrics backend holds a week of labelled synthetic history (not evidence of anything)"

# --- the demo workload and its autoscaler (Recommend) -------------------------------------------------------------
k apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: web, namespace: demo}
spec:
  replicas: 1
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec: {containers: [{name: web, image: registry.k8s.io/pause:3.10}]}
EOF

# --- install the chart ----------------------------------------------------------------------------------------------
api_ep="$(k get endpoints kubernetes -o jsonpath='{.subsets[0].addresses[0].ip}')"
api_port="$(k get endpoints kubernetes -o jsonpath='{.subsets[0].ports[0].port}')"
cat > "$WORK/values.yaml" <<EOF
images:
  operator: {repository: ${OPERATOR_IMAGE%:*}, tag: ${OPERATOR_IMAGE##*:}}
  forecaster: {repository: ${FORECASTER_IMAGE%:*}, tag: ${FORECASTER_IMAGE##*:}}
  pullPolicy: Never
prometheus: {url: "http://vm.monitoring.svc:8428"}
forecaster: {resources: {requests: {cpu: 200m, memory: 512Mi}, limits: {memory: 3Gi}}, persistence: {size: 1Gi}}
training:
  targets: [{namespace: demo, name: web-pa, schedule: "0 0 1 1 *"}]
  epochs: 2
  resources: {requests: {cpu: 200m, memory: 512Mi}, limits: {memory: 3Gi}}
networkPolicy:
  kubeAPI: {cidrs: ["$api_ep/32"], ports: [$api_port]}
  prometheus:
    namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: monitoring}}
    podSelector: {matchLabels: {app: vm}}
    ports: [8428]
EOF
guard
h install pa "$ROOT/charts/predictive-autoscaler" -n "$NS" --create-namespace -f "$WORK/values.yaml" --wait --timeout 6m >/dev/null \
  || die "helm install"
pass "chart installed (operator passed the CRD gate: ready)"

k apply -f - >/dev/null <<EOF
apiVersion: autoscaling.devkuban.com/v1alpha1
kind: PredictiveAutoscaler
metadata: {name: web-pa, namespace: demo}
spec:
  targetDeployment: {name: web, namespace: demo}
  minReplicas: 1
  maxReplicas: 6
  metrics:
    requests:
      enabled: true
      targetRPS: 4
      source: {preset: prometheus, query: '$QUERY'}
  prediction: {updateIntervalSeconds: 60, horizonMinutes: 60, leadTimeMinutes: 15}
EOF

# 1. cold install
wait_for 240 "the autoscaler compiles its query and measures telemetry" sh -c "[ \"\$(kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n demo get pa web-pa -o jsonpath='{.status.conditions[?(@.type==\"TelemetryAvailable\")].status}')\" = True ]"
[ -n "$(k -n demo get pa web-pa -o jsonpath='{.status.metricSource.sha256}')" ] || die "status.metricSource not published"
[ "$(k -n demo get pa web-pa -o jsonpath='{.status.mode}')" = Recommend ] || die "a new autoscaler must run in Recommend mode"
[ "$(cond ForecastAvailable)" != True ] || die "no model exists yet, yet a forecast was used"
[ -n "$(k -n demo get pa web-pa -o jsonpath='{.status.calculatedReplicas}')" ] || die "no reactive recommendation"
[ "$(cond ConflictDetected)" = False ] || die "ConflictDetected must be False"
[ "$(k -n demo get deploy web -o jsonpath='{.spec.replicas}')" = 1 ] || die "Recommend mode changed the Deployment"
pass "cold install: query compiled, telemetry measured, reactive recommendation $(k -n demo get pa web-pa -o jsonpath='{.status.calculatedReplicas}'), nothing scaled"

# 2. real permissions
can() { k auth can-i --as="system:serviceaccount:$NS:pa-predictive-autoscaler-$1" "${@:2}" 2>/dev/null || true; }
# (TYPE/NAME means a named object to can-i: the scale subresource needs --subresource)
[ "$(can operator update deployments --subresource=scale -n demo)" = yes ] || die "the operator cannot update deployments/scale"
[ "$(can operator update deployments -n demo)" = no ] || die "the operator may update whole Deployments"
[ "$(can operator get customresourcedefinitions/predictiveautoscalers.autoscaling.devkuban.com)" = yes ] || die "the operator cannot read its CRD"
for sa in operator forecaster trainer; do
  for denied in "create jobs -n $NS" "create pods -n demo" "get secrets -n $NS" "list secrets -A" "get customresourcedefinitions"; do
    # shellcheck disable=SC2086
    [ "$(can $sa $denied)" = no ] || die "$sa may $denied"
  done
done
for sa in forecaster trainer; do
  [ "$(can $sa get predictiveautoscalers -n demo)" = yes ] || die "$sa cannot get autoscalers"
  [ "$(can $sa list predictiveautoscalers -n demo)" = no ] || die "$sa may list autoscalers"
  [ "$(can $sa update deployments --subresource=scale -n demo)" = no ] || die "$sa may scale"
done
pass "service accounts: exactly the documented permissions (spot-checked with auth can-i)"

# 3. training: publish, reload, use
train() {
  local cj; cj="$(k -n "$NS" get cronjob -l app.kubernetes.io/component=trainer -o jsonpath='{.items[0].metadata.name}')"
  k -n "$NS" create job --from="cronjob/$cj" "train-$1" >/dev/null
  wait_for 1500 "training $1 completes" k -n "$NS" wait --for=condition=complete "job/train-$1" --timeout=5s
}
train 1
wait_for 300 "the forecasting service reloads the model and the operator uses it" sh -c "[ \"\$(kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n demo get pa web-pa -o jsonpath='{.status.conditions[?(@.type==\"ForecastAvailable\")].status}')\" = True ]"
! fresh_forecast_after "" || die "fresh_forecast_after accepted an empty reference time with an old forecast"
! wait_replacement forecaster "" >/dev/null 2>&1 || die "wait_replacement accepted an empty old UID (the incumbent)"
pass "trained, published, reloaded, forecast used (forecastReplicas $(k -n demo get pa web-pa -o jsonpath='{.status.forecastReplicas}'))"

# 4. Active scales
k -n demo patch pa web-pa --type merge -p '{"spec":{"mode":"Active"}}' >/dev/null
wait_for 240 "Active mode writes the calculated replica count" sh -c "
  k() { kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' \"\$@\"; }
  want=\$(k -n demo get pa web-pa -o jsonpath='{.status.calculatedReplicas}'); got=\$(k -n demo get deploy web -o jsonpath='{.spec.replicas}')
  [ \"\$(k -n demo get pa web-pa -o jsonpath='{.status.mode}')\" = Active ] && [ -n \"\$want\" ] && [ \"\$want\" = \"\$got\" ]"
pass "Active mode scaled web to $(k -n demo get deploy web -o jsonpath='{.spec.replicas}') through /scale"

# 5. persistent reload
cur="$(current_app_pod forecaster)" || die "no single forecasting-service pod before the restart"
read -r old_name old_uid <<<"$cur"
k -n "$NS" delete pod "$old_name" --wait=true >/dev/null
read -r _ _ ready_at <<<"$(wait_replacement forecaster "$old_uid")"
[ -n "$ready_at" ] || die "no Ready replacement forecasting-service pod"
wait_for 300 "a forecast from the restarted forecasting service is used" fresh_forecast_after "$ready_at"
pass "the model survives a forecasting-service restart (loaded from the volume, no retraining)"

# 6. a query change refuses the old model until retrained
new_query="${QUERY/\[2m\]/[3m]}"
k -n demo patch pa web-pa --type merge -p "{\"spec\":{\"metrics\":{\"requests\":{\"source\":{\"query\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$new_query")}}}}}" >/dev/null
wait_for 240 "the model for the old query is refused" sh -c "[ \"\$(kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n demo get pa web-pa -o jsonpath='{.status.conditions[?(@.type==\"ForecastAvailable\")].status}')\" = False ]"
train 2
wait_for 300 "the retrained model is used" sh -c "[ \"\$(kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n demo get pa web-pa -o jsonpath='{.status.conditions[?(@.type==\"ForecastAvailable\")].status}')\" = True ]"
pass "a query change refused the old model; retraining restored the forecast"

# 7. the CRD gate
crd=predictiveautoscalers.autoscaling.devkuban.com
guard
k label crd "$crd" autoscaling.devkuban.com/crd-revision=0 --overwrite >/dev/null
cur="$(current_app_pod operator)" || die "no single operator pod before the gate check"
read -r old_op old_op_uid <<<"$cur"
k -n "$NS" delete pod "$old_op" --wait=true >/dev/null
sleep 20
op_pod="$(app_pods operator | awk -v old="$old_op_uid" '$2 != old {print $1}' | head -1)"
[ -n "$op_pod" ] || die "no replacement operator pod"
[ "$(k -n "$NS" get pod "$op_pod" -o jsonpath='{.status.containerStatuses[0].ready}')" = false ] || die "the operator is ready on an incompatible CRD"
[ "$(k -n "$NS" get pod "$op_pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')" = 0 ] || die "the gate must keep the operator alive (no restarts)"
k -n "$NS" logs "$op_pod" | grep -q "Waiting for a compatible CRD" || die "the gate's reason is not logged"
k label crd "$crd" autoscaling.devkuban.com/crd-revision=1 --overwrite >/dev/null
wait_for 120 "the operator becomes ready once the CRD fits" k -n "$NS" wait --for=condition=ready "pod/$op_pod" --timeout=5s
pass "the CRD gate held the operator unready (alive) on an older revision and released it"

# 8. network policies
# probe <name> <labels> <namespace> <url> <expect ok|blocked>: prints what a pod with those labels observes. Fresh
# connections every time: an expected "ok" must answer at least once within 60 s; an expected "blocked" must fail 3
# times in a row within 60 s (kind's policy agent lets a brand-new pod's first connections through until it has
# learned the pod: seen 2026-10-10). Probe pods have a readiness gate nothing sets, so a probe carrying an application's
# labels can never become an endpoint of that application's Service.
probe() {
  local name=$1 labels=$2 ns=$3 url=$4 expect=$5
  local code='import ssl,sys,time,urllib.request,urllib.error
url, expect = sys.argv[1], sys.argv[2]
ctx = ssl._create_unverified_context()
def reach():
    try:
        urllib.request.urlopen(url, timeout=4, context=ctx); return True
    except urllib.error.HTTPError: return True
    except Exception: return False
deadline, streak = time.time() + 60, 0
while time.time() < deadline:
    if expect == "ok" and reach(): print("ok"); sys.exit(0)
    if expect == "blocked":
        streak = 0 if reach() else streak + 1
        if streak >= 3: print("blocked"); sys.exit(0)
    time.sleep(2)
print("blocked" if expect == "ok" else "ok")'
  k -n "$ns" run "$name" --restart=Never --image="$FORECASTER_IMAGE" --image-pull-policy=Never --labels="$labels" \
    --overrides='{"apiVersion":"v1","spec":{"readinessGates":[{"conditionType":"smoke.devkuban.com/never-ready"}]}}' \
    --command -- python3 -c "$code" "$url" "$expect" >/dev/null
  k -n "$ns" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$name" --timeout=120s >/dev/null 2>&1 || true
  k -n "$ns" logs "$name" 2>/dev/null | tail -1
  k -n "$ns" delete pod "$name" --wait=false >/dev/null 2>&1 || true
}
sel="app.kubernetes.io/name=predictive-autoscaler,app.kubernetes.io/instance=pa"
forecaster="http://pa-predictive-autoscaler-forecaster.$NS.svc:8000/health"
https_other="https://synth.monitoring.svc:6443/"     # HTTPS on the API server's port, at another address
# Positive controls from a pod no policy selects: the destinations denied below are reachable, so a denial is the policy.
[ "$(probe np-ctl-synth app=control demo http://synth.monitoring.svc:8000/ ok)" = ok ] || die "control: synth unreachable"
[ "$(probe np-ctl-https app=control demo "$https_other" ok)" = ok ] || die "control: the HTTPS destination is unreachable"
[ "$(probe np-1 "$sel,app.kubernetes.io/component=operator" "$NS" "$forecaster" ok)" = ok ] || die "the operator cannot reach the forecasting service"
[ "$(probe np-2 app=intruder "$NS" "$forecaster" blocked)" = blocked ] || die "an unlisted pod in the release namespace reached the forecasting service"
[ "$(probe np-3 app=intruder demo "$forecaster" blocked)" = blocked ] || die "a pod in another namespace reached the forecasting service"
for c in operator forecaster trainer; do
  [ "$(probe "np-api-$c" "$sel,app.kubernetes.io/component=$c" "$NS" https://kubernetes.default.svc/healthz ok)" = ok ] \
    || die "$c cannot reach the API server through its Service (tightened CIDRs)"
  [ "$(probe "np-vm-$c" "$sel,app.kubernetes.io/component=$c" "$NS" http://vm.monitoring.svc:8428/health ok)" = ok ] \
    || die "$c cannot reach Prometheus"
  [ "$(probe "np-https-$c" "$sel,app.kubernetes.io/component=$c" "$NS" "$https_other" blocked)" = blocked ] \
    || die "$c reached HTTPS on the API port at an unlisted address (the tightened CIDRs leak)"
  [ "$(probe "np-other-$c" "$sel,app.kubernetes.io/component=$c" "$NS" http://synth.monitoring.svc:8000/ blocked)" = blocked ] \
    || die "$c reached a pod outside its peers"
done
[ "$(app_pods forecaster | wc -l | tr -d ' ')" = 1 ] || die "the forecaster Service must have one application pod"
pass "network policies: forecaster reachable only from the operator; every component reaches DNS, the API Service and Prometheus only; HTTPS on the API port elsewhere is denied"

# 9. upgrade and uninstall retention
replicas_before="$(k -n demo get deploy web -o jsonpath='{.spec.replicas}')"
guard
cur="$(current_app_pod forecaster)" || die "no single forecasting-service pod before the upgrade"
read -r _ old_uid <<<"$cur"
# The upgrade changes the forecasting service's pod template, so it is replaced (Recreate) and must reload the model.
h upgrade pa "$ROOT/charts/predictive-autoscaler" -n "$NS" -f "$WORK/values.yaml" --set training.hours=192 \
  --set-string forecaster.podAnnotations.smoke/upgrade=2 --wait --timeout 6m >/dev/null || die "helm upgrade"
read -r _ _ ready_at <<<"$(wait_replacement forecaster "$old_uid")"
[ -n "$ready_at" ] || die "no Ready replacement forecasting-service pod"     # dies if the pod was not replaced
wait_for 300 "a forecast from the upgraded forecasting service is used" fresh_forecast_after "$ready_at"
h uninstall pa -n "$NS" --wait >/dev/null || die "helm uninstall"
k get crd "$crd" >/dev/null || die "uninstall deleted the CRD"
k -n demo get pa web-pa >/dev/null || die "uninstall deleted the autoscaler"
k -n "$NS" get pvc pa-predictive-autoscaler-models >/dev/null || die "uninstall deleted the model claim"
[ "$(k -n demo get deploy web -o jsonpath='{.spec.replicas}')" = "$replicas_before" ] || die "uninstall changed the replica count"
pass "upgrade kept the model; uninstall kept the CRD, the autoscaler, the claim and the replica count ($replicas_before)"

log "smoke: ALL PASS"
