#!/usr/bin/env bash
# Release gate (apps' PR #8 review, 2026-10-26): what the install smoke test does not prove, on a throwaway kind cluster.
#   A. Leader election with two operator replicas: the Lease is created when absent and renewed; on a graceful shutdown
#      the leader releases it (an update with an empty holder and a 1 s duration, seen in a Lease watch) and the standby
#      that already existed acquires it; LeaderElection events are delivered.
#   B. RBAC narrowing on upgrade: every namespace (cluster-wide write binding) -> two watched namespaces (per-namespace
#      bindings, the cluster-wide one removed) -> one (the dropped namespace's binding removed) -> every namespace again.
#      After each step the complete set of write bindings is compared with the expected one, the release namespace keeps
#      only its Lease and events, and the leader still renews under the new permissions. Checked with
#      `kubectl auth can-i` as the operator's ServiceAccount and with real writes made the way the operator makes them.
#   C. Negative authorization that holds in every scope: no Secrets, no Deployment writes except updating /scale (no
#      PATCH of /scale), no Lease delete, no other Lease, no Leases outside the release namespace.
# NetworkPolicy and Cilium enforcement are separate gates; this one does not establish them.
# It only ever talks to its own cluster: a kubeconfig in its work directory and a context checked before every step.
#   hack/release-gate-kind.sh           build the operator image, create the cluster, run, delete the cluster
#   KEEP=1 hack/release-gate-kind.sh    keep the cluster for inspection (never after a memory-floor stop)
# CLUSTER (default pa-gate-<pid>): the run refuses a name that already exists and deletes only a cluster it created.
# OPERATOR_IMAGE / FORECASTER_IMAGE skip the builds (the forecaster is not under test here; any build of it will do).
# MIN_AVAILABLE_MIB (default 2000): a background monitor stops the run (and deletes the cluster) as soon as the host's
#   available memory falls below it, from before the builds to the end (shared test hosts). Long commands (builds,
#   cluster creation, image loads, helm, rollout waits) run supervised, so the stop interrupts them at once: their
#   process tree gets TERM, then KILL after 10 s, and the run checks that it is gone.
# SELFTEST_MEMFLOOR=1: a control for that path. After the cluster is up the monitor is made to trip while a supervised
#   `sleep 600` blocks; the run must end with 143 within seconds, with the sleep gone and the cluster deleted (also
#   with KEEP=1).
# SELFTEST_STOP_AFTER_UP=1: a control for cleanup. The run stops with success right after the cluster is up; with a kind
#   whose delete fails, the final status must be nonzero.
# Every other foreground command is a kubectl call bounded by --request-timeout or a short local tool.
# NODE_CPUS / NODE_MEMORY (default 1.5 / 4g): caps on the kind node container.
set -euo pipefail
# fd 3 = the run log. Messages go there, so a trap that fires inside a redirected command (e.g. the cluster list written
# to a file) still logs to the run, never into that file.
exec 3>&1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${CLUSTER:-pa-gate-$$}"
WORK="${WORK:-$(mktemp -d)}"
mkdir -p "$WORK"
KCFG="$WORK/kubeconfig"
KIND="${KIND:-kind}"
NS=predictive-autoscaler
REL=pa
FULL=pa-predictive-autoscaler
SA="system:serviceaccount:$NS:$FULL-operator"
LEASE=predictive-autoscaler.autoscaling.devkuban.com
PA_STATUS=predictiveautoscalers.autoscaling.devkuban.com#status
OPERATOR_IMAGE="${OPERATOR_IMAGE:-}"
FORECASTER_IMAGE="${FORECASTER_IMAGE:-}"
MIN_AVAILABLE_MIB="${MIN_AVAILABLE_MIB:-2000}"
DELETE_TIMEOUT="${DELETE_TIMEOUT:-180}"   # seconds for `kind delete` at cleanup, then KILL after 10 s more
NODE_CPUS="${NODE_CPUS:-1.5}"
NODE_MEMORY="${NODE_MEMORY:-4g}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
export KUBECONFIG="$KCFG"
MEMWATCH_PID=""
LEASEWATCH_PID=""
CORDONED=""
CREATED=0      # set just before `kind create`: only a cluster this run created is ever deleted
SUP_PID=""     # the supervised foreground command, if one is running
BREACH="$WORK/memory-floor-breached"

log() { printf '%s  %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&3; }
pass() { log "PASS $*"; }
die() { log "FAIL $*"; diagnose; exit 1; }
# Every API call is bounded, so a hung request cannot outlive a wait_for deadline.
k() { kubectl --kubeconfig "$KCFG" --context "kind-$CLUSTER" --request-timeout=20s "$@"; }
h() { helm --kubeconfig "$KCFG" --kube-context "kind-$CLUSTER" "$@"; }
avail_mib() { awk '/^MemAvailable:/ {print int($2/1024)}' /proc/meminfo; }
guard() {
  [ "$(kubectl --kubeconfig "$KCFG" config current-context 2>/dev/null)" = "kind-$CLUSTER" ] \
    || { echo "gate: the kubeconfig does not point at kind-$CLUSTER; refusing" >&2; exit 2; }
}
wait_for() {
  local secs=$1 what=$2; shift 2
  local deadline=$(( $(date +%s) + secs ))
  until "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || die "$what (after ${secs}s)"
    sleep 2
  done
}
diagnose() {
  [ -f "$KCFG" ] || return 0
  log "---- diagnostics"
  k get pods -A -o wide 2>&1 | tail -20 || true
  k -n "$NS" get lease "$LEASE" -o yaml 2>&1 | sed -n '/^spec:/,$p' || true
  k -n "$NS" logs deploy/$FULL-operator --tail=30 2>&1 || true
}
# tree <pid>: the process and every descendant, children first.
tree() {
  local c
  for c in $(pgrep -P "$1" 2>/dev/null); do tree "$c"; done
  echo "$1"
}
alive() { local p; for p in "$@"; do kill -0 "$p" 2>/dev/null && echo "$p"; done; return 0; }
# stop_child: TERM the supervised command's whole tree (collected once, so an orphaned grandchild is still covered),
# KILL whatever is left after 10 s, and verify that every one of those processes is gone.
stop_child() {
  local pid=$SUP_PID i pids left
  [ -n "$pid" ] || return 0
  SUP_PID=""
  pids="$(tree "$pid")"
  kill -TERM $pids 2>/dev/null || true
  for i in $(seq 1 20); do left="$(alive $pids)"; [ -z "$left" ] && break; sleep 0.5; done
  if [ -n "$left" ]; then kill -KILL $left 2>/dev/null || true; sleep 1; left="$(alive $pids)"; fi
  if [ -n "$left" ]; then log "FAIL supervised processes survived KILL: $(echo $left)"
  else log "stopped the supervised command (pids $(echo $pids))"; fi
}
# sup <command...>: run a long command in the background and wait for it, so a TERM from the memory monitor is handled
# at once (bash defers a trapped signal until a foreground command ends, but not while it waits on a background one).
sup() {
  local rc=0
  "$@" &
  SUP_PID=$!
  wait "$SUP_PID" || rc=$?
  SUP_PID=""
  return "$rc"
}
# cleanup keeps the run's exit status, and a cluster that could not be deleted makes a passing run fail.
cleanup() {
  local rc=$?
  trap - ERR
  stop_child
  [ -n "$LEASEWATCH_PID" ] && kill "$LEASEWATCH_PID" 2>/dev/null || true
  [ -n "$MEMWATCH_PID" ] && kill "$MEMWATCH_PID" 2>/dev/null || true
  if [ "$CREATED" = 1 ]; then
    if [ "${KEEP:-0}" = 1 ] && [ ! -e "$BREACH" ]; then
      [ -n "$CORDONED" ] && k uncordon "$CORDONED" >/dev/null 2>&1 || true
      log "cluster kept: KUBECONFIG=$KCFG"
    else
      [ -e "$BREACH" ] && [ "${KEEP:-0}" = 1 ] && log "memory floor breached: deleting the cluster despite KEEP=1"
      if ! timeout --kill-after=10s "$DELETE_TIMEOUT" "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1; then
        log "FAIL kind delete cluster $CLUSTER (it may still exist)"
        [ "$rc" -ne 0 ] || rc=3
      fi
    fi
  fi
  exit "$rc"
}
trap cleanup EXIT
# A stop is expected, not an error: drop the ERR trap first so the interrupted wait is not reported as one.
trap 'trap - ERR; stop_child; exit 143' TERM INT
# set -e must never end a run silently: name the failing command and line (stderr, so a $(...) never captures it).
set -E
trap 'printf "%s  FAIL unexpected exit %s at line %s: %s\n" "$(date -u +%H:%M:%SZ)" "$?" "$LINENO" "$BASH_COMMAND" >&2' ERR

# --- memory floor: checked from before the builds to the end ---------------------------------------------------------
[ "$(avail_mib)" -ge "$MIN_AVAILABLE_MIB" ] || { echo "gate: host available memory $(avail_mib) MiB < $MIN_AVAILABLE_MIB MiB" >&2; exit 2; }
( main=$$
  while sleep 5; do
    a=$(avail_mib)
    [ "${SELFTEST_MEMFLOOR:-0}" = 1 ] && [ -e "$WORK/selftest-trip" ] && a=0
    if [ "$a" -lt "$MIN_AVAILABLE_MIB" ]; then
      : > "$BREACH"
      log "FAIL host available memory ${a} MiB < ${MIN_AVAILABLE_MIB} MiB; stopping"; kill -TERM "$main"; exit 0
    fi
  done ) &
MEMWATCH_PID=$!

holder() { k -n "$NS" get lease "$LEASE" -o jsonpath='{.spec.holderIdentity}'; }
lease_field() { k -n "$NS" get lease "$LEASE" -o jsonpath="{.spec.$1}"; }
# The holder identity is "<pod name>_<uuid>".
holder_pod() { local h; h="$(holder)"; printf '%s\n' "${h%%_*}"; }
pod_uid() { k -n "$NS" get pod "$1" -o jsonpath='{.metadata.uid}'; }
# Ready operator pods that are not being deleted.
live_operator_pods() {
  k -n "$NS" get pod -l app.kubernetes.io/component=operator -o json \
    | python3 -c 'import json,sys
for p in json.load(sys.stdin)["items"]:
    ready = any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))
    if not p["metadata"].get("deletionTimestamp") and ready: print(p["metadata"]["name"])'
}
# Predicates for wait_for.
has_holder() { [ -n "$(holder)" ]; }
holder_is_live() { local p; p="$(holder_pod)"; [ -n "$p" ] && live_operator_pods | grep -qx "$p"; }
election_event_for() { k -n "$NS" get events -o jsonpath='{range .items[?(@.reason=="LeaderElection")]}{.message}{"\n"}{end}' | grep -q "$1"; }
two_live_operators() { [ "$(live_operator_pods | grep -c .)" -eq 2 ]; }
# renewing <step>: the holder is a Ready, non-terminating operator pod and renews the Lease (same holder, later
# renewTime) under the permissions it has now. Sets RENEWER (not a command substitution, so that die stops the run).
RENEWER=""
renewing() {
  local step=$1 p r1 r2
  wait_for 120 "$step a Ready, non-terminating holder" holder_is_live
  p="$(holder_pod)"; r1="$(lease_field renewTime)"; sup sleep 12; r2="$(lease_field renewTime)"
  [ "$(holder_pod)" = "$p" ] || die "$step the holder changed while sampling renewals"
  [ -n "$r1" ] && [[ "$r2" > "$r1" ]] || die "$step renewTime did not advance ($r1 -> $r2)"
  RENEWER="$p"
}
# can <yes|no> <verb> <resource> [namespace]: the operator's ServiceAccount, as the API server authorizes it.
# <resource> is "res", "res/name" (a named object, as kubectl reads it) or "res#sub" (a subresource such as scale).
can() {
  local want=$1 verb=$2 res=$3 ns=${4:-} got
  local args=("$verb" "${res%%#*}" --as="$SA")
  case "$res" in *'#'*) args+=(--subresource="${res#*#}") ;; esac
  [ -n "$ns" ] && args+=(-n "$ns")
  got="$(k auth can-i "${args[@]}" 2>/dev/null || true)"
  [ "$got" = "$want" ] || die "can-i $verb $res ${ns:+-n $ns}: got '$got', want '$want'"
}
# Presence and absence fail closed: a lookup error aborts instead of reading as "absent".
cluster_write_binding() {
  local out
  out="$(k get clusterrolebinding "$FULL-operator-write" --ignore-not-found -o name)" || die "lookup of the cluster-wide write binding failed"
  [ -n "$out" ]
}
# The namespaces holding the operator's write RoleBinding, sorted, space-separated.
write_binding_namespaces() {
  local out
  out="$(k get rolebinding -A --field-selector "metadata.name=$FULL-operator-write" \
    -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}')" || die "listing the write RoleBindings failed"
  # No match is a valid answer (B1/B4 expect none): grep's exit 1 must not trip pipefail.
  printf '%s\n' "$out" | { grep . || true; } | sort | tr '\n' ' ' | sed 's/ $//'
}
# bindings <step> <cluster|none> "<namespaces>": the complete expected set of write bindings.
bindings() {
  local step=$1 cluster=$2 want=$3 got
  if cluster_write_binding; then [ "$cluster" = cluster ] || die "$step a cluster-wide write binding exists"
  else [ "$cluster" = none ] || die "$step no cluster-wide write binding"; fi
  got="$(write_binding_namespaces)" || die "$step listing the write RoleBindings failed"
  [ "$got" = "$want" ] || die "$step write RoleBindings in '$got', want '$want'"
}
# scale_as <yes|no> <ns> <replicas>: a real write the way the operator makes it, a GET and an update (PUT) of
# deployments/scale as its ServiceAccount (kubectl scale would PATCH, which the chart deliberately does not grant).
# A forbidden namespace is read by the administrator so that only the PUT is under test there.
scale_as() {
  local want=$1 ns=$2 n=$3 out obj path="/apis/apps/v1/namespaces/$2/deployments/web/scale"
  if [ "$want" = yes ]; then obj="$(k get --raw "$path" --as="$SA")" || die "GET /scale in $ns as the operator failed"
  else obj="$(k get --raw "$path")" || die "GET /scale in $ns failed"; fi
  obj="$(printf '%s' "$obj" | python3 -c 'import json,sys; o=json.load(sys.stdin); o["spec"]["replicas"]=int(sys.argv[1]); print(json.dumps(o))' "$n")"
  if out="$(printf '%s' "$obj" | k replace --raw "$path" -f - --as="$SA" 2>&1)"; then
    [ "$want" = yes ] || die "impersonated scale update in $ns succeeded; want forbidden"
    [ "$(k -n "$ns" get deploy web -o jsonpath='{.spec.replicas}')" = "$n" ] || die "scale update in $ns did not set $n replicas"
  else
    [ "$want" = no ] || die "impersonated scale update in $ns failed: $out"
    case "$out" in *Forbidden*|*forbidden*) ;; *) die "impersonated scale update in $ns failed for another reason: $out" ;; esac
  fi
}
watched() {  # the operator may scale and report here
  local ns=$1
  can yes get deployments#scale "$ns"; can yes update deployments#scale "$ns"
  can yes update "$PA_STATUS" "$ns"; can yes patch "$PA_STATUS" "$ns"
  can yes create events "$ns"; can yes create events.events.k8s.io "$ns"
}
unwatched() {  # nothing written here (the release namespace is checked separately)
  local ns=$1 g
  can no get deployments#scale "$ns"; can no update deployments#scale "$ns"
  can no update "$PA_STATUS" "$ns"; can no patch "$PA_STATUS" "$ns"
  for g in events events.events.k8s.io; do can no create "$g" "$ns"; can no patch "$g" "$ns"; done
}
release_ns_narrowed() {  # with namespaces narrowed: the release namespace keeps only its Lease and events
  local g
  can no update deployments#scale "$NS"; can no update "$PA_STATUS" "$NS"; can no patch "$PA_STATUS" "$NS"
  for g in events events.events.k8s.io; do can yes create "$g" "$NS"; can yes patch "$g" "$NS"; done
  can yes get "leases/$LEASE" "$NS"; can yes update "leases/$LEASE" "$NS"; can yes create leases "$NS"
}
negatives() {  # C: holds in every scope
  local ns
  for ns in "$NS" ns-a ns-b ns-c; do
    can no get secrets "$ns"; can no list secrets "$ns"
    can no update deployments "$ns"; can no patch deployments "$ns"
    can no patch deployments#scale "$ns"   # the operator only updates /scale
    can no delete leases "$ns"
    can no update leases/other-lease "$ns"
  done
  can no create leases ns-a
  can no update "leases/$LEASE" ns-a
  can no create clusterrolebindings
}
upgrade() {  # upgrade <description> <helm --set args...>
  local what=$1; shift
  guard
  sup h upgrade "$REL" "$ROOT/charts/predictive-autoscaler" -n "$NS" -f "$WORK/values.yaml" "$@" >/dev/null || die "helm upgrade ($what)"
  sup k -n "$NS" rollout status deploy/$FULL-operator --timeout=300s >/dev/null || die "operator rollout ($what)"
}

# --- cluster and images ---------------------------------------------------------------------------------------------
for t in docker "$KIND" kubectl helm python3 pgrep timeout; do command -v "$t" >/dev/null || { echo "gate: $t not found" >&2; exit 2; }; done
# Into a file, not $(...): a command substitution is a foreground child the monitor's TERM would wait for.
sup timeout --kill-after=10s 60 "$KIND" get clusters > "$WORK/clusters-before.txt" 2>/dev/null || { echo "gate: kind get clusters failed" >&2; exit 2; }
if grep -qx -- "$CLUSTER" "$WORK/clusters-before.txt"; then echo "gate: a kind cluster named $CLUSTER already exists; refusing (it is not this run's)" >&2; exit 2; fi
if [ -z "$OPERATOR_IMAGE" ]; then OPERATOR_IMAGE=pa-gate/operator:dev; sup docker build -q -t "$OPERATOR_IMAGE" "$ROOT/k8s-operator" >/dev/null; fi
if [ -z "$FORECASTER_IMAGE" ]; then FORECASTER_IMAGE=pa-gate/ml-api:dev; sup docker build -q -t "$FORECASTER_IMAGE" "$ROOT/ml-engine" >/dev/null; fi
log "creating kind cluster $CLUSTER (kubeconfig $KCFG)"
CREATED=1   # from here a partial creation is this run's too
sup "$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --kubeconfig "$KCFG" --wait 120s >/dev/null
sup timeout --kill-after=10s 60 docker update --cpus "$NODE_CPUS" --memory "$NODE_MEMORY" --memory-swap "$NODE_MEMORY" "$CLUSTER-control-plane" >/dev/null
guard
for img in "$OPERATOR_IMAGE" "$FORECASTER_IMAGE"; do sup "$KIND" load docker-image --name "$CLUSTER" "$img" >/dev/null; done
pass "cluster up, node capped at $NODE_CPUS CPU / $NODE_MEMORY, images loaded"
if [ "${SELFTEST_STOP_AFTER_UP:-0}" = 1 ]; then log "SELFTEST stopping with success after the cluster is up"; exit 0; fi
if [ "${SELFTEST_MEMFLOOR:-0}" = 1 ]; then
  log "SELFTEST memory floor: tripping the monitor while a supervised sleep 600 blocks"
  : > "$WORK/selftest-trip"
  sup sleep 600
  die "SELFTEST the supervised sleep was not interrupted"
fi

for ns in ns-a ns-b ns-c; do
  k create namespace "$ns" >/dev/null
  k -n "$ns" create deployment web --image=registry.k8s.io/pause:3.10 --replicas=1 >/dev/null
done
leases="$(k get lease -A --field-selector "metadata.name=$LEASE" -o name)" || die "listing Leases failed"
[ -z "$leases" ] || die "the operator Lease exists before the install"
pass "fixture: ns-a/ns-b/ns-c each with deployment web; no operator Lease yet"

api_ep="$(k get endpoints kubernetes -o jsonpath='{.subsets[0].addresses[0].ip}')"
api_port="$(k get endpoints kubernetes -o jsonpath='{.subsets[0].ports[0].port}')"
cat > "$WORK/values.yaml" <<EOF
images:
  operator: {repository: ${OPERATOR_IMAGE%:*}, tag: ${OPERATOR_IMAGE##*:}}
  forecaster: {repository: ${FORECASTER_IMAGE%:*}, tag: ${FORECASTER_IMAGE##*:}}
  pullPolicy: Never
prometheus: {url: "http://vm.monitoring.svc:8428"}
operator: {replicas: 2}
forecaster: {resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 2Gi}}, persistence: {size: 1Gi}}
training:
  targets: [{namespace: ns-a, name: web-pa, schedule: "0 0 1 1 *"}]
  epochs: 1
networkPolicy:   # the chart's required values, as in the smoke test (enforcement is not tested here)
  kubeAPI: {cidrs: ["$api_ep/32"], ports: [$api_port]}
  prometheus:
    namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: monitoring}}
    podSelector: {matchLabels: {app: vm}}
    ports: [8428]
EOF

# --- A. leader election ---------------------------------------------------------------------------------------------
guard
sup h install "$REL" "$ROOT/charts/predictive-autoscaler" -n "$NS" --create-namespace -f "$WORK/values.yaml" >/dev/null || die "helm install"
sup k -n "$NS" rollout status deploy/$FULL-operator --timeout=300s >/dev/null || die "operator rollout"
wait_for 90 "a holder in the Lease" has_holder
wait_for 90 "the holder is a Ready operator pod" holder_is_live
leader="$(holder_pod)"
pass "A1 Lease created when absent; holder $leader"

r1="$(lease_field renewTime)"; sup sleep 25; r2="$(lease_field renewTime)"
[ -n "$r1" ] && [[ "$r2" > "$r1" ]] || die "A2 renewTime did not advance ($r1 -> $r2)"
[ "$(holder_pod)" = "$leader" ] || die "A2 the holder changed without a disruption"
pass "A2 renewed ($r1 -> $r2), same holder"

wait_for 60 "a LeaderElection event for $leader" election_event_for "$leader"
pass "A3 LeaderElection event delivered in $NS"

wait_for 120 "two Ready operator pods" two_live_operators
standby="$(live_operator_pods | grep -v -x "$leader" | head -1)"
[ -n "$standby" ] || die "A4 no standby pod"
standby_uid="$(pod_uid "$standby")"
transitions="$(lease_field leaseTransitions)"
# After a release any candidate may acquire, and the ReplicaSet's replacement pod can win the race (seen in an earlier
# run). Cordoning the only node keeps the replacement Pending, so the pre-existing standby is the only candidate.
node="$(k get nodes -o jsonpath='{.items[0].metadata.name}')"
k cordon "$node" >/dev/null; CORDONED="$node"
# Every Lease update from here on: resourceVersion|holder|leaseDurationSeconds. No request timeout on the watch.
kubectl --kubeconfig "$KCFG" --context "kind-$CLUSTER" -n "$NS" get lease "$LEASE" -w \
  -o jsonpath='{.metadata.resourceVersion}{"|"}{.spec.holderIdentity}{"|"}{.spec.leaseDurationSeconds}{"\n"}' \
  > "$WORK/lease-watch.txt" 2>"$WORK/lease-watch.err" &
LEASEWATCH_PID=$!
wait_for 30 "the Lease watch to start" test -s "$WORK/lease-watch.txt"
t0=$(date +%s%3N)
k -n "$NS" delete pod "$leader" --wait=false >/dev/null
while [ "$(holder_pod)" = "$leader" ] || [ -z "$(holder)" ]; do
  [ $(( $(date +%s%3N) - t0 )) -gt 60000 ] && die "A4 no takeover within 60 s"
  sleep 0.5
done
took_ms=$(( $(date +%s%3N) - t0 )); took="$((took_ms / 1000)).$(printf %03d $((took_ms % 1000)))"
sleep 2; kill "$LEASEWATCH_PID" 2>/dev/null || true; wait "$LEASEWATCH_PID" 2>/dev/null || true; LEASEWATCH_PID=""
new="$(holder_pod)"
[ "$new" = "$standby" ] || die "A4 the Lease went to $new, not the pre-existing standby $standby"
[ "$(pod_uid "$new")" = "$standby_uid" ] || die "A4 the new holder's UID is not the recorded standby's ($standby_uid)"
[ "$(lease_field leaseTransitions)" -gt "${transitions:-0}" ] || die "A4 leaseTransitions did not increase"
# The release is proven by the watch, not by timing: the old leader's record, then a release (empty holder, 1 s
# duration), then the standby's acquisition, in increasing resourceVersion order.
python3 - "$WORK/lease-watch.txt" "$leader" "$standby" <<'PY' || die "A4 the Lease watch does not show release then standby acquisition: $(tr '\n' ' ' < "$WORK/lease-watch.txt")"
import sys
path, leader, standby = sys.argv[1:4]
rows = [l.rstrip("\n").split("|") for l in open(path) if l.count("|") == 2]
rvs = [int(r[0]) for r in rows]
assert rvs == sorted(rvs), "resourceVersions out of order"
pod = lambda h: h.split("_", 1)[0]
i = next(n for n, r in enumerate(rows) if pod(r[1]) == leader)
j = next(n for n, r in enumerate(rows) if n > i and r[1] == "" and r[2] == "1")
next(n for n, r in enumerate(rows) if n > j and pod(r[1]) == standby)
PY
# Separately: with a release the standby acquires on its next 2 s retry; without one it waits out the lease duration.
[ "$took_ms" -lt 10000 ] || die "A4 takeover took ${took}s"
pass "A4 graceful release seen in the Lease watch (empty holder, 1 s) + takeover by the recorded standby $standby (same UID) in ${took}s (transitions ${transitions:-0} -> $(lease_field leaseTransitions))"
wait_for 60 "a LeaderElection event for $standby" election_event_for "$standby"
pass "A5 LeaderElection event for the new leader"
k uncordon "$node" >/dev/null; CORDONED=""
wait_for 120 "two Ready operator pods after uncordon" two_live_operators
[ "$(holder_pod)" = "$standby" ] || die "A6 the new standby took the Lease from a healthy leader"
pass "A6 the replacement joined as a standby; $standby keeps the Lease"

# --- B. RBAC narrowing on upgrade -----------------------------------------------------------------------------------
bindings B1 cluster ""
for ns in ns-a ns-b ns-c "$NS"; do watched "$ns"; done
scale_as yes ns-c 2
negatives
pass "B1 every namespace: only the cluster-wide write binding; scale/status/events allowed everywhere; C negatives hold"

upgrade "watch ns-a,ns-b" --set 'operator.watchNamespaces={ns-a,ns-b}'
bindings B2 none "ns-a ns-b"
watched ns-a; watched ns-b; unwatched ns-c; release_ns_narrowed
scale_as yes ns-a 2; scale_as no ns-c 3
negatives
renewing B2; p="$RENEWER"
pass "B2 narrowed to ns-a,ns-b: write bindings exactly {ns-a, ns-b}; ns-c and the release namespace forbidden to scale/report (can-i + real write); $p holds and renews the Lease"

upgrade "watch ns-a" --set 'operator.watchNamespaces={ns-a}'
bindings B3 none "ns-a"
watched ns-a; unwatched ns-b; unwatched ns-c; release_ns_narrowed
scale_as yes ns-a 1; scale_as no ns-b 2
negatives
renewing B3; p="$RENEWER"
pass "B3 ns-b removed: write bindings exactly {ns-a}; ns-b forbidden (can-i + real write); $p holds and renews the Lease"

upgrade "watch every namespace again" --set 'operator.watchNamespaces=null'
bindings B4 cluster ""
for ns in ns-a ns-b ns-c; do watched "$ns"; done
scale_as yes ns-b 2
negatives
renewing B4; p="$RENEWER"
pass "B4 widened back: only the cluster-wide write binding; per-namespace bindings pruned; $p holds and renews the Lease"

log "release gate: ALL PASS"
