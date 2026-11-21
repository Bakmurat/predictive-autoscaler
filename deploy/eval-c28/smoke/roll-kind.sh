#!/usr/bin/env bash
# The image-roll smoke (staging/imageroll.py against a real API server, a real Deployment controller and a real scrape
# path), on its OWN throwaway kind cluster and kubeconfig. Functional smoke evidence, not production performance or
# admission evidence. It never touches another cluster (every call names this kubeconfig and context; the API server
# must be local; the helpers check the recorded cluster UID) and deletes only the cluster it created.
#
#   deploy/eval-c28/smoke/roll-kind.sh --images images.json --new-fixture <repo>@sha256:... --out DIR \
#                                      [--scenario forward|reverse] [STAGE...]
#
# Two scenarios, each on a cluster of its own (run one after the other; smoke/roll_kind.py describes every stage):
#   forward (default; 12 replicas per target; about 90 minutes): install, bedcheck, forward (open; a roll whose
#           executor is killed after two members; recovery; the reviewed resume; cold completion), admission (ONE
#           unmodified bounded admission run: the origin's first minute), churn;
#   reverse (4 replicas, a reduced size; about 10 minutes): install, bedcheck, reverse (a roll held and killed before
#           the fourth member, then taken back through a successor transition).
# What it does NOT show: a lost patch response (covered by the API-server test and the unit tests), the same endpoint
# identity leaving and returning (the fixture's unit tests), a lab cluster's nodes, steal or timing.
#
# The bed: one control plane, a workload node W and a support node S; the victoria-metrics-k8s-stack chart at the
# version the lab runs (operator, vmagent, vmsingle, node-exporter, the kubelet/cAdvisor scrape), pinned to the control
# plane so that W and S hold only what a reservation allows; the reviewed images pulled by digest on the nodes with no
# registry credential. smoke/roll_kind.py then installs the demonstration as a real installation is made and drives
# the roll. KEEP=1 keeps the cluster. Needs: docker, kind, kubectl, helm, python3 with PyYAML.
# A fixture built locally and pushed to a throwaway local registry (never a shared one) is reached the same way, by
# digest (development only; a qualifying run uses published digests):
#   REGISTRY_MIRROR=localhost:5002=http://<registry container>:5000 REGISTRY_CONTAINER=<registry container> ...
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PY="${PYTHON:-python3}"
KIND="${KIND:-kind}"
CLUSTER="${CLUSTER:-pa-c28-roll-smoke}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
VM_CHART_VERSION="${VM_CHART_VERSION:-0.73.0}"
VM_CHART_SHA256="${VM_CHART_SHA256:-de46d8cfcd32007b43074bafc58a1b0c27d1ff7aab513c212049fe1edffafa16}"
IMAGES_FILE="" NEW_FIXTURE="" OUT="" SCENARIO=forward
STAGES=()
while [ $# -gt 0 ]; do
  case "$1" in
    --images) IMAGES_FILE="$2"; shift ;;
    --new-fixture) NEW_FIXTURE="$2"; shift ;;
    --out) OUT="$2"; shift ;;
    --scenario) SCENARIO="$2"; shift ;;
    -*) echo "roll-smoke: unknown argument: $1" >&2; exit 2 ;;
    *) STAGES+=("$1") ;;
  esac
  shift
done
[ -n "$IMAGES_FILE" ] && [ -n "$OUT" ] || { echo "roll-smoke: --images and --out are required" >&2; exit 2; }
case "$SCENARIO" in
  forward) DEFAULT_STAGES=(install bedcheck forward admission churn); DEFAULT_REPLICAS=12 ;;
  reverse) DEFAULT_STAGES=(install bedcheck reverse); DEFAULT_REPLICAS=4 ;;
  *) echo "roll-smoke: --scenario is forward or reverse" >&2; exit 2 ;;
esac
REPLICAS="${REPLICAS:-$DEFAULT_REPLICAS}"
[ "${#STAGES[@]}" -gt 0 ] || STAGES=("${DEFAULT_STAGES[@]}")
log() { printf '%s roll-smoke: %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
for tool in docker "$KIND" kubectl helm "$PY"; do command -v "$tool" >/dev/null || die "missing tool: $tool"; done
"$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER" && die "a kind cluster named $CLUSTER exists: not ours, refusing"
[ ! -e "$OUT" ] || die "$OUT exists: every run writes a new directory"
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
KCFG="$OUT/kubeconfig"
export HELM_CONFIG_HOME="$OUT/helm/config" HELM_CACHE_HOME="$OUT/helm/cache" HELM_DATA_HOME="$OUT/helm/data"
OWN_CLUSTER=0 FORWARD_PID=""
cleanup() {
  local rc=$?
  [ -z "$FORWARD_PID" ] || kill "$FORWARD_PID" 2>/dev/null || true
  if [ "$OWN_CLUSTER" = 1 ] && [ "${KEEP:-0}" != 1 ]; then
    if "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1; then echo "deleted $CLUSTER" > "$OUT/cleanup.txt"
    else echo "COULD NOT delete $CLUSTER" > "$OUT/cleanup.txt"; log "could not delete $CLUSTER"; fi
  elif [ "$OWN_CLUSTER" = 1 ]; then
    echo "kept $CLUSTER (KEEP=1)" > "$OUT/cleanup.txt"
    log "kept: kubeconfig $KCFG ; delete with: $KIND delete cluster --name $CLUSTER"
  fi
  exit "$rc"
}
trap cleanup EXIT
k() { kubectl --request-timeout=30s --kubeconfig "$KCFG" --context "kind-$CLUSTER" "$@"; }
h() { helm --kubeconfig "$KCFG" --kube-context "kind-$CLUSTER" "$@"; }
guard() {
  [ "$(kubectl --kubeconfig "$KCFG" config current-context 2>/dev/null)" = "kind-$CLUSTER" ] || die "kubeconfig context"
  case "$(kubectl --kubeconfig "$KCFG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" in
    https://127.0.0.1:*) ;;
    *) die "the kubeconfig does not point at a local kind API server" ;;
  esac
}
mapfile -t REFS < <("$PY" - "$IMAGES_FILE" <<'PY'
import json, re, sys
d = json.load(open(sys.argv[1]))
assert d.get("schema") == "c28-images/v1" and d.get("verified") is True, "not a verified c28-images/v1 record"
for role in ("fixture", "operator", "statistical", "generator"):
    ref = d["images"][role]["ref"]
    assert re.fullmatch(r"[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}", ref), f"{role} is not pinned by digest"
    print(ref)
PY
) || die "images: $IMAGES_FILE is not a verified c28-images/v1 record pinned by digest"
FIXTURE_REF="${REFS[0]}" OPERATOR_REF="${REFS[1]}" STATISTICAL_REF="${REFS[2]}" GENERATOR_REF="${REFS[3]}"
[ -z "$NEW_FIXTURE" ] || [[ "$NEW_FIXTURE" =~ ^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$ ]] || die "--new-fixture is not pinned by digest"
[ "$NEW_FIXTURE" != "$FIXTURE_REF" ] || die "--new-fixture is the installed fixture"
cp "$IMAGES_FILE" "$OUT/images.json"

cat > "$OUT/kind.yaml" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
YAML
OWN_CLUSTER=1
log "creating $CLUSTER"
"$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --config "$OUT/kind.yaml" --kubeconfig "$KCFG" \
  --wait 180s >/dev/null 2>&1 || die "kind create cluster"
guard
CP="$CLUSTER-control-plane"; W="$CLUSTER-worker"; S="$CLUSTER-worker2"
k taint node "$CP" node-role.kubernetes.io/control-plane:NoSchedule- >/dev/null

if [ -n "${REGISTRY_MIRROR:-}" ]; then
  MIRROR_HOST="${REGISTRY_MIRROR%%=*}"; MIRROR_URL="${REGISTRY_MIRROR#*=}"
  [[ "$MIRROR_HOST" =~ ^localhost:[0-9]+$ && "$MIRROR_URL" =~ ^http://[a-z0-9.-]+:[0-9]+$ ]] \
    || die "REGISTRY_MIRROR must be localhost:<port>=http://<container>:<port>"
  [ -n "${REGISTRY_CONTAINER:-}" ] && docker container inspect "$REGISTRY_CONTAINER" >/dev/null 2>&1 \
    || die "REGISTRY_CONTAINER must name the running local registry"
  docker network connect kind "$REGISTRY_CONTAINER" 2>/dev/null || true
  for node in "$CP" "$W" "$S"; do
    docker exec "$node" sh -c "mkdir -p /etc/containerd/certs.d/$MIRROR_HOST && printf '[host.\"%s\"]\n' \
      '$MIRROR_URL' > /etc/containerd/certs.d/$MIRROR_HOST/hosts.toml" || die "node $node: registry mirror"
  done
  echo "local registry $MIRROR_HOST -> $MIRROR_URL (development: not a qualifying run)" > "$OUT/local-registry.txt"
  log "local registry $MIRROR_HOST -> $MIRROR_URL on every node (development)"
fi
# The reviewed images reach the nodes by digest, pulled INSIDE the nodes with no registry credential configured
# there (containerd verifies the manifest digest). A pull that needs a credential fails here, and that is the answer.
pull() {   # pull <node> <ref>
  docker exec "$1" crictl pull "$2" >/dev/null 2>"$OUT/pull.err" || die "node $1: anonymous pull of $2 failed"
  echo "$1 $2" >> "$OUT/pulled-by-digest.txt"
}
for ref in "$FIXTURE_REF" ${NEW_FIXTURE:+"$NEW_FIXTURE"}; do pull "$W" "$ref"; pull "$S" "$ref"; done
pull "$S" "$OPERATOR_REF"; pull "$S" "$STATISTICAL_REF"; pull "$S" "$GENERATOR_REF"
log "images pulled by digest on W and S without a credential"

# The metrics path of the lab: the same chart, reconciled by its operator; everything but node-exporter on the
# control plane, so W and S hold only what a reservation allows.
h repo add vm https://victoriametrics.github.io/helm-charts/ >/dev/null 2>&1 || die "helm repo add"
( cd "$OUT" && helm pull vm/victoria-metrics-k8s-stack --version "$VM_CHART_VERSION" ) || die "chart pull"
CHART_TGZ="$OUT/victoria-metrics-k8s-stack-$VM_CHART_VERSION.tgz"
[ "$(sha256sum "$CHART_TGZ" | cut -c1-64)" = "$VM_CHART_SHA256" ] || die "the metrics chart is not the pinned file"
cat > "$OUT/vm-values.yaml" <<'YAML'
fullnameOverride: vmst
vmsingle: {enabled: true, spec: {retentionPeriod: "1d", nodeSelector: {node-role.kubernetes.io/control-plane: ""}}}
vmagent: {enabled: true, spec: {nodeSelector: {node-role.kubernetes.io/control-plane: ""}}}
victoria-metrics-operator: {nodeSelector: {node-role.kubernetes.io/control-plane: ""}}
prometheus-node-exporter: {enabled: true}
kubelet: {enabled: true}
vmcluster: {enabled: false}
vmalert: {enabled: false}
alertmanager: {enabled: false}
grafana: {enabled: false}
defaultRules: {create: false}
defaultDashboards: {enabled: false}
kube-state-metrics: {enabled: false}
kubeApiServer: {enabled: false}
kubeControllerManager: {enabled: false}
kubeScheduler: {enabled: false}
kubeEtcd: {enabled: false}
coreDns: {enabled: false}
YAML
guard
h install vms "$CHART_TGZ" -n monitoring --create-namespace -f "$OUT/vm-values.yaml" --wait --timeout 12m >/dev/null \
  || die "metrics stack install"
k -n monitoring get pods -o json | "$PY" -c '
import json, sys
pods = json.load(sys.stdin)["items"]
out = [{"pod": p["metadata"]["name"], "node": p["spec"].get("nodeName"),
        "images": sorted(s.get("imageID", "") for s in p["status"].get("containerStatuses") or [])} for p in pods]
json.dump(out, open(sys.argv[1], "w"), indent=1)
bad = [o["pod"] for o in out if not o["pod"].startswith("vms-prometheus-node-exporter-") and not o["node"].endswith("-control-plane")]
assert not bad, f"metrics pods outside the control plane: {bad}"
' "$OUT/metrics-stack-pods.json" || die "the metrics stack is not where the bed needs it"
log "metrics stack $VM_CHART_VERSION up (chart sha256 pinned; component images in metrics-stack-pods.json)"

PORT="$("$PY" -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
k -n monitoring port-forward svc/vmsingle-vmst "$PORT:8428" >"$OUT/port-forward.log" 2>&1 &
FORWARD_PID=$!
guard
"$PY" "$ROOT/deploy/eval-c28/smoke/roll_kind.py" --work "$OUT" --kubeconfig "$KCFG" --context "kind-$CLUSTER" \
  --images "$OUT/images.json" ${NEW_FIXTURE:+--new-fixture "$NEW_FIXTURE"} \
  --query-api "http://127.0.0.1:$PORT/prometheus" --replicas "$REPLICAS" "${STAGES[@]}" || die "the driver failed"
log "PASS: scenario $SCENARIO, $REPLICAS replicas per target: ${STAGES[*]} (functional smoke evidence)"
