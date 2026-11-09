#!/usr/bin/env bash
# The kind smoke of the S5a calibration path (staging/calibrate.py): on its OWN throwaway kind cluster and kubeconfig
# (one control plane, a workload node W and a support node S with the run taint and role labels), with the EXACT
# reviewed fixture and k6 images of a c28-images/v1 record pulled by digest on the nodes, calibrate_kind.py checks
# non-root summary delivery through the termination message, the actual scenario timing, zero drops with exact
# reconciliation against the ingress's own counters, reported drops when the VUs are starved, refusal of truncated or
# malformed summaries, stop (UID-bound Job delete) and activeDeadlineSeconds expiry, cleanup, and image identity.
# It never touches another cluster (every kubectl call names this kubeconfig and context; the server must be local) and
# deletes only the cluster it created. Nothing here is C2.8 evidence.
#
#   deploy/eval-c28/smoke/calibrate-kind.sh --images images.json --out report.json      (KEEP=1 keeps the cluster)
# A fixture built locally and pushed to a throwaway local registry (never Harbor) is reached the same way, by digest:
#   REGISTRY_MIRROR=localhost:5002=http://<registry container>:5000 REGISTRY_CONTAINER=<registry container> ...
# writes a containerd hosts.toml for localhost:5002 on every node and connects that container to the kind network.
# Needs: docker, kind, kubectl, python3 with PyYAML.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PY="${PYTHON:-python3}"
KIND="${KIND:-kind}"
CLUSTER="${CLUSTER:-pa-c28-cal-smoke}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
IMAGES_FILE="" OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --images) IMAGES_FILE="$2"; shift ;;
    --out) OUT="$2"; shift ;;
    *) echo "calibrate-smoke: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
[ -n "$IMAGES_FILE" ] && [ -n "$OUT" ] || { echo "calibrate-smoke: --images and --out are required" >&2; exit 2; }
log() { printf '%s calibrate-smoke: %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
for tool in docker "$KIND" kubectl "$PY"; do command -v "$tool" >/dev/null || die "missing tool: $tool"; done
"$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER" && die "a kind cluster named $CLUSTER exists: not ours, refusing"

WORK="$(mktemp -d)"
KCFG="$WORK/kubeconfig"
OWN_CLUSTER=0
cleanup() {
  local rc=$?
  if [ "$OWN_CLUSTER" = 1 ] && [ "${KEEP:-0}" != 1 ]; then
    "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || log "could not delete $CLUSTER"
  elif [ "$OWN_CLUSTER" = 1 ]; then
    log "kept: KUBECONFIG=$KCFG ; delete with: $KIND delete cluster --name $CLUSTER"
  fi
  [ "${KEEP:-0}" = 1 ] || rm -rf -- "${WORK:?}"
  exit "$rc"
}
trap cleanup EXIT
k() { kubectl --request-timeout=30s --kubeconfig "$KCFG" --context "kind-$CLUSTER" "$@"; }
guard() {
  [ "$(kubectl --kubeconfig "$KCFG" config current-context 2>/dev/null)" = "kind-$CLUSTER" ] || die "kubeconfig context"
  case "$(kubectl --kubeconfig "$KCFG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" in
    https://127.0.0.1:*) ;;
    *) die "the kubeconfig does not point at a local kind API server" ;;
  esac
}

read -r FIXTURE_REF GENERATOR_REF < <("$PY" - "$IMAGES_FILE" <<'PY'
import json, re, sys
d = json.load(open(sys.argv[1]))
assert d.get("schema") == "c28-images/v1" and d.get("verified") is True, "not a verified c28-images/v1 record"
refs = [d["images"][r]["ref"] for r in ("fixture", "generator")]
assert all(re.fullmatch(r"[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}", r) for r in refs), "not pinned by digest"
print(*refs)
PY
) || die "images: $IMAGES_FILE is not a verified c28-images/v1 record pinned by digest"

cat > "$WORK/kind.yaml" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
YAML
OWN_CLUSTER=1
log "creating $CLUSTER"
"$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --config "$WORK/kind.yaml" --kubeconfig "$KCFG" \
  --wait 120s >/dev/null
guard
W="$CLUSTER-worker"; S="$CLUSTER-worker2"
if [ -n "${REGISTRY_MIRROR:-}" ]; then
  MIRROR_HOST="${REGISTRY_MIRROR%%=*}"; MIRROR_URL="${REGISTRY_MIRROR#*=}"
  [[ "$MIRROR_HOST" =~ ^localhost:[0-9]+$ && "$MIRROR_URL" =~ ^http://[a-z0-9.-]+:[0-9]+$ ]] \
    || die "REGISTRY_MIRROR must be localhost:<port>=http://<container>:<port>"
  [ -n "${REGISTRY_CONTAINER:-}" ] && docker container inspect "$REGISTRY_CONTAINER" >/dev/null 2>&1 \
    || die "REGISTRY_CONTAINER must name the running local registry"
  docker network connect kind "$REGISTRY_CONTAINER" 2>/dev/null || true
  for node in "$CLUSTER-control-plane" "$W" "$S"; do
    docker exec "$node" sh -c "mkdir -p /etc/containerd/certs.d/$MIRROR_HOST && printf '[host.\"%s\"]\n' \
      '$MIRROR_URL' > /etc/containerd/certs.d/$MIRROR_HOST/hosts.toml" || die "node $node: registry mirror"
  done
  log "local registry $MIRROR_HOST -> $MIRROR_URL on every node"
fi
# The reviewed images reach the nodes by digest (containerd verifies the manifest digest), as smoke/kind.sh does.
while read -r node ref; do
  docker exec "$node" crictl pull "$ref" >/dev/null || die "node $node: pull $ref"
  docker exec "$node" crictl inspecti -o json "$ref" | "$PY" -c '
import json, sys
st = json.load(sys.stdin)["status"]
short = lambda r: r.split("docker.io/", 1)[-1].split("library/", 1)[-1]
assert short(sys.argv[1]) in [short(x) for x in st.get("repoDigests") or []], st.get("repoDigests")
' "$ref" || die "node $node: $ref is not present by its digest"
done <<NODES
$W $FIXTURE_REF
$S $GENERATOR_REF
NODES
log "reviewed images pulled by digest on W and S"
guard
k taint node "$CLUSTER-control-plane" node-role.kubernetes.io/control-plane:NoSchedule- >/dev/null
k label node "$W" c28.devkuban.com/role=workload --overwrite >/dev/null
k label node "$S" c28.devkuban.com/role=support --overwrite >/dev/null
k taint node "$W" c28.devkuban.com/reserved=c28-cal-smoke:NoSchedule --overwrite >/dev/null
k taint node "$S" c28.devkuban.com/reserved=c28-cal-smoke:NoSchedule --overwrite >/dev/null
guard
rc=0
KUBECONFIG=/dev/null "$PY" "$ROOT/deploy/eval-c28/smoke/calibrate_kind.py" --kubeconfig "$KCFG" \
  --context "kind-$CLUSTER" --images "$IMAGES_FILE" --workload "$W" --support "$S" --out "$OUT" || rc=$?
log "report: $OUT (exit $rc)"
exit "$rc"
