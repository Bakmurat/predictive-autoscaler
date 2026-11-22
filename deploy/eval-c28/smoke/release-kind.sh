#!/usr/bin/env bash
# The release helper's smoke (staging/release.py with the REAL pinned Helm and the real c28-secretmeta against a real
# API server), on its OWN throwaway kind cluster and kubeconfig. Functional smoke evidence, not production evidence.
# It never touches another cluster (every call names this kubeconfig and context; the API server must be local; the
# helpers check the recorded cluster UID) and deletes only the cluster it created.
#
#   deploy/eval-c28/smoke/release-kind.sh --images images.json --new-statistical <repo>@sha256:... \
#       --helm /abs/helm --secretmeta /abs/c28-secretmeta --out DIR --scenario A|B|C|D|E|F|G|H|I|K [STAGE...]
#
# One scenario per cluster (smoke/release_kind.py describes each): the release is installed as revision 1 from the
# INSTALLED chart tree (release.chart_artifact of --from-commit) with the installed values and statistical image, and
# the scenario then drives release.py. The bed: one control plane, a workload node W and a support node S; the reviewed
# images pulled by digest on the nodes with no registry credential; NO metrics stack (nothing here reads telemetry),
# but the scrape CRDs the preparation and the chart create objects of, taken from the pinned metrics chart.
# KEEP=1 keeps the cluster and the result says so. Needs: docker, kind, kubectl, python3 with PyYAML, and the two
# pinned executables. A new statistical image built locally and pushed to a throwaway local registry (never a shared
# one) is reached the same way, by digest (development only; a qualifying run uses published digests):
#   REGISTRY_MIRROR=localhost:5002=http://<registry container>:5000 REGISTRY_CONTAINER=<registry container> ...
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PY="${PYTHON:-python3}"
KIND="${KIND:-kind}"
CLUSTER="${CLUSTER:-pa-c28-release-smoke}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
VM_CHART_VERSION="${VM_CHART_VERSION:-0.73.0}"
VM_CHART_SHA256="${VM_CHART_SHA256:-de46d8cfcd32007b43074bafc58a1b0c27d1ff7aab513c212049fe1edffafa16}"
IMAGES_FILE="" NEW_STATISTICAL="" OUT="" SCENARIO="" HELM_BIN="" META_BIN=""
FROM_COMMIT="${FROM_COMMIT:-e3a0d54cdcc208438f59e499d12aa36272148398}"   # the chart tree the lab release was installed from
STAGES=()
while [ $# -gt 0 ]; do
  case "$1" in
    --images) IMAGES_FILE="$2"; shift ;;
    --new-statistical) NEW_STATISTICAL="$2"; shift ;;
    --helm) HELM_BIN="$2"; shift ;;
    --secretmeta) META_BIN="$2"; shift ;;
    --from-commit) FROM_COMMIT="$2"; shift ;;
    --out) OUT="$2"; shift ;;
    --scenario) SCENARIO="$2"; shift ;;
    -*) echo "release-smoke: unknown argument: $1" >&2; exit 2 ;;
    *) STAGES+=("$1") ;;
  esac
  shift
done
[ -n "$IMAGES_FILE" ] && [ -n "$OUT" ] && [ -n "$NEW_STATISTICAL" ] && [ -n "$HELM_BIN" ] && [ -n "$META_BIN" ] || {
  echo "release-smoke: --images, --new-statistical, --helm, --secretmeta and --out are required" >&2; exit 2; }
case "$SCENARIO" in
  A|B|C|D|E|F|G|H|I|K) ;;
  *) echo "release-smoke: --scenario is one of A B C D E F G H I K" >&2; exit 2 ;;
esac
REPLICAS="${REPLICAS:-2}"
[ "${#STAGES[@]}" -gt 0 ] || STAGES=(install "$SCENARIO")
log() { printf '%s release-smoke: %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
for tool in docker "$KIND" kubectl "$PY"; do command -v "$tool" >/dev/null || die "missing tool: $tool"; done
for bin in "$HELM_BIN" "$META_BIN"; do
  case "$bin" in /*) [ -x "$bin" ] || die "$bin is not an executable file" ;; *) die "$bin: the pinned executables are named by absolute path" ;; esac
done
"$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER" && die "a kind cluster named $CLUSTER exists: not ours, refusing"
[ ! -e "$OUT" ] || die "$OUT exists: every run writes a new directory"
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
KCFG="$OUT/kubeconfig"
export HELM_CONFIG_HOME="$OUT/helm/config" HELM_CACHE_HOME="$OUT/helm/cache" HELM_DATA_HOME="$OUT/helm/data"
OWN_CLUSTER=0 FORWARD_PID="" RESULT=""
# The result is published HERE, after the cleanup: a cluster that could not be deleted is a failed run, whatever the
# stages said. KEEP=1 is the one exception, and the result line then says that the cluster was kept.
cleanup() {
  local rc=$? kept=""
  [ -z "$FORWARD_PID" ] || kill "$FORWARD_PID" 2>/dev/null || true
  if [ "$OWN_CLUSTER" = 1 ] && [ "${KEEP:-0}" != 1 ]; then
    if "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1; then echo "deleted $CLUSTER" > "$OUT/cleanup.txt"
    else
      echo "COULD NOT delete $CLUSTER" > "$OUT/cleanup.txt"
      log "FAIL: could not delete $CLUSTER (delete it with: $KIND delete cluster --name $CLUSTER)"
      [ "$rc" -ne 0 ] || rc=1
    fi
  elif [ "$OWN_CLUSTER" = 1 ]; then
    echo "kept $CLUSTER (KEEP=1)" > "$OUT/cleanup.txt"
    kept=" -- the cluster was KEPT (KEEP=1): kubeconfig $KCFG ; delete with: $KIND delete cluster --name $CLUSTER"
  fi
  if [ "$rc" -eq 0 ] && [ -n "$RESULT" ]; then log "PASS: $RESULT$kept"
  elif [ -n "$kept" ]; then log "${kept# -- }"; fi
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
[[ "$NEW_STATISTICAL" =~ ^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$ ]] || die "--new-statistical is not pinned by digest"
[ "$NEW_STATISTICAL" != "$STATISTICAL_REF" ] || die "--new-statistical is the installed statistical image"
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
pull "$W" "$FIXTURE_REF"; pull "$S" "$FIXTURE_REF"
pull "$S" "$OPERATOR_REF"; pull "$S" "$STATISTICAL_REF"; pull "$S" "$NEW_STATISTICAL"; pull "$S" "$GENERATOR_REF"
log "images pulled by digest on W and S without a credential"

# No metrics stack: nothing in this smoke reads telemetry. The preparation and the chart still create scrape objects,
# so their CRDs come from the pinned metrics chart (the file is checked by sha256; only CustomResourceDefinitions of
# it are applied, nothing of it runs).
( cd "$OUT" && "$HELM_BIN" pull victoria-metrics-k8s-stack --repo https://victoriametrics.github.io/helm-charts/ \
    --version "$VM_CHART_VERSION" >/dev/null 2>&1 ) || die "chart pull"
CHART_TGZ="$OUT/victoria-metrics-k8s-stack-$VM_CHART_VERSION.tgz"
[ "$(sha256sum "$CHART_TGZ" | cut -c1-64)" = "$VM_CHART_SHA256" ] || die "the metrics chart is not the pinned file"
"$HELM_BIN" template crds "$CHART_TGZ" --include-crds --namespace monitoring 2>/dev/null | "$PY" -c '
import sys, yaml
crds = [d for d in yaml.safe_load_all(sys.stdin) if d and d.get("kind") == "CustomResourceDefinition"]
assert crds, "the pinned chart rendered no CustomResourceDefinition"
yaml.safe_dump_all(crds, open(sys.argv[1], "w"))
print(len(crds))' "$OUT/scrape-crds.yaml" > "$OUT/scrape-crds.count" || die "the scrape CRDs could not be taken from the pinned chart"
guard
k apply --server-side -f "$OUT/scrape-crds.yaml" >/dev/null || die "scrape CRDs"
log "scrape CRDs applied from the pinned chart ($(cat "$OUT/scrape-crds.count") definitions; no metrics stack)"

guard
"$PY" "$ROOT/deploy/eval-c28/smoke/release_kind.py" --work "$OUT" --kubeconfig "$KCFG" --context "kind-$CLUSTER" \
  --images "$OUT/images.json" --new-statistical "$NEW_STATISTICAL" --helm "$HELM_BIN" --secretmeta "$META_BIN" \
  --from-commit "$FROM_COMMIT" --replicas "$REPLICAS" --scenario "$SCENARIO" "${STAGES[@]}" || die "the driver failed"
RESULT="scenario $SCENARIO: ${STAGES[*]} (functional smoke evidence)"
