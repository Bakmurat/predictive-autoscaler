#!/bin/bash
# Deploy the predictive-autoscaler benchmark to a self-hosted cluster (prodcluster overlay).
#
#   deploy.sh render          kustomize both parts, substitute placeholders -> rendered/{ml-engine,demo}.yaml
#   deploy.sh label-nodes     ML_NODE, SEASONAL_GEN_NODE, ENSEMBLE_GEN_NODE get the placement labels
#   deploy.sh mask            (re)create ConfigMap ml-engine/validity-mask from validity-mask.json
#   deploy.sh ml-engine       CRD (wait Established), then the rest of rendered/ml-engine.yaml
#   deploy.sh demo            rendered/demo.yaml (namespace, arms, autoscalers, KEDA objects, generators)
#   deploy.sh status          pods, autoscalers and the image references actually running
#
# Required environment (nothing cluster-specific is committed):
#   KUBE_CONTEXT     kubectl context of the target cluster
#   HARBOR_REGISTRY  registry/project holding predictive-autoscaler-ml-api and predictive-autoscaler-operator
#   ML_API_DIGEST, OPERATOR_DIGEST   sha256:<digest> of the images built from GIT_COMMIT_VALUE (see build log)
#   GIT_COMMIT_VALUE full commit the images were built from
#   VM_QUERY_URL     in-cluster Prometheus-compatible query base, e.g. http://vmselect.<ns>.svc:8481/select/0/prometheus
#   VM_WRITE_URL     in-cluster remote-write endpoint, e.g. http://vminsert.<ns>.svc:8480/insert/0/prometheus/api/v1/write
#   VM_IMPORT_URL    in-cluster Prometheus-text import endpoint for the arms' final snapshots,
#                    e.g. http://vminsert.<ns>.svc:8480/insert/0/prometheus/api/v1/import/prometheus
# Optional: KUBECTL (default: kubectl), KUSTOMIZE (default: kustomize, v5.8+; the kubectl-embedded one is too old),
#   RENDER_OUT (default: ./rendered; freeze_capture.py renders into its own private directory),
#   FORCE_CONFLICTS=1 to take over fields owned by another field manager (otherwise conflicts fail the apply).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="${RENDER_OUT:-$HERE/rendered}"
KUBECTL="${KUBECTL:-kubectl}"; KUSTOMIZE="${KUSTOMIZE:-kustomize}"
k(){ "$KUBECTL" --context "$KUBE_CONTEXT" "$@"; }
# Server-side apply: client-side apply merges list entries by the strategic-merge key only (for
# topologySpreadConstraints that is topologyKey), which folded the arms' two hostname constraints into one.
# SSA keys them by (topologyKey, whenUnsatisfiable) and makes the applied manifests authoritative.
# Ownership conflicts are shown, not overridden: set FORCE_CONFLICTS=1 only for a deliberate takeover (e.g. the first
# server-side apply over objects written by client-side applies, done once on 2026-10-05 04:57Z).
sa(){ k apply --server-side ${FORCE_CONFLICTS:+--force-conflicts} --field-manager=predictive-bench-deploy "$@"; }
# Validate a whole file with a server-side dry run before applying any of it: on 2026-10-05 12:20Z the API rejected the
# six arm Deployments (duplicate topologySpreadConstraints key) while the other objects of the same apply went through.
sa_checked(){ sa --dry-run=server -f "$1" > /dev/null || { echo "server-side dry run rejected $1; nothing applied" >&2; exit 3; }; sa -f "$1"; }
need(){ for v in "$@"; do [ -n "${!v:-}" ] || { echo "missing environment variable $v" >&2; exit 2; }; done; }

render() {
  need HARBOR_REGISTRY ML_API_DIGEST OPERATOR_DIGEST GIT_COMMIT_VALUE VM_QUERY_URL VM_WRITE_URL VM_IMPORT_URL
  mkdir -p "$OUT"
  for part in ml-engine demo; do
    "$KUSTOMIZE" build --load-restrictor LoadRestrictionsNone "$HERE/$part" | python3 -c '
import os, re, sys
s = sys.stdin.read()
for key in ("HARBOR_REGISTRY", "ML_API_DIGEST", "OPERATOR_DIGEST", "GIT_COMMIT_VALUE", "VM_QUERY_URL", "VM_WRITE_URL", "VM_IMPORT_URL"):
    s = s.replace(key, os.environ[key])
left = sorted(set(re.findall(r"\b(HARBOR_REGISTRY|ML_API_DIGEST|OPERATOR_DIGEST|GIT_COMMIT_VALUE|VM_QUERY_URL|VM_WRITE_URL|VM_IMPORT_URL|ECR_REGISTRY|IMAGE_TAG)\b", s)))
if left: sys.exit("placeholders left after substitution: %s" % left)
sys.stdout.write(s)' > "$OUT/$part.yaml"
    echo "rendered $OUT/$part.yaml ($(grep -c '^kind:' "$OUT/$part.yaml") objects)"
  done
}

label_nodes() {
  need KUBE_CONTEXT ML_NODE SEASONAL_GEN_NODE ENSEMBLE_GEN_NODE
  [ "$ML_NODE" != "$SEASONAL_GEN_NODE" ] && [ "$ML_NODE" != "$ENSEMBLE_GEN_NODE" ] || { echo "generator nodes must differ from ML_NODE" >&2; exit 2; }
  k label node "$ML_NODE" predictive-bench/ml-node=true --overwrite
  k label node "$SEASONAL_GEN_NODE" predictive-bench/seasonal-generator=true --overwrite
  k label node "$ENSEMBLE_GEN_NODE" predictive-bench/ensemble-generator=true --overwrite
}

mask() {
  need KUBE_CONTEXT
  python3 -c "import json,sys; json.load(open(sys.argv[1]))" "$HERE/validity-mask.json"
  k -n ml-engine create configmap validity-mask --from-file=validity-mask.json="$HERE/validity-mask.json" \
    --dry-run=client -o yaml | sa -f -
}

ml_engine() {
  need KUBE_CONTEXT; [ -s "$OUT/ml-engine.yaml" ] || { echo "run render first" >&2; exit 2; }
  python3 - "$OUT/ml-engine.yaml" "$OUT/ml-engine.crd.yaml" "$OUT/ml-engine.rest.yaml" <<'PY'
import sys
docs = [d for d in open(sys.argv[1]).read().split("\n---\n") if d.strip()]
crd = [d for d in docs if "\nkind: CustomResourceDefinition" in "\n" + d]
rest = [d for d in docs if d not in crd]
open(sys.argv[2], "w").write("\n---\n".join(crd) + "\n"); open(sys.argv[3], "w").write("\n---\n".join(rest) + "\n")
PY
  sa_checked "$OUT/ml-engine.crd.yaml"
  k wait --for=condition=Established crd/predictiveautoscalers.autoscaler.example.com --timeout=60s
  sa_checked "$OUT/ml-engine.rest.yaml"
}

demo() {
  need KUBE_CONTEXT; [ -s "$OUT/demo.yaml" ] || { echo "run render first" >&2; exit 2; }
  sa_checked "$OUT/demo.yaml"
}

status() {
  need KUBE_CONTEXT
  k -n ml-engine get pods -o wide
  k -n demo get pods -o wide
  k -n demo get predictiveautoscalers.autoscaler.example.com,scaledobjects.keda.sh
  k -n ml-engine get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .status.containerStatuses[*]}{.imageID}{" "}{end}{"\n"}{end}'
}

case "${1:-}" in
  render) render ;; label-nodes) label_nodes ;; mask) mask ;; ml-engine) ml_engine ;; demo) demo ;; status) status ;;
  *) sed -n '2,22p' "$0"; exit 2 ;;
esac
