#!/usr/bin/env bash
# The chart's static checks: helm lint and helm template of every value set in charts/predictive-autoscaler/ci/, each
# rendered output validated by kubeconform (strict) against Kubernetes 1.33, 1.34 and 1.35. Only kinds without a schema
# there are skipped, by name: the CRD itself (envtest validates it on a real API server) and the optional monitoring
# resources; any other unknown or misspelled kind fails. The render tests
# (ml-engine/tests/test_chart.py) pin what the objects promise; the kind smoke test proves the install works.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/charts/predictive-autoscaler"
VERSIONS="${KUBE_VERSIONS:-1.33.0 1.34.0 1.35.0}"
command -v helm >/dev/null || { echo "chart-check: helm not found" >&2; exit 2; }
command -v kubeconform >/dev/null || { echo "chart-check: kubeconform not found" >&2; exit 2; }
echo "chart-check: $(helm version --short), kubeconform $(kubeconform -v)"
rc=0
for values in "$CHART"/ci/*-values.yaml; do
  name="$(basename "$values")"
  helm lint --strict "$CHART" -f "$values" --kube-version 1.35.0 >/dev/null || { echo "FAIL lint $name" >&2; rc=1; continue; }
  for v in $VERSIONS; do
    if helm template pa "$CHART" -n pa-system --include-crds --kube-version "$v" -f "$values" \
        | kubeconform -strict -summary -kubernetes-version "$v" -output text \
            -skip CustomResourceDefinition,ServiceMonitor,VMServiceScrape; then
      echo "ok   $name on Kubernetes $v"
    else
      echo "FAIL $name on Kubernetes $v" >&2; rc=1
    fi
  done
done
exit "$rc"
