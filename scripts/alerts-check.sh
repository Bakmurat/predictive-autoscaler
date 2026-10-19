#!/usr/bin/env bash
# The chart's alert rules: render them (release "pa", namespace "pa-system", default thresholds), check them with
# promtool and run the unit tests in hack/alerts/alerts-test.yaml (firing, recovery and missing-series cases). The
# PrometheusRule and the VMRule must carry the same rule groups.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v helm >/dev/null || { echo "alerts-check: helm not found" >&2; exit 2; }
if ! command -v promtool >/dev/null; then
  [ -n "${CI:-}" ] && { echo "alerts-check: promtool is required in CI" >&2; exit 2; }
  echo "alerts-check: SKIP (promtool not found)"; exit 0
fi
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
cat > "$WORK/values.yaml" <<'V'
prometheus: {url: "http://prometheus.monitoring.svc:9090"}
networkPolicy: {prometheus: {namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: monitoring}}}}
metrics: {alerts: {prometheusRule: {enabled: true}, vmRule: {enabled: true}}}
V
helm template pa "$ROOT/charts/predictive-autoscaler" -n pa-system -f "$WORK/values.yaml" \
  --show-only templates/alerts.yaml > "$WORK/rendered.yaml"
python3 - "$WORK" <<'PY'
import sys, yaml
work = sys.argv[1]
docs = {d["kind"]: d for d in yaml.safe_load_all(open(f"{work}/rendered.yaml")) if d}
assert set(docs) == {"PrometheusRule", "VMRule"}, sorted(docs)
assert docs["PrometheusRule"]["spec"] == docs["VMRule"]["spec"], "the two rule kinds differ"
yaml.safe_dump({"groups": docs["PrometheusRule"]["spec"]["groups"]}, open(f"{work}/rules.yaml", "w"))
PY
cp "$ROOT/hack/alerts/alerts-test.yaml" "$WORK/"
promtool check rules "$WORK/rules.yaml"
( cd "$WORK" && promtool test rules alerts-test.yaml )
