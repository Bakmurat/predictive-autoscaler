#!/usr/bin/env bash
# Reproducible benchmark environment: EKS (Terraform) + predictive autoscaler + demo workloads.
# Prerequisites: aws CLI (authenticated), terraform >= 1.5, kubectl >= 1.30, docker, python3.
# Usage:  ./deploy.sh infra    # terraform apply (two stages), kubeconfig, gp2 un-default
#         ./deploy.sh images   # build + push ml-api and operator to ECR (arch of the nodes)
#         ./deploy.sh app      # monitors, dashboards, ml-engine stack, demo workloads + traffic
#         ./deploy.sh all
#         ./deploy.sh status
#         ./deploy.sh destroy  # kubectl delete PVC-bearing namespaces, then terraform destroy
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
TAG="${IMAGE_TAG:-bench-$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo local)}"
ARCH="${IMAGE_ARCH:-arm64}"   # must match var.instance_type (t4g = arm64)

tf() { (cd "$HERE" && terraform "$@"); }
out() { tf output -raw "$1"; }

infra() {
  tf init -input=false >/dev/null
  tf validate
  # Stage 1: network + cluster, so the kubernetes/helm providers have a live endpoint.
  tf apply -input=false -auto-approve -target=module.vpc -target=module.eks -target=module.ebs_csi_irsa
  # Adopt ECR repositories that already exist (import needs the cluster outputs to be known).
  for r in ml_api:predictive-autoscaler/ml-api operator:predictive-autoscaler/operator; do
    tf state show "aws_ecr_repository.${r%%:*}" >/dev/null 2>&1 && continue
    aws ecr describe-repositories --repository-names "${r#*:}" --region "$(tf output -raw region 2>/dev/null || echo "${AWS_REGION:-ap-southeast-1}")" >/dev/null 2>&1 \
      && tf import -input=false "aws_ecr_repository.${r%%:*}" "${r#*:}" || true
  done
  # Stage 2: everything else.
  tf apply -input=false -auto-approve
  eval "$(out kubeconfig_command)"
  kubectl config use-context "$(out cluster_name)"
  kubectl annotate storageclass gp2 storageclass.kubernetes.io/is-default-class=false --overwrite >/dev/null || true
  kubectl get nodes
}

images() {
  local reg; reg="$(out ecr_registry)"
  aws ecr get-login-password --region "$(out region)" | docker login --username AWS --password-stdin "$reg" >/dev/null
  docker build --platform "linux/$ARCH" -t "$reg/predictive-autoscaler/ml-api:$TAG"   "$REPO/ml-engine"
  docker build --platform "linux/$ARCH" -t "$reg/predictive-autoscaler/operator:$TAG" "$REPO/k8s-operator"
  docker push "$reg/predictive-autoscaler/ml-api:$TAG"
  docker push "$reg/predictive-autoscaler/operator:$TAG"
}

app() {
  local reg prom; reg="$(out ecr_registry)"; prom="$(out prometheus_url_in_cluster)"
  kubectl apply -f "$HERE/monitoring/podmonitor-istio.yaml" -f "$HERE/monitoring/servicemonitors-ml.yaml"
  python3 "$HERE/scripts/render-dashboards.py" | kubectl apply -f -
  mkdir -p "$HERE/rendered"
  (cd "$HERE/overlay" && kubectl kustomize .) \
    | sed -e "s#ECR_REGISTRY#$reg#g" -e "s#IMAGE_TAG#$TAG#g" -e "s#VM_QUERY_URL#$prom#g" > "$HERE/rendered/ml-engine.yaml"
  kubectl apply -f "$HERE/rendered/ml-engine.yaml"
  kubectl wait --for condition=established crd/predictiveautoscalers.autoscaler.example.com --timeout=90s
  { cat "$HERE/demo/00-namespace.yaml"; for f in "$HERE"/demo/nginx-test-*.yaml "$HERE"/demo/myapptwo-*.yaml; do echo "---"; sed -e "s#VM_QUERY_URL#$prom#g" "$f"; done; } > "$HERE/rendered/demo.yaml"
  kubectl apply -f "$HERE/rendered/demo.yaml"
  echo "T0 (traffic start, UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)"
}

status() {
  kubectl get nodes
  kubectl -n ml-engine get pods,pvc,cronjob
  kubectl -n demo get pods,predictiveautoscaler,scaledobject
  echo "Grafana:    kubectl -n monitoring port-forward svc/kps-grafana 3000:80        (admin / var.grafana_admin_password)"
  echo "Prometheus: kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090:9090"
  echo "ml-api:     kubectl -n ml-engine port-forward svc/ml-api-service 8000:8000    -> /health"
  echo "Operator:   kubectl -n ml-engine logs deploy/predictive-operator -f"
}

destroy() {
  # Delete PVC-bearing namespaces first so the EBS CSI driver releases the volumes.
  kubectl delete namespace demo ml-engine --ignore-not-found --wait=true || true
  tf destroy -input=false -auto-approve -target=helm_release.kube_prometheus_stack || true
  kubectl -n monitoring delete pvc --all --ignore-not-found || true
  tf destroy -input=false -auto-approve
}

case "${1:-all}" in
  infra) infra ;; images) images ;; app) app ;; status) status ;; destroy) destroy ;;
  all) infra; images; app; status ;;
  *) echo "usage: $0 {infra|images|app|all|status|destroy}"; exit 1 ;;
esac
