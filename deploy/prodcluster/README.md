# Benchmark on a self-hosted cluster (`prodcluster` overlay)

The same six-arm benchmark as [`deploy/eks-benchmark`](../eks-benchmark/README.md), deployed to a self-hosted
Kubernetes cluster instead of AWS EKS. The EKS cluster was torn down on 2026-10-02; this overlay starts a **new
campaign**, so nothing measured on EKS is pooled with it. No result is claimed here.

## What differs from the EKS deployment

| Area | EKS (`deploy/eks-benchmark`) | Self-hosted (`deploy/prodcluster`) |
|---|---|---|
| Cluster | Terraform-built EKS, arm64 nodes | existing RKE2 cluster, amd64 nodes, Istio and KEDA already installed |
| Metrics | kube-prometheus-stack (Prometheus, 15 d) | the cluster's VictoriaMetrics (query via vmselect, remote write via vminsert); retention ≥ 15 d is required because `ENSEMBLE_HISTORY_HOURS=360` |
| Scrapes | PodMonitor/ServiceMonitors in `eks-benchmark/monitoring/` | the base `VMServiceScrape`s (ml-api, operator); Envoy request metrics come from the cluster's existing Envoy pod scrape |
| Storage | `gp3` (EBS CSI) | `harvester` |
| Images | ECR, arm64, tag `bench-<sha>` | any registry (`HARBOR_REGISTRY` placeholder), linux/amd64, pinned by digest; pull Secret `harbor-creds` |
| Placement | EBS zones + Prometheus anti-affinity | node labels: `predictive-bench/ml-node` (ml-api, its RWO model volume, the trainer and the evidence archive), `predictive-bench/{seasonal,ensemble}-generator` (three generators) |
| ml-api experiments | live `kubectl set env` patches | declared in `ml-engine/ml-api-experiments.yaml` (S1, E1 q90, E2 q95; R1 not enabled) |
| Generators | five of six switched to challenge-v1 by hand | all six mount `k6-load-script-challenge-v1` |
| `nginx-ensemble` | pinned to one EKS zone | spread by hostname like the other arms |
| Generator connections | keep-alive per VU | one connection per request (`K6_NO_VU_CONNECTION_REUSE`): with keep-alive and no generator sidecar, pods added by a scale-up received no requests |
| Pod termination | default | `final-snapshot.sh` preStop hook: drains Envoy's inbound listeners and pushes the pod's final request counters (`bench_final_istio_*` plus a receipt) to VictoriaMetrics, so requests served after the last scrape are not lost |

The manifests themselves (arms, generators, autoscalers, CronJobs, archive) are reused unchanged from
`k8s-manifests/base` and `deploy/eks-benchmark`.

## Deploy

Requires `kustomize` v5.8+ (the kubectl-embedded kustomize cannot parse some EKS patches) and a kubectl matching
the cluster.

```bash
cd deploy/prodcluster
export KUBE_CONTEXT=<context>
export HARBOR_REGISTRY=<registry>/<project>             # holds predictive-autoscaler-{ml-api,operator}
export GIT_COMMIT_VALUE=<full commit the images were built from>
export ML_API_DIGEST=sha256:<…> OPERATOR_DIGEST=sha256:<…>
export VM_QUERY_URL=http://<vmselect>.<ns>.svc:8481/select/0/prometheus
export VM_WRITE_URL=http://<vminsert>.<ns>.svc:8480/insert/0/prometheus/api/v1/write
export VM_IMPORT_URL=http://<vminsert>.<ns>.svc:8480/insert/0/prometheus/api/v1/import/prometheus
export ML_NODE=<worker with the most free root disk> SEASONAL_GEN_NODE=<worker> ENSEMBLE_GEN_NODE=<worker>

./deploy.sh render        # rendered/{ml-engine,demo}.yaml; fails if any placeholder is left
./deploy.sh label-nodes
./deploy.sh ml-engine     # CRD first, then RBAC, ConfigMaps, PVCs, ml-api, operator, CronJobs, scrapes
./deploy.sh mask          # validity mask ConfigMap from validity-mask.json
./deploy.sh demo          # namespace (Istio injection), arms, Services, autoscalers, KEDA objects, generators
./deploy.sh status
```

Images are built for linux/amd64 from a clean `git archive` of one commit (`ml-engine/Dockerfile` for the API and
the trainer, `k8s-operator/Dockerfile` for the operator) and pushed with a tag; the build log records the digests.

## Validity mask

`validity-mask.json` is versioned separately from the EKS mask. Version 1 leaves `benchmark_history_start` null
until all six generators run the challenge-v1 script without restarts; the next version sets it to the first
ten-minute grid instant after that. The rules of the EKS mask apply (`ml-engine/tests/test_validity_mask_prodcluster.py`).
