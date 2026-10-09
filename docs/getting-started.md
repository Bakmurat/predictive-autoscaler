# Getting started (current version: build and install from source)

There are no published images, Helm chart or releases yet; a chart and a kind quickstart are planned. This guide installs
the current version from source. Read `limitations.md` first: in particular, do not run it next to another autoscaler on
the same Deployment (`coexistence.md`).

## 1. Prerequisites
- A Kubernetes cluster and `kubectl` pointed at it explicitly (`kubectl config current-context`); try it on a disposable
  cluster first.
- The Deployment you want to scale already exists, in its own namespace, and receives traffic.
- **Istio** sidecars on the workload you want to scale (the request-rate signal is
  `istio_requests_total{reporter="destination"}`).
- A **Prometheus-compatible query endpoint** that stores those Istio metrics (Prometheus or VictoriaMetrics), with at
  least 7 days of retention once you want trained forecasts.
- A storage class for two small ReadWriteOnce volumes (models, forecast ledger).
- Optional: the VictoriaMetrics operator. The base includes `11-vmservicescrapes.yaml` (VMServiceScrape objects), which
  fails to apply without its CRDs; remove it from `k8s-manifests/base/kustomization.yaml` if you scrape another way.

## 2. Build and push the images
```sh
docker build -t <registry>/predictive-autoscaler-ml-api:dev ml-engine
docker build -t <registry>/predictive-autoscaler-operator:dev k8s-operator
docker push <registry>/predictive-autoscaler-ml-api:dev
docker push <registry>/predictive-autoscaler-operator:dev
```

## 3. Configure the base (`k8s-manifests/base`)
| File | Change |
|---|---|
| `kustomization.yaml` | `images:` → your two images (example below). The ml-api entry covers the API and the trainer. |
| `02-configmap-victoriametrics.yaml` | `victoria_metrics_url` → your query endpoint (read by the forecasting API and the trainer). |
| `09-operator-deployment.yaml` | `VICTORIAMETRICS_URL` → the same endpoint (the operator does not read the ConfigMap). |
| `10-training-cronjob.yaml` | `TRAINING_WORKLOAD` / `TRAINING_NAMESPACE` → the Deployment to forecast (one workload per CronJob). |
| `03-pvc.yaml`, `12-forecast-log-pvc.yaml` | `storageClassName` for your cluster. |

Image overrides keep the original `name` as the key and set `newName`/`newTag`:
```yaml
images:
  - name: registry.example.com/predictive-autoscaler/predictive-autoscaler-ml-api
    newName: <registry>/predictive-autoscaler-ml-api
    newTag: dev
  - name: registry.example.com/predictive-autoscaler/predictive-autoscaler-operator
    newName: <registry>/predictive-autoscaler-operator
    newTag: dev
```
Apply and check that everything came up:
```sh
kubectl apply -k k8s-manifests/base
kubectl wait --for=condition=Established crd/predictiveautoscalers.autoscaler.example.com --timeout=60s
kubectl -n ml-engine rollout status deploy/ml-api --timeout=300s
kubectl -n ml-engine rollout status deploy/predictive-operator --timeout=120s
```

## 4. Train a first model
Training needs history in the metrics store (the scheduled CronJob uses the last 7 days). To train now:
```sh
kubectl -n ml-engine create job first-training --from=cronjob/ml-training
kubectl -n ml-engine logs -f job/first-training
```
A successful run ends with a JSON line containing `"status": "success"` and the published `artifact_sha256`, and the API
logs `Reloaded model …` shortly after. With too little history it stops with `insufficient history: have N of M
ten-minute points` and publishes nothing; that is expected on a new cluster. Until a model exists, the operator scales
reactively from the current request rate only.

## 5. Create a PredictiveAutoscaler
Start from `k8s-operator/config/samples/autoscaler_v1alpha1_predictiveautoscaler.yaml`. Set
`metrics.requests.enabled: true` and `targetRPS` (requests per second one pod should handle), and set
`leadTimeMinutes` and `updateIntervalSeconds` explicitly (the CRD defaults are 15 and 300; see `configuration.md`).
```sh
kubectl apply -f my-autoscaler.yaml
kubectl get pa -A
kubectl -n ml-engine logs deploy/predictive-operator | grep -i "decision\|prediction"
```

## 6. Stop or remove
**Stop scaling one workload** (its replica count stays where it is):
```sh
kubectl -n <namespace> delete pa <name>
```
**Remove this installation** but keep the CRD (other installations or your CRs elsewhere may use it): delete the CRs you
created, then the components:
```sh
kubectl -n ml-engine delete deploy/predictive-operator deploy/ml-api cronjob/ml-training svc/ml-api-service
kubectl delete clusterrolebinding predictive-operator-binding
kubectl delete clusterrole predictive-operator-role
kubectl -n ml-engine delete pvc ml-models-pvc forecast-log-pvc   # trained models and the ledger; see your reclaim policy
```
**Complete teardown — disposable clusters only.** `kubectl delete -k k8s-manifests/base` also deletes the CRD, which
deletes **every** PredictiveAutoscaler in the cluster, and the whole `ml-engine` namespace with its PVCs. Whether the
volumes' data survives depends on the StorageClass reclaim policy (`Delete` loses it).
