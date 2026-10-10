# Getting started

For a disposable local installation, start with the [kind quickstart](../quickstart/README.md). It builds images,
installs the Helm chart with TLS and authentication, and verifies recommendations against real demo requests.
An optional synthetic history demonstrates training and forecasts without waiting a week.

For your own workload, use the source Helm chart below. Release images and a published chart are not available yet.
Use an explicit kubeconfig and context for every command, and start on a test cluster.

## 1. Prepare metrics and images

You need Kubernetes 1.33–1.35, Helm, a Deployment, a default storage class supporting ReadWriteOnce and filesystem
`flock`, and a Prometheus-compatible endpoint holding the workload's request counter. Istio is needed only for the
`istio` source preset; a custom `prometheus` query works without a service mesh. KEDA is optional.

```sh
docker build -t <registry>/predictive-autoscaler-operator:dev k8s-operator
docker build -t <registry>/predictive-autoscaler-ml-api:dev ml-engine
docker push <registry>/predictive-autoscaler-operator:dev
docker push <registry>/predictive-autoscaler-ml-api:dev
```

Build for your nodes' architecture. The trainer and forecasting service use the same image. The quickstart loads
local images into kind and requires no registry.

## 2. Install the chart

Create `my-values.yaml`, replacing the registry, metrics address and network selectors with your own:

```yaml
images:
  operator: {repository: <registry>/predictive-autoscaler-operator, tag: dev}
  forecaster: {repository: <registry>/predictive-autoscaler-ml-api, tag: dev}
prometheus:
  url: http://prometheus-server.monitoring.svc:9090
networkPolicy:
  prometheus:
    namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: monitoring}}
    podSelector: {matchLabels: {app: prometheus}}
    ports: [9090]
training:
  targets:
    - {namespace: shop, name: web-pa, schedule: "0 */6 * * *"}
```

The chart separates service accounts and permissions, uses non-root containers and NetworkPolicies, and protects the
forecasting API with TokenReview over TLS. The default self-signed certificate is for plain Helm; Argo CD needs
cert-manager or an existing TLS Secret. Tighten the Kubernetes API egress addresses for your cluster. See
[Helm configuration](helm.md), including storage and CRD upgrade requirements.

```sh
helm --kubeconfig <file> --kube-context <context> install pa charts/predictive-autoscaler \
  -n predictive-autoscaler --create-namespace -f my-values.yaml --wait --timeout 6m
```

## 3. Create an autoscaler in Recommend mode

For an application exporting `http_requests_total` with `namespace` and `service` labels:

```yaml
apiVersion: autoscaling.devkuban.com/v1alpha1
kind: PredictiveAutoscaler
metadata: {name: web-pa, namespace: shop}
spec:
  mode: Recommend
  targetDeployment: {name: web, namespace: shop}
  minReplicas: 2
  maxReplicas: 10
  metrics:
    requests:
      enabled: true
      targetRPS: 100
      source:
        preset: prometheus
        query: 'sum(rate(http_requests_total{namespace="{{ .Namespace }}",service="{{ .Name }}"}[2m]))'
  prediction: {enabled: true, leadTimeMinutes: 15, horizonMinutes: 60, updateIntervalSeconds: 60}
```

Adapt the metric and labels to your scrape. The query must return exactly one finite, non-negative series, in
**requests per second for this Deployment**. `{{ .Name }}` is the Deployment name: use the actual Service label if it
differs. Choose `targetRPS` from a load test of one pod at acceptable latency; 100 above is an example, not measured
capacity. [Configuration](configuration.md) explains templates, units and nested defaults.

```sh
kubectl --kubeconfig <file> --context <context> apply -f my-autoscaler.yaml
kubectl --kubeconfig <file> --context <context> -n shop get pa web-pa -o yaml
```

Wait for current-generation `Ready=True` and `TelemetryAvailable=True`, and `status.metricSource` with
`observedGeneration` matching `metadata.generation`. Recommend publishes `calculatedReplicas` without changing the
Deployment. Before training, `ForecastAvailable=False` / model unavailable is expected: recommendations are reactive.
Missing telemetry holds the replica count; it is not interpreted as zero traffic.

## 4. Train and verify a forecast

Training requires roughly a week of usable history and a complete recent inference window. Merely waiting for the
first CronJob does not create that history. With insufficient history training publishes nothing and reactive
recommendations continue. The chart schedules one CronJob per `training.targets` entry; its annotation
`autoscaling.devkuban.com/training-target` identifies the target.

```sh
kubectl --kubeconfig <file> --context <context> -n predictive-autoscaler get cronjobs \
  -o custom-columns='NAME:.metadata.name,TARGET:.metadata.annotations.autoscaling\.devkuban\.com/training-target'
kubectl --kubeconfig <file> --context <context> -n predictive-autoscaler create job first-training \
  --from=cronjob/<matching-cronjob>
kubectl --kubeconfig <file> --context <context> -n predictive-autoscaler wait \
  --for=condition=complete job/first-training --timeout=30m
kubectl --kubeconfig <file> --context <context> -n predictive-autoscaler logs job/first-training
```

A successful training log ends with `"status": "success"` and an `artifact_sha256`. Verify current-generation
`ForecastAvailable=True`, a recent `status.lastPrediction`, and `status.forecastReplicas`. A changed query, autoscaler
UID or target UID requires retraining; incompatible models are refused. See [upgrading](upgrading.md) for compatibility
and rollback boundaries. The [limitations](limitations.md) describe the experimental neural model and storage limits.

## 5. Enable scaling, then stop

Review recommendations and [coexistence](coexistence.md) first. Remove or correctly pause other replica writers,
including GitOps applying a fixed replica count. Active mode refuses a detected conflicting scaler.

```sh
kubectl --kubeconfig <file> --context <context> -n shop patch pa web-pa \
  --type merge -p '{"spec":{"mode":"Active"}}'
# Check ScalingActive, current/stabilized/applied replicas and the Deployment's ready replicas.
kubectl --kubeconfig <file> --context <context> -n shop get pa/web-pa deploy/web
# Stop writes and retain the current replica count:
kubectl --kubeconfig <file> --context <context> -n shop patch pa web-pa \
  --type merge -p '{"spec":{"mode":"Recommend"}}'
```

To remove the installation, stop scaling first, then `helm uninstall pa` with the same explicit kubeconfig, context
and namespace. Helm retains the CRD, PredictiveAutoscalers and model PVC by default. Delete your own autoscaler objects
separately when no longer needed. Delete retained model data only intentionally; never delete the CRD as routine
uninstall, because doing so deletes every PredictiveAutoscaler in the cluster.
