# Predictive Autoscaler

**Scale before the traffic arrives, not after.**

Predictive Autoscaler is an open-source Kubernetes autoscaler that forecasts request demand with a deep-learning model and provisions capacity ahead of the load. Conventional autoscalers react to metrics that have already crossed a threshold, which means the first users of every traffic surge pay for it in latency and errors. Predictive Autoscaler closes that gap: it learns each workload's daily rhythm, predicts the next hour, and has the pods ready when the wave hits, while a reactive safety net guarantees it can never do worse than today's autoscaling.

Built by [Bakmurat Kubanaliev](#author). Apache-2.0.

## Why it matters

- **Latency budgets are lost in the lag.** Between a metric crossing its threshold, the autoscaler noticing, new pods being scheduled, and the application becoming ready, minutes pass. For workloads with sharp daily peaks that lag is the whole incident.
- **Over-provisioning is the expensive workaround.** Teams pin replica minimums high enough to survive the morning peak and pay for that capacity all night. Forecasting lets capacity follow demand in both directions.
- **Forecasting must be safe to adopt.** A predictor that can scale a service *down* on a bad forecast will never be trusted. Predictive Autoscaler is additive by design: it can only bring capacity forward, never withhold it.

## What you get

| Capability | What it does |
|---|---|
| **Demand forecasting** | A three-layer bidirectional LSTM (128/64/32 units) forecasts the next 60 minutes of request rate in six 10-minute steps from the last 24 hours of history. |
| **Asymmetric training objective** | The loss penalizes under-prediction twice as heavily as over-prediction, because scaling too late costs more than scaling too early. |
| **Hybrid forecast** | Model output is blended with the workload's own seven-day time-of-day pattern (blend weight 0.70 to 0.95 across the horizon), so forecasts keep the true shape of the daily peak instead of regressing to the mean. |
| **Continuous learning** | A scheduled job retrains every six hours on the previous seven days and publishes the model to the forecasting service without downtime. |
| **Declarative operator** | A Go operator (controller-runtime) driven by a `PredictiveAutoscaler` custom resource: target Deployment, replica bounds, per-pod targets, horizon, lead time, and reconcile interval. |
| **Additive safety model** | Replicas are set to the **highest** of the forecast baseline, the live reactive requirement, and the configured minimum. The forecast can only add capacity. |
| **Guard rails** | Overestimate detection, scale-down stabilization (five-minute post-scale-up hold, bounded step size, cooldown), and confidence dampening keep a wrong forecast from causing churn or cost. |
| **Reactive backstop** | A dormant KEDA `ScaledObject` stays configured on the same Deployment, so conventional autoscaling takes over instantly if the forecasting service is unavailable. |
| **Observability** | Prometheus metrics from both components and three Grafana dashboards: forecast versus actual, replica decisions, and guard-rail state. |

## How it works

```
VictoriaMetrics ──► Collector ──► Training job (every 6 h) ──► Model store
   (Istio RPS,          │                                           │
    CPU, memory)        └──────────► Forecasting service (FastAPI) ◄┘
                                              │  next 60 min, 6 steps
                                              ▼
                        Go operator ── max(forecast, reactive, min) ──► Deployment replicas
                                              │
                                    KEDA ScaledObject (dormant backstop)
```

1. **Collect.** Per-pod request rate from the Istio service mesh, plus CPU and memory, are pulled from VictoriaMetrics at ten-minute resolution.
2. **Train.** Every six hours a CronJob retrains the model on the previous seven days and writes it to a shared volume; the service reloads it by modification time. Per-model locks and the job-based design eliminated the concurrent-retraining failures of early versions.
3. **Forecast.** The service returns the expected demand for the next hour in six steps, blended with the seven-day pattern, with a confidence signal.
4. **Scale.** On every reconcile (60 seconds by default) the operator converts the forecast at the configured lead time (20 minutes by default) into a replica count and applies the additive rule.
5. **Guard.** Overestimate detection compares recent forecasts with actuals and shrinks the forecast's influence when it runs hot; stabilization and cooldown bound every scale-down; confidence dampening discounts low-confidence forecasts.

## Evaluation approach

Predictive Autoscaler was developed and evaluated in a dedicated Kubernetes environment with a purpose-built harness: a k6 traffic generator replaying a full 24-hour daily traffic profile against a stock nginx Deployment with a 1 to 40 replica range, run side by side with an identical Deployment scaled by KEDA alone as the control. The harness ships in `examples/` so anyone can reproduce the setup. Across a dozen operator releases (v3.1.1 to v4.3.0) this loop drove every design decision listed under [Lessons learned](#lessons-learned).

Accuracy figures are deliberately not published yet. A controlled, repeatable benchmark with held-out days and a documented metric is the first item on the roadmap, and numbers will be published with it.

## Getting started

Both components ship as container images:

```sh
cd ml-engine    && docker build -t <your-registry>/predictive-autoscaler-ml-api:dev .
cd k8s-operator && docker build -t <your-registry>/predictive-autoscaler-operator:dev .
```

Set the image names in `k8s-manifests/base/kustomization.yaml` and the Deployment manifests, choose a storage class in `03-pvc.yaml`, point `02-configmap-victoriametrics.yaml` at your VictoriaMetrics query endpoint, then:

```sh
kubectl apply -k k8s-manifests/base
```

Prerequisites: VictoriaMetrics with Istio request metrics, and KEDA for the reactive backstop. The API group `autoscaler.example.com` is a placeholder; rename it (CRD, RBAC, Go types, samples) to a domain you control.

### Declare an autoscaler

```yaml
apiVersion: autoscaler.example.com/v1alpha1
kind: PredictiveAutoscaler
metadata:
  name: web-autoscaler
  namespace: demo
spec:
  targetDeployment:
    name: web
    namespace: demo
    container: web
  minReplicas: 2
  maxReplicas: 40
  metrics:
    requests:
      enabled: true
      targetRPS: 300          # requests per second per pod
  prediction:
    horizonMinutes: 60
    leadTimeMinutes: 20
    updateIntervalSeconds: 60
```

`spec.metrics.cpu` and `spec.metrics.memory` are reserved in the schema; the request-rate model is the one trained today.

### Tests

```sh
cd k8s-operator && go test -race ./...
cd ml-engine    && pip install -r requirements.txt && python -m pytest tests
```

Run the Python suite on a machine with TensorFlow installed; a few validation tests in `tests/test_validation.py` are tracked as open items on the roadmap.

## Repository layout

| Path | Contents |
|---|---|
| `ml-engine/` | Forecasting service: FastAPI application (`api/`), model code (`models/lstm_model.py`), VictoriaMetrics collector (`data/`), training scripts (`training/`), unit tests (`tests/`), `Dockerfile`. |
| `k8s-operator/` | Go operator: CRD types (`api/v1alpha1/`), reconciler and overestimate logic (`controllers/`), sample custom resources (`config/samples/`), `Dockerfile`. |
| `k8s-manifests/base/` | Kustomize base: namespace, CRD, RBAC, both Deployments, the training CronJob, the model volume, and VictoriaMetrics scrape objects. |
| `grafana/dashboards/` | Three dashboards (overview, forecast components, guard mechanisms). |
| `examples/nginx-test/` | The reference workload scaled by Predictive Autoscaler, with its k6 traffic generator and dormant KEDA backstop. |
| `examples/myapptwo/` | The identical KEDA-only control workload. |

## Lessons learned

- **Forecasts regress to the mean.** A plain LSTM flattened toward the average, useless for anticipating a daily peak. Blending with the workload's own seven-day pattern, weighted more heavily at longer horizons, restored the shape of the forecast.
- **Under-prediction hurts more than over-prediction.** The asymmetric loss was the single most valuable change to the training objective.
- **Additive by construction.** Taking the maximum of forecast, reactive, and minimum is what makes a predictive layer safe to run continuously next to a reactive autoscaler.
- **Retraining is an operational feature, not a script.** Moving training into a CronJob with per-model locks, and reloading by file modification time, removed a whole class of resource-exhaustion incidents.
- **Scale-down must be slow and bounded.** Stabilization windows, a cooldown, and a bounded step size eliminated oscillation on the way down.
- **Version-pin the training image to the serving image.** Manifest-consistency tests (`ml-engine/tests/test_kustomize_sync.py`) now catch mismatches that once let a stale model overwrite a good one.

## Roadmap

1. Controlled accuracy benchmark with held-out days and a documented metric; published results.
2. Validation of the newest forecasting model (five input features, direct six-step output, robust scaling) against the benchmark.
3. Adaptive blend weights in place of the fixed 0.70 to 0.95 schedule.
4. A training-time validation gate so a new model can never replace a better one.
5. Multi-tenant model management, CPU and memory forecasting paths, CRD validation and defaults via webhooks, Helm chart, end-to-end operator tests on kind.

## Status

Predictive Autoscaler is an actively developed personal research project (October 2025 to present). It is built to run alongside KEDA, which stays in place as the reactive backstop, and it is published so the design, code, and lessons are available to everyone working on the same problem. Evaluate it in your own environment with the shipped harness before relying on it.

Note on numbers: request-rate figures in the simulator configuration (for example a 60,000 requests-per-minute peak) are the synthetic load generator's settings, not measured throughput.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`. The operator scaffolding was generated with Kubebuilder, whose boilerplate header is retained in `k8s-operator/hack/boilerplate.go.txt` and the generated deep-copy file.

## Author

Bakmurat Kubanaliev. This is a personal project; it is not affiliated with, endorsed by, or derived from the work of any employer.
