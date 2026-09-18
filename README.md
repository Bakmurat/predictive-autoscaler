# Predictive Autoscaler (prototype)

A personal, experimental Kubernetes autoscaler that forecasts request demand with a recurrent neural network and scales a Deployment ahead of the load instead of after it. It pairs a Python forecasting service (TensorFlow/Keras, FastAPI) with a Go operator (controller-runtime) driven by a `PredictiveAutoscaler` custom resource.

## Status: incomplete prototype

Read this section before anything else.

- This is a **prototype**, developed on personal time in a personal test environment between **October 2025 and March 2026**.
- It was run only against **synthetic traffic** on a small test cluster. It has **never been deployed to production** and has never scaled a real user-facing workload.
- **No accuracy figures are claimed.** Forecast quality was observed to vary widely from day to day during testing, and no controlled, repeatable evaluation exists yet.
- The project is **unfinished** (see [Remaining work](#remaining-work)). Parts of the code base reflect different stages of the design; the unreleased changes on the forecasting model have not been validated.
- Further evaluation and engineering are required before anyone should consider using it for anything that matters.

If you are looking for a production autoscaler, use the Kubernetes Horizontal Pod Autoscaler or KEDA. This repository is published so that the design, the code, and the lessons learned are available to people working on the same problem.

## What it does

1. **Collect.** A collector pulls per-pod request rate (from the Istio service mesh), CPU, and memory time series from VictoriaMetrics at ten-minute resolution.
2. **Train.** A scheduled job retrains a forecasting model every six hours on the previous seven days of history and writes it to a shared volume. The model is a three-layer bidirectional LSTM (128, 64, and 32 units) trained with an asymmetric loss that penalizes under-prediction twice as heavily as over-prediction, because scaling too late is worse than scaling too early.
3. **Forecast.** The forecasting service takes the previous 24 hours of data and returns the expected demand for the next 60 minutes in six ten-minute steps. The network's output is blended with the workload's own seven-day time-of-day pattern; the further ahead the step, the more weight the historical pattern receives (blend weight rises from 0.70 to 0.95 across the six steps). This blending was added because raw forecasts tended to drift back toward the average.
4. **Scale.** The Go operator reconciles each `PredictiveAutoscaler` resource on a fixed interval (60 seconds by default, never below 30). It converts the forecast for the configured lead time (20 minutes by default) into a replica count and sets the target Deployment to the **highest of three values**: the forecast baseline, the current reactive requirement from live metrics, and the configured minimum. It never scales below what the live metrics require.
5. **Guard.** Several mechanisms keep the forecast from doing harm:
   - **Overestimate detection** compares recent forecasts with what actually happened and reduces the forecast's influence when it has been consistently too high.
   - **Scale-down stabilization**: no scale-down for five minutes after a scale-up, at most one scale-down every two minutes, and each step removes at most 10% of the pods (or two pods, whichever is greater).
   - **Confidence dampening** shrinks the forecast's contribution when the model reports low confidence.
   - **Dormant reactive backstop**: a conventional autoscaler (a KEDA `ScaledObject`) stays configured for the same Deployment so that reactive scaling takes over if the forecasting service is unavailable or wrong.
6. **Observe.** The operator and the forecasting service expose Prometheus metrics; three Grafana dashboards under `grafana/dashboards/` show forecasts against actuals, replica decisions, and the state of the guard mechanisms.

## Repository layout

| Path | Contents |
|---|---|
| `ml-engine/` | Forecasting service: FastAPI application (`api/`), model code (`models/lstm_model.py`), VictoriaMetrics collector (`data/`), training scripts (`training/`), unit tests (`tests/`), `Dockerfile`. |
| `k8s-operator/` | Go operator: CRD types (`api/v1alpha1/`), reconciler and overestimate logic (`controllers/`), sample custom resources (`config/samples/`), `Dockerfile`. |
| `k8s-manifests/base/` | Kustomize base: namespace, CRD, RBAC, the two Deployments, the training CronJob, the model volume, and VictoriaMetrics scrape objects. |
| `grafana/dashboards/` | Three dashboards (overview, forecast components, guard mechanisms). |
| `examples/nginx-test/` | A synthetic workload scaled by the predictive autoscaler, with its traffic generator (k6) and its dormant KEDA backstop. |
| `examples/myapptwo/` | An identical workload scaled by KEDA only, used as a side-by-side baseline during testing. |

## Custom resource

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

`spec.metrics.cpu` and `spec.metrics.memory` exist in the schema, but after March 2026 only the request-rate model is trained; CPU and memory forecasting were removed from the training path and remain as unfinished options.

The API group `autoscaler.example.com` is a placeholder. Rename it (CRD, RBAC, Go types, and samples) to a domain you control before installing.

## Building and running

The forecasting service and the operator are built as container images:

```sh
# forecasting service
cd ml-engine && docker build -t <your-registry>/predictive-autoscaler-ml-api:dev .

# operator
cd k8s-operator && docker build -t <your-registry>/predictive-autoscaler-operator:dev .
```

Then set the image names in `k8s-manifests/base/kustomization.yaml` and the Deployment manifests, adjust the storage class in `03-pvc.yaml`, point `02-configmap-victoriametrics.yaml` at your VictoriaMetrics query endpoint, and apply the base with `kubectl apply -k k8s-manifests/base`. The forecasting service needs VictoriaMetrics with Istio request metrics; the examples assume an Istio ingress gateway and KEDA.

Tests:

```sh
cd k8s-operator && go test -race ./...
cd ml-engine && pip install -r requirements.txt && python -m pytest tests
```

Some Python tests in `tests/test_validation.py` were known to fail at the time of the last development session and were never fixed (see below). The Python test suite was not run before this export was prepared (no TensorFlow environment was available on the export machine); run it on a machine with the requirements installed before relying on it.

## Test harness

Everything was evaluated with a synthetic harness: a k6 traffic generator replaying a twenty-four-hour daily pattern against a stock nginx Deployment, a scaling range of 1 to 40 pods, and a second identical Deployment scaled by KEDA alone as the baseline. The harness is in `examples/`. No result figures from it are published here, because the only measurements taken were single-day readings from an uncontrolled environment.

## Design notes and lessons learned

- **Forecasts regress to the mean.** The plain LSTM forecast flattened toward the average, which is useless for anticipating a daily peak. Blending with the workload's own seven-day pattern, weighted more heavily at longer horizons, fixed the shape of the forecast at the cost of making the model mostly a corrector of the historical pattern rather than an independent predictor.
- **Under-prediction hurts more than over-prediction.** The asymmetric loss was the single most useful change to the training objective.
- **Never scale below the reactive requirement.** Taking the maximum of forecast, reactive, and minimum makes the forecast purely additive: it can only bring capacity forward, never withhold it. This is what made it safe to run continuously next to a reactive autoscaler.
- **Concurrent retraining is a real failure mode.** Early versions triggered retraining from multiple requests at once and exhausted the node. Per-model training locks and moving training into a CronJob, with the service only reloading models by file modification time, removed the problem.
- **Scale-down needs to be slow and bounded.** Oscillation on the way down was the most visible misbehavior; stabilization windows, a cooldown, and a bounded step size removed it.
- **Operational fragility.** A version mismatch between the training job's image and the service's image silently overwrote a good model with a stale one. Manifest-consistency tests (`ml-engine/tests/test_kustomize_sync.py`) were added after that incident.

## Known limitations

- Single tenant, single target metric in practice (requests per second per pod); CPU and memory paths are stale.
- Forecast quality was never evaluated in a controlled way and varied widely during testing.
- The training path assumes Istio request metrics in VictoriaMetrics with a specific label layout.
- Model files live on a ReadWriteOnce volume; the design does not support running more than one forecasting replica.
- No Helm chart, no webhooks, no CRD validation beyond types, no upgrade path between CRD versions.
- The forecasting model at the head of this repository (five input features, direct six-step output, robust scaling) is the last development state and was **not** the model that ran during the test period; it has not been validated.

## Remaining work

- A controlled, repeatable accuracy evaluation with held-out days and a documented metric, before any accuracy statement is made.
- Validate or revert the unreleased forecasting model changes.
- Fix the failing validation tests and add end-to-end tests for the operator against a kind cluster.
- Adaptive blend weights instead of the fixed 0.70–0.95 schedule.
- A validation gate so the training job cannot overwrite a good model with a worse one.
- Multi-tenant model management, CRD validation and defaults via webhooks, and a Helm chart.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`. The operator scaffolding was generated with Kubebuilder, whose boilerplate header is retained in `k8s-operator/hack/boilerplate.go.txt` and the generated deep-copy file.

## Author

Bakmurat Kubanaliev. This is a personal project; it is not affiliated with, endorsed by, or derived from the work of any employer.

Note on numbers: any request-rate figure in the simulator configuration (for example a 60,000 requests-per-minute peak) is the synthetic load generator's setting, not a measured or claimed throughput of the prototype.
