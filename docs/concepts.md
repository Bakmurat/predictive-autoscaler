# Concepts

## Components
| Component | What it does | Where |
|---|---|---|
| **Operator** (Go, controller-runtime) | Watches `PredictiveAutoscaler` objects; every reconcile reads the current request rate, asks the forecasting service for a forecast, combines both and sets the target Deployment's replica count. Appends one JSON line per decision (and per forecast attempt) to an optional ledger (`FORECAST_LOG`). | `k8s-operator/` |
| **Forecasting service** (`ml-api`, Python/FastAPI) | Serves forecasts for the next hour in six 10-minute steps from the trained model blended with the seven-day pattern; refuses when its input window is not complete enough. | `ml-engine/api/` |
| **Trainer** (CronJob) | Every six hours trains a model for one workload from the last seven days of request rate and writes it, with a provenance sidecar, to the model volume; the service reloads it. | `ml-engine/training/`, `k8s-manifests/base/10-training-cronjob.yaml` |
| **Metrics store** (yours) | Prometheus-compatible endpoint holding Istio request metrics; read by all three. | — |

## One decision
1. **Reactive:** current request rate (req/min) → `ceil(rate / per-pod capacity)`.
2. **Forecast:** the highest forecast inside the lead window (`leadTimeMinutes`) → replicas; scaled toward `minReplicas`
   when the forecast's confidence is below 0.7; discarded when its first step exceeds 10× the current rate; limited by an
   overestimate cap after consecutive forecasts well above the reactive need.
3. **Combine:** `max(forecast, reactive)`, clamped to `[minReplicas, maxReplicas]`.
4. **Apply:** scale up at once; scale down gradually (5 minutes continuously lower, 2-minute cooldown, at most
   `max(ceil(10 %), 2)` pods per step).
5. **Fallbacks:** no usable forecast → reactive only; forecast and metrics both failing with errors → keep current replicas
   (see the known issue with empty metric results in `configuration.md`).

## Signals and units
- The request rate is in **requests per minute** internally; `targetRPS` in the CRD is per second (×60).
- Forecast steps are anchored at the end of the input window on a 10-minute grid; targets are 10–60 minutes ahead.

## Evidence trail
The ledger (`FORECAST_LOG`) records every forecast attempt (issued, refused, unavailable) and every decision with its
inputs, which makes each scaling action auditable afterwards. The research evaluation in this repository uses it.
