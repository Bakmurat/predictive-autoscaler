# Configuration reference

`PredictiveAutoscaler` (`autoscaler.example.com/v1alpha1`, short name `pa`, namespaced). The API group is a placeholder
and will be renamed to `autoscaling.devkuban.com` before v0.1.0 (a breaking change, decided 2026-10-09).

**Defaults depend on whether the parent object is present.** The API server fills in a field's CRD default only inside
an object that exists: with `prediction: {}` an omitted `leadTimeMinutes` becomes 15 and `updateIntervalSeconds` 300 (the
CRD defaults), but with `prediction` omitted entirely nothing is filled in and the operator uses its own constants, shown in
brackets (20 and 60). The same applies to `metrics.requests`. Set these fields explicitly.

## spec
| Field | Type | Required | Effective default | Read by the operator | Notes |
|---|---|---|---|---|---|
| `mode` | `Recommend` \| `Active` | no | **Recommend** (also when absent) | yes | `Recommend` computes and publishes the decision (status, metrics, ledger) and writes nothing; only `Active` scales the target. Existing objects without `mode` stop scaling after an upgrade until they set `Active`. |
| `targetDeployment.name` | string | yes | — | yes | The Deployment to scale. |
| `targetDeployment.namespace` | string | yes | — | yes | Any namespace is accepted in this version; keep it equal to the CR's namespace. |
| `targetDeployment.container` | string | no | — | **no** | Accepted, ignored. |
| `minReplicas` | int ≥ 1 | yes | — | yes | `minReplicas ≤ maxReplicas` is not validated yet. |
| `maxReplicas` | int ≥ 1 | yes | — | yes | |
| `metrics.requests.enabled` | bool | no | **false** (when `requests` is present) | yes | Must be `true` for forecasting: with `false` the operator asks the forecaster for a CPU forecast, which it rejects, so every decision falls back to the reactive rule. |
| `metrics.requests.targetRPS` | int ≥ 1 | no | — | yes | Requests per second one pod should handle. If unset the operator uses 20,000 req/min per pod for the reactive part and 30,000 for the forecast part (an inconsistency to be fixed). |
| `metrics.cpu.*`, `metrics.memory.*` | — | no | enabled, 70 / 60 % | **no** | Accepted, ignored: only the request rate is used. |
| `prediction.enabled` | bool | no | true | yes | `false` = reactive-only. |
| `prediction.horizonMinutes` | int ≥ 5 | no | 60 [60] | yes | How far ahead the forecaster predicts. |
| `prediction.leadTimeMinutes` | int ≥ 1 | no | **15** [20] | yes | The operator sizes for the highest forecast in the next `leadTimeMinutes` (see the decision rule). The README and examples use 20; set it explicitly. |
| `prediction.updateIntervalSeconds` | int ≥ 60 | no | **300** [60] | yes | Reconcile period. Examples set 60. |
| `resources.*` | — | no | 100 m / 128 MB / 10,000 rpm | **no** | Accepted, ignored. |

## status
| Field | Meaning |
|---|---|
| `mode` | The mode applied in the last reconcile. |
| `observedGeneration` | The spec generation the status describes. |
| `currentReplicas` / `readyReplicas` | The Deployment's replicas and ready replicas (a zero is reported). |
| `forecastReplicas` | Replicas from the forecast alone; absent when no forecast was used. |
| `calculatedReplicas` | The decision rule's result, or the held count when the request rate is unavailable. |
| `stabilizedReplicas` | Active only: what is applied after scale-down stabilization and cooldown. Absent in Recommend mode, which keeps no hypothetical scale history. |
| `appliedReplicas`, `lastScaleTime` | Active only: the last successful write. Cleared when the target Deployment is replaced (`targetUID` changes). |
| `targetUID` | UID of the target Deployment the status describes. |
| `predictedReplicas` | Deprecated: the calculated count (historical meaning). Use `calculatedReplicas` and `forecastReplicas`. |
| `lastPrediction` | Issue time of the last forecast used. |
| `conditions` | `Ready` (the reconcile completed), `TelemetryAvailable` (`Measured` / `MetricsUnavailable`), `ForecastAvailable` (`Used`, `Disabled`, `Unavailable`, `HorizonElapsed`, `SanityRejected`), `ScalingActive` (`Active`, `RecommendMode`, `TelemetryHold`, or the failure reason). |

Events are emitted when a condition changes (and on `ScaledUp` / `ScaledDown`), never on every reconcile; `kubectl
describe pa <name>` shows them. `kubectl get pa` shows Target, Mode, Min, Max, Current, Calculated, Applied, Ready, Age.

## Decision rule
```
reactive  = ceil(current request rate / per-pod capacity)            # 0 when the rate is 0
forecast  = ceil(peak forecast in the lead window / per-pod capacity) # when a usable forecast exists
            if confidence < 0.7: forecast = ceil(minReplicas + confidence × (forecast − minReplicas))
desired   = clamp(max(forecast, reactive), minReplicas, maxReplicas)
```
Safeguards on the forecast: it is discarded when its **first step** exceeds 10× the current rate (checked only when the
current rate is positive); an overestimate cap limits its influence after consecutive forecasts well above the reactive
need.
Scale-up is applied at once. Scale-down needs desired below current **continuously for 5 minutes** and no scale-up in the
last 5 minutes (stabilization), at least 2 minutes since the previous scale-down (cooldown), and then removes at most
`max(ceil(10 % × current), 2)` pods per step. When the overestimate cap is active, the stabilization checks are skipped but
the cooldown still applies. This state is kept in memory, so an operator restart resets it.

**When the current request rate cannot be measured, the replica count is held.** Only a complete, successful answer with
exactly one finite, non-negative sample counts as a measurement; a failed or partial query, an empty result (no series),
several series or a non-finite value is "unavailable" — never zero traffic. Without a measurement the operator holds the
current replica count (within `[minReplicas, maxReplicas]`), whether or not a forecast exists: it neither scales down on
missing data nor scales on a forecast alone. The decision ledger records `telemetry_status: unavailable` with the reason and
the safeguard `telemetry_unavailable_hold`, and the measurement gauges (current RPM, actual need, prediction error) are
removed instead of showing stale or invented values. A workload whose request counter exists but sees no traffic is
measured as a real zero and scales normally.

## Operator environment
| Variable | Default | Purpose |
|---|---|---|
| `ML_API_URL` | `http://ml-api-service.ml-engine.svc.cluster.local:8000` | Forecasting service. |
| `VICTORIAMETRICS_URL` | `http://vmselect-vmst.monitoring.svc.cluster.local:8481/select/0/prometheus` | Prometheus-compatible query endpoint (any Prometheus API works). |
| `FORECAST_LOG` | unset | Path of the append-only forecast/decision ledger (JSONL). |
| `WATCH_NAMESPACES` | all | Comma-separated namespaces to watch. |
The forecasting service reads the endpoint from a differently named variable, `VICTORIA_METRICS_URL`; set both.
