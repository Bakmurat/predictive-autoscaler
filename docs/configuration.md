# Configuration reference

`PredictiveAutoscaler` (`autoscaling.devkuban.com/v1alpha1`, short name `pa`, namespaced). The group replaced the
placeholder `autoscaler.example.com` before v0.1.0 (a breaking change, decided 2026-10-09); objects of the old group are
moved with the procedure in [upgrading.md](upgrading.md). The CRD is generated from the API types (`make manifests`).

**Defaults depend on whether the parent object is present.** The API server fills in a field's CRD default only inside
an object that exists: with `prediction: {}` an omitted `leadTimeMinutes` becomes 15 and `updateIntervalSeconds` 300 (the
CRD defaults), but with `prediction` omitted entirely nothing is filled in and the operator uses its own constants, shown in
brackets (20 and 60). The same applies to `metrics.requests`. Set these fields explicitly.

## spec
| Field | Type | Required | Effective default | Read by the operator | Notes |
|---|---|---|---|---|---|
| `mode` | `Recommend` \| `Active` | no | **Recommend** (also when absent) | yes | `Recommend` computes and publishes the decision (status, metrics, ledger) and writes nothing; only `Active` scales the target. Existing objects without `mode` stop scaling after an upgrade until they set `Active`. |
| `targetDeployment.name` | string (non-empty) | yes | — | yes | The Deployment to scale. |
| `targetDeployment.namespace` | string | yes | — | yes | Must equal the CR's namespace; any other value is rejected (`Ready=False`, reason `CrossNamespaceTarget`) and nothing is written. |
| `targetDeployment.container` | string | no | — | **no** | Accepted, ignored. |
| `minReplicas` | int ≥ 1 | yes | — | yes | The API server rejects `minReplicas > maxReplicas`. |
| `maxReplicas` | int ≥ 1 | yes | — | yes | |
| `metrics.requests.enabled` | bool | no | **false** (when `requests` is present) | yes | Must be `true` for forecasting: with `false` the operator asks the forecaster for a CPU forecast, which it rejects, so every decision falls back to the reactive rule. |
| `metrics.requests.capacityPerReplica` | quantity, > 0 and ≤ 1,000,000 | no, but nothing is scaled without it | **none** | yes | Requests per second **one ready replica** serves within its SLO (for example its p99 latency target) under the real request mix. Decimals are allowed (`"12.5"`, or `12500m`). The reactive rule, the forecast rule and the scale-down headroom floor all divide by this one value. There is no default: without a capacity the operator calculates no replica count and holds the current one (see [the decision rule](#decision-rule)). Measure it with a load test ([below](#measuring-capacitypereplica)). |
| `metrics.requests.targetRPS` | int ≥ 1 | no | — | yes | **Deprecated:** the integer form of `capacityPerReplica`, with the same meaning. Still honoured (`CapacityConfigured` reason `DeprecatedTargetRPS`); move to `capacityPerReplica`. The API server rejects an object that sets both. |
| `metrics.requests.source.preset` | `istio` \| `prometheus` | no | `istio` | yes | Where the request rate comes from. `istio` uses `sum(rate(istio_requests_total{reporter="destination",destination_workload="<name>",destination_workload_namespace="<namespace>"}[1m]))`; `prometheus` uses `source.query`. |
| `metrics.requests.source.query` | string ≤ 2 KiB | with `prometheus` only | — | yes | An instant PromQL expression returning **exactly one series, in requests per second**, for the target. It may use only `{{ .Namespace }}` and `{{ .Name }}` (the target Deployment), escaped as PromQL string content. No other template features are allowed: no functions, conditions or variables. An invalid query sets `Ready=False` (reason `InvalidMetricQuery`), and nothing is written. Example for ingress-nginx: `sum(rate(nginx_ingress_controller_requests{namespace="{{ .Namespace }}",service="{{ .Name }}"}[1m]))`. `{{ .Name }}` is the Deployment's name: if the Service is named differently, write the Service's name into the query. Check which namespace label your scrape keeps (`namespace` or `exported_namespace`), and that the series counts this Deployment's traffic only. |
| `metrics.cpu.*`, `metrics.memory.*` | — | no | enabled, 70 / 60 % | **no** | Accepted, ignored: only the request rate is used. |
| `prediction.enabled` | bool | no | true | yes | `false` = reactive-only. |
| `prediction.horizonMinutes` | int ≥ 5 | no | 60 [60] | yes | How far ahead the forecaster predicts. |
| `prediction.leadTimeMinutes` | int ≥ 1 | no | **15** [20] | yes | The operator sizes for the highest forecast in the next `leadTimeMinutes` (see the decision rule). Set it explicitly; the quickstart uses 15. |
| `prediction.updateIntervalSeconds` | int ≥ 60 | no | **300** [60] | yes | Reconcile period. Examples set 60. |
| `prediction.engine` | `statistical` \| `legacyLstm` | no | — (the v1 path) | yes | `statistical` selects forecast contract v2 ([below](#forecast-contract-v2-engine-statistical-preview)); absent or `legacyLstm` keeps the v1 path unchanged. |
| `prediction.sampleIntervalSeconds`, `seasonality`, `historyDays`, `settlementSeconds`, `outcomeGraceSeconds`, `risk.miscoverageBps`, `calibration.*`, `trust.*` | see the CRD | no | the forecasting service's defaults (300 s grid, daily, 14 days, 120 s, 600 s, 1000 bps, ...) | yes (engine `statistical`) | The v2 forecast settings. The forecasting service derives them from the live object and the operator hashes them the same way (the settings fingerprint both sides check). Cross-field limits (for example a horizon that is a multiple of the grid, at most 60 minutes and 60 steps) make the settings invalid: `PredictionReady=False`, reason `SettingsInvalid`, and decisions stay reactive. |
| `prediction.predictiveExtra.maxReplicas`, `maxPercent` | int 0–1000 | no | 2, 50 | yes (engine `statistical`) | The most replicas a verified forecast may add above the reactive need: `min(maxReplicas, ceil(maxPercent × max(reactive, minReplicas) / 100))`. `0` in either disables predictive excess. |
| `behavior.startup.fallbackDelaySeconds` | int 1–3600 | no | **none** | yes (engine `statistical`) | Scale-out-to-Ready budget used until enough starts are measured (20 completed starts of the current pod template in 7 days). Without it, and without measurements, no predictive lead is chosen and decisions stay reactive (`LeadInsufficient`). |
| `behavior.startup.minimumDelaySeconds` | int 0–3600 | no | 0 | yes | A floor for the budget, kept even when measurements are lower. |
| `behavior.startup.actuationAllowanceSeconds` | int 0–300 | no | 15 | yes | Time for the forecast request and the `/scale` write. |
| `behavior.scaleDown.headroomPercent` | int 0–99 | no | 10 | yes | Spare request capacity a scale-down must leave in the resulting fleet at the measured rate (see the decision rule). `0` disables it. |
| `resources.*` | — | no | 100 m / 128 MB / 10,000 rpm | **no** | Accepted, ignored. |

## status
| Field | Meaning |
|---|---|
| `mode` | The mode applied in the last reconcile. |
| `observedGeneration` | The spec generation the status describes. |
| `currentReplicas` / `readyReplicas` | The Deployment's replicas and ready replicas (a zero is reported). |
| `forecastReplicas` | Replicas from the forecast alone; absent when no forecast was used. |
| `calculatedReplicas` | The decision rule's result, or the held count when the request rate is unavailable. **Absent while no per-replica capacity is configured** (`CapacityConfigured=False`): the recommendation is incomplete, and no number stands in for it. |
| `stabilizedReplicas` | Active only: what is applied after scale-down stabilization and cooldown. Absent in Recommend mode, which keeps no hypothetical scale history. |
| `appliedReplicas`, `lastScaleTime` | Active only: the last successful write. Cleared when the target Deployment is replaced (`targetUID` changes). |
| `targetUID` | UID of the target Deployment the status describes. |
| `conflicts` | The other replica writers the last coexistence check found on the target, each as `{group, kind, namespace, name, reason}`. `reason` is set only where the kind alone does not explain it: `PausedReplicas` (a paused KEDA ScaledObject holding a fixed count) or `LegacyAPIGroup` (an autoscaler of the legacy group). Empty when there are none. After a failed check (`ConflictDetected=Unknown`) it lists those found before the failure. Conditions, events and the decision ledger show the same writers as text (`Kind/name`). |
| `metricSource` | The compiled request-rate query: `query`, `sha256` (of the exact query text), `observedGeneration`, `targetUID`, `contract` (`requests-per-second/v1`). It is published before the forecasting service is asked. The service uses a forecast only when it was computed for this query, generation and target; any other forecast is refused, and the reactive rule applies. It is absent while the configuration is invalid. |
| `prediction` | Engine `statistical` only: `engine`, `settingsSHA256`, the published `lead` (`seconds`, `startupDelaySeconds`, `actuationAllowanceSeconds`, `method` `configured`/`measured`, `sampleCount`, `observedGeneration`; the forecasting service reads it), `leadBasis` and `frozenLeadSeconds`, `startup` (`delaySeconds`, `method`, `sampleCount`, `measuredP90Seconds`, `pendingSeconds` with `pendingEvidence` `neverReady`/`uncertain`, `observable`), and the last forecast: `phase`, `reason`, `model`, `issuanceID`, `decisionOrigin`, `validUntil`, `baseWindowMaxRPS`, `upperWindowMaxRPS`, `calibrationWindows`, `prospectiveWindows`, `gateCoverageBps`, the operator's `eligible` verdict, and `predictiveReplicas` / `cappedReplicas` / `capBinding`. |
| `predictedReplicas` | Deprecated: the calculated count (historical meaning). Use `calculatedReplicas` and `forecastReplicas`. |
| `lastPrediction` | Issue time of the last forecast used. |
| `conditions` | `Ready` (the reconcile completed: `ReconcileCompleted`; when False the reason names the failure: `CrossNamespaceTarget`, `DeploymentNotFound`, `InvalidMetricQuery` or `ScalingError`, and `ScalingActive` gets the same reason), `CapacityConfigured` (`CapacityPerReplica`, or `DeprecatedTargetRPS` while the capacity comes from `targetRPS`; False with `CapacityMissing` when neither is set, or `CapacityInvalid` for a value the API server would reject; the message names the resolved requests per second), `TelemetryAvailable` (`Measured` / `MetricsUnavailable`), `ForecastAvailable` (`Used`, `Disabled`, `Unavailable`, `HorizonElapsed`, `SanityRejected`, `CapacityMissing` (no forecast is requested without a capacity), on the v2 path `NotTrusted`, `Rejected`, `LeadInsufficient` and `SettingsInvalid`, and for the operator-to-forecaster authentication `Unauthorized` (401), `Forbidden` (403), `AuthUnavailable` (TokenReview could not judge) and `AuthMisconfigured` (the operator's token or CA file is missing, empty or invalid, or a token is set with a non-https URL)); a cached forecast used during a forecaster outage counts as `Used`, `PredictionReady` (engine `statistical` only; True `Ready` when the operator verified a forecast as usable now, which is readiness to activate, not authorization: Recommend still writes nothing; False with the forecasting service's reason, `HistoryInsufficient`, `CalibrationInsufficient`, `ShadowInsufficient`, `CoverageLow`, `DataUnavailable`, `IdentityMismatch`, `StateUnavailable`, `LeadInsufficient` or `ReplayGateMissing`, or the operator's own: `SettingsInvalid`, `CapacityMissing`, `RequestRefused`, `ForecastRejected`, `NotEligible`, `EvidenceInsufficient`, `EvidenceStale`, `IssuedInFuture`, `DeadlinePassed`), `ScaleOutBlocked` (True `Unschedulable`, `QuotaExceeded` or `FailedCreate` while a scale-out cannot complete; False `None`; `Unknown`/`PodsUnreadable` when the target's ReplicaSets or pods could not be read, `Unknown`/`OwnershipUnresolved` when its current ReplicaSet could not be identified with certainty), `ScalingActive` (`Active`, `RecommendMode`, `CapacityMissing`, `TelemetryHold`, `Conflict`, `CheckFailed`, `GuardAborted`, or the failure reason), `ConflictDetected` (`NoConflict`; `ReplicaWriter` when an HPA, an unpaused KEDA ScaledObject or another Active PredictiveAutoscaler targets the Deployment; `Unknown`/`CheckFailed` when the check failed), `VPAInterference` (`None`; `VPAUpdatesPods` for a VPA in any mode except `Off`; `Unknown`/`CheckFailed` when the check did not complete), `ScaleDownHeadroom` (`Holding` while a scale-down is held for headroom; `NotHolding` when headroom is not currently blocking a scale-down, which says nothing about how much spare capacity exists; `RecommendMode`; informational: it never affects `Ready` or `ScalingActive`). See coexistence.md. |

Events are emitted when a condition changes (and on `ScaledUp` / `ScaledDown`), never on every reconcile. The
transition events are `TelemetryUnavailable` / `TelemetryRestored`, `ForecastUnavailable` / `ForecastRestored`,
`ScalingInactive` / `ScalingActive`, `CapacityNotConfigured` / `CapacityConfigured`, `ScaleOutBlocked` (Warning) /
`ScaleOutUnblocked`, `PredictionReady` / `PredictionNotReady` (Normal, except Warning for `ForecastRejected`,
`RequestRefused` and `SettingsInvalid`) and `ReconcileFailed` /
`ReconcileRecovered` (Warning when the condition turns
False, Normal when it turns True; a first healthy observation emits nothing); a change to the configured states
`RecommendMode` or `Disabled` is a Normal event of that name. The
coexistence events are `ConflictDetected`, `ConflictResolved`, `ConflictCheckFailed`, `VPAInterference` and
`VPAInterferenceCleared`; `kubectl
describe pa <name>` shows them. `kubectl get pa` shows Target, Min, Max, Mode, Current, Calculated, Applied, Ready, Age; Mode is `status.mode`, Applied
is `status.appliedReplicas` (the last successful write, kept after a switch to Recommend), and Ready is the Deployment's
ready replicas (`status.readyReplicas`), not the `Ready` condition.

## Decision rule
```
reactive  = ceil(current request rate / capacity per replica)            # 0 when the rate is 0
forecast  = ceil(peak forecast in the lead window / capacity per replica) # when a usable forecast exists
            if confidence < 0.7: forecast = ceil(minReplicas + confidence × (forecast − minReplicas))
desired   = clamp(max(forecast, reactive), minReplicas, maxReplicas)
```
Safeguards on the forecast: it is discarded when its **first step** exceeds 10× the current rate (checked only when the
current rate is positive); an overestimate cap limits its influence after consecutive forecasts well above the reactive
need.
Scale-up is applied at once. Scale-down never goes below the **headroom floor**: the smallest count that leaves
`behavior.scaleDown.headroomPercent` (default 10 %) of spare capacity at the measured rate,
`ceil(rate / (capacity per replica × (1 − headroom/100)))`, capped at `maxReplicas`. For example, at 798 rpm and 2 requests/s per pod
(120 rpm) seven pods would run at 95 % of capacity, so eight are kept; seven are allowed from 756 rpm down. While the
floor holds the count (ledger action `hold_headroom`, condition `ScaleDownHeadroom=True`), the scale-down window starts
again. The floor uses the measured rate only, never the forecast. It is applied after the forecast adjustments (the
sanity check and the overestimate cap) and before the scale-down holds, so neither the overestimate override nor the
restart floor bypasses it; a forecast-derived count above the floor still bounds the scale-down. Scale-down then needs desired below current **continuously for 5 minutes** and no scale-up in the
last 5 minutes (stabilization), at least 2 minutes since the previous scale-down (cooldown), and then removes at most
`max(ceil(10 % × current), 2)` pods per step. When the overestimate cap is active, the stabilization checks are skipped but
the cooldown still applies. This state is kept in memory. A fresh Active state (an operator restart, a leadership change,
a switch to Active or a re-created target) is treated as if a scale write had just happened. This is internal hold
bookkeeping only: nothing is written and `status.lastScaleTime` does not change. Scale-down then waits **at least 10
minutes** (5 minutes after the assumed write, then the 5-minute continuously-lower window; longer with a slow reconcile
period), and under the overestimate cap at least the 2-minute cooldown. This does not depend on the persisted status.
Scale-up is not delayed. A restart also clears the forecast cache and the overestimate and ramp tracking; a switch to
Active resets the tracking but keeps the cache, and a re-created target invalidates both. Forecast decisions may
therefore differ after a restart.

**When the current request rate cannot be measured, the replica count is held.** Only a complete, successful answer with
exactly one finite, non-negative sample counts as a measurement; a failed or partial query, an empty result (no series),
several series or a non-finite value is "unavailable" — never zero traffic. Without a measurement the operator holds the
current replica count (within `[minReplicas, maxReplicas]`), whether or not a forecast exists: it neither scales down on
missing data nor scales on a forecast alone. The decision ledger records `telemetry_status: unavailable` with the reason and
the safeguard `telemetry_unavailable_hold`, and the measurement gauges (current RPM, actual need, prediction error) are
removed instead of showing stale or invented values.

**Without a per-replica capacity, the replica count is held too.** The capacity is `capacityPerReplica`, or the
deprecated `targetRPS`; one value serves the reactive rule, the forecast rule and the headroom floor. When neither is set
the operator does not guess one (earlier versions silently assumed 20,000 requests per minute per pod for the reactive
part and 30,000 for the forecast part). It holds the current count (within `[minReplicas, maxReplicas]`, as above),
requests no forecast, leaves `calculatedReplicas` absent, sets `CapacityConfigured=False` (reason `CapacityMissing`) with
a Warning event `CapacityNotConfigured`, and in Active mode `ScalingActive=False` (reason `CapacityMissing`). The measured
rate is still published (`predictive_autoscaler_current_rpm`); the replica gauges (desired, predicted, actual need,
prediction error) are removed. The ledger records `capacity_source: missing` and the safeguard `capacity_missing_hold`;
with a capacity it records `capacity_source` (`capacityPerReplica` or `targetRPS`) and `capacity_rpm_per_replica`.
Recommend mode still writes nothing. Creating or updating an object without a capacity is still accepted, so existing
objects keep working while they are migrated; a later release will require a capacity for Active mode.

### Measuring capacityPerReplica
`capacityPerReplica` is the rate one replica sustains **while meeting its SLO**, not the most it completes while requests
time out. Measure it on one replica with the production resources (CPU and memory requests and limits, concurrency
settings) and a representative request mix:

1. Run the Deployment at one replica in a test environment, with nothing else scaling it.
2. Drive it with an open-loop load generator (one that keeps sending at the set rate when responses slow down, such as
   k6's `constant-arrival-rate` executor) at increasing steps, for example 5, 10, 20, 40 requests/s, several minutes each.
3. At each step record the latency percentile your SLO uses (say p99) and the error rate.
4. Take the highest step that still meets the SLO with no errors, and set `capacityPerReplica` a margin below it (for
   example 80 % of it). If one replica met p99 at 50 requests/s but not at 60, `capacityPerReplica: "40"` is a reasonable
   start.
5. Measure in the same unit as the query: the metric source counts requests per second **arriving at the target** (offered
   load), so a capacity measured from completed requests only understates what the replica was asked to handle.

Measure again when the per-replica capacity can change: new CPU or memory requests (a VPA in any mode except `Off` does
this continuously), a different concurrency limit, new hardware or a changed request mix. Do not run a load test against
a production service without its owner's approval. Recommend mode shows what a capacity would recommend without writing
anything, so a new value can be checked against observed traffic before `Active`.

`predictive_autoscaler_prediction_error_percent` is `|forecast-side replicas − actual need| / actual need × 100` for the
same decision, where the actual need is the reactive count for the current measured rate (at least `minReplicas`) and
the forecast-side count is the forecast's replicas after confidence dampening and the overestimate cap. It compares two
replica counts, not the accuracy of a forecast against the traffic that followed. It is 100 whenever the rate is
measured and the forecast-side count is 0 (for example when no forecast is used or forecasting is disabled); after a
sanity rejection the reactive count stands in for the forecast, so it can be 0; without a measurement the series is
removed. A workload whose request counter exists but sees no traffic is
measured as a real zero and scales normally.

## Forecast contract v2 (engine statistical, preview)

With `spec.prediction.engine: statistical` the operator uses the statistical forecasting service's contract v2
(`POST /v2/predict`, `window-max-rps/v1`) instead of the v1 forecast. The service forecasts the **peak request rate
over a fixed future window** `W = {t+Δ, ..., t+L}` (Δ the grid, `t` the current grid origin, `L` the lead) and, once it
has the evidence, a calibrated **upper bound** `U` of that peak, in requests per second. The operator:

1. **Chooses the lead.** Startup budget `D`: the 90th percentile of measured starts plus 25 % once 20 completed starts
   exist in the last 7 days, otherwise `behavior.startup.fallbackDelaySeconds`; at least `minimumDelaySeconds`; and a
   start still pending for longer than the budget becomes the budget. A measured start is **pod creation to Ready** of a
   pod of the target's current ReplicaSet (controlled by the Deployment, at its revision, with its pod template) that
   never restarted; only Ready pods count as serving, and pods of other owners or older ReplicaSets that share the
   selector are ignored. It does not include the time from the scale request to the pod's creation, which the actuation
   allowance `A` covers (`status.prediction.startup.measures: podCreationToReady`); the configured fallback should cover
   the whole scale request to Ready. When the current ReplicaSet cannot be identified with certainty, nothing is
   measured and the fallback applies. A new current ReplicaSet (a rollout) discards the samples.

   **A pending start is a pod that has never been Ready** (`status.prediction.startup.pendingSeconds`, its age since
   creation). A pod that was Ready and is not Ready now (a failed readiness probe, a restarted container) is a
   readiness loss, not a start; a terminal pod (`Failed` or `Succeeded`, for example an evicted pod that stays listed
   until pod garbage collection) and a terminating pod are not starts either. None of them changes the budget or the
   lead. The operator decides this per pod from what it saw itself and from the pod's status, which also holds after
   an operator restart: the kubelet stamps `status.startTime` and the first `Ready=False` at its first report and moves
   the `Ready` condition's `lastTransitionTime` only when the status changes, so `Ready=False` since the first report
   means never Ready, and a later transition means the pod was Ready before. A restart count proves neither. When the
   status shows neither (no transition time, no start time, or conditions that contradict each other), the pod counts
   as a pending start and `pendingEvidence` is `uncertain` instead of `neverReady`: that can lengthen the lead but
   never shorten it.

   `L = ceil((D + C + A + Δ) / Δ) × Δ`, with `C` the decision interval: the reconcile interval rounded up to the grid,
   plus 5 s for the start offset and queueing jitter. A lead longer than the horizon, or no budget at all, means no
   lead (`LeadInsufficient`) and reactive decisions. A pending start is never cut to fit the horizon: while a pod has
   been starting for longer than the horizon allows, no forecast is requested and the reactive rule carries on
   (`PredictionReady=False`, reason `LeadInsufficient`, with the pending time in the message). The lead is published in
   `status.prediction.lead` before any request.

   **The lead never shrinks automatically** (`frozenLeadSeconds`): not when the slow pod is gone, not after a rollout,
   not after an operator restart, and not with time. It grows at once when a slower start needs it, and every new lead
   starts a new evidence epoch on the service (calibration and the shadow period start again). A shorter lead needs a
   new **lead basis** (`status.prediction.leadBasis`), which is a fingerprint of: the forecast settings
   (`settingsSHA256`: `prediction.horizonMinutes`, `engine` and the statistical engine's settings after it),
   `behavior.startup.fallbackDelaySeconds`, `minimumDelaySeconds` and `actuationAllowanceSeconds`, and the decision
   interval (`prediction.updateIntervalSeconds` on the sample grid). Changing one of them starts a new basis, and the
   lead is then chosen from the current budget;
   so after a one-off slow start has raised the lead, a deliberate change of the startup configuration (for example
   `minimumDelaySeconds`) brings it back. There is no other reset: other spec changes keep the frozen lead. Leaving
   the statistical engine removes `status.prediction`, and the frozen lead with it.
2. **Asks for the current origin** and checks the answer itself: every field present and nothing else, the identity
   echo (autoscaler and target UIDs, generation, query hash, settings fingerprint, origin, lead), the evidence epoch,
   the window and its targets, `valid_until`, finite non-negative rates in requests per second. A cached answer is reused
   only for the same request and only while its own window is still actionable (`now + A` before the first target and
   before `valid_until`); it is never moved to a later window.
3. **Decides eligibility independently** of the service's flag: trust state `Trusted` and reason `Ready`, an upper bound,
   at least `calibration.minWindows` calibration and `trust.minProspectiveWindows` prospective windows, a recent evidence
   cutoff, an issue time not ahead of its clock, and the remaining window.
4. **Forms the decision** with the shared capacity `c` (`capacityPerReplica`):

   ```
   R = ceil(rate / c)                                           # reactive
   P = min(ceil(U / c), R + min(maxReplicas, ceil(maxPercent × max(R, minReplicas) / 100)))   # eligible forecasts only
   desired = clamp(max(R, P, minReplicas), minReplicas, maxReplicas)
   ```

   The schedule floor is 0 (schedules are not implemented yet). Without an eligible forecast `P` is absent and the decision is reactive; without a
   measured rate the count is held, as on the v1 path. Confidence damping, the overestimate cap and the divergence check
   do not run on v2. The headroom floor, the scale-down stabilization, the restart holds, the coexistence check and the
   `/scale` write guards apply unchanged; Recommend mode writes nothing in any state.
5. **Rechecks the window immediately before the write**, on its clock, after the fresh scale read and the identity
   check: a decision that used a forecast whose window is no longer actionable is not written (`guard_abort`, verdict
   `DeadlinePassed`), and the next reconcile, seconds later, decides again without it. The verdict in step 3 also uses
   the time after the request, not the reconcile's start.

The decision ledger records `forecast_contract: window-max-rps/v1` and a `v2` block per decision: `issuance_id`,
`decision_origin`, `valid_until`, `cached`, the service's `trust_state`, `service_reason` and `service_eligible`, the
operator's `accepted` and `verdict`, `base_window_max_rps`, `upper_window_max_rps`, `capacity_rpm_per_replica`,
`predictive_replicas_uncapped`, `predictive_extra_allowance`, `predictive_replicas` and `predictive_cap_binding`, the
lead and the startup budget. The capped value is a budget, not the quantile: both are kept. Metrics:
`predictive_autoscaler_predictive_extra_replicas`, `predictive_autoscaler_predictive_capped` and
`predictive_autoscaler_prediction_gate{phase}`; `predictive_autoscaler_predicted_rpm` shows `U` (in requests per
minute) when a v2 forecast was used.

**Scheduling.** v2 decisions are anchored to grid origins: at most one per decision interval `C`, each starting one
second after its origin, and the next one is requeued for `C` after this decision's origin, net of the time this one
took. A reconcile triggered in between (a status update, a watch event) decides nothing and keeps that start.

Reading the target's ReplicaSets and pods needs `get`/`list` on `replicasets` and `pods` where the operator scales (the
chart grants them with the write bindings). Where it may not read them, `ScaleOutBlocked` is `Unknown`
(`PodsUnreadable`); where the current ReplicaSet is uncertain, `Unknown` (`OwnershipUnresolved`); in both cases no start
is measured and only the configured budget applies.

## Operator environment
| Variable | Default | Purpose |
|---|---|---|
| `ML_API_URL` | `http://ml-api-service.ml-engine.svc.cluster.local:8000` | Forecasting service. |
| `PROMETHEUS_URL` | `http://vmselect-vmst.monitoring.svc.cluster.local:8481/select/0/prometheus` | Prometheus-compatible query endpoint (any Prometheus API works). `VICTORIAMETRICS_URL` is still read as a deprecated fallback. |
| `FORECAST_LOG` | unset | Path of the append-only forecast/decision ledger (JSONL). |
| `WATCH_NAMESPACES` | all | Comma-separated namespaces to watch. |
| `ML_API_TOKEN_FILE` | unset | A projected service account token for the forecasting service's audience, read on every request and sent as `Authorization: Bearer`. With it set, `ML_API_URL` must be https, and a missing or empty file sends nothing (forecast status `auth_misconfigured`, reactive rule). |
| `ML_API_CA_FILE` | unset | The forecasting service's CA bundle. It is reloaded when it changes (checked at most every 60 s); a configured file that is missing or invalid sends nothing. Redirects are never followed. |
The chart sets `PROMETHEUS_URL` consistently for the operator, forecasting service and trainer.

## Forecasting service
| Variable | Default | Purpose |
|---|---|---|
| `PROMETHEUS_URL` | the in-cluster VictoriaMetrics address | Prometheus-compatible query endpoint for the history. `VICTORIA_METRICS_URL` is still read as a deprecated fallback. |
| `MODEL_DIR` | `/app/models/trained` | Model store (artifacts `lstm_<key>.pkl` + `.meta.json`). |
| `COLD_START_CRONJOB` | unset | Without any compatible model at startup, create one Job from this CronJob. Unset = never (a local run or a test cannot create Jobs). |
| `FORECAST_ADMISSION` / `FORECAST_INFERENCE_SLOTS` | `4` / `2` | Concurrent forecast jobs / concurrent inferences; beyond that the service answers 503. |
| `FORECASTER_AUTH` | `off` | `tokenreview`: every path except `/health`, `/ready` and open `/metrics` needs exactly one `Authorization: Bearer` token that TokenReview authenticates for `FORECASTER_AUTH_AUDIENCE` (default `predictive-autoscaler-forecaster`) and whose user is in `FORECASTER_AUTH_SUBJECTS` (comma-separated `system:serviceaccount:<ns>:<sa>`; required). This includes API docs, path variants and future endpoints. The token is checked before the body is read; the body is capped at 1 MiB. Answers: 401 Unauthenticated, 403 Forbidden, 503 AuthUnavailable (TokenReview cannot judge: never an open door), 500 TLSRequired (the request did not arrive over TLS). Positive results are cached for at most 60 s and never past the token's expiry, so a revocation takes effect within 60 s. The service needs `create` on `tokenreviews`. Unsafe settings stop the service at startup. |
| `FORECASTER_TLS_CERT` / `FORECASTER_TLS_KEY` | unset | Serve HTTPS (port 8443, `FORECASTER_PORT`). Required with `FORECASTER_AUTH=tokenreview`. When the files change (a renewal), the service restarts itself to load them (checked every 60 s). |
| `FORECASTER_AUTH_METRICS` / `FORECASTER_AUTH_METRICS_SUBJECTS` | `false` / unset | Protect `/metrics` too, for the scraper subjects only (the Helm chart does not offer it yet). Otherwise `/metrics` and `/health` are open; keep them behind network policies. |
| `ALLOW_BENCHMARK_EXPERIMENTS` | unset | Benchmark experiment arms (`SEASONAL_EXPERIMENT`, `ENSEMBLE_EXPERIMENT`) refuse to start without it; their answers never carry the product attestation. |

The service never accepts a query or a history from a request. It reads the PredictiveAutoscaler named in the request
and its target Deployment from the Kubernetes API, checks the generation, the UIDs, the query hash and the contract,
and only then reads the history (168 h, 10-minute grid) with the compiled query. A refusal is 422 (provenance, no
compatible model, an answer that is not one valid series, an incomplete input window); an outage is 503 (Kubernetes or
metrics unreachable, at capacity). Models are keyed by namespace and name and carry their query hash, contract and
autoscaler/target UIDs. **Models trained before this version are not loaded: retrain** (the training job must run
once with the current version). `POST /train` is disabled (410): models come only from the training job.

Trainings of one autoscaler are serialized with an exclusive `flock` on `lstm_<key>.lock` in the model store, held from
the candidate's creation to the sidecar's publication (`TRAINING_LOCK_WAIT_S`, default 1800 s). The model filesystem
must therefore support cross-process `flock`. The base manifests use a ReadWriteOnce volume with the trainer placed on
the API's node; check `flock` on your storage class before relying on it (same-node placement alone is not that check).

## Operator flags
| Flag | Default | Purpose |
|---|---|---|
| `--leader-elect` | `true` | Only the instance holding the Lease `predictive-autoscaler.autoscaling.devkuban.com` reconciles (the legacy operator's Lease was `predictive-autoscaler-leader`, so the two do not block each other during a migration). Installations whose watched namespaces overlap must share the Lease namespace. Set `false` only for a single local run. |
| `--leader-election-namespace` | the pod's namespace | Namespace of the Lease; required when running outside a cluster with leader election on. |
| `--metrics-bind-address` | `:8080` | Prometheus metrics (plain HTTP). |
| `--health-probe-bind-address` | `:8081` | `/healthz` and `/readyz`. |
