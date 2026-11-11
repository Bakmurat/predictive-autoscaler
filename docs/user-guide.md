# User Guide

This guide walks you through Predictive Autoscaler from a first local try to running it on your own workload, and
explains every feature you can see or change. It is written for an operator who is new to the product. The reference
pages hold the full details; this guide links them where they matter.

> **Status:** a research prototype, tested only in test environments with generated load. It has no published
> performance figures. Try it on a test cluster first, and keep a reactive autoscaler you trust ready as a fallback.

Contents:
[1. What it does](#1-what-it-does-and-when-to-use-it) ·
[2. The parts](#2-the-parts) ·
[3. Try it locally](#3-try-it-locally-quickstart) ·
[4. Install with Helm](#4-install-with-helm) ·
[5. Your first autoscaler](#5-create-your-first-autoscaler-in-recommend-mode) ·
[6. Read its status](#6-read-its-status) ·
[7. Get forecasts](#7-get-forecasts) ·
[8. Switch to Active, and stop](#8-switch-to-active-and-how-to-stop) ·
[9. Scale-down and headroom](#9-scale-down-behaviour-and-headroom) ·
[10. Next to HPA/KEDA/VPA/GitOps](#10-running-next-to-hpa-keda-vpa-and-gitops) ·
[11. Monitoring](#11-monitoring-metrics-the-help-dashboard-and-alerts) ·
[12. Security](#12-security) ·
[13. Upgrading](#13-upgrading-and-the-api-group-migration) ·
[14. Uninstalling](#14-uninstalling-safely) ·
[15. Troubleshooting](#15-troubleshooting) ·
[16. Limitations](#16-limitations) ·
[17. Glossary](#17-glossary)

## 1. What it does and when to use it

Predictive Autoscaler sets the replica count of a Kubernetes **Deployment** from its **request rate**. It combines two
answers on every reconcile:

- **Reactive:** how many pods the request rate measured right now needs;
- **Forecast:** how many pods the highest forecast request rate in the next few minutes (the *lead window*) needs.

It takes the **higher** of the two, kept within your minimum and maximum. A forecast can therefore only add capacity
ahead of a rise; it never removes capacity the measured rate needs. When no usable forecast exists, the decision is
reactive only. When the request rate cannot be measured at all, the replica count is **held**, never scaled on a guess.
Both answers divide by one number you configure: the request rate one replica serves within its SLO
(`capacityPerReplica`). Without it the operator calculates nothing and holds the current count; it never assumes one.

Use it for a stateless Deployment whose traffic follows a daily pattern and whose pods take long enough to start that
scaling after the rise is too late. It is less useful for traffic without a repeating pattern, and forecasts start only
after a successful training on enough request history (by default at least 235 usable 10-minute points, about 39 hours,
out of a 7-day lookback; [section 7](#7-get-forecasts)). Only the request rate is used; CPU and memory are not.

Every autoscaler starts in **Recommend** mode: it computes and publishes what it would do and changes nothing. You
switch it to **Active** when you trust its recommendations.

## 2. The parts

| Part | What it does |
|---|---|
| **Operator** (Go) | Watches `PredictiveAutoscaler` objects. On each reconcile it measures the request rate, asks the forecasting service for a forecast, decides a replica count, publishes it in the status and metrics and, in Active mode only, writes it to the Deployment's `/scale`. |
| **Forecasting service** (`ml-api`, Python) | Serves a forecast for the next hour in six 10-minute steps for one autoscaler. It reads the autoscaler and its Deployment from the Kubernetes API, reads the history itself with the autoscaler's compiled query, and refuses when its input or model does not fit. |
| **Trainer** (one CronJob per autoscaler) | Every six hours (your schedule) trains a model for one autoscaler from its recent request rate (up to seven days by default) and publishes it to the model volume; the service picks it up. |
| **Your metrics store** | Any Prometheus-compatible query API (Prometheus, VictoriaMetrics, Thanos Query, ...) that holds your workload's request counter. All three parts read it. |

The Helm chart installs the first three; it does not install any `PredictiveAutoscaler`. Those live with the workloads
they scale. Details: [concepts.md](concepts.md).

## 3. Try it locally (quickstart)

The fastest way to see the whole loop is the kind quickstart. It creates its own throwaway kind cluster with its own
kubeconfig (it never touches `~/.kube/config`), builds the images from your checkout, installs the chart with TLS,
authentication and NetworkPolicies on, and runs a tiny HTTP app with a request generator and a small metrics store.

You need Docker, kind v0.33 or later, kubectl, Helm and Python 3, about 6 GiB of memory and 15 GiB of free Docker
disk. The first image build can take several minutes.

```sh
quickstart/kind.sh up       # install; the autoscaler starts in Recommend mode
quickstart/kind.sh status   # conditions, replica counts, last forecast
quickstart/kind.sh active   # let this demo's operator scale
quickstart/kind.sh stop     # back to Recommend; the replica count stays
quickstart/kind.sh down     # delete only this quickstart's cluster
```

On a fresh run the demo stays at one replica while the operator *recommends* at least two, and
`ForecastAvailable=False` is expected: there is no history and no model yet. To see a forecast at once, use a separate
run with `--synthetic-history` (and its own `PA_QUICKSTART_STATE` directory): it imports eight days of generated
traffic and trains a small demonstration model. Generated history proves that installation, training and scaling work;
it says nothing about forecast quality. Details: [quickstart/README.md](../quickstart/README.md).

## 4. Install with Helm

Requirements: Kubernetes 1.33–1.35 (tested; the chart admits any version from 1.33, later ones are untested), Helm, a default storage class (ReadWriteOnce with `flock` support), and a
Prometheus-compatible query API holding your workload's request counter. Istio is needed only for the `istio` metric
preset; KEDA is optional. There are no published release images yet: build them from source
([getting-started.md](getting-started.md), step 1). Always pass an explicit kubeconfig and context, and start on a
test cluster.

The values you must set:

```yaml
images:
  operator: {repository: <registry>/predictive-autoscaler-operator, tag: dev}
  forecaster: {repository: <registry>/predictive-autoscaler-ml-api, tag: dev}
prometheus:
  url: http://prometheus-server.monitoring.svc:9090        # your query API
networkPolicy:
  prometheus:                                               # where it runs (or networkPolicy.enabled: false)
    namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: monitoring}}
training:
  targets:                                                  # one entry per PredictiveAutoscaler to train for
    - {namespace: shop, name: web-pa, schedule: "0 */6 * * *"}
```

```sh
helm --kubeconfig <file> --kube-context <context> install pa charts/predictive-autoscaler \
  -n predictive-autoscaler --create-namespace -f my-values.yaml --wait --timeout 6m
```

`training.targets[].name` is the **name of the PredictiveAutoscaler** (not of the Deployment). Install one release per
cluster, or give several releases disjoint `operator.watchNamespaces`. Other options you may want:

| Value | What it does |
|---|---|
| `operator.watchNamespaces` | Limit the operator to these namespaces (empty: all). |
| `operator.forecastLedger.enabled` | Keep an append-only JSON-lines ledger of every forecast attempt and decision, for auditing; on an emptyDir unless you give `persistence.existingClaim`. |
| `forecaster.persistence.*` | The model volume: size, storage class, access mode (`ReadWriteOnce` or `ReadWriteMany`, never `ReadWriteOncePod`), `retain` (`true` by default: the chart-created claim is kept on uninstall), or your own `existingClaim`. |
| `forecaster.auth.*`, `forecaster.tls.*` | Authentication and TLS between the operator and the forecasting service (on by default; see [Security](#12-security)). |
| `training.hours`, `training.epochs`, `training.activeDeadlineSeconds` | History per training (168 h), epochs, and the time limit of a training run. |
| `networkPolicy.*` | The NetworkPolicies (on by default): DNS, Kubernetes API addresses and ports, Prometheus peers, who may scrape metrics. |
| `metrics.serviceMonitor` / `metrics.vmServiceScrape` | Scrape objects for the Prometheus Operator or the VictoriaMetrics Operator. |
| `metrics.alerts.*` | Alert rules as a `PrometheusRule` or `VMRule`, their thresholds and extra labels ([Monitoring](#11-monitoring-metrics-the-help-dashboard-and-alerts)). |
| `dashboards.*` | The Grafana "Help & Overview" dashboard as a ConfigMap for the Grafana sidecar. |

`values.yaml` documents every option and `values.schema.json` rejects unknown keys. After the install, `helm status`
prints the chart's notes. Full reference: [helm.md](helm.md).

## 5. Create your first autoscaler in Recommend mode

Before you start, check that nothing else scales the Deployment
([checklist](coexistence.md#checklist-before-creating-a-predictiveautoscaler)). In Recommend mode a conflict is only
reported, so you can also create the autoscaler next to an existing HPA to compare the two.

```yaml
apiVersion: autoscaling.devkuban.com/v1alpha1
kind: PredictiveAutoscaler
metadata: {name: web-pa, namespace: shop}
spec:
  mode: Recommend
  targetDeployment: {name: web, namespace: shop}      # same namespace as the autoscaler
  minReplicas: 2
  maxReplicas: 10
  metrics:
    requests:
      enabled: true                                   # required for forecasting
      capacityPerReplica: "40"                        # requests per second ONE ready replica serves within its SLO
      source:
        preset: prometheus
        query: 'sum(rate(http_requests_total{namespace="{{ .Namespace }}",service="{{ .Name }}"}[2m]))'
  prediction: {enabled: true, leadTimeMinutes: 15, horizonMinutes: 60, updateIntervalSeconds: 60}
```

The fields that matter:

- **`mode`**: `Recommend` (the default, also when absent) or `Active`.
- **`minReplicas` / `maxReplicas`**: the bounds of every decision.
- **`metrics.requests.capacityPerReplica`**: the requests per second one ready replica serves while meeting its SLO
  (for example its p99 latency target). Decimals are allowed (`"12.5"`). The number above is only an example: measure
  yours with a load test ([how](configuration.md#measuring-capacitypereplica)). There is **no default**: without it the
  autoscaler reports `CapacityConfigured=False` (reason `CapacityMissing`), shows no calculated count and holds the
  current one. The older integer field `targetRPS` means the same and still works, but is deprecated; set one of the
  two, never both.
- **`metrics.requests.source`**: where the request rate comes from. `preset: istio` (the default) reads Istio's
  `istio_requests_total` for the Deployment. `preset: prometheus` uses your `query`, which must return **exactly one
  series, in requests per second, for this Deployment**. It may use only `{{ .Namespace }}` and `{{ .Name }}` (the
  Deployment's namespace and name); no other template features are allowed.
- **`prediction`**: `enabled` (false = reactive only), `leadTimeMinutes` (how far ahead the decision looks),
  `horizonMinutes`, and `updateIntervalSeconds` (the reconcile period).
- **`behavior.scaleDown.headroomPercent`**: spare capacity a scale-down must leave (default 10; see
  [section 9](#9-scale-down-behaviour-and-headroom)).

Set these fields explicitly: defaults differ depending on whether the parent object (`prediction`, `metrics.requests`)
is present, and `metrics.requests.enabled` defaults to `false`. The CRD accepts some fields the operator ignores
(`metrics.cpu`, `metrics.memory`, `resources`, `targetDeployment.container`). Every field: [configuration.md](configuration.md).

```sh
kubectl --kubeconfig <file> --context <context> apply -f web-pa.yaml
kubectl --kubeconfig <file> --context <context> -n shop get pa web-pa
```

Within one reconcile you should see `Ready=True`, `CapacityConfigured=True` and `TelemetryAvailable=True` for the
current generation, and a calculated replica count. The Deployment does not change.

## 6. Read its status

### `kubectl get pa`

| Column | Meaning |
|---|---|
| Target | The Deployment it scales (`spec.targetDeployment.name`). |
| Min, Max | `spec.minReplicas` and `spec.maxReplicas`. |
| Mode | The mode applied in the last reconcile (`status.mode`). |
| Current | The Deployment's replica count (`status.currentReplicas`). |
| Calculated | The decision: what the operator would set now (`status.calculatedReplicas`), or the held count while the request rate cannot be measured. Empty while no per-replica capacity is configured (the recommendation is incomplete) and while the target is paused at 0 replicas. |
| Applied | The last count the operator successfully wrote. Empty until the first write; it keeps showing that last write after a switch back to Recommend, and is cleared only when the target Deployment is replaced. |
| Ready | The Deployment's **ready replicas** (`status.readyReplicas`), not the `Ready` condition. |
| Age | Since the autoscaler was created. |

### More in `kubectl get pa <name> -o yaml`

- `observedGeneration`: the spec generation the status describes. Trust a condition only when it matches
  `metadata.generation`.
- `forecastReplicas`: the replica count from the forecast alone (absent when no forecast was used).
- `stabilizedReplicas`: Active mode only, what will be applied after the scale-down rules.
- `appliedReplicas`, `lastScaleTime`: the last successful write (kept after a switch to Recommend). `lastPrediction`: when the forecast in use was issued.
- `metricSource`: the compiled query (`query`, `sha256`, `observedGeneration`, `targetUID`, `contract`). Run this query
  yourself when the telemetry looks wrong.
- `conflicts`: other replica writers found on the target (`group`, `kind`, `namespace`, `name`, `reason`).
- `targetUID`: the Deployment the status describes; a replaced Deployment clears the write history.
- `predictedReplicas`: deprecated; use `calculatedReplicas` and `forecastReplicas`.

### Conditions and reasons

| Condition | True means | Reasons |
|---|---|---|
| `Ready` | The last reconcile completed. | `ReconcileCompleted` (True). When False, the reason names the failure: `CrossNamespaceTarget` (the target is in another namespace; nothing is written), `DeploymentNotFound`, `InvalidMetricQuery` (the query breaks the template rules; nothing is written), `ScalingError` (the write to `/scale` failed). |
| `CapacityConfigured` | A per-replica capacity is configured; the message names it in requests per second. | `CapacityPerReplica` (True); `DeprecatedTargetRPS` (True, from the deprecated `targetRPS`: move to `capacityPerReplica`); `CapacityMissing` (False: neither field is set, so nothing is calculated and the count is held); `CapacityInvalid` (False: a value the API server would reject, such as both fields at once). |
| `TelemetryAvailable` | The request rate was measured. | `Measured`; `MetricsUnavailable` when the query failed, returned no series, several series, a partial answer or a non-finite value. The replica count is then held. |
| `ForecastAvailable` | A forecast took part in the decision. | `Used`; `Disabled` (`spec.prediction.enabled: false`); `Unavailable` (no model yet, a refusal or an outage: see [section 7](#7-get-forecasts)); `HorizonElapsed` (every step of the cached forecast has passed); `SanityRejected` (its first step exceeded ten times the measured rate, so it was discarded); `CapacityMissing` (no forecast is requested without a per-replica capacity); with engine `statistical`, `NotTrusted` (not yet eligible: see `PredictionReady`), `Rejected` (the answer failed the operator's checks), `LeadInsufficient` and `SettingsInvalid`; and the authentication failures `Unauthorized`, `Forbidden`, `AuthUnavailable`, `AuthMisconfigured` ([section 12](#12-security)). |
| `ScalingActive` | Scaling is permitted: Active mode, a target above 0 replicas, no conflict, the check succeeded, no guard abort, a capacity configured, telemetry measured. It does not mean a write happened; `Applied`, `lastScaleTime` and the `ScaledUp`/`ScaledDown` events show writes. | `Active`; `RecommendMode`; `TargetPaused` (the Deployment has 0 desired replicas: nothing is written in any mode, see [Pausing a target](#pausing-a-target)); `CapacityMissing` (holding because no per-replica capacity is configured); `TelemetryHold` (holding because the rate is unavailable); `BoundsEnforced` (nothing could be measured or calculated, and the count was outside `[minReplicas, maxReplicas]`: it is brought inside them, and the message says whether that was written); `TargetNotAllowed` (the operator's write allowlist does not name this Deployment at its current UID; nothing is written until the list does); `Conflict` (another writer); `CheckFailed` (the coexistence check failed); `GuardAborted` (a last-moment check stopped a write; it retries within seconds); or the failure reason from `Ready`. |
| `ConflictDetected` | Another replica writer targets the Deployment. | `NoConflict` (False); `ReplicaWriter` (True: an HPA, an unpaused KEDA ScaledObject, another Active PredictiveAutoscaler or a legacy-group autoscaler); `CheckFailed` (status `Unknown`: the check could not finish, which blocks writes like a conflict). |
| `VPAInterference` | A VerticalPodAutoscaler changes the pods' requests. | `None` (False); `VPAUpdatesPods` (True: a VPA in any mode except `Off`; scaling continues, but per-pod capacity may drift); `CheckFailed` (status `Unknown`). |
| `PredictionReady` | Engine `statistical` only: the operator verified the last forecast as usable now (ready to activate; Recommend still makes no writes). | `Ready` (True). False with the forecasting service's reason (`HistoryInsufficient`, `CalibrationInsufficient`, `ShadowInsufficient`, `CoverageLow`, `DataUnavailable`, `IdentityMismatch`, `StateUnavailable`, `ConfigurationNotValidated` (the grid, lead, target and family set are not a configuration the trust rules were validated for: see "Validated configurations" in section 11), `LeadInsufficient`, `ReplayGateMissing`) or the operator's own: `SettingsInvalid`, `CapacityMissing`, `RequestRefused` (the service refused the request), `AutoscalerLimitReached` (the service already serves as many autoscalers as it is configured for; it lasts while every registration is in use, see [helm.md](helm.md)), `ForecastRejected` (the answer failed the operator's checks), `NotEligible`, `EvidenceInsufficient`, `EvidenceStale`, `IssuedInFuture`, `DeadlinePassed`. Collecting evidence is normal: decisions are reactive meanwhile. |
| `ScaleOutBlocked` | A scale-out cannot complete. | `Unschedulable` (a pod the scheduler cannot place), `QuotaExceeded` or `FailedCreate` (the ReplicaSet cannot create pods) (True); `None` (False); `PodsUnreadable` (status `Unknown`: the operator may not read the target's ReplicaSets or pods); `OwnershipUnresolved` (status `Unknown`: the Deployment's current ReplicaSet could not be identified with certainty). Only the current ReplicaSet's pods count. |
| `ScaleDownHeadroom` | A scale-down is held to keep headroom. | `Holding` (True); `NotHolding` (headroom is not blocking a scale-down; it says nothing about how much spare capacity exists); `RecommendMode`. Informational: never affects `Ready` or `ScalingActive`. |

### Events

`kubectl describe pa <name>` shows events. They are emitted when a condition changes, not on every reconcile:
`ScaledUp` and `ScaledDown` (each applied change); `TelemetryUnavailable` / `TelemetryRestored`;
`ForecastUnavailable` / `ForecastRestored`; `ScalingInactive` / `ScalingActive`; `CapacityNotConfigured` (Warning) /
`CapacityConfigured`; `ScaleOutBlocked` (Warning) / `ScaleOutUnblocked`; `PredictionReady` / `PredictionNotReady`
(engine `statistical`; a warning only for `ForecastRejected`, `RequestRefused`, `SettingsInvalid` and `AutoscalerLimitReached`); `ReconcileFailed` /
`ReconcileRecovered`; `RecommendMode`, `Disabled` and `TargetPaused` (configured states, type Normal); and for coexistence
`ConflictDetected`, `ConflictResolved`, `ConflictCheckFailed`, `VPAInterference` and `VPAInterferenceCleared`.

## 7. Get forecasts

**What a forecast needs.**
1. A **training target**: the autoscaler must be listed in the chart's `training.targets`. The chart creates one
   CronJob per entry; its annotation `autoscaling.devkuban.com/training-target` names the autoscaler.
2. **History**: the trainer reads the last `training.hours` (default 168, seven days) of the autoscaler's request
   rate on a 10-minute grid. Seven days is the default lookback, not a minimum: with the default 24-hour input
   sequence the trainer's preflight needs at least **235 usable 10-minute points** (about 39 hours) after the validity
   mask, and the gap and sequence checks must still leave at least one training and one validation sequence;
   otherwise the training publishes nothing and decisions stay reactive. More history gives the model more daily
   cycles to learn from. Serving a forecast also needs a **complete recent input window** (the model's 24 hours, with a
   fresh, really observed latest sample and only bounded interior gaps filled); an incomplete window is refused.
3. A **successful training**. The chart has no automatic first training (no cold start). To train now instead of
   waiting for the schedule:

```sh
kubectl --kubeconfig <file> --context <context> -n predictive-autoscaler get cronjobs \
  -o custom-columns='NAME:.metadata.name,TARGET:.metadata.annotations.autoscaling\.devkuban\.com/training-target'
kubectl --kubeconfig <file> --context <context> -n predictive-autoscaler create job first-training \
  --from=cronjob/<matching-cronjob>
kubectl --kubeconfig <file> --context <context> -n predictive-autoscaler wait --for=condition=complete \
  job/first-training --timeout=30m
kubectl --kubeconfig <file> --context <context> -n predictive-autoscaler logs job/first-training
# a successful log ends with "status": "success" and an artifact_sha256
```

Then check `ForecastAvailable=True` (reason `Used`), a recent `status.lastPrediction` and `status.forecastReplicas`.
Retraining runs on your schedule; the model is used without restarting anything.

**A model belongs to one autoscaler.** It is bound to the autoscaler's namespace and name, its UID, its target
Deployment's UID, and the exact compiled query. If you change the query, or re-create the autoscaler or the
Deployment, the old model is refused: retrain. Models from earlier versions without this provenance are not loaded.

**How forecasts are used.** The operator asks for a new forecast about every five minutes and caches it in between.
It sizes for the **highest forecast inside the lead window** (`leadTimeMinutes`). A forecast with low confidence
(below 0.7) is pulled toward `minReplicas`; a forecast whose first step is more than ten times the measured rate is
discarded (`SanityRejected`); after consecutive forecasts well above the measured need, an overestimate cap limits the
forecast's influence.

**What refusals and outages mean.** The forecasting service refuses (HTTP 422) when the request does not fit: no
compatible model, a provenance mismatch (generation, UIDs, query hash), an answer from your query that is not one valid
series, or an incomplete input window. A refusal (any HTTP 4xx) drops the cached forecast and makes the decision
reactive at once, with `ForecastAvailable=False` and reason `Unavailable`; an authentication refusal shows
`Unauthorized` or `Forbidden` instead ([section 12](#12-security)). An outage (an HTTP 5xx such as 503: the Kubernetes
API or the metrics store unreachable, or the service at capacity) or an unreachable service is different: the operator
keeps using its last forecast while that forecast is less than ten minutes old and still covers the lead window. Such
a cached forecast is a usable forecast, so `ForecastAvailable` can stay True (`Used`) during the outage. Only when no
usable forecast remains does it turn False (`Unavailable`, or `HorizonElapsed` when the cached steps have passed), and
the decision becomes reactive. A TokenReview outage is reported as `AuthUnavailable`, and an operator-side token or CA
problem as `AuthMisconfigured`. A TokenReview outage is an HTTP 503 like any other outage, so a recent usable cached
forecast is still reused; `AuthMisconfigured` and the 401/403 refusals (`Unauthorized`, `Forbidden`) drop the cache
when they occur.

**A slow or silent forecasting service is waited for only within an allowance.** One forecast request waits at most
5 seconds, and all forecast requests of all autoscalers together at most 5 seconds plus one sixth of the elapsed time
(10 seconds in a 30-second round), whether the service answers slowly, answers with errors or does not answer at all.
Requests the allowance does not cover are not sent; reactive scaling goes on, and an answer that arrives too late is
never used. The same rule, with its own allowance, bounds the metrics query; without a measured rate the count is held.
A forecast that takes longer than 5 seconds is not received, also on the legacy engine, whose client used to wait up
to two minutes ([configuration.md](configuration.md#waiting-for-the-forecasting-service-and-the-metrics-backend)).

Losing the forecast leaves you with reactive scaling. Losing the **request rate** is more serious: the operator then
holds the current replica count and does not follow demand in either direction until telemetry returns
(`TelemetryAvailable=False`, `ScalingActive=False` with `TelemetryHold`). Only the bounds still apply: a count below
`minReplicas` is raised to it at once and a count above `maxReplicas` is lowered to it through the scale-down holds,
shown as `BoundsEnforced` instead of `TelemetryHold`.

## 8. Switch to Active, and how to stop

Before you switch:
- compare the recommendations (`Calculated`, `forecastReplicas`) with what you expect for a while;
- remove or pause every other replica writer: HPAs, KEDA ScaledObjects (`autoscaling.keda.sh/paused: "true"`), and
  GitOps applying a fixed `replicas` ([section 10](#10-running-next-to-hpa-keda-vpa-and-gitops)).

```sh
kubectl --kubeconfig <file> --context <context> -n shop patch pa web-pa --type merge -p '{"spec":{"mode":"Active"}}'
kubectl --kubeconfig <file> --context <context> -n shop get pa/web-pa deploy/web   # Applied, Calculated, ready replicas
```

`ScalingActive=True` (reason `Active`) means the operator is permitted to write; whether it writes depends on the
decision. Writes show in `Applied`, `lastScaleTime` and the `ScaledUp`/`ScaledDown` events. The operator writes only
the replica count, only through the Deployment's `/scale` subresource, and only if the count is still the one its
decision was based on. Scale-ups are applied at once; scale-downs follow the rules in
[section 9](#9-scale-down-behaviour-and-headroom). After a switch to Active (or an operator restart) a scale-down
normally waits at least ten minutes; under an active overestimate override only the 2-minute cooldown applies.

**To stop**, set the mode back:

```sh
kubectl --kubeconfig <file> --context <context> -n shop patch pa web-pa --type merge -p '{"spec":{"mode":"Recommend"}}'
```

Writes stop at the next reconcile and the Deployment keeps its current replica count. `ScalingActive` shows
`RecommendMode`. `Applied` keeps showing the last write made while Active. Deleting the autoscaler also stops scaling and leaves the replica count as it is (the operator uses no
finalizer). To hand over to KEDA, follow the [paused-KEDA fallback pattern](coexistence.md#the-paused-keda-fallback-pattern).

### Pausing a target

To take a workload down for maintenance without touching the autoscaler, scale the Deployment to 0:

```sh
kubectl --kubeconfig <file> --context <context> -n shop scale deployment web --replicas=0
```

A Deployment with 0 desired replicas is **paused**: the operator writes nothing to it in any mode and does not bring it
back to `minReplicas`. `ScalingActive` shows `TargetPaused`, the Calculated column is empty, and the ledger records
`action: paused`. Set a positive replica count to resume; the next reconcile then decides normally (a scale-up at once,
a scale-down after the usual ten minutes of a fresh start). This is not scale-to-zero: the operator never sets 0 itself
and does not wake a paused workload when traffic returns
([configuration.md](configuration.md#pausing-a-target)).

## 9. Scale-down behaviour and headroom

Scaling down is deliberately slow and bounded; scaling up is immediate.

- **Headroom floor:** a scale-down never goes below the count that leaves `behavior.scaleDown.headroomPercent` (default
  10 %) spare capacity at the measured rate. Example: at 798 requests per minute and 2 requests/s (120 rpm) per pod,
  seven pods would run at 95 % of capacity, so eight are kept. While the floor holds, `ScaleDownHeadroom=True`
  (`Holding`). The floor uses the measured rate only, never the forecast. `0` switches it off.
- **Stabilization, two windows one after the other:** first, no scale-down within 5 minutes of the last scale-up (or
  of the assumed write after a fresh start); only then does the second window start: the decision must stay below the
  current count **continuously for 5 minutes**. So a fresh state waits at least 10 minutes.
- **Cooldown:** at least 2 minutes between two scale-downs.
- **Step size:** each step removes at most `max(ceil(10 % of current), 2)` pods.
- **Overestimate override:** when the overestimate cap is active, both stabilization windows are skipped, but the
  2-minute cooldown and the headroom floor still apply.
- **After a restart, a leadership change, a switch to Active or a re-created Deployment**, the operator behaves as if
  it had just scaled: scale-down normally waits at least 10 minutes (the two windows above), or the 2-minute cooldown
  under an overestimate override. This state lives in memory.

The full rule with its formulas: [configuration.md, Decision rule](configuration.md#decision-rule).

## 10. Running next to HPA, KEDA, VPA and GitOps

A Deployment's replica count must have exactly **one** owner. In Active mode the operator refuses to write while any of
these targets the Deployment: an HPA (KEDA creates one too), a KEDA ScaledObject that is not paused, another Active
PredictiveAutoscaler, or a legacy-group (`autoscaler.example.com`) autoscaler. It shows `ConflictDetected=True`,
`ScalingActive=False` (reason `Conflict`), names them in `status.conflicts`, and continues to publish its decision. When
the other writer is gone or paused, it scales again at the next reconcile. If the check itself fails (permissions, API
discovery, a timeout) it writes nothing either (`CheckFailed`).

- **KEDA as a fallback:** keep a ScaledObject for the same Deployment with `autoscaling.keda.sh/paused: "true"`. Hand
  over by switching the autoscaler to Recommend first, then unpausing KEDA; hand back by pausing KEDA, checking that
  no HPA targets the Deployment any more, then switching to Active. A `paused-replicas` annotation is not a pause.
- **VPA:** allowed, but a VPA that changes requests (any mode except `Off`) changes the per-replica capacity
  `capacityPerReplica` assumes; the operator warns with `VPAInterference=True`. Tune the two together, or use `Off`.
- **GitOps (Argo CD, Flux):** exclude `spec.replicas` from the Deployment's comparison (Argo CD `ignoreDifferences` on
  `/spec/replicas`) or remove `replicas` from the managed manifest. The operator cannot detect a GitOps tool, a CI job
  or `kubectl scale` writing the count.
- **Recommend mode** next to an existing HPA or KEDA is safe: it reports the conflict and writes nothing anyway.

Details, limits and the hand-over checklist: [coexistence.md](coexistence.md).

## 11. Monitoring: metrics, the Help dashboard and alerts

### Scraping

Set `metrics.serviceMonitor.enabled` (Prometheus Operator) or `metrics.vmServiceScrape.enabled` (VictoriaMetrics
Operator). They scrape the operator (port 8080) and the forecasting service, keep the autoscalers' own `namespace`
label, and add `pa_install_namespace` (the namespace the chart is installed in).

### What the operator exports

Most series carry `application` (the target Deployment's name) and `namespace`:

- **Request rate:** `predictive_autoscaler_current_rpm` (measured, requests per minute; absent while unmeasurable) and
  `predictive_autoscaler_predicted_rpm` (the forecast peak in the lead window used by the last decision; absent when no
  forecast was used, so absence is not zero demand).
- **Replicas:** `predictive_autoscaler_desired_replicas` (the decision before scale-down stabilization),
  `predictive_autoscaler_actual_needed_replicas` (what the measured rate alone needs) and
  `predictive_autoscaler_predicted_replicas` (the forecast-side count after its safeguards: 0 while no forecast is
  used, the reactive count after a sanity rejection).
- **Replica disagreement (deprecated name "prediction error"):** `predictive_autoscaler_prediction_error_percent` =
  `|forecast-side replicas − measured need| / measured need × 100`, where the measured need is the reactive count for the
  **current** rate (at least `minReplicas`). It compares two replica counts of the same decision; it is **not** forecast
  accuracy and does not score how a forecast turned out against the traffic it predicted (see "Forecast quality" below).
  It reads 100 whenever the rate is measured and the forecast-side count is 0 (for example while no forecast is used),
  can read 0 after a sanity rejection (the reactive count stands in), and the series is removed while the rate cannot be
  measured. Read it with `ForecastAvailable`.
- **Scaling:** `predictive_autoscaler_scale_events_total{direction="up"|"down"}`.
- **Forecast record:** `predictive_autoscaler_forecast_rpm{step}` and
  `predictive_autoscaler_forecast_target_timestamp_seconds{step}` (one series per step),
  `predictive_autoscaler_forecast_issued_timestamp_seconds`, `predictive_autoscaler_forecast_step_minutes`.
- **Model:** `predictive_autoscaler_model_info{model_name, model_version}`,
  `predictive_autoscaler_model_trained_timestamp_seconds` and
  `predictive_autoscaler_model_training_cutoff_timestamp_seconds` (-1 when unknown).
- **Guard rails:** `predictive_autoscaler_sanity_rejections_total`, `predictive_autoscaler_overestimate_overrides_total`
  and `predictive_autoscaler_overestimate_streak`.

Two series carry `namespace` and `autoscaler` (the PredictiveAutoscaler's name) instead:
`predictive_autoscaler_condition{type, status}` (1 for each condition's current status, 0 for the other two) and
`predictive_autoscaler_forecasting_enabled` (1 unless `spec.prediction.enabled` is false). They disappear with the
autoscaler. The forecasting service exports its own metrics as well.

The same two labels carry what a dashboard needs to follow one autoscaler through a reconcile. A value that does not
exist has no series (nothing is shown as 0 that was not measured, decided or confirmed), and no label holds free
text: reasons and outcomes are fixed names, anything else is `Other`. The stage, decision, capacity and forecast series
describe the last reconcile that reached a decision: a reconcile that ends before that (no target, an invalid query)
removes them, so an old value is never shown as current.

| Series | Meaning |
|---|---|
| `predictive_autoscaler_mode{mode}` | 1 for the current mode (`Recommend` or `Active`), 0 for the other. |
| `predictive_autoscaler_condition_reason{type, reason}` | One series with value 1 per condition, labelled with its current reason (for example `type="ScalingActive", reason="TargetNotAllowed"`). |
| `predictive_autoscaler_decision_action{action}` | One series with value 1: what the last decision did (`scale_up`, `hold_stabilizing`, `recommend`, `conflict_hold`, ...), the `action` of the decision record. |
| `predictive_autoscaler_calculated_replicas` | Stage 1: the calculated count, in any mode. |
| `predictive_autoscaler_requested_replicas` | Stage 2: the count the write path intended: the target of a write attempt, or the held count. A final guard may still have prevented the write (see `scale_writes_total`). Active mode only; absent when nothing was intended (Recommend, a paused target, a conflict, a target that is not allowed). |
| `predictive_autoscaler_applied_replicas`, `predictive_autoscaler_applied_timestamp_seconds` | Stage 3: the last write the API server confirmed, and when. Absent until this operator process has written once, and removed when the Deployment or the autoscaler is replaced by another object; a switch to Recommend keeps it. |
| `predictive_autoscaler_target_replicas`, `predictive_autoscaler_target_ready_replicas` | Stages 4 and 5: the Deployment's desired and Ready replicas as the last reconcile read them. |
| `predictive_autoscaler_scale_writes_total{result}` | Write attempts by outcome: `applied`; `stale` (the target changed since the decision); `guard_abort`; `not_allowed`; `read_failed` (nothing was sent); `rejected` (the API server refused it); `unknown` (no answer: the write may or may not have been applied). Every outcome starts at 0. |
| `predictive_autoscaler_capacity_per_replica_rps{source}` | The configured capacity of one replica in requests per second and the field it comes from (`capacityPerReplica` or `targetRPS`). |
| `predictive_autoscaler_prediction_selected_family{family}`, `predictive_autoscaler_prediction_used_family{family}` | Forecast contract v2: the family the service selected in the last verified forecast, trusted or not, and the family of the forecast the operator accepted into its calculation and still accepted at the final check (absent when it accepted none; accepted does not mean it added replicas or that a write happened). |
| `predictive_autoscaler_prediction_lead_seconds`, `predictive_autoscaler_prediction_issued_timestamp_seconds`, `predictive_autoscaler_prediction_window_start_timestamp_seconds`, `predictive_autoscaler_prediction_window_end_timestamp_seconds`, `predictive_autoscaler_prediction_valid_until_timestamp_seconds` | Forecast contract v2: the last verified forecast's lead, when it was issued, the future window it covers and until when it may authorize a write. A forecast's values are about its window, not about the time it was issued: plot them against the window. |

### Forecast quality (forecast contract v2, preview)

The statistical forecasting service (forecast contract v2, `window-max-rps/v1`; in development, not in the current
forecaster image) measures its forecasts against what actually happened. Each forecast covers a fixed future window and
issues a point peak and, once calibrated, an upper bound for the window's peak request rate. When the window has passed
and every sample in it has arrived, the window **matures**: its realised peak is compared with what was issued. Windows
whose data never arrives are **unscorable** and stay in the availability count; nothing is filled in.

The upper bound is the point peak plus a calibrated multiple of a **scale** that grows with the load: the scale is
`max(s_min, a + beta x peak)`, fitted once a day (at the first forecast of each UTC day) from how much the signal
differed from the same time one day earlier at each load level, so the margin is wider at high load when the noise is.
The fit needs samples one day apart for at least 80 % of a day, so about 1.8 days of history
(with engine `statistical`, `prediction.historyDays` below 2 is refused, at admission and by the forecaster): a younger signal gets point forecasts without a bound. Each forecast
records the scale it was issued with. A limit to know: in the acceptance simulations the bound kept its coverage
across load levels for continuous noise, but not for very low or discrete load. With request counts around 0.5 per
second or less (zero at night) the middle load range was covered about 80 % of the time instead of 90 %, while
windows with a zero forecast were covered almost always. For a workload that idles at or near zero the bound is not
validated.

Its series (all listed here) carry `namespace`, `autoscaler` and `model` (`seasonal_naive`, `statistical` or `lstm`),
and at most one more fixed label:

- **Coverage:** `predictive_autoscaler_forecast_window_coverage_ratio{cohort}`: the share of issued upper bounds the
  realised peak stayed under over the last 7 days (`cohort="gate"`: non-overlapping windows, the ones trust uses;
  `cohort="all"`: every window), next to `predictive_autoscaler_forecast_nominal_coverage_ratio` (the target, 0.9 by
  default) and `predictive_autoscaler_forecast_working_miscoverage_ratio` (the adaptive level). Absent, not 0 or 100 %,
  while no bound has matured.
- **Error:** `predictive_autoscaler_forecast_window_mae_rps` and `predictive_autoscaler_forecast_window_bias_rps`
  (forecast minus realised; positive = over-forecast) of the window peak, `predictive_autoscaler_forecast_step_mae_rps`
  at the first, middle and last target, and `predictive_autoscaler_forecast_window_pinball_loss_rps`.
- **Participation:** `predictive_autoscaler_forecast_issued_total`, `predictive_autoscaler_forecast_outcomes_total`
  (`covered`, `missed`, `unscorable`), `predictive_autoscaler_forecast_scored_windows`,
  `predictive_autoscaler_forecast_outcome_availability_ratio` (scored share of the last day's due windows, overdue
  ones included), `predictive_autoscaler_forecast_prediction_availability_ratio` (decision origins of the last day
  that got a forecast, as the trust gate reads it) with its numerator
  `predictive_autoscaler_forecast_prediction_issued_windows` and denominator
  `predictive_autoscaler_forecast_prediction_expected_windows` (a denominator of 0 means the ratio is unknown, and
  the ratio then has no series),
  `predictive_autoscaler_forecast_calibration_windows`, `predictive_autoscaler_forecast_history_seconds`,
  `predictive_autoscaler_forecast_evidence_age_seconds` and `predictive_autoscaler_forecast_fit_failures_total`.
- **Trust:** `predictive_autoscaler_forecast_trusted` (1 when that model's evidence gates pass) and
  `predictive_autoscaler_forecast_trust_reason{reason}` (one series with value 1: the gate's reason for the model
  when its latest committed forecast was issued, for example `CalibrationInsufficient` or `Ready`). It is the reason
  at issuance, not a statement that the forecast may act now: for that, read the operator's `PredictionReady`
  condition and `predictive_autoscaler_prediction_used_family`.
- **The latest committed forecast** (the newest one in the journal, whichever request finished last), shown for
  every model in it, trusted or not, so a forecast can be watched before it may act: `predictive_autoscaler_forecast_base_window_rps` (the point maximum over the window) and
  `predictive_autoscaler_forecast_upper_window_rps` (the upper bound; no series while the model has none, a bound is
  never made up), with `predictive_autoscaler_forecast_latest_issued_timestamp_seconds`,
  `predictive_autoscaler_forecast_latest_window_start_timestamp_seconds` and
  `predictive_autoscaler_forecast_latest_window_end_timestamp_seconds`. The two values are about that future window:
  compare them with the request rate measured in the window, not with the rate at the time of issuance.

A model is trusted only on **prospective** evidence (forecasts issued live before their outcome; offline replays warm up
calibration but never count, not even in availability or error figures): at least 48 hours and 64 non-overlapping
windows with an issued bound, 7-day coverage at least the target minus 5 points, no completed UTC day with far more
misses than the target allows (the day in progress is never judged), no long run of misses in a row and not too many
misses among the last 64 windows, enough calibration history, fresh input, the history minimum (1 day for seasonal-naive, 7 days for the statistical
model), and over the last day at least 95 % of the due windows scored and 95 % of the forecast opportunities issued. A
window whose outcome is overdue (its data deadline passed without one) counts as not scored, and an origin without a
forecast counts as a missed opportunity. Evidence belongs to one model version (recipe): a new recipe starts again. A
trusted model that fails a gate is demoted at once; it is trusted again only after its gates have held for 24 hours and
32 new windows. A new window result is one look of these tests; asking again without a new result changes nothing.

*How the miss limits are set, and what that does and does not show.* The four coverage rules (misses among the last 64
windows, misses in a row, a completed day, the last 7 days) are not fixed percentages. Each threshold is computed
under one stated model of the misses: they occur at the target rate, and a miss follows a miss with probability 0.175
(a two-state Markov chain, started in its steady state; with the default 90 % target an independent sequence would
have 0.10). Under that model the four rules together demote a correctly calibrated forecast with probability at most
5 % in 7 days, shared by the enabled families. This is a computed property of the model, not a promise for every
workload: real misses can cluster differently, and nothing is claimed for them beyond what was measured.

*Validated configurations.* The rules are qualified by a simulation job that runs the complete monitor
(`ml-engine/evaluation/trust_qualification.py`), with acceptance rules frozen beforehand: at most 5 % false coverage
demotions in 7 days, and a bound that misses 40 % of the time detected within 64 windows in at least 95 % of cases;
on independent misses, on noise without serial dependence and on strongly autocorrelated noise (AR(1), coefficient
0.7 per minute). The allowlist holds **one configuration**: a 60-second grid (`sampleIntervalSeconds: 60`), a
60-second update interval (`updateIntervalSeconds: 60`; set it, the CRD's default is 300), a
600-second lead (`minLeadSeconds: 600`), the default 90 % target and `families: [seasonalNaive]`, with the default
calibration, trust and timing settings. Only a configuration on the allowlist can become `PredictionReady`. Every other configuration is forecast, calibrated,
scored and reported in exactly the same way, and stays reactive with the reason `ConfigurationNotValidated`: at shorter
leads the same rules false-alarmed too often on autocorrelated noise or detected too slowly, and no other lead, grid,
target or family set has been validated. A second model family needs its own evidence. At the validated
configuration the limits are 18 misses among the last 64 windows, 7 in a row, 29 in a completed day of 144 windows,
and for the last 7 days a coverage below 85 % together with a miss count a correctly calibrated forecast would
almost never reach (153 of 1,008 windows in a full week; 59 of 288 after two days).

*What it costs.* The limits react to clear miscalibration, not to a mild one. At a 600-second lead 64 windows are
about 10 hours 40 minutes, so even a bound that misses 40 % of the time can stay trusted for hours; a bound that
misses every time is demoted after at most 7 windows (about 70 minutes). A moderate sustained undercoverage (for
example 15 to 20 % misses instead of 10 %) is caught slowly or not at all, because a week of windows at a long lead
cannot tell it from noise at this false-alarm budget. The statistical model is used only when it is trusted **and** has beaten a trusted seasonal-naive
forecast on the same windows (spanning at least 48 hours) for 24 hours in a row; it is then kept until it does worse
than the seasonal-naive forecast, so small differences do not make the choice flip back and forth, but any safety
failure falls back at once. Otherwise the seasonal-naive forecast is used when trusted, and otherwise scaling stays
reactive. Time windows are counted in whole hours ("the last day" starts at the first whole hour inside it, so it can be
up to an hour shorter). "Ready" here means ready to activate: Recommend mode still makes no writes, and coverage of
forecast windows is not a latency or error-rate guarantee.

**What the operator does with it** (`spec.prediction.engine: statistical`). It never takes the service's word for a
write. It publishes the lead it needs: a startup budget (pod creation to Ready of the current ReplicaSet's pods once 20
starts are measured, otherwise `spec.behavior.startup.fallbackDelaySeconds`), plus a decision interval and an allowance
that also covers the time from the scale request to the pod's creation; and never less than
`spec.prediction.minLeadSeconds` when that is set (rounded up to the grid; the measured startup delay is not changed by
it). A pod that has never been Ready and is already
older than the budget raises it; a pod that was Ready and failed a readiness probe, an evicted pod and a terminating pod
do not. The lead never gets shorter by itself, and a start too slow for the forecast horizon switches the forecast off
(`LeadInsufficient`) while reactive scaling continues; changing `spec.behavior.startup` starts again from the current
budget. The forecast covers the window from the next grid step to the lead: a longer lead widens that window, it
does not mean scaling exactly that long before a rise. It asks for the window starting at the current
grid time and checks the answer itself: identity, settings, window, an upper bound backed by enough evidence, and enough
time left in the window, checked again right before the write. Only then does the bound count:
`ceil(bound / capacityPerReplica)` replicas, but never more than `spec.prediction.predictiveExtra` above the reactive need
(2 replicas or 50 % by default). Everything else stays as described here: the reactive need, the hold on missing
telemetry, headroom, stabilization and every write check; Recommend never writes. `kubectl get pa -o yaml` shows it in
`status.prediction`; `PredictionReady` says whether it would be used; the decision ledger links each decision to the
forecast's `issuance_id`. The operator exports `predictive_autoscaler_predictive_extra_replicas`,
`predictive_autoscaler_predictive_capped` and `predictive_autoscaler_prediction_gate`. A scale-out that cannot
complete (unschedulable pods, an exceeded quota) shows as `ScaleOutBlocked` on every autoscaler. Details:
[configuration.md](configuration.md#forecast-contract-v2-engine-statistical-preview).

### The Help & Overview dashboard

Set `dashboards.enabled: true` and the chart ships the Grafana dashboard **"Predictive Autoscaler — Help & Overview
(<namespace>/<release>)"** as a ConfigMap labelled `grafana_dashboard: "1"`, which the Grafana dashboard sidecar loads.
Its uid and file name are derived from the release's namespace and name, so several installations can share one
Grafana without replacing each other's dashboard. Add labels for your sidecar with `dashboards.labels` and, for
example, a folder annotation with `dashboards.annotations`.

The sidecar must be able to see the ConfigMap: by default the Grafana sidecar reads only its own namespace, so either
install the chart there or configure the sidecar to search the chart's namespace or all namespaces (in the Grafana
Helm chart: `sidecar.dashboards.searchNamespace`), which needs RBAC to list and watch ConfigMaps in those namespaces.
The sidecar's label must match `dashboards.labels`.

At the top, choose the data source, then the installation: **Installed in** (`pa_install_namespace`, the chart's
namespace, added by the chart's scrape objects) and **Installation** (the release's full name, from the scrape `job`).
The chart pre-selects its own installation. Every query is limited to that installation; then filter by namespace,
autoscaler and application. Its first panel is a short version of this
guide; the others show the conditions table, forecasting switch, measured vs forecast request rate, replicas, scale
events, prediction error, model age and model in use, forecast freshness, guard rails and the scrape targets. The
repository's `grafana/dashboards/` holds further, more detailed research dashboards.

### Alerts

Enable `metrics.alerts.prometheusRule.enabled` or `metrics.alerts.vmRule.enabled` (they need the matching scrape object).
Each rule watches only its own installation, and every alert carries `pa_release` and `pa_release_namespace`;
`metrics.alerts.labels` adds your routing labels. Thresholds are values (`notReadyFor`, `telemetryFor`, `forecastFor`,
`conflictFor`, `modelStaleHours`, `absentFor`).

| Alert | Severity | Means | What to do |
|---|---|---|---|
| `PredictiveAutoscalerNotReady` | warning | `Ready` has not been True for 15 minutes: reconciles fail. | `kubectl describe pa`: fix the reason (`DeploymentNotFound`, `InvalidMetricQuery`, `CrossNamespaceTarget`, `ScalingError`) and read the operator log. |
| `PredictiveAutoscalerTelemetryUnavailable` | warning | The request rate has been unmeasurable for 10 minutes; the replica count is held. | Check the metrics store and run `status.metricSource.query` yourself: it must return exactly one finite series. Fix the scrape or the query. |
| `PredictiveAutoscalerForecastUnavailable` | info | No forecast used for 2 hours. Never fires when forecasting is switched off. Label `fallback=reactive`: decisions use the measured rate. `fallback=capacity_hold`: no per-replica capacity is configured, so no forecast is requested and the replica count is held. | `reactive`: expected until the first successful training; otherwise read the `ForecastAvailable` reason, the training CronJob's last job and log, and the forecasting service log. `capacity_hold`: set `capacityPerReplica` ([section 5](#5-create-your-first-autoscaler-in-recommend-mode)). |
| `PredictiveAutoscalerConflict` | warning | Another replica writer has targeted the Deployment for 5 minutes; nothing is written. | Remove or pause the writer in `status.conflicts`, or switch this autoscaler to Recommend. |
| `PredictiveAutoscalerModelStale` | warning | Forecasts in the last hour came from a model whose training data ends more than 26 hours ago. | Check the training CronJob (schedule, failed jobs, logs, `activeDeadlineSeconds`); train by hand ([section 7](#7-get-forecasts)). |
| `PredictiveAutoscalerOperatorDown` | critical | No operator scrape target has been up for 10 minutes. | Check the operator pod, its events and log. An unready operator after an upgrade usually means an old CRD ([section 13](#13-upgrading-and-the-api-group-migration)). Also check the scrape object and NetworkPolicy `metricsFrom`. |
| `PredictiveAutoscalerForecasterDown` | warning | No forecasting-service scrape target has been up for 10 minutes. | Check the forecasting service pod, its log and the model volume. Decisions are reactive meanwhile. |
| `PredictiveAutoscalerForecastJournalUnhealthy` | warning | Engine `statistical` only: the forecasting service's journal has been unwritable or untrustworthy for 15 minutes, although the service retries by itself. It answers 503 and is not ready. | Read the reason on the forecasting service's `/ready` (or its log). A full quota or state volume: enlarge it, or wait for retention to free expired windows; a corrupt journal: move the file away and the service starts an empty one ([helm.md](helm.md#statistical-forecaster-profile-preview)). Decisions are reactive meanwhile. |

Reference: [helm.md, Monitoring](helm.md#monitoring).

## 12. Security

- **Operator → forecasting service:** on by default. The operator sends a short-lived projected service account token
  over TLS (port 8443); the service checks it with a Kubernetes TokenReview and accepts only the operator's service
  account. Another caller gets 401 or 403; if TokenReview cannot answer, the service returns 503 rather than letting
  the request through. Only `/health`, `/ready` and `/metrics` are open. The matching `ForecastAvailable` reasons are
  `Unauthorized`, `Forbidden`, `AuthUnavailable` (TokenReview could not judge) and `AuthMisconfigured` (the operator's
  token or CA file is missing, empty or invalid, or a token is configured with a non-https service URL).
- **Certificates** (`forecaster.tls.source`): `selfSigned` for plain Helm (not with Argo CD), `certManager` for Argo
  CD and long-running installs, or `existingSecret` (`tls.crt`, `tls.key`, `ca.crt`). A renewed certificate reaches
  the pods through the mounted Secret (the kubelet's periodic sync); the forecasting service checks its files every 60
  s and, when they changed, exits so that the kubelet restarts its container with the new files, and the operator
  reloads the CA. Expect a delay of the Secret sync plus up to a minute, and a short restart, not an instant reload;
  meanwhile the operator uses a recent cached forecast or the reactive rule.
- **NetworkPolicies** (on by default, need an enforcing CNI): only the operator may call the forecasting service, the
  trainer accepts no connections, and all three may reach only DNS, the Kubernetes API and your metrics store.
  **With `networkPolicy.kubeAPI.cidrs` empty (the default) the "Kubernetes API" rule allows any destination on the
  configured API ports (443 and 6443)**; set it to your API server's addresses.
- **Permissions:** each part has its own service account. The operator reads autoscalers, Deployments and the other
  scalers, and writes only `deployments/scale`, its status, events and its leader-election Lease; with
  `operator.watchNamespaces` set, those writes are granted only in the listed namespaces (its own namespace keeps just the Lease and its events). The forecasting
  service and the trainer only read autoscalers and Deployments (plus TokenReviews for the service). No role can read
  Secrets or create Pods or Jobs. Every container runs non-root with a read-only root filesystem and no capabilities.
- The plain manifests in `k8s-manifests/` do not have these protections; use the chart.

Details: [helm.md](helm.md) (What it installs; Authentication).

## 13. Upgrading and the API-group migration

**Chart upgrades.** Helm never upgrades a CRD. Before `helm upgrade` to a release with a newer CRD, apply it:

```sh
kubectl --kubeconfig <file> --context <context> apply --server-side --force-conflicts \
  -f charts/predictive-autoscaler/crds/
```

The operator checks the CRD's revision label at start. If the CRD is older than it needs, it starts no controller:
the pod stays unready with the reason in its log. After an upgrade, check whether models must be retrained (a version
that changes model provenance refuses older models).

**The hidden capacity defaults are gone (CRD revision 3).** Earlier versions assumed 20,000 requests per minute per pod
for the reactive rule and 30,000 for the forecast when `targetRPS` was unset. Now an autoscaler without a capacity holds
its current count and reports `CapacityConfigured=False`. Before upgrading, list the autoscalers without one, and set
`capacityPerReplica` on each. Keep `targetRPS` where it is set until the new operator runs and is verified, and never
hand a capacity-only object back to an older Active operator
([upgrading.md](upgrading.md#capacityperreplica-replaces-the-hidden-defaults), with the rollback steps).

**Forecast contract v2 (CRD revision 4).** Revision 4 adds the v2 settings under `spec.prediction`,
`spec.behavior.startup` and `status.prediction`. Nothing changes for an autoscaler without `engine: statistical`. To try
the statistical engine, set `spec.behavior.startup.fallbackDelaySeconds` too (or wait for 20 measured starts): without a
startup budget there is no lead, and decisions stay reactive.

**Forecast families (CRD revision 5).** `spec.prediction.families` restricts the statistical forecaster's candidates;
`[seasonalNaive]` never forecasts or selects E1-product. Apply the CRD before the operator
([upgrading.md](upgrading.md#forecast-families-crd-revision-5)).

**From the legacy API group.** Before v0.1.0 the group was `autoscaler.example.com`; it is now
`autoscaling.devkuban.com`. Objects are not converted; each one is re-created in the new group, in steps that never
let two operators scale one Deployment: install the new version side by side, then `hack/migrate-api-group.py` `plan`,
`prepare` (replacements in Recommend mode), train, `verify`, `cutover` (stops the legacy operators, deletes the legacy
autoscalers), and `activate` one autoscaler at a time. Full procedure: [upgrading.md](upgrading.md).

## 14. Uninstalling safely

1. Switch every autoscaler to `mode: Recommend` (or delete them). Their Deployments keep their replica counts.
2. `helm uninstall pa -n predictive-autoscaler` with an explicit kubeconfig and context.
3. What stays on purpose: the CRD and every PredictiveAutoscaler. The chart-created model claim stays only with
   `forecaster.persistence.retain: true` (the default); with `retain: false` it is deleted with the release, and a claim
   you brought with `existingClaim` is never the chart's to delete. Delete your autoscaler objects when you no longer
   need them, and a retained model claim only if you mean to lose the models.
4. **Do not delete the CRD as routine.** Deleting it deletes every PredictiveAutoscaler in the cluster.

With Argo CD the CRD (and, with `retain: true`, the model claim) are also protected from pruning ([helm.md](helm.md#argo-cd)).

## 15. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Ready=False`, `CrossNamespaceTarget` | `targetDeployment.namespace` differs from the autoscaler's namespace. | Create the autoscaler in the Deployment's namespace. |
| `Ready=False`, `DeploymentNotFound` | Wrong Deployment name, or it does not exist yet. | Fix `targetDeployment.name`. |
| `Ready=False`, `InvalidMetricQuery` | The query uses template features other than `{{ .Namespace }}` / `{{ .Name }}`, or is too long. | Rewrite the query ([configuration.md](configuration.md)). |
| `TelemetryAvailable=False`, `MetricsUnavailable`; replicas do not change | The query returns no series, several series or an error; or the metrics store is down. | Run `status.metricSource.query` against your metrics store; aggregate to one series; check label names (`namespace` vs `exported_namespace`) and that a Service name differs from the Deployment name. |
| Calculated is empty, `CapacityConfigured=False` (`CapacityMissing`) | Neither `capacityPerReplica` nor `targetRPS` is set. The operator calculates nothing and holds the current count; it no longer assumes a capacity. | Measure the capacity ([how](configuration.md#measuring-capacitypereplica)) and set `metrics.requests.capacityPerReplica`. |
| `CapacityConfigured=True`, `DeprecatedTargetRPS` | The capacity comes from the deprecated `targetRPS`. | Replace `targetRPS: N` with `capacityPerReplica: "N"` in one change (setting both is rejected). |
| Calculated is always `minReplicas` | `capacityPerReplica` far too high, or the query counts another workload's traffic or is in the wrong unit (it must be per second). | Check `predictive_autoscaler_current_rpm` against what you expect (requests per minute = 60 × per second). |
| `ForecastAvailable=False`, `Unavailable` on a new autoscaler | No model yet: not enough history or no training. | Expected. List it in `training.targets`; wait until the history passes the trainer's preflight (by default at least 235 usable 10-minute points); train ([section 7](#7-get-forecasts)). |
| `ForecastAvailable=False`, `Unavailable` after it worked | The query, the autoscaler or the Deployment changed (model refused), or the forecasting service is down. | Retrain after a change; check the forecasting service pod and log. |
| Training job fails or publishes nothing | Too little history, the deadline (`training.activeDeadlineSeconds`) or memory reached, or the model volume lacks `flock` support. | Read the job's log; raise the limits; use a storage class with `flock`. |
| `ForecastAvailable=False`, `Unauthorized` / `Forbidden` | The caller is not the operator's service account, or the audience differs. | Use the chart's defaults; do not change `forecaster.auth.audience` on one side only. |
| `ForecastAvailable=False`, `AuthUnavailable` | The service cannot reach TokenReview (Kubernetes API egress blocked or slow). | Check `networkPolicy.kubeAPI` and the forecasting service's role. |
| `ForecastAvailable=False`, `AuthMisconfigured` | The operator's projected token or CA file is missing, empty or invalid, or the service URL is not https. | Re-install with the chart; check the operator pod's volumes. |
| `ForecastAvailable=False`, `SanityRejected` | The forecast's first step was more than ten times the measured rate. | Usually a model trained on different traffic: retrain. |
| `ForecastAvailable=False`, `HorizonElapsed` | The cached forecast's steps have all passed and no new one arrived. | Check the forecasting service. |
| `ScalingActive=False`, `RecommendMode` | The autoscaler is in Recommend mode. | Set `spec.mode: Active` when ready ([section 8](#8-switch-to-active-and-how-to-stop)). |
| `ScalingActive=False`, `Conflict` | An HPA, an unpaused KEDA ScaledObject or another Active autoscaler targets the Deployment. | Remove or pause it (see `status.conflicts`). |
| `ScalingActive=False`, `CheckFailed` | The coexistence check failed: permissions, API discovery, a timeout. | Read the condition message and the operator log; check the operator's RBAC. |
| `ScalingActive=False`, `TargetNotAllowed` | The install limits replica writes to reviewed Deployments (`operator.writeAllowlist`), and this target is not listed or was deleted and created again (its UID changed). The message says which. | Decisions are still computed and published; nothing is written. Review the Deployment, then set its current UID in `operator.writeAllowlist` and upgrade the release. See [helm.md](helm.md). |
| `ScalingActive=False`, `GuardAborted` now and then | The autoscaler or the Deployment changed between the decision and the write. | Normal; a fresh reconcile follows within seconds. Frequent aborts mean another writer is changing the replica count. |
| `ScalingActive=False`, `CapacityMissing` | No per-replica capacity is configured, so the count is held. | Set `capacityPerReplica` (see above). |
| `ScalingActive=False`, `TelemetryHold` | The request rate cannot be measured, so the count is held. | Fix the telemetry (see above). |
| `ScalingActive=False`, `BoundsEnforced` | Nothing could be measured or calculated, and the replica count was outside `[minReplicas, maxReplicas]`: it was raised to `minReplicas`, or is being lowered to `maxReplicas` through the scale-down holds. | Fix the telemetry or the capacity; the message says whether a write happened. |
| `ScalingActive=False`, `TargetPaused`; the Deployment stays at 0 | The Deployment has 0 desired replicas, so the operator leaves it alone. | Set a positive replica count to resume ([Pausing a target](#pausing-a-target)). |
| Replicas flip back and forth | Another writer (GitOps `replicas`, a CI job, `kubectl scale`) that the operator cannot detect. | Remove `replicas` from the managed manifest or ignore the field in GitOps. |
| No scale-down although traffic dropped | Stabilization, cooldown, the headroom floor (`ScaleDownHeadroom=True`), or a forecast that still asks for more pods. | Wait for the windows; compare `forecastReplicas`; see [section 9](#9-scale-down-behaviour-and-headroom). |
| Operator pod unready after an upgrade | The CRD is older than the operator needs (revision gate). | Apply the new CRD ([section 13](#13-upgrading-and-the-api-group-migration)). |
| `VPAInterference=True` | A VPA changes the pods' requests. | Use VPA mode `Off`, or measure `capacityPerReplica` again with it. |
| Dashboard panels are empty | No scrape object, NetworkPolicy blocks the scraper, or the wrong data source. | Enable `metrics.serviceMonitor`/`vmServiceScrape`, set `networkPolicy.metricsFrom`, pick the data source variable. |

## 16. Limitations

The most important ones: during a metrics outage the replica count is held, so load changes are not followed; only
known scalers are detected; only one request-rate series per workload is used (no CPU or memory); one model per
autoscaler, a single forecasting replica on a ReadWriteOnce volume; the operator's scale-down timers and forecast cache
live in memory; some CRD fields are accepted but ignored; the forecasting image is heavy; and there are no published
releases yet. The full, ordered list: [limitations.md](limitations.md).

## 17. Glossary

| Term | Meaning |
|---|---|
| **PredictiveAutoscaler** (`pa`) | The custom resource you create per Deployment (`autoscaling.devkuban.com/v1alpha1`). |
| **Recommend mode** | Computes and publishes decisions, writes nothing. The default. |
| **Active mode** | Writes the replica count to the Deployment's `/scale`. |
| **Reconcile** | One evaluation of an autoscaler, every `updateIntervalSeconds`. |
| **Reactive rule** | `ceil(measured rate / capacity per replica)`: the decision without a forecast. |
| **Calculated replicas** | The decision: the higher of reactive and forecast, within `[minReplicas, maxReplicas]`. |
| **Lead window** | The next `leadTimeMinutes`; the operator sizes for the highest forecast inside it. |
| **Horizon** | How far ahead the forecast reaches (`horizonMinutes`, six 10-minute steps for one hour). |
| **capacityPerReplica** | Requests per second one ready replica serves within its SLO, measured with a load test. The reactive rule, the forecast rule and the headroom floor all use it; there is no default. Internally the operator works in requests per minute (RPM). |
| **targetRPS** | The deprecated integer form of `capacityPerReplica`. |
| **Telemetry hold** | Keeping the current replica count because the request rate cannot be measured. |
| **Headroom floor** | The lowest count a scale-down may reach while keeping `headroomPercent` spare capacity. |
| **Stabilization / cooldown** | The waits before and between scale-downs. |
| **Overestimate cap / override** | Limits a forecast that has stayed well above the measured need. |
| **Sanity rejection** | Discarding a forecast whose first step exceeds ten times the measured rate. |
| **Training target** | The autoscaler a training CronJob trains for (`training.targets`). |
| **Training cutoff** | The time of the last observation a model was trained on. |
| **Provenance** | What a model and a forecast are bound to: the autoscaler, its target, their UIDs and the query hash. |
| **Replica writer** | Anything that sets a Deployment's replica count: HPA, KEDA, another autoscaler, GitOps, a person. |
| **Forecast ledger** | The optional append-only JSON-lines log of every forecast attempt and decision. |
| **CRD revision gate** | The operator's start-up check that the installed CRD is new enough. |
