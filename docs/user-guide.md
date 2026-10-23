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

Use it for a stateless Deployment whose traffic follows a daily pattern and whose pods take long enough to start that
scaling after the rise is too late. It is less useful for traffic without a repeating pattern, and it needs about a
week of request history per workload before forecasts start. Only the request rate is used; CPU and memory are not.

Every autoscaler starts in **Recommend** mode: it computes and publishes what it would do and changes nothing. You
switch it to **Active** when you trust its recommendations.

## 2. The parts

| Part | What it does |
|---|---|
| **Operator** (Go) | Watches `PredictiveAutoscaler` objects. On each reconcile it measures the request rate, asks the forecasting service for a forecast, decides a replica count, publishes it in the status and metrics and, in Active mode only, writes it to the Deployment's `/scale`. |
| **Forecasting service** (`ml-api`, Python) | Serves a forecast for the next hour in six 10-minute steps for one autoscaler. It reads the autoscaler and its Deployment from the Kubernetes API, reads the history itself with the autoscaler's compiled query, and refuses when its input or model does not fit. |
| **Trainer** (one CronJob per autoscaler) | Every six hours (your schedule) trains a model for one autoscaler from the last week of request rate and publishes it to the model volume; the service picks it up. |
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

Requirements: Kubernetes 1.33 or later, Helm, a default storage class (ReadWriteOnce with `flock` support), and a
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
| `forecaster.persistence.*` | The model volume: size, storage class, access mode (`ReadWriteOnce` or `ReadWriteMany`, never `ReadWriteOncePod`), `retain` (kept on uninstall by default), or your own `existingClaim`. |
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
      targetRPS: 100                                  # requests per second ONE pod should handle
      source:
        preset: prometheus
        query: 'sum(rate(http_requests_total{namespace="{{ .Namespace }}",service="{{ .Name }}"}[2m]))'
  prediction: {enabled: true, leadTimeMinutes: 15, horizonMinutes: 60, updateIntervalSeconds: 60}
```

The fields that matter:

- **`mode`**: `Recommend` (the default, also when absent) or `Active`.
- **`minReplicas` / `maxReplicas`**: the bounds of every decision.
- **`metrics.requests.targetRPS`**: the requests per second one pod can serve at acceptable latency. Measure it with a
  load test; the number above is only an example.
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

Within one reconcile you should see `Ready=True` and `TelemetryAvailable=True` for the current generation, and a
calculated replica count. The Deployment does not change.

## 6. Read its status

### `kubectl get pa`

| Column | Meaning |
|---|---|
| Target | The Deployment it scales (`spec.targetDeployment.name`). |
| Min, Max | `spec.minReplicas` and `spec.maxReplicas`. |
| Mode | The mode applied in the last reconcile (`status.mode`). |
| Current | The Deployment's replica count (`status.currentReplicas`). |
| Calculated | The decision: what the operator would set now (`status.calculatedReplicas`), or the held count while the request rate cannot be measured. |
| Applied | The last count the operator actually wrote (Active mode only; empty in Recommend). |
| Ready | The Deployment's **ready replicas** (`status.readyReplicas`), not the `Ready` condition. |
| Age | Since the autoscaler was created. |

### More in `kubectl get pa <name> -o yaml`

- `observedGeneration`: the spec generation the status describes. Trust a condition only when it matches
  `metadata.generation`.
- `forecastReplicas`: the replica count from the forecast alone (absent when no forecast was used).
- `stabilizedReplicas`: Active mode only, what will be applied after the scale-down rules.
- `appliedReplicas`, `lastScaleTime`: the last write. `lastPrediction`: when the forecast in use was issued.
- `metricSource`: the compiled query (`query`, `sha256`, `observedGeneration`, `targetUID`, `contract`). Run this query
  yourself when the telemetry looks wrong.
- `conflicts`: other replica writers found on the target (`group`, `kind`, `namespace`, `name`, `reason`).
- `targetUID`: the Deployment the status describes; a replaced Deployment clears the write history.
- `predictedReplicas`: deprecated; use `calculatedReplicas` and `forecastReplicas`.

### Conditions and reasons

| Condition | True means | Reasons |
|---|---|---|
| `Ready` | The last reconcile completed. | `ReconcileCompleted` (True). When False, the reason names the failure: `CrossNamespaceTarget` (the target is in another namespace; nothing is written), `DeploymentNotFound`, `InvalidMetricQuery` (the query breaks the template rules; nothing is written), `ScalingError` (the write to `/scale` failed). |
| `TelemetryAvailable` | The request rate was measured. | `Measured`; `MetricsUnavailable` when the query failed, returned no series, several series, a partial answer or a non-finite value. The replica count is then held. |
| `ForecastAvailable` | A forecast took part in the decision. | `Used`; `Disabled` (`spec.prediction.enabled: false`); `Unavailable` (no model yet, a refusal or an outage: see [section 7](#7-get-forecasts)); `HorizonElapsed` (every step of the cached forecast has passed); `SanityRejected` (its first step exceeded ten times the measured rate, so it was discarded); and the authentication failures `Unauthorized`, `Forbidden`, `AuthUnavailable`, `AuthMisconfigured` ([section 12](#12-security)). |
| `ScalingActive` | The operator is writing the replica count. | `Active`; `RecommendMode`; `TelemetryHold` (holding because the rate is unavailable); `Conflict` (another writer); `CheckFailed` (the coexistence check failed); `GuardAborted` (a last-moment check stopped a write; it retries within seconds); or the failure reason from `Ready`. |
| `ConflictDetected` | Another replica writer targets the Deployment. | `NoConflict` (False); `ReplicaWriter` (True: an HPA, an unpaused KEDA ScaledObject, another Active PredictiveAutoscaler or a legacy-group autoscaler); `CheckFailed` (status `Unknown`: the check could not finish, which blocks writes like a conflict). |
| `VPAInterference` | A VerticalPodAutoscaler changes the pods' requests. | `None` (False); `VPAUpdatesPods` (True: a VPA in any mode except `Off`; scaling continues, but per-pod capacity may drift); `CheckFailed` (status `Unknown`). |
| `ScaleDownHeadroom` | A scale-down is held to keep headroom. | `Holding` (True); `NotHolding` (headroom is not blocking a scale-down; it says nothing about how much spare capacity exists); `RecommendMode`. Informational: never affects `Ready` or `ScalingActive`. |

### Events

`kubectl describe pa <name>` shows events. They are emitted when a condition changes, not on every reconcile:
`ScaledUp` and `ScaledDown` (each applied change); `TelemetryUnavailable` / `TelemetryRestored`;
`ForecastUnavailable` / `ForecastRestored`; `ScalingInactive` / `ScalingActive`; `ReconcileFailed` /
`ReconcileRecovered`; `RecommendMode` and `Disabled` (configured states, type Normal); and for coexistence
`ConflictDetected`, `ConflictResolved`, `ConflictCheckFailed`, `VPAInterference` and `VPAInterferenceCleared`.

## 7. Get forecasts

**What a forecast needs.**
1. A **training target**: the autoscaler must be listed in the chart's `training.targets`. The chart creates one
   CronJob per entry; its annotation `autoscaling.devkuban.com/training-target` names the autoscaler.
2. **History**: roughly a week of usable request-rate history for the autoscaler's query, plus a complete recent input
   window. With too little history a training publishes nothing, and decisions stay reactive.
3. A **successful training**. The chart has no automatic first training (no cold start). To train now instead of
   waiting for the schedule:

```sh
kubectl -n predictive-autoscaler get cronjobs \
  -o custom-columns='NAME:.metadata.name,TARGET:.metadata.annotations.autoscaling\.devkuban\.com/training-target'
kubectl -n predictive-autoscaler create job first-training --from=cronjob/<matching-cronjob>
kubectl -n predictive-autoscaler wait --for=condition=complete job/first-training --timeout=30m
kubectl -n predictive-autoscaler logs job/first-training     # ends with "status": "success" and an artifact_sha256
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
series, or an incomplete input window. A refusal makes the decision reactive at once. An outage (HTTP 503: the
Kubernetes API or the metrics store unreachable, or the service at capacity) or an unreachable service lets the
operator keep using its last forecast while it is less than ten minutes old, and then it falls back to the reactive
rule. Either way `ForecastAvailable` is False with reason `Unavailable`, and nothing breaks: the autoscaler simply
behaves like a reactive one.

## 8. Switch to Active, and how to stop

Before you switch:
- compare the recommendations (`Calculated`, `forecastReplicas`) with what you expect for a while;
- remove or pause every other replica writer: HPAs, KEDA ScaledObjects (`autoscaling.keda.sh/paused: "true"`), and
  GitOps applying a fixed `replicas` ([section 10](#10-running-next-to-hpa-keda-vpa-and-gitops)).

```sh
kubectl -n shop patch pa web-pa --type merge -p '{"spec":{"mode":"Active"}}'
kubectl -n shop get pa/web-pa deploy/web       # Applied follows Calculated; check the Deployment's ready replicas
```

`ScalingActive=True` (reason `Active`) confirms the operator writes. It writes only the replica count, only through
the Deployment's `/scale` subresource, and only if the count is still the one its decision was based on. Scale-ups are
applied at once; scale-downs follow the rules in [section 9](#9-scale-down-behaviour-and-headroom). After a switch to
Active (or an operator restart) scale-down waits at least ten minutes.

**To stop**, set the mode back:

```sh
kubectl -n shop patch pa web-pa --type merge -p '{"spec":{"mode":"Recommend"}}'
```

Writes stop at the next reconcile and the Deployment keeps its current replica count. `ScalingActive` shows
`RecommendMode`. Deleting the autoscaler also stops scaling and leaves the replica count as it is (the operator uses no
finalizer). To hand over to KEDA, follow the [paused-KEDA fallback pattern](coexistence.md#the-paused-keda-fallback-pattern).

## 9. Scale-down behaviour and headroom

Scaling down is deliberately slow and bounded; scaling up is immediate.

- **Headroom floor:** a scale-down never goes below the count that leaves `behavior.scaleDown.headroomPercent` (default
  10 %) spare capacity at the measured rate. Example: at 798 requests per minute and 2 requests/s (120 rpm) per pod,
  seven pods would run at 95 % of capacity, so eight are kept. While the floor holds, `ScaleDownHeadroom=True`
  (`Holding`). The floor uses the measured rate only, never the forecast. `0` switches it off.
- **Stabilization:** the decision must stay below the current count **continuously for 5 minutes**, and no scale-up may
  have happened in the last 5 minutes.
- **Cooldown:** at least 2 minutes between two scale-downs.
- **Step size:** each step removes at most `max(ceil(10 % of current), 2)` pods.
- **Overestimate override:** when the overestimate cap is active, the stabilization checks are skipped, but the
  cooldown and the headroom floor still apply.
- **After a restart, a leadership change, a switch to Active or a re-created Deployment**, the operator behaves as if
  it had just scaled: scale-down waits at least 10 minutes. This state lives in memory.

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
- **VPA:** allowed, but a VPA that changes requests (any mode except `Off`) changes the per-pod capacity `targetRPS`
  assumes; the operator warns with `VPAInterference=True`. Tune the two together, or use `Off`.
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
  `predictive_autoscaler_predicted_replicas` (the forecast's count; 0 while no forecast is used).
- **Prediction error:** `predictive_autoscaler_prediction_error_percent`, the forecast's replica count against the
  measured need, in percent. It reads 100 while no forecast is used.
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

### The Help & Overview dashboard

Set `dashboards.enabled: true` and the chart ships the Grafana dashboard **"Predictive Autoscaler — Help & Overview"**
as a ConfigMap labelled `grafana_dashboard: "1"`, which the Grafana dashboard sidecar loads. Add labels for your
sidecar with `dashboards.labels` and, for example, a folder annotation with `dashboards.annotations`. Choose your
Prometheus data source, namespace, autoscaler and application at the top. Its first panel is a short version of this
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
| `PredictiveAutoscalerForecastUnavailable` | info | No forecast used for 2 hours. Never fires when forecasting is switched off. | Expected until the first successful training. Otherwise read the `ForecastAvailable` reason, the training CronJob's last job and log, and the forecasting service log. |
| `PredictiveAutoscalerConflict` | warning | Another replica writer has targeted the Deployment for 5 minutes; nothing is written. | Remove or pause the writer in `status.conflicts`, or switch this autoscaler to Recommend. |
| `PredictiveAutoscalerModelStale` | warning | Forecasts in the last hour came from a model whose training data ends more than 26 hours ago. | Check the training CronJob (schedule, failed jobs, logs, `activeDeadlineSeconds`); train by hand with `kubectl create job --from=cronjob/...`. |
| `PredictiveAutoscalerOperatorDown` | critical | No operator scrape target has been up for 10 minutes. | Check the operator pod, its events and log. An unready operator after an upgrade usually means an old CRD ([section 13](#13-upgrading-and-the-api-group-migration)). Also check the scrape object and NetworkPolicy `metricsFrom`. |
| `PredictiveAutoscalerForecasterDown` | warning | No forecasting-service scrape target has been up for 10 minutes. | Check the forecasting service pod, its log and the model volume. Decisions are reactive meanwhile. |

Reference: [helm.md, Monitoring](helm.md#monitoring).

## 12. Security

- **Operator → forecasting service:** on by default. The operator sends a short-lived projected service account token
  over TLS (port 8443); the service checks it with a Kubernetes TokenReview and accepts only the operator's service
  account. Another caller gets 401 or 403; if TokenReview cannot answer, the service returns 503 rather than letting
  the request through. Only `/health`, `/ready` and `/metrics` are open. The matching `ForecastAvailable` reasons are
  `Unauthorized`, `Forbidden`, `AuthUnavailable` (TokenReview could not judge) and `AuthMisconfigured` (the operator's
  token or CA file is missing, empty or invalid, or a token is configured with a non-https service URL).
- **Certificates** (`forecaster.tls.source`): `selfSigned` for plain Helm (not with Argo CD), `certManager` for Argo
  CD and long-running installs, or `existingSecret` (`tls.crt`, `tls.key`, `ca.crt`). A renewed certificate is
  picked up within about a minute.
- **NetworkPolicies** (on by default, need an enforcing CNI): only the operator may call the forecasting service, the
  trainer accepts no connections, and all three may reach only DNS, the Kubernetes API and your metrics store. Narrow
  `networkPolicy.kubeAPI.cidrs` to your API server's addresses.
- **Permissions:** each part has its own service account. The operator reads autoscalers, Deployments and the other
  scalers, and writes only `deployments/scale`, its status, events and its leader-election Lease. The forecasting
  service and the trainer only read autoscalers and Deployments (plus TokenReviews for the service). No role can read
  Secrets or create Pods or Jobs. Every container runs non-root with a read-only root filesystem and no capabilities.
- The plain manifests in `k8s-manifests/` do not have these protections; use the chart.

Details: [helm.md](helm.md) (What it installs; Authentication).

## 13. Upgrading and the API-group migration

**Chart upgrades.** Helm never upgrades a CRD. Before `helm upgrade` to a release with a newer CRD, apply it:

```sh
kubectl apply --server-side --force-conflicts -f charts/predictive-autoscaler/crds/
```

The operator checks the CRD's revision label at start. If the CRD is older than it needs, it starts no controller:
the pod stays unready with the reason in its log. After an upgrade, check whether models must be retrained (a version
that changes model provenance refuses older models).

**From the legacy API group.** Before v0.1.0 the group was `autoscaler.example.com`; it is now
`autoscaling.devkuban.com`. Objects are not converted; each one is re-created in the new group, in steps that never
let two operators scale one Deployment: install the new version side by side, then `hack/migrate-api-group.py` `plan`,
`prepare` (replacements in Recommend mode), train, `verify`, `cutover` (stops the legacy operators, deletes the legacy
autoscalers), and `activate` one autoscaler at a time. Full procedure: [upgrading.md](upgrading.md).

## 14. Uninstalling safely

1. Switch every autoscaler to `mode: Recommend` (or delete them). Their Deployments keep their replica counts.
2. `helm uninstall pa -n predictive-autoscaler` with an explicit kubeconfig and context.
3. What stays on purpose: the CRD, every PredictiveAutoscaler and the model volume (`forecaster.persistence.retain`).
   Delete your autoscaler objects when you no longer need them, and the model claim only if you mean to lose the models.
4. **Do not delete the CRD as routine.** Deleting it deletes every PredictiveAutoscaler in the cluster.

With Argo CD the CRD and the model volume are also protected from pruning ([helm.md](helm.md#argo-cd)).

## 15. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Ready=False`, `CrossNamespaceTarget` | `targetDeployment.namespace` differs from the autoscaler's namespace. | Create the autoscaler in the Deployment's namespace. |
| `Ready=False`, `DeploymentNotFound` | Wrong Deployment name, or it does not exist yet. | Fix `targetDeployment.name`. |
| `Ready=False`, `InvalidMetricQuery` | The query uses template features other than `{{ .Namespace }}` / `{{ .Name }}`, or is too long. | Rewrite the query ([configuration.md](configuration.md)). |
| `TelemetryAvailable=False`, `MetricsUnavailable`; replicas do not change | The query returns no series, several series or an error; or the metrics store is down. | Run `status.metricSource.query` against your metrics store; aggregate to one series; check label names (`namespace` vs `exported_namespace`) and that a Service name differs from the Deployment name. |
| Calculated is always `minReplicas` | `targetRPS` far too high, or the query counts another workload's traffic or is in the wrong unit (it must be per second). | Check `predictive_autoscaler_current_rpm` against what you expect (requests per minute = 60 × per second). |
| `ForecastAvailable=False`, `Unavailable` on a new autoscaler | No model yet: not enough history or no training. | Expected. List it in `training.targets`; wait for about a week of history; train ([section 7](#7-get-forecasts)). |
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
| `ScalingActive=False`, `GuardAborted` now and then | The autoscaler or the Deployment changed between the decision and the write. | Normal; a fresh reconcile follows within seconds. Frequent aborts mean another writer is changing the replica count. |
| `ScalingActive=False`, `TelemetryHold` | The request rate cannot be measured, so the count is held. | Fix the telemetry (see above). |
| Replicas flip back and forth | Another writer (GitOps `replicas`, a CI job, `kubectl scale`) that the operator cannot detect. | Remove `replicas` from the managed manifest or ignore the field in GitOps. |
| No scale-down although traffic dropped | Stabilization, cooldown, the headroom floor (`ScaleDownHeadroom=True`), or a forecast that still asks for more pods. | Wait for the windows; compare `forecastReplicas`; see [section 9](#9-scale-down-behaviour-and-headroom). |
| Operator pod unready after an upgrade | The CRD is older than the operator needs (revision gate). | Apply the new CRD ([section 13](#13-upgrading-and-the-api-group-migration)). |
| `VPAInterference=True` | A VPA changes the pods' requests. | Use VPA mode `Off`, or retune `targetRPS` with it. |
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
| **Reactive rule** | `ceil(measured rate / per-pod capacity)`: the decision without a forecast. |
| **Calculated replicas** | The decision: the higher of reactive and forecast, within `[minReplicas, maxReplicas]`. |
| **Lead window** | The next `leadTimeMinutes`; the operator sizes for the highest forecast inside it. |
| **Horizon** | How far ahead the forecast reaches (`horizonMinutes`, six 10-minute steps for one hour). |
| **targetRPS** | Requests per second one pod should handle. Internally the operator works in requests per minute (RPM). |
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
