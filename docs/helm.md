# Installing with Helm

The chart `charts/predictive-autoscaler` installs the three components, each with its own service account and the
least privilege it needs. It installs no PredictiveAutoscaler: those belong with the workloads they scale.

```
helm install pa charts/predictive-autoscaler -n predictive-autoscaler --create-namespace -f my-values.yaml
# from a release (docs/releasing.md):
# helm install pa oci://ghcr.io/bakmurat/charts/predictive-autoscaler --version X.Y.Z -n predictive-autoscaler --create-namespace -f my-values.yaml
```

Requirements: Kubernetes 1.33–1.35 (the versions the chart is tested against: rendered and schema-checked on 1.33, 1.34
and 1.35, the operator's envtest on 1.35; the chart's `kubeVersion` admits any version from 1.33, but later ones are
untested), and a Prometheus-compatible query API with the request metrics of your
workloads. Install **one release per cluster**, or give several releases disjoint `operator.watchNamespaces`: each
release elects its own leader in its own namespace, so two releases watching the same namespace would both scale its
autoscalers. With Helm 3, `helm template` needs `--kube-version` (its offline default is older than 1.33). Argo CD passes
the cluster's version itself.

## The values you must set

```yaml
prometheus:
  url: http://prometheus-server.monitoring.svc:80   # Prometheus, VictoriaMetrics vmselect, Thanos Query, ...
networkPolicy:
  prometheus:                                        # where Prometheus runs (or set networkPolicy.enabled: false)
    namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: monitoring}}
training:
  targets:                                           # one training CronJob per autoscaler
    - {namespace: shop, name: web-pa, schedule: "0 */6 * * *"}   # the PredictiveAutoscaler's name (TRAINING_TARGET=shop/web-pa)
```

`values.yaml` documents every option, and `values.schema.json` rejects unknown keys and malformed values. A forecast
needs enough request history (seven-day default lookback; at least 235 usable 10-minute points with the default model,
subject to the trainer's sequence and gap checks and a complete recent inference window) and a training run per autoscaler. Until then each autoscaler runs on the reactive rule,
and new autoscalers run in Recommend mode (they publish recommendations and change nothing until `spec.mode: Active`).

## What it installs

| Component | Runs as | Can do | Writes to |
|---|---|---|---|
| operator (Deployment; leader election always on; one replica and Recreate when the ledger uses a claim) | uid 65532 | read autoscalers and Deployments; write replicas only through `deployments/scale`, its status and events, cluster-wide when every namespace is watched, otherwise only in `operator.watchNamespaces` (the reads stay cluster-wide; the release namespace gets only the Lease and its events). The watched namespaces must exist before install, and when you narrow or change them on an upgrade, check that the old write bindings are gone (with pruning off, Argo CD keeps them); read HPAs, KEDA ScaledObjects, VPAs and legacy-group autoscalers (coexistence); read its own CRD (revision gate); emit events; a Lease in the release namespace | nothing (or `/var/lib/predictive-autoscaler` with `operator.forecastLedger.enabled`) |
| forecasting service (Deployment, 1 replica) | uid 10001 | get autoscalers and Deployments; create TokenReviews when authentication is enabled | `/models` (the model volume), `/tmp` (an emptyDir with a size limit) |
| trainer (one CronJob per `training.targets` entry) | uid 10001 | get autoscalers and Deployments | `/models`, `/tmp` |

Every container runs non-root with a read-only root filesystem, no privilege escalation, all capabilities dropped and
seccomp `RuntimeDefault`. HOME, TMPDIR, the XDG and Keras directories point into `/tmp`. No role can create Jobs or
Pods or read Secrets. The chart has no cold start: retrain by hand with
`kubectl -n <ns> create job --from=cronjob/<cronjob> <name>` (the CronJob's annotation
`autoscaling.devkuban.com/training-target` names its autoscaler).

## The CRD

- Helm installs `crds/` once and never upgrades or deletes it. Before `helm upgrade` to a release with a newer CRD,
  apply it explicitly:

  ```
  kubectl apply --server-side --force-conflicts -f charts/predictive-autoscaler/crds/
  ```

  `--force-conflicts` is needed because Helm created the CRD with another field manager; without it the apply stops on
  a conflict and leaves the old revision in place, and the new operator then waits, unready, on the revision gate (seen
  in the soak, observation O2). The CRD is the only object it takes over.

- Upgrading the forecasting service is a short outage: it is one replica with the Recreate strategy (its model volume
  may be ReadWriteOnce), so the old pod stops before the new one is ready. While the operator keeps running it uses a
  usable cached forecast, or else the reactive rule on live telemetry, subject to the usual scaling safeguards
  (headroom, stabilization, cooldown, conflict checks). Losing the forecaster adds no hold of its own; missing telemetry
  triggers the existing telemetry hold (user guide, [section 7](user-guide.md#7-get-forecasts)). Upgrade outside your
  peak if you rely on the forecast's lead time.

- The CRD carries a revision label (`autoscaling.devkuban.com/crd-revision`). The operator reads it before starting.
  Revisions: 1 = the first `autoscaling.devkuban.com` schema; 2 = adds `spec.behavior.scaleDown.headroomPercent`.
  While the installed CRD is older than it needs, the operator starts no controller and does not campaign for
  leadership: its liveness probe passes and its readiness probe fails, with the reason in the log.
- `helm uninstall` keeps the CRD, and with it every PredictiveAutoscaler. Delete the CRD by hand only if you want them
  gone.

## Argo CD

- The CRD is annotated `argocd.argoproj.io/sync-options: Prune=false,Delete=false,ServerSideApply=true` and
  `argocd.argoproj.io/sync-wave: "-1"`. It is applied before the workloads, and deleting the chart's Application
  leaves it, and all autoscalers, in place.
- The model volume created by the chart is kept too (`forecaster.persistence.retain`, on by default): Helm's
  `resource-policy: keep` plus Argo's `Prune=false,Delete=false`. Use `forecaster.persistence.existingClaim` to manage
  the claim yourself. **Turning `retain` off on a running release removes that protection from the existing claim**:
  a later uninstall (or Application delete) then deletes the volume and every trained model. The install notes warn
  when it is off.
- The autoscalers are not the chart's. They live with their workloads, in the app's own Application, and are deleted
  with it. That is safe: the operator has no finalizer, and the Deployment keeps its replica count; only autoscaling
  stops. To keep an autoscaler when its Application is deleted, annotate it
  `argocd.argoproj.io/sync-options: Delete=false,Prune=false`.

## Network policies

They are on by default, and work only with a CNI that enforces them.
- The forecasting service accepts connections only from the operator (and from `networkPolicy.metricsFrom` on its
  port).
- The trainer accepts none.
- All three may reach DNS, the Kubernetes API and Prometheus. The operator may also reach the forecasting service.

Enforcement depends on the CNI. Some implementations apply a new pod's policies only once their agent has learned the
pod, and let its first connections through (kind's kube-network-policies did for about ten seconds in the smoke test).
Tighten `networkPolicy.kubeAPI.cidrs` to your API server's addresses: the default allows any address on ports
443/6443. Whether a policy sees the `kubernetes` Service's ClusterIP or the translated endpoint address depends on the
CNI (Kubernetes leaves the order of Service translation and policy evaluation to the implementation), so an allow for
the ClusterIP alone is not portable. Configure the addresses and ports your CNI evaluates; the endpoints are listed by
`kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes` (often the control-plane node IPs on
6443, but check). **Cilium** normally excludes node addresses from CIDR matching (unless `policyCIDRMatchMode: nodes`),
so an ipBlock for the API server may never match; Cilium's documented way is a CiliumNetworkPolicy with
`toEntities: [kube-apiserver]`. Note that `cidrs: []` renders a ports-only rule that allows **every** destination on
those ports, and allow rules add up, so adding a Cilium policy next to it does not narrow anything: the default is a
connectivity fallback, not API-only isolation. On Cilium, set `networkPolicy.kubeAPI.cilium: true` (with `cidrs: []`):
the chart then leaves the API rule out of its NetworkPolicies and renders one CiliumNetworkPolicy for the operator,
forecaster and trainer that allows the `kubeAPI.ports` to the `kube-apiserver` entity only. It needs Cilium's CRDs
(`cilium.io/v2`) and fails on a non-empty `cidrs`. Set `networkPolicy.enabled: false` if your CNI ignores policies or you manage them elsewhere.

## Persistence

The forecasting service and the trainer share the model volume.
- With `ReadWriteOnce` (the default), each training pod is scheduled on the forecasting service's node.
- `ReadWriteMany` removes that constraint.
- Never use `ReadWriteOncePod`, for the chart's claim or an `existingClaim`: the trainer must mount the claim the
  forecasting service holds.
- The trainer serializes publications with `flock`, so the volume's filesystem must support it. Local disks and most
  block CSI drivers do; some NFS setups do not.

## Monitoring

`metrics.serviceMonitor.enabled` (Prometheus Operator) or `metrics.vmServiceScrape.enabled` (VictoriaMetrics
Operator) scrapes the operator (port 8080) and the forecasting service (port 8000). The operator's scrape keeps the
metrics' own `namespace` label (the autoscaler's namespace; `honorLabels`), so it is not renamed to
`exported_namespace`. Both scrapes add `pa_install_namespace`, the namespace the chart is installed in; together with
`job` (the Service name) it identifies one installation.

The operator exports every autoscaler's conditions as `predictive_autoscaler_condition{namespace, autoscaler, type,
status}`: 1 for the current status (`True`, `False` or `Unknown`), 0 for the other two, in the style of
kube-state-metrics. `predictive_autoscaler_forecasting_enabled{namespace, autoscaler}` is 1 unless
`spec.prediction.enabled` is false. The series are removed with the autoscaler.

### Dashboard

`dashboards.enabled` installs the Grafana dashboard "Predictive Autoscaler — Help & Overview (<namespace>/<release>)"
(`dashboards/overview.json`) as a ConfigMap labelled `grafana_dashboard: "1"` for the Grafana dashboard sidecar;
`dashboards.labels` and `dashboards.annotations` add your sidecar's labels or a folder annotation. Its uid and data key
are derived from the release's namespace and name, so several installations can share one Grafana, and it
pre-selects its own installation (`pa_install_namespace` and the scrape `job`, from the scrape objects above); every
query is limited to the selected installation. The sidecar reads only its own namespace by default: install the chart
there, or let the sidecar search this namespace or all namespaces (Grafana chart: `sidecar.dashboards.searchNamespace`),
which needs RBAC to list and watch ConfigMaps there. Its first panel is a Help / User Guide text (walkthrough, panels,
alerts); the panels use only the operator's metrics and `up`, with a data source variable. The full guide is
[user-guide.md](user-guide.md).

### Alerts

`metrics.alerts.prometheusRule.enabled` (Prometheus Operator) or `metrics.alerts.vmRule.enabled` (VictoriaMetrics
Operator) installs these rules. They need the matching scrape object above. Every rule selects only this
installation's metrics (its `job` and `pa_install_namespace`), and every alert carries `pa_release` and
`pa_release_namespace`, so several installations in one cluster never fire or merge each other's alerts.

| Alert | Fires when | Default `for` | Severity |
|---|---|---|---|
| `PredictiveAutoscalerNotReady` | `Ready` is not True (reconciles fail) | `notReadyFor` 15m | warning |
| `PredictiveAutoscalerTelemetryUnavailable` | the request rate cannot be measured, so the replica count is held | `telemetryFor` 10m | warning |
| `PredictiveAutoscalerForecastUnavailable` | no forecast is used; expected until the first training has enough history; never for `spec.prediction.enabled: false` | `forecastFor` 2h | info |
| `PredictiveAutoscalerConflict` | another replica writer targets the Deployment, so the operator writes nothing | `conflictFor` 5m | warning |
| `PredictiveAutoscalerModelStale` | forecasts in the last hour came from a model whose training cutoff is older than `modelStaleHours` (26) | 15m | warning |
| `PredictiveAutoscalerOperatorDown` | no operator scrape target is up (down or missing); a healthy scrape does not prove reconciles succeed, which `NotReady` covers | `absentFor` 10m | critical |
| `PredictiveAutoscalerForecasterDown` | no forecaster scrape target is up | `absentFor` 10m | warning |

`metrics.alerts.labels` adds labels (for example a team label for routing) to every alert. `make chart-check` runs
the rules' unit tests with promtool (`hack/alerts/alerts-test.yaml`): each alert firing, recovering and staying silent,
including missing series.

## Authentication between the operator and the forecasting service

It is on by default (`forecaster.auth.enabled`).
- The operator sends a projected service account token for the forecasting service's audience. The kubelet rotates
  it (it is valid for an hour) and the operator reads it on every request.
- The forecasting service checks each token with TokenReview, before reading the request, and accepts only the
  operator's service account: another caller gets 401 or 403. If TokenReview cannot answer it gets 503, never an open
  door. Kubernetes may report a wrong-audience token as a TokenReview error; that also returns 503 AuthUnavailable.
  API docs and new endpoints require authentication too; only `/health`, `/ready` and open `/metrics` are exempt.
- Everything runs over TLS (port 8443). The operator verifies the certificate against the CA, and it mounts only the
  CA, never the private key. `/health` and `/metrics` stay open, behind the network policies.

The certificate comes from `forecaster.tls.source`:

| Source | Use it for | Renewal |
|---|---|---|
| `selfSigned` (default) | plain Helm, the quickstart | valid `selfSigned.validityDays` (365); it is **not** renewed automatically (the install notes say so): renew before it expires by deleting the Secret `<release>-predictive-autoscaler-forecaster-tls` and running `helm upgrade`; prefer `certManager` in production. The Secret's `ca.crt` does not tell you the served certificate's expiry: check `tls.crt` or the served certificate. `helm upgrade` keeps it through `lookup`, which Argo CD and `helm template` cannot do, so **do not use it with Argo CD** |
| `certManager` | Argo CD, production | cert-manager renews it (`duration`, `renewBefore`); the Issuer must put its CA into the Secret's `ca.crt` (a CA issuer does) |
| `existingSecret` | certificates managed elsewhere | yours: a Secret with `tls.crt`, `tls.key` and `ca.crt` |

When the certificate files change, the forecasting service restarts itself within about a minute to load them, and
the operator reloads the CA. For those seconds the operator uses a recent cached forecast or the reactive rule. The
smoke test runs a rotation.

`forecaster.auth.enabled: false` goes back to plain HTTP on port 8000, with the network policies as the only boundary.

## Not in the chart (yet)
- The release images and the published chart: these come with v0.1.0.
- Benchmark tooling (forecast experiments, evidence archiving): repository only.

Moving from a legacy (`autoscaler.example.com`) install: see [upgrading.md](upgrading.md).
