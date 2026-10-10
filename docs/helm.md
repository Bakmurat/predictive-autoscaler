# Installing with Helm

The chart `charts/predictive-autoscaler` installs the three components, each with its own service account and the
least privilege it needs. It installs no PredictiveAutoscaler: those belong with the workloads they scale.

```
helm install pa charts/predictive-autoscaler -n predictive-autoscaler --create-namespace -f my-values.yaml
```

Requirements: Kubernetes 1.33 or later, and a Prometheus-compatible query API with the request metrics of your
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
    - {namespace: shop, name: web, schedule: "0 */6 * * *"}
```

`values.yaml` documents every option, and `values.schema.json` rejects unknown keys and malformed values. A forecast
needs about a week of history and a training run per autoscaler. Until then each autoscaler runs on the reactive rule,
and new autoscalers run in Recommend mode (they publish recommendations and change nothing until `spec.mode: Active`).

## What it installs

| Component | Runs as | Can do | Writes to |
|---|---|---|---|
| operator (Deployment; leader election always on; one replica and Recreate when the ledger uses a claim) | uid 65532 | read autoscalers and Deployments; write replicas only through `deployments/scale`; read HPAs, KEDA ScaledObjects, VPAs and legacy-group autoscalers (coexistence); read its own CRD (revision gate); emit events; a Lease in the release namespace | nothing (or `/var/lib/predictive-autoscaler` with `operator.forecastLedger.enabled`) |
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
  kubectl apply --server-side -f charts/predictive-autoscaler/crds/
  ```

- The CRD carries a revision label (`autoscaling.devkuban.com/crd-revision`). The operator reads it before starting.
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
  the claim yourself.
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
443/6443. Set `networkPolicy.enabled: false` if your CNI ignores policies or you manage them elsewhere.

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
Operator) scrapes the operator (port 8080) and the forecasting service (port 8000).

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
| `selfSigned` (default) | plain Helm, the quickstart | valid `selfSigned.validityDays` (365); renew by deleting the Secret `<release>-predictive-autoscaler-forecaster-tls` and running `helm upgrade`. `helm upgrade` keeps it through `lookup`, which Argo CD and `helm template` cannot do, so **do not use it with Argo CD** |
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
