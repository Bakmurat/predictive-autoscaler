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
| operator (Deployment; leader election always on; one replica and Recreate when the ledger uses a claim) | uid 65532 | read autoscalers and Deployments; write replicas only through `deployments/scale` (none with `operator.rbac.scaleWrite: false`, a shadow Recommend-only install: the operator may then only get `deployments/scale`, and an Active autoscaler's write fails with Forbidden; with `operator.rbac.scaleTargets`, a list of Deployment names, `update` on `deployments/scale` carries `resourceNames` with exactly those names while `get` stays unrestricted, see below), its status and events, cluster-wide when every namespace is watched, otherwise only in `operator.watchNamespaces` (the reads stay cluster-wide; the release namespace gets only the Lease and its events). The watched namespaces must exist before install, and when you narrow or change them on an upgrade, check that the old write bindings are gone (with pruning off, Argo CD keeps them); read HPAs, KEDA ScaledObjects, VPAs and legacy-group autoscalers (coexistence); get/list the ReplicaSets and Pods of its targets (startup measurement, `ScaleOutBlocked`), bound like the writes; read its own CRD (revision gate); emit events; a Lease in the release namespace | nothing (or `/var/lib/predictive-autoscaler` with `operator.forecastLedger.enabled`) |
| forecasting service (Deployment, 1 replica) | uid 10001 | get autoscalers and Deployments; create TokenReviews when authentication is enabled | `/models` (the model volume), `/tmp` (an emptyDir with a size limit) |
| trainer (one CronJob per `training.targets` entry) | uid 10001 | get autoscalers and Deployments | `/models`, `/tmp` |

Every container runs non-root with a read-only root filesystem, no privilege escalation, all capabilities dropped and
seccomp `RuntimeDefault`. HOME, TMPDIR, the XDG and Keras directories point into `/tmp`. No role can create Jobs or
Pods or read Secrets; Pods and ReplicaSets are only read (get/list), by the operator, where it scales. The chart has no cold start: retrain by hand with
`kubectl -n <ns> create job --from=cronjob/<cronjob> <name>` (the CronJob's annotation
`autoscaling.devkuban.com/training-target` names its autoscaler).

### Limiting the replica write to named Deployments

`operator.rbac.scaleTargets` (a list of Deployment names, empty by default) narrows the one replica write the operator
has:

| `scaleWrite` | `scaleTargets` | Rules on `apps` `deployments/scale` in the write role |
|---|---|---|
| `true` (default) | `[]` (default) | `get`, `update` in one rule, no names: every Deployment where the role is bound (unchanged) |
| `true` | `[web, api]` | `get` in its own rule, no names; `update` with `resourceNames: [web, api]` |
| `false` | `[]` | `get` only (a shadow install) |
| `false` | not empty | refused at render time (the intent is ambiguous) |

- For a subresource, `resourceNames` holds the parent object's name, so the entries are the Deployments' names. Only
  the write verb the chart grants (`update`) carries them; no `patch` is granted. Names must be distinct DNS-1123
  subdomains (lower-case alphanumeric labels that may contain `-`, joined by single dots, at most 253 characters; so
  `web..api`, `web.-api` and `Web` are refused), at most 64 of them; the schema and the template both check.
- The names apply in every namespace the write role is bound in: each of `operator.watchNamespaces` (one RoleBinding
  per namespace), or the whole cluster when no namespace is watched. To allow `web` in `shop` only, set both
  `watchNamespaces: [shop]` and `scaleTargets: [web]`; with several watched namespaces a Deployment of that name is
  writable in each of them.
- RBAC binds the **name**, not the object. A Deployment deleted and recreated under an allowed name is still
  authorized by RBAC. The operator refuses a replaced target only within the reconcile that read the old object (its
  fresh Scale read and identity guard compare UIDs); on its next reconcile it reads the replacement, starts its state
  again for the new UID, and can then scale it reactively. Restricting writes to reviewed UIDs needs an operator-side
  check, which this chart value does not provide.
- An autoscaler whose target is not listed keeps working in Recommend mode; in Active mode its write fails with
  Forbidden, as in a shadow install.
- After changing the list, check the effective permissions as the operator's ServiceAccount, for example
  `kubectl auth can-i update deployments/web --subresource=scale -n shop --as system:serviceaccount:<release namespace>:<operator ServiceAccount>`
  (expect `yes` for a listed name and `no` for any other name or namespace). Bindings made outside the chart can
  grant more than the chart renders, so check the answer, not only the rendered role.

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
  Revisions: 1 = the first `autoscaling.devkuban.com` schema; 2 = adds `spec.behavior.scaleDown.headroomPercent`;
  3 = adds `spec.metrics.requests.capacityPerReplica` and deprecates `targetRPS` ([upgrading.md](upgrading.md));
  4 = adds the forecast contract v2 settings (`spec.prediction.engine` and the keys after it), `spec.behavior.startup`
  and `status.prediction`.
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

## Statistical forecaster profile (preview)

`forecaster.engine: statistical` replaces the TensorFlow forecasting service with the light statistical forecaster
(forecast contract v2, `window-max-rps/v1`): image `images.statisticalForecaster` (built from
`ml-engine/Dockerfile.statistical`; no TensorFlow, Keras, CUDA, pandas or scikit-learn), started as
`python -m api.serve statistical`. The default stays `legacyLstm` until the release evaluation (design C2.8).

- **What it serves:** `POST /v2/predict`, `/health`, `/ready` and `/metrics` only, behind the same TokenReview
  authentication and TLS as the legacy service. Benchmark and legacy experiment switches (`ALLOW_BENCHMARK_EXPERIMENTS`,
  `SEASONAL_EXPERIMENT`, ...) stop it from starting.
- **A Recommend-only preview:** it issues shadow forecasts, calibrates them on matured windows and reports their quality
  (the `predictive_autoscaler_forecast_*` metrics), but every answer has `predictive_eligible: false` until its release
  gates pass. The current operator does not call `/v2/predict` yet.
- **Resources:** `forecaster.statistical.resources` (requests 256Mi and 250m CPU, limits 512Mi and 1 CPU) instead of
  `forecaster.resources`; one replica, `Recreate`.
- **State:** a retained 2Gi claim (`<release>-forecast-state`, `forecaster.statistical.persistence`) mounted at
  `/state` holds the forecast journal (SQLite, WAL) and the fit artifacts, with a journal budget of
  `forecaster.statistical.stateQuota` (1536Mi). Issuances and outcomes are kept 7 days online, then compacted into
  per-epoch counts. A lost or corrupt journal never carries evidence over: the service answers 503 until it can write
  again, and a new journal starts every trust epoch from nothing.
- **No trainers:** fits run inside the service on a bounded worker (one process, 30 s, 384 MiB address space); the
  training CronJobs are not rendered, and `training.targets` with this engine is refused.
- **Switching:** the Deployment, Service and operator URL stay the same in both profiles, and each profile's claim is
  kept (`helm.sh/resource-policy: keep`) when the other one is installed, so an upgrade to `statistical` and a rollback
  to `legacyLstm` both find their volume again.
- **Checks:** `hack/sbom-check-statistical.py` checks the pinned requirements and the Dockerfile offline (part of the
  test suite); with `--image <ref>` it lists the built image's packages and shared libraries and imports the service
  with TensorFlow blocked (needs docker: a release gate). `hack/statistical-envelope.py` estimates memory, CPU, latency
  and journal growth offline. `hack/statistical-container-envelope.sh` (docker, on the aspire kind bed host) runs it
  inside the built image under the profile's limits (512 MiB, 1 CPU, read-only root) with the fit worker on and 8
  days of history, and fails on an OOM kill, a budget breach, any failed fit, no issuance that used a fitted artifact,
  or a missing cgroup measurement: the gate for the 384 MiB fit limit and these resources.
- **Readiness:** `/ready` answers 503 while the journal cannot support issuance (and before start-up); `/health`
  stays the liveness probe. Young history or untrusted models do not make the service unready.
- **Families and the challenger:** `spec.prediction.families` (`[seasonalNaive, statistical]` by default) lists what
  is forecast; `[seasonalNaive]` removes E1-product entirely (no fit, no candidate), and seasonal-naive is always
  required (CRD revision 5). E1 is selected over a trusted seasonal-naive only on bound **same-policy costs**: once the
  retained paired evidence meets `challengerMinPairs` and `challengerMinHours`, both forecasts' issued bounds are
  replayed through the operator's own v2 policy for that autoscaler (`ml-engine/forecasting/policy_sim.py`, held
  decision for decision to the Go replay by `k8s-operator/controllers/testdata/v2-policy-parity.json`), and E1's
  shortage and replica-time must each be at most `challengerCostRatioBps` of seasonal-naive's. The controller input at
  each decision is the newest observation that decision's issuance was computed from (what was available then); the
  eventual measurement only scores. Startup is modelled per cohort: a scale-up is ready only after the startup delay,
  and a scale-down removes starting replicas first, so a quick down-then-up does not count as ready capacity. Costs
  need adequate evidence: at least `challengerMinPairs` matched decisions, at least `minAvailabilityBps` of the
  replayed ones, and at most `1 - minAvailabilityBps` of the time without a measurement (5% by default). The replay
  runs again only when the paired evidence or the policy changed. The costs carry their
  epoch, models, recipes, window and policy fingerprint (capacity, bounds, caps, headroom, startup delay, decision
  interval) and count only when all of them match; an autoscaler without `capacityPerReplica` (or `targetRPS`) has no
  policy, so E1 is never promoted for it. Each issuance records the policy fingerprint and the costs it used.
- **Lab qualification (C2.8 evaluation only):** `forecaster.statistical.labQualification.configMap` mounts a reviewed
  `lab-qualification/v1` file (`ml-engine/forecasting/qualification.py`) and lets the forecaster read the
  `kube-system` Namespace (the cluster UID). It is honoured only by the evaluation image of a pushed commit, built
  with `hack/build-statistical-evaluation.sh --push <commit> <ref>` (the context is `git archive` of the commit,
  verified file by file in the image's verify stage; a dirty or mismatched context fails the build), and only when the
  chart runs that image by digest (`images.statisticalForecaster.digest`, required with this option): the file names
  both the commit and the image digest. A release image, a malformed, expired or over-wide file, another tree or
  another image stops the forecaster from starting. The file also names the run-lock digest, G0-G4 as `pass`, at
  least two reviewers, a validity window of at most 60 days, and its
  scope: one cluster UID, namespaces and every target Deployment by namespace, name and UID. For a request whose live
  cluster, namespaces and target are all in that scope while it is valid, it stands in for the release gate and
  nothing else: identity, settings, calibration, coverage, availability, prospective evidence, selection, the issuance
  deadline, caps, telemetry and the operator's write guards apply as always, so an untrusted forecast stays
  ineligible. Every issuance records `release_gate` (`passed`, `basis`, the qualification's digest), and every answer,
  a retry included, re-checks it: once the qualification has expired or been removed or replaced, even a stored
  issuance is not eligible, and a lab-qualified answer whose `valid_until` falls after the expiry is never eligible.
  `/health` shows the build and the qualification.
- **Demonstration qualification (a scoped live demonstration only):**
  `forecaster.statistical.demonstrationQualification.configMap` mounts two reviewed files from one ConfigMap and lets
  the forecaster read the `kube-system` Namespace (the cluster UID). It is a demonstration basis. It is **not** a
  release and **not** an evaluation (lab) qualification: it records no gate and grants no G0-G5 pass, and neither file
  has a `gates` field (a document that carries one is refused). What it does is exactly what the lab qualification
  does, for a narrower scope: it stands in for the release gate and nothing else. Identity, settings, calibration,
  coverage, availability, prospective evidence, selection, the issuance deadline, caps, telemetry and the operator's
  own verdict and write guards apply as always. An eligible answer is an answer, not an action: the operator still
  needs Active mode and the RBAC write (`operator.rbac.scaleWrite`), so the document can be mounted while every
  replica write is denied.
  - **The two files** (`ml-engine/forecasting/demonstration.py`), each one JSON object with exactly these keys:

    | File (value key) | Keys |
    |---|---|
    | `demonstration-qualification/v1` (`key`, default `demonstration-qualification.json`) | `schema`, `source_tree`, `image_digest`, `contract_sha256`, `reviewers`, `not_before`, `expires`, `families`, `scope` {`cluster_uid`, `namespace`, `targets` [{`namespace`, `name`, `uid`}]} |
    | `demonstration-contract/v1` (`contractKey`, default `demonstration-contract.json`) | `schema`, `checked` {`qualification` {`expires_not_after`}, `families`, `cadence` {`grid_seconds`, `update_interval_seconds`}, `targets` [{`namespace`, `name`, `uid`, `min_replicas`, `max_replicas`, `capacity_per_replica`, `settings_sha256`}]}, optionally `unchecked` (any object: the rest of the reviewed contract, hashed with the file, never interpreted) |

    `settings_sha256` is the resolved forecast settings fingerprint of the target's autoscaler (the
    `settings_sha256` the operator sends with every request: risk, calibration, trust, predictive extra, grid,
    history, horizon and families), so a reviewed trust or calibration setting cannot change under the same contract.
    `capacity_per_replica` is compared as **exact text** with `spec.metrics.requests.capacityPerReplica`: a
    numerically equal spelling (`5.50` for `5.5`) is refused. Generate the contract from the admitted
    PredictiveAutoscaler object as the API server returns it, never from a number typed again. Two more keys of
    `checked` are **reserved and not enforced by this build**: `calibration_policy_revision` and
    `trust_policy_revision`. A contract that carries either is refused, so that nobody takes them for compared before
    they are. Every digest, time, name and number in both files is matched whole: no trailing newline passes.

  - **At start-up** the forecaster refuses to start unless all of this holds: the image is the evaluation build of
    the `source_tree` the qualification names, run by the `image_digest` it names
    (`images.statisticalForecaster.digest`, required with this option); the mounted contract's sha256 equals
    `contract_sha256`; the qualification's families and targets equal the contract's and it expires no later than the
    contract's `expires_not_after`; at least two distinct reviewers; a validity window that is positive, at most 21
    days and not yet over; reviewers compared without surrounding whitespace and without regard to case; one cluster
    UID, one namespace (a DNS-1123 label) and one or two targets, each by namespace, name and UID;
    families from `seasonalNaive` and `statistical` (the names of `spec.prediction.families`); a cadence of exactly
    60 s grid and 60 s update interval; replica bounds and a plain decimal `capacity_per_replica` for each target.
    The chart refuses `demonstrationQualification` together with `labQualification`, and the forecaster refuses it
    when the release gates have passed (no silent precedence between the three bases).
  - **For each request** the demonstration basis holds only when: both mounted files still hold exactly the bytes
    admitted at start-up; the live cluster, the autoscaler's namespace and its target (name and UID) are in scope;
    the document is valid now and at the answer's `valid_until`; the live autoscaler is configured as the contract
    says (`spec.prediction.families` within the listed families, `sampleIntervalSeconds` 60 and
    `updateIntervalSeconds` 60 set explicitly, the resolved settings fingerprint, `minReplicas`, `maxReplicas`, and
    the exact text of `spec.metrics.requests.capacityPerReplica` without `targetRPS`); and the family that was
    actually **selected** for that answer is listed. Otherwise the answer is not eligible under it, with the first
    failing check recorded.
  - **The mounted files are checked at every authorization.** Both files are read through their paths and hashed
    again each time an answer is issued or a stored answer is served (no cache and no reliance on modification
    times, so the delay is zero: the next authorization after a change sees it). A missing, unreadable, oversized or
    changed file withdraws the basis (`qualification-file` or `contract-file`). What is found there is never loaded:
    a replacement document is admitted only by restarting the forecaster. If the mounted bytes are again exactly the
    admitted ones, the same authority applies again to answers issued from then on.
  - **Withdrawing is not revoking.** Removing or changing the mounted qualification stops further authorization by
    the forecaster, but it does not instantly recall an answer the operator has already received and holds until its
    `valid_until`. The emergency stop is revoking the scale permission (`operator.rbac.scaleWrite: false`, or the
    autoscaler back to Recommend).
  - **What is recorded and shown:** every issuance's `release_gate` holds `basis: demonstration`, the qualification's
    and the contract's sha256 and, when it did not pass, `refused` (`release`, `qualification-file`, `contract-file`,
    `cluster`, `namespace`, `target`, `validity`, `families`, `cadence`, `settings`, `bounds`, `capacity` or
    `family`). Every answer, a retry of a stored one
    included, re-checks the basis, both digests, the live configuration and the selected family: after expiry,
    removal, replacement or drift a stored issuance is not eligible. `/health` shows `demonstration_qualification`
    (with `withdrawn`: the mounted file that no longer matches, if any) and `qualification_basis`. `/metrics` has
    `predictive_autoscaler_forecaster_qualification_basis{basis="none|lab|demonstration|release"}` (1 for the basis in
    force for the process, by validity window; scope is per request),
    `predictive_autoscaler_forecaster_qualification_expires_timestamp_seconds{document="lab|demonstration"}` (it stays
    exported after the expiry) and `predictive_autoscaler_forecaster_demonstration_refusals_total{reason}`.
  - Authority is admitted once, at start-up: after changing the ConfigMap the running forecaster authorizes nothing
    under the demonstration basis until it is restarted with the new files (or the admitted files are back).

## Monitoring

`metrics.serviceMonitor.enabled` (Prometheus Operator) or `metrics.vmServiceScrape.enabled` (VictoriaMetrics
Operator) scrapes the operator (port 8080) and the forecasting service (port 8000). Both scrapes keep the metrics' own
`namespace` label (the autoscaler's and its workload's namespace; `honorLabels`), so it is not renamed to
`exported_namespace` even for a workload outside the chart's namespace. Both add `pa_install_namespace`, the namespace
the chart is installed in; together with `job` (the Service name) it identifies one installation.

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
| `PredictiveAutoscalerForecastUnavailable` | no forecast is used; expected until the first training has enough history; never for `spec.prediction.enabled: false`. Label `fallback`: `reactive` (decisions use the measured rate) or `capacity_hold` (no per-replica capacity: nothing is forecast or calculated and the count is held; set `capacityPerReplica`) | `forecastFor` 2h | info |
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

**Extra certificate names and routes.** `forecaster.tls.extraDNSNames` adds names to the certificate, so the operator
can dial the forecaster through a named route (`operator.forecasterAddress`, for example the C2.8 evaluation's
forecast route) with verification on. The chart refuses an address whose host is not in that list. With `selfSigned`,
adding names does **not** change a certificate that already exists: `helm upgrade` keeps the stored Secret (`lookup`),
so the new names are absent until you rotate. To rotate:

1. Delete the Secret `<release>-predictive-autoscaler-forecaster-tls`.
2. Run `helm upgrade` with the new names.
3. Wait for the forecasting service to reload, and the operator its CA (about a minute).
4. Check the served certificate's SANs, for example with `openssl s_client -connect <route>:8443 -servername <route>`.

During those seconds the operator falls back as described above. With `certManager`, the Certificate's `dnsNames`
change on upgrade and cert-manager reissues it. With `existingSecret`, you issue the names yourself.

`forecaster.auth.enabled: false` goes back to plain HTTP on port 8000, with the network policies as the only boundary.

## Not in the chart (yet)
- The release images and the published chart: these come with v0.1.0.
- Benchmark tooling (forecast experiments, evidence archiving): repository only.

Moving from a legacy (`autoscaler.example.com`) install: see [upgrading.md](upgrading.md).
