# Limitations (current version)

This is a research prototype that has run only in test environments with synthetic, generated load. It is not
production-ready. Known limits, most important first:

1. **Holds during metrics outages.** Without a measured request rate the operator holds the current replica count (see
   configuration.md, Decision rule), so a real load change during a monitoring outage is not followed until metrics return.
2. **Only known scalers are detected.** In `Active` mode the operator refuses to scale while one of these targets the
   Deployment: an HPA, an unpaused KEDA ScaledObject, or another Active PredictiveAutoscaler. It does not detect other
   writers (CI jobs, `kubectl scale`, GitOps applying `replicas`), and the check is not atomic. A write is refused if
   the replica count changed after the decision (see coexistence.md).
3. **One request-rate series per workload.** The `istio` preset needs Istio sidecars. Other sources need a `prometheus`
   query, restricted to `{{ .Namespace }}` and `{{ .Name }}` substitutions, that returns exactly one series in requests
   per second. Only request rate is supported; CPU and memory are not.
4. **One workload per trainer.** The training CronJob trains for one PredictiveAutoscaler (`TRAINING_TARGET`), on its
   compiled query. A workload without a model gets no forecast (422) and runs reactive-only. Models are keyed by
   namespace and name. Models from earlier versions carry no query provenance and must be retrained after an upgrade.
5. **Single forecasting replica on a ReadWriteOnce volume:** the API and the trainer must run on the same node; the API is
   a single point of failure (the operator falls back to the reactive rule when it is down).
6. **Operator state in memory:** the stabilization timers, overestimate tracking and forecast cache are lost on restart
   and when leadership moves to another instance. Scale-down holds restart conservatively (see Configuration: a fresh
   state is held as if a scale write had just happened, at least 10 minutes before a scale-down), but forecast-dependent decisions may differ from an uninterrupted run.
7. **Permissions and authentication outside the chart:** the Helm chart gives each component its own least-privilege
   service account, NetworkPolicies, and TokenReview authentication over TLS between the operator and the forecasting
   service (helm.md). The plain manifests in `k8s-manifests/` still share one broad role and run the forecasting service
   without authentication.
8. **Silent configuration mistakes:** ignored CRD fields (CPU, memory, resources, container), defaults that differ from
   the README, and `metrics.requests.enabled` defaulting to `false` (see configuration.md). A missing per-replica
   capacity is no longer silent (it holds and reports `CapacityConfigured=False`), but admission does not require one
   yet.
9. **Heavy footprint:** the forecasting image includes TensorFlow (about 1 CPU / 1 GiB requested); the neural component
   has not shown a consistent advantage over the seasonal pattern in the evaluations so far.
10. **Statistical forecasts are eligible in one validated configuration only** (engine `statistical`): a 60-second
    grid, a 600-second lead, the default 90 % target and the seasonal-naive family alone, with default calibration and
    trust settings. Any other grid, lead, target or family set is forecast and scored but stays reactive
    (`ConfigurationNotValidated`): at shorter leads the trust rules either demoted a correctly calibrated forecast too
    often on autocorrelated noise or detected a bad one too slowly, and nothing else has been validated. The rules'
    thresholds are computed under one stated model of the misses and qualified by simulation on named noise models;
    they are not a guarantee for real traffic, they need about 10 hours 40 minutes (64 windows) to detect a bound
    that misses 40 % of the time, and they do not detect a mild sustained undercoverage. The calibration itself is
    not validated for workloads that idle at or near zero (user guide, section 11: coverage about 80 % instead of
    90 % at low discrete load in the acceptance simulations).
11. **No published images, chart or releases yet:** the chart is installed from the source tree (helm.md); images are
    built from source (getting-started.md).

The plan to address these is tracked by the maintainers; items 1–2 come first.
