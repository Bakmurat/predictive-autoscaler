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
6. **Operator state in memory:** cooldowns, stabilization windows and forecast caches are lost on restart, and when
   leadership moves to another instance.
7. **Unauthenticated forecasting API:** the Helm chart gives each component its own least-privilege service account,
   and its NetworkPolicies admit only the operator to the forecasting service. The API itself does not authenticate
   callers yet (`/train` is disabled). The manifests in `k8s-manifests/` still share one broad role (see helm.md).
8. **Silent configuration mistakes:** ignored CRD fields (CPU, memory, resources, container), defaults that differ from
   the README, and `metrics.requests.enabled` defaulting to `false` (see configuration.md).
9. **Heavy footprint:** the forecasting image includes TensorFlow (about 1 CPU / 1 GiB requested); the neural component
   has not shown a consistent advantage over the seasonal pattern in the evaluations so far.
10. **No published images, chart or releases yet:** the chart is installed from the source tree (helm.md); images are
    built from source (getting-started.md).

The plan to address these is tracked by the maintainers; items 1–2 come first.
