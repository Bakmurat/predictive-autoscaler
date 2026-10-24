# Limitations (current version)

This is a research prototype that has run only in test environments with synthetic, generated load. It is not
production-ready. Known limits, most important first:

1. **Missing metrics can scale down.** A metrics query that returns an empty or unsuccessful result is read as zero
   traffic; without a forecast the desired count then falls to `minReplicas` (see configuration.md, Decision rule).
2. **No protection against a second autoscaler** on the same Deployment (see coexistence.md).
3. **Istio is required** for the request-rate signal (`istio_requests_total{reporter="destination"}`), and the queries
   are not configurable.
4. **One workload per trainer.** The training CronJob trains one workload (`TRAINING_WORKLOAD`/`TRAINING_NAMESPACE`), and
   models are keyed by application name only, so the same name in two namespaces collides. A workload without a model
   runs reactive-only, silently.
5. **Single forecasting replica on a ReadWriteOnce volume:** the API and the trainer must run on the same node; the API is
   a single point of failure (the operator falls back to the reactive rule when it is down).
6. **Operator state in memory:** cooldowns, stabilization windows and forecast caches are lost on restart; leader
   election is off by default.
7. **Broad permissions:** one cluster-wide role is shared by the operator and the forecasting API, and the API's
   `/predict` and `/train` endpoints are unauthenticated.
8. **Silent configuration mistakes:** ignored CRD fields (CPU, memory, resources, container), defaults that differ from
   the README, and `metrics.requests.enabled` defaulting to `false` (see configuration.md).
9. **Heavy footprint:** the forecasting image includes TensorFlow (about 1 CPU / 1 GiB requested); the neural component
   has not shown a consistent advantage over the seasonal pattern in the evaluations so far.
10. **No published images, chart or releases yet;** see getting-started.md for building and installing from source.

The plan to address these is tracked by the maintainers; items 1–2 come first.
