# E1: seasonal ensemble with a q90 capacity margin

This opt-in, pre-T0 arm adds `demo/nginx-ensemble` alongside the four existing workloads
(neural/seasonal hybrid, reactive-only, KEDA and the seasonal-only S1 arm). It does not
change their algorithms, bounds or traffic. It makes no performance claim; any formal
scoring needs a later human T0 decision and a protocol covering the participating arms.

## What it forecasts

`ml-engine/models/seasonal_ensemble.py`, selected by the API's `ENSEMBLE_EXPERIMENT`
environment variable (the JSON object in `api-config.json`; unset = off, malformed =
startup failure). For each of six ten-minute steps:

- **Holt-Winters**: additive trend and additive daily season (144 slots), fitted by
  least squares with heuristic initial states, trailing 14 days before the refit boundary.
- **Profile + AR(3)**: the mean of the same slot on the previous seven days (observed
  values only) plus an AR(3) model of the residual, fitted on the trailing seven days.
- **Ensemble**: 0.5 × each. Both components refit on 00:00/06:00/12:00/18:00 UTC and
  advance their state with every observation in between. An invalid refit falls back once
  to the previous boundary's generation, then the API refuses.
- **Margin**: the 0.90 quantile of this forecaster's own lead-window errors,
  max(actual +10, +20) − max(forecast +10, +20), over the trailing 24 hours of matured
  ten-minute ticks. It needs at least 30 samples, else the margin is zero, and it is
  clipped to [0, 0.8 × lead-window forecast]. The served value is forecast + margin on
  every step. The user prefers over-provisioning to shortage (decision 2026-09-27), and
  the q90 margin comes from that choice.

The forecaster is stateless per request: generations and past forecasts are recomputed
from the source history, so a restart reproduces the same output. History is
`demo/nginx-test`'s observed request rate (shared input; the k6 schedule is identical),
360 hours, with the validity mask applied. A partial forecast is refused with HTTP 422,
which the operator maps to its reactive rule. Confidence is served as 0.95, so the
operator's confidence damping (below 0.7) never applies to this arm.

The response carries `ensemble.raw` (margin-free), `ensemble.hw`,
`ensemble.profile_ar`, `ensemble.margin`, the margin's sample count and the generation
(boundary, fitted parameters, input fingerprint). The operator records the served
predictions, target times and `artifact_sha256` (= the generation fingerprint).
Prometheus carries `ml_api_prediction_rpm{component=~"ensemble_.*|final"}` and
`ml_api_ensemble_margin_rpm`. The API logs one `ENSEMBLE_ISSUANCE` JSON line per
forecast.

## Controller and bounds

The unchanged operator owns the Deployment: `max(forecast replicas, reactive replicas,
min)` with its overestimate cap, stabilisation and cooldown, `targetRPS 10`, bounds
1..12, lead window 20 minutes. These are the same as the other predictive arms. The
benchmark peak (6,000 req/min) needs about 10 pods; the user kept the ceiling at 12.

## Placement (differs from the four original arms)

At every arm's 12-pod ceiling, the ap-southeast-1a node (Prometheus plus three
generators) has 176 MiB of requests left, too little for four more 64 MiB pods. Letting
this arm take that room would push the original arms' strict spread into Pending pods.
`nginx-ensemble` is therefore restricted to the two ap-southeast-1b nodes, with an even
spread across them (at most six per node at its ceiling). Its generator runs on the node
labelled `predictive-bench/ensemble-generator=true`, never with the API.

## Evidence

The arm's issuance record carries the generation fingerprint as `artifact_sha256` (there is no
model file). The API logs one `ENSEMBLE_ISSUANCE` JSON line per served forecast with the same
fingerprint; the in-cluster archiver preserves those lines (`logs/api-ensemble.jsonl`), and the
archive reconciler verifies each issuance against a logged fingerprint within five minutes.
Lines the archiver did not see are not evidence, so **run the archive job by hand and wait
for it before replacing the API pod**; a Recreate rollout between two fifteen-minute runs
loses the old pod's last lines.

## Deployment order

1. Build the API as a thin image over the deployed digest, using the reviewed source only,
   and keep the build log as `images-<commit>.log`.
2. Run the archive job (`kubectl create job --from=cronjob/evidence-archive …`) and wait for
   it, then render the live API Deployment with only the image and `ENSEMBLE_EXPERIMENT` changed.
   Inspect `kubectl diff`, keep `strategy: Recreate`, and verify the existing arms'
   forecasts after the rollout.
3. Label the generator node, then apply the service, app, generator (with `VM_WRITE_URL`
   substituted) and PredictiveAutoscaler. Verify the first forecasts, decisions and
   scaling.

An API or node change restarts qualification; any T0 needs fresh clean hours and a
fresh freeze.

## Rollback

Delete the generator, PredictiveAutoscaler, app and service, remove the node label, and
restore the previous API Deployment. Never delete the model or evidence volumes.

## Since 2026-09-28 (U-23)

`ENSEMBLE_EXPERIMENT` may hold a JSON list; the q95 sibling arm lives in `../seasonal-ensemble-q95-v1/` and shares this arm's source history and generation cache. This arm's configuration and behaviour are unchanged.
