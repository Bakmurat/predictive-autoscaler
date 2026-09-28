# E2: the E1 forecaster with a q95 capacity margin (Task 03 U-23)

Sixth benchmark arm, chosen by the user on 2026-09-28 after the offline bake-off
(`deploy/eks-benchmark/scoring/challenge_bakeoff.py`, agent-coordination D-1055): on the challenge-v1
process every good forecaster ties, and the margin quantile is the one lever with a visible effect.
E2 serves **exactly the E1 forecast** (0.5 additive Holt-Winters + 0.5 seven-day profile with AR(3)
residual, refit on 00/06/12/18 UTC, computed on `demo/nginx-test`'s history) and adds the **95th**
percentile of its own trailing-24 h lead-window errors instead of the 90th. Offline (5 fresh seeds,
14 challenge days): shortage 8.0 vs 15.6 replica-minutes, surplus 8,840 vs 7,464. Live, on identical
traffic, the two arms measure that trade directly.

## Configuration

`ENSEMBLE_EXPERIMENT` on the ml-api Deployment now holds a JSON **list** (`api-config.json`): the q90
arm unchanged and the q95 arm with `"margin_quantile": 0.95`. `seasonal-ensemble-1.1.0` takes the
quantile per call; the default path is numerically the 1.0.0 behaviour (tests
`test_q95_margin_is_at_least_the_q90_margin_and_recorded`, `test_api_serves_each_ensemble_app_with_its_own_quantile`).
Both arms share one generation cache (same source), so the refit cost is paid once. Every issuance
logs `ENSEMBLE_ISSUANCE` with `experiment` and `margin_quantile`; `model_version` is
`seasonal-ensemble-q95-v1:<config sha12>:seasonal-ensemble-1.1.0@<fingerprint12>`.

## Controller, bounds, placement

Same PredictiveAutoscaler as E1: bounds 1..12, `targetRPS` 10, lead window 20 minutes, reconcile 60 s.
The app spreads over all three nodes (4 + 4 + 4 at the ceiling; see the deployment's comment for the
node-budget numbers). Its generator joins the others on the labelled generator node and mounts
`k6-load-script-challenge-v1`; because the offered rate is a pure function of wall-clock time and the
sealed seed, the sixth generator offers the same traffic as the other five from its first slot.

## Deployment order

1. Build the API as a thin image over the deployed digest from `git archive` of the pushed commit;
   keep the build log as `images-<commit>.log`; push; `image-provenance.py --require-observed`.
2. Wait for any running qualification to collect and freeze (a rollout voids the declared hours).
3. Run the archive job by hand (`kubectl create job --from=cronjob/evidence-archive …`) and wait for it,
   so the API log before the rollout is archived.
4. Apply the service, app, generator (`VM_WRITE_URL` substituted) and PredictiveAutoscaler.
5. Patch ml-api: image digest, `IMAGE_REF`, `GIT_COMMIT`, `ENSEMBLE_EXPERIMENT` = `api-config.json`
   (single line). Strategy is Recreate; expect ~45 s without forecasts (operators fall back to reactive).
6. Verify: `/predict` for `nginx-ensemble-q95` returns `forecast_mode: seasonal-ensemble-q95`; the
   E1 route still returns q90; `ml_api_ensemble_margin_rpm{application="nginx-ensemble-q95"}` present;
   `node-budget.py --requirement` still FITS.
7. Declare a fresh six-arm qualification (two consecutive clean hours after the rollout).

## Rollback

Patch `ENSEMBLE_EXPERIMENT` back to the single q90 object (the 1.1.0 image accepts both forms),
delete the q95 PredictiveAutoscaler, generator, app and service. E1 is unaffected.

## seasonal-ensemble-1.2.0 options (2026-09-28, from the model lab's results; not yet used by any live arm)

Each experiment may also declare `"margin_mode": "relative"` (the margin is the lead times the quantile of past
`max(a10, a20) / lead − 1`, so it scales with the load; default `"absolute"`, the 1.0.0 rule) and
`"partial_rule": "finite"` (a step whose Holt-Winters or profile-AR component is missing is served from the finite
component alone; default `"refuse"`). Offline on the challenge process (lab report
`agent-coordination/07-jupyter-model-lab/LAB-RESULTS-20260928.md`) the relative q90 margin cut E1's shortage by 8–22
replica-minutes per 14 days in 13 of 14 configurations with intervals excluding zero, for less surplus than q95, and
the finite rule removed the 5–8× loss under component failure. `forecast_mode` becomes e.g.
`seasonal-ensemble-rq90-finite`; every issuance logs `margin_mode`, `partial_rule` and `served_components`.
Switching a live arm to these options is a configuration change plus an API image and a re-qualification.
