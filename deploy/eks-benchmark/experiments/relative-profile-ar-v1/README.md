# Arm R1: relative-residual profile-AR forecaster with a relative q90 margin

Declared 2026-09-29 for the AWS benchmark (Task 03). R1 replaces the supplemental S1 arm
(`nginx-seasonal`, seasonal pattern only) **after** at least one complete UTC day in which the
hybrid arm and S1 served identical forecasts (the hybrid has served its pattern alone since
2026-09-28T18:14Z, so S1 duplicated it; that duplicate day is the live A/A noise reference the
scoring keeps). The workload, generator and PredictiveAutoscaler of `nginx-seasonal` are unchanged;
only the API's routing for that application changes.

## What R1 is

`ml-engine/models/relative_profile_ar.py`, version `relative-profile-ar-1.0.0`, a port of the model
lab's `pr_ar_rob` (specification and test vectors of 2026-09-29, reproduced to 1e-6 by
`ml-engine/tests/test_relative_profile_ar.py`):

* relative residual `q[t] = y[t] / profile(t) - 1` against the seven-day same-slot profile;
* one AR(3) with intercept per 00/06/12/18Z generation, fitted by least squares on the trailing
  seven days of `q` with the training copy clipped to +/- 50 %; at least 36 rows;
* the recursion is seeded with the unclipped residuals at the origin and the two slots before it;
  `y_hat = max(0, profile x (1 + q_hat))` for the six ten-minute steps;
* margin: relative q90 (`margin_mode: relative`) of **this forecaster's own** past lead errors over
  the trailing 24 h (>= 30 samples, clip [0, 0.8]);
* no Holt-Winters, no optimiser, one component (no partial rule): a step whose target profile is
  missing is refused; an invalid generation falls back once to the previous boundary.

Why this arm: it is the genuinely different forecaster (E1 and E2 already share the HW + profile-AR
ensemble), it separates the forecaster question (R1 vs E1 on raw MAE, margin-independent) from the
margin question (R1 relative q90 vs E2 absolute q95 on replica outcomes), and it costs a tenth of
E1's refit. Offline (14 seeds, challenge-v1 process) it ties E1 on shortage under the relative
margin and is better inside bursts; slightly weaker after a down-shift.

## Configuration

`api-config.json` is the full `ENSEMBLE_EXPERIMENT` list for the API: E1 (q90), E2 (q95) unchanged
and the new entry

```json
{"id": "relative-profile-ar-rq90-v1", "application": "nginx-seasonal", "namespace": "demo",
 "source_application": "nginx-test", "source_namespace": "demo",
 "forecaster": "relative-profile-ar", "margin_mode": "relative", "margin_quantile": 0.9}
```

At rollout the API's `SEASONAL_EXPERIMENT` (S1's seasonal-pattern route for the same application)
is removed; the ensemble route takes precedence anyway, but a shadowed setting is a lie in the
provenance. `forecast_mode` is `relative-profile-ar-rq90`; the issuance log line
(`ENSEMBLE_ISSUANCE`) carries `"forecaster": "relative-profile-ar"`, so the archive's ledger keeps
E1, E2 and R1 apart by `experiment` and `forecaster`.

The E1/E2 `config_sha256` values are unchanged by the new field (pinned in the tests); their
`model_version` strings do change at this rollout because the forecaster module's version
(`seasonal-ensemble-1.2.0`, defaults unchanged, outputs identical) replaces the deployed 1.1.0.

## Rollout (archive first, Recreate)

1. Run the evidence-archive job by hand and confirm the pair before replacing the API pod.
2. Set `ENSEMBLE_EXPERIMENT` to the contents of `api-config.json`, delete `SEASONAL_EXPERIMENT`,
   point the Deployment at the new thin image digest (built from the pushed commit over the
   previous digest; build log kept as `images-<commit>.log`), strategy Recreate.
3. Post-rollout gates: all six arms delivering, `nginx-seasonal` issuances now `relative-profile-ar`,
   margin samples reach 30 within about 5 h (the margin window is the forecaster's own leads, so R1
   serves with margin 0 until then), Grafana labels updated (S1 -> R1), capacity report label updated.
4. Re-qualify: two declared consecutive clock hours on all six arms (the qualification runner).

The A/A record: S1 = hybrid from 2026-09-28T18:14Z until the rollout; the scoring notebook treats
`nginx-seasonal` before the rollout timestamp as S1 and after it as R1.
