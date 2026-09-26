# Offline notebook forecasting lab

`notebook_lab.py` compares models at identical rolling origins and target times. It
is optional research tooling; it changes no served model, operator or qualification
criterion. Keep private observations, trained models and reports outside this repository.

The input CSV has `Time` and `Expected` columns. Targets are **exact ten-minute
snapshots**, not ten-minute averages. Original naive clock timestamps are preserved;
no timezone is inferred. Inputs may be forward-filled from preceding observations.
Neural labels and scoring targets are never filled. Each scored origin needs all six
observed targets, and that exclusion applies to every arm.

Candidates include persistence, yesterday, the repository's seven-day weighted
pattern at a fixed 70th percentile, a fixed median-ratio adjustment, the existing
damped trend predictor, ARIMA(5,1,2), additive Holt–Winters with period144, Prophet,
and a compact direct six-output tanh LSTM. These are explicit forecasting pipelines,
not a claim about every possible configuration of each model family.

Parameters normally refit every six hours on a seven-day window. ARIMA advances its
state through new observations. Holt–Winters replays its state with fixed parameters
between fits. Prophet's six-hour arm only learns observations at refits; the separate
`prophet_origin` arm refits at every origin. LSTM inference uses updated history;
its last training day is chronological validation, training labels are purged at the
boundary, and its scaler fits earlier observations only. Two seeded fits expose some
initialization variation. The experiment models **zero publication delay**, clamps
all predictions at zero and does not round them to replicas. It does not replay
controller delays, readiness, failure recovery or operational capacity cost.

Use an isolated Python environment with numpy, pandas, TensorFlow, scikit-learn,
statsmodels and Prophet. It is distinct from the production ML environment. The run
records imported package versions, source/data hashes and per-fit losses and warnings.

```sh
/path/to/lab-python eval/notebook_lab.py \
  --csv /private/observations.csv \
  --protocol /private/frozen-protocol.json \
  --out /private/new-run-directory
```

The protocol supplies `csv_sha256`, `source_sha256` (paths relative to `eval/`),
`datasets`, `data_seed`, `model_seeds`, `epochs`, `adaptive_k`, and `classical`.
Optional fields are `dataset_sha256`, `first_scored_day` (default12),
`include_existing_trend_adaptive`, and `origin_refit_prophet`. Source hashes cover
`notebook_lab.py`, `offline_eval.py`, and `../ml-engine/models/lstm_model.py`.
Never overwrite a run; freeze settings before examining its outcomes.

Built-in synthetic cases reuse `offline_eval.make_series` with a declared14-day
calendar and noise seed: repeating, trend, a1.55× level shift at day12.5, and two
2.5× three-slot bursts at days12.5 and13.5. The second burst repeats at yesterday's
clock time; it is not another unpredictable event. Compare the bursts separately.

`predictions.csv` retains origin, exact target, horizon, actual and forecast;
`fits.json` retains fit times and warnings; LSTM artifacts include weights and
training history. `scores.csv` reports MAE, signed bias (positive = overprediction),
RMSE and unavailable counts. An unavailable forecast makes that arm/horizon's full
score incomplete. Do not hide failures by calling a restricted subset a complete
score. A failed refit makes that arm unavailable, rather than serving its old model.

Historical data that already informed model development is exploratory evidence.
Different target units must never be pooled. A favourable offline result requires a
separate prospective experiment before changing live model policy.
