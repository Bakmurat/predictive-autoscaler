# Offline evaluation

An honest answer to one question: **does the neural network add anything over a trivial
baseline on this workload?**

The harness drives the repository's own training and inference code — the same
`LSTMForecastModel`, the same blend, the same operator decision rule — so what it measures
is what is deployed, not a reimplementation.

## Why this exists

The project's central claim is that a demand forecast lets capacity arrive before the load.
That claim is only worth making if the forecast is better than what you get for free. The
cheapest useful forecast for a workload with a daily rhythm is "whatever happened at this
time yesterday". If the network cannot beat that, the network is not earning its place.

## What it compares

Six predictors, each seeing exactly the same information at each origin:

| Predictor | What it is |
|---|---|
| `served_blend` | what the deployed service returns: network blended with the pattern |
| `network_only` | the LSTM with the blend disabled (ablation) |
| `seasonal_pattern` | the repository's own pattern component alone |
| `previous_day` | the value observed at the same clock time yesterday |
| `persistence` | the last observed value, repeated |
| `trend_adaptive` | previous-day scaled by the recent level ratio, damped |

## Method

- **Rolling origins.** At each origin the predictor sees history up to and including that
  origin, and is scored against the six ten-minute steps that follow.
- **Training and publication delay.** A model is retrained on a six-hour schedule, matching
  the CronJob, and only becomes usable after a publication delay. Before the first
  publication the model-based predictors produce nothing — the cold start is real.
- **Scenario families.** A repeating daily profile (what the benchmark generates), plus
  trend, level shift, unpredictable spike, and a weekly pattern. Multiple seeds.
- **Metrics.** Per-step MAE and signed bias are primary; MAPE is reported but secondary
  because it misbehaves near zero.
- **Operational replay.** Each predictor's forecasts are fed through the operator's real
  rules — `max(forecast, reactive, minimum)`, the overestimate cap, the scale-down
  stabilisation window, the cooldown and the bounded step — with a readiness delay, so a
  scale-up decided now only serves traffic two minutes later. Reported: replica-minutes
  consumed, scaling events, and the magnitude and duration of any capacity deficit against
  what the reactive rule would have required.

## Decision rule, preregistered

The network is worth keeping if it delivers **at least 10% lower MAE than the strongest
baseline**, repeatably across days and scenarios, *and* an operational benefit at a matched
resource budget. One favourable trace is not enough.

## What simulation cannot show

Replay cannot prove a latency improvement. It models capacity arriving after a readiness
delay; it does not model queueing, connection behaviour, or user-visible response time.
A latency claim needs a live experiment with a latency measurement, which this is not.

## Running it

```sh
python3.12 -m venv eval/.venv
eval/.venv/bin/pip install "numpy>=1.26,<2.0" "pandas>=2.1" "scikit-learn>=1.3" "tensorflow>=2.15"

eval/.venv/bin/python eval/offline_eval.py --quick           # smoke, about a minute
eval/.venv/bin/python eval/offline_eval.py --full            # the reported run
eval/.venv/bin/python eval/offline_eval.py --failure-modes   # robustness checks
```

Results land in `eval/results.json`; the written-up findings are in `eval/RESULTS-*.md`.

`eval/data/benchmark-nginx-test.json` is a read-only export from the benchmark cluster's
Prometheus, kept so the real-data run is reproducible.

## The selector experiment

`eval/selector.py` is a separate offline experiment (D-116) asking a different question: given
that the best cheap forecaster flips between `seasonal_pattern` and `trend_adaptive` depending on
the workload, does an online selector that serves whichever has been cheapest lately beat serving
a fixed one? It adds a cheap lagged-linear candidate, uses a constant blend as a control arm,
preselects every parameter on validation seeds and dates the test set does not contain, and
replays the real Go controller separately for each arm. The ranking metric is a documented
asymmetric **absolute**-error proxy, not operating cost.

```sh
eval/.venv/bin/python eval/selector.py --out eval/selector-20260922.json   # ~4.5 min
eval/.venv/bin/python eval/selector.py --no-replay                          # accuracy only
```

Findings: `eval/SELECTOR-RESULTS-2026-09-22.md`. It did not clear its predeclared bar.

## Chronological replay of the real benchmark traffic

`eval/export_benchmark_series.py` exports the canonical series from the benchmark cluster's
Prometheus — a single read-only `query_range`, with the query and window read from the
predeclared `deploy/eks-benchmark/validity-mask.json` rather than chosen at export time.
`eval/replay_real.py` then replays that genuine history offline: rolling origins in
chronological order, every forecaster seeing only `series[:i+1]`, and the real Go controller
replayed separately per arm.

It **refuses to score** below a minimum scale declared in the module before the data is read
(three whole daily cycles of origins, three days of warmup, seventy-two non-overlapping origin
blocks) and prints a census instead. As of 2026-09-22 the history is 174 valid points and
yields 24 origins, so the run is a census.

**`--mode` is required and has no default** (Codex C-100). `census` runs descriptively with a
free warmup and can **never** emit a score; `scoring` forces the warmup the gate requires and is
the only mode that can. The same 870-point history gives 720 origins in census mode and 432 in
scoring mode, so the two are kept apart by the mode rather than by a flag. The seventy-two
blocks are **non-overlapping, not independent** — they share history, daily structure and
carried controller state, so no interval may be sized from that count.

```sh
kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 19090:9090 &
eval/.venv/bin/python eval/export_benchmark_series.py --base-url http://127.0.0.1:19090 \
    --out eval/data/benchmark-real-<utc>.json

# descriptive, cannot score:
eval/.venv/bin/python eval/replay_real.py --mode census \
    --export eval/data/benchmark-real-<utc>.json --out eval/replay-real-<utc>.json

# a scored run, once 870 contiguous valid points exist (earliest ~2026-09-26T19:20Z):
eval/.venv/bin/python eval/replay_real.py --mode scoring \
    --export eval/data/benchmark-real-<utc>.json --out eval/replay-real-<utc>.json
```

Findings: `eval/RESULTS-real-traffic-replay-2026-09-22.md`.

## Other files here

| File | What it is |
|---|---|
| `reproduce_divergence.py` | the arms re-run under the conditions that originally failed; `repair_divergence_record.py` rebuilds its record from the console log (Codex C-95) |
| `PROTOCOL-seasonal-default-selector.md` | the predeclared design of the next selector experiment — written, **not run** |
| `TOLERANCE-DERIVATION.md` | the non-inferiority margin as a **decision for the owner**: the three quantities it must come from, three worked options, and **none in force**. Records that the old 2 % was underived and that the 21.3 % that briefly replaced it is withdrawn |
| `PROTOCOL-clipping-resolution.md` | one bounded matched-seed rolling-retraining experiment to settle whether gradient clipping earns a place in the serving pipeline — written, **not run**; clipping is not adopted |
| `PROTOCOL-challenge-workload.md` | the separately versioned `challenge-v1` generator profile — bounded correlated noise, drift and level shifts, after the capture and the repaired deployment — written, **not run, nothing deployed** |
