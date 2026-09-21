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
