# Synthetic warm-start experiment (offline harness approximation)

**Not benchmark evidence.** This answers one descriptive question while the live arms are still collecting real
history: *what would each forecasting method have done on the real prodcluster observations if it had started with
14 days of history?* The live arms, their validity mask, their models and their history are not touched.

## What it does
1. Reads the real observed request rate of one app (`nginx-test` by default) on the 10-minute grid from the history
   start (2026-10-05T06:40Z) to `--end`, with the campaign's canonical MetricsQL expression. The input fails closed:
   one series, no partial response, every grid instant present and finite, else the run stops.
2. Generates 14 days of synthetic history with the challenge-v1 process (`challenge_bakeoff.offered_rates` +
   `sampled_series`) from a **fresh seed**; the sealed seed is refused.
3. Concatenates synthetic + real and evaluates on the real origins only:
   S1 (serving-path pattern, directional p70/p75, MAPE term 0), E1 (seasonal ensemble + q90 margin), E2 (+ q95),
   the deployed BiLSTM trained on the synthetic rows only, blended with the pattern (deployed ramp, 0.85, 0.95) and
   alone, reactive only, and an oracle.
4. Replays the harness capacity rule slot by slot (asserted equal to `challenge_bakeoff.replay`).

## Limits (Codex r28)
Harness approximation: no confidence damping, scheduling or readiness delays; the neural harness fits its scaler on
all synthetic rows (production fits before its validation boundary); S1's MAPE-adaptive term is 0. Synthetic days
dominate the pattern and the ensemble profile for the whole real window and calibrate the first margins. The seeds
share one real evaluation window, so they are not independent live tests. Zero replay shortage in a short window
is not a reliability guarantee. Requirements are clipped to 12 replicas; `uncapped_required_over_max` counts slots
where real demand exceeded that.

## Run
```
./eval/.venv/bin/python deploy/prodcluster/experiments/synthetic-warmstart/run_synthetic_warmstart.py \
  --prom http://127.0.0.1:18481/select/0/prometheus --app nginx-test --end 2026-10-06T20:30:00Z --seed 101 \
  --json out.json --import-file out.prom
```
The JSON carries the run identity (git head, dirty paths, file hashes, library versions, real-input hash), the
per-method totals and per-slot traces. The optional import file holds `bench_synth_*` series labelled
`experiment="synthetic-warmstart-1-s<seed>-n<net seed>-<run time>"` for the Grafana dashboard in `dashboard.json`
(uid `pa-synth-experiment`); no consumer of the campaign reads these names.
