# Chronological replay of the real accumulated benchmark traffic — 2026-09-22

**Verdict: CENSUS ONLY — NOT SCORED.** The pipeline runs end to end on genuine history; the
history is too short to compare forecasters, and the predeclared rule refuses to try.

> ### ⚠ Corrected 2026-09-22 after Codex round 26 (C-100 / D-126)
>
> Two things in the first version of this file were wrong, and the census numbers below are
> not among them — 174 valid points, 24 origins, 4 blocks and the three ✗ checks all stand.
>
> 1. **The gate could be reached two ways and only a flag said which.** `replay_real.py` took
>    a free `--warmup-days` defaulting to 1 while the scoring rule required 3. On an
>    870-point history that is **720 origins and FAIL** under the default against **432
>    origins and PASS** with three days — the same data, two verdicts. The run is now split
>    into an explicitly `--mode census` (free warmup, **can never** emit SCORED) and an
>    explicitly `--mode scoring` (warmup forced to `MIN_WARMUP_DAYS`, `--warmup-days`
>    refused), `--mode` is required, and each mode has a test at exactly that 870-point
>    boundary. This run was, and remains, a census.
> 2. **"Independent blocks" claimed more than disjoint targets buy.** Non-overlapping blocks
>    still share the history every forecaster reads, the daily period, the generator, and the
>    controller state carried in from the preceding block. The check is renamed
>    `non_overlapping_blocks`, the payload carries a note saying so, and no interval anywhere
>    may be sized from that count. The honest resampling unit stays a block bootstrap over
>    whole days.
>
> The **earliest scoring date is unchanged at ≈ 2026-09-26T19:20Z** — the scoring gate always
> required three warmup days, so correcting the default moved no bar — but §3 now states what
> it is conditional on. The JSON artifact
> [`replay-real-20260921T2330Z.json`](replay-real-20260921T2330Z.json) is left exactly as it
> was written, under the old key names; it is the record of the run, not a live document.

Codex D-124 put this ahead of any further neural-architecture change: every comparison in
this repository so far has run on **synthetic** series, so nothing yet establishes anything
about the traffic the benchmark actually accumulated. This replays that real traffic in
chronological order with rolling origins, and drives the **real Go controller** over it, one
replay per arm.

Code: [`eval/export_benchmark_series.py`](export_benchmark_series.py) (read-only export),
[`eval/replay_real.py`](replay_real.py) (replay). Raw output:
[`eval/replay-real-20260921T2330Z.json`](replay-real-20260921T2330Z.json). Export:
[`eval/data/benchmark-real-20260921T2321Z.json`](data/benchmark-real-20260921T2321Z.json).

---

## 1. How the data was obtained, and what was touched

A **read-only** `GET /api/v1/query_range` against the benchmark cluster's Prometheus over a
temporary `kubectl port-forward`, closed immediately afterwards. **No cluster object was
created, changed or deleted**, nothing was deployed, and the 2026-09-22T12:00Z capture was
not disturbed.

The query and the window are **not** parameters chosen at export time. Both are read from
[`deploy/eks-benchmark/validity-mask.json`](../deploy/eks-benchmark/validity-mask.json),
predeclared before any benchmark training (Codex C-14), so an export cannot quietly widen the
window or change the series:

```
sum(rate(istio_requests_total{reporter="destination",
                              destination_workload="nginx-test",
                              destination_workload_namespace="demo"}[1m])) * 60
```
step 600 s, from the declared `benchmark_history_start` = 2026-09-20T18:20:00Z.

The mask, the ten-minute grid check and the bounded interior-gap fill are applied **by the
trainer's own module** (`ml-engine/data/gapfill.py`), so the replay and the trainer cannot
disagree about which samples are valid.

## 2. The census

| | |
|---|---|
| Raw points returned | 175 |
| Dropped by the validity mask | 1 |
| Off-grid / duplicate / non-finite dropped | 0 |
| **Valid contiguous points** | **174** |
| Imputed (gap-filled) points | **0** |
| Window | 2026-09-20T18:30:00Z → 2026-09-21T23:20:00Z |
| Span | **28.83 h** |
| Eligible rolling origins (1-day warmup) | **24** — 2026-09-21T18:30Z → 22:20Z |
| Origins with all six steps backed by a genuine same-time-yesterday observation | **24 of 24** |
| Non-overlapping origin blocks (origins ÷ 6) | **4** |
| Daily cycles of origins | **0.17** |

**There is no earlier data to recover.** A query before the declared history start returns
nothing for this series: the constraint is the generator redesign of 2026-09-20T18:17Z and the
mask that followed it, not Prometheus retention.

**The trainer still refuses this history** — its own preflight reports *"insufficient history:
have 174 of 235 ten-minute points"* — so **no model has ever been trained on it** and there is
no network arm to replay. Any claim about the neural forecaster on real benchmark traffic
would have no artifact behind it.

## 3. The refusal rule, declared in code before the data was read

`eval/replay_real.py` refuses to report a comparison unless all three hold:

| Check | Required | Observed | |
|---|---|---|---|
| Complete daily cycles of origins | ≥ 3 | **0.17** | ✗ |
| Days of history before the first origin | ≥ 3 | **1.00** | ✗ |
| Non-overlapping origin blocks | ≥ 72 | **4** | ✗ |

The reasoning, in the module docstring rather than invented here: the workload is
daily-periodic, so a window shorter than several whole cycles measures the *time of day*
rather than the forecaster; the deployed pattern component averages up to seven
same-time-yesterday observations and collapses to a different estimator with only one day
back (D-84); and overlapping rolling origins are not independent samples (C-92), so the
honest count is origins ÷ 6.

**Short by 696 points = 116 h ≈ 4.8 days**, recomputed under the corrected gate: scoring needs
`(MIN_WARMUP_DAYS + MIN_ORIGIN_DAYS) × 144 + 6 = 870` contiguous valid points, 174 exist, and
696 × 10 min = 116.0 h from the last sample at 2026-09-21T23:20Z gives
**2026-09-26T19:20Z**.

**That date is a floor, not a schedule** (Codex C-100). It assumes every one of the next 696
ten-minute slots lands on the grid, passes the validity mask, and extends the *same* contiguous
run. It is not automatic in three further ways:

* **Any gap restarts the count.** The 870 points must be contiguous, so a masked interval or a
  scrape gap does not delay the date by its own length — it discards the run before it and
  pushes the date out by up to the whole 116 h again.
* **A mask amendment is retroactive.** Adding an interval to `validity-mask.json` removes
  points already banked.
* **Meeting the gate is permission to score, not a result.** The three checks are this
  repository's protocol choice about when a comparison becomes worth reporting. They are not a
  prohibition on descriptive measurement before then — that is what `--mode census` is for —
  and passing them establishes scale, not that the workload has anything to learn.

## 4. What ran anyway, and why it is not a result

The full pipeline executed: four history-only forecasters at each of the 24 origins from
`series[:i+1]` alone, then the **real Go controller** replayed separately per arm
(`decisions_from: go_controller` for all four).

| Arm | MAE (rpm) | bias | replica-minutes | shortage min | scaling events |
|---|---|---|---|---|---|
| `persistence` | 369.08 | +368.72 | 601.0 | 4.0 | 5 |
| `previous_day` | **2.12** | −1.68 | 611.0 | 2.0 | 4 |
| `seasonal_pattern` | **2.12** | −1.68 | 611.0 | 2.0 | 4 |
| `trend_adaptive` | 2.78 | −0.80 | 611.0 | 2.0 | 4 |

**Do not read that as a comparison.** Three reasons, any one of which is sufficient:

1. **`seasonal_pattern` and `previous_day` are the same estimator here** — identical to two
   decimals at every one of the six horizon steps. With one day of history the seven-day
   weighted percentile has exactly one value to weight, so it returns it unchanged. This is
   the first *direct measurement* of the effect D-84 predicted, and it means the two arms are
   not independent comparators on this window.
2. **The traffic is generated, and the generator is nearly deterministic.** An MAE of 2.12 rpm
   against a level of roughly 4 000 rpm is not forecasting skill; it is a measurement of how
   exactly the k6 generator repeats its daily profile. Anything that looks up yesterday's
   value wins by construction. `persistence` scores badly only because these 24 origins sit on
   a ramp.
3. **Twenty-four overlapping origins spanning four hours of one evening** cannot separate
   forecasters from the time of day they were sampled at.

## 5. What this does establish

* The real accumulated series **runs end to end** through the deployed forecasters and the
  real Go controller, chronologically, with rolling origins and no look-ahead. The hygiene is
  enforced by tests that change the *future* and require the forecast not to move
  (`ml-engine/tests/test_replay_real.py`).
* **How much genuine history exists after the validity mask** — 174 points, 0 imputed, one
  contiguous run — and how many origins it yields.
* **Which arms are even defined** on this much history, and that the pattern arm currently
  degenerates into the previous-day arm.
* That the neural forecaster **has no artifact on this data at all**.

## 6. What it cannot establish

* Which forecaster is better — too few whole daily cycles, and non-independent origins.
* A sample size. The 4 blocks are non-overlapping, not independent: they share history, daily
  structure and carried controller state (C-100).
* Anything about the neural arm — no model has ever trained on this history.
* An operational benefit — the replay models capacity arriving after a readiness delay and
  prices no queueing, latency or dropped request.
* Generalisation beyond one workload, one cluster and one generator design.

## 7. Next

1. **Let the history accumulate, then ask for a score explicitly.** Re-export after
   ~2026-09-26T19:20Z and re-run with **`--mode scoring`** — which is a different command from
   the one that produced this census, deliberately (C-100). Re-running the census command can
   never produce a score however much history exists. The rule itself is unchanged.
2. **Re-export rather than extend.** Each export carries the mask version and its SHA-256, so
   a later export is a new artifact, not an edit of this one.
3. **When it scores, it scores in operating units first** — shortage and replica-minutes from
   the controller replay — with the cost proxy as a screening statistic only
   (`TOLERANCE-DERIVATION.md` §4 — the proxy-to-shortage conversion is withdrawn and no
   margin may use it).
4. **Watch for a mask amendment.** Any new interval added to `validity-mask.json` shortens the
   contiguous run and pushes the date out; the census reports `missing_slots` and
   `contiguous_runs` precisely so that is visible rather than silent.

## Reproducing

```sh
kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 19090:9090 &
eval/.venv/bin/python eval/export_benchmark_series.py \
    --base-url http://127.0.0.1:19090 --out eval/data/benchmark-real-<utc>.json
kill %1

# what produced THIS file: descriptive, cannot score, free warmup
eval/.venv/bin/python eval/replay_real.py --mode census --warmup-days 1 \
    --export eval/data/benchmark-real-<utc>.json --out eval/replay-real-<utc>.json

# what a scored run will be, once 870 contiguous valid points exist: warmup fixed at 3 days
eval/.venv/bin/python eval/replay_real.py --mode scoring \
    --export eval/data/benchmark-real-<utc>.json --out eval/replay-real-<utc>.json
```

`--mode` is required; there is no default, so neither mode can be reached by accident.
The export is read-only; the replay is entirely offline and never contacts the cluster.
