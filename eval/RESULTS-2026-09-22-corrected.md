# Corrected evaluation results — 2026-09-22

Supersedes the tables in [`RESULTS-2026-09-21.md`](RESULTS-2026-09-21.md), whose numbers were
**withdrawn**: its stress scenarios never entered the scored window and its operational
replay was a flawed Python approximation of the controller. Read that withdrawal note first
if you have seen the earlier figures. Nothing from it is carried forward here; every number
below comes from a fresh run of the rebuilt harness.

## Verdict

**The neural component does not earn its place on any workload tested, and the gap is not
close.** Across ten runs it failed the preregistered bar — 10% lower MAE than the strongest
baseline — **twenty times out of twenty** (both the raw network and the served blend, in
every run). Ratios to the strongest baseline run from **0.98 to 4.16**; the single best case
is a 2% improvement, against a 10% requirement.

**What the best forecaster is depends on the workload**, which is the more interesting
result and the one the withdrawn run could not have found:

- steady daily traffic → the **seasonal pattern** (this project's own arithmetic component)
- gradual trend, level shift, weekly cycle → the **trend-adaptive** baseline
- neither is the neural network, in any scenario

**Not established:** anything about real traffic (all series are synthetic), anything about
latency (the replay models capacity, not response time), and whether the network's
across-seed divergence is fixable — that is the separate stabilisation experiment, still
running at the time of writing.

## Method

| Setting | Value |
|---|---|
| Training | 50 epochs (the trainer's default), early stopping patience 15 |
| History | rolling **7-day** window, as the deployed trainer uses |
| Retraining | every 6 hours, matching the CronJob, with a 20-minute publication delay |
| Origins | 400 per run, **centred on the scenario's event** |
| Pairing | an origin is scored only when **every** arm produced a finite forecast |
| Series | 12 days at 10-minute resolution, the generator's **hourly step profile** |
| Data seeds | 1, 2 (recorded separately from the model seed) |
| Model seed | 101 (TensorFlow seeded; this run holds it fixed) |
| Capacity calibration | pre-origin portion only |
| Controller decisions | the **real Go controller**, via `controllers/replay_harness_test.go` |

Every run passed `assert_window_covers_events()`; none was skipped. Event coverage:

| Scenario | seed | scored target range | events in series | events inside window |
|---|---|---|---|---|
| repeating | 1 | 289-693 | 0 | n/a (no event by design) |
| repeating | 2 | 289-693 | 0 | n/a |
| trend | 1 | 405-809 | 1 | 1 |
| trend | 2 | 405-809 | 1 | 1 |
| levelshift | 1 | 578-982 | 1 | 1 |
| levelshift | 2 | 578-982 | 1 | 1 |
| spike | 1 | 847-1251 | 3 | 2 |
| spike | 2 | 631-1035 | 3 | 2 |
| weekly | 1 | 593-997 | 12 | 2 |
| weekly | 2 | 593-997 | 12 | 2 |

Reproduce:

```sh
eval/.venv/bin/python eval/offline_eval.py --full --epochs 50 --cap 400 \
    --model-seed 101 --out eval/results-corrected.json
```

Raw output: `eval/results-corrected.json`.

## Accuracy — MAE, lower is better

| Scenario | data seed | origins scored | served blend | network only | seasonal pattern | previous day | persistence | trend adaptive | strongest |
|---|---|---|---|---|---|---|---|---|---|
| repeating | 1 | 398/400 | 175.9 | 535.6 | **146.3** | 159.8 | 347.1 | 166.5 | seasonal pattern |
| repeating | 2 | 398/400 | 163.6 | 439.4 | **144.8** | 151.7 | 333.4 | 159.6 | seasonal pattern |
| trend | 1 | 398/400 | 280.8 | 816.4 | 219.3 | 232.8 | 424.9 | **196.1** | trend adaptive |
| trend | 2 | 398/400 | 267.5 | 666.2 | 219.1 | 230.2 | 401.4 | **183.8** | trend adaptive |
| levelshift | 1 | 398/400 | 859.4 | 1245.7 | 851.9 | 881.7 | 464.2 | **397.9** | trend adaptive |
| levelshift | 2 | 362/400 | 812.3 | 1285.3 | 795.9 | 810.9 | 469.5 | **395.1** | trend adaptive |
| spike | 1 | 398/400 | 252.1 | 410.2 | **244.3** | 251.6 | 394.4 | 316.7 | seasonal pattern |
| spike | 2 | 398/400 | **365.1** | 495.4 | 371.3 | 374.8 | 491.7 | 470.7 | served blend |
| weekly | 1 | 362/400 | 685.6 | 621.3 | 730.1 | 717.2 | 319.6 | **258.7** | trend adaptive |
| weekly | 2 | 326/400 | 773.9 | 825.4 | 794.1 | 775.4 | 301.7 | **265.9** | trend adaptive |

Between 326 and 398 of 400 origins were scored in each run; the shortfall is the cold start
(no published model yet) plus paired-origin exclusions, and every exclusion applies to all
arms equally.

## The preregistered bar

| Scenario | seed | arm | arm MAE | strongest baseline | baseline MAE | ratio | bar (<=0.90) |
|---|---|---|---|---|---|---|---|
| repeating | 1 | served blend | 175.9 | seasonal pattern | 146.3 | 1.20 | fail |
| repeating | 1 | network only | 535.6 | seasonal pattern | 146.3 | 3.66 | fail |
| repeating | 2 | served blend | 163.6 | seasonal pattern | 144.8 | 1.13 | fail |
| repeating | 2 | network only | 439.4 | seasonal pattern | 144.8 | 3.03 | fail |
| trend | 1 | served blend | 280.8 | trend adaptive | 196.1 | 1.43 | fail |
| trend | 1 | network only | 816.4 | trend adaptive | 196.1 | 4.16 | fail |
| trend | 2 | served blend | 267.5 | trend adaptive | 183.8 | 1.46 | fail |
| trend | 2 | network only | 666.2 | trend adaptive | 183.8 | 3.62 | fail |
| levelshift | 1 | served blend | 859.4 | trend adaptive | 397.9 | 2.16 | fail |
| levelshift | 1 | network only | 1245.7 | trend adaptive | 397.9 | 3.13 | fail |
| levelshift | 2 | served blend | 812.3 | trend adaptive | 395.1 | 2.06 | fail |
| levelshift | 2 | network only | 1285.3 | trend adaptive | 395.1 | 3.25 | fail |
| spike | 1 | served blend | 252.1 | seasonal pattern | 244.3 | 1.03 | fail |
| spike | 1 | network only | 410.2 | seasonal pattern | 244.3 | 1.68 | fail |
| spike | 2 | served blend | 365.1 | seasonal pattern | 371.3 | 0.98 | fail |
| spike | 2 | network only | 495.4 | seasonal pattern | 371.3 | 1.33 | fail |
| weekly | 1 | served blend | 685.6 | trend adaptive | 258.7 | 2.65 | fail |
| weekly | 1 | network only | 621.3 | trend adaptive | 258.7 | 2.40 | fail |
| weekly | 2 | served blend | 773.9 | trend adaptive | 265.9 | 2.91 | fail |
| weekly | 2 | network only | 825.4 | trend adaptive | 265.9 | 3.10 | fail |

Twenty comparisons, zero passes.

## Per-step MAE and signed bias

Primary metrics. Steady traffic (repeating, seed 1):

| Predictor | | +10m | +20m | +30m | +40m | +50m | +60m |
|---|---|---|---|---|---|---|---|
| served blend | MAE | 201.4 | 195.5 | 177.7 | 166.7 | 156.8 | 157.4 |
| | bias | +34.7 | +21.9 | +44.1 | +50.2 | +28.0 | +46.4 |
| network only | MAE | 520.0 | 556.8 | 509.4 | 508.1 | 540.0 | 579.2 |
| | bias | +20.4 | −37.4 | +46.5 | +84.1 | −67.9 | +81.4 |
| seasonal pattern | MAE | 144.6 | 146.1 | 146.5 | 146.8 | 146.9 | 147.0 |
| | bias | +44.7 | +46.3 | +46.8 | +46.5 | +46.3 | +46.4 |
| previous day | MAE | 158.0 | 159.5 | 160.0 | 160.5 | 160.5 | 160.5 |
| | bias | −5.0 | −3.3 | −2.8 | −3.2 | −3.5 | −3.3 |
| trend adaptive | MAE | 171.5 | 165.8 | 165.7 | 165.7 | 166.2 | 164.4 |
| | bias | +0.3 | +1.1 | +1.2 | +0.4 | −0.2 | −0.4 |

Across a level shift (levelshift, seed 1) — the case the withdrawn run never reached:

| Predictor | | +10m | +20m | +30m | +40m | +50m | +60m |
|---|---|---|---|---|---|---|---|
| trend adaptive | MAE | 279.7 | 310.0 | 365.2 | 424.9 | 479.2 | 528.8 |
| | bias | +10.1 | −63.2 | −129.2 | −187.6 | −241.1 | −290.5 |
| seasonal pattern | MAE | 850.7 | 851.7 | 851.8 | 852.2 | 852.2 | 852.8 |
| | bias | −703.3 | −704.2 | −704.6 | −703.8 | −703.8 | −704.5 |
| served blend | MAE | 848.0 | 911.3 | 868.6 | 852.3 | 836.0 | 840.1 |
| | bias | −738.3 | −804.8 | −770.7 | −741.8 | −730.1 | −720.4 |
| network only | MAE | 1096.1 | 1384.8 | 1308.0 | 1286.9 | 1231.1 | 1167.4 |
| | bias | −803.6 | −1073.6 | −983.4 | −886.7 | −853.8 | −806.9 |

The bias column carries the story. After a level shift every history-based forecaster
under-predicts by roughly 700-800 requests per minute and stays there, because yesterday is
simply the wrong answer now. Only the trend-adaptive rule re-levels, and even it decays with
horizon. The seasonal pattern's flat +44 bias on steady traffic is deliberate — it takes the
75th percentile of recent days — and is useful for an autoscaler, but it is a systematic
error and is reported as one.

MAPE (secondary, shown because it is conventional): on repeating seed 1, seasonal 3.74%,
trend-adaptive 4.25%, served blend 5.65%, network 20.26%. On levelshift seed 1, trend-adaptive
6.94%, served blend 14.9%, seasonal 15.14%, network 23.37%.

## Operational replay

Decisions come from the **real Go controller**; Python performs capacity accounting on a
one-minute event-time grid. Level shift, seed 1:

| Predictor | replica-minutes | scaling events | minutes short | minutes demand exceeded ceiling |
|---|---|---|---|---|
| trend adaptive | 14775 | 31 | 10 | 0 |
| network only | 14825 | 28 | 22 | 0 |
| previous day | 14525 | 32 | 24 | 0 |
| seasonal pattern | 14535 | 32 | 24 | 0 |
| served blend | 14445 | 32 | 26 | 0 |
| persistence | 14315 | 36 | 40 | 0 |
| **reactive only** | **14315** | **36** | **40** | **0** |

Read this narrowly. It is one run, the differences are tens of minutes out of about 4000,
and the replay models capacity arriving, not requests being served. It is consistent with
forecasting reducing shortfall at a small capacity cost, and it is **not** sufficient to
claim that — the earlier version of this document made exactly that mistake. A claim of
operational benefit needs repetition across seeds and scenarios with an uncertainty estimate.

## What is still not established

- **Real traffic.** Every series is synthetic. The benchmark cluster's own history was too
  short to score (248 ten-minute points against the 288 needed).
- **Latency.** Nothing here measures response time.
- **Operational benefit.** One run per scenario, differences inside plausible noise.
- **Whether the divergence is fixable.** The stabilisation arms are a separate experiment.
- **Generalisation beyond these five synthetic families.**

## Decision for the user

The evidence supports removing the neural network and shipping the seasonal pattern —
together with the trend-adaptive rule, which is the stronger choice whenever the level
moves. It does not *compel* that, and the choice is the user's:

1. **Remove the network**, ship the seasonal pattern with trend adaptation, and keep the
   operator, the guard rails and the controller as they are. The project keeps its premise
   and loses a component that has never beaten fifty lines of arithmetic.
2. **Keep the network as an option**, default off, pending the stabilisation arms and a run
   against real traffic. Costs nothing to leave in place; costs credibility if it is
   described as the reason the system works.

I recommend (1) and am not taking it: this is a design decision about the user's own
project, and the numbers, not I, should persuade.
