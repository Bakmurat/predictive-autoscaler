# Online selector between cheap forecasters — offline experiment, 2026-09-22

**Verdict: the selector does not clear its predeclared bar. Keep the fixed baseline.**

It clears the pooled-cost bar comfortably and it never loses badly, but it is **2.4 % worse than
the seasonal pattern on the repeating workload** — the benchmark's own traffic profile — and the
predeclared non-inferiority tolerance there was 2 %. The miss is small and the interval is tight
(+1.4 % to +3.4 %), which means it is a real difference and not noise. The bar was written before
the test set was built; it is not being moved now.

This is an **offline experiment** (Codex C-92 / D-116). Nothing was deployed, no cluster was
touched, and no served forecast changed. Code: [`eval/selector.py`](selector.py). Raw output:
[`eval/selector-20260922.json`](selector-20260922.json).

---

## 1. What was compared

| Arm | What it is |
|---|---|
| `seasonal_pattern` | the repository's own pattern component (weighted percentile over the last 7 same-clock-time days) |
| `trend_adaptive` | previous-day value scaled by the recent level ratio, damped |
| `lagged_linear` | **new** — direct per-step ridge regression on lags {1, 2, 3, 6, 144}, refit at every origin on the trailing 7 days |
| `constant_blend` | **control arm** — a fixed mixture of the two incumbents, at the mixture weight that was best on validation |
| `selector` | serves whichever of the first three has the lowest exponentially-smoothed matured cost, with a switching penalty |

The selector's candidate set is the first three. The constant blend is a *control*, not a
candidate: it answers "would simply averaging the two incumbents have got you the same thing?"

### The cost number, stated plainly

Arms are ranked by a **proxy**, in requests per minute:

```
cost(origin) = Σ_h w_h · ( 2 · max(0, actual_h − forecast_h)      # under-forecast
                         + 1 · max(0, forecast_h − actual_h) )    # over-forecast
               / Σ_h w_h
```

**This is not operating cost.** It prices no pod start-up delay, no integer replica step, no
stabilisation window, no dropped request and no dollars. Its only justification is that it is an
asymmetric *absolute* loss, so it is consistent for a quantile (Ehm et al. 2016, Eq. 5–6) and is
therefore a coherent way to *rank* forecasts — unlike the asymmetric *squared* loss the trainer
uses, whose optimum is the 2/3 expectile rather than the 2/3 quantile (see
[`docs/RESEARCH-2026-09-22.md`](../docs/RESEARCH-2026-09-22.md) §3, "[Corrected 2026-09-22]").

Operational consequences are measured separately, by replaying the **real Go controller**
independently for every arm.

### Where the design comes from

The mechanism is Autopilot's (Rzadca et al., EuroSys 2020, Eq. 8–9): per-candidate
exponentially-smoothed realised cost, arg-min selection, explicit switching penalty. **No number
is borrowed from that paper.** Its published 31 % → 23 % slack difference compares observational
cohorts of jobs rather than running a selector ablation, and its candidates are vertical
resource-limit recommenders rather than forecasts. "Autopilot-inspired" is the whole of the
claim; the size of any gain here had to be measured here.

---

## 2. Information hygiene

These are the properties the experiment is worthless without, and how each is enforced.

| Requirement | How |
|---|---|
| Forecasts recorded before their targets | every arm forecasts at origin *i* from `series[:i+1]` only; targets are `i+1 … i+6` |
| The selector updates only on **matured** observations | at origin *k* the newest evidence consulted is origin *k − 6*, whose six targets are all observed by *k*. Nothing newer ever enters the EWMA |
| Identical information availability across arms | all arms forecast at every one of the 720 origins in every run; the selector serves a pointer, never an extra forecast |
| Explicit missing-data behaviour | `lagged_linear` falls back to same-time-yesterday (then to the last observation) when fewer than 200 usable fit rows exist, and every fallback is **counted**; a non-finite forecast from any arm would be replaced by the last observation and counted. Observed: 0 non-finite forecasts; 60 of 720 degraded origins per `repeating` run and 3 per `trend` run, all at the start of the window; 0 elsewhere. A missing matured cost updates nothing — the arm is neither rewarded nor penalised for it |
| Separate controller replay state per arm | `replay_controller` is called once per arm per run, 75 times in total, each with its own state. No arm's operational result is inferred from another's decisions. All 75 replays used the real Go controller (`decisions_from: go_controller`); the Python fallback was never taken |
| Deterministic startup | until **every** candidate has ≥ `min_obs` matured observations, the preselected default arm is served. No candidate is ever preferred for having been observed less |
| Deterministic tie-breaking | the incumbent wins any exact tie (its switching penalty is zero); among non-incumbents the earliest arm in the canonical order `seasonal_pattern, trend_adaptive, lagged_linear` wins. Comparisons use a 1e-9 relative tolerance |
| Test set untouched by tuning | `_assert_disjoint()` refuses to run if the validation and test seed sets intersect or their calendar windows overlap |

Two notes carried from the review (D-117), both implemented:

* **Lag 1008 is omitted.** At 10-minute resolution 1008 steps is *seven days* — the entire rolling
  window the deployed trainer reads — so the feature would be undefined for every row unless the
  history is expanded first. `lagged_linear` uses {1, 2, 3, 6, 144} and says so in the code.
* **Offered demand is preserved.** The scenario series is offered demand, and `replica_need()` is
  deliberately unconstrained by the replica ceiling, so an overloaded period appears as a deficit
  rather than as falling demand. Minutes where demand exceeded the ceiling are reported
  separately (0 in every run here).

---

## 3. Preselected parameters, and where each came from

**Selected on validation only:** data seeds 1 and 2 from 2026-03-01 — the seeds and calendar
window [`RESULTS-2026-09-22-corrected.md`](RESULTS-2026-09-22-corrected.md) already used — over
5 scenarios × 2 seeds = 10 runs, 7 200 origins. A grid of **216** settings was scored; the
winner was taken by lowest pooled validation cost, then fewest switches, then grid order.

| Parameter | Chosen | Candidates searched | Validation basis |
|---|---|---|---|
| EWMA half-life | **6 matured origins** (60 min of matured evidence) | 6, 18, 72 | lowest pooled validation cost |
| Horizon weights | **`lead2` = [1, 1, 0, 0, 0, 0]** | uniform, deployed decay [1, .9, .8, .7, .6, .5], `lead2` | lowest pooled validation cost — and it matches what the controller reads (`lead_steps = 2`) |
| Minimum matured observations before selecting | **6** | 6, 18 | lowest pooled validation cost |
| Switching penalty | **5.402 rpm** = 2 % of the validation median per-origin cost (270.09 rpm) | 0 %, 2 %, 5 %, 10 % of that median | lowest pooled validation cost; expressed in the cost proxy's own units, as required |
| Startup default arm | **`trend_adaptive`** | each of the three candidates | lowest pooled validation cost |
| Control blend weight | **0.25 seasonal / 0.75 trend** | 0.25, 0.5, 0.75 | chosen *separately* (it cannot affect the selector), giving the control arm its own best-on-validation setting: 438.5 / 503.9 / 574.3 rpm |

Validation reference points, under the chosen weights: `seasonal_pattern` 651.4, `trend_adaptive`
332.5, `lagged_linear` 354.4, selector **295.0** rpm, switching on 4.4 % of origins. The grid's
*worst* setting scored 349.5, so the selector is not razor-sensitive to these choices — the whole
216-point spread is 295–350 rpm.

**Test set, built only after the parameters were frozen:** data seeds 11, 12, 13 from 2026-06-07,
5 scenarios × 3 seeds = **15 runs, 10 800 origins**, 14-day series at 10-minute resolution, 720
origins per run centred on each scenario's event, replica bounds 1–12, capacity denominator
calibrated on pre-origin data only. Note that the test window starts on a different weekday, so
the `weekly` scenario is genuinely a different series and not a reseeding of the same one.

---

## 4. Results

### 4.1 Pooled over the test set (10 800 origins, 84 whole-day bootstrap blocks)

| Arm | Cost proxy (rpm) | MAE (rpm) | Replica-minutes | Scaling events | Shortage minutes |
|---|---|---|---|---|---|
| `seasonal_pattern` | 642.92 | 432.1 | 362 496 | 838 | 290 |
| `trend_adaptive` | **325.08** | 248.4 | 353 506 | 793 | **216** |
| `lagged_linear` | 346.60 | 302.5 | **348 046** | **762** | 508 |
| `constant_blend` (w = 0.25) | 377.97 | 283.2 | 355 776 | 816 | 248 |
| **`selector`** | **293.68** | **246.1** | 353 956 | 780 | 224 |

MAE is reported separately from the cost proxy on purpose — see §5.

### 4.2 Selector against each fixed arm, block bootstrap over whole days

2 000 resamples of the 84 (run, calendar-day) blocks, paired (both arms resampled on the same
blocks, since they share origins). Overlapping rolling origins are not independent, which is why
the block is a whole day and not an origin.

| Comparator | Δ cost proxy (rpm) | 95 % CI | Relative | 95 % CI |
|---|---|---|---|---|
| `seasonal_pattern` | −349.23 | [−526.56, −190.30] | −54.3 % | [−63.9 %, −39.7 %] |
| `trend_adaptive` (best fixed, pooled) | **−31.40** | **[−41.81, −19.90]** | **−9.7 %** | **[−13.3 %, −6.0 %]** |
| `lagged_linear` | −52.92 | [−68.15, −35.39] | −15.3 % | [−19.9 %, −10.1 %] |
| `constant_blend` | −84.29 | [−114.65, −56.69] | −22.3 % | [−28.0 %, −16.5 %] |

The selector beats every fixed arm and the constant blend pooled, with intervals excluding zero
in all four comparisons. **Averaging the two incumbents is not a substitute for choosing between
them:** the constant blend is 22 % worse than the selector and 16 % worse than simply serving
`trend_adaptive`.

### 4.3 Per scenario — diagnostics only

A per-scenario winner is not a baseline anyone could serve, because serving it requires knowing
in advance which workload you are on. These rows exist to show *where* the pooled number comes
from, not to be selected between.

| Scenario | Best fixed arm here | Selector vs that arm | 95 % CI | Selector vs `trend_adaptive` |
|---|---|---|---|---|
| repeating | `seasonal_pattern` | **+2.4 %** | [+1.4 %, +3.4 %] | −23.3 % |
| trend | `trend_adaptive` | −2.3 % | [−4.6 %, +0.1 %] | −2.3 % |
| levelshift | `trend_adaptive` | −4.5 % | [−15.9 %, +5.9 %] | −4.5 % |
| spike | `seasonal_pattern` | **+5.7 %** | [+2.5 %, +12.2 %] | −12.2 % |
| weekly | `trend_adaptive` | −8.0 % | [−15.2 %, −0.6 %] | −8.0 % |

The shape is exactly what the selector is for and exactly where it falls short: it converts two
catastrophic fixed-arm failures (`seasonal_pattern` on weekly and levelshift, 3–5× cost) into no
failure at all, at the price of a small, *consistent* tax on the two workloads where
`seasonal_pattern` is already the right answer.

### 4.4 Operational replay, per arm

Shortage minutes are minutes in which ready capacity was below the **unconstrained** requirement.
No run in this set was ceiling-limited (0 minutes of demand above the ceiling, every arm).

| Arm | Replica-minutes | vs `trend_adaptive` | Shortage minutes | Scaling events |
|---|---|---|---|---|
| `seasonal_pattern` | 362 496 | +2.5 % | 290 | 838 |
| `trend_adaptive` | 353 506 | — | 216 | 793 |
| `lagged_linear` | 348 046 | −1.5 % | 508 | 762 |
| `constant_blend` | 355 776 | +0.6 % | 248 | 816 |
| `selector` | 353 956 | +0.13 % | 224 | 780 |

The cheap linear arm buys its lower replica-minutes by running short more than twice as often;
it is the cheapest arm on paper and the worst one to operate. The selector is within 0.13 % of
`trend_adaptive` on capacity consumed, 8 minutes worse on shortage across 15 runs, and switches
arm on 4.2 % of origins (mean over runs; 1.8 %–7.5 % per run).

---

## 5. The bar, and the verdict

Predeclared in `eval/selector.py` before the test set existed.

| Test | Requirement | Observed | |
|---|---|---|---|
| **S1** practical cost improvement | ≥ 5 % lower pooled cost proxy than the best fixed arm, with the 95 % CI on the difference excluding zero | −9.7 %, CI [−41.8, −19.9] rpm | **PASS** |
| **S2** non-inferiority on the repeating workload | ≤ 2 % worse than the best fixed arm there, and 95 % CI upper bound ≤ +5 % | **+2.35 %**, CI [+1.4 %, +3.4 %] | **FAIL** (the interval condition passed; the point estimate did not) |
| **S3** no unacceptable operational regression | shortage ≤ +10 %, no new forecast failures, switching ≤ 10 % of origins | +3.7 % shortage, 0 failures, 4.2 % switching | **PASS** |

**Verdict: KEEP THE FIXED BASELINE.** Two of three clear; the one that fails is the workload the
benchmark actually runs, and it fails by a margin whose confidence interval does not contain
zero. A 2.4 % regression on steady traffic is not catastrophic, but the bar said 2 %, the bar was
written first, and moving it after seeing the number would make every future bar worthless.

### What this does establish

- The **best cheap forecaster genuinely flips with the workload**, and a selector that watches
  matured cost tracks the flip without being told which workload it is on. Its worst
  per-scenario gap to the locally best fixed arm is +5.7 % (spike), and it is *better* than that
  arm on three of the five families — whereas `seasonal_pattern`, the arm that wins on steady
  traffic, is **3–5× worse** than the best arm on levelshift and weekly. Avoiding a 5× failure
  is the point of the mechanism, and that part worked.
- **A constant blend is not the cheap way out.** Averaging the incumbents was 22 % worse than
  selecting between them and 16 % worse than just serving `trend_adaptive`.
- The selection layer is **operationally cheap**: +0.13 % replica-minutes, +8 shortage minutes
  over 15 runs, fewer scaling events than the arm it beats.
- The **new lagged linear arm is not worth serving on its own** — worst MAE of the three
  candidates and more than double anyone else's shortage — but the selector served it on 44–54 %
  of `trend` origins and 22–31 % of `weekly` origins, so it earns its place as a *candidate*.

### What this does not establish

- **Nothing about real traffic.** Every series here is synthetic, from the repository's own k6
  step-profile generator. A selector's whole value is adaptation, and synthetic scenarios are the
  friendliest possible test of adaptation because their regimes are clean and labelled.
- **Nothing about latency.** The replay models capacity arriving after a readiness delay. It does
  not model queueing or response time.
- **Nothing about operating cost.** The ranking metric is a forecast-error proxy. The two
  quantities it omits — start-up delay and the integer replica step — are precisely the ones that
  would decide whether a 9.7 % proxy improvement is worth anything.
- **Nothing about the neural arm.** It is absent from this experiment by design.
- **The MAE story is not the cost story, and the difference is a warning.** The selector was
  tuned on a `lead2` objective: steps +10 and +20 minutes only, because those are the steps the
  controller reads. Over all six steps it is pooled-best on MAE (246.1 vs 248.4) but on the
  `trend` scenario its MAE (227 avg) is worse than `trend_adaptive` (196) and worse than the
  constant blend (192) while its proxy cost is better. **A selector tuned on a two-step
  asymmetric objective is not thereby a better six-step forecaster**, and any future use of the
  longer horizons must retune rather than inherit.
- **The intervals are narrower than the uncertainty.** The block bootstrap resamples days within
  these 15 runs. It does not cover the choice of scenario families, the choice of parameter grid,
  or the single validation/test split. Treat ±3 points as the resolution of this experiment, not
  ±0.3.

---

## 6. If this is revisited

Cheapest things that would change the answer, in order:

1. **Give the repeating workload back to `seasonal_pattern`.** The selector's only failure is
   that it drifts off the right arm on steady traffic — 92 %, 93 % and 88 % served share for
   `seasonal_pattern` on the three repeating runs, so roughly one origin in ten is served by the
   wrong arm. A larger `min_obs`, a longer half-life or a larger switching penalty are all
   plausible fixes, but **each must be preselected on validation again**, not tuned against the
   +2.35 % that is now known.
2. **Run it on the recorded benchmark series** (`eval/data/benchmark-nginx-test.json`) rather
   than synthetic scenarios, once enough matured history exists.
3. **Separate the three margin knobs before touching any of them.** The forecasting objective,
   the pattern component's historical percentile and the controller's confidence dampener all
   move the same effective capacity margin (D-115). One per experiment.
4. **Do not deploy this.** It is an offline result about a cost proxy on synthetic series, and it
   did not clear its own bar.

## Reproducing

```sh
eval/.venv/bin/python eval/selector.py --out eval/selector-20260922.json
```

Deterministic: fixed data seeds, a fixed bootstrap seed (20260922), no model training and no
random initialisation anywhere in the path. Runtime about 4.5 minutes, of which ~2.5 are the 75
Go controller replays. `--no-replay` skips those and produces the accuracy tables alone.
