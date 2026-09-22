# Protocol — seasonal-default selector with hysteresis and minimum dwell

**Status: WRITTEN, NOT RUN — and now BLOCKED.** Nothing in this file has been executed. It is
the predeclared design for the next offline experiment (Codex D-124), written before any of its
data exists, so that "we chose this afterwards" is not available as an explanation of the
result.

> ### ⛔ Blocked on an owner decision (Codex C-99 / D-125), 2026-09-22
>
> **S2 has no threshold.** The 21.3 % "derived" margin this file was written against is
> withdrawn — `TOLERANCE-DERIVATION.md` now presents the shortage and replica budgets as a
> choice the owner must make, with three worked options and none of them in force. This
> experiment **cannot consume its test set** until Q1, Q2 and the measured Q3 are recorded
> here with a commit hash. Everything else in this file is unaffected.
>
> ### ⚠ Corrected 2026-09-22 after Codex round 26 (C-101 / D-127)
>
> **S4 compared incompatible objectives.** It required beating the historical **2.35 %**, a
> number measured under `lead2` horizon weights, while this experiment scores everything under
> `uniform` (§3). The two are not commensurable: re-running the same grid under the repaired
> objective moved the winner from `lead2`/min-obs 6 to `uniform`/min-obs 18 and turned the
> `lead2` rows that had topped the table at 295.04 rpm into the grid's **worst** at 391.89 rpm.
> S4 is now a **paired comparison against the frozen unhysteretic selector, run on the identical
> new data under the identical objective** — see §6.

Prerequisites, in order, per D-124: the evaluation defects C-95, C-96 and C-97 are repaired
(done, 2026-09-22); the 2026-09-22T12:00Z capture is preserved; the serving repairs are
verified and deployed in a **new** window by whoever owns the cluster; and the chronological
replay of real accumulated traffic takes priority over any further neural-architecture change
(started — `RESULTS-real-traffic-replay-2026-09-22.md`, census only).

---

## 1. The hypothesis, stated so it can fail

> Serving the **seasonal pattern by default**, and moving to a challenger only after
> *sustained* matured evidence that the challenger is better — and then staying there for a
> minimum dwell — captures the selector's gains on the workloads where a fixed arm fails
> catastrophically, **without** the small persistent tax it pays on the repeating profile.

It is plausible and it is **not guaranteed to pass**. The previous selector's repeating-workload
regression came from *frequent small re-decisions* on a workload where the incumbent was
already right; hysteresis and dwell are aimed exactly at that. They may equally just make the
selector slower to leave a losing arm when a regime actually shifts. Both outcomes are
publishable; only one is hoped for.

## 2. The mechanism

Identical candidate set and matured-evidence rule as `eval/selector.py`, with three changes.

**Default arm.** `seasonal_pattern`, not `trend_adaptive`. Startup serves it, and it is the
arm the selector falls back to whenever no challenger holds the floor.

**Hysteresis.** A challenger `c` replaces the incumbent `i` only if, for **`H` consecutive
matured origins**, its penalised smoothed cost has been better than the incumbent's by at
least a relative band `B`:

```
for each of the last H matured origins:   ewma[c] + penalty  <  ewma[i] * (1 - B)
```

A single origin's advantage never switches anything. The band is relative so it does not
need rescaling between workloads.

**Minimum dwell.** After any switch, the served arm is held for at least `D` origins
regardless of evidence, *except* under the safety release below. Dwell is what stops
oscillation between two arms that are genuinely close.

**Safety release (the one way dwell is broken).** If the served arm produces a non-finite
forecast, or its matured cost exceeds the default arm's by more than `R` relative for `H`
consecutive matured origins, the selector returns to `seasonal_pattern` immediately. Dwell
must never be able to hold a failing arm in place. This is a **one-way** release: it returns
to the default, never to another challenger.

**Ties keep the incumbent** (C-97, already repaired). **Failures are inherited** from the arm
actually served (C-97, already repaired).

## 3. One fixed objective

`EVAL_OBJECTIVE = "uniform"` — the same single external objective now used throughout
`eval/selector.py`, matching the preregistered decision rule in `evaluation-protocol.md` §9.

It is used for **every number compared against another number**: the development sweep's
ranking, the control blend's weight, the test tables, the bootstrap and the bar. The
selector's *internal* horizon weighting stays a tunable parameter and is tuned **against**
this objective, never used to score itself (that was C-96).

MAE is reported separately and in full, per step, alongside signed bias. The proxy is a
screening statistic, not operating cost; **the controller-replay comparison in operating units
is primary**, and the proxy never converts into a margin (see `TOLERANCE-DERIVATION.md` §4).

## 4. Parameters, all preselected on development data

Grid searched on **development runs only**, scored under the single external objective,
winner by lowest development cost, then fewest switches, then grid order — then **frozen and
written into this file before the test data is generated**.

| Parameter | Symbol | Grid |
|---|---|---|
| EWMA half-life (matured origins) | — | 6, 18, 72 |
| Selector-internal horizon weights | — | uniform, deployed decay, lead2 |
| Minimum matured observations before any selection | — | 6, 18 |
| Switching penalty (cost-proxy units) | — | 0 %, 2 %, 5 %, 10 % of the development median per-origin cost |
| **Hysteresis length** | `H` | 3, 6, 12, 18 consecutive matured origins |
| **Hysteresis band** | `B` | 0 %, 2 %, 5 %, 10 % relative |
| **Minimum dwell** | `D` | 6, 18, 72, 144 origins (1 h, 3 h, 12 h, 24 h) |
| Safety-release ratio | `R` | 25 %, 50 % |
| Control blend weight | — | 0.25, 0.5, 0.75 (chosen separately; cannot affect the selector) |
| Default arm | — | **fixed** at `seasonal_pattern` — the hypothesis, not a free parameter |

`H`, `B`, `D` and `R` are the only additions; everything else keeps the meaning it has today.

> **Reduce the grid before running it.** 3 × 3 × 2 × 4 × 4 × 4 × 4 × 2 × 3 = 55 296 settings is
> far too many to select over on the development set without overfitting it. Cut by fixing
> `R = 50 %` and the internal horizon weights to the objective's own weighting unless there is
> a reason not to, and by searching `H`, `B`, `D` on a coarse grid first. Record the reduction
> **in this file** before the sweep, with its reasoning.

## 5. Data — and what is already spent

> ### Seeds 11, 12 and 13 are CONSUMED.
> They were used as the test set for the selector experiment on 2026-09-22, and the result was
> read and acted on. They are development data now, and must never again be described as
> held-out. A repaired selection rule re-scored on them would be a second look at a used test
> set, which is why the repaired `eval/selector.py` offers `--validation-only` and why the old
> test tables are relabelled descriptive rather than re-run.

| Split | Seeds | Calendar start | Role |
|---|---|---|---|
| Development | 1, 2 (original validation) **and 11, 12, 13 (now spent)** | 2026-03-01 / 2026-06-07 | parameter selection, exchange-rate estimation, margin derivation |
| **Test** | **new, chosen at freeze time and recorded here — none used before** | **a start date later than every development window** | one evaluation, once |

**The test set must also contain regime shapes the development set does not.** Repeating,
trend, level shift, spike and weekly have all been seen. Add at least two unseen shapes —
candidates: a *double-peak* daily profile, a *weekend/weekday* asymmetry, a *gradual seasonal
drift*, a *step down* (the mirror of level shift), and a *noise-regime change* (same mean,
different variance). A selector that only ever meets shapes it was tuned on is not being
tested on its ability to generalise, which is the only property that would justify the extra
moving part.

**Freeze before the run.** Before the test series are generated, append to this file: the
chosen parameter values, the chosen test seeds and start date, the list of regime shapes, the
frozen unhysteretic selector's parameters (S4), **the owner's answers to Q1, Q2 and the measured
Q3 together with the resulting per-shape margin in operating units**, and the commit hash of the
code that will run. After that, `git commit`. Any change afterwards makes it a new experiment
with a new test set.

**The margin line is the blocking one.** If it is absent, the run does not start — S2 has no
threshold and a non-inferiority test without a threshold is not a test.

## 6. The bar

Predeclared here. Read against the **upper bound** of a 95 % block bootstrap interval, whole
calendar days as blocks (overlapping rolling origins are not independent samples).

| | Requirement |
|---|---|
| **S1** pooled improvement | ≥ 5 % lower pooled cost proxy than the best fixed arm, 95 % CI on the difference excluding zero |
| **S2** repeating-workload non-inferiority | **⛔ NO THRESHOLD YET.** The margin comes from the owner's answers to Q1/Q2/Q3 in `TOLERANCE-DERIVATION.md` §3, recomputed on development data for this window and divided by measured prevalence. Judged on the **95 % CI upper bound**, **in operating units** — never via a proxy conversion (§4 there). The run may not start until the chosen budgets and the resulting margin are written into §5 of this file |
| **S3a** operational shortage | additional shortage ≤ the owner's **Q1** budget, measured by controller replay, **primary**. Note the word is *shortage*, not availability: it counts minutes of under-capacity and prices no queueing, latency or dropped request |
| **S3b** operational replicas | additional replica-minutes ≤ the owner's **Q2** budget. Not a fixed 1 % — that figure was one of the withdrawn inventions |
| **S3c** failures | inherited failure count ≤ the worst fixed arm's, per run — now actually measurable (C-97) |
| **S3d** switching | ≤ 10 % of origins, and — new — **median dwell ≥ `D`**, which is a check that the mechanism did what it says |
| **S4** the hypothesis itself | **paired comparison, same data, same objective.** Run the **frozen unhysteretic selector** (`H = 1`, `D = 0`, `B = 0` — the mechanism this one is meant to improve on) as a fifth arm over the **identical** new seeds, dates and regime shapes, scored under the **same** `EVAL_OBJECTIVE = "uniform"`. The hysteretic selector's repeating-workload regression must be **lower than the unhysteretic selector's, per paired run**, with the 95 % CI on the paired difference excluding zero. If hysteresis and dwell do not reduce the tax they were added to reduce, the hypothesis failed even if S1–S3 pass |

S4 is the point of the experiment. A selector that clears S1–S3 by being a different selector,
rather than by fixing the thing that failed, has not answered the question.

**Why S4 is paired and not a comparison against 2.35 % (Codex C-101).** The 2.35 % was measured
under `lead2` horizon weights on seeds 11–13; this experiment scores under `uniform` on new
seeds. Comparing across a changed objective would attribute the difference between two
weightings to hysteresis. The paired form removes both confounds at once: the two selectors see
the same series, the same origins and the same scorer, differing only in the mechanism under
test, and the difference is taken **within** each run before it is pooled. It also removes the
one-sided ratchet in the old wording — a historical number can only be beaten or missed, while a
paired contrast can also come back **null**, which is a real and publishable answer here.

**The frozen unhysteretic selector is a preselected artifact, not a re-run of the old
experiment.** Its parameters are fixed at the values recorded in §5 before any test data exists,
and its seeds 11–13 results stay consumed and are not reused.

## 7. Repeating-workload constraints, explicitly

The repeating profile is what the benchmark actually generates, so it gets constraints the
other shapes do not:

1. **It is tested separately, never pooled away.** A pooled gain that hides a repeating-workload
   regression is not an improvement to this system.
2. **Its margin comes from declared budgets, not from a round number and not from the observed
   miss** — the owner's Q1/Q2 minus the incumbent's own measured consumption in the development
   window, divided by measured prevalence (`TOLERANCE-DERIVATION.md` §1 and §3). Unanswered as
   of 2026-09-22.
3. **Prevalence is measured, not asserted.** If the evaluation runs a workload mixture, *p* is
   the fraction of *hours* the repeating profile occupies in that mixture.
4. **The default arm must win by default there.** On repeating, `seasonal_pattern` is already
   the right answer; a correct hysteresis setting should therefore produce **few or no
   switches** on repeating runs. Report the per-run switch count on repeating as a direct test
   of the mechanism, not only as a summary statistic.
5. **Operating units decide.** If the proxy regresses on repeating but shortage and
   replica-minutes do not move, say exactly that — as happened at +2.35 % proxy with zero
   *observed*
   additional shortage-minutes — and do not dress a proxy difference as an operational one.

## 8. What this experiment still cannot establish

* Anything about **live** behaviour. It is offline, on synthetic series, with a partial
  controller path and no matured-error feedback loop.
* Anything about the **neural** arm. This is a selector between cheap forecasters.
* **Latency.** Replay models capacity arriving after a readiness delay; it prices no queueing
  and no user-visible response time.
* **Generalisation beyond the generator.** Synthetic regime shapes are chosen by hand; real
  traffic changes in ways nobody wrote down. That is what the chronological replay of real
  accumulated traffic is for, and it is currently short of data
  (`RESULTS-real-traffic-replay-2026-09-22.md`).
