# Spec — the challenge-phase workload profile

**Status: WRITTEN, NOT RUN. NOTHING DEPLOYED.** No generator was changed, no manifest applied,
no cluster touched. This is the predeclared design for a **future** workload regime (Codex
D-129), to be started only after the preconditions in §1 hold and only by whoever owns the
cluster.

---

## 1. Preconditions — this does not start early

In order, all of them:

1. **The 2026-09-22T12:00Z training capture is complete and preserved.** It is protected
   evidence; a workload change during it destroys what it was capturing.
2. **The repaired serving build is deployed and verified in a new window** by the cluster's
   owner.
3. **The baseline gate has been dealt with deliberately.** The real-traffic replay needs **870
   contiguous valid points** to score, earliest ≈ **2026-09-26T19:20Z**
   (`RESULTS-real-traffic-replay-2026-09-22.md` §3). A regime boundary before then does not
   destroy the samples, but it means the baseline replay's window and the challenge window are
   different regimes and must not be pooled. **Decide which comes first, and record the
   decision here**, rather than discovering it in a census.
4. **The owner's Q1/Q2 budgets exist** (`TOLERANCE-DERIVATION.md` §3) if any decision, as
   opposed to any description, is to come out of the challenge phase.

**Waiting alone does not broaden the coverage.** The current generator will keep producing the
same daily profile indefinitely; more of it is more evidence about one shape, not a second
shape. That is the argument for this phase — and it is not an argument for starting it before
the four preconditions above.

## 2. What the current regime is, and why it stays

The benchmark generates a **repeating daily profile**, essentially all the time. That is a
legitimate baseline challenge and it is **kept**: it can establish the finding that matters most
cheaply — *that simple forecasting suffices here* — and the census already produced a version of
it (`seasonal_pattern` and `previous_day` identical to two decimals; MAE 2.12 rpm against
~4000 rpm on near-deterministic traffic).

Two honesties that carry into the new phase unchanged:

* These are **live measurements of synthetic traffic**, not external production traces. A
  generator that a forecaster finds easy has not shown the forecaster is good.
* **Prevalence is measured in hours, not in scenario names** (`TOLERANCE-DERIVATION.md` Q3). A
  mixture that runs the challenge profile for two hours a day has p = 1/12 for that shape, and
  the margin scales accordingly.

## 3. Separate versioning, so the two regimes can never be silently merged

The challenge profile is **not an edit** of the current generator. It is a new, separately
versioned artifact:

| | |
|---|---|
| Profile id | `challenge-v1` |
| Current profile id (retrospectively named) | `repeating-v2` — the 2026-09-20T18:17Z redesign |
| Where the parameters live | a sealed generator-profile file, committed **before** the phase starts, on no path the trainer or any forecaster reads |
| Where the boundary is recorded | `validity-mask.json` **v3**, as a labelled regime, not an exclusion (§6) |

Every artifact produced during the phase — export, census, replay, result — carries the profile
id and the mask version it was taken under. An evaluation that spans a boundary must say so and
must not average across it.

## 4. The components, all bounded

"Bounded" is the operative word throughout: every component has a declared minimum and maximum,
and the composed offered rate stays inside the cluster's provisioned envelope. An unbounded
component measures the cluster's ceiling, not the forecaster.

| Component | Form | Bound |
|---|---|---|
| **Daily cycle** | retained from `repeating-v2`, unchanged | as today |
| **Correlated noise** | first-order autoregressive (OU-like), so successive samples are dependent rather than white — white noise is trivially unforecastable and tests nothing | amplitude ≤ a declared fraction of the daily amplitude; correlation time declared |
| **Drift** | slow monotone ramp in the level, sign and rate declared, reversing within the phase so the series does not simply walk away | total excursion ≤ a declared fraction of the daily amplitude |
| **Level shifts** | step changes in the level, at times drawn from a declared distribution | step size bounded; minimum dwell between steps ≥ the forecast horizon, so each shift is observable before the next |

**Bounds are declared before the phase and are not tuned during it.** A bound adjusted because
an arm was struggling turns the workload into a parameter of the result.

### Spikes are a separate question and get a separate sub-phase

Large unpredictable spikes may be run, but **only in a labelled robustness sub-phase scored on
different criteria**. An unpredictable spike is by construction not forecastable: scoring MAE
across it measures how large the spike was. What it legitimately tests is the **pipeline's
behaviour under one** — did the forecast get rejected, did the incumbent hold, did the
controller recover, how long was the shortage. Those are the metrics for that sub-phase, and
forecast accuracy is not reported for it at all.

## 5. Identical offered traffic across all arms

The arms must be compared on the **same offered load**, and this is a design constraint on the
generator, not a bookkeeping rule:

* **Open-loop arrival rate, never a closed-loop VU model.** A generator that holds a fixed
  number of virtual users offers less traffic when the service is slower — so the autoscaler's
  own decisions feed back into the series being forecast, and each arm is scored on traffic it
  partly caused. Offered rate must be a function of time and the seed only.
* **One recorded series, replayed offline to every arm** wherever the comparison is offline
  (as all comparisons in this repository currently are). That makes identity exact.
* **If an online A/B is ever run**, the arms must be driven from the same generator instance
  with the same seed over the same wall-clock window, and any arm that cannot be is not
  comparable — say so rather than adjusting.

## 6. The regime boundary is labelled, and the old history is kept

`validity-mask.json` currently expresses two things: a `benchmark_history_start` and
**exclusion** `intervals`. A regime change is neither. Mask **v3** adds a third concept:

```
"regimes": [
  {"profile": "repeating-v2", "start": "2026-09-20T18:20:00Z", "end": "<boundary>"},
  {"profile": "challenge-v1", "start": "<boundary>",           "end": null}
]
```

Rules that go with it:

* **The existing history is preserved, not discarded.** Samples under `repeating-v2` remain
  valid evidence *about `repeating-v2`*. Changing the traffic does not retroactively invalidate
  observations already made, and nothing in this phase deletes or re-masks them.
* **A short exclusion interval covers the cutover itself** — the seconds in which one generator
  stops and another starts — exactly as the 2026-09-20T18:10–18:20Z interval did. That is an
  exclusion; the regime label is not.
* **No training window, evaluation window or replay may span a boundary** unless the result
  explicitly reports that it did and why.
* **The boundary does not imply a fixed delay before anything can be measured.** How much new
  history a given claim needs depends on the claim (§7), not on a blanket "one day".

## 7. What may be claimed, and when

* **A weekly term needs weekly exposure.** A day-of-week effect cannot be separated from drift
  with two observations per weekday. No weekly component may be fitted, served or claimed before
  at least **three complete weeks** of contiguous masked history in the same regime — and even
  then the claim is weak, because three points per weekday is three points.
* **Robustness and forecastability are different claims** (see §4). "The system survived a
  spike" and "the system predicted the spike" may never be reported as the same result.
* **Adaptation claims need the boundary in view.** The interesting measurement at a level shift
  is *how many origins the system took to recover*, which requires the boundary label from §6 to
  be in the record.
* **Nothing here establishes production behaviour.** It remains synthetic traffic on one
  cluster with one generator design, however much richer the generator becomes.

## 8. Freeze before the phase

Append to this file before the generator changes: the exact bound values for every component in
§4, the correlation time, the level-shift distribution and its seed, the sealed profile file's
hash, the mask v3 content, the chosen order relative to precondition 3, and the commit hash.
Then `git commit`. The profile is frozen from that point; a changed bound is a new profile
version and a new regime.
