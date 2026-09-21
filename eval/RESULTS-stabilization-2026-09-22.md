# Stabilization experiment — 2026-09-22

> **Corrected 2026-09-22 after Codex round 25 (C-94, C-95 / D-119, D-120).** Three claims in
> the first version were wrong or overstated: that variance is generally lower, that only
> `tanh` improves accuracy, and that identical `tanh`/`both` results prove the gradient never
> reached the clipping threshold. Every number below was **recomputed from
> [`stabilization.json`](stabilization.json) and
> [`divergence-repaired.json`](divergence-repaired.json)**, not carried over from the earlier
> prose. The Part 2 record had lost 32 of its 40 repetitions; see §2.

One bounded experiment, in two parts, answering the question Codex set: **does the
across-seed divergence disappear?** Accuracy is secondary and is judged against the
strongest baseline, not previous-day alone.

Arms, varied one at a time so an improvement can be attributed:

| Arm | activation | clipnorm |
|---|---|---|
| baseline / original | `relu` (ships today) | none |
| a | **`tanh`** | none |
| b | `relu` | **1.0** |
| c | `tanh` | 1.0 — interpreted only after a and b |

## Headline

**Not established: that any arm removes the divergence.**

1. **The matched control did not fail either.** Under a seeded, 50-epoch, rolling-7-day
   configuration, *every* arm including ReLU had zero failed seeds. An experiment in which
   the control never fails cannot show that a change prevents failure. This is why the first
   reading of these arms — "tanh removes the divergence" — was withdrawn before publication.
2. **Under the conditions that originally failed, ReLU still did not fail**, so the failure
   was not reproduced and no arm can be credited with preventing it.
3. **What the arms do show is a smaller observed spread in the corrected four-seed
   experiments.** That is a sampled observation on eight seed-runs across two scenarios. It
   is not a demonstration of generally lower variance, and it is not evidence that any
   divergence was prevented.

---

## Part 1 — stability under the corrected configuration

50 epochs, rolling 7-day window, seeded (11/22/33/44), 120 scored origins, one training per
arm and seed. `spread` is max MAE ÷ min MAE across the four seeds; lower is a smaller
observed spread. **Mean** is the mean across the same four seeds and is reported because a
minimum alone says nothing about accuracy.

### repeating, data seed 1 — baselines: seasonal 170.90, previous-day 187.79, trend-adaptive 192.38, persistence 367.05

| Arm | network MAE per seed (11/22/33/44) | **mean** | min | max | spread | failed seeds | blend MAE mean (best) |
|---|---|---|---|---|---|---|---|
| baseline `relu` | 211.50 / 201.25 / 227.30 / 277.85 | 229.48 | 201.25 | 277.85 | 1.38 | 0 | 164.62 (161.69) |
| a `tanh` | 184.66 / 203.93 / 200.54 / 186.99 | **194.03** | **184.66** | 203.93 | **1.10** | 0 | **160.61** (160.21) |
| b `clipnorm` | 215.35 / 201.25 / 207.78 / 213.54 | **209.48** | 201.25 | 215.35 | **1.07** | 0 | 162.99 (161.69) |
| c both | 184.66 / 203.93 / 200.54 / 186.99 | 194.03 | 184.66 | 203.93 | 1.10 | 0 | 160.61 (160.21) |

### levelshift, data seed 1 — baselines: seasonal 267.63, previous-day 291.08, trend-adaptive 298.19, persistence 568.93

| Arm | network MAE per seed (11/22/33/44) | **mean** | min | max | spread | failed seeds | blend MAE mean (best) |
|---|---|---|---|---|---|---|---|
| baseline `relu` | 599.94 / 1115.73 / 627.39 / 1851.33 | 1048.60 | 599.94 | 1851.33 | 3.09 | 0 | 328.94 (282.93) |
| a `tanh` | 484.38 / 595.23 / 526.37 / 621.51 | **556.87** | **484.38** | 621.51 | **1.28** | 0 | **281.99** (271.23) |
| b `clipnorm` | 599.94 / 662.60 / 627.39 / 686.65 | **644.15** | 599.94 | 686.65 | **1.14** | 0 | 291.06 (282.93) |
| c both | 484.38 / 595.23 / 526.37 / 621.51 | 556.87 | 484.38 | 621.51 | 1.28 | 0 | 281.99 (271.23) |

### What these tables actually say

- **Clipping improves mean network accuracy. The earlier claim that "only `tanh` improves
  accuracy" was false.** Mean network MAE falls **229.48 → 209.48** on repeating (−8.7 %) and
  **1048.60 → 644.15** on level shift (−38.6 %). What is unchanged is the *minimum*, and an
  unchanged minimum is not unchanged accuracy: it is one seed out of four. `tanh` improves
  the mean more (−15.4 % and −46.9 %), which is a difference of degree, not of kind.
- **Why the minimum looked unchanged, measured rather than assumed.** Comparing `clipnorm`
  against `relu` seed by seed, the two are **identical to the reported precision in 3 of 8
  seed-runs** (repeating seed 22; level-shift seeds 11 and 33) and differ in the other 5. The
  seed that happened to produce each scenario's minimum is one of the identical ones — so
  the minimum was inherited, not preserved by the arm. Of the 5 runs clipping did change, **4
  improved and 1 got worse** (repeating seed 11, 211.50 → 215.35).
- **Arm c reproduces arm a exactly** — every recorded metric field (network MAE, blend MAE,
  bias, worst absolute error) is identical across all four seeds in both scenarios; only
  wall-clock training time differs. That is **consistent with** clipping never binding under
  `tanh`, and it is the natural reading given that clipping demonstrably *did* bind under
  `relu` in 5 of 8 runs. But it does **not establish** it: this experiment recorded no
  gradient norms, and identical outputs are evidence about outputs. If the question matters,
  instrument the gradient norm; until then, arm c is reported as "indistinguishable from arm
  a here", not as "clipping was inactive".
- **The blend still does not clear the bar.** Best blend on repeating is 160.21 against a
  seasonal baseline of 170.90 — 6.3 % better, short of the 10 % requirement. On level shift
  the best blend (271.23) is 1.4 % *worse* than the seasonal baseline (267.63), and the
  four-seed mean (281.99) is 5.4 % worse.

---

## Part 2 — reproducing the conditions that actually failed

The withdrawn run's divergence appeared under **unseeded initialisation, 12 epochs,
expanding history**. Part 1 used none of those. So the arms were re-run under the original
configuration, five unseeded repetitions each.

> ### The record was defective, and has been rebuilt
>
> `reproduce_divergence.py` keyed each repetition by `str(model_seed)`. Every unseeded
> repetition carries the seed `None`, so **all five landed under the single key `"None"` and
> four were overwritten** — 32 of 40 repetitions lost, while the console log kept all of
> them. The console log (`divergence.log`) is the surviving complete record;
> `repair_divergence_record.py` parses it into
> [`divergence-repaired.json`](divergence-repaired.json), with a unique id per repetition, an
> explicit failure type, and spread over **successful repetitions only**. The defective
> `divergence.json` is kept unchanged as the evidence of the fault. One field is
> unrecoverable — per-repetition `worst_abs_error` was only ever written for the surviving
> repetition — and is null rather than invented.
>
> Failures and threshold exceedances are now counted **separately**. A repetition that
> produced no finite MAE cannot be compared against a threshold, and a repetition whose MAE
> merely exceeded the threshold did not fail. The old single "diverged" count mixed the two.

A repetition **fails** when training raises or the forecast is non-finite. It **exceeds the
threshold** when its network MAE is more than ten times the seasonal baseline's.

### repeating, data seed 2 — threshold 1746.4 (10 × seasonal 174.64)

| Arm | failed | over threshold | MAE min | MAE max | spread | spread over |
|---|---|---|---|---|---|---|
| original `relu` | 0/5 | 0/5 | 175.55 | 209.81 | 1.20 | 5 successful |
| a `tanh` | 0/5 | 0/5 | 170.51 | 231.52 | 1.36 | 5 successful |
| b `clipnorm` | **2/5** (rep01, rep02 — non-finite forecast) | 0/3 | 180.54 | 216.62 | 1.20 | **3 successful** |
| c both | 0/5 | 0/5 | 181.38 | 204.38 | 1.13 | 5 successful |

### weekly, data seed 2 — threshold 1752.3 (10 × seasonal 175.23)

| Arm | failed | over threshold | MAE min | MAE max | spread | spread over |
|---|---|---|---|---|---|---|
| original `relu` | 0/5 | 0/5 | 200.12 | **1485.85** | **7.42** | 5 successful |
| a `tanh` | 0/5 | 0/5 | 201.22 | 216.79 | **1.08** | 5 successful |
| b `clipnorm` | **1/5** (rep01 — non-finite forecast) | 0/4 | 202.16 | 564.68 | 2.79 | **4 successful** |
| c both | 0/5 | 0/5 | 187.22 | 233.82 | 1.25 | 5 successful |

### What Part 2 supports, and what it does not

- **Supported: failures occurred only in the clipping arm here.** All three non-finite runs
  across both scenarios were `b_clipnorm`; the other three arms had none in 30 repetitions.
- **NOT supported: "clipping caused divergence."** The comparison is small and **unmatched** —
  unseeded initialisations are not paired across arms, so the four arms did not start from
  the same weights and the difference is not attributable to the arm by design. Three events
  in fifteen repetitions is also a thin base for a causal claim about a rare failure.
- **NOT supported: that this reproduced the original exposure at all.** Each repetition here
  trains **once**. The withdrawn sweep retrained on a rolling schedule roughly **eleven times
  per run**, and a run's score was ruined if any one of those retrainings diverged.
  Single-training trials do not reproduce rolling-retraining exposure; they test a different
  and much easier condition. That, and not any property of the arms, is the most likely
  reason the failure did not reappear.
- **ReLU's instability is visible without any threshold being crossed.** On weekly its spread
  is 7.42× — one repetition at 1485.85 against another at 200.12, on the same data — while
  `tanh` holds 1.08×. Note this is *observed* spread over five unseeded repetitions, not a
  variance estimate with an interval.

---

## Conclusions

1. **Do not adopt gradient clipping** — but for the right reason. It is the only arm that
   produced failures here (3 in 15 repetitions, all non-finite). It is **not** true that it
   fails to improve accuracy: it lowers mean network MAE in both Part 1 scenarios.
2. **`tanh` is the defensible change**: lower mean network MAE (−15.4 % and −46.9 %) and a
   smaller observed spread. It remains a *sampled* improvement on four seeds and two
   scenarios, not a demonstrated fix for the divergence, and it does not get the network past
   the 10 % bar.
3. **The serving decision is unchanged**: the network stays experimental and optional, and the
   live deployment does not change on this evidence.
4. **The open question, restated.** Instrument the sweep so each published model is scored
   separately, and find out whether the failure is a per-training event whose probability
   compounds over a retrain schedule. Until that is done, nothing here — in either part —
   speaks to the configuration that actually failed.

## Failure-mode suite, re-run with corrected windows

Every entry is now marked scored or not scored; the two "recovery" cases that the withdrawn
run reported without ever seeing their events are genuinely scored here, with event coverage
confirmed **after** exclusions.

| Condition | Status | Result |
|---|---|---|
| Cold start, less than one window | SCORED | refused cleanly: "Insufficient data: 72 points (need 150)" |
| All-zero series | SCORED | finite output, no crash |
| Gap in the seasonal history | SCORED | pattern still sourced from real history; no invented values |
| Delayed observation (40 min stale) | SCORED | anchored to the data's clock |
| Level-shift recovery | **SCORED** | 1 of 1 event inside the window after exclusions; blend 1064.9 MAE, strongest baseline trend-adaptive |
| Spike recovery | **SCORED** | 2 of 3 events inside the window after exclusions; blend 457.4 MAE, strongest baseline seasonal |
| Old-format / mismatched artifact | SCORED | detected and refused |
| API failure (refusal vs transport, bounded cache) | NOT SCORED | covered by the operator's Go tests and `test_model_swap.py`, not by this harness |
| Retrain landing mid-peak | SCORED | completed without error |

Raw: `eval/failure-modes-corrected.json`.

## Reproducing

```sh
eval/.venv/bin/python eval/stabilization.py --scenarios repeating,levelshift \
    --model-seeds 11,22,33,44 --origins 120 --epochs 50 --out eval/stabilization.json
eval/.venv/bin/python eval/reproduce_divergence.py --scenarios repeating,weekly \
    --data-seed 2 --runs 5 --epochs 12 --origins 80 --history expanding \
    --out eval/divergence.json
# rebuild the Part 2 record from the console log of the 2026-09-21T22:53Z run:
eval/.venv/bin/python eval/repair_divergence_record.py
```

Re-running Part 2 produces a **different** experiment: the repetitions are unseeded by
design, so they are not reproducible, only repeatable. The repaired record is of the run that
was reviewed.

Raw output: `eval/stabilization.json`, `eval/divergence-repaired.json` (and
`eval/divergence.json`, kept as the defective original). Record integrity is enforced by
`ml-engine/tests/test_divergence_record.py`.

**Equivalence caveats** (these apply to every number above): the API's matured-error feedback
loop is not exercised, and the controller path is partial. See the note in
`RESULTS-2026-09-22-corrected.md`.
