# Stabilization experiment — 2026-09-22

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

**Not established: that any arm removes the divergence.** The honest summary is narrower
and comes in three parts.

1. **The matched control did not fail either.** Under a seeded, 50-epoch, rolling-7-day
   configuration, *every* arm including ReLU had zero failed seeds. An experiment in which
   the control never fails cannot show that a change prevents failure. This is why the first
   reading of these arms — "tanh removes the divergence" — was withdrawn before publication.
2. **Under the conditions that originally failed, ReLU still did not diverge**, so the
   failure was not reproduced and no arm can be credited with preventing it.
3. **What the arms do show is a large and consistent reduction in seed-to-seed spread**,
   which is a weaker but real claim.

## Part 1 — stability under the corrected configuration

50 epochs, rolling 7-day window, seeded (11/22/33/44), 120 scored origins, one training per
arm and seed. `spread` is max MAE ÷ min MAE across the four seeds; lower is more stable.

### repeating, data seed 1 — baselines: seasonal 170.9, previous-day 187.8, trend-adaptive 192.4

| Arm | network MAE min | max | spread | failed seeds | blend MAE (4 seeds) |
|---|---|---|---|---|---|
| baseline `relu` | 201.25 | 277.85 | 1.38 | 0 | 163.6, 161.7, 164.2, 169.0 |
| a `tanh` | **184.66** | **203.93** | **1.10** | 0 | 160.2, 161.4, 160.5, 160.3 |
| b `clipnorm` | 201.25 | 215.35 | **1.07** | 0 | 164.0, 161.7, 163.4, 162.8 |
| c both | 184.66 | 203.93 | 1.10 | 0 | 160.2, 161.4, 160.5, 160.3 |

### levelshift, data seed 1 — baselines: seasonal 267.6, previous-day 291.1, trend-adaptive 298.2

| Arm | network MAE min | max | spread | failed seeds | blend MAE (4 seeds) |
|---|---|---|---|---|---|
| baseline `relu` | 599.94 | 1851.33 | **3.09** | 0 | 282.9, 336.9, 291.4, 404.5 |
| a `tanh` | **484.38** | 621.51 | **1.28** | 0 | 271.2, 287.9, 278.2, 290.7 |
| b `clipnorm` | 599.94 | 686.65 | **1.14** | 0 | 282.9, 292.5, 291.4, 297.4 |
| c both | 484.38 | 621.51 | 1.28 | 0 | 271.2, 287.9, 278.2, 290.7 |

Three things worth stating:

- **`tanh` is the only arm that improves accuracy as well as stability.** It lowers the
  network's error floor (201→185 on repeating, 600→484 on level shift) *and* compresses the
  spread. Clipping compresses the spread without moving the floor at all — its minimum is
  identical to the baseline's to two decimals in both scenarios.
- **Arm c is numerically identical to arm a**, digit for digit, in both scenarios. With
  `tanh` the gradient norm never reaches the clipping threshold, so the combination adds
  nothing. That is why c must not be reported as "the best arm": it *is* arm a.
- **The blend still does not clear the bar.** Best blend on repeating is 160.2 against a
  seasonal baseline of 170.9 — 6.3% better, short of the 10% requirement. On level shift the
  best blend (271.2) is slightly *worse* than the seasonal baseline (267.6).

## Part 2 — reproducing the conditions that actually failed

The withdrawn run's divergence appeared under **unseeded initialisation, 12 epochs,
expanding history**. Part 1 used none of those. So the arms were re-run under the original
configuration, five unseeded repetitions each. A run counts as diverged when the network's
MAE exceeds **ten times** the seasonal baseline's, or goes non-finite.

### repeating, data seed 2 — diverged if network MAE > 1746.4

| Arm | diverged | MAE min | MAE max | spread |
|---|---|---|---|---|
| original `relu` | **0/5** | 175.55 | 209.81 | 1.20 |
| a `tanh` | 0/5 | 170.51 | 231.52 | 1.36 |
| b `clipnorm` | **2/5** | 180.54 | 216.62 | 1.20 |
| c both | 0/5 | 181.38 | 204.38 | 1.13 |

### weekly, data seed 2 — diverged if network MAE > 1752.3

| Arm | diverged | MAE min | MAE max | spread |
|---|---|---|---|---|
| original `relu` | 0/5 | 200.12 | **1485.85** | **7.42** |
| a `tanh` | 0/5 | 201.22 | 216.79 | **1.08** |
| b `clipnorm` | 1/5 | 202.16 | 564.68 | 2.79 |
| c both | 0/5 | 187.22 | 233.82 | 1.25 |

**The failure did not reproduce.** ReLU diverged in 0 of 10 runs across the two scenarios,
so the experiment has no failure to prevent, and no arm can be credited with preventing one.
Two observations that do survive:

- **Gradient clipping made things worse.** It is the only arm that produced divergences at
  all — 2 of 5 on repeating, 1 of 5 on weekly. Clipping the gradient norm to 1.0 appears to
  destabilise training here rather than steady it. It should not be adopted.
- **ReLU's instability is visible without a formal divergence.** On weekly its spread is
  **7.42×** — one run at 1485.85 against another at 200.12, on the same data — while `tanh`
  holds 1.08×. That is the clearest signal in the whole experiment, and it is about variance,
  not about the threshold-crossing failures the withdrawn run recorded.

## Why the original divergence probably did not reappear

A hypothesis, stated as one and not tested: the withdrawn sweep retrained roughly eleven
times per run on a rolling schedule, and a run's score was ruined if *any* of those
retrainings diverged. Each experiment here trains **once**. Eleven chances to fail per run
against one is a large difference in exposure, and it would explain why single-training
repetitions look stable while the rolling sweep did not. Confirming it means instrumenting
the sweep to score every published model separately — not done.

## Conclusions

1. **Do not adopt gradient clipping.** It is the only change that caused divergences.
2. **`tanh` is the defensible change** — lower error floor and a large reduction in spread
   (7.42× → 1.08× where ReLU is worst) — but it is a *variance* improvement, not a
   demonstrated fix for the divergence, and it does not get the network past the 10% bar.
3. **The serving decision is unchanged**: the network stays experimental and optional, and
   the live deployment does not change on this evidence.
4. Before any further work on the network: instrument the sweep so each published model is
   scored separately, and find out whether the divergence is a per-training event whose
   probability compounds over the retrain schedule. That is the question this experiment
   raised and did not answer.

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
```

Raw output: `eval/stabilization.json`, `eval/divergence.json`.

**Equivalence caveats** (these apply to every number above): the API's matured-error feedback
loop is not exercised, and the controller path is partial. See the note in
`RESULTS-2026-09-22-corrected.md`.
