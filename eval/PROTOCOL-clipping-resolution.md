# Protocol — does gradient-norm clipping earn a place in the serving pipeline?

**Status: WRITTEN, NOT RUN.** Nothing in this file has been executed. No model was trained, no
cluster was touched, no configuration changed. It is the predeclared design for one bounded
experiment (Codex D-128), written before its data exists.

**Clipping is NOT adopted, and nothing here proposes adopting it.** `clipnorm` stays absent
from the shipped trainer until this experiment runs and clears the bar in §6.

---

## 1. The tension this settles, stated as it actually is

Two recorded observations look contradictory and are not:

* **C-94, from `stabilization.json`:** among runs that finished, clipping lowered mean network
  MAE **229.48 → 209.48** on `repeating` and **1048.60 → 644.15** on `level_shift`.
* **C-95, from `divergence-repaired.json`:** all three non-finite runs in the divergence
  reproduction were in the clipping arm (`b_clipnorm`), 3 of 3.

Improved *mean accuracy among survivors* and *occasional catastrophic failure* are entirely
compatible: an intervention that changes the optimisation can help most of the time and blow up
some of the time, and a mean over survivors cannot see the second thing because the failures are
not in it. The tension is a **selection effect in how the two numbers were computed**, not a
contradiction in the evidence.

Three further limits on the existing evidence, all of which this design removes:

1. **The divergence reproduction trained once per repetition.** The withdrawn sweep retrained on
   a rolling schedule roughly **eleven** times per run. Single-training trials do not reproduce
   the exposure under which the failures were originally seen, so they cannot measure its rate.
2. **The arms were not matched.** Initialisations were unseeded, so no clipping run is paired
   with a no-clipping run that started from the same weights.
3. **Clipping was bit-identical to ReLU in 3 of 8 seed-runs** in the stabilization study,
   including the run that produced the minimum. Nominal repetitions overstate the informative
   ones.

## 2. What is held fixed, so that only clipping varies

| | Value | Why it is fixed |
|---|---|---|
| **Activation** | `relu` in **both** arms | C-94 confounded activation with clipping; `c_both` differed from `a_tanh` in two ways at once. One variable per experiment |
| **Arms** | `control` = no clipping · `treatment` = `clipnorm = 1.0` | The only difference between them |
| **Initialisation** | **matched seed per pair** — both arms start each repetition from bit-identical weights and see identically ordered batches | Makes the comparison paired; removes between-seed variance from the contrast |
| **Data window** | identical series, identical origins, identical train/validation split per pair | A different window is a different experiment |
| **Training budget** | identical epochs, batch size, learning-rate schedule, early-stopping rule | Clipping must not win by training longer |
| **Retraining schedule** | **rolling, 11 retrainings per run**, identical cadence in both arms | Reproduces the exposure under which the failures were actually seen (limit 1 above) |
| **Publication delay** | identical delay between a model finishing training and its forecasts being served | A shorter delay is an unearned advantage; this is a serving-pipeline experiment |
| **Scenarios** | `repeating` and `level_shift` | The two where C-94 reported an effect. Scored separately, never pooled into one verdict |
| **Objective** | `EVAL_OBJECTIVE = "uniform"` | The one external objective used everywhere since C-96 |

## 3. Size, and the predeclared failure risk it buys

**Acceptable failure risk, declared before the run:** a **served** non-finite forecast is
**never** acceptable. The pipeline is expected to reject such a forecast and retain the
incumbent model; the experiment therefore measures two different rates and holds them to two
different standards.

| Quantity | Standard |
|---|---|
| **Served non-finite forecasts** | must be **zero**, in both arms, over the whole experiment |
| **Rejected forecasts** (pipeline caught it, incumbent retained) | treatment's rate must not exceed control's by more than **2× **, reported per training |
| **Trainings ending in non-finite weights** | counted and reported per arm; not disqualifying on its own if every one was caught |

**Repetitions, sized to that:**

| | |
|---|---|
| Matched seed pairs per scenario | **20** |
| Scenarios | 2 (`repeating`, `level_shift`) |
| Runs per arm | **40** |
| Rolling retrainings per run | **11** |
| **Trainings per arm** | **440** |
| Expected *informative* pairs (not bit-identical) | **≈ 25** of 40, from the 5-of-8 rate observed in C-94 |

**What zero observed served-failures would establish, stated in advance so it cannot be
overstated afterwards.** With no events observed, the conventional 95 % upper bound is three
events:

* per training: 3 / 440 = **0.68 %**
* per run: 3 / 40 = **7.5 %**

**That run-level bound is the honest ceiling on this experiment's claim.** A clean result says
*"the served-failure rate is below roughly 7.5 % per run"*, not *"clipping is safe"*. Buying a
2 % run-level bound would need **150 runs per arm** — that cost is stated here so the owner can
choose the claim they want before the run rather than discover the limit afterwards. The
experiment is deliberately **bounded**: it is sized to settle a tension, not to certify safety.

## 4. Every artifact is recorded, including the ones that failed

The failure mode this replaces is C-95's: the reproduction keyed repetitions by model seed, and
every unseeded repetition shared the key `None`, so **32 of 40 repetitions were silently
overwritten** and the surviving JSON showed only each arm's last outcome. Nothing here may be
keyed by anything that can collide.

Per **(scenario, pair index, arm, retraining index)** — a key that is unique by construction —
record:

* a unique repetition id, the matched seed, and the arm;
* the **model artifact hash**, and whether the artifact was written at all;
* every **non-finite event**: which training, which origin, which horizon step, and the value;
* every **forecast rejection** by the serving contract, with the rejecting rule;
* every **incumbent-retention outcome** — which model actually served, for how many origins;
* the **gradient norms** before and after clipping, so "clipping never bound" is a measurement
  rather than the inference it was in C-94 (`c_both` matching `a_tanh` was *consistent with*
  clipping not binding under tanh, and no gradient norm existed to confirm it);
* whether the pair's two runs were **bit-identical**, so informative pairs can be counted;
* accuracy computed **three ways**: over survivors only, over all runs with failures scored as
  the incumbent's served forecast, and over all runs with failures excluded but counted.

Spreads and means are computed over **successful runs only**, with the denominator stated next
to every figure, and failures reported as counts beside them — never absorbed into an average.

## 5. Adoptability is a property of the serving pipeline, not of survivor MAE

The question is not *"does clipping lower MAE in the runs that finished?"* — C-94 already
answers that, and the answer is not sufficient. The question is *"does a pipeline that clips
serve better forecasts than a pipeline that does not, counting everything that happens when it
fails?"*

So the primary accuracy figure is computed over the **forecasts actually served**, with the
incumbent's forecast standing in wherever a new model was rejected. A clipping arm that trains a
better model and then has it rejected has not improved anything. A clipping arm whose failures
are all caught and whose survivors are better has improved something real, and the record will
show which.

Operational consequence is measured the same way it is everywhere else in this repository: by
**controller replay over the served forecasts**, in shortage-minutes and replica-minutes.

## 6. The bar for adoption, predeclared

All four must hold. Any one failing means clipping is not adopted.

| | Requirement |
|---|---|
| **K1** accuracy | paired mean MAE reduction on **served** forecasts ≥ **5 %**, with the 95 % CI on the paired difference excluding zero, computed over **informative pairs** with the bit-identical count reported alongside. Held per scenario, not pooled |
| **K2** failures | **zero** served non-finite forecasts in either arm; treatment's rejection rate ≤ 2× control's |
| **K3** operational | no shortage regression beyond the owner's **Q1** budget and no replica regression beyond **Q2** (`TOLERANCE-DERIVATION.md` §3), measured by controller replay over served forecasts |
| **K4** mechanism | the recorded gradient norms show clipping actually **bound** in the treatment arm. If it never bound, any difference came from somewhere else and the experiment did not test what it claims |

**K3 is blocked** on the same owner decision as `PROTOCOL-seasonal-default-selector.md` §5:
Q1 and Q2 do not yet exist. K1, K2 and K4 are self-contained and can be evaluated without them,
but **adoption requires all four**, so no adoption decision can be reached until the budgets are
set.

## 7. What this experiment cannot establish

* **That clipping is safe.** See §3: zero failures in 40 runs bounds the rate near 7.5 % per
  run, and no further.
* **That clipping caused the earlier divergences.** The C-95 comparison remains small and
  unmatched; the permitted claim stays *"failures occurred only in the clipping arm here"*. This
  experiment can produce a *matched* rate comparison, which is a different and better statement,
  but a rate is not a cause.
* **Anything about clipping under `tanh`.** Activation is fixed at `relu` by design (§2). The
  `c_both` question is not asked here.
* **Generalisation beyond these two synthetic scenarios**, one architecture and one training
  budget.

## 8. Freeze before the run

Append to this file before any data is generated: the matched seed list, the exact training
configuration for both arms, the retraining cadence, the publication delay, the scenario
definitions, the owner's Q1/Q2 for K3, and the commit hash of the code that will run. Then
`git commit`. Any change afterwards makes it a new experiment.
