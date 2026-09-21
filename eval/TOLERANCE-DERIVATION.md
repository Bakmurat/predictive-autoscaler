# Deriving a non-inferiority margin, 2026-09-22

Codex C-98 / D-123 asked for one thing: a tolerance that comes **from acceptable additional
shortage, replica consumption and workload prevalence** — never from the observed miss — and
that is judged with an uncertainty bound against that justified margin.

This file does that derivation. It applies to **future** evaluations. It does **not** reopen
the recorded S2 rejection: see §6.

---

## 0. The previous 2 % was not derived this way. Stated plainly.

`BAR_REPEATING_TOLERANCE = 0.02` in `eval/selector.py` was **a round number chosen because it
sounded tolerable.** No shortage budget, no replica budget and no prevalence weighting stand
behind it. It was written before the test set, which makes it honest as a *preregistration*
and worthless as a *justification*. Everything below exists because a preregistered arbitrary
number is still an arbitrary number.

---

## 1. The shape of the rule

A non-inferiority margin should answer: *how much worse may the challenger be on this
workload before a person running the system would notice or care?* Three inputs, in operating
units, never in proxy units:

```
margin_operating = min over consequences of ( acceptable_additional_consequence )
                   / workload_prevalence

decision: the challenger is non-inferior iff the UPPER bound of the 95 % interval on the
          observed regression is <= margin -- not the point estimate.
```

Two properties this shape has and a bare percentage does not:

* **It is recomputed per evaluation window**, from the *incumbent's own measured* consumption
  in that window. A window in which the incumbent already uses most of the allowance leaves
  less room for a challenger, and the margin tightens automatically.
* **It is frozen before the evaluation**, on development data, exactly like every other
  preselected parameter.

---

## 2. Input 1 — acceptable additional shortage

Shortage is the only consequence with a user-visible meaning: minutes in which ready
capacity was below the unconstrained requirement.

There is **no published SLO for this benchmark**, so the allowance has to be declared rather
than looked up. The declaration, and its reasoning:

> **Capacity-availability allowance: 99.9 % of minutes**, i.e. at most **1.44 shortage-minutes
> per day**. Chosen as the weakest conventional availability target that is still a target at
> all; a service that tolerates more than ~1.4 min/day of under-capacity is not making an
> availability claim worth testing.

The budget for a challenger is the allowance **minus what the incumbent already spends**:

| | value | source |
|---|---|---|
| Replay window per arm (repeating workload) | 21 780 min = **15.12 days** | 3 runs × 726 steps × 10 min |
| Incumbent (`seasonal_pattern`) shortage | 8 min = **0.529 min/day** (0.037 % of minutes) | controller replay |
| Allowance at 99.9 % | **1.440 min/day** | declared above |
| **Additional-shortage budget** | **0.911 min/day** | allowance − incumbent |

## 3. Input 2 — acceptable additional replica consumption

Replica-minutes cost money and carry no availability meaning, so the budget is an efficiency
policy rather than a safety one.

> **Replica budget: 1 % of the incumbent's consumption**, declared for the same reason as
> above — it is the smallest round efficiency regression a capacity owner would still call
> acceptable rather than free.

| | value |
|---|---|
| Incumbent replica-minutes | 62 806 over the window = **4 152 rm/day** (mean 2.88 replicas) |
| **Additional-replica budget at 1 %** | **41.5 rm/day** |

**On this workload replica consumption is not the binding constraint, and the data says so.**
Across all five arms the total spread in replica-minutes is 510 rm, **0.81 %** of the
incumbent's consumption, and every alternative arm used *fewer* replica-minutes than the
incumbent, not more. An arm cannot breach an upper budget it is already under.

## 4. Input 3 — workload prevalence

A regression on a workload present a fraction *p* of the time costs *p* × regression in
expectation, so the per-instance margin scales as `1 / p`.

The benchmark generator runs the repeating daily profile essentially **all** the time, so
**p = 1.0** and there is no relaxation. This is precisely why `repeating` earned a
non-inferiority test rather than being pooled away. If a future evaluation runs a workload
mixture, *p* must be measured from that mixture — from hours observed, not from the number of
scenario names.

## 5. Converting to the ranking proxy, and why that conversion is the weak link

The bar is read from a **cost proxy in rpm**; the budgets above are in minutes and
replica-minutes. The exchange rate has to be estimated, and it is the least trustworthy step
in this document.

Estimated from the five arms actually observed on the repeating workload (descriptive data
from the defective evaluation — see `SELECTOR-RESULTS-2026-09-22.md`):

| Arm | Δ proxy vs incumbent | Δ proxy % | Δ shortage (min/day) | implied rpm per shortage-min/day | Δ replica-minutes |
|---|---|---|---|---|---|
| `selector` | +4.39 | **+2.35 %** | **0.000** | — (proxy moved, shortage did not) | **−20** |
| `constant_blend` | +39.65 | +21.24 % | +0.264 | 149.9 | −20 |
| `trend_adaptive` | +62.30 | +33.37 % | +0.529 | 117.8 | −60 |
| `lagged_linear` | +67.57 | +36.20 % | +1.455 | 46.5 | −510 |

A least-squares fit over all five arms gives **43.7 rpm per shortage-minute per day**
(R² = 0.70); the pairwise estimates span **46–150**. Take the *most pessimistic* (smallest)
rate, 43.7 rpm, so the margin errs tight:

```
margin_proxy = 0.911 shortage-min/day  x  43.7 rpm per shortage-min/day
             = 39.8 rpm
             = 21.3 % of the incumbent's 186.67 rpm
```

**Sensitivity**, because a single number here would be false precision:

| exchange rate used | derived margin |
|---|---|
| 43.7 rpm (least-squares, most pessimistic) | 39.8 rpm = **21.3 %** |
| 117.8 rpm (`trend_adaptive` pair) | 107.3 rpm = 57.5 % |
| 149.9 rpm (`constant_blend` pair) | 136.6 rpm = 73.2 % |

Every route lands an order of magnitude above 2 %.

### The resolution floor — the number that matters most

The finest difference the operating measurement can even *see* on this workload is **one
shortage-minute per day**, which is **≈ 43.7 rpm ≈ 23 % of the proxy**. A 2 % proxy difference
is therefore **below the resolution of any operational consequence measured here.** The
selector row proves it directly: **+2.35 % proxy, zero additional shortage-minutes, twenty
*fewer* replica-minutes, identical scaling-event count.**

## 6. What this does and does not change

**Does not:** the S2 rejection recorded in `SELECTOR-RESULTS-2026-09-22.md` **stands and is
not waived.** The rule was written before the test set, the test set was consumed against it,
and a tolerance derived afterwards — however much better derived — is still a tolerance chosen
after seeing the number. Retrofitting a margin that happens to pass is exactly the failure
mode preregistration exists to prevent. The honest record is: *the experiment failed a bar
that was, in hindsight, far stricter than operating consequences justify.*

**Does:** the next evaluation uses a derived margin instead of a round one. Concretely, for
the protocol in `PROTOCOL-seasonal-default-selector.md`:

1. Declare the availability allowance and the replica budget **before** the run (they are in
   §2 and §3 and are now frozen).
2. Measure the **incumbent's** shortage and replica consumption on **development** data from
   the new window; compute `margin_operating` from the remainder; divide by measured
   prevalence.
3. Convert with the exchange rate re-estimated **on that development data**, taking the most
   pessimistic pairwise rate.
4. Freeze the resulting margin, in both operating and proxy units, in the protocol file
   before the test data is touched.
5. Judge with the **95 % interval's upper bound against the margin**, not the point estimate.
   Report the point estimate, the interval, and the margin together, always.
6. **Report the operating-unit comparison as primary.** The controller replay measures
   shortage and replica-minutes directly; the proxy is a screening statistic with an R² = 0.70
   relationship to the thing that matters, and must never be the last word.

### Stating a result correctly, worked on the numbers we have

Observed: relative gap **+2.35 %**, 95 % CI **[+1.35 %, +3.39 %]**, old margin 2 %.

* ✗ "the interval is tight, so this is a real regression beyond tolerance" — conflates two
  questions.
* ✓ "the interval excludes zero, so there is a regression relative to none; the interval
  **contains** 2 %, so exceedance of that tolerance is **not** established; and against the
  derived margin of 21 % the upper bound of +3.39 % is comfortably non-inferior — a margin
  that was not in force at the time."

## 7. Reproducing

Every number in §2–§5 comes from `eval/selector-20260922.json`, repeating scenario, seeds
11–13, and from the replay window arithmetic 3 × 726 × 10 min. The derivation is arithmetic
on those values; no new run is needed to check it.
