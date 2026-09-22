# The non-inferiority margin: a decision the owner has to make

**Status: NO MARGIN IS IN FORCE.** This file does not set one. It states the quantities a
margin must be built from, shows what two or three defensible choices would imply, and stops
there. Until the owner picks the budgets in §3, **no non-inferiority test in this repository
has a threshold**, and the S2 bar in `PROTOCOL-seasonal-default-selector.md` is blocked rather
than merely unset.

> ### ⚠ This file was rewritten on 2026-09-22 after Codex round 26 (C-99 / D-125)
>
> The previous version called itself a *derivation* and proposed **21.3 %**. It was not a
> derivation, for three reasons, and the number is **withdrawn**:
>
> 1. **It invented its own budgets.** It declared a 99.9 % "capacity-availability allowance"
>    and a 1 % replica budget and then derived from them. Nobody authorised either. Replacing
>    an arbitrary 2 % with two arbitrary numbers and an arithmetic chain is not a derivation;
>    it is the same arbitrary act, further from the reader.
> 2. **It conflated shortage with availability.** A shortage-minute here is a minute in which
>    ready capacity was below the unconstrained requirement. That is not a minute in which the
>    service was down — see §2. Reading an availability SLO onto it imports a number from a
>    different quantity.
> 3. **Its resolution arithmetic was wrong.** It claimed the finest measurable difference was
>    *one shortage-minute per day*. One replay-minute spread across 15.12 pooled days is
>    **0.066 minute/day**, about fifteen times finer. Everything that followed from the error
>    — including "2 % is below the resolution of anything measured here" — is withdrawn in §5.
>
> The proxy-to-shortage conversion the 21.3 % rested on is withdrawn too (§4). What replaces
> all of it: **margins stated directly in operating units**, chosen by the owner, in §3.

---

## 1. What has to be decided, in one place

Three quantities, and nothing else, determine a defensible margin. None of them is a
measurement; all three are **policy**, and the system cannot supply them:

| | Question for the owner | Units |
|---|---|---|
| **Q1** | How many **additional shortage-minutes per day** is it acceptable to incur by choosing the challenger over the incumbent? | shortage-min/day |
| **Q2** | How much **additional replica consumption** is acceptable for that choice? | replica-min/day, or % of the incumbent's |
| **Q3** | What fraction of hours does each **workload shape** occupy in the mixture being evaluated? | dimensionless, measured in hours |

From those, and only those:

```
margin(shape) = acceptable_additional_consequence / prevalence(shape)

decision: non-inferior iff the UPPER bound of the 95 % interval on the observed
          regression, IN OPERATING UNITS, is <= margin(shape).
```

Two properties this keeps from the previous version, because they were the parts that were
right:

* **Recomputed per window from the incumbent's own measured consumption.** A window in which
  the incumbent already spends most of the allowance leaves a challenger less room, and the
  margin tightens by itself.
* **Frozen on development data before the test data is touched**, like every other
  preselected parameter.

And one property it did not have: **the comparison is made in operating units end to end.**
The cost proxy is a screening statistic. It never converts into a margin (§4).

## 2. Why the budgets cannot be read off a standard, and what shortage is

There is no published SLO for this benchmark, so Q1 and Q2 must be *declared*. The previous
version treated that as licence to declare them itself. It is not.

**Shortage is not availability.** `shortage_minutes` is produced by the controller replay and
counts minutes in which ready replicas were fewer than the unconstrained requirement. In such
a minute the service is very probably still serving — more slowly, with a longer queue, with
less headroom for the next arrival. The replay **prices no queueing, no latency and no dropped
request**. So:

* a 99.9 % availability target does **not** translate to 1.44 shortage-min/day;
* conversely, a shortage-minute is not free merely because no request failed.

What the owner is actually being asked in Q1 is *how much under-capacity, measured this way,
they are willing to buy with whatever the challenger offers*. That is a judgement about their
own risk appetite. It has no correct answer derivable from this repository.

## 3. Three worked options. The owner picks one, or supplies their own.

All three are computed against the **incumbent's measured consumption in the spent window**,
which is descriptive context, not evidence for any option:

| Incumbent (`seasonal_pattern`), pooled repeating window | value |
|---|---|
| Window per arm | 21 780 min = **15.125 days** (3 runs × 726 steps × 10 min) |
| Shortage | 8 min = **0.529 min/day** |
| Replica consumption | 62 806 rm = **4 152 rm/day** (mean 2.88 replicas) |
| **Measurement resolution** — one replay-minute over the pooled window | **0.066 min/day** |

The last row is the one that constrains every option, and it is why "strict" is not free:
a budget of *B* shortage-min/day is only **B / 0.066** resolution units wide in a 15.1-day
window, and a test that cannot see several units cannot distinguish "inside the budget" from
"at the budget".

| | **A — no measurable regression** | **B — one shortage-minute a week** | **C — availability-style allowance** |
|---|---|---|---|
| **Q1** additional shortage | **0.07 min/day**<br>(≈ 1 min per 15 days) | **0.14 min/day**<br>(≈ 1 min per 7 days) | **1.44 min/day**<br>(what 99.9 % of minutes *would* give, read as under-capacity) |
| **Q2** additional replicas | **0.5 %** = 20.8 rm/day | **1 %** = 41.5 rm/day | **1 %** = 41.5 rm/day |
| Margin at prevalence p = 1.0 | 0.07 min/day | 0.14 min/day | 1.44 min/day |
| Margin at prevalence p = 0.5 | 0.14 min/day | 0.28 min/day | 2.88 min/day |
| Resolution units in a 15.1-day window | **1.1** | **2.1** | **21.8** |
| Window needed for 3 resolution units | **≈ 43 days/arm** | **≈ 21 days/arm** | **≈ 2.1 days/arm** |
| Is Q2 binding on the evidence we have? | **Yes** — below the 33.7 rm/day spread observed across all five arms | No — above it | No — above it |
| What it buys | a challenger must be indistinguishable from the incumbent at the measurement's own floor | a regression a capacity owner would notice about once a week | a regression bounded by a conventional-sounding number, with §2's caveat attached |
| What it costs | a six-week window per arm, or a decision resting on a single shortage-minute | a three-week window per arm | admits regressions ~22× the measurement floor; the number is borrowed from a quantity this benchmark does not measure |

**None of these is chosen.** They exist so that the owner is choosing between stated
consequences rather than between percentages.

**Q3 is not an option, it is a measurement.** The current generator runs the repeating daily
profile essentially all the time, so p = 1.0 for `repeating` and there is no relaxation — which
is exactly why `repeating` gets its own non-inferiority test instead of being pooled away. If
the challenge phase (`PROTOCOL-challenge-workload.md`) introduces a mixture, p must be counted
from **hours observed in the validity-masked series**, never from the number of scenario names
in a config.

### Once an option is picked

1. Record the chosen Q1, Q2 and the measured Q3 **in the protocol file**, with a commit hash,
   before any test data is generated.
2. Measure the **incumbent's** shortage and replica consumption on **development** data from
   the new window; the challenger's budget is the allowance minus what the incumbent already
   spends.
3. Divide by measured prevalence to get the per-shape margin.
4. Check the implied window length against the resolution row above. **If the window cannot
   resolve the margin to at least three units, the experiment is under-powered for that margin
   and must either run longer or adopt a looser one — decided before the run, not after.**
5. Judge on the **95 % interval's upper bound**, in operating units, against the margin. Report
   the point estimate, the interval and the margin together, always.

## 4. The proxy-to-shortage conversion is withdrawn

The previous version converted a proxy difference in rpm into shortage-minutes at **43.7 rpm
per shortage-min/day**, least-squares over five arms, R² = 0.70. That conversion is withdrawn
and no margin may use it. The descriptive table it came from (spent test, seeds 11–13):

| Arm | Δ proxy | Δ proxy % | Δ shortage (min/day) | implied rpm per shortage-min/day | Δ replica-minutes |
|---|---|---|---|---|---|
| `selector` | +4.39 | **+2.35 %** | **0.000** | **undefined** | **−20** |
| `constant_blend` | +39.65 | +21.24 % | +0.264 | 149.9 | −20 |
| `trend_adaptive` | +62.30 | +33.37 % | +0.529 | 117.8 | −60 |
| `lagged_linear` | +67.57 | +36.20 % | +1.455 | 46.5 | −510 |

Why five points cannot license a conversion:

* **They are not five independent observations.** All four contrasts share one incumbent, one
  window and the same three seeds. n = 5 arms is not n = 5 experiments.
* **The pairwise rates span 46–150 rpm**, a factor of 3.3. An R² of 0.70 across a 3.3× spread
  is the fit reporting that the relationship is not a line, not that it found one.
* **The row the decision was about carries no slope information.** `selector` moved the proxy
  by +4.39 with a shortage change of exactly zero. It lies on no line through the origin; the
  one arm the margin had to adjudicate is the one the fit cannot see.
* **It was estimated on the data that produced the miss and used to excuse the miss.** A
  conversion fitted to the spent test, then applied to widen the bar that test failed, is
  precisely the retrofit that preregistration exists to prevent — regardless of how carefully
  the arithmetic was done.

Operating units need no conversion: the controller replay reports shortage-minutes and
replica-minutes directly. The proxy stays useful for **ranking candidates cheaply during
development**, and may never be the last word on a decision.

## 5. The corrected resolution arithmetic, and what it does and does not license

**Wrong, in the previous version:** *"the finest difference the operating measurement can see
is one shortage-minute per day ≈ 43.7 rpm ≈ 23 % of the proxy; a 2 % proxy difference is
therefore below the resolution of any operational consequence measured here."*

**Corrected:** the finest difference is one replay-minute across the **pooled** window:

```
1 min / 15.125 days = 0.066 shortage-min/day
```

about fifteen times finer than claimed. If one insisted on expressing that in proxy units at
the (now withdrawn) 43.7 rpm rate it would be **2.89 rpm = 1.55 %** of the incumbent's 186.67
rpm — so the old 2 % tolerance sat **just above** the measurement floor, not an order of
magnitude beneath it. **The claim that 2 % was below the resolution of anything measured here
is withdrawn**, and so is the conclusion that leaned on it.

**What zero observed additional shortage-minutes actually bounds.** The `selector` row's
0.000 min/day is an observation of *no events in 21 780 minutes*, which is not a rate of zero.
Taking the usual zero-event bound, three events is the 95 % upper limit on the count, i.e.
**≈ 0.2 additional shortage-min/day** — and even that is optimistic, because shortage-minutes
arrive in runs rather than independently, so an honest block-based bound is wider still. The
correct statement is therefore: *the selector's additional shortage rate is bounded above by
roughly 0.2 min/day on this window*, **not** *the selector adds no shortage*.

**What still stands as fact about that window:** +2.35 % proxy, zero observed additional
shortage-minutes, 20 **fewer** replica-minutes, identical scaling-event count. That is a real
observation and it is worth recording. It is not a demonstration of operational equivalence —
neither the resolution arithmetic nor an unchanged count establishes that.

## 6. The S2 rejection: a procedural outcome, and nothing more

The rejection recorded in `SELECTOR-RESULTS-2026-09-22.md` **stands and is not waived.**

**What it is.** The rule was fixed before the test set. The test set was consumed against it.
The observed relative gap was **+2.35 %, 95 % CI [+1.35 %, +3.39 %]**, against a **2 %** bar.
The interval excludes zero, so there is a regression relative to none; the interval **contains
2 %**, so exceedance of the bar is **not** statistically established. The experiment therefore
failed a preregistered threshold. That is the whole of it, and it is binding: a tolerance
revised after seeing the number cannot retroactively pass a run.

**What it is not.** It is **not evidence that the selector is worse in practice.** The bar it
missed was a round number with nothing behind it (§7), the miss was not statistically
established as an exceedance, and the operating measurements moved either not at all or in the
selector's favour. Anyone citing this rejection as a finding about the selector's operational
quality is citing it wrongly. The honest one-line summary is:

> *A preregistered procedural bar was missed. The bar was underived, the exceedance was not
> established, and no operational disadvantage was measured. The result is a procedural
> failure, not a demonstration of practical inferiority.*

This distinction is why the next experiment needs a margin that means something — and why it
cannot start until §3 is answered.

## 7. The record of the old 2 %, kept deliberately

`BAR_REPEATING_TOLERANCE = 0.02` in `eval/selector.py` was **a round number chosen because it
sounded tolerable.** No shortage budget, no replica budget and no prevalence weighting stood
behind it. It was written before the test set, which makes it honest as a *preregistration* and
worthless as a *justification*. This paragraph stays in the file permanently: a preregistered
arbitrary number is still an arbitrary number, and the failure mode it represents is the one
this document exists to stop repeating — including in its own first version.

## 8. Reproducing

Every number in §3–§5 comes from `eval/selector-20260922.json`, repeating scenario, seeds
11–13, and from the window arithmetic 3 × 726 × 10 min = 21 780 min = 15.125 days. Nothing here
needs a new run; it is arithmetic on recorded values, and the arithmetic is the part the
previous version got wrong.
