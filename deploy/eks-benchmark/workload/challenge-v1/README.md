# challenge-v1 offered load (frozen 2026-09-27)

The declared challenge regime from `eval/PROTOCOL-challenge-workload.md` (user decision U-22,
2026-09-27: one clean day of repeating traffic first, then this). Five generators, one per arm, run
`k6-load-script-challenge-v1.yaml`. The offered rate is a pure function of wall-clock time, the
sealed seed and `profile.json`, so every arm gets identical traffic.

- **Before `regime_start` (2026-09-28T00:00:00Z)** it is exactly the repeating-v2 hourly profile,
  so the script can be installed ahead of time. The switch happens on the clock, not at a restart.
- **From `regime_start`**, per ten-minute slot, rate = hourly profile × clip(1 + noise + drift +
  level, 0.75, 1.15):
  - noise: AR(1), phi 0.85 per slot (about 1 h memory), stationary sd 5 %, output clipped to ±12 %;
  - drift: a triangle wave with a 72 h period and ±6 % amplitude, which reverses within the phase;
  - level shifts: Bernoulli 1/72 per slot (about two per day), at least 12 slots (2 h, longer than
    the 60-minute horizon) apart; each new level is uniform in ±12 %.
- **Bounds:** the peak stays ≤ 6,000 × 1.15 = 6,900 rpm, inside the 12 × 600 rpm ceiling of every
  arm, so the workload never measures the replica cap instead of the forecaster.

`challenge_profile.py` is the Python reference: `rate_at(t)` and `planned_requests(hour_start)`
serve the hourly load gates, which must use it instead of the fixed hourly table once the regime
starts. `ml-engine/tests/test_challenge_workload.py` checks the bounds, statistics and dwell, and
that the script's schedule block run in Node reproduces the Python random stream bit for bit,
multipliers within 1e-9 and integer rates exactly. A probe on the real k6 0.48 runtime (goja)
matched 80/80 rates and built 2,305 stages.

Parameters are frozen: a changed bound is a new profile version and a new regime. The regime
boundary is to be labelled in validity-mask v3; history under repeating-v2 stays valid evidence
about repeating-v2. The live forecasters keep training across the boundary on purpose (production
does not get a clean restart), and every evaluation reports each regime separately.
