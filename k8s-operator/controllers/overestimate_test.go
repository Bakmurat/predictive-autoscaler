package controllers

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAdjustForOverestimation_RatioAboveThreshold_StreakIncrements(t *testing.T) {
	// ratio 1.3 (predicted=13, reactive=10), streak increments each call, caps at streak 3
	state := &scaleState{}
	log := logr.Discard()

	// Call 1: streak -> 1, no cap yet
	result := adjustForOverestimation(log, 13, 10, state, 0, nil, "test-app", "test-ns")
	if result != 13 {
		t.Errorf("call 1: expected 13 (no cap yet), got %d", result)
	}
	if state.overestimateStreak != 1 {
		t.Errorf("call 1: expected streak 1, got %d", state.overestimateStreak)
	}

	// Call 2: streak -> 2, still no cap
	result = adjustForOverestimation(log, 13, 10, state, 0, nil, "test-app", "test-ns")
	if result != 13 {
		t.Errorf("call 2: expected 13 (no cap yet), got %d", result)
	}
	if state.overestimateStreak != 2 {
		t.Errorf("call 2: expected streak 2, got %d", state.overestimateStreak)
	}

	// Call 3: streak -> 3, cap fires: ceil(10*1.1) = 11
	result = adjustForOverestimation(log, 13, 10, state, 0, nil, "test-app", "test-ns")
	if result != 11 {
		t.Errorf("call 3: expected 11 (capped), got %d", result)
	}
	if state.overestimateStreak != 3 {
		t.Errorf("call 3: expected streak 3, got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_RatioBelowThreshold_StreakResets(t *testing.T) {
	// ratio 1.15 (predicted=23, reactive=20), streak resets to 0, returns predicted unchanged
	state := &scaleState{overestimateStreak: 2} // pre-existing streak
	log := logr.Discard()

	result := adjustForOverestimation(log, 23, 20, state, 0, nil, "test-app", "test-ns")
	if result != 23 {
		t.Errorf("expected 23 (no cap), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset), got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_StreakReachesThreeThenDrops(t *testing.T) {
	// streak reaches 3 then ratio drops to 1.1 on 4th call, streak resets, cap released
	state := &scaleState{}
	log := logr.Discard()

	// Build up to streak 3
	adjustForOverestimation(log, 13, 10, state, 0, nil, "test-app", "test-ns")
	adjustForOverestimation(log, 13, 10, state, 0, nil, "test-app", "test-ns")
	adjustForOverestimation(log, 13, 10, state, 0, nil, "test-app", "test-ns")

	if state.overestimateStreak != 3 {
		t.Fatalf("setup: expected streak 3, got %d", state.overestimateStreak)
	}

	// Call 4: ratio 1.1 (predicted=11, reactive=10), below threshold -> reset
	result := adjustForOverestimation(log, 11, 10, state, 0, nil, "test-app", "test-ns")
	if result != 11 {
		t.Errorf("expected 11 (no cap, ratio dropped), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset), got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_ReactiveZero_SkipsDetection(t *testing.T) {
	state := &scaleState{overestimateStreak: 5}
	log := logr.Discard()

	result := adjustForOverestimation(log, 10, 0, state, 0, nil, "test-app", "test-ns")
	if result != 10 {
		t.Errorf("expected 10 (unchanged), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset), got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_PredictedLessOrEqual_StreakResets(t *testing.T) {
	state := &scaleState{overestimateStreak: 3}
	log := logr.Discard()

	// predicted == reactive
	result := adjustForOverestimation(log, 10, 10, state, 0, nil, "test-app", "test-ns")
	if result != 10 {
		t.Errorf("expected 10 (unchanged), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset), got %d", state.overestimateStreak)
	}

	// predicted < reactive
	state.overestimateStreak = 3
	result = adjustForOverestimation(log, 8, 10, state, 0, nil, "test-app", "test-ns")
	if result != 8 {
		t.Errorf("expected 8 (unchanged), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset), got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_StreakGaugeRecords(t *testing.T) {
	state := &scaleState{}
	log := logr.Discard()

	// After one call with ratio > 1.2, gauge should be 1
	adjustForOverestimation(log, 13, 10, state, 0, nil, "test-gauge", "test-ns")
	val := testutil.ToFloat64(overestimateStreakGauge.WithLabelValues("test-gauge", "test-ns"))
	if val != 1.0 {
		t.Errorf("expected streak gauge 1.0, got %f", val)
	}

	// After reset, gauge should be 0
	adjustForOverestimation(log, 10, 10, state, 0, nil, "test-gauge", "test-ns")
	val = testutil.ToFloat64(overestimateStreakGauge.WithLabelValues("test-gauge", "test-ns"))
	if val != 0.0 {
		t.Errorf("expected streak gauge 0.0, got %f", val)
	}
}

func TestAdjustForOverestimation_OverrideCounterIncrements(t *testing.T) {
	state := &scaleState{}
	log := logr.Discard()

	// Get baseline counter value
	baseline := testutil.ToFloat64(overestimateOverridesTotal.WithLabelValues("test-counter", "test-ns"))

	// Build streak to 3 with ratio > 1.2
	adjustForOverestimation(log, 13, 10, state, 0, nil, "test-counter", "test-ns")
	adjustForOverestimation(log, 13, 10, state, 0, nil, "test-counter", "test-ns")
	adjustForOverestimation(log, 13, 10, state, 0, nil, "test-counter", "test-ns") // cap fires here

	val := testutil.ToFloat64(overestimateOverridesTotal.WithLabelValues("test-counter", "test-ns"))
	if val != baseline+1.0 {
		t.Errorf("expected overrides counter %f, got %f", baseline+1.0, val)
	}
}

func TestAdjustForOverestimation_RampUpResetsStreak(t *testing.T) {
	// During ramp-up (currentRPM > lastRPM by >5%), streak should reset
	state := &scaleState{
		overestimateStreak: 5,
		lastRPM:            10000,
	}
	log := logr.Discard()

	// currentRPM=12000 is 20% above lastRPM=10000 -> ramp-up detected
	result := adjustForOverestimation(log, 8, 5, state, 12000, nil, "test-ramp", "test-ns")

	if result != 8 {
		t.Errorf("expected 8 (ramp-up, no cap), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset by ramp-up), got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_FlatTraffic_StreakStillBuilds(t *testing.T) {
	// When traffic is flat (no ramp-up), overestimate detection should work normally
	state := &scaleState{
		lastRPM: 10000,
	}
	log := logr.Discard()

	// Flat predictions (no upcoming ramp in prediction array)
	flatPreds := []float64{60000, 61000, 62000, 63000, 64000, 65000}

	// currentRPM=10200 is 2% above lastRPM=10000 -> not ramp-up
	adjustForOverestimation(log, 13, 10, state, 10200, flatPreds, "test-flat", "test-ns")
	if state.overestimateStreak != 1 {
		t.Errorf("call 1: expected streak 1, got %d", state.overestimateStreak)
	}

	state.lastRPM = 10200
	adjustForOverestimation(log, 13, 10, state, 10300, flatPreds, "test-flat", "test-ns")
	if state.overestimateStreak != 2 {
		t.Errorf("call 2: expected streak 2, got %d", state.overestimateStreak)
	}

	state.lastRPM = 10300
	result := adjustForOverestimation(log, 13, 10, state, 10350, flatPreds, "test-flat", "test-ns")
	if result != 11 {
		t.Errorf("call 3: expected 11 (capped), got %d", result)
	}
	if state.overestimateStreak != 3 {
		t.Errorf("call 3: expected streak 3, got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_RampUpWithZeroLastRPM_NoRampDetection(t *testing.T) {
	// When lastRPM is 0 (first reconcile), don't detect ramp-up
	state := &scaleState{
		lastRPM: 0,
	}
	log := logr.Discard()

	adjustForOverestimation(log, 13, 10, state, 50000, nil, "test-first", "test-ns")
	if state.overestimateStreak != 1 {
		t.Errorf("expected streak 1 (no ramp-up detection on first call), got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_PredictionTrendResetsStreak(t *testing.T) {
	// When predictions show an upcoming ramp (step2 > step1 * 1.2), streak should reset
	// even during flat traffic. This is the key fix for the morning ramp-up issue.
	state := &scaleState{
		overestimateStreak: 5,
		lastRPM:            45000,
	}
	log := logr.Discard()

	// Predictions show 43% jump between step1 and step2 (like the real 8am ramp)
	rampPreds := []float64{57748, 84939, 86437, 87762, 88651, 89217}

	// currentRPM=45100 is flat (0.2% increase), so ramp-up detection won't fire
	// But prediction trend WILL fire because 84939/57748 = 1.47 > 1.2
	result := adjustForOverestimation(log, 5, 3, state, 45100, rampPreds, "test-predtrend", "test-ns")

	if result != 5 {
		t.Errorf("expected 5 (prediction trend bypass), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset by prediction trend), got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_MaxStreakBypassesCap(t *testing.T) {
	// After overestimateMaxStreak (10) consecutive reconciles, trust the model
	state := &scaleState{
		overestimateStreak: 10, // already at max
		lastRPM:            120000,
	}
	log := logr.Discard()

	// Flat predictions (no trend detection), flat traffic (no ramp-up detection)
	flatPreds := []float64{200000, 205000, 208000, 210000, 212000, 213000}

	// streak is at 10, next increment makes it 11 which exceeds maxStreak (10)
	result := adjustForOverestimation(log, 12, 7, state, 120200, flatPreds, "test-maxstreak", "test-ns")

	// Should return predicted (12) because streak > maxStreak
	if result != 12 {
		t.Errorf("expected 12 (max streak bypass), got %d", result)
	}
}

func TestAdjustForOverestimation_FlatPredictions_StreakStillCaps(t *testing.T) {
	// When predictions are truly flat (no ramp), overestimate cap should still work
	// This ensures we don't break the stale-model protection
	state := &scaleState{
		lastRPM: 45000,
	}
	log := logr.Discard()

	// Flat predictions — step2/step1 ratio = 1.02, below 1.2 threshold
	flatPreds := []float64{90000, 91800, 92000, 92500, 93000, 93200}

	adjustForOverestimation(log, 6, 3, state, 45100, flatPreds, "test-flatpred", "test-ns")
	state.lastRPM = 45100
	adjustForOverestimation(log, 6, 3, state, 45150, flatPreds, "test-flatpred", "test-ns")
	state.lastRPM = 45150
	result := adjustForOverestimation(log, 6, 3, state, 45200, flatPreds, "test-flatpred", "test-ns")

	// Should cap because predictions are flat and streak reached 3
	if result != 4 { // ceil(3 * 1.1) = 4
		t.Errorf("expected 4 (capped, flat predictions), got %d", result)
	}
	if state.overestimateStreak != 3 {
		t.Errorf("expected streak 3, got %d", state.overestimateStreak)
	}
}

// --- OPER-01: Periodic re-evaluation tests ---

func TestAdjustForOverestimation_ReEvalAfter30Reconciles(t *testing.T) {
	// Build streak past max (11), which also sets reEvalCounter to 1.
	// Then call 28 more times (counter reaches 29), and on the 29th additional
	// call counter hits 30, triggering re-evaluation: streak and counter reset to 0.
	state := &scaleState{lastRPM: 10000}
	log := logr.Discard()
	flatPreds := []float64{90000, 91000, 92000, 93000, 94000, 95000}

	// Build streak to 11 (past max of 10). The 11th call enters the max-streak
	// block and increments reEvalCounter to 1.
	for i := 0; i < 11; i++ {
		adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval", "test-ns")
	}
	if state.overestimateStreak != 11 {
		t.Fatalf("setup: expected streak 11, got %d", state.overestimateStreak)
	}
	if state.reEvalCounter != 1 {
		t.Fatalf("setup: expected reEvalCounter 1, got %d", state.reEvalCounter)
	}

	// Call 28 more times — streak stays >10, reEvalCounter goes from 2 to 29
	for i := 0; i < 28; i++ {
		adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval", "test-ns")
		if state.overestimateStreak <= 10 {
			t.Fatalf("reconcile %d: streak should stay >10, got %d", i+1, state.overestimateStreak)
		}
	}
	if state.reEvalCounter != 29 {
		t.Fatalf("expected reEvalCounter 29 before final call, got %d", state.reEvalCounter)
	}

	// 29th additional call (30th total past max): counter hits 30 → re-evaluation
	adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval", "test-ns")
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 after re-evaluation, got %d", state.overestimateStreak)
	}
	if state.reEvalCounter != 0 {
		t.Errorf("expected reEvalCounter 0 after re-evaluation, got %d", state.reEvalCounter)
	}
}

func TestAdjustForOverestimation_ReEvalThenRecap(t *testing.T) {
	// After re-eval reset, 3 more overestimate calls rebuild streak to 3 and cap fires
	state := &scaleState{lastRPM: 10000}
	log := logr.Discard()
	flatPreds := []float64{90000, 91000, 92000, 93000, 94000, 95000}

	// Build streak to 11 (reEvalCounter becomes 1)
	for i := 0; i < 11; i++ {
		adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval-recap", "test-ns")
	}

	// Trigger re-evaluation: need 29 more calls (counter goes from 1 to 30)
	for i := 0; i < 29; i++ {
		adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval-recap", "test-ns")
	}
	if state.overestimateStreak != 0 {
		t.Fatalf("expected streak 0 after re-eval, got %d", state.overestimateStreak)
	}

	// Now rebuild streak: 3 more calls should cap
	adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval-recap", "test-ns")
	adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval-recap", "test-ns")
	result := adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval-recap", "test-ns")

	if state.overestimateStreak != 3 {
		t.Errorf("expected streak 3 after recap, got %d", state.overestimateStreak)
	}
	if result != 11 { // ceil(10 * 1.1) = 11
		t.Errorf("expected 11 (capped after recap), got %d", result)
	}
}

func TestAdjustForOverestimation_ReEvalCounterResetsOnStreakReset(t *testing.T) {
	// If traffic normalizes while past max streak, both streak AND reEvalCounter reset
	state := &scaleState{lastRPM: 10000}
	log := logr.Discard()
	flatPreds := []float64{90000, 91000, 92000, 93000, 94000, 95000}

	// Build streak to 11
	for i := 0; i < 11; i++ {
		adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval-reset", "test-ns")
	}

	// Call 5 more times (reEvalCounter at 5)
	for i := 0; i < 5; i++ {
		adjustForOverestimation(log, 13, 10, state, 10000, flatPreds, "test-reeval-reset", "test-ns")
	}

	// Now traffic normalizes: predicted <= reactive → streak resets
	adjustForOverestimation(log, 10, 10, state, 10000, flatPreds, "test-reeval-reset", "test-ns")
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 after normalization, got %d", state.overestimateStreak)
	}
	if state.reEvalCounter != 0 {
		t.Errorf("expected reEvalCounter 0 after normalization, got %d", state.reEvalCounter)
	}
}

// --- OPER-02: 2% ramp-up detection test ---

func TestAdjustForOverestimation_TwoPercentRampDetected(t *testing.T) {
	// 3% RPM increase (10300/10000 - 1 = 0.03) exceeds 2% threshold → ramp detected, streak resets
	state := &scaleState{
		overestimateStreak: 5,
		lastRPM:            10000,
	}
	log := logr.Discard()

	result := adjustForOverestimation(log, 13, 10, state, 10300, nil, "test-2pct-ramp", "test-ns")
	if result != 13 {
		t.Errorf("expected 13 (ramp-up bypass), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset by ramp-up), got %d", state.overestimateStreak)
	}
}

// --- OPER-03: Override bypass scale-down stabilization tests ---

func TestCalculateScaleDownTarget_OverrideBypassesStabilization(t *testing.T) {
	// With overrideActive=true and lastScaleUp 1 minute ago (within 5-min stabilization),
	// scale-down should still proceed (bypass Rules 1 and 2)
	state := &scaleState{
		overrideActive:    true,
		lastScaleUp:       time.Now().Add(-1 * time.Minute),
		belowCurrentSince: time.Now().Add(-10 * time.Minute),
	}
	log := logr.Discard()
	r := &PredictiveAutoscalerReconciler{}

	target := r.calculateScaleDownTarget(log, state, 20, 15)
	if target >= 20 {
		t.Errorf("expected target < 20 (override bypasses stabilization), got %d", target)
	}
}

func TestCalculateScaleDownTarget_NormalScaleDownRespectsStabilization(t *testing.T) {
	// With overrideActive=false and lastScaleUp 1 minute ago, scale-down is blocked
	state := &scaleState{
		overrideActive:    false,
		lastScaleUp:       time.Now().Add(-1 * time.Minute),
		belowCurrentSince: time.Now().Add(-10 * time.Minute),
	}
	log := logr.Discard()
	r := &PredictiveAutoscalerReconciler{}

	target := r.calculateScaleDownTarget(log, state, 20, 15)
	if target != 20 {
		t.Errorf("expected target 20 (blocked by stabilization), got %d", target)
	}
}

func TestCalculateScaleDownTarget_OverrideRespectsCooldown(t *testing.T) {
	// With overrideActive=true but lastScaleDown 1 minute ago (within 2-min cooldown),
	// scale-down is still blocked
	state := &scaleState{
		overrideActive:    true,
		lastScaleDown:     time.Now().Add(-1 * time.Minute),
		belowCurrentSince: time.Now().Add(-10 * time.Minute),
	}
	log := logr.Discard()
	r := &PredictiveAutoscalerReconciler{}

	target := r.calculateScaleDownTarget(log, state, 20, 15)
	if target != 20 {
		t.Errorf("expected target 20 (blocked by cooldown even with override), got %d", target)
	}
}

// --- OPER-04: Wide prediction trend window tests ---

func TestAdjustForOverestimation_LongTermTrendResetsStreak(t *testing.T) {
	// predictions[5] > predictions[0] * 1.2 but predictions[1] < predictions[0] * 1.2
	// Short-term is flat, but long-term shows ramp → streak resets
	state := &scaleState{
		overestimateStreak: 5,
		lastRPM:            45000,
	}
	log := logr.Discard()

	// step1=50000, step2=55000 (ratio 1.1 < 1.2 — no short-term ramp)
	// step6=65000 (ratio 1.3 > 1.2 — long-term ramp detected)
	longRampPreds := []float64{50000, 55000, 57000, 59000, 62000, 65000}

	result := adjustForOverestimation(log, 8, 5, state, 45100, longRampPreds, "test-longtrend", "test-ns")
	if result != 8 {
		t.Errorf("expected 8 (long-term trend bypass), got %d", result)
	}
	if state.overestimateStreak != 0 {
		t.Errorf("expected streak 0 (reset by long-term trend), got %d", state.overestimateStreak)
	}
}

func TestAdjustForOverestimation_LongTermTrendInsufficientPreds(t *testing.T) {
	// With only 3 predictions, long-term check does not fire (no panic)
	// Short-term check still works: step2/step1 = 1.1 < 1.2 → no trend reset
	state := &scaleState{
		lastRPM: 10000,
	}
	log := logr.Discard()

	shortPreds := []float64{50000, 55000, 57000} // only 3 steps, step2/step1 = 1.1

	adjustForOverestimation(log, 13, 10, state, 10000, shortPreds, "test-shortpred", "test-ns")
	if state.overestimateStreak != 1 {
		t.Errorf("expected streak 1 (no trend detection with 3 preds), got %d", state.overestimateStreak)
	}
}
