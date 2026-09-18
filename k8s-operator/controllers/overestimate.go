package controllers

import (
	"fmt"
	"math"

	"github.com/go-logr/logr"
)

// Overestimate detection constants.
// When predicted/actual ratio exceeds the threshold for N consecutive reconciles, cap predicted.
const (
	overestimateRatioThreshold  = 1.2  // predicted/reactive must exceed this to count
	overestimateHeadroom        = 1.1  // cap = reactive * headroom
	overestimateStreakRequired   = 3    // consecutive reconciles before cap fires
	overestimateMaxStreak        = 10   // max streak before trusting the model regardless
	rampUpThreshold             = 0.02 // 2% RPM increase between reconciles indicates ramp-up
	predictionRampRatio         = 1.2  // if lead-time prediction step2 > step1 * this, model sees upcoming ramp
	overestimateReEvalInterval  = 30   // reconciles between re-evaluation resets after max streak
)

// adjustForOverestimation detects when ML-predicted replicas consistently exceed
// reactive (actual-based) replicas by more than 20%. After 3 consecutive such
// reconciles, it caps predicted to reactive * 1.1 to allow scale-down.
//
// Three safeguards prevent false overestimate detection during proactive scaling:
// 1. Ramp-up detection: if currentRPM is increasing, predictions SHOULD be ahead
// 2. Prediction-trend awareness: if ML predictions show rising traffic (step2 > step1 * 1.2),
//    the model sees an upcoming ramp — trust it
// 3. Streak cap: after 10 consecutive reconciles, trust the model regardless
func adjustForOverestimation(
	log logr.Logger,
	predictedReplicas, reactiveReplicas int32,
	state *scaleState,
	currentRPM float64,
	predictions []float64,
	appName, appNS string,
) int32 {
	// Skip detection when reactive is unknown
	if reactiveReplicas == 0 {
		state.overestimateStreak = 0
		state.reEvalCounter = 0
		state.overrideActive = false
		overestimateStreakGauge.WithLabelValues(appName, appNS).Set(0)
		return predictedReplicas
	}

	// Not overestimating if predicted <= reactive
	if predictedReplicas <= reactiveReplicas {
		state.overestimateStreak = 0
		state.reEvalCounter = 0
		state.overrideActive = false
		overestimateStreakGauge.WithLabelValues(appName, appNS).Set(0)
		return predictedReplicas
	}

	ratio := float64(predictedReplicas) / float64(reactiveReplicas)

	// --- Safeguard 1: Ramp-up detection (reactive RPM increasing) ---
	if state.lastRPM > 0 && currentRPM > 0 {
		rpmIncrease := (currentRPM - state.lastRPM) / state.lastRPM
		if rpmIncrease > rampUpThreshold {
			state.overestimateStreak = 0
			state.reEvalCounter = 0
			state.overrideActive = false
			overestimateStreakGauge.WithLabelValues(appName, appNS).Set(0)
			log.Info("Overestimate check (ramp-up detected, streak reset)",
				"predicted", predictedReplicas,
				"reactive", reactiveReplicas,
				"ratio", fmt.Sprintf("%.2f", ratio),
				"currentRPM", fmt.Sprintf("%.0f", currentRPM),
				"lastRPM", fmt.Sprintf("%.0f", state.lastRPM),
				"rpmIncreasePct", fmt.Sprintf("%.1f", rpmIncrease*100))
			return predictedReplicas
		}
	}

	// --- Safeguard 2: Prediction-trend awareness ---
	// Short-term: step 2 > step 1 * 1.2 (existing check, per D-08)
	// Long-term: step 6 > step 1 * 1.2 (new check, per D-07)
	// Either condition means the model sees an upcoming ramp — not overestimation.
	if len(predictions) >= 2 && predictions[0] > 0 {
		shortTermRamp := predictions[1] > predictions[0]*predictionRampRatio
		longTermRamp := len(predictions) >= 6 && predictions[5] > predictions[0]*predictionRampRatio
		if shortTermRamp || longTermRamp {
			state.overestimateStreak = 0
			state.reEvalCounter = 0
			state.overrideActive = false
			overestimateStreakGauge.WithLabelValues(appName, appNS).Set(0)
			triggerType := "short-term"
			if longTermRamp && !shortTermRamp {
				triggerType = "long-term"
			} else if longTermRamp && shortTermRamp {
				triggerType = "both"
			}
			log.Info("Overestimate check (prediction trend shows upcoming ramp, streak reset)",
				"predicted", predictedReplicas,
				"reactive", reactiveReplicas,
				"ratio", fmt.Sprintf("%.2f", ratio),
				"triggerType", triggerType,
				"predStep1RPM", fmt.Sprintf("%.0f", predictions[0]),
				"predStep2RPM", fmt.Sprintf("%.0f", predictions[1]),
				"predStep6RPM", fmt.Sprintf("%.0f", predictions[len(predictions)-1]))
			return predictedReplicas
		}
	}

	if ratio > overestimateRatioThreshold {
		state.overestimateStreak++
	} else {
		state.overestimateStreak = 0
	}

	overestimateStreakGauge.WithLabelValues(appName, appNS).Set(float64(state.overestimateStreak))

	// --- Safeguard 3: Periodic re-evaluation (replaces permanent max-streak bypass) ---
	// After max streak, count reconciles. Every 30 reconciles, reset streak to 0 so
	// detection can re-evaluate from scratch. Per D-01, D-02, D-03.
	if state.overestimateStreak > overestimateMaxStreak {
		state.reEvalCounter++
		if state.reEvalCounter >= overestimateReEvalInterval {
			state.reEvalCounter = 0
			state.overestimateStreak = 0
			state.overrideActive = false
			overestimateStreakGauge.WithLabelValues(appName, appNS).Set(0)
			log.Info("Overestimate re-evaluation: streak reset after 30 reconciles",
				"predicted", predictedReplicas,
				"reactive", reactiveReplicas)
			return predictedReplicas
		}
		log.Info("Overestimate check (past max streak, awaiting re-evaluation)",
			"predicted", predictedReplicas,
			"reactive", reactiveReplicas,
			"ratio", fmt.Sprintf("%.2f", ratio),
			"streak", state.overestimateStreak,
			"reEvalCounter", state.reEvalCounter,
			"reEvalInterval", overestimateReEvalInterval)
		return predictedReplicas
	}

	log.Info("Overestimate check",
		"predicted", predictedReplicas,
		"reactive", reactiveReplicas,
		"ratio", fmt.Sprintf("%.2f", ratio),
		"streak", state.overestimateStreak,
		"threshold", overestimateRatioThreshold,
		"requiredStreak", overestimateStreakRequired)

	if state.overestimateStreak >= overestimateStreakRequired {
		capped := int32(math.Ceil(float64(reactiveReplicas) * overestimateHeadroom))
		if capped < predictedReplicas {
			log.Info("Overestimate override: capping predicted replicas",
				"originalPredicted", predictedReplicas,
				"reactive", reactiveReplicas,
				"capped", capped,
				"ratio", fmt.Sprintf("%.2f", ratio),
				"streak", state.overestimateStreak)
			overestimateOverridesTotal.WithLabelValues(appName, appNS).Inc()
			state.overrideActive = true
			return capped
		}
	}

	return predictedReplicas
}
