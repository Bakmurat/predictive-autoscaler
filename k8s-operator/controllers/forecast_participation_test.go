package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-logr/logr"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Regression sequences (Codex Task 03 C-15): an overestimate override set by an earlier forecast
// must be cleared whenever no usable forecast participates, so the scale-down holds apply again.
func TestClearForecastOverride_HoldsApplyAgain(t *testing.T) {
	for _, reason := range []string{"sanity_rejected", "prediction_unavailable", "forecasting_disabled"} {
		t.Run(reason, func(t *testing.T) {
			r := &PredictiveAutoscalerReconciler{}
			scaleUp := time.Now().Add(-1 * time.Minute)
			state := &scaleState{
				overrideActive:     true,
				overestimateStreak: 3,
				reEvalCounter:      2,
				lastScaleUp:        scaleUp,
				belowCurrentSince:  time.Now().Add(-10 * time.Minute),
			}
			// Before clearing, the override bypasses the 5-minute hold after a scale-up.
			if got := r.calculateScaleDownTarget(logr.Discard(), state, 4, 1); got >= 4 {
				t.Fatalf("precondition: override should bypass the hold, got %d", got)
			}
			r.clearForecastOverride(logr.Discard(), state, "app", "ns", reason)
			if state.overrideActive || state.overestimateStreak != 0 || state.reEvalCounter != 0 {
				t.Fatalf("override state not cleared: %+v", state)
			}
			if !state.lastScaleUp.Equal(scaleUp) {
				t.Fatalf("lastScaleUp must be preserved")
			}
			if got := r.calculateScaleDownTarget(logr.Discard(), state, 4, 1); got != 4 {
				t.Fatalf("after clearing, the post-scale-up hold must block the scale-down, got %d", got)
			}
		})
	}
}

// Clearing is a no-op when nothing is set (no log noise, no gauge writes needed).
func TestClearForecastOverride_NoopWhenClean(t *testing.T) {
	r := &PredictiveAutoscalerReconciler{}
	state := &scaleState{lastScaleDown: time.Now()}
	r.clearForecastOverride(logr.Discard(), state, "app", "ns", "prediction_unavailable")
	if state.lastScaleDown.IsZero() {
		t.Fatalf("cooldown timestamp must be preserved")
	}
}

// Lead-time window selection measured from now (Codex Task 03 C-17): elapsed steps are excluded
// and a forecast whose remaining horizon does not reach now+lead is not usable.
func TestSelectLeadTimeWindow(t *testing.T) {
	preds := []float64{10, 20, 30, 40, 50, 60} // 60-min horizon, 10-min steps: targets +10..+60
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)

	// Fresh forecast: steps +10 and +20 lie inside a 20-minute lead time.
	w, ok := selectLeadTimeWindow(preds, now, now, 60, 20)
	if !ok || len(w) != 2 || w[0] != 10 || w[1] != 20 {
		t.Fatalf("fresh: want [10 20] ok, got %v %v", w, ok)
	}
	// Anchor 25 minutes old: targets at now-15 and now-5 have elapsed; window is now+5, now+15.
	w, ok = selectLeadTimeWindow(preds, now.Add(-25*time.Minute), now, 60, 20)
	if !ok || len(w) != 2 || w[0] != 30 || w[1] != 40 {
		t.Fatalf("25 min old: want [30 40] ok, got %v %v", w, ok)
	}
	// Anchor 45 minutes old: only now+5 and now+15 remain; furthest target < now+20 → not usable.
	if _, ok = selectLeadTimeWindow(preds, now.Add(-45*time.Minute), now, 60, 20); ok {
		t.Fatalf("45 min old: coverage insufficient, must not be usable")
	}
	// Everything elapsed → not usable.
	if _, ok = selectLeadTimeWindow(preds, now.Add(-61*time.Minute), now, 60, 20); ok {
		t.Fatalf("fully elapsed forecast must not be usable")
	}
	// Lead time shorter than one step: the first future step is used.
	w, ok = selectLeadTimeWindow(preds, now, now, 60, 5)
	if !ok || len(w) != 1 || w[0] != 10 {
		t.Fatalf("short lead: want [10] ok, got %v %v", w, ok)
	}
	// Grid boundary: anchor exactly 10 minutes old → the first target equals now and is elapsed.
	w, ok = selectLeadTimeWindow(preds, now.Add(-10*time.Minute), now, 60, 20)
	if !ok || len(w) != 2 || w[0] != 20 || w[1] != 30 {
		t.Fatalf("grid boundary: want [20 30] ok, got %v %v", w, ok)
	}
}

// calculatePredictedReplicas reports a cached forecast as unusable once its horizon has elapsed.
func TestCalculatePredictedReplicas_ElapsedHorizonIsUnusable(t *testing.T) {
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard()}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{}
	a.Spec.MinReplicas, a.Spec.MaxReplicas = 1, 10
	a.Spec.Prediction.HorizonMinutes, a.Spec.Prediction.LeadTimeMinutes = 60, 20
	p := &MLPredictionResponse{Predictions: []float64{600, 600, 600, 600, 600, 600}, Confidence: 0.9}
	p.anchorAt = time.Now().Add(-50 * time.Minute)
	if _, ok := r.calculatePredictedReplicas(a, p); ok {
		t.Fatalf("forecast anchored 50 minutes ago must be unusable for a 20-minute lead time")
	}
	p.anchorAt = time.Now()
	if n, ok := r.calculatePredictedReplicas(a, p); !ok || n < 1 {
		t.Fatalf("fresh forecast must be usable, got %d %v", n, ok)
	}
}

// HTTP 422 (the API's freshness refusal) must not fall back to a cached forecast; transport-class
// failures still reuse a young cache.
func TestGetCachedPrediction_RefusalDropsCache(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"detail":"latest observation too old"}`))
	}))
	defer srv.Close()
	t.Setenv("ML_API_URL", srv.URL)
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard(), predictionCache: map[string]*cachedPrediction{}}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{}
	a.Spec.TargetDeployment.Name, a.Spec.TargetDeployment.Namespace = "app", "ns"
	resp := &MLPredictionResponse{Predictions: []float64{100, 100}}
	r.predictionCache["k"] = &cachedPrediction{response: resp, fetchedAt: time.Now().Add(-predictionCacheTTL - time.Minute)}
	_, err := r.getCachedPrediction(context.Background(), a, "k")
	if err == nil || !isForecastRefusal(err) {
		t.Fatalf("expected a refusal error, got %v", err)
	}
	if _, ok := r.predictionCache["k"]; ok {
		t.Fatalf("cache must be dropped on refusal")
	}

	// A 5xx is a transport-class failure: the young cache is reused.
	srv5 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv5.Close()
	t.Setenv("ML_API_URL", srv5.URL)
	r.predictionCache["k"] = &cachedPrediction{response: resp, fetchedAt: time.Now().Add(-predictionCacheTTL - time.Minute)}
	got, err := r.getCachedPrediction(context.Background(), a, "k")
	if err != nil || got != resp {
		t.Fatalf("5xx should reuse the young cache: got=%v err=%v", got, err)
	}
}

// Cache age just below, at, and above predictionStaleMax with the API unreachable.
func TestGetCachedPrediction_StaleBoundaries(t *testing.T) {
	t.Setenv("ML_API_URL", "http://127.0.0.1:1")
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard(), predictionCache: map[string]*cachedPrediction{}}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{}
	a.Spec.TargetDeployment.Name, a.Spec.TargetDeployment.Namespace = "app", "ns"
	resp := &MLPredictionResponse{Predictions: []float64{100, 100}}
	cases := []struct {
		name  string
		age   time.Duration
		reuse bool
	}{
		{"just below", predictionStaleMax - 30*time.Second, true},
		{"at bound", predictionStaleMax, false}, // time elapses between set and check → age > bound
		{"above", predictionStaleMax + time.Second, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r.predictionCache["k"] = &cachedPrediction{response: resp, fetchedAt: time.Now().Add(-c.age)}
			got, err := r.getCachedPrediction(context.Background(), a, "k")
			if c.reuse && (err != nil || got != resp) {
				t.Fatalf("expected reuse, got %v %v", got, err)
			}
			if !c.reuse && err == nil {
				t.Fatalf("expected no reuse at age %s", c.age)
			}
		})
	}
}

// A fresh cache entry (inside the TTL) is served without calling the API at all, even when the
// API is down: the grid-boundary case is then handled by selectLeadTimeWindow, not by refetching.
func TestGetCachedPrediction_FreshCacheServedWithoutFetch(t *testing.T) {
	t.Setenv("ML_API_URL", "http://127.0.0.1:1")
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard(), predictionCache: map[string]*cachedPrediction{}}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{}
	resp := &MLPredictionResponse{Predictions: []float64{100, 100}}
	r.predictionCache["k"] = &cachedPrediction{response: resp, fetchedAt: time.Now().Add(-predictionCacheTTL + time.Minute)}
	if got, err := r.getCachedPrediction(context.Background(), a, "k"); err != nil || got != resp {
		t.Fatalf("fresh cache must be served: %v %v", got, err)
	}
}
