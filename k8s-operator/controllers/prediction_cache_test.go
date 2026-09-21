package controllers

import (
	"context"
	"testing"
	"time"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// With the ML API unreachable, a cached forecast is reused only while younger than
// predictionStaleMax; older entries are dropped and the caller gets an error
// (the reactive-only fallback).
func TestGetCachedPrediction_StaleBound(t *testing.T) {
	t.Setenv("ML_API_URL", "http://127.0.0.1:1") // nothing listens here: every fetch fails fast
	r := &PredictiveAutoscalerReconciler{predictionCache: map[string]*cachedPrediction{}}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{}
	a.Spec.TargetDeployment.Name, a.Spec.TargetDeployment.Namespace = "app", "ns"
	resp := &MLPredictionResponse{Predictions: []float64{100, 100}}

	r.predictionCache["k"] = &cachedPrediction{response: resp, fetchedAt: time.Now().Add(-predictionCacheTTL - time.Minute)}
	got, err := r.getCachedPrediction(context.Background(), a, "k")
	if err != nil || got != resp {
		t.Fatalf("young stale cache should be reused: got=%v err=%v", got, err)
	}

	r.predictionCache["k"] = &cachedPrediction{response: resp, fetchedAt: time.Now().Add(-predictionStaleMax - time.Second)}
	if _, err := r.getCachedPrediction(context.Background(), a, "k"); err == nil {
		t.Fatalf("cache older than predictionStaleMax must not be reused")
	}
	if _, ok := r.predictionCache["k"]; ok {
		t.Fatalf("expired cache entry should be dropped")
	}
}
