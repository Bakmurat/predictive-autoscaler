package controllers

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	predictedReplicasGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_predicted_replicas",
			Help: "Number of replicas predicted by ML model",
		},
		[]string{"application", "namespace"},
	)

	actualNeededReplicasGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_actual_needed_replicas",
			Help: "Number of replicas actually needed based on current metrics",
		},
		[]string{"application", "namespace"},
	)

	predictionErrorPercentGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_prediction_error_percent",
			Help: "Prediction error as percentage: abs(predicted - actual) / actual * 100",
		},
		[]string{"application", "namespace"},
	)

	overestimateOverridesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "predictive_autoscaler_overestimate_overrides_total",
			Help: "Total number of overestimate detection overrides",
		},
		[]string{"application", "namespace"},
	)

	overestimateStreakGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_overestimate_streak",
			Help: "Current consecutive overestimate count",
		},
		[]string{"application", "namespace"},
	)

	predictedRpmGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_predicted_rpm",
			Help: "Peak predicted RPM from ML model lead-time window",
		},
		[]string{"application", "namespace"},
	)

	currentRpmGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_current_rpm",
			Help: "Current RPM from VictoriaMetrics query",
		},
		[]string{"application", "namespace"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		predictedReplicasGauge,
		actualNeededReplicasGauge,
		predictionErrorPercentGauge,
		overestimateOverridesTotal,
		overestimateStreakGauge,
		predictedRpmGauge,
		currentRpmGauge,
	)
}
