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

	// sanityRejectionsTotal counts forecasts discarded by the divergence check (first step > 10x live rate).
	sanityRejectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "predictive_autoscaler_sanity_rejections_total",
			Help: "Forecasts discarded by the divergence sanity check",
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
			Help: "Raw lead-window peak RPM selected for the last scaling decision, before replica safeguards; absent when no nonempty usable forecast participated. Early reconcile errors do not refresh this metric.",
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

	// Forecast record (set at issuance, before the outcome exists). One series per horizon step.
	forecastRpmGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_forecast_rpm",
			Help: "Forecast request rate (requests per minute) for horizon step N, set when the forecast is issued",
		},
		[]string{"application", "namespace", "step"},
	)
	forecastTargetTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_forecast_target_timestamp_seconds",
			Help: "Unix time the forecast for horizon step N refers to",
		},
		[]string{"application", "namespace", "step"},
	)
	forecastIssuedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_forecast_issued_timestamp_seconds",
			Help: "Unix time the current forecast was issued (fetched from the forecasting service)",
		},
		[]string{"application", "namespace"},
	)
	forecastStepMinutesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_forecast_step_minutes",
			Help: "Minutes between consecutive horizon steps of the current forecast",
		},
		[]string{"application", "namespace"},
	)
	modelTrainedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_model_trained_timestamp_seconds",
			Help: "Unix time the model behind the current forecast finished training; -1 if unknown",
		},
		[]string{"application", "namespace"},
	)
	modelTrainingCutoffTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_model_training_cutoff_timestamp_seconds",
			Help: "Unix time of the last observation the model behind the current forecast was trained on; -1 if unknown",
		},
		[]string{"application", "namespace"},
	)
	modelInfoGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_model_info",
			Help: "Model name and version behind the current forecast (value is always 1)",
		},
		[]string{"application", "namespace", "model_name", "model_version"},
	)
	scaleEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "predictive_autoscaler_scale_events_total",
			Help: "Replica changes applied by the operator, by direction (up or down)",
		},
		[]string{"application", "namespace", "direction"},
	)
	desiredReplicasGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_desired_replicas",
			Help: "Replica count the operator decided on in the last reconcile (before scale-down stabilization)",
		},
		[]string{"application", "namespace"},
	)
)

func init() {
	metrics.Registry.MustRegister(forecastRpmGauge, forecastTargetTsGauge, forecastIssuedTsGauge, forecastStepMinutesGauge, modelTrainedTsGauge, modelTrainingCutoffTsGauge, modelInfoGauge, scaleEventsTotal, desiredReplicasGauge)
	metrics.Registry.MustRegister(
		predictedReplicasGauge,
		actualNeededReplicasGauge,
		predictionErrorPercentGauge,
		overestimateOverridesTotal,
		sanityRejectionsTotal,
		overestimateStreakGauge,
		predictedRpmGauge,
		currentRpmGauge,
	)
}
