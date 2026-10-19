package controllers

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// One series per (autoscaler, condition type, status), kube-state-metrics style: 1 for the condition's current
	// status, 0 for the other two. Bounded by conditionMetricTypes; the alert rules in the chart read it.
	conditionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_condition",
			Help: "Status condition of a PredictiveAutoscaler: 1 for the current status (True, False or Unknown), 0 for the others",
		},
		[]string{"namespace", "autoscaler", "type", "status"},
	)
	// 1 when spec.prediction.enabled is on (the default), 0 when forecasting is switched off on purpose: the chart's
	// forecast alert skips those autoscalers.
	forecastingEnabledGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "predictive_autoscaler_forecasting_enabled",
			Help: "1 when the autoscaler's forecasting component is enabled (spec.prediction.enabled), 0 when disabled",
		},
		[]string{"namespace", "autoscaler"},
	)
)

// conditionMetricTypes are the condition types exported by conditionGauge (every type updateStatus sets).
var conditionMetricTypes = []string{
	"Ready", "TelemetryAvailable", "ForecastAvailable", "ConflictDetected", "VPAInterference", "ScalingActive",
	"ScaleDownHeadroom",
}

// recordConditionMetrics publishes an autoscaler's persisted conditions (a type it does not carry has no series) and
// whether its forecasting is enabled.
func recordConditionMetrics(ns, name string, forecasting bool, conds []metav1.Condition) {
	forecastingEnabledGauge.WithLabelValues(ns, name).Set(map[bool]float64{true: 1, false: 0}[forecasting])
	for _, t := range conditionMetricTypes {
		c := meta.FindStatusCondition(conds, t)
		for _, s := range []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionUnknown} {
			if c == nil {
				conditionGauge.DeleteLabelValues(ns, name, t, string(s))
				continue
			}
			v := 0.0
			if c.Status == s {
				v = 1
			}
			conditionGauge.WithLabelValues(ns, name, t, string(s)).Set(v)
		}
	}
}

// forgetConditionMetrics removes a deleted autoscaler's condition and forecasting-enabled series.
func forgetConditionMetrics(ns, name string) {
	conditionGauge.DeletePartialMatch(prometheus.Labels{"namespace": ns, "autoscaler": name})
	forecastingEnabledGauge.DeleteLabelValues(ns, name)
}

func init() {
	metrics.Registry.MustRegister(forecastRpmGauge, forecastTargetTsGauge, forecastIssuedTsGauge, forecastStepMinutesGauge, modelTrainedTsGauge, modelTrainingCutoffTsGauge, modelInfoGauge, scaleEventsTotal, desiredReplicasGauge, conditionGauge, forecastingEnabledGauge)
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
