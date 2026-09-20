package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Scaling configuration constants
const (
	predictionCacheTTL      = 5 * time.Minute   // How often to refresh ML API predictions
	scaleDownStabilization  = 5 * time.Minute   // Wait after scale-up before any scale-down
	scaleDownCooldown       = 2 * time.Minute   // Minimum time between scale-down operations
	scaleDownMaxPercent     = 10                // Max % of pods to remove per scale-down
	scaleDownMinPods        = 2                 // Min pods to remove per scale-down (whichever is greater)
	defaultLeadTimeMinutes  = 20                // Default prediction lead time
	defaultHorizonMinutes   = 60                // Default prediction horizon
	defaultReconcileSeconds = 60                // Default reconcile interval (fast for reactive)
	minReconcileSeconds     = 30                // Minimum reconcile interval
	vmQueryTimeout          = 10 * time.Second  // VictoriaMetrics query timeout (short — it's lightweight)
	mlAPITimeout            = 120 * time.Second // ML API timeout (long — allows for model training)
)

// cachedPrediction stores ML API predictions with a TTL to avoid
// hammering the ML API on every 60s reconcile cycle.
type cachedPrediction struct {
	response  *MLPredictionResponse
	fetchedAt time.Time
}

// scaleState tracks scaling history per deployment for stabilization.
type scaleState struct {
	lastScaleUp        time.Time // When we last scaled up (blocks scale-down for stabilization window)
	lastScaleDown      time.Time // When we last scaled down (enforces cooldown between scale-downs)
	belowCurrentSince  time.Time // When desired first dropped below current (zero = not below)
	overestimateStreak int       // Consecutive reconciles where predicted/reactive > threshold
	lastRPM            float64   // Previous reconcile's current RPM (for ramp-up detection)
	reEvalCounter      int       // Counts reconciles since last periodic re-evaluation (OPER-01)
	overrideActive     bool      // True when overestimate cap fired; bypasses stabilization (OPER-03)
}

// PredictiveAutoscalerReconciler reconciles a PredictiveAutoscaler object.
// It is the SINGLE SOURCE OF TRUTH for deployment replicas, combining:
//   - Predictive baseline: ML predictions within lead-time window
//   - Reactive floor: current RPM from VictoriaMetrics
//   - Formula: desired = max(predicted, reactive, minReplicas)
type PredictiveAutoscalerReconciler struct {
	client.Client
	Log              logr.Logger
	Scheme           *runtime.Scheme
	lastReconcileMap map[string]time.Time
	predictionCache  map[string]*cachedPrediction
	scaleStates      map[string]*scaleState
}

// MLPredictionRequest represents the request to ML API
type MLPredictionRequest struct {
	Application    string `json:"application"`
	Namespace      string `json:"namespace"`
	MetricType     string `json:"metric_type"`
	HorizonMinutes int32  `json:"horizon_minutes"`
}

// MLPredictionResponse represents the response from ML API
type MLPredictionResponse struct {
	Predictions    []float64 `json:"predictions"`
	Confidence     float64   `json:"confidence"`
	ModelName      string    `json:"model_name"`
	ModelVersion   string    `json:"model_version"`
	ModelTrainedAt string    `json:"model_trained_at"`
	HorizonMinutes int32     `json:"horizon_minutes"`
	Timestamp      string    `json:"timestamp"`
	MAPE           float64   `json:"mape"`
}

// predictionEnabled reports whether the forecasting component is on for this autoscaler
// (spec.prediction.enabled; nil means true).
func predictionEnabled(a *autoscalerv1alpha1.PredictiveAutoscaler) bool {
	return a.Spec.Prediction.Enabled == nil || *a.Spec.Prediction.Enabled
}

// VMInstantQueryResponse represents the VictoriaMetrics instant query response
type VMInstantQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value [2]interface{} `json:"value"` // [timestamp, "value_string"]
		} `json:"result"`
	} `json:"data"`
}

//+kubebuilder:rbac:groups=autoscaler.example.com,resources=predictiveautoscalers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=autoscaler.example.com,resources=predictiveautoscalers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=autoscaler.example.com,resources=predictiveautoscalers/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=apps,resources=deployments/scale,verbs=get;update;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is the unified scaling loop. On every cycle (default 60s):
// 1. Get ML predictions (cached 5 min) → predicted replicas within lead-time window
// 2. Query VictoriaMetrics for current RPM → reactive replicas
// 3. desired = max(predicted, reactive, minReplicas)
// 4. Scale up immediately, scale down with stabilization
func (r *PredictiveAutoscalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("predictiveautoscaler", req.NamespacedName)

	// Initialize maps if nil
	if r.lastReconcileMap == nil {
		r.lastReconcileMap = make(map[string]time.Time)
	}
	if r.predictionCache == nil {
		r.predictionCache = make(map[string]*cachedPrediction)
	}
	if r.scaleStates == nil {
		r.scaleStates = make(map[string]*scaleState)
	}

	// Fetch the PredictiveAutoscaler instance
	var autoscaler autoscalerv1alpha1.PredictiveAutoscaler
	if err := r.Get(ctx, req.NamespacedName, &autoscaler); err != nil {
		if errors.IsNotFound(err) {
			log.Info("PredictiveAutoscaler resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get PredictiveAutoscaler")
		return ctrl.Result{}, err
	}

	// Throttle reconciliation to prevent tight loops from status updates
	key := req.NamespacedName.String()
	reconcileInterval := time.Duration(autoscaler.Spec.Prediction.UpdateIntervalSeconds) * time.Second
	if reconcileInterval == 0 {
		reconcileInterval = time.Duration(defaultReconcileSeconds) * time.Second
	}
	if reconcileInterval < time.Duration(minReconcileSeconds)*time.Second {
		reconcileInterval = time.Duration(minReconcileSeconds) * time.Second
	}
	if lastTime, ok := r.lastReconcileMap[key]; ok {
		if elapsed := time.Since(lastTime); elapsed < reconcileInterval {
			return ctrl.Result{RequeueAfter: reconcileInterval - elapsed}, nil
		}
	}
	r.lastReconcileMap[key] = time.Now()

	log.Info("Reconciling PredictiveAutoscaler",
		"target", autoscaler.Spec.TargetDeployment.Name,
		"namespace", autoscaler.Spec.TargetDeployment.Namespace)

	// Get target deployment
	deployment, err := r.getTargetDeployment(ctx, &autoscaler)
	if err != nil {
		log.Error(err, "Failed to get target deployment")
		delete(r.lastReconcileMap, key) // Allow fast retry on error
		return r.updateStatusWithError(ctx, &autoscaler, "DeploymentNotFound", err.Error())
	}
	currentReplicas := *deployment.Spec.Replicas

	// --- PREDICTIVE COMPONENT ---
	// Get ML prediction (cached, refresh every 5 min)
	var prediction *MLPredictionResponse
	var predErr error
	forecasting := predictionEnabled(&autoscaler)
	if forecasting {
		prediction, predErr = r.getCachedPrediction(ctx, &autoscaler, key)
	}
	predictedReplicas := int32(0)
	if !forecasting {
		log.V(1).Info("Forecasting disabled for this autoscaler; reactive rule only")
	} else if predErr != nil {
		log.Info("Prediction unavailable, using reactive only", "error", predErr.Error())
	} else if prediction != nil {
		predictedReplicas = r.calculatePredictedReplicas(&autoscaler, prediction)
	}

	// --- REACTIVE COMPONENT ---
	// Query VictoriaMetrics for current RPM
	currentRPM, vmErr := r.queryCurrentRPM(autoscaler.Spec.TargetDeployment.Name, autoscaler.Spec.TargetDeployment.Namespace)
	reactiveReplicas := int32(0)
	if vmErr != nil {
		log.Info("VM query failed, using prediction only", "error", vmErr.Error())
	} else {
		targetRPM := int32(20000)
		if autoscaler.Spec.Metrics.Requests != nil && autoscaler.Spec.Metrics.Requests.TargetRPS > 0 {
			targetRPM = autoscaler.Spec.Metrics.Requests.TargetRPS * 60
		}
		if currentRPM > 0 {
			reactiveReplicas = int32(math.Ceil(currentRPM / float64(targetRPM)))
		}
	}

	// --- PREDICTION SANITY CHECK ---
	// Guard against diverging LSTM predictions (exponential blowup).
	// If any lead-time prediction exceeds 10x current RPM, discard predictions
	// entirely and fall back to reactive scaling.
	if prediction != nil && predErr == nil && currentRPM > 0 && len(prediction.Predictions) > 0 {
		maxSaneRPM := currentRPM * 10
		if prediction.Predictions[0] > maxSaneRPM {
			log.Info("Prediction sanity check failed: values diverged, falling back to reactive",
				"nearPredictionRPM", fmt.Sprintf("%.0f", prediction.Predictions[0]),
				"currentRPM", fmt.Sprintf("%.0f", currentRPM),
				"maxSaneRPM", fmt.Sprintf("%.0f", maxSaneRPM))
			predictedReplicas = reactiveReplicas
			prediction = nil // Clear so overestimate check doesn't use garbage
		}
	}

	// --- OVERESTIMATE DETECTION ---
	// Ratio-based: if predicted/reactive > 1.2 for 3+ consecutive reconciles,
	// cap predicted to reactive * 1.1 to allow scale-down.
	state := r.getOrCreateScaleState(key)
	appName := autoscaler.Spec.TargetDeployment.Name
	appNS := autoscaler.Spec.TargetDeployment.Namespace
	if appNS == "" {
		appNS = req.NamespacedName.Namespace
	}
	if prediction != nil && predErr == nil {
		predictedReplicas = adjustForOverestimation(
			log, predictedReplicas, reactiveReplicas, state, currentRPM, prediction.Predictions, appName, appNS)
	}
	// Track RPM for next reconcile's ramp-up detection
	state.lastRPM = currentRPM

	// --- UNIFIED DECISION ---
	// desired = max(predicted_baseline, reactive_needed, minReplicas)
	desiredReplicas := predictedReplicas
	if reactiveReplicas > desiredReplicas {
		desiredReplicas = reactiveReplicas
	}
	if desiredReplicas < autoscaler.Spec.MinReplicas {
		desiredReplicas = autoscaler.Spec.MinReplicas
	}
	if desiredReplicas > autoscaler.Spec.MaxReplicas {
		desiredReplicas = autoscaler.Spec.MaxReplicas
	}

	// No usable input (forecast off or failed, and the metrics query failed) — keep current replicas
	if (predErr != nil || !forecasting) && vmErr != nil {
		log.Info("Both prediction and VM query failed, keeping current replicas",
			"current", currentReplicas)
		desiredReplicas = currentReplicas
	}

	// --- ACCURACY METRICS ---
	actualNeeded := reactiveReplicas
	if actualNeeded < autoscaler.Spec.MinReplicas {
		actualNeeded = autoscaler.Spec.MinReplicas
	}

	errorPct := float64(0)
	if actualNeeded > 0 {
		errorPct = math.Abs(float64(predictedReplicas-actualNeeded)) / float64(actualNeeded) * 100
	}

	predictedReplicasGauge.WithLabelValues(appName, appNS).Set(float64(predictedReplicas))
	desiredReplicasGauge.WithLabelValues(appName, appNS).Set(float64(desiredReplicas))
	actualNeededReplicasGauge.WithLabelValues(appName, appNS).Set(float64(actualNeeded))
	predictionErrorPercentGauge.WithLabelValues(appName, appNS).Set(errorPct)

	// RPM gauges (D-08, D-09) — update every reconcile cycle
	if prediction != nil && len(prediction.Predictions) > 0 {
		// Report peak RPM from lead-time window (first 2 steps) — matches replica calculation input
		peakRPM := prediction.Predictions[0]
		for i := 1; i < len(prediction.Predictions) && i < 2; i++ {
			if prediction.Predictions[i] > peakRPM {
				peakRPM = prediction.Predictions[i]
			}
		}
		predictedRpmGauge.WithLabelValues(appName, appNS).Set(peakRPM)
	}
	if currentRPM > 0 {
		currentRpmGauge.WithLabelValues(appName, appNS).Set(currentRPM)
	}

	log.Info("Unified scaling decision",
		"predictedReplicas", predictedReplicas,
		"reactiveReplicas", reactiveReplicas,
		"actualNeededReplicas", actualNeeded,
		"predictionErrorPercent", fmt.Sprintf("%.1f", errorPct),
		"desiredReplicas", desiredReplicas,
		"currentReplicas", currentReplicas,
		"currentRPM", fmt.Sprintf("%.0f", currentRPM))

	// --- APPLY SCALING ---

	if desiredReplicas > currentReplicas {
		// SCALE UP: immediate — no delay for predicted or reactive
		log.Info("Scaling UP",
			"from", currentReplicas, "to", desiredReplicas,
			"predictedComponent", predictedReplicas,
			"reactiveComponent", reactiveReplicas)
		if err := r.scaleDeployment(ctx, deployment, desiredReplicas); err != nil {
			log.Error(err, "Failed to scale up")
			return r.updateStatusWithError(ctx, &autoscaler, "ScalingError", err.Error())
		}
		state.lastScaleUp = time.Now()
		state.belowCurrentSince = time.Time{} // reset scale-down timer
		log.Info("Scaled deployment", "from", currentReplicas, "to", desiredReplicas)

	} else if desiredReplicas < currentReplicas {
		// SCALE DOWN: with stabilization window and gradual reduction
		target := r.calculateScaleDownTarget(log, state, currentReplicas, desiredReplicas)
		if target < currentReplicas {
			log.Info("Scaling DOWN",
				"from", currentReplicas, "to", target, "eventualTarget", desiredReplicas)
			if err := r.scaleDeployment(ctx, deployment, target); err != nil {
				log.Error(err, "Failed to scale down")
				return r.updateStatusWithError(ctx, &autoscaler, "ScalingError", err.Error())
			}
			state.lastScaleDown = time.Now()
			state.overrideActive = false // clear override after scale-down completes (per D-12)
			log.Info("Scaled deployment", "from", currentReplicas, "to", target)
		} else {
			// Still in stabilization or cooldown
			reason := "stabilizing"
			if !state.belowCurrentSince.IsZero() {
				elapsed := time.Since(state.belowCurrentSince)
				if elapsed >= scaleDownStabilization {
					reason = "cooldown"
				}
			}
			log.Info("Scale-down pending ("+reason+")",
				"desired", desiredReplicas, "current", currentReplicas)
		}

	} else {
		// AT TARGET: reset scale-down timer
		state.belowCurrentSince = time.Time{}
		log.Info("At target replicas", "replicas", currentReplicas)
	}

	// Update status
	if err := r.updateStatus(ctx, &autoscaler, desiredReplicas, currentReplicas); err != nil {
		log.Error(err, "Failed to update status")
	}

	return ctrl.Result{RequeueAfter: reconcileInterval}, nil
}

// calculatePredictedReplicas computes replica count from ML predictions
// using only the lead-time window (e.g., first 20 min of a 60-min horizon).
func (r *PredictiveAutoscalerReconciler) calculatePredictedReplicas(
	autoscaler *autoscalerv1alpha1.PredictiveAutoscaler,
	prediction *MLPredictionResponse,
) int32 {
	log := r.Log.WithValues("predictiveautoscaler", autoscaler.Name)

	if len(prediction.Predictions) == 0 {
		log.Info("No predictions available, using minReplicas")
		return autoscaler.Spec.MinReplicas
	}

	// Determine lead time and step size
	horizonMinutes := autoscaler.Spec.Prediction.HorizonMinutes
	if horizonMinutes == 0 {
		horizonMinutes = defaultHorizonMinutes
	}
	leadTimeMinutes := autoscaler.Spec.Prediction.LeadTimeMinutes
	if leadTimeMinutes == 0 {
		leadTimeMinutes = defaultLeadTimeMinutes
	}

	// Calculate how many prediction steps fall within the lead-time window
	// e.g., 60-min horizon, 6 steps → 10-min/step. leadTime=20 → 2 steps.
	stepSize := float64(horizonMinutes) / float64(len(prediction.Predictions))
	leadTimeSteps := int(math.Ceil(float64(leadTimeMinutes) / stepSize))
	if leadTimeSteps < 1 {
		leadTimeSteps = 1
	}
	if leadTimeSteps > len(prediction.Predictions) {
		leadTimeSteps = len(prediction.Predictions)
	}

	// Peak RPM within lead-time window
	peakRPM := prediction.Predictions[0]
	for i := 1; i < leadTimeSteps; i++ {
		if prediction.Predictions[i] > peakRPM {
			peakRPM = prediction.Predictions[i]
		}
	}

	// Calculate replicas from RPM (CR specifies targetRPS, convert to RPM internally)
	targetRPM := int32(30000)
	if autoscaler.Spec.Metrics.Requests != nil && autoscaler.Spec.Metrics.Requests.TargetRPS > 0 {
		targetRPM = autoscaler.Spec.Metrics.Requests.TargetRPS * 60
	}

	desiredReplicas := int32(math.Ceil(peakRPM / float64(targetRPM)))

	log.Info("Predicted replicas (lead-time window)",
		"leadTimeMinutes", leadTimeMinutes,
		"leadTimeSteps", leadTimeSteps,
		"peakRPM", fmt.Sprintf("%.0f", peakRPM),
		"targetRPS", targetRPM/60,
		"replicas", desiredReplicas,
		"allPredictions", prediction.Predictions)

	// Confidence-based dampening: if model is unsure, scale less aggressively
	if prediction.Confidence < 0.7 && prediction.Confidence > 0 {
		dampened := float64(autoscaler.Spec.MinReplicas) +
			prediction.Confidence*float64(desiredReplicas-autoscaler.Spec.MinReplicas)
		desiredReplicas = int32(math.Ceil(dampened))
		log.Info("Low confidence dampening",
			"confidence", prediction.Confidence, "dampened", desiredReplicas)
	}

	// Apply min/max constraints
	if desiredReplicas < autoscaler.Spec.MinReplicas {
		desiredReplicas = autoscaler.Spec.MinReplicas
	}
	if desiredReplicas > autoscaler.Spec.MaxReplicas {
		desiredReplicas = autoscaler.Spec.MaxReplicas
	}

	return desiredReplicas
}

// queryCurrentRPM queries VictoriaMetrics for the current requests per minute
// of the target deployment. This is the REACTIVE signal.
func (r *PredictiveAutoscalerReconciler) queryCurrentRPM(deploymentName, namespace string) (float64, error) {
	vmURL := os.Getenv("VICTORIAMETRICS_URL")
	if vmURL == "" {
		vmURL = "http://vmselect-vmst.monitoring.svc.cluster.local:8481/select/0/prometheus"
	}

	// Canonical request-count definition (shared with the forecasting service, the KEDA comparison,
	// and the scorer): destination-reported requests only, one workload, one namespace.
	query := fmt.Sprintf(`sum(rate(istio_requests_total{reporter="destination",destination_workload="%s",destination_workload_namespace="%s"}[1m])) * 60`, deploymentName, namespace)

	endpoint := fmt.Sprintf("%s/api/v1/query", vmURL)
	params := url.Values{}
	params.Set("query", query)

	httpClient := &http.Client{Timeout: vmQueryTimeout}
	resp, err := httpClient.Get(fmt.Sprintf("%s?%s", endpoint, params.Encode()))
	if err != nil {
		return 0, fmt.Errorf("VM query failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("VM returned status %d: %s", resp.StatusCode, string(body))
	}

	var vmResp VMInstantQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&vmResp); err != nil {
		return 0, fmt.Errorf("failed to decode VM response: %w", err)
	}

	if vmResp.Status != "success" || len(vmResp.Data.Result) == 0 {
		return 0, nil // No data = 0 RPM (no traffic or metric not available yet)
	}

	valueStr, ok := vmResp.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("unexpected value type in VM response")
	}

	rpm, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse RPM value '%s': %w", valueStr, err)
	}

	return rpm, nil
}

// getCachedPrediction returns ML predictions, using a 5-min cache to avoid
// calling the ML API on every 60s reconcile cycle. Falls back to stale cache on error.
func (r *PredictiveAutoscalerReconciler) getCachedPrediction(
	ctx context.Context,
	autoscaler *autoscalerv1alpha1.PredictiveAutoscaler,
	key string,
) (*MLPredictionResponse, error) {
	// Check cache
	if cached, ok := r.predictionCache[key]; ok {
		if time.Since(cached.fetchedAt) < predictionCacheTTL {
			return cached.response, nil
		}
	}

	// Fetch fresh prediction from ML API
	prediction, err := r.getPrediction(ctx, autoscaler)
	if err != nil {
		// On error, return stale cache if available (better than nothing)
		if cached, ok := r.predictionCache[key]; ok {
			r.Log.Info("Using stale prediction cache due to ML API error",
				"cacheAge", time.Since(cached.fetchedAt).Round(time.Second),
				"error", err.Error())
			return cached.response, nil
		}
		return nil, err
	}

	// Update cache and record the forecast at issuance (before any outcome exists)
	now := time.Now()
	r.predictionCache[key] = &cachedPrediction{
		response:  prediction,
		fetchedAt: now,
	}
	r.recordForecast(autoscaler, prediction, now)
	return prediction, nil
}

// forecastRecord is one JSON line in the append-only forecast log (FORECAST_LOG).
type forecastRecord struct {
	IssuedAt       string  `json:"issued_at"`
	Application    string  `json:"application"`
	Namespace      string  `json:"namespace"`
	HorizonMinutes int32   `json:"horizon_minutes"`
	StepMinutes    float64 `json:"step_minutes"`
	ModelName      string  `json:"model_name"`
	ModelVersion   string  `json:"model_version"`
	ModelTrainedAt string  `json:"model_trained_at"`
	Confidence     float64 `json:"confidence"`
	Forecasts      []struct {
		Step     int     `json:"step"`
		TargetAt string  `json:"target_at"`
		RPM      float64 `json:"rpm"`
	} `json:"forecasts"`
}

// recordForecast exposes every horizon step of a freshly issued forecast as Prometheus series
// (value, target time, issuance time, model identity) and appends one line to the JSONL log.
// Both are written before the forecast horizon elapses, so a scorer can join each step with the
// observation at its target time.
func (r *PredictiveAutoscalerReconciler) recordForecast(
	autoscaler *autoscalerv1alpha1.PredictiveAutoscaler,
	prediction *MLPredictionResponse,
	issuedAt time.Time,
) {
	if prediction == nil || len(prediction.Predictions) == 0 {
		return
	}
	app := autoscaler.Spec.TargetDeployment.Name
	ns := autoscaler.Spec.TargetDeployment.Namespace
	horizon := autoscaler.Spec.Prediction.HorizonMinutes
	if horizon == 0 {
		horizon = defaultHorizonMinutes
	}
	stepMin := float64(horizon) / float64(len(prediction.Predictions))
	rec := forecastRecord{
		IssuedAt: issuedAt.UTC().Format(time.RFC3339), Application: app, Namespace: ns,
		HorizonMinutes: horizon, StepMinutes: stepMin, ModelName: prediction.ModelName,
		ModelVersion: prediction.ModelVersion, ModelTrainedAt: prediction.ModelTrainedAt, Confidence: prediction.Confidence,
	}
	forecastIssuedTsGauge.WithLabelValues(app, ns).Set(float64(issuedAt.Unix()))
	forecastStepMinutesGauge.WithLabelValues(app, ns).Set(stepMin)
	trainedTs := float64(-1)
	if t, err := time.Parse(time.RFC3339, prediction.ModelTrainedAt); err == nil {
		trainedTs = float64(t.Unix())
	}
	modelTrainedTsGauge.WithLabelValues(app, ns).Set(trainedTs)
	modelInfoGauge.DeletePartialMatch(prometheus.Labels{"application": app, "namespace": ns})
	modelInfoGauge.WithLabelValues(app, ns, prediction.ModelName, prediction.ModelVersion).Set(1)
	for i, v := range prediction.Predictions {
		step := i + 1
		target := issuedAt.Add(time.Duration(float64(step) * stepMin * float64(time.Minute)))
		forecastRpmGauge.WithLabelValues(app, ns, strconv.Itoa(step)).Set(v)
		forecastTargetTsGauge.WithLabelValues(app, ns, strconv.Itoa(step)).Set(float64(target.Unix()))
		rec.Forecasts = append(rec.Forecasts, struct {
			Step     int     `json:"step"`
			TargetAt string  `json:"target_at"`
			RPM      float64 `json:"rpm"`
		}{Step: step, TargetAt: target.UTC().Format(time.RFC3339), RPM: v})
	}
	if path := os.Getenv("FORECAST_LOG"); path != "" {
		if f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
			r.Log.Error(err, "forecast log not writable", "path", path)
		} else {
			line, _ := json.Marshal(rec)
			if _, err := f.Write(append(line, '\n')); err != nil {
				r.Log.Error(err, "forecast log write failed", "path", path)
			}
			_ = f.Close()
		}
	}
}

// calculateScaleDownTarget applies stabilization and gradual reduction to scale-down.
// Returns the target replica count — equal to current if scale-down is blocked.
//
// Scale-down rules:
// 1. Block for scaleDownStabilization (5 min) after any scale-up
// 2. Wait for scaleDownStabilization since desired first dropped below current
// 3. Enforce scaleDownCooldown (2 min) between scale-down operations
// 4. Remove at most max(scaleDownMinPods, current*scaleDownMaxPercent%) per cycle
func (r *PredictiveAutoscalerReconciler) calculateScaleDownTarget(
	log logr.Logger,
	state *scaleState,
	current, desired int32,
) int32 {
	now := time.Now()

	if !state.overrideActive {
		// Rule 1: Block scale-down within stabilization window of a scale-up
		if !state.lastScaleUp.IsZero() && now.Sub(state.lastScaleUp) < scaleDownStabilization {
			return current
		}

		// Rule 2: Track how long desired has been below current
		if state.belowCurrentSince.IsZero() {
			state.belowCurrentSince = now
			return current // First detection — start the timer, don't scale yet
		}
		if now.Sub(state.belowCurrentSince) < scaleDownStabilization {
			return current // Still stabilizing
		}
	}

	// Rule 3: Enforce cooldown between scale-down operations (D-11: always applies)
	if !state.lastScaleDown.IsZero() && now.Sub(state.lastScaleDown) < scaleDownCooldown {
		return current
	}

	// Rule 4: Gradual reduction
	maxRemove := int32(math.Ceil(float64(current) * float64(scaleDownMaxPercent) / 100.0))
	if maxRemove < int32(scaleDownMinPods) {
		maxRemove = int32(scaleDownMinPods)
	}

	target := current - maxRemove
	if target < desired {
		target = desired
	}
	if target < 1 {
		target = 1
	}

	return target
}

// getOrCreateScaleState returns the scale state for a given key, creating it if needed.
func (r *PredictiveAutoscalerReconciler) getOrCreateScaleState(key string) *scaleState {
	if state, ok := r.scaleStates[key]; ok {
		return state
	}
	state := &scaleState{}
	r.scaleStates[key] = state
	return state
}

// getTargetDeployment retrieves the target deployment
func (r *PredictiveAutoscalerReconciler) getTargetDeployment(
	ctx context.Context,
	autoscaler *autoscalerv1alpha1.PredictiveAutoscaler,
) (*appsv1.Deployment, error) {
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      autoscaler.Spec.TargetDeployment.Name,
		Namespace: autoscaler.Spec.TargetDeployment.Namespace,
	}, deployment)
	return deployment, err
}

// getPrediction calls the ML API to get predictions
func (r *PredictiveAutoscalerReconciler) getPrediction(
	ctx context.Context,
	autoscaler *autoscalerv1alpha1.PredictiveAutoscaler,
) (*MLPredictionResponse, error) {
	mlAPIURL := os.Getenv("ML_API_URL")
	if mlAPIURL == "" {
		mlAPIURL = "http://ml-api-service.ml-engine.svc.cluster.local:8000"
	}

	// Determine primary metric type
	metricType := "cpu"
	if autoscaler.Spec.Metrics.Requests != nil && autoscaler.Spec.Metrics.Requests.Enabled {
		metricType = "requests"
	}

	// Use full horizon for ML API (it returns all steps, we select lead-time window later)
	horizonMinutes := autoscaler.Spec.Prediction.HorizonMinutes
	if horizonMinutes == 0 {
		horizonMinutes = defaultHorizonMinutes
	}

	reqBody := MLPredictionRequest{
		Application:    autoscaler.Spec.TargetDeployment.Name,
		Namespace:      autoscaler.Spec.TargetDeployment.Namespace,
		MetricType:     metricType,
		HorizonMinutes: horizonMinutes,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Call ML API (120s timeout to allow for LSTM training on first call)
	httpClient := &http.Client{Timeout: mlAPITimeout}
	resp, err := httpClient.Post(
		fmt.Sprintf("%s/predict", mlAPIURL),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to call ML API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ML API returned status %d: %s", resp.StatusCode, string(body))
	}

	var prediction MLPredictionResponse
	if err := json.NewDecoder(resp.Body).Decode(&prediction); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &prediction, nil
}

// scaleDeployment scales the target deployment to the specified replica count.
func (r *PredictiveAutoscalerReconciler) scaleDeployment(
	ctx context.Context,
	deployment *appsv1.Deployment,
	replicas int32,
) error {
	if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == replicas {
		return nil // Already at target
	}
	direction := "up"
	if deployment.Spec.Replicas != nil && replicas < *deployment.Spec.Replicas {
		direction = "down"
	}
	deployment.Spec.Replicas = &replicas
	if err := r.Update(ctx, deployment); err != nil {
		return err
	}
	scaleEventsTotal.WithLabelValues(deployment.Name, deployment.Namespace, direction).Inc()
	return nil
}

// updateStatus updates the PredictiveAutoscaler status
func (r *PredictiveAutoscalerReconciler) updateStatus(
	ctx context.Context,
	autoscaler *autoscalerv1alpha1.PredictiveAutoscaler,
	predictedReplicas int32,
	currentReplicas int32,
) error {
	now := metav1.Now()
	autoscaler.Status.PredictedReplicas = predictedReplicas
	autoscaler.Status.CurrentReplicas = currentReplicas
	autoscaler.Status.LastPrediction = &now

	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "ScalingSuccessful",
		Message:            fmt.Sprintf("Unified: predicted=%d, desired=%d, current=%d", predictedReplicas, predictedReplicas, currentReplicas),
	}

	found := false
	for i, cond := range autoscaler.Status.Conditions {
		if cond.Type == "Ready" {
			autoscaler.Status.Conditions[i] = condition
			found = true
			break
		}
	}
	if !found {
		autoscaler.Status.Conditions = append(autoscaler.Status.Conditions, condition)
	}

	return r.Status().Update(ctx, autoscaler)
}

// updateStatusWithError updates status with error condition
func (r *PredictiveAutoscalerReconciler) updateStatusWithError(
	ctx context.Context,
	autoscaler *autoscalerv1alpha1.PredictiveAutoscaler,
	reason string,
	message string,
) (ctrl.Result, error) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}

	found := false
	for i, cond := range autoscaler.Status.Conditions {
		if cond.Type == "Ready" {
			autoscaler.Status.Conditions[i] = condition
			found = true
			break
		}
	}
	if !found {
		autoscaler.Status.Conditions = append(autoscaler.Status.Conditions, condition)
	}

	if err := r.Status().Update(ctx, autoscaler); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *PredictiveAutoscalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&autoscalerv1alpha1.PredictiveAutoscaler{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}
