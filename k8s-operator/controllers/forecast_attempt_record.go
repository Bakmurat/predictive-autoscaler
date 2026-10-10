package controllers

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"time"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

const forecastAttemptSchema = "forecast-attempt-v1"

// Only the log is serialized here. The existing single reconcile worker remains
// responsible for the prediction cache and scale state; this is not their lock.
type forecastLedgerState struct {
	mu                  sync.Mutex
	initialized         bool
	runID               string
	eventSeq, lookupSeq uint64
	// Test seams affect evidence only. Nil selects standard production I/O.
	random io.Reader
	open   func(string) (io.WriteCloser, error)
}

type forecastRequestIdentity struct {
	AutoscalerNamespace string `json:"autoscaler_namespace"`
	AutoscalerName      string `json:"autoscaler_name"`
	AutoscalerUID       string `json:"autoscaler_uid"`
	Application         string `json:"application"`
	Namespace           string `json:"namespace"`
	RequestNamespace    string `json:"request_namespace"`
	MetricType          string `json:"metric_type"`
	HorizonMinutes      int32  `json:"horizon_minutes"`
}

type forecastLookup struct {
	Schema                string `json:"schema"`
	InstrumentationStatus string `json:"instrumentation_status"`
	OperatorRunID         string `json:"operator_run_id,omitempty"`
	LookupID              string `json:"lookup_id,omitempty"`
	forecastRequestIdentity
	Resolution         string   `json:"resolution"`
	CacheAction        string   `json:"cache_action"`
	CacheAgeSeconds    *float64 `json:"cache_age_seconds"`
	FreshAttemptID     string   `json:"fresh_attempt_id,omitempty"`
	PriorIssuanceID    string   `json:"prior_issuance_id,omitempty"`
	ReturnedIssuanceID string   `json:"returned_issuance_id,omitempty"`
	ReturnedLinkStatus string   `json:"returned_link_status"`
	completion         *forecastAttemptCompleted
}

type forecastLedgerEvent struct {
	Schema        string `json:"schema"`
	Event         string `json:"event"`
	At            string `json:"at"`
	OperatorRunID string `json:"operator_run_id"`
	EventSeq      uint64 `json:"event_seq"`
}

type forecastAttemptStarted struct {
	forecastLedgerEvent
	forecastRequestIdentity
	LookupID          string `json:"lookup_id"`
	AttemptID         string `json:"attempt_id"`
	RequestBodySHA256 string `json:"request_body_sha256"`
	PriorIssuanceID   string `json:"prior_issuance_id,omitempty"`
}

type forecastAttemptCompleted struct {
	forecastAttemptStarted
	StartedAt               string   `json:"started_at"`
	ElapsedSeconds          float64  `json:"elapsed_seconds"`
	HTTPStatus              *int     `json:"http_status"`
	Outcome                 string   `json:"outcome"`
	ErrorClass              string   `json:"error_class"`
	ErrorStage              string   `json:"error_stage"`
	ServedPredictionsStatus string   `json:"served_predictions_status"`
	ServedStepCount         int      `json:"served_step_count"`
	ServedStepStatus        []string `json:"served_step_status"`
}

func predictionRequest(a *autoscalerv1alpha1.PredictiveAutoscaler) MLPredictionRequest {
	metric := "cpu"
	if a.Spec.Metrics.Requests != nil && a.Spec.Metrics.Requests.Enabled {
		metric = "requests"
	}
	horizon := a.Spec.Prediction.HorizonMinutes
	if horizon == 0 {
		horizon = defaultHorizonMinutes
	}
	req := MLPredictionRequest{Application: a.Spec.TargetDeployment.Name, Namespace: a.Spec.TargetDeployment.Namespace,
		MetricType: metric, HorizonMinutes: horizon,
		AutoscalerName: a.Name, AutoscalerNamespace: a.Namespace, AutoscalerUID: string(a.UID), AutoscalerGeneration: a.Generation}
	if ms := a.Status.MetricSource; ms != nil {
		req.TargetUID, req.MetricQuerySHA256, req.Contract = ms.TargetUID, ms.SHA256, ms.Contract
	}
	return req
}

func (r *PredictiveAutoscalerReconciler) newForecastLookup(a *autoscalerv1alpha1.PredictiveAutoscaler) *forecastLookup {
	request := predictionRequest(a)
	ns := request.Namespace
	if ns == "" {
		ns = a.Namespace
	}
	lookup := &forecastLookup{Schema: forecastAttemptSchema, InstrumentationStatus: "disabled", Resolution: "unavailable",
		CacheAction: "none", ReturnedLinkStatus: "none", forecastRequestIdentity: forecastRequestIdentity{
			AutoscalerNamespace: a.Namespace, AutoscalerName: a.Name, AutoscalerUID: string(a.UID),
			Application: request.Application, Namespace: ns, RequestNamespace: request.Namespace,
			MetricType: request.MetricType, HorizonMinutes: request.HorizonMinutes}}
	if os.Getenv("FORECAST_LOG") == "" {
		return lookup
	}
	state := &r.forecastLedger
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.initialized {
		state.initialized = true
		reader := state.random
		if reader == nil {
			reader = rand.Reader
		}
		var nonce [16]byte
		if _, err := io.ReadFull(reader, nonce[:]); err != nil {
			r.Log.Error(err, "forecast ledger identity unavailable; scaling continues without links")
		} else {
			state.runID = hex.EncodeToString(nonce[:])
			session := forecastLedgerEvent{Schema: forecastAttemptSchema, Event: "forecast_ledger_session",
				At: time.Now().UTC().Format(time.RFC3339Nano), OperatorRunID: state.runID}
			state.eventSeq++
			session.EventSeq = state.eventSeq
			r.appendForecastLogLocked(session)
		}
	}
	if state.runID == "" {
		lookup.InstrumentationStatus = "initialization_failed"
		return lookup
	}
	state.lookupSeq++
	lookup.InstrumentationStatus, lookup.OperatorRunID = "enabled", state.runID
	lookup.LookupID = fmt.Sprintf("%s:lookup:%d", state.runID, state.lookupSeq)
	return lookup
}

func (r *PredictiveAutoscalerReconciler) appendLedgerEvent(event *forecastLedgerEvent, value interface{}) {
	r.forecastLedger.mu.Lock()
	defer r.forecastLedger.mu.Unlock()
	r.forecastLedger.eventSeq++
	event.EventSeq = r.forecastLedger.eventSeq
	r.appendForecastLogLocked(value)
}

func (r *PredictiveAutoscalerReconciler) beginForecastAttempt(lookup *forecastLookup, request []byte) (*forecastAttemptCompleted, time.Time) {
	if lookup == nil || lookup.InstrumentationStatus != "enabled" {
		return nil, time.Time{}
	}
	now := time.Now()
	lookup.FreshAttemptID = lookup.LookupID + ":attempt"
	hash := sha256.Sum256(request)
	start := forecastAttemptStarted{forecastLedgerEvent: forecastLedgerEvent{Schema: forecastAttemptSchema,
		Event: "forecast_attempt_started", At: now.UTC().Format(time.RFC3339Nano), OperatorRunID: lookup.OperatorRunID},
		forecastRequestIdentity: lookup.forecastRequestIdentity, LookupID: lookup.LookupID, AttemptID: lookup.FreshAttemptID,
		RequestBodySHA256: hex.EncodeToString(hash[:]), PriorIssuanceID: lookup.PriorIssuanceID}
	r.appendLedgerEvent(&start.forecastLedgerEvent, &start)
	completion := &forecastAttemptCompleted{forecastAttemptStarted: start, StartedAt: start.At,
		ErrorClass: "none", ErrorStage: "none", ServedPredictionsStatus: "not_evaluated", ServedStepStatus: []string{}}
	completion.Event = "forecast_attempt_completed"
	return completion, now
}

// A single deferred append follows the lookup's existing policy time observations.
// The completion timestamp was captured at HTTP return/decode. An issuance can
// precede this line in file order; readers join the full verified buffer by ID.
func (r *PredictiveAutoscalerReconciler) recordForecastCompletion(lookup *forecastLookup) {
	if lookup != nil && lookup.completion != nil {
		r.appendLedgerEvent(&lookup.completion.forecastLedgerEvent, lookup.completion)
		lookup.completion = nil
	}
}

func (lookup *forecastLookup) returned(p *MLPredictionResponse, resolution, action string) {
	lookup.Resolution, lookup.CacheAction = resolution, action
	lookup.ReturnedLinkStatus = "none"
	if p != nil {
		lookup.ReturnedIssuanceID = p.issuanceID
		if p.issuanceID != "" {
			lookup.ReturnedLinkStatus = "known"
		} else if len(p.Predictions) != 0 {
			lookup.ReturnedLinkStatus = "unavailable"
		}
	}
}

func timeoutError(err error) bool {
	var networkError net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkError) && networkError.Timeout())
}

func (completion *forecastAttemptCompleted) failure(outcome, class, stage string, err error) {
	if completion == nil {
		return
	}
	completion.Outcome, completion.ErrorClass, completion.ErrorStage = outcome, class, stage
	if timeoutError(err) {
		completion.ErrorClass = "timeout"
		if outcome != "http_refusal" && outcome != "http_error" {
			completion.Outcome = "timeout"
		}
	}
}

// Raw served values, not component flags or the decoded []float64, preserve null.
func (completion *forecastAttemptCompleted) classifyServed(raw []byte) {
	if completion == nil {
		return
	}
	completion.ServedPredictionsStatus = "malformed"
	var envelope map[string]json.RawMessage
	// Match the response decoder's first-document boundary, including when its
	// buffered read captured trailing bytes that do not affect acceptance.
	if json.NewDecoder(bytes.NewReader(raw)).Decode(&envelope) != nil {
		return
	}
	values, exists := envelope["predictions"]
	if !exists {
		completion.ServedPredictionsStatus = "absent"
		return
	}
	if bytes.Equal(bytes.TrimSpace(values), []byte("null")) {
		completion.ServedPredictionsStatus = "null"
		return
	}
	var items []json.RawMessage
	if json.Unmarshal(values, &items) != nil {
		return
	}
	completion.ServedPredictionsStatus, completion.ServedStepCount = "array", len(items)
	for _, item := range items {
		status := "malformed"
		if bytes.Equal(bytes.TrimSpace(item), []byte("null")) {
			status = "null"
		} else {
			var number json.Number
			decoder := json.NewDecoder(bytes.NewReader(item))
			decoder.UseNumber()
			var value interface{}
			if decoder.Decode(&value) == nil {
				if parsed, ok := value.(json.Number); ok {
					number = parsed
					n, err := number.Float64()
					if math.IsInf(n, 0) || math.IsNaN(n) {
						status = "non_finite"
					} else if err == nil {
						status = "finite"
					}
				}
			}
		}
		completion.ServedStepStatus = append(completion.ServedStepStatus, status)
	}
}

// Called only with the narrow writer mutex held. No failure changes application behavior.
func (r *PredictiveAutoscalerReconciler) appendForecastLogLocked(value interface{}) {
	path := os.Getenv("FORECAST_LOG")
	if path == "" {
		return
	}
	line, err := json.Marshal(value)
	if err != nil {
		r.Log.Error(err, "forecast log marshal failed", "path", path)
		return
	}
	var file io.WriteCloser
	if r.forecastLedger.open != nil {
		file, err = r.forecastLedger.open(path)
	} else {
		file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	}
	if err != nil {
		r.Log.Error(err, "forecast log not writable", "path", path)
		return
	}
	line = append(line, '\n')
	n, writeErr := file.Write(line)
	if writeErr == nil && n != len(line) {
		writeErr = io.ErrShortWrite
	}
	if writeErr != nil {
		r.Log.Error(writeErr, "forecast log write failed", "path", path)
	}
	if closeErr := file.Close(); closeErr != nil {
		r.Log.Error(closeErr, "forecast log close failed", "path", path)
	}
}
