package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// forecastComponentValues preserves zero versus unavailable diagnostics. Pointers
// also prevent a null boolean from silently becoming a false availability flag.
type forecastComponentValues struct {
	Pattern                 []*float64 `json:"pattern"`
	PatternAvailablePerStep []*bool    `json:"pattern_available_per_step"`
	PatternWeights          []*float64 `json:"pattern_weights"`
	LSTM                    []*float64 `json:"lstm"`
	Blended                 []*float64 `json:"blended"`
	Final                   []*float64 `json:"final"`
	PatternSource           *string    `json:"pattern_source"`
	NetworkFailed           *string    `json:"network_failed"`
	NetworkFinitePerStep    []*bool    `json:"network_finite_per_step"`
}

// forecastComponents is evidence from the same API response as the core forecast.
// Only status=ok includes numeric data; it can still contain unavailable steps.
type forecastComponents struct {
	Schema           string   `json:"schema"`
	Status           string   `json:"status"`
	Reason           *string  `json:"reason"`
	TargetTimestamps []string `json:"target_timestamps,omitempty"`
	*forecastComponentValues
}

func unavailableComponents(status, reason string) *forecastComponents {
	return &forecastComponents{Schema: "component-v1", Status: status, Reason: &reason}
}

// Decode independently after the core succeeded. An optional field with the
// wrong shape/type must not turn a usable forecast into a transport failure.
func decodeForecastComponents(raw []byte) *forecastComponents {
	var envelope struct {
		Components json.RawMessage `json:"components"`
		Targets    []*string       `json:"target_timestamps"`
	}
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&envelope); err != nil {
		return unavailableComponents("invalid", "malformed_diagnostics")
	}
	if len(envelope.Components) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Components), []byte("null")) {
		return unavailableComponents("absent", "components_not_provided")
	}
	var values forecastComponentValues
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(envelope.Components, &values); err != nil {
		return unavailableComponents("invalid", "malformed_diagnostics")
	}
	if err := json.Unmarshal(envelope.Components, &fields); err != nil {
		return unavailableComponents("invalid", "malformed_diagnostics")
	}
	// network_failed is explicitly null on success or an exception string on
	// failure. Missing metadata must not be silently promoted to success.
	if _, present := fields["network_failed"]; !present {
		return unavailableComponents("invalid", "missing_network_status")
	}
	if len(envelope.Targets) != 6 {
		return unavailableComponents("invalid", "invalid_target_count")
	}
	targets := make([]string, 6)
	for i, target := range envelope.Targets {
		if target == nil {
			return unavailableComponents("invalid", "null_target")
		}
		targets[i] = *target
	}
	return &forecastComponents{Schema: "component-v1", Status: "ok",
		TargetTimestamps: targets, forecastComponentValues: &values}
}

// forTargets binds diagnostics to the targets actually serialized in the core
// issuance. Validation changes only evidence availability, never served values.
func (e *forecastComponents) forTargets(served []float64, targets []string, horizon int32) *forecastComponents {
	if e == nil {
		return unavailableComponents("absent", "components_not_provided")
	}
	if e.Status != "ok" {
		if (e.Status == "absent" || e.Status == "invalid") && e.Reason != nil {
			return unavailableComponents(e.Status, *e.Reason)
		}
		return unavailableComponents("invalid", "invalid_diagnostic_state")
	}
	if horizon != 60 || len(served) != 6 || len(targets) != 6 {
		return unavailableComponents("invalid", "unsupported_forecast_geometry")
	}
	c := e.forecastComponentValues
	if c == nil || len(c.Pattern) != 6 || len(c.PatternAvailablePerStep) != 6 ||
		len(c.PatternWeights) != 6 || len(c.LSTM) != 6 || len(c.Blended) != 6 ||
		len(c.Final) != 6 || len(c.NetworkFinitePerStep) != 6 {
		return unavailableComponents("invalid", "missing_or_invalid_component_shape")
	}
	// JSON decode rejects overflowing numbers, but recheck typed values too: no
	// optional diagnostic may make the core issuance's JSON marshal fail.
	for _, values := range [][]*float64{c.Pattern, c.PatternWeights, c.LSTM, c.Blended, c.Final} {
		for _, value := range values {
			if value != nil && (math.IsNaN(*value) || math.IsInf(*value, 0)) {
				return unavailableComponents("invalid", "nonfinite_component")
			}
		}
	}
	for i := 0; i < 6; i++ {
		if c.PatternAvailablePerStep[i] == nil || c.NetworkFinitePerStep[i] == nil ||
			c.PatternWeights[i] == nil || c.Final[i] == nil {
			return unavailableComponents("invalid", "null_required_component")
		}
		if (c.Pattern[i] != nil) != *c.PatternAvailablePerStep[i] ||
			(c.LSTM[i] != nil) != *c.NetworkFinitePerStep[i] {
			return unavailableComponents("invalid", "component_availability_mismatch")
		}
		if *c.PatternWeights[i] < 0 || *c.PatternWeights[i] > 1 {
			return unavailableComponents("invalid", "invalid_pattern_weight")
		}
		// Python round(float, 2) rounds the binary input, not an intermediate
		// value multiplied by 100. Preserve raw values; compare its decimal form.
		rounded, err := strconv.ParseFloat(strconv.FormatFloat(*c.Final[i], 'f', 2, 64), 64)
		if err != nil || rounded != served[i] {
			return unavailableComponents("invalid", "final_value_mismatch")
		}
	}
	if len(e.TargetTimestamps) != 6 {
		return unavailableComponents("invalid", "invalid_target_count")
	}
	normalized := make([]string, 6)
	misaligned := false
	for i, raw := range e.TargetTimestamps {
		// Preserve fractional seconds, unlike the legacy core helper. A target
		// that loses precision in the core log must be marked misaligned.
		at, err := parseComponentTarget(raw)
		if err != nil {
			return unavailableComponents("invalid", "invalid_target_timestamp")
		}
		core, err := time.Parse(time.RFC3339Nano, targets[i])
		if err != nil {
			return unavailableComponents("invalid", "invalid_core_target")
		}
		normalized[i] = at.UTC().Format(time.RFC3339Nano)
		misaligned = misaligned || !at.Equal(core)
	}
	if misaligned {
		out := unavailableComponents("misaligned", "target_mismatch")
		out.TargetTimestamps = normalized
		return out
	}
	return &forecastComponents{Schema: "component-v1", Status: "ok",
		TargetTimestamps: normalized, forecastComponentValues: c}
}

// parseComponentTarget accepts the API's naive UTC ISO strings and explicit
// offsets without silently truncating subsecond precision or repairing errors.
func parseComponentTarget(raw string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05"} {
		if at, err := time.Parse(layout, raw); err == nil {
			return at, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid component timestamp %q", raw)
}
