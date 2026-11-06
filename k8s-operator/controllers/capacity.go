package controllers

import (
	"fmt"
	"math"
	"strconv"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The per-replica capacity contract (item 1, gap G1): one resolver turns spec.metrics.requests into the request rate one
// ready replica serves within its SLO, and the reactive rule, the forecast rule and the scale-down headroom floor all
// use it. There is no default: without a configured capacity the operator computes no replica count and holds the
// current one (the hidden 20,000 and 30,000 requests-per-minute fallbacks, which disagreed with each other, are gone).

const (
	// capacitySourcePerReplica: spec.metrics.requests.capacityPerReplica (requests per second, decimals allowed).
	capacitySourcePerReplica = "capacityPerReplica"
	// capacitySourceTargetRPS: the deprecated integer spec.metrics.requests.targetRPS.
	capacitySourceTargetRPS = "targetRPS"
	// capacitySourceMissing is recorded in the decision ledger when no capacity resolves.
	capacitySourceMissing = "missing"

	// maxCapacityPerReplicaRPS is the CRD's upper bound on capacityPerReplica (requests per second).
	maxCapacityPerReplicaRPS = 1e6
)

// perReplicaCapacityRPM resolves the per-replica capacity in requests per minute (the operator's internal unit) and
// names the field it came from. ok is false when none resolves: neither field is set, both are, or capacityPerReplica
// is outside (0, 1e6] requests per second. The API server rejects the last two; they are refused here too, so an
// object that bypassed validation is never scaled on a guessed capacity.
func perReplicaCapacityRPM(spec autoscalerv1alpha1.PredictiveAutoscalerSpec) (rpm float64, source string, ok bool) {
	req := spec.Metrics.Requests
	if req == nil {
		return 0, "", false
	}
	if q := req.CapacityPerReplica; q != nil {
		rps := q.AsApproximateFloat64()
		if req.TargetRPS != 0 || math.IsNaN(rps) || rps <= 0 || rps > maxCapacityPerReplicaRPS {
			return 0, "", false
		}
		return rps * 60, capacitySourcePerReplica, true
	}
	if req.TargetRPS > 0 {
		return float64(req.TargetRPS) * 60, capacitySourceTargetRPS, true
	}
	return 0, "", false
}

// capacityCondition is the CapacityConfigured condition for a spec: its status, reason and message.
func capacityCondition(spec autoscalerv1alpha1.PredictiveAutoscalerSpec) (ok bool, reason, msg string) {
	if rpm, source, ok := perReplicaCapacityRPM(spec); ok {
		rps := strconv.FormatFloat(rpm/60, 'g', -1, 64)
		if source == capacitySourceTargetRPS {
			return true, "DeprecatedTargetRPS", fmt.Sprintf("%s requests per second per replica from spec.metrics.requests.targetRPS "+
				"(deprecated: set capacityPerReplica instead)", rps)
		}
		return true, "CapacityPerReplica", fmt.Sprintf("%s requests per second per replica from spec.metrics.requests.capacityPerReplica", rps)
	}
	req := spec.Metrics.Requests
	if req != nil && req.CapacityPerReplica != nil {
		if req.TargetRPS != 0 {
			return false, "CapacityInvalid", "spec.metrics.requests sets both capacityPerReplica and the deprecated targetRPS; keep only capacityPerReplica. " +
				"No replica count is calculated: the current count is held"
		}
		return false, "CapacityInvalid", fmt.Sprintf("spec.metrics.requests.capacityPerReplica %q is not greater than 0 and at most %g requests per second. "+
			"No replica count is calculated: the current count is held", req.CapacityPerReplica.String(), float64(maxCapacityPerReplicaRPS))
	}
	return false, "CapacityMissing", "no per-replica capacity is configured: set spec.metrics.requests.capacityPerReplica (requests per second one " +
		"ready replica serves within its SLO, measured with a load test). No replica count is calculated: the current count is held"
}
