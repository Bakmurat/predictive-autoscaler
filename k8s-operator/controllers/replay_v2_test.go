package controllers

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Same-policy replay for forecast contract v2: replayV2 runs a recorded sequence through the very decision functions
// Reconcile uses (v2PredictiveReplicas, unifiedDesired, headroomFloor/headroomEventual, calculateScaleDownTarget, the
// fresh-Active restart hold), and TestV2ReplayMatchesReconcile proves that Reconcile, driven step by step with the same
// inputs, decides the same. An offline evaluation can therefore replay recorded inputs (REPLAY_V2_IN) and get the
// operator's policy, not an approximation of it.
//
// Input JSON: {"min":1,"max":12,"capacity_rpm":600,"headroom_percent":10,"max_extra_replicas":2,"max_extra_percent":50,
//              "steps":[{"t":"RFC3339","rpm":3000,"upper_rps":42.5}, ...]}
// rpm null = the rate could not be measured (hold); upper_rps null = no eligible forecast (reactive only).
// Output JSON: {"decisions":[{"t":...,"current":N,"reactive":N,"predictive":N|null,"cap_binding":bool,"desired":N,
//               "applied":N}, ...]}

type replayV2Step struct {
	T        string   `json:"t"`
	RPM      *float64 `json:"rpm"`
	UpperRPS *float64 `json:"upper_rps"`
}

type replayV2Input struct {
	Min, Max         int32
	CapacityRPM      float64 `json:"capacity_rpm"`
	HeadroomPercent  int32   `json:"headroom_percent"`
	MaxExtraReplicas int64   `json:"max_extra_replicas"`
	MaxExtraPercent  int64   `json:"max_extra_percent"`
	Steps            []replayV2Step
}

type replayV2Decision struct {
	T          string `json:"t"`
	Current    int32  `json:"current"`
	Reactive   int32  `json:"reactive"`
	Predictive *int32 `json:"predictive"`
	CapBinding bool   `json:"cap_binding"`
	Desired    int32  `json:"desired"`
	Applied    int32  `json:"applied"`
}

// replayV2 replays an Active autoscaler from a fresh state (as after a start: the restart hold applies) at `current`.
func replayV2(in replayV2Input, current int32) []replayV2Decision {
	r := &PredictiveAutoscalerReconciler{scaleStates: map[string]*scaleState{}}
	state := r.getOrCreateScaleState("replay/v2")
	now := time.Now()
	state.lastScaleUp, state.lastScaleDown = now, now // a fresh Active state
	var out []replayV2Decision
	for _, s := range in.Steps {
		d := replayV2Decision{T: s.T, Current: current}
		measured := s.RPM != nil
		if measured {
			d.Reactive = boundedReplicas(math.Ceil(*s.RPM / in.CapacityRPM))
		}
		predicted, used := int32(0), false
		if s.UpperRPS != nil {
			p := v2PredictiveReplicas(*s.UpperRPS, in.CapacityRPM, d.Reactive, in.Min, in.MaxExtraReplicas, in.MaxExtraPercent)
			predicted, used, d.CapBinding = p.Capped, true, p.Binding
			d.Predictive = &p.Capped
		}
		desired, _ := unifiedDesired(predicted, d.Reactive, in.Min, in.Max, current, used, measured)
		d.Desired, d.Applied = desired, current
		switch {
		case desired > current:
			d.Applied = desired
			state.lastScaleUp, state.belowCurrentSince = time.Now(), time.Time{}
		case desired < current:
			floor := int32(0)
			if measured {
				floor = headroomFloor(*s.RPM, in.CapacityRPM, in.HeadroomPercent, in.Max)
			}
			if eventual, hold := headroomEventual(state, current, desired, floor); !hold {
				d.Applied = r.calculateScaleDownTarget(logr.Discard(), state, current, eventual)
			}
			if d.Applied < current {
				state.lastScaleDown = time.Now()
			}
		default:
			state.belowCurrentSince = time.Time{}
		}
		out = append(out, d)
		current = d.Applied
	}
	return out
}

func f64(v float64) *float64 { return &v }

// The same sequence through Reconcile (stub service, injected clock) and through replayV2.
func TestV2ReplayMatchesReconcile(t *testing.T) {
	in := replayV2Input{Min: 1, Max: 12, CapacityRPM: 600, HeadroomPercent: 10, MaxExtraReplicas: 2, MaxExtraPercent: 50,
		Steps: []replayV2Step{
			{T: "a", RPM: f64(3000), UpperRPS: f64(40)},   // R 5, P 4: reactive scale-up to 5
			{T: "b", RPM: f64(3000), UpperRPS: f64(1000)}, // P capped at 7
			{T: "c", RPM: f64(6000), UpperRPS: nil},       // untrusted: R 10
			{T: "d", RPM: f64(1200), UpperRPS: f64(30)},   // below current: held (stabilization)
			{T: "e", RPM: nil, UpperRPS: f64(500)},        // telemetry missing: hold whatever the forecast
			{T: "f", RPM: f64(7000), UpperRPS: f64(80)},   // R 12, P 8: max 12
			{T: "g", RPM: f64(600), UpperRPS: f64(1000)},  // R 1, P = 1 + min(2, ceil(50%)) = 2
		}}
	want := replayV2(in, 1)

	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
	f.r.scaleStates = map[string]*scaleState{}
	for i, s := range in.Steps {
		f.at(10 + 300*i)
		if s.RPM != nil {
			vmServer(t, rateBody(t, formatRPM(*s.RPM)), 200)
		} else {
			vmServer(t, `{"status":"success","data":{"resultType":"vector","result":[]}}`, 200)
		}
		if s.UpperRPS != nil {
			f.stub.set("eligible", *s.UpperRPS)
		} else {
			f.stub.set("collecting", 0)
		}
		d := f.reconcile(t)
		w := want[i]
		var gotP *int32
		if d.V2 != nil && d.V2.Accepted {
			gotP = d.V2.CappedReplicas
		}
		if d.CurrentReplicas != w.Current || d.ReactiveReplicas != w.Reactive || d.DesiredReplicas != w.Desired ||
			d.AppliedReplicas != w.Applied || (gotP == nil) != (w.Predictive == nil) || (gotP != nil && *gotP != *w.Predictive) ||
			(d.V2 != nil && d.V2.CapBinding != w.CapBinding) {
			t.Fatalf("step %s: Reconcile current %d reactive %d predictive %v desired %d applied %d; replay %+v (predictive %v)",
				s.T, d.CurrentReplicas, d.ReactiveReplicas, gotP, d.DesiredReplicas, d.AppliedReplicas, w, w.Predictive)
		}
	}
	if want[1].Desired != 7 || !want[1].CapBinding || want[3].Applied != 10 || want[4].Applied != 10 || want[5].Desired != 12 {
		t.Fatalf("replay expectations: %+v", want)
	}
}

func formatRPM(v float64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// TestReplayHarnessV2 is the offline entry point: REPLAY_V2_IN/REPLAY_V2_OUT (see the header), starting at min.
func TestReplayHarnessV2(t *testing.T) {
	inPath, outPath := os.Getenv("REPLAY_V2_IN"), os.Getenv("REPLAY_V2_OUT")
	if inPath == "" || outPath == "" {
		t.Skip("REPLAY_V2_IN/REPLAY_V2_OUT not set; this is a replay harness, not a unit test")
	}
	raw, err := os.ReadFile(inPath)
	if err != nil {
		t.Fatal(err)
	}
	var in replayV2Input
	if err := json.Unmarshal(raw, &in); err != nil {
		t.Fatal(err)
	}
	if !(in.CapacityRPM > 0) || in.Min < 1 || in.Max < in.Min || in.HeadroomPercent < 0 || in.HeadroomPercent > 99 {
		t.Fatalf("invalid replay input: capacity_rpm %v, min %d, max %d, headroom %d", in.CapacityRPM, in.Min, in.Max, in.HeadroomPercent)
	}
	blob, _ := json.MarshalIndent(map[string]interface{}{"decisions": replayV2(in, in.Min)}, "", "  ")
	if err := os.WriteFile(outPath, blob, 0o644); err != nil {
		t.Fatal(err)
	}
}
