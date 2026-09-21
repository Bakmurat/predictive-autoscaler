package controllers

// Differential replay harness (Codex C-52).
//
// The offline evaluation replays the controller's decisions in Python. A Python
// approximation is not evidence about the controller, so this harness executes the REAL
// decision functions -- adjustForOverestimation and calculateScaleDownTarget, against the
// real scaleState -- over a recorded input sequence, and writes the decisions out. The
// Python replay is then validated against this file rather than trusted.
//
// Run:
//
//	REPLAY_IN=in.json REPLAY_OUT=out.json go test ./controllers -run TestReplayHarness
//
// Input JSON:  {"min":1,"max":12,"steps":[{"t":"RFC3339","reactive":3,"predicted":5,
//               "current_rpm":1234.5,"predictions":[...]}, ...]}
// Output JSON: {"decisions":[{"t":...,"current":N,"desired":N,"applied":N,
//               "override_active":bool,"streak":N}, ...]}

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

type replayStep struct {
	T           string    `json:"t"`
	Reactive    int32     `json:"reactive"`
	Predicted   int32     `json:"predicted"`
	CurrentRPM  float64   `json:"current_rpm"`
	Predictions []float64 `json:"predictions"`
}

type replayInput struct {
	Min   int32        `json:"min"`
	Max   int32        `json:"max"`
	Steps []replayStep `json:"steps"`
}

type replayDecision struct {
	T              string `json:"t"`
	Current        int32  `json:"current"`
	Desired        int32  `json:"desired"`
	Applied        int32  `json:"applied"`
	OverrideActive bool   `json:"override_active"`
	Streak         int    `json:"streak"`
}

type replayOutput struct {
	Decisions []replayDecision `json:"decisions"`
}

// TestReplayHarness is a harness, not an assertion: it runs only when REPLAY_IN is set.
func TestReplayHarness(t *testing.T) {
	inPath := os.Getenv("REPLAY_IN")
	outPath := os.Getenv("REPLAY_OUT")
	if inPath == "" || outPath == "" {
		t.Skip("REPLAY_IN/REPLAY_OUT not set; this is a differential harness, not a unit test")
	}

	raw, err := os.ReadFile(inPath)
	if err != nil {
		t.Fatalf("read %s: %v", inPath, err)
	}
	var in replayInput
	if err := json.Unmarshal(raw, &in); err != nil {
		t.Fatalf("parse %s: %v", inPath, err)
	}

	r := &PredictiveAutoscalerReconciler{scaleStates: map[string]*scaleState{}}
	state := r.getOrCreateScaleState("replay/app")
	log := logr.Discard()

	current := in.Min
	out := replayOutput{}

	for _, s := range in.Steps {
		ts, err := time.Parse(time.RFC3339, s.T)
		if err != nil {
			t.Fatalf("bad timestamp %q: %v", s.T, err)
		}
		// The real functions read time.Now(); the recorded sequence is replayed at its own
		// cadence by advancing the state's timestamps relative to this step instead.
		shiftState(state, ts)

		predicted := adjustForOverestimation(log, s.Predicted, s.Reactive, state,
			s.CurrentRPM, s.Predictions, "replay", "replay")

		desired := predicted
		if s.Reactive > desired {
			desired = s.Reactive
		}
		if in.Min > desired {
			desired = in.Min
		}
		if desired > in.Max {
			desired = in.Max
		}

		applied := current
		switch {
		case desired > current:
			applied = desired
			state.lastScaleUp = time.Now()
			state.belowCurrentSince = time.Time{}
		case desired < current:
			applied = r.calculateScaleDownTarget(log, state, current, desired)
			if applied < current {
				state.lastScaleDown = time.Now()
				state.belowCurrentSince = time.Time{}
			}
		default:
			state.belowCurrentSince = time.Time{}
		}

		out.Decisions = append(out.Decisions, replayDecision{
			T: s.T, Current: current, Desired: desired, Applied: applied,
			OverrideActive: state.overrideActive, Streak: state.overestimateStreak,
		})
		current = applied
	}

	blob, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(outPath, blob, 0o644); err != nil {
		t.Fatalf("write %s: %v", outPath, err)
	}
	t.Logf("replayed %d steps -> %s", len(out.Decisions), outPath)
}

// shiftState rebases the state's wall-clock timestamps so that a recorded sequence replays
// at its own cadence against functions that call time.Now().
func shiftState(state *scaleState, stepTime time.Time) {
	if state.replayAnchor.IsZero() {
		state.replayAnchor = stepTime
		state.replayWall = time.Now()
		return
	}
	elapsed := stepTime.Sub(state.replayAnchor)
	target := state.replayWall.Add(elapsed)
	delta := time.Now().Sub(target)
	if !state.lastScaleUp.IsZero() {
		state.lastScaleUp = state.lastScaleUp.Add(delta)
	}
	if !state.lastScaleDown.IsZero() {
		state.lastScaleDown = state.lastScaleDown.Add(delta)
	}
	if !state.belowCurrentSince.IsZero() {
		state.belowCurrentSince = state.belowCurrentSince.Add(delta)
	}
	state.replayAnchor = stepTime
	state.replayWall = time.Now()
}
