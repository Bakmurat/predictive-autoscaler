package controllers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func runLifecycleReplay(t *testing.T, input any) []replayDecision {
	t.Helper()
	dir := t.TempDir()
	in, out := filepath.Join(dir, "input.json"), filepath.Join(dir, "output.json")
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(in, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REPLAY_IN", in)
	t.Setenv("REPLAY_OUT", out)
	TestReplayHarness(t)
	raw, err = os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var got replayOutput
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	return got.Decisions
}

func TestReplayLifecycleTelemetryRampAndOverride(t *testing.T) {
	for _, tc := range []struct {
		name             string
		lastRPM          float64
		desired, applied int32
		streak           int
	}{
		{"rising", 1030, 6, 6, 0},
		{"steady", 1000, 3, 4, 3},
		{"subthreshold", 1010, 3, 4, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := replayInput{Min: 1, Max: 12}
			for i, stamp := range []string{"2026-09-27T00:00:00Z", "2026-09-27T00:01:00Z", "2026-09-27T00:02:00Z"} {
				rpm := 1000.0
				if i == 2 {
					rpm = tc.lastRPM
				}
				input.Steps = append(input.Steps, replayStep{T: stamp, Reactive: 2, Predicted: 6,
					CurrentRPM: rpm, Predictions: []float64{3600, 3600, 3600, 3600, 3600, 3600}})
			}
			got := runLifecycleReplay(t, input)
			if len(got) != 3 {
				t.Fatalf("got %d decisions", len(got))
			}
			for i := 0; i < 2; i++ {
				if got[i].Desired != 6 || got[i].Applied != 6 || got[i].Streak != i+1 || got[i].OverrideActive {
					t.Fatalf("initial decision %d: %+v", i, got[i])
				}
			}
			last := got[2]
			if last.Desired != tc.desired || last.Applied != tc.applied || last.Streak != tc.streak {
				t.Errorf("last decision %+v; want desired=%d applied=%d streak=%d", last, tc.desired, tc.applied, tc.streak)
			}
			// Live reconciliation clears the override after a successful scale-down.
			if last.OverrideActive {
				t.Errorf("override remains active after decision: %+v", last)
			}
		})
	}
}

func TestReplayLifecyclePreservesStabilizationAfterScaleDown(t *testing.T) {
	input := replayInput{Min: 1, Max: 12, Steps: []replayStep{
		{T: "2026-09-27T00:00:00Z", Reactive: 6, Predicted: 6, CurrentRPM: 3600},
		// Start the below-current timer after the scale-up hold has expired.
		{T: "2026-09-27T00:06:00Z", Reactive: 3, Predicted: 3, CurrentRPM: 1800},
		{T: "2026-09-27T00:11:30Z", Reactive: 3, Predicted: 3, CurrentRPM: 1800},
		// Cooldown holds four replicas without restarting stabilization.
		{T: "2026-09-27T00:12:30Z", Reactive: 3, Predicted: 3, CurrentRPM: 1800},
		{T: "2026-09-27T00:13:45Z", Reactive: 3, Predicted: 3, CurrentRPM: 1800},
	}}
	got := runLifecycleReplay(t, input)
	want := []int32{6, 6, 4, 4, 3}
	if len(got) != len(want) {
		t.Fatalf("got %d decisions", len(got))
	}
	for i := range got {
		if got[i].Applied != want[i] {
			t.Errorf("decision %d applied=%d, want %d", i, got[i].Applied, want[i])
		}
	}
}
