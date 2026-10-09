package controllers

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayMissingInputsKeepCurrent(t *testing.T) {
	for _, missing := range []bool{true, false} {
		name := "query_error_and_no_forecast"
		if !missing {
			name = "available_telemetry_and_no_forecast"
		}
		t.Run(name, func(t *testing.T) {
			steps := []map[string]any{{"t": "2026-09-27T00:00:00Z", "reactive": 6, "predicted": 6, "current_rpm": 3600}}
			for _, stamp := range []string{"2026-09-27T00:10:00Z", "2026-09-27T00:20:00Z", "2026-09-27T00:30:00Z"} {
				step := map[string]any{"t": stamp, "reactive": 0, "predicted": 0, "current_rpm": 0, "keep_current": missing}
				if !missing {
					step["reactive"], step["predicted"], step["current_rpm"] = 2, 2, 1200
				}
				steps = append(steps, step)
			}
			got := runLifecycleReplay(t, map[string]any{"min": 1, "max": 12, "steps": steps})
			want := []int32{6, 6, 4, 2}
			if missing {
				want = []int32{6, 6, 6, 6}
			}
			if len(got) != len(want) {
				t.Fatalf("got %d decisions, want %d", len(got), len(want))
			}
			for i, d := range got {
				if d.Applied != want[i] || (missing && d.Desired != 6) {
					t.Errorf("step %d: %+v, want applied %d", i, d, want[i])
				}
			}
		})
	}
}

func TestReplayMissingInputResetsLastRPM(t *testing.T) {
	got := runLifecycleReplay(t, map[string]any{"min": 1, "max": 12, "steps": []map[string]any{
		{"t": "2026-09-27T00:00:00Z", "reactive": 2, "predicted": 6, "current_rpm": 1000},
		{"t": "2026-09-27T00:10:00Z", "reactive": 0, "predicted": 0, "current_rpm": 0, "keep_current": true},
		{"t": "2026-09-27T00:20:00Z", "reactive": 2, "predicted": 6, "current_rpm": 1030, "predictions": []float64{3600, 3600, 3600, 3600, 3600, 3600}},
	}})
	if len(got) != 3 || got[2].Streak != 1 || got[2].Desired != 6 || got[2].Applied != 6 {
		t.Fatalf("missing RPM must prevent comparing to an older measurement: %+v", got)
	}
}

func TestReplayRejectsContradictoryKeepCurrent(t *testing.T) {
	for _, field := range []string{"reactive", "current_rpm"} {
		t.Run(field, func(t *testing.T) {
			step := map[string]any{"t": "2026-09-27T00:10:00Z", "keep_current": true}
			step[field] = 1
			if field == "predictions" {
				step[field] = []float64{1, 1, 1, 1, 1, 1}
			}
			dir := t.TempDir()
			in, out := filepath.Join(dir, "input.json"), filepath.Join(dir, "output.json")
			raw, err := json.Marshal(map[string]any{"min": 1, "max": 12, "steps": []any{step}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(in, raw, 0600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(exe, "-test.run=^TestReplayHarness$")
			cmd.Env = append(os.Environ(), "REPLAY_IN="+in, "REPLAY_OUT="+out)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "keep_current requires missing telemetry") {
				t.Fatalf("contradictory input was not rejected: err=%v output=%s", err, output)
			}
		})
	}
}

// Missing telemetry holds the current count even when a forecast is present (lower or higher): without a measured
// rate the controller neither scales down on "no data" nor scales on a forecast alone (user decision 2026-10-09).
func TestReplayMissingTelemetryHoldsDespiteAForecast(t *testing.T) {
	got := runLifecycleReplay(t, map[string]any{"min": 1, "max": 12, "steps": []map[string]any{
		{"t": "2026-09-27T00:00:00Z", "reactive": 6, "predicted": 6, "current_rpm": 3600},
		{"t": "2026-09-27T00:10:00Z", "keep_current": true, "predicted": 2, "predictions": []float64{1200, 1200, 1200, 1200, 1200, 1200}},
		{"t": "2026-09-27T00:20:00Z", "keep_current": true, "predicted": 10, "predictions": []float64{6000, 6000, 6000, 6000, 6000, 6000}},
	}})
	if len(got) != 3 {
		t.Fatalf("got %d decisions", len(got))
	}
	for i, d := range got[1:] {
		if d.Desired != 6 || d.Applied != 6 {
			t.Errorf("step %d: %+v, want desired = applied = 6 (hold)", i+1, d)
		}
	}
}
