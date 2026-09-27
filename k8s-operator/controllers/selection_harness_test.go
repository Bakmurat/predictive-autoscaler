package controllers

// Test-only adapter for factual window-selection audits. It executes the production
// selector at explicit timestamps; it does not replay readiness or scaling actions.
//
// WINDOW_REPLAY_IN=in.json WINDOW_REPLAY_OUT=out.json \
//   go test ./controllers -run '^TestSelectionHarness$' -count=1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type selectionCase struct {
	ID          string    `json:"id"`
	Anchor      string    `json:"anchor"`
	Now         string    `json:"now"`
	Horizon     int32     `json:"horizon"`
	Lead        int32     `json:"lead"`
	Predictions []float64 `json:"predictions"`
}

type selectionInput struct {
	Cases []selectionCase `json:"cases"`
}

type selectionResult struct {
	ID     string    `json:"id"`
	Usable bool      `json:"usable"`
	Steps  []int     `json:"steps"`
	Values []float64 `json:"values"`
}

type selectionOutput struct {
	Cases []selectionResult `json:"cases"`
}

func selectHarnessCase(c selectionCase) (selectionResult, error) {
	out := selectionResult{ID: c.ID, Steps: []int{}, Values: []float64{}}
	if strings.TrimSpace(c.ID) == "" {
		return out, fmt.Errorf("case id is required")
	}
	// Production converts minutes to time.Duration. Reject overflow as well as
	// nonpositive geometry rather than accidentally auditing a wrapped interval.
	const maxMinutes = int64((1<<63 - 1) / int64(time.Minute))
	if c.Horizon <= 0 || c.Lead <= 0 || int64(c.Horizon) > maxMinutes || int64(c.Lead) > maxMinutes {
		return out, fmt.Errorf("case %q: horizon and lead must be positive, representable durations", c.ID)
	}
	anchor, err := time.Parse(time.RFC3339Nano, c.Anchor)
	if err != nil {
		return out, fmt.Errorf("case %q: invalid anchor: %w", c.ID, err)
	}
	now, err := time.Parse(time.RFC3339Nano, c.Now)
	if err != nil {
		return out, fmt.Errorf("case %q: invalid now: %w", c.ID, err)
	}
	values, usable := selectLeadTimeWindow(c.Predictions, anchor, now, c.Horizon, c.Lead)
	markers := make([]float64, len(c.Predictions))
	for i := range markers {
		markers[i] = float64(i + 1)
	}
	steps, markerUsable := selectLeadTimeWindow(markers, anchor, now, c.Horizon, c.Lead)
	if usable != markerUsable || len(values) != len(steps) {
		return out, fmt.Errorf("case %q: marker and value selection disagree", c.ID)
	}
	out.Usable = usable
	if usable {
		out.Values = values
		for _, step := range steps {
			out.Steps = append(out.Steps, int(step))
		}
	}
	return out, nil
}

func runSelectionHarness(inPath, outPath string) error {
	raw, err := os.ReadFile(inPath)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var in selectionInput
	if err := decoder.Decode(&in); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return fmt.Errorf("input must contain exactly one JSON object")
	}
	if len(in.Cases) == 0 {
		return fmt.Errorf("at least one case is required")
	}
	out := selectionOutput{Cases: []selectionResult{}}
	seen := map[string]bool{}
	for _, c := range in.Cases {
		if seen[c.ID] {
			return fmt.Errorf("duplicate case id %q", c.ID)
		}
		seen[c.ID] = true
		result, err := selectHarnessCase(c)
		if err != nil {
			return err
		}
		out.Cases = append(out.Cases, result)
	}
	blob, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	// O_EXCL prevents replacing an earlier receipt, including through a symlink.
	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(blob, '\n'))
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		_ = os.Remove(outPath) // Only the file exclusively created by this call.
		return fmt.Errorf("write output: write=%v close=%v", writeErr, closeErr)
	}
	return nil
}

func TestSelectionHarness(t *testing.T) {
	inPath, outPath := os.Getenv("WINDOW_REPLAY_IN"), os.Getenv("WINDOW_REPLAY_OUT")
	if inPath == "" && outPath == "" {
		t.Skip("WINDOW_REPLAY_IN/OUT not set; test-only selection adapter")
	}
	if inPath == "" || outPath == "" {
		t.Fatal("both WINDOW_REPLAY_IN and WINDOW_REPLAY_OUT are required")
	}
	if err := runSelectionHarness(inPath, outPath); err != nil {
		t.Fatal(err)
	}
}

func TestSelectionWindowCases(t *testing.T) {
	anchor := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		age    time.Duration
		lead   int32
		values []float64
		steps  []int
	}{
		{"before expiration", 10*time.Minute - time.Nanosecond, 20, nil, []int{1, 2}},
		{"at expiration", 10 * time.Minute, 20, nil, []int{2, 3}},
		{"after expiration", 10*time.Minute + time.Nanosecond, 20, nil, []int{2, 3}},
		{"before lead entry", 5*time.Minute - time.Nanosecond, 15, nil, []int{1}},
		{"at lead entry", 5 * time.Minute, 15, nil, []int{1, 2}},
		{"after lead entry", 5*time.Minute + time.Nanosecond, 15, nil, []int{1, 2}},
		{"short lead fallback", 0, 5, nil, []int{1}},
		{"before coverage end", 40*time.Minute - time.Nanosecond, 20, nil, []int{4, 5}},
		{"at coverage end", 40 * time.Minute, 20, nil, []int{5, 6}},
		{"after coverage end", 40*time.Minute + time.Nanosecond, 20, nil, []int{}},
		{"all elapsed", 61 * time.Minute, 20, nil, []int{}},
		{"repeated values", 10 * time.Minute, 20, []float64{7, 7, 7, 7, 7, 7}, []int{2, 3}},
		{"empty predictions", 0, 20, []float64{}, []int{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := tc.values
			if values == nil {
				values = []float64{10, 20, 30, 40, 50, 60}
			}
			got, err := selectHarnessCase(selectionCase{ID: tc.name,
				Anchor: anchor.Format(time.RFC3339Nano), Now: anchor.Add(tc.age).Format(time.RFC3339Nano),
				Horizon: 60, Lead: tc.lead, Predictions: values})
			if err != nil {
				t.Fatal(err)
			}
			wantValues := []float64{}
			for _, step := range tc.steps {
				wantValues = append(wantValues, values[step-1])
			}
			if got.Usable != (len(tc.steps) > 0) || !reflect.DeepEqual(got.Steps, tc.steps) || !reflect.DeepEqual(got.Values, wantValues) {
				t.Fatalf("got %+v; want steps %v values %v", got, tc.steps, wantValues)
			}
		})
	}
}

func TestSelectionHarnessFiles(t *testing.T) {
	valid := selectionCase{ID: "sample", Anchor: "2026-09-26T00:00:00Z", Now: "2026-09-26T00:10:00Z",
		Horizon: 60, Lead: 20, Predictions: []float64{7, 7, 7, 7, 7, 7}}
	marshal := func(cases []selectionCase) string {
		b, err := json.Marshal(selectionInput{Cases: cases})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	t.Run("environment interface and no overwrite", func(t *testing.T) {
		dir := t.TempDir()
		inPath, outPath := filepath.Join(dir, "in.json"), filepath.Join(dir, "out.json")
		if err := os.WriteFile(inPath, []byte(marshal([]selectionCase{valid})), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("WINDOW_REPLAY_IN", inPath)
		t.Setenv("WINDOW_REPLAY_OUT", outPath)
		TestSelectionHarness(t)
		before, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatal(err)
		}
		var got selectionOutput
		if err := json.Unmarshal(before, &got); err != nil {
			t.Fatal(err)
		}
		want := selectionOutput{Cases: []selectionResult{{ID: "sample", Usable: true, Steps: []int{2, 3}, Values: []float64{7, 7}}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		if err := runSelectionHarness(inPath, outPath); !os.IsExist(err) {
			t.Fatalf("existing output must be refused, got %v", err)
		}
		after, err := os.ReadFile(outPath)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatalf("existing output changed: %v", err)
		}
	})
	invalid := map[string]string{
		"empty cases": `{"cases":[]}`, "missing cases": `{}`, "null input": `null`,
		"malformed json": `{`, "trailing json": marshal([]selectionCase{valid}) + `{}`,
		"unknown field": `{"cases":[],"unexpected":1}`, "duplicate ids": marshal([]selectionCase{valid, valid}),
	}
	for _, field := range []string{"anchor", "now", "id", "horizon", "lead", "overflow"} {
		c := valid
		switch field {
		case "anchor":
			c.Anchor = ""
		case "now":
			c.Now = "not a timestamp"
		case "id":
			c.ID = " "
		case "horizon":
			c.Horizon = 0
		case "lead":
			c.Lead = -1
		case "overflow":
			c.Lead = 2147483647
		}
		invalid[field] = marshal([]selectionCase{c})
	}
	for name, raw := range invalid {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			inPath, outPath := filepath.Join(dir, "in.json"), filepath.Join(dir, "out.json")
			if err := os.WriteFile(inPath, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := runSelectionHarness(inPath, outPath); err == nil {
				t.Fatal("invalid input accepted")
			}
			if _, err := os.Stat(outPath); !os.IsNotExist(err) {
				t.Fatalf("invalid input produced an output: %v", err)
			}
		})
	}
}
