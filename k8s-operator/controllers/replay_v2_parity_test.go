package controllers

import (
	"encoding/json"
	"math"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

// The Go/Python parity fixture of the v2 policy replay. The statistical service compares a challenger's policy cost with
// seasonal-naive's by replaying the operator's v2 decision rule over recorded steps (ml-engine/forecasting/policy_sim.py);
// it must decide exactly what replayV2 decides here, and replayV2 decides what Reconcile does
// (TestV2ReplayMatchesReconcile). This test generates seeded random cases (bounds, capacity, headroom, caps, missing
// rates, missing or capped forecasts, scale-ups and every scale-down hold on 60-second steps with gaps) and checks
// the committed testdata/v2-policy-parity.json against them; ml-engine/tests/test_policy_sim.py replays the same
// file in Python. Regenerate after a deliberate policy change with UPDATE_V2_POLICY_FIXTURES=1.

const v2PolicyParityFile = "testdata/v2-policy-parity.json"

type v2ParityCase struct {
	Input     replayV2Input      `json:"input"`
	Decisions []replayV2Decision `json:"decisions"`
}

func round3(v float64) float64 { return math.Round(v*1000) / 1000 }

func v2ParityCases() []v2ParityCase {
	rng := rand.New(rand.NewSource(2808))
	base := time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)
	var cases []v2ParityCase
	for c := 0; c < 48; c++ {
		minR := int32(1 + rng.Intn(3))
		in := replayV2Input{Min: minR, Max: minR + int32(3+rng.Intn(12)), CapacityRPM: float64(300 + 60*rng.Intn(16)),
			HeadroomPercent: []int32{0, 0, 10, 25}[rng.Intn(4)], MaxExtraReplicas: int64(rng.Intn(4)),
			MaxExtraPercent: []int64{0, 25, 50, 100}[rng.Intn(4)]}
		t := base.Add(time.Duration(c) * time.Hour)
		rpm := in.CapacityRPM * float64(1+rng.Intn(int(in.Max)))
		for i, n := 0, 40+rng.Intn(80); i < n; i++ {
			step := 60 * time.Second
			if rng.Float64() < 0.05 {
				step = time.Duration(120+60*rng.Intn(8)) * time.Second // a missed decision or two
			}
			t = t.Add(step)
			rpm *= math.Exp(rng.NormFloat64() * 0.25)
			if rpm < 1 {
				rpm = 1
			}
			if rng.Float64() < 0.02 {
				rpm *= 3 // a sudden step
			}
			s := replayV2Step{T: t.Format(time.RFC3339)}
			if rng.Float64() >= 0.05 {
				v := round3(rpm)
				s.RPM = &v
			}
			if rng.Float64() >= 0.3 {
				u := round3(rpm / 60 * (0.6 + rng.Float64()*1.4))
				s.UpperRPS = &u
			}
			in.Steps = append(in.Steps, s)
		}
		cases = append(cases, v2ParityCase{Input: in, Decisions: replayV2(in, in.Min)})
	}
	return cases
}

func TestV2PolicyParityFixtures(t *testing.T) {
	got := v2ParityCases()
	blob, err := json.Marshal(map[string]interface{}{ // compact: generated data, about half the indented size
		"generated_by": "k8s-operator/controllers/replay_v2_parity_test.go (replayV2, seed 2808)", "cases": got})
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("UPDATE_V2_POLICY_FIXTURES") == "1" {
		if err := os.WriteFile(v2PolicyParityFile, append(blob, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(v2PolicyParityFile)
	if err != nil {
		t.Fatalf("%v (generate it with UPDATE_V2_POLICY_FIXTURES=1)", err)
	}
	var file struct {
		Cases []v2ParityCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(file.Cases, got) {
		t.Fatalf("%s is stale: the v2 policy changed; review the change, then regenerate with "+
			"UPDATE_V2_POLICY_FIXTURES=1 so the Python replay is held to it", v2PolicyParityFile)
	}
	holds, ups, downs := 0, 0, 0
	for _, c := range got {
		for _, d := range c.Decisions {
			switch {
			case d.Applied > d.Current:
				ups++
			case d.Applied < d.Current:
				downs++
			case d.Desired < d.Current:
				holds++
			}
		}
	}
	if ups < 50 || downs < 50 || holds < 100 {
		t.Fatalf("the fixture does not exercise the policy: %d scale-ups, %d scale-downs, %d holds", ups, downs, holds)
	}
}
