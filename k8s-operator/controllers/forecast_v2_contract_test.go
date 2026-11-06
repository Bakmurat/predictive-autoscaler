package controllers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The Go side of forecast contract v2 against fixtures generated from ml-engine/forecasting/contracts.py
// (k8s-operator/hack/gen_v2_fixtures.py; scripts/verify.sh fails when they are stale). Every expectation below was
// computed or checked by the Python module.

type v2Fixtures struct {
	ForecastContract string   `json:"forecast_contract"`
	SettingsSchema   string   `json:"settings_schema"`
	EvidenceSchema   string   `json:"evidence_schema"`
	Unit             string   `json:"unit"`
	AllowedIntervals []int64  `json:"allowed_intervals"`
	Phases           []string `json:"phases"`
	Reasons          []string `json:"reasons"`
	ModelFamilies    []string `json:"model_families"`
	Settings         []struct {
		Name        string          `json:"name"`
		Prediction  json.RawMessage `json:"prediction"`
		Refused     *string         `json:"refused"`
		Canonical   string          `json:"canonical"`
		Fingerprint string          `json:"fingerprint"`
		Steps       int64           `json:"steps"`
	} `json:"settings"`
	Evidence []struct {
		SettingsCase string `json:"settings_case"`
		LeadSeconds  int64  `json:"lead_seconds"`
		Signal       struct {
			AutoscalerUID string `json:"autoscaler_uid"`
			TargetUID     string `json:"target_uid"`
			QuerySHA256   string `json:"query_sha256"`
		} `json:"signal"`
		EvidenceKey string `json:"evidence_key"`
	} `json:"evidence"`
	Lead struct {
		Automatic []struct {
			Startup          int64 `json:"startup"`
			DecisionInterval int64 `json:"decision_interval"`
			Allowance        int64 `json:"allowance"`
			Interval         int64 `json:"interval"`
			Lead             int64 `json:"lead"`
		} `json:"automatic"`
		Resolve []struct {
			Name  string                 `json:"name"`
			Lead  map[string]interface{} `json:"lead"`
			Valid bool                   `json:"valid"`
		} `json:"resolve"`
		Windows []struct {
			Origin     string   `json:"origin"`
			Interval   int64    `json:"interval"`
			Lead       int64    `json:"lead"`
			Startup    int64    `json:"startup"`
			Allowance  int64    `json:"allowance"`
			Targets    []string `json:"targets"`
			ValidUntil string   `json:"valid_until"`
		} `json:"windows"`
	} `json:"lead"`
	Responses struct {
		Binding struct {
			AutoscalerNamespace   string                            `json:"autoscaler_namespace"`
			AutoscalerName        string                            `json:"autoscaler_name"`
			AutoscalerUID         string                            `json:"autoscaler_uid"`
			AutoscalerGeneration  int64                             `json:"autoscaler_generation"`
			TargetNamespace       string                            `json:"target_namespace"`
			TargetName            string                            `json:"target_name"`
			TargetUID             string                            `json:"target_uid"`
			MetricQuerySHA256     string                            `json:"metric_query_sha256"`
			SettingsSHA256        string                            `json:"settings_sha256"`
			DecisionOrigin        int64                             `json:"decision_origin"`
			SampleIntervalSeconds int64                             `json:"sample_interval_seconds"`
			NominalMiscoverageBps int64                             `json:"nominal_miscoverage_bps"`
			Lead                  autoscalerv1alpha1.PredictionLead `json:"lead"`
			EvidenceKey           string                            `json:"evidence_key"`
		} `json:"binding"`
		Request map[string]interface{} `json:"request"`
		Valid   []struct {
			Name string          `json:"name"`
			Wire json.RawMessage `json:"wire"`
		} `json:"valid"`
		Invalid []struct {
			Name  string          `json:"name"`
			Wire  json.RawMessage `json:"wire"`
			Stage string          `json:"stage"`
			Why   string          `json:"why"`
		} `json:"invalid"`
	} `json:"responses"`
}

func loadV2Fixtures(t *testing.T) *v2Fixtures {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "v2-contract-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f v2Fixtures
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return &f
}

func specWithPrediction(t *testing.T, raw json.RawMessage) autoscalerv1alpha1.PredictiveAutoscalerSpec {
	t.Helper()
	var spec autoscalerv1alpha1.PredictiveAutoscalerSpec
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &spec.Prediction); err != nil {
			t.Fatal(err)
		}
	}
	return spec
}

func fixtureSettings(t *testing.T, f *v2Fixtures, name string) v2Settings {
	t.Helper()
	for _, c := range f.Settings {
		if c.Name == name {
			s, err := resolveV2Settings(specWithPrediction(t, c.Prediction))
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
	}
	t.Fatalf("no settings case %s", name)
	return v2Settings{}
}

func TestV2FixtureConstantsMatch(t *testing.T) {
	f := loadV2Fixtures(t)
	if f.ForecastContract != forecastContractV2 || f.SettingsSchema != v2SettingsSchema || f.EvidenceSchema != v2EvidenceSchema || f.Unit != v2Unit {
		t.Fatalf("contract constants differ: %+v", f)
	}
	for _, pair := range [][2]interface{}{{f.AllowedIntervals, v2AllowedIntervals}, {f.Phases, v2Phases}, {f.Reasons, v2Reasons},
		{f.ModelFamilies, v2ModelFamilies}} {
		if !reflect.DeepEqual(pair[0], pair[1]) {
			t.Fatalf("enumeration differs from contracts.py: %v vs %v", pair[0], pair[1])
		}
	}
}

func TestV2SettingsFingerprintMatchesPython(t *testing.T) {
	f := loadV2Fixtures(t)
	if len(f.Settings) < 20 {
		t.Fatalf("too few settings cases: %d", len(f.Settings))
	}
	for _, c := range f.Settings {
		s, err := resolveV2Settings(specWithPrediction(t, c.Prediction))
		if c.Refused != nil {
			if err == nil {
				t.Errorf("%s: Python refuses (%s), Go accepts", c.Name, *c.Refused)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: Go refuses what Python accepts: %v", c.Name, err)
			continue
		}
		got, _ := v2CanonicalJSON(s.canonical())
		if string(got) != c.Canonical {
			t.Errorf("%s: canonical settings differ\n go %s\n py %s", c.Name, got, c.Canonical)
		}
		if s.fingerprint() != c.Fingerprint || s.HorizonSeconds/s.SampleIntervalSeconds != c.Steps {
			t.Errorf("%s: fingerprint %s, want %s", c.Name, s.fingerprint(), c.Fingerprint)
		}
	}
}

func TestV2EvidenceKeyMatchesPython(t *testing.T) {
	f := loadV2Fixtures(t)
	for _, c := range f.Evidence {
		s := fixtureSettings(t, f, c.SettingsCase)
		if got := v2EvidenceKey(c.Signal.AutoscalerUID, c.Signal.TargetUID, c.Signal.QuerySHA256, s, c.LeadSeconds); got != c.EvidenceKey {
			t.Errorf("%s lead %d: evidence key %s, want %s", c.SettingsCase, c.LeadSeconds, got, c.EvidenceKey)
		}
	}
}

func TestV2LeadAndWindowMatchPython(t *testing.T) {
	f := loadV2Fixtures(t)
	for _, c := range f.Lead.Automatic {
		if got := v2AutomaticLead(c.Startup, c.DecisionInterval, c.Allowance, c.Interval); got != c.Lead {
			t.Errorf("automatic lead %+v: got %d", c, got)
		}
	}
	s := fixtureSettings(t, f, "statistical")
	for _, c := range f.Lead.Resolve {
		if c.Name == "missing-key" {
			continue // the Go type always carries every key (TestV2PublishedLeadCarriesEveryKey)
		}
		raw, _ := json.Marshal(c.Lead)
		var l autoscalerv1alpha1.PredictionLead
		if err := json.Unmarshal(raw, &l); err != nil {
			t.Fatal(err)
		}
		if got := v2LeadProblem(&l, s, 3) == ""; got != c.Valid {
			t.Errorf("lead %s: Go valid=%v, Python valid=%v (%s)", c.Name, got, c.Valid, v2LeadProblem(&l, s, 3))
		}
	}
	for _, c := range f.Lead.Windows {
		origin, err := v2ParseTS(c.Origin, "origin")
		if err != nil {
			t.Fatal(err)
		}
		w := v2Window{origin, c.Interval, c.Lead}
		var targets []string
		for _, x := range w.targets() {
			targets = append(targets, v2FormatTS(x))
		}
		if !reflect.DeepEqual(targets, c.Targets) || v2FormatTS(w.validUntil(c.Startup, c.Allowance)) != c.ValidUntil {
			t.Errorf("window %+v: targets %v valid until %s", c, targets, v2FormatTS(w.validUntil(c.Startup, c.Allowance)))
		}
	}
}

func TestV2PublishedLeadCarriesEveryKey(t *testing.T) {
	raw, _ := json.Marshal(autoscalerv1alpha1.PredictionLead{Method: "configured"}) // zeros included
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	want := []string{"actuationAllowanceSeconds", "method", "observedGeneration", "sampleCount", "seconds", "startupDelaySeconds"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("status.prediction.lead keys %v, want exactly %v (contracts.resolve_lead)", keys, want)
	}
}

func fixtureBinding(t *testing.T, f *v2Fixtures) v2Binding {
	b := f.Responses.Binding
	s := fixtureSettings(t, f, "statistical")
	if s.fingerprint() != b.SettingsSHA256 || s.MiscoverageBps != b.NominalMiscoverageBps || s.SampleIntervalSeconds != b.SampleIntervalSeconds {
		t.Fatal("the fixture binding's settings differ")
	}
	out := v2Binding{AutoscalerNamespace: b.AutoscalerNamespace, AutoscalerName: b.AutoscalerName,
		AutoscalerUID: b.AutoscalerUID, Generation: b.AutoscalerGeneration, TargetNamespace: b.TargetNamespace,
		TargetName: b.TargetName, TargetUID: b.TargetUID, QuerySHA: b.MetricQuerySHA256, Settings: s,
		SettingsSHA: b.SettingsSHA256, Origin: b.DecisionOrigin, Lead: b.Lead}
	out.EvidenceKey = v2EvidenceKey(out.AutoscalerUID, out.TargetUID, out.QuerySHA, s, int64(b.Lead.Seconds))
	if out.EvidenceKey != b.EvidenceKey {
		t.Fatalf("evidence key %s, Python %s", out.EvidenceKey, b.EvidenceKey)
	}
	return out
}

func TestV2RequestMatchesPython(t *testing.T) {
	f := loadV2Fixtures(t)
	raw, _ := json.Marshal(fixtureBinding(t, f).request())
	var got map[string]interface{}
	_ = json.Unmarshal(raw, &got)
	if !reflect.DeepEqual(got, f.Responses.Request) {
		t.Fatalf("request\n go %v\n py %v", got, f.Responses.Request)
	}
}

func TestV2ResponsesDecodeExactlyAsPython(t *testing.T) {
	f := loadV2Fixtures(t)
	b := fixtureBinding(t, f)
	for _, c := range f.Responses.Valid {
		r, err := decodeV2Response(c.Wire)
		if err != nil {
			t.Errorf("%s: Go refuses a response Python accepts: %v", c.Name, err)
			continue
		}
		if err := r.bound(b); err != nil {
			t.Errorf("%s: Go refuses the binding Python accepts: %v", c.Name, err)
		}
	}
	if len(f.Responses.Invalid) < 30 {
		t.Fatalf("too few invalid cases: %d", len(f.Responses.Invalid))
	}
	for _, c := range f.Responses.Invalid {
		r, err := decodeV2Response(c.Wire)
		switch c.Stage {
		case "decode":
			if err == nil {
				t.Errorf("%s: Go accepts a response Python refuses (%s)", c.Name, c.Why)
			}
		case "bound":
			if err != nil {
				t.Errorf("%s: Go refuses at decode a response Python decodes: %v", c.Name, err)
			} else if r.bound(b) == nil {
				t.Errorf("%s: Go accepts a binding Python refuses (%s)", c.Name, c.Why)
			}
		default:
			t.Fatalf("%s: unknown stage %s", c.Name, c.Stage)
		}
	}
}

// The operator's own verdict: independent of the service's boolean, on its own clock, never past the deadline.
func TestV2VerdictIsTheOperatorsOwn(t *testing.T) {
	f := loadV2Fixtures(t)
	s := fixtureSettings(t, f, "statistical")
	decode := func(name string) *v2Response {
		for _, c := range f.Responses.Valid {
			if c.Name == name {
				r, err := decodeV2Response(c.Wire)
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
		}
		t.Fatalf("no valid case %s", name)
		return nil
	}
	origin := f.Responses.Binding.DecisionOrigin
	at := func(sec int64) time.Time { return time.Unix(origin, 0).Add(time.Duration(sec) * time.Second) }
	eligible := decode("eligible") // valid until origin + 270 (Δ 300, A 30)
	for _, c := range []struct {
		name   string
		r      func() *v2Response
		now    time.Time
		ok     bool
		reason string
	}{
		{"eligible now", func() *v2Response { return eligible }, at(10), true, "Ready"},
		{"just before the deadline", func() *v2Response { return eligible }, at(269), true, "Ready"},
		{"at the deadline", func() *v2Response { return eligible }, at(270), false, "DeadlinePassed"},
		{"next origin: never slid forward", func() *v2Response { return eligible }, at(310), false, "DeadlinePassed"},
		{"issued ahead of the operator's clock", func() *v2Response { return eligible }, at(1), false, "IssuedInFuture"},
		{"shadow", func() *v2Response { return decode("shadow") }, at(10), false, "ShadowInsufficient"},
		{"too few calibration windows", func() *v2Response { r := *eligible; r.CalibrationWindows = 127; return &r }, at(10), false, "EvidenceInsufficient"},
		{"too few prospective windows", func() *v2Response { r := *eligible; r.ProspectiveWindows = 63; return &r }, at(10), false, "EvidenceInsufficient"},
		{"no evidence cutoff", func() *v2Response { r := *eligible; r.EvidenceCutoff = nil; return &r }, at(10), false, "EvidenceStale"},
		{"stale evidence", func() *v2Response {
			r := *eligible
			old := origin - 3*3600
			r.EvidenceCutoff = &old
			return &r
		}, at(10), false, "EvidenceStale"},
	} {
		v := c.r().verdict(s, c.now)
		if v.Eligible != c.ok || v.Reason != c.reason {
			t.Errorf("%s: %+v, want eligible=%v reason %s", c.name, v, c.ok, c.reason)
		}
	}
}

func TestV2PredictiveReplicasCap(t *testing.T) {
	for _, c := range []struct {
		name                  string
		upper, capRPM         float64
		reactive, min         int32
		maxExtra, maxPercent  int64
		uncapped, allow, capd int32
		binding               bool
	}{
		{"below the cap", 30, 600, 2, 1, 2, 50, 3, 1, 3, false},  // ceil(30/10)=3, E=min(2, ceil(50%*2)=1)=1, R+E=3
		{"cap binds", 100, 600, 5, 1, 2, 50, 10, 2, 7, true},     // E=min(2, ceil(2.5)=3)=2
		{"percent binds", 100, 600, 2, 1, 5, 50, 10, 1, 3, true}, // E=min(5, 1)=1
		{"minReplicas is the percent base", 100, 600, 0, 4, 5, 50, 10, 2, 2, true},
		{"zero cap disables excess", 100, 600, 3, 1, 0, 50, 10, 0, 3, true},
		{"zero percent disables excess", 100, 600, 3, 1, 5, 0, 10, 0, 3, true},
		{"fractional capacity 12.5 rps", 37.5, 750, 1, 1, 10, 1000, 3, 10, 3, false},
		{"just above a multiple", 37.51, 750, 1, 1, 10, 1000, 4, 10, 4, false},
		{"forecast below the reactive need", 5, 600, 4, 1, 2, 50, 1, 2, 1, false},
		{"zero forecast", 0, 600, 4, 1, 2, 50, 0, 2, 0, false},
	} {
		p := v2PredictiveReplicas(c.upper, c.capRPM, c.reactive, c.min, c.maxExtra, c.maxPercent)
		if p.Uncapped != c.uncapped || p.Allowance != c.allow || p.Capped != c.capd || p.Binding != c.binding {
			t.Errorf("%s: %+v", c.name, p)
		}
	}
}

func TestV2SelectionNeedsTheStatisticalEngine(t *testing.T) {
	off := false
	for _, c := range []struct {
		pred autoscalerv1alpha1.PredictionConfig
		want bool
	}{
		{autoscalerv1alpha1.PredictionConfig{}, false},
		{autoscalerv1alpha1.PredictionConfig{ForecastV2Settings: autoscalerv1alpha1.ForecastV2Settings{Engine: "legacyLstm"}}, false},
		{autoscalerv1alpha1.PredictionConfig{ForecastV2Settings: autoscalerv1alpha1.ForecastV2Settings{Engine: "statistical"}}, true},
		{autoscalerv1alpha1.PredictionConfig{Enabled: &off, ForecastV2Settings: autoscalerv1alpha1.ForecastV2Settings{Engine: "statistical"}}, false},
	} {
		a := &autoscalerv1alpha1.PredictiveAutoscaler{Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{Prediction: c.pred}}
		if v2Selected(a) != c.want {
			t.Errorf("%+v: selected=%v", c.pred, !c.want)
		}
	}
}

// A grid-aligned but huge lead must be refused before anything is allocated (Codex #26: makeslice panic).
func TestV2HugeLeadIsRefusedWithoutAllocating(t *testing.T) {
	f := loadV2Fixtures(t)
	var wire map[string]interface{}
	if err := json.Unmarshal(f.Responses.Valid[0].Wire, &wire); err != nil {
		t.Fatal(err)
	}
	for _, lead := range []float64{300 * 1e12, 3900, 300 * 61} {
		wire["lead_seconds"] = lead
		raw, _ := json.Marshal(wire)
		if _, err := decodeV2Response(raw); err == nil || !strings.Contains(err.Error(), "refused") {
			t.Fatalf("lead %v: %v", lead, err)
		}
	}
	wire["lead_seconds"] = 1200
	wire["target_timestamps"] = make([]string, 100000)
	raw, _ := json.Marshal(wire)
	if _, err := decodeV2Response(raw); err == nil {
		t.Fatal("a huge target list must be refused")
	}
}
