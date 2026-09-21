package controllers

import (
	"encoding/json"
	"testing"
)

// C-85: a null mape must decode as "not measured", never as a perfect 0.0.
func TestMAPEProvenanceDecodesNullAsNotMeasured(t *testing.T) {
	var resp MLPredictionResponse
	body := []byte(`{"predictions":[1,2,3,4,5,6],"confidence":0.7,"mape":null,"mape_measured":false,"mape_scored":0}`)
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.MAPEMeasured {
		t.Fatalf("null mape decoded as measured")
	}
	if resp.MAPEScored != 0 {
		t.Fatalf("scored = %d, want 0", resp.MAPEScored)
	}
	if resp.MAPE != 0 {
		t.Fatalf("Go leaves a null float64 at 0 -- that is why MAPEMeasured exists; got %v", resp.MAPE)
	}
	body = []byte(`{"predictions":[1,2,3,4,5,6],"confidence":0.7,"mape":12.5,"mape_measured":true,"mape_scored":7}`)
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.MAPEMeasured || resp.MAPEScored != 7 || resp.MAPE != 12.5 {
		t.Fatalf("measured payload decoded wrong: %+v", resp)
	}
}
