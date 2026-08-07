package media

import (
	"encoding/json"
	"strings"
	"testing"
)

func float64ptr(v float64) *float64 { return &v }

func TestBuildSarvamTTSRequest_v3NeverEmitsPitchLoudnessEvenIfSet(t *testing.T) {
	// D-4 addendum B: bulbul:v3 returns HTTP 400 if pitch/loudness keys are present.
	// Builder must drop them even when a caller supplies values on opts.
	req := BuildSarvamTTSRequest(
		"नमस्ते", "hi-IN", "neha", "bulbul:v3", 8000,
		SarvamTTSBuildOpts{
			Pitch:    float64ptr(-0.75),
			Loudness: float64ptr(0.75),
			Pace:     float64ptr(1.0),
		},
	)
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"pitch", "loudness"} {
		if _, ok := m[banned]; ok {
			t.Fatalf("v3 body must not include %q (got %s)", banned, raw)
		}
	}
	if _, ok := m["pace"]; !ok {
		t.Fatalf("pace=1.0 should be present: %s", raw)
	}
}

func TestBuildSarvamTTSRequest_v3PaceClampAndOmitWhenUnset(t *testing.T) {
	// D-4 addendum C: pace=2.5 → 400 from API; builder clamps to 2.0 on v3.
	req := BuildSarvamTTSRequest(
		"hi", "hi-IN", "neha", "bulbul:v3", 8000,
		SarvamTTSBuildOpts{Pace: float64ptr(2.5)},
	)
	if req.Pace == nil || *req.Pace != 2.0 {
		t.Fatalf("pace=2.5 on v3 want clamp 2.0, got %v", req.Pace)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["pace"] != 2.0 {
		t.Fatalf("JSON pace want 2.0, got %v (%s)", m["pace"], raw)
	}

	// pace unset → key absent (requestPCM path).
	req = BuildSarvamTTSRequest("hi", "hi-IN", "neha", "bulbul:v3", 8000)
	if req.Pace != nil {
		t.Fatalf("unexpected pace when unset: %v", req.Pace)
	}
	raw, _ = json.Marshal(req)
	if strings.Contains(string(raw), "pace") {
		t.Fatalf("pace must be omitempty when unset: %s", raw)
	}
	var m2 map[string]any
	if err := json.Unmarshal(raw, &m2); err != nil {
		t.Fatal(err)
	}
	if _, ok := m2["pace"]; ok {
		t.Fatalf("pace key present when unset: %s", raw)
	}
}

func TestBuildSarvamTTSRequest_baselineFields(t *testing.T) {
	req := BuildSarvamTTSRequest("नमस्ते", "hi-IN", "neha", "bulbul:v3", 8000)
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["speaker"] != "neha" || m["model"] != "bulbul:v3" {
		t.Fatalf("speaker/model = %v / %v", m["speaker"], m["model"])
	}
	if m["enable_preprocessing"] != true {
		t.Fatalf("enable_preprocessing = %v", m["enable_preprocessing"])
	}
	if int(m["speech_sample_rate"].(float64)) != 8000 {
		t.Fatalf("speech_sample_rate = %v", m["speech_sample_rate"])
	}
}

func TestSarvamTTSStream_SetTurnVoiceOverridesProviderDefaults(t *testing.T) {
	p := &SarvamTTSProvider{
		speaker: defaultSarvamTTSSpeaker,
		model:   defaultSarvamTTSModel,
		lang:    defaultSarvamTTSLang,
	}
	s := &sarvamTTSStream{
		provider:  p,
		turnVoice: make(map[string]sarvamTurnVoice),
	}
	sp, mod, pace := s.resolveVoice("t1")
	if sp != defaultSarvamTTSSpeaker || mod != defaultSarvamTTSModel || pace != nil {
		t.Fatalf("defaults: %s / %s / %v", sp, mod, pace)
	}
	s.SetTurnVoice("t1", "neha", "bulbul:v3", float64ptr(0.9))
	sp, mod, pace = s.resolveVoice("t1")
	if sp != "neha" || mod != "bulbul:v3" || pace == nil || *pace != 0.9 {
		t.Fatalf("override: %s / %s / %v", sp, mod, pace)
	}
	sp, mod, pace = s.resolveVoice("t2")
	if sp != defaultSarvamTTSSpeaker || mod != defaultSarvamTTSModel || pace != nil {
		t.Fatalf("untouched turn: %s / %s / %v", sp, mod, pace)
	}
	s.SetTurnVoice("t2", "kabir", "", nil)
	sp, mod, pace = s.resolveVoice("t2")
	if sp != "kabir" || mod != defaultSarvamTTSModel || pace != nil {
		t.Fatalf("partial: %s / %s / %v", sp, mod, pace)
	}
}
