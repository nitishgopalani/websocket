package media

import "testing"

func TestNormalizeSarvamLanguage(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"en", "en-IN"},
		{"EN", "en-IN"},
		{"en-IN", "en-IN"},
		{"hi", "hi-IN"},
		{"en-US", "en-IN"},
		{"", "en-IN"},
		{"unknown", "en-IN"},
		{"ta", "ta-IN"},
		{"doi", "doi-IN"},
		{"junk", "en-IN"},
	}
	for _, tc := range tests {
		got := NormalizeSarvamLanguage(tc.in, nil, "test")
		if got != tc.want {
			t.Fatalf("NormalizeSarvamLanguage(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestOutputSampleRateFromParams(t *testing.T) {
	if got := OutputSampleRateFromParams(map[string]string{"output_sample_rate": "16000"}); got != 16000 {
		t.Fatalf("got %d", got)
	}
	if got := OutputSampleRateFromParams(map[string]string{"language": "en"}); got != 0 {
		t.Fatalf("got %d", got)
	}
}

func TestElevenLabsPCMFormatForRate(t *testing.T) {
	if got := ElevenLabsPCMFormatForRate(16000); got != "pcm_16000" {
		t.Fatalf("got %q", got)
	}
	if got := ElevenLabsPCMFormatForRate(24000); got != "pcm_24000" {
		t.Fatalf("got %q", got)
	}
}

func TestResamplePCM16Linear24to16(t *testing.T) {
	in := make([]byte, 480*2) // 10ms @ 24k = 240 samples * 2 for simpler: use 4 samples
	for i := 0; i < len(in); i += 2 {
		in[i] = byte(i)
		in[i+1] = 0
	}
	out, err := ResamplePCM16Linear(in, 24000, 16000)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 || len(out)%2 != 0 {
		t.Fatalf("bad out len %d", len(out))
	}
}
