package translator

import (
	"testing"
	"time"
)

func TestConfigFromEnv_bidirectionalDefaults(t *testing.T) {
	t.Setenv("TRANSLATOR_BIDIRECTIONAL", "true")
	t.Setenv("TRANSLATOR_ECHO_TAIL_MS", "500")
	t.Setenv("TRANSLATOR_LOOP_BREAKER_MAX", "8")
	cfg := ConfigFromEnv()
	if !cfg.Bidirectional {
		t.Fatal("expected bidirectional")
	}
	if cfg.EchoTail != 500*time.Millisecond {
		t.Fatalf("echo tail=%v", cfg.EchoTail)
	}
	if cfg.LoopBreakerMax != 8 {
		t.Fatalf("loop max=%d", cfg.LoopBreakerMax)
	}
}

func TestParseLaneMode(t *testing.T) {
	cases := map[string]LaneMode{
		"translate":  LaneModeTranslate,
		"echo":       LaneModeEcho,
		"ECHO":       LaneModeEcho,
		"hindi_echo": LaneModeEcho,
		"":           LaneModeTranslate,
		"unknown":    LaneModeTranslate,
	}
	for in, want := range cases {
		if got := parseLaneMode(in); got != want {
			t.Fatalf("parseLaneMode(%q)=%q want %q", in, got, want)
		}
	}
}
