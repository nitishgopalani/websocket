package media

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/gorilla/websocket"
)

// TestIsCreditAuthClose verifies the credit/auth-class classifier used by the
// ASR + TTS fallback swap. These are the close frames Sarvam emits when the
// account is out of credits or the key is rejected.
func TestIsCreditAuthClose(t *testing.T) {
	cases := []struct {
		name        string
		closeCode   int
		closeReason string
		err         error
		want        bool
	}{
		{"1003 credits exhausted", 1003, "Credits exhausted. Visit the API Dashboard", nil, true},
		{"1000 insufficient credits (reason match)", 1000, "Insufficient credits", nil, true},
		{"1000 normal closure no reason match", 1000, "", nil, false},
		{"4001 policy violation", 4001, "auth", nil, true},
		{"4401 auth range", 4401, "", nil, true},
		{"dial 401 unauthorized", 0, "", errors.New("websocket: bad handshake: 401 Unauthorized"), true},
		{"dial 403 forbidden", 0, "", errors.New("websocket: bad handshake: 403 Forbidden"), true},
		{"dial subscription invalid", 0, "", errors.New("invalid api-subscription-key"), true},
		{"dial network timeout (not credit/auth)", 0, "", errors.New("dial tcp: i/o timeout"), false},
		{"normal going away", websocket.CloseGoingAway, "", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isCreditAuthClose(c.closeCode, c.closeReason, c.err)
			if got != c.want {
				t.Fatalf("isCreditAuthClose(%d, %q, %v) = %v, want %v", c.closeCode, c.closeReason, c.err, got, c.want)
			}
		})
	}
}

// TestASRSwapToFallbackOnCreditClose verifies that an ASR session served by a
// provider with a configured fallback key swaps s.apiKey to the fallback and
// resets the reconnect budget when it sees a credit/auth-class close frame.
func TestASRSwapToFallbackOnCreditClose(t *testing.T) {
	provider := NewSarvamASRProvider("primary-key", "fallback-key", SarvamConfig{
		Endpoint: defaultSarvamEndpoint,
	}, nil)
	s := &sarvamSession{
		provider:       provider,
		apiKey:         provider.apiKey,
		apiKeyFallback: provider.apiKeyFallback,
		keyUsed:        "primary",
		logger:         provider.logger,
	}

	// Simulate Sarvam closing with 1003 "Credits exhausted".
	creditErr := errors.New("websocket: close 1003 (Credits exhausted): ")
	swapped := s.maybeSwapToFallbackLocked(creditErr, 1003, "Credits exhausted")
	if !swapped {
		t.Fatal("expected swap to fallback on 1003 credit close, got no swap")
	}
	if s.apiKey != "fallback-key" {
		t.Fatalf("after swap, apiKey = %q, want %q", s.apiKey, "fallback-key")
	}
	if s.keyUsed != "fallback" {
		t.Fatalf("after swap, keyUsed = %q, want %q", s.keyUsed, "fallback")
	}

	// A second credit close must NOT swap back / loop (one-time only).
	swapped2 := s.maybeSwapToFallbackLocked(creditErr, 1003, "Credits exhausted")
	if swapped2 {
		t.Fatal("second credit close must not swap again (one-time only)")
	}
	if s.apiKey != "fallback-key" {
		t.Fatalf("after second close, apiKey = %q, want %q", s.apiKey, "fallback-key")
	}
}

// TestASRNoSwapWithoutFallback verifies that a credit close on a session with
// NO fallback configured does not swap (and does not panic).
func TestASRNoSwapWithoutFallback(t *testing.T) {
	provider := NewSarvamASRProvider("primary-key", "", SarvamConfig{
		Endpoint: defaultSarvamEndpoint,
	}, nil)
	s := &sarvamSession{
		provider:       provider,
		apiKey:         provider.apiKey,
		apiKeyFallback: provider.apiKeyFallback,
		keyUsed:        "primary",
		logger:         provider.logger,
	}
	swapped := s.maybeSwapToFallbackLocked(errors.New("close 1003"), 1003, "Credits exhausted")
	if swapped {
		t.Fatal("expected no swap when fallback is empty")
	}
	if s.apiKey != "primary-key" {
		t.Fatalf("apiKey = %q, want %q", s.apiKey, "primary-key")
	}
}

// TestASRNoSwapOnNormalClose verifies that a normal (non-credit) close does not
// trigger the fallback swap — the fallback is reserved for credit/auth-class
// failures only.
func TestASRNoSwapOnNormalClose(t *testing.T) {
	provider := NewSarvamASRProvider("primary-key", "fallback-key", SarvamConfig{
		Endpoint: defaultSarvamEndpoint,
	}, nil)
	s := &sarvamSession{
		provider:       provider,
		apiKey:         provider.apiKey,
		apiKeyFallback: provider.apiKeyFallback,
		keyUsed:        "primary",
		logger:         provider.logger,
	}
	normalErr := errors.New("websocket: close 1000 (normal): ")
	swapped := s.maybeSwapToFallbackLocked(normalErr, websocket.CloseNormalClosure, "")
	if swapped {
		t.Fatal("normal close must not trigger fallback swap")
	}
	if s.keyUsed != "primary" {
		t.Fatalf("keyUsed = %q, want %q", s.keyUsed, "primary")
	}
}

// TestTTSSwapToFallbackOnCreditClose mirrors the ASR test for the TTS-WS stream.
func TestTTSSwapToFallbackOnCreditClose(t *testing.T) {
	provider := &SarvamTTSProvider{
		apiKey:         "primary-key",
		apiKeyFallback: "fallback-key",
		baseURL:        defaultSarvamTTSBaseURL,
		model:          defaultSarvamTTSModel,
		speaker:        defaultSarvamTTSSpeaker,
		lang:           defaultSarvamTTSLang,
		logger:         slog.Default(),
	}
	s := &sarvamTTSWSStream{
		provider:       provider,
		apiKey:         provider.apiKey,
		apiKeyFallback: provider.apiKeyFallback,
		keyUsed:        "primary",
		logger:         provider.logger,
	}

	creditErr := errors.New("websocket: close 1003 (Credits exhausted): ")
	swapped := s.maybeSwapToFallbackLocked(creditErr, 1003, "Credits exhausted")
	if !swapped {
		t.Fatal("expected TTS swap to fallback on 1003 credit close")
	}
	if s.apiKey != "fallback-key" {
		t.Fatalf("after swap, apiKey = %q, want %q", s.apiKey, "fallback-key")
	}
	if s.keyUsed != "fallback" {
		t.Fatalf("after swap, keyUsed = %q, want %q", s.keyUsed, "fallback")
	}

	// One-time only.
	swapped2 := s.maybeSwapToFallbackLocked(creditErr, 1003, "Credits exhausted")
	if swapped2 {
		t.Fatal("second TTS credit close must not swap again")
	}
}

// TestTTSNoSwapOnNormalClose verifies the TTS swap is reserved for
// credit/auth-class closes only.
func TestTTSNoSwapOnNormalClose(t *testing.T) {
	provider := &SarvamTTSProvider{
		apiKey:         "primary-key",
		apiKeyFallback: "fallback-key",
		logger:         slog.Default(),
	}
	s := &sarvamTTSWSStream{
		provider:       provider,
		apiKey:         provider.apiKey,
		apiKeyFallback: provider.apiKeyFallback,
		keyUsed:        "primary",
		logger:         provider.logger,
	}
	normalErr := errors.New("websocket: close 1000 (normal): ")
	swapped := s.maybeSwapToFallbackLocked(normalErr, websocket.CloseNormalClosure, "")
	if swapped {
		t.Fatal("normal TTS close must not trigger fallback swap")
	}
}
