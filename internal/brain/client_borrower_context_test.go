package brain

import (
	"testing"

	"websocket/internal/media"
)

func TestBuildBorrowerContextUsesCallSIDFallback(t *testing.T) {
	session := &media.Session{
		StreamSID: "MZ-1",
		CallSID:   "+919810587857",
		Params: map[string]string{
			"language": "en-IN",
		},
	}
	ctx := buildBorrowerContext(session)
	if ctx == nil {
		t.Fatal("expected borrower context")
	}
	if ctx.Phone != "+919810587857" {
		t.Fatalf("phone = %q, want caller number from CallSID", ctx.Phone)
	}
}

func TestResolveBrainLocalePrefersLocaleParam(t *testing.T) {
	session := &media.Session{
		StreamSID: "MZ-1",
		Params: map[string]string{
			"locale":   "ta-IN",
			"language": "en-IN",
		},
	}
	if got := resolveBrainLocale(session); got != "ta-IN" {
		t.Fatalf("locale = %q, want ta-IN", got)
	}
}
