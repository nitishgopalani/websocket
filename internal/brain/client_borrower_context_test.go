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

func TestBuildBorrowerContextDerivesPhoneFromStreamSID(t *testing.T) {
	session := &media.Session{
		StreamSID: "73136989-60ad-abab-75e3-e69810587857",
		Params: map[string]string{
			"language": "hi-IN",
		},
	}
	ctx := buildBorrowerContext(session)
	if ctx == nil {
		t.Fatal("expected borrower context")
	}
	if ctx.Phone != "9810587857" {
		t.Fatalf("phone = %q, want 9810587857 derived from stream SID", ctx.Phone)
	}
}

func TestPhoneFromStreamSID(t *testing.T) {
	cases := map[string]string{
		"73136989-60ad-abab-75e3-e69810587857": "9810587857",
		"plain-9810587857":                     "9810587857",
		"no-phone-here-abcdef":                 "",
		"short-12345":                          "",
		"73136989-60ad-abab-75e3-deadbeefcafe": "",
	}
	for in, want := range cases {
		if got := phoneFromStreamSID(in); got != want {
			t.Fatalf("phoneFromStreamSID(%q) = %q, want %q", in, got, want)
		}
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
