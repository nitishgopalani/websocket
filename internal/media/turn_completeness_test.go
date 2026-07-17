package media_test

import (
	"testing"

	"websocket/internal/media"
)

func TestTextIsCompleteEnglish(t *testing.T) {
	if !media.TextIsComplete("Can you hear me?", "en") {
		t.Fatal("expected complete sentence")
	}
	if media.TextIsComplete("Can you and", "en") {
		t.Fatal("connector ending should be incomplete")
	}
	if media.TextIsComplete("because", "en") {
		t.Fatal("single connector token should be incomplete")
	}
	if media.TextIsComplete("Hmm", "en", "hmm") {
		t.Fatal("single filler should be incomplete (<2 content words)")
	}
}

func TestTextIsCompleteHindi(t *testing.T) {
	if !media.TextIsComplete("मैं आ रहा हूँ", "hi") {
		t.Fatal("expected complete Hindi sentence")
	}
	if media.TextIsComplete("और", "hi") {
		t.Fatal("hindi connector alone should be incomplete")
	}
}

func TestEndsWithConnector(t *testing.T) {
	cases := []struct {
		text, lang string
		want       bool
	}{
		{"I think and", "en", true},
		{"hello there", "en", false},
		{"कुछ और", "hi", true},
	}
	for _, tc := range cases {
		got := !media.TextIsComplete(tc.text, tc.lang)
		if tc.want && !got {
			t.Fatalf("%q (%s): want incomplete", tc.text, tc.lang)
		}
		if !tc.want && got {
			t.Fatalf("%q (%s): want complete", tc.text, tc.lang)
		}
	}
}
