package translator_test

import (
	"testing"

	"websocket/internal/translator"
)

func TestFillerLexiconPureFiller(t *testing.T) {
	lex := translator.NewFillerLexicon("en", "")

	if !lex.IsPureFiller("en", "hmm") {
		t.Fatal("expected hmm suppressed")
	}
	if !lex.IsPureFiller("en", "uh um") {
		t.Fatal("expected multi-token filler suppressed")
	}
	if lex.IsPureFiller("en", "hello") {
		t.Fatal("hello must not be suppressed")
	}
	if lex.IsPureFiller("en", "yeah, I am here") {
		t.Fatal("filler+content must not be pure filler")
	}
}

func TestFillerLexiconHindi(t *testing.T) {
	lex := translator.NewFillerLexicon("hi", "")

	if !lex.IsPureFiller("hi", "हम्म") {
		t.Fatal("expected hindi hmm suppressed")
	}
	if !lex.IsPureFiller("hi", "ठीक है") {
		t.Fatal("expected ठीक है suppressed")
	}
	if lex.IsPureFiller("hi", "हाँ, मैं आ रहा हूँ") {
		t.Fatal("filler+content hindi must translate")
	}
}

func TestTurnAuditInvariant(t *testing.T) {
	a := translator.NewTurnAudit("a2b", nil)
	a.RecordASRFinal()
	a.RecordASRFinal()
	a.RecordMerged()
	a.RecordTranslation()

	if a.ASRFinalsReceived.Load() != 2 {
		t.Fatalf("received=%d", a.ASRFinalsReceived.Load())
	}
	accounted := a.TranslationsEmitted.Load() + a.FillersSuppressed.Load() + a.MergedTurns.Load()
	if a.ASRFinalsReceived.Load() != accounted {
		t.Fatalf("invariant failed: received=%d accounted=%d", a.ASRFinalsReceived.Load(), accounted)
	}
}
