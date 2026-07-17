package media_test

import (
	"context"
	"testing"
	"time"

	"websocket/internal/media"
)

func translatorTurnConfig(silenceMs int) media.EndpointConfig {
	return media.EndpointConfig{
		SilenceMs: map[media.FlowClass]int{
			media.FlowDefault: silenceMs,
		},
		DefaultSilenceMs:       silenceMs,
		ShortFragmentSilenceMs: silenceMs,
		ShortFragmentMaxWords:  2,
		MaxUtteranceMs:         8000,
	}
}

func TestTurnManagerLateFinalOpensInstantTurn(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := translatorTurnConfig(300)
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	session := &media.Session{StreamSID: "late-final"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "Hmm", IsFinal: true})
	clock.Advance(300 * time.Millisecond)

	ends := filterTurnKind(listener.events, media.TurnEndOfTurn)
	if len(ends) != 1 || ends[0].Transcript != "Hmm" {
		t.Fatalf("first end_of_turn = %+v", ends)
	}

	before := len(listener.events)
	tm.OnFinal(ctx, session, media.Transcript{Text: "Can you say anything?", IsFinal: true})
	if len(listener.events) != before+1 {
		t.Fatalf("late final should emit immediately without clock advance, events=%+v", listener.events)
	}
	ends = filterTurnKind(listener.events, media.TurnEndOfTurn)
	if len(ends) != 2 {
		t.Fatalf("EndOfTurn count = %d, want 2", len(ends))
	}
	if ends[1].Transcript != "Can you say anything?" {
		t.Fatalf("late turn transcript = %q", ends[1].Transcript)
	}
}

func TestTurnManagerPureFillerDoesNotCloseTurn(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := translatorTurnConfig(300)
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	suppressed := 0
	tm.SetTurnPolicy(media.TurnPolicy{
		FillerSuppress: func(text string) bool {
			return text == "Hmm"
		},
		OnFillerSuppressed: func(string) { suppressed++ },
	}, nil)
	session := &media.Session{StreamSID: "filler"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "Hmm", IsFinal: true})
	clock.Advance(500 * time.Millisecond)

	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("pure filler must not close the turn")
	}
	if suppressed != 1 {
		t.Fatalf("suppressed = %d, want 1", suppressed)
	}

	tm.OnFinal(ctx, session, media.Transcript{Text: "Can you say anything?", IsFinal: true})
	clock.Advance(300 * time.Millisecond)
	ends := filterTurnKind(listener.events, media.TurnEndOfTurn)
	if len(ends) != 1 || ends[0].Transcript != "Can you say anything?" {
		t.Fatalf("expected real phrase end_of_turn, got %+v", ends)
	}
}

func TestTurnManagerAdaptiveCompleteClosesFast(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := translatorTurnConfig(300)
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	tm.SetTurnPolicy(media.TurnPolicy{
		CompletenessLang:  "en",
		IncompleteExtraMs: 300,
	}, nil)
	session := &media.Session{StreamSID: "complete-fast"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "Can you hear me?", IsFinal: true})
	clock.Advance(299 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("complete sentence should not close before 300ms")
	}
	clock.Advance(2 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("complete sentence should close at base 300ms silence")
	}
}

func TestTurnManagerAdaptiveIncompleteWaitsAndMerges(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := translatorTurnConfig(300)
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	merged := 0
	tm.SetTurnPolicy(media.TurnPolicy{
		CompletenessLang:  "en",
		IncompleteExtraMs: 300,
		OnTurnMerged:      func() { merged++ },
	}, nil)
	session := &media.Session{StreamSID: "merge"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "Can you and", IsFinal: true})
	clock.Advance(400 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("incomplete fragment should not close at 400ms (needs 600ms)")
	}

	tm.OnFinal(ctx, session, media.Transcript{Text: "say anything", IsFinal: true})
	if merged != 1 {
		t.Fatalf("merged = %d, want 1", merged)
	}
	clock.Advance(300 * time.Millisecond)
	ends := filterTurnKind(listener.events, media.TurnEndOfTurn)
	if len(ends) != 1 {
		t.Fatalf("expected single merged end_of_turn, got %d", len(ends))
	}
	if !containsAll(ends[0].Transcript, "Can you", "say anything") {
		t.Fatalf("merged transcript = %q", ends[0].Transcript)
	}
}

func TestTurnManagerAdaptiveIncompleteClosesAfterExtension(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := translatorTurnConfig(300)
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	tm.SetTurnPolicy(media.TurnPolicy{
		CompletenessLang:  "en",
		IncompleteExtraMs: 300,
	}, nil)
	session := &media.Session{StreamSID: "incomplete-alone"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "because", IsFinal: true})
	clock.Advance(500 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("dangling connector should not close at 500ms")
	}
	clock.Advance(150 * time.Millisecond)
	ends := filterTurnKind(listener.events, media.TurnEndOfTurn)
	if len(ends) != 1 || ends[0].Transcript != "because" {
		t.Fatalf("expected fragment translated after extension, got %+v", ends)
	}
}

func containsAll(text string, parts ...string) bool {
	for _, p := range parts {
		if !containsFold(text, p) {
			return false
		}
	}
	return true
}

func containsFold(hay, needle string) bool {
	return len(needle) == 0 || (len(hay) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(hay); i++ {
			if equalFold(hay[i:i+len(needle)], needle) {
				return true
			}
		}
		return false
	})())
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
