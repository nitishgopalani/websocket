package media_test

import (
	"context"
	"testing"
	"time"

	"websocket/internal/media"
)

type recordingTurnListener struct {
	events []media.TurnEvent
}

func (l *recordingTurnListener) OnTurnEvent(_ context.Context, _ *media.Session, event media.TurnEvent) {
	l.events = append(l.events, event)
}

func defaultTurnTestConfig() media.EndpointConfig {
	return media.EndpointConfig{
		SilenceMs: map[media.FlowClass]int{
			media.FlowYesNo:        400,
			media.FlowDefault:      600,
			media.FlowSpelledInput: 1200,
		},
		DefaultSilenceMs: 600,
		MaxUtteranceMs:   5000,
	}
}

func TestTurnManagerEndOfTurnAfterSilence(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	session := &media.Session{StreamSID: "MZ-T1"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnPartial(ctx, session, media.Transcript{Text: "hello"})
	tm.OnPartial(ctx, session, media.Transcript{Text: "hello there"})
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "hello there", IsFinal: true})

	clock.Advance(599 * time.Millisecond)
	if len(listener.events) != 1 || listener.events[0].Kind != media.TurnSpeechStarted {
		t.Fatalf("before silence expiry events = %+v", listener.events)
	}

	clock.Advance(2 * time.Millisecond)
	endEvents := filterTurnKind(listener.events, media.TurnEndOfTurn)
	if len(endEvents) != 1 {
		t.Fatalf("EndOfTurn events = %d, want 1", len(endEvents))
	}
	if endEvents[0].Transcript != "hello there" {
		t.Fatalf("transcript = %q", endEvents[0].Transcript)
	}
}

func TestTurnManagerPerFlowThresholds(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	session := &media.Session{StreamSID: "MZ-FLOW"}
	ctx := context.Background()

	tm.SetFlowClass(session, media.FlowYesNo)
	tm.OnSpeechStart(ctx, session)
	tm.OnPartial(ctx, session, media.Transcript{Text: "haan"})
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "haan", IsFinal: true})
	clock.Advance(450 * time.Millisecond)

	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("YesNo flow should end turn after 400ms silence")
	}

	listener.events = nil
	clock2 := media.NewFakeClock(time.Now())
	tm2 := media.NewTurnManager(listener, cfg, clock2, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	tm2.SetFlowClass(session, media.FlowSpelledInput)
	tm2.OnSpeechStart(ctx, session)
	tm2.OnPartial(ctx, session, media.Transcript{Text: "one two"})
	tm2.OnSpeechEnd(ctx, session)
	clock2.Advance(800 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("SpelledInput should not end turn at 800ms pause")
	}
	clock2.Advance(500 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("SpelledInput should not end turn at 1300ms without final")
	}
	tm2.OnFinal(ctx, session, media.Transcript{Text: "one two three", IsFinal: true})
	clock2.Advance(1200 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("SpelledInput should end turn after 1200ms silence post-final")
	}
}

func TestTurnManagerInterruptWhenAgentSpeaking(t *testing.T) {
	listener := &recordingTurnListener{}
	tm := media.NewTurnManager(listener, defaultTurnTestConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	session := &media.Session{StreamSID: "MZ-INT"}
	ctx := context.Background()

	tm.SetAgentSpeaking(session, true)
	tm.OnSpeechStart(ctx, session)

	interrupts := filterTurnKind(listener.events, media.TurnInterrupt)
	if len(interrupts) != 1 {
		t.Fatalf("interrupts = %d, want 1", len(interrupts))
	}
}

func TestTurnManagerMaxUtteranceForcesEndOfTurn(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	cfg.MaxUtteranceMs = 1000
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	session := &media.Session{StreamSID: "MZ-MAX"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	for i := 0; i < 5; i++ {
		tm.OnPartial(ctx, session, media.Transcript{Text: "still talking"})
		clock.Advance(100 * time.Millisecond)
	}
	clock.Advance(600 * time.Millisecond)

	ends := filterTurnKind(listener.events, media.TurnEndOfTurn)
	if len(ends) != 1 {
		t.Fatalf("forced EndOfTurn count = %d, want 1", len(ends))
	}
	if !ends[0].Forced {
		t.Fatal("expected forced EndOfTurn")
	}
}

func TestTurnManagerNoDoubleEndOfTurn(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	tm := media.NewTurnManager(listener, defaultTurnTestConfig(), clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	session := &media.Session{StreamSID: "MZ-ONCE"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnPartial(ctx, session, media.Transcript{Text: "done"})
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "done", IsFinal: true})
	clock.Advance(700 * time.Millisecond)
	clock.Advance(700 * time.Millisecond)

	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatalf("events = %+v", listener.events)
	}
}

func TestTurnManagerPartialResetsSilenceTimer(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	session := &media.Session{StreamSID: "MZ-RESET"}
	ctx := context.Background()

	tm.SetFlowClass(session, media.FlowYesNo)
	tm.OnSpeechStart(ctx, session)
	tm.OnSpeechEnd(ctx, session)
	clock.Advance(300 * time.Millisecond)
	tm.OnPartial(ctx, session, media.Transcript{Text: "more"})
	tm.OnFinal(ctx, session, media.Transcript{Text: "more", IsFinal: true})
	clock.Advance(350 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("partial should reset silence timer; turn not ended yet")
	}
	clock.Advance(100 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("expected turn after full yes_no silence from last partial")
	}
}

func TestEnergyVADDetectsSpeech(t *testing.T) {
	vad := media.NewEnergyVAD(500)
	loud := make([]byte, 320)
	for i := 0; i < len(loud)/2; i++ {
		loud[i*2] = 0xFF
		loud[i*2+1] = 0x7F
	}
	if !vad.IsSpeech(loud, 8000) {
		t.Fatal("expected energy VAD to detect loud frame")
	}
	silent := make([]byte, 320)
	if vad.IsSpeech(silent, 8000) {
		t.Fatal("expected silence")
	}
}

func TestTurnManagerShortFragmentUsesLongerSilence(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := media.EndpointConfig{
		SilenceMs: map[media.FlowClass]int{
			media.FlowDefault: 600,
		},
		DefaultSilenceMs:       600,
		ShortFragmentSilenceMs: 950,
		ShortFragmentMaxWords:  2,
		MaxUtteranceMs:         5000,
	}
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	session := &media.Session{StreamSID: "MZ-SHORT"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnPartial(ctx, session, media.Transcript{Text: "Bhakti"})
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "Bhakti", IsFinal: true})

	clock.Advance(850 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("short fragment should not end turn at 850ms (needs 950ms)")
	}
	clock.Advance(120 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("short fragment should end turn after 950ms silence")
	}
}

func filterTurnKind(events []media.TurnEvent, kind media.TurnKind) []media.TurnEvent {
	var out []media.TurnEvent
	for _, e := range events {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func TestTurnManagerIsTranscriptConsumer(t *testing.T) {
	var _ media.TranscriptConsumer = media.NewTurnManager(nil, media.DefaultEndpointConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
}
