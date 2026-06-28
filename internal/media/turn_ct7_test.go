package media_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"websocket/internal/media"
)

type fakeSemanticTurn struct {
	pred  media.EOUPrediction
	err   error
	calls int
}

func (f *fakeSemanticTurn) Predict(_ context.Context, _ string, _ []byte, _ int) (media.EOUPrediction, error) {
	f.calls++
	if f.err != nil {
		return media.EOUPrediction{}, f.err
	}
	return f.pred, nil
}

func (f *fakeSemanticTurn) Close() error { return nil }

type fakeBackchannel struct {
	backchannel bool
	err         error
	calls       int
}

func (f *fakeBackchannel) IsBackchannel(_ context.Context, _ string, _ []byte, _ int) (bool, error) {
	f.calls++
	if f.err != nil {
		return false, f.err
	}
	return f.backchannel, nil
}

func (f *fakeBackchannel) Close() error { return nil }

func semanticEnabledCfg() media.SemanticTurnConfig {
	cfg := media.DefaultSemanticTurnConfig()
	cfg.Enabled = true
	cfg.CompleteSilenceMs = 250
	cfg.ConfidenceThreshold = 0.5
	return cfg
}

func TestTurnManagerSemanticCompleteShorterTimer(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	semantic := &fakeSemanticTurn{pred: media.EOUPrediction{Complete: true, Confidence: 0.9}}
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, semantic, semanticEnabledCfg(), media.NoopBackchannel{}, nil)
	session := &media.Session{StreamSID: "MZ-SEM-COMPLETE"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnPartial(ctx, session, media.Transcript{Text: "I want to pay my bill"})
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "I want to pay my bill", IsFinal: true})

	clock.Advance(249 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("EndOfTurn should not fire before semantic-complete timer")
	}
	clock.Advance(2 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("expected EndOfTurn at semantic-complete timer")
	}
}

func TestTurnManagerSemanticIncompleteHoldsUntilMaxUtterance(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	cfg.MaxUtteranceMs = 2000
	semCfg := semanticEnabledCfg()
	semCfg.LongSilenceFallbackMs = 10000
	semantic := &fakeSemanticTurn{pred: media.EOUPrediction{Complete: false, Confidence: 0.9}}
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, semantic, semCfg, media.NoopBackchannel{}, nil)
	session := &media.Session{StreamSID: "MZ-SEM-HOLD"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnPartial(ctx, session, media.Transcript{Text: "account number is"})
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "account number is", IsFinal: true})

	clock.Advance(700 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("Incomplete semantic prediction should hold EndOfTurn past silence")
	}
	clock.Advance(1500 * time.Millisecond)
	ends := filterTurnKind(listener.events, media.TurnEndOfTurn)
	if len(ends) != 1 {
		t.Fatalf("expected forced EndOfTurn at MaxUtteranceMs, got %d", len(ends))
	}
	if !ends[0].Forced {
		t.Fatal("expected forced EndOfTurn")
	}
}

func TestTurnManagerSemanticIncompleteResumesOnMoreSpeech(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	cfg.MaxUtteranceMs = 5000
	semantic := &fakeSemanticTurn{pred: media.EOUPrediction{Complete: false, Confidence: 0.9}}
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, semantic, semanticEnabledCfg(), media.NoopBackchannel{}, nil)
	session := &media.Session{StreamSID: "MZ-SEM-RESUME"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "one two", IsFinal: true})
	clock.Advance(700 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 0 {
		t.Fatal("should hold on incomplete semantic")
	}

	semantic.pred = media.EOUPrediction{Complete: true, Confidence: 0.95}
	tm.OnPartial(ctx, session, media.Transcript{Text: "one two three"})
	tm.OnFinal(ctx, session, media.Transcript{Text: "one two three", IsFinal: true})
	clock.Advance(250 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("expected EndOfTurn after resumed speech with complete semantic")
	}
}

func TestTurnManagerSemanticErrorFallsBackToCT6(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	semantic := &fakeSemanticTurn{err: errors.New("worker down")}
	tm := media.NewTurnManager(listener, cfg, clock, media.NoopVAD{}, semantic, semanticEnabledCfg(), media.NoopBackchannel{}, nil)
	session := &media.Session{StreamSID: "MZ-SEM-ERR"}
	ctx := context.Background()

	tm.OnSpeechStart(ctx, session)
	tm.OnSpeechEnd(ctx, session)
	tm.OnFinal(ctx, session, media.Transcript{Text: "hello there", IsFinal: true})
	clock.Advance(600 * time.Millisecond)

	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("semantic error should fall back to CT-6 silence timer EndOfTurn")
	}
}

func TestTurnManagerBackchannelSuppressesInterrupt(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	lexicon := media.NewLexiconBackchannel(media.DefaultBackchannelConfig())
	tm := media.NewTurnManager(listener, defaultTurnTestConfig(), clock, media.NoopVAD{}, nil, media.SemanticTurnConfig{}, lexicon, nil)
	session := &media.Session{StreamSID: "MZ-BC-SUPPRESS"}
	ctx := context.Background()

	tm.OnPartial(ctx, session, media.Transcript{Text: "haan ji"})
	tm.SetAgentSpeaking(session, true)
	tm.OnSpeechStart(ctx, session)

	if len(filterTurnKind(listener.events, media.TurnInterrupt)) != 0 {
		t.Fatal("backchannel should suppress Interrupt")
	}
	if tm.BackchannelsSuppressed() != 1 {
		t.Fatalf("backchannels_suppressed = %d, want 1", tm.BackchannelsSuppressed())
	}
}

func TestTurnManagerBackchannelFalseEmitsInterrupt(t *testing.T) {
	listener := &recordingTurnListener{}
	lexicon := media.NewLexiconBackchannel(media.DefaultBackchannelConfig())
	tm := media.NewTurnManager(listener, defaultTurnTestConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, lexicon, nil)
	session := &media.Session{StreamSID: "MZ-BC-INT"}
	ctx := context.Background()

	tm.OnPartial(ctx, session, media.Transcript{Text: "wait I have a question about my balance"})
	tm.SetAgentSpeaking(session, true)
	tm.OnSpeechStart(ctx, session)

	if len(filterTurnKind(listener.events, media.TurnInterrupt)) != 1 {
		t.Fatal("real interruption should emit Interrupt")
	}
}

func TestTurnManagerBackchannelErrorYieldsFloor(t *testing.T) {
	listener := &recordingTurnListener{}
	bc := &fakeBackchannel{err: errors.New("classifier down")}
	tm := media.NewTurnManager(listener, defaultTurnTestConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, bc, nil)
	session := &media.Session{StreamSID: "MZ-BC-ERR"}
	ctx := context.Background()

	tm.OnPartial(ctx, session, media.Transcript{Text: "haan ji"})
	tm.SetAgentSpeaking(session, true)
	tm.OnSpeechStart(ctx, session)

	if len(filterTurnKind(listener.events, media.TurnInterrupt)) != 1 {
		t.Fatal("backchannel error should fail-safe to Interrupt (yield floor)")
	}
}

func TestTurnManagerCT6RegressionNoopSemanticDisabledBackchannel(t *testing.T) {
	clock := media.NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	cfg := defaultTurnTestConfig()
	tm := media.NewTurnManager(
		listener,
		cfg,
		clock,
		media.NoopVAD{},
		media.NoopSemanticTurn{},
		media.SemanticTurnConfig{Enabled: false},
		media.NoopBackchannel{},
		nil,
	)
	session := &media.Session{StreamSID: "MZ-REG"}
	ctx := context.Background()

	tm.SetAgentSpeaking(session, true)
	tm.OnSpeechStart(ctx, session)
	if len(filterTurnKind(listener.events, media.TurnInterrupt)) != 1 {
		t.Fatal("disabled backchannel should emit Interrupt like CT-6")
	}

	listener.events = nil
	clock2 := media.NewFakeClock(time.Now())
	tm2 := media.NewTurnManager(
		listener,
		cfg,
		clock2,
		media.NoopVAD{},
		media.NoopSemanticTurn{},
		media.SemanticTurnConfig{Enabled: false},
		media.NoopBackchannel{},
		nil,
	)
	tm2.OnSpeechStart(ctx, session)
	tm2.OnPartial(ctx, session, media.Transcript{Text: "hello there"})
	tm2.OnSpeechEnd(ctx, session)
	tm2.OnFinal(ctx, session, media.Transcript{Text: "hello there", IsFinal: true})
	clock2.Advance(601 * time.Millisecond)
	if len(filterTurnKind(listener.events, media.TurnEndOfTurn)) != 1 {
		t.Fatal("disabled semantic should use CT-6 silence EndOfTurn timing")
	}
}

func TestLexiconBackchannelMatchesShortAcknowledgements(t *testing.T) {
	bc := media.NewLexiconBackchannel(media.DefaultBackchannelConfig())
	ok, err := bc.IsBackchannel(context.Background(), "haan ji", nil, 8000)
	if err != nil || !ok {
		t.Fatalf("haan ji backchannel = %v err = %v", ok, err)
	}
	ok, err = bc.IsBackchannel(context.Background(), "I need help with my account", nil, 8000)
	if err != nil || ok {
		t.Fatalf("long utterance should not be backchannel: %v", ok)
	}
}
