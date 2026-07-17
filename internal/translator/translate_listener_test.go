package translator_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"websocket/internal/media"
	"websocket/internal/translator"
)

type mockMayura struct {
	out string
	err error
}

func (m *mockMayura) Translate(_ context.Context, text, _, _ string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	if m.out != "" {
		return m.out, nil
	}
	return "translated:" + text, nil
}

type mockTTS struct {
	audio chan media.TTSAudioChunk
}

func newMockTTS() *mockTTS {
	return &mockTTS{audio: make(chan media.TTSAudioChunk, 4)}
}

func (m *mockTTS) Speak(turnID, _ string) error {
	m.audio <- media.TTSAudioChunk{TurnID: turnID, Seq: 1, MuLaw: []byte{1, 2, 3, 4}, Final: false}
	m.audio <- media.TTSAudioChunk{TurnID: turnID, Seq: 2, Final: true}
	return nil
}
func (m *mockTTS) Cancel(string) error { return nil }
func (m *mockTTS) Audio() <-chan media.TTSAudioChunk { return m.audio }
func (m *mockTTS) Close() error { close(m.audio); return nil }

type recordingEgress struct {
	mu     sync.Mutex
	chunks []media.TTSAudioChunk
}

func (e *recordingEgress) SendAudio(_ context.Context, _ *media.Session, chunk media.TTSAudioChunk) error {
	e.mu.Lock()
	e.chunks = append(e.chunks, chunk)
	e.mu.Unlock()
	return nil
}
func (e *recordingEgress) chunkCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.chunks)
}
func (e *recordingEgress) Mark(context.Context, *media.Session, string) error { return nil }
func (e *recordingEgress) ClearPlayback(context.Context, *media.Session) error { return nil }

func TestTranslateListener_endToEnd(t *testing.T) {
	egress := &recordingEgress{}
	tts := newMockTTS()
	target := &media.Session{StreamSID: "leg-b"}
	latency := translator.NewLatencyTracker(nil)
	player := translator.NewTTSPlayerForTest(tts, egress, target, func(turnID string) {
		latency.MarkTTSFirstByte(turnID, time.Now())
	})
	var failOpen atomic.Bool
	turnID := "utt-1"
	listener := translator.NewTranslateListener(translator.TranslateListenerConfig{
		Mayura:     &mockMayura{out: "Hello"},
		TTS:        player,
		SourceLang: "hi-IN",
		TargetLang: "en-IN",
		Latency:    latency,
		FailOpen:   &failOpen,
		TurnID:     func() string { return turnID },
		TargetSID:  "leg-b",
	})
	latency.BeginTurn(turnID, "leg-a", "leg-b")
	latency.MarkSpeechEnd(turnID, time.Now().Add(-500*time.Millisecond))
	latency.MarkASRFinal(turnID, time.Now().Add(-400*time.Millisecond))

	source := &media.Session{StreamSID: "leg-a"}
	listener.OnTurnEvent(context.Background(), source, media.TurnEvent{
		Kind:       media.TurnEndOfTurn,
		Transcript: "नमस्ते",
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if egress.chunkCount() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if egress.chunkCount() == 0 {
		t.Fatal("expected egress audio chunks")
	}
	if failOpen.Load() {
		t.Fatal("unexpected fail-open")
	}
	// Production finishes on first egress; simulate deferred completion.
	latency.MarkTTSFirstByte(turnID, time.Now())
	latency.MarkEgressFirst(turnID, time.Now())
	latency.Finish(turnID, false, "")
	done := latency.Completed()
	if len(done) != 1 {
		t.Fatalf("completed after finish=%d", len(done))
	}
	if done[0].TranslatedText != "Hello" {
		t.Fatalf("translated=%q", done[0].TranslatedText)
	}
}

func TestTranslateListener_failOpenOnMayuraError(t *testing.T) {
	egress := &recordingEgress{}
	tts := newMockTTS()
	player := translator.NewTTSPlayerForTest(tts, egress, &media.Session{StreamSID: "leg-b"}, nil)
	var failOpen atomic.Bool
	listener := translator.NewTranslateListener(translator.TranslateListenerConfig{
		Mayura:     &mockMayura{err: errors.New("mayura down")},
		TTS:        player,
		SourceLang: "hi-IN",
		TargetLang: "en-IN",
		FailOpen:   &failOpen,
		TurnID:     func() string { return "utt-fail" },
	})
	listener.OnTurnEvent(context.Background(), &media.Session{StreamSID: "leg-a"}, media.TurnEvent{
		Kind:       media.TurnEndOfTurn,
		Transcript: "test",
	})
	time.Sleep(50 * time.Millisecond)
	if !failOpen.Load() {
		t.Fatal("expected fail-open")
	}
}

func TestEchoListener_passthroughSameText(t *testing.T) {
	egress := &recordingEgress{}
	tts := newMockTTS()
	target := &media.Session{StreamSID: "leg-b"}
	latency := translator.NewLatencyTracker(nil)
	player := translator.NewTTSPlayerForTest(tts, egress, target, func(turnID string) {
		latency.MarkTTSFirstByte(turnID, time.Now())
	})
	var failOpen atomic.Bool
	turnID := "echo-1"
	listener := translator.NewEchoListener(translator.EchoListenerConfig{
		TTS:        player,
		SourceLang: "hi-IN",
		Latency:    latency,
		FailOpen:   &failOpen,
		TurnID:     func() string { return turnID },
	})
	latency.BeginTurn(turnID, "leg-a", "leg-b")
	listener.OnTurnEvent(context.Background(), &media.Session{StreamSID: "leg-a"}, media.TurnEvent{
		Kind:       media.TurnEndOfTurn,
		Transcript: "मेरा नाम सत्य है।",
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if egress.chunkCount() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if egress.chunkCount() == 0 {
		t.Fatal("expected egress audio chunks")
	}
	if failOpen.Load() {
		t.Fatal("unexpected fail-open")
	}
	latency.MarkEgressFirst(turnID, time.Now())
	latency.Finish(turnID, false, "")
	done := latency.Completed()
	if len(done) != 1 {
		t.Fatalf("completed=%d", len(done))
	}
	if done[0].TranslatedText != "मेरा नाम सत्य है।" {
		t.Fatalf("passthrough=%q", done[0].TranslatedText)
	}
}
