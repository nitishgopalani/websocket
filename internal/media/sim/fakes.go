package sim

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"websocket/internal/brain"
	"websocket/internal/media"
)

// FakeASRConfig controls deterministic ASR behavior for smoke tests.
type FakeASRConfig struct {
	FinalText         string
	FramesBeforeFinal int
	EmitSpeechStart   bool
}

// FakeASRProvider emits canned transcript events after N audio frames.
type FakeASRProvider struct {
	cfg FakeASRConfig

	mu       sync.Mutex
	lastMeta media.ASRSessionMeta
}

// LastMeta returns the most recent ASRSessionMeta passed to Open.
func (p *FakeASRProvider) LastMeta() media.ASRSessionMeta {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastMeta
}

func (p *FakeASRProvider) Open(_ context.Context, meta media.ASRSessionMeta) (media.ASRSession, error) {
	p.mu.Lock()
	p.lastMeta = meta
	p.mu.Unlock()
	cfg := p.cfg
	if cfg.FramesBeforeFinal <= 0 {
		cfg.FramesBeforeFinal = 5
	}
	if cfg.FinalText == "" {
		cfg.FinalText = "haan ji"
	}
	return &fakeASRSession{
		cfg:    cfg,
		events: make(chan media.ASREvent, 8),
	}, nil
}

type fakeASRSession struct {
	cfg      FakeASRConfig
	events   chan media.ASREvent
	frames   int
	closed   atomic.Bool
	emitOnce sync.Once
}

func (s *fakeASRSession) SendAudio(_ []byte) error {
	if s.closed.Load() {
		return media.ErrASRSessionClosed
	}
	s.frames++
	if s.frames == 1 {
		s.events <- media.ASREvent{Type: media.ASREventSpeechStart}
	}
	if s.frames >= s.cfg.FramesBeforeFinal {
		s.emitOnce.Do(func() {
			s.events <- media.ASREvent{
				Type:       media.ASREventFinal,
				Transcript: media.Transcript{Text: s.cfg.FinalText, IsFinal: true},
			}
			s.events <- media.ASREvent{Type: media.ASREventSpeechEnd}
		})
	}
	return nil
}

func (s *fakeASRSession) Events() <-chan media.ASREvent { return s.events }

func (s *fakeASRSession) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	close(s.events)
	return nil
}

// FakeTTSConfig controls fake TTS audio emission.
type FakeTTSConfig struct {
	FramesPerSpeak int
	FrameBytes     int
}

// FakeTTSProvider synthesizes μ-law audio on Speak().
type FakeTTSProvider struct {
	cfg FakeTTSConfig

	mu       sync.Mutex
	lastMeta media.TTSSessionMeta
}

// LastMeta returns the most recent TTSSessionMeta passed to Open.
func (p *FakeTTSProvider) LastMeta() media.TTSSessionMeta {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastMeta
}

func (p *FakeTTSProvider) Open(_ context.Context, meta media.TTSSessionMeta) (media.TTSStream, error) {
	p.mu.Lock()
	p.lastMeta = meta
	p.mu.Unlock()
	cfg := p.cfg
	if cfg.FramesPerSpeak <= 0 {
		cfg.FramesPerSpeak = 3
	}
	if cfg.FrameBytes <= 0 {
		cfg.FrameBytes = 160
	}
	return &fakeTTSStream{
		cfg:   cfg,
		audio: make(chan media.TTSAudioChunk, 16),
	}, nil
}

type fakeTTSStream struct {
	cfg   FakeTTSConfig
	audio chan media.TTSAudioChunk
	mu    sync.Mutex
	seq   map[string]int
}

func (s *fakeTTSStream) Speak(turnID, text string) error {
	s.mu.Lock()
	if s.seq == nil {
		s.seq = make(map[string]int)
	}
	if strings.TrimSpace(text) == "" {
		s.mu.Unlock()
		if turnID != "" {
			s.audio <- media.TTSAudioChunk{TurnID: turnID, Final: true}
		}
		return nil
	}
	seq := s.seq[turnID]
	s.mu.Unlock()

	frame := make([]byte, s.cfg.FrameBytes)
	for i := range frame {
		frame[i] = 0xD5
	}
	for i := 0; i < s.cfg.FramesPerSpeak; i++ {
		s.audio <- media.TTSAudioChunk{TurnID: turnID, Seq: seq + i, MuLaw: append([]byte(nil), frame...)}
	}
	s.mu.Lock()
	s.seq[turnID] = seq + s.cfg.FramesPerSpeak
	s.mu.Unlock()
	return nil
}

func (s *fakeTTSStream) Cancel(_ string) error             { return nil }
func (s *fakeTTSStream) Audio() <-chan media.TTSAudioChunk { return s.audio }
func (s *fakeTTSStream) Close() error {
	close(s.audio)
	return nil
}

// FakeBrainConfig controls canned brain replies.
type FakeBrainConfig struct {
	OpenerText string
	ReplyText  string
}

// StartFakeBrainServer returns a brain WS URL and cleanup for smoke tests.
func StartFakeBrainServer(t testingTB, cfg FakeBrainConfig) (string, func()) {
	t.Helper()
	if cfg.OpenerText == "" {
		cfg.OpenerText = "Namaste, main aapki madad ke liye hoon."
	}
	if cfg.ReplyText == "" {
		cfg.ReplyText = "Theek hai, samajh gaya."
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var header struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(data, &header); err != nil {
				continue
			}
			switch header.Type {
			case brain.TypeSessionStart:
				var start brain.SessionStartPayload
				if err := json.Unmarshal(data, &start); err != nil {
					continue
				}
				_ = conn.WriteJSON(brain.SessionReadyPayload{
					Type:        brain.TypeSessionReady,
					SessionID:   start.SessionID,
					BorrowerID:  start.BorrowerID,
					AsrLanguage: "hi-IN",
				})
			case brain.TypeTurn:
				var turn brain.TurnPayload
				if err := json.Unmarshal(data, &turn); err != nil {
					continue
				}
				text := cfg.ReplyText
				if turn.Transcript == "" {
					text = cfg.OpenerText
				}
				_ = conn.WriteJSON(brain.ChunkMessage{Type: brain.TypeChunk, TurnID: turn.TurnID, Seq: 0, Text: text})
				_ = conn.WriteJSON(brain.FlowClassMessage{Type: brain.TypeFlowClass, TurnID: turn.TurnID, Next: "YesNo"})
				_ = conn.WriteJSON(brain.DoneMessage{Type: brain.TypeDone, TurnID: turn.TurnID, AuditID: "fake-audit"})
			case brain.TypeSessionEnd, brain.TypeCancel:
				return
			}
		}
	}))
	return "ws" + strings.TrimPrefix(srv.URL, "http"), func() { srv.Close() }
}

// testingTB is satisfied by *testing.T and *testing.B.
type testingTB interface {
	Helper()
}

// RecordingTurnListener counts EndOfTurn events for smoke assertions.
type RecordingTurnListener struct {
	mu     sync.Mutex
	events []media.TurnEvent
	inner  media.TurnListener
}

func NewRecordingTurnListener(inner media.TurnListener) *RecordingTurnListener {
	return &RecordingTurnListener{inner: inner}
}

// SetInner attaches the downstream listener (e.g. brain client).
func (r *RecordingTurnListener) SetInner(inner media.TurnListener) {
	r.mu.Lock()
	r.inner = inner
	r.mu.Unlock()
}

func (r *RecordingTurnListener) OnTurnEvent(ctx context.Context, session *media.Session, event media.TurnEvent) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	if r.inner != nil {
		r.inner.OnTurnEvent(ctx, session, event)
	}
}

func (r *RecordingTurnListener) Snapshot() []media.TurnEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]media.TurnEvent, len(r.events))
	copy(out, r.events)
	return out
}

func (r *RecordingTurnListener) EndOfTurnCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.Kind == media.TurnEndOfTurn {
			n++
		}
	}
	return n
}

// TranscriptTap captures ASR finals for WER evaluation.
type TranscriptTap struct {
	mu     sync.Mutex
	Finals []string
	inner  media.TranscriptConsumer
}

func NewTranscriptTap(inner media.TranscriptConsumer) *TranscriptTap {
	return &TranscriptTap{inner: inner}
}

func (t *TranscriptTap) OnPartial(ctx context.Context, session *media.Session, transcript media.Transcript) {
	if t.inner != nil {
		t.inner.OnPartial(ctx, session, transcript)
	}
}

func (t *TranscriptTap) OnFinal(ctx context.Context, session *media.Session, transcript media.Transcript) {
	if transcript.Text != "" {
		t.mu.Lock()
		t.Finals = append(t.Finals, transcript.Text)
		t.mu.Unlock()
	}
	if t.inner != nil {
		t.inner.OnFinal(ctx, session, transcript)
	}
}

func (t *TranscriptTap) OnSpeechStart(ctx context.Context, session *media.Session) {
	if t.inner != nil {
		t.inner.OnSpeechStart(ctx, session)
	}
}

func (t *TranscriptTap) OnSpeechEnd(ctx context.Context, session *media.Session) {
	if t.inner != nil {
		t.inner.OnSpeechEnd(ctx, session)
	}
}

func (t *TranscriptTap) Snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.Finals...)
}
