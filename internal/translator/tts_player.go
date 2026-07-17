package translator

import (
	"context"
	"log/slog"
	"sync"

	"websocket/internal/media"
)

// ttsPlayer routes TTS audio chunks to a target leg egress.
// Speak() serializes playback: each new utterance cancels the prior turn and
// clears egress before starting synthesis (mirrors media.TTSReplyConsumer.CancelPlayback).
type ttsPlayer struct {
	tts    media.TTSStream
	egress media.AudioEgress
	target *media.Session
	logger *slog.Logger

	mu           sync.Mutex
	speakMu      sync.Mutex
	activeTurnID string
	routeStarted bool
	onFirstByte  func(turnID string)
	firstMarked  map[string]bool
}

func newTTSPlayer(
	tts media.TTSStream,
	egress media.AudioEgress,
	target *media.Session,
	onFirstByte func(string),
	logger *slog.Logger,
) *ttsPlayer {
	if logger == nil {
		logger = slog.Default()
	}
	p := &ttsPlayer{
		tts:         tts,
		egress:      egress,
		target:      target,
		logger:      logger,
		onFirstByte: onFirstByte,
		firstMarked: make(map[string]bool),
	}
	if tts != nil {
		p.routeStarted = true
		go p.routeAudio()
	}
	return p
}

// NewTTSPlayerForTest exposes ttsPlayer construction for unit tests.
func NewTTSPlayerForTest(
	tts media.TTSStream,
	egress media.AudioEgress,
	target *media.Session,
	onFirstByte func(string),
) *ttsPlayer {
	return newTTSPlayer(tts, egress, target, onFirstByte, nil)
}

func (p *ttsPlayer) Speak(ctx context.Context, turnID, text string) error {
	if p.tts == nil || text == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.speakMu.Lock()
	defer p.speakMu.Unlock()

	p.mu.Lock()
	prev := p.activeTurnID
	p.activeTurnID = turnID
	p.mu.Unlock()

	if prev != "" && prev != turnID {
		p.cancelPlayback(ctx, prev)
	}
	p.clearEgress(ctx)

	return p.tts.Speak(turnID, text)
}

// cancelPlayback stops synthesis for turnID and flushes queued egress audio.
func (p *ttsPlayer) cancelPlayback(ctx context.Context, turnID string) {
	if p.tts != nil && turnID != "" {
		_ = p.tts.Cancel(turnID)
	}
	p.clearEgress(ctx)
	p.mu.Lock()
	delete(p.firstMarked, turnID)
	p.mu.Unlock()
}

func (p *ttsPlayer) clearEgress(ctx context.Context) {
	session := p.targetSession()
	if session != nil && p.egress != nil {
		_ = p.egress.ClearPlayback(ctx, session)
	}
}

func (p *ttsPlayer) routeAudio() {
	if p.tts == nil {
		return
	}
	for chunk := range p.tts.Audio() {
		active := p.activeTurn()
		if chunk.TurnID != "" && active != "" && chunk.TurnID != active {
			continue
		}
		if len(chunk.MuLaw) > 0 && p.onFirstByte != nil {
			p.mu.Lock()
			if !p.firstMarked[chunk.TurnID] {
				p.firstMarked[chunk.TurnID] = true
				p.mu.Unlock()
				p.onFirstByte(chunk.TurnID)
			} else {
				p.mu.Unlock()
			}
		}
		session := p.targetSession()
		if session == nil {
			continue
		}
		if err := p.egress.SendAudio(context.Background(), session, chunk); err != nil && p.logger != nil {
			p.logger.Warn("tts egress failed", "turn_id", chunk.TurnID, "error", err)
		}
	}
}

func (p *ttsPlayer) activeTurn() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeTurnID
}

func (p *ttsPlayer) targetSession() *media.Session {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.target
}

func (p *ttsPlayer) BindTarget(session *media.Session) {
	p.mu.Lock()
	p.target = session
	p.mu.Unlock()
}

func (p *ttsPlayer) Close() error {
	if p.tts == nil {
		return nil
	}
	return p.tts.Close()
}

var _ interface {
	Speak(context.Context, string, string) error
} = (*ttsPlayer)(nil)
