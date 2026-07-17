package translator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"websocket/internal/media"
)

// legEgress plays synthesized PCM16 to a translator leg outbound queue.
type legEgress struct {
	leg              *leg
	frameBytes       int
	frameDuration    time.Duration
	sampleRate       int
	logger           *slog.Logger
	onFirstFrame        func(turnID string, at time.Time)
	onPlaybackComplete  func(turnID string, playbackEnd time.Time)
	mu               sync.Mutex
	pending          []byte
	activeTurnID     string
	firstMarked      map[string]bool
	nextPlayAt       time.Time
	now              func() time.Time
}

func newLegEgress(leg *leg, sampleRate, frameDurationMs int, onFirst func(string, time.Time), onPlaybackComplete func(string, time.Time), logger *slog.Logger) *legEgress {
	if frameDurationMs <= 0 {
		frameDurationMs = 20
	}
	if sampleRate <= 0 {
		sampleRate = 8000
	}
	frameBytes := sampleRate * frameDurationMs / 1000 * 2
	if frameBytes < 1 {
		frameBytes = 320
	}
	return &legEgress{
		leg:           leg,
		frameBytes:    frameBytes,
		frameDuration: time.Duration(frameDurationMs) * time.Millisecond,
		sampleRate:    sampleRate,
		logger:        logger,
		onFirstFrame:       onFirst,
		onPlaybackComplete: onPlaybackComplete,
		firstMarked:   make(map[string]bool),
		now:           time.Now,
	}
}

func (e *legEgress) BindLeg(leg *leg) {
	e.mu.Lock()
	e.leg = leg
	e.mu.Unlock()
}

func (e *legEgress) SendAudio(_ context.Context, _ *media.Session, chunk media.TTSAudioChunk) error {
	if len(chunk.MuLaw) == 0 && !chunk.Final {
		return nil
	}
	e.mu.Lock()
	if chunk.TurnID != "" && e.activeTurnID != "" && chunk.TurnID != e.activeTurnID {
		e.pending = nil
	}
	if chunk.TurnID != "" {
		e.activeTurnID = chunk.TurnID
	}
	if len(chunk.MuLaw) > 0 {
		e.pending = append(e.pending, chunk.MuLaw...)
	}
	e.mu.Unlock()

	if chunk.Final {
		e.flushFinal()
	} else {
		e.flushFrames()
	}
	return nil
}

func (e *legEgress) Mark(_ context.Context, _ *media.Session, _ string) error {
	return nil
}

func (e *legEgress) ClearPlayback(_ context.Context, _ *media.Session) error {
	e.mu.Lock()
	e.pending = nil
	e.activeTurnID = ""
	e.nextPlayAt = time.Time{}
	e.mu.Unlock()
	if e.leg != nil {
		e.leg.outbound.clear()
	}
	return nil
}

func (e *legEgress) flushFinal() {
	e.mu.Lock()
	turnID := e.activeTurnID
	if len(e.pending) > 0 && len(e.pending) < e.frameBytes {
		pad := make([]byte, e.frameBytes-len(e.pending))
		e.pending = append(e.pending, pad...)
	}
	e.mu.Unlock()
	e.flushFrames()
	if e.onPlaybackComplete != nil && turnID != "" {
		e.mu.Lock()
		playbackEnd := e.nextPlayAt
		if playbackEnd.IsZero() {
			playbackEnd = e.now()
		}
		e.mu.Unlock()
		e.onPlaybackComplete(turnID, playbackEnd)
	}
}

func (e *legEgress) flushFrames() {
	for {
		e.mu.Lock()
		if len(e.pending) < e.frameBytes {
			e.mu.Unlock()
			return
		}
		frame := make([]byte, e.frameBytes)
		copy(frame, e.pending[:e.frameBytes])
		e.pending = e.pending[e.frameBytes:]
		turnID := e.activeTurnID

		now := e.now()
		if e.nextPlayAt.IsZero() || e.nextPlayAt.Before(now) {
			e.nextPlayAt = now
		}
		playAt := e.nextPlayAt
		e.nextPlayAt = playAt.Add(e.frameDuration)
		leg := e.leg
		e.mu.Unlock()

		if leg != nil {
			leg.enqueueBinaryPaced(frame, playAt)
		}
		e.markFirst(turnID)
	}
}

func (e *legEgress) markFirst(turnID string) {
	if turnID == "" || e.onFirstFrame == nil {
		return
	}
	e.mu.Lock()
	if e.firstMarked[turnID] {
		e.mu.Unlock()
		return
	}
	e.firstMarked[turnID] = true
	e.mu.Unlock()
	e.onFirstFrame(turnID, e.now())
}

var _ media.AudioEgress = (*legEgress)(nil)
