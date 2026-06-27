package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// CarrierSerializer frames outbound carrier websocket JSON (Exotel/Fonada; GO-A swaps variants).
type CarrierSerializer interface {
	Media(streamSID string, muLaw []byte) ([]byte, error)
	Mark(streamSID string, turnID string) ([]byte, error)
	Clear(streamSID string) ([]byte, error)
}

// ExotelFonadaSerializer emits Exotel/Fonada bidirectional stream JSON.
type ExotelFonadaSerializer struct{}

func (ExotelFonadaSerializer) Media(streamSID string, muLaw []byte) ([]byte, error) {
	payload := base64.StdEncoding.EncodeToString(muLaw)
	return json.Marshal(map[string]any{
		"event":      EventMedia,
		"stream_sid": streamSID,
		"media": map[string]any{
			"payload": payload,
		},
	})
}

func (ExotelFonadaSerializer) Mark(streamSID string, turnID string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"event":      EventMark,
		"stream_sid": streamSID,
		"mark": map[string]string{
			"name": turnID,
		},
	})
}

func (ExotelFonadaSerializer) Clear(streamSID string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"event":      "clear",
		"stream_sid": streamSID,
	})
}

// DeferredPlaybackEgress marks egress that waits for carrier mark echo before playback-complete.
type DeferredPlaybackEgress interface {
	DefersPlaybackComplete() bool
}

// CarrierEgress implements AudioEgress: paced μ-law media, mark checkpoints, and clear for barge-in.
type CarrierEgress struct {
	cfg        EgressConfig
	serializer CarrierSerializer
	clock      Clock
	logger     *slog.Logger

	frameBytes   int
	frameDur     time.Duration
	sendAheadCap int

	mu             sync.Mutex
	session        *Session
	pendingFrames  [][]byte
	pendingMark    string
	framesSent     int
	playbackStart  time.Time
	pendingDropped int64
	stopped        bool
	paused         bool
	tickHandle     TimerHandle

	timingHub    *TurnTimingHub
	watchdog     *DeadAirWatchdog
	activeTurnID string
	egressMarked map[string]bool
}

// NewCarrierEgress constructs carrier egress with injectable clock for deterministic tests.
func NewCarrierEgress(cfg EgressConfig, frameDurationMs int, clock Clock, logger *slog.Logger) *CarrierEgress {
	cfg = cfg.withDefaults()
	if clock == nil {
		clock = RealClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if frameDurationMs <= 0 {
		frameDurationMs = defaultFrameDurationMs
	}
	frameDur := time.Duration(frameDurationMs) * time.Millisecond
	sendAheadCap := cfg.JitterMs / frameDurationMs
	if sendAheadCap < 1 {
		sendAheadCap = 1
	}
	sampleRate := defaultTargetSampleRate
	frameBytes := sampleRate * frameDurationMs / 1000
	if frameBytes < 1 {
		frameBytes = 160
	}
	return &CarrierEgress{
		cfg:          cfg,
		serializer:   ExotelFonadaSerializer{},
		clock:        clock,
		logger:       logger,
		frameBytes:   frameBytes,
		frameDur:     frameDur,
		sendAheadCap: sendAheadCap,
		egressMarked: make(map[string]bool),
	}
}

// SetObservability attaches CT-12 timing and watchdog hooks.
func (e *CarrierEgress) SetObservability(timing *TurnTimingHub, watchdog *DeadAirWatchdog) {
	e.timingHub = timing
	e.watchdog = watchdog
}

// BindSession associates egress with a session and starts the pacing loop.
func (e *CarrierEgress) BindSession(session *Session) {
	e.mu.Lock()
	e.session = session
	e.playbackStart = e.clock.Now()
	e.stopped = false
	e.mu.Unlock()
	e.scheduleTick()
}

// Unbind stops pacing for session teardown.
func (e *CarrierEgress) Unbind() {
	e.mu.Lock()
	e.stopped = true
	if e.tickHandle != nil {
		e.tickHandle.Stop()
		e.tickHandle = nil
	}
	e.session = nil
	e.pendingFrames = nil
	e.pendingMark = ""
	e.mu.Unlock()
}

func (e *CarrierEgress) DefersPlaybackComplete() bool { return true }

// Pause stops the paced drainer from dequeuing frames (buffer retained). Idempotent.
// The session outbound writer goroutine is unchanged; only the pacer stops emitting.
func (e *CarrierEgress) Pause() {
	e.mu.Lock()
	e.paused = true
	e.mu.Unlock()
}

// Resume continues paced draining from the paused position. Idempotent.
func (e *CarrierEgress) Resume() {
	e.mu.Lock()
	e.paused = false
	e.mu.Unlock()
}

// Paused reports whether the paced drainer is paused.
func (e *CarrierEgress) Paused() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.paused
}

// PendingDropped returns audio frames dropped locally by ClearPlayback before they were sent.
func (e *CarrierEgress) PendingDropped() int64 {
	return atomic.LoadInt64(&e.pendingDropped)
}

func (e *CarrierEgress) SendAudio(_ context.Context, session *Session, chunk TTSAudioChunk) error {
	if len(chunk.MuLaw) == 0 {
		return nil
	}
	frames := splitMuLawFrames(chunk.MuLaw, e.frameBytes)
	e.mu.Lock()
	e.pendingFrames = append(e.pendingFrames, frames...)
	e.activeTurnID = chunk.TurnID
	e.mu.Unlock()
	return nil
}

func (e *CarrierEgress) Mark(_ context.Context, _ *Session, turnID string) error {
	e.mu.Lock()
	e.pendingMark = turnID
	e.mu.Unlock()
	return nil
}

func (e *CarrierEgress) ClearPlayback(_ context.Context, session *Session) error {
	e.mu.Lock()
	dropped := len(e.pendingFrames)
	e.pendingFrames = nil
	e.pendingMark = ""
	e.framesSent = 0
	e.playbackStart = e.clock.Now()
	e.paused = false
	e.mu.Unlock()
	if dropped > 0 {
		atomic.AddInt64(&e.pendingDropped, int64(dropped))
	}
	if session == nil {
		return nil
	}
	data, err := e.serializer.Clear(session.StreamSID)
	if err != nil {
		return err
	}
	session.EnqueueOutbound(data, false)
	return nil
}

func splitMuLawFrames(muLaw []byte, frameBytes int) [][]byte {
	if len(muLaw) == 0 {
		return nil
	}
	out := make([][]byte, 0, (len(muLaw)+frameBytes-1)/frameBytes)
	for off := 0; off < len(muLaw); off += frameBytes {
		end := off + frameBytes
		if end > len(muLaw) {
			end = len(muLaw)
		}
		frame := make([]byte, end-off)
		copy(frame, muLaw[off:end])
		out = append(out, frame)
	}
	return out
}

func (e *CarrierEgress) scheduleTick() {
	e.mu.Lock()
	if e.stopped || e.session == nil {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
	e.tickHandle = e.clock.AfterFunc(e.frameDur, func() {
		if e.cfg.Pacing == egressPacingBurst {
			e.drainBurst()
		} else {
			e.onTick()
		}
		e.scheduleTick()
	})
}

func (e *CarrierEgress) maxFramesAllowed(now time.Time) int {
	e.mu.Lock()
	start := e.playbackStart
	e.mu.Unlock()
	elapsed := now.Sub(start)
	if elapsed < 0 {
		elapsed = 0
	}
	realtimePos := int(elapsed / e.frameDur)
	return realtimePos + e.sendAheadCap
}

func (e *CarrierEgress) onTick() {
	e.mu.Lock()
	if e.paused {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	session := e.currentSession()
	if session == nil {
		return
	}
	now := e.clock.Now()
	maxSent := e.maxFramesAllowed(now)
	if maxSent <= 0 {
		return
	}

	e.mu.Lock()
	if e.framesSent >= maxSent {
		e.mu.Unlock()
		return
	}
	if len(e.pendingFrames) > 0 {
		frame := e.pendingFrames[0]
		e.pendingFrames = e.pendingFrames[1:]
		e.framesSent++
		markTurn := ""
		if len(e.pendingFrames) == 0 && e.pendingMark != "" {
			markTurn = e.pendingMark
			e.pendingMark = ""
		}
		e.mu.Unlock()

		data, err := e.serializer.Media(session.StreamSID, frame)
		if err != nil {
			if e.logger != nil {
				e.logger.Warn("serialize media failed", "error", err)
			}
			return
		}
		session.EnqueueOutbound(data, true)
		e.markEgressFirstFrame(session.StreamSID)
		if markTurn != "" {
			markData, err := e.serializer.Mark(session.StreamSID, markTurn)
			if err != nil {
				if e.logger != nil {
					e.logger.Warn("serialize mark failed", "error", err)
				}
				return
			}
			session.EnqueueOutbound(markData, false)
		}
		return
	}
	if e.pendingMark != "" && len(e.pendingFrames) == 0 {
		turnID := e.pendingMark
		e.pendingMark = ""
		e.mu.Unlock()

		data, err := e.serializer.Mark(session.StreamSID, turnID)
		if err != nil {
			if e.logger != nil {
				e.logger.Warn("serialize mark failed", "error", err)
			}
			return
		}
		session.EnqueueOutbound(data, false)
		return
	}
	e.mu.Unlock()
}

func (e *CarrierEgress) drainBurst() {
	e.mu.Lock()
	if e.paused {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	session := e.currentSession()
	if session == nil {
		return
	}
	allowed := e.maxFramesAllowed(e.clock.Now())

	for {
		e.mu.Lock()
		if e.framesSent >= allowed || len(e.pendingFrames) == 0 {
			if len(e.pendingFrames) == 0 && e.pendingMark != "" {
				turnID := e.pendingMark
				e.pendingMark = ""
				e.mu.Unlock()
				data, err := e.serializer.Mark(session.StreamSID, turnID)
				if err == nil {
					session.EnqueueOutbound(data, false)
				}
				return
			}
			e.mu.Unlock()
			return
		}
		frame := e.pendingFrames[0]
		e.pendingFrames = e.pendingFrames[1:]
		e.framesSent++
		e.mu.Unlock()

		data, err := e.serializer.Media(session.StreamSID, frame)
		if err != nil {
			continue
		}
		session.EnqueueOutbound(data, true)
		e.markEgressFirstFrame(session.StreamSID)
	}
}

func (e *CarrierEgress) markEgressFirstFrame(sessionID string) {
	e.mu.Lock()
	turnID := e.activeTurnID
	if turnID == "" || e.egressMarked[turnID] {
		e.mu.Unlock()
		return
	}
	e.egressMarked[turnID] = true
	timing := e.timingHub
	watchdog := e.watchdog
	e.mu.Unlock()
	if timing != nil {
		timing.MarkTurn(turnID, StageEgressFirstFrame)
	}
	if watchdog != nil {
		watchdog.OnEgressAudio(turnID)
	}
	_ = sessionID
}

func (e *CarrierEgress) currentSession() *Session {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.session
}

var _ AudioEgress = (*CarrierEgress)(nil)
var _ DeferredPlaybackEgress = (*CarrierEgress)(nil)
