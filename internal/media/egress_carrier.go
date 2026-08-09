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

// pendingFrame is one paced egress media frame tagged with its TTS turn.
type pendingFrame struct {
	turnID string
	data   []byte
}

// CarrierEgress implements AudioEgress: paced carrier media, mark checkpoints, and clear for barge-in.
type CarrierEgress struct {
	cfg        EgressConfig
	profile    CarrierProfile
	serializer CarrierSerializer
	clock      Clock
	logger     *slog.Logger

	frameBytes   int
	frameDur     time.Duration
	sendAheadCap int

	mu             sync.Mutex
	session        *Session
	pendingFrames  []pendingFrame
	pendingMark    string
	framesSent     int
	playbackStart  time.Time
	pendingDropped int64
	stopped        bool
	paused         bool
	tickHandle     TimerHandle

	timingHub    *TurnTimingHub
	watchdog     *DeadAirWatchdog
	// Monotonic active-turn watermark: only frames for watermarkTurnID egress;
	// watermark only advances (never demotes on late older-turn audio).
	watermarkTurnID string
	watermarkSeq    int
	supersedeCount  int64
	egressMarked    map[string]bool
	humanGated      bool
	// DEBT-040: drain-ready gate. The egress pacer starts on BindSession but
	// the Asterisk bridge may not be draining yet (audiosocket accept / bridge
	// create still pending). Without a gate, the opener TTS burst is written
	// before Asterisk is listening → opener audio clipped (~740ms in live
	// session 0cc56de1). The drain-ready gate pauses the pacer until the
	// first binary ingress frame arrives (Asterisk is sending = bridge is
	// live), then resumes. pendingFrames (unbounded) holds the burst during
	// the wait. A timeout fallback (drainReadyTimeoutMs) auto-resumes if no
	// ingress frame ever arrives, so a misconfigured/late bridge can never
	// deadlock the call into silence.
	drainReadyGated bool
}

// NewCarrierEgress constructs carrier egress with injectable clock for deterministic tests.
func NewCarrierEgress(cfg EgressConfig, frameDurationMs int, clock Clock, serializer CarrierSerializer, profile CarrierProfile, logger *slog.Logger) *CarrierEgress {
	cfg = cfg.withDefaults()
	if clock == nil {
		clock = RealClock{}
	}
	if serializer == nil {
		serializer = NewCarrierSerializer(DefaultCarrierConfig())
	}
	if profile.Variant == "" {
		profile = DefaultCarrierConfig().Profile()
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
	sampleRate := profile.EgressSampleRate
	if sampleRate <= 0 {
		sampleRate = defaultTargetSampleRate
	}
	bytesPerSample := profile.EgressBytesPerSample
	if bytesPerSample <= 0 {
		bytesPerSample = 1
	}
	frameBytes := sampleRate * frameDurationMs / 1000 * bytesPerSample
	if frameBytes < 1 {
		frameBytes = 160
	}
	return &CarrierEgress{
		cfg:          cfg,
		profile:      profile,
		serializer:   serializer,
		clock:        clock,
		logger:       logger,
		frameBytes:   frameBytes,
		frameDur:     frameDur,
		sendAheadCap: sendAheadCap,
		egressMarked: make(map[string]bool),
	}
}

// EnableHumanGate blocks outbound audio until ConfirmHuman (CT-14 AMD pilot).
func (e *CarrierEgress) EnableHumanGate() {
	e.mu.Lock()
	e.humanGated = true
	e.paused = true
	e.mu.Unlock()
}

// ConfirmHuman releases the human gate and resumes paced egress.
func (e *CarrierEgress) ConfirmHuman() {
	e.mu.Lock()
	e.humanGated = false
	e.mu.Unlock()
	e.Resume()
}

// HumanGated reports whether egress is waiting for AMD human confirmation.
func (e *CarrierEgress) HumanGated() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.humanGated
}

// EnableDrainReadyGate blocks outbound audio until ConfirmDrainReady (DEBT-040).
// Mirrors EnableHumanGate but triggers on the first binary ingress frame
// (Asterisk is sending = bridge is live) instead of AMD human confirmation.
// Idempotent; no-op if the human gate is already active (AMD owns the pause).
func (e *CarrierEgress) EnableDrainReadyGate() {
	e.mu.Lock()
	if e.humanGated {
		// AMD human gate already owns the pause; don't double-gate.
		e.mu.Unlock()
		return
	}
	e.drainReadyGated = true
	e.paused = true
	e.mu.Unlock()
}

// ConfirmDrainReady releases the drain-ready gate and resumes paced egress.
// Idempotent; no-op if the gate was never enabled or already released.
func (e *CarrierEgress) ConfirmDrainReady() {
	e.mu.Lock()
	if !e.drainReadyGated {
		e.mu.Unlock()
		return
	}
	e.drainReadyGated = false
	e.mu.Unlock()
	e.Resume()
}

// DrainReadyGated reports whether egress is waiting for the first ingress
// frame (drain-ready gate).
func (e *CarrierEgress) DrainReadyGated() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.drainReadyGated
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
	rate := OutputSampleRateFromParams(session.Params)
	if rate <= 0 {
		rate = e.profile.EgressSampleRate
	}
	bytesPerSample := e.profile.EgressBytesPerSample
	if bytesPerSample <= 0 {
		bytesPerSample = 2
	}
	frameMs := int(e.frameDur / time.Millisecond)
	if frameMs <= 0 {
		frameMs = defaultFrameDurationMs
	}
	e.frameBytes = rate * frameMs / 1000 * bytesPerSample
	if e.frameBytes < 1 {
		e.frameBytes = 160
	}
	if e.logger != nil && rate > 0 {
		e.logger.Info("egress bound to session",
			"stream_sid", session.StreamSID,
			"output_sample_rate", rate,
			"frame_bytes", e.frameBytes,
		)
	}
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
	if e.logger != nil && len(chunk.MuLaw) > 0 {
		e.logger.Info("egress audio",
			"stream_sid", session.StreamSID,
			"turn_id", chunk.TurnID,
			"seq", chunk.Seq,
			"bytes", len(chunk.MuLaw),
			"final", chunk.Final,
		)
	}
	if len(chunk.MuLaw) == 0 {
		return nil
	}
	frames := splitMuLawFrames(chunk.MuLaw, e.frameBytes)
	e.mu.Lock()
	if chunk.TurnID != "" {
		seq := TurnSeq(chunk.TurnID)
		switch {
		case e.watermarkTurnID == "":
			e.watermarkTurnID = chunk.TurnID
			e.watermarkSeq = seq
		case chunk.TurnID == e.watermarkTurnID:
			// same active turn — admit
		case seq > 0 && seq > e.watermarkSeq:
			prior := e.watermarkTurnID
			dropped := len(e.pendingFrames)
			e.pendingFrames = nil
			if e.pendingMark != "" && e.pendingMark != chunk.TurnID {
				e.pendingMark = ""
			}
			e.watermarkTurnID = chunk.TurnID
			e.watermarkSeq = seq
			atomic.AddInt64(&e.supersedeCount, 1)
			if dropped > 0 {
				atomic.AddInt64(&e.pendingDropped, int64(dropped))
			}
			if e.logger != nil {
				e.logger.Info("egress turn superseded; dropped prior frames",
					"stream_sid", session.StreamSID,
					"prior_turn_id", prior,
					"new_turn_id", chunk.TurnID,
					"dropped_frames", dropped,
				)
			}
		default:
			// Older or unparseable non-matching turn: never demote.
			e.mu.Unlock()
			return nil
		}
		if chunk.TurnID != e.watermarkTurnID {
			e.mu.Unlock()
			return nil
		}
	}
	for _, fr := range frames {
		e.pendingFrames = append(e.pendingFrames, pendingFrame{turnID: chunk.TurnID, data: fr})
	}
	e.mu.Unlock()
	return nil
}

// SupersedeCount returns how many times the watermark advanced (test/observability).
func (e *CarrierEgress) SupersedeCount() int64 {
	return atomic.LoadInt64(&e.supersedeCount)
}

func (e *CarrierEgress) Mark(_ context.Context, _ *Session, turnID string) error {
	e.mu.Lock()
	e.pendingMark = turnID
	e.mu.Unlock()
	return nil
}

// DropPending clears locally queued egress frames without touching the carrier
// edge and without barge-in WARN spam. Used on Speak(newTurn) so prior-turn
// audio already in the pacer stops immediately (Cancel alone only stops TTS).
func (e *CarrierEgress) DropPending() int {
	e.mu.Lock()
	dropped := len(e.pendingFrames)
	e.pendingFrames = nil
	e.pendingMark = ""
	e.framesSent = 0
	e.playbackStart = e.clock.Now()
	e.mu.Unlock()
	if dropped > 0 {
		atomic.AddInt64(&e.pendingDropped, int64(dropped))
	}
	return dropped
}

// AdvanceWatermark sticks the egress admit gate to turnID immediately (before
// first audio). Only advances; never demotes. Paired with DropPending on Speak.
func (e *CarrierEgress) AdvanceWatermark(turnID string) {
	if turnID == "" {
		return
	}
	seq := TurnSeq(turnID)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.watermarkTurnID == "" || (seq > 0 && seq > e.watermarkSeq) {
		e.watermarkTurnID = turnID
		e.watermarkSeq = seq
	}
}

func (e *CarrierEgress) ClearPlayback(_ context.Context, session *Session) error {
	dropped := e.DropPending()
	e.mu.Lock()
	e.paused = false
	edgeBudgetMs := e.cfg.JitterMs
	e.mu.Unlock()
	if session == nil {
		return nil
	}
	if !e.profile.BargeInFlushSupported {
		if e.logger != nil {
			e.logger.Warn("barge-in: no carrier flush; buffered audio may still play on Asterisk edge",
				"stream_sid", session.StreamSID,
				"BARGEIN_FLUSH_SUPPORTED", false,
				"residual", "asterisk_edge_buffer_uncleared",
				"edge_buffer_budget_ms", edgeBudgetMs,
				"channel_interface", "dinesh_audiosocket_binary_ws",
				"pending_dropped", dropped,
			)
		}
		return nil
	}
	data, err := e.serializer.Clear(session.StreamSID)
	if err != nil {
		if e.logger != nil {
			e.logger.Warn("barge-in: carrier clear serialize failed",
				"stream_sid", session.StreamSID,
				"error", err,
				"residual", "asterisk_edge_buffer_uncleared",
				"edge_buffer_budget_ms", edgeBudgetMs,
				"pending_dropped", dropped,
			)
		}
		return err
	}
	if len(data) == 0 {
		if e.logger != nil {
			e.logger.Warn("barge-in: carrier clear returned empty frame",
				"stream_sid", session.StreamSID,
				"residual", "asterisk_edge_buffer_uncleared",
				"edge_buffer_budget_ms", edgeBudgetMs,
				"pending_dropped", dropped,
			)
		}
		return nil
	}
	// Text control frame (ready/clear/end_of_call) — not binary PCM.
	session.EnqueueControl(data)
	if e.logger != nil {
		// Post-flush residual is TCP + Asterisk internal only (local pacer cleared).
		e.logger.Info("barge-in: carrier clear sent",
			"stream_sid", session.StreamSID,
			"pending_dropped", dropped,
			"edge_buffer_budget_ms", 40, // post-flush: ~1–2 frames TCP/Asterisk internal
			"residual", "tcp_asterisk_internal_only",
		)
	}
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

		e.enqueueMediaFrame(session, frame.data)
		e.markEgressFirstFrame(session.StreamSID)
		if markTurn != "" {
			e.completePlaybackMark(session, markTurn)
		}
		return
	}
	if e.pendingMark != "" && len(e.pendingFrames) == 0 {
		turnID := e.pendingMark
		e.pendingMark = ""
		e.mu.Unlock()

		e.completePlaybackMark(session, turnID)
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
				e.completePlaybackMark(session, turnID)
				return
			}
			e.mu.Unlock()
			return
		}
		frame := e.pendingFrames[0]
		e.pendingFrames = e.pendingFrames[1:]
		e.framesSent++
		e.mu.Unlock()

		e.enqueueMediaFrame(session, frame.data)
		e.markEgressFirstFrame(session.StreamSID)
	}
}

func (e *CarrierEgress) enqueueMediaFrame(session *Session, frame []byte) {
	data, err := e.serializer.Media(session.StreamSID, frame)
	if err != nil {
		if e.logger != nil {
			e.logger.Warn("serialize media failed", "error", err)
		}
		return
	}
	if e.profile.BinaryEgress {
		session.EnqueueOutboundBinary(data)
	} else {
		session.EnqueueOutbound(data, true)
	}
}

func (e *CarrierEgress) completePlaybackMark(session *Session, turnID string) {
	if e.profile.RequiresMarkEcho {
		markData, err := e.serializer.Mark(session.StreamSID, turnID)
		if err != nil {
			if e.logger != nil {
				e.logger.Warn("serialize mark failed", "error", err)
			}
			return
		}
		if len(markData) > 0 {
			session.EnqueueOutbound(markData, false)
		}
		return
	}
	_ = session.NotifyPlaybackComplete(context.Background(), turnID)
}

func (e *CarrierEgress) markEgressFirstFrame(sessionID string) {
	e.mu.Lock()
	turnID := e.watermarkTurnID
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
