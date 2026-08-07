package media

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// AudioEgress receives synthesized audio for outbound playback (CT-10 implements Fonada WS).
type AudioEgress interface {
	SendAudio(ctx context.Context, session *Session, chunk TTSAudioChunk) error
	Mark(ctx context.Context, session *Session, turnID string) error
	ClearPlayback(ctx context.Context, session *Session) error
}

// PendingDropper clears locally queued egress frames (no carrier flush).
type PendingDropper interface {
	DropPending() int
}

// WatermarkAdvancer sticks egress admit to a new turn before first audio.
type WatermarkAdvancer interface {
	AdvanceWatermark(turnID string)
}

// LoggingEgress logs audio egress until CT-10 wires Fonada playback.
type LoggingEgress struct {
	logger *slog.Logger
}

// NewLoggingEgress returns an egress seam that logs chunk sizes and mark/clear events.
func NewLoggingEgress(logger *slog.Logger) *LoggingEgress {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingEgress{logger: logger}
}

func (e *LoggingEgress) SendAudio(_ context.Context, session *Session, chunk TTSAudioChunk) error {
	if e.logger != nil {
		e.logger.Info("egress audio",
			"stream_sid", session.StreamSID,
			"turn_id", chunk.TurnID,
			"seq", chunk.Seq,
			"bytes", len(chunk.MuLaw),
			"final", chunk.Final,
		)
	}
	return nil
}

func (e *LoggingEgress) Mark(_ context.Context, session *Session, turnID string) error {
	if e.logger != nil {
		e.logger.Info("egress mark", "stream_sid", session.StreamSID, "turn_id", turnID)
	}
	return nil
}

func (e *LoggingEgress) ClearPlayback(_ context.Context, session *Session) error {
	if e.logger != nil {
		e.logger.Info("egress clear", "stream_sid", session.StreamSID)
	}
	return nil
}

func (e *LoggingEgress) DropPending() int { return 0 }

// SessionCloseHook is invoked when the brain signals end_call after playback mark.
type SessionCloseHook func(ctx context.Context, session *Session)

// PlaybackDoneNotifier is told when a reply turn's audio has finished playing
// to the caller (last paced frame egressed / carrier mark echoed). The brain
// client implements this to forward a playback_done message so the engine can
// sequence actions that must wait for the caller to HEAR a line first.
type PlaybackDoneNotifier interface {
	NotifyPlaybackDone(session *Session, turnID string)
}

// defaultTTSFinalFallback is how long after the brain's done (and the last TTS
// audio chunk for the turn) we wait before finalizing the turn ourselves.
// ElevenLabs stream-input does NOT emit isFinal per flush on a persistent
// connection, so without this fallback the egress mark — and everything keyed
// on it (playback_done to the brain, end_call hangup, turn timing) — never
// fires.
const defaultTTSFinalFallback = 400 * time.Millisecond

// TTSReplyConsumer implements ReplyConsumer: text chunks → TTS → AudioEgress.
type TTSReplyConsumer struct {
	tts          TTSStream
	egress       AudioEgress
	turnManager  *TurnManager
	onEndCall    SessionCloseHook
	playbackDone PlaybackDoneNotifier
	logger       *slog.Logger

	mu            sync.Mutex
	session       *Session
	pendingMark   map[string]bool
	endCallAfter  map[string]bool
	endCallDelay  map[string]time.Duration
	agentSpeaking bool

	finalFallback time.Duration
	finalTimers   map[string]*time.Timer
	finalized     map[string]bool

	timingHub *TurnTimingHub
	watchdog  *DeadAirWatchdog
	turnMeta  map[string]TurnOutcome
	ttsMarked map[string]bool

	// turnHadText / turnHadAudio gate final-fallback arming (W1): for non-empty
	// text turns, do not arm at OnReplyDone until ≥1 audio chunk arrives.
	turnHadText  map[string]bool
	turnHadAudio map[string]bool

	// Monotonic speak watermark: Speak(newTurn) advances; routeAudio drops
	// chunks for older turn_ids (belt after producer cancel).
	activeSpeakTurn string
	speakWatermark  int

	routeStarted bool
}

// NewTTSReplyConsumer constructs a reply consumer that streams text to TTS and routes audio to egress.
func NewTTSReplyConsumer(
	tts TTSStream,
	egress AudioEgress,
	turnManager *TurnManager,
	onEndCall SessionCloseHook,
	logger *slog.Logger,
) *TTSReplyConsumer {
	if egress == nil {
		egress = NewLoggingEgress(logger)
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &TTSReplyConsumer{
		tts:           tts,
		egress:        egress,
		turnManager:   turnManager,
		onEndCall:     onEndCall,
		logger:        logger,
		pendingMark:   make(map[string]bool),
		endCallAfter:  make(map[string]bool),
		endCallDelay:  make(map[string]time.Duration),
		finalFallback: defaultTTSFinalFallback,
		finalTimers:   make(map[string]*time.Timer),
		finalized:     make(map[string]bool),
		turnMeta:      make(map[string]TurnOutcome),
		ttsMarked:     make(map[string]bool),
		turnHadText:   make(map[string]bool),
		turnHadAudio:  make(map[string]bool),
	}
	if tts != nil {
		c.routeStarted = true
		go c.routeAudio()
	}
	return c
}

// AttachStream binds a per-session TTS stream opened after session_start (Asterisk output rate).
func (c *TTSReplyConsumer) AttachStream(stream TTSStream, session *Session) {
	if stream == nil || session == nil {
		return
	}
	c.BindSession(session)
	c.mu.Lock()
	c.tts = stream
	startRoute := !c.routeStarted
	if startRoute {
		c.routeStarted = true
	}
	c.mu.Unlock()
	if startRoute {
		go c.routeAudio()
	}
}

// SetObservability attaches CT-12 timing and watchdog hooks.
func (c *TTSReplyConsumer) SetObservability(timing *TurnTimingHub, watchdog *DeadAirWatchdog) {
	c.timingHub = timing
	c.watchdog = watchdog
}

// SetPlaybackDoneNotifier attaches the brain client so it can forward
// playback_done to the engine when a turn's audio finishes playing.
func (c *TTSReplyConsumer) SetPlaybackDoneNotifier(n PlaybackDoneNotifier) {
	c.mu.Lock()
	c.playbackDone = n
	c.mu.Unlock()
}

// SetEndCallDelay makes an end_call turn hang up this long AFTER its playback
// completes (brain's done.end_call_delay_ms), e.g. a 3s grace period after
// "I am disconnecting this call". Call before OnReplyDone for the turn.
func (c *TTSReplyConsumer) SetEndCallDelay(turnID string, d time.Duration) {
	if turnID == "" || d <= 0 {
		return
	}
	c.mu.Lock()
	c.endCallDelay[turnID] = d
	c.mu.Unlock()
}

// SpeakHoldingLine plays a configured holding utterance (dead-air watchdog).
func (c *TTSReplyConsumer) SpeakHoldingLine(ctx context.Context, session *Session, turnID, text string) {
	if text == "" || session == nil {
		return
	}
	c.OnReplyChunk(ctx, session, turnID, 0, text)
	c.OnReplyDone(ctx, session, turnID, false, "watchdog_holding")
}

// BindSession associates the consumer with the active telephony session (one per sink factory).
func (c *TTSReplyConsumer) BindSession(session *Session) {
	c.mu.Lock()
	c.session = session
	c.mu.Unlock()
}

// CancelTTS stops TTS synthesis for a turn without clearing egress (CT-11 commit path).
func (c *TTSReplyConsumer) CancelTTS(turnID string) {
	if c.tts != nil {
		_ = c.tts.Cancel(turnID)
	}
	c.mu.Lock()
	delete(c.pendingMark, turnID)
	delete(c.endCallAfter, turnID)
	delete(c.endCallDelay, turnID)
	c.stopFinalTimerLocked(turnID)
	c.agentSpeaking = false
	c.mu.Unlock()
}

// CancelPlayback stops TTS and clears egress for barge-in (CT-11 seam).
func (c *TTSReplyConsumer) CancelPlayback(ctx context.Context, turnID string) {
	c.CancelTTS(turnID)
	c.mu.Lock()
	session := c.session
	c.mu.Unlock()
	if session != nil && c.turnManager != nil {
		c.turnManager.SetAgentSpeaking(session, false)
	}
	if session != nil {
		_ = c.egress.ClearPlayback(ctx, session)
	}
}

// SetReplyVoice applies optional per-turn voice_id / tts_model / tts_pace from
// the brain chunk payload before Speak. Empty fields leave stream env defaults.
func (c *TTSReplyConsumer) SetReplyVoice(turnID, voiceID, ttsModel string, ttsPace *float64) {
	c.mu.Lock()
	stream := c.tts
	c.mu.Unlock()
	ApplyTTSTurnVoice(stream, turnID, voiceID, ttsModel, ttsPace)
}

func (c *TTSReplyConsumer) OnReplyChunk(ctx context.Context, session *Session, turnID string, seq int, text string) {
	c.BindSession(session)
	if text == "" {
		return
	}
	c.mu.Lock()
	c.turnHadText[turnID] = true
	prev := c.activeSpeakTurn
	advance := turnID != "" && prev != turnID
	if advance {
		c.activeSpeakTurn = turnID
		if n := TurnSeq(turnID); n > c.speakWatermark {
			c.speakWatermark = n
		}
	}
	c.mu.Unlock()
	if c.logger != nil {
		c.logger.Info("reply chunk",
			"stream_sid", session.StreamSID,
			"turn_id", turnID,
			"seq", seq,
			"text_len", len(text),
		)
	}
	if advance && prev != "" {
		// Cancel prior synthesis at source (Sarvam: close+reopen) AND drop
		// already-queued prior frames in the pacer. Without DropPending, Cancel
		// alone lets ~1s+ of stale audio keep pacing until the new turn's first
		// chunk arrives (heard as overlap on call f1d04252).
		if c.tts != nil {
			_ = c.tts.Cancel(prev)
		}
		if w, ok := c.egress.(WatermarkAdvancer); ok {
			w.AdvanceWatermark(turnID)
		}
		if d, ok := c.egress.(PendingDropper); ok {
			n := d.DropPending()
			if n > 0 && c.logger != nil {
				c.logger.Info("egress pending dropped on speak advance",
					"stream_sid", session.StreamSID,
					"prior_turn_id", prev,
					"new_turn_id", turnID,
					"dropped_frames", n,
				)
			}
		}
	}
	if c.tts == nil {
		return
	}
	if err := c.tts.Speak(turnID, text); err != nil && c.logger != nil {
		c.logger.Warn("tts speak failed", "turn_id", turnID, "error", err)
	}
}

func (c *TTSReplyConsumer) OnReplyDone(ctx context.Context, session *Session, turnID string, endCall bool, disposition string) {
	c.BindSession(session)
	c.mu.Lock()
	c.pendingMark[turnID] = true
	if endCall {
		c.endCallAfter[turnID] = true
	}
	c.turnMeta[turnID] = TurnOutcome{Disposition: disposition, EndCall: endCall}
	// W1: non-empty text turns arm only after first audio; empty-text unchanged.
	if !c.turnHadText[turnID] || c.turnHadAudio[turnID] {
		c.armFinalFallbackLocked(turnID)
	}
	c.mu.Unlock()

	if c.timingHub != nil {
		c.timingHub.SetTurnOutcome(turnID, TurnOutcome{Disposition: disposition, EndCall: endCall})
	}

	if c.tts != nil {
		_ = c.tts.Speak(turnID, "")
	}
}

func (c *TTSReplyConsumer) OnReplyError(ctx context.Context, session *Session, turnID, fallbackText string) {
	if fallbackText == "" {
		return
	}
	c.mu.Lock()
	c.turnMeta[turnID] = TurnOutcome{Fallback: true, FallbackReason: "brain_error", Disposition: "error"}
	c.mu.Unlock()
	if c.timingHub != nil {
		c.timingHub.SetTurnOutcome(turnID, TurnOutcome{Fallback: true, FallbackReason: "brain_error", Disposition: "error"})
	}
	c.OnReplyChunk(ctx, session, turnID, 0, fallbackText)
	c.OnReplyDone(ctx, session, turnID, false, "error")
}

func (c *TTSReplyConsumer) routeAudio() {
	if c.tts == nil {
		return
	}
	for chunk := range c.tts.Audio() {
		c.mu.Lock()
		session := c.session
		wm := c.speakWatermark
		active := c.activeSpeakTurn
		c.mu.Unlock()
		if session == nil {
			continue
		}
		// Belt: drop audio for turn_ids below the active speak watermark.
		if chunk.TurnID != "" && wm > 0 {
			if seq := TurnSeq(chunk.TurnID); seq > 0 && seq < wm {
				continue
			}
			if active != "" && TurnSeqLess(chunk.TurnID, active) {
				continue
			}
		}

		if len(chunk.MuLaw) > 0 {
			c.mu.Lock()
			if !c.agentSpeaking && c.turnManager != nil {
				c.turnManager.SetAgentTurn(session, chunk.TurnID, true)
			}
			c.agentSpeaking = true
			markTTS := !c.ttsMarked[chunk.TurnID]
			if markTTS {
				c.ttsMarked[chunk.TurnID] = true
			}
			c.turnHadAudio[chunk.TurnID] = true
			// Audio flowing for a done turn: (re)arm so fallback fires after LAST chunk.
			// Also arms turns that deferred arming at OnReplyDone until first audio (W1).
			if c.pendingMark[chunk.TurnID] {
				c.armFinalFallbackLocked(chunk.TurnID)
			}
			c.mu.Unlock()
			if markTTS && c.timingHub != nil {
				c.timingHub.MarkTurn(chunk.TurnID, StageTTSFirstAudio)
				ttsPath := "rest"
				if p, ok := c.tts.(interface{ Path() string }); ok {
					if v := p.Path(); v != "" {
						ttsPath = v
					}
				}
				c.timingHub.SetTurnMediaPath(chunk.TurnID, "", ttsPath)
			}
			if err := c.egress.SendAudio(context.Background(), session, chunk); err != nil && c.logger != nil {
				c.logger.Warn("egress send failed", "error", err)
			}
		}

		if !chunk.Final {
			continue
		}
		// Ignore premature Final before first audio on non-empty text turns (W1).
		c.mu.Lock()
		premature := c.turnHadText[chunk.TurnID] && !c.turnHadAudio[chunk.TurnID]
		c.mu.Unlock()
		if premature {
			continue
		}
		c.finalizeTurn(chunk.TurnID)
	}
}

// armFinalFallbackLocked (re)schedules local turn finalization. ElevenLabs
// stream-input does not emit isFinal per flush on a persistent connection, so
// a genuine Final chunk may never arrive; without this the egress mark — and
// everything keyed on it (playback_done to the brain, end_call hangup, turn
// timing) — silently never fires. Re-armed on every audio chunk of a done
// turn, so it triggers finalFallback after synthesis output stops.
func (c *TTSReplyConsumer) armFinalFallbackLocked(turnID string) {
	if c.finalFallback <= 0 || c.finalized[turnID] {
		return
	}
	if t := c.finalTimers[turnID]; t != nil {
		t.Stop()
	}
	c.finalTimers[turnID] = time.AfterFunc(c.finalFallback, func() {
		c.finalizeTurn(turnID)
	})
}

func (c *TTSReplyConsumer) stopFinalTimerLocked(turnID string) {
	if t := c.finalTimers[turnID]; t != nil {
		t.Stop()
		delete(c.finalTimers, turnID)
	}
}

// finalizeTurn completes a reply turn's synthesis phase: registers the egress
// mark and (for non-deferred egress) runs post-playback actions. Called from
// a genuine TTS Final chunk or the final-fallback timer; idempotent per turn.
// If the brain's done has not arrived yet (no pendingMark), it leaves state
// alone — OnReplyDone re-arms the fallback which finalizes later.
func (c *TTSReplyConsumer) finalizeTurn(turnID string) {
	c.mu.Lock()
	if c.finalized[turnID] {
		c.mu.Unlock()
		return
	}
	c.stopFinalTimerLocked(turnID)
	if !c.pendingMark[turnID] {
		c.mu.Unlock()
		return
	}
	c.finalized[turnID] = true
	session := c.session
	endCall := c.endCallAfter[turnID]
	delete(c.pendingMark, turnID)
	c.mu.Unlock()
	if session == nil {
		return
	}

	if err := c.egress.Mark(context.Background(), session, turnID); err != nil && c.logger != nil {
		c.logger.Warn("egress mark failed", "error", err)
	}
	if de, ok := c.egress.(DeferredPlaybackEgress); ok && de.DefersPlaybackComplete() {
		// Playback completion arrives later via OnPlaybackComplete when the
		// paced egress drains past the mark.
		return
	}
	c.mu.Lock()
	delete(c.endCallAfter, turnID)
	c.agentSpeaking = false
	c.mu.Unlock()
	if c.turnManager != nil {
		c.turnManager.SetAgentSpeaking(session, false)
	}
	c.finishPlayback(context.Background(), session, turnID, endCall)
	c.completeTurnTiming(turnID)
}

// OnPlaybackComplete is invoked when the carrier echoes a mark after playback reaches it.
func (c *TTSReplyConsumer) OnPlaybackComplete(ctx context.Context, session *Session, turnID string) {
	c.mu.Lock()
	endCall := c.endCallAfter[turnID]
	delete(c.endCallAfter, turnID)
	c.agentSpeaking = false
	c.mu.Unlock()
	if c.turnManager != nil {
		c.turnManager.SetAgentSpeaking(session, false)
	}
	c.finishPlayback(ctx, session, turnID, endCall)
	c.completeTurnTiming(turnID)
}

// finishPlayback runs the post-playback actions for a reply turn: tell the
// brain playback finished, then hang up (immediately or after the brain's
// requested grace delay) when the turn carried end_call.
func (c *TTSReplyConsumer) finishPlayback(ctx context.Context, session *Session, turnID string, endCall bool) {
	c.mu.Lock()
	delay := c.endCallDelay[turnID]
	delete(c.endCallDelay, turnID)
	notifier := c.playbackDone
	c.mu.Unlock()
	if notifier != nil {
		notifier.NotifyPlaybackDone(session, turnID)
	}
	if !endCall || c.onEndCall == nil {
		return
	}
	if delay > 0 {
		if c.logger != nil {
			c.logger.Info("end_call delayed after playback",
				"stream_sid", session.StreamSID, "turn_id", turnID, "delay_ms", delay.Milliseconds())
		}
		time.AfterFunc(delay, func() { c.onEndCall(context.Background(), session) })
		return
	}
	c.onEndCall(ctx, session)
}

func (c *TTSReplyConsumer) completeTurnTiming(turnID string) {
	if c.timingHub == nil || turnID == "" {
		return
	}
	c.mu.Lock()
	outcome := c.turnMeta[turnID]
	delete(c.turnMeta, turnID)
	delete(c.ttsMarked, turnID)
	c.mu.Unlock()
	c.timingHub.CompleteTurn(turnID, outcome)
}

// Close shuts down the TTS stream.
func (c *TTSReplyConsumer) Close() error {
	if c.tts != nil {
		return c.tts.Close()
	}
	return nil
}

var _ ReplyConsumer = (*TTSReplyConsumer)(nil)
var _ PlaybackListener = (*TTSReplyConsumer)(nil)
var _ HoldingLineSpeaker = (*TTSReplyConsumer)(nil)
