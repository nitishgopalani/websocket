package media

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	defaultFallbackNoAudioMs = 2000
	defaultHoldingLine       = "ek minute"
)

// WatchdogConfig controls the dead-air watchdog.
type WatchdogConfig struct {
	NoAudioMs   int
	HoldingLine string
}

// DefaultWatchdogConfig returns CT-12 watchdog defaults.
func DefaultWatchdogConfig() WatchdogConfig {
	return WatchdogConfig{
		NoAudioMs:   defaultFallbackNoAudioMs,
		HoldingLine: defaultHoldingLine,
	}
}

// WatchdogConfigFromEnv loads watchdog settings.
func WatchdogConfigFromEnv() WatchdogConfig {
	cfg := DefaultWatchdogConfig()
	if v := os.Getenv("FALLBACK_NO_AUDIO_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.NoAudioMs = n
		}
	}
	if v := os.Getenv("HOLDING_LINE"); v != "" {
		cfg.HoldingLine = v
	}
	return cfg
}

// HoldingLineSpeaker plays a short holding utterance via TTS.
type HoldingLineSpeaker interface {
	SpeakHoldingLine(ctx context.Context, session *Session, turnID, text string)
}

type watchdogState struct {
	turnID      string
	timer       TimerHandle
	fired       bool
	audioSeen   bool
	holdingTurn string
}

// DeadAirWatchdog guarantees a holding line when no outbound audio arrives in time.
type DeadAirWatchdog struct {
	cfg     WatchdogConfig
	clock   Clock
	speaker HoldingLineSpeaker
	metrics *Metrics
	logger  *slog.Logger

	mu    sync.Mutex
	turns map[string]*watchdogState
}

// NewDeadAirWatchdog constructs a per-session dead-air watchdog.
func NewDeadAirWatchdog(cfg WatchdogConfig, clock Clock, speaker HoldingLineSpeaker, metrics *Metrics, logger *slog.Logger) *DeadAirWatchdog {
	if clock == nil {
		clock = RealClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.NoAudioMs <= 0 {
		cfg.NoAudioMs = defaultFallbackNoAudioMs
	}
	if cfg.HoldingLine == "" {
		cfg.HoldingLine = defaultHoldingLine
	}
	return &DeadAirWatchdog{
		cfg:     cfg,
		clock:   clock,
		speaker: speaker,
		metrics: metrics,
		logger:  logger,
		turns:   make(map[string]*watchdogState),
	}
}

// ArmOpener arms the watchdog at session/opener start.
func (w *DeadAirWatchdog) ArmOpener(session *Session, turnID string) {
	if w == nil || session == nil || turnID == "" {
		return
	}
	w.arm(session, turnID)
}

// ArmCallerTurn arms the watchdog when the caller finishes speaking (EndOfTurn).
func (w *DeadAirWatchdog) ArmCallerTurn(session *Session, turnID string) {
	if w == nil || session == nil || turnID == "" {
		return
	}
	w.arm(session, turnID)
}

func (w *DeadAirWatchdog) arm(session *Session, turnID string) {
	w.mu.Lock()
	if st, ok := w.turns[turnID]; ok && (st.fired || st.audioSeen) {
		w.mu.Unlock()
		return
	}
	if st, ok := w.turns[turnID]; ok && st.timer != nil {
		st.timer.Stop()
	}
	st := &watchdogState{turnID: turnID, holdingTurn: turnID + ":hold"}
	delay := time.Duration(w.cfg.NoAudioMs) * time.Millisecond
	sess := session
	turn := turnID
	st.timer = w.clock.AfterFunc(delay, func() {
		w.onTimeout(context.Background(), sess, turn)
	})
	w.turns[turnID] = st
	w.mu.Unlock()
}

// OnEgressAudio cancels the watchdog when outbound audio is written for the turn.
func (w *DeadAirWatchdog) OnEgressAudio(turnID string) {
	if w == nil || turnID == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	st, ok := w.turns[turnID]
	if !ok {
		return
	}
	st.audioSeen = true
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
}

func (w *DeadAirWatchdog) onTimeout(ctx context.Context, session *Session, turnID string) {
	if w == nil || session == nil || turnID == "" {
		return
	}
	w.mu.Lock()
	st, ok := w.turns[turnID]
	if !ok || st.fired || st.audioSeen {
		w.mu.Unlock()
		return
	}
	st.fired = true
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
	holdingTurn := st.holdingTurn
	w.mu.Unlock()

	if w.metrics != nil {
		w.metrics.IncFallback("dead_air_watchdog")
	}
	if w.logger != nil {
		w.logger.Warn("dead-air watchdog fired holding line",
			"stream_sid", session.StreamSID,
			"turn_id", turnID,
			"holding_line", w.cfg.HoldingLine,
		)
	}
	if w.speaker != nil {
		w.speaker.SpeakHoldingLine(ctx, session, holdingTurn, w.cfg.HoldingLine)
	}
}

// CancelTurn removes watchdog state for a turn (e.g. barge-in commit).
func (w *DeadAirWatchdog) CancelTurn(turnID string) {
	if w == nil || turnID == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if st, ok := w.turns[turnID]; ok {
		if st.timer != nil {
			st.timer.Stop()
		}
		delete(w.turns, turnID)
	}
}
