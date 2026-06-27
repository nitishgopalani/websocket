package media

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const defaultBargeInClassifyTimeoutMs = 300

// BargeInConfig controls barge-in orchestration (CT-11).
type BargeInConfig struct {
	Enabled           bool
	ClassifyTimeoutMs int
}

// DefaultBargeInConfig returns CT-11 defaults.
func DefaultBargeInConfig() BargeInConfig {
	return BargeInConfig{
		Enabled:           true,
		ClassifyTimeoutMs: defaultBargeInClassifyTimeoutMs,
	}
}

func (c BargeInConfig) withDefaults() BargeInConfig {
	if c.ClassifyTimeoutMs <= 0 {
		c.ClassifyTimeoutMs = defaultBargeInClassifyTimeoutMs
	}
	return c
}

// BargeInConfigFromEnv loads barge-in settings from the environment.
func BargeInConfigFromEnv() BargeInConfig {
	cfg := DefaultBargeInConfig()
	if v := os.Getenv("BARGEIN_ENABLED"); v == "0" || v == "false" || v == "FALSE" {
		cfg.Enabled = false
	}
	if v := os.Getenv("BARGEIN_CLASSIFY_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.ClassifyTimeoutMs = n
		}
	}
	return cfg.withDefaults()
}

// BargeInEgress is the egress seam used for fast pause/resume during barge-in.
type BargeInEgress interface {
	Pause()
	Resume()
	ClearPlayback(ctx context.Context, session *Session) error
}

// BargeInTTS cancels in-flight TTS synthesis for a turn (without clearing egress).
type BargeInTTS interface {
	CancelTTS(turnID string)
}

// EngineSession cancels an in-flight brain/engine turn (CT-8 / EB-6).
type EngineSession interface {
	Cancel(turnID string) error
}

// BargeInHandler orchestrates fast local-VAD pause, backchannel classification, and commit/resume.
type BargeInHandler struct {
	cfg    BargeInConfig
	egress BargeInEgress
	tts    BargeInTTS
	engine EngineSession
	tm     *TurnManager
	clock  Clock
	logger *slog.Logger

	mu                  sync.Mutex
	pending             bool
	agentTurnID         string
	onsetTime           time.Time
	timeoutHandle       TimerHandle
	committed           map[string]struct{}
	pauseLatencyTotalNs int64
	pauseLatencyCount   int64
	committedCount      atomic.Int64
	resumedCount        atomic.Int64
}

// NewBargeInHandler constructs a barge-in orchestrator.
func NewBargeInHandler(
	cfg BargeInConfig,
	egress BargeInEgress,
	tts BargeInTTS,
	engine EngineSession,
	tm *TurnManager,
	clock Clock,
	logger *slog.Logger,
) *BargeInHandler {
	cfg = cfg.withDefaults()
	if clock == nil {
		clock = RealClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BargeInHandler{
		cfg:       cfg,
		egress:    egress,
		tts:       tts,
		engine:    engine,
		tm:        tm,
		clock:     clock,
		logger:    logger,
		committed: make(map[string]struct{}),
	}
}

// Enabled reports whether barge-in orchestration is active.
func (h *BargeInHandler) Enabled() bool {
	return h != nil && h.cfg.Enabled
}

// IsPending reports whether a barge-in episode is awaiting classification.
func (h *BargeInHandler) IsPending() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pending
}

// BargeInsCommitted returns committed barge-in count.
func (h *BargeInHandler) BargeInsCommitted() int64 {
	if h == nil {
		return 0
	}
	return h.committedCount.Load()
}

// BackchannelsResumed returns backchannel resume count.
func (h *BargeInHandler) BackchannelsResumed() int64 {
	if h == nil {
		return 0
	}
	return h.resumedCount.Load()
}

// BargeInPauseLatency returns mean pause latency in milliseconds (onset to Pause call).
func (h *BargeInHandler) BargeInPauseLatencyMs() float64 {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pauseLatencyCount == 0 {
		return 0
	}
	return float64(h.pauseLatencyTotalNs) / float64(h.pauseLatencyCount) / float64(time.Millisecond)
}

// OnSpeechOnset is the fast local-VAD path: pause egress synchronously and arm classify timeout.
func (h *BargeInHandler) OnSpeechOnset(ctx context.Context, session *Session, agentTurnID string) {
	if !h.Enabled() || session == nil {
		return
	}
	if h.tm != nil && !h.tm.isAgentSpeaking() {
		return
	}
	if agentTurnID == "" && h.tm != nil {
		agentTurnID = h.tm.agentTurnID()
	}
	if agentTurnID == "" {
		return
	}

	now := h.clock.Now()
	h.mu.Lock()
	if _, done := h.committed[agentTurnID]; done {
		h.mu.Unlock()
		return
	}
	if h.pending && h.agentTurnID == agentTurnID {
		h.mu.Unlock()
		return
	}
	h.pending = true
	h.agentTurnID = agentTurnID
	h.onsetTime = now
	h.stopTimeoutLocked()
	h.mu.Unlock()

	if h.egress != nil {
		start := h.clock.Now()
		h.egress.Pause()
		latency := h.clock.Now().Sub(start)
		h.mu.Lock()
		h.pauseLatencyTotalNs += latency.Nanoseconds()
		h.pauseLatencyCount++
		h.mu.Unlock()
	}

	h.mu.Lock()
	delay := time.Duration(h.cfg.ClassifyTimeoutMs) * time.Millisecond
	sess := session
	turnID := agentTurnID
	h.timeoutHandle = h.clock.AfterFunc(delay, func() {
		h.onClassifyTimeout(ctx, sess, turnID)
	})
	h.mu.Unlock()
}

// OnClassified applies the CT-7 backchannel decision after the fast pause.
func (h *BargeInHandler) OnClassified(ctx context.Context, session *Session, agentTurnID string, isBackchannel bool) {
	if !h.Enabled() || session == nil {
		return
	}
	h.mu.Lock()
	if !h.pending {
		h.mu.Unlock()
		return
	}
	if agentTurnID != "" && h.agentTurnID != "" && agentTurnID != h.agentTurnID {
		h.mu.Unlock()
		return
	}
	if agentTurnID == "" {
		agentTurnID = h.agentTurnID
	}
	h.stopTimeoutLocked()
	h.pending = false
	h.mu.Unlock()

	if isBackchannel {
		if h.egress != nil {
			h.egress.Resume()
		}
		h.resumedCount.Add(1)
		if h.tm != nil {
			h.tm.recordBackchannelSuppressed()
		}
		return
	}
	h.commit(ctx, session, agentTurnID)
}

func (h *BargeInHandler) onClassifyTimeout(ctx context.Context, session *Session, agentTurnID string) {
	if !h.Enabled() {
		return
	}
	h.mu.Lock()
	if !h.pending || h.agentTurnID != agentTurnID {
		h.mu.Unlock()
		return
	}
	h.pending = false
	h.mu.Unlock()
	h.commit(ctx, session, agentTurnID)
}

func (h *BargeInHandler) commit(ctx context.Context, session *Session, turnID string) {
	if turnID == "" {
		return
	}
	h.mu.Lock()
	if _, done := h.committed[turnID]; done {
		h.mu.Unlock()
		return
	}
	h.committed[turnID] = struct{}{}
	h.pending = false
	h.stopTimeoutLocked()
	h.mu.Unlock()

	if h.egress != nil {
		_ = h.egress.ClearPlayback(ctx, session)
	}
	if h.tts != nil {
		h.tts.CancelTTS(turnID)
	}
	if h.engine != nil {
		_ = h.engine.Cancel(turnID)
	}
	if h.tm != nil {
		h.tm.SetAgentSpeaking(session, false)
	}
	h.committedCount.Add(1)
}

func (h *BargeInHandler) stopTimeoutLocked() {
	if h.timeoutHandle != nil {
		h.timeoutHandle.Stop()
		h.timeoutHandle = nil
	}
}
