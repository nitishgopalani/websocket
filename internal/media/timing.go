package media

import (
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	StageSessionStart     = "session_start"
	StageCallerEnd        = "caller_end"
	StageASRFinal         = "asr_final"
	StageEngineSent       = "engine_sent"
	StageEngineFirstChunk = "engine_first_chunk"
	StageTTSFirstAudio    = "tts_first_audio"
	StageEgressFirstFrame = "egress_first_frame"
	StagePlaybackComplete = "playback_complete"
	StageSpeechEnd        = "speech_end"
)

// LatencyBudget holds soft latency targets for observability warnings.
type LatencyBudget struct {
	MouthToEarTargetMs int
}

// DefaultLatencyBudget returns CT-12 latency budget defaults.
func DefaultLatencyBudget() LatencyBudget {
	return LatencyBudget{MouthToEarTargetMs: 1200}
}

// LatencyBudgetFromEnv loads latency budget settings.
func LatencyBudgetFromEnv() LatencyBudget {
	cfg := DefaultLatencyBudget()
	if v := os.Getenv("MOUTH_TO_EAR_TARGET_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MouthToEarTargetMs = n
		}
	}
	return cfg
}

// TurnOutcome captures per-turn result metadata for structured logging.
type TurnOutcome struct {
	Disposition    string
	EndCall        bool
	Fallback       bool
	FallbackReason string
	BargeIn        bool
}

// TurnTiming tracks stage timestamps for one conversational turn.
type TurnTiming struct {
	SessionID string
	TurnID    string
	Opener    bool

	mu      sync.Mutex
	marks   map[string]time.Time
	outcome TurnOutcome
	closed  bool
}

// NewTurnTiming constructs a turn timing tracker.
func NewTurnTiming(sessionID, turnID string, opener bool) *TurnTiming {
	return &TurnTiming{
		SessionID: sessionID,
		TurnID:    turnID,
		Opener:    opener,
		marks:     make(map[string]time.Time),
	}
}

// Mark records a stage timestamp once.
func (t *TurnTiming) Mark(stage string, at time.Time) {
	if t == nil || stage == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	if _, ok := t.marks[stage]; ok {
		return
	}
	t.marks[stage] = at
}

// SetOutcome records turn result metadata before completion.
func (t *TurnTiming) SetOutcome(outcome TurnOutcome) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.outcome = outcome
}

type turnDurations struct {
	ASRMS          int64
	EndpointMS     int64
	EngineMS       int64
	TTSMS          int64
	MouthToEarMS   int64
	OpenerMS       int64
	PlaybackTailMS int64
}

func (t *TurnTiming) durations() turnDurations {
	t.mu.Lock()
	defer t.mu.Unlock()
	var d turnDurations
	if a, ok := t.marks[StageASRFinal]; ok {
		if s, ok := t.marks[StageSpeechEnd]; ok && a.After(s) {
			d.ASRMS = a.Sub(s).Milliseconds()
		}
		if c, ok := t.marks[StageCallerEnd]; ok && c.After(a) {
			d.EndpointMS = c.Sub(a).Milliseconds()
		}
	}
	if sent, ok := t.marks[StageEngineSent]; ok {
		if fc, ok := t.marks[StageEngineFirstChunk]; ok && fc.After(sent) {
			d.EngineMS = fc.Sub(sent).Milliseconds()
		}
	}
	if fc, ok := t.marks[StageEngineFirstChunk]; ok {
		if ta, ok := t.marks[StageTTSFirstAudio]; ok && ta.After(fc) {
			d.TTSMS = ta.Sub(fc).Milliseconds()
		}
	}
	if caller, ok := t.marks[StageCallerEnd]; ok {
		if eg, ok := t.marks[StageEgressFirstFrame]; ok && eg.After(caller) {
			d.MouthToEarMS = eg.Sub(caller).Milliseconds()
		}
	}
	if start, ok := t.marks[StageSessionStart]; ok {
		if eg, ok := t.marks[StageEgressFirstFrame]; ok && eg.After(start) {
			d.OpenerMS = eg.Sub(start).Milliseconds()
		}
	}
	if eg, ok := t.marks[StageEgressFirstFrame]; ok {
		if pc, ok := t.marks[StagePlaybackComplete]; ok && pc.After(eg) {
			d.PlaybackTailMS = pc.Sub(eg).Milliseconds()
		}
	}
	return d
}

// TurnTimingHub coordinates per-session turn timing instances.
type TurnTimingHub struct {
	clock   Clock
	logger  *slog.Logger
	metrics *Metrics
	budget  LatencyBudget

	mu           sync.Mutex
	sessionID    string
	sessionStart time.Time
	byTurnID     map[string]*TurnTiming
	pending      *TurnTiming
	activeTurnID string
}

// NewTurnTimingHub constructs a per-session timing hub.
func NewTurnTimingHub(sessionID string, clock Clock, logger *slog.Logger, metrics *Metrics, budget LatencyBudget) *TurnTimingHub {
	if clock == nil {
		clock = RealClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &TurnTimingHub{
		clock:     clock,
		logger:    logger,
		metrics:   metrics,
		budget:    budget,
		sessionID: sessionID,
		byTurnID:  make(map[string]*TurnTiming),
	}
}

// BindSession sets the stream SID for per-turn correlation.
func (h *TurnTimingHub) BindSession(sessionID string) {
	if h == nil || sessionID == "" {
		return
	}
	h.mu.Lock()
	h.sessionID = sessionID
	h.mu.Unlock()
}

// MarkSessionStart records session start for opener timing.
func (h *TurnTimingHub) MarkSessionStart() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.sessionStart = h.clock.Now()
	h.mu.Unlock()
}

// MarkSpeechEnd records ASR speech-end for ASR latency derivation.
func (h *TurnTimingHub) MarkSpeechEnd() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.pending == nil {
		h.pending = NewTurnTiming(h.sessionID, "", false)
	}
	pending := h.pending
	h.mu.Unlock()
	pending.Mark(StageSpeechEnd, h.clock.Now())
}

// MarkASRFinal records final transcript readiness.
func (h *TurnTimingHub) MarkASRFinal() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.pending == nil {
		h.pending = NewTurnTiming(h.sessionID, "", false)
	}
	pending := h.pending
	h.mu.Unlock()
	pending.Mark(StageASRFinal, h.clock.Now())
}

// BeginCallerTurn starts timing at caller EndOfTurn (before engine turn ID is known).
func (h *TurnTimingHub) BeginCallerTurn() *TurnTiming {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.pending == nil {
		h.pending = NewTurnTiming(h.sessionID, "", false)
	}
	t := h.pending
	h.mu.Unlock()
	t.Mark(StageCallerEnd, h.clock.Now())
	return t
}

// BindEngineTurn associates a brain turn ID and marks engine_sent.
func (h *TurnTimingHub) BindEngineTurn(turnID string, opener bool) *TurnTiming {
	if h == nil || turnID == "" {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.clock.Now()
	var t *TurnTiming
	if h.pending != nil {
		t = h.pending
		t.TurnID = turnID
		t.Opener = opener
		h.pending = nil
	} else {
		t = NewTurnTiming(h.sessionID, turnID, opener)
	}
	if opener && !h.sessionStart.IsZero() {
		t.Mark(StageSessionStart, h.sessionStart)
	}
	t.Mark(StageEngineSent, now)
	h.byTurnID[turnID] = t
	h.activeTurnID = turnID
	return t
}

// MarkTurn records a stage for a known turn ID.
func (h *TurnTimingHub) MarkTurn(turnID, stage string) {
	if h == nil || turnID == "" {
		return
	}
	h.mu.Lock()
	t := h.byTurnID[turnID]
	h.mu.Unlock()
	if t == nil {
		return
	}
	t.Mark(stage, h.clock.Now())
}

// CompleteTurn finalizes timing, emits structured log, and records metrics.
func (h *TurnTimingHub) CompleteTurn(turnID string, outcome TurnOutcome) {
	if h == nil || turnID == "" {
		return
	}
	h.mu.Lock()
	t := h.byTurnID[turnID]
	if t == nil {
		h.mu.Unlock()
		return
	}
	delete(h.byTurnID, turnID)
	if h.activeTurnID == turnID {
		h.activeTurnID = ""
	}
	h.mu.Unlock()

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	t.outcome = outcome
	t.mu.Unlock()

	t.Mark(StagePlaybackComplete, h.clock.Now())
	d := t.durations()

	if h.metrics != nil {
		h.metrics.IncTurnsTotal()
		if d.ASRMS > 0 {
			h.metrics.ObserveStage(StageASRFinal, float64(d.ASRMS))
		}
		if d.EndpointMS > 0 {
			h.metrics.ObserveStage("endpoint", float64(d.EndpointMS))
		}
		if d.EngineMS > 0 {
			h.metrics.ObserveStage(StageEngineSent, float64(d.EngineMS))
		}
		if d.TTSMS > 0 {
			h.metrics.ObserveStage(StageTTSFirstAudio, float64(d.TTSMS))
		}
		if d.MouthToEarMS > 0 {
			h.metrics.ObserveMouthToEar(float64(d.MouthToEarMS))
			if h.budget.MouthToEarTargetMs > 0 && d.MouthToEarMS > int64(h.budget.MouthToEarTargetMs) {
				h.metrics.IncLatencyBudgetExceeded()
				if h.logger != nil {
					h.logger.Warn("mouth-to-ear latency budget exceeded",
						"session_id", t.SessionID,
						"turn_id", turnID,
						"mouth_to_ear_ms", d.MouthToEarMS,
						"target_ms", h.budget.MouthToEarTargetMs,
					)
				}
			}
		}
		if t.Opener && d.OpenerMS > 0 {
			h.metrics.ObserveOpener(float64(d.OpenerMS))
		}
		if outcome.BargeIn {
			h.metrics.IncBargeInsCommitted()
		}
		if outcome.Fallback {
			reason := outcome.FallbackReason
			if reason == "" {
				reason = "unknown"
			}
			h.metrics.IncFallback(reason)
		}
	}

	if h.logger != nil {
		h.logger.Info("turn timing complete",
			"session_id", t.SessionID,
			"turn_id", turnID,
			"opener", t.Opener,
			"asr_ms", d.ASRMS,
			"endpoint_ms", d.EndpointMS,
			"engine_ms", d.EngineMS,
			"tts_ms", d.TTSMS,
			"mouth_to_ear_ms", d.MouthToEarMS,
			"opener_ms", d.OpenerMS,
			"playback_tail_ms", d.PlaybackTailMS,
			"disposition", outcome.Disposition,
			"end_call", outcome.EndCall,
			"fallback", outcome.Fallback,
			"fallback_reason", outcome.FallbackReason,
			"barge_in", outcome.BargeIn,
		)
	}
}

// ActiveTurnID returns the in-flight reply turn ID for egress/watchdog correlation.
func (h *TurnTimingHub) ActiveTurnID() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.activeTurnID
}

// SetTurnOutcome stores outcome metadata before playback completes.
func (h *TurnTimingHub) SetTurnOutcome(turnID string, outcome TurnOutcome) {
	if h == nil || turnID == "" {
		return
	}
	h.mu.Lock()
	t := h.byTurnID[turnID]
	h.mu.Unlock()
	if t != nil {
		t.SetOutcome(outcome)
	}
}
