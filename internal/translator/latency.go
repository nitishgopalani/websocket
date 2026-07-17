package translator

import (
	"log/slog"
	"sync"
	"time"
)

// utteranceLatency records per-utterance stage timings for 3b-i instrumentation.
type utteranceLatency struct {
	TurnID           string
	SourceSessionID  string
	TargetSessionID  string
	SourceText       string
	TranslatedText   string
	SpeechEndAt      time.Time
	ASRFinalAt       time.Time
	MayuraDoneAt     time.Time
	TTSFirstByteAt   time.Time
	EgressFirstAt    time.Time
	FailOpen         bool
	ErrorStage       string
}

func (u *utteranceLatency) speechEndToEgressMs() int64 {
	if u.SpeechEndAt.IsZero() || u.EgressFirstAt.IsZero() {
		return -1
	}
	return u.EgressFirstAt.Sub(u.SpeechEndAt).Milliseconds()
}

func (u *utteranceLatency) asrFinalToMayuraMs() int64 {
	if u.ASRFinalAt.IsZero() || u.MayuraDoneAt.IsZero() {
		return -1
	}
	return u.MayuraDoneAt.Sub(u.ASRFinalAt).Milliseconds()
}

func (u *utteranceLatency) mayuraToTTSFirstMs() int64 {
	if u.MayuraDoneAt.IsZero() || u.TTSFirstByteAt.IsZero() {
		return -1
	}
	return u.TTSFirstByteAt.Sub(u.MayuraDoneAt).Milliseconds()
}

func (u *utteranceLatency) ttsToEgressMs() int64 {
	if u.TTSFirstByteAt.IsZero() || u.EgressFirstAt.IsZero() {
		return -1
	}
	return u.EgressFirstAt.Sub(u.TTSFirstByteAt).Milliseconds()
}

// LatencyTracker accumulates per-utterance marks for one A→B lane.
type LatencyTracker struct {
	logger *slog.Logger
	mu     sync.Mutex
	active map[string]*utteranceLatency
	done   []*utteranceLatency
}

func NewLatencyTracker(logger *slog.Logger) *LatencyTracker {
	if logger == nil {
		logger = slog.Default()
	}
	return &LatencyTracker{
		logger: logger,
		active: make(map[string]*utteranceLatency),
	}
}

func (t *LatencyTracker) BeginTurn(turnID, sourceSessionID, targetSessionID string) *utteranceLatency {
	u := &utteranceLatency{
		TurnID:          turnID,
		SourceSessionID: sourceSessionID,
		TargetSessionID: targetSessionID,
	}
	t.mu.Lock()
	t.active[turnID] = u
	t.mu.Unlock()
	return u
}

func (t *LatencyTracker) MarkSpeechEnd(turnID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if u := t.active[turnID]; u != nil && u.SpeechEndAt.IsZero() {
		u.SpeechEndAt = at
	}
}

func (t *LatencyTracker) MarkASRFinal(turnID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if u := t.active[turnID]; u != nil && u.ASRFinalAt.IsZero() {
		u.ASRFinalAt = at
	}
}

func (t *LatencyTracker) MarkMayuraDone(turnID string, at time.Time, translated string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if u := t.active[turnID]; u != nil {
		u.MayuraDoneAt = at
		u.TranslatedText = translated
	}
}

func (t *LatencyTracker) MarkTTSFirstByte(turnID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if u := t.active[turnID]; u != nil && u.TTSFirstByteAt.IsZero() {
		u.TTSFirstByteAt = at
	}
}

func (t *LatencyTracker) MarkEgressFirst(turnID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if u := t.active[turnID]; u != nil && u.EgressFirstAt.IsZero() {
		u.EgressFirstAt = at
	}
}

func (t *LatencyTracker) SetSourceText(turnID, text string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if u := t.active[turnID]; u != nil {
		u.SourceText = text
	}
}

func (t *LatencyTracker) Finish(turnID string, failOpen bool, errStage string) {
	t.mu.Lock()
	u := t.active[turnID]
	if u == nil {
		t.mu.Unlock()
		return
	}
	u.FailOpen = failOpen
	u.ErrorStage = errStage
	delete(t.active, turnID)
	t.done = append(t.done, u)
	t.mu.Unlock()
	t.logUtterance(u)
}

func (t *LatencyTracker) logUtterance(u *utteranceLatency) {
	if t.logger == nil {
		return
	}
	t.logger.Info("translator_utterance_latency",
		"turn_id", u.TurnID,
		"source_session", u.SourceSessionID,
		"target_session", u.TargetSessionID,
		"source_text", u.SourceText,
		"translated_text", u.TranslatedText,
		"speech_end_to_egress_ms", u.speechEndToEgressMs(),
		"asr_final_to_mayura_ms", u.asrFinalToMayuraMs(),
		"mayura_to_tts_first_ms", u.mayuraToTTSFirstMs(),
		"tts_to_egress_ms", u.ttsToEgressMs(),
		"fail_open", u.FailOpen,
		"error_stage", u.ErrorStage,
	)
}

// Completed returns finished utterance records (tests).
func (t *LatencyTracker) Completed() []*utteranceLatency {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*utteranceLatency, len(t.done))
	copy(out, t.done)
	return out
}
