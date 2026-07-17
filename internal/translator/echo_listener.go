package translator

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"websocket/internal/media"
)

// EchoListener implements media.TurnListener for Hindi→Hindi passthrough (no Mayura).
// On TurnEndOfTurn: ASR transcript → TTS same text → peer leg egress.
type EchoListener struct {
	tts        *ttsPlayer
	sourceLang string
	latency    *LatencyTracker
	failOpen   *atomic.Bool
	logger     *slog.Logger
	turnID     func() string
	onTTSStart func()
}

// EchoListenerConfig wires the echo lane listener.
type EchoListenerConfig struct {
	TTS        *ttsPlayer
	SourceLang string
	Latency    *LatencyTracker
	FailOpen   *atomic.Bool
	Logger     *slog.Logger
	TurnID     func() string
	OnTTSStart func()
}

// NewEchoListener constructs the A→B echo listener.
func NewEchoListener(cfg EchoListenerConfig) *EchoListener {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.FailOpen == nil {
		var v atomic.Bool
		cfg.FailOpen = &v
	}
	return &EchoListener{
		tts:        cfg.TTS,
		sourceLang: cfg.SourceLang,
		latency:    cfg.Latency,
		failOpen:   cfg.FailOpen,
		logger:     cfg.Logger,
		turnID:     cfg.TurnID,
		onTTSStart: cfg.OnTTSStart,
	}
}

func (l *EchoListener) OnTurnEvent(ctx context.Context, session *media.Session, event media.TurnEvent) {
	if event.Kind != media.TurnEndOfTurn {
		return
	}
	text := event.Transcript
	if text == "" {
		return
	}
	turnID := ""
	if l.turnID != nil {
		turnID = l.turnID()
	}
	if turnID == "" {
		turnID = fmt.Sprintf("echo-%d", time.Now().UnixNano())
	}
	if l.latency != nil {
		l.latency.SetSourceText(turnID, text)
	}
	go l.handleUtterance(ctx, session, turnID, text)
}

func (l *EchoListener) handleUtterance(ctx context.Context, session *media.Session, turnID, text string) {
	if l.tts == nil {
		l.enableFailOpen(turnID, "unconfigured")
		return
	}
	// Passthrough mark keeps latency buckets comparable (asr_final_to_mayura ≈ 0).
	if l.latency != nil {
		l.latency.MarkMayuraDone(turnID, time.Now(), text)
	}
	if l.onTTSStart != nil {
		l.onTTSStart()
	}
	if err := l.tts.Speak(ctx, turnID, text); err != nil {
		l.logger.Warn("echo tts speak failed; fail-open relay",
			"stream_sid", session.StreamSID,
			"turn_id", turnID,
			"error", err,
		)
		l.enableFailOpen(turnID, "tts")
		return
	}
	if l.logger != nil {
		l.logger.Info("translator_utterance_ok",
			"turn_id", turnID,
			"mode", "echo",
			"source_lang", l.sourceLang,
			"target_lang", l.sourceLang,
		)
	}
}

func (l *EchoListener) enableFailOpen(turnID, stage string) {
	l.failOpen.Store(true)
	if l.latency != nil {
		l.latency.Finish(turnID, true, stage)
	}
}

var _ media.TurnListener = (*EchoListener)(nil)
