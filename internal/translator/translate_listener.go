package translator

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"websocket/internal/mayura"
	"websocket/internal/media"
)

// MayuraTranslator translates text via Mayura REST.
type MayuraTranslator interface {
	Translate(ctx context.Context, text, sourceLang, targetLang string) (string, error)
}

// TranslateListener implements media.TurnListener for the A→B lane.
// On TurnEndOfTurn: Mayura hi→en → TTS → peer leg B egress.
type TranslateListener struct {
	mayura     MayuraTranslator
	tts        *ttsPlayer
	sourceLang string
	targetLang string
	latency    *LatencyTracker
	failOpen   *atomic.Bool
	logger            *slog.Logger
	turnID            func() string
	targetSID         string
	onTTSStart        func()
	onTranslationDone func(sourceText, translatedText string)
	onTranslationEmitted func()
}

// TranslateListenerConfig wires the one-direction listener.
type TranslateListenerConfig struct {
	Mayura            MayuraTranslator
	TTS               *ttsPlayer
	SourceLang        string
	TargetLang        string
	Latency           *LatencyTracker
	FailOpen          *atomic.Bool
	Logger            *slog.Logger
	TurnID            func() string
	TargetSID         string
	OnTTSStart        func()
	OnTranslationDone func(sourceText, translatedText string)
	OnTranslationEmitted func()
}

// NewTranslateListener constructs the A→B translation listener.
func NewTranslateListener(cfg TranslateListenerConfig) *TranslateListener {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.FailOpen == nil {
		var v atomic.Bool
		cfg.FailOpen = &v
	}
	return &TranslateListener{
		mayura:            cfg.Mayura,
		tts:               cfg.TTS,
		sourceLang:        cfg.SourceLang,
		targetLang:        cfg.TargetLang,
		latency:           cfg.Latency,
		failOpen:          cfg.FailOpen,
		logger:            cfg.Logger,
		turnID:            cfg.TurnID,
		targetSID:         cfg.TargetSID,
		onTTSStart:        cfg.OnTTSStart,
		onTranslationDone: cfg.OnTranslationDone,
		onTranslationEmitted: cfg.OnTranslationEmitted,
	}
}

func (l *TranslateListener) OnTurnEvent(ctx context.Context, session *media.Session, event media.TurnEvent) {
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
		turnID = fmt.Sprintf("a2b-%d", time.Now().UnixNano())
	}
	if l.latency != nil {
		l.latency.SetSourceText(turnID, text)
	}
	go l.handleUtterance(ctx, session, turnID, text)
}

func (l *TranslateListener) handleUtterance(ctx context.Context, session *media.Session, turnID, text string) {
	if l.mayura == nil || l.tts == nil {
		l.enableFailOpen(turnID, "unconfigured")
		return
	}

	translated, err := l.mayura.Translate(ctx, text, l.sourceLang, l.targetLang)
	if err != nil {
		l.logger.Warn("mayura translate failed; fail-open relay",
			"stream_sid", session.StreamSID,
			"turn_id", turnID,
			"error", err,
		)
		l.enableFailOpen(turnID, "mayura")
		return
	}
	if l.latency != nil {
		l.latency.MarkMayuraDone(turnID, time.Now(), translated)
	}
	if l.onTranslationDone != nil {
		l.onTranslationDone(text, translated)
	}
	if l.onTranslationEmitted != nil {
		l.onTranslationEmitted()
	}

	if l.onTTSStart != nil {
		l.onTTSStart()
	}
	if err := l.tts.Speak(ctx, turnID, translated); err != nil {
		l.logger.Warn("tts speak failed; fail-open relay",
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
			"source_lang", l.sourceLang,
			"target_lang", l.targetLang,
		)
	}
}

func (l *TranslateListener) enableFailOpen(turnID, stage string) {
	l.failOpen.Store(true)
	if l.latency != nil {
		l.latency.Finish(turnID, true, stage)
	}
}

var _ media.TurnListener = (*TranslateListener)(nil)

// Static compile-time check that mayura.Client satisfies MayuraTranslator.
var _ MayuraTranslator = (*mayura.Client)(nil)
