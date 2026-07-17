package translator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"websocket/internal/mayura"
	"websocket/internal/media"
)

// LaneDirection identifies which party speaks on the source leg.
type LaneDirection int

const (
	DirectionAToB LaneDirection = iota
	DirectionBToA
)

func (d LaneDirection) sourceRole() LegRole {
	if d == DirectionBToA {
		return LegB
	}
	return LegA
}

func (d LaneDirection) targetRole() LegRole {
	if d == DirectionBToA {
		return LegA
	}
	return LegB
}

func (d LaneDirection) turnPrefix() string {
	if d == DirectionBToA {
		return "b2a"
	}
	return "a2b"
}

// latencyBridge wraps a TranscriptConsumer to stamp speech_end and asr_final marks.
type latencyBridge struct {
	inner          media.TranscriptConsumer
	tracker        *LatencyTracker
	beginTurn      func() string
	currentTurn    func() string
	onSpeechStart  func()
	onASRFinal     func()
}

func (b *latencyBridge) OnPartial(ctx context.Context, session *media.Session, transcript media.Transcript) {
	b.inner.OnPartial(ctx, session, transcript)
}

func (b *latencyBridge) OnFinal(ctx context.Context, session *media.Session, transcript media.Transcript) {
	if transcript.Text != "" && b.onASRFinal != nil {
		b.onASRFinal()
	}
	if b.tracker != nil && b.currentTurn != nil {
		if id := b.currentTurn(); id != "" {
			b.tracker.MarkASRFinal(id, time.Now())
		}
	}
	b.inner.OnFinal(ctx, session, transcript)
}

func (b *latencyBridge) OnSpeechStart(ctx context.Context, session *media.Session) {
	if b.onSpeechStart != nil {
		b.onSpeechStart()
	}
	b.inner.OnSpeechStart(ctx, session)
}

func (b *latencyBridge) OnSpeechEnd(ctx context.Context, session *media.Session) {
	turnID := ""
	if b.beginTurn != nil {
		turnID = b.beginTurn()
	}
	if b.tracker != nil && turnID != "" {
		b.tracker.MarkSpeechEnd(turnID, time.Now())
	}
	b.inner.OnSpeechEnd(ctx, session)
}

var _ media.TranscriptConsumer = (*latencyBridge)(nil)

func buildIngressPipeline(
	asrProvider media.ASRProvider,
	consumer media.TranscriptConsumer,
	sampleRate int,
	logger *slog.Logger,
) media.AudioSink {
	target := media.TargetFormat{SampleRate: sampleRate, Channels: 1}
	return media.NewTranscodeSink(
		media.NewASRSink(asrProvider, consumer, sampleRate, logger),
		target,
		20,
		logger,
	)
}

func translatorEndpointConfig(silenceMs int) media.EndpointConfig {
	if silenceMs <= 0 {
		silenceMs = defaultSilenceMsDefault
	}
	return media.EndpointConfig{
		SilenceMs: map[media.FlowClass]int{
			media.FlowYesNo:        300,
			media.FlowDefault:      silenceMs,
			media.FlowSpelledInput: 600,
		},
		DefaultSilenceMs: silenceMs,
		MaxUtteranceMs:   8000,
	}
}

func newMediaSession(streamSID string, sampleRate int, params map[string]string) *media.Session {
	if params == nil {
		params = map[string]string{}
	}
	return &media.Session{
		StreamSID: streamSID,
		CallSID:   streamSID,
		Format: media.AudioFormat{
			Encoding:   "audio/x-l16",
			SampleRate: sampleRate,
			Channels:   1,
		},
		Params: params,
	}
}

func openLaneTTS(
	ctx context.Context,
	provider media.TTSProvider,
	base media.TTSConfig,
	session *media.Session,
	legOutRate int,
	synthRate int,
	logger *slog.Logger,
) (media.TTSStream, int, error) {
	if legOutRate <= 0 {
		legOutRate = 8000
	}
	providerName := strings.ToLower(strings.TrimSpace(base.Provider))
	if providerName == "sarvam" {
		return openSarvamTTS(ctx, provider, base, session, logger)
	}
	if synthRate <= 0 {
		synthRate = defaultTTSSynthRate
	}
	format := media.ElevenLabsPCMFormatForRate(synthRate)
	sourceRate := media.SampleRateFromPCMFormat(format)
	meta := media.TTSSessionMeta{
		StreamSID:        session.StreamSID,
		CallSID:          session.CallSID,
		Params:           session.Params,
		OutputSampleRate: synthRate,
		OutputFormat:     format,
	}
	if logger != nil {
		logger.Info("translator tts session",
			"stream_sid", session.StreamSID,
			"provider", "elevenlabs",
			"declared_leg_output_rate", legOutRate,
			"synth_sample_rate", sourceRate,
			"output_format", format,
		)
	}
	stream, err := provider.Open(ctx, meta)
	if err != nil {
		return nil, 0, err
	}
	if sourceRate != legOutRate {
		stream = media.NewResamplingTTSStream(stream, sourceRate, legOutRate, logger, session.StreamSID)
	}
	return stream, legOutRate, nil
}

func openSarvamTTS(
	ctx context.Context,
	provider media.TTSProvider,
	base media.TTSConfig,
	session *media.Session,
	logger *slog.Logger,
) (media.TTSStream, int, error) {
	_ = base
	declared := media.OutputSampleRateFromParams(session.Params)
	if declared <= 0 {
		declared = 8000
	}
	format := "pcm_8000"
	if declared > 8000 {
		format = media.ElevenLabsPCMFormatForRate(declared)
	}
	sourceRate := media.SampleRateFromPCMFormat(format)
	meta := media.TTSSessionMeta{
		StreamSID:        session.StreamSID,
		CallSID:          session.CallSID,
		Params:           session.Params,
		OutputSampleRate: declared,
		OutputFormat:     format,
	}
	if logger != nil {
		logger.Info("translator tts session",
			"stream_sid", session.StreamSID,
			"declared_output_sample_rate", declared,
			"sarvam_request_rate", sourceRate,
			"output_format", format,
		)
	}
	stream, err := provider.Open(ctx, meta)
	if err != nil {
		return nil, 0, err
	}
	stream = media.NewResamplingTTSStream(stream, sourceRate, declared, logger, session.StreamSID)
	return stream, declared, nil
}

// LaneDeps groups shared server dependencies for lane construction.
type LaneDeps struct {
	ASRProvider       media.ASRProvider
	TTSProvider       media.TTSProvider
	TTSConfig         media.TTSConfig
	Mayura            MayuraTranslator
	Logger            *slog.Logger
	EndpointSilenceMs int
	LaneMode          LaneMode
	ASRSampleRate     int
	TTSSynthRate      int
	IncompleteExtraMs int
	FillerLexiconPath string
}

// LaneHooks wires room-level coordinator and loop-breaker callbacks.
type LaneHooks struct {
	OnSpeechStart       func(source LegRole)
	OnTTSStarted        func(source LegRole)
	OnPlaybackComplete  func(target LegRole, playbackEnd time.Time)
	OnTranslationDone   func(source LegRole, sourceText, translatedText string)
}

// translationLane is one direction of the processing pipeline (translate or echo).
type translationLane struct {
	direction   LaneDirection
	sourceLeg   *leg
	targetLeg   *leg
	sourceSess  *media.Session
	targetSess  *media.Session
	sink        media.AudioSink
	turnManager *media.TurnManager
	listener    media.TurnListener
	ttsPlayer   *ttsPlayer
	egress      *legEgress
	latency     *LatencyTracker
	failOpen    atomic.Bool
	turnSeq     atomic.Uint64
	pendingTurn atomic.Value
	mode        LaneMode
	audit       *TurnAudit
	filler      *FillerLexicon

	sourceLang string
	targetLang string
}

// aToBLane is kept as an alias for 3b-i call sites and tests.
type aToBLane = translationLane

func newAToBLane(
	deps LaneDeps,
	sourceLeg, targetLeg *leg,
	sourceLang, targetLang string,
	hooks *LaneHooks,
) (*translationLane, error) {
	return newTranslationLane(DirectionAToB, deps, sourceLeg, targetLeg, sourceLang, targetLang, deps.TTSConfig, hooks)
}

func newBToALane(
	deps LaneDeps,
	sourceLeg, targetLeg *leg,
	sourceLang, targetLang string,
	ttsCfg media.TTSConfig,
	hooks LaneHooks,
) (*translationLane, error) {
	return newTranslationLane(DirectionBToA, deps, sourceLeg, targetLeg, sourceLang, targetLang, ttsCfg, &hooks)
}

func newTranslationLane(
	direction LaneDirection,
	deps LaneDeps,
	sourceLeg, targetLeg *leg,
	sourceLang, targetLang string,
	ttsCfg media.TTSConfig,
	hooks *LaneHooks,
) (*translationLane, error) {
	if sourceLang == "" {
		sourceLang = "hi-IN"
	}
	if targetLang == "" {
		targetLang = "en-IN"
	}

	lane := &translationLane{
		direction:  direction,
		sourceLeg:  sourceLeg,
		targetLeg:  targetLeg,
		sourceLang: sourceLang,
		targetLang: targetLang,
		latency:    NewLatencyTracker(deps.Logger),
		mode:       deps.LaneMode,
	}
	if lane.mode == "" {
		lane.mode = LaneModeTranslate
	}

	asrRate := deps.ASRSampleRate
	if asrRate <= 0 {
		asrRate = defaultASRSampleRate
	}
	legInRate := sourceLeg.rates.inputRate
	if legInRate <= 0 {
		legInRate = 8000
	}

	lane.targetSess = newMediaSession(targetLeg.sessionID, targetLeg.rates.outputRate, map[string]string{
		"output_sample_rate": itoa(targetLeg.rates.outputRate),
		"lang_b":             targetLang,
	})
	lane.sourceSess = newMediaSession(sourceLeg.sessionID, legInRate, map[string]string{
		"asr_language": sourceLang,
		"lang_a":       sourceLang,
	})

	if deps.Logger != nil {
		deps.Logger.Info("translator_lane_audio_path",
			"direction", lane.direction.turnPrefix(),
			"mode", lane.mode,
			"leg_input_rate_hz", legInRate,
			"asr_target_rate_hz", asrRate,
			"leg_output_rate_hz", targetLeg.rates.outputRate,
			"tts_provider", ttsCfg.Provider,
			"tts_synth_rate_hz", deps.TTSSynthRate,
		)
	}

	sourceRole := direction.sourceRole()
	targetRole := direction.targetRole()

	var hook LaneHooks
	if hooks != nil {
		hook = *hooks
	}

	lane.egress = newLegEgress(
		targetLeg,
		targetLeg.rates.outputRate,
		20,
		func(turnID string, at time.Time) {
			lane.latency.MarkEgressFirst(turnID, at)
			lane.latency.Finish(turnID, false, "")
		},
		func(turnID string, playbackEnd time.Time) {
			if hook.OnPlaybackComplete != nil {
				hook.OnPlaybackComplete(targetRole, playbackEnd)
			}
		},
		deps.Logger,
	)

	ttsStream, _, err := openLaneTTS(
		context.Background(),
		deps.TTSProvider,
		ttsCfg,
		lane.targetSess,
		targetLeg.rates.outputRate,
		deps.TTSSynthRate,
		deps.Logger,
	)
	if err != nil {
		return nil, err
	}
	lane.ttsPlayer = newTTSPlayer(ttsStream, lane.egress, lane.targetSess, func(turnID string) {
		lane.latency.MarkTTSFirstByte(turnID, time.Now())
	}, deps.Logger)

	lane.audit = NewTurnAudit(lane.direction.turnPrefix(), deps.Logger)
	langBase := baseLang(sourceLang)
	lane.filler = NewFillerLexicon(langBase, deps.FillerLexiconPath)

	emitTranslation := func() {
		if lane.audit != nil {
			lane.audit.RecordTranslation()
		}
	}

	switch lane.mode {
	case LaneModeEcho:
		lane.listener = NewEchoListener(EchoListenerConfig{
			TTS:        lane.ttsPlayer,
			SourceLang: sourceLang,
			Latency:    lane.latency,
			FailOpen:   &lane.failOpen,
			Logger:     deps.Logger,
			TurnID:     lane.turnIDForEndOfTurn,
			OnTTSStart: func() {
				if hook.OnTTSStarted != nil {
					hook.OnTTSStarted(sourceRole)
				}
			},
			OnTranslationEmitted: emitTranslation,
		})
	default:
		lane.listener = NewTranslateListener(TranslateListenerConfig{
			Mayura:     deps.Mayura,
			TTS:        lane.ttsPlayer,
			SourceLang: sourceLang,
			TargetLang: targetLang,
			Latency:    lane.latency,
			FailOpen:   &lane.failOpen,
			Logger:     deps.Logger,
			TurnID:     lane.turnIDForEndOfTurn,
			TargetSID:  targetLeg.sessionID,
			OnTTSStart: func() {
				if hook.OnTTSStarted != nil {
					hook.OnTTSStarted(sourceRole)
				}
			},
			OnTranslationDone: func(sourceText, translatedText string) {
				if hook.OnTranslationDone != nil {
					hook.OnTranslationDone(sourceRole, sourceText, translatedText)
				}
			},
			OnTranslationEmitted: emitTranslation,
		})
	}

	lane.turnManager = media.NewTurnManager(
		lane.listener,
		translatorEndpointConfig(deps.EndpointSilenceMs),
		media.RealClock{},
		media.NoopVAD{},
		media.NoopSemanticTurn{},
		media.SemanticTurnConfig{Enabled: false},
		media.NoopBackchannel{},
		deps.Logger,
	)
	incompleteExtra := deps.IncompleteExtraMs
	if incompleteExtra <= 0 {
		incompleteExtra = defaultIncompleteExtraMS
	}
	lane.turnManager.SetTurnPolicy(media.TurnPolicy{
		CompletenessLang:  langBase,
		IncompleteExtraMs: incompleteExtra,
		FillerSuppress: func(text string) bool {
			return lane.filler.IsPureFiller(langBase, text)
		},
		OnFillerSuppressed: func(text string) {
			if lane.audit != nil {
				lane.audit.RecordFillerSuppressed(text)
			}
		},
		OnTurnMerged: func() {
			if lane.audit != nil {
				lane.audit.RecordMerged()
			}
		},
	}, lane.filler.Words(langBase))

	bridge := &latencyBridge{
		inner:       lane.turnManager,
		tracker:     lane.latency,
		beginTurn:   lane.beginTurn,
		currentTurn: lane.readTurnID,
		onASRFinal: func() {
			if lane.audit != nil {
				lane.audit.RecordASRFinal()
			}
		},
		onSpeechStart: func() {
			if hook.OnSpeechStart != nil {
				hook.OnSpeechStart(sourceRole)
			}
		},
	}
	lane.sink = buildIngressPipeline(deps.ASRProvider, bridge, asrRate, deps.Logger)

	if err := lane.sink.OnStart(context.Background(), lane.sourceSess); err != nil {
		_ = lane.Close()
		return nil, err
	}
	return lane, nil
}

func (l *translationLane) beginTurn() string {
	n := l.turnSeq.Add(1)
	id := fmt.Sprintf("%s-%s-%d", l.direction.turnPrefix(), l.sourceSess.StreamSID, n)
	l.pendingTurn.Store(id)
	if l.latency != nil {
		l.latency.BeginTurn(id, l.sourceSess.StreamSID, l.targetSess.StreamSID)
	}
	return id
}

func (l *translationLane) turnIDForEndOfTurn() string {
	if id := l.readTurnID(); id != "" {
		return id
	}
	return l.beginTurn()
}

func (l *translationLane) readTurnID() string {
	if v := l.pendingTurn.Load(); v != nil {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func (l *translationLane) sourceRole() LegRole {
	return l.direction.sourceRole()
}

func (l *translationLane) ingestAudio(ctx context.Context, pcm []byte) error {
	if l.sink == nil || l.sourceSess == nil {
		return nil
	}
	return l.sink.OnAudio(ctx, l.sourceSess, pcm)
}

func (l *translationLane) failOpenActive() bool {
	return l.failOpen.Load()
}

func (l *translationLane) Close() error {
	if l.audit != nil {
		l.audit.VerifyAndLog()
	}
	if l.sink != nil && l.sourceSess != nil {
		_ = l.sink.OnStop(context.Background(), l.sourceSess)
	}
	if l.ttsPlayer != nil {
		_ = l.ttsPlayer.Close()
	}
	return nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

var _ MayuraTranslator = (*mayura.Client)(nil)
