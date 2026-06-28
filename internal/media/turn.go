package media

import (
	"context"
	"encoding/binary"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultSilenceMsYesNo         = 400
	defaultSilenceMsDefault       = 850
	defaultSilenceMsSpelled       = 1200
	defaultShortFragmentSilenceMs = 950
	defaultShortFragmentMaxWords  = 2
	defaultMaxUtteranceMs         = 30000
	defaultEnergyVADThreshold     = 800.0
)

// TurnKind categorizes turn-manager events surfaced to downstream consumers.
type TurnKind int

const (
	TurnSpeechStarted TurnKind = iota
	TurnEndOfTurn
	TurnInterrupt
)

func (k TurnKind) String() string {
	switch k {
	case TurnSpeechStarted:
		return "speech_started"
	case TurnEndOfTurn:
		return "end_of_turn"
	case TurnInterrupt:
		return "interrupt"
	default:
		return "unknown"
	}
}

// TurnEvent signals turn lifecycle milestones for engine dispatch (CT-8).
type TurnEvent struct {
	Kind       TurnKind
	Transcript string
	FlowClass  FlowClass
	Forced     bool
}

// TurnListener receives turn events from TurnManager.
type TurnListener interface {
	OnTurnEvent(ctx context.Context, session *Session, event TurnEvent)
}

// FlowClass selects per-flow end-of-utterance silence thresholds.
type FlowClass string

const (
	FlowYesNo        FlowClass = "yes_no"
	FlowDefault      FlowClass = "default"
	FlowSpelledInput FlowClass = "spelled_input"
)

// EndpointConfig holds silence and utterance-cap settings per flow class.
type EndpointConfig struct {
	SilenceMs              map[FlowClass]int
	DefaultSilenceMs       int
	ShortFragmentSilenceMs int
	ShortFragmentMaxWords  int
	MaxUtteranceMs         int
}

// DefaultEndpointConfig returns CT-6 baseline endpointing thresholds.
func DefaultEndpointConfig() EndpointConfig {
	return EndpointConfig{
		SilenceMs: map[FlowClass]int{
			FlowYesNo:        defaultSilenceMsYesNo,
			FlowDefault:      defaultSilenceMsDefault,
			FlowSpelledInput: defaultSilenceMsSpelled,
		},
		DefaultSilenceMs:       defaultSilenceMsDefault,
		ShortFragmentSilenceMs: defaultShortFragmentSilenceMs,
		ShortFragmentMaxWords:  defaultShortFragmentMaxWords,
		MaxUtteranceMs:         defaultMaxUtteranceMs,
	}
}

func (c EndpointConfig) withDefaults() EndpointConfig {
	if c.SilenceMs == nil {
		c.SilenceMs = DefaultEndpointConfig().SilenceMs
	}
	if c.DefaultSilenceMs <= 0 {
		c.DefaultSilenceMs = defaultSilenceMsDefault
	}
	if c.ShortFragmentSilenceMs <= 0 {
		c.ShortFragmentSilenceMs = defaultShortFragmentSilenceMs
	}
	if c.ShortFragmentMaxWords <= 0 {
		c.ShortFragmentMaxWords = defaultShortFragmentMaxWords
	}
	if c.MaxUtteranceMs <= 0 {
		c.MaxUtteranceMs = defaultMaxUtteranceMs
	}
	return c
}

func (c EndpointConfig) silenceFor(class FlowClass) time.Duration {
	ms, ok := c.SilenceMs[class]
	if !ok || ms <= 0 {
		ms = c.DefaultSilenceMs
	}
	return time.Duration(ms) * time.Millisecond
}

// SilenceForTranscript returns endpoint silence tuned for natural pacing; short fragments
// (e.g. a name spoken with a mid-word pause) wait slightly longer before EndOfTurn.
func (c EndpointConfig) SilenceForTranscript(class FlowClass, transcript string) time.Duration {
	base := c.silenceFor(class)
	if class != FlowDefault {
		return base
	}
	text := strings.TrimSpace(transcript)
	if text == "" {
		return base
	}
	maxWords := c.ShortFragmentMaxWords
	if maxWords <= 0 {
		maxWords = defaultShortFragmentMaxWords
	}
	if len(strings.Fields(text)) >= maxWords {
		return base
	}
	shortMs := c.ShortFragmentSilenceMs
	if shortMs <= 0 {
		shortMs = defaultShortFragmentSilenceMs
	}
	if shortMs > int(base/time.Millisecond) {
		return time.Duration(shortMs) * time.Millisecond
	}
	return base
}

// EndpointConfigFromEnv loads endpoint thresholds from environment variables.
func EndpointConfigFromEnv() EndpointConfig {
	cfg := DefaultEndpointConfig()
	if v := os.Getenv("SILENCE_MS_YESNO"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			cfg.SilenceMs[FlowYesNo] = ms
		}
	}
	if v := os.Getenv("SILENCE_MS_DEFAULT"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			cfg.SilenceMs[FlowDefault] = ms
			cfg.DefaultSilenceMs = ms
		}
	}
	if v := os.Getenv("SILENCE_MS_SPELLED"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			cfg.SilenceMs[FlowSpelledInput] = ms
		}
	}
	if v := os.Getenv("SILENCE_MS_SHORT_FRAGMENT"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			cfg.ShortFragmentSilenceMs = ms
		}
	}
	if v := os.Getenv("MAX_UTTERANCE_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			cfg.MaxUtteranceMs = ms
		}
	}
	return cfg.withDefaults()
}

// Clock abstracts time for deterministic endpointing tests.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) TimerHandle
}

// TimerHandle cancels a scheduled callback.
type TimerHandle interface {
	Stop() bool
}

// RealClock uses the system clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) AfterFunc(d time.Duration, f func()) TimerHandle {
	return realTimer{timer: time.AfterFunc(d, f)}
}

type realTimer struct {
	timer *time.Timer
}

func (t realTimer) Stop() bool { return t.timer.Stop() }

// LocalVAD detects speech in PCM16 frames for fast local interrupt hints.
type LocalVAD interface {
	IsSpeech(pcm16 []byte, rate int) bool
	Close() error
}

// NoopVAD never detects speech (default when local VAD is disabled).
type NoopVAD struct{}

func (NoopVAD) IsSpeech(_ []byte, _ int) bool { return false }
func (NoopVAD) Close() error                  { return nil }

// EnergyVAD is a pure-Go RMS energy baseline for local speech detection.
type EnergyVAD struct {
	ThresholdRMS float64
}

// NewEnergyVAD returns an RMS-based local VAD.
func NewEnergyVAD(thresholdRMS float64) *EnergyVAD {
	if thresholdRMS <= 0 {
		thresholdRMS = defaultEnergyVADThreshold
	}
	return &EnergyVAD{ThresholdRMS: thresholdRMS}
}

func (v *EnergyVAD) IsSpeech(pcm16 []byte, rate int) bool {
	_ = rate
	if len(pcm16) < 2 {
		return false
	}
	var sumSquares float64
	samples := len(pcm16) / 2
	for i := 0; i < samples; i++ {
		sample := float64(int16(binary.LittleEndian.Uint16(pcm16[i*2:])))
		sumSquares += sample * sample
	}
	rms := math.Sqrt(sumSquares / float64(samples))
	return rms >= v.ThresholdRMS
}

func (v *EnergyVAD) Close() error { return nil }

// SileroVAD is a seam for an out-of-process Silero ONNX worker (CT-6 stub, disabled by default).
type SileroVAD struct {
	enabled bool
}

// NewSileroVAD returns a Silero worker seam; disabled instances behave like NoopVAD.
func NewSileroVAD(enabled bool) *SileroVAD {
	return &SileroVAD{enabled: enabled}
}

func (v *SileroVAD) IsSpeech(_ []byte, _ int) bool {
	if !v.enabled {
		return false
	}
	return false
}

func (v *SileroVAD) Close() error { return nil }

// NewLocalVAD selects the configured local VAD implementation.
func NewLocalVAD(enabled bool, useSilero bool) LocalVAD {
	if !enabled {
		return NoopVAD{}
	}
	if useSilero {
		return NewSileroVAD(true)
	}
	return NewEnergyVAD(defaultEnergyVADThreshold)
}

// LocalVADConfigFromEnv loads local VAD settings.
func LocalVADConfigFromEnv() (enabled bool, useSilero bool) {
	if v := os.Getenv("LOCAL_VAD_ENABLED"); v == "1" || v == "true" || v == "TRUE" {
		enabled = true
	}
	if v := os.Getenv("LOCAL_VAD_SILERO"); v == "1" || v == "true" || v == "TRUE" {
		useSilero = true
	}
	return enabled, useSilero
}

// LoggingTurnListener logs turn events.
type LoggingTurnListener struct {
	logger *slog.Logger
}

// NewLoggingTurnListener returns a listener that logs turn events.
func NewLoggingTurnListener(logger *slog.Logger) *LoggingTurnListener {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingTurnListener{logger: logger}
}

func (l *LoggingTurnListener) OnTurnEvent(_ context.Context, session *Session, event TurnEvent) {
	if l.logger == nil {
		return
	}
	l.logger.Info("turn event",
		"stream_sid", session.StreamSID,
		"kind", event.Kind.String(),
		"transcript", event.Transcript,
		"flow_class", string(event.FlowClass),
		"forced", event.Forced,
	)
}
