package media

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"
)

const (
	defaultSarvamEndpoint     = "wss://api.sarvam.ai/speech-to-text/ws"
	defaultASRModel           = "saaras:v3"
	defaultASRMode            = "transcribe"
	defaultASRLanguage        = "unknown"
	defaultASRKeepalivePeriod = 25 * time.Second
	defaultASRReconnectBase   = 1 * time.Second
	defaultASRReconnectMax    = 30 * time.Second
	defaultASRMaxReconnects   = 5
	defaultASREventBuffer     = 64
	defaultASRReconnectBuffer = 8
)

var (
	ErrASRSessionClosed = errors.New("asr session closed")
	ErrASRNotConfigured = errors.New("asr enabled but SARVAM_API_KEY is not set")
)

// Transcript is a speech-to-text result from an ASR provider.
type Transcript struct {
	Text    string
	IsFinal bool
}

// ASREventType categorizes streaming ASR events.
type ASREventType int

const (
	ASREventPartial ASREventType = iota
	ASREventFinal
	ASREventSpeechStart
	ASREventSpeechEnd
	ASREventError
)

// ASREvent is emitted on an ASRSession event stream.
type ASREvent struct {
	Type       ASREventType
	Transcript Transcript
	Err        error
}

// ASRSessionMeta carries per-call metadata passed when opening ASR.
type ASRSessionMeta struct {
	StreamSID  string
	CallSID    string
	SampleRate int
	Language   string
	Params     map[string]string
}

// ASRSession streams PCM16 frames to an ASR backend and emits transcript events.
type ASRSession interface {
	SendAudio(pcm16 []byte) error
	Events() <-chan ASREvent
	Close() error
}

// ASRProvider opens a persistent per-session ASR connection.
type ASRProvider interface {
	Open(ctx context.Context, meta ASRSessionMeta) (ASRSession, error)
}

// ASRConfig controls ASR provider selection.
type ASRConfig struct {
	Enabled            bool
	APIKey             string
	Endpoint           string
	Model              string
	Mode               string
	Language           string
	HighVADSensitivity bool
	VADSignals         bool
	KeepalivePeriod    time.Duration
	ReconnectBaseDelay time.Duration
	ReconnectMaxDelay  time.Duration
	MaxReconnects      int
}

// SarvamConfig holds Sarvam-specific streaming settings.
type SarvamConfig struct {
	Endpoint           string
	Model              string
	Mode               string
	Language           string
	HighVADSensitivity bool
	VADSignals         bool
	KeepalivePeriod    time.Duration
	ReconnectBaseDelay time.Duration
	ReconnectMaxDelay  time.Duration
	MaxReconnects      int
}

// DefaultASRConfig returns disabled ASR settings.
func DefaultASRConfig() ASRConfig {
	return ASRConfig{
		Enabled:            false,
		Endpoint:           defaultSarvamEndpoint,
		Model:              defaultASRModel,
		Mode:               defaultASRMode,
		Language:           defaultASRLanguage,
		HighVADSensitivity: true,
		VADSignals:         true,
		KeepalivePeriod:    defaultASRKeepalivePeriod,
		ReconnectBaseDelay: defaultASRReconnectBase,
		ReconnectMaxDelay:  defaultASRReconnectMax,
		MaxReconnects:      defaultASRMaxReconnects,
	}
}

// ASRConfigFromEnv loads ASR settings from environment variables.
func ASRConfigFromEnv() ASRConfig {
	cfg := DefaultASRConfig()
	if v := os.Getenv("ASR_ENABLED"); v == "1" || v == "true" || v == "TRUE" {
		cfg.Enabled = true
	}
	if v := os.Getenv("SARVAM_API_KEY"); v != "" {
		cfg.APIKey = v
	}
	if v := os.Getenv("SARVAM_ENDPOINT"); v != "" {
		cfg.Endpoint = v
	}
	if v := os.Getenv("ASR_MODEL"); v != "" {
		cfg.Model = v
	}
	if v := os.Getenv("ASR_MODE"); v != "" {
		cfg.Mode = v
	}
	if v := os.Getenv("ASR_LANGUAGE"); v != "" {
		cfg.Language = v
	}
	if v := os.Getenv("ASR_HIGH_VAD_SENSITIVITY"); v == "0" || v == "false" || v == "FALSE" {
		cfg.HighVADSensitivity = false
	}
	if v := os.Getenv("ASR_VAD_SIGNALS"); v == "0" || v == "false" || v == "FALSE" {
		cfg.VADSignals = false
	}
	return cfg.withDefaults()
}

func (c ASRConfig) withDefaults() ASRConfig {
	if c.Endpoint == "" {
		c.Endpoint = defaultSarvamEndpoint
	}
	if c.Model == "" {
		c.Model = defaultASRModel
	}
	if c.Mode == "" {
		c.Mode = defaultASRMode
	}
	if c.Language == "" {
		c.Language = defaultASRLanguage
	}
	if c.KeepalivePeriod <= 0 {
		c.KeepalivePeriod = defaultASRKeepalivePeriod
	}
	if c.ReconnectBaseDelay <= 0 {
		c.ReconnectBaseDelay = defaultASRReconnectBase
	}
	if c.ReconnectMaxDelay <= 0 {
		c.ReconnectMaxDelay = defaultASRReconnectMax
	}
	if c.MaxReconnects <= 0 {
		c.MaxReconnects = defaultASRMaxReconnects
	}
	return c
}

func (c ASRConfig) SarvamConfig() SarvamConfig {
	c = c.withDefaults()
	return SarvamConfig{
		Endpoint:           c.Endpoint,
		Model:              c.Model,
		Mode:               c.Mode,
		Language:           c.Language,
		HighVADSensitivity: c.HighVADSensitivity,
		VADSignals:         c.VADSignals,
		KeepalivePeriod:    c.KeepalivePeriod,
		ReconnectBaseDelay: c.ReconnectBaseDelay,
		ReconnectMaxDelay:  c.ReconnectMaxDelay,
		MaxReconnects:      c.MaxReconnects,
	}
}

// NewASRProvider returns Noop when disabled, Sarvam when enabled.
func NewASRProvider(cfg ASRConfig) (ASRProvider, error) {
	cfg = cfg.withDefaults()
	if !cfg.Enabled {
		return NoopASRProvider{}, nil
	}
	if cfg.APIKey == "" {
		return nil, ErrASRNotConfigured
	}
	return NewSarvamASRProvider(cfg.APIKey, cfg.SarvamConfig(), nil), nil
}

// NoopASRProvider discards audio and emits no transcript events.
type NoopASRProvider struct{}

func (NoopASRProvider) Open(_ context.Context, _ ASRSessionMeta) (ASRSession, error) {
	return &noopASRSession{}, nil
}

type noopASRSession struct {
	closed bool
}

func (s *noopASRSession) SendAudio(_ []byte) error {
	if s.closed {
		return ErrASRSessionClosed
	}
	return nil
}

func (s *noopASRSession) Events() <-chan ASREvent {
	ch := make(chan ASREvent)
	close(ch)
	return ch
}

func (s *noopASRSession) Close() error {
	s.closed = true
	return nil
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func intString(v int) string {
	return strconv.Itoa(v)
}
