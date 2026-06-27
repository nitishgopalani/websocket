package media

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"
)

const (
	defaultTTSOutputFormat   = "ulaw_8000"
	defaultElevenLabsModel   = "eleven_flash_v2_5"
	defaultElevenLabsVoice   = "21m00Tcm4TlvDq8ikWAM"
	defaultElevenLabsBaseURL = "wss://api.elevenlabs.io"
	defaultTTSReconnectBase  = 500 * time.Millisecond
	defaultTTSReconnectMax   = 4 * time.Second
	defaultTTSAudioBuffer    = 64
)

var ErrTTSNotConfigured = errors.New("tts enabled but ELEVENLABS_API_KEY is not set")

// TTSAudioChunk is one telephony-format audio frame from TTS (μ-law @ 8 kHz by default).
type TTSAudioChunk struct {
	TurnID string
	Seq    int
	MuLaw  []byte
	Final  bool
}

// TTSStream synthesizes speech for a single call session.
type TTSStream interface {
	Speak(turnID string, text string) error
	Cancel(turnID string) error
	Audio() <-chan TTSAudioChunk
	Close() error
}

// TTSSessionMeta carries per-call metadata when opening TTS.
type TTSSessionMeta struct {
	StreamSID string
	CallSID   string
	Params    map[string]string
}

// TTSProvider opens a persistent per-session TTS stream.
type TTSProvider interface {
	Open(ctx context.Context, meta TTSSessionMeta) (TTSStream, error)
}

// TTSConfig controls TTS provider selection.
type TTSConfig struct {
	Enabled        bool
	APIKey         string
	VoiceID        string
	Model          string
	OutputFormat   string
	BaseURL        string
	ReconnectBase  time.Duration
	ReconnectMax   time.Duration
	InactivitySecs int
}

// DefaultTTSConfig returns disabled TTS settings.
func DefaultTTSConfig() TTSConfig {
	return TTSConfig{
		Enabled:        false,
		VoiceID:        defaultElevenLabsVoice,
		Model:          defaultElevenLabsModel,
		OutputFormat:   defaultTTSOutputFormat,
		BaseURL:        defaultElevenLabsBaseURL,
		ReconnectBase:  defaultTTSReconnectBase,
		ReconnectMax:   defaultTTSReconnectMax,
		InactivitySecs: 20,
	}
}

func (c TTSConfig) withDefaults() TTSConfig {
	if c.VoiceID == "" {
		c.VoiceID = defaultElevenLabsVoice
	}
	if c.Model == "" {
		c.Model = defaultElevenLabsModel
	}
	if c.OutputFormat == "" {
		c.OutputFormat = defaultTTSOutputFormat
	}
	if c.BaseURL == "" {
		c.BaseURL = defaultElevenLabsBaseURL
	}
	if c.ReconnectBase <= 0 {
		c.ReconnectBase = defaultTTSReconnectBase
	}
	if c.ReconnectMax <= 0 {
		c.ReconnectMax = defaultTTSReconnectMax
	}
	if c.InactivitySecs <= 0 {
		c.InactivitySecs = 20
	}
	return c
}

// TTSConfigFromEnv loads TTS settings from environment variables.
func TTSConfigFromEnv() TTSConfig {
	cfg := DefaultTTSConfig()
	if v := os.Getenv("TTS_ENABLED"); v == "1" || v == "true" || v == "TRUE" {
		cfg.Enabled = true
	}
	if v := os.Getenv("ELEVENLABS_API_KEY"); v != "" {
		cfg.APIKey = v
	}
	if v := os.Getenv("ELEVENLABS_VOICE_ID"); v != "" {
		cfg.VoiceID = v
	}
	if v := os.Getenv("ELEVENLABS_MODEL"); v != "" {
		cfg.Model = v
	}
	if v := os.Getenv("TTS_OUTPUT_FORMAT"); v != "" {
		cfg.OutputFormat = v
	}
	if v := os.Getenv("ELEVENLABS_BASE_URL"); v != "" {
		cfg.BaseURL = v
	}
	if v := os.Getenv("TTS_RECONNECT_BASE_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			cfg.ReconnectBase = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv("TTS_RECONNECT_MAX_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			cfg.ReconnectMax = time.Duration(ms) * time.Millisecond
		}
	}
	return cfg.withDefaults()
}

// NewTTSProvider returns Noop when disabled, ElevenLabs when enabled.
func NewTTSProvider(cfg TTSConfig) (TTSProvider, error) {
	cfg = cfg.withDefaults()
	if !cfg.Enabled {
		return NoopTTSProvider{}, nil
	}
	if cfg.APIKey == "" {
		return nil, ErrTTSNotConfigured
	}
	return NewElevenLabsTTSProvider(cfg)
}

// NoopTTSProvider drops text and emits no audio (pipeline runs without TTS).
type NoopTTSProvider struct{}

func (NoopTTSProvider) Open(_ context.Context, _ TTSSessionMeta) (TTSStream, error) {
	return newNoopTTSStream(), nil
}

type noopTTSStream struct {
	audio chan TTSAudioChunk
}

func newNoopTTSStream() *noopTTSStream {
	return &noopTTSStream{audio: make(chan TTSAudioChunk)}
}

func (n *noopTTSStream) Speak(_ string, _ string) error { return nil }
func (n *noopTTSStream) Cancel(_ string) error          { return nil }
func (n *noopTTSStream) Audio() <-chan TTSAudioChunk    { return n.audio }
func (n *noopTTSStream) Close() error {
	close(n.audio)
	return nil
}
