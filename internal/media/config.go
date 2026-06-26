package media

import "time"

const (
	defaultListenAddr            = ":8080"
	defaultWSPath                = "/stream"
	defaultMaxConcurrentSessions = 1000
	defaultAudioBufferSize       = 8
	defaultTargetSampleRate      = 16000
	defaultFrameDurationMs       = 20
	defaultReadTimeout           = 60 * time.Second
	defaultWriteTimeout          = 10 * time.Second
)

// Config holds runtime settings for the media ingress server.
type Config struct {
	ListenAddr            string
	WSPath                string
	TLSCertFile           string
	TLSKeyFile            string
	MaxConcurrentSessions int
	AudioBufferSize       int
	TargetSampleRate      int
	FrameDurationMs       int
}

// DefaultConfig returns a Config populated with sensible CT-1 defaults.
func DefaultConfig() Config {
	return Config{
		ListenAddr:            defaultListenAddr,
		WSPath:                defaultWSPath,
		MaxConcurrentSessions: defaultMaxConcurrentSessions,
		AudioBufferSize:       defaultAudioBufferSize,
		TargetSampleRate:      defaultTargetSampleRate,
		FrameDurationMs:       defaultFrameDurationMs,
	}
}

// TargetFormat returns the canonical PCM16 layout configured for transcoding.
func (c Config) TargetFormat() TargetFormat {
	c = c.withDefaults()
	return TargetFormat{
		SampleRate: c.TargetSampleRate,
		Channels:   1,
	}
}

func (c Config) withDefaults() Config {
	if c.ListenAddr == "" {
		c.ListenAddr = defaultListenAddr
	}
	if c.WSPath == "" {
		c.WSPath = defaultWSPath
	}
	if c.MaxConcurrentSessions <= 0 {
		c.MaxConcurrentSessions = defaultMaxConcurrentSessions
	}
	if c.AudioBufferSize <= 0 {
		c.AudioBufferSize = defaultAudioBufferSize
	}
	if c.TargetSampleRate != 8000 && c.TargetSampleRate != 16000 {
		c.TargetSampleRate = defaultTargetSampleRate
	}
	if c.FrameDurationMs <= 0 {
		c.FrameDurationMs = defaultFrameDurationMs
	}
	return c
}

func (c Config) useTLS() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}
