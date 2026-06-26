package media

import "time"

const (
	defaultListenAddr            = ":8080"
	defaultWSPath                = "/stream"
	defaultMaxConcurrentSessions = 1000
	defaultAudioBufferSize       = 8
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
}

// DefaultConfig returns a Config populated with sensible CT-1 defaults.
func DefaultConfig() Config {
	return Config{
		ListenAddr:            defaultListenAddr,
		WSPath:                defaultWSPath,
		MaxConcurrentSessions: defaultMaxConcurrentSessions,
		AudioBufferSize:       defaultAudioBufferSize,
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
	return c
}

func (c Config) useTLS() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}
