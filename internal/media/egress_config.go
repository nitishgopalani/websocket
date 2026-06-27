package media

import (
	"os"
	"strconv"
	"strings"
)

const (
	defaultEgressJitterMs       = 300
	defaultOutboundBufferFrames = 64
	egressPacingRealtime        = "realtime"
	egressPacingBurst           = "burst"
)

// EgressConfig controls outbound carrier audio pacing and buffering.
type EgressConfig struct {
	JitterMs             int
	Pacing               string // realtime | burst
	OutboundBufferFrames int
}

// DefaultEgressConfig returns CT-10 egress defaults.
func DefaultEgressConfig() EgressConfig {
	return EgressConfig{
		JitterMs:             defaultEgressJitterMs,
		Pacing:               egressPacingRealtime,
		OutboundBufferFrames: defaultOutboundBufferFrames,
	}
}

func (c EgressConfig) withDefaults() EgressConfig {
	if c.JitterMs <= 0 {
		c.JitterMs = defaultEgressJitterMs
	}
	if c.Pacing == "" {
		c.Pacing = egressPacingRealtime
	}
	if c.OutboundBufferFrames <= 0 {
		c.OutboundBufferFrames = defaultOutboundBufferFrames
	}
	return c
}

// EgressConfigFromEnv loads egress settings from the environment.
func EgressConfigFromEnv() EgressConfig {
	cfg := DefaultEgressConfig()
	if v := os.Getenv("EGRESS_JITTER_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.JitterMs = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EGRESS_PACING")); v != "" {
		switch strings.ToLower(v) {
		case egressPacingBurst:
			cfg.Pacing = egressPacingBurst
		default:
			cfg.Pacing = egressPacingRealtime
		}
	}
	if v := os.Getenv("OUTBOUND_BUFFER_FRAMES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.OutboundBufferFrames = n
		}
	}
	return cfg.withDefaults()
}
