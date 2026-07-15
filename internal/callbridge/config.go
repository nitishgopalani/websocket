package callbridge

import (
	"os"
	"strconv"
	"time"
)

const (
	defaultListenAddr        = ":18445"
	defaultWSPath            = "/bridge"
	defaultQueueFrames       = 100
	defaultMaxFrameAgeMS     = 200
	defaultPairingTimeoutMS  = 60000
	defaultMetricsEnabled    = true
	defaultAuthMaxSkewSec    = 300
)

// Config holds callbridge runtime settings.
type Config struct {
	ListenAddr      string
	WSPath          string
	MediaSecret     string
	QueueFrames     int
	MaxFrameAge     time.Duration
	PairingTimeout  time.Duration
	TLSCertFile     string
	TLSKeyFile      string
	MetricsEnabled  bool
	AuthMaxSkew     time.Duration
}

// ConfigFromEnv loads configuration from environment variables.
func ConfigFromEnv() Config {
	cfg := Config{
		ListenAddr:     getenv("LISTEN_ADDR", defaultListenAddr),
		WSPath:         getenv("BRIDGE_WS_PATH", defaultWSPath),
		MediaSecret:    os.Getenv("FONADA_MEDIA_SECRET"),
		QueueFrames:    getenvInt("BRIDGE_QUEUE_FRAMES", defaultQueueFrames),
		MaxFrameAge:    time.Duration(getenvInt("BRIDGE_MAX_FRAME_AGE_MS", defaultMaxFrameAgeMS)) * time.Millisecond,
		PairingTimeout: time.Duration(getenvInt("BRIDGE_PAIRING_TIMEOUT_MS", defaultPairingTimeoutMS)) * time.Millisecond,
		TLSCertFile:    os.Getenv("TLS_CERT"),
		TLSKeyFile:     os.Getenv("TLS_KEY"),
		MetricsEnabled: getenvBool("METRICS_ENABLED", defaultMetricsEnabled),
		AuthMaxSkew:    time.Duration(getenvInt("BRIDGE_AUTH_MAX_SKEW_SEC", defaultAuthMaxSkewSec)) * time.Second,
	}
	return cfg
}

func (c Config) useTLS() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func getenvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	switch v {
	case "0", "false", "FALSE", "no", "NO":
		return false
	default:
		return true
	}
}
