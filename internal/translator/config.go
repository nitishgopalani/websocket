package translator

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultListenAddr            = ":18447"
	defaultWSPath                = "/translate"
	defaultQueueFrames           = 100
	defaultMaxFrameAgeMS         = 200
	defaultPairingTimeoutMS      = 60000
	defaultMetricsEnabled        = true
	defaultAuthMaxSkewSec        = 300
	defaultSilenceMsDefault      = 300
	defaultASRSampleRate         = 16000
	defaultTTSSynthRate          = 16000
	defaultLaneMode              = "translate"
	defaultEchoTailMS            = 350
	defaultFloorDebounceMS       = 150
	defaultLoopBreakerMax        = 6
	defaultLoopBreakerWindowMS   = 10000
	defaultTTSLanguageB          = "hi"
	defaultTTSVoiceB             = "21m00Tcm4TlvDq8ikWAM"
	defaultIncompleteExtraMS     = 300
)

// LaneMode selects translator lane behavior.
type LaneMode string

const (
	LaneModeTranslate LaneMode = "translate"
	LaneModeEcho      LaneMode = "echo"
)

var (
	errMissingBridgeID = errors.New("translator: missing bridge id")
	errRoomClosed      = errors.New("translator: room closed")
	errBridgeFull      = errors.New("translator: bridge full")
	errLegTaken        = errors.New("translator: leg already taken")
	errRateMismatch    = errors.New("translator: audio rate mismatch")
)

// Config holds translator runtime settings.
type Config struct {
	ListenAddr         string
	WSPath             string
	MediaSecret        string
	QueueFrames        int
	MaxFrameAge        time.Duration
	PairingTimeout     time.Duration
	TLSCertFile        string
	TLSKeyFile         string
	MetricsEnabled     bool
	AuthMaxSkew        time.Duration
	TranslationEnabled bool
	TargetSampleRate   int
	FrameDurationMs    int
	EndpointSilenceMs  int
	LaneMode              LaneMode
	ASRSampleRate         int
	TTSSynthRate          int
	Bidirectional         bool
	EchoTail              time.Duration
	FloorDebounce         time.Duration
	LoopBreakerMax        int
	LoopBreakerWindow     time.Duration
	TTSLanguageB          string
	TTSVoiceIDB           string
	IncompleteExtraMs     int
	FillerLexiconPath     string
}

// ConfigFromEnv loads configuration from environment variables.
func ConfigFromEnv() Config {
	cfg := Config{
		ListenAddr:         getenv("LISTEN_ADDR", defaultListenAddr),
		WSPath:             getenv("TRANSLATOR_WS_PATH", defaultWSPath),
		MediaSecret:        os.Getenv("FONADA_MEDIA_SECRET"),
		QueueFrames:        getenvInt("TRANSLATOR_QUEUE_FRAMES", defaultQueueFrames),
		MaxFrameAge:        time.Duration(getenvInt("TRANSLATOR_MAX_FRAME_AGE_MS", defaultMaxFrameAgeMS)) * time.Millisecond,
		PairingTimeout:     time.Duration(getenvInt("TRANSLATOR_PAIRING_TIMEOUT_MS", defaultPairingTimeoutMS)) * time.Millisecond,
		TLSCertFile:        os.Getenv("TLS_CERT"),
		TLSKeyFile:         os.Getenv("TLS_KEY"),
		MetricsEnabled:     getenvBool("METRICS_ENABLED", defaultMetricsEnabled),
		AuthMaxSkew:        time.Duration(getenvInt("TRANSLATOR_AUTH_MAX_SKEW_SEC", defaultAuthMaxSkewSec)) * time.Second,
		TranslationEnabled: getenvBool("TRANSLATION_ENABLED", false),
		TargetSampleRate:   getenvInt("TARGET_SAMPLE_RATE", 8000),
		FrameDurationMs:    getenvInt("FRAME_DURATION_MS", 20),
		EndpointSilenceMs:  getenvInt("TRANSLATOR_SILENCE_MS_DEFAULT", defaultSilenceMsDefault),
		LaneMode:           parseLaneMode(getenv("TRANSLATOR_MODE", defaultLaneMode)),
		ASRSampleRate:         getenvInt("TRANSLATOR_ASR_SAMPLE_RATE", defaultASRSampleRate),
		TTSSynthRate:          getenvInt("TRANSLATOR_TTS_SYNTH_RATE", defaultTTSSynthRate),
		Bidirectional:         getenvBool("TRANSLATOR_BIDIRECTIONAL", false),
		EchoTail:              time.Duration(getenvInt("TRANSLATOR_ECHO_TAIL_MS", defaultEchoTailMS)) * time.Millisecond,
		FloorDebounce:         time.Duration(getenvInt("TRANSLATOR_FLOOR_DEBOUNCE_MS", defaultFloorDebounceMS)) * time.Millisecond,
		LoopBreakerMax:        getenvInt("TRANSLATOR_LOOP_BREAKER_MAX", defaultLoopBreakerMax),
		LoopBreakerWindow:     time.Duration(getenvInt("TRANSLATOR_LOOP_BREAKER_WINDOW_MS", defaultLoopBreakerWindowMS)) * time.Millisecond,
		TTSLanguageB:          getenv("TRANSLATOR_TTS_LANGUAGE_B", defaultTTSLanguageB),
		TTSVoiceIDB:           getenv("TRANSLATOR_TTS_VOICE_ID_B", defaultTTSVoiceB),
		IncompleteExtraMs:     getenvInt("TRANSLATOR_INCOMPLETE_EXTRA_MS", defaultIncompleteExtraMS),
		FillerLexiconPath:     os.Getenv("TRANSLATOR_FILLER_LEXICON"),
	}
	return cfg
}

func parseLaneMode(v string) LaneMode {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "echo", "echo_hi", "hindi_echo":
		return LaneModeEcho
	default:
		return LaneModeTranslate
	}
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
