package media

import (
	"fmt"
	"os"
	"strings"
)

const (
	CarrierFonada   = "fonada"
	CarrierExotel   = "exotel"
	CarrierAsterisk = "asterisk"
)

// CarrierConfig selects the outbound carrier adapter variant.
type CarrierConfig struct {
	Variant string
}

// CarrierProfile describes ingress/egress framing for a carrier variant.
type CarrierProfile struct {
	Variant               string
	BinaryIngress         bool
	BinaryEgress          bool
	InputSampleRate       int
	EgressSampleRate      int
	EgressBytesPerSample  int
	RequiresMarkEcho      bool
	BargeInFlushSupported bool
}

// Profile returns runtime framing settings for the configured carrier.
func (c CarrierConfig) Profile() CarrierProfile {
	switch strings.ToLower(c.Variant) {
	case CarrierAsterisk:
		return CarrierProfile{
			Variant:               CarrierAsterisk,
			BinaryIngress:         true,
			BinaryEgress:          true,
			InputSampleRate:       16000,
			EgressSampleRate:      24000,
			EgressBytesPerSample:  2,
			RequiresMarkEcho:      false,
			// Connector MsgClear drains toAst; residual after successful send is
			// TCP + Asterisk internal only (not our local pacer).
			BargeInFlushSupported: true,
		}
	default:
		return CarrierProfile{
			Variant:               c.Variant,
			EgressSampleRate:      defaultTargetSampleRate,
			EgressBytesPerSample:  1,
			RequiresMarkEcho:      true,
			BargeInFlushSupported: true,
		}
	}
}

// DefaultCarrierConfig returns Fonada as the pilot default.
func DefaultCarrierConfig() CarrierConfig {
	return CarrierConfig{Variant: CarrierFonada}
}

// DefaultCarrierProfile returns framing defaults for the default carrier (Fonada).
func DefaultCarrierProfile() CarrierProfile {
	return DefaultCarrierConfig().Profile()
}

// CarrierConfigFromEnv loads CARRIER (fonada|exotel|asterisk).
func CarrierConfigFromEnv() CarrierConfig {
	cfg := DefaultCarrierConfig()
	if v := strings.TrimSpace(strings.ToLower(os.Getenv("CARRIER"))); v != "" {
		cfg.Variant = v
	}
	return cfg
}

// CarrierRequirementError is returned by ValidateCarrierRequirements when a
// carrier-mode hard requirement is unmet. W1-B.3: startup FAILS LOUDLY
// (os.Exit(1) in main) rather than running deaf/mute.
type CarrierRequirementError struct {
	Carrier string
	Reasons []string
}

func (e *CarrierRequirementError) Error() string {
	return fmt.Sprintf("carrier %q requirements unmet: %s", e.Carrier, strings.Join(e.Reasons, "; "))
}

// ValidateCarrierRequirements enforces carrier-mode hard requirements.
//
// W1-B.3 (H2 dead-air defense): under carrier=asterisk the media server
// owns the call audio path end-to-end (binary PCM ingress + egress). Running
// deaf (ASR off) or mute (TTS off) is a silent failure — the caller hears
// nothing or is not heard. Fail loudly at startup instead.
//
// asrCfg.Enabled / ttsCfg.Enabled reflect the parsed ASR_ENABLED / TTS_ENABLED
// env flags. Returns a *CarrierRequirementError listing every unmet
// requirement (so the operator sees the full gap in one shot).
func ValidateCarrierRequirements(carrier CarrierConfig, asrCfg ASRConfig, ttsCfg TTSConfig) error {
	if strings.ToLower(carrier.Variant) != CarrierAsterisk {
		return nil
	}
	var reasons []string
	if !asrCfg.Enabled {
		reasons = append(reasons, "ASR_ENABLED must be true under carrier=asterisk (deaf call is never acceptable)")
	}
	if !ttsCfg.Enabled {
		reasons = append(reasons, "TTS_ENABLED must be true under carrier=asterisk (mute call is never acceptable)")
	}
	if len(reasons) == 0 {
		return nil
	}
	return &CarrierRequirementError{Carrier: CarrierAsterisk, Reasons: reasons}
}

// NewCarrierSerializer returns the serializer for the configured carrier variant.
func NewCarrierSerializer(cfg CarrierConfig) CarrierSerializer {
	switch strings.ToLower(cfg.Variant) {
	case CarrierExotel:
		return ExotelSerializer{}
	case CarrierAsterisk:
		return AsteriskSerializer{}
	default:
		return FonadaSerializer{}
	}
}

// FonadaSerializer emits Fonada bidirectional media stream JSON (pilot default).
type FonadaSerializer struct{}

func (FonadaSerializer) Media(streamSID string, muLaw []byte) ([]byte, error) {
	return ExotelFonadaSerializer{}.Media(streamSID, muLaw)
}

func (FonadaSerializer) Mark(streamSID string, turnID string) ([]byte, error) {
	return ExotelFonadaSerializer{}.Mark(streamSID, turnID)
}

func (FonadaSerializer) Clear(streamSID string) ([]byte, error) {
	return ExotelFonadaSerializer{}.Clear(streamSID)
}

// ExotelSerializer emits Exotel bidirectional stream JSON (GO-A variant).
type ExotelSerializer struct{}

func (ExotelSerializer) Media(streamSID string, muLaw []byte) ([]byte, error) {
	return ExotelFonadaSerializer{}.Media(streamSID, muLaw)
}

func (ExotelSerializer) Mark(streamSID string, turnID string) ([]byte, error) {
	return ExotelFonadaSerializer{}.Mark(streamSID, turnID)
}

func (ExotelSerializer) Clear(streamSID string) ([]byte, error) {
	return ExotelFonadaSerializer{}.Clear(streamSID)
}
