package media

import (
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
			BargeInFlushSupported: false,
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
