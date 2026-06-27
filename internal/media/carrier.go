package media

import (
	"os"
	"strings"
)

const (
	CarrierFonada = "fonada"
	CarrierExotel = "exotel"
)

// CarrierConfig selects the outbound carrier JSON adapter.
type CarrierConfig struct {
	Variant string
}

// DefaultCarrierConfig returns Fonada as the pilot default.
func DefaultCarrierConfig() CarrierConfig {
	return CarrierConfig{Variant: CarrierFonada}
}

// CarrierConfigFromEnv loads CARRIER (fonada|exotel).
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
