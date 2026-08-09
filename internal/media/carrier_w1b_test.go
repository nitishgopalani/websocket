package media

import (
	"strings"
	"testing"
)

// W1-B.3: carrier=asterisk must FAIL LOUDLY at startup when ASR/TTS is off.
// Fonada/Exotel carriers are unaffected (they rely on the carrier side for
// audio and tolerate a deaf/mute brain leg during bring-up).

func TestValidateCarrierRequirements_AsteriskBothDisabled(t *testing.T) {
	err := ValidateCarrierRequirements(
		CarrierConfig{Variant: CarrierAsterisk},
		ASRConfig{Enabled: false},
		TTSConfig{Enabled: false},
	)
	if err == nil {
		t.Fatal("expected CarrierRequirementError, got nil")
	}
	cre, ok := err.(*CarrierRequirementError)
	if !ok {
		t.Fatalf("expected *CarrierRequirementError, got %T", err)
	}
	if !strings.Contains(cre.Error(), "ASR_ENABLED") {
		t.Errorf("error should mention ASR_ENABLED, got: %s", cre.Error())
	}
	if !strings.Contains(cre.Error(), "TTS_ENABLED") {
		t.Errorf("error should mention TTS_ENABLED, got: %s", cre.Error())
	}
	if len(cre.Reasons) != 2 {
		t.Errorf("expected 2 reasons, got %d (%v)", len(cre.Reasons), cre.Reasons)
	}
}

func TestValidateCarrierRequirements_AsteriskASROff(t *testing.T) {
	err := ValidateCarrierRequirements(
		CarrierConfig{Variant: CarrierAsterisk},
		ASRConfig{Enabled: false},
		TTSConfig{Enabled: true},
	)
	if err == nil {
		t.Fatal("expected error for ASR off under asterisk")
	}
	cre := err.(*CarrierRequirementError)
	if len(cre.Reasons) != 1 {
		t.Errorf("expected 1 reason, got %d", len(cre.Reasons))
	}
	if !strings.Contains(cre.Reasons[0], "ASR_ENABLED") {
		t.Errorf("reason should mention ASR_ENABLED, got: %s", cre.Reasons[0])
	}
}

func TestValidateCarrierRequirements_AsteriskTTSOff(t *testing.T) {
	err := ValidateCarrierRequirements(
		CarrierConfig{Variant: CarrierAsterisk},
		ASRConfig{Enabled: true},
		TTSConfig{Enabled: false},
	)
	if err == nil {
		t.Fatal("expected error for TTS off under asterisk")
	}
	cre := err.(*CarrierRequirementError)
	if len(cre.Reasons) != 1 {
		t.Errorf("expected 1 reason, got %d", len(cre.Reasons))
	}
	if !strings.Contains(cre.Reasons[0], "TTS_ENABLED") {
		t.Errorf("reason should mention TTS_ENABLED, got: %s", cre.Reasons[0])
	}
}

func TestValidateCarrierRequirements_AsteriskBothOn(t *testing.T) {
	err := ValidateCarrierRequirements(
		CarrierConfig{Variant: CarrierAsterisk},
		ASRConfig{Enabled: true},
		TTSConfig{Enabled: true},
	)
	if err != nil {
		t.Fatalf("expected nil when both enabled, got: %v", err)
	}
}

func TestValidateCarrierRequirements_FonadaToleratesDisabled(t *testing.T) {
	// Fonada is the pilot default; it tolerates deaf/mute brain leg (carrier
	// side owns audio). No requirement enforced.
	err := ValidateCarrierRequirements(
		CarrierConfig{Variant: CarrierFonada},
		ASRConfig{Enabled: false},
		TTSConfig{Enabled: false},
	)
	if err != nil {
		t.Fatalf("fonada should tolerate disabled ASR/TTS, got: %v", err)
	}
}

func TestValidateCarrierRequirements_ExotelToleratesDisabled(t *testing.T) {
	err := ValidateCarrierRequirements(
		CarrierConfig{Variant: CarrierExotel},
		ASRConfig{Enabled: false},
		TTSConfig{Enabled: false},
	)
	if err != nil {
		t.Fatalf("exotel should tolerate disabled ASR/TTS, got: %v", err)
	}
}

func TestValidateCarrierRequirements_UnknownCarrierTolerates(t *testing.T) {
	err := ValidateCarrierRequirements(
		CarrierConfig{Variant: "future-carrier"},
		ASRConfig{Enabled: false},
		TTSConfig{Enabled: false},
	)
	if err != nil {
		t.Fatalf("unknown carrier should not enforce, got: %v", err)
	}
}
