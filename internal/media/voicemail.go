package media

import (
	"context"
	"os"
	"strings"
)

const (
	VoicemailActionHangup       = "hangup"
	VoicemailActionLeaveMessage = "leave_message"
)

// VoicemailConfig controls the AMD machine (voicemail) branch.
type VoicemailConfig struct {
	Action  string
	Message string
}

// DefaultVoicemailConfig returns pilot-safe defaults (immediate hangup).
func DefaultVoicemailConfig() VoicemailConfig {
	return VoicemailConfig{
		Action:  VoicemailActionHangup,
		Message: "Namaste. Kripya hamen baad mein call karein.",
	}
}

// VoicemailConfigFromEnv loads voicemail branch settings.
func VoicemailConfigFromEnv() VoicemailConfig {
	cfg := DefaultVoicemailConfig()
	if v := strings.TrimSpace(os.Getenv("VOICEMAIL_ACTION")); v != "" {
		cfg.Action = strings.ToLower(v)
	}
	if v := os.Getenv("VOICEMAIL_MESSAGE"); v != "" {
		cfg.Message = v
	}
	if cfg.Action != VoicemailActionLeaveMessage {
		cfg.Action = VoicemailActionHangup
	}
	return cfg
}

// SessionCloser ends a carrier session (e.g. pilot hangup after AMD machine).
type SessionCloser interface {
	CloseSession(ctx context.Context, streamSID string)
}

// SessionCloserHolder binds a SessionManager for deferred pilot hangup wiring.
type SessionCloserHolder struct {
	mgr *SessionManager
}

// SetManager attaches the session manager after server construction.
func (h *SessionCloserHolder) SetManager(mgr *SessionManager) {
	h.mgr = mgr
}

func (h *SessionCloserHolder) CloseSession(ctx context.Context, streamSID string) {
	if h.mgr != nil {
		h.mgr.Close(ctx, streamSID)
	}
}

var _ SessionCloser = (*SessionCloserHolder)(nil)
