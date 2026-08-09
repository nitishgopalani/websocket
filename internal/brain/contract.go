package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"websocket/internal/media"
)

// Outbound message types (Go → brain).
const (
	TypeSessionStart = "session_start"
	TypeTurn         = "turn"
	TypeCancel       = "cancel"
	TypeSessionEnd   = "session_end"
	TypePlaybackDone = "playback_done"
)

// Inbound message types (brain → Go).
const (
	TypeSessionReady = "session_ready"
	TypeChunk        = "chunk"
	TypeFlowClass    = "flow_class"
	TypeDone         = "done"
	TypeError        = "error"
)

// BorrowerContextPayload carries per-call campaign variables (Excel upload / metadata).
type BorrowerContextPayload struct {
	BorrowerName      string `json:"borrower_name,omitempty"`
	Phone             string `json:"phone,omitempty"`
	AmountDue         any    `json:"amount_due,omitempty"`
	AccountRef        string `json:"account_ref,omitempty"`
	Language          string `json:"language,omitempty"`
	SpeakerLabel      string `json:"speaker_label,omitempty"`
	TapOnly           bool   `json:"tap_only,omitempty"`
	ParentSessionUUID string `json:"parent_session_uuid,omitempty"`
}

// SessionStartPayload opens a persistent EB-6 session.
type SessionStartPayload struct {
	Type            string                  `json:"type"`
	SessionID       string                  `json:"session_id"`
	BorrowerID      string                  `json:"borrower_id"`
	AgentID         string                  `json:"agent_id"`
	PackID          string                  `json:"pack_id,omitempty"`
	Locale          string                  `json:"locale,omitempty"`
	TenantID        string                  `json:"tenant_id,omitempty"`
	// ClientID is the connector's per-DID client_id forwarded verbatim
	// (HARDEN-1 F2, G-A3-03). The brain resolves tenant as
	// client_id > session_tenant_id > reject, so this field is the source of
	// truth on the BYO/media-meta path. TenantID above is still injected
	// (explicit tenant_id param or BRAIN_TENANT_ID fallback) for one more
	// release so older brain builds keep working.
	ClientID        string                  `json:"client_id,omitempty"`
	BorrowerContext *BorrowerContextPayload `json:"borrower_context,omitempty"`
}

// TurnPayload sends a caller turn after EndOfTurn (or empty transcript for opener).
type TurnPayload struct {
	Type       string `json:"type"`
	SessionID  string `json:"session_id"`
	TurnID     string `json:"turn_id"`
	Transcript string `json:"transcript"`
	FlowClass  string `json:"flow_class"`
}

// CancelPayload cancels an in-flight brain turn (barge-in).
type CancelPayload struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	TurnID    string `json:"turn_id"`
}

// SessionEndPayload closes the EB-6 session.
type SessionEndPayload struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
}

// PlaybackDonePayload tells the brain a turn's audio finished playing to the
// caller (last paced frame egressed / carrier mark echoed). The brain uses it
// to sequence actions that must wait for the caller to HEAR a line first
// (e.g. start a consult only after the hold announcement completes) and to
// arm its no-input reprompt timer.
type PlaybackDonePayload struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	TurnID    string `json:"turn_id"`
}

// SessionReadyPayload acknowledges session_start with resolved borrower ASR locale.
type SessionReadyPayload struct {
	Type         string `json:"type"`
	SessionID    string `json:"session_id"`
	BorrowerID   string `json:"borrower_id,omitempty"`
	BorrowerName string `json:"borrower_name,omitempty"`
	AsrLanguage  string `json:"asr_language"`
	// ApologyText + ApologyVoiceID (W1-C C0 / DEBT-026): the tenant's
	// dead-air apology line + the unknown_info-register TTS voice, sent by
	// the brain on session_ready so the go-server's DeadAirHandler can speak
	// it via TTS before clean-close on ASR-reconnect-exhaustion. Empty =
	// handler closes silently (no apology spoken). See PAISALO_FRAGMENT_LIBRARY
	// §H candidate #55 (PENDING-CLIENT-APPROVAL).
	ApologyText    string `json:"apology_text,omitempty"`
	ApologyVoiceID string `json:"apology_voice_id,omitempty"`
}

// ChunkMessage is one TTS-able sentence chunk of the gated reply.
type ChunkMessage struct {
	Type   string `json:"type"`
	TurnID string `json:"turn_id"`
	Seq    int    `json:"seq"`
	Text   string `json:"text"`
	// Optional per-call TTS overrides (PaisaLo scenario voice, etc.).
	// Empty → media keeps SARVAM_TTS_SPEAKER / SARVAM_TTS_MODEL / code defaults.
	VoiceID  string   `json:"voice_id,omitempty"`
	TTSModel string   `json:"tts_model,omitempty"`
	TTSPace  *float64 `json:"tts_pace,omitempty"`
}

// FlowClassMessage hints the next expected input class for endpointing.
type FlowClassMessage struct {
	Type   string `json:"type"`
	TurnID string `json:"turn_id"`
	Next   string `json:"next"`
}

// DoneMessage marks turn completion from the brain.
type DoneMessage struct {
	Type        string `json:"type"`
	TurnID      string `json:"turn_id"`
	Disposition string `json:"disposition,omitempty"`
	EndCall     bool   `json:"end_call,omitempty"`
	// EndCallDelayMs delays the hangup this long AFTER the turn's playback
	// completes (e.g. 3s grace after "I am disconnecting this call").
	EndCallDelayMs int    `json:"end_call_delay_ms,omitempty"`
	AuditID        string `json:"audit_id,omitempty"`
}

// ErrorMessage is a fail-safe fallback line on brain error/deadline.
type ErrorMessage struct {
	Type         string `json:"type"`
	TurnID       string `json:"turn_id"`
	FallbackText string `json:"fallback_text"`
}

// ReplyHandler is deprecated; use media.ReplyConsumer (CT-8).
type ReplyHandler = media.ReplyConsumer

// LoggingReplyHandler logs inbound brain messages including flow_class hints.
type LoggingReplyHandler struct {
	Inner  media.ReplyConsumer
	Logger *slog.Logger
	Turns  *media.TurnManager
}

func (h *LoggingReplyHandler) OnReplyChunk(ctx context.Context, session *media.Session, turnID string, seq int, text string) {
	if h.Inner != nil {
		h.Inner.OnReplyChunk(ctx, session, turnID, seq, text)
	} else if h.Logger != nil {
		h.Logger.Info("brain chunk", "stream_sid", session.StreamSID, "turn_id", turnID, "seq", seq, "text", text)
	}
}

func (h *LoggingReplyHandler) OnReplyDone(ctx context.Context, session *media.Session, turnID string, endCall bool, disposition string) {
	if h.Inner != nil {
		h.Inner.OnReplyDone(ctx, session, turnID, endCall, disposition)
	} else if h.Logger != nil {
		h.Logger.Info("brain done", "stream_sid", session.StreamSID, "turn_id", turnID, "end_call", endCall)
	}
}

func (h *LoggingReplyHandler) OnReplyError(ctx context.Context, session *media.Session, turnID, fallback string) {
	if h.Inner != nil {
		h.Inner.OnReplyError(ctx, session, turnID, fallback)
	} else if h.Logger != nil {
		h.Logger.Info("brain error", "stream_sid", session.StreamSID, "turn_id", turnID, "fallback", fallback)
	}
}

func (h *LoggingReplyHandler) OnFlowClassHint(_ context.Context, session *media.Session, turnID string, class media.FlowClass) {
	if h.Turns != nil {
		h.Turns.SetFlowClass(session, class)
	}
	if h.Logger != nil {
		h.Logger.Info("brain flow_class", "stream_sid", session.StreamSID, "turn_id", turnID, "next", string(class))
	}
}

// ParseFlowClassHint maps brain flow_class.next to media.FlowClass.
func ParseFlowClassHint(next string) media.FlowClass {
	switch next {
	case "YesNo":
		return media.FlowYesNo
	case "SpelledInput":
		return media.FlowSpelledInput
	default:
		return media.FlowDefault
	}
}

// FlowClassToWire maps media.FlowClass to the wire string sent on turn messages.
func FlowClassToWire(class media.FlowClass) string {
	switch class {
	case media.FlowYesNo:
		return "YesNo"
	case media.FlowSpelledInput:
		return "SpelledInput"
	default:
		return "Default"
	}
}

func decodeInbound(data []byte) (string, error) {
	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return "", err
	}
	return header.Type, nil
}

func unmarshalInbound(data []byte, typ string) (any, error) {
	switch typ {
	case TypeSessionReady:
		var msg SessionReadyPayload
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return msg, nil
	case TypeChunk:
		var msg ChunkMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return msg, nil
	case TypeFlowClass:
		var msg FlowClassMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return msg, nil
	case TypeDone:
		var msg DoneMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return msg, nil
	case TypeError:
		var msg ErrorMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return msg, nil
	default:
		return nil, fmt.Errorf("unknown brain message type %q", typ)
	}
}
