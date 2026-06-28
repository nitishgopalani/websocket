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
)

// Inbound message types (brain → Go).
const (
	TypeChunk     = "chunk"
	TypeFlowClass = "flow_class"
	TypeDone      = "done"
	TypeError     = "error"
)

// BorrowerContextPayload carries per-call campaign variables (Excel upload / metadata).
type BorrowerContextPayload struct {
	BorrowerName string  `json:"borrower_name,omitempty"`
	Phone        string  `json:"phone,omitempty"`
	AmountDue    any     `json:"amount_due,omitempty"`
	AccountRef   string  `json:"account_ref,omitempty"`
	Language     string  `json:"language,omitempty"`
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

// ChunkMessage is one TTS-able sentence chunk of the gated reply.
type ChunkMessage struct {
	Type   string `json:"type"`
	TurnID string `json:"turn_id"`
	Seq    int    `json:"seq"`
	Text   string `json:"text"`
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
	AuditID     string `json:"audit_id,omitempty"`
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
