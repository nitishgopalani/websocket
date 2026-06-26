package media

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

const (
	EventConnected = "connected"
	EventStart     = "start"
	EventMedia     = "media"
	EventDTMF      = "dtmf"
	EventStop      = "stop"
)

// AudioFormat describes inbound stream media characteristics from the start event.
type AudioFormat struct {
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
	Channels   int    `json:"channels"`
}

// ConnectedEvent is the first message on a bidirectional stream.
type ConnectedEvent struct {
	Event string `json:"event"`
}

// StartEvent carries stream metadata once per call.
type StartEvent struct {
	Event            string            `json:"event"`
	StreamSID        string            `json:"stream_sid"`
	CallSID          string            `json:"call_sid"`
	MediaFormat      AudioFormat       `json:"media_format"`
	CustomParameters map[string]string `json:"custom_parameters"`
}

// MediaChunk is a single inbound audio frame.
type MediaChunk struct {
	Payload   string `json:"payload"`
	Timestamp string `json:"timestamp"`
	Chunk     int64  `json:"chunk"`
}

// MediaEvent carries a base64-encoded audio frame.
type MediaEvent struct {
	Event     string     `json:"event"`
	StreamSID string     `json:"stream_sid"`
	Media     MediaChunk `json:"media"`
}

// DTMFEvent carries a keypad digit.
type DTMFEvent struct {
	Event     string `json:"event"`
	StreamSID string `json:"stream_sid"`
	DTMF      struct {
		Digit string `json:"digit"`
	} `json:"dtmf"`
}

// StopEvent marks the end of a stream.
type StopEvent struct {
	Event     string `json:"event"`
	StreamSID string `json:"stream_sid"`
}

// InboundEvent is a parsed inbound websocket message.
type InboundEvent struct {
	Type      string
	Connected *ConnectedEvent
	Start     *StartEvent
	Media     *MediaEvent
	DTMF      *DTMFEvent
	Stop      *StopEvent
	RawType   string
}

// ParseInboundEvent inspects the event field and unmarshals into the appropriate type.
// Unknown events are returned with Type set to the raw value; callers should log and ignore.
func ParseInboundEvent(data []byte, logger *slog.Logger) (InboundEvent, error) {
	var envelope struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return InboundEvent{}, fmt.Errorf("decode event envelope: %w", err)
	}
	if envelope.Event == "" {
		return InboundEvent{}, fmt.Errorf("missing event field")
	}

	switch envelope.Event {
	case EventConnected:
		var evt ConnectedEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return InboundEvent{}, fmt.Errorf("decode connected event: %w", err)
		}
		return InboundEvent{Type: EventConnected, Connected: &evt}, nil
	case EventStart:
		var evt StartEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return InboundEvent{}, fmt.Errorf("decode start event: %w", err)
		}
		return InboundEvent{Type: EventStart, Start: &evt}, nil
	case EventMedia:
		var evt MediaEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return InboundEvent{}, fmt.Errorf("decode media event: %w", err)
		}
		return InboundEvent{Type: EventMedia, Media: &evt}, nil
	case EventDTMF:
		var evt DTMFEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return InboundEvent{}, fmt.Errorf("decode dtmf event: %w", err)
		}
		return InboundEvent{Type: EventDTMF, DTMF: &evt}, nil
	case EventStop:
		var evt StopEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return InboundEvent{}, fmt.Errorf("decode stop event: %w", err)
		}
		return InboundEvent{Type: EventStop, Stop: &evt}, nil
	default:
		if logger != nil {
			logger.Warn("ignoring unknown inbound event", "event", envelope.Event)
		}
		return InboundEvent{Type: envelope.Event, RawType: envelope.Event}, nil
	}
}
