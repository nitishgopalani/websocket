package media

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Dinesh / Asterisk binary-PCM16 WebSocket protocol (text control + binary audio).
const (
	AsteriskMsgSessionStart = "session_start"
	AsteriskMsgSessionEnd   = "session_end"
	AsteriskMsgReady        = "ready"
	AsteriskMsgEndOfCall    = "end_of_call"
	AsteriskMsgError        = "error"
)

// AsteriskSessionStart is the inbound session_start control frame.
type AsteriskSessionStart struct {
	Type           string                 `json:"type"`
	SessionID      string                 `json:"session_id"`
	ClientID       string                 `json:"client_id"`
	CustomerPhone  string                 `json:"customer_phone"`
	BusinessPhone  string                 `json:"business_phone"`
	Audio          AsteriskAudioSpec      `json:"audio"`
	Metadata       map[string]string      `json:"metadata"`
	MetadataLegacy AsteriskSessionMetaRaw `json:"-"`
}

// AsteriskAudioSpec describes codec and sample rates for the session.
type AsteriskAudioSpec struct {
	Codec            string `json:"codec"`
	InputSampleRate  int    `json:"input_sample_rate"`
	OutputSampleRate int    `json:"output_sample_rate"`
	Channels         int    `json:"channels"`
}

// AsteriskSessionMetaRaw captures metadata when sent as a nested object.
type AsteriskSessionMetaRaw struct {
	Language string `json:"language"`
	AgentID  string `json:"agent_id"`
}

// AsteriskControl is a parsed inbound text control frame.
type AsteriskControl struct {
	Type  string
	Start *AsteriskSessionStart
}

// ParseAsteriskControl unmarshals a session_start or session_end text frame.
func ParseAsteriskControl(data []byte) (AsteriskControl, error) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return AsteriskControl{}, fmt.Errorf("decode asterisk envelope: %w", err)
	}
	if envelope.Type == "" {
		return AsteriskControl{}, fmt.Errorf("missing type field")
	}
	switch envelope.Type {
	case AsteriskMsgSessionStart:
		var start AsteriskSessionStart
		if err := json.Unmarshal(data, &start); err != nil {
			return AsteriskControl{}, fmt.Errorf("decode session_start: %w", err)
		}
		if start.Metadata == nil {
			start.Metadata = map[string]string{}
		}
		var metaWrap struct {
			Metadata json.RawMessage `json:"metadata"`
		}
		_ = json.Unmarshal(data, &metaWrap)
		if len(metaWrap.Metadata) > 0 {
			var metaObj struct {
				Language string `json:"language"`
				AgentID  string `json:"agent_id"`
			}
			if err := json.Unmarshal(metaWrap.Metadata, &metaObj); err == nil {
				if metaObj.Language != "" {
					start.Metadata["language"] = metaObj.Language
				}
				if metaObj.AgentID != "" {
					start.Metadata["agent_id"] = metaObj.AgentID
				}
			}
		}
		return AsteriskControl{Type: AsteriskMsgSessionStart, Start: &start}, nil
	case AsteriskMsgSessionEnd:
		return AsteriskControl{Type: AsteriskMsgSessionEnd}, nil
	default:
		return AsteriskControl{Type: envelope.Type}, nil
	}
}

// AsteriskStartToStartEvent maps session_start to the internal StartEvent shape.
func AsteriskStartToStartEvent(s AsteriskSessionStart) StartEvent {
	inRate := s.Audio.InputSampleRate
	if inRate == 0 {
		inRate = 16000
	}
	outRate := s.Audio.OutputSampleRate
	if outRate == 0 {
		outRate = 24000
	}
	channels := s.Audio.Channels
	if channels == 0 {
		channels = 1
	}

	params := map[string]string{
		"client_id":          s.ClientID,
		"customer_phone":     s.CustomerPhone,
		"business_phone":     s.BusinessPhone,
		"output_sample_rate": strconv.Itoa(outRate),
		"carrier":            CarrierAsterisk,
	}
	for k, v := range s.Metadata {
		if v != "" {
			params[k] = v
		}
	}
	if lang := params["language"]; lang != "" {
		params["asr_language"] = lang
	}

	return StartEvent{
		Event:     EventStart,
		StreamSID: s.SessionID,
		CallSID:   s.CustomerPhone,
		MediaFormat: AudioFormat{
			Encoding:   "audio/x-l16",
			SampleRate: inRate,
			Channels:   channels,
		},
		CustomParameters: params,
	}
}

// AsteriskReadyMessage returns the ready control JSON.
func AsteriskReadyMessage() ([]byte, error) {
	return json.Marshal(map[string]string{"type": AsteriskMsgReady})
}

// AsteriskEndOfCallMessage returns the end_of_call control JSON.
func AsteriskEndOfCallMessage() ([]byte, error) {
	return json.Marshal(map[string]string{"type": AsteriskMsgEndOfCall})
}

// AsteriskErrorMessage returns an error control JSON frame.
func AsteriskErrorMessage(message, code string) ([]byte, error) {
	return json.Marshal(map[string]string{
		"type":    AsteriskMsgError,
		"message": message,
		"code":    code,
	})
}
