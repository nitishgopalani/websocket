package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"websocket/internal/media"
)

// AsteriskRunConfig configures the Dinesh binary-PCM16 protocol simulator.
type AsteriskRunConfig struct {
	WSURL            string
	SessionID        string
	ClientID         string
	CustomerPhone    string
	InputSampleRate  int
	OutputSampleRate int
	Language         string
	AgentID          string
	PCM16            []byte
	ChunkBytes       int
	Pace             PacingMode
	RunTimeout       time.Duration
	Clock            media.Clock
}

func (c AsteriskRunConfig) withDefaults() AsteriskRunConfig {
	if c.SessionID == "" {
		c.SessionID = "AST-SIM"
	}
	if c.ClientID == "" {
		c.ClientID = "sim-client"
	}
	if c.CustomerPhone == "" {
		c.CustomerPhone = "+919999999999"
	}
	if c.InputSampleRate == 0 {
		c.InputSampleRate = 16000
	}
	if c.OutputSampleRate == 0 {
		c.OutputSampleRate = 24000
	}
	if c.ChunkBytes <= 0 {
		c.ChunkBytes = 3200 // 100ms @ 16kHz PCM16
	}
	if c.Pace == "" {
		c.Pace = PaceFast
	}
	if c.RunTimeout <= 0 {
		c.RunTimeout = 15 * time.Second
	}
	if c.Clock == nil {
		c.Clock = media.RealClock{}
	}
	return c
}

// AsteriskRunResult summarizes one Asterisk protocol simulation.
type AsteriskRunResult struct {
	ReadyReceived        bool
	OutboundBinaryBytes  int
	OutboundBinaryFrames int
	EndOfCallReceived    bool
	ControlMessages      []string
}

// AsteriskSimulator speaks the Dinesh Asterisk binary-PCM16 WebSocket protocol.
type AsteriskSimulator struct {
	cfg AsteriskRunConfig
}

// NewAsteriskSimulator constructs a simulator with defaults applied.
func NewAsteriskSimulator(cfg AsteriskRunConfig) *AsteriskSimulator {
	return &AsteriskSimulator{cfg: cfg.withDefaults()}
}

// Run connects, sends session_start + binary audio + session_end, and collects outbound frames.
func (s *AsteriskSimulator) Run(ctx context.Context) (*AsteriskRunResult, error) {
	cfg := s.cfg
	if cfg.WSURL == "" {
		return nil, fmt.Errorf("WSURL required")
	}
	runCtx, cancel := context.WithTimeout(ctx, cfg.RunTimeout)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.DialContext(runCtx, cfg.WSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	result := &AsteriskRunResult{}
	var readMu sync.Mutex
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			readMu.Lock()
			switch msgType {
			case websocket.TextMessage:
				var ctrl struct {
					Type string `json:"type"`
				}
				_ = json.Unmarshal(data, &ctrl)
				result.ControlMessages = append(result.ControlMessages, ctrl.Type)
				switch ctrl.Type {
				case media.AsteriskMsgReady:
					result.ReadyReceived = true
				case media.AsteriskMsgEndOfCall:
					result.EndOfCallReceived = true
				}
			case websocket.BinaryMessage:
				result.OutboundBinaryBytes += len(data)
				result.OutboundBinaryFrames++
			}
			readMu.Unlock()
		}
	}()

	startPayload, err := json.Marshal(map[string]any{
		"type":           media.AsteriskMsgSessionStart,
		"session_id":     cfg.SessionID,
		"client_id":      cfg.ClientID,
		"customer_phone": cfg.CustomerPhone,
		"business_phone": "+918000000000",
		"audio": map[string]any{
			"codec":              "pcm16",
			"input_sample_rate":  cfg.InputSampleRate,
			"output_sample_rate": cfg.OutputSampleRate,
			"channels":           1,
		},
		"metadata": map[string]string{
			"language": cfg.Language,
			"agent_id": cfg.AgentID,
		},
	})
	if err != nil {
		return nil, err
	}
	if err := conn.WriteMessage(websocket.TextMessage, startPayload); err != nil {
		return nil, fmt.Errorf("session_start: %w", err)
	}

	deadline := cfg.Clock.Now().Add(2 * time.Second)
	for cfg.Clock.Now().Before(deadline) {
		readMu.Lock()
		ready := result.ReadyReceived
		readMu.Unlock()
		if ready {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	readMu.Lock()
	if !result.ReadyReceived {
		readMu.Unlock()
		return result, fmt.Errorf("ready not received")
	}
	readMu.Unlock()

	pcm := cfg.PCM16
	if len(pcm) == 0 {
		pcm = make([]byte, cfg.ChunkBytes*5)
	}
	frameDur := time.Millisecond
	if cfg.Pace == PaceRealtime {
		frameDur = 100 * time.Millisecond
	}
	for off := 0; off < len(pcm); off += cfg.ChunkBytes {
		end := off + cfg.ChunkBytes
		if end > len(pcm) {
			end = len(pcm)
		}
		chunk := pcm[off:end]
		if err := conn.WriteMessage(websocket.BinaryMessage, chunk); err != nil {
			return result, fmt.Errorf("binary audio: %w", err)
		}
		if cfg.Pace == PaceRealtime {
			select {
			case <-runCtx.Done():
				return result, runCtx.Err()
			case <-time.After(frameDur):
			}
		}
	}

	time.Sleep(200 * time.Millisecond)

	endPayload, _ := json.Marshal(map[string]string{"type": media.AsteriskMsgSessionEnd})
	_ = conn.WriteMessage(websocket.TextMessage, endPayload)

	select {
	case <-readDone:
	case <-time.After(500 * time.Millisecond):
	}

	readMu.Lock()
	defer readMu.Unlock()
	return result, nil
}

// PCM16Silence returns n bytes of zero-valued PCM16 LE silence.
func PCM16Silence(bytes int) []byte {
	if bytes < 0 {
		bytes = 0
	}
	return make([]byte, bytes)
}
