package sim

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"websocket/internal/media"
)

// PacingMode controls inbound media frame delivery timing.
type PacingMode string

const (
	PaceRealtime PacingMode = "realtime"
	PaceFast     PacingMode = "fast"
)

// RunConfig configures a carrier simulator run.
type RunConfig struct {
	WSURL           string
	StreamSID       string
	CallSID         string
	MediaFormat     media.AudioFormat
	Source          FrameSource
	Pace            PacingMode
	FrameDurationMs int
	RunTimeout      time.Duration
	Clock           media.Clock
}

func (c RunConfig) withDefaults() RunConfig {
	if c.StreamSID == "" {
		c.StreamSID = "MZ-SIM"
	}
	if c.CallSID == "" {
		c.CallSID = "CA-SIM"
	}
	if c.MediaFormat.Encoding == "" {
		c.MediaFormat = media.AudioFormat{
			Encoding:   "audio/x-mulaw",
			SampleRate: 8000,
			Channels:   1,
		}
	}
	if c.FrameDurationMs <= 0 {
		c.FrameDurationMs = 20
	}
	if c.Pace == "" {
		c.Pace = PaceRealtime
	}
	if c.RunTimeout <= 0 {
		c.RunTimeout = 30 * time.Second
	}
	if c.Clock == nil {
		c.Clock = media.RealClock{}
	}
	return c
}

// MarkRecord captures an outbound mark and optional echo time.
type MarkRecord struct {
	Name     string
	Received time.Time
	Echoed   time.Time
	AudioMs  int
}

// RunResult summarizes one simulated carrier call.
type RunResult struct {
	Turns              int
	OutboundAudioBytes int
	OutboundFrames     int
	Marks              []MarkRecord
	Clears             []time.Time
	FirstAudioLatency  time.Duration
	StartedAt          time.Time
	StoppedAt          time.Time
	Errors             []error
}

// CarrierSimulator acts as a Fonada/Exotel carrier client against the media server WS.
type CarrierSimulator struct {
	cfg RunConfig
}

// NewCarrierSimulator constructs a simulator with the given config.
func NewCarrierSimulator(cfg RunConfig) *CarrierSimulator {
	cfg = cfg.withDefaults()
	return &CarrierSimulator{cfg: cfg}
}

// Run dials the server, streams audio, records outbound events, echoes marks, and sends stop.
func (s *CarrierSimulator) Run(ctx context.Context) (*RunResult, error) {
	cfg := s.cfg
	if cfg.Source == nil {
		return nil, fmt.Errorf("frame source required")
	}
	_ = cfg.Source.Reset()

	runCtx, cancel := context.WithTimeout(ctx, cfg.RunTimeout)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.DialContext(runCtx, cfg.WSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	result := &RunResult{StartedAt: cfg.Clock.Now()}
	var recvMu sync.Mutex
	pendingAudioMs := 0

	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			recvMu.Lock()
			s.handleOutbound(data, result, &pendingAudioMs, conn)
			recvMu.Unlock()
		}
	}()

	if err := writeJSON(conn, map[string]any{"event": media.EventConnected}); err != nil {
		return result, err
	}
	startAt := cfg.Clock.Now()
	if err := writeJSON(conn, map[string]any{
		"event":      media.EventStart,
		"stream_sid": cfg.StreamSID,
		"call_sid":   cfg.CallSID,
		"media_format": map[string]any{
			"encoding":    cfg.MediaFormat.Encoding,
			"sample_rate": cfg.MediaFormat.SampleRate,
			"channels":    cfg.MediaFormat.Channels,
		},
	}); err != nil {
		return result, err
	}

	if cfg.Pace == PaceFast {
		select {
		case <-runCtx.Done():
			result.Errors = append(result.Errors, runCtx.Err())
		case <-time.After(300 * time.Millisecond):
		}
	}

	frameDur := time.Duration(cfg.FrameDurationMs) * time.Millisecond
	chunk := 0
	for {
		select {
		case <-runCtx.Done():
			result.Errors = append(result.Errors, runCtx.Err())
			goto stop
		default:
		}
		frame, err := cfg.Source.NextFrame()
		if err != nil {
			break
		}
		chunk++
		if err := writeJSON(conn, map[string]any{
			"event":      media.EventMedia,
			"stream_sid": cfg.StreamSID,
			"media": map[string]any{
				"payload": base64.StdEncoding.EncodeToString(frame),
				"chunk":   chunk,
			},
		}); err != nil {
			result.Errors = append(result.Errors, err)
			goto stop
		}
		if cfg.Pace == PaceRealtime {
			deadline := cfg.Clock.Now().Add(frameDur)
			for cfg.Clock.Now().Before(deadline) {
				select {
				case <-runCtx.Done():
					goto stop
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}
	}

stop:
	if cfg.Pace == PaceFast {
		select {
		case <-runCtx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
	_ = writeJSON(conn, map[string]any{
		"event":      media.EventStop,
		"stream_sid": cfg.StreamSID,
	})

	select {
	case <-recvDone:
	case <-time.After(2 * time.Second):
	}

	result.StoppedAt = cfg.Clock.Now()
	if result.FirstAudioLatency == 0 && result.OutboundAudioBytes > 0 {
		result.FirstAudioLatency = result.StoppedAt.Sub(startAt)
	}
	result.Turns = len(result.Marks)
	_ = startAt
	return result, nil
}

func (s *CarrierSimulator) handleOutbound(
	data []byte,
	result *RunResult,
	pendingAudioMs *int,
	conn *websocket.Conn,
) {
	var env struct {
		Event     string `json:"event"`
		StreamSID string `json:"stream_sid"`
		Media     struct {
			Payload string `json:"payload"`
		} `json:"media"`
		Mark struct {
			Name string `json:"name"`
		} `json:"mark"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return
	}
	now := s.cfg.Clock.Now()
	switch env.Event {
	case media.EventMedia:
		raw, err := base64.StdEncoding.DecodeString(env.Media.Payload)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("decode outbound media: %w", err))
			return
		}
		if result.OutboundAudioBytes == 0 {
			result.FirstAudioLatency = now.Sub(result.StartedAt)
		}
		result.OutboundAudioBytes += len(raw)
		result.OutboundFrames++
		*pendingAudioMs += len(raw) / 8
	case "clear":
		result.Clears = append(result.Clears, now)
	case media.EventMark:
		name := env.Mark.Name
		if name == "" {
			return
		}
		rec := MarkRecord{Name: name, Received: now, Echoed: now}
		_ = writeJSON(conn, map[string]any{
			"event":      media.EventMark,
			"stream_sid": s.cfg.StreamSID,
			"mark":       map[string]string{"name": name},
		})
		result.Marks = append(result.Marks, rec)
	}
}

func writeJSON(conn *websocket.Conn, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}
