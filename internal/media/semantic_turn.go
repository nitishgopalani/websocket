package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	defaultSemanticTurnTimeout       = 100 * time.Millisecond
	defaultSemanticCompleteSilenceMs = 280
	defaultSemanticConfidence        = 0.5
)

// EOUPrediction is the end-of-utterance signal from a semantic turn detector.
type EOUPrediction struct {
	Complete   bool
	Confidence float64
}

// SemanticTurnDetector predicts whether a transcript represents a complete turn.
type SemanticTurnDetector interface {
	Predict(ctx context.Context, transcript string, recentAudio []byte, rate int) (EOUPrediction, error)
	Close() error
}

// SemanticTurnConfig controls semantic endpoint refinement in TurnManager.
type SemanticTurnConfig struct {
	Enabled               bool
	Socket                string
	Addr                  string
	Timeout               time.Duration
	CompleteSilenceMs     int
	ConfidenceThreshold   float64
	LongSilenceFallbackMs int
}

// DefaultSemanticTurnConfig returns disabled semantic turn settings (CT-6 behavior).
func DefaultSemanticTurnConfig() SemanticTurnConfig {
	return SemanticTurnConfig{
		Enabled:               false,
		Timeout:               defaultSemanticTurnTimeout,
		CompleteSilenceMs:     defaultSemanticCompleteSilenceMs,
		ConfidenceThreshold:   defaultSemanticConfidence,
		LongSilenceFallbackMs: 0,
	}
}

func (c SemanticTurnConfig) withDefaults() SemanticTurnConfig {
	if c.Timeout <= 0 {
		c.Timeout = defaultSemanticTurnTimeout
	}
	if c.CompleteSilenceMs <= 0 {
		c.CompleteSilenceMs = defaultSemanticCompleteSilenceMs
	}
	if c.ConfidenceThreshold <= 0 {
		c.ConfidenceThreshold = defaultSemanticConfidence
	}
	return c
}

// SemanticTurnConfigFromEnv loads semantic turn settings from environment variables.
func SemanticTurnConfigFromEnv() SemanticTurnConfig {
	cfg := DefaultSemanticTurnConfig()
	if v := os.Getenv("SEMANTIC_TURN_ENABLED"); v == "1" || v == "true" || v == "TRUE" {
		cfg.Enabled = true
	}
	if v := os.Getenv("SEMANTIC_TURN_SOCKET"); v != "" {
		cfg.Socket = v
	}
	if v := os.Getenv("SEMANTIC_TURN_ADDR"); v != "" {
		cfg.Addr = v
	}
	if v := os.Getenv("SEMANTIC_TURN_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			cfg.Timeout = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv("SEMANTIC_COMPLETE_SILENCE_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			cfg.CompleteSilenceMs = ms
		}
	}
	return cfg.withDefaults()
}

// NewSemanticTurnDetector returns Noop when disabled, Remote when enabled.
func NewSemanticTurnDetector(cfg SemanticTurnConfig) (SemanticTurnDetector, error) {
	cfg = cfg.withDefaults()
	if !cfg.Enabled {
		return NoopSemanticTurn{}, nil
	}
	return NewRemoteSemanticTurn(cfg)
}

// NoopSemanticTurn is the default detector; TurnManager skips semantic refinement when disabled.
type NoopSemanticTurn struct{}

func (NoopSemanticTurn) Predict(_ context.Context, _ string, _ []byte, _ int) (EOUPrediction, error) {
	return EOUPrediction{Complete: true, Confidence: 1}, nil
}

func (NoopSemanticTurn) Close() error { return nil }

// IsNoopSemanticTurn reports whether semantic refinement is bypassed.
func IsNoopSemanticTurn(d SemanticTurnDetector) bool {
	_, ok := d.(NoopSemanticTurn)
	return ok
}

// RemoteSemanticTurn calls an out-of-process EOU worker (transcript v1; audio reserved for upgrades).
type RemoteSemanticTurn struct {
	dial    func() (net.Conn, error)
	timeout time.Duration
	logger  *slog.Logger

	fallbacks atomic.Int64
}

// NewRemoteSemanticTurn dials the configured semantic turn worker endpoint.
func NewRemoteSemanticTurn(cfg SemanticTurnConfig) (*RemoteSemanticTurn, error) {
	cfg = cfg.withDefaults()
	dial, err := semanticTurnDialer(cfg)
	if err != nil {
		return nil, err
	}
	return &RemoteSemanticTurn{
		dial:    dial,
		timeout: cfg.Timeout,
		logger:  slog.Default(),
	}, nil
}

func semanticTurnDialer(cfg SemanticTurnConfig) (func() (net.Conn, error), error) {
	switch {
	case cfg.Socket != "":
		return func() (net.Conn, error) { return net.Dial("unix", cfg.Socket) }, nil
	case cfg.Addr != "":
		return func() (net.Conn, error) { return net.Dial("tcp", cfg.Addr) }, nil
	default:
		return nil, errors.New("semantic turn enabled but neither SEMANTIC_TURN_SOCKET nor SEMANTIC_TURN_ADDR is set")
	}
}

func (r *RemoteSemanticTurn) Predict(ctx context.Context, transcript string, recentAudio []byte, rate int) (EOUPrediction, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	pred, err := r.predict(ctx, transcript, recentAudio, rate)
	if err != nil {
		r.fallbacks.Add(1)
		return EOUPrediction{}, err
	}
	return pred, nil
}

func (r *RemoteSemanticTurn) predict(ctx context.Context, transcript string, recentAudio []byte, rate int) (EOUPrediction, error) {
	req, err := encodeSemanticTurnRequest(transcript, recentAudio, rate)
	if err != nil {
		return EOUPrediction{}, err
	}

	conn, err := r.dialWithContext(ctx)
	if err != nil {
		return EOUPrediction{}, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return EOUPrediction{}, err
	}
	if _, err := conn.Write(req); err != nil {
		return EOUPrediction{}, err
	}

	body, err := readLengthPrefixedPayload(conn)
	if err != nil {
		return EOUPrediction{}, err
	}
	return parseSemanticTurnResponse(body)
}

func (r *RemoteSemanticTurn) Close() error { return nil }

func (r *RemoteSemanticTurn) dialWithContext(ctx context.Context) (net.Conn, error) {
	type connResult struct {
		conn net.Conn
		err  error
	}
	ch := make(chan connResult, 1)
	go func() {
		conn, err := r.dial()
		ch <- connResult{conn, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		return res.conn, res.err
	}
}

type semanticTurnWireRequest struct {
	Transcript string `json:"transcript"`
	RateHz     int    `json:"rate_hz,omitempty"`
	AudioB64   string `json:"audio_b64,omitempty"`
}

type semanticTurnWireResponse struct {
	Complete   bool    `json:"complete"`
	Confidence float64 `json:"confidence"`
}

func encodeSemanticTurnRequest(transcript string, recentAudio []byte, rate int) ([]byte, error) {
	wire := semanticTurnWireRequest{
		Transcript: transcript,
		RateHz:     rate,
	}
	if len(recentAudio) > 0 {
		const maxAudioBytes = 8000 * 2 * 3
		if len(recentAudio) > maxAudioBytes {
			recentAudio = recentAudio[len(recentAudio)-maxAudioBytes:]
		}
		wire.AudioB64 = base64.StdEncoding.EncodeToString(recentAudio)
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, denoiseHeaderSize+len(body))
	binaryLittleEndianPutUint32(buf[0:4], uint32(len(body)))
	copy(buf[4:], body)
	return buf, nil
}

func parseSemanticTurnResponse(body []byte) (EOUPrediction, error) {
	var wire semanticTurnWireResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		return EOUPrediction{}, fmt.Errorf("decode semantic turn response: %w", err)
	}
	return EOUPrediction{
		Complete:   wire.Complete,
		Confidence: wire.Confidence,
	}, nil
}

func binaryLittleEndianPutUint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
