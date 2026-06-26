package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	defaultAMDWindowMs            = 2000
	defaultAMDTimeout             = 500 * time.Millisecond
	defaultAMDProbaHumanThreshold = 0.4
	defaultAMDBufferMarginMs      = 500
)

var ErrAMDWorkerUnavailable = errors.New("amd worker unavailable")

// AMDResult classifies the callee on an outbound dial.
type AMDResult int

const (
	AMDHuman AMDResult = iota
	AMDMachine
	AMDUnknown
)

func (r AMDResult) String() string {
	switch r {
	case AMDHuman:
		return "human"
	case AMDMachine:
		return "machine"
	default:
		return "unknown"
	}
}

// AMDDecision is the outcome of answering-machine detection.
type AMDDecision struct {
	Result     AMDResult
	ProbaHuman float64
	Reason     string
	Final      bool
}

// AMDClassifier inspects the first seconds of call audio.
type AMDClassifier interface {
	Classify(ctx context.Context, pcm16 []byte, rate int) (AMDDecision, error)
	Close() error
}

// AMDConfig controls the AMD worker client and gate thresholds.
type AMDConfig struct {
	Enabled             bool
	Socket              string
	Addr                string
	WindowMs            int
	Timeout             time.Duration
	ProbaHumanThreshold float64
	BufferMarginMs      int
}

// DefaultAMDConfig returns disabled AMD settings (fail-open Noop path).
func DefaultAMDConfig() AMDConfig {
	return AMDConfig{
		Enabled:             false,
		WindowMs:            defaultAMDWindowMs,
		Timeout:             defaultAMDTimeout,
		ProbaHumanThreshold: defaultAMDProbaHumanThreshold,
		BufferMarginMs:      defaultAMDBufferMarginMs,
	}
}

// AMDConfigFromEnv loads AMD settings from environment variables.
func AMDConfigFromEnv() AMDConfig {
	cfg := DefaultAMDConfig()
	if v := os.Getenv("AMD_ENABLED"); v == "1" || v == "true" || v == "TRUE" {
		cfg.Enabled = true
	}
	if v := os.Getenv("AMD_SOCKET"); v != "" {
		cfg.Socket = v
	}
	if v := os.Getenv("AMD_ADDR"); v != "" {
		cfg.Addr = v
	}
	if v := os.Getenv("AMD_WINDOW_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			cfg.WindowMs = ms
		}
	}
	if v := os.Getenv("AMD_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			cfg.Timeout = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv("AMD_PROBA_HUMAN_THRESHOLD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.ProbaHumanThreshold = f
		}
	}
	return cfg.withDefaults()
}

func (c AMDConfig) withDefaults() AMDConfig {
	if c.WindowMs <= 0 {
		c.WindowMs = defaultAMDWindowMs
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultAMDTimeout
	}
	if c.ProbaHumanThreshold <= 0 {
		c.ProbaHumanThreshold = defaultAMDProbaHumanThreshold
	}
	if c.BufferMarginMs <= 0 {
		c.BufferMarginMs = defaultAMDBufferMarginMs
	}
	return c
}

// NewAMDClassifier returns Noop when disabled, Remote when enabled.
func NewAMDClassifier(cfg AMDConfig) (AMDClassifier, error) {
	cfg = cfg.withDefaults()
	if !cfg.Enabled {
		return NoopAMDClassifier{}, nil
	}
	return NewRemoteAMDClassifier(cfg)
}

// NoopAMDClassifier always returns Human (fail-open default).
type NoopAMDClassifier struct{}

func (NoopAMDClassifier) Classify(_ context.Context, _ []byte, _ int) (AMDDecision, error) {
	return AMDDecision{
		Result:     AMDHuman,
		ProbaHuman: 1,
		Reason:     "noop_amd_disabled",
		Final:      true,
	}, nil
}

func (NoopAMDClassifier) Close() error { return nil }

// IsNoopAMD reports whether the classifier bypasses detection.
func IsNoopAMD(c AMDClassifier) bool {
	_, ok := c.(NoopAMDClassifier)
	return ok
}

// RemoteAMDClassifier calls the out-of-process Whisper-small AMD worker.
type RemoteAMDClassifier struct {
	dial                func() (net.Conn, error)
	timeout             time.Duration
	probaHumanThreshold float64
	logger              *slog.Logger
	fallbacks           atomic.Int64
}

// NewRemoteAMDClassifier dials the configured AMD worker endpoint.
func NewRemoteAMDClassifier(cfg AMDConfig) (*RemoteAMDClassifier, error) {
	cfg = cfg.withDefaults()
	dial, err := amdDialer(cfg)
	if err != nil {
		return nil, err
	}
	return &RemoteAMDClassifier{
		dial:                dial,
		timeout:             cfg.Timeout,
		probaHumanThreshold: cfg.ProbaHumanThreshold,
		logger:              slog.Default(),
	}, nil
}

func amdDialer(cfg AMDConfig) (func() (net.Conn, error), error) {
	switch {
	case cfg.Socket != "":
		return func() (net.Conn, error) { return net.Dial("unix", cfg.Socket) }, nil
	case cfg.Addr != "":
		return func() (net.Conn, error) { return net.Dial("tcp", cfg.Addr) }, nil
	default:
		return nil, errors.New("amd enabled but neither AMD_SOCKET nor AMD_ADDR is set")
	}
}

func (r *RemoteAMDClassifier) Classify(ctx context.Context, pcm16 []byte, rate int) (AMDDecision, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	decision, err := r.classify(ctx, pcm16, rate)
	if err != nil {
		r.fallbacks.Add(1)
		return AMDDecision{}, err
	}
	decision = applyAMDThreshold(decision, r.probaHumanThreshold)
	decision.Final = true
	return decision, nil
}

func (r *RemoteAMDClassifier) classify(ctx context.Context, pcm16 []byte, rate int) (AMDDecision, error) {
	if len(pcm16)%2 != 0 {
		return AMDDecision{}, ErrInvalidPCM16Length
	}
	if rate != 8000 && rate != 16000 {
		return AMDDecision{}, fmt.Errorf("unsupported amd sample rate: %d", rate)
	}

	req, err := encodeDenoiseRequest(pcm16, rate)
	if err != nil {
		return AMDDecision{}, err
	}

	conn, err := r.dialWithContext(ctx)
	if err != nil {
		return AMDDecision{}, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return AMDDecision{}, err
	}
	if _, err := conn.Write(req); err != nil {
		return AMDDecision{}, err
	}

	body, err := readLengthPrefixedPayload(conn)
	if err != nil {
		return AMDDecision{}, err
	}
	return parseAMDResponse(body)
}

func (r *RemoteAMDClassifier) Close() error { return nil }

func (r *RemoteAMDClassifier) dialWithContext(ctx context.Context) (net.Conn, error) {
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

type amdWireResponse struct {
	Result     string  `json:"result"`
	ProbaHuman float64 `json:"proba_human"`
	Reason     string  `json:"reason"`
}

func parseAMDResponse(body []byte) (AMDDecision, error) {
	var wire amdWireResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		return AMDDecision{}, fmt.Errorf("decode amd response: %w", err)
	}
	decision := AMDDecision{
		ProbaHuman: wire.ProbaHuman,
		Reason:     wire.Reason,
	}
	switch wire.Result {
	case "human":
		decision.Result = AMDHuman
	case "machine":
		decision.Result = AMDMachine
	default:
		decision.Result = AMDUnknown
	}
	return decision, nil
}

func applyAMDThreshold(decision AMDDecision, threshold float64) AMDDecision {
	if decision.Result == AMDMachine && decision.ProbaHuman >= threshold {
		decision.Result = AMDHuman
		decision.Reason = decision.Reason + "; fail_open_threshold"
	}
	if decision.Result == AMDUnknown {
		decision.Result = AMDHuman
		decision.ProbaHuman = 1
		decision.Reason = "unknown_fail_open_human"
	}
	return decision
}

func FailOpenHumanDecision(reason string) AMDDecision {
	return AMDDecision{
		Result:     AMDHuman,
		ProbaHuman: 1,
		Reason:     reason,
		Final:      true,
	}
}

func readLengthPrefixedPayload(r io.Reader) ([]byte, error) {
	hdr := make([]byte, denoiseHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	n := binaryLittleEndianUint32(hdr)
	if n == 0 || n > 1<<20 {
		return nil, fmt.Errorf("invalid amd response length: %d", n)
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

func binaryLittleEndianUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func pcmBytesForDurationMs(ms, sampleRate int) int {
	return ms * sampleRate * 2 / 1000
}
