package media

import (
	"context"
	"encoding/binary"
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
	defaultDenoiseTimeout = 15 * time.Millisecond
	denoiseHeaderSize     = 4
	denoiseRateFieldSize  = 2
)

// DenoiseConfig controls the out-of-process denoise worker client.
type DenoiseConfig struct {
	Enabled bool
	Socket  string
	Addr    string
	Timeout time.Duration
}

// DefaultDenoiseConfig returns disabled denoise settings (Noop path).
func DefaultDenoiseConfig() DenoiseConfig {
	return DenoiseConfig{
		Enabled: false,
		Timeout: defaultDenoiseTimeout,
	}
}

// DenoiseConfigFromEnv loads denoise settings from environment variables.
func DenoiseConfigFromEnv() DenoiseConfig {
	cfg := DefaultDenoiseConfig()
	if v := os.Getenv("DENOISE_ENABLED"); v == "1" || v == "true" || v == "TRUE" {
		cfg.Enabled = true
	}
	if v := os.Getenv("DENOISE_SOCKET"); v != "" {
		cfg.Socket = v
	}
	if v := os.Getenv("DENOISE_ADDR"); v != "" {
		cfg.Addr = v
	}
	if v := os.Getenv("DENOISE_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			cfg.Timeout = time.Duration(ms) * time.Millisecond
		}
	}
	return cfg.withDefaults()
}

func (c DenoiseConfig) withDefaults() DenoiseConfig {
	if c.Timeout <= 0 {
		c.Timeout = defaultDenoiseTimeout
	}
	return c
}

// Denoiser removes noise from a single PCM16 mono frame.
type Denoiser interface {
	Process(ctx context.Context, pcm16 []byte, rate int) ([]byte, error)
	Close() error
}

// NoopDenoiser passes PCM16 through unchanged.
type NoopDenoiser struct{}

func (NoopDenoiser) Process(_ context.Context, pcm16 []byte, _ int) ([]byte, error) {
	out := make([]byte, len(pcm16))
	copy(out, pcm16)
	return out, nil
}

func (NoopDenoiser) Close() error { return nil }

// RemoteDenoiser calls an out-of-process worker over a length-prefixed binary protocol.
// Each Process call uses a fresh connection, matching the worker's one-request-per-accept model.
type RemoteDenoiser struct {
	dial    func() (net.Conn, error)
	timeout time.Duration
	logger  *slog.Logger

	fallbacks atomic.Int64
}

// NewDenoiser returns Noop when disabled, Remote when enabled.
func NewDenoiser(cfg DenoiseConfig) (Denoiser, error) {
	cfg = cfg.withDefaults()
	if !cfg.Enabled {
		return NoopDenoiser{}, nil
	}
	return NewRemoteDenoiser(cfg)
}

// NewRemoteDenoiser dials the configured worker endpoint.
func NewRemoteDenoiser(cfg DenoiseConfig) (*RemoteDenoiser, error) {
	cfg = cfg.withDefaults()
	dial, err := denoiseDialer(cfg)
	if err != nil {
		return nil, err
	}
	logger := slog.Default()
	return &RemoteDenoiser{
		dial:    dial,
		timeout: cfg.Timeout,
		logger:  logger,
	}, nil
}

func denoiseDialer(cfg DenoiseConfig) (func() (net.Conn, error), error) {
	switch {
	case cfg.Socket != "":
		return func() (net.Conn, error) {
			return net.Dial("unix", cfg.Socket)
		}, nil
	case cfg.Addr != "":
		return func() (net.Conn, error) {
			return net.Dial("tcp", cfg.Addr)
		}, nil
	default:
		return nil, errors.New("denoise enabled but neither DENOISE_SOCKET nor DENOISE_ADDR is set")
	}
}

// Fallbacks returns the number of fail-open fallbacks due to worker errors/timeouts.
func (r *RemoteDenoiser) Fallbacks() int64 {
	return r.fallbacks.Load()
}

func (r *RemoteDenoiser) Process(ctx context.Context, pcm16 []byte, rate int) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	out, err := r.process(ctx, pcm16, rate)
	if err != nil {
		r.fallbacks.Add(1)
		return nil, err
	}
	return out, nil
}

func (r *RemoteDenoiser) process(ctx context.Context, pcm16 []byte, rate int) ([]byte, error) {
	if len(pcm16)%2 != 0 {
		return nil, ErrInvalidPCM16Length
	}
	if rate != 8000 && rate != 16000 {
		return nil, fmt.Errorf("unsupported denoise sample rate: %d", rate)
	}

	req, err := encodeDenoiseRequest(pcm16, rate)
	if err != nil {
		return nil, err
	}

	conn, err := r.dialWithContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return nil, err
	}
	if _, err := conn.Write(req); err != nil {
		return nil, err
	}

	resp, err := readDenoiseResponse(conn)
	if err != nil {
		return nil, err
	}
	if len(resp) != len(pcm16) {
		return nil, fmt.Errorf("denoise worker returned %d bytes, want %d", len(resp), len(pcm16))
	}
	return resp, nil
}

func (r *RemoteDenoiser) Close() error {
	return nil
}

func (r *RemoteDenoiser) dialWithContext(ctx context.Context) (net.Conn, error) {
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

// Wire format (little-endian):
// Request:  [uint32 len][uint16 rate_hz][pcm16 bytes]  where len = 2 + len(pcm16)
// Response: [uint32 len][pcm16 bytes]
func encodeDenoiseRequest(pcm16 []byte, rate int) ([]byte, error) {
	if len(pcm16)%2 != 0 {
		return nil, ErrInvalidPCM16Length
	}
	bodyLen := denoiseRateFieldSize + len(pcm16)
	buf := make([]byte, denoiseHeaderSize+bodyLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(bodyLen))
	binary.LittleEndian.PutUint16(buf[4:6], uint16(rate))
	copy(buf[6:], pcm16)
	return buf, nil
}

func readDenoiseResponse(r io.Reader) ([]byte, error) {
	hdr := make([]byte, denoiseHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr)
	if n == 0 || n > 1<<20 {
		return nil, fmt.Errorf("invalid denoise response length: %d", n)
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	if len(out)%2 != 0 {
		return nil, ErrInvalidPCM16Length
	}
	return out, nil
}

func encodeDenoiseResponse(pcm16 []byte) []byte {
	buf := make([]byte, denoiseHeaderSize+len(pcm16))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(pcm16)))
	copy(buf[4:], pcm16)
	return buf
}
