package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
)

const (
	defaultBackchannelMaxWords = 4
	defaultBackchannelMaxRunes = 32
	defaultBackchannelTimeout  = 50 * time.Millisecond
)

var defaultBackchannelLexicon = []string{
	"haan", "haan ji", "ji", "ji haan", "theek hai", "achha", "accha",
	"hmm", "hm", "ok", "okay", "right", "yeah", "sahi", "bilkul",
}

// BackchannelConfig controls backchannel classification in TurnManager.
type BackchannelConfig struct {
	Enabled  bool
	Lexicon  []string
	MaxWords int
	MaxRunes int
	Socket   string
	Addr     string
	Timeout  time.Duration
}

// DefaultBackchannelConfig returns lexicon-based backchannel settings (enabled by default).
func DefaultBackchannelConfig() BackchannelConfig {
	return BackchannelConfig{
		Enabled:  true,
		Lexicon:  append([]string(nil), defaultBackchannelLexicon...),
		MaxWords: defaultBackchannelMaxWords,
		MaxRunes: defaultBackchannelMaxRunes,
		Timeout:  defaultBackchannelTimeout,
	}
}

func (c BackchannelConfig) withDefaults() BackchannelConfig {
	if c.MaxWords <= 0 {
		c.MaxWords = defaultBackchannelMaxWords
	}
	if c.MaxRunes <= 0 {
		c.MaxRunes = defaultBackchannelMaxRunes
	}
	if len(c.Lexicon) == 0 {
		c.Lexicon = append([]string(nil), defaultBackchannelLexicon...)
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultBackchannelTimeout
	}
	return c
}

// BackchannelConfigFromEnv loads backchannel settings from environment variables.
func BackchannelConfigFromEnv() BackchannelConfig {
	cfg := DefaultBackchannelConfig()
	if v := os.Getenv("BACKCHANNEL_ENABLED"); v == "0" || v == "false" || v == "FALSE" {
		cfg.Enabled = false
	}
	if v := os.Getenv("BACKCHANNEL_LEXICON"); v != "" {
		parts := strings.Split(v, ",")
		lex := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				lex = append(lex, p)
			}
		}
		if len(lex) > 0 {
			cfg.Lexicon = lex
		}
	}
	if v := os.Getenv("BACKCHANNEL_SOCKET"); v != "" {
		cfg.Socket = v
	}
	if v := os.Getenv("BACKCHANNEL_ADDR"); v != "" {
		cfg.Addr = v
	}
	return cfg.withDefaults()
}

// BackchannelClassifier detects short acknowledgement backchannels during agent speech.
type BackchannelClassifier interface {
	IsBackchannel(ctx context.Context, transcript string, audio []byte, rate int) (bool, error)
	Close() error
}

// NewBackchannelClassifier returns Noop when disabled, Remote when remote configured, Lexicon otherwise.
func NewBackchannelClassifier(cfg BackchannelConfig) (BackchannelClassifier, error) {
	cfg = cfg.withDefaults()
	if !cfg.Enabled {
		return NoopBackchannel{}, nil
	}
	if cfg.Socket != "" || cfg.Addr != "" {
		return NewRemoteBackchannel(cfg)
	}
	return NewLexiconBackchannel(cfg), nil
}

// NoopBackchannel never classifies utterances as backchannels.
type NoopBackchannel struct{}

func (NoopBackchannel) IsBackchannel(_ context.Context, _ string, _ []byte, _ int) (bool, error) {
	return false, nil
}

func (NoopBackchannel) Close() error { return nil }

// IsNoopBackchannel reports whether backchannel suppression is bypassed.
func IsNoopBackchannel(c BackchannelClassifier) bool {
	_, ok := c.(NoopBackchannel)
	return ok
}

// LexiconBackchannel matches short utterances against a Hindi/English acknowledgement lexicon.
type LexiconBackchannel struct {
	lexicon  map[string]struct{}
	maxWords int
	maxRunes int
}

// NewLexiconBackchannel returns a pure-Go lexicon backchannel classifier.
func NewLexiconBackchannel(cfg BackchannelConfig) *LexiconBackchannel {
	cfg = cfg.withDefaults()
	lex := make(map[string]struct{}, len(cfg.Lexicon))
	for _, phrase := range cfg.Lexicon {
		norm := normalizeBackchannelText(phrase)
		if norm != "" {
			lex[norm] = struct{}{}
		}
	}
	return &LexiconBackchannel{
		lexicon:  lex,
		maxWords: cfg.MaxWords,
		maxRunes: cfg.MaxRunes,
	}
}

func (l *LexiconBackchannel) IsBackchannel(_ context.Context, transcript string, _ []byte, _ int) (bool, error) {
	norm := normalizeBackchannelText(transcript)
	if norm == "" {
		return false, nil
	}
	if !isShortUtterance(norm, l.maxWords, l.maxRunes) {
		return false, nil
	}
	_, ok := l.lexicon[norm]
	return ok, nil
}

func (l *LexiconBackchannel) Close() error { return nil }

func normalizeBackchannelText(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.Trim(s, ".,!?;:\"'")
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func isShortUtterance(norm string, maxWords, maxRunes int) bool {
	words := strings.Fields(norm)
	if len(words) == 0 || len(words) > maxWords {
		return false
	}
	runes := 0
	for _, r := range norm {
		if unicode.IsSpace(r) {
			continue
		}
		runes++
	}
	return runes <= maxRunes
}

// RemoteBackchannel is an optional out-of-process backchannel classifier seam.
type RemoteBackchannel struct {
	dial    func() (net.Conn, error)
	timeout time.Duration
	logger  *slog.Logger

	fallbacks atomic.Int64
}

// NewRemoteBackchannel dials the configured backchannel worker endpoint.
func NewRemoteBackchannel(cfg BackchannelConfig) (*RemoteBackchannel, error) {
	cfg = cfg.withDefaults()
	dial, err := backchannelDialer(cfg)
	if err != nil {
		return nil, err
	}
	return &RemoteBackchannel{
		dial:    dial,
		timeout: cfg.Timeout,
		logger:  slog.Default(),
	}, nil
}

func backchannelDialer(cfg BackchannelConfig) (func() (net.Conn, error), error) {
	switch {
	case cfg.Socket != "":
		return func() (net.Conn, error) { return net.Dial("unix", cfg.Socket) }, nil
	case cfg.Addr != "":
		return func() (net.Conn, error) { return net.Dial("tcp", cfg.Addr) }, nil
	default:
		return nil, errors.New("backchannel remote configured without BACKCHANNEL_SOCKET or BACKCHANNEL_ADDR")
	}
}

func (r *RemoteBackchannel) IsBackchannel(ctx context.Context, transcript string, audio []byte, rate int) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	ok, err := r.classify(ctx, transcript, audio, rate)
	if err != nil {
		r.fallbacks.Add(1)
		return false, err
	}
	return ok, nil
}

func (r *RemoteBackchannel) classify(ctx context.Context, transcript string, audio []byte, rate int) (bool, error) {
	req, err := encodeBackchannelRequest(transcript, audio, rate)
	if err != nil {
		return false, err
	}

	conn, err := r.dialWithContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return false, err
	}
	if _, err := conn.Write(req); err != nil {
		return false, err
	}

	body, err := readLengthPrefixedPayload(conn)
	if err != nil {
		return false, err
	}
	return parseBackchannelResponse(body)
}

func (r *RemoteBackchannel) Close() error { return nil }

func (r *RemoteBackchannel) dialWithContext(ctx context.Context) (net.Conn, error) {
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

type backchannelWireRequest struct {
	Transcript string `json:"transcript"`
	RateHz     int    `json:"rate_hz,omitempty"`
}

type backchannelWireResponse struct {
	Backchannel bool `json:"backchannel"`
}

func encodeBackchannelRequest(transcript string, _ []byte, rate int) ([]byte, error) {
	body, err := json.Marshal(backchannelWireRequest{Transcript: transcript, RateHz: rate})
	if err != nil {
		return nil, err
	}
	buf := make([]byte, denoiseHeaderSize+len(body))
	binaryLittleEndianPutUint32(buf[0:4], uint32(len(body)))
	copy(buf[4:], body)
	return buf, nil
}

func parseBackchannelResponse(body []byte) (bool, error) {
	var wire backchannelWireResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		return false, fmt.Errorf("decode backchannel response: %w", err)
	}
	return wire.Backchannel, nil
}
