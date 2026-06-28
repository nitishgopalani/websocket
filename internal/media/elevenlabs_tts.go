package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type ttsDial func(ctx context.Context, wsURL string, header http.Header) (*websocket.Conn, *http.Response, error)

// ElevenLabsTTSProvider opens persistent ElevenLabs stream-input WebSocket sessions.
type ElevenLabsTTSProvider struct {
	cfg    TTSConfig
	apiKey string
	dial   ttsDial
	logger *slog.Logger
}

// NewElevenLabsTTSProvider constructs an ElevenLabs streaming TTS provider.
func NewElevenLabsTTSProvider(cfg TTSConfig) (*ElevenLabsTTSProvider, error) {
	cfg = cfg.withDefaults()
	return &ElevenLabsTTSProvider{
		cfg:    cfg,
		apiKey: cfg.APIKey,
		dial:   defaultTTSDial,
		logger: slog.Default(),
	}, nil
}

func defaultTTSDial(ctx context.Context, wsURL string, header http.Header) (*websocket.Conn, *http.Response, error) {
	d := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	return d.DialContext(ctx, wsURL, header)
}

func (p *ElevenLabsTTSProvider) Open(ctx context.Context, meta TTSSessionMeta) (TTSStream, error) {
	cfg := p.cfg.withDefaults()
	if meta.OutputFormat != "" {
		cfg.OutputFormat = meta.OutputFormat
	}
	s := &elevenLabsStream{
		provider:  p,
		meta:      meta,
		cfg:       cfg,
		apiKey:    p.apiKey,
		dial:      p.dial,
		logger:    p.logger,
		audio:     make(chan TTSAudioChunk, defaultTTSAudioBuffer),
		done:      make(chan struct{}),
		cancelled: make(map[string]struct{}),
		turnSeq:   make(map[string]int),
	}
	if err := s.connect(ctx); err != nil {
		return nil, err
	}
	p.logger.Info("elevenlabs session dialed",
		"stream_sid", meta.StreamSID,
		"output_format", cfg.OutputFormat,
		"output_sample_rate", meta.OutputSampleRate,
	)
	s.wg.Add(1)
	go s.readLoop()
	return s, nil
}

type elevenLabsStream struct {
	provider *ElevenLabsTTSProvider
	meta     TTSSessionMeta
	cfg      TTSConfig
	apiKey   string
	dial     ttsDial
	logger   *slog.Logger

	mu          sync.Mutex
	conn        *websocket.Conn
	closed      bool
	initialized bool
	activeTurn  string
	cancelled   map[string]struct{}
	turnSeq     map[string]int

	audio chan TTSAudioChunk
	done  chan struct{}
	wg    sync.WaitGroup

	reconnects atomic.Int64
	fallbacks  atomic.Int64
}

func (s *elevenLabsStream) Reconnects() int64 { return s.reconnects.Load() }
func (s *elevenLabsStream) Fallbacks() int64  { return s.fallbacks.Load() }

func (s *elevenLabsStream) buildWSURL() string {
	base := s.cfg.BaseURL
	path := fmt.Sprintf("/v1/text-to-speech/%s/stream-input", url.PathEscape(s.cfg.VoiceID))
	u, err := url.Parse(base + path)
	if err != nil {
		return base + path
	}
	q := u.Query()
	q.Set("model_id", s.cfg.Model)
	q.Set("output_format", s.cfg.OutputFormat)
	if s.cfg.InactivitySecs > 0 {
		q.Set("inactivity_timeout", fmt.Sprintf("%d", s.cfg.InactivitySecs))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *elevenLabsStream) connect(ctx context.Context) error {
	header := http.Header{}
	header.Set("xi-api-key", s.apiKey)

	conn, _, err := s.dial(ctx, s.buildWSURL(), header)
	if err != nil {
		return fmt.Errorf("elevenlabs dial: %w", err)
	}

	s.mu.Lock()
	s.conn = conn
	s.initialized = false
	s.mu.Unlock()

	return s.sendInitLocked()
}

func (s *elevenLabsStream) sendInitLocked() error {
	if s.conn == nil {
		return fmt.Errorf("elevenlabs connection not ready")
	}
	init := map[string]any{
		"text": " ",
		"voice_settings": map[string]any{
			"stability":         0.5,
			"similarity_boost":  0.75,
			"use_speaker_boost": false,
		},
		"generation_config": map[string]any{
			"chunk_length_schedule": []int{50, 80, 120},
		},
	}
	payload, err := json.Marshal(init)
	if err != nil {
		return err
	}
	if err := s.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return err
	}
	s.initialized = true
	return nil
}

func (s *elevenLabsStream) Speak(turnID string, text string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrTTSStreamClosed
	}
	if _, cancelled := s.cancelled[turnID]; cancelled && text != "" {
		s.mu.Unlock()
		return nil
	}
	s.activeTurn = turnID
	conn := s.conn
	s.mu.Unlock()

	if conn == nil {
		s.emitFailSoft(turnID)
		return nil
	}

	msg := map[string]any{"text": text}
	if text != "" {
		msg["flush"] = true
	} else {
		msg["flush"] = true
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		s.logger.Warn("elevenlabs speak write failed", "stream_sid", s.meta.StreamSID, "error", err)
		go s.tryReconnect(context.Background())
		s.emitFailSoft(turnID)
		return nil
	}
	return nil
}

func (s *elevenLabsStream) Cancel(turnID string) error {
	s.mu.Lock()
	s.cancelled[turnID] = struct{}{}
	active := s.activeTurn == turnID
	conn := s.conn
	s.mu.Unlock()

	if active && conn != nil {
		payload, _ := json.Marshal(map[string]any{"text": "", "flush": true})
		_ = conn.WriteMessage(websocket.TextMessage, payload)
	}
	return nil
}

func (s *elevenLabsStream) Audio() <-chan TTSAudioChunk { return s.audio }

func (s *elevenLabsStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()

	close(s.done)
	if conn != nil {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"text":""}`))
		_ = conn.Close()
	}
	s.wg.Wait()
	close(s.audio)
	return nil
}

func (s *elevenLabsStream) readLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.mu.Lock()
		conn := s.conn
		s.mu.Unlock()
		if conn == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
			}
			s.logger.Warn("elevenlabs read ended; scheduling reconnect",
				"stream_sid", s.meta.StreamSID,
				"error", err,
			)
			s.mu.Lock()
			_ = s.closeConnLocked()
			s.mu.Unlock()
			go s.tryReconnect(context.Background())
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.handleInbound(data)
	}
}

func (s *elevenLabsStream) handleInbound(data []byte) {
	var wire struct {
		Audio   string `json:"audio"`
		IsFinal *bool  `json:"isFinal"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return
	}

	s.mu.Lock()
	turnID := s.activeTurn
	if turnID == "" {
		s.mu.Unlock()
		return
	}
	if _, cancelled := s.cancelled[turnID]; cancelled {
		s.mu.Unlock()
		return
	}

	if wire.IsFinal != nil && *wire.IsFinal {
		s.turnSeq[turnID]++
		seq := s.turnSeq[turnID]
		s.mu.Unlock()
		s.emit(TTSAudioChunk{TurnID: turnID, Seq: seq, Final: true})
		return
	}

	if wire.Audio == "" {
		s.mu.Unlock()
		return
	}

	raw, err := base64.StdEncoding.DecodeString(wire.Audio)
	if err != nil {
		s.mu.Unlock()
		return
	}
	s.turnSeq[turnID]++
	seq := s.turnSeq[turnID]
	s.mu.Unlock()

	s.emit(TTSAudioChunk{
		TurnID: turnID,
		Seq:    seq,
		MuLaw:  raw,
		Final:  false,
	})
}

func (s *elevenLabsStream) emit(chunk TTSAudioChunk) {
	select {
	case <-s.done:
	case s.audio <- chunk:
	default:
		s.logger.Warn("tts audio buffer full; dropping chunk", "stream_sid", s.meta.StreamSID)
	}
}

func (s *elevenLabsStream) emitFailSoft(turnID string) {
	s.fallbacks.Add(1)
	s.emit(TTSAudioChunk{TurnID: turnID, Final: true})
}

func (s *elevenLabsStream) tryReconnect(ctx context.Context) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	backoff := s.cfg.ReconnectBase
	for attempt := 0; attempt < 5; attempt++ {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		default:
		}
		if err := s.connect(ctx); err == nil {
			s.reconnects.Add(1)
			GlobalMetrics().IncTTSReconnect()
			s.logger.Info("elevenlabs reconnected", "stream_sid", s.meta.StreamSID, "reconnects", s.reconnects.Load())
			return
		}
		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		time.Sleep(backoff + jitter)
		if backoff < s.cfg.ReconnectMax {
			backoff *= 2
			if backoff > s.cfg.ReconnectMax {
				backoff = s.cfg.ReconnectMax
			}
		}
	}
}

func (s *elevenLabsStream) closeConnLocked() error {
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	s.initialized = false
	return err
}

var ErrTTSStreamClosed = fmt.Errorf("tts stream closed")

// SetTTSDialer replaces the WebSocket dialer (tests).
func (p *ElevenLabsTTSProvider) SetTTSDialer(d ttsDial) {
	p.dial = d
}
