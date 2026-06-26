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

type sarvamDial func(ctx context.Context, wsURL string, header http.Header) (*websocket.Conn, *http.Response, error)

// SarvamASRProvider opens persistent Sarvam streaming STT sessions.
type SarvamASRProvider struct {
	apiKey string
	cfg    SarvamConfig
	dial   sarvamDial
	logger *slog.Logger
}

// NewSarvamASRProvider constructs a Sarvam ASR provider.
func NewSarvamASRProvider(apiKey string, cfg SarvamConfig, logger *slog.Logger) *SarvamASRProvider {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultSarvamEndpoint
	}
	return &SarvamASRProvider{
		apiKey: apiKey,
		cfg:    cfg,
		dial:   defaultSarvamDial,
		logger: logger,
	}
}

func defaultSarvamDial(ctx context.Context, wsURL string, header http.Header) (*websocket.Conn, *http.Response, error) {
	d := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	return d.DialContext(ctx, wsURL, header)
}

func (p *SarvamASRProvider) Open(ctx context.Context, meta ASRSessionMeta) (ASRSession, error) {
	if meta.SampleRate == 0 {
		meta.SampleRate = defaultTargetSampleRate
	}
	s := &sarvamSession{
		provider: p,
		meta:     meta,
		cfg:      p.cfg,
		apiKey:   p.apiKey,
		dial:     p.dial,
		logger:   p.logger,
		events:   make(chan ASREvent, defaultASREventBuffer),
		done:     make(chan struct{}),
	}
	if err := s.connect(ctx); err != nil {
		return nil, err
	}
	s.wg.Add(1)
	go s.readLoop()
	if p.cfg.KeepalivePeriod > 0 {
		s.wg.Add(1)
		go s.keepaliveLoop()
	}
	return s, nil
}

type sarvamSession struct {
	provider *SarvamASRProvider
	meta     ASRSessionMeta
	cfg      SarvamConfig
	apiKey   string
	dial     sarvamDial
	logger   *slog.Logger

	mu       sync.Mutex
	conn     *websocket.Conn
	closed   bool
	closeErr error

	events chan ASREvent
	done   chan struct{}
	wg     sync.WaitGroup

	reconnects     atomic.Int64
	droppedOnRetry atomic.Int64
	sendFailures   atomic.Int64

	reconnectBuf [][]byte
}

func (s *sarvamSession) Reconnects() int64 {
	return s.reconnects.Load()
}

func (s *sarvamSession) DroppedDuringReconnect() int64 {
	return s.droppedOnRetry.Load()
}

func (s *sarvamSession) Events() <-chan ASREvent {
	return s.events
}

func (s *sarvamSession) SendAudio(pcm16 []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrASRSessionClosed
	}
	if len(pcm16) == 0 {
		return nil
	}
	if len(pcm16)%2 != 0 {
		return ErrInvalidPCM16Length
	}
	if s.conn == nil {
		return s.bufferWhileDisconnected(pcm16)
	}
	if err := s.writeAudioLocked(pcm16); err != nil {
		s.sendFailures.Add(1)
		s.logger.Warn("sarvam send audio failed; buffering for reconnect",
			"stream_sid", s.meta.StreamSID,
			"error", err,
		)
		_ = s.closeConnLocked()
		return s.bufferWhileDisconnected(pcm16)
	}
	return nil
}

func (s *sarvamSession) bufferWhileDisconnected(pcm16 []byte) error {
	frame := make([]byte, len(pcm16))
	copy(frame, pcm16)
	if len(s.reconnectBuf) >= defaultASRReconnectBuffer {
		s.droppedOnRetry.Add(1)
		s.reconnectBuf = s.reconnectBuf[1:]
	}
	s.reconnectBuf = append(s.reconnectBuf, frame)
	go s.tryReconnect(context.Background())
	return nil
}

func (s *sarvamSession) Close() error {
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	_ = s.closeConnLocked()
	s.mu.Unlock()

	close(s.done)
	s.wg.Wait()
	close(s.events)
	s.closeErr = nil
	return nil
}

func (s *sarvamSession) connect(ctx context.Context) error {
	wsURL, err := s.buildWSURL()
	if err != nil {
		return err
	}
	header := http.Header{}
	header.Set("api-subscription-key", s.apiKey)

	conn, _, err := s.dial(ctx, wsURL, header)
	if err != nil {
		return fmt.Errorf("sarvam dial: %w", err)
	}

	s.mu.Lock()
	s.conn = conn
	buf := append([][]byte(nil), s.reconnectBuf...)
	s.reconnectBuf = nil
	s.mu.Unlock()

	for _, frame := range buf {
		s.mu.Lock()
		err := s.writeAudioLocked(frame)
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *sarvamSession) buildWSURL() (string, error) {
	u, err := url.Parse(s.cfg.Endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("model", s.cfg.Model)
	q.Set("mode", s.cfg.Mode)
	if s.meta.Language != "" && s.meta.Language != "unknown" {
		q.Set("language_code", s.meta.Language)
	} else if s.cfg.Language != "" && s.cfg.Language != "unknown" {
		q.Set("language_code", s.cfg.Language)
	}
	q.Set("sample_rate", intString(s.meta.SampleRate))
	q.Set("input_audio_codec", "pcm_s16le")
	q.Set("vad_signals", boolString(s.cfg.VADSignals))
	q.Set("high_vad_sensitivity", boolString(s.cfg.HighVADSensitivity))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (s *sarvamSession) writeAudioLocked(pcm16 []byte) error {
	if s.conn == nil {
		return fmt.Errorf("sarvam connection not ready")
	}
	msg := sarvamAudioMessage{
		Audio:      base64.StdEncoding.EncodeToString(pcm16),
		SampleRate: s.meta.SampleRate,
		Encoding:   "pcm_s16le",
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return s.conn.WriteMessage(websocket.TextMessage, payload)
}

func (s *sarvamSession) closeConnLocked() error {
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}

func (s *sarvamSession) readLoop() {
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
			s.logger.Warn("sarvam read ended; scheduling reconnect",
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

		for _, evt := range parseSarvamMessages(data) {
			s.emit(evt)
		}
	}
}

func (s *sarvamSession) keepaliveLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.KeepalivePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.mu.Lock()
			conn := s.conn
			if conn != nil {
				err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
				if err != nil {
					_ = s.closeConnLocked()
					go s.tryReconnect(context.Background())
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *sarvamSession) tryReconnect(ctx context.Context) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if s.conn != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	attempt := 0
	for {
		select {
		case <-s.done:
			return
		default:
		}

		delay := backoffDelay(s.cfg.ReconnectBaseDelay, s.cfg.ReconnectMaxDelay, attempt)
		time.Sleep(delay)

		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return
		}

		if err := s.connect(ctx); err != nil {
			s.logger.Warn("sarvam reconnect failed",
				"stream_sid", s.meta.StreamSID,
				"attempt", attempt+1,
				"error", err,
			)
			s.emit(ASREvent{Type: ASREventError, Err: err})
			attempt++
			continue
		}

		s.reconnects.Add(1)
		s.logger.Info("sarvam reconnected",
			"stream_sid", s.meta.StreamSID,
			"reconnects", s.Reconnects(),
		)
		return
	}
}

func (s *sarvamSession) emit(evt ASREvent) {
	select {
	case <-s.done:
	case s.events <- evt:
	default:
		s.logger.Warn("asr event channel full; dropping event",
			"stream_sid", s.meta.StreamSID,
			"type", evt.Type,
		)
	}
}

func backoffDelay(base, max time.Duration, attempt int) time.Duration {
	if attempt <= 0 {
		return base
	}
	delay := base
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= max {
			return max + jitter(max/5)
		}
	}
	return delay + jitter(max/5)
}

func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(max)))
}

type sarvamAudioMessage struct {
	Audio      string `json:"audio"`
	SampleRate int    `json:"sample_rate"`
	Encoding   string `json:"encoding"`
}

type sarvamMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type sarvamDataMessage struct {
	Transcript string `json:"transcript"`
	IsFinal    *bool  `json:"is_final"`
	Final      *bool  `json:"final"`
}

type sarvamEventMessage struct {
	SignalType string `json:"signal_type"`
}

func parseSarvamMessages(data []byte) []ASREvent {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return nil
	}

	var msgType string
	if raw, ok := top["type"]; ok {
		_ = json.Unmarshal(raw, &msgType)
	}

	switch msgType {
	case "events":
		var payload sarvamEventMessage
		if raw, ok := top["data"]; ok {
			_ = json.Unmarshal(raw, &payload)
		}
		return []ASREvent{mapSarvamSignal(payload.SignalType)}
	case "speech_start", "START_SPEECH":
		return []ASREvent{{Type: ASREventSpeechStart}}
	case "speech_end", "END_SPEECH":
		return []ASREvent{{Type: ASREventSpeechEnd}}
	case "data", "transcript", "partial":
		var payload sarvamDataMessage
		if raw, ok := top["data"]; ok {
			_ = json.Unmarshal(raw, &payload)
		}
		if payload.Transcript == "" {
			_ = json.Unmarshal(data, &payload)
		}
		isFinal := msgType == "transcript"
		if payload.IsFinal != nil {
			isFinal = *payload.IsFinal
		}
		if payload.Final != nil {
			isFinal = *payload.Final
		}
		evtType := ASREventPartial
		if isFinal {
			evtType = ASREventFinal
		}
		if payload.Transcript == "" {
			return nil
		}
		return []ASREvent{{
			Type:       evtType,
			Transcript: Transcript{Text: payload.Transcript, IsFinal: isFinal},
		}}
	default:
		return nil
	}
}

func mapSarvamSignal(signal string) ASREvent {
	switch signal {
	case "START_SPEECH", "speech_start":
		return ASREvent{Type: ASREventSpeechStart}
	case "END_SPEECH", "speech_end":
		return ASREvent{Type: ASREventSpeechEnd}
	default:
		return ASREvent{Type: ASREventError, Err: fmt.Errorf("unknown sarvam signal: %s", signal)}
	}
}
