package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
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
	// Lazy dial on first SendAudio — avoids idle close before ingress audio arrives.
	s.wg.Add(1)
	go s.readLoop()
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

	reconnects      atomic.Int64
	droppedOnRetry  atomic.Int64
	sendFailures    atomic.Int64
	dialCount       atomic.Int64
	reconnectFails  atomic.Int64
	reconnectGiveUp atomic.Bool

	reconnectMu  sync.Mutex
	reconnecting bool

	keepaliveOnce sync.Once

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
		if err := s.connectLocked(context.Background()); err != nil {
			return s.bufferWhileDisconnectedLocked(pcm16, err)
		}
	}
	if err := s.writeAudioLocked(pcm16); err != nil {
		s.sendFailures.Add(1)
		s.logger.Warn("sarvam send audio failed; buffering for reconnect",
			"stream_sid", s.meta.StreamSID,
			"error", err,
		)
		_ = s.closeConnLocked()
		s.scheduleReconnect(context.Background())
		return s.bufferWhileDisconnectedLocked(pcm16, nil)
	}
	return nil
}

func (s *sarvamSession) bufferWhileDisconnected(pcm16 []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bufferWhileDisconnectedLocked(pcm16, nil)
}

func (s *sarvamSession) bufferWhileDisconnectedLocked(pcm16 []byte, dialErr error) error {
	frame := make([]byte, len(pcm16))
	copy(frame, pcm16)
	if len(s.reconnectBuf) >= defaultASRReconnectBuffer {
		s.droppedOnRetry.Add(1)
		s.reconnectBuf = s.reconnectBuf[1:]
	}
	s.reconnectBuf = append(s.reconnectBuf, frame)
	s.scheduleReconnect(context.Background())
	if dialErr != nil {
		return dialErr
	}
	return nil
}

func (s *sarvamSession) maxReconnects() int {
	if s.cfg.MaxReconnects > 0 {
		return s.cfg.MaxReconnects
	}
	return defaultASRMaxReconnects
}

func (s *sarvamSession) scheduleReconnect(ctx context.Context) {
	if s.reconnectGiveUp.Load() {
		return
	}
	s.reconnectMu.Lock()
	if s.reconnecting {
		s.reconnectMu.Unlock()
		return
	}
	s.reconnecting = true
	s.reconnectMu.Unlock()

	go func() {
		defer func() {
			s.reconnectMu.Lock()
			s.reconnecting = false
			s.reconnectMu.Unlock()
		}()
		s.tryReconnect(ctx)
	}()
}

func (s *sarvamSession) Close() error {
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	conn := s.conn
	s.mu.Unlock()

	if conn != nil {
		_ = s.sendFlush(conn)
	}
	s.mu.Lock()
	_ = s.closeConnLocked()
	s.mu.Unlock()

	close(s.done)
	s.wg.Wait()
	close(s.events)
	s.closeErr = nil
	return nil
}

func (s *sarvamSession) connectLocked(ctx context.Context) error {
	if s.reconnectGiveUp.Load() {
		return fmt.Errorf("sarvam reconnect exhausted")
	}
	maxDials := int64(1 + s.maxReconnects())
	if s.dialCount.Load() >= maxDials {
		s.reconnectGiveUp.Store(true)
		return fmt.Errorf("sarvam max dials (%d) reached for session", maxDials)
	}
	dialN := s.dialCount.Add(1)

	wsURL, queryLog, lang, err := s.buildWSURL()
	if err != nil {
		return err
	}
	s.logger.Info("sarvam ws dial",
		"stream_sid", s.meta.StreamSID,
		"dial", dialN,
		"language_code", lang,
		"url", maskSarvamWSURL(wsURL),
		"query", queryLog,
	)

	header := http.Header{}
	header.Set("api-subscription-key", s.apiKey)

	conn, _, err := s.dial(ctx, wsURL, header)
	if err != nil {
		return fmt.Errorf("sarvam dial: %w", err)
	}

	s.conn = conn
	buf := append([][]byte(nil), s.reconnectBuf...)
	s.reconnectBuf = nil

	s.logger.Info("sarvam ws connected",
		"stream_sid", s.meta.StreamSID,
		"dial", dialN,
		"order", "connect->send_audio (no separate config frame; params in query string)",
	)

	for _, frame := range buf {
		if err := s.writeAudioLocked(frame); err != nil {
			return err
		}
	}

	s.keepaliveOnce.Do(func() {
		if s.cfg.KeepalivePeriod > 0 {
			s.wg.Add(1)
			go s.keepaliveLoop()
		}
	})
	return nil
}

func (s *sarvamSession) buildWSURL() (raw string, queryLog string, languageCode string, err error) {
	u, err := url.Parse(s.cfg.Endpoint)
	if err != nil {
		return "", "", "", err
	}
	q := u.Query()
	q.Set("model", s.cfg.Model)
	q.Set("mode", s.cfg.Mode)
	lang := NormalizeSarvamLanguage(s.meta.Language, s.logger, s.meta.StreamSID)
	if lang == "" || lang == "unknown" {
		lang = NormalizeSarvamLanguage(s.cfg.Language, s.logger, s.meta.StreamSID)
	}
	if lang != "" && lang != "unknown" {
		// AsyncAPI + SDK use hyphenated query key language-code (not language_code).
		q.Set("language-code", lang)
	}
	q.Set("sample_rate", intString(s.meta.SampleRate))
	q.Set("input_audio_codec", "pcm_s16le")
	q.Set("vad_signals", boolString(s.cfg.VADSignals))
	q.Set("high_vad_sensitivity", boolString(s.cfg.HighVADSensitivity))
	u.RawQuery = q.Encode()

	parts := make([]string, 0, len(q))
	for k, vals := range q {
		for _, v := range vals {
			parts = append(parts, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return u.String(), strings.Join(parts, "&"), lang, nil
}

func (s *sarvamSession) writeAudioLocked(pcm16 []byte) error {
	if s.conn == nil {
		return fmt.Errorf("sarvam connection not ready")
	}
	msg := sarvamAudioMessage{
		Audio: sarvamAudioPayload{
			Data:       base64.StdEncoding.EncodeToString(pcm16),
			SampleRate: intString(s.meta.SampleRate),
			Encoding:   sarvamMessageEncoding,
		},
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	s.logger.Info("sarvam ws send",
		"stream_sid", s.meta.StreamSID,
		"kind", "audio",
		"pcm_bytes", len(pcm16),
		"frame", truncateAudioFrameLog(payload),
	)
	return s.conn.WriteMessage(websocket.TextMessage, payload)
}

func (s *sarvamSession) sendFlush(conn *websocket.Conn) error {
	payload, err := json.Marshal(sarvamFlushMessage{Type: "flush"})
	if err != nil {
		return err
	}
	s.logger.Info("sarvam ws send",
		"stream_sid", s.meta.StreamSID,
		"kind", "flush",
		"frame", string(payload),
	)
	return conn.WriteMessage(websocket.TextMessage, payload)
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
			closeCode, closeReason := extractCloseInfo(err)
			normalClose := websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway)
			if normalClose {
				s.logger.Info("sarvam ws closed",
					"stream_sid", s.meta.StreamSID,
					"close_code", closeCode,
					"close_reason", closeReason,
				)
			} else {
				s.logger.Warn("sarvam read ended; scheduling reconnect",
					"stream_sid", s.meta.StreamSID,
					"close_code", closeCode,
					"close_reason", closeReason,
					"error", err,
				)
			}
			s.mu.Lock()
			_ = s.closeConnLocked()
			hasBuf := len(s.reconnectBuf) > 0
			s.mu.Unlock()
			if !normalClose || hasBuf {
				s.scheduleReconnect(context.Background())
			}
			select {
			case <-s.done:
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		s.logger.Info("sarvam ws recv",
			"stream_sid", s.meta.StreamSID,
			"at", time.Now().Format(time.RFC3339Nano),
			"payload", truncateForLog(string(data), 512),
		)
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
					s.scheduleReconnect(context.Background())
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

	maxAttempts := s.maxReconnects()
	for {
		select {
		case <-s.done:
			return
		default:
		}

		if s.reconnectGiveUp.Load() {
			return
		}

		failN := int(s.reconnectFails.Load())
		if failN >= maxAttempts {
			s.reconnectGiveUp.Store(true)
			s.logger.Error("sarvam reconnect exhausted; giving up",
				"stream_sid", s.meta.StreamSID,
				"attempts", failN,
				"dials", s.dialCount.Load(),
			)
			s.emit(ASREvent{
				Type: ASREventError,
				Err:  fmt.Errorf("sarvam reconnect exhausted after %d attempts", failN),
			})
			return
		}

		delay := backoffDelay(s.cfg.ReconnectBaseDelay, s.cfg.ReconnectMaxDelay, failN)
		time.Sleep(delay)

		s.mu.Lock()
		closed := s.closed
		if !closed {
			err := s.connectLocked(ctx)
			if err != nil {
				s.reconnectFails.Add(1)
				s.mu.Unlock()
				s.logger.Warn("sarvam reconnect failed",
					"stream_sid", s.meta.StreamSID,
					"attempt", failN+1,
					"dials", s.dialCount.Load(),
					"error", err,
				)
				if s.reconnectGiveUp.Load() {
					s.emit(ASREvent{Type: ASREventError, Err: err})
					return
				}
				continue
			}
			s.reconnects.Add(1)
			GlobalMetrics().IncASRReconnect()
			s.logger.Info("sarvam reconnected",
				"stream_sid", s.meta.StreamSID,
				"reconnects", s.Reconnects(),
				"dials", s.dialCount.Load(),
			)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
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

const sarvamMessageEncoding = "audio/wav"

type sarvamAudioMessage struct {
	Audio sarvamAudioPayload `json:"audio"`
}

type sarvamAudioPayload struct {
	Data       string `json:"data"`
	SampleRate string `json:"sample_rate"`
	Encoding   string `json:"encoding"`
}

type sarvamFlushMessage struct {
	Type string `json:"type"`
}

type sarvamDataMessage struct {
	Transcript string `json:"transcript"`
	IsFinal    *bool  `json:"is_final"`
	Final      *bool  `json:"final"`
}

type sarvamErrorData struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Message string `json:"message"`
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
	case "error":
		var payload sarvamErrorData
		if raw, ok := top["data"]; ok {
			_ = json.Unmarshal(raw, &payload)
		}
		msg := payload.Error
		if msg == "" {
			msg = payload.Message
		}
		if msg == "" {
			msg = string(data)
		}
		return []ASREvent{{Type: ASREventError, Err: fmt.Errorf("sarvam: %s", msg)}}
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
		} else if payload.Final != nil {
			isFinal = *payload.Final
		} else if msgType == "data" {
			// Saaras v3 streaming sends utterance transcripts on type "data" without is_final.
			isFinal = true
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

func maskSarvamWSURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Scheme + "://" + u.Host + u.Path + "?" + u.Query().Encode()
}

func extractCloseInfo(err error) (code int, reason string) {
	if err == nil {
		return 0, ""
	}
	var ce *websocket.CloseError
	if errors.As(err, &ce) && ce != nil {
		return ce.Code, ce.Text
	}
	return -1, err.Error()
}

func truncateAudioFrameLog(raw []byte) string {
	var msg sarvamAudioMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return truncateForLog(string(raw), 256)
	}
	if len(msg.Audio.Data) > 48 {
		msg.Audio.Data = msg.Audio.Data[:48] + "...(truncated)"
	}
	out, err := json.Marshal(msg)
	if err != nil {
		return truncateForLog(string(raw), 256)
	}
	return string(out)
}

func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
