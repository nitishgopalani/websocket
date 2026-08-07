package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultSarvamTTSWSURL        = "wss://api.sarvam.ai/text-to-speech/ws"
	sarvamWSHandshakeTimeout     = 10 * time.Second
	sarvamWSWriteTimeout         = 5 * time.Second
	sarvamWSReadTimeout          = 30 * time.Second
	sarvamWSPingInterval         = 15 * time.Second
	sarvamWSFirstAudioTargetMs   = 500
	sarvamWSReconnectMaxAttempts = 1
)

// sarvamWSMessage is a JSON message sent to Sarvam TTS WebSocket.
type sarvamWSMessage struct {
	Type string         `json:"type"`
	Data map[string]any `json:"data,omitempty"`
}

// sarvamWSAudioResponse is an audio chunk from Sarvam WS.
type sarvamWSAudioResponse struct {
	Type string `json:"type"`
	Data struct {
		Audio       string `json:"audio"`
		ContentType string `json:"content_type"`
	} `json:"data"`
}

// sarvamWSEventResponse is an event from Sarvam WS (e.g., final).
type sarvamWSEventResponse struct {
	Type string `json:"type"`
	Data struct {
		EventType string `json:"event_type"`
	} `json:"data"`
}

// sarvamWSErrorResponse is an error from Sarvam WS.
type sarvamWSErrorResponse struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

// sarvamWSConfig holds per-connection config for Sarvam TTS WS.
type sarvamWSConfig struct {
	speaker    string
	model      string
	pace       *float64
	sampleRate int
	language   string
}

func (c sarvamWSConfig) equal(other sarvamWSConfig) bool {
	if c.speaker != other.speaker || c.model != other.model || c.sampleRate != other.sampleRate || c.language != other.language {
		return false
	}
	if c.pace == nil && other.pace == nil {
		return true
	}
	if c.pace == nil || other.pace == nil {
		return false
	}
	return *c.pace == *other.pace
}

// sarvamTTSWSStream implements TTSStream using Sarvam's WebSocket API.
// Falls back to REST on WS failure after one reconnect attempt.
type sarvamTTSWSStream struct {
	provider   *SarvamTTSProvider
	meta       TTSSessionMeta
	sampleRate int
	wsURL      string
	apiKey     string
	dial       ttsDial

	mu            sync.Mutex
	conn          *websocket.Conn
	connConfig    sarvamWSConfig
	closed        bool
	inFlight      map[string]*sarvamSynthesisState
	cancelled     map[string]struct{}
	turnSeq       map[string]int
	turnVoice     map[string]sarvamTurnVoice
	lastWriteNano atomic.Int64

	audio chan TTSAudioChunk
	done  chan struct{}
	wg    sync.WaitGroup

	wsPath        string
	wsFallbacks   atomic.Int64
	wsReconnects  atomic.Int64
	restFallbacks atomic.Int64

	logger *slog.Logger
}

// sarvamSynthesisState tracks in-flight synthesis for a single turn.
type sarvamSynthesisState struct {
	turnID       string
	textSent     bool
	audioStarted bool
	finalSent    bool
	cancelled    bool
	doneCh       chan struct{}
}

// SarvamTTSStreamingEnabled returns true if WebSocket TTS is enabled.
func SarvamTTSStreamingEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("SARVAM_TTS_STREAMING")))
	if v == "" {
		return true // default enabled
	}
	return v == "1" || v == "true" || v == "yes"
}

// SarvamTTSDefaultPace returns the default pace from SARVAM_TTS_PACE env.
func SarvamTTSDefaultPace() *float64 {
	v := strings.TrimSpace(os.Getenv("SARVAM_TTS_PACE"))
	if v == "" {
		return nil
	}
	var pace float64
	if _, err := fmt.Sscanf(v, "%f", &pace); err == nil && pace > 0 {
		return &pace
	}
	return nil
}

// newSarvamTTSWSStream creates a WebSocket-backed TTS stream.
func newSarvamTTSWSStream(provider *SarvamTTSProvider, meta TTSSessionMeta, sampleRate int) *sarvamTTSWSStream {
	wsURL := defaultSarvamTTSWSURL
	if v := strings.TrimSpace(os.Getenv("SARVAM_TTS_WS_URL")); v != "" {
		wsURL = v
	} else if provider.baseURL != "" && provider.baseURL != defaultSarvamTTSBaseURL {
		base := strings.TrimRight(provider.baseURL, "/")
		base = strings.Replace(base, "https://", "wss://", 1)
		base = strings.Replace(base, "http://", "ws://", 1)
		wsURL = base + "/text-to-speech/ws"
	}
	s := &sarvamTTSWSStream{
		provider:   provider,
		meta:       meta,
		sampleRate: sampleRate,
		wsURL:      wsURL,
		apiKey:     provider.apiKey,
		dial:       defaultTTSDial,
		inFlight:   make(map[string]*sarvamSynthesisState),
		cancelled:  make(map[string]struct{}),
		turnSeq:    make(map[string]int),
		turnVoice:  make(map[string]sarvamTurnVoice),
		audio:      make(chan TTSAudioChunk, defaultTTSAudioBuffer),
		done:       make(chan struct{}),
		wsPath:     "ws",
		logger:     provider.logger,
	}
	return s
}

// Open dials the WebSocket and starts background loops.
func (s *sarvamTTSWSStream) Open(ctx context.Context) error {
	cfg := s.defaultConfig()
	if err := s.connect(ctx, cfg); err != nil {
		return err
	}
	s.wg.Add(2)
	go s.readLoop()
	go s.keepaliveLoop()
	return nil
}

func (s *sarvamTTSWSStream) defaultConfig() sarvamWSConfig {
	cfg := sarvamWSConfig{
		speaker:    s.provider.speaker,
		model:      s.provider.model,
		sampleRate: s.sampleRate,
		language:   s.provider.lang,
		pace:       SarvamTTSDefaultPace(),
	}
	return cfg
}

func (s *sarvamTTSWSStream) buildWSURL() string {
	u, err := url.Parse(s.wsURL)
	if err != nil {
		return s.wsURL
	}
	q := u.Query()
	q.Set("model", s.provider.model)
	q.Set("send_completion_event", "true")
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *sarvamTTSWSStream) connect(ctx context.Context, cfg sarvamWSConfig) error {
	s.mu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.mu.Unlock()

	header := http.Header{}
	header.Set("api-subscription-key", s.apiKey)

	wsURL := s.buildWSURL()
	conn, _, err := s.dial(ctx, wsURL, header)
	if err != nil {
		return fmt.Errorf("sarvam ws dial: %w", err)
	}

	s.mu.Lock()
	s.conn = conn
	s.connConfig = cfg
	s.mu.Unlock()

	if err := s.sendConfig(cfg); err != nil {
		s.mu.Lock()
		_ = conn.Close()
		s.conn = nil
		s.mu.Unlock()
		return fmt.Errorf("sarvam ws config: %w", err)
	}
	return nil
}

func (s *sarvamTTSWSStream) sendConfig(cfg sarvamWSConfig) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("no connection")
	}

	data := map[string]any{
		"target_language_code":  cfg.language,
		"speaker":               cfg.speaker,
		"speech_sample_rate":    cfg.sampleRate,
		"enable_preprocessing":  true,
		"output_audio_codec":    "linear_pcm",
	}
	if cfg.model != "" {
		data["model"] = cfg.model
	}
	if cfg.pace != nil {
		p := *cfg.pace
		if isSarvamBulbulV3(cfg.model) {
			p = clampSarvamV3Pace(p)
		}
		data["pace"] = p
	}

	msg := sarvamWSMessage{Type: "config", Data: data}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(sarvamWSWriteTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return err
	}
	s.lastWriteNano.Store(time.Now().UnixNano())
	return nil
}

func (s *sarvamTTSWSStream) SetTurnVoice(turnID, voiceID, model string, pace *float64) {
	if turnID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.turnVoice[turnID]
	if v := strings.TrimSpace(voiceID); v != "" {
		cur.speaker = v
	}
	if m := strings.TrimSpace(model); m != "" {
		cur.model = m
	}
	if pace != nil {
		p := *pace
		cur.pace = &p
	}
	s.turnVoice[turnID] = cur
}

func (s *sarvamTTSWSStream) resolveConfig(turnID string) sarvamWSConfig {
	cfg := s.defaultConfig()
	s.mu.Lock()
	defer s.mu.Unlock()
	if ov, ok := s.turnVoice[turnID]; ok {
		if ov.speaker != "" {
			cfg.speaker = ov.speaker
		}
		if ov.model != "" {
			cfg.model = ov.model
		}
		if ov.pace != nil {
			p := *ov.pace
			cfg.pace = &p
		}
	}
	return cfg
}

// Speak sends text to the TTS WebSocket or falls back to REST.
func (s *sarvamTTSWSStream) Speak(turnID string, text string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrTTSStreamClosed
	}
	if _, cancelled := s.cancelled[turnID]; cancelled && text != "" {
		s.mu.Unlock()
		return nil
	}

	state := s.inFlight[turnID]
	if state == nil {
		state = &sarvamSynthesisState{
			turnID: turnID,
			doneCh: make(chan struct{}),
		}
		s.inFlight[turnID] = state
	}
	s.mu.Unlock()

	text = strings.TrimSpace(text)
	if text == "" {
		return s.flushAndWait(turnID, state)
	}

	cfg := s.resolveConfig(turnID)
	if err := s.ensureConnection(context.Background(), cfg); err != nil {
		s.logger.Warn("sarvam ws connection failed, falling back to REST",
			"stream_sid", s.meta.StreamSID,
			"turn_id", turnID,
			"error", err,
		)
		return s.speakREST(turnID, text, cfg)
	}

	if err := s.sendText(turnID, text); err != nil {
		s.logger.Warn("sarvam ws send failed, attempting reconnect",
			"stream_sid", s.meta.StreamSID,
			"turn_id", turnID,
			"error", err,
		)
		if err := s.reconnectOnce(context.Background(), cfg); err != nil {
			s.logger.Warn("sarvam ws reconnect failed, falling back to REST",
				"stream_sid", s.meta.StreamSID,
				"turn_id", turnID,
				"error", err,
			)
			return s.speakREST(turnID, text, cfg)
		}
		if err := s.sendText(turnID, text); err != nil {
			return s.speakREST(turnID, text, cfg)
		}
	}

	s.mu.Lock()
	state.textSent = true
	s.mu.Unlock()
	return nil
}

func (s *sarvamTTSWSStream) flushAndWait(turnID string, state *sarvamSynthesisState) error {
	s.mu.Lock()
	if state.finalSent || state.cancelled {
		s.mu.Unlock()
		return nil
	}
	if !state.textSent {
		state.finalSent = true
		s.mu.Unlock()
		s.emitFinal(turnID)
		return nil
	}
	conn := s.conn
	s.mu.Unlock()

	if conn != nil {
		if err := s.sendFlush(turnID); err != nil {
			s.logger.Debug("sarvam ws flush failed", "turn_id", turnID, "error", err)
		}
	}
	return nil
}

func (s *sarvamTTSWSStream) ensureConnection(ctx context.Context, cfg sarvamWSConfig) error {
	s.mu.Lock()
	conn := s.conn
	currentCfg := s.connConfig
	s.mu.Unlock()

	if conn == nil {
		return s.connect(ctx, cfg)
	}

	if !cfg.equal(currentCfg) {
		s.logger.Info("sarvam ws voice changed, reopening connection",
			"stream_sid", s.meta.StreamSID,
			"old_speaker", currentCfg.speaker,
			"new_speaker", cfg.speaker,
			"old_model", currentCfg.model,
			"new_model", cfg.model,
		)
		return s.connect(ctx, cfg)
	}
	return nil
}

func (s *sarvamTTSWSStream) reconnectOnce(ctx context.Context, cfg sarvamWSConfig) error {
	s.wsReconnects.Add(1)
	GlobalMetrics().IncTTSReconnect()
	return s.connect(ctx, cfg)
}

func (s *sarvamTTSWSStream) sendText(turnID, text string) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("no connection")
	}

	msg := sarvamWSMessage{
		Type: "text",
		Data: map[string]any{"text": text},
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(sarvamWSWriteTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return err
	}
	s.lastWriteNano.Store(time.Now().UnixNano())
	return nil
}

func (s *sarvamTTSWSStream) sendFlush(turnID string) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("no connection")
	}

	msg := sarvamWSMessage{Type: "flush"}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(sarvamWSWriteTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return err
	}
	s.lastWriteNano.Store(time.Now().UnixNano())
	return nil
}

func (s *sarvamTTSWSStream) sendPing() error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return nil
	}

	_ = conn.SetWriteDeadline(time.Now().Add(sarvamWSWriteTimeout))
	return conn.WriteMessage(websocket.PingMessage, nil)
}

// speakREST falls back to REST synthesis, marking the fallback path.
func (s *sarvamTTSWSStream) speakREST(turnID, text string, cfg sarvamWSConfig) error {
	s.wsFallbacks.Add(1)
	s.restFallbacks.Add(1)
	s.mu.Lock()
	s.wsPath = "rest"
	s.mu.Unlock()

	s.logger.Warn("tts_ws_fallback",
		"stream_sid", s.meta.StreamSID,
		"turn_id", turnID,
		"speaker", cfg.speaker,
		"model", cfg.model,
	)

	restStream := &sarvamTTSStream{
		provider:   s.provider,
		meta:       s.meta,
		sampleRate: s.sampleRate,
		audio:      s.audio,
		done:       s.done,
		cancelled:  s.cancelled,
		turnSeq:    s.turnSeq,
		turnVoice:  s.turnVoice,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		restStream.synthesize(turnID, text, cfg.speaker, cfg.model, cfg.pace)
	}()
	return nil
}

func (s *sarvamTTSWSStream) readLoop() {
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

		_ = conn.SetReadDeadline(time.Now().Add(sarvamWSReadTimeout))
		_, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
			}
			s.logger.Debug("sarvam ws read error",
				"stream_sid", s.meta.StreamSID,
				"error", err,
			)
			s.mu.Lock()
			if s.conn == conn {
				_ = conn.Close()
				s.conn = nil
			}
			s.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.handleInbound(data)
	}
}

func (s *sarvamTTSWSStream) handleInbound(data []byte) {
	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return
	}

	switch base.Type {
	case "audio":
		var resp sarvamWSAudioResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			return
		}
		s.handleAudio(resp)

	case "event":
		var resp sarvamWSEventResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			return
		}
		if resp.Data.EventType == "final" {
			s.handleFinal()
		}

	case "error":
		var resp sarvamWSErrorResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			return
		}
		s.handleError(resp)
	}
}

func (s *sarvamTTSWSStream) handleAudio(resp sarvamWSAudioResponse) {
	if resp.Data.Audio == "" {
		return
	}

	raw, err := base64.StdEncoding.DecodeString(resp.Data.Audio)
	if err != nil {
		return
	}

	pcm := raw
	if strings.Contains(strings.ToLower(resp.Data.ContentType), "wav") {
		pcm, _ = wavParsePCM16(raw)
	}

	s.mu.Lock()
	var turnID string
	for tid, state := range s.inFlight {
		if !state.finalSent && state.textSent {
			turnID = tid
			state.audioStarted = true
			break
		}
	}
	if turnID == "" {
		s.mu.Unlock()
		return
	}
	if _, cancelled := s.cancelled[turnID]; cancelled {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	s.emitPCM(turnID, pcm)
}

func (s *sarvamTTSWSStream) handleFinal() {
	s.mu.Lock()
	var turnID string
	for tid, state := range s.inFlight {
		if !state.finalSent && state.textSent {
			turnID = tid
			state.finalSent = true
			close(state.doneCh)
			delete(s.inFlight, tid)
			break
		}
	}
	s.mu.Unlock()

	if turnID != "" {
		s.emitFinal(turnID)
	}
}

func (s *sarvamTTSWSStream) handleError(resp sarvamWSErrorResponse) {
	msg := resp.Message
	if msg == "" {
		msg = resp.Error
	}
	s.logger.Warn("sarvam ws error",
		"stream_sid", s.meta.StreamSID,
		"error", msg,
	)
}

func (s *sarvamTTSWSStream) keepaliveLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(sarvamWSPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			last := s.lastWriteNano.Load()
			if last == 0 || time.Since(time.Unix(0, last)) >= sarvamWSPingInterval {
				_ = s.sendPing()
			}
		}
	}
}

func (s *sarvamTTSWSStream) emitPCM(turnID string, pcm []byte) bool {
	for off := 0; off < len(pcm); off += sarvamTTSEmitChunkBytes {
		if s.isCancelled(turnID) {
			return false
		}
		end := off + sarvamTTSEmitChunkBytes
		if end > len(pcm) {
			end = len(pcm)
		}
		frame := make([]byte, end-off)
		copy(frame, pcm[off:end])

		s.mu.Lock()
		s.turnSeq[turnID]++
		seq := s.turnSeq[turnID]
		s.mu.Unlock()

		select {
		case <-s.done:
			return false
		case s.audio <- TTSAudioChunk{TurnID: turnID, Seq: seq, MuLaw: frame, Final: false}:
		}
	}
	return true
}

func (s *sarvamTTSWSStream) emitFinal(turnID string) {
	s.mu.Lock()
	s.turnSeq[turnID]++
	seq := s.turnSeq[turnID]
	s.mu.Unlock()
	select {
	case <-s.done:
	case s.audio <- TTSAudioChunk{TurnID: turnID, Seq: seq, Final: true}:
	}
}

func (s *sarvamTTSWSStream) isCancelled(turnID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return true
	}
	_, c := s.cancelled[turnID]
	return c
}

func (s *sarvamTTSWSStream) Cancel(turnID string) error {
	s.mu.Lock()
	s.cancelled[turnID] = struct{}{}
	if state, ok := s.inFlight[turnID]; ok {
		state.cancelled = true
		if state.doneCh != nil {
			select {
			case <-state.doneCh:
			default:
				close(state.doneCh)
			}
		}
		delete(s.inFlight, turnID)
	}
	s.mu.Unlock()
	return nil
}

func (s *sarvamTTSWSStream) Audio() <-chan TTSAudioChunk { return s.audio }

func (s *sarvamTTSWSStream) Close() error {
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
		_ = conn.Close()
	}
	s.wg.Wait()
	close(s.audio)
	return nil
}

func (s *sarvamTTSWSStream) Fallbacks() int64  { return s.wsFallbacks.Load() }
func (s *sarvamTTSWSStream) Reconnects() int64 { return s.wsReconnects.Load() }
func (s *sarvamTTSWSStream) Path() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wsPath
}

// SetDialer replaces the WebSocket dialer (for tests).
func (s *sarvamTTSWSStream) SetDialer(d ttsDial) {
	s.dial = d
}
