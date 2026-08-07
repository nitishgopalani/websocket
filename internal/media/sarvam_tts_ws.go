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
// AsyncAPI nests fields under data; some SDKs also emit top-level message.
type sarvamWSErrorResponse struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Error   string `json:"error"`
	Data    struct {
		Message   string `json:"message"`
		Code      int    `json:"code"`
		RequestID string `json:"request_id"`
	} `json:"data"`
}

func (e sarvamWSErrorResponse) message() string {
	if m := strings.TrimSpace(e.Data.Message); m != "" {
		return m
	}
	if m := strings.TrimSpace(e.Message); m != "" {
		return m
	}
	return strings.TrimSpace(e.Error)
}

func (e sarvamWSErrorResponse) code() int { return e.Data.Code }

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

	wsPath           string
	wsFallbacks      atomic.Int64
	wsReconnects     atomic.Int64
	restFallbacks    atomic.Int64
	loggedErrorTypes map[string]bool // D1: one raw-frame log per distinct error text

	logger *slog.Logger
}

// sarvamSynthesisState tracks in-flight synthesis for a single turn.
type sarvamSynthesisState struct {
	turnID       string
	text         string // accumulated Speak text for REST fallback
	textSent     bool
	audioStarted bool
	finalSent    bool
	cancelled    bool
	fallingBack  bool
	doneCh       chan struct{}
}

// SarvamTTSStreamingEnabled returns true if WebSocket TTS is enabled.
// Default false — REST stays primary until WS is proven on UAT.
func SarvamTTSStreamingEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("SARVAM_TTS_STREAMING")))
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
		"target_language_code": cfg.language,
		"speaker":              cfg.speaker,
		// linear16 @ session rate → raw PCM16 (D1.5: mulaw/alaw/wav/mp3 also OK;
		// linear_pcm is INVALID and returns 422).
		"speech_sample_rate": cfg.sampleRate,
		"output_audio_codec": "linear16",
	}
	if cfg.model != "" {
		data["model"] = cfg.model
	}
	// bulbul:v3: preprocessing is always on — do not send enable_preprocessing.
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
// On Speak(newTurn) with text, prior in-flight turns are cancelled at source.
// Sarvam WS has no cancel/clear message — Cancel closes+reopens the socket.
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

	// Cancel any other in-flight turn before synthesizing the new one.
	s.cancelOtherTurns(turnID)

	s.mu.Lock()
	if state.text != "" {
		state.text = state.text + " " + text
	} else {
		state.text = text
	}
	s.mu.Unlock()

	cfg := s.resolveConfig(turnID)
	if err := s.ensureConnection(context.Background(), cfg); err != nil {
		return s.speakREST(turnID, text, cfg, "handshake: "+err.Error())
	}

	if err := s.sendText(turnID, text); err != nil {
		s.logger.Warn("sarvam ws send failed, attempting reconnect",
			"stream_sid", s.meta.StreamSID,
			"turn_id", turnID,
			"error", err,
		)
		if err := s.reconnectOnce(context.Background(), cfg); err != nil {
			return s.speakREST(turnID, text, cfg, "reconnect: "+err.Error())
		}
		if err := s.sendText(turnID, text); err != nil {
			return s.speakREST(turnID, text, cfg, "send: "+err.Error())
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
func (s *sarvamTTSWSStream) speakREST(turnID, text string, cfg sarvamWSConfig, reason string) error {
	s.wsFallbacks.Add(1)
	s.restFallbacks.Add(1)
	s.mu.Lock()
	s.wsPath = "rest"
	if st := s.inFlight[turnID]; st != nil {
		st.fallingBack = true
		if text == "" && st.text != "" {
			text = st.text
		}
	}
	s.mu.Unlock()

	s.logger.Warn("tts_ws_fallback",
		"stream_sid", s.meta.StreamSID,
		"turn_id", turnID,
		"reason", reason,
		"speaker", cfg.speaker,
		"model", cfg.model,
	)

	if strings.TrimSpace(text) == "" {
		s.emitFinal(turnID)
		return nil
	}

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
		s.mu.Lock()
		if st := s.inFlight[turnID]; st != nil {
			st.finalSent = true
			delete(s.inFlight, turnID)
		}
		s.mu.Unlock()
		s.emitFinal(turnID)
	}()
	return nil
}

// fallbackTurnOnWSError triggers per-turn REST when Sarvam sends type:error (F1).
func (s *sarvamTTSWSStream) fallbackTurnOnWSError(reason string) {
	s.mu.Lock()
	var turnID, text string
	for tid, st := range s.inFlight {
		if st.finalSent || st.fallingBack || st.cancelled {
			continue
		}
		turnID = tid
		text = st.text
		st.fallingBack = true
		break
	}
	s.mu.Unlock()
	if turnID == "" {
		// Config-time error before any Speak — nothing to synthesize yet.
		return
	}
	cfg := s.resolveConfig(turnID)
	_ = s.speakREST(turnID, text, cfg, reason)
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
			s.logger.Warn("sarvam ws error unmarshal failed",
				"stream_sid", s.meta.StreamSID,
				"raw", string(data),
				"error", err,
			)
			return
		}
		msg := s.handleErrorRaw(data, resp)
		s.fallbackTurnOnWSError(msg)
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

	ct := strings.ToLower(resp.Data.ContentType)
	pcm := raw
	switch {
	case strings.Contains(ct, "wav"):
		if decoded, _ := wavParsePCM16(raw); len(decoded) > 0 {
			pcm = decoded
		}
	case strings.Contains(ct, "mulaw") || strings.Contains(ct, "pcmu"):
		pcm = MuLawToPCM16(raw)
	case strings.Contains(ct, "mpeg") || strings.Contains(ct, "mp3"):
		s.logger.Warn("sarvam ws unexpected mp3 chunk; falling back to REST",
			"stream_sid", s.meta.StreamSID,
			"content_type", resp.Data.ContentType,
		)
		s.fallbackTurnOnWSError("unexpected mp3 content_type on ws")
		return
	default:
		// audio/pcm, audio/raw, linear16 — PCM16 LE passthrough
	}

	s.mu.Lock()
	var turnID string
	for tid, state := range s.inFlight {
		if !state.finalSent && state.textSent && !state.fallingBack {
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
	if st := s.inFlight[turnID]; st != nil && st.fallingBack {
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
		if state.fallingBack || state.finalSent || !state.textSent {
			continue
		}
		turnID = tid
		state.finalSent = true
		close(state.doneCh)
		delete(s.inFlight, tid)
		break
	}
	s.mu.Unlock()

	if turnID != "" {
		s.emitFinal(turnID)
	}
}

func (s *sarvamTTSWSStream) handleError(resp sarvamWSErrorResponse) {
	msg := resp.message()
	code := resp.code()
	s.logger.Warn("sarvam ws error",
		"stream_sid", s.meta.StreamSID,
		"error", msg,
		"code", code,
		"request_id", resp.Data.RequestID,
	)
}

// handleErrorRaw logs the complete raw error frame once per distinct message
// (D1 visibility) and returns the parsed message for fallback.
func (s *sarvamTTSWSStream) handleErrorRaw(raw []byte, resp sarvamWSErrorResponse) string {
	msg := resp.message()
	key := msg
	if key == "" {
		key = string(raw)
	}
	s.mu.Lock()
	if s.loggedErrorTypes == nil {
		s.loggedErrorTypes = make(map[string]bool)
	}
	already := s.loggedErrorTypes[key]
	if !already {
		s.loggedErrorTypes[key] = true
	}
	s.mu.Unlock()
	if !already {
		s.logger.Warn("sarvam ws error raw frame",
			"stream_sid", s.meta.StreamSID,
			"error", msg,
			"code", resp.code(),
			"raw", string(raw),
		)
	}
	s.handleError(resp)
	return msg
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
	hadInFlight := false
	if state, ok := s.inFlight[turnID]; ok {
		hadInFlight = true
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
	cfg := s.connConfig
	conn := s.conn
	closed := s.closed
	s.mu.Unlock()

	// Docs: no server-side cancel/clear — close+reopen stops generation.
	if !closed && hadInFlight && conn != nil {
		s.logger.Info("sarvam ws reopen on cancel (no protocol cancel)",
			"stream_sid", s.meta.StreamSID,
			"turn_id", turnID,
		)
		_ = s.connect(context.Background(), cfg)
	}
	return nil
}

// cancelOtherTurns locally cancels every in-flight turn except keepTurnID and
// reopens the WS once if any were cancelled (Sarvam has no in-band cancel).
func (s *sarvamTTSWSStream) cancelOtherTurns(keepTurnID string) {
	s.mu.Lock()
	var victims []string
	for tid, state := range s.inFlight {
		if tid == keepTurnID || state == nil || state.cancelled || state.finalSent {
			continue
		}
		victims = append(victims, tid)
		s.cancelled[tid] = struct{}{}
		state.cancelled = true
		if state.doneCh != nil {
			select {
			case <-state.doneCh:
			default:
				close(state.doneCh)
			}
		}
		delete(s.inFlight, tid)
	}
	cfg := s.connConfig
	conn := s.conn
	closed := s.closed
	s.mu.Unlock()
	if len(victims) == 0 || closed || conn == nil {
		return
	}
	s.logger.Info("sarvam ws reopen on cancel (no protocol cancel)",
		"stream_sid", s.meta.StreamSID,
		"cancelled_turns", victims,
		"keep_turn_id", keepTurnID,
	)
	_ = s.connect(context.Background(), cfg)
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
