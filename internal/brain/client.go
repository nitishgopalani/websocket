package brain

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"websocket/internal/media"
)

// Config controls the EB-6 brain WebSocket client.
type Config struct {
	Enabled         bool
	URL             string
	TenantID        string
	BorrowerIDParam string
	AgentIDParam    string
	PackIDParam     string
	LocaleParam     string
}

// DefaultConfig returns disabled brain client settings.
func DefaultConfig() Config {
	return Config{
		Enabled:         false,
		BorrowerIDParam: "borrower_id",
		AgentIDParam:    "agent_id",
		PackIDParam:     "pack_id",
		LocaleParam:     "locale",
	}
}

func (c Config) withDefaults() Config {
	if c.BorrowerIDParam == "" {
		c.BorrowerIDParam = "borrower_id"
	}
	if c.AgentIDParam == "" {
		c.AgentIDParam = "agent_id"
	}
	if c.PackIDParam == "" {
		c.PackIDParam = "pack_id"
	}
	if c.LocaleParam == "" {
		c.LocaleParam = "locale"
	}
	return c
}

// ConfigFromEnv loads brain WebSocket settings.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()
	if v := os.Getenv("BRAIN_WS_ENABLED"); v == "1" || v == "true" || v == "TRUE" {
		cfg.Enabled = true
	}
	if v := os.Getenv("BRAIN_WS_URL"); v != "" {
		cfg.URL = v
	}
	if v := os.Getenv("BRAIN_TENANT_ID"); v != "" {
		cfg.TenantID = v
	}
	return cfg.withDefaults()
}

type dialFunc func(ctx context.Context, url string, header http.Header) (*websocket.Conn, *http.Response, error)

// Client is the Go-side EB-6 WebSocket client; implements media.TurnListener.
type Client struct {
	cfg    Config
	reply  media.ReplyConsumer
	dial   dialFunc
	logger *slog.Logger

	mu           sync.Mutex
	conn         *websocket.Conn
	sessionID    string
	sessionOpen  bool
	turnSeq      atomic.Uint64
	inflightTurn string
	turnManager  *media.TurnManager
	readCancel   context.CancelFunc
	timingHub    *media.TurnTimingHub
	watchdog     *media.DeadAirWatchdog
	engineMarked map[string]bool
}

// NewClient constructs a brain WebSocket client.
func NewClient(cfg Config, reply media.ReplyConsumer, turnManager *media.TurnManager, logger *slog.Logger) *Client {
	cfg = cfg.withDefaults()
	if reply == nil {
		reply = media.NewLoggingReplyConsumer(logger)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		cfg:          cfg,
		reply:        reply,
		dial:         websocket.DefaultDialer.DialContext,
		logger:       logger,
		turnManager:  turnManager,
		engineMarked: make(map[string]bool),
	}
}

// SetObservability attaches CT-12 timing and watchdog hooks.
func (c *Client) SetObservability(timing *media.TurnTimingHub, watchdog *media.DeadAirWatchdog) {
	c.timingHub = timing
	c.watchdog = watchdog
}

// Connect opens the persistent brain WebSocket for a telephony session.
func (c *Client) Connect(ctx context.Context, session *media.Session) error {
	if !c.cfg.Enabled {
		return nil
	}
	if c.cfg.URL == "" {
		return fmt.Errorf("brain ws enabled but BRAIN_WS_URL is empty")
	}

	conn, _, err := c.dial(ctx, c.cfg.URL, nil)
	if err != nil {
		return fmt.Errorf("brain ws dial: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.sessionID = session.StreamSID
	c.sessionOpen = true
	c.mu.Unlock()

	start := SessionStartPayload{
		Type:       TypeSessionStart,
		SessionID:  session.StreamSID,
		BorrowerID: sessionParam(session, c.cfg.BorrowerIDParam, "unknown"),
		AgentID:    sessionParam(session, c.cfg.AgentIDParam, "default"),
		PackID:     sessionParam(session, c.cfg.PackIDParam, ""),
		Locale:     sessionParam(session, c.cfg.LocaleParam, "hi-IN"),
	}
	if err := c.writeJSON(start); err != nil {
		_ = c.Close()
		return err
	}

	readCtx, cancel := context.WithCancel(context.Background())
	c.readCancel = cancel
	go c.readLoop(readCtx, session)

	return nil
}

// OnTurnEvent implements media.TurnListener (EB-6 outbound from Go).
func (c *Client) OnTurnEvent(ctx context.Context, session *media.Session, event media.TurnEvent) {
	if !c.cfg.Enabled {
		return
	}
	switch event.Kind {
	case media.TurnEndOfTurn:
		c.mu.Lock()
		open := c.sessionOpen && c.conn != nil
		c.mu.Unlock()
		if !open {
			return
		}
		turnID := c.nextTurnID()
		c.mu.Lock()
		c.inflightTurn = turnID
		c.mu.Unlock()
		if c.timingHub != nil {
			c.timingHub.BindEngineTurn(turnID, false)
		}
		if c.watchdog != nil {
			c.watchdog.ArmCallerTurn(session, turnID)
		}
		payload := TurnPayload{
			Type:       TypeTurn,
			SessionID:  session.StreamSID,
			TurnID:     turnID,
			Transcript: event.Transcript,
			FlowClass:  FlowClassToWire(event.FlowClass),
		}
		if err := c.writeJSON(payload); err != nil {
			c.logger.Warn("brain turn send failed", "error", err, "stream_sid", session.StreamSID)
		}
	case media.TurnInterrupt:
		c.sendCancel(session)
	case media.TurnSpeechStarted:
		// no-op on brain wire
	}
}

// SendOpenerTurn sends the empty-transcript opening turn after session_start.
func (c *Client) SendOpenerTurn(session *media.Session) error {
	if !c.cfg.Enabled {
		return nil
	}
	turnID := c.nextTurnID()
	c.mu.Lock()
	c.inflightTurn = turnID
	c.mu.Unlock()
	if c.timingHub != nil {
		c.timingHub.BindEngineTurn(turnID, true)
	}
	if c.watchdog != nil {
		c.watchdog.ArmOpener(session, turnID)
	}
	return c.writeJSON(TurnPayload{
		Type:       TypeTurn,
		SessionID:  session.StreamSID,
		TurnID:     turnID,
		Transcript: "",
		FlowClass:  "Default",
	})
}

func (c *Client) sendCancel(session *media.Session) {
	c.mu.Lock()
	turnID := c.inflightTurn
	c.mu.Unlock()
	if turnID == "" {
		return
	}
	_ = c.Cancel(turnID)
}

// Cancel sends a brain cancel for an in-flight turn (CT-8 / CT-11 barge-in commit).
func (c *Client) Cancel(turnID string) error {
	if !c.cfg.Enabled || turnID == "" {
		return nil
	}
	c.mu.Lock()
	inflight := c.inflightTurn
	sessionID := c.sessionID
	c.mu.Unlock()
	if inflight != turnID {
		return nil
	}
	if err := c.writeJSON(CancelPayload{
		Type:      TypeCancel,
		SessionID: sessionID,
		TurnID:    turnID,
	}); err != nil {
		c.logger.Warn("brain cancel send failed", "error", err, "stream_sid", sessionID, "turn_id", turnID)
		return err
	}
	return nil
}

func (c *Client) readLoop(ctx context.Context, session *media.Session) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn == nil {
			return
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			c.logger.Warn("brain ws read ended", "error", err, "stream_sid", session.StreamSID)
			return
		}
		c.dispatchInbound(ctx, session, data)
	}
}

func (c *Client) dispatchInbound(ctx context.Context, session *media.Session, data []byte) {
	typ, err := decodeInbound(data)
	if err != nil {
		c.logger.Warn("brain ws decode failed", "error", err)
		return
	}
	msg, err := unmarshalInbound(data, typ)
	if err != nil {
		c.logger.Warn("brain ws unmarshal failed", "error", err, "type", typ)
		return
	}

	switch m := msg.(type) {
	case ChunkMessage:
		c.markEngineFirstChunk(m.TurnID)
		c.reply.OnReplyChunk(ctx, session, m.TurnID, m.Seq, m.Text)
	case FlowClassMessage:
		class := ParseFlowClassHint(m.Next)
		if c.turnManager != nil {
			c.turnManager.SetFlowClass(session, class)
		}
	case DoneMessage:
		c.mu.Lock()
		if c.inflightTurn == m.TurnID {
			c.inflightTurn = ""
		}
		c.mu.Unlock()
		c.reply.OnReplyDone(ctx, session, m.TurnID, m.EndCall, m.Disposition)
	case ErrorMessage:
		c.mu.Lock()
		if c.inflightTurn == m.TurnID {
			c.inflightTurn = ""
		}
		c.mu.Unlock()
		c.reply.OnReplyError(ctx, session, m.TurnID, m.FallbackText)
	}
}

func (c *Client) markEngineFirstChunk(turnID string) {
	if c.timingHub == nil || turnID == "" {
		return
	}
	c.mu.Lock()
	if c.engineMarked[turnID] {
		c.mu.Unlock()
		return
	}
	c.engineMarked[turnID] = true
	c.mu.Unlock()
	c.timingHub.MarkTurn(turnID, media.StageEngineFirstChunk)
}

func (c *Client) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return fmt.Errorf("brain ws not connected")
	}
	return c.conn.WriteJSON(v)
}

func (c *Client) nextTurnID() string {
	n := c.turnSeq.Add(1)
	c.mu.Lock()
	sid := c.sessionID
	c.mu.Unlock()
	return fmt.Sprintf("%s-t%d", sid, n)
}

func sessionParam(session *media.Session, key, fallback string) string {
	if session == nil || session.Params == nil {
		return fallback
	}
	if v := session.Params[key]; v != "" {
		return v
	}
	return fallback
}

// Close sends session_end and closes the WebSocket.
func (c *Client) Close() error {
	if c.readCancel != nil {
		c.readCancel()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionOpen = false
	c.inflightTurn = ""
	if c.conn == nil {
		return nil
	}
	if c.sessionID != "" {
		_ = c.conn.WriteJSON(SessionEndPayload{Type: TypeSessionEnd, SessionID: c.sessionID})
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// SetDialer replaces the WebSocket dialer (tests).
func (c *Client) SetDialer(d dialFunc) {
	c.dial = d
}

var _ media.TurnListener = (*Client)(nil)
