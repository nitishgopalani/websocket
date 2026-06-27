package media

import (
	"context"
	"log/slog"
	"sync"
)

// AudioEgress receives synthesized audio for outbound playback (CT-10 implements Fonada WS).
type AudioEgress interface {
	SendAudio(ctx context.Context, session *Session, chunk TTSAudioChunk) error
	Mark(ctx context.Context, session *Session, turnID string) error
	ClearPlayback(ctx context.Context, session *Session) error
}

// LoggingEgress logs audio egress until CT-10 wires Fonada playback.
type LoggingEgress struct {
	logger *slog.Logger
}

// NewLoggingEgress returns an egress seam that logs chunk sizes and mark/clear events.
func NewLoggingEgress(logger *slog.Logger) *LoggingEgress {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingEgress{logger: logger}
}

func (e *LoggingEgress) SendAudio(_ context.Context, session *Session, chunk TTSAudioChunk) error {
	if e.logger != nil {
		e.logger.Info("egress audio",
			"stream_sid", session.StreamSID,
			"turn_id", chunk.TurnID,
			"seq", chunk.Seq,
			"bytes", len(chunk.MuLaw),
			"final", chunk.Final,
		)
	}
	return nil
}

func (e *LoggingEgress) Mark(_ context.Context, session *Session, turnID string) error {
	if e.logger != nil {
		e.logger.Info("egress mark", "stream_sid", session.StreamSID, "turn_id", turnID)
	}
	return nil
}

func (e *LoggingEgress) ClearPlayback(_ context.Context, session *Session) error {
	if e.logger != nil {
		e.logger.Info("egress clear", "stream_sid", session.StreamSID)
	}
	return nil
}

// SessionCloseHook is invoked when the brain signals end_call after playback mark.
type SessionCloseHook func(ctx context.Context, session *Session)

// TTSReplyConsumer implements ReplyConsumer: text chunks → TTS → AudioEgress.
type TTSReplyConsumer struct {
	tts         TTSStream
	egress      AudioEgress
	turnManager *TurnManager
	onEndCall   SessionCloseHook
	logger      *slog.Logger

	mu            sync.Mutex
	session       *Session
	pendingMark   map[string]bool
	endCallAfter  map[string]bool
	agentSpeaking bool
}

// NewTTSReplyConsumer constructs a reply consumer that streams text to TTS and routes audio to egress.
func NewTTSReplyConsumer(
	tts TTSStream,
	egress AudioEgress,
	turnManager *TurnManager,
	onEndCall SessionCloseHook,
	logger *slog.Logger,
) *TTSReplyConsumer {
	if egress == nil {
		egress = NewLoggingEgress(logger)
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &TTSReplyConsumer{
		tts:          tts,
		egress:       egress,
		turnManager:  turnManager,
		onEndCall:    onEndCall,
		logger:       logger,
		pendingMark:  make(map[string]bool),
		endCallAfter: make(map[string]bool),
	}
	go c.routeAudio()
	return c
}

// BindSession associates the consumer with the active telephony session (one per sink factory).
func (c *TTSReplyConsumer) BindSession(session *Session) {
	c.mu.Lock()
	c.session = session
	c.mu.Unlock()
}

// CancelPlayback stops TTS and clears egress for barge-in (CT-11 seam).
func (c *TTSReplyConsumer) CancelPlayback(ctx context.Context, turnID string) {
	if c.tts != nil {
		_ = c.tts.Cancel(turnID)
	}
	c.mu.Lock()
	session := c.session
	delete(c.pendingMark, turnID)
	delete(c.endCallAfter, turnID)
	c.agentSpeaking = false
	c.mu.Unlock()
	if session != nil && c.turnManager != nil {
		c.turnManager.SetAgentSpeaking(session, false)
	}
	if session != nil {
		_ = c.egress.ClearPlayback(ctx, session)
	}
}

func (c *TTSReplyConsumer) OnReplyChunk(ctx context.Context, session *Session, turnID string, seq int, text string) {
	c.BindSession(session)
	if text == "" {
		return
	}
	if c.tts == nil {
		return
	}
	if err := c.tts.Speak(turnID, text); err != nil && c.logger != nil {
		c.logger.Warn("tts speak failed", "turn_id", turnID, "error", err)
	}
}

func (c *TTSReplyConsumer) OnReplyDone(ctx context.Context, session *Session, turnID string, endCall bool, disposition string) {
	_ = disposition
	c.BindSession(session)
	c.mu.Lock()
	c.pendingMark[turnID] = true
	if endCall {
		c.endCallAfter[turnID] = true
	}
	c.mu.Unlock()

	if c.tts != nil {
		_ = c.tts.Speak(turnID, "")
	}
}

func (c *TTSReplyConsumer) OnReplyError(ctx context.Context, session *Session, turnID, fallbackText string) {
	if fallbackText == "" {
		return
	}
	c.OnReplyChunk(ctx, session, turnID, 0, fallbackText)
	c.OnReplyDone(ctx, session, turnID, false, "")
}

func (c *TTSReplyConsumer) routeAudio() {
	if c.tts == nil {
		return
	}
	for chunk := range c.tts.Audio() {
		c.mu.Lock()
		session := c.session
		c.mu.Unlock()
		if session == nil {
			continue
		}

		if len(chunk.MuLaw) > 0 {
			c.mu.Lock()
			if !c.agentSpeaking && c.turnManager != nil {
				c.turnManager.SetAgentSpeaking(session, true)
			}
			c.agentSpeaking = true
			c.mu.Unlock()
			if err := c.egress.SendAudio(context.Background(), session, chunk); err != nil && c.logger != nil {
				c.logger.Warn("egress send failed", "error", err)
			}
		}

		if !chunk.Final {
			continue
		}

		c.mu.Lock()
		mark := c.pendingMark[chunk.TurnID]
		endCall := c.endCallAfter[chunk.TurnID]
		delete(c.pendingMark, chunk.TurnID)
		c.mu.Unlock()

		if mark {
			if err := c.egress.Mark(context.Background(), session, chunk.TurnID); err != nil && c.logger != nil {
				c.logger.Warn("egress mark failed", "error", err)
			}
			if de, ok := c.egress.(DeferredPlaybackEgress); ok && de.DefersPlaybackComplete() {
				continue
			}
			c.mu.Lock()
			delete(c.endCallAfter, chunk.TurnID)
			c.agentSpeaking = false
			c.mu.Unlock()
			if c.turnManager != nil {
				c.turnManager.SetAgentSpeaking(session, false)
			}
			if endCall && c.onEndCall != nil {
				c.onEndCall(context.Background(), session)
			}
			continue
		}

		c.mu.Lock()
		delete(c.endCallAfter, chunk.TurnID)
		c.mu.Unlock()
	}
}

// OnPlaybackComplete is invoked when the carrier echoes a mark after playback reaches it.
func (c *TTSReplyConsumer) OnPlaybackComplete(ctx context.Context, session *Session, turnID string) {
	c.mu.Lock()
	endCall := c.endCallAfter[turnID]
	delete(c.endCallAfter, turnID)
	c.agentSpeaking = false
	c.mu.Unlock()
	if c.turnManager != nil {
		c.turnManager.SetAgentSpeaking(session, false)
	}
	if endCall && c.onEndCall != nil {
		c.onEndCall(ctx, session)
	}
}

// Close shuts down the TTS stream.
func (c *TTSReplyConsumer) Close() error {
	if c.tts != nil {
		return c.tts.Close()
	}
	return nil
}

var _ ReplyConsumer = (*TTSReplyConsumer)(nil)
var _ PlaybackListener = (*TTSReplyConsumer)(nil)
