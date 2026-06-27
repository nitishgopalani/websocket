package brain

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"websocket/internal/media"
)

// CallControlConfig configures AMD-gated opener and voicemail handling (CT-14).
type CallControlConfig struct {
	AMDEnabled bool
	Voicemail  media.VoicemailConfig
	Logger     *slog.Logger
}

// CallControl implements media.AMDOutcomeListener for pilot call control.
type CallControl struct {
	cfg CallControlConfig

	mu              sync.Mutex
	brain           *Client
	tts             *media.TTSReplyConsumer
	egress          *media.CarrierEgress
	closer          media.SessionCloser
	ttsPlayback     media.PlaybackListener
	brainConnected  bool
	voicemailTurnID string
	openerCount     atomic.Int32
}

// NewCallControl constructs a per-session AMD outcome handler.
func NewCallControl(cfg CallControlConfig) *CallControl {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &CallControl{cfg: cfg}
}

// Bind attaches session-scoped dependencies after sink construction.
func (c *CallControl) Bind(brain *Client, tts *media.TTSReplyConsumer, egress *media.CarrierEgress, closer media.SessionCloser) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.brain = brain
	c.tts = tts
	c.egress = egress
	c.closer = closer
	if tts != nil {
		c.ttsPlayback = tts
	}
}

// OpenerCount returns how many opener turns were dispatched (tests).
func (c *CallControl) OpenerCount() int {
	return int(c.openerCount.Load())
}

func (c *CallControl) markBrainConnected() {
	c.mu.Lock()
	c.brainConnected = true
	c.mu.Unlock()
}

func (c *CallControl) recordOpener() {
	c.openerCount.Add(1)
}

func (c *CallControl) OnHuman(ctx context.Context, session *media.Session) {
	c.mu.Lock()
	brain := c.brain
	tts := c.tts
	egress := c.egress
	amdEnabled := c.cfg.AMDEnabled
	c.mu.Unlock()

	if egress != nil && amdEnabled {
		egress.ConfirmHuman()
	}

	if brain != nil {
		c.mu.Lock()
		connected := c.brainConnected
		c.mu.Unlock()
		if amdEnabled && !connected {
			if err := brain.Connect(ctx, session); err != nil {
				c.cfg.Logger.Warn("brain connect on amd human failed", "error", err, "stream_sid", session.StreamSID)
				return
			}
			c.mu.Lock()
			c.brainConnected = true
			c.mu.Unlock()
		}
		if err := brain.SendOpenerTurn(session); err != nil {
			c.cfg.Logger.Warn("opener turn failed", "error", err, "stream_sid", session.StreamSID)
			return
		}
		c.openerCount.Add(1)
	}

	_ = tts
	c.cfg.Logger.Info("amd human confirmed; conversation opened",
		"stream_sid", session.StreamSID,
		"call_sid", session.CallSID,
	)
}

func (c *CallControl) OnMachine(ctx context.Context, session *media.Session, decision media.AMDDecision) {
	c.cfg.Logger.Info("amd machine; voicemail branch",
		"stream_sid", session.StreamSID,
		"proba_human", decision.ProbaHuman,
		"reason", decision.Reason,
		"action", c.cfg.Voicemail.Action,
	)
	if c.cfg.Voicemail.Action == media.VoicemailActionLeaveMessage {
		c.leaveVoicemailMessage(ctx, session)
		return
	}
	c.hangup(ctx, session)
}

func (c *CallControl) leaveVoicemailMessage(ctx context.Context, session *media.Session) {
	c.mu.Lock()
	tts := c.tts
	egress := c.egress
	msg := c.cfg.Voicemail.Message
	c.mu.Unlock()

	if msg == "" || tts == nil {
		c.hangup(ctx, session)
		return
	}
	if egress != nil {
		egress.ConfirmHuman()
	}
	turnID := "voicemail:" + session.StreamSID
	c.mu.Lock()
	c.voicemailTurnID = turnID
	c.mu.Unlock()
	session.SetPlaybackListener(c)
	tts.SpeakHoldingLine(ctx, session, turnID, msg)
}

func (c *CallControl) hangup(ctx context.Context, session *media.Session) {
	c.mu.Lock()
	closer := c.closer
	c.mu.Unlock()
	if closer != nil {
		closer.CloseSession(ctx, session.StreamSID)
	}
}

// OnPlaybackComplete implements media.PlaybackListener for voicemail hangup.
func (c *CallControl) OnPlaybackComplete(ctx context.Context, session *media.Session, turnID string) {
	c.mu.Lock()
	vmTurn := c.voicemailTurnID
	inner := c.ttsPlayback
	c.mu.Unlock()

	if vmTurn != "" && turnID == vmTurn {
		c.mu.Lock()
		c.voicemailTurnID = ""
		c.mu.Unlock()
		if inner != nil {
			session.SetPlaybackListener(inner)
		}
		c.hangup(ctx, session)
		return
	}
	if inner != nil {
		inner.OnPlaybackComplete(ctx, session, turnID)
	}
}

var _ media.AMDOutcomeListener = (*CallControl)(nil)
var _ media.PlaybackListener = (*CallControl)(nil)
