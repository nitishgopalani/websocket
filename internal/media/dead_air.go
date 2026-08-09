package media

import (
	"context"
	"log/slog"
)

// DeadAirHandler implements ASRDeadListener (W1-B.1, H2 dead-air defense).
//
// When ASR reconnect is exhausted the session is permanently deaf. The handler
// delegates to the TTS reply consumer's SpeakApologyAndClose, which speaks the
// tenant apology line (configured via SetApologyLine from the session_start
// tenant profile) in the unknown_info-register voice and clean-closes the
// session. Never continues deaf.
//
// One handler per sink-factory invocation (per session). The apology text +
// voice are set on the TTS consumer by the brain-side session_start wiring
// (tenant profile.apology_dead_air + profile.voice_id) before the first turn.
type DeadAirHandler struct {
	tts    *TTSReplyConsumer
	logger *slog.Logger
}

// NewDeadAirHandler wires the W1-B.1 ASR-dead → apology+close path.
func NewDeadAirHandler(tts *TTSReplyConsumer, logger *slog.Logger) *DeadAirHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &DeadAirHandler{tts: tts, logger: logger}
}

// OnASRDead is invoked by ASRSink when ASR reconnect is exhausted.
func (h *DeadAirHandler) OnASRDead(ctx context.Context, session *Session) {
	if h.tts == nil {
		if h.logger != nil {
			h.logger.Error("dead_air asr_dead but no tts consumer wired; closing silently",
				"stream_sid", session.StreamSID,
			)
		}
		return
	}
	h.tts.SpeakApologyAndClose(ctx, session, "asr_reconnect_exhausted")
}

var _ ASRDeadListener = (*DeadAirHandler)(nil)
