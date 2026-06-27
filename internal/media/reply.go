package media

import (
	"context"
	"log/slog"
)

// ReplyConsumer receives gated reply text from the engine (CT-8 seam).
type ReplyConsumer interface {
	OnReplyChunk(ctx context.Context, session *Session, turnID string, seq int, text string)
	OnReplyDone(ctx context.Context, session *Session, turnID string, endCall bool, disposition string)
	OnReplyError(ctx context.Context, session *Session, turnID string, fallbackText string)
}

// LoggingReplyConsumer logs reply events until TTS or egress is wired (CT-8 default).
type LoggingReplyConsumer struct {
	logger *slog.Logger
}

// NewLoggingReplyConsumer returns a reply consumer that logs text chunks and turn completion.
func NewLoggingReplyConsumer(logger *slog.Logger) *LoggingReplyConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingReplyConsumer{logger: logger}
}

func (l *LoggingReplyConsumer) OnReplyChunk(_ context.Context, session *Session, turnID string, seq int, text string) {
	if l.logger != nil {
		l.logger.Info("reply chunk",
			"stream_sid", session.StreamSID,
			"turn_id", turnID,
			"seq", seq,
			"text", text,
		)
	}
}

func (l *LoggingReplyConsumer) OnReplyDone(_ context.Context, session *Session, turnID string, endCall bool, disposition string) {
	if l.logger != nil {
		l.logger.Info("reply done",
			"stream_sid", session.StreamSID,
			"turn_id", turnID,
			"end_call", endCall,
			"disposition", disposition,
		)
	}
}

func (l *LoggingReplyConsumer) OnReplyError(_ context.Context, session *Session, turnID, fallbackText string) {
	if l.logger != nil {
		l.logger.Info("reply error",
			"stream_sid", session.StreamSID,
			"turn_id", turnID,
			"fallback", fallbackText,
		)
	}
}
