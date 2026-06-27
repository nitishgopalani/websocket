package media

import (
	"context"
	"log/slog"
)

// PlaybackListener is notified when the carrier echoes a mark after playback reaches it.
type PlaybackListener interface {
	OnPlaybackComplete(ctx context.Context, session *Session, turnID string)
}

// LoggingPlaybackListener logs playback-complete events (default when no engine hook is wired).
type LoggingPlaybackListener struct {
	logger *slog.Logger
}

// NewLoggingPlaybackListener returns a listener that logs mark echoes.
func NewLoggingPlaybackListener(logger *slog.Logger) *LoggingPlaybackListener {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingPlaybackListener{logger: logger}
}

func (l *LoggingPlaybackListener) OnPlaybackComplete(_ context.Context, session *Session, turnID string) {
	if l.logger != nil {
		l.logger.Info("playback complete",
			"stream_sid", session.StreamSID,
			"turn_id", turnID,
		)
	}
}
