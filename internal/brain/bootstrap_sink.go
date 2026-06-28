package brain

import (
	"context"
	"log/slog"

	"websocket/internal/media"
)

// BootstrapSink wraps an AudioSink to connect/disconnect the EB-6 brain client per session.
type BootstrapSink struct {
	Inner         media.AudioSink
	Brain         *Client
	TTSReply      *media.TTSReplyConsumer
	TTSProvider   media.TTSProvider
	TTSBaseCfg    media.TTSConfig
	Logger        *slog.Logger
	CarrierEgress *media.CarrierEgress
	Observability *media.SessionObservability
	AMDEnabled    bool
	CallControl   *CallControl
}

func (s *BootstrapSink) OnStart(ctx context.Context, session *media.Session) error {
	if s.Observability != nil && s.Observability.Timing != nil {
		s.Observability.Timing.BindSession(session.StreamSID)
		s.Observability.Timing.MarkSessionStart()
	}
	if s.TTSReply != nil && s.TTSProvider != nil {
		logger := s.Logger
		if logger == nil {
			logger = slog.Default()
		}
		stream, err := media.OpenSessionTTSStream(ctx, s.TTSProvider, s.TTSBaseCfg, session, logger)
		if err != nil {
			logger.Warn("tts session open failed; replies will not play audio",
				"stream_sid", session.StreamSID,
				"error", err,
			)
		} else {
			s.TTSReply.AttachStream(stream, session)
		}
	}
	if s.CarrierEgress != nil {
		if s.AMDEnabled {
			s.CarrierEgress.EnableHumanGate()
		}
		s.CarrierEgress.BindSession(session)
	}
	if s.TTSReply != nil {
		s.TTSReply.BindSession(session)
		session.SetPlaybackListener(s.playbackListener())
	} else if s.CallControl != nil {
		session.SetPlaybackListener(s.CallControl)
	} else {
		session.SetPlaybackListener(media.NewLoggingPlaybackListener(nil))
	}
	if s.Brain != nil && !s.AMDEnabled {
		if err := s.Brain.Connect(ctx, session); err != nil {
			return err
		}
		if s.CallControl != nil {
			s.CallControl.markBrainConnected()
		}
		if err := s.Brain.SendOpenerTurn(session); err != nil {
			return err
		}
		if s.CallControl != nil {
			s.CallControl.recordOpener()
		}
	}
	if s.Inner == nil {
		return nil
	}
	return s.Inner.OnStart(ctx, session)
}

func (s *BootstrapSink) playbackListener() media.PlaybackListener {
	if s.CallControl != nil {
		return s.CallControl
	}
	return s.TTSReply
}

func (s *BootstrapSink) OnAudio(ctx context.Context, session *media.Session, frame []byte) error {
	if s.Inner == nil {
		return nil
	}
	return s.Inner.OnAudio(ctx, session, frame)
}

func (s *BootstrapSink) OnDTMF(ctx context.Context, session *media.Session, digit string) error {
	if s.Inner == nil {
		return nil
	}
	return s.Inner.OnDTMF(ctx, session, digit)
}

func (s *BootstrapSink) OnStop(ctx context.Context, session *media.Session) error {
	if s.Observability != nil {
		s.Observability.Shutdown()
	}
	if s.CarrierEgress != nil {
		s.CarrierEgress.Unbind()
	}
	if s.TTSReply != nil {
		_ = s.TTSReply.Close()
	}
	if s.Brain != nil {
		_ = s.Brain.Close()
	}
	if s.Inner == nil {
		return nil
	}
	return s.Inner.OnStop(ctx, session)
}

var _ media.AudioSink = (*BootstrapSink)(nil)
