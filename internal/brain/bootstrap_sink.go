package brain

import (
	"context"

	"websocket/internal/media"
)

// BootstrapSink wraps an AudioSink to connect/disconnect the EB-6 brain client per session.
type BootstrapSink struct {
	Inner media.AudioSink
	Brain *Client
}

func (s *BootstrapSink) OnStart(ctx context.Context, session *media.Session) error {
	if s.Brain != nil {
		if err := s.Brain.Connect(ctx, session); err != nil {
			return err
		}
		if err := s.Brain.SendOpenerTurn(session); err != nil {
			return err
		}
	}
	if s.Inner == nil {
		return nil
	}
	return s.Inner.OnStart(ctx, session)
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
	if s.Brain != nil {
		_ = s.Brain.Close()
	}
	if s.Inner == nil {
		return nil
	}
	return s.Inner.OnStop(ctx, session)
}

var _ media.AudioSink = (*BootstrapSink)(nil)
