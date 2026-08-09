package brain

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"websocket/internal/media"
)

// defaultDrainReadyTimeoutMs is the fallback that auto-releases the egress
// drain-ready gate (DEBT-040) if no ingress frame ever arrives — prevents
// a misconfigured/late Asterisk bridge from deadlocking the call into
// silence. 2000ms covers the observed SIP-answer + bridge-create latency.
const defaultDrainReadyTimeoutMs = 2000

func drainReadyTimeoutFromEnv() int {
	if v := os.Getenv("DRAIN_READY_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

func isTapOnlySession(session *media.Session) bool {
	if session == nil || session.Params == nil {
		return false
	}
	return session.Params["tap_only"] == "true"
}

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
		// Sarvam ASR is WS streaming when enabled; dump alias asr_path=ws.
		s.Observability.Timing.SetSessionASRPath("ws")
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
		} else {
			// DEBT-040: when AMD is off, the opener TTS burst fires inside
			// OnStart with no egress gate. The Asterisk bridge may not be
			// draining yet → opener audio clipped (~740ms in live session
			// 0cc56de1). Gate the pacer until the first ingress frame
			// (Asterisk is sending = bridge is live); pendingFrames holds
			// the burst during the wait. A timeout fallback auto-resumes
			// if no ingress frame ever arrives (avoids a silent deadlock).
			s.CarrierEgress.EnableDrainReadyGate()
			_drainTimeout := defaultDrainReadyTimeoutMs
			if v := drainReadyTimeoutFromEnv(); v > 0 {
				_drainTimeout = v
			}
			_session := session
			_egress := s.CarrierEgress
			_logger := s.Logger
			_session.SetDrainReadyCallback(func() {
				_egress.ConfirmDrainReady()
				if _logger != nil {
					_logger.Info("egress drain-ready gate released on first ingress frame",
						"stream_sid", _session.StreamSID,
					)
				}
			})
			time.AfterFunc(time.Duration(_drainTimeout)*time.Millisecond, func() {
				if _egress.DrainReadyGated() {
					_egress.ConfirmDrainReady()
					if _logger != nil {
						_logger.Warn("egress drain-ready gate auto-released on timeout",
							"stream_sid", _session.StreamSID,
							"timeout_ms", _drainTimeout,
						)
					}
				}
			})
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
		if !isTapOnlySession(session) {
			if err := s.Brain.SendOpenerTurn(session); err != nil {
				return err
			}
			if s.CallControl != nil {
				s.CallControl.recordOpener()
			}
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
