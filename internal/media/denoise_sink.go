package media

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// AEC cancels echo from near-end mic using far-end reference audio.
// Real implementation is deferred; NoopAEC is wired today.
type AEC interface {
	Cancel(near, far []byte) []byte
}

// NoopAEC returns near-end audio unchanged.
type NoopAEC struct{}

func (NoopAEC) Cancel(near, _ []byte) []byte {
	out := make([]byte, len(near))
	copy(out, near)
	return out
}

// DenoiseSink applies optional AEC then denoising before forwarding PCM16 frames.
//
// CT-1 delivers audio for each session on a single worker goroutine, so per-session
// counters on this sink do not require locking.
type DenoiseSink struct {
	next        AudioSink
	d           Denoiser
	aec         AEC
	sampleRate  int
	logger      *slog.Logger
	ownDenoiser bool

	framesDenoised atomic.Int64
	fallbacks      atomic.Int64
}

// NewDenoiseSink wraps next with denoise (+ optional AEC) processing.
func NewDenoiseSink(next AudioSink, d Denoiser, aec AEC, sampleRate int, logger *slog.Logger) *DenoiseSink {
	if next == nil {
		next = NewLoggingSink(logger)
	}
	if d == nil {
		d = NoopDenoiser{}
	}
	if aec == nil {
		aec = NoopAEC{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &DenoiseSink{
		next:       next,
		d:          d,
		aec:        aec,
		sampleRate: sampleRate,
		logger:     logger,
	}
}

// NewDenoiseSinkWithOwnedDenoiser marks the denoiser as session-owned so Close runs on OnStop.
func NewDenoiseSinkWithOwnedDenoiser(next AudioSink, d Denoiser, aec AEC, sampleRate int, logger *slog.Logger) *DenoiseSink {
	s := NewDenoiseSink(next, d, aec, sampleRate, logger)
	s.ownDenoiser = true
	return s
}

// FramesDenoised returns successfully denoised frame count for this session.
func (s *DenoiseSink) FramesDenoised() int64 {
	return s.framesDenoised.Load()
}

// Fallbacks returns fail-open fallback count for this session.
func (s *DenoiseSink) Fallbacks() int64 {
	return s.fallbacks.Load()
}

func (s *DenoiseSink) OnStart(ctx context.Context, session *Session) error {
	return s.next.OnStart(ctx, session)
}

func (s *DenoiseSink) OnAudio(ctx context.Context, session *Session, frame []byte) error {
	original := frame
	near := s.aec.Cancel(frame, nil)

	denoised, err := s.d.Process(ctx, near, s.sampleRate)
	if err != nil {
		s.fallbacks.Add(1)
		s.logger.Warn("denoise fail-open",
			"stream_sid", session.StreamSID,
			"error", err,
			"session_fallbacks", s.Fallbacks(),
		)
		denoised = original
	} else {
		s.framesDenoised.Add(1)
	}

	return s.next.OnAudio(ctx, session, denoised)
}

func (s *DenoiseSink) OnDTMF(ctx context.Context, session *Session, digit string) error {
	return s.next.OnDTMF(ctx, session, digit)
}

func (s *DenoiseSink) OnStop(ctx context.Context, session *Session) error {
	s.logger.Info("denoise session complete",
		"stream_sid", session.StreamSID,
		"frames_denoised", s.FramesDenoised(),
		"fallbacks", s.Fallbacks(),
	)
	if s.ownDenoiser && s.d != nil {
		if err := s.d.Close(); err != nil {
			s.logger.Warn("denoise close failed", "stream_sid", session.StreamSID, "error", err)
		}
	}
	return s.next.OnStop(ctx, session)
}
