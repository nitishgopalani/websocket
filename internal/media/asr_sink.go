package media

import (
	"context"
	"log/slog"
	"sync"
)

// TranscriptConsumer receives transcript and VAD events from ASRSink.
type TranscriptConsumer interface {
	OnPartial(ctx context.Context, session *Session, transcript Transcript)
	OnFinal(ctx context.Context, session *Session, transcript Transcript)
	OnSpeechStart(ctx context.Context, session *Session)
	OnSpeechEnd(ctx context.Context, session *Session)
}

// ASRDeadListener is notified by ASRSink when ASR reconnect is exhausted
// (W1-B.1, H2 dead-air defense). The listener speaks the tenant apology line
// via TTS and clean-closes the session. Implemented by the dead-air handler
// wired in the sink factory.
type ASRDeadListener interface {
	OnASRDead(ctx context.Context, session *Session)
}

// LoggingTranscriptConsumer logs transcript and VAD events.
type LoggingTranscriptConsumer struct {
	logger *slog.Logger
}

// NewLoggingTranscriptConsumer returns a consumer that logs all transcript events.
func NewLoggingTranscriptConsumer(logger *slog.Logger) *LoggingTranscriptConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingTranscriptConsumer{logger: logger}
}

func (c *LoggingTranscriptConsumer) OnPartial(_ context.Context, session *Session, transcript Transcript) {
	c.logger.Info("asr partial",
		"stream_sid", session.StreamSID,
		"text", transcript.Text,
	)
}

func (c *LoggingTranscriptConsumer) OnFinal(_ context.Context, session *Session, transcript Transcript) {
	c.logger.Info("asr final",
		"stream_sid", session.StreamSID,
		"text", transcript.Text,
	)
}

func (c *LoggingTranscriptConsumer) OnSpeechStart(_ context.Context, session *Session) {
	c.logger.Info("asr speech start", "stream_sid", session.StreamSID)
}

func (c *LoggingTranscriptConsumer) OnSpeechEnd(_ context.Context, session *Session) {
	c.logger.Info("asr speech end", "stream_sid", session.StreamSID)
}

// ASRSink terminates the audio sink chain and streams PCM16 to ASR.
type ASRSink struct {
	provider   ASRProvider
	consumer   TranscriptConsumer
	sampleRate int
	logger     *slog.Logger

	mu         sync.Mutex
	session    ASRSession
	eventsDone chan struct{}
	asrErrors  int64

	// deadAir (W1-B.1): invoked when ASR reconnect is exhausted. May be nil
	// (no dead-air wiring, e.g. unit tests); in that case ASREventDead is
	// logged but the call is not closed by the sink.
	deadAir ASRDeadListener

	// DEBT-035: ingress setup buffer. Frames arriving before the ASR WS is
	// ready (s.session == nil) are buffered here instead of dropped, then
	// drained to ASR in order once OnStart completes. Prevents early-caller-
	// speech loss during the SIP-answer + connector→go-server setup window.
	// Bounded by setupBufferMaxFrames; if the cap is hit the oldest frame is
	// dropped (logged) so memory stays bounded if ASR never opens.
	setupMu           sync.Mutex
	setupBuffer       [][]byte
	setupBufferDrops  int64
	setupBufferMaxFrames int
}

// asrSetupBufferMaxFrames is the cap on the DEBT-035 setup buffer. At 20ms
// frames this is ~10s of audio — well above the ~6s setup window observed
// in UAT (SIP-answer 5.16s + connector→go-server 1.01s).
const asrSetupBufferMaxFrames = 500

// NewASRSink creates the terminal audio sink that forwards transcripts downstream.
func NewASRSink(provider ASRProvider, consumer TranscriptConsumer, sampleRate int, logger *slog.Logger) *ASRSink {
	if provider == nil {
		provider = NoopASRProvider{}
	}
	if consumer == nil {
		consumer = NewLoggingTranscriptConsumer(logger)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ASRSink{
		provider:   provider,
		consumer:   consumer,
		sampleRate: sampleRate,
		logger:     logger,
		setupBufferMaxFrames: asrSetupBufferMaxFrames,
	}
}

// SetDeadAirListener wires the W1-B.1 terminal-ASR handler (speak apology +
// clean-close). Call from the sink factory after construction. Nil leaves
// ASREventDead logged-only (unit-test convenience).
func (s *ASRSink) SetDeadAirListener(l ASRDeadListener) {
	s.mu.Lock()
	s.deadAir = l
	s.mu.Unlock()
}

func (s *ASRSink) OnStart(ctx context.Context, session *Session) error {
	// Prefer per-session wire rate from session_start (e.g. g711=8000) over process TARGET.
	rate := s.sampleRate
	if session != nil && session.Format.SampleRate > 0 {
		rate = session.Format.SampleRate
	}
	meta := ASRSessionMeta{
		StreamSID:  session.StreamSID,
		CallSID:    session.CallSID,
		SampleRate: rate,
		Params:     session.Params,
	}
	lang := ResolveSessionASRLanguage(session.Params, s.logger, session.StreamSID)
	meta.Language = lang

	asrSession, err := s.provider.Open(ctx, meta)
	if err != nil {
		s.logger.Warn("asr open failed; continuing without transcripts",
			"stream_sid", session.StreamSID,
			"error", err,
		)
		return nil
	}

	s.mu.Lock()
	s.session = asrSession
	s.eventsDone = make(chan struct{})
	s.mu.Unlock()

	// DEBT-035: drain the setup buffer to ASR now that the WS is ready.
	// Frames arrived during the setup window (SIP-answer + connector→go-server)
	// are forwarded in order so early caller speech is not lost.
	s.setupMu.Lock()
	pending := s.setupBuffer
	s.setupBuffer = nil
	setupDrops := s.setupBufferDrops
	s.setupMu.Unlock()
	for _, f := range pending {
		if err := asrSession.SendAudio(f); err != nil {
			if err == ErrASRSessionClosed {
				break
			}
			s.logger.Warn("asr send (setup buffer drain) failed; continuing call",
				"stream_sid", session.StreamSID,
				"error", err,
			)
		}
	}
	if len(pending) > 0 || setupDrops > 0 {
		s.logger.Info("asr drained setup buffer on ready",
			"stream_sid", session.StreamSID,
			"frames_drained", len(pending),
			"setup_buffer_drops", setupDrops,
		)
	}

	go s.consumeEvents(ctx, session, asrSession)
	return nil
}

func (s *ASRSink) consumeEvents(ctx context.Context, session *Session, asrSession ASRSession) {
	defer close(s.eventsDone)
	for evt := range asrSession.Events() {
		switch evt.Type {
		case ASREventPartial:
			s.logger.Info("asr partial",
				"stream_sid", session.StreamSID,
				"text", evt.Transcript.Text,
			)
			s.consumer.OnPartial(ctx, session, evt.Transcript)
		case ASREventFinal:
			s.logger.Info("asr final",
				"stream_sid", session.StreamSID,
				"text", evt.Transcript.Text,
				"is_final", evt.Transcript.IsFinal,
			)
			s.consumer.OnFinal(ctx, session, evt.Transcript)
		case ASREventSpeechStart:
			s.consumer.OnSpeechStart(ctx, session)
		case ASREventSpeechEnd:
			s.consumer.OnSpeechEnd(ctx, session)
		case ASREventError:
			s.asrErrors++
			s.logger.Warn("asr event error",
				"stream_sid", session.StreamSID,
				"error", evt.Err,
			)
		case ASREventDead:
			// W1-B.1 (H2 dead-air defense): ASR reconnect exhausted — the
			// session is permanently deaf. Log asr_dead=true (always visible)
			// and hand off to the dead-air listener to speak the tenant apology
			// line via TTS and clean-close. Never continue deaf.
			s.asrErrors++
			s.mu.Lock()
			deadAir := s.deadAir
			s.mu.Unlock()
			s.logger.Error("asr_dead=true asr reconnect exhausted",
				"stream_sid", session.StreamSID,
				"call_sid", session.CallSID,
				"asr_errors", s.asrErrors,
				"error", evt.Err,
				"dead_air_handler_wired", deadAir != nil,
			)
			if deadAir != nil {
				deadAir.OnASRDead(ctx, session)
			}
		}
	}
}

func (s *ASRSink) OnAudio(ctx context.Context, session *Session, frame []byte) error {
	s.mu.Lock()
	asrSession := s.session
	s.mu.Unlock()
	if asrSession == nil {
		// DEBT-035: ASR WS not ready yet — buffer the ingress frame instead of
		// dropping it. Drained in order by OnStart once the WS connects. Bounded
		// by setupBufferMaxFrames; if the cap is hit the oldest frame is dropped
		// (logged) so memory stays bounded if ASR never opens.
		s.setupMu.Lock()
		if s.setupBufferMaxFrames > 0 && len(s.setupBuffer) >= s.setupBufferMaxFrames {
			s.setupBuffer = s.setupBuffer[1:]
			s.setupBufferDrops++
			s.logger.Warn("asr setup buffer full; dropping oldest frame",
				"stream_sid", session.StreamSID,
				"setup_buffer_drops", s.setupBufferDrops,
				"cap", s.setupBufferMaxFrames,
			)
		}
		s.setupBuffer = append(s.setupBuffer, frame)
		s.setupMu.Unlock()
		return nil
	}
	if err := asrSession.SendAudio(frame); err != nil {
		if err == ErrASRSessionClosed {
			return nil
		}
		s.logger.Warn("asr send failed; continuing call",
			"stream_sid", session.StreamSID,
			"error", err,
		)
	}
	return nil
}

func (s *ASRSink) OnDTMF(ctx context.Context, session *Session, digit string) error {
	s.logger.Info("dtmf at asr terminal sink",
		"stream_sid", session.StreamSID,
		"digit", digit,
	)
	return nil
}

func (s *ASRSink) OnStop(ctx context.Context, session *Session) error {
	s.mu.Lock()
	asrSession := s.session
	doneCh := s.eventsDone
	s.session = nil
	s.mu.Unlock()

	if asrSession != nil {
		if err := asrSession.Close(); err != nil {
			s.logger.Warn("asr session close failed",
				"stream_sid", session.StreamSID,
				"error", err,
			)
		}
	}
	if doneCh != nil {
		<-doneCh
	}
	s.logger.Info("asr session complete",
		"stream_sid", session.StreamSID,
		"asr_errors", s.asrErrors,
	)
	return nil
}
