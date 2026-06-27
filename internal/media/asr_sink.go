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
}

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
	}
}

func (s *ASRSink) OnStart(ctx context.Context, session *Session) error {
	meta := ASRSessionMeta{
		StreamSID:  session.StreamSID,
		CallSID:    session.CallSID,
		SampleRate: s.sampleRate,
		Language:   session.Params["language"],
		Params:     session.Params,
	}
	if meta.Language == "" {
		meta.Language = DefaultASRConfig().Language
	}

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
		}
	}
}

func (s *ASRSink) OnAudio(ctx context.Context, session *Session, frame []byte) error {
	s.mu.Lock()
	asrSession := s.session
	s.mu.Unlock()
	if asrSession == nil {
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
