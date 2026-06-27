package media

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ErrSessionNotFound      = errors.New("session not found")
	ErrMaxSessionsExceeded  = errors.New("max concurrent sessions exceeded")
	ErrMissingStreamSID     = errors.New("missing stream_sid")
	ErrInvalidStartEvent    = errors.New("invalid start event")
	ErrSessionAlreadyExists = errors.New("session already exists")
)

// Session tracks one bidirectional media stream.
type Session struct {
	StreamSID string
	CallSID   string
	Format    AudioFormat
	Params    map[string]string
	StartedAt time.Time
	FramesIn  int64

	sink             AudioSink
	logger           *slog.Logger
	audioCh          chan []byte
	outboundQueue    []outboundFrame
	outboundQueueMu  sync.Mutex
	outboundWake     chan struct{}
	outboundCap      int
	stopCh           chan struct{}
	closeOnce        sync.Once
	closed           atomic.Bool
	framesDropped    int64
	outboundDropped  int64
	framesDelivered  int64
	playbackListener PlaybackListener
	metrics          *Metrics
	wg               sync.WaitGroup
	outboundOnce     sync.Once
}

// outboundFrame is one serialized JSON message queued for the single outbound WS writer.
type outboundFrame struct {
	data    []byte
	isAudio bool
}

// FramesDropped returns the number of audio frames dropped due to backpressure.
func (s *Session) FramesDropped() int64 {
	return atomic.LoadInt64(&s.framesDropped)
}

// OutboundDropped returns outbound audio frames dropped due to backpressure.
func (s *Session) OutboundDropped() int64 {
	return atomic.LoadInt64(&s.outboundDropped)
}

// SetMetrics attaches CT-12 metrics for outbound drop counting.
func (s *Session) SetMetrics(m *Metrics) {
	s.metrics = m
}
func (s *Session) SetPlaybackListener(l PlaybackListener) {
	s.playbackListener = l
}

// SessionManager owns active sessions keyed by stream_sid.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	cfg      Config
	logger   *slog.Logger
	newSink  func() AudioSink
	metrics  *Metrics
}

// NewSessionManager creates a manager with the provided sink factory.
func NewSessionManager(cfg Config, logger *slog.Logger, newSink func() AudioSink, metrics *Metrics) *SessionManager {
	if logger == nil {
		logger = slog.Default()
	}
	if newSink == nil {
		newSink = func() AudioSink { return NewLoggingSink(logger) }
	}
	return &SessionManager{
		sessions: make(map[string]*Session),
		cfg:      cfg.withDefaults(),
		logger:   logger,
		newSink:  newSink,
		metrics:  metrics,
	}
}

// Count returns the number of active sessions.
func (m *SessionManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// Get returns an active session by stream_sid.
func (m *SessionManager) Get(streamSID string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	session, ok := m.sessions[streamSID]
	return session, ok
}

func (m *SessionManager) updateActiveSessionsGauge() {
	if m.metrics == nil {
		return
	}
	m.metrics.SetActiveSessions(m.Count())
}

// Create opens a session from a start event. When conn is non-nil the session starts its
// single outbound writer goroutine (gorilla/websocket requires one writer per connection).
func (m *SessionManager) Create(ctx context.Context, start StartEvent, conn *websocket.Conn) (*Session, error) {
	if start.StreamSID == "" {
		return nil, fmt.Errorf("%w: start event", ErrMissingStreamSID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sessions[start.StreamSID]; exists {
		return nil, fmt.Errorf("%w: %s", ErrSessionAlreadyExists, start.StreamSID)
	}
	if len(m.sessions) >= m.cfg.MaxConcurrentSessions {
		return nil, ErrMaxSessionsExceeded
	}

	params := start.CustomParameters
	if params == nil {
		params = map[string]string{}
	}

	egressCfg := EgressConfigFromEnv()
	session := &Session{
		StreamSID:    start.StreamSID,
		CallSID:      start.CallSID,
		Format:       start.MediaFormat,
		Params:       params,
		StartedAt:    time.Now(),
		sink:         m.newSink(),
		logger:       m.logger,
		audioCh:      make(chan []byte, m.cfg.AudioBufferSize),
		outboundWake: make(chan struct{}, 1),
		outboundCap:  egressCfg.OutboundBufferFrames,
		stopCh:       make(chan struct{}),
	}

	m.sessions[start.StreamSID] = session
	session.SetMetrics(m.metrics)
	m.updateActiveSessionsGauge()
	session.startWorker(ctx)
	if conn != nil {
		session.startOutboundWriter(conn)
	}

	if err := session.sink.OnStart(ctx, session); err != nil {
		session.close(ctx)
		delete(m.sessions, start.StreamSID)
		return nil, fmt.Errorf("sink on start: %w", err)
	}

	m.logger.Info("session created",
		"stream_sid", session.StreamSID,
		"call_sid", session.CallSID,
		"active_sessions", len(m.sessions),
	)
	return session, nil
}

// HandleMedia decodes and enqueues one audio frame for the session.
func (m *SessionManager) HandleMedia(ctx context.Context, evt MediaEvent) error {
	if evt.StreamSID == "" {
		return fmt.Errorf("%w: media event", ErrMissingStreamSID)
	}

	session, ok := m.Get(evt.StreamSID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, evt.StreamSID)
	}
	return session.enqueueMedia(ctx, evt)
}

// HandleMark forwards a carrier mark echo to the playback listener.
func (m *SessionManager) HandleMark(ctx context.Context, evt MarkEvent) error {
	if evt.StreamSID == "" {
		return fmt.Errorf("%w: mark event", ErrMissingStreamSID)
	}

	session, ok := m.Get(evt.StreamSID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, evt.StreamSID)
	}
	return session.handleMark(ctx, evt)
}

// HandleDTMF forwards a keypad digit to the session sink.
func (m *SessionManager) HandleDTMF(ctx context.Context, evt DTMFEvent) error {
	if evt.StreamSID == "" {
		return fmt.Errorf("%w: dtmf event", ErrMissingStreamSID)
	}

	session, ok := m.Get(evt.StreamSID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, evt.StreamSID)
	}
	return session.sink.OnDTMF(ctx, session, evt.DTMF.Digit)
}

// Close ends a session and removes it from the manager. Safe to call multiple times.
func (m *SessionManager) Close(ctx context.Context, streamSID string) {
	if streamSID == "" {
		return
	}

	m.mu.Lock()
	session, ok := m.sessions[streamSID]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.sessions, streamSID)
	m.mu.Unlock()

	session.close(ctx)
	m.updateActiveSessionsGauge()
	m.logger.Info("session closed",
		"stream_sid", streamSID,
		"active_sessions", m.Count(),
	)
}

// CloseAll closes every active session.
func (m *SessionManager) CloseAll(ctx context.Context) {
	m.mu.Lock()
	streamSIDs := make([]string, 0, len(m.sessions))
	for streamSID := range m.sessions {
		streamSIDs = append(streamSIDs, streamSID)
	}
	m.mu.Unlock()

	for _, streamSID := range streamSIDs {
		m.Close(ctx, streamSID)
	}
}

func (s *Session) handleMark(ctx context.Context, evt MarkEvent) error {
	if s.playbackListener == nil {
		return nil
	}
	s.playbackListener.OnPlaybackComplete(ctx, s, evt.Mark.Name)
	return nil
}

// EnqueueOutbound enqueues a serialized outbound WS message (non-blocking).
// When the buffer is full, the oldest queued audio frame is dropped and OutboundDropped increments.
func (s *Session) EnqueueOutbound(data []byte, isAudio bool) {
	if s.closed.Load() || len(data) == 0 {
		return
	}
	item := outboundFrame{data: data, isAudio: isAudio}

	s.outboundQueueMu.Lock()
	if len(s.outboundQueue) < s.outboundCap {
		s.outboundQueue = append(s.outboundQueue, item)
		s.outboundQueueMu.Unlock()
		s.signalOutboundWriter()
		return
	}

	if item.isAudio {
		droppedAudio := false
		for i, q := range s.outboundQueue {
			if q.isAudio {
				s.outboundQueue = append(s.outboundQueue[:i], s.outboundQueue[i+1:]...)
				atomic.AddInt64(&s.outboundDropped, 1)
				if s.metrics != nil {
					s.metrics.IncOutboundDrop()
				}
				droppedAudio = true
				break
			}
		}
		if droppedAudio {
			s.outboundQueue = append(s.outboundQueue, item)
		} else {
			atomic.AddInt64(&s.outboundDropped, 1)
			if s.metrics != nil {
				s.metrics.IncOutboundDrop()
			}
		}
	} else {
		droppedAudio := false
		for i, q := range s.outboundQueue {
			if q.isAudio {
				s.outboundQueue = append(s.outboundQueue[:i], s.outboundQueue[i+1:]...)
				atomic.AddInt64(&s.outboundDropped, 1)
				if s.metrics != nil {
					s.metrics.IncOutboundDrop()
				}
				droppedAudio = true
				break
			}
		}
		if droppedAudio {
			s.outboundQueue = append(s.outboundQueue, item)
		} else if len(s.outboundQueue) > 0 {
			s.outboundQueue = s.outboundQueue[1:]
			s.outboundQueue = append(s.outboundQueue, item)
		}
	}
	s.outboundQueueMu.Unlock()
	s.signalOutboundWriter()
}

func (s *Session) signalOutboundWriter() {
	select {
	case s.outboundWake <- struct{}{}:
	default:
	}
}

func (s *Session) dequeueOutbound() (outboundFrame, bool) {
	s.outboundQueueMu.Lock()
	defer s.outboundQueueMu.Unlock()
	if len(s.outboundQueue) == 0 {
		return outboundFrame{}, false
	}
	item := s.outboundQueue[0]
	s.outboundQueue = s.outboundQueue[1:]
	return item, true
}

// startOutboundWriter runs the sole goroutine allowed to write on the carrier websocket.
func (s *Session) startOutboundWriter(conn *websocket.Conn) {
	s.outboundOnce.Do(func() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				select {
				case <-s.stopCh:
					return
				case <-s.outboundWake:
					for {
						item, ok := s.dequeueOutbound()
						if !ok {
							break
						}
						_ = conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
						if err := conn.WriteMessage(websocket.TextMessage, item.data); err != nil {
							s.logger.Debug("outbound write failed",
								"stream_sid", s.StreamSID,
								"error", err,
							)
							return
						}
					}
				}
			}
		}()
	})
}

func (s *Session) startWorker(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.stopCh:
				s.drainAudio(ctx)
				return
			case <-ctx.Done():
				s.drainAudio(ctx)
				return
			case frame, ok := <-s.audioCh:
				if !ok {
					return
				}
				s.deliverAudio(ctx, frame)
			}
		}
	}()
}

func (s *Session) deliverAudio(ctx context.Context, frame []byte) {
	if err := s.sink.OnAudio(ctx, s, frame); err != nil {
		s.logger.Error("sink on audio failed",
			"stream_sid", s.StreamSID,
			"error", err,
		)
	}
}

func (s *Session) drainAudio(ctx context.Context) {
	for {
		select {
		case frame := <-s.audioCh:
			s.deliverAudio(ctx, frame)
		default:
			return
		}
	}
}

func (s *Session) enqueueMedia(ctx context.Context, evt MediaEvent) error {
	if s.closed.Load() {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, s.StreamSID)
	}

	payload := evt.Media.Payload
	if payload == "" {
		s.logger.Warn("media event missing payload", "stream_sid", s.StreamSID)
		return nil
	}

	frame, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		s.logger.Warn("failed to decode media payload",
			"stream_sid", s.StreamSID,
			"error", err,
		)
		return nil
	}

	atomic.AddInt64(&s.FramesIn, 1)

	select {
	case s.audioCh <- frame:
		return nil
	default:
		select {
		case <-s.audioCh:
			atomic.AddInt64(&s.framesDropped, 1)
			s.logger.Warn("dropping oldest audio frame due to backpressure",
				"stream_sid", s.StreamSID,
				"dropped_total", s.FramesDropped(),
			)
		default:
		}

		select {
		case s.audioCh <- frame:
			return nil
		case <-s.stopCh:
			return fmt.Errorf("%w: %s", ErrSessionNotFound, s.StreamSID)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Session) close(ctx context.Context) {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.stopCh)
		s.wg.Wait()

		if s.sink != nil {
			if err := s.sink.OnStop(ctx, s); err != nil {
				s.logger.Error("sink on stop failed",
					"stream_sid", s.StreamSID,
					"error", err,
				)
			}
		}
	})
}
