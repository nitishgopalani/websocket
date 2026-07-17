package translator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type writeKind int

const (
	writeControl writeKind = iota
	writeBinary
)

type writeRequest struct {
	kind      writeKind
	payload   []byte
	arrivedAt time.Time
	ack       chan struct{}
}

// leg is one connector WebSocket session.
type leg struct {
	server    *Server
	room      *room
	peer      *leg
	conn      *websocket.Conn
	sessionID string
	role      LegRole
	rates     audioRates
	paired    atomic.Bool

	outbound *outboundQueue
	writeCh  chan writeRequest
	done     chan struct{}
	closeOnce sync.Once

	// lastPeerAudioAt suppresses comfort noise while the peer uplink or TTS is active.
	lastPeerAudioAt atomic.Int64
	// lastComfortNoiseAt paces one-way comfort noise to ~20ms cadence.
	lastComfortNoiseAt atomic.Int64
}

func (s *leg) directionOut() string {
	if s.role == LegA {
		return "a_to_b"
	}
	return "b_to_a"
}

func (s *leg) directionIn() string {
	if s.role == LegA {
		return "b_to_a"
	}
	return "a_to_b"
}

func (s *leg) startWriter() {
	go s.writeLoop()
}

func (s *leg) writeLoop() {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case req := <-s.writeCh:
			s.writeOne(req)
		case <-ticker.C:
			now := time.Now()
			if f, ok := s.outbound.dequeueReady(now); ok {
				s.writeFrame(f)
			} else {
				s.maybeInjectComfortNoise(now)
			}
		}
	}
}

// maybeInjectComfortNoise keeps line audio when the peer is quiet (3b-i one-way only).
// Bidirectional mode uses silence between TTS utterances — synthetic hiss was misheard as "chrrrr".
func (s *leg) maybeInjectComfortNoise(now time.Time) {
	if !s.paired.Load() || !s.server.cfg.TranslationEnabled {
		return
	}
	if s.server.cfg.Bidirectional {
		return
	}
	if s.role != LegA {
		return
	}
	if !s.outbound.isEmpty() {
		return
	}
	last := s.lastPeerAudioAt.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < 40*time.Millisecond {
		return
	}
	prev := s.lastComfortNoiseAt.Load()
	if prev != 0 && now.Sub(time.Unix(0, prev)) < 20*time.Millisecond {
		return
	}
	s.lastComfortNoiseAt.Store(now.UnixNano())
	s.outbound.enqueue(comfortNoisePCM(), now)
}

// notifyPeerUplink marks the peer leg while this leg's microphone is active (relay may be muted in 3b-ii).
func (s *leg) notifyPeerUplink(at time.Time) {
	if s.peer != nil {
		s.peer.lastPeerAudioAt.Store(at.UnixNano())
	}
}

func (s *leg) writeOne(req writeRequest) {
	_ = s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	switch req.kind {
	case writeControl:
		_ = s.conn.WriteMessage(websocket.TextMessage, req.payload)
	case writeBinary:
		s.writeFrame(relayFrame{payload: req.payload, arrivedAt: req.arrivedAt})
	}
	if req.ack != nil {
		close(req.ack)
	}
}

func (s *leg) writeFrame(f relayFrame) {
	now := time.Now()
	if s.server.cfg.MaxFrameAge > 0 && now.Sub(f.arrivedAt) > s.server.cfg.MaxFrameAge {
		s.server.metrics.incDropAge(s.directionOut())
		return
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := s.conn.WriteMessage(websocket.BinaryMessage, f.payload); err != nil {
		s.server.logger.Debug("translator write failed", "session_id", s.sessionID, "error", err)
		return
	}
	s.server.metrics.incRelay(s.directionOut())
	if !f.arrivedAt.IsZero() {
		s.server.metrics.observeLatency(s.directionOut(), float64(now.Sub(f.arrivedAt).Microseconds())/1000.0)
	}
}

func (s *leg) enqueueBinary(payload []byte, arrivedAt time.Time) {
	s.outbound.enqueueFrame(relayFrame{payload: payload, arrivedAt: arrivedAt})
}

// enqueueBinaryPaced schedules TTS frames for real-time playback (one frame per frame period).
func (s *leg) enqueueBinaryPaced(payload []byte, playAt time.Time) {
	arrivedAt := playAt
	if arrivedAt.IsZero() {
		arrivedAt = time.Now()
	}
	// TTS playback keeps the line active — suppress comfort noise gaps on one-way leg A.
	s.lastPeerAudioAt.Store(arrivedAt.UnixNano())
	s.outbound.enqueueFrame(relayFrame{payload: payload, arrivedAt: arrivedAt, playAt: playAt})
}

func (s *leg) sendControl(payload []byte, wait bool) {
	req := writeRequest{kind: writeControl, payload: payload}
	if wait {
		req.ack = make(chan struct{})
	}
	s.writeCh <- req
	if wait {
		<-req.ack
	}
}

func (s *leg) signalReady() {
	if s.paired.Load() {
		return
	}
	ready, err := s.server.readyMessage()
	if err != nil {
		return
	}
	s.sendControl(ready, true)
	s.paired.Store(true)
}

func (s *leg) sendEndOfCall() {
	msg, err := s.server.endOfCallMessage()
	if err != nil {
		return
	}
	s.sendControl(msg, true)
}

func (s *leg) sendError(code, message string) {
	msg, err := s.server.errorMessage(message, code)
	if err != nil {
		return
	}
	s.sendControl(msg, true)
}

func (s *leg) shouldRelayToPeer() bool {
	if !s.server.cfg.TranslationEnabled || s.peer == nil {
		return true
	}
	if s.room == nil {
		return true
	}
	if s.room.loopBreakerTripped() {
		return true
	}
	relayLane := s.room.relayLaneFor(s.role)
	if relayLane == nil {
		return true
	}
	if s.role == LegB && !s.server.cfg.Bidirectional {
		return true
	}
	return relayLane.failOpenActive()
}

func (s *leg) relayToPeer(payload []byte, arrivedAt time.Time) {
	if s.peer == nil || !s.paired.Load() || !s.shouldRelayToPeer() {
		return
	}
	s.peer.lastPeerAudioAt.Store(arrivedAt.UnixNano())
	s.peer.outbound.enqueue(payload, arrivedAt) // relay: unpaced, low-latency drain
}

func (s *leg) closeConn() {
	s.closeOnce.Do(func() {
		close(s.done)
		_ = s.conn.Close()
	})
}

func (s *leg) teardown(reason string) {
	peer := s.peer
	room := s.room
	s.closeConn()
	if room != nil {
		room.detach(s, reason)
	}
	_ = peer
}

func newLeg(srv *Server, conn *websocket.Conn, sessionID string) *leg {
	sess := &leg{
		server:    srv,
		conn:      conn,
		sessionID: sessionID,
		writeCh:   make(chan writeRequest, 16),
		done:      make(chan struct{}),
	}
	sess.outbound = newOutboundQueue(
		srv.cfg.QueueFrames,
		srv.cfg.MaxFrameAge,
		func() { srv.metrics.incDropAge(sess.directionOut()) },
		func() { srv.metrics.incDropDepth(sess.directionOut()) },
	)
	sess.startWriter()
	return sess
}

func (s *leg) readLoop(logger *slog.Logger) {
	defer s.teardown("leg_disconnect")
	for {
		mt, data, err := s.conn.ReadMessage()
		if err != nil {
			if s.paired.Load() {
				if s.role == LegA {
					s.server.metrics.incTeardown("leg_a_end")
				} else {
					s.server.metrics.incTeardown("leg_b_end")
				}
			}
			return
		}
		switch mt {
		case websocket.TextMessage:
			if err := s.handleControl(data); err != nil {
				logger.Debug("translator control", "session_id", s.sessionID, "error", err)
				return
			}
		case websocket.BinaryMessage:
			if !s.paired.Load() {
				continue
			}
			arrived := time.Now()
			s.notifyPeerUplink(arrived)
			if s.server.cfg.TranslationEnabled && s.room != nil {
				if s.room.allowASRIngest(s.role) {
					if lane := s.room.laneForSource(s.role); lane != nil {
						_ = lane.ingestAudio(context.Background(), data)
					}
				}
			}
			s.relayToPeer(data, arrived)
		}
	}
}

func (s *leg) handleControl(data []byte) error {
	ctrl, err := s.server.parseControl(data)
	if err != nil {
		return err
	}
	switch ctrl.Type {
	case s.server.msgSessionEnd():
		if s.role == LegA {
			s.teardown("leg_a_end")
		} else {
			s.teardown("leg_b_end")
		}
		return errSessionEnded
	default:
	}
	return nil
}

var errSessionEnded = errors.New("translator: session ended")
