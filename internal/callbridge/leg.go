package callbridge

import (
	"errors"
	"log/slog"
	"sync"
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

// session is one connector WebSocket leg.
type session struct {
	server    *Server
	room      *room
	peer      *session
	conn      *websocket.Conn
	sessionID string
	role      LegRole
	rates     audioRates
	paired    bool

	outbound *outboundQueue
	writeCh  chan writeRequest
	done     chan struct{}
	closeOnce sync.Once
}

func (s *session) directionOut() string {
	if s.role == LegA {
		return "a_to_b"
	}
	return "b_to_a"
}

func (s *session) directionIn() string {
	if s.role == LegA {
		return "b_to_a"
	}
	return "a_to_b"
}

func (s *session) startWriter() {
	go s.writeLoop()
}

func (s *session) writeLoop() {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case req := <-s.writeCh:
			s.writeOne(req)
		case <-ticker.C:
			if f, ok := s.outbound.dequeue(time.Now()); ok {
				s.writeFrame(f)
			}
		}
	}
}

func (s *session) writeOne(req writeRequest) {
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

func (s *session) writeFrame(f relayFrame) {
	now := time.Now()
	if s.server.cfg.MaxFrameAge > 0 && now.Sub(f.arrivedAt) > s.server.cfg.MaxFrameAge {
		s.server.metrics.incDropAge(s.directionOut())
		return
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := s.conn.WriteMessage(websocket.BinaryMessage, f.payload); err != nil {
		s.server.logger.Debug("bridge write failed", "session_id", s.sessionID, "error", err)
		return
	}
	s.server.metrics.incRelay(s.directionOut())
	if !f.arrivedAt.IsZero() {
		s.server.metrics.observeLatency(s.directionOut(), float64(now.Sub(f.arrivedAt).Microseconds())/1000.0)
	}
}

func (s *session) sendControl(payload []byte, wait bool) {
	req := writeRequest{kind: writeControl, payload: payload}
	if wait {
		req.ack = make(chan struct{})
	}
	s.writeCh <- req
	if wait {
		<-req.ack
	}
}

func (s *session) signalReady() {
	ready, err := s.server.readyMessage()
	if err != nil {
		return
	}
	s.sendControl(ready, true)
	s.paired = true
}

func (s *session) sendEndOfCall() {
	msg, err := s.server.endOfCallMessage()
	if err != nil {
		return
	}
	s.sendControl(msg, true)
}

func (s *session) sendError(code, message string) {
	msg, err := s.server.errorMessage(message, code)
	if err != nil {
		return
	}
	s.sendControl(msg, true)
}

func (s *session) relayToPeer(payload []byte, arrivedAt time.Time) {
	if s.peer == nil || !s.paired {
		return
	}
	s.peer.outbound.enqueue(payload, arrivedAt)
}

func (s *session) closeConn() {
	s.closeOnce.Do(func() {
		close(s.done)
		_ = s.conn.Close()
	})
}

func (s *session) teardown(reason string) {
	peer := s.peer
	room := s.room
	s.closeConn()
	if room != nil {
		room.detach(s, reason)
	}
	_ = peer
}

func newSession(srv *Server, conn *websocket.Conn, sessionID string) *session {
	sess := &session{
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

func (s *session) readLoop(logger *slog.Logger) {
	defer s.teardown("leg_disconnect")
	for {
		mt, data, err := s.conn.ReadMessage()
		if err != nil {
			if s.paired {
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
				logger.Debug("bridge control", "session_id", s.sessionID, "error", err)
				return
			}
		case websocket.BinaryMessage:
			if !s.paired {
				continue
			}
			s.relayToPeer(data, time.Now())
		}
	}
}

func (s *session) handleControl(data []byte) error {
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

var errSessionEnded = errors.New("callbridge: session ended")
