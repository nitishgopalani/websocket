package media

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func (s *Server) handleExotelWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	ctx := r.Context()
	var activeStreamSID string
	var closeOnce sync.Once
	closeSession := func() {
		closeOnce.Do(func() {
			if activeStreamSID != "" {
				s.manager.Close(ctx, activeStreamSID)
				activeStreamSID = ""
			}
			_ = conn.Close()
		})
	}
	defer closeSession()

	conn.SetReadLimit(1 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(defaultReadTimeout))

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				s.logger.Debug("websocket read ended", "error", err)
			}
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(defaultReadTimeout))

		evt, err := ParseInboundEvent(data, s.logger)
		if err != nil {
			s.logger.Warn("failed to parse inbound event", "error", err)
			continue
		}

		switch evt.Type {
		case EventConnected:
			s.logger.Info("stream connected")
		case EventStart:
			if evt.Start == nil {
				s.logger.Warn("start event missing payload")
				continue
			}
			session, err := s.manager.Create(ctx, *evt.Start, conn)
			if err != nil {
				s.rejectSession(conn, evt.Start.StreamSID, err)
				return
			}
			activeStreamSID = session.StreamSID
		case EventMedia:
			if evt.Media == nil {
				continue
			}
			if err := s.manager.HandleMedia(ctx, *evt.Media); err != nil {
				s.logger.Warn("media handling failed", "error", err)
			}
		case EventDTMF:
			if evt.DTMF == nil {
				continue
			}
			if err := s.manager.HandleDTMF(ctx, *evt.DTMF); err != nil {
				s.logger.Warn("dtmf handling failed", "error", err)
			}
		case EventMark:
			if evt.Mark == nil {
				continue
			}
			if err := s.manager.HandleMark(ctx, *evt.Mark); err != nil {
				s.logger.Warn("mark handling failed", "error", err)
			}
		case EventStop:
			if evt.Stop == nil {
				continue
			}
			streamSID := evt.Stop.StreamSID
			if streamSID == "" && activeStreamSID != "" {
				streamSID = activeStreamSID
			}
			s.manager.Close(ctx, streamSID)
			if streamSID == activeStreamSID {
				activeStreamSID = ""
			}
			return
		default:
		}
	}
}

func (s *Server) handleAsteriskWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	ctx := r.Context()
	var activeStreamSID string
	var closeOnce sync.Once
	closeSession := func() {
		closeOnce.Do(func() {
			if activeStreamSID != "" {
				s.manager.Close(ctx, activeStreamSID)
				activeStreamSID = ""
			}
			_ = conn.Close()
		})
	}
	defer closeSession()

	conn.SetReadLimit(4 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(defaultReadTimeout))

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				s.logger.Debug("asterisk websocket read ended", "error", err)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(defaultReadTimeout))

		switch msgType {
		case websocket.TextMessage:
			ctrl, err := ParseAsteriskControl(data)
			if err != nil {
				s.logger.Warn("failed to parse asterisk control", "error", err)
				continue
			}
			switch ctrl.Type {
			case AsteriskMsgSessionStart:
				if ctrl.Start == nil {
					continue
				}
				start := AsteriskStartToStartEvent(*ctrl.Start)
				session, err := s.manager.Create(ctx, start, conn)
				if err != nil {
					s.rejectAsteriskSession(conn, start.StreamSID, err)
					return
				}
				activeStreamSID = session.StreamSID
				if ready, err := AsteriskReadyMessage(); err == nil {
					session.EnqueueControl(ready)
				}
			case AsteriskMsgSessionEnd:
				if activeStreamSID != "" {
					s.manager.Close(ctx, activeStreamSID)
					activeStreamSID = ""
				}
				return
			default:
				s.logger.Warn("ignoring unknown asterisk control", "type", ctrl.Type)
			}
		case websocket.BinaryMessage:
			if activeStreamSID == "" {
				s.logger.Warn("binary audio before session_start")
				continue
			}
			if err := s.manager.HandleBinaryMedia(ctx, activeStreamSID, data); err != nil {
				s.logger.Warn("binary media handling failed", "error", err)
			}
		default:
			s.logger.Warn("unsupported websocket message type", "type", msgType)
		}
	}
}

func (s *Server) rejectSession(conn *websocket.Conn, streamSID string, err error) {
	if errors.Is(err, ErrMaxSessionsExceeded) {
		s.logger.Warn("rejecting stream: max concurrent sessions exceeded",
			"stream_sid", streamSID,
			"max", s.cfg.MaxConcurrentSessions,
		)
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "max sessions exceeded"),
			time.Now().Add(defaultWriteTimeout),
		)
	} else {
		s.logger.Warn("failed to create session", "error", err)
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "session rejected"),
			time.Now().Add(defaultWriteTimeout),
		)
	}
}

func (s *Server) rejectAsteriskSession(conn *websocket.Conn, streamSID string, err error) {
	if errors.Is(err, ErrMaxSessionsExceeded) {
		s.logger.Warn("rejecting asterisk stream: max concurrent sessions exceeded",
			"stream_sid", streamSID,
			"max", s.cfg.MaxConcurrentSessions,
		)
	} else {
		s.logger.Warn("failed to create asterisk session", "error", err)
	}
	if payload, encErr := AsteriskErrorMessage("session rejected", "session_rejected"); encErr == nil {
		_ = conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
		_ = conn.WriteMessage(websocket.TextMessage, payload)
	}
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "session rejected"),
		time.Now().Add(defaultWriteTimeout),
	)
}
