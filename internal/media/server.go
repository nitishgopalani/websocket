package media

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server serves the Exotel-style bidirectional media websocket and health endpoint.
type Server struct {
	cfg     Config
	logger  *slog.Logger
	manager *SessionManager
	httpSrv *http.Server
}

// NewServer constructs a media ingress server.
func NewServer(cfg Config, logger *slog.Logger, newSink func() AudioSink) *Server {
	cfg = cfg.withDefaults()
	if logger == nil {
		logger = slog.Default()
	}
	manager := NewSessionManager(cfg, logger, newSink)
	s := &Server{
		cfg:     cfg,
		logger:  logger,
		manager: manager,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET "+cfg.WSPath, s.handleWebSocket)

	s.httpSrv = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Manager exposes the session manager for tests and downstream wiring.
func (s *Server) Manager() *SessionManager {
	return s.manager
}

// HTTPServer returns the underlying HTTP server (for httptest).
func (s *Server) HTTPServer() *http.Server {
	return s.httpSrv
}

// Run starts the server and blocks until SIGINT/SIGTERM or ctx cancellation.
func (s *Server) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("media server listening",
			"addr", s.cfg.ListenAddr,
			"ws_path", s.cfg.WSPath,
			"tls", s.cfg.useTLS(),
		)
		var err error
		if s.cfg.useTLS() {
			err = s.httpSrv.ListenAndServeTLS(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
		} else {
			err = s.httpSrv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-sigCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.logger.Info("shutting down media server")
		s.manager.CloseAll(shutdownCtx)
		return s.httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
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
				if errors.Is(err, ErrMaxSessionsExceeded) {
					s.logger.Warn("rejecting stream: max concurrent sessions exceeded",
						"stream_sid", evt.Start.StreamSID,
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
			// Unknown events are logged in ParseInboundEvent and ignored here.
		}
	}
}

// ServeHTTP allows tests to mount the server handler directly.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpSrv.Handler.ServeHTTP(w, r)
}

// ListenAndServe is a convenience wrapper around Run with signal handling.
func ListenAndServe(cfg Config, logger *slog.Logger) error {
	srv := NewServer(cfg, logger, nil)
	return srv.Run(context.Background())
}

// FormatListenError helps main packages report bind failures clearly.
func FormatListenError(addr string, err error) error {
	return fmt.Errorf("listen on %s: %w", addr, err)
}
