package callbridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"websocket/internal/media"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server is the callbridge media WebSocket server.
type Server struct {
	cfg      Config
	logger   *slog.Logger
	metrics  *Metrics
	registry *registry
	httpSrv  *http.Server
	now      func() time.Time
}

// NewServer constructs a callbridge server.
func NewServer(cfg Config, logger *slog.Logger, metrics *Metrics) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = NewMetrics(false)
	}
	s := &Server{
		cfg:      cfg,
		logger:   logger,
		metrics:  metrics,
		registry: newRegistry(),
		now:      time.Now,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if metrics.enabled {
		mux.Handle("GET /metrics", metrics.Handler())
	}
	path := strings.TrimRight(cfg.WSPath, "/")
	if path == "" {
		path = "/bridge"
	}
	mux.HandleFunc("GET "+path, s.handleBridge)
	s.httpSrv = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// HTTPServer exposes the underlying HTTP server for tests.
func (s *Server) HTTPServer() *http.Server {
	return s.httpSrv
}

// Run starts the server until ctx or signal cancellation.
func (s *Server) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("callbridge listening",
			"addr", s.cfg.ListenAddr,
			"path", s.cfg.WSPath,
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
		return s.httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleBridge(w http.ResponseWriter, r *http.Request) {
	sessionID := sessionIDFromRequest(r)
	if sessionID == "" {
		http.Error(w, "missing session id", http.StatusBadRequest)
		return
	}
	if !VerifyMediaSignature(s.cfg.MediaSecret, sessionID, signatureFromRequest(r), s.now(), s.cfg.AuthMaxSkew) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("bridge upgrade failed", "error", err)
		return
	}

	pair := pairingFromQuery(r.URL.Query())
	if pair.bridgeID == "" {
		s.rejectConn(conn, "missing_bridge_id", "bridge query parameter required")
		return
	}

	if err := s.handshake(conn, sessionID, pair); err != nil {
		s.logger.Info("bridge handshake failed", "session_id", sessionID, "error", err)
		_ = conn.Close()
		return
	}
}

func (s *Server) handshake(conn *websocket.Conn, headerSessionID string, pair pairingInfo) error {
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read session_start: %w", err)
	}
	ctrl, err := s.parseControl(data)
	if err != nil {
		return err
	}
	if ctrl.Type != media.AsteriskMsgSessionStart || ctrl.Start == nil {
		return fmt.Errorf("expected session_start, got %s", ctrl.Type)
	}
	start := ctrl.Start
	sessionID := strings.TrimSpace(start.SessionID)
	if sessionID == "" {
		sessionID = headerSessionID
	}
	pair = pairingFromMetadata(start.Metadata, pair)
	if pair.bridgeID == "" {
		return errMissingBridgeID
	}

	inRate := start.Audio.InputSampleRate
	outRate := start.Audio.OutputSampleRate
	if inRate == 0 {
		inRate = 16000
	}
	if outRate == 0 {
		outRate = inRate
	}
	rates := audioRates{inputRate: inRate, outputRate: outRate}

	room := s.registry.getOrCreate(pair.bridgeID, s)
	sess := newSession(s, conn, sessionID)
	sess.room = room

	if err := room.attach(sess, pair.leg, rates); err != nil {
		s.rejectConn(conn, "pairing_failed", err.Error())
		if errors.Is(err, errRateMismatch) {
			room.detach(sess, "error")
		}
		return err
	}

	_ = conn.SetReadDeadline(time.Time{})
	go sess.readLoop(s.logger)
	return nil
}

func (s *Server) rejectConn(conn *websocket.Conn, code, message string) {
	msg, err := s.errorMessage(message, code)
	if err == nil {
		_ = conn.WriteMessage(websocket.TextMessage, msg)
	}
	_ = conn.Close()
}

func (s *Server) parseControl(data []byte) (media.AsteriskControl, error) {
	return media.ParseAsteriskControl(data)
}

func (s *Server) readyMessage() ([]byte, error) {
	return media.AsteriskReadyMessage()
}

func (s *Server) endOfCallMessage() ([]byte, error) {
	return media.AsteriskEndOfCallMessage()
}

func (s *Server) errorMessage(message, code string) ([]byte, error) {
	return media.AsteriskErrorMessage(message, code)
}

func (s *Server) msgSessionEnd() string {
	return media.AsteriskMsgSessionEnd
}

// Registry exposes the room registry for tests.
func (s *Server) Registry() *registry {
	return s.registry
}

// SetClock overrides time source in tests.
func (s *Server) SetClock(fn func() time.Time) {
	if fn != nil {
		s.now = fn
	}
}
