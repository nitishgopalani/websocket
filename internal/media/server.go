package media

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
func NewServer(cfg Config, logger *slog.Logger, newSink func() AudioSink, metrics *Metrics) *Server {
	cfg = cfg.withDefaults()
	if logger == nil {
		logger = slog.Default()
	}
	manager := NewSessionManager(cfg, logger, newSink, metrics)
	s := &Server{
		cfg:     cfg,
		logger:  logger,
		manager: manager,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	if metrics != nil && metrics.Enabled() {
		mux.Handle("GET /metrics", metrics.Handler())
	}
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
	if s.cfg.CarrierProfile().BinaryIngress {
		s.handleAsteriskWebSocket(w, r)
		return
	}
	s.handleExotelWebSocket(w, r)
}

// ServeHTTP allows tests to mount the server handler directly.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpSrv.Handler.ServeHTTP(w, r)
}

// ListenAndServe is a convenience wrapper around Run with signal handling.
func ListenAndServe(cfg Config, logger *slog.Logger) error {
	srv := NewServer(cfg, logger, nil, nil)
	return srv.Run(context.Background())
}

// FormatListenError helps main packages report bind failures clearly.
func FormatListenError(addr string, err error) error {
	return fmt.Errorf("listen on %s: %w", addr, err)
}
