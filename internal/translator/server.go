package translator

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

	"websocket/internal/mayura"
	"websocket/internal/media"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server is the translator media WebSocket server.
type Server struct {
	cfg      Config
	logger   *slog.Logger
	metrics  *Metrics
	registry *registry
	httpSrv  *http.Server
	now      func() time.Time

	deps LaneDeps
}

// NewServer constructs a translator server.
func NewServer(cfg Config, logger *slog.Logger, metrics *Metrics, deps LaneDeps) *Server {
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
		deps:     deps,
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
		path = defaultWSPath
	}
	mux.HandleFunc("GET "+path, s.handleTranslate)
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

// Run starts the server until ctx or signal cancellation.
func (s *Server) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("translator listening",
			"addr", s.cfg.ListenAddr,
			"path", s.cfg.WSPath,
			"tls", s.cfg.useTLS(),
			"translation_enabled", s.cfg.TranslationEnabled,
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

func (s *Server) handleTranslate(w http.ResponseWriter, r *http.Request) {
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
		s.logger.Error("translator upgrade failed", "error", err)
		return
	}

	pair := pairingFromQuery(r.URL.Query())
	if pair.bridgeID == "" {
		s.rejectConn(conn, "missing_bridge_id", "bridge query parameter required")
		return
	}
	if pair.langA == "" {
		pair.langA = "hi-IN"
	}
	if pair.langB == "" {
		pair.langB = "en-IN"
	}

	if err := s.handshake(conn, sessionID, pair); err != nil {
		s.logger.Info("translator handshake failed", "session_id", sessionID, "error", err)
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
	if pair.langA == "" {
		pair.langA = "hi-IN"
	}
	if pair.langB == "" {
		pair.langB = "en-IN"
	}

	inRate := start.Audio.InputSampleRate
	outRate := start.Audio.OutputSampleRate
	if inRate == 0 {
		inRate = s.cfg.TargetSampleRate
	}
	if outRate == 0 {
		outRate = inRate
	}
	rates := audioRates{inputRate: inRate, outputRate: outRate}

	room := s.registry.getOrCreate(pair.bridgeID, s, pair.langA, pair.langB)
	sess := newLeg(s, conn, sessionID)
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

func (s *Server) onRoomPaired(room *room, legA, legB *leg, langA, langB string) error {
	if !s.cfg.TranslationEnabled {
		return nil
	}
	if s.deps.ASRProvider == nil || s.deps.TTSProvider == nil {
		return fmt.Errorf("translation dependencies not configured")
	}
	if s.cfg.LaneMode != LaneModeEcho && s.deps.Mayura == nil {
		return fmt.Errorf("mayura not configured for translate mode")
	}

	var coord *roomCoordinator
	var breaker *loopBreaker
	if s.cfg.Bidirectional {
		coord = newRoomCoordinator(s.cfg, s.logger)
		breaker = newLoopBreaker(s.cfg.LoopBreakerMax, s.cfg.LoopBreakerWindow, s.logger, func() {
			if coord != nil {
				coord.Disable()
			}
		})
		room.coord = coord
		room.breaker = breaker
	}

	laneHooks := func(sourceRole LegRole) LaneHooks {
		return LaneHooks{
			OnSpeechStart: func(src LegRole) {
				if coord != nil {
					coord.OnSpeechStart(src)
				}
			},
			OnTTSStarted: func(src LegRole) {
				if coord != nil {
					coord.OnTTSStarted(src)
				}
			},
			OnPlaybackComplete: func(target LegRole, playbackEnd time.Time) {
				if coord != nil {
					coord.OnPlaybackComplete(target, playbackEnd)
				}
			},
			OnTranslationDone: func(src LegRole, sourceText, translatedText string) {
				if breaker != nil {
					breaker.Record(src, sourceText, translatedText)
				}
			},
		}
	}

	hooksAToB := laneHooks(LegA)
	laneAToB, err := newAToBLane(s.deps, legA, legB, langA, langB, hooksPtr(hooksAToB, s.cfg.Bidirectional))
	if err != nil {
		return err
	}

	var laneBToA *translationLane
	if s.cfg.Bidirectional {
		ttsCfgB := s.deps.TTSConfig
		if s.cfg.TTSLanguageB != "" {
			ttsCfgB.Language = s.cfg.TTSLanguageB
		}
		if s.cfg.TTSVoiceIDB != "" {
			ttsCfgB.VoiceID = s.cfg.TTSVoiceIDB
		}
		hooksBToA := laneHooks(LegB)
		laneBToA, err = newBToALane(s.deps, legB, legA, langB, langA, ttsCfgB, hooksBToA)
		if err != nil {
			_ = laneAToB.Close()
			return err
		}
	}

	room.mu.Lock()
	room.lane = laneAToB
	room.laneBToA = laneBToA
	room.mu.Unlock()

	s.logger.Info("translator_lane_ready",
		"bridge_id", room.id,
		"lang_a", langA,
		"lang_b", langB,
		"mode", s.cfg.LaneMode,
		"bidirectional", s.cfg.Bidirectional,
		"asr_sample_rate", s.cfg.ASRSampleRate,
		"tts_provider", s.deps.TTSConfig.Provider,
	)
	return nil
}

func hooksPtr(h LaneHooks, bidirectional bool) *LaneHooks {
	if !bidirectional {
		return nil
	}
	cp := h
	return &cp
}

func (s *Server) teardownLane(room *room) {
	room.mu.Lock()
	laneA := room.lane
	laneB := room.laneBToA
	room.lane = nil
	room.laneBToA = nil
	room.coord = nil
	room.breaker = nil
	room.mu.Unlock()
	if laneA != nil {
		_ = laneA.Close()
	}
	if laneB != nil {
		_ = laneB.Close()
	}
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

// BuildDepsFromEnv wires ASR/TTS/Mayura from environment variables.
func BuildDepsFromEnv(logger *slog.Logger) (LaneDeps, error) {
	cfg := ConfigFromEnv()
	asrCfg := media.ASRConfigFromEnv()
	asrCfg.Enabled = true

	ttsCfg := media.TTSConfigFromEnv()
	ttsCfg.Enabled = true
	if cfg.LaneMode == LaneModeEcho {
		if strings.TrimSpace(ttsCfg.Provider) == "" {
			ttsCfg.Provider = "elevenlabs"
		}
		if strings.TrimSpace(ttsCfg.Language) == "" {
			ttsCfg.Language = "hi"
		}
	} else {
		if strings.TrimSpace(ttsCfg.Provider) == "" {
			ttsCfg.Provider = "sarvam"
		}
		if strings.TrimSpace(ttsCfg.Language) == "" {
			ttsCfg.Language = "en-IN"
		}
	}

	asrProvider, err := media.NewASRProvider(asrCfg)
	if err != nil {
		return LaneDeps{}, err
	}
	ttsProvider, err := media.NewTTSProvider(ttsCfg)
	if err != nil {
		return LaneDeps{}, err
	}

	var mayuraClient MayuraTranslator
	if cfg.LaneMode != LaneModeEcho {
		mc, err := mayura.NewClient(mayura.ConfigFromEnv())
		if err != nil {
			return LaneDeps{}, err
		}
		mayuraClient = mc
	}

	return LaneDeps{
		ASRProvider:       asrProvider,
		TTSProvider:       ttsProvider,
		TTSConfig:         ttsCfg,
		Mayura:            mayuraClient,
		Logger:            logger,
		EndpointSilenceMs: cfg.EndpointSilenceMs,
		LaneMode:          cfg.LaneMode,
		ASRSampleRate:     cfg.ASRSampleRate,
		TTSSynthRate:      cfg.TTSSynthRate,
	}, nil
}
