package media

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type holdingSpeakerRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (h *holdingSpeakerRecorder) SpeakHoldingLine(_ context.Context, _ *Session, turnID, text string) {
	h.mu.Lock()
	h.calls = append(h.calls, turnID+":"+text)
	h.mu.Unlock()
}

func TestTurnTimingDerivedDurations(t *testing.T) {
	start := time.Date(2025, 7, 1, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := NewMetrics(MetricsConfig{Enabled: false})
	hub := NewTurnTimingHub("MZ-T", clock, logger, metrics, LatencyBudget{MouthToEarTargetMs: 1200})

	hub.MarkSessionStart()
	hub.MarkSpeechEnd()
	clock.Advance(50 * time.Millisecond)
	hub.MarkASRFinal()
	clock.Advance(100 * time.Millisecond)
	hub.BeginCallerTurn()
	clock.Advance(10 * time.Millisecond)
	turn := hub.BindEngineTurn("MZ-T-t1", false)
	if turn == nil {
		t.Fatal("expected turn timing")
	}
	clock.Advance(200 * time.Millisecond)
	hub.MarkTurn("MZ-T-t1", StageEngineFirstChunk)
	clock.Advance(150 * time.Millisecond)
	hub.MarkTurn("MZ-T-t1", StageTTSFirstAudio)
	clock.Advance(100 * time.Millisecond)
	hub.MarkTurn("MZ-T-t1", StageEgressFirstFrame)

	hub.CompleteTurn("MZ-T-t1", TurnOutcome{Disposition: "resolved"})

	d := turn.durations()
	if d.ASRMS != 50 {
		t.Fatalf("asr_ms = %d, want 50", d.ASRMS)
	}
	if d.EndpointMS != 100 {
		t.Fatalf("endpoint_ms = %d, want 100", d.EndpointMS)
	}
	if d.EngineMS != 200 {
		t.Fatalf("engine_ms = %d, want 200", d.EngineMS)
	}
	if d.TTSMS != 150 {
		t.Fatalf("tts_ms = %d, want 150", d.TTSMS)
	}
	if d.MouthToEarMS != 460 {
		t.Fatalf("mouth_to_ear_ms = %d, want 460", d.MouthToEarMS)
	}
}

func TestDeadAirWatchdogFiresOnce(t *testing.T) {
	clock := NewFakeClock(time.Now())
	speaker := &holdingSpeakerRecorder{}
	metrics := NewMetrics(MetricsConfig{Enabled: false})
	cfg := DefaultWatchdogConfig()
	cfg.NoAudioMs = 500
	cfg.HoldingLine = "ek minute"
	wd := NewDeadAirWatchdog(cfg, clock, speaker, metrics, nil)
	session := &Session{StreamSID: "MZ-WD"}

	wd.ArmCallerTurn(session, "turn-1")
	clock.Advance(600 * time.Millisecond)

	speaker.mu.Lock()
	n := len(speaker.calls)
	speaker.mu.Unlock()
	if n != 1 {
		t.Fatalf("holding calls = %d, want 1", n)
	}

	wd.ArmCallerTurn(session, "turn-1")
	clock.Advance(600 * time.Millisecond)
	speaker.mu.Lock()
	n = len(speaker.calls)
	speaker.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected fire-once, calls = %d", n)
	}
}

func TestDeadAirWatchdogCancelledOnEgress(t *testing.T) {
	clock := NewFakeClock(time.Now())
	speaker := &holdingSpeakerRecorder{}
	wd := NewDeadAirWatchdog(DefaultWatchdogConfig(), clock, speaker, nil, nil)
	session := &Session{StreamSID: "MZ-WD2"}

	wd.ArmCallerTurn(session, "turn-2")
	wd.OnEgressAudio("turn-2")
	clock.Advance(3 * time.Second)

	speaker.mu.Lock()
	defer speaker.mu.Unlock()
	if len(speaker.calls) != 0 {
		t.Fatalf("expected no holding line, got %v", speaker.calls)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	metrics := NewMetrics(MetricsConfig{Enabled: true})
	metrics.IncTurnsTotal()
	metrics.ObserveMouthToEar(900)

	ts := httptest.NewServer(metrics.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	if !strings.Contains(text, "media_turns_total") {
		t.Fatalf("missing turns counter: %s", text)
	}
	if !strings.Contains(text, "media_mouth_to_ear_ms") {
		t.Fatalf("missing mouth_to_ear histogram: %s", text)
	}
}

func TestMouthToEarBudgetExceeded(t *testing.T) {
	start := time.Date(2025, 7, 1, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	metrics := NewMetrics(MetricsConfig{Enabled: true})
	hub := NewTurnTimingHub("MZ-BUD", clock, slog.New(slog.NewJSONHandler(io.Discard, nil)), metrics, LatencyBudget{MouthToEarTargetMs: 100})

	hub.BeginCallerTurn()
	hub.BindEngineTurn("MZ-BUD-t1", false)
	clock.Advance(250 * time.Millisecond)
	hub.MarkTurn("MZ-BUD-t1", StageEgressFirstFrame)
	hub.CompleteTurn("MZ-BUD-t1", TurnOutcome{})
}

func TestObservabilityDisabledRegression(t *testing.T) {
	tm := NewTurnManager(NewLoggingTurnListener(nil), DefaultEndpointConfig(), NewFakeClock(time.Now()), NoopVAD{}, nil, SemanticTurnConfig{}, NoopBackchannel{}, nil)
	session := &Session{StreamSID: "MZ-REG"}
	ctx := context.Background()
	tm.OnSpeechStart(ctx, session)
	tm.OnFinal(ctx, session, Transcript{Text: "hello", IsFinal: true})
	tm.OnSpeechEnd(ctx, session)
}

func TestServerMetricsRoute(t *testing.T) {
	cfg := DefaultConfig()
	metrics := NewMetrics(MetricsConfig{Enabled: true})
	srv := NewServer(cfg, nil, func() AudioSink { return NewLoggingSink(nil) }, metrics)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
