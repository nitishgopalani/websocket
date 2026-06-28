package sim

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"websocket/internal/brain"
	"websocket/internal/media"
)

const defaultSmokeTimeout = 15 * time.Second

// SmokeHarness wires an in-process media server with fake providers for CI smoke tests.
type SmokeHarness struct {
	Server   *media.Server
	HTTPSrv  *httptest.Server
	WSURL    string
	Clock    *media.FakeClock
	Turns    *RecordingTurnListener
	Metrics  *media.Metrics
	BrainURL string
	cleanup  []func()
}

// SmokeHarnessConfig configures the smoke test server.
type SmokeHarnessConfig struct {
	ASR            FakeASRConfig
	Brain          FakeBrainConfig
	TTS            FakeTTSConfig
	SilenceMs      int
	MetricsEnabled bool
}

// NewSmokeHarness starts an httptest server with fake ASR/brain/TTS and burst egress.
func NewSmokeHarness(cfg SmokeHarnessConfig) (*SmokeHarness, error) {
	brainURL, brainCleanup := StartFakeBrainServer(noopTB{}, cfg.Brain)
	start := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	clock := media.NewFakeClock(start)
	turns := NewRecordingTurnListener(nil)

	endpointCfg := media.DefaultEndpointConfig()
	if cfg.SilenceMs > 0 {
		endpointCfg.DefaultSilenceMs = cfg.SilenceMs
		endpointCfg.SilenceMs = map[media.FlowClass]int{
			media.FlowYesNo:        cfg.SilenceMs,
			media.FlowDefault:      cfg.SilenceMs,
			media.FlowSpelledInput: cfg.SilenceMs,
		}
	} else {
		endpointCfg.DefaultSilenceMs = 50
		endpointCfg.SilenceMs = map[media.FlowClass]int{
			media.FlowYesNo:        50,
			media.FlowDefault:      50,
			media.FlowSpelledInput: 50,
		}
	}

	metrics := media.NewMetrics(media.MetricsConfig{Enabled: cfg.MetricsEnabled})
	asrProvider := &FakeASRProvider{cfg: cfg.ASR}
	ttsProvider := &FakeTTSProvider{cfg: cfg.TTS}

	egressCfg := media.DefaultEgressConfig()
	egressCfg.Pacing = "burst"

	sessionClock := media.RealClock{}
	var sharedClock media.Clock = sessionClock
	sinkFactory := func() media.AudioSink {
		sessionClock := sharedClock
		turnManager := media.NewTurnManager(
			turns,
			endpointCfg,
			sessionClock,
			media.NoopVAD{},
			nil,
			media.SemanticTurnConfig{},
			nil,
			nil,
		)

		ttsStream, _ := ttsProvider.Open(context.Background(), media.TTSSessionMeta{})
		carrierEgress := media.NewCarrierEgress(egressCfg, 20, sessionClock, nil, media.DefaultCarrierProfile(), nil)
		ttsConsumer := media.NewTTSReplyConsumer(ttsStream, carrierEgress, turnManager, nil, nil)

		brainCfg := brain.Config{Enabled: true, URL: brainURL}
		brainClient := brain.NewClient(brainCfg, ttsConsumer, turnManager, nil)
		turns.SetInner(brainClient)
		turnManager.SetListener(turns)

		obs := &media.SessionObservability{Metrics: metrics}
		obs.Timing = media.NewTurnTimingHub("", sessionClock, nil, metrics, media.DefaultLatencyBudget())
		ttsConsumer.SetObservability(obs.Timing, nil)
		turnManager.SetObservability(obs.Timing, nil)
		carrierEgress.SetObservability(obs.Timing, nil)
		brainClient.SetObservability(obs.Timing, nil)

		pipeline := media.NewTranscodeSink(
			media.NewASRSink(
				asrProvider,
				turnManager,
				8000,
				nil,
			),
			media.DefaultConfig().TargetFormat(),
			20,
			nil,
		)

		return &brain.BootstrapSink{
			Inner:         pipeline,
			Brain:         brainClient,
			TTSReply:      ttsConsumer,
			CarrierEgress: carrierEgress,
			Observability: obs,
		}
	}

	mediaCfg := media.DefaultConfig()
	srv := media.NewServer(mediaCfg, nil, sinkFactory, metrics)
	ts := httptest.NewServer(srv.HTTPServer().Handler)
	cleanup := []func(){brainCleanup, ts.Close}

	return &SmokeHarness{
		Server:   srv,
		HTTPSrv:  ts,
		WSURL:    "ws" + strings.TrimPrefix(ts.URL, "http") + mediaCfg.WSPath,
		Clock:    clock,
		Turns:    turns,
		Metrics:  metrics,
		BrainURL: brainURL,
		cleanup:  cleanup,
	}, nil
}

// Close releases harness resources.
func (h *SmokeHarness) Close() {
	for _, fn := range h.cleanup {
		fn()
	}
}

// RunSmoke executes a fast carrier simulation with real-time egress pacing.
func (h *SmokeHarness) RunSmoke(ctx context.Context, source FrameSource, frameCount int) (*RunResult, error) {
	if source == nil {
		source = NewSilenceGenerator(defaultMuLawFrameBytes, frameCount)
	}
	sim := NewCarrierSimulator(RunConfig{
		WSURL:           h.WSURL,
		StreamSID:       "MZ-SMOKE",
		CallSID:         "CA-SMOKE",
		Source:          source,
		Pace:            PaceFast,
		FrameDurationMs: 20,
		RunTimeout:      defaultSmokeTimeout,
		Clock:           media.RealClock{},
	})
	return sim.Run(ctx)
}

// noopTB satisfies testingTB for harness construction outside tests.
type noopTB struct{}

func (noopTB) Helper() {}

// PumpClockUntil advances the fake clock until cond is true or timeout.
func PumpClockUntil(clock *media.FakeClock, timeout time.Duration, step time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		clock.Advance(step)
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

var _ media.TranscriptConsumer = (*TranscriptTap)(nil)

// BuildSmokePipelineSink returns a sink factory body for custom smoke wiring (exported for live eval).
func BuildSmokePipelineSink(
	brainURL string,
	clock media.Clock,
	turns media.TurnListener,
	asr media.ASRProvider,
	tts media.TTSProvider,
	metrics *media.Metrics,
) media.AudioSink {
	endpointCfg := media.DefaultEndpointConfig()
	endpointCfg.DefaultSilenceMs = 50

	turnManager := media.NewTurnManager(
		turns,
		endpointCfg,
		clock,
		media.NoopVAD{},
		nil,
		media.SemanticTurnConfig{},
		nil,
		nil,
	)

	egressCfg := media.DefaultEgressConfig()
	egressCfg.Pacing = "burst"

	ttsStream, _ := tts.Open(context.Background(), media.TTSSessionMeta{})
	carrierEgress := media.NewCarrierEgress(egressCfg, 20, clock, nil, media.DefaultCarrierProfile(), nil)
	ttsConsumer := media.NewTTSReplyConsumer(ttsStream, carrierEgress, turnManager, nil, nil)

	brainCfg := brain.Config{Enabled: true, URL: brainURL}
	brainClient := brain.NewClient(brainCfg, ttsConsumer, turnManager, nil)
	turnManager.SetListener(brainClient)

	if metrics != nil {
		obs := &media.SessionObservability{Metrics: metrics}
		obs.Timing = media.NewTurnTimingHub("", clock, nil, metrics, media.DefaultLatencyBudget())
		ttsConsumer.SetObservability(obs.Timing, nil)
		turnManager.SetObservability(obs.Timing, nil)
		carrierEgress.SetObservability(obs.Timing, nil)
		brainClient.SetObservability(obs.Timing, nil)
	}

	pipeline := media.NewTranscodeSink(
		media.NewASRSink(asr, turnManager, 8000, nil),
		media.DefaultConfig().TargetFormat(),
		20,
		nil,
	)

	return &brain.BootstrapSink{
		Inner:         pipeline,
		Brain:         brainClient,
		TTSReply:      ttsConsumer,
		CarrierEgress: carrierEgress,
	}
}

// ClockPump runs clock advancement in a background goroutine until stop is closed.
func ClockPump(clock *media.FakeClock, step time.Duration, stop <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clock.Advance(step)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
}
