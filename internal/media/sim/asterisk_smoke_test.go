package sim

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"websocket/internal/brain"
	"websocket/internal/media"
)

func TestAsteriskProtocolSmoke(t *testing.T) {
	brainURL, brainCleanup := StartFakeBrainServer(t, FakeBrainConfig{})
	defer brainCleanup()

	carrierCfg := media.CarrierConfig{Variant: media.CarrierAsterisk}
	carrierProfile := carrierCfg.Profile()
	serializer := media.NewCarrierSerializer(carrierCfg)

	endpointCfg := media.DefaultEndpointConfig()
	endpointCfg.DefaultSilenceMs = 50
	endpointCfg.SilenceMs = map[media.FlowClass]int{
		media.FlowYesNo:        50,
		media.FlowDefault:      50,
		media.FlowSpelledInput: 50,
	}

	asrProvider := &FakeASRProvider{cfg: FakeASRConfig{
		FinalText:         "payment due kitna hai",
		FramesBeforeFinal: 3,
	}}
	ttsProvider := &FakeTTSProvider{cfg: FakeTTSConfig{
		FramesPerSpeak: 4,
		FrameBytes:     640, // 20ms @ 16kHz PCM16
	}}

	sinkFactory := func() media.AudioSink {
		clock := media.RealClock{}
		turns := NewRecordingTurnListener(nil)
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

		carrierEgress := media.NewCarrierEgress(egressCfg, 20, clock, serializer, carrierProfile, nil)
		ttsConsumer := media.NewTTSReplyConsumer(nil, carrierEgress, turnManager, nil, nil)

		brainCfg := brain.Config{Enabled: true, URL: brainURL}
		brainClient := brain.NewClient(brainCfg, ttsConsumer, turnManager, nil)
		turnManager.SetListener(brainClient)

		target := media.TargetFormat{SampleRate: 16000, Channels: 1}
		pipeline := media.NewTranscodeSink(
			media.NewASRSink(asrProvider, turnManager, target.SampleRate, nil),
			target,
			20,
			nil,
		)

		return &brain.BootstrapSink{
			Inner:         pipeline,
			Brain:         brainClient,
			TTSReply:      ttsConsumer,
			TTSProvider:   ttsProvider,
			TTSBaseCfg:    media.DefaultTTSConfig(),
			CarrierEgress: carrierEgress,
		}
	}

	mediaCfg := media.DefaultConfig()
	mediaCfg.Carrier = carrierCfg
	mediaCfg.TargetSampleRate = 16000

	srv := media.NewServer(mediaCfg, nil, sinkFactory, nil)
	ts := httptest.NewServer(srv.HTTPServer().Handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + mediaCfg.WSPath
	pcm := PCM16Silence(16000)

	astSim := NewAsteriskSimulator(AsteriskRunConfig{
		WSURL:            wsURL,
		SessionID:        "AST-SMOKE",
		PCM16:            pcm,
		ChunkBytes:       3200,
		Pace:             PaceFast,
		RunTimeout:       10 * time.Second,
		Language:         "en",
		OutputSampleRate: 16000,
		AgentID:          "agent-smoke",
	})
	result, err := astSim.Run(context.Background())
	if err != nil {
		t.Fatalf("asterisk sim: %v", err)
	}
	if !result.ReadyReceived {
		t.Fatal("expected ready control frame")
	}
	if result.OutboundBinaryBytes == 0 {
		t.Fatal("expected outbound binary TTS audio")
	}
	if got := asrProvider.LastMeta().Language; got != "hi-IN" {
		t.Fatalf("ASR language = %q, want hi-IN", got)
	}
	ttsMeta := ttsProvider.LastMeta()
	if ttsMeta.OutputSampleRate != 16000 {
		t.Fatalf("TTS output_sample_rate = %d, want 16000", ttsMeta.OutputSampleRate)
	}
	if ttsMeta.OutputFormat != "pcm_16000" {
		t.Fatalf("TTS output_format = %q, want pcm_16000", ttsMeta.OutputFormat)
	}
}
