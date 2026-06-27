package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	"websocket/internal/brain"
	"websocket/internal/media"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := media.DefaultConfig()
	denoiseCfg := media.DenoiseConfigFromEnv()
	asrCfg := media.ASRConfigFromEnv()
	amdCfg := media.AMDConfigFromEnv()
	endpointCfg := media.EndpointConfigFromEnv()
	semanticCfg := media.SemanticTurnConfigFromEnv()
	backchannelCfg := media.BackchannelConfigFromEnv()
	brainCfg := brain.ConfigFromEnv()
	ttsCfg := media.TTSConfigFromEnv()
	localVADEnabled, localVADSilero := media.LocalVADConfigFromEnv()

	if addr := os.Getenv("LISTEN_ADDR"); addr != "" {
		cfg.ListenAddr = addr
	}
	if rate := os.Getenv("TARGET_SAMPLE_RATE"); rate != "" {
		if v, err := strconv.Atoi(rate); err == nil {
			cfg.TargetSampleRate = v
		}
	}
	if ms := os.Getenv("FRAME_DURATION_MS"); ms != "" {
		if v, err := strconv.Atoi(ms); err == nil {
			cfg.FrameDurationMs = v
		}
	}

	denoiser, err := media.NewDenoiser(denoiseCfg)
	if err != nil {
		logger.Error("denoiser init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := denoiser.Close(); err != nil {
			logger.Warn("denoiser close failed", "error", err)
		}
	}()

	asrProvider, err := media.NewASRProvider(asrCfg)
	if err != nil {
		logger.Error("asr provider init failed", "error", err)
		os.Exit(1)
	}

	amdClassifier, err := media.NewAMDClassifier(amdCfg)
	if err != nil {
		logger.Error("amd classifier init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := amdClassifier.Close(); err != nil {
			logger.Warn("amd classifier close failed", "error", err)
		}
	}()

	localVAD := media.NewLocalVAD(localVADEnabled, localVADSilero)
	turnListener := media.NewLoggingTurnListener(logger)

	semanticTurn, err := media.NewSemanticTurnDetector(semanticCfg)
	if err != nil {
		logger.Error("semantic turn detector init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := semanticTurn.Close(); err != nil {
			logger.Warn("semantic turn detector close failed", "error", err)
		}
	}()

	backchannel, err := media.NewBackchannelClassifier(backchannelCfg)
	if err != nil {
		logger.Error("backchannel classifier init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := backchannel.Close(); err != nil {
			logger.Warn("backchannel classifier close failed", "error", err)
		}
	}()

	ttsProvider, err := media.NewTTSProvider(ttsCfg)
	if err != nil {
		logger.Error("tts provider init failed", "error", err)
		os.Exit(1)
	}

	target := cfg.TargetFormat()
	amdListener := media.NewLoggingAMDListener(logger)

	sinkFactory := func() media.AudioSink {
		turnManager := media.NewTurnManager(
			turnListener,
			endpointCfg,
			media.RealClock{},
			localVAD,
			semanticTurn,
			semanticCfg,
			backchannel,
			logger,
		)

		var replyConsumer media.ReplyConsumer = media.NewLoggingReplyConsumer(logger)
		var ttsConsumer *media.TTSReplyConsumer

		if ttsCfg.Enabled {
			stream, err := ttsProvider.Open(context.Background(), media.TTSSessionMeta{})
			if err != nil {
				logger.Warn("tts stream open failed; using logging reply consumer", "error", err)
			} else {
				ttsConsumer = media.NewTTSReplyConsumer(stream, media.NewLoggingEgress(logger), turnManager, nil, logger)
				replyConsumer = ttsConsumer
			}
		}

		var brainClient *brain.Client
		if brainCfg.Enabled {
			brainClient = brain.NewClient(brainCfg, replyConsumer, turnManager, logger)
			turnManager.SetListener(brainClient)
		}

		pipeline := media.NewTranscodeSink(
			media.NewDenoiseSink(
				media.NewAMDGateSink(
					media.NewASRSink(
						asrProvider,
						turnManager,
						target.SampleRate,
						logger,
					),
					amdClassifier,
					amdListener,
					target.SampleRate,
					amdCfg,
					logger,
				),
				denoiser,
				media.NoopAEC{},
				target.SampleRate,
				logger,
			),
			target,
			cfg.FrameDurationMs,
			logger,
		)

		if brainClient != nil || ttsConsumer != nil {
			return &brain.BootstrapSink{Inner: pipeline, Brain: brainClient, TTSReply: ttsConsumer}
		}
		return pipeline
	}

	logger.Info("audio pipeline ready",
		"target_sample_rate", target.SampleRate,
		"frame_duration_ms", cfg.FrameDurationMs,
		"denoise_enabled", denoiseCfg.Enabled,
		"amd_enabled", amdCfg.Enabled,
		"asr_enabled", asrCfg.Enabled,
		"local_vad_enabled", localVADEnabled,
		"semantic_turn_enabled", semanticCfg.Enabled,
		"backchannel_enabled", backchannelCfg.Enabled,
		"brain_ws_enabled", brainCfg.Enabled,
		"tts_enabled", ttsCfg.Enabled,
	)

	srv := media.NewServer(cfg, logger, sinkFactory)
	if err := srv.Run(context.Background()); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
