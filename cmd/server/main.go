package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	"websocket/internal/media"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := media.DefaultConfig()
	denoiseCfg := media.DenoiseConfigFromEnv()
	asrCfg := media.ASRConfigFromEnv()
	amdCfg := media.AMDConfigFromEnv()

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

	target := cfg.TargetFormat()
	transcriptConsumer := media.NewLoggingTranscriptConsumer(logger)
	amdListener := media.NewLoggingAMDListener(logger)

	sinkFactory := func() media.AudioSink {
		return media.NewTranscodeSink(
			media.NewDenoiseSink(
				media.NewAMDGateSink(
					media.NewASRSink(
						asrProvider,
						transcriptConsumer,
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
	}

	logger.Info("audio pipeline ready",
		"target_sample_rate", target.SampleRate,
		"frame_duration_ms", cfg.FrameDurationMs,
		"denoise_enabled", denoiseCfg.Enabled,
		"amd_enabled", amdCfg.Enabled,
		"asr_enabled", asrCfg.Enabled,
	)

	srv := media.NewServer(cfg, logger, sinkFactory)
	if err := srv.Run(context.Background()); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
