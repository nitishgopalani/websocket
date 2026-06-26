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

	target := cfg.TargetFormat()
	sinkFactory := func() media.AudioSink {
		return media.NewTranscodeSink(
			media.NewLoggingSink(logger),
			target,
			cfg.FrameDurationMs,
			logger,
		)
	}

	srv := media.NewServer(cfg, logger, sinkFactory)
	if err := srv.Run(context.Background()); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
