package main

import (
	"context"
	"log/slog"
	"os"

	"websocket/internal/callbridge"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := callbridge.ConfigFromEnv()
	if len(cfg.MediaSecret) < 16 {
		logger.Error("FONADA_MEDIA_SECRET required (>=16 chars)")
		os.Exit(1)
	}
	metrics := callbridge.NewMetrics(cfg.MetricsEnabled)
	srv := callbridge.NewServer(cfg, logger, metrics)
	if err := srv.Run(context.Background()); err != nil {
		logger.Error("callbridge exited", "error", err)
		os.Exit(1)
	}
}
