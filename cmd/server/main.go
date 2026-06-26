package main

import (
	"log/slog"
	"os"

	"websocket/internal/media"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := media.DefaultConfig()
	if addr := os.Getenv("LISTEN_ADDR"); addr != "" {
		cfg.ListenAddr = addr
	}
	if err := media.ListenAndServe(cfg, logger); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
