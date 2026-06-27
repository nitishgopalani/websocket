package main_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"websocket/internal/media/sim"
)

func TestReplayCLIAgainstInProcessServer(t *testing.T) {
	h, err := sim.NewSmokeHarness(sim.SmokeHarnessConfig{SilenceMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	bin := filepath.Join(t.TempDir(), "replay.exe")
	repoRoot := mustRepoRoot(t)
	buildCmd := exec.Command("go", "build", "-o", bin, "websocket/cmd/replay")
	buildCmd.Dir = repoRoot
	out, err := buildCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build replay: %v\n%s", err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin,
		"-addr", h.WSURL,
		"-pace", "fast",
		"-stream-sid", "MZ-CLI",
		"-call-sid", "CA-CLI",
	)
	cmd.Dir = repoRoot
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("replay run: %v\n%s", err, out)
	}
	text := string(out)
	if !strings.Contains(text, "outbound audio") {
		t.Fatalf("unexpected output: %s", text)
	}
}

func mustRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// cmd/replay -> repo root is ../..
	return filepath.Join(wd, "..", "..")
}
