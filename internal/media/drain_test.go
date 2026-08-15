package media

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testStart(sid string) StartEvent {
	return StartEvent{
		Event:     EventStart,
		StreamSID: sid,
		CallSID:   "CA-" + sid,
		MediaFormat: AudioFormat{
			Encoding:   "audio/x-mulaw",
			SampleRate: 8000,
			Channels:   1,
		},
	}
}

func TestDrainRejectsNewSessionsAndWaitsForInflight(t *testing.T) {
	mgr := NewSessionManager(DefaultConfig(), nil, nil, nil)
	ctx := context.Background()
	session, err := mgr.Create(ctx, testStart("MZ-DRAIN"), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if session == nil {
		t.Fatal("expected session")
	}

	mgr.BeginDrain()
	if !mgr.IsDraining() {
		t.Fatal("expected draining")
	}
	_, err = mgr.Create(ctx, testStart("MZ-NEW"), nil)
	if !errors.Is(err, ErrDraining) {
		t.Fatalf("expected ErrDraining, got %v", err)
	}

	done := make(chan error, 1)
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	go func() { done <- mgr.WaitIdle(waitCtx) }()

	time.Sleep(30 * time.Millisecond)
	if mgr.Count() != 1 {
		t.Fatalf("in-flight session should still be live, count=%d", mgr.Count())
	}
	mgr.Close(ctx, "MZ-DRAIN")
	if err := <-done; err != nil {
		t.Fatalf("WaitIdle: %v", err)
	}
	if mgr.Count() != 0 {
		t.Fatalf("expected idle, count=%d", mgr.Count())
	}
}

func TestDrainTimeoutThenCloseAll(t *testing.T) {
	mgr := NewSessionManager(DefaultConfig(), nil, nil, nil)
	ctx := context.Background()
	if _, err := mgr.Create(ctx, testStart("MZ-STUCK"), nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mgr.BeginDrain()
	waitCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	if err := mgr.WaitIdle(waitCtx); err == nil {
		t.Fatal("expected wait timeout while session still live")
	}
	mgr.CloseAll(ctx)
	if mgr.Count() != 0 {
		t.Fatalf("CloseAll left %d sessions", mgr.Count())
	}
}

func TestDrainRejectsNewWebsocket(t *testing.T) {
	srv := NewServer(DefaultConfig(), nil, nil, nil)
	srv.Manager().BeginDrain()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	srv.handleWebSocket(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", rec.Code)
	}
}
