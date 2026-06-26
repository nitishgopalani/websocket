package media_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"websocket/internal/media"
)

type recordingSink struct {
	mu     sync.Mutex
	starts []*media.Session
	audio  [][]byte
	dtmfs  []string
	stops  int
}

func newRecordingSink() *recordingSink {
	return &recordingSink{}
}

func (s *recordingSink) OnStart(_ context.Context, session *media.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.starts = append(s.starts, session)
	return nil
}

func (s *recordingSink) OnAudio(_ context.Context, _ *media.Session, frame []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]byte, len(frame))
	copy(copied, frame)
	s.audio = append(s.audio, copied)
	return nil
}

func (s *recordingSink) OnDTMF(_ context.Context, _ *media.Session, digit string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dtmfs = append(s.dtmfs, digit)
	return nil
}

func (s *recordingSink) OnStop(_ context.Context, _ *media.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stops++
	return nil
}

func (s *recordingSink) snapshot() (starts []*media.Session, audio [][]byte, dtmfs []string, stops int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	starts = append([]*media.Session(nil), s.starts...)
	for _, frame := range s.audio {
		copied := make([]byte, len(frame))
		copy(copied, frame)
		audio = append(audio, copied)
	}
	dtmfs = append([]string(nil), s.dtmfs...)
	return starts, audio, dtmfs, s.stops
}

func dialTestServer(t *testing.T, cfg media.Config, sinkFactory func() media.AudioSink) (*websocket.Conn, *httptest.Server) {
	t.Helper()

	srv := media.NewServer(cfg, nil, sinkFactory)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	wsPath := cfg.WSPath
	if wsPath == "" {
		wsPath = "/stream"
	}
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + wsPath

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, ts
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func mustWriteJSON(t *testing.T, conn *websocket.Conn, payload map[string]any) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		if !errors.Is(err, websocket.ErrCloseSent) {
			t.Fatalf("write message: %v", err)
		}
	}
}

func TestHealthz(t *testing.T) {
	cfg := media.DefaultConfig()
	srv := media.NewServer(cfg, nil, func() media.AudioSink { return newRecordingSink() })
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestFullStreamLifecycle(t *testing.T) {
	cfg := media.DefaultConfig()
	sink := newRecordingSink()
	conn, _ := dialTestServer(t, cfg, func() media.AudioSink { return sink })

	payloads := [][]byte{
		{0x01, 0x02, 0x03},
		{0x04, 0x05},
		{0xff, 0x00, 0xab, 0xcd},
	}

	mustWriteJSON(t, conn, map[string]any{"event": "connected"})
	mustWriteJSON(t, conn, map[string]any{
		"event":      "start",
		"stream_sid": "MZ123",
		"call_sid":   "CA456",
		"media_format": map[string]any{
			"encoding":    "audio/x-mulaw",
			"sample_rate": 8000,
			"channels":    1,
		},
		"custom_parameters": map[string]string{
			"borrower_id": "B789",
		},
	})

	for i, payload := range payloads {
		mustWriteJSON(t, conn, map[string]any{
			"event":      "media",
			"stream_sid": "MZ123",
			"media": map[string]any{
				"payload":   base64.StdEncoding.EncodeToString(payload),
				"timestamp": "1000",
				"chunk":     i + 1,
			},
		})
	}

	mustWriteJSON(t, conn, map[string]any{
		"event":      "dtmf",
		"stream_sid": "MZ123",
		"dtmf":       map[string]string{"digit": "5"},
	})
	mustWriteJSON(t, conn, map[string]any{
		"event":      "stop",
		"stream_sid": "MZ123",
	})

	waitFor(t, func() bool {
		_, audio, _, stops := sink.snapshot()
		return stops == 1 && len(audio) == len(payloads)
	})

	starts, audio, dtmfs, stops := sink.snapshot()
	if len(starts) != 1 {
		t.Fatalf("starts = %d, want 1", len(starts))
	}
	if starts[0].StreamSID != "MZ123" || starts[0].CallSID != "CA456" {
		t.Fatalf("unexpected session ids: %+v", starts[0])
	}
	if starts[0].Format.Encoding != "audio/x-mulaw" || starts[0].Format.SampleRate != 8000 || starts[0].Format.Channels != 1 {
		t.Fatalf("unexpected format: %+v", starts[0].Format)
	}
	if starts[0].Params["borrower_id"] != "B789" {
		t.Fatalf("params = %#v, want borrower_id=B789", starts[0].Params)
	}
	if len(audio) != len(payloads) {
		t.Fatalf("audio frames = %d, want %d", len(audio), len(payloads))
	}
	for i := range payloads {
		if string(audio[i]) != string(payloads[i]) {
			t.Fatalf("frame %d = %v, want %v", i, audio[i], payloads[i])
		}
	}
	if len(dtmfs) != 1 || dtmfs[0] != "5" {
		t.Fatalf("dtmfs = %#v, want [5]", dtmfs)
	}
	if stops != 1 {
		t.Fatalf("stops = %d, want 1", stops)
	}
}

func TestAbruptCloseCleansUpSession(t *testing.T) {
	cfg := media.DefaultConfig()
	sink := newRecordingSink()
	conn, _ := dialTestServer(t, cfg, func() media.AudioSink { return sink })

	mustWriteJSON(t, conn, map[string]any{"event": "connected"})
	mustWriteJSON(t, conn, map[string]any{
		"event":      "start",
		"stream_sid": "MZ999",
		"call_sid":   "CA999",
		"media_format": map[string]any{
			"encoding":    "audio/x-mulaw",
			"sample_rate": 8000,
			"channels":    1,
		},
	})

	waitFor(t, func() bool {
		starts, _, _, _ := sink.snapshot()
		return len(starts) == 1
	})

	if err := conn.Close(); err != nil {
		t.Fatalf("close conn: %v", err)
	}

	waitFor(t, func() bool {
		_, _, _, stops := sink.snapshot()
		return stops == 1
	})
}

func TestUnknownEventIgnored(t *testing.T) {
	cfg := media.DefaultConfig()
	sink := newRecordingSink()
	conn, _ := dialTestServer(t, cfg, func() media.AudioSink { return sink })

	mustWriteJSON(t, conn, map[string]any{"event": "connected"})
	mustWriteJSON(t, conn, map[string]any{"event": "custom_ping", "stream_sid": "ignored"})
	mustWriteJSON(t, conn, map[string]any{
		"event":      "start",
		"stream_sid": "MZ777",
		"call_sid":   "CA777",
		"media_format": map[string]any{
			"encoding":    "audio/x-mulaw",
			"sample_rate": 8000,
			"channels":    1,
		},
	})
	mustWriteJSON(t, conn, map[string]any{
		"event":      "stop",
		"stream_sid": "MZ777",
	})

	waitFor(t, func() bool {
		_, _, _, stops := sink.snapshot()
		return stops == 1
	})
}

func TestMaxConcurrentSessionsEnforced(t *testing.T) {
	cfg := media.DefaultConfig()
	cfg.MaxConcurrentSessions = 1

	var sinkIndex atomic.Int32
	sinks := []*recordingSink{newRecordingSink(), newRecordingSink()}

	srv := media.NewServer(cfg, nil, func() media.AudioSink {
		idx := int(sinkIndex.Add(1)) - 1
		if idx >= len(sinks) {
			idx = len(sinks) - 1
		}
		return sinks[idx]
	})

	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + cfg.WSPath

	conn1, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial first conn: %v", err)
	}
	t.Cleanup(func() { _ = conn1.Close() })

	mustWriteJSON(t, conn1, map[string]any{"event": "connected"})
	mustWriteJSON(t, conn1, map[string]any{
		"event":      "start",
		"stream_sid": "MZ-A",
		"call_sid":   "CA-A",
		"media_format": map[string]any{
			"encoding":    "audio/x-mulaw",
			"sample_rate": 8000,
			"channels":    1,
		},
	})

	waitFor(t, func() bool {
		starts, _, _, _ := sinks[0].snapshot()
		return len(starts) == 1
	})

	conn2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial second conn: %v", err)
	}
	t.Cleanup(func() { _ = conn2.Close() })

	mustWriteJSON(t, conn2, map[string]any{"event": "connected"})
	mustWriteJSON(t, conn2, map[string]any{
		"event":      "start",
		"stream_sid": "MZ-B",
		"call_sid":   "CA-B",
		"media_format": map[string]any{
			"encoding":    "audio/x-mulaw",
			"sample_rate": 8000,
			"channels":    1,
		},
	})

	_ = conn2.SetReadDeadline(time.Now().Add(time.Second))
	_, _, readErr := conn2.ReadMessage()
	if readErr == nil {
		t.Fatal("expected server to close rejected connection")
	}

	starts, _, _, _ := sinks[1].snapshot()
	if len(starts) != 0 {
		t.Fatalf("second session should be rejected, got starts=%d", len(starts))
	}

	mustWriteJSON(t, conn1, map[string]any{
		"event":      "stop",
		"stream_sid": "MZ-A",
	})
}

func TestParseInboundEventUnknown(t *testing.T) {
	evt, err := media.ParseInboundEvent([]byte(`{"event":"foobar","stream_sid":"X"}`), nil)
	if err != nil {
		t.Fatalf("ParseInboundEvent: %v", err)
	}
	if evt.Type != "foobar" {
		t.Fatalf("type = %q, want foobar", evt.Type)
	}
}

func TestParseInboundEventMissingEvent(t *testing.T) {
	_, err := media.ParseInboundEvent([]byte(`{"stream_sid":"X"}`), nil)
	if err == nil {
		t.Fatal("expected error for missing event field")
	}
}

func TestSessionManagerIdempotentClose(t *testing.T) {
	cfg := media.DefaultConfig()
	mgr := media.NewSessionManager(cfg, nil, func() media.AudioSink { return newRecordingSink() })
	ctx := context.Background()

	_, err := mgr.Create(ctx, media.StartEvent{
		Event:     media.EventStart,
		StreamSID: "MZ-IDEM",
		CallSID:   "CA-IDEM",
		MediaFormat: media.AudioFormat{
			Encoding:   "audio/x-mulaw",
			SampleRate: 8000,
			Channels:   1,
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	mgr.Close(ctx, "MZ-IDEM")
	mgr.Close(ctx, "MZ-IDEM")

	if mgr.Count() != 0 {
		t.Fatalf("count = %d, want 0", mgr.Count())
	}
}

func TestInvalidBase64MediaDoesNotCrash(t *testing.T) {
	cfg := media.DefaultConfig()
	sink := newRecordingSink()
	conn, _ := dialTestServer(t, cfg, func() media.AudioSink { return sink })

	mustWriteJSON(t, conn, map[string]any{"event": "connected"})
	mustWriteJSON(t, conn, map[string]any{
		"event":      "start",
		"stream_sid": "MZ-B64",
		"call_sid":   "CA-B64",
		"media_format": map[string]any{
			"encoding":    "audio/x-mulaw",
			"sample_rate": 8000,
			"channels":    1,
		},
	})
	mustWriteJSON(t, conn, map[string]any{
		"event":      "media",
		"stream_sid": "MZ-B64",
		"media": map[string]any{
			"payload": "!!!not-base64!!!",
		},
	})
	mustWriteJSON(t, conn, map[string]any{
		"event":      "stop",
		"stream_sid": "MZ-B64",
	})

	waitFor(t, func() bool {
		_, _, _, stops := sink.snapshot()
		return stops == 1
	})
}
