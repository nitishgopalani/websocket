package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type capturedOutbound struct {
	data    []byte
	isAudio bool
	at      time.Time
}

type outboundCapture struct {
	mu    sync.Mutex
	msgs  []capturedOutbound
	clock Clock
}

func (c *outboundCapture) record(data []byte, isAudio bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var now time.Time
	if c.clock != nil {
		now = c.clock.Now()
	}
	c.msgs = append(c.msgs, capturedOutbound{data: append([]byte(nil), data...), isAudio: isAudio, at: now})
}

func (c *outboundCapture) snapshot() []capturedOutbound {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]capturedOutbound, len(c.msgs))
	copy(out, c.msgs)
	return out
}

func setupCarrierEgressTest(t *testing.T, clock Clock) (*CarrierEgress, *Session, *outboundCapture) {
	t.Helper()
	cap := &outboundCapture{clock: clock}
	cfg := DefaultEgressConfig()
	cfg.JitterMs = 300
	egress := NewCarrierEgress(cfg, 20, clock, nil, nil)

	mgr := NewSessionManager(DefaultConfig(), nil, func() AudioSink {
		return NewLoggingSink(nil)
	}, nil)
	ctx := context.Background()
	serverConn, clientConn := newWSConnPair(t)
	session, err := mgr.Create(ctx, StartEvent{
		Event:       EventStart,
		StreamSID:   "MZ-EGRESS",
		CallSID:     "CA-EGRESS",
		MediaFormat: AudioFormat{Encoding: "audio/x-mulaw", SampleRate: 8000, Channels: 1},
	}, serverConn)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() {
		mgr.Close(ctx, session.StreamSID)
		_ = clientConn.Close()
	})

	go func() {
		for {
			_, data, err := clientConn.ReadMessage()
			if err != nil {
				return
			}
			cap.record(data, isMediaOutbound(data))
		}
	}()

	egress.BindSession(session)
	return egress, session, cap
}

func isMediaOutbound(data []byte) bool {
	var env struct {
		Event string `json:"event"`
	}
	_ = json.Unmarshal(data, &env)
	return env.Event == EventMedia
}

func newWSConnPair(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	var serverConn *websocket.Conn
	ready := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		serverConn, err = upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		close(ready)
		<-done
		_ = serverConn.Close()
	}))
	t.Cleanup(ts.Close)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = clientConn.Close() })
	<-ready
	return serverConn, clientConn
}

func parseOutboundMedia(t *testing.T, data []byte) (streamSID string, payload []byte) {
	t.Helper()
	var msg struct {
		Event     string `json:"event"`
		StreamSID string `json:"stream_sid"`
		Media     struct {
			Payload string `json:"payload"`
		} `json:"media"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal media: %v", err)
	}
	if msg.Event != EventMedia {
		t.Fatalf("event = %q, want media", msg.Event)
	}
	decoded, err := base64.StdEncoding.DecodeString(msg.Media.Payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return msg.StreamSID, decoded
}

func waitCapture(t *testing.T, cap *outboundCapture, min int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(cap.snapshot()) >= min {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("capture has %d messages, want >= %d", len(cap.snapshot()), min)
}

func TestCarrierEgressHumanGate(t *testing.T) {
	clock := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	egress, session, cap := setupCarrierEgressTest(t, clock)
	egress.EnableHumanGate()

	audio := make([]byte, 160)
	if err := egress.SendAudio(context.Background(), session, TTSAudioChunk{TurnID: "t1", MuLaw: audio}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(40 * time.Millisecond)
	if len(cap.snapshot()) != 0 {
		t.Fatal("expected no outbound while human gated")
	}
	egress.ConfirmHuman()
	clock.Advance(20 * time.Millisecond)
	waitCapture(t, cap, 1, time.Second)
}

func TestCarrierEgressSendAudioMediaFraming(t *testing.T) {
	clock := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	egress, session, cap := setupCarrierEgressTest(t, clock)

	audio := make([]byte, 480) // 3 x 160-byte frames
	for i := range audio {
		audio[i] = byte(i)
	}
	if err := egress.SendAudio(context.Background(), session, TTSAudioChunk{TurnID: "t1", MuLaw: audio}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		clock.Advance(20 * time.Millisecond)
	}
	waitCapture(t, cap, 3, time.Second)

	msgs := cap.snapshot()
	if len(msgs) < 3 {
		t.Fatalf("got %d media messages, want 3", len(msgs))
	}
	var combined []byte
	for i, msg := range msgs[:3] {
		sid, payload := parseOutboundMedia(t, msg.data)
		if sid != "MZ-EGRESS" {
			t.Fatalf("frame %d stream_sid = %q", i, sid)
		}
		combined = append(combined, payload...)
	}
	if string(combined) != string(audio) {
		t.Fatalf("combined payload mismatch")
	}
}

func TestCarrierEgressPacingNotBurst(t *testing.T) {
	clock := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	egress, session, cap := setupCarrierEgressTest(t, clock)

	audio := make([]byte, 160*5)
	if err := egress.SendAudio(context.Background(), session, TTSAudioChunk{TurnID: "t1", MuLaw: audio}); err != nil {
		t.Fatal(err)
	}

	clock.Advance(20 * time.Millisecond)
	waitCapture(t, cap, 1, time.Second)
	if len(cap.snapshot()) != 1 {
		t.Fatalf("after 20ms expected 1 frame, got %d", len(cap.snapshot()))
	}

	clock.Advance(20 * time.Millisecond)
	waitCapture(t, cap, 2, time.Second)
	if len(cap.snapshot()) != 2 {
		t.Fatalf("after 40ms expected 2 frames, got %d", len(cap.snapshot()))
	}
}

func TestCarrierEgressMarkAfterAudio(t *testing.T) {
	clock := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	egress, session, cap := setupCarrierEgressTest(t, clock)

	audio := make([]byte, 160)
	_ = egress.SendAudio(context.Background(), session, TTSAudioChunk{TurnID: "turn-mark", MuLaw: audio})
	_ = egress.Mark(context.Background(), session, "turn-mark")

	clock.Advance(20 * time.Millisecond)
	waitCapture(t, cap, 2, time.Second)

	msgs := cap.snapshot()
	last := msgs[len(msgs)-1]
	var markMsg struct {
		Event     string `json:"event"`
		StreamSID string `json:"stream_sid"`
		Mark      struct {
			Name string `json:"name"`
		} `json:"mark"`
	}
	if err := json.Unmarshal(last.data, &markMsg); err != nil {
		t.Fatalf("unmarshal mark: %v", err)
	}
	if markMsg.Event != EventMark || markMsg.Mark.Name != "turn-mark" {
		t.Fatalf("mark message = %+v", markMsg)
	}
}

func TestCarrierEgressClearPlaybackDropsPending(t *testing.T) {
	clock := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	egress, session, cap := setupCarrierEgressTest(t, clock)

	audio := make([]byte, 160*10)
	_ = egress.SendAudio(context.Background(), session, TTSAudioChunk{TurnID: "t1", MuLaw: audio})
	_ = egress.ClearPlayback(context.Background(), session)

	clock.Advance(200 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	if got := egress.PendingDropped(); got < 9 {
		t.Fatalf("pending dropped = %d, want >= 9", got)
	}

	msgs := cap.snapshot()
	clearFound := false
	mediaCount := 0
	for _, msg := range msgs {
		var env struct {
			Event string `json:"event"`
		}
		_ = json.Unmarshal(msg.data, &env)
		switch env.Event {
		case "clear":
			clearFound = true
		case EventMedia:
			mediaCount++
		}
	}
	if !clearFound {
		t.Fatal("expected clear message")
	}
	if mediaCount > 2 {
		t.Fatalf("expected at most ~1-2 media frames before clear, got %d", mediaCount)
	}
}

func TestInboundMarkEchoPlaybackComplete(t *testing.T) {
	var completed atomic.Int32
	var turnID string
	mgr := NewSessionManager(DefaultConfig(), nil, func() AudioSink {
		return &markSinkListener{
			onStart: func(s *Session) {
				s.SetPlaybackListener(PlaybackListenerFunc(func(_ context.Context, _ *Session, id string) {
					turnID = id
					completed.Add(1)
				}))
			},
		}
	}, nil)
	ctx := context.Background()
	_, err := mgr.Create(ctx, StartEvent{
		Event: EventStart, StreamSID: "MZ-MARK", CallSID: "CA",
		MediaFormat: AudioFormat{SampleRate: 8000, Channels: 1},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.HandleMark(ctx, MarkEvent{
		Event: EventMark, StreamSID: "MZ-MARK",
		Mark: MarkChunk{Name: "turn-echo"},
	}); err != nil {
		t.Fatal(err)
	}
	if completed.Load() != 1 || turnID != "turn-echo" {
		t.Fatalf("completed=%d turnID=%q", completed.Load(), turnID)
	}
}

type PlaybackListenerFunc func(ctx context.Context, session *Session, turnID string)

func (f PlaybackListenerFunc) OnPlaybackComplete(ctx context.Context, session *Session, turnID string) {
	f(ctx, session, turnID)
}

type markSinkListener struct {
	onStart func(*Session)
}

func (m *markSinkListener) OnStart(_ context.Context, session *Session) error {
	if m.onStart != nil {
		m.onStart(session)
	}
	return nil
}
func (m *markSinkListener) OnAudio(context.Context, *Session, []byte) error { return nil }
func (m *markSinkListener) OnDTMF(context.Context, *Session, string) error  { return nil }
func (m *markSinkListener) OnStop(context.Context, *Session) error          { return nil }

func TestOutboundBufferDropOldestAudio(t *testing.T) {
	s := &Session{
		StreamSID:    "MZ-OVER",
		outboundWake: make(chan struct{}, 1),
		outboundCap:  2,
		stopCh:       make(chan struct{}),
	}
	for i := 0; i < 3; i++ {
		s.EnqueueOutbound([]byte("audio"), true)
	}
	if s.OutboundDropped() != 1 {
		t.Fatalf("outbound dropped = %d, want 1", s.OutboundDropped())
	}
	s.outboundQueueMu.Lock()
	n := len(s.outboundQueue)
	s.outboundQueueMu.Unlock()
	if n != 2 {
		t.Fatalf("queue len = %d, want 2", n)
	}
}

func TestFullDuplexInboundWhileOutbound(t *testing.T) {
	cfg := DefaultConfig()
	var inboundFrames atomic.Int32
	sink := &duplexSink{frames: &inboundFrames}
	srv := NewServer(cfg, nil, func() AudioSink { return sink }, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + cfg.WSPath
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mustWriteJSONConn(t, conn, map[string]any{"event": "connected"})
	mustWriteJSONConn(t, conn, map[string]any{
		"event": "start", "stream_sid": "MZ-DX", "call_sid": "CA",
		"media_format": map[string]any{"encoding": "audio/x-mulaw", "sample_rate": 8000, "channels": 1},
	})

	waitForSession(t, srv, "MZ-DX")

	clock := NewFakeClock(time.Now())
	egress := NewCarrierEgress(DefaultEgressConfig(), 20, clock, nil, nil)
	session, ok := srv.Manager().Get("MZ-DX")
	if !ok {
		t.Fatal("session not found")
	}
	egress.BindSession(session)
	audio := make([]byte, 160*3)
	_ = egress.SendAudio(context.Background(), session, TTSAudioChunk{TurnID: "t", MuLaw: audio})

	for i := 0; i < 3; i++ {
		payload := base64.StdEncoding.EncodeToString([]byte{byte(i)})
		mustWriteJSONConn(t, conn, map[string]any{
			"event": "media", "stream_sid": "MZ-DX",
			"media": map[string]any{"payload": payload},
		})
	}
	clock.Advance(60 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && inboundFrames.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	if inboundFrames.Load() != 3 {
		t.Fatalf("inbound frames = %d, want 3", inboundFrames.Load())
	}

	outbound := readAvailableWS(t, conn)
	if len(outbound) == 0 {
		t.Fatal("expected outbound messages while receiving inbound")
	}
}

type duplexSink struct {
	frames *atomic.Int32
}

func (d *duplexSink) OnStart(context.Context, *Session) error { return nil }
func (d *duplexSink) OnAudio(_ context.Context, _ *Session, _ []byte) error {
	d.frames.Add(1)
	return nil
}
func (d *duplexSink) OnDTMF(context.Context, *Session, string) error { return nil }
func (d *duplexSink) OnStop(context.Context, *Session) error         { return nil }

func mustWriteJSONConn(t *testing.T, conn *websocket.Conn, payload map[string]any) {
	t.Helper()
	data, _ := json.Marshal(payload)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
}

func readAvailableWS(t *testing.T, conn *websocket.Conn) [][]byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var out [][]byte
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		out = append(out, data)
	}
	return out
}

func waitForSession(t *testing.T, srv *Server, streamSID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := srv.Manager().Get(streamSID); ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("session %s not found", streamSID)
}

func TestSplitMuLawFrames(t *testing.T) {
	frames := splitMuLawFrames(make([]byte, 400), 160)
	if len(frames) != 3 {
		t.Fatalf("frames = %d, want 3", len(frames))
	}
	if len(frames[0]) != 160 || len(frames[1]) != 160 || len(frames[2]) != 80 {
		t.Fatalf("frame sizes = %d, %d, %d", len(frames[0]), len(frames[1]), len(frames[2]))
	}
}
