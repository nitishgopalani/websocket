package callbridge_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"websocket/internal/callbridge"
)

const testSecret = "ms_loadtest_secret_value_ok_32chars"

func testConfig() callbridge.Config {
	return callbridge.Config{
		ListenAddr:     ":0",
		WSPath:         "/bridge",
		MediaSecret:    testSecret,
		QueueFrames:    100,
		MaxFrameAge:    200 * time.Millisecond,
		PairingTimeout: 60 * time.Second,
		MetricsEnabled: true,
	}
}

func sign(secret, sessionID string, now int64) string {
	msg := fmt.Sprintf("%d.%s", now, sessionID)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(msg))
	return fmt.Sprintf("t=%d,v1=%s", now, hex.EncodeToString(mac.Sum(nil)))
}

func startTestServer(t *testing.T, cfg callbridge.Config) (*callbridge.Server, *httptest.Server, string) {
	t.Helper()
	metrics := callbridge.NewMetrics(cfg.MetricsEnabled)
	srv := callbridge.NewServer(cfg, nil, metrics)
	ts := httptest.NewServer(srv.HTTPServer().Handler)
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + cfg.WSPath
	return srv, ts, wsURL
}

func dialLeg(t *testing.T, wsURL, bridgeID, leg, sessionID string) *websocket.Conn {
	t.Helper()
	now := time.Now().Unix()
	hdr := http.Header{}
	hdr.Set("X-Fonada-Session-Id", sessionID)
	hdr.Set("X-Fonada-Media-Signature", sign(testSecret, sessionID, now))
	q := fmt.Sprintf("?bridge=%s", bridgeID)
	if leg != "" {
		q += "&leg=" + leg
	}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL+q, hdr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func sendSessionStart(t *testing.T, conn *websocket.Conn, sessionID string, meta map[string]string, inRate, outRate int) {
	t.Helper()
	if inRate == 0 {
		inRate = 16000
	}
	if outRate == 0 {
		outRate = 16000
	}
	payload := map[string]any{
		"type":       "session_start",
		"session_id": sessionID,
		"client_id":  "salary-on-time",
		"audio": map[string]any{
			"codec":              "slin16",
			"input_sample_rate":  inRate,
			"output_sample_rate": outRate,
			"channels":           1,
		},
		"metadata": meta,
	}
	b, _ := json.Marshal(payload)
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		t.Fatalf("session_start: %v", err)
	}
}

func readType(t *testing.T, conn *websocket.Conn, want string) {
	t.Helper()
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.Type != want {
		t.Fatalf("got type %q want %q body=%s", msg.Type, want, string(data))
	}
}

func connectLeg(t *testing.T, wsURL, bridgeID, leg, sessionID string, meta map[string]string) *websocket.Conn {
	t.Helper()
	conn := dialLeg(t, wsURL, bridgeID, leg, sessionID)
	sendSessionStart(t, conn, sessionID, meta, 16000, 16000)
	return conn
}

func TestPairing_autoAssign(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "room1", "", "sess-a", nil)
	defer a.Close()
	b := connectLeg(t, wsURL, "room1", "", "sess-b", nil)
	defer b.Close()

	readType(t, a, "ready")
	readType(t, b, "ready")
}

func TestPairing_explicitLeg(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "room2", "a", "sess-a2", nil)
	defer a.Close()
	b := connectLeg(t, wsURL, "room2", "b", "sess-b2", nil)
	defer b.Close()

	readType(t, a, "ready")
	readType(t, b, "ready")
}

func TestPairing_metadataOverride(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "ignored", "", "sess-ma", map[string]string{
		"bridge_id":  "meta-room",
		"bridge_leg": "a",
	})
	defer a.Close()
	b := connectLeg(t, wsURL, "ignored", "", "sess-mb", map[string]string{
		"bridge_id":  "meta-room",
		"bridge_leg": "b",
	})
	defer b.Close()

	readType(t, a, "ready")
	readType(t, b, "ready")
}

func TestPairing_rateMismatch(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "rm", "a", "sess-ra", nil)
	defer a.Close()

	conn := dialLeg(t, wsURL, "rm", "b", "sess-rb")
	sendSessionStart(t, conn, "sess-rb", nil, 8000, 8000)
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(string(data), "error") {
		t.Fatalf("expected error frame, got %s", string(data))
	}
	_ = conn.Close()
}

func TestPairingTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.PairingTimeout = 80 * time.Millisecond
	_, ts, wsURL := startTestServer(t, cfg)
	defer ts.Close()

	conn := connectLeg(t, wsURL, "timeout-room", "a", "sess-timeout", nil)
	defer conn.Close()

	time.Sleep(150 * time.Millisecond)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Type != "error" {
		t.Fatalf("want error got %s", string(data))
	}
}

func TestRelay_bidirectional(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "relay", "a", "relay-a", nil)
	defer a.Close()
	b := connectLeg(t, wsURL, "relay", "b", "relay-b", nil)
	defer b.Close()
	readType(t, a, "ready")
	readType(t, b, "ready")

	time.Sleep(20 * time.Millisecond)

	frameA := []byte{1, 2, 3, 4}
	frameB := []byte{9, 8, 7, 6}
	if err := a.WriteMessage(websocket.BinaryMessage, frameA); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteMessage(websocket.BinaryMessage, frameB); err != nil {
		t.Fatal(err)
	}

	_, gotB, err := b.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(gotB) != string(frameA) {
		t.Fatalf("B got %v want %v", gotB, frameA)
	}
	_, gotA, err := a.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(gotA) != string(frameB) {
		t.Fatalf("A got %v want %v", gotA, frameB)
	}
}

func TestAgeDrop_enqueue(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFrameAge = 50 * time.Millisecond
	metrics := callbridge.NewMetrics(true)
	q := callbridge.NewOutboundQueueForTest(cfg.QueueFrames, cfg.MaxFrameAge, metrics, "a_to_b")
	old := time.Now().Add(-100 * time.Millisecond)
	q.EnqueueForTest([]byte{1}, old)
	if _, ok := q.DequeueForTest(time.Now()); ok {
		t.Fatal("expected stale frame dropped at dequeue")
	}
}

func TestDequeueAgeDrop(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFrameAge = 30 * time.Millisecond
	metrics := callbridge.NewMetrics(true)
	q := callbridge.NewOutboundQueueForTest(cfg.QueueFrames, cfg.MaxFrameAge, metrics, "b_to_a")
	stale := time.Now().Add(-60 * time.Millisecond)
	q.EnqueueForTest([]byte{9, 9}, stale)
	time.Sleep(5 * time.Millisecond)
	if f, ok := q.DequeueForTest(time.Now()); ok {
		t.Fatalf("expected drop at dequeue, got frame len=%d", len(f.Payload))
	}
}

func TestTeardown_oneLeg(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "tear", "a", "tear-a", nil)
	b := connectLeg(t, wsURL, "tear", "b", "tear-b", nil)
	readType(t, a, "ready")
	readType(t, b, "ready")

	_ = a.WriteMessage(websocket.TextMessage, []byte(`{"type":"session_end"}`))
	readType(t, b, "end_of_call")
}
