package translator_test

import (
	"context"
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
	"websocket/internal/translator"
)

const testSecret = "ms_loadtest_secret_value_ok_32chars"

func testConfig() translator.Config {
	return translator.Config{
		ListenAddr:         ":0",
		WSPath:             "/translate",
		MediaSecret:        testSecret,
		QueueFrames:        100,
		MaxFrameAge:        200 * time.Millisecond,
		PairingTimeout:     60 * time.Second,
		MetricsEnabled:     true,
		TranslationEnabled: false,
		TargetSampleRate:   8000,
	}
}

func sign(secret, sessionID string, now int64) string {
	msg := fmt.Sprintf("%d.%s", now, sessionID)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(msg))
	return fmt.Sprintf("t=%d,v1=%s", now, hex.EncodeToString(mac.Sum(nil)))
}

func startTestServer(t *testing.T, cfg translator.Config) (*translator.Server, *httptest.Server, string) {
	t.Helper()
	metrics := translator.NewMetrics(cfg.MetricsEnabled)
	srv := translator.NewServer(cfg, nil, metrics, translator.LaneDeps{})
	ts := httptest.NewServer(srv.HTTPServer().Handler)
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + cfg.WSPath
	return srv, ts, wsURL
}

func dialLeg(t *testing.T, wsURL, bridgeID, leg, sessionID string, langs ...string) *websocket.Conn {
	t.Helper()
	now := time.Now().Unix()
	hdr := http.Header{}
	hdr.Set("X-Fonada-Session-Id", sessionID)
	hdr.Set("X-Fonada-Media-Signature", sign(testSecret, sessionID, now))
	q := fmt.Sprintf("?bridge=%s", bridgeID)
	if leg != "" {
		q += "&leg=" + leg
	}
	if len(langs) >= 2 {
		q += "&lang_a=" + langs[0] + "&lang_b=" + langs[1]
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
		inRate = 8000
	}
	if outRate == 0 {
		outRate = 8000
	}
	payload := map[string]any{
		"type":       "session_start",
		"session_id": sessionID,
		"client_id":  "salary-on-time",
		"audio": map[string]any{
			"codec":              "slin",
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

func connectLeg(t *testing.T, wsURL, bridgeID, leg, sessionID string) *websocket.Conn {
	t.Helper()
	conn := dialLeg(t, wsURL, bridgeID, leg, sessionID, "hi-IN", "en-IN")
	sendSessionStart(t, conn, sessionID, nil, 8000, 8000)
	return conn
}

func TestPairing_autoAssign(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "room1", "", "sess-a")
	defer a.Close()
	b := connectLeg(t, wsURL, "room1", "", "sess-b")
	defer b.Close()

	readType(t, a, "ready")
	readType(t, b, "ready")
}

func TestPairing_explicitLeg(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "room2", "a", "sess-a2")
	defer a.Close()
	b := connectLeg(t, wsURL, "room2", "b", "sess-b2")
	defer b.Close()

	readType(t, a, "ready")
	readType(t, b, "ready")
}

func TestPairing_rateMismatch(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := dialLeg(t, wsURL, "room3", "a", "sess-a3", "hi-IN", "en-IN")
	defer a.Close()
	sendSessionStart(t, a, "sess-a3", nil, 8000, 8000)

	b := dialLeg(t, wsURL, "room3", "b", "sess-b3", "hi-IN", "en-IN")
	defer b.Close()
	sendSessionStart(t, b, "sess-b3", nil, 16000, 16000)

	_, data, err := b.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg struct {
		Type string `json:"type"`
		Code string `json:"code"`
	}
	_ = json.Unmarshal(data, &msg)
	if msg.Type != "error" {
		t.Fatalf("expected error, got %s", string(data))
	}
}

func TestRelay_bidirectional_whenTranslationOff(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()

	a := connectLeg(t, wsURL, "relay1", "a", "sess-ra")
	defer a.Close()
	b := connectLeg(t, wsURL, "relay1", "b", "sess-rb")
	defer b.Close()
	readType(t, a, "ready")
	readType(t, b, "ready")

	frame := make([]byte, 320)
	for i := range frame {
		frame[i] = byte(i)
	}
	if err := a.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		t.Fatal(err)
	}
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := b.ReadMessage()
	if err != nil {
		t.Fatalf("read binary: %v", err)
	}
	if len(got) != len(frame) {
		t.Fatalf("len=%d want %d", len(got), len(frame))
	}
}

func TestHealthz(t *testing.T) {
	_, ts, _ := startTestServer(t, testConfig())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestUnauthorized_badSignature(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()
	hdr := http.Header{}
	hdr.Set("X-Fonada-Session-Id", "sess-x")
	hdr.Set("X-Fonada-Media-Signature", "t=1,v1=deadbeef")
	_, resp, err := websocket.DefaultDialer.Dial(wsURL+"?bridge=x&leg=a", hdr)
	if err == nil {
		t.Fatal("expected dial error")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%v", resp)
	}
}

func TestMissingSessionID(t *testing.T) {
	_, ts, wsURL := startTestServer(t, testConfig())
	defer ts.Close()
	_, resp, err := websocket.DefaultDialer.Dial(wsURL+"?bridge=x", nil)
	if err == nil {
		t.Fatal("expected dial error")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%v", resp)
	}
}

// Ensure context import is used by test package.
var _ = context.Background
