package media_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
)

func startFakeSarvamForSink(t *testing.T) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		for _, payload := range []map[string]any{
			{"type": "events", "data": map[string]string{"signal_type": "START_SPEECH"}},
			{"type": "data", "data": map[string]any{"transcript": "partial text", "is_final": false}},
			{"type": "data", "data": map[string]any{"transcript": "final text", "is_final": true}},
			{"type": "events", "data": map[string]string{"signal_type": "END_SPEECH"}},
		} {
			data, _ := json.Marshal(payload)
			_ = conn.WriteMessage(websocket.TextMessage, data)
		}
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/"
	return wsURL, ts.Close
}

func startFrameCaptureServer(t *testing.T) string {
	t.Helper()
	var lastLen atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg struct {
			Audio string `json:"audio"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		decoded, err := base64.StdEncoding.DecodeString(msg.Audio)
		if err != nil {
			return
		}
		lastLen.Store(int32(len(decoded)))
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	t.Cleanup(func() {
		if lastLen.Load() != 320 {
			t.Errorf("last audio payload bytes = %d, want 320", lastLen.Load())
		}
	})
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/"
}
