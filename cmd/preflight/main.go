package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"websocket/internal/brain"
	"websocket/internal/media"
)

type result struct {
	pass   bool
	reason string
}

func main() {
	fmt.Printf("SARVAM_API_KEY: %s\n", keyStatus("SARVAM_API_KEY"))
	fmt.Printf("ELEVENLABS_API_KEY: %s\n", keyStatus("ELEVENLABS_API_KEY"))
	fmt.Println()

	failed := false
	for _, check := range []struct {
		name string
		fn   func() result
	}{
		{"Sarvam", checkSarvam},
		{"ElevenLabs", checkElevenLabs},
		{"Brain", checkBrain},
	} {
		r := check.fn()
		status := "PASS"
		if !r.pass {
			status = "FAIL"
			failed = true
		}
		fmt.Printf("[%s] %s — %s\n", check.name, status, r.reason)
	}
	if failed {
		os.Exit(1)
	}
}

func keyStatus(name string) string {
	if strings.TrimSpace(os.Getenv(name)) == "" {
		return "empty"
	}
	return "set"
}

func checkSarvam() result {
	key := strings.TrimSpace(os.Getenv("SARVAM_API_KEY"))
	if key == "" {
		return result{false, "SARVAM_API_KEY empty"}
	}
	cfg := media.ASRConfigFromEnv()
	cfg.Enabled = true
	cfg.APIKey = key
	provider, err := media.NewASRProvider(cfg)
	if err != nil {
		return result{false, fmt.Sprintf("provider init: %v", err)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sess, err := provider.Open(ctx, media.ASRSessionMeta{
		StreamSID:  "preflight",
		SampleRate: 8000,
		Language:   "en-IN",
	})
	if err != nil {
		msg := err.Error()
		if strings.Contains(strings.ToLower(msg), "401") || strings.Contains(strings.ToLower(msg), "403") ||
			strings.Contains(strings.ToLower(msg), "unauthorized") || strings.Contains(strings.ToLower(msg), "forbidden") {
			return result{false, "auth rejected (check SARVAM_API_KEY)"}
		}
		return result{false, fmt.Sprintf("dial/open: %v", err)}
	}
	defer sess.Close()

	if err := sess.SendAudio(make([]byte, 16000)); err != nil {
		return result{false, fmt.Sprintf("send audio: %v", err)}
	}

	deadline := time.After(8 * time.Second)
	for {
		select {
		case evt, ok := <-sess.Events():
			if !ok {
				return result{true, "WS connected; audio accepted (session closed)"}
			}
			switch evt.Type {
			case media.ASREventPartial, media.ASREventFinal, media.ASREventSpeechStart, media.ASREventSpeechEnd:
				return result{true, fmt.Sprintf("WS connected; got ASR event type=%d", evt.Type)}
			case media.ASREventError:
				if evt.Err != nil {
					return result{false, fmt.Sprintf("ASR error: %v", evt.Err)}
				}
			}
		case <-deadline:
			return result{true, "WS connected; sent 1s PCM (no transcript on silence — OK)"}
		}
	}
}

func checkElevenLabs() result {
	key := strings.TrimSpace(os.Getenv("ELEVENLABS_API_KEY"))
	if key == "" {
		return result{false, "ELEVENLABS_API_KEY empty"}
	}
	cfg := media.TTSConfigFromEnv()
	cfg.Enabled = true
	cfg.APIKey = key
	provider, err := media.NewTTSProvider(cfg)
	if err != nil {
		return result{false, fmt.Sprintf("provider init: %v", err)}
	}
	el, ok := provider.(*media.ElevenLabsTTSProvider)
	if !ok {
		return result{false, "expected ElevenLabsTTSProvider"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream, err := el.Open(ctx, media.TTSSessionMeta{StreamSID: "preflight"})
	if err != nil {
		msg := err.Error()
		if strings.Contains(strings.ToLower(msg), "401") || strings.Contains(strings.ToLower(msg), "403") {
			return result{false, "auth rejected (check ELEVENLABS_API_KEY)"}
		}
		return result{false, fmt.Sprintf("dial/open: %v", err)}
	}
	defer stream.Close()

	if err := stream.Speak("preflight-turn", "test one"); err != nil {
		return result{false, fmt.Sprintf("speak: %v", err)}
	}

	deadline := time.After(15 * time.Second)
	var audioBytes int
	var frames int
	for {
		select {
		case chunk, ok := <-stream.Audio():
			if !ok {
				if audioBytes > 0 {
					return result{true, fmt.Sprintf("received %d ulaw frames (%d bytes)", frames, audioBytes)}
				}
				return result{false, "stream closed without audio"}
			}
			if len(chunk.MuLaw) > 0 {
				frames++
				audioBytes += len(chunk.MuLaw)
			}
			if chunk.Final && audioBytes > 0 {
				return result{true, fmt.Sprintf("received %d ulaw frames (%d bytes)", frames, audioBytes)}
			}
		case <-deadline:
			if audioBytes > 0 {
				return result{true, fmt.Sprintf("received %d ulaw frames (%d bytes)", frames, audioBytes)}
			}
			return result{false, "timeout waiting for ulaw_8000 audio frames"}
		}
	}
}

func checkBrain() result {
	if strings.TrimSpace(os.Getenv("BRAIN_WS_ENABLED")) != "true" &&
		os.Getenv("BRAIN_WS_ENABLED") != "1" {
		os.Setenv("BRAIN_WS_ENABLED", "true")
	}
	cfg := brain.ConfigFromEnv()
	if cfg.URL == "" {
		cfg.URL = "ws://127.0.0.1:8000/ws/brain"
	}
	cfg.Enabled = true

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, cfg.URL, nil)
	if err != nil {
		if resp != nil {
			return result{false, fmt.Sprintf("dial %s: HTTP %d — is Collection brain running? (uvicorn app.main:app --port 8000)", cfg.URL, resp.StatusCode)}
		}
		return result{false, fmt.Sprintf("dial %s: %v — start Collection: cd ../Collection && uvicorn app.main:app --host 0.0.0.0 --port 8000", cfg.URL, err)}
	}
	defer conn.Close()

	sessionID := "preflight-session"
	if err := conn.WriteJSON(brain.SessionStartPayload{
		Type:       brain.TypeSessionStart,
		SessionID:  sessionID,
		BorrowerID: "preflight-borrower",
		AgentID:    "default",
		Locale:     "hi-IN",
	}); err != nil {
		return result{false, fmt.Sprintf("session_start send: %v", err)}
	}
	if err := conn.WriteJSON(brain.TurnPayload{
		Type:       brain.TypeTurn,
		SessionID:  sessionID,
		TurnID:     sessionID + "-t1",
		Transcript: "",
		FlowClass:  "Default",
	}); err != nil {
		return result{false, fmt.Sprintf("opener turn send: %v", err)}
	}

	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	for i := 0; i < 20; i++ {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return result{false, fmt.Sprintf("read: %v", err)}
		}
		typ, _ := decodeType(data)
		switch typ {
		case brain.TypeChunk:
			var m brain.ChunkMessage
			_ = json.Unmarshal(data, &m)
			return result{true, fmt.Sprintf("got chunk seq=%d text_len=%d", m.Seq, len(m.Text))}
		case brain.TypeDone:
			return result{true, "got done (no chunk)"}
		case brain.TypeError:
			var m brain.ErrorMessage
			_ = json.Unmarshal(data, &m)
			if m.FallbackText != "" {
				return result{true, "got error with fallback_text (brain reachable)"}
			}
			return result{false, "brain returned error without fallback"}
		case brain.TypeFlowClass:
			continue
		default:
			continue
		}
	}
	return result{false, "no chunk/done after opener turn"}
}

func decodeType(data []byte) (string, error) {
	var top struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &top); err != nil {
		return "", err
	}
	return top.Type, nil
}

var _ = http.StatusOK
