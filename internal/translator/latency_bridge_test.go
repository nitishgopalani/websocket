package translator

import (
	"context"
	"testing"
	"time"

	"websocket/internal/media"
)

func TestLatencyBridge_singleTurnIDAndDeferredFinish(t *testing.T) {
	tracker := NewLatencyTracker(nil)
	var speechEndID string

	bridge := &latencyBridge{
		inner: &noopConsumer{},
		tracker: tracker,
		beginTurn: func() string {
			speechEndID = "turn-1"
			tracker.BeginTurn(speechEndID, "leg-a", "leg-b")
			return speechEndID
		},
		currentTurn: func() string { return speechEndID },
	}

	sess := &media.Session{StreamSID: "leg-a"}
	bridge.OnSpeechEnd(context.Background(), sess)
	if speechEndID == "" {
		t.Fatal("expected turn id at speech end")
	}

	tracker.MarkASRFinal(speechEndID, time.Now().Add(-400*time.Millisecond))
	tracker.MarkMayuraDone(speechEndID, time.Now().Add(-200*time.Millisecond), "Hello")
	tracker.MarkTTSFirstByte(speechEndID, time.Now().Add(-100*time.Millisecond))
	tracker.MarkEgressFirst(speechEndID, time.Now())
	tracker.Finish(speechEndID, false, "")

	done := tracker.Completed()
	if len(done) != 1 {
		t.Fatalf("completed=%d", len(done))
	}
	u := done[0]
	if u.speechEndToEgressMs() < 0 {
		t.Fatalf("speech_end_to_egress_ms=%d", u.speechEndToEgressMs())
	}
	if u.mayuraToTTSFirstMs() < 0 {
		t.Fatalf("mayura_to_tts_first_ms=%d", u.mayuraToTTSFirstMs())
	}
	if u.ttsToEgressMs() < 0 {
		t.Fatalf("tts_to_egress_ms=%d", u.ttsToEgressMs())
	}
}

type noopConsumer struct{}

func (n *noopConsumer) OnPartial(context.Context, *media.Session, media.Transcript) {}
func (n *noopConsumer) OnFinal(context.Context, *media.Session, media.Transcript)   {}
func (n *noopConsumer) OnSpeechStart(context.Context, *media.Session)                 {}
func (n *noopConsumer) OnSpeechEnd(context.Context, *media.Session)                     {}
