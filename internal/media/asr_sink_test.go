package media_test

import (
	"context"
	"testing"
	"time"

	"websocket/internal/media"
)

type recordingConsumer struct {
	partials     []string
	finals       []string
	speechStarts int
	speechEnds   int
}

func (c *recordingConsumer) OnPartial(_ context.Context, _ *media.Session, transcript media.Transcript) {
	c.partials = append(c.partials, transcript.Text)
}

func (c *recordingConsumer) OnFinal(_ context.Context, _ *media.Session, transcript media.Transcript) {
	c.finals = append(c.finals, transcript.Text)
}

func (c *recordingConsumer) OnSpeechStart(_ context.Context, _ *media.Session) {
	c.speechStarts++
}

func (c *recordingConsumer) OnSpeechEnd(_ context.Context, _ *media.Session) {
	c.speechEnds++
}

func TestASRSinkWithFakeSarvam(t *testing.T) {
	wsURL, cleanup := startFakeSarvamForSink(t)
	defer cleanup()

	provider := media.NewSarvamASRProvider("test-key", media.SarvamConfig{
		Endpoint:           wsURL,
		Model:              "saaras:v3",
		Mode:               "transcribe",
		Language:           "hi-IN",
		HighVADSensitivity: true,
		VADSignals:         true,
		KeepalivePeriod:    0,
	}, nil)

	consumer := &recordingConsumer{}
	sink := media.NewASRSink(provider, consumer, 8000, nil)
	session := &media.Session{
		StreamSID: "MZ-SINK",
		CallSID:   "CA-SINK",
		Params:    map[string]string{"language": "hi-IN"},
	}
	ctx := context.Background()

	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}

	frame := make([]byte, 320)
	if err := sink.OnAudio(ctx, session, frame); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}

	waitForASRConsumer(t, consumer, 4)
	if err := sink.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}

	if consumer.speechStarts != 1 || consumer.speechEnds != 1 {
		t.Fatalf("speech events start=%d end=%d", consumer.speechStarts, consumer.speechEnds)
	}
	if len(consumer.partials) != 1 || consumer.partials[0] != "partial text" {
		t.Fatalf("partials = %#v", consumer.partials)
	}
	if len(consumer.finals) != 1 || consumer.finals[0] != "final text" {
		t.Fatalf("finals = %#v", consumer.finals)
	}
}

func TestASRSinkNoopProvider(t *testing.T) {
	consumer := &recordingConsumer{}
	sink := media.NewASRSink(media.NoopASRProvider{}, consumer, 8000, nil)
	session := &media.Session{StreamSID: "MZ-NOOP"}
	ctx := context.Background()

	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	frame := []byte{0, 0, 1, 0}
	if err := sink.OnAudio(ctx, session, frame); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}
	if err := sink.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}
	if len(consumer.partials)+len(consumer.finals) != 0 {
		t.Fatal("expected no transcript events from noop asr")
	}
}

func TestASRSinkPreservesFrameLengthToProvider(t *testing.T) {
	provider := media.NewSarvamASRProvider("test-key", media.SarvamConfig{
		Endpoint:        startFrameCaptureServer(t),
		Model:           "saaras:v3",
		KeepalivePeriod: 0,
	}, nil)

	sink := media.NewASRSink(provider, &recordingConsumer{}, 8000, nil)
	session := &media.Session{StreamSID: "MZ-LEN"}
	ctx := context.Background()
	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	frame := make([]byte, 320)
	if err := sink.OnAudio(ctx, session, frame); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := sink.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}
}

func waitForASRConsumer(t *testing.T, c *recordingConsumer, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := c.speechStarts + c.speechEnds + len(c.partials) + len(c.finals)
		if got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("consumer events = %d, want >= %d", c.speechStarts+c.speechEnds+len(c.partials)+len(c.finals), want)
}
