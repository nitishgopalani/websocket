package media_test

import (
	"context"
	"testing"

	"websocket/internal/media"
)

type collectSink struct {
	frames [][]byte
}

func (c *collectSink) OnStart(_ context.Context, _ *media.Session) error { return nil }
func (c *collectSink) OnDTMF(_ context.Context, _ *media.Session, _ string) error {
	return nil
}
func (c *collectSink) OnStop(_ context.Context, _ *media.Session) error { return nil }

func (c *collectSink) OnAudio(_ context.Context, _ *media.Session, frame []byte) error {
	copied := make([]byte, len(frame))
	copy(copied, frame)
	c.frames = append(c.frames, copied)
	return nil
}

func TestDenoiseSinkNoopPassthrough(t *testing.T) {
	collector := &collectSink{}
	sink := media.NewDenoiseSink(collector, media.NoopDenoiser{}, media.NoopAEC{}, 8000, nil)

	session := &media.Session{StreamSID: "MZ-DN1"}
	ctx := context.Background()
	frame := []byte{0x10, 0x00, 0x20, 0x00}

	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	if err := sink.OnAudio(ctx, session, frame); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}
	if err := sink.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}

	if len(collector.frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(collector.frames))
	}
	if string(collector.frames[0]) != string(frame) {
		t.Fatalf("frame = %v, want %v", collector.frames[0], frame)
	}
	if sink.FramesDenoised() != 1 {
		t.Fatalf("frames_denoised = %d, want 1", sink.FramesDenoised())
	}
}

type errorDenoiser struct{}

func (errorDenoiser) Process(_ context.Context, _ []byte, _ int) ([]byte, error) {
	return nil, context.DeadlineExceeded
}
func (errorDenoiser) Close() error { return nil }

func TestDenoiseSinkFailOpenForwardsOriginal(t *testing.T) {
	collector := &collectSink{}
	sink := media.NewDenoiseSink(collector, errorDenoiser{}, media.NoopAEC{}, 8000, nil)

	session := &media.Session{StreamSID: "MZ-DN2"}
	ctx := context.Background()
	frame := []byte{0x01, 0x02, 0x03, 0x04}

	if err := sink.OnAudio(ctx, session, frame); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}
	if sink.Fallbacks() != 1 {
		t.Fatalf("fallbacks = %d, want 1", sink.Fallbacks())
	}
	if len(collector.frames) != 1 || string(collector.frames[0]) != string(frame) {
		t.Fatalf("expected original frame forwarded, got %v", collector.frames)
	}
}

func TestDenoiseSinkPreservesFrameLength(t *testing.T) {
	collector := &collectSink{}
	sink := media.NewDenoiseSink(collector, media.NoopDenoiser{}, media.NoopAEC{}, 8000, nil)
	session := &media.Session{StreamSID: "MZ-DN3"}
	ctx := context.Background()

	frame := make([]byte, 320) // 20ms @ 8k PCM16
	for i := range frame {
		frame[i] = byte(i)
	}
	if err := sink.OnAudio(ctx, session, frame); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}
	if len(collector.frames[0]) != len(frame) {
		t.Fatalf("len = %d, want %d", len(collector.frames[0]), len(frame))
	}
}

func TestDenoiseSinkWithRemoteWorker(t *testing.T) {
	// Exercise chain: DenoiseSink -> collector using in-process fake via TCP worker setup in media tests.
	// Here we validate integration with a flipping denoiser stub.
	collector := &collectSink{}
	flipper := flipDenoiser{}
	sink := media.NewDenoiseSink(collector, flipper, media.NoopAEC{}, 8000, nil)

	session := &media.Session{StreamSID: "MZ-DN4"}
	ctx := context.Background()
	frame := []byte{0x00, 0x01}

	if err := sink.OnAudio(ctx, session, frame); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}
	if collector.frames[0][0] != 0xFF || collector.frames[0][1] != 0xFE {
		t.Fatalf("expected flipped bytes, got %v", collector.frames[0])
	}
}

type flipDenoiser struct{}

func (flipDenoiser) Process(_ context.Context, pcm16 []byte, _ int) ([]byte, error) {
	out := make([]byte, len(pcm16))
	for i := range pcm16 {
		out[i] = pcm16[i] ^ 0xFF
	}
	return out, nil
}
func (flipDenoiser) Close() error { return nil }
