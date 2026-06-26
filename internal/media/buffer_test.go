package media_test

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"websocket/internal/media"
)

type frameCollector struct {
	mu     sync.Mutex
	frames [][]byte
}

func (c *frameCollector) OnStart(_ context.Context, _ *media.Session) error { return nil }
func (c *frameCollector) OnDTMF(_ context.Context, _ *media.Session, _ string) error {
	return nil
}
func (c *frameCollector) OnStop(_ context.Context, _ *media.Session) error { return nil }

func (c *frameCollector) OnAudio(_ context.Context, _ *media.Session, frame []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	copied := make([]byte, len(frame))
	copy(copied, frame)
	c.frames = append(c.frames, copied)
	return nil
}

func (c *frameCollector) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.frames))
	for i, f := range c.frames {
		copied := make([]byte, len(f))
		copy(copied, f)
		out[i] = copied
	}
	return out
}

func pcm16Sample(b []byte, idx int) int16 {
	return int16(binary.LittleEndian.Uint16(b[idx*2:]))
}

func mulawSilence(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 0xFF
	}
	return b
}

func TestTranscodeSinkMuLawFixedFrames(t *testing.T) {
	target := media.TargetFormat{SampleRate: 16000, Channels: 1}
	frameBytes := target.FrameSizeBytes(20)
	collector := &frameCollector{}
	sink := media.NewTranscodeSink(collector, target, 20, nil)

	session := &media.Session{
		StreamSID: "MZ-TC1",
		Format: media.AudioFormat{
			Encoding:   "audio/x-mulaw",
			SampleRate: 8000,
			Channels:   1,
		},
	}

	ctx := context.Background()
	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}

	// 320 μ-law bytes -> 640 PCM16 samples at 8k -> 1280 samples at 16k -> two 20ms frames.
	chunks := [][]byte{
		mulawSilence(73),
		mulawSilence(107),
		mulawSilence(140),
	}
	totalIn := 0
	for _, chunk := range chunks {
		totalIn += len(chunk)
		if err := sink.OnAudio(ctx, session, chunk); err != nil {
			t.Fatalf("OnAudio: %v", err)
		}
	}
	if totalIn != 320 {
		t.Fatalf("input mulaw bytes = %d, want 320", totalIn)
	}
	if err := sink.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}

	frames := collector.snapshot()
	if len(frames) != 2 {
		t.Fatalf("downstream frames = %d, want 2", len(frames))
	}
	for i, frame := range frames {
		if len(frame) != frameBytes {
			t.Fatalf("frame[%d] len = %d, want %d", i, len(frame), frameBytes)
		}
	}

	inSamples := totalIn
	outSamples := 0
	for _, frame := range frames {
		outSamples += len(frame) / 2
	}
	if outSamples != inSamples*2 {
		t.Fatalf("output samples = %d, want %d", outSamples, inSamples*2)
	}
}

func TestTranscodeSinkMuLawSampleConservationIrregularChunks(t *testing.T) {
	target := media.TargetFormat{SampleRate: 8000, Channels: 1}
	frameBytes := target.FrameSizeBytes(20)
	collector := &frameCollector{}
	sink := media.NewTranscodeSink(collector, target, 20, nil)

	session := &media.Session{
		StreamSID: "MZ-TC2",
		Format: media.AudioFormat{
			Encoding:   "audio/x-mulaw",
			SampleRate: 8000,
			Channels:   1,
		},
	}
	ctx := context.Background()
	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}

	// 100 μ-law samples at 8k -> 100 PCM16 samples; repacketized into 20ms (160-sample) frames
	// with a final partial flush.
	totalIn := 0
	for _, size := range []int{17, 31, 52} {
		totalIn += size
		if err := sink.OnAudio(ctx, session, mulawSilence(size)); err != nil {
			t.Fatalf("OnAudio: %v", err)
		}
	}
	if err := sink.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}

	frames := collector.snapshot()
	outSamples := 0
	for _, frame := range frames {
		outSamples += len(frame) / 2
		if len(frame) > frameBytes {
			t.Fatalf("frame len %d exceeds max %d", len(frame), frameBytes)
		}
	}
	if outSamples != totalIn {
		t.Fatalf("output samples = %d, want %d", outSamples, totalIn)
	}
}

func TestTranscodeSinkL16PassthroughReframe(t *testing.T) {
	target := media.TargetFormat{SampleRate: 8000, Channels: 1}
	frameBytes := target.FrameSizeBytes(20)
	collector := &frameCollector{}
	sink := media.NewTranscodeSink(collector, target, 20, nil)

	session := &media.Session{
		StreamSID: "MZ-L16",
		Format: media.AudioFormat{
			Encoding:   "audio/x-l16",
			SampleRate: 8000,
			Channels:   1,
		},
	}
	ctx := context.Background()
	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}

	makePCM := func(values ...int16) []byte {
		b := make([]byte, len(values)*2)
		for i, v := range values {
			binary.LittleEndian.PutUint16(b[i*2:], uint16(v))
		}
		return b
	}

	chunks := [][]byte{
		makePCM(100, 200, 300),
		makePCM(400, 500),
	}
	totalSamples := 0
	for _, chunk := range chunks {
		totalSamples += len(chunk) / 2
		if err := sink.OnAudio(ctx, session, chunk); err != nil {
			t.Fatalf("OnAudio: %v", err)
		}
	}
	if err := sink.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}

	frames := collector.snapshot()
	reassembled := make([]byte, 0)
	for i, frame := range frames {
		isLast := i == len(frames)-1
		if len(frame) != frameBytes && !isLast {
			t.Fatalf("non-terminal frame len = %d, want %d", len(frame), frameBytes)
		}
		reassembled = append(reassembled, frame...)
	}
	if len(reassembled)/2 != totalSamples {
		t.Fatalf("reassembled samples = %d, want %d", len(reassembled)/2, totalSamples)
	}

	want := append(chunks[0], chunks[1]...)
	for i := 0; i < len(want)/2; i++ {
		if pcm16Sample(reassembled, i) != pcm16Sample(want, i) {
			t.Fatalf("sample[%d] = %d, want %d", i, pcm16Sample(reassembled, i), pcm16Sample(want, i))
		}
	}
}

func TestTranscodeSinkUnsupportedEncodingDoesNotCrash(t *testing.T) {
	collector := &frameCollector{}
	sink := media.NewTranscodeSink(collector, media.DefaultTargetFormat(), 20, nil)

	session := &media.Session{
		StreamSID: "MZ-BAD",
		Format: media.AudioFormat{
			Encoding:   "audio/opus",
			SampleRate: 48000,
		},
	}
	ctx := context.Background()
	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart should not fail session: %v", err)
	}
	if err := sink.OnAudio(ctx, session, []byte{1, 2, 3}); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}
	if err := sink.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}
	if len(collector.snapshot()) != 0 {
		t.Fatal("expected no downstream frames for unsupported encoding")
	}
}

func TestNewDecoderErrorIsUnsupportedEncoding(t *testing.T) {
	_, err := media.NewDecoder(media.AudioFormat{Encoding: "audio/opus"}, media.DefaultTargetFormat())
	if !errors.Is(err, media.ErrUnsupportedEncoding) {
		t.Fatalf("err = %v, want ErrUnsupportedEncoding", err)
	}
}
