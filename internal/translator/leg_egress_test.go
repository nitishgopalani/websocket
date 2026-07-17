package translator

import (
	"context"
	"testing"
	"time"

	"websocket/internal/media"
)

func TestLegEgress_frames8kPCM_at20ms(t *testing.T) {
	leg := &leg{outbound: newOutboundQueue(100, 0, nil, nil)}
	e := newLegEgress(leg, 8000, 20, nil, nil, nil)

	// 20ms @ 8kHz PCM16 mono = 320 bytes
	frame := make([]byte, 320)
	for i := range frame {
		frame[i] = byte(i % 256)
	}
	if err := e.SendAudio(context.Background(), nil, media.TTSAudioChunk{
		TurnID: "t1",
		MuLaw:  frame,
		Final:  true,
	}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	pending := len(e.pending)
	e.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending=%d want 0 after exact frame", pending)
	}
}

func TestLegEgress_finalPadsPartialFrame(t *testing.T) {
	leg := &leg{outbound: newOutboundQueue(100, 0, nil, nil)}
	start := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	e := newLegEgress(leg, 8000, 20, nil, nil, nil)
	e.now = func() time.Time { return start }

	half := make([]byte, 160)
	if err := e.SendAudio(context.Background(), nil, media.TTSAudioChunk{
		TurnID: "t1",
		MuLaw:  half,
	}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	pending := len(e.pending)
	e.mu.Unlock()
	if pending != 160 {
		t.Fatalf("pending after half frame=%d want 160", pending)
	}

	if err := e.SendAudio(context.Background(), nil, media.TTSAudioChunk{
		TurnID: "t1",
		Final:  true,
	}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	pending = len(e.pending)
	e.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending after final=%d want 0 (padded flush)", pending)
	}

	schedule := leg.outbound.playAtSchedule()
	if len(schedule) != 1 {
		t.Fatalf("outbound frames=%d want 1 padded frame", len(schedule))
	}
	f, ok := leg.outbound.dequeueReady(start)
	if !ok || len(f.payload) != 320 {
		t.Fatalf("dequeue ok=%v len=%d want 320", ok, len(f.payload))
	}
	for i := 160; i < 320; i++ {
		if f.payload[i] != 0 {
			t.Fatalf("pad byte[%d]=%d want 0 silence", i, f.payload[i])
		}
	}
}

func TestLegEgress_turnBoundaryDropsStalePartial(t *testing.T) {
	leg := &leg{outbound: newOutboundQueue(100, 0, nil, nil)}
	e := newLegEgress(leg, 8000, 20, nil, nil, nil)

	half := make([]byte, 160)
	if err := e.SendAudio(context.Background(), nil, media.TTSAudioChunk{
		TurnID: "t1",
		MuLaw:  half,
	}); err != nil {
		t.Fatal(err)
	}

	if err := e.SendAudio(context.Background(), nil, media.TTSAudioChunk{
		TurnID: "t2",
		MuLaw:  make([]byte, 320),
		Final:  true,
	}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	active := e.activeTurnID
	e.mu.Unlock()
	if active != "t2" {
		t.Fatalf("activeTurnID=%q want t2", active)
	}
	schedule := leg.outbound.playAtSchedule()
	if len(schedule) != 1 {
		t.Fatalf("outbound frames=%d want 1 from t2 only", len(schedule))
	}
}

func TestLegEgress_burstPacedAt20msCadence(t *testing.T) {
	leg := &leg{outbound: newOutboundQueue(100, 0, nil, nil)}
	start := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	e := newLegEgress(leg, 8000, 20, nil, nil, nil)
	e.now = func() time.Time { return start }

	burst := make([]byte, 320*4)
	for i := range burst {
		burst[i] = byte(i % 256)
	}
	if err := e.SendAudio(context.Background(), nil, media.TTSAudioChunk{
		TurnID: "t1",
		MuLaw:  burst,
		Final:  true,
	}); err != nil {
		t.Fatal(err)
	}

	schedule := leg.outbound.playAtSchedule()
	if len(schedule) != 4 {
		t.Fatalf("scheduled frames=%d want 4", len(schedule))
	}
	for i := 1; i < len(schedule); i++ {
		gap := schedule[i].Sub(schedule[i-1])
		if gap != 20*time.Millisecond {
			t.Fatalf("frame[%d] gap=%v want 20ms", i, gap)
		}
	}

	// Not all frames due at once — only first is ready at start.
	ready := 0
	for i := 0; i < 4; i++ {
		at := start.Add(time.Duration(i) * 20 * time.Millisecond)
		if _, ok := leg.outbound.dequeueReady(at); ok {
			ready++
		}
	}
	if ready != 4 {
		t.Fatalf("dequeued at cadence=%d want 4 (not burst at t=0)", ready)
	}
	if _, again := leg.outbound.dequeueReady(start.Add(80 * time.Millisecond)); again {
		t.Fatal("expected empty queue after paced drain")
	}
}

func TestOutboundQueue_relayUnpacedDequeuesImmediately(t *testing.T) {
	q := newOutboundQueue(10, 0, nil, nil)
	now := time.Now()
	q.enqueue([]byte{1, 2}, now)
	f, ok := q.dequeueReady(now)
	if !ok || len(f.payload) != 2 {
		t.Fatalf("relay dequeue ok=%v len=%d", ok, len(f.payload))
	}
	if !f.playAt.IsZero() {
		t.Fatalf("relay playAt=%v want zero", f.playAt)
	}
}

func TestLegEgress_clearPlaybackResetsPacing(t *testing.T) {
	leg := &leg{outbound: newOutboundQueue(100, 0, nil, nil)}
	start := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	e := newLegEgress(leg, 8000, 20, nil, nil, nil)
	e.now = func() time.Time { return start }

	_ = e.SendAudio(context.Background(), nil, media.TTSAudioChunk{
		TurnID: "t1",
		MuLaw:  make([]byte, 640),
	})
	if leg.outbound.playAtSchedule(); len(leg.outbound.playAtSchedule()) == 0 {
		t.Fatal("expected paced frames before clear")
	}

	if err := e.ClearPlayback(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if n := len(leg.outbound.playAtSchedule()); n != 0 {
		t.Fatalf("outbound after clear=%d want 0", n)
	}

	later := start.Add(500 * time.Millisecond)
	e.now = func() time.Time { return later }
	_ = e.SendAudio(context.Background(), nil, media.TTSAudioChunk{
		TurnID: "t2",
		MuLaw:  make([]byte, 320),
		Final:  true,
	})
	schedule := leg.outbound.playAtSchedule()
	if len(schedule) != 1 {
		t.Fatalf("frames after clear+speak=%d want 1", len(schedule))
	}
	if !schedule[0].Equal(later) {
		t.Fatalf("first playAt after clear=%v want %v (scheduler reset)", schedule[0], later)
	}
}

func TestLegEgress_resampled8kFrom16kInput(t *testing.T) {
	in16k := make([]byte, 640) // 20ms @ 16k
	for i := range in16k {
		in16k[i] = byte(i % 256)
	}
	out8k, err := media.ResamplePCM16Linear(in16k, 16000, 8000)
	if err != nil {
		t.Fatal(err)
	}
	if len(out8k) != 320 {
		t.Fatalf("resampled bytes=%d want 320", len(out8k))
	}
	leg := &leg{outbound: newOutboundQueue(100, 0, nil, nil)}
	e := newLegEgress(leg, 8000, 20, nil, nil, nil)
	if err := e.SendAudio(context.Background(), nil, media.TTSAudioChunk{TurnID: "t1", MuLaw: out8k, Final: true}); err != nil {
		t.Fatal(err)
	}
}
