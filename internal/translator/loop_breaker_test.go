package translator

import (
	"testing"
	"time"
)

func TestLoopBreaker_tripsOnRate(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	current := now
	b := newLoopBreaker(3, 10*time.Second, nil, nil)
	b.SetClock(func() time.Time { return current })

	for i := 0; i < 3; i++ {
		b.Record(LegA, "hello", "namaste")
	}
	if b.Tripped() {
		t.Fatal("should not trip at exactly max")
	}
	b.Record(LegA, "again", "phir")
	if !b.Tripped() {
		t.Fatal("expected trip above max translations in window")
	}
}

func TestLoopBreaker_tripsOnPingPong(t *testing.T) {
	b := newLoopBreaker(20, 10*time.Second, nil, nil)
	b.Record(LegA, "hello", "namaste")
	b.Record(LegB, "namaste", "hello")
	if !b.Tripped() {
		t.Fatal("expected ping-pong trip")
	}
}

func TestLoopBreaker_fallsBackWhenTripped(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: true, Bidirectional: true}}
	room := &room{
		server: srv,
		lane:   &translationLane{},
		laneBToA: &translationLane{},
		breaker: newLoopBreaker(1, 10*time.Second, nil, nil),
	}
	room.breaker.Record(LegA, "a", "b")
	room.breaker.Record(LegA, "c", "d")
	if !room.loopBreakerTripped() {
		t.Fatal("expected tripped")
	}
	legA := &leg{server: srv, room: room, role: LegA}
	if !legA.shouldRelayToPeer() {
		t.Fatal("loop breaker should restore raw relay on leg A")
	}
	if room.allowASRIngest(LegA) {
		t.Fatal("ASR ingest disabled when loop breaker tripped")
	}
}

func TestTextsSimilar(t *testing.T) {
	if !textsSimilar("hello", "hello") {
		t.Fatal("exact match")
	}
	if !textsSimilar("helloworld", "hello") {
		t.Fatal("substring match")
	}
	if textsSimilar("abc", "xyz") {
		t.Fatal("unrelated")
	}
}
