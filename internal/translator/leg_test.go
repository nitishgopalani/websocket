package translator

import (
	"sync/atomic"
	"testing"
)

func TestLeg_shouldRelay_mutesAToBWhenTranslationActive(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: true}}
	room := &room{lane: &translationLane{}}
	legA := &leg{server: srv, room: room, role: LegA, peer: &leg{}}
	legA.paired.Store(true)
	if legA.shouldRelayToPeer() {
		t.Fatal("expected A→B relay muted when translation active")
	}
	room.lane.failOpen.Store(true)
	if !legA.shouldRelayToPeer() {
		t.Fatal("expected fail-open raw relay")
	}
}

func TestLeg_shouldRelay_bToAMutedWhenBidirectional(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: true, Bidirectional: true}}
	room := &room{laneBToA: &translationLane{}}
	legB := &leg{server: srv, room: room, role: LegB, peer: &leg{}}
	legB.paired.Store(true)
	if legB.shouldRelayToPeer() {
		t.Fatal("expected B→A relay muted in 3b-ii")
	}
	room.laneBToA.failOpen.Store(true)
	if !legB.shouldRelayToPeer() {
		t.Fatal("expected B→A fail-open raw relay")
	}
}

func TestLeg_shouldRelay_bToAAlwaysWhenOneWay(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: true, Bidirectional: false}}
	room := &room{lane: &translationLane{}}
	legB := &leg{server: srv, room: room, role: LegB, peer: &leg{}}
	legB.paired.Store(true)
	if !legB.shouldRelayToPeer() {
		t.Fatal("expected B→A relay allowed in 3b-i one-way")
	}
}

func TestLeg_shouldRelay_whenTranslationDisabled(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: false}}
	legA := &leg{server: srv, role: LegA, peer: &leg{}}
	legA.paired.Store(true)
	if !legA.shouldRelayToPeer() {
		t.Fatal("expected relay when translation off")
	}
}

func TestRoom_laneIsolationBindings(t *testing.T) {
	legA := &leg{role: LegA, sessionID: "sess-a"}
	legB := &leg{role: LegB, sessionID: "sess-b"}
	laneAToB := &translationLane{direction: DirectionAToB, sourceLeg: legA, targetLeg: legB}
	laneBToA := &translationLane{direction: DirectionBToA, sourceLeg: legB, targetLeg: legA}
	room := &room{lane: laneAToB, laneBToA: laneBToA}

	if room.laneForSource(LegA) != laneAToB {
		t.Fatal("A uplink must bind to A→B lane")
	}
	if room.laneForSource(LegB) != laneBToA {
		t.Fatal("B uplink must bind to B→A lane")
	}
	if laneAToB.sourceLeg != legA || laneAToB.targetLeg != legB {
		t.Fatal("A→B lane isolation violated")
	}
	if laneBToA.sourceLeg != legB || laneBToA.targetLeg != legA {
		t.Fatal("B→A lane isolation violated")
	}
	if laneAToB.targetLeg == legA {
		t.Fatal("A must never hear A→B TTS on own leg")
	}
}

func TestRoom_perLaneFailOpenIndependent(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: true, Bidirectional: true}}
	room := &room{
		server:   srv,
		lane:     &translationLane{},
		laneBToA: &translationLane{},
	}
	room.lane.failOpen.Store(true)
	legA := &leg{server: srv, room: room, role: LegA, peer: &leg{}}
	legB := &leg{server: srv, room: room, role: LegB, peer: &leg{}}
	legA.paired.Store(true)
	legB.paired.Store(true)

	if !legA.shouldRelayToPeer() {
		t.Fatal("A→B fail-open should restore A relay")
	}
	if legB.shouldRelayToPeer() {
		t.Fatal("B→A lane healthy should keep B relay muted")
	}

	room.lane.failOpen.Store(false)
	room.laneBToA.failOpen.Store(true)
	if legA.shouldRelayToPeer() {
		t.Fatal("A→B healthy should mute A relay")
	}
	if !legB.shouldRelayToPeer() {
		t.Fatal("B→A fail-open should restore B relay")
	}
}

func TestRoom_allowASRIngest_oneWayOnlyLegA(t *testing.T) {
	room := &room{lane: &translationLane{}}
	if !room.allowASRIngest(LegA) {
		t.Fatal("3b-i allows A ingest")
	}
	if room.allowASRIngest(LegB) {
		t.Fatal("3b-i blocks B ingest (no B→A lane)")
	}
}

var _ = atomic.Bool{}
