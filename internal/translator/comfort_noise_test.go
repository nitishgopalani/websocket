package translator

import (
	"testing"
	"time"
)

func TestComfortNoise_disabledInBidirectional(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: true, Bidirectional: true}}
	legB := &leg{
		server: srv,
		role:   LegB,
		outbound: newOutboundQueue(100, 0, nil, nil),
	}
	legB.paired.Store(true)
	legB.maybeInjectComfortNoise(time.Now())
	if !legB.outbound.isEmpty() {
		t.Fatal("bidirectional mode must not inject comfort noise")
	}
}

func TestComfortNoise_oneWayLegAOnly(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: true, Bidirectional: false}}
	legA := &leg{
		server: srv,
		role:   LegA,
		outbound: newOutboundQueue(100, 0, nil, nil),
	}
	legB := &leg{server: srv, role: LegB, outbound: newOutboundQueue(100, 0, nil, nil)}
	legB.paired.Store(true)
	legB.maybeInjectComfortNoise(time.Now())
	if !legB.outbound.isEmpty() {
		t.Fatal("one-way comfort noise must not inject on leg B")
	}
	legA.paired.Store(true)
	legA.maybeInjectComfortNoise(time.Now())
	if legA.outbound.isEmpty() {
		t.Fatal("one-way comfort noise should inject on leg A when quiet")
	}
}

func TestNotifyPeerUplink_suppressesPeerComfort(t *testing.T) {
	srv := &Server{cfg: Config{TranslationEnabled: true, Bidirectional: false}}
	legA := &leg{server: srv, role: LegA, outbound: newOutboundQueue(100, 0, nil, nil)}
	legB := &leg{server: srv, role: LegB, outbound: newOutboundQueue(100, 0, nil, nil), peer: legA}
	legA.peer = legB
	legA.paired.Store(true)
	legB.paired.Store(true)
	legA.notifyPeerUplink(time.Now())
	legB.maybeInjectComfortNoise(time.Now())
	if !legB.outbound.isEmpty() {
		t.Fatal("peer uplink should suppress comfort noise on leg A")
	}
}
