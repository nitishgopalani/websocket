package translator

import (
	"strings"
	"sync"
	"time"
)

// LegRole identifies which side of a translator pair a session occupies.
type LegRole string

const (
	LegA LegRole = "a"
	LegB LegRole = "b"
)

type pairingInfo struct {
	bridgeID string
	leg      LegRole
	langA    string
	langB    string
}

func pairingFromQuery(q map[string][]string) pairingInfo {
	bridgeID := strings.TrimSpace(firstQuery(q, "bridge"))
	leg := LegRole(strings.ToLower(strings.TrimSpace(firstQuery(q, "leg"))))
	switch leg {
	case LegA, LegB:
	default:
		leg = ""
	}
	return pairingInfo{
		bridgeID: bridgeID,
		leg:      leg,
		langA:    strings.TrimSpace(firstQuery(q, "lang_a")),
		langB:    strings.TrimSpace(firstQuery(q, "lang_b")),
	}
}

func pairingFromMetadata(meta map[string]string, base pairingInfo) pairingInfo {
	out := base
	if v := strings.TrimSpace(meta["bridge_id"]); v != "" {
		out.bridgeID = v
	}
	if v := strings.ToLower(strings.TrimSpace(meta["bridge_leg"])); v == "a" || v == "b" {
		out.leg = LegRole(v)
	}
	if v := strings.TrimSpace(meta["lang_a"]); v != "" {
		out.langA = v
	}
	if v := strings.TrimSpace(meta["lang_b"]); v != "" {
		out.langB = v
	}
	return out
}

func firstQuery(q map[string][]string, key string) string {
	if vals, ok := q[key]; ok && len(vals) > 0 {
		return vals[0]
	}
	return ""
}

type audioRates struct {
	inputRate  int
	outputRate int
}

func ratesMatch(a, b audioRates) bool {
	return a.inputRate == b.inputRate && a.outputRate == b.outputRate
}

// room holds up to two legs waiting to be paired.
type room struct {
	id      string
	server  *Server
	created time.Time
	timer   *time.Timer
	mu      sync.Mutex
	legA    *leg
	legB    *leg
	paired  bool
	closed  bool
	langA   string
	langB   string
	lane    *translationLane
	laneBToA *translationLane
	coord   *roomCoordinator
	breaker *loopBreaker
}

func newRoom(id string, s *Server, langA, langB string) *room {
	r := &room{
		id:      id,
		server:  s,
		created: time.Now(),
		langA:   langA,
		langB:   langB,
	}
	r.timer = time.AfterFunc(s.cfg.PairingTimeout, r.onPairingTimeout)
	return r
}

func (r *room) onPairingTimeout() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.paired {
		return
	}
	if r.legA != nil && r.legB != nil {
		return
	}
	r.server.metrics.incPairing("timeout")
	var waiting *leg
	if r.legA != nil && r.legB == nil {
		waiting = r.legA
	} else if r.legB != nil && r.legA == nil {
		waiting = r.legB
	}
	r.closed = true
	if waiting != nil {
		waiting.sendError("pairing_timeout", "translator pairing timed out")
		time.Sleep(10 * time.Millisecond)
		waiting.closeConn()
	}
	if r.legA != nil && r.legA != waiting {
		r.legA.closeConn()
	}
	if r.legB != nil && r.legB != waiting {
		r.legB.closeConn()
	}
	r.server.registry.remove(r.id)
	r.server.metrics.decWaitingLegs()
}

func (r *room) attach(sess *leg, role LegRole, rates audioRates) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errRoomClosed
	}
	if r.paired {
		r.mu.Unlock()
		return errBridgeFull
	}

	switch role {
	case LegA:
		if r.legA != nil {
			r.mu.Unlock()
			return errLegTaken
		}
		r.legA = sess
	case LegB:
		if r.legB != nil {
			r.mu.Unlock()
			return errLegTaken
		}
		r.legB = sess
	default:
		if r.legA == nil {
			role = LegA
			r.legA = sess
		} else if r.legB == nil {
			role = LegB
			r.legB = sess
		} else {
			r.mu.Unlock()
			return errBridgeFull
		}
	}
	sess.role = role
	sess.rates = rates

	if r.legA != nil && r.legB != nil {
		if !ratesMatch(r.legA.rates, r.legB.rates) {
			if r.legB == sess {
				r.legB = nil
			} else if r.legA == sess {
				r.legA = nil
			}
			r.server.metrics.incPairing("rejected")
			r.mu.Unlock()
			return errRateMismatch
		}
		r.paired = true
		if r.timer != nil {
			r.timer.Stop()
		}
		r.legA.peer = r.legB
		r.legB.peer = r.legA
		r.server.metrics.incPairing("ok")
		r.server.metrics.incActivePairs()
		r.server.metrics.decWaitingLegs()
		legA := r.legA
		legB := r.legB
		bridgeID := r.id
		langA := r.langA
		langB := r.langB
		r.mu.Unlock()

		if err := r.server.onRoomPaired(r, legA, legB, langA, langB); err != nil {
			r.server.logger.Warn("translator lane setup failed; fail-open relay",
				"bridge_id", bridgeID, "error", err)
		}

		legA.signalReady()
		legB.signalReady()
		r.server.logger.Info("translator_paired",
			"bridge_id", bridgeID,
			"leg_a_session", legA.sessionID,
			"leg_b_session", legB.sessionID,
			"translation_enabled", r.server.cfg.TranslationEnabled,
			"bidirectional", r.server.cfg.Bidirectional,
			"lang_a", langA,
			"lang_b", langB,
			"b_to_a_raw_relay", !r.server.cfg.Bidirectional,
			"comfort_noise_when_peer_quiet", r.server.cfg.TranslationEnabled,
		)
		return nil
	}
	r.server.metrics.incWaitingLegs()
	r.mu.Unlock()
	// Early ready: connector default ready timeout is 5s on deployed boxes; the
	// second leg may still be ringing. Unblock media without waiting for pair.
	sess.signalReady()
	return nil
}

func (r *room) detach(sess *leg, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if r.timer != nil {
		r.timer.Stop()
	}

	var survivor *leg
	if r.legA == sess {
		r.legA = nil
		survivor = r.legB
	} else if r.legB == sess {
		r.legB = nil
		survivor = r.legA
	}

	if r.paired {
		r.server.teardownLane(r)
	}

	if !r.paired {
		r.closed = true
		r.server.registry.remove(r.id)
		r.server.metrics.decWaitingLegs()
		if survivor != nil {
			survivor.closeConn()
		}
		return
	}

	r.closed = true
	r.paired = false
	r.server.registry.remove(r.id)
	r.server.metrics.incTeardown(reason)
	r.server.metrics.decActivePairs()

	departed := sess.sessionID
	if survivor != nil {
		r.server.logger.Info("translator_teardown",
			"bridge_id", r.id,
			"reason", reason,
			"survivor_session", survivor.sessionID,
			"departed_session", departed,
		)
		survivor.sendEndOfCall()
		survivor.closeConn()
	}
}

type registry struct {
	mu    sync.Mutex
	rooms map[string]*room
}

func newRegistry() *registry {
	return &registry{rooms: make(map[string]*room)}
}

func (reg *registry) getOrCreate(id string, s *Server, langA, langB string) *room {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if r, ok := reg.rooms[id]; ok {
		if langA != "" {
			r.langA = langA
		}
		if langB != "" {
			r.langB = langB
		}
		return r
	}
	r := newRoom(id, s, langA, langB)
	reg.rooms[id] = r
	return r
}

func (reg *registry) remove(id string) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	delete(reg.rooms, id)
}
