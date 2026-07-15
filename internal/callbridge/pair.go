package callbridge

import (
	"strings"
	"sync"
	"time"
)

// LegRole identifies which side of a bridge pair a session occupies.
type LegRole string

const (
	LegA LegRole = "a"
	LegB LegRole = "b"
)

type pairingInfo struct {
	bridgeID string
	leg      LegRole
}

func pairingFromQuery(q map[string][]string) pairingInfo {
	bridgeID := strings.TrimSpace(firstQuery(q, "bridge"))
	leg := LegRole(strings.ToLower(strings.TrimSpace(firstQuery(q, "leg"))))
	switch leg {
	case LegA, LegB:
	default:
		leg = ""
	}
	return pairingInfo{bridgeID: bridgeID, leg: leg}
}

func pairingFromMetadata(meta map[string]string, base pairingInfo) pairingInfo {
	out := base
	if v := strings.TrimSpace(meta["bridge_id"]); v != "" {
		out.bridgeID = v
	}
	if v := strings.ToLower(strings.TrimSpace(meta["bridge_leg"])); v == "a" || v == "b" {
		out.leg = LegRole(v)
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
	id       string
	server   *Server
	created  time.Time
	timer    *time.Timer
	mu       sync.Mutex
	legA     *session
	legB     *session
	paired   bool
	closed   bool
}

func newRoom(id string, s *Server) *room {
	r := &room{
		id:      id,
		server:  s,
		created: time.Now(),
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
	var waiting *session
	if r.legA != nil && r.legB == nil {
		waiting = r.legA
	} else if r.legB != nil && r.legA == nil {
		waiting = r.legB
	}
	r.closed = true
	if waiting != nil {
		waiting.sendError("pairing_timeout", "bridge pairing timed out")
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

func (r *room) attach(sess *session, role LegRole, rates audioRates) error {
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
		r.mu.Unlock()
		legA.signalReady()
		legB.signalReady()
		r.server.logger.Info("bridge_paired",
			"bridge_id", bridgeID,
			"leg_a_session", legA.sessionID,
			"leg_b_session", legB.sessionID,
		)
		return nil
	}
	r.server.metrics.incWaitingLegs()
	r.mu.Unlock()
	return nil
}

func (r *room) detach(sess *session, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if r.timer != nil {
		r.timer.Stop()
	}

	var survivor *session
	if r.legA == sess {
		r.legA = nil
		survivor = r.legB
	} else if r.legB == sess {
		r.legB = nil
		survivor = r.legA
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
		r.server.logger.Info("bridge_teardown",
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

func (reg *registry) getOrCreate(id string, s *Server) *room {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if r, ok := reg.rooms[id]; ok {
		return r
	}
	r := newRoom(id, s)
	reg.rooms[id] = r
	return r
}

func (reg *registry) remove(id string) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	delete(reg.rooms, id)
}

func (reg *registry) pairedCount() int {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	n := 0
	for _, r := range reg.rooms {
		r.mu.Lock()
		if r.paired {
			n++
		}
		r.mu.Unlock()
	}
	return n
}

func (reg *registry) waitingCount() int {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	n := 0
	for _, r := range reg.rooms {
		r.mu.Lock()
		if !r.paired && !r.closed && (r.legA != nil || r.legB != nil) {
			n++
		}
		r.mu.Unlock()
	}
	return n
}
