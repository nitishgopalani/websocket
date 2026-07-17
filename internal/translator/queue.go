package translator

import (
	"sync"
	"time"
)

type relayFrame struct {
	payload   []byte
	arrivedAt time.Time
	// playAt schedules TTS egress at real-time cadence; zero means send ASAP (relay).
	playAt time.Time
}

// outboundQueue is a bounded per-leg queue with age-based drop at enqueue and dequeue.
type outboundQueue struct {
	mu          sync.Mutex
	frames      []relayFrame
	maxDepth    int
	maxAge      time.Duration
	onDropAge   func()
	onDropDepth func()
}

func newOutboundQueue(maxDepth int, maxAge time.Duration, onDropAge, onDropDepth func()) *outboundQueue {
	if maxDepth <= 0 {
		maxDepth = 100
	}
	return &outboundQueue{
		maxDepth:    maxDepth,
		maxAge:      maxAge,
		onDropAge:   onDropAge,
		onDropDepth: onDropDepth,
	}
}

func (q *outboundQueue) enqueue(payload []byte, arrivedAt time.Time) {
	q.enqueueFrame(relayFrame{payload: payload, arrivedAt: arrivedAt})
}

func (q *outboundQueue) enqueueFrame(f relayFrame) {
	if len(f.payload) == 0 {
		return
	}
	cp := make([]byte, len(f.payload))
	copy(cp, f.payload)
	f.payload = cp
	if f.arrivedAt.IsZero() {
		f.arrivedAt = time.Now()
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.dropExpiredLocked(time.Now())
	for len(q.frames) >= q.maxDepth {
		q.dropOldestLocked()
	}
	q.frames = append(q.frames, f)
}

func (q *outboundQueue) dequeue(now time.Time) (relayFrame, bool) {
	return q.dequeueReady(now)
}

// dequeueReady returns the head frame only when it is due (playAt zero or playAt <= now).
func (q *outboundQueue) dequeueReady(now time.Time) (relayFrame, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dropExpiredLocked(now)
	if len(q.frames) == 0 {
		return relayFrame{}, false
	}
	f := q.frames[0]
	if !f.playAt.IsZero() && now.Before(f.playAt) {
		return relayFrame{}, false
	}
	q.frames = q.frames[1:]
	if q.maxAge > 0 && now.Sub(f.arrivedAt) > q.maxAge {
		if q.onDropAge != nil {
			q.onDropAge()
		}
		return relayFrame{}, false
	}
	return f, true
}

// playAtSchedule returns scheduled playAt times for queued frames (tests).
func (q *outboundQueue) playAtSchedule() []time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]time.Time, len(q.frames))
	for i, f := range q.frames {
		out[i] = f.playAt
	}
	return out
}

func (q *outboundQueue) dropExpiredLocked(now time.Time) {
	if q.maxAge <= 0 {
		return
	}
	for len(q.frames) > 0 && now.Sub(q.frames[0].arrivedAt) > q.maxAge {
		q.frames = q.frames[1:]
		if q.onDropAge != nil {
			q.onDropAge()
		}
	}
}

func (q *outboundQueue) dropOldestLocked() {
	if len(q.frames) == 0 {
		return
	}
	q.frames = q.frames[1:]
	if q.onDropDepth != nil {
		q.onDropDepth()
	}
}

func (q *outboundQueue) clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.frames = nil
}

func (q *outboundQueue) isEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.frames) == 0
}
