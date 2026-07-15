package callbridge

import (
	"sync"
	"time"
)

type relayFrame struct {
	payload   []byte
	arrivedAt time.Time
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
	if len(payload) == 0 {
		return
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	if arrivedAt.IsZero() {
		arrivedAt = time.Now()
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.dropExpiredLocked(time.Now())
	for len(q.frames) >= q.maxDepth {
		q.dropOldestLocked()
	}
	q.frames = append(q.frames, relayFrame{payload: cp, arrivedAt: arrivedAt})
}

func (q *outboundQueue) dequeue(now time.Time) (relayFrame, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dropExpiredLocked(now)
	if len(q.frames) == 0 {
		return relayFrame{}, false
	}
	f := q.frames[0]
	q.frames = q.frames[1:]
	if q.maxAge > 0 && now.Sub(f.arrivedAt) > q.maxAge {
		if q.onDropAge != nil {
			q.onDropAge()
		}
		return relayFrame{}, false
	}
	return f, true
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

func (q *outboundQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.frames)
}
