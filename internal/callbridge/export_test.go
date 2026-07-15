package callbridge

import "time"

// Test hooks exported for bridge_test package.

type TestFrame struct {
	Payload   []byte
	ArrivedAt time.Time
}

// NewOutboundQueueForTest builds a queue wired to metrics for white-box tests.
func NewOutboundQueueForTest(maxDepth int, maxAge time.Duration, m *Metrics, direction string) *testQueue {
	q := newOutboundQueue(
		maxDepth,
		maxAge,
		func() { m.incDropAge(direction) },
		func() { m.incDropDepth(direction) },
	)
	return &testQueue{q: q}
}

type testQueue struct {
	q *outboundQueue
}

func (t *testQueue) EnqueueForTest(payload []byte, arrivedAt time.Time) {
	t.q.enqueue(payload, arrivedAt)
}

func (t *testQueue) DequeueForTest(now time.Time) (TestFrame, bool) {
	f, ok := t.q.dequeue(now)
	if !ok {
		return TestFrame{}, false
	}
	return TestFrame{Payload: f.payload, ArrivedAt: f.arrivedAt}, true
}
