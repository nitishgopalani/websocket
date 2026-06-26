package media

import (
	"container/heap"
	"sync"
	"time"
)

type fakeTimerEntry struct {
	at       time.Time
	index    int
	callback func()
	stopped  bool
}

type fakeTimerHeap []*fakeTimerEntry

func (h fakeTimerHeap) Len() int           { return len(h) }
func (h fakeTimerHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h fakeTimerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *fakeTimerHeap) Push(x any) {
	entry := x.(*fakeTimerEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *fakeTimerHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	*h = old[:n-1]
	return entry
}

type fakeTimerHandle struct {
	entry *fakeTimerEntry
	clock *FakeClock
}

func (t fakeTimerHandle) Stop() bool {
	if t.entry == nil || t.entry.stopped {
		return false
	}
	t.entry.stopped = true
	if t.clock != nil {
		idx := t.entry.index
		if idx >= 0 && idx < t.clock.timers.Len() {
			heap.Remove(&t.clock.timers, idx)
		}
	}
	return true
}

// FakeClock is an injectable clock for deterministic turn-manager tests.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers fakeTimerHeap
}

// NewFakeClock returns a fake clock starting at a fixed instant.
func NewFakeClock(start time.Time) *FakeClock {
	if start.IsZero() {
		start = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) AfterFunc(d time.Duration, f func()) TimerHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := &fakeTimerEntry{at: c.now.Add(d), callback: f}
	heap.Push(&c.timers, entry)
	return fakeTimerHandle{entry: entry, clock: c}
}

// Advance moves time forward and runs due timer callbacks in order.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	for c.timers.Len() > 0 && !c.timers[0].at.After(c.now) {
		entry := heap.Pop(&c.timers).(*fakeTimerEntry)
		if entry.stopped {
			continue
		}
		entry.index = -1
		cb := entry.callback
		c.mu.Unlock()
		if cb != nil {
			cb()
		}
		c.mu.Lock()
	}
	c.mu.Unlock()
}
