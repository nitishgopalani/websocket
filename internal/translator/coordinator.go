package translator

import (
	"log/slog"
	"sync"
	"time"
)

type floorState int

const (
	floorIdle floorState = iota
	floorACapturing
	floorBCapturing
	floorAPlaying
	floorBPlaying
)

func peerRole(role LegRole) LegRole {
	if role == LegA {
		return LegB
	}
	return LegA
}

// roomCoordinator enforces half-duplex floor control and echo-aware ASR gating.
type roomCoordinator struct {
	echoTail      time.Duration
	floorDebounce time.Duration
	now           func() time.Time
	afterFunc     func(time.Duration, func()) *time.Timer
	logger        *slog.Logger

	mu               sync.Mutex
	state            floorState
	asrMutedUntil    map[LegRole]time.Time
	pendingReleaseAt time.Time
	releaseTimer     *time.Timer
	firstCaptureAt   time.Time
	firstCaptureRole LegRole
	disabled         bool
}

func newRoomCoordinator(cfg Config, logger *slog.Logger) *roomCoordinator {
	echoTail := cfg.EchoTail
	if echoTail <= 0 {
		echoTail = time.Duration(defaultEchoTailMS) * time.Millisecond
	}
	debounce := cfg.FloorDebounce
	if debounce <= 0 {
		debounce = time.Duration(defaultFloorDebounceMS) * time.Millisecond
	}
	return &roomCoordinator{
		echoTail:      echoTail,
		floorDebounce: debounce,
		now:           time.Now,
		afterFunc:     time.AfterFunc,
		logger:        logger,
		asrMutedUntil: make(map[LegRole]time.Time),
	}
}

func (c *roomCoordinator) SetClock(now func() time.Time, after func(time.Duration, func()) *time.Timer) {
	if now != nil {
		c.now = now
	}
	if after != nil {
		c.afterFunc = after
	}
}

func (c *roomCoordinator) Disable() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disabled = true
	c.state = floorIdle
	c.clearReleaseTimerLocked()
}

func (c *roomCoordinator) State() floorState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *roomCoordinator) AllowASRIngest(role LegRole) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disabled {
		return false
	}
	now := c.now()
	if until, ok := c.asrMutedUntil[role]; ok && now.Before(until) {
		return false
	}
	switch c.state {
	case floorIdle:
		return true
	case floorACapturing, floorAPlaying:
		return role == LegA
	case floorBCapturing, floorBPlaying:
		return role == LegB
	default:
		return false
	}
}

func (c *roomCoordinator) OnSpeechStart(role LegRole) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disabled {
		return
	}
	now := c.now()
	switch c.state {
	case floorIdle:
		if c.firstCaptureAt.IsZero() || now.Sub(c.firstCaptureAt) > c.floorDebounce {
			c.firstCaptureAt = now
			c.firstCaptureRole = role
		} else if role != c.firstCaptureRole {
			return
		}
		if c.firstCaptureRole == LegA {
			c.state = floorACapturing
		} else {
			c.state = floorBCapturing
		}
	case floorACapturing, floorAPlaying:
		if role != LegA {
			return
		}
	case floorBCapturing, floorBPlaying:
		if role != LegB {
			return
		}
	}
}

func (c *roomCoordinator) withinDebounce(role LegRole, now time.Time) bool {
	return !c.firstCaptureAt.IsZero() && now.Sub(c.firstCaptureAt) <= c.floorDebounce && c.firstCaptureRole == role
}

func (c *roomCoordinator) OnTTSStarted(source LegRole) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disabled {
		return
	}
	target := peerRole(source)
	if source == LegA {
		c.state = floorAPlaying
	} else {
		c.state = floorBPlaying
	}
	c.asrMutedUntil[target] = c.now().Add(24 * time.Hour)
}

func (c *roomCoordinator) OnPlaybackComplete(target LegRole, playbackEnd time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disabled {
		return
	}
	releaseAt := playbackEnd.Add(c.echoTail)
	c.asrMutedUntil[target] = releaseAt
	c.scheduleFloorReleaseLocked(releaseAt)
}

func (c *roomCoordinator) scheduleFloorReleaseLocked(at time.Time) {
	c.pendingReleaseAt = at
	c.clearReleaseTimerLocked()
	delay := at.Sub(c.now())
	if delay < 0 {
		delay = 0
	}
	c.releaseTimer = c.afterFunc(delay, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.disabled {
			return
		}
		if !c.pendingReleaseAt.IsZero() && c.now().Before(c.pendingReleaseAt) {
			return
		}
		c.state = floorIdle
		c.pendingReleaseAt = time.Time{}
		c.firstCaptureAt = time.Time{}
		c.firstCaptureRole = ""
	})
}

func (c *roomCoordinator) clearReleaseTimerLocked() {
	if c.releaseTimer != nil {
		c.releaseTimer.Stop()
		c.releaseTimer = nil
	}
}

func (c *roomCoordinator) ASRMutedUntil(role LegRole) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.asrMutedUntil[role]
}
