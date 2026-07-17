package translator

import (
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"
)

type translationRecord struct {
	at           time.Time
	sourceRole   LegRole
	sourceText   string
	translated   string
}

// loopBreaker trips when translation volume or ping-pong exceeds thresholds.
type loopBreaker struct {
	max     int
	window  time.Duration
	now     func() time.Time
	logger  *slog.Logger
	onTrip  func()

	mu      sync.Mutex
	records []translationRecord
	tripped bool
}

func newLoopBreaker(max int, window time.Duration, logger *slog.Logger, onTrip func()) *loopBreaker {
	if max <= 0 {
		max = defaultLoopBreakerMax
	}
	if window <= 0 {
		window = time.Duration(defaultLoopBreakerWindowMS) * time.Millisecond
	}
	return &loopBreaker{
		max:    max,
		window: window,
		now:    time.Now,
		logger: logger,
		onTrip: onTrip,
	}
}

func (b *loopBreaker) SetClock(fn func() time.Time) {
	if fn != nil {
		b.now = fn
	}
}

func (b *loopBreaker) Tripped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tripped
}

func (b *loopBreaker) Record(sourceRole LegRole, sourceText, translated string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tripped {
		return
	}
	now := b.now()
	b.prune(now)
	rec := translationRecord{
		at:         now,
		sourceRole: sourceRole,
		sourceText: sourceText,
		translated: translated,
	}
	b.records = append(b.records, rec)
	if len(b.records) > b.max {
		b.trip("rate", len(b.records))
		return
	}
	if b.detectPingPong(rec) {
		b.trip("ping_pong", len(b.records))
	}
}

func (b *loopBreaker) prune(now time.Time) {
	cutoff := now.Add(-b.window)
	i := 0
	for _, r := range b.records {
		if !r.at.Before(cutoff) {
			b.records[i] = r
			i++
		}
	}
	b.records = b.records[:i]
}

func (b *loopBreaker) detectPingPong(latest translationRecord) bool {
	srcNorm := normalizeText(latest.sourceText)
	outNorm := normalizeText(latest.translated)
	for _, prev := range b.records[:len(b.records)-1] {
		if prev.sourceRole == latest.sourceRole {
			continue
		}
		prevOut := normalizeText(prev.translated)
		prevSrc := normalizeText(prev.sourceText)
		if textsSimilar(outNorm, prevSrc) || textsSimilar(srcNorm, prevOut) {
			return true
		}
	}
	return false
}

func (b *loopBreaker) trip(reason string, count int) {
	if b.tripped {
		return
	}
	b.tripped = true
	if b.logger != nil {
		b.logger.Error("translator_loop_breaker_tripped",
			"reason", reason,
			"count_in_window", count,
			"max", b.max,
			"window_ms", b.window.Milliseconds(),
		)
	}
	if b.onTrip != nil {
		b.onTrip()
	}
}

func normalizeText(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func textsSimilar(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	if strings.Contains(a, b) || strings.Contains(b, a) {
		minLen := len(a)
		if len(b) < minLen {
			minLen = len(b)
		}
		return minLen >= 4
	}
	return false
}
