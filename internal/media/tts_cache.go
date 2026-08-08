package media

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
)

// TTS full-utterance cache.
//
// The salary-on-time script speaks many fixed lines (greeting, pushes, objection
// replies, closings) and lines that become fixed once slots are filled. Synthesizing
// them every call re-pays the TTS network round-trip — the biggest chunk of turn
// latency. This cache stores the *final* audio frames (already resampled to the
// call's output rate) keyed by a hash of provider+voice+model+language+format+rate+text,
// so a repeat of the exact same sentence is served locally with zero TTS latency.
//
// Multi-Speak turns (B2 offer chunking): the wrapper owns one worker per turn_id that
// drains an ordered segment queue sequentially — cached segments are replayed, live
// segments are forwarded from the inner stream — and stamps every emitted frame with
// a wrapper-owned monotonic seq. Exactly ONE Final is emitted per turn (on flush, not
// per segment). Only single-content-Speak turns are recorded; multi-segment turns are
// not recorded because the inner WS provider appends segment texts into one synthesis,
// making frame→segment attribution impossible.

const (
	defaultTTSCacheMax = 512
)

// TTSCache is a bounded, concurrency-safe store of synthesized audio frames.
type TTSCache struct {
	mu    sync.RWMutex
	m     map[string][][]byte
	order []string
	max   int
	hits  uint64
	miss  uint64
}

// NewTTSCache creates a cache holding at most max distinct utterances (FIFO eviction).
func NewTTSCache(max int) *TTSCache {
	if max <= 0 {
		max = defaultTTSCacheMax
	}
	return &TTSCache{m: make(map[string][][]byte), max: max}
}

// Get returns the cached frames for key, if present.
func (c *TTSCache) Get(key string) ([][]byte, bool) {
	c.mu.RLock()
	frames, ok := c.m[key]
	c.mu.RUnlock()
	c.mu.Lock()
	if ok {
		c.hits++
	} else {
		c.miss++
	}
	c.mu.Unlock()
	return frames, ok
}

// Put stores frames under key (no-op if already present or empty). FIFO-evicts the
// oldest entry when at capacity.
func (c *TTSCache) Put(key string, frames [][]byte) {
	if len(frames) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.m[key]; ok {
		return
	}
	if len(c.order) >= c.max {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.m, oldest)
	}
	c.m[key] = frames
	c.order = append(c.order, key)
}

// Stats returns current hit/miss counters and entry count.
func (c *TTSCache) Stats() (hits, miss uint64, entries int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.miss, len(c.m)
}

var (
	globalTTSCache     *TTSCache
	globalTTSCacheOnce sync.Once
)

// GlobalTTSCache lazily builds the process-wide cache (size from TTS_CACHE_MAX).
func GlobalTTSCache() *TTSCache {
	globalTTSCacheOnce.Do(func() {
		max := defaultTTSCacheMax
		if v := strings.TrimSpace(os.Getenv("TTS_CACHE_MAX")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				max = n
			}
		}
		globalTTSCache = NewTTSCache(max)
	})
	return globalTTSCache
}

// ttsCacheEnabled reports whether utterance caching is on (default: on; set
// TTS_CACHE=0/false/off to disable).
func ttsCacheEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("TTS_CACHE")))
	switch v {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

func ttsCacheKey(prefix, text string) string {
	sum := sha256.Sum256([]byte(prefix + "\x00" + text))
	return hex.EncodeToString(sum[:])
}

// cachingTTSStream wraps a TTSStream, serving repeated utterances from a shared
// cache and recording fresh (cache-miss) synthesis for next time.
//
// Per-turn serialization: one worker goroutine per turn_id drains an ordered segment
// queue. Cached segments are replayed from the cache; live segments are produced by
// the inner stream. The wrapper stamps every emitted frame with a wrapper-owned
// monotonic seq (replay AND live) and emits exactly ONE Final per turn on flush. This
// guarantees a single producer per turn — no concurrent replay+live, no seq reset.
//
// The pump goroutine is the sole reader of inner.Audio(); it routes each inner frame
// to the active turn worker's innerCh (so the worker re-stamps seq and forwards), or
// straight to out for passthrough turns (no wrapper worker).
type cachingTTSStream struct {
	inner     TTSStream
	cache     *TTSCache
	keyPrefix string
	logger    *slog.Logger
	streamSID string
	out       chan TTSAudioChunk

	mu        sync.Mutex
	turns     map[string]*turnSession // turnID -> per-turn worker state
	turnVoice map[string]string       // turnID -> "voice|model|pace" override suffix for cache key
	done      chan struct{}
	wg        sync.WaitGroup
}

// turnSession is the per-turn worker state.
type turnSession struct {
	segCh       chan turnSegment   // buffered; Speak enqueues content segments here
	flushCh     chan struct{}      // empty Speak signals flush (start processing)
	innerCh     chan TTSAudioChunk // pump routes inner frames for this turn here
	workerDone  chan struct{}     // closed when worker has emitted the turn's Final
	contentCnt  int                // number of non-empty content Speaks (set under stream mu)
	// recording: only when contentCnt == 1 (single content segment)
	recordKey    string
	recordFrames [][]byte
}

// turnSegment is one enqueued Speak for a turn.
type turnSegment struct {
	text         string
	key          string
	cachedFrames [][]byte // non-nil + len>0 if cache hit (replay); nil if live
}

func newCachingTTSStream(inner TTSStream, keyPrefix string, cache *TTSCache, logger *slog.Logger, streamSID string) *cachingTTSStream {
	c := &cachingTTSStream{
		inner:     inner,
		cache:     cache,
		keyPrefix: keyPrefix,
		logger:    logger,
		streamSID: streamSID,
		out:       make(chan TTSAudioChunk, defaultTTSAudioBuffer),
		turns:     make(map[string]*turnSession),
		turnVoice: make(map[string]string),
		done:      make(chan struct{}),
	}
	c.wg.Add(1)
	go c.pump()
	return c
}

func (c *cachingTTSStream) SetTurnVoice(turnID, voiceID, model string, pace *float64) {
	ApplyTTSTurnVoice(c.inner, turnID, voiceID, model, pace)
	if turnID == "" {
		return
	}
	paceKey := ""
	if pace != nil {
		paceKey = strconv.FormatFloat(*pace, 'f', 3, 64)
	}
	c.mu.Lock()
	c.turnVoice[turnID] = strings.TrimSpace(voiceID) + "|" + strings.TrimSpace(model) + "|" + paceKey
	c.mu.Unlock()
}

// Speak enqueues a segment (non-empty text) or a flush signal (empty text) for the
// turn's worker. The worker drains segments sequentially and emits one Final.
func (c *cachingTTSStream) Speak(turnID string, text string) error {
	if turnID == "" {
		return c.inner.Speak(turnID, text)
	}
	text = strings.TrimSpace(text)
	c.mu.Lock()
	ts := c.turns[turnID]
	if ts == nil {
		ts = &turnSession{
			segCh:      make(chan turnSegment, 32),
			innerCh:    make(chan TTSAudioChunk, defaultTTSAudioBuffer),
			workerDone: make(chan struct{}),
		}
		c.turns[turnID] = ts
	}
	if text != "" {
		ts.contentCnt++
	}
	c.mu.Unlock()

	if text == "" {
		c.startWorker(turnID, ts)
		select {
		case ts.flushCh <- struct{}{}:
		case <-c.done:
		}
		return nil
	}

	c.mu.Lock()
	suffix := c.turnVoice[turnID]
	c.mu.Unlock()
	prefix := c.keyPrefix
	if suffix != "" && suffix != "|" {
		prefix = prefix + "|" + suffix
	}
	key := ttsCacheKey(prefix, text)
	frames, hit := c.cache.Get(key)
	if hit {
		if c.logger != nil {
			c.logger.Info("tts cache hit",
				"stream_sid", c.streamSID, "turn_id", turnID,
				"frames", len(frames), "chars", len(text))
		}
	}
	select {
	case ts.segCh <- turnSegment{text: text, key: key, cachedFrames: orFrames(hit, frames)}:
	case <-c.done:
	}
	return nil
}

func orFrames(hit bool, frames [][]byte) [][]byte {
	if !hit || len(frames) == 0 {
		return nil
	}
	return frames
}

// startWorker launches the per-turn drain goroutine exactly once. Called on flush.
func (c *cachingTTSStream) startWorker(turnID string, ts *turnSession) {
	c.mu.Lock()
	if ts.flushCh != nil {
		c.mu.Unlock()
		return
	}
	ts.flushCh = make(chan struct{}, 1)
	c.mu.Unlock()
	c.wg.Add(1)
	go c.turnWorker(turnID, ts)
}

// turnWorker drains the segment queue sequentially, emitting one monotonic seq stream
// and exactly one Final for the turn. Never two producers at once.
func (c *cachingTTSStream) turnWorker(turnID string, ts *turnSession) {
	defer c.wg.Done()
	defer close(ts.workerDone)
	defer c.removeTurn(turnID)

	select {
	case <-ts.flushCh:
	case <-c.done:
		return
	}

	// Drain all queued segments (Speak calls happened before flush).
	var segments []turnSegment
drainLoop:
	for {
		select {
		case seg := <-ts.segCh:
			segments = append(segments, seg)
		default:
			break drainLoop
		}
	}

	if len(segments) == 0 {
		// Empty turn flush: emit a single Final so the consumer's finalizeTurn fires.
		c.emitChunk(turnID, 1, nil, true)
		return
	}

	c.mu.Lock()
	contentCnt := ts.contentCnt
	c.mu.Unlock()
	recordable := contentCnt == 1

	seq := 0
	liveProduced := false
	replayProduced := false

	for i := range segments {
		seg := segments[i]
		if len(seg.cachedFrames) > 0 {
			replayProduced = true
			c.guardMultiProducer(turnID, replayProduced, liveProduced)
			for _, f := range seg.cachedFrames {
				seq++
				if !c.emitChunk(turnID, seq, f, false) {
					return
				}
			}
			continue
		}
		// Live segment: ask inner to synthesize this segment's text, then forward
		// its frames (routed by pump to ts.innerCh) until inner's Final for this
		// batch. We flush inner after each live segment so inner emits a self-
		// contained batch (inner WS appends texts across Speaks for the same
		// turnID otherwise).
		liveProduced = true
		c.guardMultiProducer(turnID, replayProduced, liveProduced)
		if err := c.inner.Speak(turnID, seg.text); err != nil {
			if c.logger != nil {
				c.logger.Warn("tts cache inner speak failed",
					"stream_sid", c.streamSID, "turn_id", turnID, "error", err)
			}
			continue
		}
		_ = c.inner.Speak(turnID, "")
		if recordable {
			ts.recordKey = seg.key
		}
		if !c.forwardInnerBatch(turnID, ts, &seq, recordable) {
			return
		}
	}

	// Exactly ONE Final per turn.
	seq++
	c.emitChunk(turnID, seq, nil, true)

	if recordable && len(ts.recordFrames) > 0 {
		c.cache.Put(ts.recordKey, ts.recordFrames)
	}
}

// forwardInnerBatch forwards inner frames (routed by pump to ts.innerCh) for the
// current live batch until inner's Final. Each frame is re-stamped with the wrapper's
// monotonic seq. Single-content turns accumulate raw frames for recording.
func (c *cachingTTSStream) forwardInnerBatch(turnID string, ts *turnSession, seq *int, recordable bool) bool {
	for {
		select {
		case chunk, ok := <-ts.innerCh:
			if !ok {
				return false
			}
			if chunk.Final {
				return true
			}
			if len(chunk.MuLaw) == 0 {
				continue
			}
			*seq++
			if recordable {
				ts.recordFrames = append(ts.recordFrames, append([]byte(nil), chunk.MuLaw...))
			}
			if !c.emitChunk(turnID, *seq, chunk.MuLaw, false) {
				return false
			}
		case <-c.done:
			return false
		}
	}
}

// guardMultiProducer logs a belt WARN if both replay and inner produced frames for
// the same turn. After the serialization fix this should be impossible.
func (c *cachingTTSStream) guardMultiProducer(turnID string, replay, live bool) {
	if replay && live && c.logger != nil {
		c.logger.Warn("tts_multi_producer",
			"stream_sid", c.streamSID, "turn_id", turnID,
			"note", "replay and live both produced for one turn")
	}
}

// emitChunk sends one re-stamped chunk to out. Returns false if stream is closing.
func (c *cachingTTSStream) emitChunk(turnID string, seq int, payload []byte, final bool) bool {
	select {
	case c.out <- TTSAudioChunk{TurnID: turnID, Seq: seq, MuLaw: payload, Final: final}:
		return true
	case <-c.done:
		return false
	}
}

func (c *cachingTTSStream) removeTurn(turnID string) {
	c.mu.Lock()
	delete(c.turns, turnID)
	delete(c.turnVoice, turnID)
	c.mu.Unlock()
}

// pump is the sole reader of inner.Audio(). It routes each frame to the active turn
// worker's innerCh (so the worker re-stamps seq and forwards), or straight to out
// for passthrough turns (no wrapper worker / empty turnID).
func (c *cachingTTSStream) pump() {
	defer c.wg.Done()
	for chunk := range c.inner.Audio() {
		if chunk.TurnID == "" {
			select {
			case c.out <- chunk:
			case <-c.done:
				return
			}
			continue
		}
		c.mu.Lock()
		ts := c.turns[chunk.TurnID]
		c.mu.Unlock()
		if ts == nil || ts.flushCh == nil {
			// No worker for this turn (passthrough / worker already done).
			select {
			case c.out <- chunk:
			case <-c.done:
				return
			}
			continue
		}
		select {
		case ts.innerCh <- chunk:
		case <-c.done:
			return
		}
	}
}

func (c *cachingTTSStream) Cancel(turnID string) error {
	// Drop the in-flight turn session so partial audio is never recorded/replayed.
	c.mu.Lock()
	delete(c.turns, turnID)
	delete(c.turnVoice, turnID)
	c.mu.Unlock()
	return c.inner.Cancel(turnID)
}

// Path forwards the inner TTS path label (ws/rest) through the cache wrapper.
func (c *cachingTTSStream) Path() string {
	if p, ok := c.inner.(interface{ Path() string }); ok {
		return p.Path()
	}
	return ""
}

func (c *cachingTTSStream) Audio() <-chan TTSAudioChunk { return c.out }

func (c *cachingTTSStream) Close() error {
	close(c.done)
	err := c.inner.Close() // closes inner.Audio(), unblocking pump's range
	c.wg.Wait()
	close(c.out)
	return err
}
