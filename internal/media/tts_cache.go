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
// so a repeat of the exact same spoken sentence is served locally with zero TTS latency.
//
// It caches whole resolved utterances (not stitched fragments), which avoids the
// prosody seams that fragment-splicing introduces. Voice/model/format/rate are part
// of the key, so changing any of them naturally invalidates old entries.

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
type cachingTTSStream struct {
	inner     TTSStream
	cache     *TTSCache
	keyPrefix string
	logger    *slog.Logger
	streamSID string
	out       chan TTSAudioChunk

	mu      sync.Mutex
	pending map[string]*pendingTurn // turnID -> in-flight accumulation (cache miss)
	done    chan struct{}
	wg      sync.WaitGroup
}

type pendingTurn struct {
	key    string
	frames [][]byte
}

func newCachingTTSStream(inner TTSStream, keyPrefix string, cache *TTSCache, logger *slog.Logger, streamSID string) *cachingTTSStream {
	c := &cachingTTSStream{
		inner:     inner,
		cache:     cache,
		keyPrefix: keyPrefix,
		logger:    logger,
		streamSID: streamSID,
		out:       make(chan TTSAudioChunk, defaultTTSAudioBuffer),
		pending:   make(map[string]*pendingTurn),
		done:      make(chan struct{}),
	}
	c.wg.Add(1)
	go c.pump()
	return c
}

func (c *cachingTTSStream) Speak(turnID string, text string) error {
	key := ttsCacheKey(c.keyPrefix, text)
	if frames, ok := c.cache.Get(key); ok {
		if c.logger != nil {
			c.logger.Info("tts cache hit",
				"stream_sid", c.streamSID, "turn_id", turnID,
				"frames", len(frames), "chars", len(text))
		}
		c.wg.Add(1)
		go c.replay(turnID, frames)
		return nil
	}
	c.mu.Lock()
	c.pending[turnID] = &pendingTurn{key: key}
	c.mu.Unlock()
	return c.inner.Speak(turnID, text)
}

// replay emits cached frames as a fresh turn (re-tagged turnID + terminating Final).
func (c *cachingTTSStream) replay(turnID string, frames [][]byte) {
	defer c.wg.Done()
	for i, f := range frames {
		select {
		case c.out <- TTSAudioChunk{TurnID: turnID, Seq: i, MuLaw: f}:
		case <-c.done:
			return
		}
	}
	select {
	case c.out <- TTSAudioChunk{TurnID: turnID, Seq: len(frames), Final: true}:
	case <-c.done:
	}
}

func (c *cachingTTSStream) pump() {
	defer c.wg.Done()
	for chunk := range c.inner.Audio() {
		if len(chunk.MuLaw) > 0 {
			c.mu.Lock()
			if p, ok := c.pending[chunk.TurnID]; ok {
				p.frames = append(p.frames, append([]byte(nil), chunk.MuLaw...))
			}
			c.mu.Unlock()
		}
		if chunk.Final {
			c.mu.Lock()
			if p, ok := c.pending[chunk.TurnID]; ok {
				c.cache.Put(p.key, p.frames)
				delete(c.pending, chunk.TurnID)
			}
			c.mu.Unlock()
		}
		select {
		case c.out <- chunk:
		case <-c.done:
			return
		}
	}
}

func (c *cachingTTSStream) Cancel(turnID string) error {
	// Drop partial accumulation so an interrupted utterance is never cached.
	c.mu.Lock()
	delete(c.pending, turnID)
	c.mu.Unlock()
	return c.inner.Cancel(turnID)
}

func (c *cachingTTSStream) Audio() <-chan TTSAudioChunk { return c.out }

func (c *cachingTTSStream) Close() error {
	close(c.done)
	err := c.inner.Close() // closes inner.Audio(), unblocking pump's range
	c.wg.Wait()
	close(c.out)
	return err
}
