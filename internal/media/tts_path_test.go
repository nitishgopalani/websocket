package media

import "testing"

type pathInner struct {
	audio chan TTSAudioChunk
	path  string
}

func (p *pathInner) Speak(string, string) error         { return nil }
func (p *pathInner) Cancel(string) error                { return nil }
func (p *pathInner) Audio() <-chan TTSAudioChunk        { return p.audio }
func (p *pathInner) Close() error                       { close(p.audio); return nil }
func (p *pathInner) Path() string                       { return p.path }

func TestTTSWrappersForwardPath(t *testing.T) {
	inner := &pathInner{audio: make(chan TTSAudioChunk), path: "ws"}
	resampled := &resamplingTTSStream{inner: inner, sourceRate: 8000, targetRate: 8000}
	cached := newCachingTTSStream(resampled, "k", NewTTSCache(4), nil, "sid")
	t.Cleanup(func() { _ = cached.Close() })
	if got := cached.Path(); got != "ws" {
		t.Fatalf("Path() through resample+cache = %q, want ws", got)
	}
}
