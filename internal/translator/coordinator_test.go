package translator

import (
	"testing"
	"time"
)

func testCoordinator(t *testing.T) (*roomCoordinator, *time.Time, func(time.Duration) func()) {
	t.Helper()
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	current := now
	var pending func()
	coord := newRoomCoordinator(Config{
		EchoTail:      350 * time.Millisecond,
		FloorDebounce: 150 * time.Millisecond,
	}, nil)
	coord.SetClock(func() time.Time { return current }, func(d time.Duration, fn func()) *time.Timer {
		pending = fn
		return time.NewTimer(time.Hour)
	})
	advance := func(d time.Duration) {
		current = current.Add(d)
	}
	return coord, &current, func(d time.Duration) func() {
		advance(d)
		return pending
	}
}

func TestCoordinator_idleAllowsBoth(t *testing.T) {
	coord, _, _ := testCoordinator(t)
	if !coord.AllowASRIngest(LegA) || !coord.AllowASRIngest(LegB) {
		t.Fatal("expected both legs allowed in IDLE")
	}
}

func TestCoordinator_simultaneousStartFirstArrivalWins(t *testing.T) {
	coord, current, fire := testCoordinator(t)
	coord.OnSpeechStart(LegA)
	if coord.State() != floorACapturing {
		t.Fatalf("state=%v", coord.State())
	}
	*current = current.Add(50 * time.Millisecond)
	coord.OnSpeechStart(LegB)
	if coord.State() != floorACapturing {
		t.Fatalf("expected A to keep floor, state=%v", coord.State())
	}
	if !coord.AllowASRIngest(LegA) {
		t.Fatal("A should ingest")
	}
	if coord.AllowASRIngest(LegB) {
		t.Fatal("B should be blocked")
	}
	_ = fire
}

func TestCoordinator_floorCaptureToPlaying(t *testing.T) {
	coord, _, _ := testCoordinator(t)
	coord.OnSpeechStart(LegA)
	coord.OnTTSStarted(LegA)
	if coord.State() != floorAPlaying {
		t.Fatalf("state=%v", coord.State())
	}
	if coord.AllowASRIngest(LegB) {
		t.Fatal("B must be muted while A TTS plays")
	}
}

func TestCoordinator_halfDuplexGatingWhileAToBTTsPlays(t *testing.T) {
	coord, _, _ := testCoordinator(t)
	coord.OnSpeechStart(LegA)
	coord.OnTTSStarted(LegA)
	if coord.AllowASRIngest(LegB) {
		t.Fatal("while A→B TTS plays, leg B ASR ingest must be BLOCKED")
	}
	if !coord.AllowASRIngest(LegA) {
		t.Fatal("floor holder A may still ingest until turn completes")
	}
}

func TestCoordinator_floorReleaseAfterPlaybackAndEchoTail(t *testing.T) {
	coord, current, fireAfter := testCoordinator(t)
	playbackEnd := *current
	coord.OnSpeechStart(LegA)
	coord.OnTTSStarted(LegA)

	if coord.AllowASRIngest(LegB) {
		t.Fatal("B muted during A_PLAYING")
	}

	coord.OnPlaybackComplete(LegB, playbackEnd)
	if coord.AllowASRIngest(LegB) {
		t.Fatal("B must stay muted until echo tail elapses")
	}
	if coord.State() != floorAPlaying {
		t.Fatalf("floor must not release before echo tail, state=%v", coord.State())
	}

	release := fireAfter(350 * time.Millisecond)
	if release == nil {
		t.Fatal("expected scheduled floor release")
	}
	release()
	if coord.State() != floorIdle {
		t.Fatalf("expected IDLE after release, got %v", coord.State())
	}
	if !coord.AllowASRIngest(LegB) {
		t.Fatal("B should ingest after full release")
	}
}

func TestCoordinator_nonFloorMutedUntilTrueRelease(t *testing.T) {
	coord, current, fireAfter := testCoordinator(t)
	coord.OnSpeechStart(LegA)
	coord.OnTTSStarted(LegA)
	playbackEnd := *current

	coord.OnPlaybackComplete(LegB, playbackEnd)
	*current = playbackEnd.Add(100 * time.Millisecond)
	if coord.AllowASRIngest(LegB) {
		t.Fatal("B ASR must stay MUTED mid echo tail")
	}

	release := fireAfter(250 * time.Millisecond)
	release()
	if !coord.AllowASRIngest(LegB) {
		t.Fatal("B ASR should unmute only after floor truly released")
	}
}

func TestCoordinator_bCapturingBlocksA(t *testing.T) {
	coord, _, _ := testCoordinator(t)
	coord.OnSpeechStart(LegB)
	if coord.State() != floorBCapturing {
		t.Fatalf("state=%v", coord.State())
	}
	if coord.AllowASRIngest(LegA) {
		t.Fatal("A blocked while B captures")
	}
	if !coord.AllowASRIngest(LegB) {
		t.Fatal("B allowed while capturing")
	}
}
