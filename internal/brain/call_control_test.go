package brain_test

import (
	"context"
	"sync/atomic"
	"testing"

	"websocket/internal/brain"
	"websocket/internal/media"
)

type closeRecorder struct {
	count atomic.Int32
}

func (c *closeRecorder) CloseSession(_ context.Context, _ string) {
	c.count.Add(1)
}

func TestCallControlOpenerOnHumanWhenAMDEnabled(t *testing.T) {
	ctrl := brain.NewCallControl(brain.CallControlConfig{
		AMDEnabled: true,
		Voicemail:  media.DefaultVoicemailConfig(),
	})
	egress := media.NewCarrierEgress(media.DefaultEgressConfig(), 20, media.RealClock{}, nil, media.DefaultCarrierProfile(), nil)
	egress.EnableHumanGate()
	session := &media.Session{StreamSID: "MZ-H"}
	ctrl.Bind(nil, nil, egress, nil)

	ctrl.OnHuman(context.Background(), session)
	if ctrl.OpenerCount() != 0 {
		t.Fatalf("opener without brain = %d, want 0", ctrl.OpenerCount())
	}
	if egress.HumanGated() {
		t.Fatal("human gate should be released")
	}
}

func TestCallControlMachineHangup(t *testing.T) {
	rec := &closeRecorder{}
	ctrl := brain.NewCallControl(brain.CallControlConfig{
		AMDEnabled: true,
		Voicemail:  media.DefaultVoicemailConfig(),
	})
	session := &media.Session{StreamSID: "MZ-VM"}
	ctrl.Bind(nil, nil, nil, rec)

	ctrl.OnMachine(context.Background(), session, media.AMDDecision{Result: media.AMDMachine})
	if rec.count.Load() != 1 {
		t.Fatalf("hangup count = %d, want 1", rec.count.Load())
	}
	if ctrl.OpenerCount() != 0 {
		t.Fatal("no opener on machine")
	}
}

func TestCallControlLeaveMessageSpeaksOnce(t *testing.T) {
	rec := &closeRecorder{}
	stream := &fakeTTSStream{
		audio:  make(chan media.TTSAudioChunk, 4),
		spoken: make(chan struct{}, 1),
	}
	tts := media.NewTTSReplyConsumer(stream, &noopEgress{}, nil, nil, nil)
	egress := media.NewCarrierEgress(media.DefaultEgressConfig(), 20, media.RealClock{}, nil, media.DefaultCarrierProfile(), nil)
	ctrl := brain.NewCallControl(brain.CallControlConfig{
		AMDEnabled: true,
		Voicemail: media.VoicemailConfig{
			Action:  media.VoicemailActionLeaveMessage,
			Message: "test voicemail line",
		},
	})
	session := &media.Session{StreamSID: "MZ-LM"}
	ctrl.Bind(nil, tts, egress, rec)

	ctrl.OnMachine(context.Background(), session, media.AMDDecision{Result: media.AMDMachine})
	select {
	case <-stream.spoken:
	default:
		t.Fatal("expected TTS speak for leave_message")
	}
	ctrl.OnPlaybackComplete(context.Background(), session, "voicemail:MZ-LM")
	if rec.count.Load() != 1 {
		t.Fatalf("hangup after mark = %d, want 1", rec.count.Load())
	}
}

type fakeTTSStream struct {
	audio  chan media.TTSAudioChunk
	spoken chan struct{}
}

func (f *fakeTTSStream) Speak(turnID, text string) error {
	if text != "" {
		if f.spoken == nil {
			f.spoken = make(chan struct{}, 1)
		}
		select {
		case f.spoken <- struct{}{}:
		default:
		}
		f.audio <- media.TTSAudioChunk{TurnID: turnID, MuLaw: []byte{0xFF}, Final: true}
	}
	return nil
}

func (f *fakeTTSStream) Cancel(_ string) error             { return nil }
func (f *fakeTTSStream) Audio() <-chan media.TTSAudioChunk { return f.audio }
func (f *fakeTTSStream) Close() error                      { return nil }

type noopEgress struct{}

func (noopEgress) SendAudio(context.Context, *media.Session, media.TTSAudioChunk) error { return nil }
func (noopEgress) Mark(context.Context, *media.Session, string) error                   { return nil }
func (noopEgress) ClearPlayback(context.Context, *media.Session) error                  { return nil }
