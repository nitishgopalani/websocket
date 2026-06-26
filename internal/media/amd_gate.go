package media

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type amdGateState int

const (
	amdStateDetecting amdGateState = iota
	amdStateHuman
	amdStateMachine
)

// AMDOutcomeListener receives AMD gate outcomes for downstream call control.
type AMDOutcomeListener interface {
	OnHuman(ctx context.Context, session *Session)
	OnMachine(ctx context.Context, session *Session, decision AMDDecision)
}

// LoggingAMDListener logs AMD gate outcomes.
type LoggingAMDListener struct {
	logger *slog.Logger
}

// NewLoggingAMDListener returns a listener that logs human/machine decisions.
func NewLoggingAMDListener(logger *slog.Logger) *LoggingAMDListener {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingAMDListener{logger: logger}
}

func (l *LoggingAMDListener) OnHuman(_ context.Context, session *Session) {
	l.logger.Info("amd human", "stream_sid", session.StreamSID, "call_sid", session.CallSID)
}

func (l *LoggingAMDListener) OnMachine(_ context.Context, session *Session, decision AMDDecision) {
	l.logger.Info("amd machine",
		"stream_sid", session.StreamSID,
		"call_sid", session.CallSID,
		"proba_human", decision.ProbaHuman,
		"reason", decision.Reason,
	)
}

// AMDGateSink buffers the first detection window and gates audio to the next sink.
//
// CT-1 delivers audio for each session on a single worker goroutine, so per-session
// gate state does not require locking.
type AMDGateSink struct {
	next                AudioSink
	clf                 AMDClassifier
	listener            AMDOutcomeListener
	sampleRate          int
	windowMs            int
	marginMs            int
	probaHumanThreshold float64
	logger              *slog.Logger
	state               amdGateState
	buffer              [][]byte
	bufferedBytes       int
	windowBytes         int
	maxBufferBytes      int
	decided             bool
	decideOnce          sync.Once
	timer               *time.Timer
	session             *Session
	droppedMachine      atomic.Int64
}

// NewAMDGateSink wraps next with answering-machine detection gating.
func NewAMDGateSink(next AudioSink, clf AMDClassifier, listener AMDOutcomeListener, sampleRate int, cfg AMDConfig, logger *slog.Logger) *AMDGateSink {
	cfg = cfg.withDefaults()
	if next == nil {
		next = NewLoggingSink(logger)
	}
	if clf == nil {
		clf = NoopAMDClassifier{}
	}
	if listener == nil {
		listener = NewLoggingAMDListener(logger)
	}
	if sampleRate <= 0 {
		sampleRate = defaultTargetSampleRate
	}
	if logger == nil {
		logger = slog.Default()
	}
	windowBytes := pcmBytesForDurationMs(cfg.WindowMs, sampleRate)
	return &AMDGateSink{
		next:                next,
		clf:                 clf,
		listener:            listener,
		sampleRate:          sampleRate,
		windowMs:            cfg.WindowMs,
		marginMs:            cfg.BufferMarginMs,
		probaHumanThreshold: cfg.ProbaHumanThreshold,
		logger:              logger,
		windowBytes:         windowBytes,
		maxBufferBytes:      pcmBytesForDurationMs(cfg.WindowMs+cfg.BufferMarginMs, sampleRate),
	}
}

// DroppedDuringMachine returns frames dropped after a machine decision.
func (g *AMDGateSink) DroppedDuringMachine() int64 {
	return g.droppedMachine.Load()
}

func (g *AMDGateSink) OnStart(ctx context.Context, session *Session) error {
	g.session = session
	if IsNoopAMD(g.clf) {
		g.state = amdStateHuman
		return g.next.OnStart(ctx, session)
	}
	g.state = amdStateDetecting
	g.timer = time.AfterFunc(time.Duration(g.windowMs)*time.Millisecond, func() {
		_ = g.decide(context.Background(), "window_timer")
	})
	return g.next.OnStart(ctx, session)
}

func (g *AMDGateSink) OnAudio(ctx context.Context, session *Session, frame []byte) error {
	switch g.state {
	case amdStateHuman:
		return g.next.OnAudio(ctx, session, frame)
	case amdStateMachine:
		g.droppedMachine.Add(1)
		return nil
	case amdStateDetecting:
		if len(frame) == 0 {
			return nil
		}
		if g.bufferedBytes+len(frame) > g.maxBufferBytes {
			g.logger.Warn("amd buffer cap reached; fail-open to human",
				"stream_sid", session.StreamSID,
				"buffered_bytes", g.bufferedBytes,
				"max_bytes", g.maxBufferBytes,
			)
			return g.failOpenWithoutClassify(ctx, "buffer_cap")
		}
		copied := make([]byte, len(frame))
		copy(copied, frame)
		g.buffer = append(g.buffer, copied)
		g.bufferedBytes += len(frame)
		if g.bufferedBytes >= g.windowBytes {
			return g.decide(ctx, "window_full")
		}
		return nil
	default:
		return nil
	}
}

func (g *AMDGateSink) OnDTMF(ctx context.Context, session *Session, digit string) error {
	return g.next.OnDTMF(ctx, session, digit)
}

func (g *AMDGateSink) OnStop(ctx context.Context, session *Session) error {
	if g.state == amdStateDetecting {
		_ = g.decide(ctx, "session_stop")
	}
	if g.timer != nil {
		g.timer.Stop()
	}
	return g.next.OnStop(ctx, session)
}

func (g *AMDGateSink) decide(ctx context.Context, trigger string) error {
	var err error
	g.decideOnce.Do(func() {
		err = g.runDecision(ctx, trigger)
	})
	return err
}

func (g *AMDGateSink) runDecision(ctx context.Context, trigger string) error {
	if g.decided {
		return nil
	}
	g.decided = true
	if g.timer != nil {
		g.timer.Stop()
	}

	session := g.session
	pcm := concatPCM(g.buffer)
	if trigger == "session_stop" && len(pcm) < g.windowBytes {
		return g.applyDecision(ctx, session, FailOpenHumanDecision("stop_insufficient_audio"))
	}
	if len(pcm) == 0 {
		return g.applyDecision(ctx, session, FailOpenHumanDecision("insufficient_audio_"+trigger))
	}

	decision, classifyErr := g.clf.Classify(ctx, pcm, g.sampleRate)
	if classifyErr != nil {
		g.logger.Warn("amd classify failed; fail-open to human",
			"stream_sid", session.StreamSID,
			"trigger", trigger,
			"error", classifyErr,
		)
		return g.applyDecision(ctx, session, FailOpenHumanDecision("classifier_error_"+trigger))
	}
	decision = applyAMDThreshold(decision, g.probaHumanThreshold)
	decision.Final = true
	if decision.Reason == "" {
		decision.Reason = trigger
	} else {
		decision.Reason = decision.Reason + "; trigger=" + trigger
	}
	return g.applyDecision(ctx, session, decision)
}

func (g *AMDGateSink) applyDecision(ctx context.Context, session *Session, decision AMDDecision) error {
	switch decision.Result {
	case AMDMachine:
		g.state = amdStateMachine
		g.buffer = nil
		g.bufferedBytes = 0
		g.listener.OnMachine(ctx, session, decision)
		return nil
	default:
		g.state = amdStateHuman
		if err := g.flushBuffer(ctx, session); err != nil {
			return err
		}
		g.listener.OnHuman(ctx, session)
		return nil
	}
}

func (g *AMDGateSink) failOpenWithoutClassify(ctx context.Context, reason string) error {
	var err error
	g.decideOnce.Do(func() {
		g.decided = true
		if g.timer != nil {
			g.timer.Stop()
		}
		err = g.applyDecision(ctx, g.session, FailOpenHumanDecision(reason))
	})
	return err
}

func (g *AMDGateSink) flushBuffer(ctx context.Context, session *Session) error {
	frames := g.buffer
	g.buffer = nil
	g.bufferedBytes = 0
	var err error
	for _, frame := range frames {
		if fwdErr := g.next.OnAudio(ctx, session, frame); fwdErr != nil {
			err = fwdErr
		}
	}
	return err
}

func concatPCM(frames [][]byte) []byte {
	total := 0
	for _, f := range frames {
		total += len(f)
	}
	if total == 0 {
		return nil
	}
	out := make([]byte, 0, total)
	for _, f := range frames {
		out = append(out, f...)
	}
	return out
}
