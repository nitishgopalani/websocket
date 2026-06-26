package media

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// TurnManager implements TranscriptConsumer and emits turn events to a TurnListener.
//
// Sarvam END_SPEECH is the primary endpoint cue; a per-flow silence timer tunes the wait
// before EndOfTurn. CT-1 delivers transcript callbacks on a single goroutine per session.
type TurnManager struct {
	next     TurnListener
	cfg      EndpointConfig
	clock    Clock
	localVAD LocalVAD
	logger   *slog.Logger

	mu    sync.Mutex
	state turnState
}

type turnState struct {
	flowClass         FlowClass
	agentSpeaking     bool
	userSpeaking      bool
	latestPartial     string
	latestFinal       string
	endSpeechSeen     bool
	turnEmitted       bool
	utteranceStarted  time.Time
	silenceTimer      TimerHandle
	maxUtteranceTimer TimerHandle
}

// NewTurnManager constructs a turn manager that consumes ASR transcript events.
func NewTurnManager(next TurnListener, cfg EndpointConfig, clock Clock, localVAD LocalVAD, logger *slog.Logger) *TurnManager {
	cfg = cfg.withDefaults()
	if next == nil {
		next = NewLoggingTurnListener(logger)
	}
	if clock == nil {
		clock = RealClock{}
	}
	if localVAD == nil {
		localVAD = NoopVAD{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &TurnManager{
		next:     next,
		cfg:      cfg,
		clock:    clock,
		localVAD: localVAD,
		logger:   logger,
		state: turnState{
			flowClass: FlowDefault,
		},
	}
}

// SetFlowClass sets the active endpointing profile for the session.
func (m *TurnManager) SetFlowClass(_ *Session, class FlowClass) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if class == "" {
		class = FlowDefault
	}
	m.state.flowClass = class
}

// SetAgentSpeaking tracks whether the agent is currently playing TTS (for interrupt detection).
func (m *TurnManager) SetAgentSpeaking(_ *Session, speaking bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.agentSpeaking = speaking
}

// ObserveAudio optionally checks local VAD for fast interrupt while the agent is speaking.
func (m *TurnManager) ObserveAudio(ctx context.Context, session *Session, pcm16 []byte, rate int) {
	m.mu.Lock()
	agentSpeaking := m.state.agentSpeaking
	m.mu.Unlock()
	if !agentSpeaking {
		return
	}
	if m.localVAD.IsSpeech(pcm16, rate) {
		m.emit(ctx, session, TurnEvent{Kind: TurnInterrupt, FlowClass: m.currentFlowClass()})
	}
}

func (m *TurnManager) OnSpeechStart(ctx context.Context, session *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state.agentSpeaking {
		m.emitLocked(ctx, session, TurnEvent{Kind: TurnInterrupt, FlowClass: m.state.flowClass})
	}

	m.resetUtteranceLocked()
	m.emitLocked(ctx, session, TurnEvent{Kind: TurnSpeechStarted, FlowClass: m.state.flowClass})
	m.state.userSpeaking = true
	m.state.utteranceStarted = m.clock.Now()
	m.armMaxUtteranceTimerLocked(ctx, session)
}

func (m *TurnManager) OnPartial(ctx context.Context, session *Session, transcript Transcript) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if transcript.Text != "" {
		m.state.latestPartial = transcript.Text
	}
	m.stopSilenceTimerLocked()
}

func (m *TurnManager) OnSpeechEnd(ctx context.Context, session *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.endSpeechSeen = true
	m.armSilenceTimerLocked(ctx, session)
}

func (m *TurnManager) OnFinal(ctx context.Context, session *Session, transcript Transcript) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if transcript.Text != "" {
		m.state.latestFinal = transcript.Text
	}
	if m.state.endSpeechSeen {
		m.armSilenceTimerLocked(ctx, session)
	}
}

func (m *TurnManager) resetUtteranceLocked() {
	m.stopSilenceTimerLocked()
	m.stopMaxUtteranceTimerLocked()
	m.state.latestPartial = ""
	m.state.latestFinal = ""
	m.state.endSpeechSeen = false
	m.state.turnEmitted = false
}

func (m *TurnManager) armSilenceTimerLocked(ctx context.Context, session *Session) {
	if m.state.turnEmitted {
		return
	}
	m.stopSilenceTimerLocked()
	delay := m.cfg.silenceFor(m.state.flowClass)
	m.state.silenceTimer = m.clock.AfterFunc(delay, func() {
		m.mu.Lock()
		m.state.silenceTimer = nil
		m.mu.Unlock()
		m.tryEmitEndOfTurn(ctx, session, false)
	})
}

func (m *TurnManager) armMaxUtteranceTimerLocked(ctx context.Context, session *Session) {
	m.stopMaxUtteranceTimerLocked()
	delay := time.Duration(m.cfg.MaxUtteranceMs) * time.Millisecond
	m.state.maxUtteranceTimer = m.clock.AfterFunc(delay, func() {
		m.mu.Lock()
		m.state.maxUtteranceTimer = nil
		m.mu.Unlock()
		m.tryEmitEndOfTurn(ctx, session, true)
	})
}

func (m *TurnManager) tryEmitEndOfTurn(ctx context.Context, session *Session, forced bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state.turnEmitted {
		return
	}
	if !forced && m.state.latestFinal == "" {
		return
	}
	text := m.state.latestFinal
	if text == "" {
		text = m.state.latestPartial
	}
	if text == "" {
		return
	}

	m.state.turnEmitted = true
	m.state.userSpeaking = false
	m.stopSilenceTimerLocked()
	m.stopMaxUtteranceTimerLocked()

	m.emitLocked(ctx, session, TurnEvent{
		Kind:       TurnEndOfTurn,
		Transcript: text,
		FlowClass:  m.state.flowClass,
		Forced:     forced,
	})
}

func (m *TurnManager) stopSilenceTimerLocked() {
	if m.state.silenceTimer != nil {
		m.state.silenceTimer.Stop()
		m.state.silenceTimer = nil
	}
}

func (m *TurnManager) stopMaxUtteranceTimerLocked() {
	if m.state.maxUtteranceTimer != nil {
		m.state.maxUtteranceTimer.Stop()
		m.state.maxUtteranceTimer = nil
	}
}

func (m *TurnManager) emit(ctx context.Context, session *Session, event TurnEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.emitLocked(ctx, session, event)
}

func (m *TurnManager) emitLocked(ctx context.Context, session *Session, event TurnEvent) {
	m.next.OnTurnEvent(ctx, session, event)
}

func (m *TurnManager) currentFlowClass() FlowClass {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state.flowClass
}

var _ TranscriptConsumer = (*TurnManager)(nil)

// Close releases local VAD resources.
func (m *TurnManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopSilenceTimerLocked()
	m.stopMaxUtteranceTimerLocked()
	if m.localVAD != nil {
		return m.localVAD.Close()
	}
	return nil
}
