package media

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const maxRecentAudioBytes = 8000 * 2 * 3

// TurnManager implements TranscriptConsumer and emits turn events to a TurnListener.
//
// Sarvam END_SPEECH is the primary endpoint cue; a per-flow silence timer tunes the wait
// before EndOfTurn. CT-7 optionally refines endpointing via semantic turn detection and
// suppresses backchannel interrupts while the agent is speaking.
type TurnManager struct {
	next         TurnListener
	cfg          EndpointConfig
	semanticCfg  SemanticTurnConfig
	clock        Clock
	localVAD     LocalVAD
	semanticTurn SemanticTurnDetector
	backchannel  BackchannelClassifier
	logger       *slog.Logger

	mu                     sync.Mutex
	state                  turnState
	backchannelsSuppressed atomic.Int64
}

type turnState struct {
	flowClass                FlowClass
	agentSpeaking            bool
	userSpeaking             bool
	latestPartial            string
	latestFinal              string
	endSpeechSeen            bool
	turnEmitted              bool
	semanticHold             bool
	utteranceStarted         time.Time
	recentAudio              []byte
	recentAudioRate          int
	silenceTimer             TimerHandle
	semanticCompleteTimer    TimerHandle
	longSilenceFallbackTimer TimerHandle
	maxUtteranceTimer        TimerHandle
}

// NewTurnManager constructs a turn manager that consumes ASR transcript events.
func NewTurnManager(
	next TurnListener,
	cfg EndpointConfig,
	clock Clock,
	localVAD LocalVAD,
	semantic SemanticTurnDetector,
	semanticCfg SemanticTurnConfig,
	backchannel BackchannelClassifier,
	logger *slog.Logger,
) *TurnManager {
	cfg = cfg.withDefaults()
	semanticCfg = semanticCfg.withDefaults()
	if next == nil {
		next = NewLoggingTurnListener(logger)
	}
	if clock == nil {
		clock = RealClock{}
	}
	if localVAD == nil {
		localVAD = NoopVAD{}
	}
	if semantic == nil {
		semantic = NoopSemanticTurn{}
	}
	if backchannel == nil {
		backchannel = NewLexiconBackchannel(DefaultBackchannelConfig())
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &TurnManager{
		next:         next,
		cfg:          cfg,
		semanticCfg:  semanticCfg,
		clock:        clock,
		localVAD:     localVAD,
		semanticTurn: semantic,
		backchannel:  backchannel,
		logger:       logger,
		state: turnState{
			flowClass: FlowDefault,
		},
	}
}

// BackchannelsSuppressed returns the number of interrupt events suppressed as backchannels.
func (m *TurnManager) BackchannelsSuppressed() int64 {
	return m.backchannelsSuppressed.Load()
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
	m.appendRecentAudioLocked(pcm16, rate)

	m.mu.Lock()
	agentSpeaking := m.state.agentSpeaking
	m.mu.Unlock()
	if !agentSpeaking {
		return
	}
	if m.localVAD.IsSpeech(pcm16, rate) {
		m.tryEmitInterrupt(ctx, session)
	}
}

// SetListener replaces the downstream TurnListener (e.g. attach EB-6 brain client after construction).
func (m *TurnManager) SetListener(next TurnListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if next == nil {
		next = NewLoggingTurnListener(m.logger)
	}
	m.next = next
}

func (m *TurnManager) OnSpeechStart(ctx context.Context, session *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state.agentSpeaking {
		m.tryEmitInterruptLocked(ctx, session)
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
	m.state.semanticHold = false
	m.stopSilenceTimerLocked()
	m.stopSemanticCompleteTimerLocked()
	m.stopLongSilenceFallbackTimerLocked()
}

func (m *TurnManager) OnSpeechEnd(ctx context.Context, session *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.endSpeechSeen = true
	m.armEndpointTimerLocked(ctx, session)
}

func (m *TurnManager) OnFinal(ctx context.Context, session *Session, transcript Transcript) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if transcript.Text != "" {
		m.state.latestFinal = transcript.Text
	}
	if m.state.endSpeechSeen {
		m.armEndpointTimerLocked(ctx, session)
	}
}

func (m *TurnManager) resetUtteranceLocked() {
	m.stopSilenceTimerLocked()
	m.stopSemanticCompleteTimerLocked()
	m.stopLongSilenceFallbackTimerLocked()
	m.stopMaxUtteranceTimerLocked()
	m.state.latestPartial = ""
	m.state.latestFinal = ""
	m.state.endSpeechSeen = false
	m.state.turnEmitted = false
	m.state.semanticHold = false
}

func (m *TurnManager) armEndpointTimerLocked(ctx context.Context, session *Session) {
	if m.state.turnEmitted {
		return
	}
	if m.state.latestFinal == "" {
		m.armSilenceTimerLocked(ctx, session, m.cfg.silenceFor(m.state.flowClass))
		return
	}
	if !m.semanticCfg.Enabled {
		m.armSilenceTimerLocked(ctx, session, m.cfg.silenceFor(m.state.flowClass))
		return
	}

	audio, rate := m.recentAudioSnapshotLocked()
	pred, err := m.semanticTurn.Predict(ctx, m.state.latestFinal, audio, rate)
	if err != nil {
		m.armSilenceTimerLocked(ctx, session, m.cfg.silenceFor(m.state.flowClass))
		return
	}
	if pred.Complete && pred.Confidence >= m.semanticCfg.ConfidenceThreshold {
		m.armSemanticCompleteTimerLocked(ctx, session)
		return
	}
	m.armSilenceTimerLocked(ctx, session, m.cfg.silenceFor(m.state.flowClass))
}

func (m *TurnManager) armSilenceTimerLocked(ctx context.Context, session *Session, delay time.Duration) {
	m.stopSilenceTimerLocked()
	m.stopSemanticCompleteTimerLocked()
	m.state.silenceTimer = m.clock.AfterFunc(delay, func() {
		m.mu.Lock()
		m.state.silenceTimer = nil
		m.mu.Unlock()
		m.onSilenceTimerExpired(ctx, session)
	})
}

func (m *TurnManager) armSemanticCompleteTimerLocked(ctx context.Context, session *Session) {
	m.stopSilenceTimerLocked()
	m.stopSemanticCompleteTimerLocked()
	delay := time.Duration(m.semanticCfg.CompleteSilenceMs) * time.Millisecond
	m.state.semanticCompleteTimer = m.clock.AfterFunc(delay, func() {
		m.mu.Lock()
		m.state.semanticCompleteTimer = nil
		m.mu.Unlock()
		m.tryEmitEndOfTurn(ctx, session, false)
	})
}

func (m *TurnManager) armLongSilenceFallbackLocked(ctx context.Context, session *Session) {
	m.stopLongSilenceFallbackTimerLocked()
	delay := m.longSilenceFallbackDurationLocked()
	m.state.longSilenceFallbackTimer = m.clock.AfterFunc(delay, func() {
		m.mu.Lock()
		m.state.longSilenceFallbackTimer = nil
		m.state.semanticHold = false
		m.mu.Unlock()
		m.tryEmitEndOfTurn(ctx, session, false)
	})
}

func (m *TurnManager) longSilenceFallbackDurationLocked() time.Duration {
	if m.semanticCfg.LongSilenceFallbackMs > 0 {
		return time.Duration(m.semanticCfg.LongSilenceFallbackMs) * time.Millisecond
	}
	return m.cfg.silenceFor(m.state.flowClass)
}

func (m *TurnManager) onSilenceTimerExpired(ctx context.Context, session *Session) {
	m.mu.Lock()
	final := m.state.latestFinal
	enabled := m.semanticCfg.Enabled
	turnEmitted := m.state.turnEmitted
	audio, rate := m.recentAudioSnapshotLocked()
	m.mu.Unlock()

	if turnEmitted || final == "" {
		return
	}
	if !enabled {
		m.tryEmitEndOfTurn(ctx, session, false)
		return
	}

	pred, err := m.semanticTurn.Predict(ctx, final, audio, rate)
	if err != nil {
		m.tryEmitEndOfTurn(ctx, session, false)
		return
	}
	if pred.Complete && pred.Confidence >= m.semanticCfg.ConfidenceThreshold {
		m.tryEmitEndOfTurn(ctx, session, false)
		return
	}

	m.mu.Lock()
	if m.state.turnEmitted {
		m.mu.Unlock()
		return
	}
	m.state.semanticHold = true
	m.mu.Unlock()
	m.armLongSilenceFallback(ctx, session)
}

func (m *TurnManager) armLongSilenceFallback(ctx context.Context, session *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.turnEmitted {
		return
	}
	m.armLongSilenceFallbackLocked(ctx, session)
}

func (m *TurnManager) armMaxUtteranceTimerLocked(ctx context.Context, session *Session) {
	m.stopMaxUtteranceTimerLocked()
	delay := time.Duration(m.cfg.MaxUtteranceMs) * time.Millisecond
	m.state.maxUtteranceTimer = m.clock.AfterFunc(delay, func() {
		m.mu.Lock()
		m.state.maxUtteranceTimer = nil
		m.state.semanticHold = false
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
	m.state.semanticHold = false
	m.stopSilenceTimerLocked()
	m.stopSemanticCompleteTimerLocked()
	m.stopLongSilenceFallbackTimerLocked()
	m.stopMaxUtteranceTimerLocked()

	m.emitLocked(ctx, session, TurnEvent{
		Kind:       TurnEndOfTurn,
		Transcript: text,
		FlowClass:  m.state.flowClass,
		Forced:     forced,
	})
}

func (m *TurnManager) tryEmitInterrupt(ctx context.Context, session *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tryEmitInterruptLocked(ctx, session)
}

func (m *TurnManager) tryEmitInterruptLocked(ctx context.Context, session *Session) {
	if !m.state.agentSpeaking {
		return
	}
	transcript := m.state.latestFinal
	if transcript == "" {
		transcript = m.state.latestPartial
	}
	audio, rate := m.recentAudioSnapshotLocked()

	if !IsNoopBackchannel(m.backchannel) {
		ok, err := m.backchannel.IsBackchannel(ctx, transcript, audio, rate)
		if err == nil && ok {
			m.backchannelsSuppressed.Add(1)
			return
		}
	}

	m.emitLocked(ctx, session, TurnEvent{Kind: TurnInterrupt, FlowClass: m.state.flowClass})
}

func (m *TurnManager) appendRecentAudioLocked(pcm16 []byte, rate int) {
	if len(pcm16) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.recentAudioRate = rate
	m.state.recentAudio = append(m.state.recentAudio, pcm16...)
	if len(m.state.recentAudio) > maxRecentAudioBytes {
		m.state.recentAudio = append([]byte(nil), m.state.recentAudio[len(m.state.recentAudio)-maxRecentAudioBytes:]...)
	}
}

func (m *TurnManager) recentAudioSnapshotLocked() ([]byte, int) {
	if len(m.state.recentAudio) == 0 {
		return nil, m.state.recentAudioRate
	}
	out := append([]byte(nil), m.state.recentAudio...)
	return out, m.state.recentAudioRate
}

func (m *TurnManager) stopSilenceTimerLocked() {
	if m.state.silenceTimer != nil {
		m.state.silenceTimer.Stop()
		m.state.silenceTimer = nil
	}
}

func (m *TurnManager) stopSemanticCompleteTimerLocked() {
	if m.state.semanticCompleteTimer != nil {
		m.state.semanticCompleteTimer.Stop()
		m.state.semanticCompleteTimer = nil
	}
}

func (m *TurnManager) stopLongSilenceFallbackTimerLocked() {
	if m.state.longSilenceFallbackTimer != nil {
		m.state.longSilenceFallbackTimer.Stop()
		m.state.longSilenceFallbackTimer = nil
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

// Close releases classifier and local VAD resources.
func (m *TurnManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopSilenceTimerLocked()
	m.stopSemanticCompleteTimerLocked()
	m.stopLongSilenceFallbackTimerLocked()
	m.stopMaxUtteranceTimerLocked()

	var firstErr error
	if m.localVAD != nil {
		if err := m.localVAD.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.semanticTurn != nil {
		if err := m.semanticTurn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.backchannel != nil {
		if err := m.backchannel.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
