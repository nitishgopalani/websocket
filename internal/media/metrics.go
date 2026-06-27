package media

import (
	"context"
	"net/http"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsConfig controls Prometheus export.
type MetricsConfig struct {
	Enabled bool
}

// DefaultMetricsConfig returns CT-12 metrics defaults.
func DefaultMetricsConfig() MetricsConfig {
	return MetricsConfig{Enabled: true}
}

// MetricsConfigFromEnv loads metrics settings.
func MetricsConfigFromEnv() MetricsConfig {
	cfg := DefaultMetricsConfig()
	if v := os.Getenv("METRICS_ENABLED"); v == "0" || v == "false" || v == "FALSE" {
		cfg.Enabled = false
	}
	return cfg
}

// Metrics exports CT-12 Prometheus counters and histograms.
type Metrics struct {
	enabled  bool
	registry *prometheus.Registry

	stageDuration          *prometheus.HistogramVec
	mouthToEar             prometheus.Histogram
	openerDuration         prometheus.Histogram
	turnsTotal             prometheus.Counter
	fallbacksTotal         *prometheus.CounterVec
	outboundDropsTotal     prometheus.Counter
	bargeInsCommittedTotal prometheus.Counter
	backchannelsResumed    prometheus.Counter
	amdMachineTotal        prometheus.Counter
	amdHumanTotal          prometheus.Counter
	asrReconnectsTotal     prometheus.Counter
	ttsReconnectsTotal     prometheus.Counter
	engineReconnectsTotal  prometheus.Counter
	denoiseFallbacksTotal  prometheus.Counter
	latencyBudgetExceeded  prometheus.Counter
	activeSessions         prometheus.Gauge
}

var (
	globalMetrics     *Metrics
	globalMetricsOnce sync.Once
)

// GlobalMetrics returns the process-wide metrics registry (lazy, respects METRICS_ENABLED).
func GlobalMetrics() *Metrics {
	globalMetricsOnce.Do(func() {
		globalMetrics = NewMetrics(MetricsConfigFromEnv())
	})
	return globalMetrics
}

// NewMetrics constructs Prometheus metrics (noop when disabled).
func NewMetrics(cfg MetricsConfig) *Metrics {
	m := &Metrics{enabled: cfg.Enabled}
	if !cfg.Enabled {
		return m
	}
	reg := prometheus.NewRegistry()
	m.registry = reg
	m.stageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "media_stage_duration_ms",
		Help:    "Per-stage latency in milliseconds",
		Buckets: prometheus.ExponentialBuckets(5, 2, 12),
	}, []string{"stage"})
	m.mouthToEar = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "media_mouth_to_ear_ms",
		Help:    "Caller end to first egress frame (milliseconds)",
		Buckets: prometheus.ExponentialBuckets(50, 2, 10),
	})
	m.openerDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "media_opener_ms",
		Help:    "Session start to first egress frame for opener turn (milliseconds)",
		Buckets: prometheus.ExponentialBuckets(50, 2, 10),
	})
	m.turnsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_turns_total",
		Help: "Completed conversational turns",
	})
	m.fallbacksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "media_fallbacks_total",
		Help: "Fallback events by reason",
	}, []string{"reason"})
	m.outboundDropsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_outbound_drops_total",
		Help: "Outbound audio frames dropped due to backpressure",
	})
	m.bargeInsCommittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_bargeins_committed_total",
		Help: "Barge-in commits",
	})
	m.backchannelsResumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_backchannels_resumed_total",
		Help: "Backchannel barge-in resumes",
	})
	m.amdMachineTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_amd_machine_total",
		Help: "AMD machine detections",
	})
	m.amdHumanTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_amd_human_total",
		Help: "AMD human detections",
	})
	m.asrReconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_asr_reconnects_total",
		Help: "ASR provider reconnects",
	})
	m.ttsReconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_tts_reconnects_total",
		Help: "TTS provider reconnects",
	})
	m.engineReconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_engine_reconnects_total",
		Help: "Brain/engine reconnects",
	})
	m.denoiseFallbacksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_denoise_fallbacks_total",
		Help: "Denoise fail-open fallbacks",
	})
	m.latencyBudgetExceeded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "media_latency_budget_exceeded_total",
		Help: "Turns exceeding mouth-to-ear latency budget",
	})
	m.activeSessions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "media_active_sessions",
		Help: "Active media sessions",
	})
	reg.MustRegister(
		m.stageDuration,
		m.mouthToEar,
		m.openerDuration,
		m.turnsTotal,
		m.fallbacksTotal,
		m.outboundDropsTotal,
		m.bargeInsCommittedTotal,
		m.backchannelsResumed,
		m.amdMachineTotal,
		m.amdHumanTotal,
		m.asrReconnectsTotal,
		m.ttsReconnectsTotal,
		m.engineReconnectsTotal,
		m.denoiseFallbacksTotal,
		m.latencyBudgetExceeded,
		m.activeSessions,
	)
	return m
}

// Enabled reports whether metrics export is active.
func (m *Metrics) Enabled() bool {
	return m != nil && m.enabled
}

// Handler returns the Prometheus scrape handler (nil when disabled).
func (m *Metrics) Handler() http.Handler {
	if m == nil || !m.enabled {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveStage(stage string, ms float64) {
	if m == nil || !m.enabled || m.stageDuration == nil {
		return
	}
	m.stageDuration.WithLabelValues(stage).Observe(ms)
}

func (m *Metrics) ObserveMouthToEar(ms float64) {
	if m == nil || !m.enabled || m.mouthToEar == nil {
		return
	}
	m.mouthToEar.Observe(ms)
}

func (m *Metrics) ObserveOpener(ms float64) {
	if m == nil || !m.enabled || m.openerDuration == nil {
		return
	}
	m.openerDuration.Observe(ms)
}

func (m *Metrics) IncTurnsTotal() {
	if m == nil || !m.enabled || m.turnsTotal == nil {
		return
	}
	m.turnsTotal.Inc()
}

func (m *Metrics) IncFallback(reason string) {
	if m == nil || !m.enabled || m.fallbacksTotal == nil {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	m.fallbacksTotal.WithLabelValues(reason).Inc()
}

func (m *Metrics) IncOutboundDrop() {
	if m == nil || !m.enabled || m.outboundDropsTotal == nil {
		return
	}
	m.outboundDropsTotal.Inc()
}

func (m *Metrics) IncBargeInsCommitted() {
	if m == nil || !m.enabled || m.bargeInsCommittedTotal == nil {
		return
	}
	m.bargeInsCommittedTotal.Inc()
}

func (m *Metrics) IncBackchannelsResumed() {
	if m == nil || !m.enabled || m.backchannelsResumed == nil {
		return
	}
	m.backchannelsResumed.Inc()
}

func (m *Metrics) IncAMDMachine() {
	if m == nil || !m.enabled || m.amdMachineTotal == nil {
		return
	}
	m.amdMachineTotal.Inc()
}

func (m *Metrics) IncAMDHuman() {
	if m == nil || !m.enabled || m.amdHumanTotal == nil {
		return
	}
	m.amdHumanTotal.Inc()
}

func (m *Metrics) IncASRReconnect() {
	if m == nil || !m.enabled || m.asrReconnectsTotal == nil {
		return
	}
	m.asrReconnectsTotal.Inc()
}

func (m *Metrics) IncTTSReconnect() {
	if m == nil || !m.enabled || m.ttsReconnectsTotal == nil {
		return
	}
	m.ttsReconnectsTotal.Inc()
}

func (m *Metrics) IncEngineReconnect() {
	if m == nil || !m.enabled || m.engineReconnectsTotal == nil {
		return
	}
	m.engineReconnectsTotal.Inc()
}

func (m *Metrics) IncDenoiseFallback() {
	if m == nil || !m.enabled || m.denoiseFallbacksTotal == nil {
		return
	}
	m.denoiseFallbacksTotal.Inc()
}

func (m *Metrics) IncLatencyBudgetExceeded() {
	if m == nil || !m.enabled || m.latencyBudgetExceeded == nil {
		return
	}
	m.latencyBudgetExceeded.Inc()
}

func (m *Metrics) SetActiveSessions(n int) {
	if m == nil || !m.enabled || m.activeSessions == nil {
		return
	}
	m.activeSessions.Set(float64(n))
}

// MetricsAMDListener wraps an AMD listener and records Prometheus counters.
type MetricsAMDListener struct {
	Inner   AMDOutcomeListener
	Metrics *Metrics
}

// NewMetricsAMDListener returns an AMD listener that increments metrics.
func NewMetricsAMDListener(inner AMDOutcomeListener, metrics *Metrics) *MetricsAMDListener {
	if inner == nil {
		inner = NewLoggingAMDListener(nil)
	}
	return &MetricsAMDListener{Inner: inner, Metrics: metrics}
}

func (l *MetricsAMDListener) OnHuman(ctx context.Context, session *Session) {
	if l.Metrics != nil {
		l.Metrics.IncAMDHuman()
	}
	if l.Inner != nil {
		l.Inner.OnHuman(ctx, session)
	}
}

func (l *MetricsAMDListener) OnMachine(ctx context.Context, session *Session, decision AMDDecision) {
	if l.Metrics != nil {
		l.Metrics.IncAMDMachine()
	}
	if l.Inner != nil {
		l.Inner.OnMachine(ctx, session, decision)
	}
}

var _ AMDOutcomeListener = (*MetricsAMDListener)(nil)
