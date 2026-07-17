package translator

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics exports translator Prometheus counters and histograms.
type Metrics struct {
	enabled bool
	reg     *prometheus.Registry

	activePairs      prometheus.Gauge
	legsConnected    *prometheus.GaugeVec
	framesRelayed    *prometheus.CounterVec
	framesDroppedAge *prometheus.CounterVec
	framesDroppedDep *prometheus.CounterVec
	relayLatency     *prometheus.HistogramVec
	pairings         *prometheus.CounterVec
	teardowns        *prometheus.CounterVec
	utterances       *prometheus.CounterVec
}

// NewMetrics constructs Prometheus metrics (noop when disabled).
func NewMetrics(enabled bool) *Metrics {
	m := &Metrics{enabled: enabled}
	if !enabled {
		return m
	}
	reg := prometheus.NewRegistry()
	m.reg = reg
	m.activePairs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "translator_active_pairs",
		Help: "Currently paired translator rooms",
	})
	m.legsConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "translator_legs_connected",
		Help: "Connected translator legs by state",
	}, []string{"state"})
	m.framesRelayed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "translator_frames_relayed_total",
		Help: "Binary frames relayed to peer legs",
	}, []string{"direction"})
	m.framesDroppedAge = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "translator_frames_dropped_age_total",
		Help: "Frames dropped for exceeding max age",
	}, []string{"direction"})
	m.framesDroppedDep = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "translator_frames_dropped_depth_total",
		Help: "Frames dropped due to queue depth",
	}, []string{"direction"})
	m.relayLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "translator_relay_latency_ms",
		Help:    "Frame relay latency at WriteMessage (arrival to write)",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 12),
	}, []string{"direction"})
	m.pairings = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "translator_pairings_total",
		Help: "Translator pairing attempts",
	}, []string{"result"})
	m.teardowns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "translator_teardowns_total",
		Help: "Translator teardown events",
	}, []string{"reason"})
	m.utterances = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "translator_utterances_total",
		Help: "Translated utterances by outcome",
	}, []string{"result"})
	reg.MustRegister(
		m.activePairs,
		m.legsConnected,
		m.framesRelayed,
		m.framesDroppedAge,
		m.framesDroppedDep,
		m.relayLatency,
		m.pairings,
		m.teardowns,
		m.utterances,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	if m == nil || !m.enabled || m.reg == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

func (m *Metrics) incPairing(result string) {
	if m == nil || !m.enabled {
		return
	}
	m.pairings.WithLabelValues(result).Inc()
}

func (m *Metrics) incTeardown(reason string) {
	if m == nil || !m.enabled {
		return
	}
	m.teardowns.WithLabelValues(reason).Inc()
}

func (m *Metrics) incRelay(direction string) {
	if m == nil || !m.enabled {
		return
	}
	m.framesRelayed.WithLabelValues(direction).Inc()
}

func (m *Metrics) incDropAge(direction string) {
	if m == nil || !m.enabled {
		return
	}
	m.framesDroppedAge.WithLabelValues(direction).Inc()
}

func (m *Metrics) incDropDepth(direction string) {
	if m == nil || !m.enabled {
		return
	}
	m.framesDroppedDep.WithLabelValues(direction).Inc()
}

func (m *Metrics) observeLatency(direction string, ms float64) {
	if m == nil || !m.enabled {
		return
	}
	m.relayLatency.WithLabelValues(direction).Observe(ms)
}

func (m *Metrics) incActivePairs() {
	if m == nil || !m.enabled {
		return
	}
	m.activePairs.Inc()
}

func (m *Metrics) decActivePairs() {
	if m == nil || !m.enabled {
		return
	}
	m.activePairs.Dec()
}

func (m *Metrics) incWaitingLegs() {
	if m == nil || !m.enabled {
		return
	}
	m.legsConnected.WithLabelValues("waiting").Inc()
}

func (m *Metrics) decWaitingLegs() {
	if m == nil || !m.enabled {
		return
	}
	m.legsConnected.WithLabelValues("waiting").Dec()
}

func (m *Metrics) incUtterance(result string) {
	if m == nil || !m.enabled {
		return
	}
	m.utterances.WithLabelValues(result).Inc()
}
