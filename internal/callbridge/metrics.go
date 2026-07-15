package callbridge

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics exports bridge Prometheus counters and histograms.
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
		Name: "bridge_active_pairs",
		Help: "Currently paired bridge rooms",
	})
	m.legsConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bridge_legs_connected",
		Help: "Connected bridge legs by state",
	}, []string{"state"})
	m.framesRelayed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_frames_relayed_total",
		Help: "Binary frames relayed to peer legs",
	}, []string{"direction"})
	m.framesDroppedAge = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_frames_dropped_age_total",
		Help: "Frames dropped for exceeding max age",
	}, []string{"direction"})
	m.framesDroppedDep = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_frames_dropped_depth_total",
		Help: "Frames dropped due to queue depth",
	}, []string{"direction"})
	m.relayLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bridge_relay_latency_ms",
		Help:    "Frame relay latency at WriteMessage (arrival to write)",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 12),
	}, []string{"direction"})
	m.pairings = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_pairings_total",
		Help: "Bridge pairing attempts",
	}, []string{"result"})
	m.teardowns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_teardowns_total",
		Help: "Bridge teardown events",
	}, []string{"reason"})
	reg.MustRegister(
		m.activePairs,
		m.legsConnected,
		m.framesRelayed,
		m.framesDroppedAge,
		m.framesDroppedDep,
		m.relayLatency,
		m.pairings,
		m.teardowns,
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

func (m *Metrics) setActivePairs(n float64) {
	if m == nil || !m.enabled {
		return
	}
	m.activePairs.Set(n)
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

func (m *Metrics) setLegs(state string, n float64) {
	if m == nil || !m.enabled {
		return
	}
	m.legsConnected.WithLabelValues(state).Set(n)
}
