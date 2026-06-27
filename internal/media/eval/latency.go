package eval

import (
	"sort"
)

// Percentiles computes p50/p95/p99 from latency samples in milliseconds.
func Percentiles(samplesMs []float64) (p50, p95, p99 float64) {
	if len(samplesMs) == 0 {
		return 0, 0, 0
	}
	sorted := append([]float64(nil), samplesMs...)
	sort.Float64s(sorted)
	p50 = percentile(sorted, 0.50)
	p95 = percentile(sorted, 0.95)
	p99 = percentile(sorted, 0.99)
	return p50, p95, p99
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := p * float64(len(sorted)-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// StageLatencySummary holds percentile stats for one pipeline stage.
type StageLatencySummary struct {
	Stage string  `json:"stage"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Count int     `json:"count"`
}

// SummarizeStageLatencies groups samples by stage name and computes percentiles.
func SummarizeStageLatencies(samples map[string][]float64) []StageLatencySummary {
	out := make([]StageLatencySummary, 0, len(samples))
	for stage, vals := range samples {
		p50, p95, p99 := Percentiles(vals)
		out = append(out, StageLatencySummary{
			Stage: stage,
			P50:   p50,
			P95:   p95,
			P99:   p99,
			Count: len(vals),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Stage < out[j].Stage })
	return out
}
