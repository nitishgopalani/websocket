package eval

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// EvalReport aggregates live-eval results.
type EvalReport struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Mode        string           `json:"mode"`
	Calls       []CallEvalResult `json:"calls,omitempty"`
	WER         *WERResult       `json:"wer,omitempty"`
	AMD         *AMDMetrics      `json:"amd,omitempty"`
	Latency     LatencyReport    `json:"latency"`
	Errors      []string         `json:"errors,omitempty"`
}

// CallEvalResult is per-call evaluation output.
type CallEvalResult struct {
	CallID       string  `json:"call_id"`
	Reference    string  `json:"reference,omitempty"`
	Hypothesis   string  `json:"hypothesis,omitempty"`
	WER          float64 `json:"wer,omitempty"`
	MouthToEarMs float64 `json:"mouth_to_ear_ms,omitempty"`
}

// WERResult aggregates word error rate across calls.
type WERResult struct {
	Mean   float64   `json:"mean"`
	Calls  int       `json:"calls"`
	Values []float64 `json:"values,omitempty"`
}

// LatencyReport holds mouth-to-ear and per-stage percentile summaries.
type LatencyReport struct {
	MouthToEar StageLatencySummary   `json:"mouth_to_ear"`
	Stages     []StageLatencySummary `json:"stages,omitempty"`
}

// WriteEvalReport writes JSON and a human-readable summary to outPath and summaryPath.
func WriteEvalReport(report EvalReport, outPath, summaryPath string) error {
	if report.GeneratedAt.IsZero() {
		report.GeneratedAt = time.Now().UTC()
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if outPath != "" {
		if err := os.WriteFile(outPath, data, 0o644); err != nil {
			return err
		}
	}
	if summaryPath != "" {
		if err := os.WriteFile(summaryPath, []byte(report.Summary()), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// Summary renders a human-readable evaluation report.
func (r EvalReport) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Eval Report (%s)\n", r.Mode)
	fmt.Fprintf(&b, "Generated: %s\n\n", r.GeneratedAt.Format(time.RFC3339))
	if r.WER != nil {
		fmt.Fprintf(&b, "WER: mean=%.3f over %d calls\n", r.WER.Mean, r.WER.Calls)
	}
	if r.AMD != nil {
		m := r.AMD
		fmt.Fprintf(&b, "AMD: accuracy=%.1f%% human_precision=%.1f%% human_recall=%.1f%%\n",
			m.Accuracy*100, m.PrecisionHuman*100, m.RecallHuman*100)
		fmt.Fprintf(&b, "     TP=%d FP=%d TN=%d FN=%d\n",
			m.Matrix.TruePositive, m.Matrix.FalsePositive, m.Matrix.TrueNegative, m.Matrix.FalseNegative)
	}
	l := r.Latency.MouthToEar
	if l.Count > 0 {
		fmt.Fprintf(&b, "Mouth-to-ear: p50=%.0fms p95=%.0fms p99=%.0fms (n=%d)\n",
			l.P50, l.P95, l.P99, l.Count)
	}
	for _, s := range r.Latency.Stages {
		fmt.Fprintf(&b, "Stage %s: p50=%.0f p95=%.0f p99=%.0f (n=%d)\n",
			s.Stage, s.P50, s.P95, s.P99, s.Count)
	}
	for _, c := range r.Calls {
		fmt.Fprintf(&b, "Call %s: wer=%.3f mouth_to_ear=%.0fms\n", c.CallID, c.WER, c.MouthToEarMs)
	}
	for _, e := range r.Errors {
		fmt.Fprintf(&b, "ERROR: %s\n", e)
	}
	return b.String()
}

// BuildWERResult aggregates per-call WER values.
func BuildWERResult(values []float64) *WERResult {
	if len(values) == 0 {
		return nil
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return &WERResult{
		Mean:   sum / float64(len(values)),
		Calls:  len(values),
		Values: append([]float64(nil), values...),
	}
}
