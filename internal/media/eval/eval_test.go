package eval_test

import (
	"math"
	"testing"

	"websocket/internal/media/eval"
)

func TestWERKnownPairs(t *testing.T) {
	cases := []struct {
		ref, hyp string
		want     float64
	}{
		{"hello world", "hello world", 0},
		{"hello world", "hello word", 0.5},
		{"a b c", "a b c d", 1.0 / 3.0},
		{"", "", 0},
		{"", "hello", 1},
	}
	for _, tc := range cases {
		got := eval.WER(tc.ref, tc.hyp)
		if math.Abs(got-tc.want) > 0.01 {
			t.Fatalf("WER(%q,%q)=%f want %f", tc.ref, tc.hyp, got, tc.want)
		}
	}
}

func TestWERFromFinals(t *testing.T) {
	got := eval.WERFromFinals("haan ji theek hai", []string{"haan ji", "theek hai"})
	if got != 0 {
		t.Fatalf("WERFromFinals = %f want 0", got)
	}
}

func TestAMDEvalConfusionMatrix(t *testing.T) {
	samples := []eval.AMDSample{
		{ID: "h1", Label: eval.AMDLabelHuman},
		{ID: "h2", Label: eval.AMDLabelHuman},
		{ID: "v1", Label: eval.AMDLabelVoicemail},
		{ID: "v2", Label: eval.AMDLabelVoicemail},
	}
	preds := []eval.AMDPrediction{
		{SampleID: "h1", Human: true},
		{SampleID: "h2", Human: false},
		{SampleID: "v1", Human: false},
		{SampleID: "v2", Human: true},
	}
	m := eval.EvaluateAMD(samples, preds)
	if m.Matrix.TruePositive != 1 || m.Matrix.FalseNegative != 1 ||
		m.Matrix.TrueNegative != 1 || m.Matrix.FalsePositive != 1 {
		t.Fatalf("matrix = %+v", m.Matrix)
	}
	if math.Abs(m.Accuracy-0.5) > 0.001 {
		t.Fatalf("accuracy = %f", m.Accuracy)
	}
	if math.Abs(m.PrecisionHuman-0.5) > 0.001 {
		t.Fatalf("human precision = %f want 0.5", m.PrecisionHuman)
	}
}

func TestLatencyPercentiles(t *testing.T) {
	samples := []float64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	p50, p95, p99 := eval.Percentiles(samples)
	if p50 < 500 || p50 > 600 {
		t.Fatalf("p50 = %f", p50)
	}
	if p95 < 900 {
		t.Fatalf("p95 = %f", p95)
	}
	if p99 < 950 {
		t.Fatalf("p99 = %f", p99)
	}
}

func TestSummarizeStageLatencies(t *testing.T) {
	summary := eval.SummarizeStageLatencies(map[string][]float64{
		"engine_ms": {100, 200, 300},
	})
	if len(summary) != 1 || summary[0].Stage != "engine_ms" || summary[0].Count != 3 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestEvalReportSummary(t *testing.T) {
	r := eval.EvalReport{
		Mode: "live",
		WER:  &eval.WERResult{Mean: 0.12, Calls: 5},
		AMD: &eval.AMDMetrics{
			Accuracy:       0.9,
			PrecisionHuman: 0.95,
			RecallHuman:    0.88,
			Matrix:         eval.ConfusionMatrix{TruePositive: 8, FalsePositive: 1},
		},
		Latency: eval.LatencyReport{
			MouthToEar: eval.StageLatencySummary{P50: 800, P95: 1200, P99: 1500, Count: 10},
		},
	}
	s := r.Summary()
	if s == "" || len(s) < 20 {
		t.Fatal("empty summary")
	}
}
