package eval

// AMDLabel is the ground-truth label for AMD evaluation.
type AMDLabel string

const (
	AMDLabelHuman     AMDLabel = "human"
	AMDLabelVoicemail AMDLabel = "voicemail"
)

// AMDPrediction is a classifier output for one sample.
type AMDPrediction struct {
	SampleID string
	Human    bool
}

// AMDSample pairs an audio sample ID with its label.
type AMDSample struct {
	ID    string
	Label AMDLabel
}

// ConfusionMatrix tallies AMD predictions vs labels.
type ConfusionMatrix struct {
	TruePositive  int // human predicted human
	FalsePositive int // human predicted voicemail (machine)
	TrueNegative  int // voicemail predicted voicemail (machine)
	FalseNegative int // voicemail predicted human
}

// AMDMetrics summarizes AMD evaluation with human-precision emphasis.
type AMDMetrics struct {
	Matrix           ConfusionMatrix
	Accuracy         float64
	PrecisionHuman   float64
	RecallHuman      float64
	PrecisionMachine float64
	RecallMachine    float64
}

// EvaluateAMD compares predictions to labeled samples.
func EvaluateAMD(samples []AMDSample, predictions []AMDPrediction) AMDMetrics {
	labelByID := make(map[string]AMDLabel, len(samples))
	for _, s := range samples {
		labelByID[s.ID] = s.Label
	}
	var m ConfusionMatrix
	for _, p := range predictions {
		label, ok := labelByID[p.SampleID]
		if !ok {
			continue
		}
		actualHuman := label == AMDLabelHuman
		if actualHuman && p.Human {
			m.TruePositive++
		} else if actualHuman && !p.Human {
			m.FalseNegative++
		} else if !actualHuman && p.Human {
			m.FalsePositive++
		} else {
			m.TrueNegative++
		}
	}
	total := m.TruePositive + m.FalsePositive + m.TrueNegative + m.FalseNegative
	var accuracy float64
	if total > 0 {
		accuracy = float64(m.TruePositive+m.TrueNegative) / float64(total)
	}
	var precHuman, recHuman, precMachine, recMachine float64
	if m.TruePositive+m.FalsePositive > 0 {
		precHuman = float64(m.TruePositive) / float64(m.TruePositive+m.FalsePositive)
	}
	if m.TruePositive+m.FalseNegative > 0 {
		recHuman = float64(m.TruePositive) / float64(m.TruePositive+m.FalseNegative)
	}
	if m.TrueNegative+m.FalseNegative > 0 {
		precMachine = float64(m.TrueNegative) / float64(m.TrueNegative+m.FalseNegative)
	}
	if m.TrueNegative+m.FalsePositive > 0 {
		recMachine = float64(m.TrueNegative) / float64(m.TrueNegative+m.FalsePositive)
	}
	return AMDMetrics{
		Matrix:           m,
		Accuracy:         accuracy,
		PrecisionHuman:   precHuman,
		RecallHuman:      recHuman,
		PrecisionMachine: precMachine,
		RecallMachine:    recMachine,
	}
}
