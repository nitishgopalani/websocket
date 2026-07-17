package media

// TurnPolicy configures optional translator-oriented turn boundary behavior.
// Zero value disables all extensions (legacy CT-6 behavior unchanged).
type TurnPolicy struct {
	// CompletenessLang is the base language tag ("en", "hi") for adaptive endpointing.
	CompletenessLang string
	// IncompleteExtraMs extends endpoint wait for incomplete fragments only.
	IncompleteExtraMs int
	// FillerSuppress returns true when text is pure filler and must not close the turn.
	FillerSuppress func(text string) bool
	// OnFillerSuppressed is invoked when a would-be end_of_turn is suppressed as filler.
	OnFillerSuppressed func(text string)
	// OnTurnMerged is invoked when a new ASR final merges into an open turn.
	OnTurnMerged func()
}
