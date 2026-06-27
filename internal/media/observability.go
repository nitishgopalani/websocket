package media

// SessionObservability bundles per-session CT-12 timing and watchdog hooks.
type SessionObservability struct {
	Timing      *TurnTimingHub
	Watchdog    *DeadAirWatchdog
	Metrics     *Metrics
	TurnManager *TurnManager
}

// Shutdown cancels in-flight timers so teardown does not fire orphaned callbacks.
func (o *SessionObservability) Shutdown() {
	if o == nil {
		return
	}
	if o.Watchdog != nil {
		o.Watchdog.CancelAll()
	}
	if o.TurnManager != nil {
		_ = o.TurnManager.Close()
	}
}
