package media

// SessionObservability bundles per-session CT-12 timing and watchdog hooks.
type SessionObservability struct {
	Timing   *TurnTimingHub
	Watchdog *DeadAirWatchdog
	Metrics  *Metrics
}
