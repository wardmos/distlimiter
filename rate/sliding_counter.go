package rate

import "time"

// SlidingWindowCounter approximates a sliding window using two fixed-window
// counters weighted by overlap. It is memory-light (two integers) and avoids
// the fixed-window boundary spike, but is best-effort: Reserve/Delay are
// estimates and Cancel is a no-op.
type SlidingWindowCounter struct {
	Limit  int
	Window time.Duration
}

func (SlidingWindowCounter) name() string { return "sliding_counter" }

func (s SlidingWindowCounter) validate() error {
	return validateWindow("SlidingWindowCounter", s.Limit, s.Window)
}

func (SlidingWindowCounter) scriptSrc() string { return slidingCounterSrc }

func (s SlidingWindowCounter) configArgs() []any {
	return []any{s.Limit, s.Window.Microseconds()}
}

func (s SlidingWindowCounter) setLimitFields(r Limit) []any {
	return windowSetLimitFields(r, s.Window)
}
func (SlidingWindowCounter) setBurstFields(b int) []any { return []any{"limit", b} }

func (SlidingWindowCounter) isInf() bool      { return false }
func (SlidingWindowCounter) bestEffort() bool { return true }

func (s SlidingWindowCounter) decodeConfig(stored map[string]string) (Limit, int) {
	return decodeWindowConfig(stored, s.Limit, s.Window)
}
