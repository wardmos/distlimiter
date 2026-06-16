package rate

import "time"

// SlidingWindowLog is an exact sliding window backed by a ZSET log of event
// timestamps. It admits at most Limit events in any trailing Window, with no
// boundary spike, at the cost of storing one entry per event.
type SlidingWindowLog struct {
	Limit  int
	Window time.Duration
}

func (SlidingWindowLog) name() string { return "sliding_log" }

func (s SlidingWindowLog) validate() error {
	return validateWindow("SlidingWindowLog", s.Limit, s.Window)
}

func (SlidingWindowLog) scriptSrc() string { return slidingLogSrc }

func (s SlidingWindowLog) configArgs() []any {
	return []any{s.Limit, s.Window.Microseconds()}
}

func (s SlidingWindowLog) setLimitFields(r Limit) []any { return windowSetLimitFields(r, s.Window) }
func (SlidingWindowLog) setBurstFields(b int) []any     { return []any{"limit", b} }

func (SlidingWindowLog) isInf() bool      { return false }
func (SlidingWindowLog) bestEffort() bool { return false }

func (s SlidingWindowLog) decodeConfig(stored map[string]string) (Limit, int) {
	return decodeWindowConfig(stored, s.Limit, s.Window)
}
