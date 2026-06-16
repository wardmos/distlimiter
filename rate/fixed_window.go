package rate

import "time"

// FixedWindow caps events per fixed calendar window (Limit per Window). It is
// the cheapest algorithm but allows up to 2x Limit across a window boundary.
type FixedWindow struct {
	Limit  int
	Window time.Duration
}

func (FixedWindow) name() string { return "fixed_window" }

func (fw FixedWindow) validate() error {
	return validateWindow("FixedWindow", fw.Limit, fw.Window)
}

func (FixedWindow) scriptSrc() string { return fixedWindowSrc }

func (fw FixedWindow) configArgs() []any {
	return []any{fw.Limit, fw.Window.Microseconds()}
}

func (fw FixedWindow) setLimitFields(r Limit) []any { return windowSetLimitFields(r, fw.Window) }
func (FixedWindow) setBurstFields(b int) []any      { return []any{"limit", b} }

func (FixedWindow) isInf() bool      { return false }
func (FixedWindow) bestEffort() bool { return false }

func (fw FixedWindow) decodeConfig(stored map[string]string) (Limit, int) {
	return decodeWindowConfig(stored, fw.Limit, fw.Window)
}
