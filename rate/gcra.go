package rate

import (
	"fmt"
	"math"
)

// GCRA is the Generic Cell Rate Algorithm: a single stored timestamp encodes
// the limiter state, giving the lowest memory footprint. Rate is the steady
// rate and Burst the tolerated burst size (same parameters as redis_rate).
type GCRA struct {
	Rate  Limit // events per second
	Burst int   // burst tolerance
}

func (GCRA) name() string { return "gcra" }

func (g GCRA) validate() error {
	if math.IsNaN(float64(g.Rate)) || g.Rate < 0 {
		return fmt.Errorf("rate: GCRA.Rate must be >= 0, got %v", g.Rate)
	}
	if g.Burst < 0 {
		return fmt.Errorf("rate: GCRA.Burst must be >= 0, got %d", g.Burst)
	}
	return nil
}

func (GCRA) scriptSrc() string { return gcraSrc }

func (g GCRA) configArgs() []any { return []any{formatRate(g.Rate), g.Burst} }

func (GCRA) setLimitFields(r Limit) []any { return []any{"rate", formatRate(r)} }
func (GCRA) setBurstFields(b int) []any   { return []any{"burst", b} }

func (g GCRA) isInf() bool    { return g.Rate == Inf }
func (GCRA) bestEffort() bool { return false }

func (g GCRA) decodeConfig(stored map[string]string) (Limit, int) {
	limit := g.Rate
	if v, ok := stored["rate"]; ok {
		limit = parseRate(v)
	}
	burst := g.Burst
	if v, ok := stored["burst"]; ok {
		if n, err := parseInt(v); err == nil {
			burst = n
		}
	}
	return limit, burst
}
