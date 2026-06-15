package rate

import (
	"fmt"
	"math"
)

// TokenBucket is the same algorithm as golang.org/x/time/rate: tokens refill at
// Rate up to a maximum of Burst, and each event consumes one token.
type TokenBucket struct {
	Rate  Limit // events per second
	Burst int   // bucket capacity
}

func (TokenBucket) name() string { return "token_bucket" }

func (tb TokenBucket) validate() error {
	if math.IsNaN(float64(tb.Rate)) || tb.Rate < 0 {
		return fmt.Errorf("rate: TokenBucket.Rate must be >= 0, got %v", tb.Rate)
	}
	if tb.Burst < 0 {
		return fmt.Errorf("rate: TokenBucket.Burst must be >= 0, got %d", tb.Burst)
	}
	return nil
}

func (TokenBucket) scriptSrc() string { return tokenBucketSrc }

func (tb TokenBucket) configArgs() []any {
	return []any{formatRate(tb.Rate), tb.Burst}
}

func (TokenBucket) setLimitFields(r Limit) []any { return []any{"rate", formatRate(r)} }
func (TokenBucket) setBurstFields(b int) []any   { return []any{"burst", b} }

func (tb TokenBucket) isInf() bool   { return tb.Rate == Inf }
func (TokenBucket) bestEffort() bool { return false }

func (tb TokenBucket) decodeConfig(stored map[string]string) (Limit, int) {
	limit := tb.Rate
	if v, ok := stored["rate"]; ok {
		limit = parseRate(v)
	}
	burst := tb.Burst
	if v, ok := stored["burst"]; ok {
		if n, err := parseInt(v); err == nil {
			burst = n
		}
	}
	return limit, burst
}
