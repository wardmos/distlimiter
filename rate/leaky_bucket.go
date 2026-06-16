package rate

import (
	"fmt"
	"math"
)

// LeakyBucket models a queue that drains at a constant Rate. Events join the
// queue (up to Capacity); the steady admission rate equals the leak Rate, which
// smooths bursts into a constant outflow.
type LeakyBucket struct {
	Rate     Limit // leak rate, events per second
	Capacity int   // queue capacity
}

func (LeakyBucket) name() string { return "leaky_bucket" }

func (lb LeakyBucket) validate() error {
	if math.IsNaN(float64(lb.Rate)) || lb.Rate < 0 {
		return fmt.Errorf("rate: LeakyBucket.Rate must be >= 0, got %v", lb.Rate)
	}
	if lb.Capacity < 0 {
		return fmt.Errorf("rate: LeakyBucket.Capacity must be >= 0, got %d", lb.Capacity)
	}
	return nil
}

func (LeakyBucket) scriptSrc() string { return leakyBucketSrc }

func (lb LeakyBucket) configArgs() []any { return []any{formatRate(lb.Rate), lb.Capacity} }

func (LeakyBucket) setLimitFields(r Limit) []any { return []any{"rate", formatRate(r)} }
func (LeakyBucket) setBurstFields(b int) []any   { return []any{"cap", b} }

func (lb LeakyBucket) isInf() bool   { return lb.Rate == Inf }
func (LeakyBucket) bestEffort() bool { return false }

func (lb LeakyBucket) decodeConfig(stored map[string]string) (Limit, int) {
	limit := lb.Rate
	if v, ok := stored["rate"]; ok {
		limit = parseRate(v)
	}
	capacity := lb.Capacity
	if v, ok := stored["cap"]; ok {
		if n, err := parseInt(v); err == nil {
			capacity = n
		}
	}
	return limit, capacity
}
