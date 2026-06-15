// Package rate is a Redis-backed distributed rate limiter whose usage interface
// is identical to golang.org/x/time/rate.Limiter, with multiple interchangeable
// algorithms selected at construction time.
//
// The call-site interface (Allow/Wait/Reserve/...) mirrors x/time/rate so the
// package is a drop-in replacement once an application scales from one process
// to many: state lives in Redis and is shared across the fleet, enforcing a
// single global limit instead of one-limit-per-replica.
//
// Construction differs from x/time/rate: NewLimiter takes a go-redis client, a
// key, and a strongly-typed algorithm config value (TokenBucket, GCRA, ...).
// The canonical clock is the Redis server clock, so the t time.Time argument of
// AllowN/ReserveN is ignored. See the package README for the full list of
// intentional divergences.
package rate

import (
	"time"

	xrate "golang.org/x/time/rate"
)

// Limit defines the maximum frequency of some events, in events per second. It
// is re-exported from golang.org/x/time/rate so callers need only this package
// and keep full type parity with upstream.
type Limit = xrate.Limit

// Inf is the infinite rate limit; it allows all events (even with zero burst).
// Limiters configured with Inf short-circuit in Go and never touch Redis.
const Inf = xrate.Inf

// Every converts a minimum time interval between events to a Limit.
func Every(interval time.Duration) Limit {
	return xrate.Every(interval)
}
