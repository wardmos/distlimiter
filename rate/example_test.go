package rate_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wardmos/distlimiter/rate"
)

// newClient builds a go-redis client for the examples. The examples have no
// "// Output:" line, so they are compiled (and documented in godoc) but not
// executed by `go test`; they therefore do not require a live Redis.
func newClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "localhost:6379"})
}

// ExampleNewLimiter shows a single-key token bucket: 10 events/second with a
// burst of 20, using the same Allow call site as golang.org/x/time/rate.
func ExampleNewLimiter() {
	rdb := newClient()
	limiter, err := rate.NewLimiter(rdb, "user:42",
		rate.TokenBucket{Rate: rate.Every(time.Second / 10), Burst: 20})
	if err != nil {
		panic(err)
	}
	if limiter.Allow() {
		fmt.Println("request admitted")
	} else {
		fmt.Println("request throttled")
	}
}

// ExampleNewStore shows the keyed factory pattern: configure the algorithm once,
// then derive a per-key limiter per request (e.g. per IP or tenant).
func ExampleNewStore() {
	rdb := newClient()
	store, err := rate.NewStore(rdb, rate.GCRA{Rate: 50, Burst: 10})
	if err != nil {
		panic(err)
	}
	limiter, err := store.Limiter("ip:1.2.3.4")
	if err != nil {
		panic(err)
	}
	_ = limiter.Allow()
}

// ExampleLimiter_Wait blocks until an event is permitted or the context is done.
func ExampleLimiter_Wait() {
	rdb := newClient()
	limiter := rate.MustNewLimiter(rdb, "api",
		rate.SlidingWindowLog{Limit: 100, Window: time.Minute})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := limiter.Wait(ctx); err != nil {
		fmt.Println("wait failed:", err)
		return
	}
	fmt.Println("proceeding")
}

// ExampleLimiter_Reserve reserves a slot, inspects the delay, and cancels it if
// the work is abandoned (returning the credit to the limiter).
func ExampleLimiter_Reserve() {
	rdb := newClient()
	limiter := rate.MustNewLimiter(rdb, "job",
		rate.TokenBucket{Rate: 5, Burst: 5})

	r := limiter.Reserve()
	if !r.OK() {
		fmt.Println("cannot reserve")
		return
	}
	if d := r.Delay(); d > 250*time.Millisecond {
		// Too long to wait: give the slot back.
		r.Cancel()
		fmt.Println("reservation cancelled")
		return
	}
	time.Sleep(r.Delay())
	fmt.Println("acting now")
}

// ExampleLeakyBucket constructs a leaky-bucket limiter: a queue of up to 30
// events draining at a constant 10 events/second.
func ExampleLeakyBucket() {
	rdb := newClient()
	_ = rate.MustNewLimiter(rdb, "egress",
		rate.LeakyBucket{Rate: 10, Capacity: 30})
}

// ExampleFixedWindow constructs a fixed-window limiter: 1000 events per minute.
func ExampleFixedWindow() {
	rdb := newClient()
	_ = rate.MustNewLimiter(rdb, "tenant:7",
		rate.FixedWindow{Limit: 1000, Window: time.Minute})
}

// ExampleSlidingWindowLog constructs an exact sliding-window limiter.
func ExampleSlidingWindowLog() {
	rdb := newClient()
	_ = rate.MustNewLimiter(rdb, "search",
		rate.SlidingWindowLog{Limit: 100, Window: time.Minute})
}

// ExampleSlidingWindowCounter constructs a memory-light approximate sliding
// window (best-effort reservation/cancel).
func ExampleSlidingWindowCounter() {
	rdb := newClient()
	_ = rate.MustNewLimiter(rdb, "feed",
		rate.SlidingWindowCounter{Limit: 100, Window: time.Minute},
		rate.WithFailOpen(), rate.WithTimeout(100*time.Millisecond))
}
