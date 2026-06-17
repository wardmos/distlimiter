//go:build integration

package rate

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These tests run against a real Redis (REDIS_ADDR, default localhost:6379) and
// are gated behind the `integration` build tag, so they do not run in the
// default `go test` pass. They verify the behaviors miniredis cannot fully
// guarantee (DESIGN sec 9): the redis.call('TIME') clock path, server-side
// atomicity under concurrency, and TTL reclamation.

func realRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("real Redis unavailable at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// uniqueKey keeps integration runs from colliding on shared Redis state.
func uniqueKey(t *testing.T) string {
	t.Helper()
	return "itest:" + t.Name() + ":" + time.Now().Format("150405.000000000")
}

// TestIntegrationServerClock checks that the limiter works with no injected
// clock, i.e. driven purely by redis.call('TIME').
func TestIntegrationServerClock(t *testing.T) {
	rdb := realRedis(t)
	lim, err := NewLimiter(rdb, uniqueKey(t), TokenBucket{Rate: 5, Burst: 5})
	if err != nil {
		t.Fatal(err)
	}
	// testNow stays nil: the script falls back to the server clock.
	if lim.testNow != nil {
		t.Fatal("testNow must be nil for the server-clock path")
	}
	admitted := 0
	for i := 0; i < 10; i++ {
		if lim.Allow() {
			admitted++
		}
	}
	if admitted != 5 {
		t.Fatalf("server-clock token bucket: admitted %d, want 5 (burst)", admitted)
	}
}

// TestIntegrationRefill verifies real-time refill against the server clock.
func TestIntegrationRefill(t *testing.T) {
	rdb := realRedis(t)
	lim, err := NewLimiter(rdb, uniqueKey(t), TokenBucket{Rate: 100, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !lim.Allow() {
		t.Fatal("first Allow should succeed")
	}
	if lim.Allow() {
		t.Fatal("second Allow should be throttled")
	}
	time.Sleep(50 * time.Millisecond) // ~5 tokens at 100/s; >=1 available
	if !lim.Allow() {
		t.Fatal("Allow should succeed after refill")
	}
}

// TestIntegrationAtomicConcurrency hammers one bucket from many goroutines and
// asserts the atomic Lua admits exactly the burst.
func TestIntegrationAtomicConcurrency(t *testing.T) {
	rdb := realRedis(t)
	const burst = 50
	lim, err := NewLimiter(rdb, uniqueKey(t), TokenBucket{Rate: 1, Burst: burst})
	if err != nil {
		t.Fatal(err)
	}
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if lim.Allow() {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := admitted.Load(); got != burst {
		t.Fatalf("concurrent admits = %d, want exactly %d (atomic burst)", got, burst)
	}
}

// TestIntegrationTTLReclaim verifies idle keys are reclaimed via PEXPIRE.
func TestIntegrationTTLReclaim(t *testing.T) {
	rdb := realRedis(t)
	key := uniqueKey(t)
	lim, err := NewLimiter(rdb, key, TokenBucket{Rate: 10, Burst: 1},
		WithTTLMargin(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if !lim.Allow() {
		t.Fatal("Allow should succeed and create state")
	}
	base := lim.kb.base
	ctx := context.Background()
	if n, _ := rdb.Exists(ctx, base).Result(); n != 1 {
		t.Fatalf("state key %q should exist after Allow", base)
	}
	// Full refill (1 token at 10/s = 100ms) + 200ms margin; wait past it.
	time.Sleep(time.Second)
	if n, _ := rdb.Exists(ctx, base).Result(); n != 0 {
		t.Fatalf("state key %q should have been reclaimed by TTL", base)
	}
}
