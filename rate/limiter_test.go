package rate

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// testClock is an injectable clock (micros) shared by a limiter's testNow.
type testClock struct{ micros atomic.Int64 }

func (c *testClock) set(t time.Duration) { c.micros.Store(t.Microseconds()) }
func (c *testClock) add(d time.Duration) { c.micros.Add(d.Microseconds()) }
func (c *testClock) now() int64          { return c.micros.Load() }

// newTestLimiter wires a limiter to a fresh miniredis with an injected clock so
// the algorithm math is fully controlled (DESIGN sec 9: miniredis FastForward
// does not drive redis TIME).
func newTestLimiter(t *testing.T, algo Algorithm, opts ...Option) (*Limiter, *testClock, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	lim, err := NewLimiter(rdb, "k", algo, opts...)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	clk := &testClock{}
	clk.set(time.Hour) // start away from zero
	lim.testNow = clk.now
	return lim, clk, mr
}

func TestTokenBucketAllowBurst(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 5})
	// Full bucket of 5 admits 5, then denies.
	for i := 0; i < 5; i++ {
		if !lim.Allow() {
			t.Fatalf("Allow %d: want true", i)
		}
	}
	if lim.Allow() {
		t.Fatal("Allow 6: want false (bucket empty)")
	}
}

func TestTokenBucketRefill(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 5})
	for i := 0; i < 5; i++ {
		lim.Allow()
	}
	if lim.Allow() {
		t.Fatal("want empty")
	}
	// 10 tokens/sec -> 1 token per 100ms. Advance 300ms -> 3 tokens.
	clk.add(300 * time.Millisecond)
	got := 0
	for i := 0; i < 5; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 3 {
		t.Fatalf("after 300ms refill: got %d, want 3", got)
	}
}

func TestTokenBucketAllowN(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 5})
	if !lim.AllowN(time.Now(), 5) {
		t.Fatal("AllowN(5): want true")
	}
	if lim.AllowN(time.Now(), 1) {
		t.Fatal("AllowN(1) after draining: want false")
	}
	// n > burst is never satisfiable.
	lim2, _, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 5})
	if lim2.AllowN(time.Now(), 6) {
		t.Fatal("AllowN(6) with burst 5: want false")
	}
}

func TestTokenBucketReserveDelay(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 5})
	for i := 0; i < 5; i++ {
		lim.Allow()
	}
	// Next token needs 100ms (10/sec).
	r := lim.ReserveN(time.Now(), 1)
	if !r.OK() {
		t.Fatal("Reserve: want OK")
	}
	d := r.Delay()
	if d < 90*time.Millisecond || d > 110*time.Millisecond {
		t.Fatalf("Delay = %v, want ~100ms", d)
	}
}

func TestTokenBucketReserveCancel(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 5})
	r := lim.ReserveN(time.Now(), 3) // 5 -> 2
	if !r.OK() {
		t.Fatal("Reserve(3): want OK")
	}
	if got := lim.Tokens(); got < 1.999 || got > 2.001 {
		t.Fatalf("tokens after reserve = %v, want ~2", got)
	}
	r.Cancel() // return the 3 reserved tokens: 2 -> 5
	if got := lim.Tokens(); got < 4.999 || got > 5.001 {
		t.Fatalf("tokens after cancel = %v, want ~5", got)
	}
	// Cancel is idempotent.
	r.Cancel()
	if got := lim.Tokens(); got > 5.001 {
		t.Fatalf("tokens after second cancel = %v, want ~5", got)
	}
}

func TestTokenBucketWait(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 1000, Burst: 1})
	lim.Allow() // drain the single token
	// Next token in ~1ms; Wait should block briefly then succeed.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := lim.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Wait blocked too long")
	}
}

func TestTokenBucketWaitDeadline(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 1, Burst: 1})
	lim.Allow() // drain; next token in 1s
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := lim.Wait(ctx); err == nil {
		t.Fatal("Wait: want error (delay exceeds deadline)")
	}
}

func TestSetLimitBurstRedisAuthoritative(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 5})
	lim.Allow()
	// Shrink burst to 2; stored tokens (4) clamp to 2 on next read.
	lim.SetBurst(2)
	if got := lim.Burst(); got != 2 {
		t.Fatalf("Burst() = %d, want 2", got)
	}
	got := 0
	for i := 0; i < 5; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 2 {
		t.Fatalf("admitted %d after SetBurst(2), want 2", got)
	}
	lim.SetLimit(50)
	if l := lim.Limit(); l != 50 {
		t.Fatalf("Limit() = %v, want 50", l)
	}
}

func TestMonotonicClockGuard(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 5})
	for i := 0; i < 5; i++ {
		lim.Allow()
	}
	// Clock jumps backward: must not refill negatively / grant extra tokens.
	clk.add(-200 * time.Millisecond)
	if lim.Allow() {
		t.Fatal("backward clock should not grant tokens")
	}
}

func TestInfShortCircuit(t *testing.T) {
	lim, _, mr := newTestLimiter(t, TokenBucket{Rate: Inf, Burst: 1})
	mr.Close() // Redis is gone; Inf must not touch it.
	for i := 0; i < 100; i++ {
		if !lim.Allow() {
			t.Fatal("Inf limiter must always allow")
		}
	}
	if err := lim.Wait(context.Background()); err != nil {
		t.Fatalf("Inf Wait: %v", err)
	}
}

func TestFailPolicy(t *testing.T) {
	mr, _ := miniredis.Run()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	var caught atomic.Int64
	lim, _ := NewLimiter(rdb, "k", TokenBucket{Rate: 10, Burst: 5},
		WithErrorHandler(func(error) { caught.Add(1) }))
	mr.Close() // force Redis errors
	if !lim.Allow() {
		t.Fatal("fail-open default: Allow should admit on error")
	}
	if caught.Load() == 0 {
		t.Fatal("error handler not invoked")
	}

	mr2, _ := miniredis.Run()
	defer mr2.Close()
	rdb2 := redis.NewClient(&redis.Options{Addr: mr2.Addr()})
	limc, _ := NewLimiter(rdb2, "k", TokenBucket{Rate: 10, Burst: 5}, WithFailClosed())
	mr2.Close()
	if limc.Allow() {
		t.Fatal("fail-closed: Allow should reject on error")
	}
}

func TestConcurrentGlobalLimit(t *testing.T) {
	// Many goroutines (simulating nodes) sharing one bucket admit ~burst total.
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 1, Burst: 20})
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if lim.Allow() {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := admitted.Load(); got != 20 {
		t.Fatalf("admitted %d concurrently, want exactly 20 (burst)", got)
	}
}

func TestKeyValidation(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cases := map[string]string{
		"":          "empty",
		"a b":       "whitespace",
		"a\x00b":    "control",
		"a{b":       "brace",
		"with}brace": "brace",
	}
	for key, name := range cases {
		if _, err := NewLimiter(rdb, key, TokenBucket{Rate: 1, Burst: 1}); err == nil {
			t.Errorf("key %q (%s): want error", key, name)
		}
	}
	if _, err := NewLimiter(rdb, "valid:key/ok-1.2", TokenBucket{Rate: 1, Burst: 1}); err != nil {
		t.Errorf("valid key rejected: %v", err)
	}
}
