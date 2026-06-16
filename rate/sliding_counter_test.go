package rate

import (
	"context"
	"testing"
	"time"
)

func TestSlidingCounterLimit(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, SlidingWindowCounter{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second)
	got := 0
	for i := 0; i < 10; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 5 {
		t.Fatalf("sliding counter: admitted %d, want 5", got)
	}
}

func TestSlidingCounterWeightedDecay(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, SlidingWindowCounter{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second)
	for i := 0; i < 5; i++ {
		lim.Allow() // fill window 10
	}
	// At t=11.5s: window 10 weighted 0.5 -> estimate 2.5; room for 2 more.
	clk.set(11500 * time.Millisecond)
	got := 0
	for i := 0; i < 5; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 2 {
		t.Fatalf("weighted estimate at +1.5 windows: admitted %d, want 2", got)
	}
}

func TestSlidingCounterCancelIsNoop(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, SlidingWindowCounter{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second)
	r := lim.ReserveN(time.Now(), 3)
	if !r.OK() {
		t.Fatal("Reserve(3): want OK")
	}
	before := lim.Tokens()
	r.Cancel() // best-effort: no-op, must not change state
	if after := lim.Tokens(); after != before {
		t.Fatalf("Cancel changed state: before=%v after=%v (want no-op)", before, after)
	}
}

func TestSlidingCounterWaitRetries(t *testing.T) {
	// Best-effort Wait verifies-and-retries; with a fast clock it should give up
	// at the deadline rather than block forever.
	lim, clk, _ := newTestLimiter(t, SlidingWindowCounter{Limit: 1, Window: time.Hour})
	clk.set(time.Hour)
	if !lim.Allow() {
		t.Fatal("first Allow should succeed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := lim.Wait(ctx); err == nil {
		t.Fatal("Wait: want deadline error (window is an hour)")
	}
}
