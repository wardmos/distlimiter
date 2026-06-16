package rate

import (
	"testing"
	"time"
)

func TestSlidingLogLimit(t *testing.T) {
	lim, _, _ := newTestLimiter(t, SlidingWindowLog{Limit: 5, Window: time.Second})
	got := 0
	for i := 0; i < 10; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 5 {
		t.Fatalf("sliding log: admitted %d, want 5", got)
	}
}

func TestSlidingLogNoBoundarySpike(t *testing.T) {
	// Unlike fixed window, sliding log never admits 2x Limit across a boundary.
	lim, clk, _ := newTestLimiter(t, SlidingWindowLog{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second)
	for i := 0; i < 5; i++ {
		lim.Allow() // 5 at t=10s
	}
	clk.add(500 * time.Millisecond) // t=10.5s: original 5 still in window
	got := 0
	for i := 0; i < 5; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 0 {
		t.Fatalf("at +500ms still within window: admitted %d, want 0", got)
	}
	clk.add(600 * time.Millisecond) // t=11.1s: original 5 aged out
	got = 0
	for i := 0; i < 10; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 5 {
		t.Fatalf("after window slid past: admitted %d, want 5", got)
	}
}

func TestSlidingLogReserveDelay(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, SlidingWindowLog{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second)
	for i := 0; i < 5; i++ {
		lim.Allow() // fill at t=10s
	}
	// Next event must wait until the oldest entry (t=10s) leaves the window at t=11s.
	r := lim.ReserveN(time.Now(), 1)
	if !r.OK() {
		t.Fatal("Reserve: want OK (future slot)")
	}
	d := r.Delay()
	if d < 900*time.Millisecond || d > 1100*time.Millisecond {
		t.Fatalf("Delay = %v, want ~1s", d)
	}
}

func TestSlidingLogReserveCancel(t *testing.T) {
	lim, _, _ := newTestLimiter(t, SlidingWindowLog{Limit: 5, Window: time.Second})
	r := lim.ReserveN(time.Now(), 3)
	if !r.OK() {
		t.Fatal("Reserve(3): want OK")
	}
	if got := lim.Tokens(); got != 2 {
		t.Fatalf("remaining after reserve = %v, want 2", got)
	}
	r.Cancel()
	if got := lim.Tokens(); got != 5 {
		t.Fatalf("remaining after cancel = %v, want 5", got)
	}
}
