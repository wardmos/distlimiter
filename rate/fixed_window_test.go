package rate

import (
	"testing"
	"time"
)

func TestFixedWindowLimit(t *testing.T) {
	lim, _, _ := newTestLimiter(t, FixedWindow{Limit: 5, Window: time.Second})
	got := 0
	for i := 0; i < 10; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 5 {
		t.Fatalf("fixed window: admitted %d, want 5", got)
	}
}

func TestFixedWindowRollover(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, FixedWindow{Limit: 5, Window: time.Second})
	// Align to a window start for determinism.
	clk.set(10 * time.Second)
	for i := 0; i < 5; i++ {
		lim.Allow()
	}
	if lim.Allow() {
		t.Fatal("want throttled within window")
	}
	// Cross into the next window: quota resets.
	clk.add(time.Second)
	got := 0
	for i := 0; i < 10; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 5 {
		t.Fatalf("after rollover: admitted %d, want 5", got)
	}
}

func TestFixedWindowReserveFutureSlot(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, FixedWindow{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second) // window start
	for i := 0; i < 5; i++ {
		lim.Allow()
	}
	// Current window full; Reserve books into the next window.
	r := lim.ReserveN(time.Now(), 1)
	if !r.OK() {
		t.Fatal("Reserve: want OK (future window)")
	}
	d := r.Delay()
	if d < 900*time.Millisecond || d > 1100*time.Millisecond {
		t.Fatalf("Delay = %v, want ~1s (next window start)", d)
	}
}

func TestFixedWindowReserveCancel(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, FixedWindow{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second)
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
