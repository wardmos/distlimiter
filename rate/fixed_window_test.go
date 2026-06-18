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

func TestFixedWindowCancelNoOrphanKey(t *testing.T) {
	lim, clk, mr := newTestLimiter(t, FixedWindow{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second)
	r := lim.ReserveN(time.Now(), 2)
	if !r.OK() {
		t.Fatal("Reserve(2): want OK")
	}
	wkey := lim.kb.base + ":w10"
	if !mr.Exists(wkey) {
		t.Fatalf("window key %q should exist after reserve", wkey)
	}
	if ttl := mr.TTL(wkey); ttl <= 0 {
		t.Fatalf("window key TTL = %v, want > 0 after reserve", ttl)
	}
	r.Cancel() // counter back to 0 -> key dropped, no TTL-less orphan
	if mr.Exists(wkey) {
		t.Fatalf("window key %q should be gone after full cancel (no orphan)", wkey)
	}
}

func TestFixedWindowCancelKeepsTTL(t *testing.T) {
	lim, clk, mr := newTestLimiter(t, FixedWindow{Limit: 5, Window: time.Second})
	clk.set(10 * time.Second)
	lim.AllowN(time.Now(), 2) // count = 2
	r := lim.ReserveN(time.Now(), 1)
	if !r.OK() {
		t.Fatal("Reserve(1): want OK")
	}
	wkey := lim.kb.base + ":w10"
	r.Cancel() // count 3 -> 2, key survives with its TTL intact
	if !mr.Exists(wkey) {
		t.Fatalf("window key %q should still exist (count 2)", wkey)
	}
	if ttl := mr.TTL(wkey); ttl <= 0 {
		t.Fatalf("window key TTL = %v, want > 0 (not cleared by cancel)", ttl)
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
