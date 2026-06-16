package rate

import (
	"testing"
	"time"
)

func TestGCRAAllowBurst(t *testing.T) {
	lim, _, _ := newTestLimiter(t, GCRA{Rate: 10, Burst: 5})
	got := 0
	for i := 0; i < 10; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 5 {
		t.Fatalf("GCRA burst: admitted %d, want 5", got)
	}
}

func TestGCRARefill(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, GCRA{Rate: 10, Burst: 5})
	for i := 0; i < 5; i++ {
		lim.Allow()
	}
	if lim.Allow() {
		t.Fatal("want throttled")
	}
	// 10/sec -> one token every 100ms.
	clk.add(100 * time.Millisecond)
	if !lim.Allow() {
		t.Fatal("want one token after 100ms")
	}
	if lim.Allow() {
		t.Fatal("want throttled again")
	}
}

func TestGCRANGreaterThanBurst(t *testing.T) {
	lim, _, _ := newTestLimiter(t, GCRA{Rate: 10, Burst: 5})
	if lim.AllowN(time.Now(), 6) {
		t.Fatal("AllowN(6) with burst 5: want false")
	}
	if r := lim.ReserveN(time.Now(), 6); r.OK() {
		t.Fatal("ReserveN(6) with burst 5: want not OK")
	}
}

func TestGCRAReserveCancel(t *testing.T) {
	lim, _, _ := newTestLimiter(t, GCRA{Rate: 10, Burst: 5})
	r := lim.ReserveN(time.Now(), 3)
	if !r.OK() {
		t.Fatal("Reserve(3): want OK")
	}
	before := lim.Tokens()
	if before < 1.999 || before > 2.001 {
		t.Fatalf("tokens after reserve = %v, want ~2", before)
	}
	r.Cancel()
	if got := lim.Tokens(); got < 4.999 || got > 5.001 {
		t.Fatalf("tokens after cancel = %v, want ~5", got)
	}
}

func TestLeakyBucketCapacity(t *testing.T) {
	lim, _, _ := newTestLimiter(t, LeakyBucket{Rate: 10, Capacity: 5})
	got := 0
	for i := 0; i < 10; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 5 {
		t.Fatalf("leaky capacity: admitted %d, want 5", got)
	}
}

func TestLeakyBucketLeak(t *testing.T) {
	lim, clk, _ := newTestLimiter(t, LeakyBucket{Rate: 10, Capacity: 5})
	for i := 0; i < 5; i++ {
		lim.Allow()
	}
	if lim.Allow() {
		t.Fatal("want full")
	}
	// Leaks 10/sec -> 3 leak out in 300ms.
	clk.add(300 * time.Millisecond)
	got := 0
	for i := 0; i < 5; i++ {
		if lim.Allow() {
			got++
		}
	}
	if got != 3 {
		t.Fatalf("after 300ms leak: admitted %d, want 3", got)
	}
}

func TestLeakyBucketReserveCancel(t *testing.T) {
	lim, _, _ := newTestLimiter(t, LeakyBucket{Rate: 10, Capacity: 5})
	r := lim.ReserveN(time.Now(), 3) // level 0 -> 3, room 2
	if !r.OK() {
		t.Fatal("Reserve(3): want OK")
	}
	if got := lim.Tokens(); got < 1.999 || got > 2.001 {
		t.Fatalf("room after reserve = %v, want ~2", got)
	}
	r.Cancel() // level 3 -> 0, room 5
	if got := lim.Tokens(); got < 4.999 || got > 5.001 {
		t.Fatalf("room after cancel = %v, want ~5", got)
	}
}
