package rate

import (
	"testing"
	"time"
)

// allAlgorithms returns one config per algorithm, each tuned to "5 events per
// unit" so a single set of cases can run against all of them through the shared
// interface (DESIGN sec 9).
func allAlgorithms() []struct {
	name string
	algo Algorithm
} {
	return []struct {
		name string
		algo Algorithm
	}{
		{"token_bucket", TokenBucket{Rate: 10, Burst: 5}},
		{"gcra", GCRA{Rate: 10, Burst: 5}},
		{"leaky_bucket", LeakyBucket{Rate: 10, Capacity: 5}},
		{"fixed_window", FixedWindow{Limit: 5, Window: time.Second}},
		{"sliding_log", SlidingWindowLog{Limit: 5, Window: time.Second}},
		{"sliding_counter", SlidingWindowCounter{Limit: 5, Window: time.Second}},
	}
}

func TestCrossAdmitExactlyLimit(t *testing.T) {
	for _, c := range allAlgorithms() {
		t.Run(c.name, func(t *testing.T) {
			lim, _, _ := newTestLimiter(t, c.algo)
			got := 0
			for i := 0; i < 20; i++ {
				if lim.Allow() {
					got++
				}
			}
			if got != 5 {
				t.Fatalf("admitted %d from a fresh limiter, want 5", got)
			}
		})
	}
}

func TestCrossNTooLargeNeverAllowed(t *testing.T) {
	for _, c := range allAlgorithms() {
		t.Run(c.name, func(t *testing.T) {
			lim, _, _ := newTestLimiter(t, c.algo)
			if lim.AllowN(time.Now(), 6) {
				t.Fatal("AllowN(6) over a capacity of 5: want false")
			}
			if r := lim.ReserveN(time.Now(), 6); r.OK() {
				t.Fatal("ReserveN(6) over a capacity of 5: want not OK")
			}
		})
	}
}

func TestCrossTokensReflectUsage(t *testing.T) {
	for _, c := range allAlgorithms() {
		t.Run(c.name, func(t *testing.T) {
			lim, _, _ := newTestLimiter(t, c.algo)
			if !lim.AllowN(time.Now(), 2) {
				t.Fatal("AllowN(2): want true")
			}
			got := lim.Tokens()
			if got < 2.999 || got > 3.001 {
				t.Fatalf("Tokens() = %v after using 2 of 5, want ~3", got)
			}
		})
	}
}

func TestCrossLimitBurstGetters(t *testing.T) {
	for _, c := range allAlgorithms() {
		t.Run(c.name, func(t *testing.T) {
			lim, _, _ := newTestLimiter(t, c.algo)
			// Burst()/Limit() must be answerable for every algorithm (approximate
			// for windows). Just assert they are positive and don't error-panic.
			if b := lim.Burst(); b != 5 {
				t.Fatalf("Burst() = %d, want 5", b)
			}
			if l := lim.Limit(); l <= 0 {
				t.Fatalf("Limit() = %v, want > 0", l)
			}
		})
	}
}

func TestCrossZeroEventsAlwaysAllowed(t *testing.T) {
	for _, c := range allAlgorithms() {
		t.Run(c.name, func(t *testing.T) {
			lim, _, _ := newTestLimiter(t, c.algo)
			for i := 0; i < 10; i++ {
				lim.Allow() // drain
			}
			// A zero-sized request is always permitted (mirrors x/time/rate).
			if ok, err := lim.AllowNContext(t.Context(), time.Now(), 0); err != nil || !ok {
				t.Fatalf("AllowN(0) on a drained limiter: ok=%v err=%v, want true/nil", ok, err)
			}
		})
	}
}
