package rate

import (
	"testing"
	"time"
)

// Benchmarks run against miniredis, so they measure the library's per-call
// overhead (arg packing, script dispatch, decode), not real Redis latency.

func benchmarkAllow(b *testing.B, algo Algorithm) {
	b.Helper()
	lim, _, _ := newTestLimiter(b, algo)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lim.Allow()
	}
}

func BenchmarkAllowTokenBucket(b *testing.B) {
	benchmarkAllow(b, TokenBucket{Rate: 1e9, Burst: 1e9})
}

func BenchmarkAllowGCRA(b *testing.B) {
	benchmarkAllow(b, GCRA{Rate: 1e9, Burst: 1e9})
}

func BenchmarkAllowFixedWindow(b *testing.B) {
	benchmarkAllow(b, FixedWindow{Limit: 1e9, Window: time.Minute})
}

func BenchmarkAllowSlidingLog(b *testing.B) {
	benchmarkAllow(b, SlidingWindowLog{Limit: 1e9, Window: time.Minute})
}

func BenchmarkAllowSlidingCounter(b *testing.B) {
	benchmarkAllow(b, SlidingWindowCounter{Limit: 1e9, Window: time.Minute})
}

func BenchmarkReserveCancelTokenBucket(b *testing.B) {
	lim, _, _ := newTestLimiter(b, TokenBucket{Rate: 1e9, Burst: 1e9})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lim.Reserve().Cancel()
	}
}
