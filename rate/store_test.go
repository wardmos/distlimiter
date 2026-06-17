package rate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T, algo Algorithm, opts ...Option) *Store {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	s, err := NewStore(rdb, algo, opts...)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestStoreKeysAreIndependent(t *testing.T) {
	s := newTestStore(t, TokenBucket{Rate: 10, Burst: 3})
	a := s.MustLimiter("tenant:a")
	b := s.MustLimiter("tenant:b")
	// Drain a; b must be unaffected.
	for i := 0; i < 3; i++ {
		if !a.Allow() {
			t.Fatalf("a Allow %d: want true", i)
		}
	}
	if a.Allow() {
		t.Fatal("a should be drained")
	}
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("b Allow %d: want true (independent key)", i)
		}
	}
}

func TestStoreLimiterValidatesKey(t *testing.T) {
	s := newTestStore(t, TokenBucket{Rate: 10, Burst: 3})
	if _, err := s.Limiter("bad key"); err == nil {
		t.Fatal("Store.Limiter: want error for invalid key")
	}
	if _, err := s.Limiter("good:key"); err != nil {
		t.Fatalf("Store.Limiter: unexpected error: %v", err)
	}
}

func TestStoreSharesScriptCache(t *testing.T) {
	s := newTestStore(t, GCRA{Rate: 5, Burst: 2})
	l1 := s.MustLimiter("k1")
	l2 := s.MustLimiter("k2")
	if l1.scripter != l2.scripter {
		t.Fatal("limiters from one Store should share the scripter (script cache)")
	}
}

func TestNewStoreValidatesAlgorithm(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	if _, err := NewStore(rdb, FixedWindow{Limit: 5, Window: 0}); err == nil {
		t.Fatal("NewStore: want error for invalid window")
	}
}

func TestStoreInheritsOptions(t *testing.T) {
	s := newTestStore(t, TokenBucket{Rate: 10, Burst: 3}, WithKeyPrefix("custom"))
	l := s.MustLimiter("x")
	if want := "custom:{x}"; l.kb.base != want {
		t.Fatalf("base = %q, want %q", l.kb.base, want)
	}
	_ = time.Second
}
