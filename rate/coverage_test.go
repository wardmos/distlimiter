package rate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// fastClient builds a client that fails quickly when Redis is down (no retry
// storm), keeping the error-path tests fast.
func fastClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:        addr,
		MaxRetries:  -1,
		DialTimeout: 100 * time.Millisecond,
	})
}

func TestAllowContext(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 10, Burst: 2})
	ok, err := lim.AllowContext(context.Background())
	if err != nil || !ok {
		t.Fatalf("AllowContext: ok=%v err=%v, want true/nil", ok, err)
	}
	if ok, err := lim.AllowNContext(context.Background(), time.Now(), 1); err != nil || !ok {
		t.Fatalf("AllowNContext: ok=%v err=%v, want true/nil", ok, err)
	}
	// Third event: bucket drained.
	if ok, _ := lim.AllowContext(context.Background()); ok {
		t.Fatal("AllowContext: want false after draining")
	}
}

func TestAllowContextReturnsRedisError(t *testing.T) {
	mr, _ := miniredis.Run()
	rdb := fastClient(mr.Addr())
	lim, _ := NewLimiter(rdb, "k", TokenBucket{Rate: 10, Burst: 2})
	mr.Close()
	if _, err := lim.AllowContext(context.Background()); !errors.Is(err, ErrRedis) {
		t.Fatalf("AllowContext error = %v, want wrapped ErrRedis", err)
	}
}

func TestPing(t *testing.T) {
	mr, _ := miniredis.Run()
	rdb := fastClient(mr.Addr())
	lim, _ := NewLimiter(rdb, "k", TokenBucket{Rate: 1, Burst: 1})
	if err := lim.Ping(context.Background()); err != nil {
		t.Fatalf("Ping (up): %v", err)
	}
	mr.Close()
	if err := lim.Ping(context.Background()); !errors.Is(err, ErrRedis) {
		t.Fatalf("Ping (down) = %v, want wrapped ErrRedis", err)
	}
}

func TestWithPingOnInit(t *testing.T) {
	mr, _ := miniredis.Run()
	addr := mr.Addr()
	rdb := fastClient(addr)
	if _, err := NewLimiter(rdb, "k", TokenBucket{Rate: 1, Burst: 1}, WithPingOnInit()); err != nil {
		t.Fatalf("NewLimiter WithPingOnInit (up): %v", err)
	}
	mr.Close()
	if _, err := NewLimiter(rdb, "k", TokenBucket{Rate: 1, Burst: 1}, WithPingOnInit()); err == nil {
		t.Fatal("NewLimiter WithPingOnInit (down): want error")
	}
}

func TestLastErr(t *testing.T) {
	mr, _ := miniredis.Run()
	rdb := fastClient(mr.Addr())
	lim, _ := NewLimiter(rdb, "k", TokenBucket{Rate: 10, Burst: 2})
	if lim.LastErr() != nil {
		t.Fatal("LastErr should be nil initially")
	}
	mr.Close()
	lim.Allow() // swallows a Redis error via fail-open
	if !errors.Is(lim.LastErr(), ErrRedis) {
		t.Fatalf("LastErr = %v, want wrapped ErrRedis", lim.LastErr())
	}
}

func TestMustNewLimiterPanics(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := fastClient(mr.Addr())
	defer func() {
		if recover() == nil {
			t.Fatal("MustNewLimiter: want panic on invalid key")
		}
	}()
	MustNewLimiter(rdb, "bad key", TokenBucket{Rate: 1, Burst: 1})
}

func TestMustNewStorePanics(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := fastClient(mr.Addr())
	defer func() {
		if recover() == nil {
			t.Fatal("MustNewStore: want panic on invalid algorithm")
		}
	}()
	MustNewStore(rdb, FixedWindow{Limit: 1, Window: 0})
}

func TestKeyCharWhitelist(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := fastClient(mr.Addr())
	// '@' is not in the whitelist [A-Za-z0-9:._\-/].
	if _, err := NewLimiter(rdb, "user@host", TokenBucket{Rate: 1, Burst: 1}, WithKeyCharWhitelist()); !errors.Is(err, ErrKeyChar) {
		t.Fatalf("whitelist reject = %v, want ErrKeyChar", err)
	}
	if _, err := NewLimiter(rdb, "user:host_1.2/3", TokenBucket{Rate: 1, Burst: 1}, WithKeyCharWhitelist()); err != nil {
		t.Fatalf("whitelisted key rejected: %v", err)
	}
}

func TestCustomKeyValidatorAndDisable(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := fastClient(mr.Addr())

	sentinel := errors.New("nope")
	if _, err := NewLimiter(rdb, "anything", TokenBucket{Rate: 1, Burst: 1},
		WithKeyValidator(func(string) error { return sentinel })); !errors.Is(err, sentinel) {
		t.Fatalf("custom validator error = %v, want sentinel", err)
	}
	// DisableKeyValidation accepts an otherwise-illegal key.
	if _, err := NewLimiter(rdb, "has space", TokenBucket{Rate: 1, Burst: 1},
		DisableKeyValidation()); err != nil {
		t.Fatalf("DisableKeyValidation rejected key: %v", err)
	}
}

func TestWaitCanceledReturnsCredit(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 1, Burst: 1})
	lim.Allow() // drain; next token is ~1s away
	before := lim.Tokens()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := lim.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context.Canceled", err)
	}
	// The reservation made by Wait must have returned its credit on cancel.
	if after := lim.Tokens(); after < before-0.001 {
		t.Fatalf("tokens dropped after canceled Wait: before=%v after=%v", before, after)
	}
}

func TestWaitNZeroImmediate(t *testing.T) {
	lim, _, _ := newTestLimiter(t, TokenBucket{Rate: 1, Burst: 1})
	lim.Allow() // drain
	// WaitN(0) is a zero-sized request: always permitted immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := lim.WaitN(ctx, 0); err != nil {
		t.Fatalf("WaitN(0): %v, want nil", err)
	}
}
