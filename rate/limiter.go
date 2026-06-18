package rate

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter is the facade exposing the golang.org/x/time/rate.Limiter usage
// interface over a Redis-backed, algorithm-pluggable implementation. Every
// method is built on a single primitive, reserveN, so the public surface is
// algorithm-agnostic.
type Limiter struct {
	rdb      redis.Cmdable
	scripter scripter
	kb       keyBuilder
	algo     Algorithm
	opts     options
	isInf    bool

	mu      sync.Mutex
	lastErr error

	// testNow, when non-nil, injects the clock (micros) for tests; production
	// leaves it nil so Lua uses the Redis server clock.
	testNow func() int64
}

// NewLimiter builds a single-key Limiter. It validates the algorithm config and
// the key once, up front; the hot path never re-validates. It does not ping
// Redis unless WithPingOnInit is set.
func NewLimiter(rdb redis.Cmdable, key string, algo Algorithm, opts ...Option) (*Limiter, error) {
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}
	if err := algo.validate(); err != nil {
		return nil, err
	}
	if err := validateKey(key, &o); err != nil {
		return nil, err
	}
	l := &Limiter{
		rdb:      rdb,
		scripter: newGoRedisScripter(rdb),
		kb:       newKeyBuilder(key, &o),
		algo:     algo,
		opts:     o,
		isInf:    algo.isInf(),
	}
	if o.pingOnInit {
		ctx, cancel := l.opCtx()
		defer cancel()
		if err := l.Ping(ctx); err != nil {
			return nil, err
		}
	}
	return l, nil
}

// MustNewLimiter is like NewLimiter but panics on error (for keys fixed at
// compile time).
func MustNewLimiter(rdb redis.Cmdable, key string, algo Algorithm, opts ...Option) *Limiter {
	l, err := NewLimiter(rdb, key, algo, opts...)
	if err != nil {
		panic(err)
	}
	return l
}

// --- usage interface (mirrors x/time/rate) -------------------------------

// Allow reports whether one event may happen now.
func (l *Limiter) Allow() bool { return l.AllowN(time.Now(), 1) }

// AllowN reports whether n events may happen now. The t argument is ignored
// (the server clock is canonical). On a Redis error the failure policy decides
// the result (fail-open by default).
func (l *Limiter) AllowN(_ time.Time, n int) bool {
	ok, err := l.allowN(l.opts.baseCtx, n, true)
	if err != nil {
		return l.failPolicy(err)
	}
	return ok
}

// AllowContext is AllowN(.,1) with an explicit context and error.
func (l *Limiter) AllowContext(ctx context.Context) (bool, error) {
	return l.allowN(ctx, 1, false)
}

// AllowNContext is AllowN with an explicit context and error. The t argument is
// ignored.
func (l *Limiter) AllowNContext(ctx context.Context, _ time.Time, n int) (bool, error) {
	return l.allowN(ctx, n, false)
}

func (l *Limiter) allowN(ctx context.Context, n int, withTimeout bool) (bool, error) {
	if l.isInf {
		return true, nil
	}
	if withTimeout {
		var cancel context.CancelFunc
		ctx, cancel = l.opCtxFrom(ctx)
		defer cancel()
	}
	res, err := l.reserveN(ctx, "reserve", n, 0, "")
	if err != nil {
		return false, err
	}
	return res.ok && res.wait == 0, nil
}

// Reserve is shorthand for ReserveN(time.Now(), 1).
func (l *Limiter) Reserve() *Reservation { return l.ReserveN(time.Now(), 1) }

// ReserveN reserves n events and returns a Reservation describing when they may
// occur. The t argument is ignored. On a Redis error the Reservation is not OK
// and its Err is set.
func (l *Limiter) ReserveN(_ time.Time, n int) *Reservation {
	if l.isInf {
		return &Reservation{ok: true, timeToAct: time.Now(), tokens: n, lim: l}
	}
	ctx, cancel := l.opCtx()
	defer cancel()
	res, err := l.reserveN(ctx, "reserve", n, unboundedWait, "")
	if err != nil {
		l.reportErr(err)
		return &Reservation{lim: l, Err: err}
	}
	r := &Reservation{ok: res.ok, tokens: n, lim: l, cancelTok: res.cancelTok}
	if res.ok {
		// timeToAct = localNow + waitMicros.
		r.timeToAct = time.Now().Add(time.Duration(res.wait) * time.Microsecond)
	}
	return r
}

// Wait is shorthand for WaitN(ctx, 1).
func (l *Limiter) Wait(ctx context.Context) error { return l.WaitN(ctx, 1) }

// WaitN blocks until n events may happen or ctx is done. It uses the caller's
// context directly (no added timeout). On a Redis error the error is returned.
func (l *Limiter) WaitN(ctx context.Context, n int) error {
	if l.isInf {
		return ctxErr(ctx)
	}
	if l.algo.bestEffort() {
		return l.waitBestEffort(ctx, n)
	}
	return l.waitExact(ctx, n)
}

// waitExact reserves an exact future slot, then sleeps once; on cancellation it
// returns the credit.
func (l *Limiter) waitExact(ctx context.Context, n int) error {
	res, err := l.reserveN(ctx, "reserve", n, unboundedWait, "")
	if err != nil {
		return err
	}
	if !res.ok {
		return fmt.Errorf("rate: WaitN(n=%d) exceeds the limiter's capacity", n)
	}
	delay := time.Duration(res.wait) * time.Microsecond
	if delay <= 0 {
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok && time.Now().Add(delay).After(deadline) {
		l.cancelQuiet(res.cancelTok, n)
		return fmt.Errorf("rate: WaitN(n=%d) would exceed context deadline", n)
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		l.cancelQuiet(res.cancelTok, n)
		return ctx.Err()
	}
}

// waitBestEffort verifies-and-retries with jitter (SlidingWindowCounter).
func (l *Limiter) waitBestEffort(ctx context.Context, n int) error {
	for {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		res, err := l.reserveN(ctx, "reserve", n, 0, "")
		if err != nil {
			return err
		}
		if res.ok {
			return nil
		}
		if res.wait < 0 {
			return fmt.Errorf("rate: WaitN(n=%d) exceeds the limiter's capacity", n)
		}
		delay := time.Duration(res.wait) * time.Microsecond
		if delay <= 0 {
			delay = time.Millisecond
		}
		delay += time.Duration(rand.Int63n(int64(delay)/4 + 1)) // up to ~25% jitter
		if deadline, ok := ctx.Deadline(); ok && time.Now().Add(delay).After(deadline) {
			return fmt.Errorf("rate: WaitN(n=%d) would exceed context deadline", n)
		}
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

// SetLimit sets the limit (events/sec). It writes through to the Redis cfg hash
// (the authoritative store); a write failure surfaces via the
// error handler.
func (l *Limiter) SetLimit(newLimit Limit) { l.SetLimitAt(time.Now(), newLimit) }

// SetLimitAt is SetLimit; the t argument is ignored.
func (l *Limiter) SetLimitAt(_ time.Time, newLimit Limit) {
	if err := l.writeCfg(l.algo.setLimitFields(newLimit)); err != nil {
		l.reportErr(err)
	}
}

// SetBurst sets the burst (capacity), writing through to Redis.
func (l *Limiter) SetBurst(newBurst int) { l.SetBurstAt(time.Now(), newBurst) }

// SetBurstAt is SetBurst; the t argument is ignored.
func (l *Limiter) SetBurstAt(_ time.Time, newBurst int) {
	if err := l.writeCfg(l.algo.setBurstFields(newBurst)); err != nil {
		l.reportErr(err)
	}
}

// Limit returns the current limit. For window algorithms it is approximate
// (derived from limit/window).
func (l *Limiter) Limit() Limit {
	lim, _ := l.readConfig()
	return lim
}

// Burst returns the current burst. For window algorithms it is the limit.
func (l *Limiter) Burst() int {
	_, b := l.readConfig()
	return b
}

// Tokens is shorthand for TokensAt(time.Now()).
func (l *Limiter) Tokens() float64 { return l.TokensAt(time.Now()) }

// TokensAt returns the number of tokens / remaining quota available now. It is
// approximate for window algorithms. The t argument is ignored. On a Redis
// error it reports 0 and routes the error to the handler.
func (l *Limiter) TokensAt(_ time.Time) float64 {
	if l.isInf {
		return math.Inf(1)
	}
	ctx, cancel := l.opCtx()
	defer cancel()
	res, err := l.reserveN(ctx, "reserve", 0, 0, "")
	if err != nil {
		l.reportErr(err)
		return 0
	}
	return res.remaining
}

// Ping probes Redis connectivity.
func (l *Limiter) Ping(ctx context.Context) error {
	if err := l.rdb.Ping(ctx).Err(); err != nil {
		return wrapRedis("ping", err)
	}
	return nil
}

// LastErr returns the most recent error swallowed by the failure policy.
func (l *Limiter) LastErr() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastErr
}

// --- internals -----------------------------------------------------------

// unboundedWait is the maxWait sentinel for Reserve.
const unboundedWait int64 = -1

// reserveN runs the algorithm script. maxWaitMicros is 0 for Allow, the actual
// budget for bounded waits, or unboundedWait (-1) for Reserve.
func (l *Limiter) reserveN(ctx context.Context, op string, n int, maxWaitMicros int64, cancelTok string) (reserveResult, error) {
	args := l.scriptArgs(op, n, maxWaitMicros, cancelTok)
	raw, err := l.scripter.runScript(ctx, l.algo.scriptSrc(), []string{l.kb.base}, args...)
	if err != nil {
		return reserveResult{}, wrapRedis("eval", err)
	}
	return decodeReserve(raw), nil
}

func (l *Limiter) scriptArgs(op string, n int, maxWaitMicros int64, cancelTok string) []any {
	now := int64(-1) // -1 => Lua uses redis TIME
	if l.testNow != nil {
		now = l.testNow()
	}
	cfg := l.algo.configArgs()
	args := make([]any, 0, 7+len(cfg))
	args = append(args,
		l.opts.sep,
		op,
		now,
		n,
		maxWaitMicros,
		cancelTok,
		l.opts.ttlMargin.Microseconds(),
	)
	return append(args, cfg...)
}

// cancel returns reserved credit (op="cancel").
func (l *Limiter) cancel(cancelTok string, n int) error {
	if cancelTok == "" {
		return nil
	}
	ctx, cancel := l.opCtx()
	defer cancel()
	_, err := l.reserveN(ctx, "cancel", n, 0, cancelTok)
	return err
}

func (l *Limiter) cancelQuiet(cancelTok string, n int) {
	if err := l.cancel(cancelTok, n); err != nil {
		l.reportErr(err)
	}
}

func (l *Limiter) writeCfg(fields []any) error {
	ctx, cancel := l.opCtx()
	defer cancel()
	cfgKey := l.kb.base + l.opts.sep + "cfg"
	if err := l.rdb.HSet(ctx, cfgKey, fields...).Err(); err != nil {
		return wrapRedis("hset cfg", err)
	}
	return nil
}

func (l *Limiter) readConfig() (Limit, int) {
	ctx, cancel := l.opCtx()
	defer cancel()
	cfgKey := l.kb.base + l.opts.sep + "cfg"
	stored, err := l.rdb.HGetAll(ctx, cfgKey).Result()
	if err != nil {
		l.reportErr(wrapRedis("hgetall cfg", err))
		stored = nil
	}
	return l.algo.decodeConfig(stored)
}

func (l *Limiter) opCtx() (context.Context, context.CancelFunc) {
	return l.opCtxFrom(l.opts.baseCtx)
}

// newTimeoutCtx derives an option-bounded context (used outside a *Limiter,
// e.g. Store construction).
func newTimeoutCtx(o options) (context.Context, context.CancelFunc) {
	if o.timeout <= 0 {
		return o.baseCtx, func() {}
	}
	return context.WithTimeout(o.baseCtx, o.timeout)
}

// wrapRedis wraps an underlying go-redis error under ErrRedis.
func wrapRedis(op string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrRedis, op, err)
}

func (l *Limiter) opCtxFrom(base context.Context) (context.Context, context.CancelFunc) {
	if l.opts.timeout <= 0 {
		return base, func() {}
	}
	return context.WithTimeout(base, l.opts.timeout)
}

func (l *Limiter) failPolicy(err error) bool {
	l.reportErr(err)
	return l.opts.failOpen
}

func (l *Limiter) reportErr(err error) {
	if err == nil {
		return
	}
	l.mu.Lock()
	l.lastErr = err
	l.mu.Unlock()
	if l.opts.errHandler != nil {
		l.opts.errHandler(err)
	}
}

func ctxErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
