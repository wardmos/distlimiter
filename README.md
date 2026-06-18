# distlimiter

**x/time/rate, but distributed via Redis, with pluggable algorithms.**

`distlimiter` is a Redis-backed distributed rate limiter whose usage interface is
identical to [`golang.org/x/time/rate`](https://pkg.go.dev/golang.org/x/time/rate).
Swap your construction call, keep every call site, and the limit is now enforced
**globally across the fleet** instead of once per replica.

> Tracks `golang.org/x/time/rate` **v0.9.0**: the usage interface (`Allow`,
> `Wait`, `Reserve`, ...) mirrors upstream exactly and is enforced in tests.

## When to use

The primary scenario: you already use `golang.org/x/time/rate` in a single
process, then scale to multiple replicas/pods. Per-instance limiters no longer
enforce a global cap — each of N replicas admits the full rate, so the real rate
becomes N×. This library gives you the **same call-site code** but enforces the
limit globally, with Redis as the shared state.

Other scenarios:

- **Gateway / microservice limits** per user, tenant, IP, or API key (the
  `Store` pattern).
- **Protecting a shared downstream** (third-party API quota, DB, SMS/email
  provider) where the aggregate rate across all workers must be capped.
- **Distributed workers / crawlers / queue consumers** collectively respecting a
  rate.
- **Choosing an algorithm to match traffic shape** (sliding window for
  smoothness, token bucket for bursts, GCRA for memory) — switchable by config,
  call sites unchanged.

## Install

```sh
go get github.com/wardmos/distlimiter
```

```go
import "github.com/wardmos/distlimiter/rate"
```

The package is named `rate` and re-exports `Limit`, `Every`, and `Inf` from
`golang.org/x/time/rate`, so you import only this one package.

## Quick start

### Single-key limiter

```go
package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wardmos/distlimiter/rate"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// 10 events/second, bursts up to 20.
	limiter, err := rate.NewLimiter(rdb, "user:42",
		rate.TokenBucket{Rate: rate.Every(time.Second / 10), Burst: 20})
	if err != nil {
		panic(err)
	}

	if limiter.Allow() {
		// admitted
	}

	// Block until a slot is free (or ctx is done).
	ctx := context.Background()
	if err := limiter.Wait(ctx); err != nil {
		// ctx cancelled or limit cannot be satisfied
	}
}
```

For keys fixed at compile time, `rate.MustNewLimiter` panics instead of returning
an error.

### Keyed factory (Store)

Write the algorithm config once, pass the key per request. All limiters from one
`Store` share a single script cache.

```go
store, err := rate.NewStore(rdb, rate.GCRA{Rate: 50, Burst: 10})
if err != nil {
	panic(err)
}

// Dynamic keys: handle the validation error.
lim, err := store.Limiter("ip:1.2.3.4")
if err != nil {
	// invalid key
}
lim.Allow()

// Trusted keys: MustLimiter panics on an invalid key.
store.MustLimiter("ip:1.2.3.4").Allow()
```

An `Inf` rate (`rate.TokenBucket{Rate: rate.Inf, Burst: 0}`) short-circuits in Go
and never touches Redis — always allowed.

## Algorithms

Selected by passing one strongly-typed config value at construction. The usage
interface is identical across all of them.

| Config value           | Fields                              | Reserve precision | Notes                                          |
| ---------------------- | ----------------------------------- | ----------------- | ---------------------------------------------- |
| `TokenBucket`          | `Rate Limit`, `Burst int`           | exact             | Same algorithm as `x/time/rate`.               |
| `LeakyBucket`          | `Rate Limit`, `Capacity int`        | exact             | Constant outflow rate; smooths bursts.         |
| `GCRA`                 | `Rate Limit`, `Burst int`           | exact             | Single timestamp of state; lowest memory.      |
| `FixedWindow`          | `Limit int`, `Window time.Duration` | exact             | Cheapest; allows up to 2× across a boundary.   |
| `SlidingWindowLog`     | `Limit int`, `Window time.Duration` | exact             | ZSET log; exact, no boundary spike.            |
| `SlidingWindowCounter` | `Limit int`, `Window time.Duration` | best-effort       | Two weighted counters; memory-light, approximate. |

All algorithms hold an exact future slot for `Reserve`/`Wait` (so concurrent
reservers stagger with no thundering herd, and `Cancel` returns exact credit)
**except** `SlidingWindowCounter`, whose weighted estimate cannot hold a precise
slot: its `Delay()` is an estimate, `Wait` verifies-and-retries, and `Cancel` is
a no-op.

## Usage interface

These mirror `golang.org/x/time/rate` exactly:

| Method | Notes |
| ------ | ----- |
| `Allow() bool`, `AllowN(t time.Time, n int) bool` | `t` is ignored (server clock). On a Redis error the failure policy decides the result. |
| `Wait(ctx) error`, `WaitN(ctx, n) error` | Blocks until admission or `ctx` is done; returns Redis errors directly. |
| `Reserve() *Reservation`, `ReserveN(t, n) *Reservation` | `t` ignored. On error the reservation is not OK and `Err` is set. |
| `Reservation.OK() bool` | |
| `Reservation.Delay() time.Duration`, `DelayFrom(t) time.Duration` | |
| `Reservation.Cancel()`, `CancelAt(t)` | Returns held credit (a no-op for best-effort algorithms). |
| `SetLimit(Limit)`, `SetLimitAt(t, Limit)` | Writes through to Redis. |
| `SetBurst(int)`, `SetBurstAt(t, int)` | Writes through to Redis. |
| `Limit() Limit`, `Burst() int` | Read from Redis (approximate for window algorithms). |
| `Tokens() float64`, `TokensAt(t) float64` | Approximate for window algorithms. |

Additive (not in upstream):

- `AllowContext(ctx) (bool, error)` and `AllowNContext(ctx, t, n) (bool, error)`
  — explicit context and error, for when a silent Redis failure on the request
  gate is unacceptable.
- `Ping(ctx) error` — explicit connectivity probe.
- `LastErr() error` — the most recent error swallowed by the failure policy.

## Options

Pass to `NewLimiter` / `NewStore`:

| Option | Default | Purpose |
| ------ | ------- | ------- |
| `WithKeyPrefix(string)` | `"ratelimit"` | Namespace prefix. |
| `WithKeySeparator(string)` | `":"` | Separator between key components. |
| `WithMaxKeyLen(int)` | `256` | Max user-key length in bytes (`<= 0` = unlimited). |
| `WithTTLMargin(time.Duration)` | `5s` | Grace period added to every key's TTL before reclamation. |
| `WithKeyCharWhitelist()` | off | Restrict keys to `[A-Za-z0-9:._\-/]`. |
| `WithKeyValidator(func(string) error)` | — | Custom key validator (overrides built-in rules). |
| `DisableKeyValidation()` | off | Skip all key validation (trusted input). |
| `WithTimeout(time.Duration)` | `100ms` | Per-call Redis deadline for no-context methods (`0` disables). |
| `WithFailOpen()` | **default** | Admit on Redis error (availability first). |
| `WithFailClosed()` | — | Reject on Redis error (safety first). |
| `WithErrorHandler(func(error))` | — | Report errors swallowed by the failure policy. |
| `WithPingOnInit()` | off | Ping Redis once at construction; constructor returns the error on failure. |

Keys are validated **once at construction**; the hot path never re-validates.
Empty keys, control characters, whitespace, and `{`/`}` (reserved for the
automatic Redis Cluster hash tag) are rejected.

## Error handling & failure policy

Because the usage interface mirrors `x/time/rate`, several methods cannot return
an error — yet every Redis operation can fail. How errors surface:

- **`Allow`/`AllowN`** can only return `bool`, so a configurable **failure
  policy** decides the result on a Redis error: **fail-open by default** (a
  limiter outage must not take down traffic), or `WithFailClosed()` for
  anti-abuse paths. The swallowed error is delivered to `WithErrorHandler` and
  readable via `LastErr()`.
- **`Wait`/`WaitN`** return the error directly.
- **`Reserve`/`ReserveN`** set `Reservation.Err` and `OK()` returns `false`.
- **`AllowContext`/`AllowNContext`** return the error explicitly.

`WithTimeout` bounds the no-context methods so a hung Redis returns within ~the
timeout instead of blocking the request path.

**At-most-once is best-effort under error.** A call that times out or errors
*after* the Lua script already ran server-side leaves the client unable to tell
whether the mutation happened. With fail-open this can mean one extra admission;
with fail-closed, one rejected-but-counted request. This is inherent to any
networked rate limiter; the library does not add idempotency keys. Callers
needing exactness should prefer `Wait` / `AllowContext` and decide explicitly.

## Compatibility

Interface parity with `golang.org/x/time/rate` is a first-class guarantee: code
written against `rate.Limiter` compiles and behaves the same against this
package's `Limiter`. Only the **usage** surface is pinned; construction differs.

Known, intentional divergences:

1. **Construction** takes `(rdb, key, algorithm config)` instead of `(r, b)`.
2. **Constructors return `error`** (`NewLimiter`, `NewStore`, `Store.Limiter`),
   with `Must*` variants that panic.
3. The **`t time.Time` argument** of `AllowN`/`ReserveN`/`TokensAt`/`SetLimitAt`
   is **ignored** — the Redis server clock is the single source of truth.
4. **Added** context-taking variants (`AllowContext`/`AllowNContext`) and a
   fail-open/fail-closed failure policy.
5. `Tokens()`/`Burst()` are **approximate** for window algorithms, and
   `Reserve`/`Delay`/`Cancel` are **best-effort** for `SlidingWindowCounter`
   (estimated delay, no-op cancel).
6. `SetLimit`/`SetBurst` **write through to Redis** (the authoritative store)
   instead of mutating per-instance memory, so they do I/O and `Limit()`/`Burst()`
   incur a Redis read. Runtime overrides are not durable across TTL reclamation —
   they revert to the construction-config baseline (which stays consistent across
   all nodes).

## Requirements

- **Go**: `1.21+`.
- **Redis client**: [go-redis](https://github.com/redis/go-redis) **v9 only**.
  The minimum required release is `v9.18` (which keeps the Go floor at 1.21);
  newer go-redis releases work too but may raise the Go requirement. The client
  is isolated behind a tiny internal interface, so adapting another SDK is
  cheap — PRs welcome.
- **Redis server**: **6.0+**. The scripts use only widely-available commands
  (`EVAL`, `TIME`, ZSET, hashes, `INCRBY`/`DECR`, `PEXPIRE`); the floor is set by
  go-redis v9, not by the algorithms.

All mutating operations run as atomic Lua scripts and use `redis.call('TIME')`
as the canonical clock, eliminating cross-node clock skew.

## License

[Apache-2.0](./LICENSE).
