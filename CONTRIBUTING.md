# Contributing

Thanks for your interest in improving distlimiter.

## Development

```sh
go test ./...                      # unit tests (miniredis, no real Redis)
go test -race ./...                # with the race detector
go test -tags integration ./...    # integration tests, needs a real Redis
golangci-lint run                  # lint
gofmt -l .                         # formatting check (must print nothing)
```

Integration tests read `REDIS_ADDR` (default `localhost:6379`) and skip
gracefully if no Redis is reachable.

## Guidelines

- **Keep the usage interface identical to `golang.org/x/time/rate`.** Any change
  to `Allow`/`Wait`/`Reserve`/etc. must preserve drop-in parity; the conformance
  tests enforce this. Intentional divergences are limited to the construction
  interface and are listed in the README.
- **Algorithms are pluggable.** A new algorithm is a Lua script in `rate/lua/`
  plus a config type in `rate/` implementing the unexported `Algorithm`
  interface. The Lua follows the wire contract in the shared prelude
  (integer-microsecond times, injectable `now` falling back to `redis.call('TIME')`,
  string-encoded `remaining`, monotonic-clock guard, capacity-shrink clamp).
- **All mutations are atomic Lua.** Never split a read-modify-write across round
  trips.
- Add tests for new behavior (table-driven cross-algorithm tests live in
  `rate/cross_test.go`).

## Adding a Redis client

The core touches Redis only through the internal `scripter` interface, so a new
client (e.g. rueidis) is a small adapter. The algorithms and Lua stay untouched.

## Commits & PRs

- Use Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`...).
- PRs run build, vet, gofmt, race tests, integration tests, and golangci-lint.

## License

By contributing you agree your contributions are licensed under Apache-2.0.
