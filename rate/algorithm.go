package rate

import "strconv"

// Algorithm is a rate-limiting algorithm config value: TokenBucket, LeakyBucket,
// FixedWindow, SlidingWindowLog, SlidingWindowCounter, or GCRA. It is passed to
// NewLimiter / NewStore to select the algorithm.
//
// The interface is closed (its methods are unexported): only this package can
// provide implementations, so the set of algorithms is controlled here while
// the public usage interface stays algorithm-agnostic.
type Algorithm interface {
	// name identifies the algorithm for diagnostics.
	name() string
	// validate checks the config at construction.
	validate() error
	// scriptSrc returns the full Lua source (shared prelude + algorithm body).
	scriptSrc() string
	// configArgs returns the algorithm-specific seed appended to ARGV[8..],
	// used to seed the Redis-authoritative cfg hash on first use.
	configArgs() []any
	// setLimitFields / setBurstFields return cfg HSET field/value pairs for the
	// Redis-authoritative SetLimit / SetBurst (DESIGN sec 4.3).
	setLimitFields(Limit) []any
	setBurstFields(int) []any
	// decodeConfig maps the stored cfg hash to (Limit, Burst), falling back to
	// the construction seed for any field not yet present in Redis.
	decodeConfig(stored map[string]string) (Limit, int)
	// isInf reports whether the configured rate is infinite, so the facade can
	// short-circuit in Go and never touch Redis.
	isInf() bool
	// bestEffort reports whether Reserve/Wait/Cancel are best-effort (estimated
	// delay, verify-and-retry Wait, no-op Cancel) rather than exact. Only
	// SlidingWindowCounter returns true (DESIGN sec 5.1).
	bestEffort() bool
}

// reserveResult is the decoded return of a reserveN call.
type reserveResult struct {
	ok        bool
	wait      int64  // micros until admission
	remaining float64
	cancelTok string
}

// formatRate encodes a Limit (events/sec) in shortest exact form for Lua.
func formatRate(r Limit) string {
	return strconv.FormatFloat(float64(r), 'g', -1, 64)
}

// parseRate decodes a rate string back to a Limit.
func parseRate(s string) Limit {
	f, _ := strconv.ParseFloat(s, 64)
	return Limit(f)
}

// parseInt decodes an integer config field.
func parseInt(s string) (int, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	return int(n), err
}

// Decoders for go-redis Lua return values, which arrive as int64, string, or
// []byte depending on the Lua type.
func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	default:
		return 0
	}
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	case []byte:
		f, _ := strconv.ParseFloat(string(x), 64)
		return f
	default:
		return 0
	}
}

func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return ""
	}
}

// decodeReserve parses the {ok, wait, remaining, cancelTok} table returned by a
// reserveN call.
func decodeReserve(raw any) reserveResult {
	arr, ok := raw.([]any)
	if !ok || len(arr) < 3 {
		return reserveResult{}
	}
	res := reserveResult{
		ok:        toInt64(arr[0]) == 1,
		wait:      toInt64(arr[1]),
		remaining: toFloat(arr[2]),
	}
	if len(arr) >= 4 {
		res.cancelTok = toStr(arr[3])
	}
	return res
}
