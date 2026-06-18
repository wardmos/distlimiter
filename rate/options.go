package rate

import (
	"context"
	"time"
)

// Default option values.
const (
	defaultPrefix    = "ratelimit"
	defaultSeparator = ":"
	defaultMaxKeyLen = 256
	defaultTimeout   = 100 * time.Millisecond
	defaultTTLMargin = 5 * time.Second
)

// options holds the resolved configuration for a Limiter or Store.
type options struct {
	prefix    string
	sep       string
	maxKeyLen int // <= 0 means unlimited
	ttlMargin time.Duration

	charWhitelist   bool
	customValidator func(string) error
	disableKeyVal   bool

	timeout    time.Duration // <= 0 disables the per-call deadline
	baseCtx    context.Context
	failOpen   bool
	errHandler func(error)
	pingOnInit bool
}

func defaultOptions() options {
	return options{
		prefix:    defaultPrefix,
		sep:       defaultSeparator,
		maxKeyLen: defaultMaxKeyLen,
		ttlMargin: defaultTTLMargin,
		timeout:   defaultTimeout,
		baseCtx:   context.Background(),
		failOpen:  true,
	}
}

// Option configures a Limiter or Store at construction time.
type Option func(*options)

// WithKeyPrefix sets the namespace prefix (default "ratelimit").
func WithKeyPrefix(p string) Option { return func(o *options) { o.prefix = p } }

// WithKeySeparator sets the separator between key components (default ":").
func WithKeySeparator(s string) Option { return func(o *options) { o.sep = s } }

// WithMaxKeyLen caps the user key length in bytes (default 256; <= 0 = unlimited).
func WithMaxKeyLen(n int) Option { return func(o *options) { o.maxKeyLen = n } }

// WithTTLMargin sets the extra TTL added to every key beyond its natural expiry
// (default 5s), giving idle keys a small grace period before reclamation.
func WithTTLMargin(d time.Duration) Option { return func(o *options) { o.ttlMargin = d } }

// WithKeyCharWhitelist restricts keys to [A-Za-z0-9:._\-/].
func WithKeyCharWhitelist() Option { return func(o *options) { o.charWhitelist = true } }

// WithKeyValidator installs a custom key validator, overriding the built-in
// rules (advanced).
func WithKeyValidator(fn func(string) error) Option {
	return func(o *options) { o.customValidator = fn }
}

// DisableKeyValidation turns off all key validation (trusted input only).
func DisableKeyValidation() Option { return func(o *options) { o.disableKeyVal = true } }

// WithTimeout bounds each no-context Redis round trip (default 100ms; 0 disables).
// A timeout surfaces as a Redis error and is resolved by the failure policy.
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithFailOpen admits requests when Redis is unavailable (availability first).
// This is the default.
func WithFailOpen() Option { return func(o *options) { o.failOpen = true } }

// WithFailClosed rejects requests when Redis is unavailable (safety first).
func WithFailClosed() Option { return func(o *options) { o.failOpen = false } }

// WithErrorHandler reports errors swallowed by the failure policy (e.g. from
// Allow, Cancel, SetLimit) to logs or metrics.
func WithErrorHandler(fn func(error)) Option { return func(o *options) { o.errHandler = fn } }

// WithPingOnInit pings Redis once during construction; NewLimiter returns the
// error on failure.
func WithPingOnInit() Option { return func(o *options) { o.pingOnInit = true } }
