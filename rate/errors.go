package rate

import "errors"

// Sentinel errors. Construction-time key errors are wrapped with %w and a short
// offending fragment; runtime Redis failures are wrapped under ErrRedis. Do not
// log a full key if it may contain sensitive user data.
var (
	// ErrEmptyKey is returned when the user-supplied key is empty.
	ErrEmptyKey = errors.New("rate: key is empty")
	// ErrKeyTooLong is returned when the key exceeds the configured max length.
	ErrKeyTooLong = errors.New("rate: key exceeds max length")
	// ErrKeyChar is returned when the key contains a forbidden character.
	ErrKeyChar = errors.New("rate: key contains forbidden character")
	// ErrRedis wraps any underlying go-redis failure so callers can errors.Is it.
	ErrRedis = errors.New("rate: redis operation failed")
)
