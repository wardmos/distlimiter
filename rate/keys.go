package rate

import (
	"fmt"
	"strings"
)

// validateKey enforces the rules in DESIGN.md sec 8.2. It runs once at
// construction; the hot path never re-validates.
func validateKey(key string, o *options) error {
	if o.disableKeyVal {
		return nil
	}
	if o.customValidor != nil {
		return o.customValidor(key)
	}
	if key == "" {
		return ErrEmptyKey
	}
	if o.maxKeyLen > 0 && len(key) > o.maxKeyLen {
		return fmt.Errorf("%w: %d > %d", ErrKeyTooLong, len(key), o.maxKeyLen)
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c <= 0x1f || c == 0x7f:
			return fmt.Errorf("%w: control char at %d", ErrKeyChar, i)
		case c == ' ' || c == '\t':
			return fmt.Errorf("%w: whitespace at %d", ErrKeyChar, i)
		case c == '{' || c == '}':
			// Reserved for the automatic hash-tag mechanism (sec 8.1).
			return fmt.Errorf("%w: %q conflicts with hash tag", ErrKeyChar, c)
		}
	}
	if o.charWhitelist {
		for i := 0; i < len(key); i++ {
			if !isWhitelisted(key[i]) {
				return fmt.Errorf("%w: %q not in whitelist at %d", ErrKeyChar, key[i], i)
			}
		}
	}
	return nil
}

func isWhitelisted(c byte) bool {
	switch {
	case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		return true
	case c == ':' || c == '.' || c == '_' || c == '-' || c == '/':
		return true
	default:
		return false
	}
}

// keyBuilder composes Redis keys from the user key. The base key wraps the user
// key in a hash tag so every algorithm sub-key (cfg, window counters) lands in
// the same Cluster slot. Sub-keys are derived from base inside Lua, never here.
type keyBuilder struct {
	base string
}

// newKeyBuilder builds "<prefix><sep>{<key>}".
func newKeyBuilder(key string, o *options) keyBuilder {
	var b strings.Builder
	b.WriteString(o.prefix)
	b.WriteString(o.sep)
	b.WriteByte('{')
	b.WriteString(key)
	b.WriteByte('}')
	return keyBuilder{base: b.String()}
}
