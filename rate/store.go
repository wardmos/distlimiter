package rate

import "github.com/redis/go-redis/v9"

// Store is a keyed limiter factory: the algorithm config and options are set
// once, then a per-key Limiter is obtained per request (gateway / multi-tenant
// patterns). All limiters from one Store share a single script cache.
type Store struct {
	rdb      redis.Cmdable
	scripter scripter
	algo     Algorithm
	opts     options
}

// NewStore builds a Store. It validates the algorithm config up front.
func NewStore(rdb redis.Cmdable, algo Algorithm, opts ...Option) (*Store, error) {
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}
	if err := algo.validate(); err != nil {
		return nil, err
	}
	s := &Store{
		rdb:      rdb,
		scripter: newGoRedisScripter(rdb),
		algo:     algo,
		opts:     o,
	}
	if o.pingOnInit {
		ctx, cancel := newTimeoutCtx(o)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			return nil, wrapRedis("ping", err)
		}
	}
	return s, nil
}

// MustNewStore is like NewStore but panics on error.
func MustNewStore(rdb redis.Cmdable, algo Algorithm, opts ...Option) *Store {
	s, err := NewStore(rdb, algo, opts...)
	if err != nil {
		panic(err)
	}
	return s
}

// Limiter returns a Limiter for key, validating the key (dynamic keys: handle
// the error). The returned limiter shares the Store's script cache.
func (s *Store) Limiter(key string) (*Limiter, error) {
	if err := validateKey(key, &s.opts); err != nil {
		return nil, err
	}
	return &Limiter{
		rdb:      s.rdb,
		scripter: s.scripter,
		kb:       newKeyBuilder(key, &s.opts),
		algo:     s.algo,
		opts:     s.opts,
		isInf:    s.algo.isInf(),
	}, nil
}

// MustLimiter is like Limiter but panics on an invalid key (trusted keys).
func (s *Store) MustLimiter(key string) *Limiter {
	l, err := s.Limiter(key)
	if err != nil {
		panic(err)
	}
	return l
}
