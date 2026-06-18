package rate

import (
	"context"
	_ "embed"
	"sync"

	"github.com/redis/go-redis/v9"
)

// scripter is the only way the core touches the Redis client: "run a script".
// Binding to one SDK is cheap to undo later behind this interface.
type scripter interface {
	runScript(ctx context.Context, src string, keys []string, args ...any) (any, error)
}

// goRedisScripter adapts a go-redis client. It caches one *redis.Script per
// source so calls go through EVALSHA with an automatic EVAL fallback.
type goRedisScripter struct {
	rdb   redis.Cmdable
	cache sync.Map // src string -> *redis.Script
}

func newGoRedisScripter(rdb redis.Cmdable) *goRedisScripter {
	return &goRedisScripter{rdb: rdb}
}

func (g *goRedisScripter) runScript(ctx context.Context, src string, keys []string, args ...any) (any, error) {
	v, ok := g.cache.Load(src)
	if !ok {
		v, _ = g.cache.LoadOrStore(src, redis.NewScript(src))
	}
	return v.(*redis.Script).Run(ctx, g.rdb, keys, args...).Result()
}

// Embedded Lua sources. The prelude is prepended to each algorithm body so the
// shared helpers (dl_now, dl_cfg_num, ...) are in scope.
var (
	//go:embed lua/prelude.lua
	preludeSrc string

	//go:embed lua/token_bucket.lua
	tokenBucketBody string

	//go:embed lua/gcra.lua
	gcraBody string

	//go:embed lua/leaky_bucket.lua
	leakyBucketBody string

	//go:embed lua/fixed_window.lua
	fixedWindowBody string

	//go:embed lua/sliding_log.lua
	slidingLogBody string

	//go:embed lua/sliding_counter.lua
	slidingCounterBody string
)

// buildScript prepends the shared prelude to an algorithm body.
func buildScript(body string) string {
	return preludeSrc + "\n" + body
}

var (
	tokenBucketSrc    = buildScript(tokenBucketBody)
	gcraSrc           = buildScript(gcraBody)
	leakyBucketSrc    = buildScript(leakyBucketBody)
	fixedWindowSrc    = buildScript(fixedWindowBody)
	slidingLogSrc     = buildScript(slidingLogBody)
	slidingCounterSrc = buildScript(slidingCounterBody)
)
