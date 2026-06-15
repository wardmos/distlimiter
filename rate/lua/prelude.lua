-- prelude.lua: shared helpers prepended to every algorithm script.
--
-- Wire contract (see DESIGN.md sec 5.3):
--   KEYS[1] = base storage key, hash-tag wrapped, e.g. "ratelimit:{user:42}".
--             All sub-keys (cfg, window counters) are derived from it in Lua so
--             they share the Cluster slot; never built on the Go side.
--   ARGV[1] = sep        : separator used to build sub-keys
--   ARGV[2] = op         : "reserve" | "cancel"
--   ARGV[3] = now        : injected micros, or < 0 to use redis TIME (single clock)
--   ARGV[4] = n          : requested events
--   ARGV[5] = maxWait    : micros the caller may wait; -1 = unbounded (Reserve)
--   ARGV[6] = cancelTok  : opaque token for op=="cancel"; "" otherwise
--   ARGV[7] = ttlMargin  : extra micros added to every key's TTL
--   ARGV[8..] = algorithm-specific config seed
--
-- reserveN returns {ok(0|1), waitMicros(int, ceil), remaining(string), cancelTok(string)}.
-- cancel  returns {1}.
-- `remaining` is a string to avoid Lua->Redis integer truncation of fractions.

-- dl_now resolves the canonical clock: an injected value (tests) or server TIME
-- (production), in micros.
local function dl_now(arg)
    local v = tonumber(arg)
    if v and v >= 0 then
        return v
    end
    local t = redis.call('TIME')
    return tonumber(t[1]) * 1000000 + tonumber(t[2])
end

-- dl_sub builds a sub-key in the same hash-tag slot as base.
local function dl_sub(base, sep, name)
    return base .. sep .. name
end

-- dl_cfg_num returns cfg[field] as a number, seeding it from `seed` if absent
-- (Redis-authoritative config with construction-config baseline, DESIGN sec 4.3).
-- Each field is seeded independently so a partial cfg (e.g. after SetLimit wrote
-- only one field) is reconciled.
local function dl_cfg_num(cfgkey, field, seed)
    local v = redis.call('HGET', cfgkey, field)
    if not v then
        redis.call('HSET', cfgkey, field, seed)
        return tonumber(seed)
    end
    return tonumber(v)
end

-- dl_pexpire sets a TTL in milliseconds (rounded up) from a micros duration.
local function dl_pexpire(key, micros)
    if micros <= 0 then
        return
    end
    redis.call('PEXPIRE', key, math.ceil(micros / 1000.0))
end
