-- token_bucket.lua: same algorithm as golang.org/x/time/rate.
-- State: hash at `base` with fields t (tokens, float) and ts (last update micros).
-- Config seed: ARGV[8] = rate (events/sec, float), ARGV[9] = burst (int).

local base = KEYS[1]
local sep = ARGV[1]
local op = ARGV[2]
local now = dl_now(ARGV[3])
local n = tonumber(ARGV[4])
local maxWait = tonumber(ARGV[5])
local cancelTok = ARGV[6]
local ttlMargin = tonumber(ARGV[7])

local cfg = dl_sub(base, sep, 'cfg')
local rate = dl_cfg_num(cfg, 'rate', ARGV[8])    -- events per second
local burst = dl_cfg_num(cfg, 'burst', ARGV[9])

-- Load and refill the bucket up to `now`.
local h = redis.call('HMGET', base, 't', 'ts')
local tokens = tonumber(h[1])
local ts = tonumber(h[2])
if tokens == nil then
    tokens = burst
    ts = now
end
-- Monotonic-clock guard: a backward TIME jump must not refill negatively.
if ts > now then
    ts = now
end
if rate > 0 then
    tokens = tokens + (now - ts) * rate / 1000000.0
end
-- Clamp to burst (also applies a SetBurst shrink).
if tokens > burst then
    tokens = burst
end
ts = now

-- TTL: time to refill back to full, plus margin.
local function ttl_micros()
    local deficit = burst - tokens
    if deficit < 0 then deficit = 0 end
    local refill = 0
    if rate > 0 then refill = deficit / rate * 1000000.0 end
    return refill + ttlMargin
end

local function persist()
    redis.call('HSET', base, 't', tostring(tokens), 'ts', tostring(ts))
    local ttl = ttl_micros()
    dl_pexpire(base, ttl)
    dl_pexpire(cfg, ttl)
end

if op == 'cancel' then
    -- Return the reserved tokens (clamped to burst).
    local give = tonumber(cancelTok)
    if give and give > 0 then
        tokens = tokens + give
        if tokens > burst then tokens = burst end
    end
    persist()
    return {1}
end

-- reserve / allow
if n == 0 then
    -- Pure read (feeds Tokens()): report state without consuming or persisting.
    return {1, 0, tostring(tokens), ''}
end
if rate <= 0 or n > burst then
    -- Can never be satisfied: rate of 0, or a request larger than capacity.
    return {0, -1, tostring(tokens), ''}
end

local wait = 0
if tokens < n then
    local deficit = n - tokens
    local waitMicros = math.ceil(deficit / rate * 1000000.0)
    if maxWait >= 0 and waitMicros > maxWait then
        -- Cannot admit within the allowed wait; do not consume.
        return {0, waitMicros, tostring(tokens), ''}
    end
    wait = waitMicros
end
tokens = tokens - n    -- may go negative, holding the future slot
persist()
return {1, wait, tostring(tokens), tostring(n)}
