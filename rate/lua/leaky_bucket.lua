-- leaky_bucket.lua: constant-outflow leaky bucket (events leak at Rate).
-- State: hash at `base` with fields level (queue fill, float) and ts (micros).
-- Config seed: ARGV[8] = rate (events/sec, float), ARGV[9] = cap (int capacity).

local base = KEYS[1]
local sep = ARGV[1]
local op = ARGV[2]
local now = dl_now(ARGV[3])
local n = tonumber(ARGV[4])
local maxWait = tonumber(ARGV[5])
local cancelTok = ARGV[6]
local ttlMargin = tonumber(ARGV[7])

local cfg = dl_sub(base, sep, 'cfg')
local rate = dl_cfg_num(cfg, 'rate', ARGV[8])    -- leak rate, events per second
local cap = dl_cfg_num(cfg, 'cap', ARGV[9])

-- Load and leak the bucket down to `now`.
local h = redis.call('HMGET', base, 'level', 'ts')
local level = tonumber(h[1])
local ts = tonumber(h[2])
if level == nil then
    level = 0
    ts = now
end
-- Monotonic-clock guard: a backward jump must not leak negatively.
if ts > now then
    ts = now
end
if rate > 0 then
    level = level - (now - ts) * rate / 1000000.0
end
if level < 0 then level = 0 end
-- Clamp to capacity (also applies a SetBurst shrink).
if level > cap then level = cap end
ts = now

local function remaining()
    local r = cap - level
    if r < 0 then r = 0 end
    return r
end

local function persist()
    redis.call('HSET', base, 'level', tostring(level), 'ts', tostring(ts))
    local drain = 0
    if rate > 0 then drain = level / rate * 1000000.0 end
    local ttl = drain + ttlMargin
    dl_pexpire(base, ttl)
    dl_pexpire(cfg, ttl)
end

if op == 'cancel' then
    local give = tonumber(cancelTok)
    if give and give > 0 then
        level = level - give
        if level < 0 then level = 0 end
    end
    persist()
    return {1}
end

if n == 0 then
    return {1, 0, tostring(remaining()), ''}
end
if n > cap then
    return {0, -1, tostring(remaining()), ''}
end

local newLevel = level + n
local wait = 0
if newLevel > cap then
    if rate <= 0 then
        -- No outflow: the overflow never drains.
        return {0, -1, tostring(remaining()), ''}
    end
    wait = math.ceil((newLevel - cap) / rate * 1000000.0)
    if maxWait >= 0 and wait > maxWait then
        return {0, wait, tostring(remaining()), ''}
    end
end
level = newLevel    -- may exceed cap, holding the future slot
persist()
return {1, wait, tostring(remaining()), tostring(n)}
