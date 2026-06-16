-- gcra.lua: Generic Cell Rate Algorithm (same shape as redis_rate).
-- State: a single value at `base` holding tat (theoretical arrival time, micros).
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

local function interval()
    return 1000000.0 / rate    -- emission interval, micros per event
end

-- remaining tokens-equivalent given a tat value.
local function remaining(tat)
    if rate <= 0 then return 0 end
    local r = burst - (tat - now) / interval()
    if r < 0 then r = 0 end
    if r > burst then r = burst end
    return r
end

local function persist(tat)
    redis.call('SET', base, tostring(tat))
    local ttl = (tat - now) + ttlMargin
    dl_pexpire(base, ttl)
    dl_pexpire(cfg, ttl)
end

if op == 'cancel' then
    local give = tonumber(cancelTok)
    local tat = tonumber(redis.call('GET', base)) or now
    if give and give > 0 and rate > 0 then
        tat = tat - give * interval()
        if tat < now then tat = now end
    end
    persist(tat)
    return {1}
end

-- Monotonic-clock guard: never let tat run behind a backward TIME jump.
local tat = tonumber(redis.call('GET', base))
if tat == nil or tat < now then
    tat = now
end

if n == 0 then
    return {1, 0, tostring(remaining(tat)), ''}
end
if rate <= 0 or n > burst then
    return {0, -1, tostring(remaining(tat)), ''}
end

local newTat = tat + n * interval()
local allowAt = newTat - burst * interval()
local wait = allowAt - now
if wait < 0 then wait = 0 end
wait = math.ceil(wait)
if maxWait >= 0 and wait > maxWait then
    return {0, wait, tostring(remaining(tat)), ''}
end
persist(newTat)
return {1, wait, tostring(remaining(newTat)), tostring(n)}
