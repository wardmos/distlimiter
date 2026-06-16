-- sliding_log.lua: exact sliding window via a ZSET log (score = effective event
-- time, micros; member = unique sequence id). State at `base`; a unique-id
-- counter at base:sep:seq.
-- Config seed: ARGV[8] = limit (int), ARGV[9] = window (micros).
--
-- A request that cannot be admitted now books an exact future slot by ZADDing a
-- member scored at the time an older entry expires (DESIGN sec 5.1); that member
-- counts toward the window until it expires, holding the slot. Cancel ZREMs it.

local base = KEYS[1]
local sep = ARGV[1]
local op = ARGV[2]
local now = dl_now(ARGV[3])
local n = tonumber(ARGV[4])
local maxWait = tonumber(ARGV[5])
local cancelTok = ARGV[6]
local ttlMargin = tonumber(ARGV[7])

local cfg = dl_sub(base, sep, 'cfg')
local limit = dl_cfg_num(cfg, 'limit', ARGV[8])
local window = dl_cfg_num(cfg, 'window', ARGV[9])
local seqKey = dl_sub(base, sep, 'seq')

if op == 'cancel' then
    if cancelTok ~= '' then
        for id in string.gmatch(cancelTok, '([^,]+)') do
            redis.call('ZREM', base, id)
        end
    end
    return {1}
end

-- Drop entries that have aged out of the window (score <= now - window).
redis.call('ZREMRANGEBYSCORE', base, '-inf', now - window)
local count = redis.call('ZCARD', base)

local function remaining()
    local r = limit - count
    if r < 0 then r = 0 end
    return r
end

if n == 0 then
    return {1, 0, tostring(remaining()), ''}
end
if limit <= 0 or n > limit then
    return {0, -1, tostring(remaining()), ''}
end

-- need = how many of the n events require an older entry to expire first.
local need = count + n - limit
local oldest = {}
if need > 0 then
    -- oldest `need` scores; WITHSCORES yields {member, score, ...}.
    oldest = redis.call('ZRANGE', base, 0, need - 1, 'WITHSCORES')
end

-- Overall wait = the n-th event's act time (the largest), 0 if all immediate.
local wait = 0
if need > 0 then
    wait = tonumber(oldest[2 * need]) + window - now
    if wait < 0 then wait = 0 end
end
wait = math.ceil(wait)
if maxWait >= 0 and wait > maxWait then
    return {0, wait, tostring(remaining()), ''}
end

-- Add the n events at their computed scores (immediate = now, else future).
local ids = {}
for i = 1, n do
    local slotNeeded = count + i - limit
    local score
    if slotNeeded <= 0 then
        score = now
    else
        score = tonumber(oldest[2 * slotNeeded]) + window
    end
    local member = tostring(redis.call('INCR', seqKey))
    redis.call('ZADD', base, score, member)
    ids[#ids + 1] = member
end

local ttl = wait + window + ttlMargin
dl_pexpire(base, ttl)
dl_pexpire(cfg, ttl)
dl_pexpire(seqKey, ttl)

local rem = limit - redis.call('ZCARD', base)
if rem < 0 then rem = 0 end
return {1, wait, tostring(rem), table.concat(ids, ',')}
