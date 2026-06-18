-- fixed_window.lua: fixed-window counter. Cheapest algorithm; allows boundary
-- spikes. State: one counter per window at base:sep:w<idx>, idx = floor(now/window).
-- Config seed: ARGV[8] = limit (int), ARGV[9] = window (micros).
--
-- Reserve holds an exact future slot by incrementing the earliest future window
-- with room; Allow (maxWait=0) only ever touches the current
-- window.

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

local function wkey(idx)
    return dl_sub(base, sep, 'w' .. idx)
end

local function count(idx)
    return tonumber(redis.call('GET', wkey(idx))) or 0
end

local curIdx = math.floor(now / window)

if op == 'cancel' then
    -- cancelTok = the absolute window index that was incremented at reserve time
    -- (independent of the current clock). If that window has already been
    -- reclaimed its reservation is moot, so skip it rather than resurrecting a
    -- TTL-less orphan key.
    local idx = tonumber(cancelTok)
    if idx ~= nil and redis.call('EXISTS', wkey(idx)) == 1 then
        local k = wkey(idx)
        if redis.call('DECRBY', k, n) <= 0 then
            redis.call('DEL', k)    -- empty: drop it (avoids a TTL-less orphan)
        end
    end
    return {1}
end

local function remaining()
    local r = limit - count(curIdx)
    if r < 0 then r = 0 end
    return r
end

if n == 0 then
    return {1, 0, tostring(remaining()), ''}
end
if limit <= 0 or n > limit then
    return {0, -1, tostring(remaining()), ''}
end

-- Scan forward for the earliest window with room. Empty future windows always
-- have room, so this terminates in at most two steps; maxAhead is a safety cap.
local maxAhead
if maxWait < 0 then
    maxAhead = 1000000
else
    maxAhead = math.floor(maxWait / window) + 1
end

local idx = curIdx
local steps = 0
while steps <= maxAhead do
    local wStart = idx * window
    local wait = wStart - now
    if wait < 0 then wait = 0 end
    wait = math.ceil(wait)
    if maxWait >= 0 and wait > maxWait then
        break    -- this and every later window exceed the budget
    end
    if count(idx) + n <= limit then
        local k = wkey(idx)
        redis.call('INCRBY', k, n)
        local ttl = (idx + 1) * window - now + ttlMargin
        dl_pexpire(k, ttl)
        dl_pexpire(cfg, ttl)
        return {1, wait, tostring(remaining()), tostring(idx)}
    end
    idx = idx + 1
    steps = steps + 1
end

local wait = math.ceil((curIdx + 1) * window - now)
return {0, wait, tostring(remaining()), ''}
