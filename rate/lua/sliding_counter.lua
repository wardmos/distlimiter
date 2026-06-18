-- sliding_counter.lua: two-window weighted approximation of a sliding window.
-- Memory-light (two counters) but best-effort: the estimate cannot hold an exact
-- future slot, so Reserve returns an estimated delay and Cancel is a no-op
-- State: counters at base:sep:c<idx>, idx = floor(now/window).
-- Config seed: ARGV[8] = limit (int), ARGV[9] = window (micros).

local base = KEYS[1]
local sep = ARGV[1]
local op = ARGV[2]
local now = dl_now(ARGV[3])
local n = tonumber(ARGV[4])
local maxWait = tonumber(ARGV[5])
local ttlMargin = tonumber(ARGV[7])

local cfg = dl_sub(base, sep, 'cfg')
local limit = dl_cfg_num(cfg, 'limit', ARGV[8])
local window = dl_cfg_num(cfg, 'window', ARGV[9])

local function ckey(idx)
    return dl_sub(base, sep, 'c' .. idx)
end

if op == 'cancel' then
    return {1}    -- best-effort: no-op
end

local curIdx = math.floor(now / window)
local curCount = tonumber(redis.call('GET', ckey(curIdx))) or 0
local prevCount = tonumber(redis.call('GET', ckey(curIdx - 1))) or 0

-- Weight the previous window by the fraction still inside the sliding window.
local elapsed = now - curIdx * window
local weight = (window - elapsed) / window
if weight < 0 then weight = 0 end
local estimate = prevCount * weight + curCount

local function remaining()
    local r = limit - estimate
    if r < 0 then r = 0 end
    return r
end

if n == 0 then
    return {1, 0, tostring(remaining()), ''}
end
if limit <= 0 or n > limit then
    return {0, -1, tostring(remaining()), ''}
end

if estimate + n <= limit then
    redis.call('INCRBY', ckey(curIdx), n)
    local ttl = 2 * window + ttlMargin
    dl_pexpire(ckey(curIdx), ttl)
    dl_pexpire(cfg, ttl)
    local rem = limit - (estimate + n)
    if rem < 0 then rem = 0 end
    return {1, 0, tostring(rem), ''}
end

-- Over the limit: estimate the wait. The estimate decays as the previous
-- window's weighted contribution shrinks (rate prevCount/window), capped at the
-- current window's end where the previous window drops out entirely.
local excess = estimate + n - limit
local wait
if prevCount > 0 then
    wait = excess * window / prevCount
    local toBoundary = (curIdx + 1) * window - now
    if wait > toBoundary then wait = toBoundary end
else
    wait = (curIdx + 1) * window - now
end
wait = math.ceil(wait)

if maxWait < 0 then
    -- Reserve: report the estimated delay; no slot is held (best-effort).
    return {1, wait, tostring(remaining()), ''}
end
-- Allow / verify-and-retry Wait: not admittable now.
return {0, wait, tostring(remaining()), ''}
