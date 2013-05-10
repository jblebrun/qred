--[[ Pseudocode
If job exists: remove from queues,
Set job key hash fields to input data
If a delay is specified, add job to delay queue, and record its priority in a hash
Else, add job to main queue with specified priority as score
]]
local kQueue, kDelayQueue, kDataHash, kOptsHash = unpack(KEYS)
local jobid, data, priority, now_ms, delay, nx, autoremove = unpack(ARGV)
delay = tonumber(delay)
local run_at = tonumber(now_ms) + delay
if nx == "1" then
    local existing = redis.call('hexists', kDataHash, jobid)
    if existing == 1 then
        return redis.status_reply('0')
    end
end
redis.call('hset', kDataHash, jobid, data)
local opts = cmsgpack.pack({delay=delay, priority=priority, nx=nx, autoremove=autoremove});
redis.call('hset', kOptsHash, jobid, opts)
if delay > 0 then
    redis.call('zadd', kDelayQueue, run_at, jobid)
    redis.call('zrem', kQueue, jobid)
else
    redis.call('zadd', kQueue, priority, jobid)
    redis.call('zrem', kDelayQueue, jobid)
end
return redis.status_reply('1')
