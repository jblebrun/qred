--[[ Pseudocode
If job exists: remove from queues,
Set job key hash fields to input data
If a delay is specified, add job to delay queue, and record its priority in a hash
Else, add job to main queue with specified priority as score
]]
local kQueue, kDelayQueue, kDelayPriorities, kLiveSet, kActiveSet, kCompleteSet, kJobKey = unpack(KEYS)
local jobid, data, priority, now_ms, delay, nx, autoremove = unpack(ARGV)
delay = tonumber(delay)
local run_at = tonumber(now_ms) + delay
local status = redis.call('hget', kJobKey, 'status')

if nx and status then
    local init = string.sub(status, 1, 1)
    if string.find(nx, init) then
        return {'0',status, nx}
    end
end

local created_at = now_ms

redis.call('hmset', kJobKey, 'data', data, 'priority', priority, 'run_at', run_at, 'delay', delay, 'autoremove', autoremove, 'c', created_at, 'u', now_ms)
if delay > 0 then
    redis.call('hset', kJobKey, 'status', 'delayed')
    redis.call('hset', kDelayPriorities, jobid, priority)
    redis.call('zadd', kDelayQueue, run_at, jobid)
    redis.call('zrem', kQueue, jobid)
else
    redis.call('hset', kJobKey, 'status', 'queued')
    redis.call('hdel', kDelayPriorities, jobid)
    redis.call('zadd', kQueue, priority, jobid)
    redis.call('zrem', kDelayQueue, jobid)
end
redis.call('srem', kActiveSet, jobid)
redis.call('srem', kCompleteSet, jobid)
redis.call('zadd', kLiveSet, '+inf', jobid)
local delayed = redis.call('zcard', kDelayQueue)
local queued = redis.call('zcard', kQueue)
local active = redis.call('scard', kActiveSet)
local complete = redis.call('scard', kCompleteSet)
return {"1",delayed,queued,active,complete}
