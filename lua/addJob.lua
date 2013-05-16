--[[ Pseudocode
If job exists: remove from queues,
Set job key hash fields to input data
If a delay is specified, add job to delay queue, and record its priority in a hash
Else, add job to main queue with specified priority as score
]]
local kQueue, kDelayQueue, kDelayPriorities, kJobKey = unpack(KEYS)
local jobid, data, priority, now_ms, delay, nx, autoremove = unpack(ARGV)
delay = tonumber(delay)
local run_at = tonumber(now_ms) + delay
if nx == "1" then
    local existing = redis.call('exists', kJobKey)
    if existing == 1 then
        return redis.status_reply('0')
    end
end
redis.call('hmset', kJobKey, 'data', data, 'priority', priority, 'delay', delay, 'nx', nx, 'autoremove', autoremove)
if delay > 0 then
    redis.call('hset', kDelayPriorities, jobid, priority)
    redis.call('zadd', kDelayQueue, run_at, jobid)
    redis.call('zrem', kQueue, jobid)
else
    redis.call('hdel', kDelayPriorities, jobid)
    redis.call('zadd', kQueue, priority, jobid)
    redis.call('zrem', kDelayQueue, jobid)
end
return redis.status_reply('1')
