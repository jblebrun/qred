--[[ Pseudocode
If job exists: remove from queues,
Set job key hash fields to input data
If a delay is specified, add job to delay queue, and record its priority in a hash
Else, add job to main queue with specified priority as score
]]
local kQueue, kDelayQueue, kDataHash, kPriorityHash, kRuntimeHash, kCreatedHash = unpack(KEYS)
local jobid, data, priority, now_ms, delay = unpack(ARGV)
delay = tonumber(delay)
local run_at = tonumber(now_ms) + delay
redis.call('hset', kDataHash, jobid, data)
redis.call('hset', kPriorityHash, jobid, priority)
redis.call('hset', kRuntimeHash, jobid, run_at)
redis.call('hset', kCreatedHash, jobid, now_ms)
if delay > 0 then
    redis.call('zadd', kDelayQueue, run_at, jobid)
    redis.call('zrem', kQueue, jobid)
else
    redis.call('zadd', kQueue, priority, jobid)
    redis.call('zrem', kDelayQueue, jobid)
end
redis.status_reply('Added')
