--[[Pseudocode
Remove from delay queue
Remove from queue
Get all job fields
Delete the job
Return job fields
]]
local kQueue, kDelayQueue, kJobKey, kDelayPriorities = unpack(KEYS)
local jobid = ARGV[1]
redis.call('del', kJobKey);
redis.call('zrem', kQueue, jobid)
redis.call('zrem', kDelayQueue, jobid)
redis.call('zrem', kDelayPriorities, jobid)
return result
