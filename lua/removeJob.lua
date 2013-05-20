--[[Pseudocode
Remove from delay queue
Remove from queue
Get all job fields
Delete the job
Return job fields
]]
local kQueue, kDelayQueue, kDelayPriorities, kJobKey = unpack(KEYS)
local jobid = ARGV[1]
redis.call('hdel', kDelayPriorities, jobid)
local removed = redis.call('del', kJobKey, jobid)
local queued = redis.call('zrem', kQueue, jobid)
local delayed = redis.call('zrem', kDelayQueue, jobid)
return {removed, queued, delayed}
