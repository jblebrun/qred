--[[Pseudocode
Remove from delay queue
Remove from queue
Get all job fields
Delete the job
Return job fields
]]
local kQueue, kDelayQueue, kJobKey, kDelayPriorities = unpack(KEYS)
local jobid = ARGV[1]
local removed = redis.call('hdel', kOptsHash, jobid)
local queued = redis.call('zrem', kQueue, jobid)
local delayed = redis.call('zrem', kDelayQueue, jobid)
return {removed, queued, delayed}
