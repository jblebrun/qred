--[[Pseudocode
Remove from delay queue
Remove from queue
Get all job fields
Delete the job
Return job fields
]]
local kQueue, kDelayQueue, kActiveSet, kCompleteSet, kLiveSet, kDelayPriorities, kJobKey = unpack(KEYS)
redis.log(redis.LOG_DEBUG , 'removeJob '..kActiveSet..' '..kCompleteSet..' '..kLiveSet..' '..' '..kDelayPriorities..' '..kJobKey)
local jobid = ARGV[1]
redis.call('hdel', kDelayPriorities, jobid)
local removed = redis.pcall('del', kJobKey, jobid)
local queued = redis.pcall('zrem', kQueue, jobid)
local delayed = redis.pcall('zrem', kDelayQueue, jobid)
local active = redis.pcall('srem', kActiveSet, jobid)
local complete = redis.pcall('srem', kCompleteSet, jobid)
local live = redis.pcall('zrem', kLiveSet, jobid)
return {removed, queued, delayed, active, complete}
