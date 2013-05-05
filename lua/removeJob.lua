--[[Pseudocode
Remove from delay queue
Remove from queue
Get all job fields
Delete the job
Return job fields
]]
local kQueue, kDelayQueue, kDataHash, kPriorityHash, kRuntimeHash, kCreatedHash = unpack(KEYS)
local jobid = ARGV[1]
redis.call('hdel', kDataHash, jobid)
redis.call('hdel', kPriorityHash, jobid)
redis.call('hdel', kRuntimeHash, jobid)
redis.call('hdel', kCreatedHash, jobid)
redis.call('zrem', kQueue, jobid)
redis.call('zrem', kDelayQueue, jobid)
return result
