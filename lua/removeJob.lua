--[[Pseudocode
Remove from delay queue
Remove from queue
Get all job fields
Delete the job
Return job fields
]]
local kQueue, kDelayQueue, kDataHash, kOptsHash = unpack(KEYS)
local jobid = ARGV[1]
redis.call('hdel', kDataHash, jobid)
redis.call('hdel', kOptsHash, jobid)
redis.call('zrem', kQueue, jobid)
redis.call('zrem', kDelayQueue, jobid)
return result
