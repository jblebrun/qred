--[[
]]
local kDataHash, kPriorityHash, kRuntimeHash, kCreatedHash = unpack(KEYS)
local jobid = ARGV[1]
local priority = redis.call('hget', kPriorityHash, jobid)
local data = redis.call('hget', kPriorityHash, jobid)
local created_at = redis.call('hget', kCreatedHash, jobid)
local runtime = redis.call('hget', kRuntimeHash, jobid)
return {jobid, priority, data, runtime, created_at}
