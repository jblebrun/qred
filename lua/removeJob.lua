--[[Pseudocode
Remove from delay queue
Remove from queue
Get all job fields
Delete the job
Return job fields
]]
local kQueue, kDelayQueue, kActiveSet, kCompleteSet, kLiveSet, kDelayPriorities, kJobKey = unpack(KEYS)
local jobid, nx = unpack(ARGV);
redis.log(redis.LOG_DEBUG , 'removeJob '..kActiveSet..' '..kCompleteSet..' '..kLiveSet..' '..' '..kDelayPriorities..' '..kJobKey..' '..jobid..' '..nx)
local status = redis.call('hget', kJobKey, 'status')

if nx and status then
    local init = string.sub(status, 1, 1)
    if string.find(nx, init) then
        return {0,status, nx}
    end
end

redis.call('hdel', kDelayPriorities, jobid)
local removed = redis.pcall('del', kJobKey, jobid)
local queued = redis.pcall('zrem', kQueue, jobid)
local delayed = redis.pcall('zrem', kDelayQueue, jobid)
local active = redis.pcall('srem', kActiveSet, jobid)
local complete = redis.pcall('srem', kCompleteSet, jobid)
local live = redis.pcall('zrem', kLiveSet, jobid)
return {removed, queued, delayed, active, complete}
