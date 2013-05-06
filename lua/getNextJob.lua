--[[
    Return the next queued job
    Each time this function is called, 
    The delay queue is checked for promotable delayed jobs
    Pseudocode:
    get all keys in delay queue that are now runnable
    move those keys into the main queue
    record the lowest time in the delay queue
    get the first job in the queued jobs list
    add that key to the active list
    remove that key from the queue list
    return [jobid, next_delay_check]
]]
local kQueue, kDelayQueue, kDataHash, kPriorityHash, kRuntimeHash, kCreatedHash = unpack(KEYS)
local now_ms = ARGV[1]

-- PART A: Delayed job promotion 
-- Find all delayed jobs that can be run now
local promoting = redis.pcall('zrangebyscore', kDelayQueue, 0, now_ms)

-- Add those jobs to the main queue
for i,jobid in ipairs(promoting) do
    local priority = redis.call('hget', kPriorityHash, jobid)
-- TODO - can we turn this into a bulk zadd?
    redis.pcall('zadd', kQueue, priority, jobid)
end

-- Remove the promoted jobs from the delay queue
redis.pcall('zremrangebyscore', kDelayQueue, 0, now_ms)


--Get and remove the next job
local jobid = redis.call('zrange', kQueue, 0, 0)[1]
redis.call('zremrangebyrank', kQueue, 0, 0)
if jobid then
-- Return the next queued job
    local priority = redis.call('hget', kPriorityHash, jobid)
    local data = redis.call('hget', kDataHash, jobid)
    local created_at = redis.call('hget', kCreatedHash, jobid)
    local runtime = redis.call('hget', kRuntimeHash, jobid)
    local job = {jobid, priority, data, runtime, created_at}
    return job
else 
--If no jobs are ready, return the time until the next delayed job is ready
    local nextdelay = redis.pcall('zrange', kDelayQueue, 0, 0, 'withscores')[2]
    redis.log(redis.LOG_WARNING, 'Returning delay '..tostring(nextdelay))
    return nextdelay
end 


