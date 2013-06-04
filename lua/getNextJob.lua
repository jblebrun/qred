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
local kQueue, kDelayQueue, kActiveQueue, kDelayPriorities = unpack(KEYS)
local now_ms = ARGV[1]

-- PART A: Delayed job promotion 
-- Find all delayed jobs that can be run now
local promoting = redis.pcall('zrangebyscore', kDelayQueue, 0, now_ms)

-- Add those jobs to the main queue
for i,jobid in ipairs(promoting) do
    local jobPriority = redis.pcall('hget', kDelayPriorities, jobid)
    redis.pcall('hdel', kDelayPriorities, jobid)
    redis.pcall('zadd', kQueue, jobPriority, jobid)
end

-- Remove the promoted jobs from the delay queue
redis.pcall('zremrangebyscore', kDelayQueue, 0, now_ms)

--Get and remove the next job
local jobid= redis.call('zrange', kQueue, 0, 0)[1]
redis.call('zremrangebyrank', kQueue, 0, 0)
if jobid then
    redis.call('sadd', kActiveQueue, jobid);
-- Return the next queued job
    return {1, jobid}
else 
--If no jobs are ready, return the time until the next delayed job is ready
    local nextdelay = redis.pcall('zrange', kDelayQueue, 0, 0, 'withscores')[2]
    return {0, nextdelay}
end 


