local kActiveSet, kCompleteSet, kLiveSet, kDelayQueue, kJobKey = unpack(KEYS)
local jobid, errored, now_ms = unpack(ARGV)

redis.call('srem', kActiveSet, jobid)
local status = redis.call('hget', kJobKey, 'status')
if status == 'requeue' then
    redis.call('sadd', kCompleteSet, jobid)
else 
    local delay = redis.call('hget', kJobKey, 'delay')
    delay = tonumber(delay)
    if not delay then delay = 0 end
    local run_at = tonumber(now_ms) + delay;
    redis.call('zadd', kDelayQueue, run_at, jobid)
end

local status
if errored == 'true' or errored == '1' then
    status = 'error'
else
    status = 'complete'
end

redis.call('hmset', kJobKey, 'status', status, 'finished_at', now_ms)

--Set up an expiry time which will get checked by a reaper process
local expire = redis.call('hget', kJobKey, 'autoremove')
if tonumber(expire) ~= nil and tonumber(expire) >= 0 then
    redis.log(redis.LOG_DEBUG, 'expiring: '..tostring(expire)..' '..kLiveSet);
    redis.call('zadd', kLiveSet, now_ms+expire, jobid)
    redis.call('hset', kJobKey, 'expires', now_ms+expire)
end

