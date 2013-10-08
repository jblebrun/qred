local kActiveSet, kCompleteSet, kLiveSet, kJobKey = unpack(KEYS)
local jobid, errored, now_ms = unpack(ARGV)

redis.log(redis.LOG_DEBUG , 'finishJob'..jobid..' error: '..errored..' now: '..now_ms)
redis.log(redis.LOG_DEBUG , 'finishJob'..kActiveSet..' '..kCompleteSet..' '..kLiveSet..' '..kJobKey)
redis.call('srem', kActiveSet, jobid)
redis.call('sadd', kCompleteSet, jobid)
redis.log(redis.LOG_DEBUG , 'made set changes finishJob'..jobid..' error: '..errored)

local status
if errored == 'true' or errored == '1' then
    status = 'error'
else
    status = 'complete'
end

redis.call('hmset', kJobKey, 'status', status, 'finished_at', now_ms)

--Set up an expiry time which will get checked by a reaper process
local expire = redis.call('hget', kJobKey, 'autoremove')
redis.log(redis.LOG_DEBUG, 'expire: '..tostring(expire))
if tonumber(expire) ~= nil and tonumber(expire) >= 0 then
    redis.log(redis.LOG_DEBUG, 'expiring: '..tostring(expire)..' '..kLiveSet);
    redis.call('zadd', kLiveSet, now_ms+expire, jobid)
    redis.call('hset', kJobKey, 'expires', now_ms+expire)
end

