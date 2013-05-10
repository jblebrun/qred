--[[
]]
local kDataHash, kOptsHash = unpack(KEYS)
local jobid = ARGV[1]
local exists = redis.call('hexists', kOptsHash, jobid)

if exists == 0 then
    return redis.status_reply('0')
end
local data = redis.call('hget', kDataHash, jobid)
local opts = cmsgpack.unpack(redis.call('hget', kOptsHash, jobid))
return {opts, data}
