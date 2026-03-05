-- 比较线程标识是否和锁中标识一样
if(redis.call('get', KEYS[1]) == ARGV[1]) then
    -- 释放锁
    redis.call('del', KEYS[1])
end
return 0
