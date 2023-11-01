-- 1.参数列表
-- 优惠券id
local voucherId = ARGV[1]
-- 用户id
local userId = ARGV[2]
-- 订单id
local orderId = ARGV[3]

-- 2.数据key
-- 库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 订单key
local orderKey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- 3.1.判断库存是否充足
if tonumber(redis.call('get', stockKey)) <= 0 then
    -- 3.2.库存不足，返回1
    return 1
end

-- 3.3.判断用户是否下单
if redis.call('sismember', orderKey, userId) == 1 then
    -- 3.4.存在，说明是重复下单，返回2
    return 2
end

-- 3.5.扣减库存
redis.call('incrby', stockKey, -1)

-- 3.6.下单成功，保存用户购买记录
redis.call('sadd', orderKey, userId)

-- 3.7.发送消息到队列中
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

-- 3.8.下单成功，返回0
return 0
