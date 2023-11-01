package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否命中
        if (StrUtil.isNotBlank(json)) {
            // 3.命中，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断查到的是否为空值，如果为空值，说明是空的商铺信息（解决缓存穿透）
        if (json != null) {
            // 返回错误信息，商铺信息不存在
            return null;
        }

        // 4.未命中，根据id查询数据库
        R r = dbFallback.apply(id);
        // 3.数据库不存在，返回错误
        if (r == null) {
            // 将空值写入redis（解决缓存穿透）
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 4.存在，写入redis
        this.set(key, r, time, unit);
        // 5.返回
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, String lockPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否命中
        if (StrUtil.isBlank(json)) {
            // 3.未命中，直接返回
            return null;
        }

        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        // redisData中的data值是json对象，需要转为Shop类型
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1.未过期，直接返回店铺信息
            return r;
        }
        // 5.2.已过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = lockPrefix + id;
        boolean isLock = tryLock(lockKey);
        // 6.2.判断是否获取锁成功
        if (isLock) {
            // TODO 获取锁成功之后，应该做二次检查，如果存在则无需重建缓存

            // 6.3.成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }

        // 6.4.返回过期的商铺信息
        return r;
    }

    private void unlock(String lockKey) {
        stringRedisTemplate.delete(lockKey);
    }

    private boolean tryLock(String lockKey) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", 10, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

}
