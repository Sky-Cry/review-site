package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {

        // 缓存穿透方案
        // Shop shop = queryWithPassThrough(id);
//        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 缓存穿透+互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        // 逻辑过期时间解决缓存穿透
//        Shop shop = queryWithLogicalExpire(id);
//        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, LOCK_SHOP_KEY, id, Shop.class, (id1) -> {
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, (id1) -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return getById(id);
        }, 20L, TimeUnit.SECONDS);


        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);

    }

//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

//    /**
//     * 逻辑过期时间解决缓存穿透，只针对redis中已存在的热点key
//     * @param id
//     * @return
//     */
//    private Shop queryWithLogicalExpire(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//
//        // 1.从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        // 2.判断是否命中
//        if (StrUtil.isBlank(shopJson)) {
//            // 3.未命中，直接返回
//            return null;
//        }
//
//        // 4.命中，需要先把json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        // redisData中的data值是json对象，需要转为Shop类型
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        // 5.判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            // 5.1.未过期，直接返回店铺信息
//            return shop;
//        }
//        // 5.2.已过期，需要缓存重建
//        // 6.缓存重建
//        // 6.1.获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        // 6.2.判断是否获取锁成功
//        if (isLock) {
//            // TODO 获取锁成功之后，应该做二次检查，如果存在则无需重建缓存
//
//            // 6.3.成功，开启独立线程，实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    // 重建缓存
//                    this.saveShop2Redis(id, 10L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    // 释放锁
//                    unlock(lockKey);
//                }
//            });
//        }
//
//        // 6.4.返回过期的商铺信息
//        return shop;
//    }
//
//    /**
//     * 将带有Shop的RedisData对象存入redis中
//     * @param id
//     * @param expireSeconds
//     */
//    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
//        // 1.查询店铺信息
//        Shop shop = getById(id);
//        // 模拟缓存重建的时间
//        Thread.sleep(200);
//
//        // 2.封装逻辑过期时间
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//
//        // 3.写入redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
//    }

//    /**
//     * 缓存穿透+缓存击穿（互斥锁）
//     * @param id
//     * @return
//     */
//    private Shop queryWithMutex(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        // 1.从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否命中
//        if (StrUtil.isNotBlank(shopJson)) {
//            // 3.命中，直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // 判断查到的是否为空值，如果为空值，说明是空的商铺信息（解决缓存穿透），这种情况其实也是命中的一种
//        if (shopJson != null) {
//            // 返回null，商铺信息不存在
//            return null;
//        }
//
//        // 4.实现缓存重建
//        String lockKey = LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            // 4.1.获取互斥锁
//            boolean isLock = tryLock(lockKey);
//            // 4.2.判断是否获取锁成功
//            if (!isLock) {
//                // 4.3.失败，则休眠并重试（这里用递归实现）
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//
//            // 4.4.成功获取锁，先判断缓存是否存在，存在无需构建缓存（二次检查）；不存在，再根据id查询数据库
//            // 二次检查
//            String sj = stringRedisTemplate.opsForValue().get(key);
//            if (StrUtil.isNotBlank(sj)) {
//                return JSONUtil.toBean(sj, Shop.class);
//            }
//            if (sj != null) {
//                return null;
//            }
//            // 查数据库
//            shop = getById(id);
//            // 模拟重建的延时
//            Thread.sleep(200);
//
//            // 5.数据库中不存在，返回null
//            if (shop == null) {
//                // 将空值写入redis（解决缓存穿透）
//                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//                // 店铺不存在
//                return null;
//            }
//            // 6.数据库中存在，写入redis
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            // 7.释放互斥锁
//            unlock(lockKey);
//        }
//        // 5.返回
//        return shop;
//    }


//    private void unlock(String lockKey) {
//        stringRedisTemplate.delete(lockKey);
//    }
//
//    private boolean tryLock(String lockKey) {
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", 10, TimeUnit.MINUTES);
//        return BooleanUtil.isTrue(flag);
//    }
//
//    /**
//     * 缓存穿透
//     * @param id
//     * @return
//     */
//    private Shop queryWithPassThrough(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        // 1.从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否命中
//        if (StrUtil.isNotBlank(shopJson)) {
//            // 2.1.命中，直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // 判断查到的是否为空值，如果为空值，说明是空的商铺信息（解决缓存穿透）
//        if (shopJson != null) {
//            // 返回错误信息，商铺信息不存在
//            return null;
//        }
//
//        // 2.2.未命中，根据id查询数据库
//        Shop shop = getById(id);
//        // 3.数据库不存在，返回错误
//        if (shop == null) {
//            // 将空值写入redis（解决缓存穿透）
//            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//        // 4.存在，写入redis
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        // 5.返回
//        return shop;
//    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {

        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据查询
            Page<Shop> page = query().eq("type_id", typeId).page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page);

        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis，按照距离排序、分页。结果：店铺id、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key, GeoReference.fromCoordinate(x, y), new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));

        // 4.解析从redis中查到的数据，将店铺id和距离解析出来
        // 4.1.如果查不到，直接返回空list
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 form - end 部分
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 用于保存符合分页条件的商铺id
        ArrayList<Long> ids = new ArrayList<>(list.size());
        // 用于保存商铺id和距该商铺的距离
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        // 5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id, " + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        // 6.返回
        return Result.ok(shops);

    }
}
