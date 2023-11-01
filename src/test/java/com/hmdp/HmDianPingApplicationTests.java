package com.hmdp;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ShopServiceImpl shopService;

    @Test
    public void test() {
        String key = CACHE_SHOP_KEY + 1;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        System.out.println(shopJson);
        Shop shop = JSONUtil.toBean(shopJson, Shop.class);
        System.out.println(shop);

    }

    @Test
    public void addHotKey() throws InterruptedException {
//        shopService.saveShop2Redis(1L, 10L);
    }

    @Test
    public void testRedisData() {
        String key = CACHE_SHOP_KEY + 1;

        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = (Shop) redisData.getData();
        System.out.println(redisData);
//        System.out.println(redisData.getData().getClass().getName());
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        System.out.println(shop);
//        LocalDateTime expireTime = redisData.getExpireTime();
    }

    @Resource
    private RedisIdWorker redisIdWorker;

    private final ExecutorService es = Executors.newFixedThreadPool(10);

    @Test
    public void testIdWorker() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id= " + id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time= " + (end - begin));

    }

    @Test
    void loadShopData() {
        // 1.查询店铺信息
        List<Shop> list = shopService.list();

        // 2.把店铺分组，按照typeid分组，typeid一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));

        // 3.分批完成写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1.获取类型id
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            // 3.2.获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            ArrayList<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            // 3.3.写入redis
            for (Shop shop : value) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }

    }

}
