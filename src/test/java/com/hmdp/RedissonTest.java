package com.hmdp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class RedissonTest {

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RedissonClient redissonClient2;

    @Resource
    private RedissonClient redissonClient3;

    private RLock lock;

    @BeforeEach
    void setUp() {
        RLock lock1 = redissonClient.getLock("order");
        RLock lock2 = redissonClient2.getLock("order");
        RLock lock3 = redissonClient3.getLock("order");

        // 创建联锁 MultiLock
        lock = redissonClient.getMultiLock(lock1, lock2, lock3);
    }

    @Test
    void method1() throws InterruptedException {
        // 创建联锁 MultiLock
        boolean isLock = lock.tryLock(1L, TimeUnit.SECONDS);
        System.out.println(isLock);
        lock.unlock();
    }

}
