package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.service.impl.UserServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

@SpringBootTest
public class TokenTest {

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private UserServiceImpl userService;

    /**
     * 批量生成tokens
     */
    @Test
    void testGetTokens() {
        List<User> users = userService.list();
        users.forEach(
                user -> {
                    // 7.1,随机生成token,作为登录令牌
                    String token = UUID.randomUUID().toString(true);
                    // 7.2,将User对象转化为HashMap存储
                    UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
                    File file = new File("G:\\Study\\黑马\\redis\\tokens.txt");
                    FileOutputStream output = null;
                    try {
                        output = new FileOutputStream(file, true);
                        byte[] bytes = token.getBytes();
                        output.write(bytes);
                        output.write("\r\n".getBytes());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            output.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                            CopyOptions.create()
                                    .setIgnoreNullValue(true)
                                    .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
                    // 7.3,存储
                    String tokenKey = LOGIN_USER_KEY + token;
                    stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
                    // 7.4,设置token有效期
//                    stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);
                }
        );

    }

}
