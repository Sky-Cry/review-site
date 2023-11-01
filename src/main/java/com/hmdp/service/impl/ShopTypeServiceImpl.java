package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        // 1.从redis获取商店类型
        String key = CACHE_SHOP_KEY + "list";
        String typeListJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否在redis中命中
        if (StrUtil.isNotBlank(typeListJson)) {
            // 3.命中，直接返回
            List<ShopType> typeList = JSONUtil.toList(typeListJson, ShopType.class);
            return Result.ok(typeList);
        }
        // 4.未命中，从数据库查询
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 5.如果在数据库中也查不到，返回错误
        if (typeList == null) {
            return Result.fail("未查到店铺类型");
        }
        // 6.如果查到，也存到redis一份
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7.返回
        return Result.ok(typeList);
    }
}
