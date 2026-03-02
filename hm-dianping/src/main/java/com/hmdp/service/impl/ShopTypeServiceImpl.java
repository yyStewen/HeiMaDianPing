package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
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
        //1.从redis查询商铺类型数据的缓存,采用字符串形式储存查询的结果在redis中
        String key = "cache:shopType:";
        List<String> shopTypeList = stringRedisTemplate.opsForList().range(key, 0, -1);


        //2.判断是否存在
        if (!shopTypeList.isEmpty()) {
            //3.存在，返回，转成Java对象。
            //        List<ShopType> typeList = typeService
//                .query().orderByAsc("sort").list();
//        return Result.ok(typeList);
           //先创建一个空的结果列表
            List<ShopType> typeList = new ArrayList<>();
            for (String shopType : shopTypeList) {
                typeList.add(JSONUtil.toBean(shopType, ShopType.class));
            }
            //返回结果
            return Result.ok(typeList);

        }

        //3.不存在，查数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();

        //4.数据库没有，返回错误
        if (typeList == null) {
            return Result.fail("没有查询到数据");
        }


        //5.数据库有，返回，写入redis
        for (ShopType shopType : typeList) {
            stringRedisTemplate.opsForList().rightPush(key, JSONUtil.toJsonStr(shopType));
        }


        //6.返回
        return Result.ok(typeList);
    }
}
