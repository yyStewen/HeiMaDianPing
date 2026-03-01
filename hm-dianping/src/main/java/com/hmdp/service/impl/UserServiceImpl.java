package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号码
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }
        //手机号符合格式要求，则生成验证码
        String code = RandomUtil.randomNumbers(6);
        log.debug("生成的验证码：{}", code);
        //2.保存验证码到redis
        //session.setAttribute("code", code);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+ phone, code,LOGIN_CODE_TTL, TimeUnit.MINUTES);


        //3.发送验证码
        log.debug("发送验证码成功！验证码为：{}", code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.校验手机号,再次进行校验
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }

        //2.校验验证码
        //Object cacheCode = session.getAttribute("code");
        //改为从redis中获取
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+ phone);
        String code = loginForm.getCode();

        if (cacheCode == null || !cacheCode.toString().equals(code)) {
            //3.不一致，报错
            return Result.fail("验证码错误！");
        }


        //4.一致，根据手机号查询用户
        User user = query().eq("phone", phone).one();

        //5，判断用户是否存在
        if (user == null) {
            //6.不存在，创建新用户,并保存
            user = createUserWithPhone(phone);
        }


        //7.保存用户信息到redis中

        //session.setAttribute("user", userDTO);
        //7.1 随机生成token作为登陆令牌
        String token = UUID.randomUUID().toString(true);

        //7.2 将userDTO转为hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));


        //7.3 存储
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+ token, userMap);
        //7.4 设置token有效期
        //stringRedisTemplate.expire(LOGIN_USER_KEY+ token, LOGIN_USER_TTL, TimeUnit.MINUTES);
        //只要用户在访问我，就刷新token的过期时间
        stringRedisTemplate.expire(LOGIN_USER_KEY+ token, LOGIN_USER_TTL, TimeUnit.MINUTES);


        //8 返回token
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));

        save(user);
        return user;

    }
}
