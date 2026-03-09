package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        //1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("博客不存在");
        }

        //2.查询blog相关用户
        queryBlogUser(blog);
        //3.查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        //1.获取用户
        Long userId = UserHolder.getUser().getId();
        if (userId == null) {//处理用户未登录情况
            return;
        }
        //2.判断当前用户是否已经点过赞
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        //Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        //blog.setIsLike(BooleanUtil.isTrue(isMember));
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        //1.判断当前登录用户有无点赞
        //1.1.获取登录用户
        Long userId = UserHolder.getUser().getId();
        //2.判断当前用户是否已经点过赞
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if(score == null){
            //3.如果没点赞，可以点赞
            //3.1 数据库点赞数＋1
            boolean isSuccess=update().setSql("liked = liked + 1").eq("id", id).update();
            //3.2 保存用户到redis set集合 zadd key value score
            if(isSuccess){
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }


        }else {
            //4.已经点赞，取消点赞
            //4.1 数据库点赞数－1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //4.2 从redis 有序集合中删除用户
            stringRedisTemplate.opsForZSet().remove(key, userId.toString());
        }
        return null;
    }

    @Override
    public Result queryBlogLikes(Long id) {
        //实现查询top5 点赞用户 zrange key 0 4
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(top5 == null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }

        //解析其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);

        //根据用户id查询
        List<User> users = userService.query().in("id", ids).last("order by field (id," + idStr + ")")
                .list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, User.class))
                .collect(Collectors.toList());

        //封装返回
        return Result.ok(users);

    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
