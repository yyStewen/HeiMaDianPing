package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FEED_KEY;

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

    @ Resource
    private IFollowService followService;

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

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("发布失败");
        }
        // 保存成功之后发送给关注者

        //查询所有粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //遍历，然后推送
        for (Follow follow : follows) {
            //获取粉丝id
            Long userId = follow.getUserId();
            //推送，每个粉丝一个收件箱，有序集合表示
            String key=FEED_KEY +userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //1.获取当前用户
        Long userId = UserHolder.getUser().getId();

        //2.找到收件箱 zrevrange key max min limit offset count
        String key=FEED_KEY +userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        //非空判断
        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }

        //3.解析数据 blogId,minTime,offset
        List<Long>ids=new ArrayList<>(typedTuples.size());
        long minTime=0;
        int os=1;
        for(ZSetOperations.TypedTuple<String> tuple : typedTuples){
            //获取id
            String idStr=tuple.getValue();
            ids.add(Long.valueOf(idStr));

            //获取分数（时间戳）
            long time = tuple.getScore().longValue();
            if(time==minTime){
                os++;
            }else{
                minTime=time;
                os=1;
            }

        }


        //4.根据id查询blog
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + StrUtil.join(",", ids) + ")").list();

        blogs.forEach(blog -> {//查询blog相关数据
            //查询blog相关用户
            queryBlogUser(blog);
            //查询blog是否被点赞
            isBlogLiked(blog);
        });

        //5.封装并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(os);
        scrollResult.setMinTime(minTime);
        return Result.ok(scrollResult);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
