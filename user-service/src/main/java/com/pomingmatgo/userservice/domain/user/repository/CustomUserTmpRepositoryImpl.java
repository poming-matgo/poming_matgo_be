package com.pomingmatgo.userservice.domain.user.repository;

import com.pomingmatgo.userservice.domain.user.UserTmp;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Set;

//데이터 많아지면 오버헤드 클 것 같은데..
@RequiredArgsConstructor
public class CustomUserTmpRepositoryImpl implements CustomUserTmpRepository {
    private final StringRedisTemplate redisTemplate;
    private static final String USER_TMP_PREFIX = "userTmp:";

    @Override
    public boolean existsByEmail(String email) {
        for (String key : redisTemplate.keys(USER_TMP_PREFIX + "*")) {
            if (email.equals((String)redisTemplate.opsForHash().get(key, "email"))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean existsByNickname(String nickname) {
        for (String key : redisTemplate.keys(USER_TMP_PREFIX + "*")) {
            if (nickname.equals((String)redisTemplate.opsForHash().get(key, "nickname"))) {
                return true;
            }
        }
        return false;
    }
}
