package com.pomingmatgo.userservice.domain.user.repository;

import com.pomingmatgo.userservice.domain.user.UserTmp;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;

//todo: 구현해야함
@RequiredArgsConstructor
public class CustomUserTmpRepositoryImpl implements CustomUserTmpRepository {
    private final RedisTemplate<String, UserTmp> redisTemplate;
    @Override
    public boolean existsByEmail(String email) {
        return false;
    }

    @Override
    public boolean existsByNickname(String nickname) {
        return false;
    }
}
