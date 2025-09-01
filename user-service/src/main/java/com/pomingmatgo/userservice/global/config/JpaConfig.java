package com.pomingmatgo.userservice.global.config;

import com.pomingmatgo.userservice.global.annotation.RedisRepository;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

@Configuration
@EnableRedisRepositories(
        basePackages = "com.pomingmatgo.userservice.domain.user.repository",
        excludeFilters = @ComponentScan.Filter(type = FilterType.ANNOTATION, classes = RedisRepository.class)
)
public class JpaConfig {
}
