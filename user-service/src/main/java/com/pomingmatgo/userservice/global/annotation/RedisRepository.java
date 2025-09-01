package com.pomingmatgo.userservice.global.annotation;

import org.springframework.stereotype.Repository;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repository // Spring이 리포지토리로 인식할 수 있도록 추가
public @interface RedisRepository {
}