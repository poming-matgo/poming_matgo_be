package com.pomingmatgo.gameservice.domain.service.matgo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GameLock {
    String key();
    long waitTime() default 3000;
    long leaseTime() default 2000;
}