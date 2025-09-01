package com.pomingmatgo.userservice.domain.user.repository;

public interface CustomUserTmpRepository {
    boolean existsByEmail(String email);
    boolean existsByNickname(String email);
}
