package com.pomingmatgo.userservice.domain.user.repository;

public interface CustomUserTmpRepository {
    boolean existsByIdentifier(String email);
    boolean existsByNickname(String email);
}
