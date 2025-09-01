package com.pomingmatgo.userservice.domain.user.repository;

import com.pomingmatgo.userservice.domain.user.User;
import org.springframework.data.repository.CrudRepository;


public interface UserRepository extends CrudRepository<User, Long> {
    boolean existsByEmail(String email);
    boolean existsByNickname(String nickname);
}
