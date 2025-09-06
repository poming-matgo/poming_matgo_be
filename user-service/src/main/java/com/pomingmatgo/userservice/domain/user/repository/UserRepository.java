package com.pomingmatgo.userservice.domain.user.repository;

import com.pomingmatgo.userservice.domain.user.User;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;


public interface UserRepository extends CrudRepository<User, Long> {
    boolean existsByIdentifier(String email);
    boolean existsByNickname(String nickname);
    Optional<User> findByIdentifier(String email);
}
