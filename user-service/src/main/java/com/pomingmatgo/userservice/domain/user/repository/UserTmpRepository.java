package com.pomingmatgo.userservice.domain.user.repository;

import com.pomingmatgo.userservice.domain.user.UserTmp;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserTmpRepository extends CrudRepository<UserTmp, String>, CustomUserTmpRepository {

}
