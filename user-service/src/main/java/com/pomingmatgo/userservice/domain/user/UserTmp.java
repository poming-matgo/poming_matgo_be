package com.pomingmatgo.userservice.domain.user;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

@RedisHash(value = "userTmp", timeToLive = 600) //10분후 자동 만료
@Getter
@Setter
@NoArgsConstructor
public class UserTmp {
    private String email;
    private String password;
    private String nickname;
    @Id
    private String authId;
}
