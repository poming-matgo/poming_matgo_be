package com.pomingmatgo.userservice.domain.user;

import lombok.AllArgsConstructor;
import lombok.Getter;

//인증서버에 보낼 user 데이터
@AllArgsConstructor
@Getter
public class AuthUser {
    private Long id;
    private String email;
}
