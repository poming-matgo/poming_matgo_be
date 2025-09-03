package com.pomingmatgo.userservice.api.user.request;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class LoginInfo {

    private String email;
    private String password;
}