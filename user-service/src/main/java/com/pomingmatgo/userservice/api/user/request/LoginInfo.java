package com.pomingmatgo.userservice.api.user.request;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class LoginInfo {

    private String identifier;
    private String password;
}