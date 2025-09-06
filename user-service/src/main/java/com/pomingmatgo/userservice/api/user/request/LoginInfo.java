package com.pomingmatgo.userservice.api.user.request;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
@NoArgsConstructor
public class LoginInfo {

    private String identifier;
    private String password;

    public void setIdentifier(String identifier) {
        this.identifier = URLDecoder.decode(identifier, StandardCharsets.UTF_8);
    }

    public void setPassword(String password) {
        this.password = URLDecoder.decode(password, StandardCharsets.UTF_8);
    }
}