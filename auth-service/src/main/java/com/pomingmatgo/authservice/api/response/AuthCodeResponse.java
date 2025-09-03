package com.pomingmatgo.authservice.api.response;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AuthCodeResponse {
    private String authorizationCode;
}
