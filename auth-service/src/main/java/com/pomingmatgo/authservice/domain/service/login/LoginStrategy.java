package com.pomingmatgo.authservice.domain.service.login;

import com.pomingmatgo.authservice.domain.LoginType;

import java.util.Map;

public interface LoginStrategy {
    String extractProviderId(Map<String, Object> attributes);
    String getNameAttributeKey();
    LoginType resolveLoginType();
}


