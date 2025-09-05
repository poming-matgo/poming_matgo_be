package com.pomingmatgo.authservice.domain.service.login;

import com.pomingmatgo.authservice.domain.LoginType;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class GoogleLoginStrategy implements LoginStrategy {
    @Override
    public String extractProviderId(Map<String, Object> attributes) {
        return (String) attributes.get("sub");
    }

    @Override
    public String getNameAttributeKey() {
        return "sub";
    }

    @Override
    public LoginType resolveLoginType() {
        return LoginType.GOOGLE;
    }
}