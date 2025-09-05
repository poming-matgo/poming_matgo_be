package com.pomingmatgo.authservice.domain.service.login;

import com.pomingmatgo.authservice.domain.LoginType;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class NaverLoginStrategy implements LoginStrategy {
    @Override
    public String extractProviderId(Map<String, Object> attributes) {
        Map<String, Object> response = (Map<String, Object>) attributes.get("response");
        return (String) response.get("id");
    }

    @Override
    public String getNameAttributeKey() {
        return "response";
    }

    @Override
    public LoginType resolveLoginType() {
        return LoginType.NAVER;
    }
}
