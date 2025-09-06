package com.pomingmatgo.authservice.global.security;

import com.pomingmatgo.authservice.domain.User;
import com.pomingmatgo.authservice.global.exception.BusinessException;
import com.pomingmatgo.authservice.global.exception.ErrorCode;
import com.pomingmatgo.authservice.global.exception.SystemException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;


@Component
public class ExternalUserAuthenticationProvider implements AuthenticationProvider {
    private final RestTemplate restTemplate = new RestTemplate();
    @Value("${user.service.address}")
    private String userServiceAddress;

    @Override
    public Authentication authenticate(Authentication authentication) {
        String identifier = authentication.getName();
        String password = authentication.getCredentials().toString();

        // 외부 User 서버로 인증 요청
        // todo: userRepository로 책임 분리
        String url = String.format(
                userServiceAddress+"/login-process?identifier=%s&password=%s",
                URLEncoder.encode(identifier, StandardCharsets.UTF_8),
                URLEncoder.encode(password, StandardCharsets.UTF_8)
        );

        try {
            User user = restTemplate.getForObject(url, User.class);
            if (user != null)
                return new UsernamePasswordAuthenticationToken(user.getId(), null, Collections.emptyList());
            else
                throw new BusinessException(ErrorCode.INVALID_ID_OR_PASSWORD);

        } catch (RestClientException e) {
            throw new SystemException(ErrorCode.SYSTEM_ERROR);
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}
