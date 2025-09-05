package com.pomingmatgo.authservice.domain.repository;

import com.pomingmatgo.authservice.domain.User;
import com.pomingmatgo.authservice.global.exception.ErrorCode;
import com.pomingmatgo.authservice.global.exception.SystemException;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class UserRepository {
    private final RestTemplate restTemplate = new RestTemplate();
    public Optional<User> findByIdentifier(String identifier) {
        String url = String.format("http://localhost:8082/login-process?identifier=%s&password=%s", identifier, "");

        try {
            User user = restTemplate.getForObject(url, User.class);
            return Optional.ofNullable(user);
        } catch (RestClientException e) {
            throw new SystemException(ErrorCode.SYSTEM_ERROR);
        }
    }

    public User save(User user) {
        String url = String.format("http://localhost:8082/register/social-signup");

        try {
            return restTemplate.postForObject(url, user, User.class);
        } catch (RestClientException e) {
            throw new SystemException(ErrorCode.SYSTEM_ERROR);
        }
    }
}
