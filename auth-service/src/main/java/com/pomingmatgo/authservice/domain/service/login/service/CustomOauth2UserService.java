package com.pomingmatgo.authservice.domain.service.login.service;
import com.pomingmatgo.authservice.domain.User;
import com.pomingmatgo.authservice.domain.repository.UserRepository;
import com.pomingmatgo.authservice.domain.LoginType;
import com.pomingmatgo.authservice.domain.service.login.LoginStrategy;
import com.pomingmatgo.authservice.domain.service.login.LoginStrategyFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.security.oauth2.client.userinfo.DefaultOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserService;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.user.DefaultOAuth2User;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class CustomOauth2UserService implements OAuth2UserService<OAuth2UserRequest, OAuth2User> {

    private final UserRepository userRepository;
    private final OAuth2UserService<OAuth2UserRequest, OAuth2User> delegate = new DefaultOAuth2UserService();
    private final LoginStrategyFactory loginStrategyFactory;

    @Override
    public OAuth2User loadUser(OAuth2UserRequest userRequest) throws OAuth2AuthenticationException {
        OAuth2User oAuth2User = delegate.loadUser(userRequest);
        Map<String, Object> attributes = new HashMap<>(oAuth2User.getAttributes());
        String registrationId = userRequest.getClientRegistration().getRegistrationId();
        LoginStrategy loginStrategy = loginStrategyFactory.getStrategy(registrationId);

        LoginType loginType = loginStrategy.resolveLoginType();
        String providerId = loginStrategy.extractProviderId(attributes);
        User user = findOrCreateUser(providerId, loginType);

        attributes.put("user", user);

        return new DefaultOAuth2User(Collections.emptyList(), attributes, loginStrategy.getNameAttributeKey());
    }

    private User findOrCreateUser(String providerId, LoginType loginType) {
        return userRepository.findByIdentifier(providerId)
                .orElseGet(() -> userRepository.save(
                        User.builder()
                                .identifier(providerId)
                                .loginType(loginType)
                                .build()
                ));
    }
}

