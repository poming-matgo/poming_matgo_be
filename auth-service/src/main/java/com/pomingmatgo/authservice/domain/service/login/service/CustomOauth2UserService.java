package com.pomingmatgo.authservice.domain.service.login.service;
import com.pomingmatgo.authservice.domain.User;
import com.pomingmatgo.authservice.domain.repository.UserRepository;
import com.pomingmatgo.authservice.domain.LoginType;
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

    @Override
    public OAuth2User loadUser(OAuth2UserRequest userRequest) throws OAuth2AuthenticationException {
        OAuth2UserService<OAuth2UserRequest, OAuth2User> delegate = new DefaultOAuth2UserService();
        OAuth2User oAuth2User = delegate.loadUser(userRequest);
        String registrationId = userRequest.getClientRegistration().getRegistrationId();
        LoginType loginType = getSocialType(registrationId);
        Map<String, Object> attributes = new HashMap<>(oAuth2User.getAttributes());
        final String providerId = switch (registrationId) {
            case "naver" -> (String) ((Map<String, Object>) attributes.get("response")).get("id");
            case "google" -> (String) attributes.get("sub");
            default -> null; // 또는 적절한 기본값 처리
        };

        User user = userRepository.findByIdentifier(providerId)
                .orElseGet(() -> userRepository.save(
                        User.builder()
                                .identifier(providerId)
                                .loginType(loginType)
                                .build()
                ));

        attributes.put("user", user);

        return new DefaultOAuth2User(Collections.emptyList(), attributes, "response");
    }

    private LoginType getSocialType(String registrationId) {
        if("naver".equals(registrationId)) {
            return LoginType.NAVER;
        }
        if("google".equals(registrationId)) {
            return LoginType.GOOGLE;
        }
        return null;
    }
}
