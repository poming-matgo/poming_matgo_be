package com.pomingmatgo.authservice.global.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;

//@Configuration
public class SocialLoginConfig {

    @Value("${spring.security.oauth2.client.registration.naver.client-id}")
    private String naverClientId;

    @Value("${spring.security.oauth2.client.registration.naver.client-secret}")
    private String naverClientSecret;

    @Value("${spring.security.oauth2.client.registration.naver.redirect-uri}")
    private String naverRedirectUri;

    @Value("${spring.security.oauth2.client.registration.naver.scope}")
    private String[] naverScopes;

    @Value("${spring.security.oauth2.client.provider.naver.authorization-uri}")
    private String naverAuthorizationUri;

    @Value("${spring.security.oauth2.client.provider.naver.token-uri}")
    private String naverTokenUri;

    @Value("${spring.security.oauth2.client.provider.naver.user-info-uri}")
    private String naverUserInfoUri;

    @Value("${spring.security.oauth2.client.provider.naver.user-name-attribute}")
    private String naverUserNameAttribute;

    //@Bean
    public ClientRegistrationRepository clientRegistrationRepository() {
        ClientRegistration naverRegistration = ClientRegistration.withRegistrationId("naver")
                .clientId(naverClientId)
                .clientSecret(naverClientSecret)
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .redirectUri(naverRedirectUri)
                .scope(naverScopes)
                .authorizationUri(naverAuthorizationUri)
                .tokenUri(naverTokenUri)
                .userInfoUri(naverUserInfoUri)
                .userNameAttributeName(naverUserNameAttribute)
                .clientName("naver")
                .build();

        return new InMemoryClientRegistrationRepository(naverRegistration);
    }
}