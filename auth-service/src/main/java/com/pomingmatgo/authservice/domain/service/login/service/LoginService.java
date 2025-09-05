package com.pomingmatgo.authservice.domain.service.login.service;

import com.pomingmatgo.authservice.api.request.LoginInfo;
import com.pomingmatgo.authservice.api.response.AuthCodeResponse;
import com.pomingmatgo.authservice.global.security.PKCEUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;
import org.springframework.security.oauth2.server.authorization.OAuth2Authorization;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationCode;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationService;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.stereotype.Service;

import java.security.Principal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class LoginService {
    private final OAuth2AuthorizationService authorizationService;
    private final AuthenticationProvider authenticationProvider;
    private final RegisteredClientRepository registeredClientRepository;

    public AuthCodeResponse authenticate(LoginInfo loginInfo) {

        Authentication authentication = new UsernamePasswordAuthenticationToken(
                loginInfo.getEmail(),
                loginInfo.getPassword()
        );

        Authentication authResult = authenticationProvider.authenticate(authentication);

        RegisteredClient registeredClient = registeredClientRepository.findByClientId("react");

        String authorizationCode = UUID.randomUUID().toString();
        String codeChallenge = PKCEUtil.generateCodeChallenge(loginInfo.getCodeVerifier());
        OAuth2AuthorizationRequest authorizationRequest = OAuth2AuthorizationRequest.authorizationCode()
                .clientId(registeredClient.getClientId())
                .authorizationUri("/oauth2/authorize")
                .redirectUri(registeredClient.getRedirectUris().stream().findFirst().get())
                .scopes(registeredClient.getScopes())
                .state(UUID.randomUUID().toString())
                .additionalParameters(params -> {
                    params.put("code_challenge", codeChallenge);
                    params.put("code_challenge_method", "S256");
                })
                .build();

        //User user = (User)authResult.getPrincipal();
        //String sub = String.valueOf(user.getId());
       // String identifier = user.getIdentifier();
// OAuth2Authorization 객체 생성 시 속성으로 추가
        OAuth2Authorization authorization = OAuth2Authorization.withRegisteredClient(registeredClient)
                .principalName(authResult.getName())
                .attributes(attribute -> {
                    attribute.put(Principal.class.getName(), authResult);
                    attribute.put(OAuth2AuthorizationRequest.class.getName(), authorizationRequest);
                })
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .token(new OAuth2AuthorizationCode(
                        authorizationCode,
                        Instant.now(),
                        Instant.now().plus(5, ChronoUnit.MINUTES)
                ))
                .build();

        // 인증 정보 저장
        authorizationService.save(authorization);

        // 생성된 Authorization Code 반환
        return new AuthCodeResponse(authorizationCode);
    }
}
