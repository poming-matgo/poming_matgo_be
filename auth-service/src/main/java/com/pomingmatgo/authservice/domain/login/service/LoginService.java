package com.pomingmatgo.authservice.domain.login.service;

import com.pomingmatgo.authservice.api.request.LoginInfo;
import com.pomingmatgo.authservice.api.response.AuthCodeResponse;
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
        // 사용자의 이메일과 비밀번호로 인증 객체 생성
        Authentication authentication = new UsernamePasswordAuthenticationToken(
                loginInfo.getEmail(), // 사용자가 입력한 이메일
                loginInfo.getPassword() // 사용자가 입력한 비밀번호
        );

        // AuthenticationProvider를 통해 인증 처리
        Authentication authResult = authenticationProvider.authenticate(authentication);

        // 등록된 클라이언트 정보 가져오기 (react 클라이언트 기준)
        RegisteredClient registeredClient = registeredClientRepository.findByClientId("react");

        String authorizationCode = UUID.randomUUID().toString();

        // OAuth2AuthorizationRequest 생성
        OAuth2AuthorizationRequest authorizationRequest = OAuth2AuthorizationRequest.authorizationCode()
                .clientId(registeredClient.getClientId()) // 클라이언트 ID
                .authorizationUri("/oauth2/authorize") // 인증 엔드포인트 URI
                .redirectUri("http://localhost:8082/callback") // 리다이렉션 URI
                .scopes(registeredClient.getScopes()) // 허용된 스코프
                .state(UUID.randomUUID().toString()) // 상태 값
                .build();

// OAuth2Authorization 객체 생성 시 속성으로 추가
        OAuth2Authorization authorization = OAuth2Authorization.withRegisteredClient(registeredClient)
                .principalName(authResult.getName()) // 인증된 사용자 이름
                .attributes(attribute -> {
                    attribute.put(Principal.class.getName(), authResult);
                    attribute.put(OAuth2AuthorizationRequest.class.getName(), authorizationRequest);
                })
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE) // Authorization Code Grant 타입
                .token(new OAuth2AuthorizationCode(
                        authorizationCode, // 생성된 인증 코드
                        Instant.now(), // 발급 시간
                        Instant.now().plus(5, ChronoUnit.MINUTES) // 만료 시간 (5분 후)
                ))
                .build();

        // 인증 정보 저장
        authorizationService.save(authorization);

        // 생성된 Authorization Code 반환
        return new AuthCodeResponse(authorizationCode);
    }
}
