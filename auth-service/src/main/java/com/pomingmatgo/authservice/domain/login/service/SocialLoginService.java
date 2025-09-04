package com.pomingmatgo.authservice.domain.login.service;

import com.pomingmatgo.authservice.domain.Token;
import com.pomingmatgo.authservice.global.exception.ErrorCode;
import com.pomingmatgo.authservice.global.exception.SystemException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class SocialLoginService {
    @Value("${spring.security.oauth2.client.registration.naver.client-id}")
    private String NAVER_CLIENT_ID;

    @Value("${spring.security.oauth2.client.registration.naver.client-secret}")
    private String NAVER_CLIENT_SECRET;

    @Value("${spring.security.oauth2.client.registration.naver.redirect-uri}")
    private String NAVER_REDIRECT_URI;

    @Value("${spring.security.oauth2.client.provider.naver.token-uri}")
    private String NAVER_TOKEN_URI;

    @Value("${spring.security.oauth2.client.registration.naver.scope}")
    private String NAVER_SCOPE;

    //private final MemberRepository memberRepository;
    //private final TokenProvider tokenProvider;
    //private final TokenRenewService tokenRenewService;

    public String getGoogleAccessToken(String code) {
        RestTemplate restTemplate = new RestTemplate();
        Map<String, String> params = Map.of(
                "code", code,
                "scope", NAVER_SCOPE,
                "client_id", NAVER_CLIENT_ID,
                "client_secret", NAVER_CLIENT_SECRET,
                "redirect_uri", NAVER_REDIRECT_URI,
                "grant_type", "authorization_code"
        );
        try {
            Token token = restTemplate.postForObject(NAVER_TOKEN_URI, params, Token.class);
            return token.accessToken();
        } catch (RestClientException e) {
            throw new SystemException(ErrorCode.EXTERNAL_SERVER_ERROR);
        }
    }

    /*@Transactional
    public ApiResponseTemplate<AuthResDto> signUpOrLogin(String googleAccessToken) {
        MemberInfo memberInfo = getMemberInfo(googleAccessToken);

        Member member = memberRepository.findByEmail(memberInfo.email())
                .orElseGet(() -> memberRepository.save(Member.builder()
                        .email(memberInfo.email())
                        .name(memberInfo.name())
                        .password(null)
                        .mileage(0)
                        .loginType(LoginType.GOOGLE_LOGIN)
                        .roleType(RoleType.ROLE_USER)
                        .build())
                );

        String accessToken = tokenProvider.createAccessToken(member);
        String refreshToken = tokenProvider.createRefreshToken(member);

        tokenRenewService.saveRefreshToken(refreshToken, member.getMemberId());

        return ApiResponseTemplate.success(SuccessCode.LOGIN_MEMBER_SUCCESS,
                AuthResDto.of(accessToken, refreshToken));
    }

    public MemberInfo getMemberInfo(String accessToken) {
        RestTemplate restTemplate = new RestTemplate();
        String url = "https://www.googleapis.com/oauth2/v2/userinfo?access_token=" + accessToken;

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + accessToken);
        headers.setContentType(MediaType.APPLICATION_JSON);

        RequestEntity<Void> requestEntity = new RequestEntity<>(headers, HttpMethod.GET, URI.create(url));
        ResponseEntity<String> responseEntity = restTemplate.exchange(requestEntity, String.class);

        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            String json = responseEntity.getBody();
            Gson gson = new Gson();

            return gson.fromJson(json, MemberInfo.class);
        }

        throw new CustomException(ErrorCode.NOT_FOUND_MEMBER_EXCEPTION,
                ErrorCode.NOT_FOUND_MEMBER_EXCEPTION.getMessage());
    }*/
}
