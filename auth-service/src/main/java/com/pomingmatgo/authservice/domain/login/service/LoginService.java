package com.pomingmatgo.authservice.domain.login.service;

import com.pomingmatgo.authservice.api.request.LoginInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
@RequiredArgsConstructor
public class LoginService {
    private final RestTemplate restTemplate = new RestTemplate();
    public void login(LoginInfo loginInfo) {
        String url = "http://localhost:8082/login";
        // HTTP 요청을 통해 로그인 정보를 확인
        ResponseEntity<Boolean> response = restTemplate.postForEntity(url, loginInfo, Boolean.class);
        if(!response.getBody()) {
            //throw
        }
        else {
            //jwt
        }
    }
}
