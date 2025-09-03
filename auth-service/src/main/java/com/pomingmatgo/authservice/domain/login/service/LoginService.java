package com.pomingmatgo.authservice.domain.login.service;

import com.pomingmatgo.authservice.api.request.LoginInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
@RequiredArgsConstructor
public class LoginService {
    private final RestTemplate restTemplate = new RestTemplate();
    public void login(LoginInfo loginInfo) {
        String url = String.format(
                "http://localhost:8082/login-process?email=%s&password=%s",
                loginInfo.getEmail(),
                loginInfo.getPassword()
        );

        HttpHeaders headers = new HttpHeaders();
        headers.add("Accept", "*/*");
        headers.add("User-Agent", "PostmanRuntime/7.15.0");
        HttpEntity<String> entity = new HttpEntity<>(headers);

        ResponseEntity<Boolean> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Boolean.class
        );

        if(!response.getBody()) {
            //throw
            System.out.println("로그인 실패");
        }
        else {
            //jwt
            System.out.println("로그인 성공");
        }
    }
}
