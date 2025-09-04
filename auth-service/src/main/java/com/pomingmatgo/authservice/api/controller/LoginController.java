package com.pomingmatgo.authservice.api.controller;

import com.pomingmatgo.authservice.api.request.LoginInfo;
import com.pomingmatgo.authservice.api.response.AuthCodeResponse;
import com.pomingmatgo.authservice.domain.login.service.LoginService;
import com.pomingmatgo.authservice.global.ApiResponseDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/custom-login")
public class LoginController {
    private final LoginService loginService;
    @PostMapping
    public ApiResponseDto<AuthCodeResponse> handleLogin(@RequestBody LoginInfo loginInfo) {
        AuthCodeResponse authorizationCode = loginService.authenticate(loginInfo);
        return new ApiResponseDto<>(HttpStatus.OK.value(), "인증코드가 발급됐습니다.", authorizationCode);
    }

    /*@PostMapping
    public ApiResponseDto<Void> handleOauth2Login(String code) {
        return null;
    }*/
}
