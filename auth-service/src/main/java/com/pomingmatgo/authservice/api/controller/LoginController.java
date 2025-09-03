package com.pomingmatgo.authservice.api.controller;

import com.pomingmatgo.authservice.api.request.LoginInfo;
import com.pomingmatgo.authservice.domain.login.service.LoginService;
import com.pomingmatgo.authservice.global.ApiResponseDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/login")
public class LoginController {
    private final LoginService loginService;
    @PostMapping
    public ApiResponseDto<Void> handleLogin(@RequestBody LoginInfo loginInfo) {
        loginService.login(loginInfo);
        return new ApiResponseDto<>(HttpStatus.OK.value(), "로그인 성공");
    }
}
