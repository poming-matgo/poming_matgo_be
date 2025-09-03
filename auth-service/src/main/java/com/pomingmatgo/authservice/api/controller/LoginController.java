package com.pomingmatgo.authservice.api.controller;

import com.pomingmatgo.authservice.global.ApiResponseDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/login")
public class LoginController {
    @GetMapping
    public ApiResponseDto<Void> handleLogin(String id, String password) {
        return new ApiResponseDto<>(HttpStatus.OK.value(), "로그인 성공");
    }
}
