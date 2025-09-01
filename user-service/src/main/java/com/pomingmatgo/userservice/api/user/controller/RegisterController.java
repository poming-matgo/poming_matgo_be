package com.pomingmatgo.userservice.api.user.controller;

import com.pomingmatgo.userservice.api.user.request.CheckEmailRequest;
import com.pomingmatgo.userservice.api.user.request.CheckNicknameRequest;
import com.pomingmatgo.userservice.api.user.request.RegisterRequest;
import com.pomingmatgo.userservice.domain.user.service.RegisterService;
import com.pomingmatgo.userservice.global.ApiResponseDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/register")
public class RegisterController {
    RegisterService registerService;

    //중복 이메일 체크
    @GetMapping("/check-email")
    public ApiResponseDto<Void> checkEmailDuplicate(CheckEmailRequest req) {

        boolean isDuplicate = registerService.isEmailDuplicate(req.getEmail());
        if(isDuplicate)
            return new ApiResponseDto<Void>(HttpStatus.CONFLICT.value(), "이메일이 중복됩니다.");
        else
            return new ApiResponseDto<Void>(HttpStatus.OK.value(), "이메일이 중복되지 않습니다.");
    }

    //중복 닉네임 체크
    @GetMapping("/check-nickname")
    public ApiResponseDto<Void> checkNicknameDuplicate(CheckNicknameRequest req) {
        boolean isDuplicate = registerService.isNicknameDuplicate(req.getNickName());
        if(isDuplicate)
            return new ApiResponseDto<Void>(HttpStatus.CONFLICT.value(), "닉네임이 중복됩니다.");
        else
            return new ApiResponseDto<Void>(HttpStatus.OK.value(), "닉네임이 중복되지 않습니다.");
    }

    //회원가입 폼 제출
    @PostMapping
    public void handlePostRegister(RegisterRequest req) {
        registerService.register(req);
    }
}
