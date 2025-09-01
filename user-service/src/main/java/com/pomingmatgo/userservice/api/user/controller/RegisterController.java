package com.pomingmatgo.userservice.api.user.controller;

import com.pomingmatgo.userservice.api.user.request.CheckEmailRequest;
import com.pomingmatgo.userservice.api.user.request.CheckNicknameRequest;
import com.pomingmatgo.userservice.api.user.request.RegisterRequest;
import com.pomingmatgo.userservice.domain.user.service.RegisterService;
import com.pomingmatgo.userservice.global.ApiResponseDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/register")
public class RegisterController {
    private final RegisterService registerService;

    //중복 이메일 체크
    @GetMapping("/check-email")
    public ApiResponseDto<Void> checkEmailDuplicate(@RequestParam @Valid CheckEmailRequest req) {

        boolean isDuplicate = registerService.isEmailDuplicate(req.getEmail());
        if(isDuplicate)
            return new ApiResponseDto<>(HttpStatus.CONFLICT.value(), "이메일이 중복됩니다.");
        else
            return new ApiResponseDto<>(HttpStatus.OK.value(), "이메일이 중복되지 않습니다.");
    }

    //중복 닉네임 체크
    @GetMapping("/check-nickname")
    public ApiResponseDto<Void> checkNicknameDuplicate(@RequestParam  @Valid CheckNicknameRequest req) {
        boolean isDuplicate = registerService.isNicknameDuplicate(req.getNickName());
        if(isDuplicate)
            return new ApiResponseDto<>(HttpStatus.CONFLICT.value(), "닉네임이 중복됩니다.");
        else
            return new ApiResponseDto<>(HttpStatus.OK.value(), "닉네임이 중복되지 않습니다.");
    }

    //회원가입 폼 제출
    @PostMapping
    public void handlePostRegister(@RequestBody @Valid RegisterRequest req) {
        registerService.register(req);
    }
}
