package com.pomingmatgo.userservice.api.user.controller;

import com.pomingmatgo.userservice.api.user.request.RegisterRequest;
import com.pomingmatgo.userservice.domain.user.service.RegisterService;
import com.pomingmatgo.userservice.global.ApiResponseDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
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
    public ApiResponseDto<Void> checkEmailDuplicate(@NotBlank String email) {

        boolean isDuplicate = registerService.isEmailDuplicate(email);
        if(isDuplicate)
            return new ApiResponseDto<>(HttpStatus.CONFLICT.value(), "이메일이 중복됩니다.");
        else
            return new ApiResponseDto<>(HttpStatus.OK.value(), "이메일이 중복되지 않습니다.");
    }

    //중복 닉네임 체크
    @GetMapping("/check-nickname")
    public ApiResponseDto<Void> checkNicknameDuplicate(@NotBlank @Size(min = 2, max = 8) String nickname) {
        boolean isDuplicate = registerService.isNicknameDuplicate(nickname);
        if(isDuplicate)
            return new ApiResponseDto<>(HttpStatus.CONFLICT.value(), "닉네임이 중복됩니다.");
        else
            return new ApiResponseDto<>(HttpStatus.OK.value(), "닉네임이 중복되지 않습니다.");
    }

    //회원가입 폼 제출
    @PostMapping
    public ApiResponseDto<Void> handlePostRegister(@RequestBody @Valid RegisterRequest req) {
        registerService.register(req);
        return new ApiResponseDto<>(HttpStatus.OK.value(), req.getEmail()+"으로 인증 메일이 발송됐습니다.");
    }
}
