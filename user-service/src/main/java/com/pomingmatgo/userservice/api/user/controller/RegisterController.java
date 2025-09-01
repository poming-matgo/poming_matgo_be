package com.pomingmatgo.userservice.api.user.controller;

import com.pomingmatgo.userservice.api.user.request.RegisterRequest;
import com.pomingmatgo.userservice.domain.user.service.RegisterService;
import com.pomingmatgo.userservice.global.ApiResponseDto;
import com.pomingmatgo.userservice.global.exception.dto.ErrorResponseDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
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

    @Operation(description = "이메일이 중복인지 확인")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "입력한 이메일 사용 가능",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(example = """
                    {
                        "status": 200,
                        "message": "사용 가능한 이메일입니다."
                    }
                    """))),
            @ApiResponse(responseCode = "407", description = "이메일 중복",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(example = """
                    {
                        "status": 200,
                        "message": "이메일이 중복됩니다."
                    }
                    """))),
    })
    @GetMapping("/check-email")
    public ApiResponseDto<Void> checkEmailDuplicate(@NotBlank String email) {

        boolean isDuplicate = registerService.isEmailDuplicate(email);
        if(isDuplicate)
            return new ApiResponseDto<>(HttpStatus.CONFLICT.value(), "이메일이 중복됩니다.");
        else
            return new ApiResponseDto<>(HttpStatus.OK.value(), "이메일이 중복되지 않습니다.");
    }

    //중복 닉네임 체크
    @Operation(description = "닉네임이 중복인지 확인")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "입력한 닉네임 사용 가능",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(example = """
                    {
                        "status": 200,
                        "message": "사용 가능한 닉네임입니다."
                    }
                    """))),
            @ApiResponse(responseCode = "407", description = "닉네임 중복",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(example = """
                    {
                        "status": 200,
                        "message": "닉네임이 중복됩니다."
                    }
                    """))),
    })
    @GetMapping("/check-nickname")
    public ApiResponseDto<Void> checkNicknameDuplicate(@NotBlank @Size(min = 2, max = 8) String nickname) {
        boolean isDuplicate = registerService.isNicknameDuplicate(nickname);
        if(isDuplicate)
            return new ApiResponseDto<>(HttpStatus.CONFLICT.value(), "닉네임이 중복됩니다.");
        else
            return new ApiResponseDto<>(HttpStatus.OK.value(), "사용 가능한 닉네임입니다.");
    }

    //회원가입 폼 제출
    @Operation(description = "회원가입 폼 제출")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "인증 메일 발송",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(example = """
                    {
                        "status": 200,
                        "message": "grace527@naver.com으로 인증 메일이 발송됐습니다."
                    }
                    """))),
            @ApiResponse(responseCode = "407", description = "이메일 또는 닉네임 중복.",
                    content = @Content(mediaType = "application/json",
                            examples = {
                                    @ExampleObject(name = "DUPLICATE_EMAIL", value = """
                                    {
                                        "errorCode": "DUPLICATE_EMAIL",
                                        "httpStatus": 407,
                                        "errorMessage": "이메일이 중복됩니다."
                                    }
                                    """),
                                    @ExampleObject(name = "DUPLICATE_NICKNAME", value = """
                                    {
                                        "errorCode": "DUPLICATE_NICKNAME",
                                        "httpStatus": 407,
                                        "errorMessage": "닉네임이 중복됩니다."
                                    }
                                    """)
                            }))
    })
    @PostMapping
    public ApiResponseDto<Void> handlePostRegister(@RequestBody @Valid RegisterRequest req) {
        registerService.register(req);
        return new ApiResponseDto<>(HttpStatus.OK.value(), req.getEmail()+"으로 인증 메일이 발송됐습니다.");
    }
}
