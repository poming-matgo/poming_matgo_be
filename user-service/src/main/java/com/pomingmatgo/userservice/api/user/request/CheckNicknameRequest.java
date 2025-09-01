package com.pomingmatgo.userservice.api.user.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class CheckNicknameRequest {
    @NotBlank
    @Size(min=2, max=8)
    private String nickName;
}
