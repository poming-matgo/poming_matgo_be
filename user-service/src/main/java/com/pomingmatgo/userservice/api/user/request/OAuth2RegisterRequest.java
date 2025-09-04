package com.pomingmatgo.userservice.api.user.request;

import com.pomingmatgo.userservice.domain.user.LoginType;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class OAuth2RegisterRequest {

    @NotBlank
    private String oauth2Id;

    @NotBlank
    private LoginType loginType;

    @NotBlank
    @Size(min = 2, max = 8)
    private String nickname;
}