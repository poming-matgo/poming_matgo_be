package com.pomingmatgo.userservice.api.user.request;

import com.pomingmatgo.userservice.domain.user.LoginType;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class SocialRegisterRequest {
    @NotBlank
    private String identifier;
    private LoginType loginType;
}
