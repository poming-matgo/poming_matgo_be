package com.pomingmatgo.userservice.api.user.request;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class CheckEmailRequest {
    @NotBlank
    @Email
    private String email;
}
