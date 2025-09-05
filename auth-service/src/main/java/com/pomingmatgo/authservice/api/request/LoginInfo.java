package com.pomingmatgo.authservice.api.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class LoginInfo {

    private String email;
    private String password;

    @JsonProperty("code_verifier")
    private String codeVerifier;
}