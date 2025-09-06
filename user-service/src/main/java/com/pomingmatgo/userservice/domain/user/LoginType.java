package com.pomingmatgo.userservice.domain.user;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum LoginType {
    NATIVE("NATIVE_LOGIN", "자체 로그인"),
    NAVER("NAVER_LOGIN", "네이버 로그인"),
    GOOGLE("GOOGLE_LOGIN", "구글 로그인");

    private final String Code;
    private final String name;
}
