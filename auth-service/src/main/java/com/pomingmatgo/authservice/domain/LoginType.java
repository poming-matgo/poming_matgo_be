package com.pomingmatgo.authservice.domain;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum LoginType {
    NAVER("NAVER_LOGIN", "네이버 로그인"),
    GOOGLE("GOOGLE_LOGIN", "구글 로그인");

    private final String Code;
    private final String name;
}